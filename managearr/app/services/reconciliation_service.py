"""Read-only Sonarr command/outcome reconciliation."""
from dataclasses import dataclass
from datetime import datetime, timezone

from ..adapters.sonarr_client import SonarrClient, SonarrError, SonarrNotFoundError
from ..persistence.outcome_repository import OutcomeRepository
from .library_readiness import ready_sonarr_library

BATCH_NOT_FOUND = "dispatch batch not found"
DRY_RUN_ERROR = "dry-run batches cannot be reconciled"
FAILED_BATCH_ERROR = "failed dispatch batches cannot be reconciled"
NO_COMMAND_ERROR = "no Sonarr command id was recorded; inspect Sonarr manually before retrying"
WRONG_STATE_ERROR = "dispatch batch is not eligible for reconciliation"
CROSS_LIBRARY_ERROR = "dispatch audit has ambiguous cross-library ownership"
NO_RESULT_GRACE_SECONDS = 15 * 60

COMMAND_TYPES = {
    "queued": ("command_queued", "nonterminal", "queued"),
    "pending": ("command_queued", "nonterminal", "queued"),
    "started": ("command_running", "nonterminal", "running"),
    "running": ("command_running", "nonterminal", "running"),
    "completed": ("command_completed", "terminal", "completed"),
    "failed": ("command_failed", "terminal", "failed"),
    "aborted": ("command_aborted", "terminal", "aborted"),
    "cancelled": ("command_aborted", "terminal", "aborted"),
}
HISTORY_TYPES = {
    "grabbed": ("grabbed", "nonterminal"),
    "downloadfolderimported": ("imported", "terminal"),
    "episodefileimported": ("imported", "terminal"),
    "downloadfailed": ("download_failed", "terminal"),
    "importfailed": ("import_failed", "terminal"),
}
# The order is the contract used for immediate results, stored summaries and detail.
OUTCOME_PRECEDENCE = {
    "no_result": 1, "grabbed": 2, "downloading": 2,
    "download_failed": 3, "import_failed": 3, "imported": 4,
}
FAILURE_TYPES = {"download_failed", "import_failed"}
TERMINAL_ITEM_TYPES = {"imported", *FAILURE_TYPES}
HUMAN_OUTCOME = {
    "pending": "Pending", "grabbed": "Grabbed/downloading",
    "downloading": "Grabbed/downloading", "imported": "Imported",
    "download_failed": "Failed", "import_failed": "Failed",
    "no_result": "No result",
}


@dataclass
class ReconciliationResult:
    batch: object
    attempt: object
    inserted_event_count: int
    item_results: list[dict]

    def to_dict(self) -> dict:
        return {
            "batch": self.batch.to_dict(), "attempt": self.attempt.to_dict(),
            "inserted_event_count": self.inserted_event_count,
            "item_results": self.item_results,
            "notice": "Reconciliation sends no search. Command completion is not evidence of a grab, download, or import.",
        }


def _event_dict(event) -> dict:
    if isinstance(event, dict):
        return event
    return {
        "dispatch_item_id": event.dispatch_item_id, "candidate_id": event.candidate_id,
        "episode_id": event.episode_id, "source_endpoint": event.source_endpoint,
        "event_type": event.event_type, "event_state": event.event_state,
        "safe_summary": event.safe_summary, "sonarr_command_id": event.sonarr_command_id,
        "sonarr_event_id": event.sonarr_event_id, "download_id": event.download_id,
        "id": event.id,
    }


def effective_item_event(events: list[dict]) -> dict | None:
    relevant = [event for event in events if event.get("event_type") in OUTCOME_PRECEDENCE]
    if not relevant:
        return None
    return max(relevant, key=lambda event: (
        OUTCOME_PRECEDENCE[event["event_type"]], event.get("sonarr_event_id") or 0,
        event.get("id") or 0,
    ))


class ReconciliationService:
    HISTORY_PAGE_SIZE = 50
    HISTORY_MAX_PAGES = 3
    QUEUE_PAGE_SIZE = 100
    QUEUE_MAX_PAGES = 3

    def __init__(self, dispatch_repo, outcome_repo: OutcomeRepository, library_repo,
                 activity_repo, candidate_repo, *, client_factory=SonarrClient,
                 timeout: int | None = None, clock=None):
        self.dispatch_repo = dispatch_repo
        self.outcome_repo = outcome_repo
        self.library_repo = library_repo
        self.activity_repo = activity_repo
        self.candidate_repo = candidate_repo
        self._client_factory = client_factory
        self._timeout = timeout
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _client(self, library):
        if self._timeout is None:
            return self._client_factory(library.url, library.api_key)
        return self._client_factory(library.url, library.api_key, timeout=self._timeout)

    def _validate(self, batch_id: int):
        batch = self.dispatch_repo.get_batch(batch_id)
        if batch is None:
            return None, None, BATCH_NOT_FOUND
        if batch.mode == "dry_run":
            return batch, None, DRY_RUN_ERROR
        if batch.state == "failed":
            return batch, None, FAILED_BATCH_ERROR
        if batch.state not in ("completed", "partial", "ambiguous"):
            return batch, None, WRONG_STATE_ERROR
        if batch.sonarr_command_id is None:
            return batch, None, NO_COMMAND_ERROR
        if batch.library_id is None:
            return batch, None, CROSS_LIBRARY_ERROR
        job = self.activity_repo.get(batch.scan_job_id)
        if job is None or job.library_id != batch.library_id or job.job_type != "sonarr_scan":
            return batch, None, CROSS_LIBRARY_ERROR
        library, error = ready_sonarr_library(self.library_repo, batch.library_id)
        if error:
            return batch, None, error
        relevant = [item for item in (batch.items or []) if item.state != "excluded"]
        candidates = {c.id: c for c in self.candidate_repo.get_many([i.candidate_id for i in relevant])}
        if any(i.candidate_id not in candidates or candidates[i.candidate_id].job_id != batch.scan_job_id
               or candidates[i.candidate_id].library_id != batch.library_id
               or candidates[i.candidate_id].episode_id != i.episode_id for i in relevant):
            return batch, None, CROSS_LIBRARY_ERROR
        return batch, library, None

    @staticmethod
    def _item_event(item, *, source, event_type, event_state, summary, evidence_key,
                    sonarr_event_id=None, download_id=None):
        return {"dispatch_item_id": item.id, "candidate_id": item.candidate_id,
                "episode_id": item.episode_id, "source_endpoint": source,
                "event_type": event_type, "event_state": event_state,
                "safe_summary": summary, "sonarr_event_id": sonarr_event_id,
                "download_id": download_id, "evidence_key": evidence_key}

    def _summarize(self, items, events, command_state, *, truncated=False):
        by_item = {item.id: [] for item in items}
        for event in events:
            if event.get("dispatch_item_id") in by_item:
                by_item[event["dispatch_item_id"]].append(event)
        results, effective = [], []
        for item in items:
            chosen = effective_item_event(by_item[item.id])
            outcome = chosen["event_type"] if chosen else "pending"
            effective.append(outcome)
            results.append({"dispatch_item_id": item.id, "candidate_id": item.candidate_id,
                            "episode_id": item.episode_id,
                            "latest_outcome": outcome if chosen else "unresolved",
                            "human_state": HUMAN_OUTCOME[outcome],
                            "terminal": outcome in TERMINAL_ITEM_TYPES or outcome == "no_result"})
        if truncated:
            return "partial", "Sonarr exceeded the bounded read limit; manual review is required.", results
        if effective and all(value == "no_result" for value in effective):
            return "no_result", "Completed search produced no grab, download, import, or failure evidence within 15 minutes.", results
        if effective and all(value in TERMINAL_ITEM_TYPES or value == "no_result" for value in effective):
            return "resolved", f"Definitive terminal outcomes were recorded for all {len(items)} dispatch items.", results
        if any(value != "pending" for value in effective):
            count = sum(value != "pending" for value in effective)
            return "partial", f"Outcome evidence was recorded for {count} of {len(items)} dispatch items; others remain pending.", results
        if command_state in ("failed", "aborted"):
            return "unresolved", "The command failed or was aborted; no per-episode outcome was inferred.", results
        if command_state == "completed":
            return "unresolved", "Search command completed; the 15-minute outcome grace period is still pending.", results
        return "unresolved", "Search command is pending or no related episode evidence was found.", results

    def reconcile(self, batch_id: int):
        batch, library, error = self._validate(batch_id)
        if error:
            return None, error
        items = [item for item in batch.items if item.state != "excluded"]
        by_episode = {item.episode_id: item for item in items}
        events, endpoint_reads = [], {"command": False, "history": False, "queue": False}
        command_state, truncated = "unknown", False
        dispatched_at = datetime.fromisoformat(batch.created_at)
        # Item finalization is the durable point at which Managearr knows the
        # accepted command became a completed local dispatch. Batch creation
        # precedes the network write and must not start the grace early.
        grace_started_at = max(
            (datetime.fromisoformat(item.updated_at) for item in items),
            default=dispatched_at,
        )
        try:
            client = self._client(library)
            try:
                command = client.get_command(batch.sonarr_command_id)
            except SonarrNotFoundError:
                # Sonarr may prune old commands. Only durable prior completion
                # authorizes continuing; 404 itself proves nothing.
                if batch.command_observed_state != "completed":
                    raise
                command_state = "completed"
            else:
                endpoint_reads["command"] = True
                event_type, event_state, command_state = COMMAND_TYPES.get(
                    command["status"], ("unknown", "unknown", "unknown"))
                events.append({"dispatch_item_id": None, "candidate_id": None,
                    "episode_id": None, "source_endpoint": "command",
                    "event_type": event_type, "event_state": event_state,
                    "safe_summary": f"EpisodeSearch command is {command_state}.",
                    "sonarr_command_id": command["id"], "sonarr_event_id": None,
                    "download_id": None,
                    "evidence_key": f"command:{command['id']}:{event_type}"})

            seen_history_ids = set()
            for episode_id, item in by_episode.items():
                for page in range(1, self.HISTORY_MAX_PAGES + 1):
                    history = client.get_history(page=page, page_size=self.HISTORY_PAGE_SIZE,
                                                 episode_id=episode_id)
                    endpoint_reads["history"] = True
                    for record in history["records"]:
                        if record["episode_id"] != episode_id or record["id"] in seen_history_ids:
                            continue
                        try:
                            event_at = datetime.fromisoformat(record["date"].replace("Z", "+00:00"))
                        except (AttributeError, ValueError):
                            continue
                        if event_at < dispatched_at:
                            continue
                        seen_history_ids.add(record["id"])
                        normalized = "".join(ch for ch in record["event_type"].lower() if ch.isalnum())
                        mapped_type, mapped_state = HISTORY_TYPES.get(normalized, ("unknown", "unknown"))
                        summary = {"grabbed": "Sonarr history records a grab.",
                            "imported": "Sonarr history records an import.",
                            "download_failed": "Sonarr history records a download failure.",
                            "import_failed": "Sonarr history records an import failure."}.get(
                                mapped_type, "Sonarr history contains an unrecognized related event.")
                        events.append(self._item_event(item, source="history", event_type=mapped_type,
                            event_state=mapped_state, summary=summary, sonarr_event_id=record["id"],
                            download_id=record["download_id"],
                            evidence_key=f"history:{record['id']}:{mapped_type}:{episode_id}"))
                    if page * self.HISTORY_PAGE_SIZE >= history["total_records"]:
                        break
                else:
                    truncated = True

            for page in range(1, self.QUEUE_MAX_PAGES + 1):
                queue = client.get_queue_details(page=page, page_size=self.QUEUE_PAGE_SIZE)
                endpoint_reads["queue"] = True
                for record in queue["records"]:
                    added = record.get("added")
                    if added:
                        try:
                            if datetime.fromisoformat(added.replace("Z", "+00:00")) < dispatched_at:
                                continue
                        except (AttributeError, ValueError):
                            continue
                    for episode_id in set(record["episode_ids"]):
                        item = by_episode.get(episode_id)
                        if item is None:
                            continue
                        tracked = (record["tracked_state"] or "").lower()
                        if record["status"] in ("failed", "warning"):
                            queue_type = "import_failed" if "import" in tracked else "download_failed"
                            queue_state, summary = "terminal", "Sonarr queue reports a failure."
                        elif record["status"] in ("queued", "downloading", "paused", "delay", "stalled"):
                            queue_type, queue_state = "downloading", "nonterminal"
                            summary = "Sonarr queue shows an active or pending download."
                        else:
                            queue_type, queue_state = "unknown", "unknown"
                            summary = "Sonarr queue contains an unrecognized related state."
                        key_download = record["download_id"] or "none"
                        events.append(self._item_event(item, source="queue", event_type=queue_type,
                            event_state=queue_state, summary=summary, download_id=record["download_id"],
                            evidence_key=f"queue:{key_download}:{episode_id}:{queue_type}:{record['status']}:{tracked}"))
                if page * self.QUEUE_PAGE_SIZE >= queue["total_records"]:
                    break
            else:
                truncated = True
        except SonarrError as exc:
            summary = str(exc)[:1000]
            attempt, inserted = self.outcome_repo.record_reconciliation(
                batch_id=batch.id, state="error", summary=summary, command_state=command_state,
                events=events, endpoint_reads=endpoint_reads)
            return ReconciliationResult(self.dispatch_repo.get_batch(batch.id), attempt, inserted, []), summary
        except Exception:
            summary = "unexpected error while reading reconciliation data from Sonarr"
            attempt, inserted = self.outcome_repo.record_reconciliation(
                batch_id=batch.id, state="error", summary=summary, command_state=command_state,
                events=events, endpoint_reads=endpoint_reads)
            return ReconciliationResult(self.dispatch_repo.get_batch(batch.id), attempt, inserted, []), summary

        persisted = [_event_dict(event) for event in self.outcome_repo.list_events(batch.id)]
        combined = persisted + events
        # Only locally finalized dispatched batches with durable/current command
        # completion can close. Ambiguous writes and bounded reads never do.
        if (not truncated and batch.state in ("completed", "partial")
                and (command_state == "completed" or batch.command_observed_state == "completed")
                and (self._clock() - grace_started_at).total_seconds() >= NO_RESULT_GRACE_SECONDS):
            for item in items:
                item_events = [event for event in combined if event.get("dispatch_item_id") == item.id]
                if effective_item_event(item_events) is None:
                    closure = self._item_event(item, source="reconciliation", event_type="no_result",
                        event_state="terminal", summary="No outcome evidence appeared within 15 minutes.",
                        evidence_key=f"no-result:{item.id}")
                    events.append(closure)
                    combined.append(closure)

        effective_command = command_state
        if effective_command == "unknown" and batch.command_observed_state:
            effective_command = batch.command_observed_state
        state, summary, item_results = self._summarize(
            items, combined, effective_command, truncated=truncated)
        attempt, inserted = self.outcome_repo.record_reconciliation(
            batch_id=batch.id, state=state, summary=summary, command_state=command_state,
            events=events, endpoint_reads=endpoint_reads)
        return ReconciliationResult(self.dispatch_repo.get_batch(batch.id), attempt,
                                    inserted, item_results), None

    def detail(self, batch_id: int):
        batch = self.dispatch_repo.get_batch(batch_id)
        if batch is None:
            return None
        events = self.outcome_repo.list_events(batch_id)
        attempts = self.outcome_repo.list_attempts(batch_id)
        by_item = {item.id: [] for item in (batch.items or [])}
        batch_events = []
        for event in events:
            data = event.to_dict()
            if event.dispatch_item_id is None:
                batch_events.append(data)
            elif event.dispatch_item_id in by_item:
                by_item[event.dispatch_item_id].append(data)
        items = []
        for item in batch.items or []:
            item_data = item.to_dict()
            item_data["outcomes"] = by_item.get(item.id, [])
            item_data["latest_outcome"] = effective_item_event(item_data["outcomes"])
            outcome_type = (item_data["latest_outcome"] or {}).get("event_type", "pending")
            item_data["human_state"] = HUMAN_OUTCOME[outcome_type]
            items.append(item_data)
        return {"batch": batch.to_dict(), "attempts": [a.to_dict() for a in attempts],
                "batch_events": batch_events, "items": items,
                "notice": "Reconciliation sends no search. A completed search command is not proof of a grab, download, or import."}

"""Manual, read-only Sonarr command/outcome reconciliation.

The service only follows a command id already recorded by the confirmed manual
dispatch path. Network reads occur before the short transaction that appends
normalized evidence and updates the dedicated reconciliation summary fields.
"""
from dataclasses import dataclass
from datetime import datetime

from ..adapters.sonarr_client import SonarrClient, SonarrError
from ..persistence.outcome_repository import OutcomeRepository
from .library_readiness import ready_sonarr_library

BATCH_NOT_FOUND = "dispatch batch not found"
DRY_RUN_ERROR = "dry-run batches cannot be reconciled"
FAILED_BATCH_ERROR = "failed dispatch batches cannot be reconciled"
NO_COMMAND_ERROR = "no Sonarr command id was recorded; inspect Sonarr manually before retrying"
WRONG_STATE_ERROR = "dispatch batch is not eligible for reconciliation"
CROSS_LIBRARY_ERROR = "dispatch audit has ambiguous cross-library ownership"

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
TERMINAL_ITEM_TYPES = {"imported", "download_failed", "import_failed"}


@dataclass
class ReconciliationResult:
    batch: object
    attempt: object
    inserted_event_count: int
    item_results: list[dict]

    def to_dict(self) -> dict:
        return {
            "batch": self.batch.to_dict(),
            "attempt": self.attempt.to_dict(),
            "inserted_event_count": self.inserted_event_count,
            "item_results": self.item_results,
            "notice": "Reconciliation sends no search. Command completion is not evidence of a grab, download, or import.",
        }


class ReconciliationService:
    HISTORY_PAGE_SIZE = 50
    HISTORY_MAX_PAGES = 3
    QUEUE_PAGE_SIZE = 100
    QUEUE_MAX_PAGES = 3

    def __init__(
        self,
        dispatch_repo,
        outcome_repo: OutcomeRepository,
        library_repo,
        activity_repo,
        candidate_repo,
        *,
        client_factory=SonarrClient,
        timeout: int | None = None,
    ):
        self.dispatch_repo = dispatch_repo
        self.outcome_repo = outcome_repo
        self.library_repo = library_repo
        self.activity_repo = activity_repo
        self.candidate_repo = candidate_repo
        self._client_factory = client_factory
        self._timeout = timeout

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
        candidates = {c.id: c for c in self.candidate_repo.get_many([item.candidate_id for item in relevant])}
        if any(
            item.candidate_id not in candidates
            or candidates[item.candidate_id].job_id != batch.scan_job_id
            or candidates[item.candidate_id].library_id != batch.library_id
            or candidates[item.candidate_id].episode_id != item.episode_id
            for item in relevant
        ):
            return batch, None, CROSS_LIBRARY_ERROR
        return batch, library, None

    @staticmethod
    def _item_event(item, *, source, event_type, event_state, summary, evidence_key,
                    sonarr_event_id=None, download_id=None):
        return {
            "dispatch_item_id": item.id,
            "candidate_id": item.candidate_id,
            "episode_id": item.episode_id,
            "source_endpoint": source,
            "event_type": event_type,
            "event_state": event_state,
            "safe_summary": summary,
            "sonarr_event_id": sonarr_event_id,
            "download_id": download_id,
            "evidence_key": evidence_key,
        }

    def reconcile(self, batch_id: int):
        batch, library, error = self._validate(batch_id)
        if error:
            return None, error

        items = [item for item in batch.items if item.state != "excluded"]
        by_episode = {item.episode_id: item for item in items}
        events: list[dict] = []
        endpoint_reads = {"command": False, "history": False, "queue": False}
        command_state = "unknown"
        truncated = False
        dispatched_at = datetime.fromisoformat(batch.created_at)

        try:
            client = self._client(library)
            command = client.get_command(batch.sonarr_command_id)
            endpoint_reads["command"] = True
            event_type, event_state, command_state = COMMAND_TYPES.get(
                command["status"], ("unknown", "unknown", "unknown")
            )
            events.append({
                "dispatch_item_id": None,
                "candidate_id": None,
                "episode_id": None,
                "source_endpoint": "command",
                "event_type": event_type,
                "event_state": event_state,
                "safe_summary": f"EpisodeSearch command is {command_state}.",
                "sonarr_command_id": command["id"],
                "sonarr_event_id": None,
                "download_id": None,
                "evidence_key": f"command:{command['id']}:{event_type}",
            })

            # Sonarr's history filter is singular, so read exactly one bounded
            # first page for each dispatched episode and still discard any
            # unrelated record if an upstream version ignores that filter.
            seen_history_ids = set()
            for episode_id, item in by_episode.items():
                for page in range(1, self.HISTORY_MAX_PAGES + 1):
                    history = client.get_history(
                        page=page, page_size=self.HISTORY_PAGE_SIZE, episode_id=episode_id
                    )
                    endpoint_reads["history"] = True
                    for record in history["records"]:
                        if record["episode_id"] != episode_id or record["id"] in seen_history_ids:
                            continue
                        try:
                            event_at = datetime.fromisoformat(record["date"].replace("Z", "+00:00"))
                        except (AttributeError, ValueError):
                            # A custom/test adapter may bypass the production
                            # shape validator. Never treat an undated event as
                            # evidence subsequent to this dispatch.
                            continue
                        if event_at < dispatched_at:
                            continue
                        seen_history_ids.add(record["id"])
                        normalized = "".join(ch for ch in record["event_type"].lower() if ch.isalnum())
                        mapped = HISTORY_TYPES.get(normalized, ("unknown", "unknown"))
                        mapped_type, mapped_state = mapped
                        summary = {
                            "grabbed": "Sonarr history records a grab.",
                            "imported": "Sonarr history records an import.",
                            "download_failed": "Sonarr history records a download failure.",
                            "import_failed": "Sonarr history records an import failure.",
                        }.get(mapped_type, "Sonarr history contains an unrecognized related event.")
                        events.append(self._item_event(
                            item, source="history", event_type=mapped_type,
                            event_state=mapped_state, summary=summary,
                            sonarr_event_id=record["id"], download_id=record["download_id"],
                            evidence_key=f"history:{record['id']}:{mapped_type}:{episode_id}",
                        ))
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
                            queue_state = "terminal"
                            summary = "Sonarr queue reports a failure."
                        elif record["status"] in ("queued", "downloading", "paused", "delay", "stalled"):
                            queue_type, queue_state = "downloading", "nonterminal"
                            summary = "Sonarr queue shows an active or pending download."
                        else:
                            queue_type, queue_state = "unknown", "unknown"
                            summary = "Sonarr queue contains an unrecognized related state."
                        key_download = record["download_id"] or "none"
                        events.append(self._item_event(
                            item, source="queue", event_type=queue_type,
                            event_state=queue_state, summary=summary,
                            download_id=record["download_id"],
                            evidence_key=f"queue:{key_download}:{episode_id}:{queue_type}:{record['status']}:{tracked}",
                        ))
                if page * self.QUEUE_PAGE_SIZE >= queue["total_records"]:
                    break
            else:
                truncated = True
        except SonarrError as exc:
            summary = str(exc)[:1000]
            attempt, inserted = self.outcome_repo.record_reconciliation(
                batch_id=batch.id, state="error", summary=summary,
                command_state=command_state, events=events, endpoint_reads=endpoint_reads,
            )
            return ReconciliationResult(
                batch=self.dispatch_repo.get_batch(batch.id), attempt=attempt,
                inserted_event_count=inserted, item_results=[]
            ), summary
        except Exception:
            summary = "unexpected error while reading reconciliation data from Sonarr"
            attempt, inserted = self.outcome_repo.record_reconciliation(
                batch_id=batch.id, state="error", summary=summary,
                command_state=command_state, events=events, endpoint_reads=endpoint_reads,
            )
            return ReconciliationResult(
                batch=self.dispatch_repo.get_batch(batch.id), attempt=attempt,
                inserted_event_count=inserted, item_results=[]
            ), summary

        evidence_by_item = {item.id: [] for item in items}
        for event in events:
            if event.get("dispatch_item_id") is not None:
                evidence_by_item[event["dispatch_item_id"]].append(event)
        item_results = []
        terminal_count = 0
        evidence_count = 0
        for item in items:
            item_events = evidence_by_item[item.id]
            terminal_events = [
                event for event in item_events if event["event_type"] in TERMINAL_ITEM_TYPES
            ]
            terminal_event = max(
                terminal_events,
                key=lambda event: (event.get("sonarr_event_id") or 0, item_events.index(event)),
                default=None,
            )
            latest = (
                terminal_event["event_type"] if terminal_event
                else item_events[-1]["event_type"] if item_events
                else "unresolved"
            )
            terminal_count += int(terminal_event is not None)
            evidence_count += int(bool(item_events))
            item_results.append({
                "dispatch_item_id": item.id,
                "candidate_id": item.candidate_id,
                "episode_id": item.episode_id,
                "latest_outcome": latest,
                "terminal": terminal_event is not None,
            })

        if items and terminal_count == len(items):
            state = "resolved"
            summary = f"Terminal import/failure evidence was found for all {len(items)} dispatch items."
        elif evidence_count or command_state in ("completed", "failed", "aborted"):
            state = "partial" if evidence_count else "unresolved"
            if command_state == "completed" and not evidence_count:
                summary = "Search command completed, but no grab, download, import, or failure evidence was found."
            else:
                summary = f"Evidence was found for {evidence_count} of {len(items)} dispatch items; unresolved items remain."
        else:
            state = "unresolved"
            summary = "The command is still pending or no related episode evidence was found."
        if truncated:
            summary = (summary + " Sonarr reported more records than the bounded read limit; the result is partial.")[:1000]
            if state == "unresolved":
                state = "partial"

        attempt, inserted = self.outcome_repo.record_reconciliation(
            batch_id=batch.id, state=state, summary=summary,
            command_state=command_state, events=events, endpoint_reads=endpoint_reads,
        )
        return ReconciliationResult(
            batch=self.dispatch_repo.get_batch(batch.id), attempt=attempt,
            inserted_event_count=inserted, item_results=item_results,
        ), None

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
            terminal = [
                event for event in item_data["outcomes"]
                if event["event_type"] in TERMINAL_ITEM_TYPES
            ]
            item_data["latest_outcome"] = (
                max(terminal, key=lambda event: (event.get("sonarr_event_id") or 0, event["id"]))
                if terminal else item_data["outcomes"][-1] if item_data["outcomes"] else None
            )
            items.append(item_data)
        return {
            "batch": batch.to_dict(),
            "attempts": [attempt.to_dict() for attempt in attempts],
            "batch_events": batch_events,
            "items": items,
            "notice": "Reconciliation sends no search. A completed search command is not proof of a grab, download, or import.",
        }

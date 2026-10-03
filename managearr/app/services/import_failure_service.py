"""Sonarr-only import-failure reason policy: reason-aware observation +
reason-gated automatic removal.

Scope is deliberately narrow - this is **not** a force-import feature and
adds **no new Sonarr write**: the only mutation is the exact same bounded
``DELETE /api/v3/queue/{id}`` the slow-download guard already uses (see
``app/adapters/sonarr_client.py:delete_queue_record``). This module only
decides *whether* to call it, never adds a new way to call Sonarr.

Monitoring (``poll_library``) uses ``ReadOnlySonarrClient`` exclusively,
exactly like ``SlowDownloadService``. Actual removal (``_attempt_removal``)
is the only place this module ever constructs a write-capable
``SonarrClient``, gated on auto_removal_enabled AND Live mode=live/running/
matching-generation/not emergency-stopped, re-checked immediately before
the DELETE under the same row-lock pattern as
``SlowDownloadService``/``SeasonPackService.confirm``. The slow-download
guard's own classification/evidence path is untouched by this module."""
from __future__ import annotations

from datetime import datetime, timezone

from ..adapters.read_only_sonarr_client import ReadOnlySonarrClient
from ..adapters.sonarr_client import (
    SonarrClient,
    SonarrError,
    SonarrPostAmbiguousError,
    SonarrPostRejectedError,
)
from ..domain.import_failure import (
    REMOVAL_SELECTABLE_REASON_KEYS,
    normalize_messages,
    should_observe,
)

QUEUE_MAX_PAGES = 10
QUEUE_PAGE_SIZE = 100

SETTINGS_RANGES = {
    "poll_seconds": (15, 3600),
}
BOOLEAN_FIELDS = ("monitoring_enabled", "auto_removal_enabled", "remove_from_client", "blocklist", "skip_redownload")
# False -> True on any of these makes an already-eligible removal more
# destructive without changing whether removal itself fires. True -> False
# is always protective and never needs confirmation.
_BOOLEAN_LOOSENING_WHEN_TRUE = frozenset({"remove_from_client", "blocklist", "skip_redownload"})


def _extract_messages(record: dict) -> list[str]:
    """Bounded raw message strings from one adapter-validated queue
    record: every status-message line plus the top-level error message,
    in the order Sonarr returned them.

    Deliberately excludes each ``status_messages`` entry's ``title`` -
    that field is the specific file/release name a message applies to
    (attacker/release-influenceable), never the rejection reason itself.
    Feeding it into normalization would risk a release name that happens
    to contain a reason-like word (e.g. "...Error...", "...Sample...")
    being misclassified as that reason."""
    messages: list[str] = []
    for entry in record.get("status_messages") or []:
        for m in entry.get("messages") or []:
            if isinstance(m, str) and m:
                messages.append(m)
    error_message = record.get("error_message")
    if isinstance(error_message, str) and error_message:
        messages.append(error_message)
    return messages


class ImportFailureService:
    def __init__(
        self, repo, library_repo, live_repo, scheduler_repo, *,
        client_factory=SonarrClient, read_only_client_factory=ReadOnlySonarrClient, timeout=None,
    ):
        self.repo = repo
        self.libraries = library_repo
        self.live_repo = live_repo
        self.scheduler_repo = scheduler_repo
        self.client_factory = client_factory
        self.read_only_client_factory = read_only_client_factory
        self.timeout = timeout

    def _client(self, library):
        return self.client_factory(library.url, library.api_key, **({"timeout": self.timeout} if self.timeout is not None else {}))

    def _read_only_client(self, library):
        return self.read_only_client_factory(
            library.url, library.api_key, **({"timeout": self.timeout} if self.timeout is not None else {})
        )

    # --- settings -----------------------------------------------------------

    def settings(self, library_id: int):
        library = self.libraries.get(library_id)
        if not library or library.type != "sonarr":
            return None
        return self.repo.settings(library_id)

    def update_settings(self, library_id: int, payload) -> tuple[dict | None, list[str]]:
        library = self.libraries.get(library_id)
        if not library or library.type != "sonarr":
            return None, ["Sonarr library not found"]
        if not isinstance(payload, dict):
            return None, ["request body must be an object"]
        current = self.repo.settings(library_id)
        expected_revision = payload.get("revision", current["revision"])
        if not isinstance(expected_revision, int) or isinstance(expected_revision, bool):
            return None, ["revision must be an integer"]
        data = {k: v for k, v in payload.items() if k not in ("revision", "removal_reasons", "confirm", "reason")}
        errors = []
        for field in BOOLEAN_FIELDS:
            if field in data and not isinstance(data[field], bool):
                errors.append(f"{field} must be a boolean")
        for field, (lo, hi) in SETTINGS_RANGES.items():
            if field in data:
                value = data[field]
                if not isinstance(value, int) or isinstance(value, bool) or not lo <= value <= hi:
                    errors.append(f"{field} must be an integer between {lo} and {hi}")
        removal_reasons = None
        if "removal_reasons" in payload:
            raw = payload["removal_reasons"]
            if not isinstance(raw, list) or any(not isinstance(x, str) for x in raw):
                errors.append("removal_reasons must be a list of strings")
            else:
                unknown = sorted(set(raw) - REMOVAL_SELECTABLE_REASON_KEYS)
                if unknown:
                    errors.append("removal_reasons contains unrecognized reason key(s): " + ", ".join(unknown))
                else:
                    removal_reasons = sorted(set(raw))
        if errors:
            return None, errors
        wants_monitoring = data.get("monitoring_enabled", current["monitoring_enabled"])
        wants_auto_removal = data.get("auto_removal_enabled", current["auto_removal_enabled"])
        if wants_monitoring is False and wants_auto_removal:
            # Disabling monitoring while automatic removal would stay/become
            # active is unsafe: with no fresh observations, an already-
            # stale remove_eligible item would still be eligible for an
            # automatic DELETE with nothing watching it. Disabling
            # monitoring always disables removal with it - a protective
            # transition, so it never itself requires confirm.
            data["auto_removal_enabled"] = False
            wants_auto_removal = False
        current_reasons = set(current["removal_reasons"])
        new_reasons = set(removal_reasons) if removal_reasons is not None else current_reasons
        added_reasons = new_reasons - current_reasons
        enabling_auto_removal = wants_auto_removal and not current["auto_removal_enabled"]
        boolean_loosening = any(
            field in data and data[field] is True and current[field] is False
            for field in _BOOLEAN_LOOSENING_WHEN_TRUE
        )
        needs_confirm = enabling_auto_removal or bool(added_reasons) or (wants_auto_removal and boolean_loosening)
        if needs_confirm:
            if not isinstance(payload.get("confirm"), bool) or payload.get("confirm") is not True:
                return None, ["confirm must be true to enable automatic removal, select a new removal reason, or loosen removal behavior"]
            reason_text = payload.get("reason")
            if not isinstance(reason_text, str) or not reason_text.strip():
                return None, ["reason is required to enable automatic removal, select a new removal reason, or loosen removal behavior"]
        else:
            reason_text = payload.get("reason") if isinstance(payload.get("reason"), str) else ""
        # Live-armed is only required for a change that actually makes
        # automatic removal more capable right now: enabling it, or
        # expanding/loosening it while it is (or will be) enabled. Merely
        # curating the reason list while the master switch stays off is
        # harmless - nothing can be deleted until auto_removal_enabled is
        # also true - so it only needs confirm+reason, never Live armed.
        needs_live_check = enabling_auto_removal or (wants_auto_removal and (added_reasons or boolean_loosening))
        if needs_live_check:
            # Fast, friendly rejection for the common case - not atomic
            # with the write below, so never the sole gate: see
            # require_live_running on repo.update_settings, which
            # rechecks this inside the same transaction as the commit.
            live_status = self._live_status()
            if not live_status["allowed"]:
                return None, ["automatic removal cannot be enabled or expanded unless Live is armed and running: " + "; ".join(live_status["reasons"])]
        updated, error = self.repo.update_settings(
            library_id, data, removal_reasons=removal_reasons, expected_revision=expected_revision,
            require_live_running=needs_live_check, confirm=bool(payload.get("confirm")), reason=reason_text or "",
        )
        if error:
            return None, [error]
        return updated, []

    # --- monitoring -----------------------------------------------------------

    def poll_library(self, library_id: int, *, heartbeat=None) -> dict:
        """Bounded, read-only poll of one Sonarr library's queue. Never
        mutates Sonarr. Returns a summary dict for worker logging/tests."""
        library = self.libraries.get(library_id)
        if not library or library.type != "sonarr" or not library.enabled:
            return {"library_id": library_id, "polled": 0, "error": "library not available"}
        settings = self.repo.settings(library_id)
        if not settings["monitoring_enabled"]:
            return {"library_id": library_id, "polled": 0, "skipped": "monitoring disabled"}
        client = self._read_only_client(library)
        present_queue_ids = set()
        observed = 0
        queue_complete = False
        lease_lost = False
        try:
            for page in range(1, QUEUE_MAX_PAGES + 1):
                result = client.get_queue_details(page=page, page_size=QUEUE_PAGE_SIZE)
                for record in result["records"]:
                    present_queue_ids.add(record["queue_id"])
                    watched = should_observe(
                        status=record.get("status"), tracked_state=record.get("tracked_state"),
                        tracked_status=record.get("tracked_status"),
                    )
                    if watched:
                        messages = _extract_messages(record)
                        normalization = normalize_messages(messages)
                        if normalization.has_any_evidence:
                            self.repo.record_observation(library_id, record, normalization, settings)
                            observed += 1
                    else:
                        self.repo.resolve_no_longer_watched(library_id, record)
                if page * QUEUE_PAGE_SIZE >= result["total_records"]:
                    queue_complete = True
                    break
                if heartbeat is not None and not heartbeat():
                    lease_lost = True
                    break
        except SonarrError as exc:
            return {"library_id": library_id, "observed": observed, "error": str(exc)}
        if lease_lost:
            return {
                "library_id": library_id, "observed": observed, "cleared": 0,
                "error": "scheduler lease lost during bounded queue read; disappearance reconciliation skipped",
            }
        if not queue_complete:
            return {
                "library_id": library_id, "observed": observed, "cleared": 0,
                "error": f"Sonarr queue exceeded the bounded {QUEUE_MAX_PAGES * QUEUE_PAGE_SIZE}-record read; disappearance reconciliation skipped",
            }
        cleared = self.repo.clear_disappeared(library_id, present_queue_ids)
        return {"library_id": library_id, "observed": observed, "cleared": cleared}

    def poll_all(self, *, heartbeat=None) -> list[dict]:
        """Bounded periodic read of every enabled Sonarr library, gated per
        library by its configured ``poll_seconds`` cadence."""
        results = []
        now = datetime.now(timezone.utc)
        for settings in self.repo.all_sonarr_settings():
            if heartbeat is not None and not heartbeat():
                break
            if not settings["monitoring_enabled"]:
                continue
            last_polled = self.repo.last_polled_at(settings["library_id"])
            if last_polled is not None and (now - last_polled).total_seconds() < settings["poll_seconds"]:
                continue
            self.repo.record_poll_attempt(settings["library_id"])
            results.append(self.poll_library(settings["library_id"], heartbeat=heartbeat))
        return results

    # --- gated automatic removal (worker-only) --------------------------------

    def _live_status(self) -> dict:
        mode = self.scheduler_repo.get_settings().mode
        control = self.live_repo.get_control()
        reasons = []
        if mode != "live":
            reasons.append("scheduler mode is not live")
        if control["state"] != "running":
            reasons.append("Live authorization is paused or emergency-stopped")
        return {"allowed": not reasons, "reasons": reasons, "generation": control.get("authorization_generation", 0)}

    def attempt_removals(self, *, max_removals: int = 1, heartbeat=None) -> list[dict]:
        """At most ``max_removals`` gated DELETEs per call. Never raises on
        an individual item's Sonarr error; each outcome is durably
        recorded instead."""
        if heartbeat is not None and not heartbeat():
            return []
        live_status = self._live_status()
        if not live_status["allowed"]:
            return []
        outcomes = []
        for settings in self.repo.all_sonarr_settings():
            if len(outcomes) >= max_removals:
                break
            if not settings["auto_removal_enabled"]:
                continue
            library = self.libraries.get(settings["library_id"])
            if not library or library.type != "sonarr" or not library.enabled:
                continue
            for item in self.repo.list_current(settings["library_id"]):
                if item["decision"] != "remove_eligible":
                    continue
                if heartbeat is not None and not heartbeat():
                    return outcomes
                outcome = self._attempt_removal(library, item, live_status["generation"])
                if outcome is not None:
                    outcomes.append(outcome)
                if len(outcomes) >= max_removals:
                    break
        return outcomes

    def _attempt_removal(self, library, item: dict, expected_generation: int) -> dict | None:
        with self.repo.authorized_removal(
            item["id"], expected_generation, expected_url=library.url, expected_api_key=library.api_key
        ) as (conn, row, error):
            if error:
                return None
            # Re-read Sonarr while authorization/settings/library locks are
            # held. Stored observations alone are never sufficient for a
            # destructive action: prove the exact queue record still
            # exists, still has the same download identity, is still in a
            # watched import-blocked/warning state, and re-derive its
            # normalized reasons/decision fresh - never trusting the
            # stored decision alone.
            try:
                current = self._find_live_queue_record(library, row["sonarr_queue_id"])
            except SonarrError as exc:
                self.repo.abort_revalidated_removal(
                    row, "ambiguous", f"final queue revalidation unavailable: {exc}", conn=conn
                )
                return None
            if current is None:
                self.repo.abort_revalidated_removal(
                    row, "removed", "queue record no longer present during final revalidation; outcome unknown", conn=conn
                )
                return None
            if row.get("download_id") and current.get("download_id") != row["download_id"]:
                self.repo.abort_revalidated_removal(
                    row, "ambiguous", "queue record download identity changed before removal", conn=conn
                )
                return None
            watched = should_observe(
                status=current.get("status"), tracked_state=current.get("tracked_state"),
                tracked_status=current.get("tracked_status"),
            )
            if not watched:
                self.repo.abort_revalidated_removal(
                    row, "resolved", "queue record is no longer in a watched import-blocked/warning state", conn=conn
                )
                return None
            fresh_normalization = normalize_messages(_extract_messages(current))
            stored_reasons = frozenset(row.get("matched_reasons") or [])
            if fresh_normalization.matched_reasons != stored_reasons or fresh_normalization.unmatched_messages:
                self.repo.abort_revalidated_removal(
                    row, "ambiguous", "observed reasons changed before removal; evidence reset", conn=conn
                )
                return None
            if stored_reasons - row.get("selected_reasons", frozenset()):
                self.repo.abort_revalidated_removal(
                    row, "leave", "a matched reason is no longer selected for automatic removal", conn=conn
                )
                return None
            self.repo.mark_removal_attempt_started(row, conn=conn)
            client = self._client(library)
            try:
                client.delete_queue_record(
                    row["sonarr_queue_id"],
                    remove_from_client=row["remove_from_client"],
                    blocklist=row["blocklist"],
                    skip_redownload=row["skip_redownload"],
                )
            except SonarrPostRejectedError as exc:
                return self.repo.finalize_removal(item["id"], "rejected", str(exc), conn=conn)
            except SonarrPostAmbiguousError as exc:
                return self.repo.finalize_removal(item["id"], "ambiguous", str(exc), conn=conn)
            except Exception:
                return self.repo.finalize_removal(item["id"], "ambiguous", "unexpected error after removal attempt", conn=conn)
            return self.repo.finalize_removal(item["id"], "completed", "removed by import-failure reason policy", conn=conn)

    def _find_live_queue_record(self, library, queue_id: int) -> dict | None:
        """Bounded read-only proof for one exact Sonarr queue record. None
        is returned only after a complete queue enumeration proves
        absence; an over-bound queue raises and therefore fails closed."""
        client = self._read_only_client(library)
        for page in range(1, QUEUE_MAX_PAGES + 1):
            result = client.get_queue_details(page=page, page_size=QUEUE_PAGE_SIZE)
            for record in result["records"]:
                if record["queue_id"] == queue_id:
                    return record
            if page * QUEUE_PAGE_SIZE >= result["total_records"]:
                return None
        raise SonarrError("Sonarr queue exceeded the bounded final revalidation read")

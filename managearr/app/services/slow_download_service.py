"""Sonarr-only slow-download guard: monitoring + gated automatic removal.

Monitoring (``poll_library``) uses ``ReadOnlySonarrClient`` exclusively - a
structural guard (see ``app/adapters/read_only_sonarr_client.py``) that
raises instead of reaching the network for anything but a handful of GET
methods. Actual removal (``_attempt_removal``) is the only place this module
ever constructs a write-capable ``SonarrClient``, and it only ever runs from
the worker process, gated on auto_removal_enabled AND Live
mode=live/running/matching-generation/not emergency-stopped, re-checked
immediately before the DELETE under the same row-lock pattern as
``SeasonPackService.confirm``.
"""
from __future__ import annotations

from datetime import datetime, timezone

from ..adapters.read_only_sonarr_client import ReadOnlySonarrClient
from ..adapters.sonarr_client import (
    SonarrClient,
    SonarrError,
    SonarrPostAmbiguousError,
    SonarrPostRejectedError,
)
from ..domain.slow_download import is_exempt_tracked

QUEUE_MAX_PAGES = 10
QUEUE_PAGE_SIZE = 100

SETTINGS_RANGES = {
    "poll_seconds": (15, 3600),
    "initial_grace_minutes": (0, 1440),
    "no_progress_window_minutes": (5, 1440),
    "no_progress_min_observations": (2, 100),
    "progress_epsilon_bytes": (65536, 1073741824),
    "very_slow_rate_bytes_per_second": (1024, 104857600),
    "very_slow_window_minutes": (15, 2880),
    "very_slow_min_remaining_bytes": (0, 1099511627776),
    "very_slow_min_observations": (2, 200),
    "strikes_required": (1, 10),
}
BOOLEAN_FIELDS = ("monitoring_enabled", "auto_removal_enabled", "remove_from_client", "blocklist", "skip_redownload")


class SlowDownloadService:
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
        if "revision" not in payload:
            # Optimistic concurrency is only meaningful if every caller is
            # forced to prove they have actually seen current settings -
            # an omitted revision must never silently fall back to
            # whatever this preliminary read happens to return, or the
            # check becomes a no-op for any caller that skips it.
            return None, ["revision is required"]
        expected_revision = payload["revision"]
        if not isinstance(expected_revision, int) or isinstance(expected_revision, bool):
            return None, ["revision must be an integer"]
        data = {k: v for k, v in payload.items() if k != "revision"}
        errors = []
        for field in BOOLEAN_FIELDS:
            if field in data and not isinstance(data[field], bool):
                errors.append(f"{field} must be a boolean")
        for field, (lo, hi) in SETTINGS_RANGES.items():
            if field in data:
                value = data[field]
                if not isinstance(value, int) or isinstance(value, bool) or not lo <= value <= hi:
                    errors.append(f"{field} must be an integer between {lo} and {hi}")
        if errors:
            return None, errors
        wants_monitoring = data.get("monitoring_enabled", current["monitoring_enabled"])
        wants_auto_removal = data.get("auto_removal_enabled", current["auto_removal_enabled"])
        if wants_monitoring is False and wants_auto_removal:
            # Disabling monitoring while automatic removal would stay/become
            # active is unsafe: with no fresh observations, any already-
            # stale removal_pending item would still be eligible for an
            # automatic DELETE with nothing watching it. Disabling
            # monitoring always disables removal with it - a protective
            # transition, so it never itself requires confirm.
            data["auto_removal_enabled"] = False
            wants_auto_removal = False
        enabling_auto_removal = wants_auto_removal and not current["auto_removal_enabled"]
        loosening = any(
            field in data and self._is_loosening(field, current[field], data[field])
            for field in SETTINGS_RANGES
        )
        boolean_loosening = any(
            field in data and data[field] is True and current[field] is False
            for field in self._BOOLEAN_LOOSENING_WHEN_TRUE
        )
        if enabling_auto_removal or (wants_auto_removal and (loosening or boolean_loosening)):
            if not isinstance(payload.get("confirm"), bool) or payload.get("confirm") is not True:
                return None, ["confirm must be true to enable or loosen automatic removal"]
            reason = payload.get("reason")
            if not isinstance(reason, str) or not reason.strip():
                return None, ["reason is required to enable or loosen automatic removal"]
        if enabling_auto_removal:
            # Fast, friendly rejection for the common case - but this is
            # only a preliminary check. It is not atomic with the write
            # below, so it must never be the sole gate: see
            # ``require_live_running`` on ``repo.update_settings``, which
            # rechecks this inside the same transaction as the commit.
            live_status = self._live_status()
            if not live_status["allowed"]:
                return None, ["automatic removal cannot be enabled unless Live is armed and running: " + "; ".join(live_status["reasons"])]
        updated, error = self.repo.update_settings(
            library_id, data, expected_revision=expected_revision, require_live_running=enabling_auto_removal
        )
        if error:
            return None, [error]
        return updated, []

    # "Loosening" means a change makes automatic removal easier/more
    # aggressive to trigger (less protective), regardless of which raw
    # direction the number itself moves.
    _LOOSENING_WHEN_SMALLER = frozenset({
        # Shorter windows/grace, fewer required observations, fewer
        # required strikes, or a lower remaining-bytes floor (so
        # near-finished downloads become eligible too) all make removal
        # easier to trigger.
        "no_progress_window_minutes", "initial_grace_minutes", "very_slow_window_minutes",
        "no_progress_min_observations", "very_slow_min_observations",
        "very_slow_min_remaining_bytes", "strikes_required",
    })
    _LOOSENING_WHEN_LARGER = frozenset({
        # A bigger epsilon makes it harder for a real decrease to count as
        # meaningful progress, so stall evidence accumulates more easily. A
        # higher throughput floor classifies more downloads as "too slow".
        "progress_epsilon_bytes", "very_slow_rate_bytes_per_second",
    })
    # False -> True on any of these makes an already-eligible removal more
    # destructive (removes the download from the client too, blocklists the
    # release, or skips the automatic redownload) without changing whether
    # removal itself fires. True -> False is always protective and never
    # needs confirmation.
    _BOOLEAN_LOOSENING_WHEN_TRUE = frozenset({"remove_from_client", "blocklist", "skip_redownload"})

    @classmethod
    def _is_loosening(cls, field: str, old_value: int, new_value: int) -> bool:
        if field in cls._LOOSENING_WHEN_SMALLER:
            return new_value < old_value
        if field in cls._LOOSENING_WHEN_LARGER:
            return new_value > old_value
        return False

    # --- monitoring -----------------------------------------------------------

    def poll_library(self, library_id: int, *, heartbeat=None) -> dict:
        """Bounded, read-only poll of one Sonarr library's queue. Never
        mutates Sonarr. Returns a summary dict for worker logging/tests.

        ``heartbeat``, if given, is called after every fetched page (a
        bounded poll can issue up to ``QUEUE_MAX_PAGES`` GETs for one
        library) so the worker's scheduler lease is renewed during a long
        multi-page read instead of only once per whole iteration. If it
        returns falsy - the lease was lost - the read stops immediately and
        is treated exactly like a truncated/bounded read: no disappearance
        reconciliation runs on an incomplete snapshot.

        Every page is first collected into memory (bounded to at most
        ``QUEUE_MAX_PAGES * QUEUE_PAGE_SIZE`` records) *before* any
        record is classified: an incomplete snapshot - the queue exceeded
        the bound, Sonarr errored mid-read, or the lease was lost - must
        suppress classification entirely, not just disappearance
        reconciliation, since a partial read not only can't prove
        disappearance, it also isn't a safe basis for scoring individual
        records either."""
        library = self.libraries.get(library_id)
        if not library or library.type != "sonarr" or not library.enabled:
            return {"library_id": library_id, "polled": 0, "error": "library not available"}
        settings = self.repo.settings(library_id)
        if not settings["monitoring_enabled"]:
            return {"library_id": library_id, "polled": 0, "skipped": "monitoring disabled"}
        client = self._read_only_client(library)
        fetched_records = []
        queue_complete = False
        lease_lost = False
        try:
            for page in range(1, QUEUE_MAX_PAGES + 1):
                result = client.get_queue_details(page=page, page_size=QUEUE_PAGE_SIZE)
                fetched_records.extend(result["records"])
                if page * QUEUE_PAGE_SIZE >= result["total_records"]:
                    queue_complete = True
                    break
                if heartbeat is not None and not heartbeat():
                    lease_lost = True
                    break
        except SonarrError as exc:
            # Fail closed: any telemetry uncertainty from this poll never
            # triggers removal logic below, and existing evidence is left
            # untouched rather than guessed at.
            return {"library_id": library_id, "polled": 0, "error": str(exc)}
        if lease_lost:
            return {
                "library_id": library_id, "polled": 0, "cleared": 0,
                "error": "scheduler lease lost during bounded queue read; disappearance reconciliation skipped",
            }
        if not queue_complete:
            return {
                "library_id": library_id, "polled": 0, "cleared": 0,
                "error": f"Sonarr queue exceeded the bounded {QUEUE_MAX_PAGES * QUEUE_PAGE_SIZE}-record read; classification skipped",
            }
        present_queue_ids = set()
        polled = 0
        for record in fetched_records:
            present_queue_ids.add(record["queue_id"])
            self.repo.record_observation(library_id, record, settings)
            polled += 1
        # queue_complete is guaranteed True here (checked above, before any
        # classification ran) - disappearance reconciliation always runs
        # against a complete snapshot, never a bounded/truncated one.
        cleared = self.repo.clear_disappeared(library_id, present_queue_ids)
        return {"library_id": library_id, "polled": polled, "cleared": cleared}

    def poll_all(self, *, heartbeat=None) -> list[dict]:
        """Bounded periodic read of every enabled Sonarr library, gated per
        library by its configured ``poll_seconds`` cadence - callers that
        want to force an immediate poll regardless of cadence should call
        ``poll_library`` directly instead (used by tests and any future
        manual "poll now" trigger).

        ``heartbeat`` is forwarded to ``poll_library`` (renewing the lease
        during each library's multi-page read) and is also checked between
        libraries; once it reports the lease lost, remaining libraries are
        skipped for this cycle rather than polled under an unowned lease."""
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
        """At most ``max_removals`` gated DELETEs per call - the worker calls
        this once per poll iteration. Never raises on an individual item's
        Sonarr error; each outcome is durably recorded instead.

        ``heartbeat``, if given, is verified immediately before this method
        does anything else, and again immediately before each individual
        removal attempt: a worker whose scheduler lease was lost during the
        (possibly long, multi-page) poll that preceded this call must never
        go on to issue a DELETE under a lease it no longer holds."""
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
                if item["classification"] != "removal_pending":
                    continue
                if heartbeat is not None and not heartbeat():
                    return outcomes
                outcome = self._attempt_removal(library, item, live_status["generation"], heartbeat=heartbeat)
                if outcome is not None:
                    outcomes.append(outcome)
                if len(outcomes) >= max_removals:
                    break
        return outcomes

    def _attempt_removal(self, library, item: dict, expected_generation: int, *, heartbeat=None) -> dict | None:
        with self.repo.authorized_removal(
            item["id"], expected_generation, expected_url=library.url, expected_api_key=library.api_key
        ) as (conn, row, error):
            if error:
                return None
            # Re-read Sonarr while authorization/settings/library locks are
            # held. Stored observations alone are never sufficient for a
            # destructive action: prove the exact queue record still exists,
            # is actively downloading with bytes remaining, has the same
            # download identity, and has not made meaningful progress.
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
            if current["status"] != "downloading" or current.get("sizeleft") is None or current["sizeleft"] <= 0:
                self.repo.abort_revalidated_removal(
                    row, "exempt", f"queue record is now {current['status']} or has no bytes remaining", conn=conn
                )
                return None
            if is_exempt_tracked(current.get("tracked_state"), current.get("tracked_status")):
                self.repo.abort_revalidated_removal(
                    row, "exempt",
                    f"trackedDownloadState '{current.get('tracked_state')}'/trackedDownloadStatus "
                    f"'{current.get('tracked_status')}' is hard-exempt from removal",
                    conn=conn,
                )
                return None
            if row.get("download_id") and current.get("download_id") != row["download_id"]:
                self.repo.abort_revalidated_removal(
                    row, "ambiguous", "queue record download identity changed before removal", conn=conn
                )
                return None
            stored_remaining = row.get("sizeleft_bytes")
            if stored_remaining is None:
                self.repo.abort_revalidated_removal(
                    row, "ambiguous", "stored remaining-byte evidence is unavailable", conn=conn
                )
                return None
            decrease = stored_remaining - current["sizeleft"]
            if decrease >= row["progress_epsilon_bytes"] or decrease < 0:
                self.repo.abort_revalidated_removal(
                    row, "healthy", "remaining bytes changed materially before removal; evidence reset", conn=conn
                )
                return None
            # The final live queue read above can issue up to
            # QUEUE_MAX_PAGES network round trips; re-verify this worker
            # still owns its scheduler lease at the destructive boundary -
            # immediately before the durable marker and the DELETE - rather
            # than trusting the single heartbeat check that preceded the
            # (possibly long) revalidation read. A lost lease fails closed:
            # no marker, no DELETE, and the item is left exactly as found
            # for whichever worker now holds the lease to reconsider.
            if heartbeat is not None and not heartbeat():
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
            return self.repo.finalize_removal(item["id"], "completed", "removed by slow-download guard", conn=conn)

    def _find_live_queue_record(self, library, queue_id: int) -> dict | None:
        """Bounded read-only proof for one exact Sonarr queue record. None is
        returned only after a complete queue enumeration proves absence; an
        over-bound queue raises and therefore fails closed."""
        client = self._read_only_client(library)
        for page in range(1, QUEUE_MAX_PAGES + 1):
            result = client.get_queue_details(page=page, page_size=QUEUE_PAGE_SIZE)
            for record in result["records"]:
                if record["queue_id"] == queue_id:
                    return record
            if page * QUEUE_PAGE_SIZE >= result["total_records"]:
                return None
        raise SonarrError("Sonarr queue exceeded the bounded final revalidation read")

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
        expected_revision = payload.get("revision", current["revision"])
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
        wants_auto_removal = data.get("auto_removal_enabled", current["auto_removal_enabled"])
        enabling_auto_removal = wants_auto_removal and not current["auto_removal_enabled"]
        loosening = any(
            field in data and self._is_loosening(field, current[field], data[field])
            for field in SETTINGS_RANGES
        )
        if enabling_auto_removal or (wants_auto_removal and loosening):
            if not isinstance(payload.get("confirm"), bool) or payload.get("confirm") is not True:
                return None, ["confirm must be true to enable or loosen automatic removal"]
            reason = payload.get("reason")
            if not isinstance(reason, str) or not reason.strip():
                return None, ["reason is required to enable or loosen automatic removal"]
        if enabling_auto_removal:
            live_status = self._live_status()
            if not live_status["allowed"]:
                return None, ["automatic removal cannot be enabled unless Live is armed and running: " + "; ".join(live_status["reasons"])]
        updated, error = self.repo.update_settings(library_id, data, expected_revision=expected_revision)
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

    @classmethod
    def _is_loosening(cls, field: str, old_value: int, new_value: int) -> bool:
        if field in cls._LOOSENING_WHEN_SMALLER:
            return new_value < old_value
        if field in cls._LOOSENING_WHEN_LARGER:
            return new_value > old_value
        return False

    # --- monitoring -----------------------------------------------------------

    def poll_library(self, library_id: int) -> dict:
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
        polled = 0
        try:
            for page in range(1, QUEUE_MAX_PAGES + 1):
                result = client.get_queue_details(page=page, page_size=QUEUE_PAGE_SIZE)
                for record in result["records"]:
                    present_queue_ids.add(record["queue_id"])
                    self.repo.record_observation(library_id, record, settings)
                    polled += 1
                if page * QUEUE_PAGE_SIZE >= result["total_records"]:
                    break
        except SonarrError as exc:
            # Fail closed: any telemetry uncertainty from this poll never
            # triggers removal logic below, and existing evidence is left
            # untouched rather than guessed at.
            return {"library_id": library_id, "polled": polled, "error": str(exc)}
        cleared = self.repo.clear_disappeared(library_id, present_queue_ids)
        return {"library_id": library_id, "polled": polled, "cleared": cleared}

    def poll_all(self) -> list[dict]:
        """Bounded periodic read of every enabled Sonarr library, gated per
        library by its configured ``poll_seconds`` cadence - callers that
        want to force an immediate poll regardless of cadence should call
        ``poll_library`` directly instead (used by tests and any future
        manual "poll now" trigger)."""
        results = []
        now = datetime.now(timezone.utc)
        for settings in self.repo.all_sonarr_settings():
            if not settings["monitoring_enabled"]:
                continue
            last_polled = self.repo.last_polled_at(settings["library_id"])
            if last_polled is not None and (now - last_polled).total_seconds() < settings["poll_seconds"]:
                continue
            self.repo.record_poll_attempt(settings["library_id"])
            results.append(self.poll_library(settings["library_id"]))
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

    def attempt_removals(self, *, max_removals: int = 1) -> list[dict]:
        """At most ``max_removals`` gated DELETEs per call - the worker calls
        this once per poll iteration. Never raises on an individual item's
        Sonarr error; each outcome is durably recorded instead."""
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
                outcome = self._attempt_removal(library, item, live_status["generation"])
                if outcome is not None:
                    outcomes.append(outcome)
                if len(outcomes) >= max_removals:
                    break
        return outcomes

    def _attempt_removal(self, library, item: dict, expected_generation: int) -> dict | None:
        settings = self.repo.settings(library.id)
        with self.repo.authorized_removal(item["id"], expected_generation) as (conn, row, error):
            if error:
                return None
            client = self._client(library)
            try:
                client.delete_queue_record(
                    row["sonarr_queue_id"],
                    remove_from_client=settings["remove_from_client"],
                    blocklist=settings["blocklist"],
                    skip_redownload=settings["skip_redownload"],
                )
            except SonarrPostRejectedError as exc:
                return self.repo.finalize_removal(item["id"], "rejected", str(exc), conn=conn)
            except SonarrPostAmbiguousError as exc:
                return self.repo.finalize_removal(item["id"], "ambiguous", str(exc), conn=conn)
            except Exception:
                return self.repo.finalize_removal(item["id"], "ambiguous", "unexpected error after removal attempt", conn=conn)
            return self.repo.finalize_removal(item["id"], "completed", "removed by slow-download guard", conn=conn)

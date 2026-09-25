"""Restart-safe simulation planning. This module has no Sonarr adapter dependency."""
from __future__ import annotations

from datetime import datetime, timezone
from random import Random

from ..domain.scheduler import MAX_SIMULATION_SELECTION, validate_scheduler_settings
from ..persistence.live_repository import LiveRepository
from ..persistence.policy_repository import PolicyRepository
from ..persistence.refresh_repository import RefreshRepository
from ..persistence.scheduler_repository import SchedulerRepository


class SchedulerService:
    def __init__(
        self,
        scheduler_repo: SchedulerRepository,
        policy_repo: PolicyRepository,
        refresh_repo: RefreshRepository | None = None,
        live_repo: LiveRepository | None = None,
    ):
        self.scheduler_repo = scheduler_repo
        self.policy_repo = policy_repo
        self.refresh_repo = refresh_repo or RefreshRepository(scheduler_repo.db)
        self.live_repo = live_repo or LiveRepository(scheduler_repo.db)

    def settings(self) -> dict:
        return self.scheduler_repo.status()

    def update_settings(self, payload) -> tuple[dict | None, list[str]]:
        errors = validate_scheduler_settings(payload)
        if errors:
            return None, errors
        policy = self.policy_repo.get()
        settings = self.scheduler_repo.update_mode(payload["mode"], policy.cycle_interval_minutes)
        # A direct PATCH can only ever land on off/simulate (validated
        # above), so this always invalidates any pending live
        # authorization - idempotent no-op if nothing was armed.
        self.live_repo.disarm_for_mode_change(new_mode=settings.mode, actor="system")
        return settings.to_dict(), []

    def queue_manual(self) -> tuple[dict, bool]:
        return self.scheduler_repo.queue_manual_simulation()

    def list_cycles(self, limit: int = 50) -> list[dict]:
        return self.scheduler_repo.list_cycles(limit)

    def cycle_detail(self, cycle_id: int) -> dict | None:
        return self.scheduler_repo.get_cycle(cycle_id)

    @staticmethod
    def _ordered(candidates: list[dict], order: str, seed: int, library_id: int) -> list[dict]:
        base = sorted(
            candidates,
            key=lambda c: (c["series_title"].casefold(), c["season_number"], c["episode_number"], c["id"]),
        )
        if order == "oldest_first":
            return sorted(base, key=lambda c: (c["air_date"] is None, c["air_date"] or "", c["id"]))
        if order == "newest_first":
            return sorted(base, key=lambda c: (c["air_date"] is not None, c["air_date"] or "", -c["id"]), reverse=True)
        if order == "random":
            Random(seed ^ library_id).shuffle(base)
        return base

    @staticmethod
    def _cap_reason(*, hourly: int, queue: int, success: int, safety: int) -> str:
        if hourly <= 0:
            return "hourly dispatch cap has no remaining capacity"
        if queue <= 0:
            return "queue target has no remaining capacity"
        if success <= 0:
            return "successful grab target already met for this policy window"
        if safety <= 0:
            return f"request safety maximum of {MAX_SIMULATION_SELECTION} candidates reached"
        return "effective simulation selection cap reached"

    def execute_cycle(self, cycle: dict, heartbeat=None) -> None:
        cycle_id = cycle["id"]
        policy = cycle["policy_snapshot"]
        # A scheduled cycle queued under one mode (simulate/live) is
        # audited as skipped if the mode drifted away before it started -
        # planning never silently runs under a different mode than the one
        # it was queued for.
        if cycle["trigger"] == "scheduled" and self.scheduler_repo.current_mode() != cycle["mode_snapshot"]:
            self.scheduler_repo.finish_cycle(
                cycle_id, "skipped", {},
                f"Scheduled {cycle['mode_snapshot']} cycle skipped because scheduler mode changed.",
            )
            return

        libraries = self.scheduler_repo.enabled_sonarr_libraries()
        totals = {
            "library_count": len(libraries), "completed_library_count": 0,
            "failed_library_count": 0, "considered_count": 0,
            "selected_count": 0, "excluded_count": 0,
        }
        skipped_count = 0

        for library in libraries:
            if heartbeat is not None and not heartbeat():
                # Stop immediately on shutdown or lease loss. Terminalization is
                # idempotent; a concurrent takeover may already have done it.
                self.scheduler_repo.finish_cycle(
                    cycle_id, "failed", totals,
                    "Worker stopped or lost its lease during simulation; it was not retried.",
                )
                return
            try:
                result, candidate_results = self._plan_library(cycle, library, policy)
                self.scheduler_repo.add_library_result(cycle_id, result, candidate_results)
                totals["considered_count"] += result["considered_count"]
                totals["selected_count"] += result["selected_count"]
                totals["excluded_count"] += result["excluded_count"]
                if result["state"] == "completed":
                    totals["completed_library_count"] += 1
                elif result["state"] == "skipped":
                    skipped_count += 1
            except Exception:
                # Do not persist exception text: database/upstream details may be sensitive.
                totals["failed_library_count"] += 1
                try:
                    self.scheduler_repo.add_library_result(
                        cycle_id,
                        {
                            "library_id": library["id"], "library_name": library["name"],
                            "state": "failed", "safe_summary": "Simulation planning failed safely; no command was sent.",
                            "upgrades_state": "unsupported" if policy["upgrades_enabled"] else "disabled",
                        },
                        [],
                    )
                except Exception:
                    pass

        is_live = cycle["mode_snapshot"] == "live"
        # Planning itself never sends a Sonarr command in either mode - see
        # module docstring. For a live cycle, whether anything was actually
        # dispatched afterward is recorded separately in
        # live_dispatch_ledger by LiveDispatchCoordinator, which runs only
        # after this method returns - see app/worker.py.
        dispatch_note = (
            "planning only; any live dispatch attempts are recorded separately in the live dispatch ledger."
            if is_live else "no Sonarr commands were sent."
        )
        label = "Live cycle planning" if is_live else "Simulation"
        if not libraries:
            state = "skipped"
            summary = f"No enabled Sonarr libraries; {label.lower()} sent no Sonarr commands."
        elif totals["failed_library_count"]:
            state = "failed" if totals["failed_library_count"] == len(libraries) else "partial"
            summary = f"{label} completed with library failures; {dispatch_note}"
        elif skipped_count == len(libraries):
            state = "skipped"
            summary = f"All enabled Sonarr libraries were skipped; {dispatch_note}"
        else:
            state = "completed"
            summary = (
                f"{label} selected {totals['selected_count']} of {totals['considered_count']} candidates; "
                f"{dispatch_note}"
            )
        self.scheduler_repo.finish_cycle(cycle_id, state, totals, summary)

    def _plan_library(self, cycle: dict, library: dict, policy: dict) -> tuple[dict, list[dict]]:
        upgrades_state = "unsupported" if policy["upgrades_enabled"] else "disabled"
        upgrade_note = (
            "Upgrade planning is unsupported in M4 and was skipped."
            if policy["upgrades_enabled"] else "Upgrade planning is disabled."
        )
        scan = self.scheduler_repo.latest_completed_scan(library["id"])
        max_age_minutes = self.refresh_repo.get_settings().scan_max_age_minutes
        now = datetime.now(timezone.utc)
        age_seconds = int((now - scan["updated_at"]).total_seconds()) if scan is not None else None
        stale = scan is None or age_seconds > max_age_minutes * 60
        if stale:
            active_refresh = self.refresh_repo.has_active_scan(library["id"])
            refresh_note = (
                "a read-only refresh scan is already queued or running."
                if active_refresh else
                "a read-only refresh scan will be queued for the next iteration."
            )
            if scan is None:
                reason = f"No completed Sonarr scan snapshot; {refresh_note}"
            else:
                reason = (
                    f"Latest scan snapshot is {age_seconds // 60} minute(s) old, past the "
                    f"{max_age_minutes} minute freshness window; {refresh_note}"
                )
            return ({
                "library_id": library["id"], "library_name": library["name"], "state": "skipped",
                "safe_summary": f"{reason} {upgrade_note}",
                "upgrades_state": upgrades_state, "considered_count": 0, "selected_count": 0,
                "excluded_count": 0, "effective_cap": 0,
            }, [])

        candidates = self.scheduler_repo.candidates_for_scan(scan["id"])
        ordered = self._ordered(candidates, policy["search_order"], cycle["random_seed"], library["id"])
        facts = self.scheduler_repo.planning_facts(
            library["id"], [c["episode_id"] for c in ordered], policy
        )
        hourly_remaining = max(0, policy["hourly_api_cap"] - facts["capacity_used"])
        queue_remaining = max(0, policy["queue_target"] - facts["queue_occupancy"])
        success_remaining = max(0, policy["successful_grab_target"] - facts["recent_success_count"])
        effective_cap = min(MAX_SIMULATION_SELECTION, hourly_remaining, queue_remaining, success_remaining)

        results = []
        eligible = []
        seen_episodes = set()
        for candidate in ordered:
            reason = None
            episode_id = candidate["episode_id"]
            if not policy["missing_enabled"]:
                reason = "missing-item planning is disabled by policy"
            elif episode_id in seen_episodes:
                reason = "duplicate Sonarr episode in scan snapshot"
            elif episode_id in facts["imported"]:
                reason = "durable outcome already records this episode as imported"
            elif episode_id in facts["active_grabbed"]:
                reason = "durable outcome records an active grab without a terminal import or failure"
            elif episode_id in facts["stale"]:
                reason = "stale or ambiguous dispatch outcome requires operator review"
            elif episode_id in facts["in_flight"]:
                reason = "episode has another dispatch in flight"
            elif episode_id in facts["cooldown"]:
                reason = f"episode was dispatched within the {policy['cooldown_minutes']} minute cooldown"
            if reason is None:
                eligible.append(candidate)
            else:
                results.append(self._candidate_result(candidate, False, reason, None))
            seen_episodes.add(episode_id)

        selected = eligible[:effective_cap]
        selected_ids = {c["id"] for c in selected}
        for position, candidate in enumerate(selected, 1):
            results.append(self._candidate_result(candidate, True, None, position))
        cap_reason = self._cap_reason(
            hourly=hourly_remaining, queue=queue_remaining, success=success_remaining,
            safety=MAX_SIMULATION_SELECTION - len(selected),
        )
        for candidate in eligible:
            if candidate["id"] not in selected_ids:
                results.append(self._candidate_result(candidate, False, cap_reason, None))

        results.sort(key=lambda r: (not r["selected"], r["order_position"] or 10**9, r["candidate_id"]))
        selected_count = len(selected)
        considered = len(candidates)
        state = "completed" if policy["missing_enabled"] else "skipped"
        summary = (
            f"Latest completed scan {scan['id']} ({age_seconds // 60} minute(s) old): "
            f"selected {selected_count} of {considered}; "
            f"effective cap {effective_cap} = min(safety {MAX_SIMULATION_SELECTION}, "
            f"hourly remaining {hourly_remaining}, queue remaining {queue_remaining}, "
            f"successful-grab remaining {success_remaining}). {upgrade_note}"
        )
        if "truncated" in (scan.get("details") or ""):
            summary += " The source scan snapshot was truncated at its candidate cap."
        return ({
            "library_id": library["id"], "library_name": library["name"], "scan_job_id": scan["id"],
            "state": state, "considered_count": considered, "selected_count": selected_count,
            "excluded_count": considered - selected_count, "effective_cap": effective_cap,
            "queue_occupancy": facts["queue_occupancy"],
            "recent_success_count": facts["recent_success_count"],
            "snapshot_taken_at": scan["updated_at"], "snapshot_age_seconds": age_seconds,
            "upgrades_state": upgrades_state, "safe_summary": summary,
        }, results)

    @staticmethod
    def _candidate_result(candidate: dict, selected: bool, reason: str | None, position: int | None) -> dict:
        return {
            "candidate_id": candidate["id"], "episode_id": candidate["episode_id"],
            "series_id": candidate["series_id"], "series_title": candidate["series_title"],
            "season_number": candidate["season_number"], "episode_number": candidate["episode_number"],
            "air_date": candidate["air_date"], "candidate_reason": candidate["reason"],
            "selected": selected, "exclusion_reason": reason, "order_position": position,
        }

"""The only worker component allowed to import ``DispatchService`` and a
write-capable Sonarr adapter for scheduled work.

``LiveDispatchCoordinator`` runs strictly after ``SchedulerService.execute_cycle``
has already planned and durably persisted a cycle's library/candidate
results - identically to a simulation cycle, using the same fresh scan
snapshot and the same deterministic ordering/cap logic (see
``SchedulerService``). This module never plans anything itself; it only
walks the already-selected candidates of a ``mode_snapshot = 'live'`` cycle
and, for each one, re-verifies live authorization from scratch before
reusing the existing manual ``DispatchService.dispatch()`` call - the same
transaction/idempotency/reservation machinery the confirmed manual dispatch
API already relies on. It never calls Sonarr directly and never duplicates
``search_episodes`` request-building logic.

Per candidate, in order:

1. Stop the cycle's remaining dispatch attempts (no further candidates)
   the moment the per-cycle dispatch budget is exhausted, live
   authorization is no longer valid (mode/arm/generation/emergency-stop),
   or a real Sonarr-side failure (not just "nothing eligible") occurs -
   this is the bounded, no-tight-retry backoff the M6 spec requires.
2. Otherwise call ``DispatchService.dispatch(job_id, [candidate_id], confirm=True)``,
   which independently re-validates cooldown/in-flight/hourly-cap/library
   readiness inside its own per-library-locked transaction immediately
   before any network call - unchanged from the M2/M3 manual path.
3. Record exactly one ``live_dispatch_ledger`` row for that candidate
   result, linking it to the resulting dispatch batch/item (if any).
4. After a successful dispatch, wait for the configured minimum delay before
   attempting the next candidate, checking heartbeat and live state
   frequently to allow interruption.
"""
from __future__ import annotations

import time
from typing import Callable, Optional

HARD_MAX_DISPATCHES_PER_CYCLE = 5
WAIT_CHECK_INTERVAL_SECONDS = 5.0

from .dispatch_service import NO_ELIGIBLE_CANDIDATES_ERROR
from ..persistence.live_repository import LiveRepository
from ..persistence.scheduler_repository import SchedulerRepository


class LiveDispatchCoordinator:
    def __init__(
        self,
        live_repo: LiveRepository,
        scheduler_repo: SchedulerRepository,
        dispatch_service,
        sleeper: Optional[Callable[[float], None]] = None,
        monotonic: Optional[Callable[[], float]] = None,
        wait_check_interval_seconds: float = WAIT_CHECK_INTERVAL_SECONDS,
    ):
        self.live_repo = live_repo
        self.scheduler_repo = scheduler_repo
        self.dispatch_service = dispatch_service
        self.sleeper = sleeper or time.sleep
        self.monotonic = monotonic or time.monotonic
        self.wait_check_interval_seconds = wait_check_interval_seconds

    def execute(self, cycle: dict, heartbeat=None) -> None:
        if cycle["mode_snapshot"] != "live":
            return

        control = self.live_repo.get_control()
        expected_generation = control["authorization_generation"]
        policy_snapshot = cycle.get("policy_snapshot") or {}
        policy_successful_grab_target = int(policy_snapshot.get("successful_grab_target", 1))
        # The policy target is the desired count; live control remains a
        # conservative operator ceiling. Schema v8 raises the legacy default
        # ceiling from one to the hard safety maximum without changing state.
        max_dispatches = min(
            policy_successful_grab_target,
            control["max_dispatches_per_cycle"],
            HARD_MAX_DISPATCHES_PER_CYCLE,
        )
        min_delay_seconds = max(
            control["min_delay_seconds_between_dispatches"],
            int((policy_snapshot).get("dispatch_interval_seconds", 0)),
        )

        if control["state"] != "running" or expected_generation == 0:
            return

        detail = self.scheduler_repo.get_cycle(cycle["id"])
        if detail is None:
            return

        dispatched_or_attempted = 0
        last_dispatch_time = None  # monotonic start time of the last write attempt
        for library in detail["libraries"]:
            if dispatched_or_attempted >= max_dispatches:
                break
            job_id = library.get("scan_job_id")
            if job_id is None:
                continue
            selected = sorted(
                (c for c in library["candidates"] if c["selected"]),
                key=lambda c: c["order_position"] or 10**9,
            )
            for candidate in selected:
                if dispatched_or_attempted >= max_dispatches:
                    break
                if heartbeat is not None and not heartbeat():
                    return

                # If we have made at least one network call in this cycle, wait for the delay since the last one
                if last_dispatch_time is not None:
                    elapsed = self.monotonic() - last_dispatch_time
                    if elapsed < min_delay_seconds:
                        remaining = min_delay_seconds - elapsed
                        if not self._wait_for_next_dispatch(remaining, heartbeat, expected_generation):
                            return  # Wait was interrupted due to loss of authorization or heartbeat

                # Recheck both lease ownership and persistent authorization
                # immediately before every potentially ambiguous write.
                if heartbeat is not None and not heartbeat():
                    return
                authorized, reason = self.live_repo.check_dispatch_authorized(expected_generation)
                if not authorized:
                    self._ledger(cycle["id"], library, candidate, expected_generation, "blocked", reason)
                    return

                write_started_at = self.monotonic()
                outcome, error = self.dispatch_service.dispatch(job_id, [candidate["candidate_id"]], True)
                if error:
                    # A validation-style rejection (e.g. the library became
                    # unready between planning and this attempt) - no
                    # network call happened, so it never counts against the
                    # cycle's dispatch budget or the minimum-delay timer.
                    self._ledger(cycle["id"], library, candidate, expected_generation, "skipped", error)
                    continue

                batch = outcome.batch
                if batch.state == "failed" and batch.error_summary == NO_ELIGIBLE_CANDIDATES_ERROR:
                    # Re-planning inside DispatchService excluded the
                    # candidate for real (cooldown/in-flight/cap/etc since
                    # this cycle's snapshot was taken) - no network call.
                    self._ledger(cycle["id"], library, candidate, expected_generation, "skipped", batch.error_summary)
                    continue

                # Network call happened
                last_dispatch_time = write_started_at
                dispatched_or_attempted += 1
                item = batch.items[0] if batch.items else None
                if batch.state in ("completed", "partial"):
                    self.live_repo.record_dispatch_attempt(
                        f"dispatched candidate {candidate['candidate_id']} (batch {batch.id})"
                    )
                    self._ledger(
                        cycle["id"], library, candidate, expected_generation, "dispatched", "dispatched to Sonarr",
                        dispatch_batch_id=batch.id, dispatch_item_id=item.id if item else None,
                        sonarr_command_id=batch.sonarr_command_id, sonarr_command_status=batch.sonarr_command_status,
                    )
                    # Note: we do not wait here because we will wait before the next network call (at the top of the loop)
                    continue

                # A real Sonarr-side failure (timeout/429/5xx/rejection).
                # Record it, update the delay timer so the next attempt
                # anywhere is still bounded, and stop this cycle rather
                # than tight-retrying.
                self.live_repo.record_dispatch_attempt(
                    f"attempt failed for candidate {candidate['candidate_id']} (batch {batch.id})"
                )
                self._ledger(
                    cycle["id"], library, candidate, expected_generation, "failed",
                    batch.error_summary or "Sonarr dispatch attempt failed",
                    dispatch_batch_id=batch.id, dispatch_item_id=item.id if item else None,
                )
                return

    def _wait_for_next_dispatch(
        self, wait_seconds: float, heartbeat, expected_generation: int
    ) -> bool:
        """Wait for the specified time to elapse, checking heartbeat and live state.

        Returns True if the wait completed normally, False if interrupted.
        """
        end_time = self.monotonic() + wait_seconds
        while self.monotonic() < end_time:
            if heartbeat is not None and not heartbeat():
                return False
            authorized, reason = self.live_repo.check_dispatch_authorized(expected_generation)
            if not authorized:
                # Authorization lost during wait - stop the cycle
                return False
            # Sleep for a short interval to avoid busy waiting
            remaining = end_time - self.monotonic()
            if remaining <= 0:
                break
            self.sleeper(min(self.wait_check_interval_seconds, remaining))
        return True

    def _ledger(
        self, cycle_id: int, library: dict, candidate: dict, arm_generation: int, state: str, reason: str,
        *, dispatch_batch_id=None, dispatch_item_id=None, sonarr_command_id=None, sonarr_command_status=None,
    ) -> None:
        self.live_repo.record_ledger_entry({
            "cycle_run_id": cycle_id,
            "library_result_id": library["id"],
            "candidate_result_id": candidate["id"],
            "library_id": library.get("library_id"),
            "candidate_id": candidate["candidate_id"],
            "dispatch_batch_id": dispatch_batch_id,
            "dispatch_item_id": dispatch_item_id,
            "attempt": 1,
            "arm_generation": arm_generation,
            "state": state,
            "sonarr_command_id": sonarr_command_id,
            "sonarr_command_status": sonarr_command_status,
            "terminal_reason": reason[:500],
        })
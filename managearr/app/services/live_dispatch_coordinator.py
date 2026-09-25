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
"""
from __future__ import annotations

from .dispatch_service import NO_ELIGIBLE_CANDIDATES_ERROR
from ..persistence.live_repository import LiveRepository
from ..persistence.scheduler_repository import SchedulerRepository


class LiveDispatchCoordinator:
    def __init__(self, live_repo: LiveRepository, scheduler_repo: SchedulerRepository, dispatch_service):
        self.live_repo = live_repo
        self.scheduler_repo = scheduler_repo
        self.dispatch_service = dispatch_service

    def execute(self, cycle: dict, heartbeat=None) -> None:
        if cycle["mode_snapshot"] != "live":
            return

        control = self.live_repo.get_control()
        expected_generation = control["authorization_generation"]
        max_dispatches = control["max_dispatches_per_cycle"]
        # Policy is snapshotted on the cycle; never dispatch faster than
        # either that operator policy or the hard live-control floor.
        min_delay_seconds = max(
            control["min_delay_seconds_between_dispatches"],
            int((cycle.get("policy_snapshot") or {}).get("dispatch_interval_seconds", 0)),
        )

        if control["state"] != "running" or expected_generation == 0:
            # Nothing was ever attempted; planning alone (identical to
            # simulate) already ran and is fully audited by the cycle's
            # own scheduler_library_results/scheduler_candidate_results.
            return

        detail = self.scheduler_repo.get_cycle(cycle["id"])
        if detail is None:
            return

        dispatched_or_attempted = 0
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

                authorized, reason = self.live_repo.check_dispatch_authorized(expected_generation)
                if not authorized:
                    self._ledger(cycle["id"], library, candidate, expected_generation, "blocked", reason)
                    return

                if not self.live_repo.check_dispatch_delay_elapsed(min_delay_seconds):
                    self._ledger(
                        cycle["id"], library, candidate, expected_generation, "skipped",
                        f"minimum delay of {min_delay_seconds}s between live dispatches has not elapsed",
                    )
                    return

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
            "terminal_reason": reason,
        })

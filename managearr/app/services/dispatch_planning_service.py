"""Computes a manual-dispatch plan for a completed Sonarr scan job.

``DispatchPlanningService.compute()`` is pure read-only planning: it
validates the requested candidate ids against the scan job, applies the
automation policy's ``cooldown_minutes`` and ``hourly_api_cap`` using the
durable dispatch ledger, and returns which candidates are ``eligible``,
which of those are actually ``selected`` (the ``eligible`` subset that
fits in the currently remaining hourly capacity), and which are
``excluded`` with a reason. **It never calls Sonarr.**

``preview()`` wraps ``compute()`` and persists a ``dry_run`` audit batch
- a durable record of both selected and explainable excluded candidates.
Dry-run items never reach ``state = 'dispatched'``, so they never count
toward a future cooldown or cap calculation.

``compute()`` also accepts an optional ``conn`` so ``DispatchService``
can re-run the exact same planning logic *inside* its reservation
transaction, immediately before dispatching, so a race between planning
and dispatch can never bypass the cap or cooldown - see
``app/services/dispatch_service.py``.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from ..domain.dispatch import MAX_SELECTION_PER_REQUEST
from ..domain.scan_candidate import ScanCandidate
from ..persistence.activity_repository import ActivityRepository
from ..persistence.dispatch_repository import DispatchRepository
from ..persistence.library_repository import LibraryRepository
from ..persistence.policy_repository import PolicyRepository
from ..persistence.scan_candidate_repository import ScanCandidateRepository
from .library_readiness import VALIDATION_ERRORS as LIBRARY_VALIDATION_ERRORS
from .library_readiness import ready_sonarr_library

JOB_NOT_FOUND_ERROR = "activity job not found"
JOB_NOT_A_SONARR_SCAN_ERROR = "activity job is not a sonarr scan"
JOB_NOT_COMPLETED_ERROR = "scan job has not completed - candidates are not final yet"
JOB_HAS_NO_LIBRARY_ERROR = "scan job has no associated library"
EMPTY_SELECTION_ERROR = "candidate_ids must be a non-empty list of integers"
TOO_MANY_SELECTED_ERROR = f"cannot select more than {MAX_SELECTION_PER_REQUEST} candidates per request"

# Validation-style errors the API layer maps to 4xx instead of 502/500.
VALIDATION_ERRORS = LIBRARY_VALIDATION_ERRORS | {
    JOB_NOT_FOUND_ERROR,
    JOB_NOT_A_SONARR_SCAN_ERROR,
    JOB_NOT_COMPLETED_ERROR,
    JOB_HAS_NO_LIBRARY_ERROR,
    EMPTY_SELECTION_ERROR,
    TOO_MANY_SELECTED_ERROR,
}

CANDIDATE_NOT_FOUND_REASON = "candidate not found"
CANDIDATE_WRONG_JOB_REASON = "candidate does not belong to this scan job"
CANDIDATE_WRONG_LIBRARY_REASON = "candidate does not belong to this scan job's library"
CANDIDATE_DUPLICATE_REASON = "duplicate candidate id in request"
DUPLICATE_EPISODE_REASON = "another selected candidate refers to the same Sonarr episode"
IN_FLIGHT_REASON = "episode has another dispatch in flight"
CAPACITY_REASON = "hourly dispatch cap has no remaining capacity for this candidate"


def _cooldown_reason(minutes: int) -> str:
    return f"episode was dispatched within the last {minutes} minute cooldown window"


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class PlanResult:
    job_id: int
    library_id: int
    requested_count: int
    eligible: list[ScanCandidate]
    selected: list[ScanCandidate]
    excluded: list[dict]
    remaining_capacity: int
    hourly_api_cap: int
    cooldown_minutes: int
    audit_batch_id: int | None = None

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "library_id": self.library_id,
            "requested_count": self.requested_count,
            "eligible": [c.to_dict() for c in self.eligible],
            "selected": [c.to_dict() for c in self.selected],
            "excluded": self.excluded,
            "remaining_capacity": self.remaining_capacity,
            "hourly_api_cap": self.hourly_api_cap,
            "cooldown_minutes": self.cooldown_minutes,
            "audit_batch_id": self.audit_batch_id,
        }


class DispatchPlanningService:
    def __init__(
        self,
        activity_repo: ActivityRepository,
        candidate_repo: ScanCandidateRepository,
        library_repo: LibraryRepository,
        policy_repo: PolicyRepository,
        dispatch_repo: DispatchRepository,
    ):
        self.activity_repo = activity_repo
        self.candidate_repo = candidate_repo
        self.library_repo = library_repo
        self.policy_repo = policy_repo
        self.dispatch_repo = dispatch_repo

    def validate_job(self, job_id: int, *, conn=None):
        job = self.activity_repo.get(job_id, conn=conn)
        if job is None:
            return None, JOB_NOT_FOUND_ERROR
        if job.job_type != "sonarr_scan":
            return None, JOB_NOT_A_SONARR_SCAN_ERROR
        if job.state != "completed":
            return None, JOB_NOT_COMPLETED_ERROR
        if job.library_id is None:
            return None, JOB_HAS_NO_LIBRARY_ERROR
        return job, None

    @staticmethod
    def _valid_id_list(candidate_ids) -> bool:
        return (
            isinstance(candidate_ids, list)
            and len(candidate_ids) > 0
            and all(isinstance(cid, int) and not isinstance(cid, bool) for cid in candidate_ids)
        )

    def compute(self, job_id: int, candidate_ids, *, conn=None) -> tuple[PlanResult | None, str | None]:
        """Read-only planning. Pass ``conn`` to run every ledger read
        inside a caller-managed transaction (see ``DispatchService``);
        omit it to let each read open its own short connection."""
        job, error = self.validate_job(job_id, conn=conn)
        if error:
            return None, error

        library, error = ready_sonarr_library(self.library_repo, job.library_id, conn=conn)
        if error:
            return None, error

        if not self._valid_id_list(candidate_ids):
            return None, EMPTY_SELECTION_ERROR

        requested_count = len(candidate_ids)
        if requested_count > MAX_SELECTION_PER_REQUEST:
            return None, TOO_MANY_SELECTED_ERROR

        seen: set[int] = set()
        deduped_ids: list[int] = []
        excluded: list[dict] = []
        for cid in candidate_ids:
            if cid in seen:
                excluded.append({"candidate_id": cid, "reason": CANDIDATE_DUPLICATE_REASON})
                continue
            seen.add(cid)
            deduped_ids.append(cid)

        candidates_by_id = {
            c.id: c for c in self.candidate_repo.get_many(deduped_ids, conn=conn)
        }
        valid_candidates: list[ScanCandidate] = []
        seen_episode_ids: set[int] = set()
        for cid in deduped_ids:
            candidate = candidates_by_id.get(cid)
            if candidate is None:
                excluded.append({"candidate_id": cid, "reason": CANDIDATE_NOT_FOUND_REASON})
                continue
            if candidate.job_id != job_id:
                excluded.append({"candidate_id": cid, "reason": CANDIDATE_WRONG_JOB_REASON})
                continue
            if candidate.library_id != job.library_id:
                excluded.append({"candidate_id": cid, "reason": CANDIDATE_WRONG_LIBRARY_REASON})
                continue
            if candidate.episode_id in seen_episode_ids:
                excluded.append({"candidate_id": cid, "reason": DUPLICATE_EPISODE_REASON})
                continue
            seen_episode_ids.add(candidate.episode_id)
            valid_candidates.append(candidate)

        policy = self.policy_repo.get(conn=conn)
        episode_ids = [c.episode_id for c in valid_candidates]
        cooldown_since = _now() - timedelta(minutes=policy.cooldown_minutes)
        cooled = self.dispatch_repo.dispatched_episode_ids_since(
            library.id, episode_ids, cooldown_since, conn=conn
        )
        in_flight = self.dispatch_repo.active_reservation_episode_ids(library.id, episode_ids, conn=conn)

        eligible: list[ScanCandidate] = []
        for candidate in valid_candidates:
            if candidate.episode_id in cooled:
                excluded.append({"candidate_id": candidate.id, "reason": _cooldown_reason(policy.cooldown_minutes)})
                continue
            if candidate.episode_id in in_flight:
                excluded.append({"candidate_id": candidate.id, "reason": IN_FLIGHT_REASON})
                continue
            eligible.append(candidate)

        capacity_used = self.dispatch_repo.count_capacity_used_since(
            library.id, _now() - timedelta(hours=1), conn=conn
        )
        remaining_capacity = max(0, policy.hourly_api_cap - capacity_used)
        selected = eligible[:remaining_capacity]
        for candidate in eligible[remaining_capacity:]:
            excluded.append({"candidate_id": candidate.id, "reason": CAPACITY_REASON})

        result = PlanResult(
            job_id=job_id,
            library_id=library.id,
            requested_count=requested_count,
            eligible=eligible,
            selected=selected,
            excluded=excluded,
            remaining_capacity=remaining_capacity,
            hourly_api_cap=policy.hourly_api_cap,
            cooldown_minutes=policy.cooldown_minutes,
        )
        return result, None

    def preview(self, job_id: int, candidate_ids) -> tuple[PlanResult | None, str | None]:
        """Dry-run: computes the plan and persists a ``dry_run`` audit
        batch (state ``planned``) so the preview itself is auditable -
        but never calls Sonarr and never affects cooldown/cap for real
        dispatches (see module docstring)."""
        result, error = self.compute(job_id, candidate_ids)
        if error:
            return None, error

        job = self.activity_repo.get(job_id)
        library = self.library_repo.get(result.library_id)

        with self.dispatch_repo.db.connect() as conn:
            batch_id = self.dispatch_repo.create_batch(
                conn,
                {
                    "scan_job_id": job.id,
                    "library_id": library.id,
                    "library_name": library.name,
                    "mode": "dry_run",
                    "state": "planned",
                    "requested_count": result.requested_count,
                    "selected_count": len(result.selected),
                    "dispatched_count": 0,
                    "error_summary": "",
                },
            )
            selected_ids = {candidate.id for candidate in result.selected}
            excluded_reasons = {
                entry["candidate_id"]: entry["reason"]
                for entry in result.excluded
                if entry["candidate_id"] not in selected_ids
            }
            valid_candidates = {
                candidate.id: candidate
                for candidate in self.candidate_repo.get_many(
                    list(selected_ids | set(excluded_reasons)), conn=conn
                )
                if candidate.job_id == job.id and candidate.library_id == library.id
            }
            audit_items = []
            for candidate_id in dict.fromkeys(candidate_ids):
                candidate = valid_candidates.get(candidate_id)
                if candidate is None:
                    continue
                audit_items.append(
                    {
                        "candidate_id": candidate.id,
                        "episode_id": candidate.episode_id,
                        "series_id": candidate.series_id,
                        "series_title": candidate.series_title,
                        "season_number": candidate.season_number,
                        "episode_number": candidate.episode_number,
                        "state": "planned" if candidate.id in selected_ids else "excluded",
                        "reason": excluded_reasons.get(candidate.id),
                    }
                )
            self.dispatch_repo.create_items(
                conn,
                batch_id,
                audit_items,
            )
        result.audit_batch_id = batch_id
        return result, None

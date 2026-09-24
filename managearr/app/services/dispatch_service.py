"""Manual, confirmed Sonarr search dispatch.

This is the only path in Managearr v1 that can make Sonarr do
something. It requires an explicit ``confirm: true`` plus a non-empty
``candidate_ids`` list; anything else is rejected before any database
write or network call happens (see ``dispatch()``'s first checks).

Flow for a confirmed request:

1. Look up the scan job to find its library (a plain read - just to get
   the advisory-lock key; every rule is re-validated for real in step 2).
2. Open **one** database transaction: acquire a per-library advisory
   lock (``DispatchRepository.acquire_library_lock``), expire any stale
   reservations, then re-run ``DispatchPlanningService.compute()``
   *inside this same transaction* so the cap/cooldown/in-flight checks
   see a consistent, locked snapshot of the ledger. If anything is
   eligible, reserve it (insert a ``manual`` batch + ``reserved`` items)
   and commit - this releases the lock and durably records the
   reservation before any network call happens, so a second concurrent
   request for the same library will see it once it acquires the lock.
3. **Outside** that transaction, call ``SonarrClient.search_episodes``
   once with every reserved episode id.
4. In a **fresh**, per-library-locked transaction, atomically finalize
   the batch/items to
   ``dispatched``/``completed``/``partial`` on success, or ``failed``
   (releasing the reservation - failed items never count toward cap or
   cooldown) on any Sonarr error.
"""
from dataclasses import dataclass

from ..adapters.sonarr_client import SonarrClient, SonarrError
from ..persistence.dispatch_repository import DispatchRepository
from ..persistence.library_repository import LibraryRepository
from .dispatch_planning_service import EMPTY_SELECTION_ERROR, DispatchPlanningService

CONFIRM_REQUIRED_ERROR = "confirm must be true to dispatch searches to Sonarr"
NO_ELIGIBLE_CANDIDATES_ERROR = "no candidates could be selected under current dispatch rules"

# Validation-style errors the API layer maps to 4xx instead of 502/500.
VALIDATION_ERRORS = {CONFIRM_REQUIRED_ERROR, EMPTY_SELECTION_ERROR}


@dataclass
class DispatchOutcome:
    batch: object  # DispatchBatch
    plan: object  # PlanResult | None - None when short-circuited before planning


class DispatchService:
    def __init__(
        self,
        planning_service: DispatchPlanningService,
        dispatch_repo: DispatchRepository,
        library_repo: LibraryRepository,
        *,
        client_factory=SonarrClient,
        timeout: int | None = None,
    ):
        self.planning_service = planning_service
        self.dispatch_repo = dispatch_repo
        self.library_repo = library_repo
        self._client_factory = client_factory
        self._timeout = timeout

    def _make_client(self, library) -> SonarrClient:
        if self._timeout is not None:
            return self._client_factory(library.url, library.api_key, timeout=self._timeout)
        return self._client_factory(library.url, library.api_key)

    def dispatch(self, job_id: int, candidate_ids, confirm: bool) -> tuple[DispatchOutcome | None, str | None]:
        # Confirmation gate: no DB write, no network call, for anything
        # that isn't an explicit, well-formed confirmed request.
        if confirm is not True:
            return None, CONFIRM_REQUIRED_ERROR
        if not isinstance(candidate_ids, list) or not candidate_ids:
            return None, EMPTY_SELECTION_ERROR

        # Plain read to find the library to lock on. Every rule this
        # implies (job exists, is a completed sonarr_scan, has a
        # library) is re-validated for real inside the locked
        # transaction below via compute() - this is only a fast path to
        # avoid opening a transaction for an obviously-bad job id.
        job, error = self.planning_service.validate_job(job_id)
        if error:
            return None, error

        item_ids: list[int] = []
        episode_ids: list[int] = []

        with self.dispatch_repo.db.connect() as conn:
            self.dispatch_repo.acquire_library_lock(conn, job.library_id)
            self.dispatch_repo.expire_stale_reservations(job.library_id, conn=conn)

            result, error = self.planning_service.compute(job_id, candidate_ids, conn=conn)
            if error:
                return None, error

            library = self.library_repo.get(result.library_id, conn=conn)

            batch_id = self.dispatch_repo.create_batch(
                conn,
                {
                    "scan_job_id": job_id,
                    "library_id": library.id,
                    "library_name": library.name,
                    "mode": "manual",
                    "state": "dispatching" if result.selected else "failed",
                    "requested_count": result.requested_count,
                    "selected_count": len(result.selected),
                    "dispatched_count": 0,
                    "error_summary": "" if result.selected else NO_ELIGIBLE_CANDIDATES_ERROR,
                },
            )

            selected_ids = {candidate.id for candidate in result.selected}
            reserved_items = [
                {
                    "candidate_id": c.id,
                    "episode_id": c.episode_id,
                    "series_id": c.series_id,
                    "series_title": c.series_title,
                    "season_number": c.season_number,
                    "episode_number": c.episode_number,
                    "state": "reserved",
                }
                for c in result.selected
            ]
            excluded_reasons = {
                entry["candidate_id"]: entry["reason"]
                for entry in result.excluded
                if entry["candidate_id"] not in selected_ids
            }
            excluded_candidates = {
                candidate.id: candidate
                for candidate in self.planning_service.candidate_repo.get_many(
                    list(excluded_reasons), conn=conn
                )
                if candidate.job_id == job_id and candidate.library_id == library.id
            }
            excluded_items = [
                {
                    "candidate_id": candidate.id,
                    "episode_id": candidate.episode_id,
                    "series_id": candidate.series_id,
                    "series_title": candidate.series_title,
                    "season_number": candidate.season_number,
                    "episode_number": candidate.episode_number,
                    "state": "excluded",
                    "reason": excluded_reasons[candidate.id],
                }
                for candidate in excluded_candidates.values()
            ]
            all_item_ids = self.dispatch_repo.create_items(
                conn, batch_id, reserved_items + excluded_items
            )

            if result.selected:
                item_ids = all_item_ids[: len(reserved_items)]
                episode_ids = [c.episode_id for c in result.selected]

        # Transaction (and the advisory lock) is closed and committed -
        # the reservation (or the no-eligible-candidates failure) is
        # durably recorded before anything below reads it back or calls
        # Sonarr, matching SonarrScanService's no-transaction-across-a-
        # network-call rule.
        if not result.selected:
            return DispatchOutcome(batch=self.dispatch_repo.get_batch(batch_id), plan=result), None

        try:
            command = self._make_client(library).search_episodes(episode_ids)
        except SonarrError as exc:
            self.dispatch_repo.finalize_attempt(
                batch_id=batch_id,
                item_ids=item_ids,
                library_id=library.id,
                batch_state="failed",
                item_state="failed",
                dispatched_count=0,
                error_summary=str(exc),
                item_reason=str(exc),
            )
            return DispatchOutcome(batch=self.dispatch_repo.get_batch(batch_id), plan=result), None
        except Exception:
            safe_error = "unexpected error while dispatching to Sonarr"
            self.dispatch_repo.finalize_attempt(
                batch_id=batch_id,
                item_ids=item_ids,
                library_id=library.id,
                batch_state="failed",
                item_state="failed",
                dispatched_count=0,
                error_summary=safe_error,
                item_reason=safe_error,
            )
            return DispatchOutcome(batch=self.dispatch_repo.get_batch(batch_id), plan=result), None

        final_state = "completed" if len(result.selected) == result.requested_count else "partial"
        self.dispatch_repo.finalize_attempt(
            batch_id=batch_id,
            item_ids=item_ids,
            library_id=library.id,
            batch_state=final_state,
            item_state="dispatched",
            dispatched_count=len(result.selected),
            sonarr_command_id=command.get("id"),
            sonarr_command_status=command.get("status"),
            error_summary="",
        )
        return DispatchOutcome(batch=self.dispatch_repo.get_batch(batch_id), plan=result), None

"""Tests for DispatchService: the confirm gate, the single EpisodeSearch
call, ledger finalization on success/failure, and concurrency-safe
reservation. The Sonarr client is always a stub here - no real network
access - except in the threaded concurrency test, which still only ever
talks to a stub, never a real Sonarr instance."""
import threading

import pytest

from app.adapters.sonarr_client import SonarrAuthError, SonarrConnectionError
from app.services.dispatch_service import (
    CONFIRM_REQUIRED_ERROR,
    NO_ELIGIBLE_CANDIDATES_ERROR,
    DispatchService,
)
from app.services.dispatch_planning_service import EMPTY_SELECTION_ERROR


class StubSonarrClient:
    def __init__(self, *, command=None, error=None):
        self._command = command or {"id": 4242, "name": "EpisodeSearch", "status": "queued"}
        self._error = error
        self.calls: list[list[int]] = []

    def search_episodes(self, episode_ids):
        self.calls.append(list(episode_ids))
        if self._error:
            raise self._error
        return self._command


def make_service(planning_service, dispatch_repo, library_repo, stub: StubSonarrClient) -> DispatchService:
    def factory(base_url, api_key, timeout=None):
        return stub

    return DispatchService(planning_service, dispatch_repo, library_repo, client_factory=factory)


def make_library(library_repo, **overrides):
    data = {
        "name": "Sonarr Main",
        "type": "sonarr",
        "url": "http://sonarr:8989",
        "api_key": "secret-key",
        "enabled": True,
    }
    data.update(overrides)
    return library_repo.create(data)


def make_completed_scan_job(activity_repo, library):
    return activity_repo.create(
        {
            "library_id": library.id,
            "library_name": library.name,
            "job_type": "sonarr_scan",
            "state": "completed",
            "title": f"Sonarr scan: {library.name}",
            "candidate_count": 0,
        }
    )


def make_candidate(candidate_repo, job_id, library_id, *, episode_id, season=1, ep=1, series_id=1, title="Show A"):
    candidate_repo.create_many(
        job_id,
        library_id,
        [
            {
                "series_id": series_id,
                "series_title": title,
                "episode_id": episode_id,
                "season_number": season,
                "episode_number": ep,
                "air_date": "2026-01-01",
                "reason": "monitored episode aired with no file on disk",
            }
        ],
    )
    return candidate_repo.list_for_job(job_id)[-1]


# --- confirmation gate: nothing sent, nothing written ----------------------

def test_dispatch_requires_confirm_true(dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=1)
    stub = StubSonarrClient()
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)

    outcome, error = service.dispatch(job.id, [c.id], confirm=False)

    assert outcome is None
    assert error == CONFIRM_REQUIRED_ERROR
    assert stub.calls == []
    assert dispatch_repo.list_for_job(job.id) == []


def test_dispatch_requires_confirm_exactly_true_not_truthy(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=1)
    stub = StubSonarrClient()
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)

    outcome, error = service.dispatch(job.id, [c.id], confirm="true")

    assert outcome is None
    assert error == CONFIRM_REQUIRED_ERROR
    assert stub.calls == []


def test_dispatch_requires_nonempty_candidate_ids(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    stub = StubSonarrClient()
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)

    outcome, error = service.dispatch(job.id, [], confirm=True)

    assert outcome is None
    assert error == EMPTY_SELECTION_ERROR
    assert stub.calls == []


def test_dispatch_missing_confirm_key_defaults_to_none_and_is_rejected(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=1)
    stub = StubSonarrClient()
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)

    outcome, error = service.dispatch(job.id, [c.id], confirm=None)

    assert error == CONFIRM_REQUIRED_ERROR
    assert stub.calls == []


# --- successful dispatch -------------------------------------------------

def test_confirmed_dispatch_calls_sonarr_once_and_completes(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c1 = make_candidate(candidate_repo, job.id, lib.id, episode_id=1)
    c2 = make_candidate(candidate_repo, job.id, lib.id, episode_id=2)
    stub = StubSonarrClient(command={"id": 999, "name": "EpisodeSearch", "status": "queued"})
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)

    outcome, error = service.dispatch(job.id, [c1.id, c2.id], confirm=True)

    assert error is None
    assert stub.calls == [[1, 2]]
    batch = outcome.batch
    assert batch.mode == "manual"
    assert batch.state == "completed"
    assert batch.dispatched_count == 2
    assert batch.sonarr_command_id == 999
    assert batch.sonarr_command_status == "queued"
    assert len(batch.items) == 2
    assert all(item.state == "dispatched" for item in batch.items)


def test_confirmed_dispatch_partial_when_some_candidates_excluded(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c1 = make_candidate(candidate_repo, job.id, lib.id, episode_id=1)
    stub = StubSonarrClient()
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)

    outcome, error = service.dispatch(job.id, [c1.id, 999999], confirm=True)

    assert error is None
    assert outcome.batch.state == "partial"
    assert outcome.batch.requested_count == 2
    assert outcome.batch.dispatched_count == 1
    assert stub.calls == [[1]]


def test_confirmed_dispatch_with_no_eligible_candidates_creates_failed_batch_and_never_calls_sonarr(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    stub = StubSonarrClient()
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)

    outcome, error = service.dispatch(job.id, [999999], confirm=True)

    assert error is None
    assert stub.calls == []
    assert outcome.batch.mode == "manual"
    assert outcome.batch.state == "failed"
    assert outcome.batch.error_summary == NO_ELIGIBLE_CANDIDATES_ERROR
    assert outcome.batch.items == []


# --- Sonarr failure releases the reservation but keeps audit history -----

def test_sonarr_auth_failure_marks_batch_and_items_failed_and_releases_reservation(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=1)
    stub = StubSonarrClient(error=SonarrAuthError("Sonarr rejected the configured API key"))
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)

    outcome, error = service.dispatch(job.id, [c.id], confirm=True)

    assert error is None
    batch = outcome.batch
    assert batch.state == "failed"
    assert batch.dispatched_count == 0
    assert batch.error_summary == "Sonarr rejected the configured API key"
    assert all(item.state == "failed" for item in batch.items)

    # The reservation is released: the same episode is eligible again on
    # a fresh plan, and doesn't count toward the hourly cap.
    result, plan_error = dispatch_planning_service.compute(job.id, [c.id])
    assert plan_error is None
    assert [x.id for x in result.eligible] == [c.id]
    assert result.remaining_capacity == result.hourly_api_cap


def test_sonarr_connection_failure_never_leaks_api_key_or_url(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo
):
    lib = make_library(library_repo, api_key="ultra-secret", url="http://sonarr.internal:8989")
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=1)
    stub = StubSonarrClient(error=SonarrConnectionError("Sonarr request timed out"))
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)

    outcome, error = service.dispatch(job.id, [c.id], confirm=True)

    assert error is None
    assert "ultra-secret" not in outcome.batch.error_summary
    assert "sonarr.internal" not in outcome.batch.error_summary


def test_retry_after_failure_is_safe_and_creates_a_new_batch(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=1)

    failing_stub = StubSonarrClient(error=SonarrConnectionError("Sonarr request timed out"))
    failing_service = make_service(dispatch_planning_service, dispatch_repo, library_repo, failing_stub)
    first_outcome, error = failing_service.dispatch(job.id, [c.id], confirm=True)
    assert error is None
    assert first_outcome.batch.state == "failed"

    succeeding_stub = StubSonarrClient()
    retry_service = make_service(dispatch_planning_service, dispatch_repo, library_repo, succeeding_stub)
    second_outcome, error = retry_service.dispatch(job.id, [c.id], confirm=True)

    assert error is None
    assert second_outcome.batch.id != first_outcome.batch.id
    assert second_outcome.batch.state == "completed"
    assert succeeding_stub.calls == [[1]]

    batches = dispatch_repo.list_for_job(job.id)
    assert len(batches) == 2


# --- job/library validation reused from planning --------------------------

def test_dispatch_job_not_found(dispatch_planning_service, dispatch_repo, library_repo):
    stub = StubSonarrClient()
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)
    outcome, error = service.dispatch(999, [1], confirm=True)
    assert outcome is None
    assert error == "activity job not found"


def test_dispatch_disabled_library_never_calls_sonarr(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=1)
    library_repo.update(lib.id, {"enabled": False})
    stub = StubSonarrClient()
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)

    outcome, error = service.dispatch(job.id, [c.id], confirm=True)

    assert outcome is None
    assert error == "library is disabled"
    assert stub.calls == []


# --- concurrency: two confirmed requests for the same episode -------------

def test_concurrent_dispatch_requests_never_double_dispatch_same_episode(
    database, dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo
):
    """Fires two confirmed dispatch requests for the *same* candidate at
    the same time (from separate threads, each with its own DB
    connection pulled from the same pool). The per-library advisory lock
    must serialize them so exactly one dispatches the episode and the
    other sees it as already in-flight/dispatched - never both."""
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=1)

    results = []
    barrier = threading.Barrier(2)

    def run():
        stub = StubSonarrClient()
        service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)
        barrier.wait()
        outcome, error = service.dispatch(job.id, [c.id], confirm=True)
        results.append((outcome, error, stub.calls))

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 2
    sonarr_call_counts = [len(calls) for _, _, calls in results]
    # Exactly one of the two requests actually dispatched the episode to
    # Sonarr; the other found nothing eligible (already in flight/done)
    # and made no Sonarr call at all.
    assert sorted(sonarr_call_counts) == [0, 1]

    dispatched_batches = [
        outcome.batch for outcome, error, _ in results if outcome is not None and outcome.batch.dispatched_count > 0
    ]
    assert len(dispatched_batches) == 1


def test_concurrent_dispatches_cannot_overbook_hourly_cap_with_different_episodes(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo, policy_repo
):
    """Live reservations consume capacity, not just completed calls."""
    lib = make_library(library_repo)
    policy_repo.update({"hourly_api_cap": 1})
    job = make_completed_scan_job(activity_repo, lib)
    candidates = [
        make_candidate(candidate_repo, job.id, lib.id, episode_id=901),
        make_candidate(candidate_repo, job.id, lib.id, episode_id=902),
    ]
    barrier = threading.Barrier(2)
    results = []

    def run(candidate):
        stub = StubSonarrClient()
        service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)
        barrier.wait()
        outcome, error = service.dispatch(job.id, [candidate.id], confirm=True)
        results.append((outcome, error, stub.calls))

    threads = [threading.Thread(target=run, args=(candidate,)) for candidate in candidates]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 2
    assert sorted(len(calls) for _, _, calls in results) == [0, 1]
    assert sum(outcome.batch.dispatched_count for outcome, _, _ in results) == 1


def test_manual_audit_keeps_excluded_items(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo, policy_repo
):
    lib = make_library(library_repo)
    policy_repo.update({"hourly_api_cap": 1})
    job = make_completed_scan_job(activity_repo, lib)
    first = make_candidate(candidate_repo, job.id, lib.id, episode_id=903)
    capped = make_candidate(candidate_repo, job.id, lib.id, episode_id=904)
    stub = StubSonarrClient()
    service = make_service(dispatch_planning_service, dispatch_repo, library_repo, stub)

    outcome, error = service.dispatch(job.id, [first.id, capped.id], confirm=True)

    assert error is None
    assert outcome.batch.state == "partial"
    by_candidate = {item.candidate_id: item for item in outcome.batch.items}
    assert by_candidate[first.id].state == "dispatched"
    assert by_candidate[capped.id].state == "excluded"
    assert "hourly dispatch cap" in by_candidate[capped.id].reason

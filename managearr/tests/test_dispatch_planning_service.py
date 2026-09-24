"""Tests for DispatchPlanningService: candidate validation, dedupe,
cross-job rejection, cooldown, hourly cap, and the dry-run preview path
(which must never call Sonarr - there's no Sonarr client wired into this
service at all, so any accidental network call would raise on its own)."""
from datetime import datetime, timedelta, timezone

from app.domain.dispatch import MAX_SELECTION_PER_REQUEST
from app.services.dispatch_planning_service import (
    CAPACITY_REASON,
    CANDIDATE_DUPLICATE_REASON,
    CANDIDATE_NOT_FOUND_REASON,
    CANDIDATE_WRONG_JOB_REASON,
    CANDIDATE_WRONG_LIBRARY_REASON,
    DUPLICATE_EPISODE_REASON,
    EMPTY_SELECTION_ERROR,
    JOB_HAS_NO_LIBRARY_ERROR,
    JOB_NOT_A_SONARR_SCAN_ERROR,
    JOB_NOT_COMPLETED_ERROR,
    JOB_NOT_FOUND_ERROR,
    TOO_MANY_SELECTED_ERROR,
)


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


# --- job/library validation ---------------------------------------------

def test_compute_job_not_found(dispatch_planning_service):
    result, error = dispatch_planning_service.compute(999, [1])
    assert result is None
    assert error == JOB_NOT_FOUND_ERROR


def test_compute_rejects_non_sonarr_scan_job(dispatch_planning_service, activity_repo):
    job = activity_repo.create(
        {"library_id": None, "library_name": "x", "job_type": "legacy", "state": "planned", "title": "t"}
    )
    result, error = dispatch_planning_service.compute(job.id, [1])
    assert error == JOB_NOT_A_SONARR_SCAN_ERROR


def test_compute_rejects_incomplete_scan_job(dispatch_planning_service, library_repo, activity_repo):
    lib = make_library(library_repo)
    job = activity_repo.create(
        {
            "library_id": lib.id,
            "library_name": lib.name,
            "job_type": "sonarr_scan",
            "state": "searching",
            "title": "t",
        }
    )
    result, error = dispatch_planning_service.compute(job.id, [1])
    assert error == JOB_NOT_COMPLETED_ERROR


def test_compute_rejects_job_with_no_library(dispatch_planning_service, activity_repo):
    job = activity_repo.create(
        {"library_id": None, "library_name": "x", "job_type": "sonarr_scan", "state": "completed", "title": "t"}
    )
    result, error = dispatch_planning_service.compute(job.id, [1])
    assert error == JOB_HAS_NO_LIBRARY_ERROR


def test_compute_rejects_disabled_library(dispatch_planning_service, library_repo, activity_repo):
    lib = make_library(library_repo, enabled=False)
    job = make_completed_scan_job(activity_repo, lib)
    result, error = dispatch_planning_service.compute(job.id, [1])
    assert error == "library is disabled"


# --- selection shape validation -----------------------------------------

def test_compute_rejects_empty_selection(dispatch_planning_service, library_repo, activity_repo):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    result, error = dispatch_planning_service.compute(job.id, [])
    assert error == EMPTY_SELECTION_ERROR


def test_compute_rejects_non_list_selection(dispatch_planning_service, library_repo, activity_repo):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    result, error = dispatch_planning_service.compute(job.id, "1,2,3")
    assert error == EMPTY_SELECTION_ERROR


def test_compute_rejects_more_than_max_selection(dispatch_planning_service, library_repo, activity_repo):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    result, error = dispatch_planning_service.compute(job.id, list(range(1, MAX_SELECTION_PER_REQUEST + 2)))
    assert error == TOO_MANY_SELECTED_ERROR


def test_compute_enforces_max_before_deduplication(dispatch_planning_service, library_repo, activity_repo):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    result, error = dispatch_planning_service.compute(
        job.id, [1] * (MAX_SELECTION_PER_REQUEST + 1)
    )
    assert result is None
    assert error == TOO_MANY_SELECTED_ERROR


# --- candidate validation: dedupe, not-found, cross-job -------------------

def test_compute_dedupes_repeated_ids(dispatch_planning_service, library_repo, activity_repo, candidate_repo):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=101)

    result, error = dispatch_planning_service.compute(job.id, [c.id, c.id])

    assert error is None
    assert result.requested_count == 2
    assert [x.id for x in result.eligible] == [c.id]
    assert any(x["reason"] == CANDIDATE_DUPLICATE_REASON for x in result.excluded)


def test_compute_excludes_candidate_not_found(dispatch_planning_service, library_repo, activity_repo):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)

    result, error = dispatch_planning_service.compute(job.id, [999999])

    assert error is None
    assert result.eligible == []
    assert result.excluded == [{"candidate_id": 999999, "reason": CANDIDATE_NOT_FOUND_REASON}]


def test_compute_excludes_candidate_from_a_different_job(
    dispatch_planning_service, library_repo, activity_repo, candidate_repo
):
    lib = make_library(library_repo)
    job_one = make_completed_scan_job(activity_repo, lib)
    job_two = make_completed_scan_job(activity_repo, lib)
    other_job_candidate = make_candidate(candidate_repo, job_two.id, lib.id, episode_id=555)

    result, error = dispatch_planning_service.compute(job_one.id, [other_job_candidate.id])

    assert error is None
    assert result.eligible == []
    assert result.excluded == [{"candidate_id": other_job_candidate.id, "reason": CANDIDATE_WRONG_JOB_REASON}]


def test_compute_excludes_candidate_with_wrong_library_ownership(
    dispatch_planning_service, library_repo, activity_repo, candidate_repo
):
    lib = make_library(library_repo)
    other_lib = make_library(library_repo, name="Other")
    job = make_completed_scan_job(activity_repo, lib)
    candidate = make_candidate(candidate_repo, job.id, other_lib.id, episode_id=556)

    result, error = dispatch_planning_service.compute(job.id, [candidate.id])

    assert error is None
    assert result.eligible == []
    assert result.excluded == [
        {"candidate_id": candidate.id, "reason": CANDIDATE_WRONG_LIBRARY_REASON}
    ]


def test_compute_dedupes_distinct_candidates_for_same_episode(
    dispatch_planning_service, library_repo, activity_repo, candidate_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    first = make_candidate(candidate_repo, job.id, lib.id, episode_id=557, ep=1)
    duplicate_episode = make_candidate(candidate_repo, job.id, lib.id, episode_id=557, ep=2)

    result, error = dispatch_planning_service.compute(job.id, [first.id, duplicate_episode.id])

    assert error is None
    assert [candidate.id for candidate in result.selected] == [first.id]
    assert {"candidate_id": duplicate_episode.id, "reason": DUPLICATE_EPISODE_REASON} in result.excluded


# --- happy path + capacity reporting --------------------------------------

def test_compute_selects_valid_candidates_within_capacity(
    dispatch_planning_service, library_repo, activity_repo, candidate_repo, policy_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c1 = make_candidate(candidate_repo, job.id, lib.id, episode_id=1)
    c2 = make_candidate(candidate_repo, job.id, lib.id, episode_id=2)

    result, error = dispatch_planning_service.compute(job.id, [c1.id, c2.id])

    assert error is None
    assert {c.id for c in result.eligible} == {c1.id, c2.id}
    assert {c.id for c in result.selected} == {c1.id, c2.id}
    assert result.excluded == []
    assert result.hourly_api_cap == policy_repo.get().hourly_api_cap


# --- cooldown --------------------------------------------------------------

def test_compute_excludes_episode_dispatched_within_cooldown(
    dispatch_planning_service, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=77)

    with dispatch_repo.db.connect() as conn:
        batch_id = dispatch_repo.create_batch(
            conn,
            {
                "scan_job_id": job.id,
                "library_id": lib.id,
                "library_name": lib.name,
                "mode": "manual",
                "state": "completed",
                "requested_count": 1,
                "selected_count": 1,
                "dispatched_count": 1,
                "sonarr_command_id": 1,
            },
        )
        item_ids = dispatch_repo.create_items(
            conn,
            batch_id,
            [
                {
                    "candidate_id": c.id,
                    "episode_id": c.episode_id,
                    "series_id": c.series_id,
                    "series_title": c.series_title,
                    "season_number": c.season_number,
                    "episode_number": c.episode_number,
                    "state": "dispatched",
                }
            ],
        )

    result, error = dispatch_planning_service.compute(job.id, [c.id])

    assert error is None
    assert result.eligible == []
    assert len(result.excluded) == 1
    assert "cooldown" in result.excluded[0]["reason"]


def test_compute_allows_episode_dispatched_outside_cooldown(
    dispatch_planning_service, library_repo, activity_repo, candidate_repo, dispatch_repo, policy_repo, monkeypatch
):
    lib = make_library(library_repo)
    policy_repo.update({"cooldown_minutes": 1})
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=88)

    # Create the completed audit at a controlled old timestamp; immutable
    # audit rows cannot be backdated after insertion.
    from app.persistence import dispatch_repository as dispatch_repository_module

    old_dispatch_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    monkeypatch.setattr(dispatch_repository_module, "_now", lambda: old_dispatch_time)
    with dispatch_repo.db.connect() as conn:
        batch_id = dispatch_repo.create_batch(
            conn,
            {
                "scan_job_id": job.id,
                "library_id": lib.id,
                "library_name": lib.name,
                "mode": "manual",
                "state": "completed",
                "requested_count": 1,
                "selected_count": 1,
                "dispatched_count": 1,
                "sonarr_command_id": 1,
            },
        )
        dispatch_repo.create_items(
            conn,
            batch_id,
            [
                {
                    "candidate_id": c.id,
                    "episode_id": c.episode_id,
                    "series_id": c.series_id,
                    "series_title": c.series_title,
                    "season_number": c.season_number,
                    "episode_number": c.episode_number,
                    "state": "dispatched",
                }
            ],
        )

    result, error = dispatch_planning_service.compute(job.id, [c.id])

    assert error is None
    assert [x.id for x in result.eligible] == [c.id]


def test_dry_run_dispatch_never_affects_cooldown(
    dispatch_planning_service, library_repo, activity_repo, candidate_repo
):
    """A preview (dry_run) batch must never count as a real dispatch -
    repeating the same preview must show the same candidate eligible
    every time."""
    lib = make_library(library_repo)
    job = make_completed_scan_job(activity_repo, lib)
    c = make_candidate(candidate_repo, job.id, lib.id, episode_id=99)

    first, error = dispatch_planning_service.preview(job.id, [c.id])
    assert error is None
    assert [x.id for x in first.eligible] == [c.id]

    second, error = dispatch_planning_service.preview(job.id, [c.id])
    assert error is None
    assert [x.id for x in second.eligible] == [c.id]


def test_dry_run_audits_selected_and_excluded_items_without_consuming_capacity(
    dispatch_planning_service, dispatch_repo, library_repo, activity_repo, candidate_repo, policy_repo
):
    lib = make_library(library_repo)
    policy_repo.update({"hourly_api_cap": 1})
    job = make_completed_scan_job(activity_repo, lib)
    first = make_candidate(candidate_repo, job.id, lib.id, episode_id=100)
    capped = make_candidate(candidate_repo, job.id, lib.id, episode_id=101)

    result, error = dispatch_planning_service.preview(job.id, [first.id, capped.id])

    assert error is None
    batch = dispatch_repo.get_batch(result.audit_batch_id)
    assert batch.mode == "dry_run"
    assert batch.state == "planned"
    assert [(item.candidate_id, item.state) for item in batch.items] == [
        (first.id, "planned"),
        (capped.id, "excluded"),
    ]
    assert batch.items[1].reason == CAPACITY_REASON
    assert dispatch_repo.count_dispatched_since(
        lib.id, datetime.now(timezone.utc) - timedelta(hours=1)
    ) == 0


# --- hourly cap --------------------------------------------------------

def test_compute_caps_selection_at_remaining_hourly_capacity(
    dispatch_planning_service, library_repo, activity_repo, candidate_repo, dispatch_repo, policy_repo
):
    lib = make_library(library_repo)
    policy_repo.update({"hourly_api_cap": 1})
    job = make_completed_scan_job(activity_repo, lib)
    c1 = make_candidate(candidate_repo, job.id, lib.id, episode_id=201)
    c2 = make_candidate(candidate_repo, job.id, lib.id, episode_id=202)

    result, error = dispatch_planning_service.compute(job.id, [c1.id, c2.id])

    assert error is None
    assert {c.id for c in result.eligible} == {c1.id, c2.id}
    assert len(result.selected) == 1
    assert result.remaining_capacity == 1
    assert {"candidate_id": c2.id, "reason": CAPACITY_REASON} in result.excluded


def test_compute_reports_zero_remaining_capacity_when_cap_already_used(
    dispatch_planning_service, library_repo, activity_repo, candidate_repo, dispatch_repo, policy_repo
):
    lib = make_library(library_repo)
    policy_repo.update({"hourly_api_cap": 1})
    job = make_completed_scan_job(activity_repo, lib)
    used = make_candidate(candidate_repo, job.id, lib.id, episode_id=301)

    with dispatch_repo.db.connect() as conn:
        batch_id = dispatch_repo.create_batch(
            conn,
            {
                "scan_job_id": job.id,
                "library_id": lib.id,
                "library_name": lib.name,
                "mode": "manual",
                "state": "completed",
                "requested_count": 1,
                "selected_count": 1,
                "dispatched_count": 1,
                "sonarr_command_id": 1,
            },
        )
        item_ids = dispatch_repo.create_items(
            conn,
            batch_id,
            [
                {
                    "candidate_id": used.id,
                    "episode_id": used.episode_id,
                    "series_id": used.series_id,
                    "series_title": used.series_title,
                    "season_number": used.season_number,
                    "episode_number": used.episode_number,
                    "state": "dispatched",
                }
            ],
        )

    other = make_candidate(candidate_repo, job.id, lib.id, episode_id=302)
    result, error = dispatch_planning_service.compute(job.id, [other.id])

    assert error is None
    # `used` isn't in cooldown scope here (default cooldown 15 minutes
    # would also exclude it, but we only asked about `other`).
    assert result.remaining_capacity == 0
    assert result.selected == []
    assert [c.id for c in result.eligible] == [other.id]

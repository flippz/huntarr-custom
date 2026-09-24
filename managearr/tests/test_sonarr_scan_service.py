"""Tests for SonarrScanService: connection tests, scan orchestration,
candidate persistence, idempotent snapshots, and error handling. The
Sonarr client is always a stub/fake here - no real network access."""
import pytest

from app.adapters.sonarr_client import SonarrConnectionError, SonarrDataError
from app.services import sonarr_scan_service as scan_service_module
from app.services.sonarr_scan_service import (
    LIBRARY_DISABLED_ERROR,
    LIBRARY_NOT_FOUND_ERROR,
    MISSING_API_KEY_ERROR,
    UNSUPPORTED_LIBRARY_TYPE_ERROR,
    SonarrScanService,
)


class StubSonarrClient:
    """Configurable fake standing in for SonarrClient. Never makes a
    network call - all behavior is pre-programmed by the test."""

    def __init__(
        self,
        *,
        status=None,
        status_error=None,
        series=None,
        series_error=None,
        episodes_by_series=None,
        episode_errors=None,
    ):
        self._status = status or {"version": "4.0.1", "instance_name": "Sonarr"}
        self._status_error = status_error
        self._series = series if series is not None else []
        self._series_error = series_error
        self._episodes_by_series = episodes_by_series or {}
        self._episode_errors = episode_errors or {}

    def system_status(self):
        if self._status_error:
            raise self._status_error
        return self._status

    def get_series(self):
        if self._series_error:
            raise self._series_error
        return self._series

    def get_episodes(self, series_id):
        if series_id in self._episode_errors:
            raise self._episode_errors[series_id]
        return self._episodes_by_series.get(series_id, [])


def make_factory(stub: StubSonarrClient):
    def factory(base_url, api_key, timeout=None):
        return stub

    return factory


def make_service(library_repo, activity_repo, candidate_repo, stub: StubSonarrClient) -> SonarrScanService:
    return SonarrScanService(library_repo, activity_repo, candidate_repo, client_factory=make_factory(stub))


def create_sonarr_library(library_repo, **overrides):
    data = {
        "name": "Sonarr Main",
        "type": "sonarr",
        "url": "http://sonarr:8989",
        "api_key": "secret-key",
        "enabled": True,
    }
    data.update(overrides)
    return library_repo.create(data)


def episode(id, season, ep, *, monitored=True, has_file=False, air_date="2026-01-01"):
    return {
        "id": id,
        "seasonNumber": season,
        "episodeNumber": ep,
        "monitored": monitored,
        "hasFile": has_file,
        "airDate": air_date,
    }


def series(id, title, *, monitored=True):
    return {"id": id, "title": title, "monitored": monitored}


# --- test_connection -------------------------------------------------------

def test_connection_success(library_repo, activity_repo, candidate_repo):
    lib = create_sonarr_library(library_repo)
    service = make_service(library_repo, activity_repo, candidate_repo, StubSonarrClient())
    status, error = service.test_connection(lib.id)
    assert error is None
    assert status["version"] == "4.0.1"


def test_connection_library_not_found(library_repo, activity_repo, candidate_repo):
    service = make_service(library_repo, activity_repo, candidate_repo, StubSonarrClient())
    status, error = service.test_connection(999)
    assert status is None
    assert error == LIBRARY_NOT_FOUND_ERROR


def test_connection_unsupported_library_type(library_repo, activity_repo, candidate_repo):
    lib = create_sonarr_library(library_repo, type="radarr")
    service = make_service(library_repo, activity_repo, candidate_repo, StubSonarrClient())
    status, error = service.test_connection(lib.id)
    assert status is None
    assert error == UNSUPPORTED_LIBRARY_TYPE_ERROR


def test_connection_disabled_library(library_repo, activity_repo, candidate_repo):
    lib = create_sonarr_library(library_repo, enabled=False)
    service = make_service(library_repo, activity_repo, candidate_repo, StubSonarrClient())
    status, error = service.test_connection(lib.id)
    assert status is None
    assert error == LIBRARY_DISABLED_ERROR


def test_connection_missing_api_key(library_repo, activity_repo, candidate_repo):
    # Bypass domain validation (which normally requires a non-empty key)
    # by writing directly through the repository, simulating a row with
    # a blank key however it got there.
    lib = library_repo.create(
        {"name": "Sonarr", "type": "sonarr", "url": "http://sonarr:8989", "api_key": "", "enabled": True}
    )
    service = make_service(library_repo, activity_repo, candidate_repo, StubSonarrClient())
    status, error = service.test_connection(lib.id)
    assert status is None
    assert error == MISSING_API_KEY_ERROR


def test_connection_timeout_returns_safe_message(library_repo, activity_repo, candidate_repo):
    lib = create_sonarr_library(library_repo)
    stub = StubSonarrClient(status_error=SonarrConnectionError("Sonarr request timed out"))
    service = make_service(library_repo, activity_repo, candidate_repo, stub)
    status, error = service.test_connection(lib.id)
    assert status is None
    assert error == "Sonarr request timed out"
    assert lib.url not in error
    assert lib.api_key not in error


# --- run_scan ----------------------------------------------------------

def test_scan_persists_missing_candidates_and_completes_job(library_repo, activity_repo, candidate_repo):
    lib = create_sonarr_library(library_repo)
    stub = StubSonarrClient(
        series=[series(1, "Show A")],
        episodes_by_series={
            1: [
                episode(10, 1, 1, has_file=True),  # has file - excluded
                episode(11, 1, 2),  # missing, aired, monitored - candidate
                episode(12, 1, 3, air_date="2099-01-01"),  # future - excluded
            ]
        },
    )
    service = make_service(library_repo, activity_repo, candidate_repo, stub)

    job, error = service.run_scan(lib.id)

    assert error is None
    assert job.state == "completed"
    assert job.job_type == "sonarr_scan"
    assert job.candidate_count == 1
    assert job.library_id == lib.id

    candidates = candidate_repo.list_for_job(job.id)
    assert len(candidates) == 1
    assert candidates[0].episode_id == 11
    assert candidates[0].series_title == "Show A"


def test_scan_excludes_unmonitored_series(library_repo, activity_repo, candidate_repo):
    lib = create_sonarr_library(library_repo)
    stub = StubSonarrClient(
        series=[series(1, "Show A", monitored=False)],
        episodes_by_series={1: [episode(10, 1, 1)]},
    )
    service = make_service(library_repo, activity_repo, candidate_repo, stub)
    job, error = service.run_scan(lib.id)
    assert error is None
    assert job.candidate_count == 0


def test_scan_library_not_found(library_repo, activity_repo, candidate_repo):
    service = make_service(library_repo, activity_repo, candidate_repo, StubSonarrClient())
    job, error = service.run_scan(999)
    assert job is None
    assert error == LIBRARY_NOT_FOUND_ERROR


def test_scan_disabled_library_never_creates_job(library_repo, activity_repo, candidate_repo):
    lib = create_sonarr_library(library_repo, enabled=False)
    service = make_service(library_repo, activity_repo, candidate_repo, StubSonarrClient())
    job, error = service.run_scan(lib.id)
    assert job is None
    assert error == LIBRARY_DISABLED_ERROR
    assert activity_repo.list_all() == []


def test_scan_marks_job_failed_on_series_fetch_error(library_repo, activity_repo, candidate_repo):
    lib = create_sonarr_library(library_repo)
    stub = StubSonarrClient(series_error=SonarrConnectionError("Could not connect to the Sonarr host"))
    service = make_service(library_repo, activity_repo, candidate_repo, stub)

    job, error = service.run_scan(lib.id)

    assert error is None
    assert job is not None
    assert job.state == "failed"
    assert job.details == "Could not connect to the Sonarr host"
    assert job.candidate_count == 0
    assert candidate_repo.list_for_job(job.id) == []


def test_scan_skips_series_with_episode_fetch_error_but_completes(library_repo, activity_repo, candidate_repo):
    lib = create_sonarr_library(library_repo)
    stub = StubSonarrClient(
        series=[series(1, "Broken Show"), series(2, "Good Show")],
        episodes_by_series={2: [episode(20, 1, 1)]},
        episode_errors={1: SonarrDataError("Sonarr episode response was not a list")},
    )
    service = make_service(library_repo, activity_repo, candidate_repo, stub)

    job, error = service.run_scan(lib.id)

    assert error is None
    assert job.state == "completed"
    assert job.candidate_count == 1
    assert "1 series were skipped" in job.details


def test_scan_error_details_never_contain_secret_api_key(library_repo, activity_repo, candidate_repo):
    lib = create_sonarr_library(library_repo, api_key="ultra-secret")
    stub = StubSonarrClient(series_error=SonarrConnectionError("Sonarr request timed out"))
    service = make_service(library_repo, activity_repo, candidate_repo, stub)
    job, error = service.run_scan(lib.id)
    assert "ultra-secret" not in job.details


def test_repeat_scans_create_separate_jobs_without_corrupting_prior_snapshot(
    library_repo, activity_repo, candidate_repo
):
    lib = create_sonarr_library(library_repo)
    stub = StubSonarrClient(
        series=[series(1, "Show A")],
        episodes_by_series={1: [episode(11, 1, 2)]},
    )
    service = make_service(library_repo, activity_repo, candidate_repo, stub)

    job_one, _ = service.run_scan(lib.id)
    job_two, _ = service.run_scan(lib.id)

    assert job_one.id != job_two.id
    candidates_one = candidate_repo.list_for_job(job_one.id)
    candidates_two = candidate_repo.list_for_job(job_two.id)
    assert len(candidates_one) == 1
    assert len(candidates_two) == 1
    assert candidates_one[0].id != candidates_two[0].id
    # First job's snapshot is untouched by the second scan.
    assert candidate_repo.list_for_job(job_one.id) == candidates_one


def test_scan_truncates_at_max_candidates_and_notes_it(library_repo, activity_repo, candidate_repo, monkeypatch):
    monkeypatch.setattr(scan_service_module, "MAX_CANDIDATES_PER_SCAN", 2)
    lib = create_sonarr_library(library_repo)
    stub = StubSonarrClient(
        series=[series(1, "Show A")],
        episodes_by_series={1: [episode(11, 1, 1), episode(12, 1, 2), episode(13, 1, 3)]},
    )
    service = make_service(library_repo, activity_repo, candidate_repo, stub)

    job, error = service.run_scan(lib.id)

    assert error is None
    assert job.candidate_count == 2
    assert "truncated" in job.details.lower()
    assert len(candidate_repo.list_for_job(job.id)) == 2

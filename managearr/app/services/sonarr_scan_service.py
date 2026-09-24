"""Orchestrates read-only Sonarr connection tests and candidate scans.

Safety invariant for this whole module: it never calls anything on
``SonarrClient`` other than ``system_status``, ``get_series``, and
``get_episodes`` - all read-only GETs. No Sonarr command is ever sent
and no Sonarr state is ever mutated.
"""
from datetime import date

from ..adapters.sonarr_client import SonarrClient, SonarrError
from ..domain.scan_candidate import derive_missing_candidate
from ..persistence.activity_repository import ActivityRepository
from ..persistence.library_repository import LibraryRepository
from ..persistence.scan_candidate_repository import ScanCandidateRepository
from .library_readiness import (
    LIBRARY_DISABLED_ERROR,
    LIBRARY_NOT_FOUND_ERROR,
    MISSING_API_KEY_ERROR,
    UNSUPPORTED_LIBRARY_TYPE_ERROR,
    VALIDATION_ERRORS,
    ready_sonarr_library,
)

# Hard cap on candidate rows persisted per scan job. Large libraries are
# truncated rather than unbounded; the job's ``details`` text says so
# when it happens (see ``run_scan`` below).
MAX_CANDIDATES_PER_SCAN = 500

# Re-exported here (rather than only in library_readiness) so existing
# imports of these names from this module keep working unchanged.
__all__ = [
    "MAX_CANDIDATES_PER_SCAN",
    "UNSUPPORTED_LIBRARY_TYPE_ERROR",
    "LIBRARY_NOT_FOUND_ERROR",
    "LIBRARY_DISABLED_ERROR",
    "MISSING_API_KEY_ERROR",
    "VALIDATION_ERRORS",
    "SonarrScanService",
]


class SonarrScanService:
    def __init__(
        self,
        library_repo: LibraryRepository,
        activity_repo: ActivityRepository,
        candidate_repo: ScanCandidateRepository,
        *,
        client_factory=SonarrClient,
        timeout: int | None = None,
    ):
        self.library_repo = library_repo
        self.activity_repo = activity_repo
        self.candidate_repo = candidate_repo
        self._client_factory = client_factory
        self._timeout = timeout

    def _make_client(self, library) -> SonarrClient:
        if self._timeout is not None:
            return self._client_factory(library.url, library.api_key, timeout=self._timeout)
        return self._client_factory(library.url, library.api_key)

    def _ready_sonarr_library(self, library_id: int):
        """Return ``(library, None)`` or ``(None, error_message)``."""
        return ready_sonarr_library(self.library_repo, library_id)

    def test_connection(self, library_id: int) -> tuple[dict | None, str | None]:
        """Read-only connectivity/version check. Never mutates Sonarr."""
        library, error = self._ready_sonarr_library(library_id)
        if error:
            return None, error

        client = self._make_client(library)
        try:
            status = client.system_status()
        except SonarrError as exc:
            return None, str(exc)
        return status, None

    def run_scan(self, library_id: int):
        """Run one read-only candidate scan and persist it as a durable
        job + candidate snapshot. Returns ``(job, error)`` - ``error`` is
        only set for validation failures that happen *before* a job is
        created (not-found/unsupported/disabled/missing key); once a job
        exists, failures are recorded on the job itself (state=failed)
        and returned as ``(job, None)``.
        """
        library, error = self._ready_sonarr_library(library_id)
        if error:
            return None, error

        job = self.activity_repo.create(
            {
                "library_id": library.id,
                "library_name": library.name,
                "job_type": "sonarr_scan",
                "state": "searching",
                "title": f"Sonarr scan: {library.name}",
                "details": "Scan in progress.",
            }
        )

        client = self._make_client(library)

        try:
            series_list = client.get_series()
        except SonarrError as exc:
            failed = self.activity_repo.update_state(job.id, state="failed", details=str(exc))
            return failed, None

        today = date.today()
        candidates: list[dict] = []
        skipped_series = 0
        truncated = False

        for series in series_list:
            if not isinstance(series, dict):
                continue
            series_id = series.get("id")
            if series_id is None:
                continue

            try:
                episodes = client.get_episodes(series_id)
            except SonarrError:
                skipped_series += 1
                continue

            for episode in episodes:
                if not isinstance(episode, dict):
                    continue
                if len(candidates) >= MAX_CANDIDATES_PER_SCAN:
                    truncated = True
                    break
                candidate = derive_missing_candidate(series, episode, today=today)
                if candidate is not None:
                    candidates.append(candidate)

            if truncated:
                break

        if candidates:
            self.candidate_repo.create_many(job.id, library.id, candidates)

        details_parts = [
            f"Found {len(candidates)} missing monitored aired episode(s) across {len(series_list)} series."
        ]
        if skipped_series:
            details_parts.append(f"{skipped_series} series were skipped due to Sonarr errors.")
        if truncated:
            details_parts.append(f"Results truncated at {MAX_CANDIDATES_PER_SCAN} candidates.")

        completed = self.activity_repo.update_state(
            job.id,
            state="completed",
            details=" ".join(details_parts),
            candidate_count=len(candidates),
        )
        return completed, None

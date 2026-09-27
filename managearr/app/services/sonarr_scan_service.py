"""Orchestrates read-only Sonarr connection tests and candidate scans.

Safety invariant for this whole module: it never calls anything on
``SonarrClient`` other than ``system_status``, ``get_series``,
``get_episodes``, and ``get_cutoff_unmet_episodes`` - all read-only GETs.
No Sonarr command is ever sent
and no Sonarr state is ever mutated.
"""
from datetime import date

from ..adapters.sonarr_client import SonarrClient, SonarrError
from ..domain.scan_candidate import derive_missing_candidate, derive_upgrade_candidate
from ..persistence.activity_repository import ActivityRepository
from ..persistence.library_repository import LibraryRepository
from ..persistence.policy_repository import PolicyRepository
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
UPGRADE_PAGE_SIZE = 100
MAX_UPGRADE_PAGES = 5

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
        policy_repo: PolicyRepository | None = None,
        *,
        client_factory=SonarrClient,
        timeout: int | None = None,
    ):
        self.library_repo = library_repo
        self.activity_repo = activity_repo
        self.candidate_repo = candidate_repo
        self.policy_repo = policy_repo or PolicyRepository(library_repo.db)
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
        policy = self.policy_repo.get()
        missing_pool: list[dict] = []
        upgrade_pool: list[dict] = []
        skipped_series = 0
        missing_source_truncated = False
        upgrade_source_truncated = False
        upgrade_error = False

        # Build the complete cross-library identity map before bounded missing
        # discovery can stop early. Cutoff records may refer to any series in
        # this Sonarr library, including one after the missing scan cap.
        known_series: dict[int, dict] = {}
        for series in series_list:
            if not isinstance(series, dict):
                continue
            series_id = series.get("id")
            if isinstance(series_id, int) and not isinstance(series_id, bool) and series_id > 0:
                known_series[series_id] = series

        if policy.missing_enabled:
            for series_id, series in known_series.items():
                try:
                    episodes = client.get_episodes(series_id)
                except SonarrError:
                    skipped_series += 1
                    continue

                for episode in episodes:
                    if not isinstance(episode, dict):
                        continue
                    if len(missing_pool) >= MAX_CANDIDATES_PER_SCAN:
                        missing_source_truncated = True
                        break
                    candidate = derive_missing_candidate(series, episode, today=today)
                    if candidate is not None:
                        missing_pool.append(candidate)
                if missing_source_truncated:
                    break

        # This read remains bounded even when missing discovery reached its
        # own cap. When both modes are enabled, allocation below reserves a
        # deterministic 20% share for eligible upgrades without exceeding the
        # one shared persistence cap.
        if policy.upgrades_enabled:
            seen_episode_ids = {candidate["episode_id"] for candidate in missing_pool}
            for page in range(1, MAX_UPGRADE_PAGES + 1):
                try:
                    result = client.get_cutoff_unmet_episodes(
                        page=page, page_size=UPGRADE_PAGE_SIZE
                    )
                except SonarrError:
                    upgrade_error = True
                    upgrade_pool = []
                    break

                records = result["records"]
                for episode in records:
                    if not isinstance(episode, dict):
                        continue
                    if len(upgrade_pool) >= MAX_CANDIDATES_PER_SCAN:
                        upgrade_source_truncated = True
                        break
                    series = known_series.get(episode.get("seriesId"))
                    if series is None:
                        continue
                    candidate = derive_upgrade_candidate(series, episode, today=today)
                    if candidate is None or candidate["episode_id"] in seen_episode_ids:
                        continue
                    upgrade_pool.append(candidate)
                    seen_episode_ids.add(candidate["episode_id"])

                if upgrade_source_truncated or not records:
                    break
                if page * UPGRADE_PAGE_SIZE >= result["total_records"]:
                    break
                if page == MAX_UPGRADE_PAGES:
                    upgrade_source_truncated = True

        if policy.missing_enabled and policy.upgrades_enabled:
            upgrade_reserve = max(1, MAX_CANDIDATES_PER_SCAN // 5)
            missing_take = min(len(missing_pool), MAX_CANDIDATES_PER_SCAN - upgrade_reserve)
            upgrade_take = min(len(upgrade_pool), MAX_CANDIDATES_PER_SCAN - missing_take)
            # Any unused upgrade reservation returns to missing candidates.
            missing_take = min(len(missing_pool), MAX_CANDIDATES_PER_SCAN - upgrade_take)
        elif policy.missing_enabled:
            missing_take = min(len(missing_pool), MAX_CANDIDATES_PER_SCAN)
            upgrade_take = 0
        elif policy.upgrades_enabled:
            missing_take = 0
            upgrade_take = min(len(upgrade_pool), MAX_CANDIDATES_PER_SCAN)
        else:
            missing_take = upgrade_take = 0

        candidates = missing_pool[:missing_take] + upgrade_pool[:upgrade_take]
        result_truncated = missing_take < len(missing_pool) or upgrade_take < len(upgrade_pool)
        missing_count = missing_take
        upgrade_count = upgrade_take

        if candidates:
            self.candidate_repo.create_many(job.id, library.id, candidates)

        details_parts = [
            f"Persisted {missing_count} missing monitored aired episode(s) and "
            f"{upgrade_count} monitored aired quality-upgrade episode(s) across {len(series_list)} series."
        ]
        if policy.missing_enabled and policy.upgrades_enabled:
            details_parts.append(
                "The shared candidate cap reserves up to 20% for eligible upgrades; unused capacity returns to either kind."
            )
        if not policy.missing_enabled:
            details_parts.append("Missing candidate hunting was disabled by policy.")
        if not policy.upgrades_enabled:
            details_parts.append("Quality-upgrade hunting was disabled by policy.")
        if upgrade_error:
            details_parts.append(
                "The quality-upgrade source read failed; the upgrade portion of this completed snapshot is incomplete and failed closed."
            )
        if skipped_series:
            details_parts.append(f"{skipped_series} series were skipped due to Sonarr errors.")
        if missing_source_truncated:
            details_parts.append(
                f"Missing source discovery was truncated after {MAX_CANDIDATES_PER_SCAN} eligible candidates."
            )
        if upgrade_source_truncated:
            details_parts.append(
                f"Quality-upgrade source discovery was truncated after at most {MAX_UPGRADE_PAGES} bounded page(s)."
            )
        if result_truncated:
            details_parts.append(
                f"Persisted results were truncated at the shared {MAX_CANDIDATES_PER_SCAN}-candidate cap."
            )

        completed = self.activity_repo.update_state(
            job.id,
            state="completed",
            details=" ".join(details_parts),
            candidate_count=len(candidates),
        )
        return completed, None

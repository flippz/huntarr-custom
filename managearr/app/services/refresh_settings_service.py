"""API-facing wrapper around the read-only refresh ledger.

Only reads and queues durable requests here - see ``app/worker.py`` and
``app/services/refresh_service.py`` for the code that actually talks to
Sonarr. There is no live mode and no way to trigger reconciliation from this
service; automatic reconciliation is entirely worker-driven and bounded.
"""
from ..domain.refresh import validate_refresh_settings


class RefreshSettingsService:
    def __init__(self, refresh_repo):
        self.refresh_repo = refresh_repo

    def status(self) -> dict:
        return self.refresh_repo.status()

    def update_settings(self, payload) -> tuple[dict | None, list[str]]:
        errors = validate_refresh_settings(payload)
        if errors:
            return None, errors
        settings = self.refresh_repo.update_settings(payload)
        return settings.to_dict(), []

    def queue_manual_scan(self, library_id: int | None) -> tuple[list[dict], str | None]:
        return self.refresh_repo.queue_manual_scans(library_id)

    def list_runs(self, limit: int = 50) -> list[dict]:
        return self.refresh_repo.list_runs(limit=limit)

    def run_detail(self, run_id: int) -> dict | None:
        return self.refresh_repo.get_run(run_id)

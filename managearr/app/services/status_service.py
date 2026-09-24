"""Application service backing /health and /api/v1/status.

Reports PostgreSQL connectivity and the applied schema version only -
never the configured host, port, user, or password.
"""
from ..persistence.database import Database
from ..persistence.library_repository import LibraryRepository
from ..persistence.migrations import current_version

APP_VERSION = "1.0.0-preview"


class StatusService:
    def __init__(self, db: Database, library_repository: LibraryRepository):
        self.db = db
        self.library_repository = library_repository

    def _schema_version(self) -> int | None:
        try:
            return current_version(self.db)
        except Exception:
            return None

    def health(self) -> dict:
        db_ok = self.db.health_check()
        return {
            "status": "ok" if db_ok else "error",
            "database": db_ok,
            "schema_version": self._schema_version() if db_ok else None,
        }

    def status(self) -> dict:
        db_ok = self.db.health_check()
        counts = self.library_repository.counts() if db_ok else {"configured": 0, "enabled": 0}
        return {
            "status": "ok" if db_ok else "error",
            "version": APP_VERSION,
            "database": {
                "connected": db_ok,
                "schema_version": self._schema_version() if db_ok else None,
            },
            "libraries": counts,
        }

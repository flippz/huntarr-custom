"""Application service backing /health and /api/v2/status."""
from ..persistence.database import Database
from ..persistence.library_repository import LibraryRepository

V2_VERSION = "2.0.0-preview"


class StatusService:
    def __init__(self, db: Database, library_repository: LibraryRepository):
        self.db = db
        self.library_repository = library_repository

    def health(self) -> dict:
        db_ok = self.db.health_check()
        return {"status": "ok" if db_ok else "error", "database": db_ok}

    def status(self) -> dict:
        db_ok = self.db.health_check()
        counts = self.library_repository.counts() if db_ok else {"configured": 0, "enabled": 0}
        return {
            "status": "ok" if db_ok else "error",
            "version": V2_VERSION,
            "database": {"connected": db_ok},
            "libraries": counts,
        }

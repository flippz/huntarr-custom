"""Persistence for durable activity jobs.

No seed data is ever inserted here - the table starts empty and only
grows through ``create`` calls made by a future search engine. Tests
use ``create`` directly to exercise the read paths.
"""
from datetime import datetime, timezone

from ..domain.activity import ActivityJob
from .database import Database


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_job(row) -> ActivityJob:
    return ActivityJob(
        id=row["id"],
        library_id=row["library_id"],
        library_name=row["library_name"],
        state=row["state"],
        title=row["title"],
        details=row["details"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class ActivityRepository:
    def __init__(self, db: Database):
        self.db = db

    def list_all(self, *, state: str | None = None, limit: int = 100) -> list[ActivityJob]:
        query = "SELECT * FROM activity_jobs"
        params: tuple = ()
        if state:
            query += " WHERE state = ?"
            params = (state,)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params = params + (limit,)

        with self.db.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [_row_to_job(row) for row in rows]

    def get(self, job_id: int) -> ActivityJob | None:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM activity_jobs WHERE id = ?", (job_id,)
            ).fetchone()
        return _row_to_job(row) if row else None

    def create(self, data: dict) -> ActivityJob:
        now = _now()
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO activity_jobs (library_id, library_name, state, title, details, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data.get("library_id"),
                    data["library_name"],
                    data["state"],
                    data["title"],
                    data.get("details", ""),
                    now,
                    now,
                ),
            )
            new_id = cursor.lastrowid
        return self.get(new_id)

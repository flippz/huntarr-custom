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
        job_type=row["job_type"],
        candidate_count=row["candidate_count"],
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
                INSERT INTO activity_jobs (
                    library_id, library_name, job_type, state, title, details,
                    candidate_count, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data.get("library_id"),
                    data["library_name"],
                    data.get("job_type", "legacy"),
                    data["state"],
                    data["title"],
                    data.get("details", ""),
                    data.get("candidate_count", 0),
                    now,
                    now,
                ),
            )
            new_id = cursor.lastrowid
        return self.get(new_id)

    def update_state(
        self,
        job_id: int,
        *,
        state: str,
        details: str | None = None,
        candidate_count: int | None = None,
    ) -> ActivityJob | None:
        """Transition an existing job's state (e.g. searching -> completed
        or searching -> failed). Used by ``SonarrScanService`` - never
        exposed as a write endpoint on the read-only activity API."""
        existing = self.get(job_id)
        if existing is None:
            return None

        now = _now()
        new_details = existing.details if details is None else details
        new_count = existing.candidate_count if candidate_count is None else candidate_count

        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE activity_jobs
                SET state = ?, details = ?, candidate_count = ?, updated_at = ?
                WHERE id = ?
                """,
                (state, new_details, new_count, now, job_id),
            )
        return self.get(job_id)

"""Persistence for durable scan candidate snapshot rows.

Rows are write-once: ``create_many`` inserts a batch tied to one
``job_id`` and nothing ever updates or deletes them directly (they are
cleaned up automatically via ``ON DELETE CASCADE`` if their parent job
is ever deleted). A repeat scan of the same library creates a new
job_id and a fresh batch of rows, leaving prior scans' rows intact.
"""
from datetime import datetime, timezone

from ..domain.scan_candidate import ScanCandidate
from .database import Database


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_candidate(row) -> ScanCandidate:
    return ScanCandidate(
        id=row["id"],
        job_id=row["job_id"],
        library_id=row["library_id"],
        series_id=row["series_id"],
        series_title=row["series_title"],
        episode_id=row["episode_id"],
        season_number=row["season_number"],
        episode_number=row["episode_number"],
        air_date=row["air_date"],
        reason=row["reason"],
        created_at=row["created_at"].isoformat(),
    )


class ScanCandidateRepository:
    def __init__(self, db: Database):
        self.db = db

    def create_many(self, job_id: int, library_id: int | None, candidates: list[dict]) -> None:
        if not candidates:
            return
        now = _now()
        with self.db.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO scan_candidates (
                        job_id, library_id, series_id, series_title, episode_id,
                        season_number, episode_number, air_date, reason, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            job_id,
                            library_id,
                            c["series_id"],
                            c["series_title"],
                            c["episode_id"],
                            c["season_number"],
                            c["episode_number"],
                            c.get("air_date"),
                            c["reason"],
                            now,
                        )
                        for c in candidates
                    ],
                )

    def list_for_job(self, job_id: int) -> list[ScanCandidate]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM scan_candidates
                WHERE job_id = %s
                ORDER BY LOWER(series_title), season_number, episode_number
                """,
                (job_id,),
            ).fetchall()
        return [_row_to_candidate(row) for row in rows]

    def count_for_job(self, job_id: int) -> int:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM scan_candidates WHERE job_id = %s", (job_id,)
            ).fetchone()
        return row["c"]

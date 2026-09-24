"""CRUD persistence for ArrLibrary rows."""
from datetime import datetime, timezone

from ..domain.arr_library import ArrLibrary
from .database import Database


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_library(row) -> ArrLibrary:
    return ArrLibrary(
        id=row["id"],
        name=row["name"],
        type=row["type"],
        url=row["url"],
        api_key=row["api_key"],
        enabled=bool(row["enabled"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class LibraryRepository:
    def __init__(self, db: Database):
        self.db = db

    def list_all(self) -> list[ArrLibrary]:
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM arr_libraries ORDER BY name COLLATE NOCASE"
            ).fetchall()
        return [_row_to_library(row) for row in rows]

    def get(self, library_id: int) -> ArrLibrary | None:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM arr_libraries WHERE id = ?", (library_id,)
            ).fetchone()
        return _row_to_library(row) if row else None

    def create(self, data: dict) -> ArrLibrary:
        now = _now()
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO arr_libraries (name, type, url, api_key, enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data["name"],
                    data["type"],
                    data["url"],
                    data["api_key"],
                    int(bool(data.get("enabled", True))),
                    now,
                    now,
                ),
            )
            new_id = cursor.lastrowid
        return self.get(new_id)

    def update(self, library_id: int, data: dict) -> ArrLibrary | None:
        existing = self.get(library_id)
        if existing is None:
            return None

        merged = existing.to_dict()
        merged.update(data)
        now = _now()

        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE arr_libraries
                SET name = ?, type = ?, url = ?, api_key = ?, enabled = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    merged["name"],
                    merged["type"],
                    merged["url"],
                    merged["api_key"],
                    int(bool(merged["enabled"])),
                    now,
                    library_id,
                ),
            )
        return self.get(library_id)

    def delete(self, library_id: int) -> bool:
        with self.db.connect() as conn:
            cursor = conn.execute(
                "DELETE FROM arr_libraries WHERE id = ?", (library_id,)
            )
        return cursor.rowcount > 0

    def counts(self) -> dict:
        with self.db.connect() as conn:
            total = conn.execute("SELECT COUNT(*) AS c FROM arr_libraries").fetchone()["c"]
            enabled = conn.execute(
                "SELECT COUNT(*) AS c FROM arr_libraries WHERE enabled = 1"
            ).fetchone()["c"]
        return {"configured": total, "enabled": enabled}

"""SQLite connection management and schema bootstrap for Huntarr v2.

Deliberately minimal: no ORM, a short-lived connection per operation
(SQLite handles this fine at the scale this preview targets), and
plain ``CREATE TABLE IF NOT EXISTS`` migrations.
"""
import os
import sqlite3
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS arr_libraries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    url TEXT NOT NULL,
    api_key TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS automation_policy (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    missing_enabled INTEGER NOT NULL,
    upgrades_enabled INTEGER NOT NULL,
    cycle_interval_minutes INTEGER NOT NULL,
    hourly_api_cap INTEGER NOT NULL,
    successful_grab_target INTEGER NOT NULL,
    dispatch_interval_seconds INTEGER NOT NULL,
    queue_target INTEGER NOT NULL,
    cooldown_minutes INTEGER NOT NULL,
    search_order TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS activity_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    library_id INTEGER,
    library_name TEXT NOT NULL,
    state TEXT NOT NULL,
    title TEXT NOT NULL,
    details TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (library_id) REFERENCES arr_libraries(id) ON DELETE SET NULL
);
"""


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        directory = os.path.dirname(os.path.abspath(db_path))
        if directory:
            os.makedirs(directory, exist_ok=True)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    def health_check(self) -> bool:
        try:
            with self.connect() as conn:
                conn.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False

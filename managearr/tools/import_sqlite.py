#!/usr/bin/env python3
"""One-time, explicit importer: copies Managearr's legacy SQLite preview
database into PostgreSQL.

Read-only against SQLite - this script never writes to the source file.
Refuses to write into a non-empty PostgreSQL target unless
``--allow-nonempty`` is passed, so accidentally re-running it can never
silently duplicate or corrupt data. IDs, timestamps, and all row data
are preserved exactly; PostgreSQL's identity sequences are reset to
continue after the highest imported ID once the import completes. The
whole import (all four tables) runs inside one PostgreSQL transaction -
any error rolls back everything this run wrote, leaving the target
exactly as it was before the script ran.

Usage:
    python tools/import_sqlite.py --sqlite-path /path/to/managearr.db [--dry-run] [--allow-nonempty]

Connection settings are read from the same MANAGEARR_DB_* environment
variables (including MANAGEARR_DB_PASSWORD_FILE) as the main
application - see config.py.

Never prints an API key, in --dry-run or normal mode - only row counts
and non-secret identifiers are logged.

Rollback (undoing a completed import):
    A successful import is itself transactional, so a *failed* run never
    needs rollback - PostgreSQL already discarded every row it wrote.
    To undo a *successful* import that you want to reverse:
      1. Preferred: restore the ``managearr_pg_data`` volume from a
         snapshot/backup taken before the import.
      2. Manual (irreversible - only if you are certain no writes have
         happened in PostgreSQL since the import completed): connect to
         the target database and run
             TRUNCATE arr_libraries, automation_policy, activity_jobs,
                 scan_candidates RESTART IDENTITY CASCADE;
         Schema migrations themselves never need to be rolled back - the
         ``schema_migrations`` table is untouched by this script and
         re-running migrations on next startup is always a safe no-op.
"""
import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import Config  # noqa: E402
from app.persistence.database import Database  # noqa: E402
from app.persistence.migrations import run_migrations  # noqa: E402

DATA_TABLES = ("arr_libraries", "automation_policy", "activity_jobs", "scan_candidates")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sqlite-path", required=True, help="Path to the legacy Managearr SQLite database (read-only)")
    parser.add_argument("--dry-run", action="store_true", help="Read and report counts only; write nothing")
    parser.add_argument(
        "--allow-nonempty",
        action="store_true",
        help="Permit importing into a PostgreSQL target that already has data (unsafe unless you know what you're doing)",
    )
    return parser.parse_args(argv)


def _open_sqlite_readonly(path: str) -> sqlite3.Connection:
    resolved = Path(path).resolve()
    if not resolved.is_file():
        raise SystemExit(f"SQLite database not found: {resolved}")
    conn = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _parse_ts(value) -> datetime:
    """Parse a stored ISO-ish timestamp string into an aware UTC datetime.
    Legacy rows may have been written without a timezone offset; those are
    assumed to already be UTC (the only timezone this app has ever used)."""
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def read_source(sqlite_conn: sqlite3.Connection) -> dict:
    """Read all rows from the legacy SQLite database. Read-only - issues
    no writes, even PRAGMA writes, against the source."""
    libraries = [dict(row) for row in sqlite_conn.execute("SELECT * FROM arr_libraries ORDER BY id")]

    policy_rows = [dict(row) for row in sqlite_conn.execute("SELECT * FROM automation_policy WHERE id = 1")]

    if _sqlite_table_exists(sqlite_conn, "activity_jobs"):
        job_columns = _sqlite_columns(sqlite_conn, "activity_jobs")
        jobs = [dict(row) for row in sqlite_conn.execute("SELECT * FROM activity_jobs ORDER BY id")]
        # Legacy pre-M1 preview databases predate job_type/candidate_count -
        # default them the same way the old ad-hoc ALTER TABLE step did.
        for job in jobs:
            if "job_type" not in job_columns:
                job["job_type"] = "legacy"
            if "candidate_count" not in job_columns:
                job["candidate_count"] = 0
    else:
        jobs = []

    if _sqlite_table_exists(sqlite_conn, "scan_candidates"):
        candidates = [dict(row) for row in sqlite_conn.execute("SELECT * FROM scan_candidates ORDER BY id")]
    else:
        candidates = []

    return {
        "arr_libraries": libraries,
        "automation_policy": policy_rows,
        "activity_jobs": jobs,
        "scan_candidates": candidates,
    }


def _print_counts(label: str, counts_by_table: dict) -> None:
    print(f"{label}:")
    for table in DATA_TABLES:
        print(f"  {table}: {counts_by_table[table]} row(s)")


def _target_is_empty(conn) -> bool:
    for table in DATA_TABLES:
        row = conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()  # noqa: S608 - table is a fixed literal
        if row is not None:
            return False
    return True


def _import_data(conn, source: dict) -> dict:
    counts = {table: 0 for table in DATA_TABLES}

    for lib in source["arr_libraries"]:
        conn.execute(
            """
            INSERT INTO arr_libraries (id, name, type, url, api_key, enabled, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                lib["id"],
                lib["name"],
                lib["type"],
                lib["url"],
                lib["api_key"],
                bool(lib["enabled"]),
                _parse_ts(lib["created_at"]),
                _parse_ts(lib["updated_at"]),
            ),
        )
        counts["arr_libraries"] += 1

    for policy in source["automation_policy"]:
        conn.execute(
            """
            INSERT INTO automation_policy (
                id, missing_enabled, upgrades_enabled, cycle_interval_minutes,
                hourly_api_cap, successful_grab_target, dispatch_interval_seconds,
                queue_target, cooldown_minutes, search_order, updated_at
            ) VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                bool(policy["missing_enabled"]),
                bool(policy["upgrades_enabled"]),
                policy["cycle_interval_minutes"],
                policy["hourly_api_cap"],
                policy["successful_grab_target"],
                policy["dispatch_interval_seconds"],
                policy["queue_target"],
                policy["cooldown_minutes"],
                policy["search_order"],
                _parse_ts(policy["updated_at"]),
            ),
        )
        counts["automation_policy"] += 1

    for job in source["activity_jobs"]:
        conn.execute(
            """
            INSERT INTO activity_jobs (
                id, library_id, library_name, job_type, state, title, details,
                candidate_count, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                job["id"],
                job["library_id"],
                job["library_name"],
                job["job_type"],
                job["state"],
                job["title"],
                job.get("details", ""),
                job["candidate_count"],
                _parse_ts(job["created_at"]),
                _parse_ts(job["updated_at"]),
            ),
        )
        counts["activity_jobs"] += 1

    for candidate in source["scan_candidates"]:
        conn.execute(
            """
            INSERT INTO scan_candidates (
                id, job_id, library_id, series_id, series_title, episode_id,
                season_number, episode_number, air_date, reason, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                candidate["id"],
                candidate["job_id"],
                candidate["library_id"],
                candidate["series_id"],
                candidate["series_title"],
                candidate["episode_id"],
                candidate["season_number"],
                candidate["episode_number"],
                candidate["air_date"],
                candidate["reason"],
                _parse_ts(candidate["created_at"]),
            ),
        )
        counts["scan_candidates"] += 1

    # Explicit id values were inserted above without touching PostgreSQL's
    # identity sequences - reset each one to continue after the highest
    # imported id so the next application-created row gets a fresh id.
    for table in ("arr_libraries", "activity_jobs", "scan_candidates"):
        conn.execute(
            f"SELECT setval(pg_get_serial_sequence(%s, 'id'), COALESCE((SELECT MAX(id) FROM {table}), 1), "  # noqa: S608
            f"(SELECT MAX(id) FROM {table}) IS NOT NULL)",
            (table,),
        )

    return counts


def import_sqlite(
    sqlite_path: str,
    db: Database,
    *,
    dry_run: bool = False,
    allow_nonempty: bool = False,
) -> int:
    """Core importer logic, decoupled from CLI arg parsing and from how
    ``db`` was configured - takes an already-constructed ``Database`` so
    tests can point it at a disposable PostgreSQL database directly.
    Returns a process-style exit code (0 success, 1 refused/failed)."""
    sqlite_conn = _open_sqlite_readonly(sqlite_path)
    try:
        source = read_source(sqlite_conn)
    finally:
        sqlite_conn.close()

    _print_counts("Read from SQLite", {table: len(source[table]) for table in DATA_TABLES})

    run_migrations(db)

    with db.connect() as conn:
        target_empty = _target_is_empty(conn)

    if not target_empty and not allow_nonempty:
        print(
            "Refusing to import: the PostgreSQL target already has data. "
            "Re-run with --allow-nonempty if this is intentional.",
            file=sys.stderr,
        )
        return 1

    if dry_run:
        print("Dry run: no rows were written to PostgreSQL.")
        print(f"Target empty: {target_empty}")
        return 0

    with db.connect() as conn:
        counts = _import_data(conn, source)

    _print_counts("Imported into PostgreSQL", counts)
    return 0


def run(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    db = Database(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        dbname=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        sslmode=Config.DB_SSLMODE,
        min_size=1,
        max_size=2,
        connect_timeout=Config.DB_CONNECT_TIMEOUT_SECONDS,
    )
    try:
        db.wait_ready(timeout_seconds=Config.DB_STARTUP_TIMEOUT_SECONDS)
        return import_sqlite(
            args.sqlite_path, db, dry_run=args.dry_run, allow_nonempty=args.allow_nonempty
        )
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(run())

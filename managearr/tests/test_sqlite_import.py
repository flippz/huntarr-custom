"""Tests for tools/import_sqlite.py against a real (disposable)
PostgreSQL target - see tests/conftest.py for the ``database`` fixture."""
import sqlite3
from datetime import datetime, timezone

import pytest

from tools.import_sqlite import import_sqlite

LEGACY_SCHEMA = """
CREATE TABLE arr_libraries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL, type TEXT NOT NULL, url TEXT NOT NULL, api_key TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE automation_policy (
    id INTEGER PRIMARY KEY CHECK (id = 1), missing_enabled INTEGER NOT NULL, upgrades_enabled INTEGER NOT NULL,
    cycle_interval_minutes INTEGER NOT NULL, hourly_api_cap INTEGER NOT NULL, successful_grab_target INTEGER NOT NULL,
    dispatch_interval_seconds INTEGER NOT NULL, queue_target INTEGER NOT NULL, cooldown_minutes INTEGER NOT NULL,
    search_order TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE activity_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, library_id INTEGER, library_name TEXT NOT NULL,
    job_type TEXT NOT NULL DEFAULT 'legacy', state TEXT NOT NULL, title TEXT NOT NULL, details TEXT NOT NULL DEFAULT '',
    candidate_count INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE scan_candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL, library_id INTEGER, series_id INTEGER NOT NULL,
    series_title TEXT NOT NULL, episode_id INTEGER NOT NULL, season_number INTEGER NOT NULL, episode_number INTEGER NOT NULL,
    air_date TEXT, reason TEXT NOT NULL, created_at TEXT NOT NULL
);
"""

SECRET_API_KEY = "totally-secret-legacy-api-key"


def _make_legacy_sqlite(path, *, with_job_type_columns=True):
    conn = sqlite3.connect(path)
    if with_job_type_columns:
        conn.executescript(LEGACY_SCHEMA)
    else:
        # Pre-M1 preview shape: activity_jobs without job_type/candidate_count,
        # no scan_candidates table at all.
        conn.executescript(
            """
            CREATE TABLE arr_libraries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL, type TEXT NOT NULL, url TEXT NOT NULL, api_key TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE automation_policy (
                id INTEGER PRIMARY KEY CHECK (id = 1), missing_enabled INTEGER NOT NULL, upgrades_enabled INTEGER NOT NULL,
                cycle_interval_minutes INTEGER NOT NULL, hourly_api_cap INTEGER NOT NULL, successful_grab_target INTEGER NOT NULL,
                dispatch_interval_seconds INTEGER NOT NULL, queue_target INTEGER NOT NULL, cooldown_minutes INTEGER NOT NULL,
                search_order TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE activity_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT, library_id INTEGER, library_name TEXT NOT NULL,
                state TEXT NOT NULL, title TEXT NOT NULL, details TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            """
        )

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO arr_libraries VALUES (5, 'Sonarr', 'sonarr', 'http://sonarr:8989', ?, 1, ?, ?)",
        (SECRET_API_KEY, now, now),
    )
    conn.execute(
        "INSERT INTO automation_policy VALUES (1, 1, 1, 60, 20, 5, 30, 10, 15, 'sequential', ?)",
        (now,),
    )
    if with_job_type_columns:
        conn.execute(
            "INSERT INTO activity_jobs VALUES (7, 5, 'Sonarr', 'sonarr_scan', 'completed', "
            "'Sonarr scan: Sonarr', 'Found 1 candidate', 1, ?, ?)",
            (now, now),
        )
        conn.execute(
            "INSERT INTO scan_candidates VALUES (9, 7, 5, 100, 'Show', 200, 1, 1, '2026-01-01', "
            "'monitored episode aired with no file on disk', ?)",
            (now,),
        )
    else:
        conn.execute(
            "INSERT INTO activity_jobs VALUES (7, 5, 'Sonarr', 'planned', 'Old job', '', ?, ?)",
            (now, now),
        )
    conn.commit()
    conn.close()


@pytest.fixture
def legacy_sqlite_path(tmp_path):
    path = tmp_path / "legacy_managearr.db"
    _make_legacy_sqlite(path)
    return str(path)


def test_dry_run_reports_counts_without_writing(database, legacy_sqlite_path, capsys):
    exit_code = import_sqlite(legacy_sqlite_path, database, dry_run=True)
    assert exit_code == 0

    with database.connect() as conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM arr_libraries").fetchone()["c"]
    assert count == 0

    out = capsys.readouterr().out
    assert "arr_libraries: 1 row(s)" in out
    assert "activity_jobs: 1 row(s)" in out
    assert "scan_candidates: 1 row(s)" in out


def test_dry_run_never_prints_api_key(database, legacy_sqlite_path, capsys):
    import_sqlite(legacy_sqlite_path, database, dry_run=True)
    out = capsys.readouterr().out
    assert SECRET_API_KEY not in out


def test_real_import_never_prints_api_key(database, legacy_sqlite_path, capsys):
    import_sqlite(legacy_sqlite_path, database)
    out = capsys.readouterr().out
    assert SECRET_API_KEY not in out


def test_real_import_preserves_ids_and_data(database, legacy_sqlite_path):
    exit_code = import_sqlite(legacy_sqlite_path, database)
    assert exit_code == 0

    with database.connect() as conn:
        lib = conn.execute("SELECT * FROM arr_libraries WHERE id = 5").fetchone()
        job = conn.execute("SELECT * FROM activity_jobs WHERE id = 7").fetchone()
        candidate = conn.execute("SELECT * FROM scan_candidates WHERE id = 9").fetchone()

    assert lib is not None
    assert lib["name"] == "Sonarr"
    assert lib["api_key"] == SECRET_API_KEY
    assert lib["enabled"] is True

    assert job is not None
    assert job["library_id"] == 5
    assert job["candidate_count"] == 1

    assert candidate is not None
    assert candidate["job_id"] == 7
    assert candidate["series_title"] == "Show"


def test_real_import_resets_sequences_for_next_insert(database, legacy_sqlite_path):
    import_sqlite(legacy_sqlite_path, database)

    from app.persistence.library_repository import LibraryRepository

    repo = LibraryRepository(database)
    new_lib = repo.create(
        {"name": "Radarr", "type": "radarr", "url": "http://radarr", "api_key": "k", "enabled": True}
    )
    assert new_lib.id == 6  # continues after the imported id=5, not colliding with it


def test_refuses_nonempty_target_without_allow_nonempty(database, legacy_sqlite_path):
    from app.persistence.library_repository import LibraryRepository

    LibraryRepository(database).create(
        {"name": "Existing", "type": "sonarr", "url": "http://x", "api_key": "k", "enabled": True}
    )

    exit_code = import_sqlite(legacy_sqlite_path, database)
    assert exit_code == 1

    with database.connect() as conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM arr_libraries").fetchone()["c"]
    assert count == 1  # nothing from the source was written


def test_allow_nonempty_permits_import_into_nonempty_target(database, legacy_sqlite_path):
    from app.persistence.library_repository import LibraryRepository

    LibraryRepository(database).create(
        {"name": "Existing", "type": "sonarr", "url": "http://x", "api_key": "k", "enabled": True}
    )

    exit_code = import_sqlite(legacy_sqlite_path, database, allow_nonempty=True)
    assert exit_code == 0

    with database.connect() as conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM arr_libraries").fetchone()["c"]
    assert count == 2


def test_repeated_import_without_allow_nonempty_does_not_duplicate_rows(database, legacy_sqlite_path):
    """Idempotence: a second run refuses rather than duplicating data."""
    assert import_sqlite(legacy_sqlite_path, database) == 0
    assert import_sqlite(legacy_sqlite_path, database) == 1  # refused, not duplicated

    with database.connect() as conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM arr_libraries").fetchone()["c"]
    assert count == 1


def test_forced_reimport_id_collision_rolls_back_transactionally(database, legacy_sqlite_path):
    """A forced re-import that collides on primary keys must roll back
    everything it attempted to write - never leave a partial import."""
    assert import_sqlite(legacy_sqlite_path, database) == 0

    with pytest.raises(Exception):
        import_sqlite(legacy_sqlite_path, database, allow_nonempty=True)

    with database.connect() as conn:
        lib_count = conn.execute("SELECT COUNT(*) AS c FROM arr_libraries").fetchone()["c"]
        job_count = conn.execute("SELECT COUNT(*) AS c FROM activity_jobs").fetchone()["c"]
        candidate_count = conn.execute("SELECT COUNT(*) AS c FROM scan_candidates").fetchone()["c"]
    # Exactly the first successful import's rows - the failed second
    # attempt left no partial rows behind.
    assert lib_count == 1
    assert job_count == 1
    assert candidate_count == 1


def test_import_from_legacy_pre_m1_schema_defaults_missing_columns(database, tmp_path):
    """A preview database predating job_type/candidate_count/scan_candidates
    must still import cleanly, defaulting the missing columns."""
    path = tmp_path / "very_old_managearr.db"
    _make_legacy_sqlite(path, with_job_type_columns=False)

    exit_code = import_sqlite(str(path), database)
    assert exit_code == 0

    with database.connect() as conn:
        job = conn.execute("SELECT * FROM activity_jobs WHERE id = 7").fetchone()
    assert job["job_type"] == "legacy"
    assert job["candidate_count"] == 0


def test_read_source_never_writes_to_sqlite_file(legacy_sqlite_path):
    """Read-only guarantee: the importer opens SQLite in read-only mode,
    so any write attempt against the source must fail immediately."""
    from tools.import_sqlite import _open_sqlite_readonly

    conn = _open_sqlite_readonly(legacy_sqlite_path)
    try:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("DELETE FROM arr_libraries")
    finally:
        conn.close()

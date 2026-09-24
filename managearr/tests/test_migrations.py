"""Tests for the deterministic, transactional schema migration runner."""
from app.persistence.migrations import MIGRATIONS, current_version, run_migrations


def test_run_migrations_is_idempotent(database):
    version_before = current_version(database)
    run_migrations(database)
    run_migrations(database)
    assert current_version(database) == version_before == len(MIGRATIONS)


def test_schema_migrations_table_records_applied_versions(database):
    with database.connect() as conn:
        rows = conn.execute(
            "SELECT version, name FROM schema_migrations ORDER BY version"
        ).fetchall()
    assert [row["version"] for row in rows] == [m.version for m in MIGRATIONS]
    assert [row["name"] for row in rows] == [m.name for m in MIGRATIONS]


def test_current_version_matches_latest_migration(database):
    assert current_version(database) == MIGRATIONS[-1].version


def test_expected_tables_and_columns_exist(database):
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN ('arr_libraries', 'automation_policy', 'activity_jobs', 'scan_candidates')
            """
        ).fetchall()
    columns_by_table: dict[str, set[str]] = {}
    for row in rows:
        columns_by_table.setdefault(row["table_name"], set()).add(row["column_name"])

    assert columns_by_table["arr_libraries"] == {
        "id", "name", "type", "url", "api_key", "enabled", "created_at", "updated_at",
    }
    assert columns_by_table["automation_policy"] == {
        "id", "missing_enabled", "upgrades_enabled", "cycle_interval_minutes",
        "hourly_api_cap", "successful_grab_target", "dispatch_interval_seconds",
        "queue_target", "cooldown_minutes", "search_order", "updated_at",
    }
    assert columns_by_table["activity_jobs"] == {
        "id", "library_id", "library_name", "job_type", "state", "title", "details",
        "candidate_count", "created_at", "updated_at",
    }
    assert columns_by_table["scan_candidates"] == {
        "id", "job_id", "library_id", "series_id", "series_title", "episode_id",
        "season_number", "episode_number", "air_date", "reason", "created_at",
    }


def test_foreign_keys_have_expected_delete_behavior(database):
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS references_table,
                rc.delete_rule
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
            JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            """
        ).fetchall()

    by_column = {(row["table_name"], row["column_name"]): row for row in rows}

    assert by_column[("activity_jobs", "library_id")]["references_table"] == "arr_libraries"
    assert by_column[("activity_jobs", "library_id")]["delete_rule"] == "SET NULL"

    assert by_column[("scan_candidates", "job_id")]["references_table"] == "activity_jobs"
    assert by_column[("scan_candidates", "job_id")]["delete_rule"] == "CASCADE"

    assert by_column[("scan_candidates", "library_id")]["references_table"] == "arr_libraries"
    assert by_column[("scan_candidates", "library_id")]["delete_rule"] == "SET NULL"


def test_expected_indexes_exist(database):
    with database.connect() as conn:
        rows = conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
        ).fetchall()
    index_names = {row["indexname"] for row in rows}
    assert "idx_arr_libraries_name_lower" in index_names
    assert "idx_activity_jobs_state" in index_names
    assert "idx_activity_jobs_library_id" in index_names
    assert "idx_scan_candidates_job_id" in index_names
    assert "idx_scan_candidates_library_id" in index_names

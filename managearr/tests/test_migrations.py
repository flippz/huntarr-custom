"""Tests for the deterministic, transactional schema migration runner."""
import psycopg
import pytest

from app.persistence import migrations as migration_module
from app.persistence.migrations import MIGRATIONS, current_version, run_migrations
from app.persistence.migrations import Migration


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
    assert "idx_dispatch_batches_scan_job_id" in index_names
    assert "idx_dispatch_batches_library_id" in index_names
    assert "idx_dispatch_batches_updated_at" in index_names
    assert "idx_dispatch_batches_mode_state" in index_names
    assert "idx_dispatch_batches_library_mode_state" in index_names
    assert "idx_dispatch_batch_items_batch_id" in index_names
    assert "idx_dispatch_batch_items_candidate_id" in index_names
    assert "idx_dispatch_batch_items_episode_id" in index_names
    assert "idx_dispatch_batch_items_state" in index_names
    assert "idx_dispatch_batch_items_episode_state_updated" in index_names


# --- dispatch ledger (v2) -----------------------------------------------

def test_dispatch_ledger_tables_and_columns_exist(database):
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN ('dispatch_batches', 'dispatch_batch_items')
            """
        ).fetchall()
    columns_by_table: dict[str, set[str]] = {}
    for row in rows:
        columns_by_table.setdefault(row["table_name"], set()).add(row["column_name"])

    assert columns_by_table["dispatch_batches"] == {
        "id", "scan_job_id", "library_id", "library_name", "mode", "state",
        "requested_count", "selected_count", "dispatched_count",
        "sonarr_command_id", "sonarr_command_status", "error_summary",
        "reconciliation_state", "reconciliation_summary", "last_reconciled_at",
        "command_observed_state",
        "created_at", "updated_at",
    }
    assert columns_by_table["dispatch_batch_items"] == {
        "id", "batch_id", "candidate_id", "episode_id", "series_id", "series_title",
        "season_number", "episode_number", "state", "reason", "created_at", "updated_at",
    }


def test_outcome_audit_tables_and_indexes_exist(database):
    with database.connect() as conn:
        column_rows = conn.execute(
            """
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN ('dispatch_reconciliation_attempts', 'dispatch_outcome_events')
            """
        ).fetchall()
        indexes = {
            row["indexname"]
            for row in conn.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
            ).fetchall()
        }
    columns = {}
    for row in column_rows:
        columns.setdefault(row["table_name"], set()).add(row["column_name"])
    assert columns["dispatch_reconciliation_attempts"] == {
        "id", "batch_id", "observed_at", "state", "safe_summary",
        "command_endpoint_read", "history_endpoint_read", "queue_endpoint_read",
        "inserted_event_count",
    }
    assert columns["dispatch_outcome_events"] == {
        "id", "batch_id", "dispatch_item_id", "candidate_id", "episode_id",
        "observed_at", "source_endpoint", "event_type", "event_state",
        "safe_summary", "sonarr_command_id", "sonarr_event_id", "download_id",
        "evidence_key",
    }
    assert "idx_dispatch_reconciliation_attempts_batch_observed" in indexes
    assert "idx_dispatch_outcome_events_batch_observed" in indexes
    assert "idx_dispatch_outcome_events_item_observed" in indexes
    assert "idx_dispatch_outcome_events_episode" in indexes


def test_dispatch_ledger_foreign_keys_have_expected_delete_behavior(database):
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
              AND tc.table_name IN ('dispatch_batches', 'dispatch_batch_items')
            """
        ).fetchall()
    by_column = {(row["table_name"], row["column_name"]): row for row in rows}

    assert by_column[("dispatch_batches", "scan_job_id")]["references_table"] == "activity_jobs"
    assert by_column[("dispatch_batches", "scan_job_id")]["delete_rule"] == "RESTRICT"

    assert by_column[("dispatch_batches", "library_id")]["references_table"] == "arr_libraries"
    assert by_column[("dispatch_batches", "library_id")]["delete_rule"] == "SET NULL"

    assert by_column[("dispatch_batch_items", "batch_id")]["references_table"] == "dispatch_batches"
    assert by_column[("dispatch_batch_items", "batch_id")]["delete_rule"] == "CASCADE"

    assert by_column[("dispatch_batch_items", "candidate_id")]["references_table"] == "scan_candidates"
    assert by_column[("dispatch_batch_items", "candidate_id")]["delete_rule"] == "RESTRICT"


def test_dispatch_ledger_check_constraints_reject_invalid_values(database, library_repo, activity_repo):
    lib = library_repo.create(
        {"name": "Sonarr", "type": "sonarr", "url": "http://sonarr:8989", "api_key": "k", "enabled": True}
    )
    job = activity_repo.create(
        {
            "library_id": lib.id,
            "library_name": lib.name,
            "job_type": "sonarr_scan",
            "state": "completed",
            "title": "scan",
        }
    )
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute(
                """
                INSERT INTO dispatch_batches (
                    scan_job_id, library_id, library_name, mode, state,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, 'not-a-real-mode', 'planned', now(), now())
                """,
                (job.id, lib.id, lib.name),
            )


def test_dispatch_ledger_count_constraints_reject_impossible_audit(database, library_repo, activity_repo):
    lib = library_repo.create(
        {"name": "Sonarr", "type": "sonarr", "url": "http://sonarr:8989", "api_key": "k", "enabled": True}
    )
    job = activity_repo.create(
        {
            "library_id": lib.id,
            "library_name": lib.name,
            "job_type": "sonarr_scan",
            "state": "completed",
            "title": "scan",
        }
    )
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute(
                """
                INSERT INTO dispatch_batches (
                    scan_job_id, library_id, library_name, mode, state,
                    requested_count, selected_count, dispatched_count,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, 'manual', 'completed', 1, 1, 2, now(), now())
                """,
                (job.id, lib.id, lib.name),
            )


def test_dispatch_audit_identity_and_rows_are_immutable(
    database, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    lib = library_repo.create(
        {"name": "Sonarr", "type": "sonarr", "url": "http://sonarr:8989", "api_key": "k", "enabled": True}
    )
    job = activity_repo.create(
        {
            "library_id": lib.id,
            "library_name": lib.name,
            "job_type": "sonarr_scan",
            "state": "completed",
            "title": "scan",
        }
    )
    candidate_repo.create_many(
        job.id,
        lib.id,
        [{
            "series_id": 1, "series_title": "Show", "episode_id": 1,
            "season_number": 1, "episode_number": 1, "reason": "missing",
        }],
    )
    candidate = candidate_repo.list_for_job(job.id)[0]
    with database.connect() as conn:
        batch_id = dispatch_repo.create_batch(
            conn,
            {
                "scan_job_id": job.id, "library_id": lib.id, "library_name": lib.name,
                "mode": "dry_run", "state": "planned", "requested_count": 1,
                "selected_count": 1,
            },
        )
        dispatch_repo.create_items(
            conn,
            batch_id,
            [{
                "candidate_id": candidate.id, "episode_id": candidate.episode_id,
                "series_id": candidate.series_id, "series_title": candidate.series_title,
                "season_number": 1, "episode_number": 1, "state": "planned",
            }],
        )

    with pytest.raises(psycopg.errors.RaiseException):
        with database.connect() as conn:
            conn.execute("UPDATE dispatch_batches SET library_name = 'changed' WHERE id = %s", (batch_id,))
    with pytest.raises(psycopg.errors.RaiseException):
        with database.connect() as conn:
            conn.execute("DELETE FROM dispatch_batches WHERE id = %s", (batch_id,))

    assert dispatch_repo.get_batch(batch_id).library_name == lib.name
    assert library_repo.delete(lib.id) is True
    surviving_audit = dispatch_repo.get_batch(batch_id)
    assert surviving_audit.library_id is None
    assert surviving_audit.library_name == lib.name


# --- read-only refresh automation (v5) -----------------------------------

def test_refresh_tables_and_columns_exist(database):
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN (
                  'refresh_settings', 'refresh_requests', 'refresh_runs',
                  'refresh_run_reconciled_batches'
              )
            """
        ).fetchall()
    columns_by_table: dict[str, set[str]] = {}
    for row in rows:
        columns_by_table.setdefault(row["table_name"], set()).add(row["column_name"])

    assert columns_by_table["refresh_settings"] == {
        "id", "scan_max_age_minutes", "reconcile_min_interval_minutes",
        "reconcile_max_per_cycle", "next_reconcile_due_at", "updated_at",
    }
    assert columns_by_table["refresh_requests"] == {
        "id", "kind", "library_id", "library_name", "state", "requested_at",
        "claimed_at", "finished_at", "claim_owner", "safe_summary",
    }
    assert columns_by_table["refresh_runs"] == {
        "id", "request_id", "kind", "trigger", "state", "library_id", "library_name",
        "scan_job_id", "worker_owner", "attempt", "queued_at", "started_at",
        "finished_at", "target_count", "succeeded_count", "failed_count",
        "skipped_count", "safe_summary",
    }
    assert columns_by_table["refresh_run_reconciled_batches"] == {
        "id", "refresh_run_id", "dispatch_batch_id", "result", "reason", "created_at",
    }


def test_scheduler_library_results_gained_snapshot_columns(database):
    with database.connect() as conn:
        columns = {
            row["column_name"]
            for row in conn.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema='public' AND table_name='scheduler_library_results'"""
            ).fetchall()
        }
    assert {"snapshot_taken_at", "snapshot_age_seconds"} <= columns


def test_refresh_settings_seeded_with_conservative_defaults(database):
    with database.connect() as conn:
        row = conn.execute("SELECT * FROM refresh_settings WHERE id = 1").fetchone()
    assert row["scan_max_age_minutes"] == 60
    assert row["reconcile_min_interval_minutes"] == 15
    assert row["reconcile_max_per_cycle"] == 10
    assert row["next_reconcile_due_at"] is None


def test_refresh_settings_rejects_out_of_range_values(database):
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute("UPDATE refresh_settings SET scan_max_age_minutes = 0 WHERE id = 1")


def test_refresh_runs_kind_library_shape_is_enforced(database):
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute(
                """
                INSERT INTO refresh_runs (kind, trigger, state)
                VALUES ('scan', 'scheduled', 'queued')
                """
            )
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute(
                """
                INSERT INTO refresh_runs (kind, trigger, state, library_id, library_name)
                VALUES ('reconcile', 'scheduled', 'queued', 1, 'x')
                """
            )


def test_refresh_run_reconciled_batches_are_append_only(
    database, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    lib = library_repo.create(
        {"name": "Sonarr", "type": "sonarr", "url": "http://sonarr:8989", "api_key": "k", "enabled": True}
    )
    job = activity_repo.create({
        "library_id": lib.id, "library_name": lib.name, "job_type": "sonarr_scan",
        "state": "completed", "title": "scan",
    })
    with database.connect() as conn:
        batch_id = dispatch_repo.create_batch(conn, {
            "scan_job_id": job.id, "library_id": lib.id, "library_name": lib.name,
            "mode": "manual", "state": "completed", "requested_count": 1,
            "selected_count": 1, "dispatched_count": 1, "sonarr_command_id": 5,
        })
        run_id = conn.execute(
            "INSERT INTO refresh_runs (kind, trigger, state) VALUES ('reconcile','scheduled','queued') RETURNING id"
        ).fetchone()["id"]
        conn.execute(
            """
            INSERT INTO refresh_run_reconciled_batches (refresh_run_id, dispatch_batch_id, result)
            VALUES (%s, %s, 'resolved')
            """,
            (run_id, batch_id),
        )
    with pytest.raises(psycopg.errors.RaiseException):
        with database.connect() as conn:
            conn.execute(
                "UPDATE refresh_run_reconciled_batches SET result = 'error' WHERE refresh_run_id = %s",
                (run_id,),
            )


def test_refresh_run_state_transitions_are_controlled(database):
    with database.connect() as conn:
        run_id = conn.execute(
            "INSERT INTO refresh_runs (kind, trigger, state) VALUES ('reconcile','scheduled','queued') RETURNING id"
        ).fetchone()["id"]
    with pytest.raises(psycopg.errors.RaiseException):
        with database.connect() as conn:
            conn.execute(
                "UPDATE refresh_runs SET state='completed', started_at=now(), finished_at=now(), "
                "worker_owner='w' WHERE id=%s",
                (run_id,),
            )


# --- v6: controlled live dispatch ----------------------------------------


def test_v6_widens_mode_constraints_without_touching_existing_rows(database):
    assert MIGRATIONS[5].version == 6
    with database.connect() as conn:
        settings = conn.execute("SELECT mode FROM scheduler_settings WHERE id = 1").fetchone()
    assert settings["mode"] == "off"  # unchanged by the migration itself
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute("UPDATE scheduler_settings SET mode = 'not-a-real-mode' WHERE id = 1")


def test_v6_live_tables_and_indexes_exist(database):
    with database.connect() as conn:
        tables = {r["table_name"] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        ).fetchall()}
        indexes = {r["indexname"] for r in conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname='public'"
        ).fetchall()}
    assert {"live_control", "live_challenges", "live_control_audit", "live_dispatch_ledger"} <= tables
    assert {
        "idx_live_challenges_kind_state_expires", "idx_live_control_audit_occurred",
        "idx_live_dispatch_ledger_cycle", "idx_live_dispatch_ledger_batch", "idx_live_dispatch_ledger_library",
    } <= indexes


def test_v6_live_control_bounds_are_enforced(database):
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute("UPDATE live_control SET max_dispatches_per_cycle = 6 WHERE id = 1")
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute("UPDATE live_control SET max_arm_ttl_minutes = 61 WHERE id = 1")
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute("UPDATE live_control SET min_delay_seconds_between_dispatches = 1 WHERE id = 1")


def test_v6_live_challenge_kind_shape_is_enforced(database):
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute(
                "INSERT INTO live_challenges (kind, token_hash, policy_digest, expires_at) "
                "VALUES ('arm', 'h', 'd', now() + interval '1 minute')"
            )  # arm challenges require requested_reason/requested_ttl_minutes


def test_failed_pending_migration_rolls_back_ddl_and_bookkeeping(database, monkeypatch):
    bad = Migration(
        version=999,
        name="rollback_probe",
        sql="CREATE TABLE migration_rollback_probe (id INTEGER); SELECT missing_migration_function();",
    )
    monkeypatch.setattr(migration_module, "MIGRATIONS", [*MIGRATIONS, bad])

    with pytest.raises(psycopg.errors.UndefinedFunction):
        run_migrations(database)

    assert current_version(database) == MIGRATIONS[-1].version
    with database.connect() as conn:
        exists = conn.execute("SELECT to_regclass('public.migration_rollback_probe') AS name").fetchone()["name"]
    assert exists is None

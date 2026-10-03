"""Tests for the deterministic, transactional schema migration runner."""
import psycopg
import pytest
from uuid import uuid4

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
        "season_number", "episode_number", "candidate_kind", "air_date", "reason", "created_at",
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
    assert "idx_scan_candidates_job_kind" in index_names
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


def test_m10_candidate_kind_defaults_backfill_and_constraints(
    database, library_repo, activity_repo, candidate_repo
):
    library = library_repo.create({
        "name": "M10", "type": "sonarr", "url": "http://sonarr", "api_key": "k", "enabled": True,
    })
    job = activity_repo.create({
        "library_id": library.id, "library_name": library.name, "job_type": "sonarr_scan",
        "state": "completed", "title": "scan",
    })
    candidate_repo.create_many(job.id, library.id, [{
        "series_id": 1, "series_title": "Show", "episode_id": 1,
        "season_number": 1, "episode_number": 1, "reason": "legacy missing",
    }])
    assert candidate_repo.list_for_job(job.id)[0].candidate_kind == "missing"
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute("UPDATE scan_candidates SET candidate_kind = 'unknown' WHERE job_id = %s", (job.id,))


def test_m10_scheduler_kind_snapshot_and_upgrade_state_constraints(database):
    with database.connect() as conn:
        candidate_columns = {
            row["column_name"] for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='scheduler_candidate_results'"
            ).fetchall()
        }
        indexes = {
            row["indexname"] for row in conn.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname='public'"
            ).fetchall()
        }
    assert "candidate_kind" in candidate_columns
    assert "idx_scheduler_candidate_results_kind" in indexes


def test_m10_real_postgresql_upgrade_backfills_existing_candidates(database):
    schema = "m10_probe_" + uuid4().hex
    quoted = psycopg.sql.Identifier(schema)
    with database.connect() as conn:
        conn.execute(psycopg.sql.SQL("CREATE SCHEMA {}").format(quoted))
        try:
            conn.execute(psycopg.sql.SQL("SET search_path TO {}").format(quoted))
            for migration in MIGRATIONS[:9]:
                conn.execute(migration.sql)
            library_id = conn.execute(
                "INSERT INTO arr_libraries (name,type,url,api_key,enabled,created_at,updated_at) "
                "VALUES ('old','sonarr','http://sonarr','k',TRUE,now(),now()) RETURNING id"
            ).fetchone()["id"]
            job_id = conn.execute(
                "INSERT INTO activity_jobs (library_id,library_name,job_type,state,title,created_at,updated_at) "
                "VALUES (%s,'old','sonarr_scan','completed','old scan',now(),now()) RETURNING id",
                (library_id,),
            ).fetchone()["id"]
            candidate_id = conn.execute(
                "INSERT INTO scan_candidates (job_id,library_id,series_id,series_title,episode_id,"
                "season_number,episode_number,reason,created_at) "
                "VALUES (%s,%s,1,'Show',1,1,1,'missing',now()) RETURNING id",
                (job_id, library_id),
            ).fetchone()["id"]
            cycle_id = conn.execute(
                "INSERT INTO scheduler_cycle_runs (trigger,state,mode_snapshot,policy_snapshot,"
                "random_seed,worker_owner,started_at,finished_at,library_count,"
                "completed_library_count,considered_count,selected_count,excluded_count) "
                "VALUES ('scheduled','completed','simulate','{}',1,'probe',now(),now(),1,1,1,1,0) "
                "RETURNING id"
            ).fetchone()["id"]
            library_result_id = conn.execute(
                "INSERT INTO scheduler_library_results (cycle_run_id,library_id,library_name,scan_job_id,"
                "state,considered_count,selected_count,excluded_count,effective_cap,queue_occupancy,"
                "recent_success_count,upgrades_state,safe_summary) "
                "VALUES (%s,%s,'old',%s,'completed',1,1,0,1,0,0,'disabled','legacy') RETURNING id",
                (cycle_id, library_id, job_id),
            ).fetchone()["id"]
            scheduler_candidate_id = conn.execute(
                "INSERT INTO scheduler_candidate_results (cycle_run_id,library_result_id,candidate_id,"
                "episode_id,series_id,series_title,season_number,episode_number,candidate_reason,"
                "selected,order_position) VALUES (%s,%s,%s,1,1,'Show',1,1,'missing',TRUE,1) RETURNING id",
                (cycle_id, library_result_id, candidate_id),
            ).fetchone()["id"]
            # The v4 append-only trigger is active here. M10 must backfill by
            # adding a constant-default column, never by updating audit rows.
            conn.execute(MIGRATIONS[9].sql)
            row = conn.execute(
                "SELECT candidate_kind FROM scan_candidates WHERE id=%s", (candidate_id,)
            ).fetchone()
            assert row["candidate_kind"] == "missing"
            scheduler_row = conn.execute(
                "SELECT candidate_kind FROM scheduler_candidate_results WHERE id=%s",
                (scheduler_candidate_id,),
            ).fetchone()
            assert scheduler_row["candidate_kind"] == "missing"
            with pytest.raises(psycopg.errors.CheckViolation):
                with conn.transaction():
                    conn.execute(
                        "INSERT INTO scan_candidates (job_id,library_id,series_id,series_title,episode_id,"
                        "season_number,episode_number,candidate_kind,reason,created_at) "
                        "VALUES (%s,%s,1,'Show',2,1,2,'unknown','bad',now())",
                        (job_id, library_id),
                    )
        finally:
            conn.execute("SET search_path TO public")
            conn.execute(psycopg.sql.SQL("DROP SCHEMA {} CASCADE").format(quoted))


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


def test_v8_raises_legacy_singleton_ceiling_without_changing_authorization(database):
    assert [item.version for item in MIGRATIONS[-8:]] == [10, 11, 12, 13, 14, 15, 16, 17]
    migration = next(item for item in MIGRATIONS if item.version == 8)
    with database.connect() as conn:
        conn.execute(
            "UPDATE live_control SET max_dispatches_per_cycle=1, "
            "authorization_state='running', authorization_generation=41, "
            "authorized_at=now(), authorized_by='operator', "
            "authorization_reason='keep running' WHERE id=1"
        )
        conn.execute(migration.sql)
        row = conn.execute(
            "SELECT max_dispatches_per_cycle, authorization_state, "
            "authorization_generation, authorization_reason FROM live_control WHERE id=1"
        ).fetchone()
    assert row["max_dispatches_per_cycle"] == 5
    assert row["authorization_state"] == "running"
    assert row["authorization_generation"] == 41
    assert row["authorization_reason"] == "keep running"


def test_season_pack_attempt_markers_are_append_only(database):
    with database.connect() as conn:
        tables = {r["table_name"] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        ).fetchall()}
    assert "season_pack_attempt_started" in tables


def test_v14_slow_download_tables_exist_with_safe_defaults(database, library_repo):
    with database.connect() as conn:
        tables = {r["table_name"] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        ).fetchall()}
    for table in (
        "slow_download_settings", "slow_download_queue_items", "slow_download_observations",
        "slow_download_actions", "slow_download_removal_attempts",
    ):
        assert table in tables

    library = library_repo.create({"name": "Sonarr", "type": "sonarr", "url": "http://sonarr", "api_key": "k", "enabled": True})
    with database.connect() as conn:
        conn.execute(
            "INSERT INTO slow_download_settings (library_id) VALUES (%s)", (library.id,)
        )
        row = conn.execute("SELECT * FROM slow_download_settings WHERE library_id = %s", (library.id,)).fetchone()
    # Monitoring enabled, automatic removal disabled - the core migration
    # safety requirement: a fresh deploy/migration never starts deleting.
    assert row["monitoring_enabled"] is True
    assert row["auto_removal_enabled"] is False
    assert row["remove_from_client"] is True and row["blocklist"] is True and row["skip_redownload"] is False


def test_v14_observations_actions_and_removal_markers_are_append_only(database, library_repo):
    library = library_repo.create({"name": "Sonarr", "type": "sonarr", "url": "http://sonarr", "api_key": "k", "enabled": True})
    with database.connect() as conn:
        item_id = conn.execute(
            "INSERT INTO slow_download_queue_items (library_id, sonarr_queue_id) VALUES (%s, 1) RETURNING id",
            (library.id,),
        ).fetchone()["id"]
        conn.execute(
            "INSERT INTO slow_download_observations (queue_item_id, status, classification) VALUES (%s, 'downloading', 'healthy')",
            (item_id,),
        )
        conn.execute(
            "INSERT INTO slow_download_actions (queue_item_id, library_id, sonarr_queue_id, action, reason) "
            "VALUES (%s, %s, 1, 'strike', 'test')",
            (item_id, library.id),
        )
        conn.execute(
            "INSERT INTO slow_download_removal_attempts (queue_item_id) VALUES (%s)", (item_id,)
        )

    with pytest.raises(Exception, match="append-only"):
        with database.connect() as conn:
            conn.execute("UPDATE slow_download_observations SET status = 'paused' WHERE queue_item_id = %s", (item_id,))
    with pytest.raises(Exception, match="append-only"):
        with database.connect() as conn:
            conn.execute("DELETE FROM slow_download_actions WHERE queue_item_id = %s", (item_id,))
    with pytest.raises(Exception, match="append-only"):
        with database.connect() as conn:
            conn.execute("UPDATE slow_download_removal_attempts SET started_at = now() WHERE queue_item_id = %s", (item_id,))
    with pytest.raises(Exception, match="append-only"):
        with database.connect() as conn:
            conn.execute("DELETE FROM slow_download_removal_attempts WHERE queue_item_id = %s", (item_id,))


def test_v14_library_deletion_succeeds_with_populated_slow_download_audit_and_retains_it(database, library_repo):
    """The v14 schema originally made slow_download_queue_items RESTRICT
    its append-only children, which made arr_libraries' cascading delete of
    those queue items fail with a foreign-key violation the instant any
    observation/action/removal-attempt had ever been recorded - i.e. on
    basically every real library delete. Deletion must keep working, and
    the append-only audit rows must be retained (nulled identity, not
    deleted) rather than silently dropped."""
    library = library_repo.create({"name": "Sonarr", "type": "sonarr", "url": "http://sonarr", "api_key": "k", "enabled": True})
    with database.connect() as conn:
        item_id = conn.execute(
            "INSERT INTO slow_download_queue_items (library_id, sonarr_queue_id) VALUES (%s, 1) RETURNING id",
            (library.id,),
        ).fetchone()["id"]
        conn.execute(
            "INSERT INTO slow_download_observations (queue_item_id, status, classification) VALUES (%s, 'downloading', 'healthy')",
            (item_id,),
        )
        conn.execute(
            "INSERT INTO slow_download_actions (queue_item_id, library_id, sonarr_queue_id, action, reason) "
            "VALUES (%s, %s, 1, 'strike', 'test')",
            (item_id, library.id),
        )
        conn.execute("INSERT INTO slow_download_removal_attempts (queue_item_id) VALUES (%s)", (item_id,))

    assert library_repo.delete(library.id) is True
    assert library_repo.get(library.id) is None

    with database.connect() as conn:
        # The mutable queue-item row cascades away with its library...
        assert conn.execute(
            "SELECT 1 FROM slow_download_queue_items WHERE id = %s", (item_id,)
        ).fetchone() is None
        # ...but every append-only audit row survives, with its now-dangling
        # identity column nulled instead of the row being deleted.
        observation = conn.execute(
            "SELECT queue_item_id, status, classification FROM slow_download_observations"
        ).fetchone()
        assert observation is not None
        assert observation["queue_item_id"] is None
        assert observation["status"] == "downloading"
        action = conn.execute(
            "SELECT queue_item_id, library_id, action, reason FROM slow_download_actions"
        ).fetchone()
        assert action is not None
        assert action["queue_item_id"] is None
        assert action["library_id"] is None
        assert action["reason"] == "test"
        marker = conn.execute("SELECT queue_item_id FROM slow_download_removal_attempts").fetchone()
        assert marker is not None
        assert marker["queue_item_id"] is None

    # The append-only trigger's cascade carve-out must not have opened the
    # door to arbitrary mutation: these rows are still fully protected.
    with pytest.raises(Exception, match="append-only"):
        with database.connect() as conn:
            conn.execute("UPDATE slow_download_observations SET status = 'paused'")
    with pytest.raises(Exception, match="append-only"):
        with database.connect() as conn:
            conn.execute("DELETE FROM slow_download_actions")


def test_v15_import_failure_tables_exist_with_safe_defaults(database, library_repo):
    with database.connect() as conn:
        tables = {r["table_name"] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        ).fetchall()}
    for table in (
        "import_failure_reason_catalog", "import_failure_policies", "import_failure_policy_reasons",
        "import_failure_policy_audit", "import_failure_queue_items", "import_failure_actions",
        "import_failure_removal_attempts",
    ):
        assert table in tables

    library = library_repo.create({"name": "Sonarr", "type": "sonarr", "url": "http://sonarr", "api_key": "k", "enabled": True})
    with database.connect() as conn:
        conn.execute("INSERT INTO import_failure_policies (library_id) VALUES (%s)", (library.id,))
        row = conn.execute("SELECT * FROM import_failure_policies WHERE library_id = %s", (library.id,)).fetchone()
    # Monitoring enabled, automatic removal disabled, no reasons selected -
    # the core migration safety requirement: a fresh deploy/migration never
    # starts deleting and every reason starts out "leave".
    assert row["monitoring_enabled"] is True
    assert row["auto_removal_enabled"] is False
    assert row["remove_from_client"] is True and row["blocklist"] is True and row["skip_redownload"] is False
    with database.connect() as conn:
        reasons = conn.execute(
            "SELECT reason_key FROM import_failure_policy_reasons WHERE library_id = %s", (library.id,)
        ).fetchall()
    assert reasons == []


def test_v15_reason_catalog_is_seeded_with_every_selectable_key_and_excludes_unknown(database):
    from app.domain.import_failure import REMOVAL_SELECTABLE_REASON_KEYS

    with database.connect() as conn:
        keys = {r["reason_key"] for r in conn.execute("SELECT reason_key FROM import_failure_reason_catalog").fetchall()}
    assert keys == REMOVAL_SELECTABLE_REASON_KEYS
    assert "Unknown" not in keys


def test_v15_policy_reasons_are_fk_validated_against_the_catalog(database, library_repo):
    library = library_repo.create({"name": "Sonarr", "type": "sonarr", "url": "http://sonarr", "api_key": "k", "enabled": True})
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with database.connect() as conn:
            conn.execute(
                "INSERT INTO import_failure_policy_reasons (library_id, reason_key) VALUES (%s, 'TotallyMadeUp')",
                (library.id,),
            )
    # "Unknown" is a real canonical catalog key but deliberately absent from
    # this lookup table - it can never be selected, enforced at the schema
    # level independent of any application-layer bug.
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with database.connect() as conn:
            conn.execute(
                "INSERT INTO import_failure_policy_reasons (library_id, reason_key) VALUES (%s, 'Unknown')",
                (library.id,),
            )
    with database.connect() as conn:
        conn.execute(
            "INSERT INTO import_failure_policy_reasons (library_id, reason_key) VALUES (%s, 'Sample')",
            (library.id,),
        )


def test_v15_auto_removal_requires_remove_from_client_or_blocklist(database, library_repo):
    library = library_repo.create({"name": "Sonarr", "type": "sonarr", "url": "http://sonarr", "api_key": "k", "enabled": True})
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute(
                "INSERT INTO import_failure_policies (library_id, auto_removal_enabled, remove_from_client, blocklist) "
                "VALUES (%s, TRUE, FALSE, FALSE)",
                (library.id,),
            )


def test_v15_policy_audit_actions_and_removal_markers_are_append_only(database, library_repo):
    library = library_repo.create({"name": "Sonarr", "type": "sonarr", "url": "http://sonarr", "api_key": "k", "enabled": True})
    with database.connect() as conn:
        conn.execute(
            "INSERT INTO import_failure_policy_audit (library_id, revision_before, revision_after, "
            "auto_removal_enabled_before, auto_removal_enabled_after) VALUES (%s, 0, 1, FALSE, TRUE)",
            (library.id,),
        )
        item_id = conn.execute(
            "INSERT INTO import_failure_queue_items (library_id, sonarr_queue_id) VALUES (%s, 1) RETURNING id",
            (library.id,),
        ).fetchone()["id"]
        conn.execute(
            "INSERT INTO import_failure_actions (queue_item_id, library_id, sonarr_queue_id, action, reason) "
            "VALUES (%s, %s, 1, 'observed', 'test')",
            (item_id, library.id),
        )
        conn.execute("INSERT INTO import_failure_removal_attempts (queue_item_id) VALUES (%s)", (item_id,))

    with pytest.raises(Exception, match="append-only"):
        with database.connect() as conn:
            conn.execute("UPDATE import_failure_policy_audit SET reason = 'tampered'")
    with pytest.raises(Exception, match="append-only"):
        with database.connect() as conn:
            conn.execute("DELETE FROM import_failure_actions WHERE queue_item_id = %s", (item_id,))
    with pytest.raises(Exception, match="append-only"):
        with database.connect() as conn:
            conn.execute("UPDATE import_failure_removal_attempts SET started_at = now() WHERE queue_item_id = %s", (item_id,))
    with pytest.raises(Exception, match="append-only"):
        with database.connect() as conn:
            conn.execute("DELETE FROM import_failure_removal_attempts WHERE queue_item_id = %s", (item_id,))


def test_v15_library_deletion_retains_audit_with_nulled_identity(database, library_repo):
    library = library_repo.create({"name": "Sonarr", "type": "sonarr", "url": "http://sonarr", "api_key": "k", "enabled": True})
    with database.connect() as conn:
        item_id = conn.execute(
            "INSERT INTO import_failure_queue_items (library_id, sonarr_queue_id) VALUES (%s, 1) RETURNING id",
            (library.id,),
        ).fetchone()["id"]
        conn.execute(
            "INSERT INTO import_failure_actions (queue_item_id, library_id, sonarr_queue_id, action, reason) "
            "VALUES (%s, %s, 1, 'observed', 'test')",
            (item_id, library.id),
        )

    assert library_repo.delete(library.id) is True
    assert library_repo.get(library.id) is None

    with database.connect() as conn:
        # The queue-item row itself survives with its identity column
        # nulled (SET NULL, not CASCADE) - the same lesson learned from the
        # slow-download guard's own P1 fix, applied here from the start.
        row = conn.execute("SELECT id, library_id FROM import_failure_queue_items WHERE id = %s", (item_id,)).fetchone()
        assert row is not None
        assert row["library_id"] is None
        action = conn.execute("SELECT queue_item_id, library_id, reason FROM import_failure_actions WHERE queue_item_id = %s", (item_id,)).fetchone()
        assert action is not None
        assert action["library_id"] == library.id  # immutable snapshot, not an FK
        assert action["reason"] == "test"


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


def test_m9_constraints_allow_only_documented_no_result_shapes(database):
    with database.connect() as conn:
        rows = conn.execute(
            """SELECT conname, pg_get_constraintdef(oid) AS definition
               FROM pg_constraint
               WHERE conname IN (
                 'dispatch_batches_reconciliation_state_check',
                 'dispatch_reconciliation_attempts_state_check',
                 'refresh_run_reconciled_batches_result_check',
                 'dispatch_outcome_events_source_endpoint_check',
                 'dispatch_outcome_events_event_type_check')"""
        ).fetchall()
    definitions = {row["conname"]: row["definition"] for row in rows}
    assert set(definitions) == {
        'dispatch_batches_reconciliation_state_check',
        'dispatch_reconciliation_attempts_state_check',
        'refresh_run_reconciled_batches_result_check',
        'dispatch_outcome_events_source_endpoint_check',
        'dispatch_outcome_events_event_type_check',
    }
    assert "no_result" in definitions['dispatch_batches_reconciliation_state_check']
    assert "no_result" in definitions['dispatch_reconciliation_attempts_state_check']
    assert "reconciliation" in definitions['dispatch_outcome_events_source_endpoint_check']
    assert "no_result" in definitions['dispatch_outcome_events_event_type_check']


def test_v13_recovery_transition_requires_stale_markerless_reservation(database,library_repo):
    library=library_repo.create({"name":"M13","type":"sonarr","url":"http://sonarr","api_key":"secret","enabled":True})
    def new_audit(conn,stale):
        # dispatch_started_at must be set at INSERT time (not via a later
        # same-state UPDATE): the v13 trigger only allows the enumerated
        # state transitions, so a bare 'dispatching'->'dispatching' update
        # that only touches dispatch_started_at is itself rejected.
        age="now()-interval '6 minutes'" if stale else "now()"
        return conn.execute(
            f"""INSERT INTO season_pack_audit(library_id,series_id,season_number,series_title,state,
               episode_count,rejected_summary,confirmation_digest,settings_snapshot,live_generation,dispatch_started_at)
               VALUES(%s,7,2,'Show','dispatching',2,'[]',%s,'{{}}',1,{age}) RETURNING id""",
            (library.id,"a"*64),
        ).fetchone()["id"]

    with database.connect() as conn:
        not_stale=new_audit(conn,stale=False)
    with pytest.raises(psycopg.errors.RaiseException,match="not stale"):
        with database.connect() as conn:
            conn.execute("UPDATE season_pack_audit SET state='previewed',live_generation=NULL,dispatch_started_at=NULL WHERE id=%s",(not_stale,))

    with database.connect() as conn:
        marked=new_audit(conn,stale=True)
        conn.execute("INSERT INTO season_pack_attempt_started(audit_id) VALUES(%s)",(marked,))
    with pytest.raises(psycopg.errors.RaiseException,match="forbids retry"):
        with database.connect() as conn:
            conn.execute("UPDATE season_pack_audit SET state='previewed',live_generation=NULL,dispatch_started_at=NULL WHERE id=%s",(marked,))

    with database.connect() as conn:
        dirty=new_audit(conn,stale=True)
    with pytest.raises(psycopg.errors.RaiseException,match="clear reservation metadata"):
        with database.connect() as conn:
            conn.execute("UPDATE season_pack_audit SET state='previewed',dispatch_started_at=NULL WHERE id=%s",(dirty,))

    with database.connect() as conn:
        clean=new_audit(conn,stale=True)
        conn.execute("UPDATE season_pack_audit SET state='previewed',live_generation=NULL,dispatch_started_at=NULL WHERE id=%s",(clean,))
        row=conn.execute("SELECT state,live_generation,dispatch_started_at FROM season_pack_audit WHERE id=%s",(clean,)).fetchone()
    assert row["state"]=="previewed" and row["live_generation"] is None and row["dispatch_started_at"] is None


def test_m11_season_pack_schema_is_idempotent_append_only_and_populated_safe(database,library_repo):
    library=library_repo.create({"name":"M11","type":"sonarr","url":"http://sonarr","api_key":"secret","enabled":True})
    with database.connect() as conn:
        conn.execute("INSERT INTO season_pack_settings(library_id,enabled,protocol,download_client_id) VALUES(%s,FALSE,NULL,NULL)",(library.id,))
        row=conn.execute("""INSERT INTO season_pack_audit(library_id,series_id,season_number,series_title,state,episode_count,rejected_summary,confirmation_digest,settings_snapshot) VALUES(%s,7,2,'Show','previewed',2,'[]',%s,'{}') RETURNING id""",(library.id,"a"*64)).fetchone()
    run_migrations(database)
    with database.connect() as conn:
        assert conn.execute("SELECT count(*) c FROM season_pack_audit WHERE id=%s",(row["id"],)).fetchone()["c"]==1
        with pytest.raises(psycopg.errors.RaiseException):conn.execute("DELETE FROM season_pack_audit WHERE id=%s",(row["id"],))


def test_m11_real_postgresql_upgrade_preserves_populated_append_only_audit(database):
    schema="m11_probe_"+uuid4().hex
    quoted=psycopg.sql.Identifier(schema)
    with database.connect() as conn:
        conn.execute(psycopg.sql.SQL("CREATE SCHEMA {}").format(quoted))
        try:
            conn.execute(psycopg.sql.SQL("SET search_path TO {}").format(quoted))
            for migration in MIGRATIONS[:10]:conn.execute(migration.sql)
            before=conn.execute("SELECT count(*) c FROM live_control_audit").fetchone()["c"]
            conn.execute(MIGRATIONS[10].sql)
            after=conn.execute("SELECT count(*) c FROM live_control_audit").fetchone()["c"]
            assert before>0 and after==before
            tables={r["table_name"] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s",(schema,)).fetchall()}
            assert {"season_pack_settings","season_pack_audit"}<=tables
            assert conn.execute("SELECT count(*) c FROM season_pack_settings").fetchone()["c"]==0
        finally:
            conn.execute("SET search_path TO public")
            conn.execute(psycopg.sql.SQL("DROP SCHEMA {} CASCADE").format(quoted))


def test_v17_adds_policy_concurrency_and_destructive_audit_columns(database):
    with database.connect() as conn:
        columns = {row["column_name"] for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='import_failure_policy_audit'"
        ).fetchall()}
        for field in ("remove_from_client", "blocklist", "skip_redownload"):
            assert f"{field}_before" in columns
            assert f"{field}_after" in columns
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM schema_migrations WHERE version=17"
        ).fetchone()["n"] == 1

"""Deterministic, transactional schema migrations for Managearr v1.

Each migration is a fixed, ordered SQL script. ``run_migrations`` takes a
PostgreSQL transaction-scoped advisory lock and applies every pending
migration plus its bookkeeping row in one transaction. A failure therefore
rolls back the entire pending set, and concurrent container startups cannot
both attempt the same DDL. Applied versions are recorded in
``schema_migrations``, so rerunning against an up-to-date database is a safe
no-op.

Existing tables (``arr_libraries``, ``automation_policy``,
``activity_jobs``, ``scan_candidates``) and their constraints match the
previous SQLite schema: same foreign keys, same ``ON DELETE
CASCADE``/``ON DELETE SET NULL`` behavior, same columns. Audit
timestamps (``created_at``/``updated_at``) are ``TIMESTAMPTZ`` so every
stored instant is unambiguously UTC - see the repository ``_now()``
helpers, which always pass timezone-aware UTC ``datetime`` values.
"""
from dataclasses import dataclass

from .database import Database

_BOOTSTRAP_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# Namespace this migration lock away from dispatch reservation locks. The
# constant only needs to be stable within this application/database.
_MIGRATION_LOCK_KEY = 486_267_921


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str


MIGRATIONS: list[Migration] = [
    Migration(
        version=1,
        name="initial_schema",
        sql="""
            CREATE TABLE arr_libraries (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                url TEXT NOT NULL,
                api_key TEXT NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );
            CREATE INDEX idx_arr_libraries_name_lower ON arr_libraries (LOWER(name));

            CREATE TABLE automation_policy (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                missing_enabled BOOLEAN NOT NULL,
                upgrades_enabled BOOLEAN NOT NULL,
                cycle_interval_minutes INTEGER NOT NULL,
                hourly_api_cap INTEGER NOT NULL,
                successful_grab_target INTEGER NOT NULL,
                dispatch_interval_seconds INTEGER NOT NULL,
                queue_target INTEGER NOT NULL,
                cooldown_minutes INTEGER NOT NULL,
                search_order TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE activity_jobs (
                id BIGSERIAL PRIMARY KEY,
                library_id BIGINT REFERENCES arr_libraries (id) ON DELETE SET NULL,
                library_name TEXT NOT NULL,
                job_type TEXT NOT NULL DEFAULT 'legacy',
                state TEXT NOT NULL,
                title TEXT NOT NULL,
                details TEXT NOT NULL DEFAULT '',
                candidate_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );
            CREATE INDEX idx_activity_jobs_state ON activity_jobs (state);
            CREATE INDEX idx_activity_jobs_library_id ON activity_jobs (library_id);
            CREATE INDEX idx_activity_jobs_updated_at ON activity_jobs (updated_at DESC);

            CREATE TABLE scan_candidates (
                id BIGSERIAL PRIMARY KEY,
                job_id BIGINT NOT NULL REFERENCES activity_jobs (id) ON DELETE CASCADE,
                library_id BIGINT REFERENCES arr_libraries (id) ON DELETE SET NULL,
                series_id INTEGER NOT NULL,
                series_title TEXT NOT NULL,
                episode_id INTEGER NOT NULL,
                season_number INTEGER NOT NULL,
                episode_number INTEGER NOT NULL,
                air_date TEXT,
                reason TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            );
            CREATE INDEX idx_scan_candidates_job_id ON scan_candidates (job_id);
            CREATE INDEX idx_scan_candidates_library_id ON scan_candidates (library_id);
        """,
    ),
    Migration(
        version=2,
        name="dispatch_ledger",
        sql="""
            CREATE TABLE dispatch_batches (
                id BIGSERIAL PRIMARY KEY,
                scan_job_id BIGINT NOT NULL REFERENCES activity_jobs (id) ON DELETE RESTRICT,
                library_id BIGINT REFERENCES arr_libraries (id) ON DELETE SET NULL,
                library_name TEXT NOT NULL,
                mode TEXT NOT NULL CHECK (mode IN ('dry_run', 'manual')),
                state TEXT NOT NULL CHECK (state IN ('planned', 'dispatching', 'completed', 'partial', 'failed')),
                requested_count INTEGER NOT NULL DEFAULT 0,
                selected_count INTEGER NOT NULL DEFAULT 0,
                dispatched_count INTEGER NOT NULL DEFAULT 0,
                sonarr_command_id INTEGER,
                sonarr_command_status TEXT,
                error_summary TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                CHECK (requested_count >= 0),
                CHECK (selected_count >= 0 AND selected_count <= requested_count),
                CHECK (dispatched_count >= 0 AND dispatched_count <= selected_count),
                CHECK (sonarr_command_id IS NULL OR sonarr_command_id > 0),
                CHECK (
                    (mode = 'dry_run' AND state = 'planned' AND dispatched_count = 0)
                    OR (mode = 'manual' AND state <> 'planned')
                ),
                CHECK (
                    (state = 'planned' AND sonarr_command_id IS NULL)
                    OR (state = 'dispatching' AND selected_count > 0
                        AND dispatched_count = 0 AND sonarr_command_id IS NULL)
                    OR (state = 'completed' AND selected_count = requested_count
                        AND dispatched_count = selected_count AND selected_count > 0
                        AND sonarr_command_id IS NOT NULL)
                    OR (state = 'partial' AND selected_count < requested_count
                        AND dispatched_count = selected_count AND selected_count > 0
                        AND sonarr_command_id IS NOT NULL)
                    OR (state = 'failed' AND dispatched_count = 0
                        AND sonarr_command_id IS NULL)
                )
            );
            CREATE INDEX idx_dispatch_batches_scan_job_id ON dispatch_batches (scan_job_id);
            CREATE INDEX idx_dispatch_batches_library_id ON dispatch_batches (library_id);
            CREATE INDEX idx_dispatch_batches_updated_at ON dispatch_batches (updated_at DESC);
            CREATE INDEX idx_dispatch_batches_mode_state ON dispatch_batches (mode, state);
            CREATE INDEX idx_dispatch_batches_library_mode_state ON dispatch_batches (library_id, mode, state);

            CREATE TABLE dispatch_batch_items (
                id BIGSERIAL PRIMARY KEY,
                batch_id BIGINT NOT NULL REFERENCES dispatch_batches (id) ON DELETE CASCADE,
                candidate_id BIGINT NOT NULL REFERENCES scan_candidates (id) ON DELETE RESTRICT,
                episode_id INTEGER NOT NULL,
                series_id INTEGER NOT NULL,
                series_title TEXT NOT NULL,
                season_number INTEGER NOT NULL,
                episode_number INTEGER NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('planned', 'reserved', 'dispatched', 'failed', 'excluded')),
                reason TEXT,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                CHECK (episode_id > 0),
                CHECK (series_id > 0),
                CHECK (season_number >= 0),
                CHECK (episode_number >= 0),
                CHECK (state NOT IN ('failed', 'excluded') OR reason IS NOT NULL),
                UNIQUE (batch_id, candidate_id)
            );
            CREATE INDEX idx_dispatch_batch_items_batch_id ON dispatch_batch_items (batch_id);
            CREATE INDEX idx_dispatch_batch_items_candidate_id ON dispatch_batch_items (candidate_id);
            CREATE INDEX idx_dispatch_batch_items_episode_id ON dispatch_batch_items (episode_id);
            CREATE INDEX idx_dispatch_batch_items_state ON dispatch_batch_items (state);
            CREATE INDEX idx_dispatch_batch_items_episode_state_updated
                ON dispatch_batch_items (episode_id, state, updated_at DESC);

            CREATE FUNCTION protect_dispatch_batch_audit() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'dispatch audit rows cannot be deleted';
                END IF;
                IF NEW.scan_job_id IS DISTINCT FROM OLD.scan_job_id
                   OR (
                       NEW.library_id IS DISTINCT FROM OLD.library_id
                       AND NOT (OLD.library_id IS NOT NULL AND NEW.library_id IS NULL)
                   )
                   OR NEW.library_name IS DISTINCT FROM OLD.library_name
                   OR NEW.mode IS DISTINCT FROM OLD.mode
                   OR NEW.requested_count IS DISTINCT FROM OLD.requested_count
                   OR NEW.selected_count IS DISTINCT FROM OLD.selected_count
                   OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION 'dispatch audit identity is immutable';
                END IF;
                IF OLD.library_id IS NOT NULL AND NEW.library_id IS NULL
                   AND NEW.state IS NOT DISTINCT FROM OLD.state
                   AND NEW.dispatched_count IS NOT DISTINCT FROM OLD.dispatched_count
                   AND NEW.sonarr_command_id IS NOT DISTINCT FROM OLD.sonarr_command_id
                   AND NEW.sonarr_command_status IS NOT DISTINCT FROM OLD.sonarr_command_status
                   AND NEW.error_summary IS NOT DISTINCT FROM OLD.error_summary
                   AND NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
                    RETURN NEW;
                END IF;
                IF OLD.state <> 'dispatching' OR NEW.state NOT IN ('completed', 'partial', 'failed') THEN
                    RAISE EXCEPTION 'invalid dispatch batch state transition';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER protect_dispatch_batch_audit_row
            BEFORE UPDATE OR DELETE ON dispatch_batches
            FOR EACH ROW EXECUTE FUNCTION protect_dispatch_batch_audit();

            CREATE FUNCTION protect_dispatch_item_audit() RETURNS trigger AS $$
            DECLARE
                parent_mode TEXT;
                parent_state TEXT;
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    SELECT mode, state INTO parent_mode, parent_state
                    FROM dispatch_batches WHERE id = NEW.batch_id;
                    IF NOT (
                        (parent_mode = 'dry_run' AND parent_state = 'planned'
                            AND NEW.state IN ('planned', 'excluded'))
                        OR (parent_mode = 'manual' AND parent_state = 'dispatching'
                            AND NEW.state IN ('reserved', 'excluded'))
                        OR (parent_mode = 'manual' AND parent_state IN ('completed', 'partial')
                            AND NEW.state IN ('dispatched', 'excluded'))
                        OR (parent_mode = 'manual' AND parent_state = 'failed'
                            AND NEW.state IN ('failed', 'excluded'))
                    ) THEN
                        RAISE EXCEPTION 'dispatch item state does not match its batch';
                    END IF;
                    RETURN NEW;
                END IF;
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'dispatch audit rows cannot be deleted';
                END IF;
                IF NEW.batch_id IS DISTINCT FROM OLD.batch_id
                   OR NEW.candidate_id IS DISTINCT FROM OLD.candidate_id
                   OR NEW.episode_id IS DISTINCT FROM OLD.episode_id
                   OR NEW.series_id IS DISTINCT FROM OLD.series_id
                   OR NEW.series_title IS DISTINCT FROM OLD.series_title
                   OR NEW.season_number IS DISTINCT FROM OLD.season_number
                   OR NEW.episode_number IS DISTINCT FROM OLD.episode_number
                   OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION 'dispatch audit identity is immutable';
                END IF;
                IF OLD.state <> 'reserved' OR NEW.state NOT IN ('dispatched', 'failed') THEN
                    RAISE EXCEPTION 'invalid dispatch item state transition';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER protect_dispatch_item_audit_row
            BEFORE INSERT OR UPDATE OR DELETE ON dispatch_batch_items
            FOR EACH ROW EXECUTE FUNCTION protect_dispatch_item_audit();
        """,
    ),
    Migration(
        version=3,
        name="dispatch_outcome_tracking",
        sql="""
            ALTER TABLE dispatch_batches
                ADD COLUMN reconciliation_state TEXT NOT NULL DEFAULT 'not_reconciled',
                ADD COLUMN reconciliation_summary VARCHAR(1000) NOT NULL DEFAULT '',
                ADD COLUMN last_reconciled_at TIMESTAMPTZ,
                ADD COLUMN command_observed_state TEXT;

            ALTER TABLE dispatch_batches
                ADD CONSTRAINT dispatch_batches_reconciliation_state_check CHECK (
                    reconciliation_state IN (
                        'not_reconciled', 'operator_review', 'unresolved',
                        'partial', 'resolved', 'error'
                    )
                ),
                ADD CONSTRAINT dispatch_batches_command_observed_state_check CHECK (
                    command_observed_state IS NULL OR command_observed_state IN (
                        'queued', 'running', 'completed', 'failed', 'aborted', 'unknown'
                    )
                ),
                ADD CONSTRAINT dispatch_batches_reconciliation_summary_length CHECK (
                    char_length(reconciliation_summary) <= 1000
                );

            ALTER TABLE dispatch_batches DROP CONSTRAINT dispatch_batches_state_check;
            ALTER TABLE dispatch_batches DROP CONSTRAINT dispatch_batches_check3;
            ALTER TABLE dispatch_batches
                ADD CONSTRAINT dispatch_batches_state_check CHECK (
                    state IN ('planned', 'dispatching', 'completed', 'partial', 'failed', 'ambiguous')
                ),
                ADD CONSTRAINT dispatch_batches_lifecycle_check CHECK (
                    (state = 'planned' AND sonarr_command_id IS NULL)
                    OR (state = 'dispatching' AND selected_count > 0
                        AND dispatched_count = 0)
                    OR (state = 'completed' AND selected_count = requested_count
                        AND dispatched_count = selected_count AND selected_count > 0
                        AND sonarr_command_id IS NOT NULL)
                    OR (state = 'partial' AND selected_count < requested_count
                        AND dispatched_count = selected_count AND selected_count > 0
                        AND sonarr_command_id IS NOT NULL)
                    OR (state = 'failed' AND dispatched_count = 0
                        AND sonarr_command_id IS NULL)
                    OR (state = 'ambiguous' AND dispatched_count = 0
                        AND selected_count > 0)
                );

            ALTER TABLE dispatch_batch_items DROP CONSTRAINT dispatch_batch_items_state_check;
            ALTER TABLE dispatch_batch_items DROP CONSTRAINT dispatch_batch_items_check;
            ALTER TABLE dispatch_batch_items
                ADD CONSTRAINT dispatch_batch_items_state_check CHECK (
                    state IN ('planned', 'reserved', 'dispatched', 'failed', 'excluded', 'ambiguous')
                ),
                ADD CONSTRAINT dispatch_batch_items_reason_check CHECK (
                    state NOT IN ('failed', 'excluded', 'ambiguous') OR reason IS NOT NULL
                );

            CREATE TABLE dispatch_reconciliation_attempts (
                id BIGSERIAL PRIMARY KEY,
                batch_id BIGINT NOT NULL REFERENCES dispatch_batches (id) ON DELETE RESTRICT,
                observed_at TIMESTAMPTZ NOT NULL,
                state TEXT NOT NULL CHECK (
                    state IN ('unresolved', 'partial', 'resolved', 'error')
                ),
                safe_summary VARCHAR(1000) NOT NULL,
                command_endpoint_read BOOLEAN NOT NULL DEFAULT FALSE,
                history_endpoint_read BOOLEAN NOT NULL DEFAULT FALSE,
                queue_endpoint_read BOOLEAN NOT NULL DEFAULT FALSE,
                inserted_event_count INTEGER NOT NULL DEFAULT 0 CHECK (inserted_event_count >= 0)
            );
            CREATE INDEX idx_dispatch_reconciliation_attempts_batch_observed
                ON dispatch_reconciliation_attempts (batch_id, observed_at DESC);

            CREATE TABLE dispatch_outcome_events (
                id BIGSERIAL PRIMARY KEY,
                batch_id BIGINT NOT NULL REFERENCES dispatch_batches (id) ON DELETE RESTRICT,
                dispatch_item_id BIGINT REFERENCES dispatch_batch_items (id) ON DELETE RESTRICT,
                candidate_id BIGINT,
                episode_id INTEGER,
                observed_at TIMESTAMPTZ NOT NULL,
                source_endpoint TEXT NOT NULL CHECK (
                    source_endpoint IN ('command', 'history', 'queue')
                ),
                event_type TEXT NOT NULL CHECK (
                    event_type IN (
                        'command_queued', 'command_running', 'command_completed',
                        'command_failed', 'command_aborted', 'grabbed', 'downloading',
                        'imported', 'download_failed', 'import_failed', 'unknown'
                    )
                ),
                event_state TEXT NOT NULL CHECK (
                    event_state IN ('nonterminal', 'terminal', 'unknown')
                ),
                safe_summary VARCHAR(1000) NOT NULL,
                sonarr_command_id INTEGER,
                sonarr_event_id BIGINT,
                download_id VARCHAR(255),
                evidence_key VARCHAR(500) NOT NULL,
                CHECK (episode_id IS NULL OR episode_id > 0),
                CHECK (sonarr_command_id IS NULL OR sonarr_command_id > 0),
                CHECK (sonarr_event_id IS NULL OR sonarr_event_id > 0),
                CHECK (
                    (dispatch_item_id IS NULL AND candidate_id IS NULL AND episode_id IS NULL)
                    OR (dispatch_item_id IS NOT NULL AND candidate_id IS NOT NULL AND episode_id IS NOT NULL)
                ),
                UNIQUE (batch_id, source_endpoint, evidence_key)
            );
            CREATE INDEX idx_dispatch_outcome_events_batch_observed
                ON dispatch_outcome_events (batch_id, observed_at DESC, id DESC);
            CREATE INDEX idx_dispatch_outcome_events_item_observed
                ON dispatch_outcome_events (dispatch_item_id, observed_at DESC, id DESC);
            CREATE INDEX idx_dispatch_outcome_events_episode
                ON dispatch_outcome_events (batch_id, episode_id, observed_at DESC);

            CREATE FUNCTION protect_outcome_audit() RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'outcome audit rows are append-only';
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER protect_reconciliation_attempt_audit
            BEFORE UPDATE OR DELETE ON dispatch_reconciliation_attempts
            FOR EACH ROW EXECUTE FUNCTION protect_outcome_audit();

            CREATE TRIGGER protect_outcome_event_audit
            BEFORE UPDATE OR DELETE ON dispatch_outcome_events
            FOR EACH ROW EXECUTE FUNCTION protect_outcome_audit();

            CREATE OR REPLACE FUNCTION protect_dispatch_batch_audit() RETURNS trigger AS $$
            DECLARE
                reconciliation_only BOOLEAN;
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'dispatch audit rows cannot be deleted';
                END IF;
                IF NEW.scan_job_id IS DISTINCT FROM OLD.scan_job_id
                   OR (
                       NEW.library_id IS DISTINCT FROM OLD.library_id
                       AND NOT (OLD.library_id IS NOT NULL AND NEW.library_id IS NULL)
                   )
                   OR NEW.library_name IS DISTINCT FROM OLD.library_name
                   OR NEW.mode IS DISTINCT FROM OLD.mode
                   OR NEW.requested_count IS DISTINCT FROM OLD.requested_count
                   OR NEW.selected_count IS DISTINCT FROM OLD.selected_count
                   OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION 'dispatch audit identity is immutable';
                END IF;

                IF OLD.library_id IS NOT NULL AND NEW.library_id IS NULL
                   AND NEW.state IS NOT DISTINCT FROM OLD.state
                   AND NEW.dispatched_count IS NOT DISTINCT FROM OLD.dispatched_count
                   AND NEW.sonarr_command_id IS NOT DISTINCT FROM OLD.sonarr_command_id
                   AND NEW.sonarr_command_status IS NOT DISTINCT FROM OLD.sonarr_command_status
                   AND NEW.error_summary IS NOT DISTINCT FROM OLD.error_summary
                   AND NEW.reconciliation_state IS NOT DISTINCT FROM OLD.reconciliation_state
                   AND NEW.reconciliation_summary IS NOT DISTINCT FROM OLD.reconciliation_summary
                   AND NEW.last_reconciled_at IS NOT DISTINCT FROM OLD.last_reconciled_at
                   AND NEW.command_observed_state IS NOT DISTINCT FROM OLD.command_observed_state
                   AND NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
                    RETURN NEW;
                END IF;

                reconciliation_only :=
                    NEW.state IS NOT DISTINCT FROM OLD.state
                    AND NEW.dispatched_count IS NOT DISTINCT FROM OLD.dispatched_count
                    AND NEW.sonarr_command_id IS NOT DISTINCT FROM OLD.sonarr_command_id
                    AND NEW.sonarr_command_status IS NOT DISTINCT FROM OLD.sonarr_command_status
                    AND NEW.error_summary IS NOT DISTINCT FROM OLD.error_summary;

                IF reconciliation_only THEN
                    IF (OLD.reconciliation_state = 'resolved'
                            AND NEW.reconciliation_state <> 'resolved')
                       OR (OLD.reconciliation_state = 'partial'
                            AND NEW.reconciliation_state IN (
                                'not_reconciled', 'operator_review', 'unresolved', 'error'
                            ))
                       OR (OLD.reconciliation_state = 'unresolved'
                            AND NEW.reconciliation_state IN (
                                'not_reconciled', 'operator_review', 'error'
                            ))
                       OR (OLD.reconciliation_state IN ('operator_review', 'error')
                            AND NEW.reconciliation_state = 'not_reconciled') THEN
                        RAISE EXCEPTION 'reconciliation state cannot regress';
                    END IF;
                    RETURN NEW;
                END IF;

                IF OLD.state = 'dispatching' AND NEW.state = 'dispatching'
                   AND OLD.sonarr_command_id IS NULL AND NEW.sonarr_command_id IS NOT NULL
                   AND NEW.dispatched_count = 0
                   AND NEW.error_summary IS NOT DISTINCT FROM OLD.error_summary THEN
                    RETURN NEW;
                END IF;

                IF OLD.state <> 'dispatching'
                   OR NEW.state NOT IN ('completed', 'partial', 'failed', 'ambiguous') THEN
                    RAISE EXCEPTION 'invalid dispatch batch state transition';
                END IF;
                IF OLD.sonarr_command_id IS NOT NULL
                   AND NEW.sonarr_command_id IS DISTINCT FROM OLD.sonarr_command_id THEN
                    RAISE EXCEPTION 'accepted Sonarr command identity is immutable';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION protect_dispatch_item_audit() RETURNS trigger AS $$
            DECLARE
                parent_mode TEXT;
                parent_state TEXT;
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    SELECT mode, state INTO parent_mode, parent_state
                    FROM dispatch_batches WHERE id = NEW.batch_id;
                    IF NOT (
                        (parent_mode = 'dry_run' AND parent_state = 'planned'
                            AND NEW.state IN ('planned', 'excluded'))
                        OR (parent_mode = 'manual' AND parent_state = 'dispatching'
                            AND NEW.state IN ('reserved', 'excluded'))
                        OR (parent_mode = 'manual' AND parent_state IN ('completed', 'partial')
                            AND NEW.state IN ('dispatched', 'excluded'))
                        OR (parent_mode = 'manual' AND parent_state = 'failed'
                            AND NEW.state IN ('failed', 'excluded'))
                        OR (parent_mode = 'manual' AND parent_state = 'ambiguous'
                            AND NEW.state IN ('ambiguous', 'excluded'))
                    ) THEN
                        RAISE EXCEPTION 'dispatch item state does not match its batch';
                    END IF;
                    RETURN NEW;
                END IF;
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'dispatch audit rows cannot be deleted';
                END IF;
                IF NEW.batch_id IS DISTINCT FROM OLD.batch_id
                   OR NEW.candidate_id IS DISTINCT FROM OLD.candidate_id
                   OR NEW.episode_id IS DISTINCT FROM OLD.episode_id
                   OR NEW.series_id IS DISTINCT FROM OLD.series_id
                   OR NEW.series_title IS DISTINCT FROM OLD.series_title
                   OR NEW.season_number IS DISTINCT FROM OLD.season_number
                   OR NEW.episode_number IS DISTINCT FROM OLD.episode_number
                   OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION 'dispatch audit identity is immutable';
                END IF;
                IF OLD.state <> 'reserved' OR NEW.state NOT IN ('dispatched', 'failed', 'ambiguous') THEN
                    RAISE EXCEPTION 'invalid dispatch item state transition';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """,
    ),
    Migration(
        version=4,
        name="simulation_scheduler",
        sql="""
            CREATE TABLE scheduler_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                mode TEXT NOT NULL DEFAULT 'off' CHECK (mode IN ('off', 'simulate')),
                next_due_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            INSERT INTO scheduler_settings (id, mode, next_due_at, updated_at)
            VALUES (1, 'off', NULL, now());

            CREATE TABLE scheduler_mode_audit (
                id BIGSERIAL PRIMARY KEY,
                previous_mode TEXT NOT NULL CHECK (previous_mode IN ('off', 'simulate')),
                new_mode TEXT NOT NULL CHECK (new_mode IN ('off', 'simulate')),
                changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                source TEXT NOT NULL DEFAULT 'application'
                    CHECK (source IN ('application', 'migration')),
                CHECK (previous_mode <> new_mode)
            );

            CREATE TABLE scheduler_leases (
                lease_name TEXT PRIMARY KEY CHECK (lease_name = 'cycle-worker'),
                owner_id VARCHAR(100),
                acquired_at TIMESTAMPTZ,
                heartbeat_at TIMESTAMPTZ,
                expires_at TIMESTAMPTZ,
                CHECK (
                    (owner_id IS NULL AND acquired_at IS NULL AND heartbeat_at IS NULL AND expires_at IS NULL)
                    OR (owner_id IS NOT NULL AND acquired_at IS NOT NULL
                        AND heartbeat_at IS NOT NULL AND expires_at IS NOT NULL
                        AND expires_at > heartbeat_at)
                )
            );
            INSERT INTO scheduler_leases (lease_name) VALUES ('cycle-worker');

            CREATE TABLE scheduler_run_requests (
                id BIGSERIAL PRIMARY KEY,
                request_kind TEXT NOT NULL DEFAULT 'simulation'
                    CHECK (request_kind = 'simulation'),
                state TEXT NOT NULL DEFAULT 'queued'
                    CHECK (state IN ('queued', 'claimed', 'completed', 'failed')),
                requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                claimed_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                claim_owner VARCHAR(100),
                safe_summary VARCHAR(1000) NOT NULL DEFAULT '',
                CHECK (
                    (state = 'queued' AND claimed_at IS NULL AND finished_at IS NULL AND claim_owner IS NULL)
                    OR (state = 'claimed' AND claimed_at IS NOT NULL AND finished_at IS NULL AND claim_owner IS NOT NULL)
                    OR (state IN ('completed', 'failed') AND claimed_at IS NOT NULL
                        AND finished_at IS NOT NULL AND claim_owner IS NOT NULL)
                )
            );
            CREATE UNIQUE INDEX uq_scheduler_one_active_manual_request
                ON scheduler_run_requests (request_kind)
                WHERE state IN ('queued', 'claimed');
            CREATE INDEX idx_scheduler_run_requests_state_requested
                ON scheduler_run_requests (state, requested_at);

            CREATE TABLE scheduler_cycle_runs (
                id BIGSERIAL PRIMARY KEY,
                request_id BIGINT UNIQUE REFERENCES scheduler_run_requests (id) ON DELETE RESTRICT,
                trigger TEXT NOT NULL CHECK (trigger IN ('scheduled', 'manual')),
                state TEXT NOT NULL CHECK (
                    state IN ('queued', 'running', 'completed', 'partial', 'failed', 'skipped')
                ),
                mode_snapshot TEXT NOT NULL CHECK (mode_snapshot = 'simulate'),
                policy_snapshot JSONB NOT NULL CHECK (jsonb_typeof(policy_snapshot) = 'object'),
                random_seed BIGINT,
                worker_owner VARCHAR(100),
                queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                library_count INTEGER NOT NULL DEFAULT 0 CHECK (library_count >= 0),
                completed_library_count INTEGER NOT NULL DEFAULT 0 CHECK (
                    completed_library_count >= 0 AND completed_library_count <= library_count
                ),
                failed_library_count INTEGER NOT NULL DEFAULT 0 CHECK (
                    failed_library_count >= 0 AND failed_library_count <= library_count
                ),
                considered_count INTEGER NOT NULL DEFAULT 0 CHECK (considered_count >= 0),
                selected_count INTEGER NOT NULL DEFAULT 0 CHECK (
                    selected_count >= 0 AND selected_count <= considered_count
                ),
                excluded_count INTEGER NOT NULL DEFAULT 0 CHECK (
                    excluded_count >= 0 AND excluded_count <= considered_count
                ),
                safe_summary VARCHAR(1000) NOT NULL DEFAULT '',
                CHECK (selected_count + excluded_count <= considered_count),
                CHECK (
                    (state = 'queued' AND started_at IS NULL AND finished_at IS NULL AND worker_owner IS NULL)
                    OR (state = 'running' AND started_at IS NOT NULL AND finished_at IS NULL AND worker_owner IS NOT NULL)
                    OR (state IN ('completed', 'partial', 'failed', 'skipped')
                        AND started_at IS NOT NULL AND finished_at IS NOT NULL AND worker_owner IS NOT NULL)
                ),
                CHECK ((trigger = 'manual' AND request_id IS NOT NULL)
                    OR (trigger = 'scheduled' AND request_id IS NULL))
            );
            CREATE INDEX idx_scheduler_cycle_runs_state_queued
                ON scheduler_cycle_runs (state, queued_at);
            CREATE INDEX idx_scheduler_cycle_runs_finished
                ON scheduler_cycle_runs (finished_at DESC NULLS LAST, id DESC);

            CREATE TABLE scheduler_library_results (
                id BIGSERIAL PRIMARY KEY,
                cycle_run_id BIGINT NOT NULL REFERENCES scheduler_cycle_runs (id) ON DELETE RESTRICT,
                library_id BIGINT REFERENCES arr_libraries (id) ON DELETE SET NULL,
                library_name VARCHAR(255) NOT NULL,
                scan_job_id BIGINT REFERENCES activity_jobs (id) ON DELETE RESTRICT,
                state TEXT NOT NULL CHECK (state IN ('completed', 'skipped', 'failed')),
                considered_count INTEGER NOT NULL DEFAULT 0 CHECK (considered_count >= 0),
                selected_count INTEGER NOT NULL DEFAULT 0 CHECK (
                    selected_count >= 0 AND selected_count <= considered_count
                ),
                excluded_count INTEGER NOT NULL DEFAULT 0 CHECK (
                    excluded_count >= 0 AND excluded_count <= considered_count
                ),
                effective_cap INTEGER NOT NULL DEFAULT 0 CHECK (effective_cap BETWEEN 0 AND 25),
                queue_occupancy INTEGER NOT NULL DEFAULT 0 CHECK (queue_occupancy >= 0),
                recent_success_count INTEGER NOT NULL DEFAULT 0 CHECK (recent_success_count >= 0),
                upgrades_state TEXT NOT NULL DEFAULT 'unsupported'
                    CHECK (upgrades_state IN ('disabled', 'unsupported')),
                safe_summary VARCHAR(1000) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CHECK (selected_count + excluded_count <= considered_count),
                UNIQUE (cycle_run_id, library_id)
            );
            CREATE INDEX idx_scheduler_library_results_cycle
                ON scheduler_library_results (cycle_run_id, id);
            CREATE INDEX idx_scheduler_library_results_library
                ON scheduler_library_results (library_id, created_at DESC);

            CREATE TABLE scheduler_candidate_results (
                id BIGSERIAL PRIMARY KEY,
                cycle_run_id BIGINT NOT NULL REFERENCES scheduler_cycle_runs (id) ON DELETE RESTRICT,
                library_result_id BIGINT NOT NULL REFERENCES scheduler_library_results (id) ON DELETE RESTRICT,
                candidate_id BIGINT NOT NULL REFERENCES scan_candidates (id) ON DELETE RESTRICT,
                episode_id INTEGER NOT NULL CHECK (episode_id > 0),
                series_id INTEGER NOT NULL CHECK (series_id > 0),
                series_title VARCHAR(500) NOT NULL,
                season_number INTEGER NOT NULL CHECK (season_number >= 0),
                episode_number INTEGER NOT NULL CHECK (episode_number >= 0),
                air_date VARCHAR(50),
                candidate_reason VARCHAR(100) NOT NULL,
                selected BOOLEAN NOT NULL,
                exclusion_reason VARCHAR(500),
                order_position INTEGER CHECK (order_position IS NULL OR order_position > 0),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CHECK ((selected AND exclusion_reason IS NULL AND order_position IS NOT NULL)
                    OR (NOT selected AND exclusion_reason IS NOT NULL)),
                UNIQUE (cycle_run_id, candidate_id)
            );
            CREATE INDEX idx_scheduler_candidate_results_cycle_selected
                ON scheduler_candidate_results (cycle_run_id, selected, order_position);
            CREATE INDEX idx_scheduler_candidate_results_library
                ON scheduler_candidate_results (library_result_id, id);

            CREATE FUNCTION audit_scheduler_mode_change() RETURNS trigger AS $$
            BEGIN
                IF NEW.mode IS DISTINCT FROM OLD.mode THEN
                    INSERT INTO scheduler_mode_audit (previous_mode, new_mode, changed_at, source)
                    VALUES (OLD.mode, NEW.mode, now(), 'application');
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER audit_scheduler_mode_change_row
                AFTER UPDATE ON scheduler_settings
                FOR EACH ROW EXECUTE FUNCTION audit_scheduler_mode_change();

            CREATE FUNCTION protect_scheduler_append_only() RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'scheduler audit rows are append-only';
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER protect_scheduler_mode_audit
                BEFORE UPDATE OR DELETE ON scheduler_mode_audit
                FOR EACH ROW EXECUTE FUNCTION protect_scheduler_append_only();
            CREATE TRIGGER protect_scheduler_library_result
                BEFORE UPDATE OR DELETE ON scheduler_library_results
                FOR EACH ROW EXECUTE FUNCTION protect_scheduler_append_only();
            CREATE TRIGGER protect_scheduler_candidate_result
                BEFORE UPDATE OR DELETE ON scheduler_candidate_results
                FOR EACH ROW EXECUTE FUNCTION protect_scheduler_append_only();

            CREATE FUNCTION protect_scheduler_cycle_run() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'scheduler cycle audit rows cannot be deleted';
                END IF;
                IF NEW.request_id IS DISTINCT FROM OLD.request_id
                   OR NEW.trigger IS DISTINCT FROM OLD.trigger
                   OR NEW.mode_snapshot IS DISTINCT FROM OLD.mode_snapshot
                   OR NEW.policy_snapshot IS DISTINCT FROM OLD.policy_snapshot
                   OR NEW.random_seed IS DISTINCT FROM OLD.random_seed
                   OR NEW.queued_at IS DISTINCT FROM OLD.queued_at THEN
                    RAISE EXCEPTION 'scheduler cycle identity is immutable';
                END IF;
                IF OLD.state = 'queued' AND NEW.state = 'running' THEN RETURN NEW; END IF;
                IF OLD.state = 'running'
                   AND NEW.state IN ('completed', 'partial', 'failed', 'skipped') THEN RETURN NEW; END IF;
                RAISE EXCEPTION 'invalid scheduler cycle state transition';
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER protect_scheduler_cycle_run_row
                BEFORE UPDATE OR DELETE ON scheduler_cycle_runs
                FOR EACH ROW EXECUTE FUNCTION protect_scheduler_cycle_run();

            CREATE FUNCTION protect_scheduler_run_request() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'scheduler request audit rows cannot be deleted';
                END IF;
                IF NEW.request_kind IS DISTINCT FROM OLD.request_kind
                   OR NEW.requested_at IS DISTINCT FROM OLD.requested_at THEN
                    RAISE EXCEPTION 'scheduler request identity is immutable';
                END IF;
                IF OLD.state = 'queued' AND NEW.state = 'claimed' THEN RETURN NEW; END IF;
                IF OLD.state = 'claimed' AND NEW.state IN ('completed', 'failed') THEN RETURN NEW; END IF;
                RAISE EXCEPTION 'invalid scheduler request state transition';
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER protect_scheduler_run_request_row
                BEFORE UPDATE OR DELETE ON scheduler_run_requests
                FOR EACH ROW EXECUTE FUNCTION protect_scheduler_run_request();
        """,
    ),
    Migration(
        version=5,
        name="readonly_refresh_automation",
        sql="""
            ALTER TABLE scheduler_library_results
                ADD COLUMN snapshot_taken_at TIMESTAMPTZ,
                ADD COLUMN snapshot_age_seconds INTEGER
                    CHECK (snapshot_age_seconds IS NULL OR snapshot_age_seconds >= 0);

            CREATE TABLE refresh_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                scan_max_age_minutes INTEGER NOT NULL DEFAULT 60
                    CHECK (scan_max_age_minutes BETWEEN 5 AND 10080),
                reconcile_min_interval_minutes INTEGER NOT NULL DEFAULT 15
                    CHECK (reconcile_min_interval_minutes BETWEEN 5 AND 1440),
                reconcile_max_per_cycle INTEGER NOT NULL DEFAULT 10
                    CHECK (reconcile_max_per_cycle BETWEEN 1 AND 50),
                next_reconcile_due_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            INSERT INTO refresh_settings (id) VALUES (1);

            CREATE TABLE refresh_requests (
                id BIGSERIAL PRIMARY KEY,
                kind TEXT NOT NULL DEFAULT 'scan' CHECK (kind = 'scan'),
                library_id BIGINT REFERENCES arr_libraries (id) ON DELETE SET NULL,
                library_name VARCHAR(255) NOT NULL,
                state TEXT NOT NULL DEFAULT 'queued'
                    CHECK (state IN ('queued', 'claimed', 'completed', 'failed')),
                requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                claimed_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                claim_owner VARCHAR(100),
                safe_summary VARCHAR(1000) NOT NULL DEFAULT '',
                CHECK (
                    (state = 'queued' AND claimed_at IS NULL AND finished_at IS NULL AND claim_owner IS NULL)
                    OR (state = 'claimed' AND claimed_at IS NOT NULL AND finished_at IS NULL AND claim_owner IS NOT NULL)
                    OR (state IN ('completed', 'failed') AND claimed_at IS NOT NULL
                        AND finished_at IS NOT NULL AND claim_owner IS NOT NULL)
                )
            );
            CREATE UNIQUE INDEX uq_refresh_requests_active_library
                ON refresh_requests (library_id)
                WHERE state IN ('queued', 'claimed') AND library_id IS NOT NULL;
            CREATE INDEX idx_refresh_requests_state_requested
                ON refresh_requests (state, requested_at);

            CREATE TABLE refresh_runs (
                id BIGSERIAL PRIMARY KEY,
                request_id BIGINT UNIQUE REFERENCES refresh_requests (id) ON DELETE RESTRICT,
                kind TEXT NOT NULL CHECK (kind IN ('scan', 'reconcile')),
                trigger TEXT NOT NULL CHECK (trigger IN ('scheduled', 'manual')),
                state TEXT NOT NULL CHECK (
                    state IN ('queued', 'running', 'completed', 'partial', 'failed', 'skipped')
                ),
                library_id BIGINT REFERENCES arr_libraries (id) ON DELETE SET NULL,
                library_name VARCHAR(255) NOT NULL DEFAULT '',
                scan_job_id BIGINT REFERENCES activity_jobs (id) ON DELETE RESTRICT,
                worker_owner VARCHAR(100),
                attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt >= 1),
                queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                target_count INTEGER NOT NULL DEFAULT 0 CHECK (target_count >= 0),
                succeeded_count INTEGER NOT NULL DEFAULT 0 CHECK (succeeded_count >= 0),
                failed_count INTEGER NOT NULL DEFAULT 0 CHECK (failed_count >= 0),
                skipped_count INTEGER NOT NULL DEFAULT 0 CHECK (skipped_count >= 0),
                safe_summary VARCHAR(1000) NOT NULL DEFAULT '',
                CHECK (
                    (kind = 'scan' AND (library_id IS NOT NULL OR state NOT IN ('queued', 'running')))
                    OR (kind = 'reconcile' AND library_id IS NULL AND scan_job_id IS NULL)
                ),
                CHECK (
                    (state = 'queued' AND started_at IS NULL AND finished_at IS NULL AND worker_owner IS NULL)
                    OR (state = 'running' AND started_at IS NOT NULL AND finished_at IS NULL AND worker_owner IS NOT NULL)
                    OR (state IN ('completed', 'partial', 'failed', 'skipped')
                        AND started_at IS NOT NULL AND finished_at IS NOT NULL AND worker_owner IS NOT NULL)
                ),
                CHECK ((trigger = 'manual' AND request_id IS NOT NULL)
                    OR (trigger = 'scheduled' AND request_id IS NULL))
            );
            CREATE UNIQUE INDEX uq_refresh_runs_active_scan_library
                ON refresh_runs (library_id)
                WHERE kind = 'scan' AND state IN ('queued', 'running');
            CREATE UNIQUE INDEX uq_refresh_runs_active_reconcile
                ON refresh_runs ((1))
                WHERE kind = 'reconcile' AND state IN ('queued', 'running');
            CREATE INDEX idx_refresh_runs_state_queued
                ON refresh_runs (state, queued_at);
            CREATE INDEX idx_refresh_runs_kind_library
                ON refresh_runs (kind, library_id, finished_at DESC NULLS LAST, id DESC);
            CREATE INDEX idx_refresh_runs_finished
                ON refresh_runs (finished_at DESC NULLS LAST, id DESC);

            CREATE TABLE refresh_run_reconciled_batches (
                id BIGSERIAL PRIMARY KEY,
                refresh_run_id BIGINT NOT NULL REFERENCES refresh_runs (id) ON DELETE RESTRICT,
                dispatch_batch_id BIGINT NOT NULL REFERENCES dispatch_batches (id) ON DELETE RESTRICT,
                result TEXT NOT NULL CHECK (
                    result IN ('resolved', 'partial', 'unresolved', 'error', 'skipped')
                ),
                reason VARCHAR(500),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (refresh_run_id, dispatch_batch_id)
            );
            CREATE INDEX idx_refresh_run_reconciled_batches_run
                ON refresh_run_reconciled_batches (refresh_run_id, id);
            CREATE INDEX idx_refresh_run_reconciled_batches_batch
                ON refresh_run_reconciled_batches (dispatch_batch_id, created_at DESC);

            CREATE FUNCTION protect_refresh_append_only() RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'refresh audit rows are append-only';
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER protect_refresh_run_reconciled_batch
                BEFORE UPDATE OR DELETE ON refresh_run_reconciled_batches
                FOR EACH ROW EXECUTE FUNCTION protect_refresh_append_only();

            CREATE FUNCTION protect_refresh_request() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'refresh request audit rows cannot be deleted';
                END IF;
                IF NEW.kind IS DISTINCT FROM OLD.kind
                   OR NEW.library_name IS DISTINCT FROM OLD.library_name
                   OR NEW.requested_at IS DISTINCT FROM OLD.requested_at THEN
                    RAISE EXCEPTION 'refresh request identity is immutable';
                END IF;
                IF OLD.library_id IS NOT NULL AND NEW.library_id IS NULL
                   AND NEW.state IS NOT DISTINCT FROM OLD.state
                   AND NEW.claimed_at IS NOT DISTINCT FROM OLD.claimed_at
                   AND NEW.finished_at IS NOT DISTINCT FROM OLD.finished_at
                   AND NEW.claim_owner IS NOT DISTINCT FROM OLD.claim_owner
                   AND NEW.safe_summary IS NOT DISTINCT FROM OLD.safe_summary THEN
                    RETURN NEW;
                END IF;
                IF OLD.state = 'queued' AND NEW.state = 'claimed' THEN RETURN NEW; END IF;
                IF OLD.state = 'claimed' AND NEW.state IN ('completed', 'failed') THEN RETURN NEW; END IF;
                RAISE EXCEPTION 'invalid refresh request state transition';
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER protect_refresh_request_row
                BEFORE UPDATE OR DELETE ON refresh_requests
                FOR EACH ROW EXECUTE FUNCTION protect_refresh_request();

            CREATE FUNCTION protect_refresh_run() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'refresh run audit rows cannot be deleted';
                END IF;
                IF NEW.request_id IS DISTINCT FROM OLD.request_id
                   OR NEW.kind IS DISTINCT FROM OLD.kind
                   OR NEW.trigger IS DISTINCT FROM OLD.trigger
                   OR NEW.queued_at IS DISTINCT FROM OLD.queued_at THEN
                    RAISE EXCEPTION 'refresh run identity is immutable';
                END IF;
                IF OLD.library_id IS NOT NULL AND NEW.library_id IS NULL
                   AND NEW.state IS NOT DISTINCT FROM OLD.state
                   AND NEW.started_at IS NOT DISTINCT FROM OLD.started_at
                   AND NEW.finished_at IS NOT DISTINCT FROM OLD.finished_at
                   AND NEW.worker_owner IS NOT DISTINCT FROM OLD.worker_owner THEN
                    RETURN NEW;
                END IF;
                IF OLD.state = 'queued' AND NEW.state = 'running' THEN RETURN NEW; END IF;
                IF OLD.state = 'running'
                   AND NEW.state IN ('completed', 'partial', 'failed', 'skipped') THEN RETURN NEW; END IF;
                RAISE EXCEPTION 'invalid refresh run state transition';
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER protect_refresh_run_row
                BEFORE UPDATE OR DELETE ON refresh_runs
                FOR EACH ROW EXECUTE FUNCTION protect_refresh_run();
        """,
    ),
    Migration(
        version=6,
        name="controlled_live_dispatch",
        sql="""
            -- Widen the mode constraints that previously hard-limited the
            -- scheduler to off/simulate. 'live' is added as a value the
            -- scheduler can be switched to; nothing about off/simulate
            -- behavior changes and no existing row's mode is touched.
            ALTER TABLE scheduler_settings DROP CONSTRAINT scheduler_settings_mode_check;
            ALTER TABLE scheduler_settings
                ADD CONSTRAINT scheduler_settings_mode_check CHECK (mode IN ('off', 'simulate', 'live'));

            ALTER TABLE scheduler_mode_audit DROP CONSTRAINT scheduler_mode_audit_previous_mode_check;
            ALTER TABLE scheduler_mode_audit DROP CONSTRAINT scheduler_mode_audit_new_mode_check;
            ALTER TABLE scheduler_mode_audit
                ADD CONSTRAINT scheduler_mode_audit_previous_mode_check
                    CHECK (previous_mode IN ('off', 'simulate', 'live')),
                ADD CONSTRAINT scheduler_mode_audit_new_mode_check
                    CHECK (new_mode IN ('off', 'simulate', 'live'));

            ALTER TABLE scheduler_cycle_runs DROP CONSTRAINT scheduler_cycle_runs_mode_snapshot_check;
            ALTER TABLE scheduler_cycle_runs
                ADD CONSTRAINT scheduler_cycle_runs_mode_snapshot_check
                    CHECK (mode_snapshot IN ('simulate', 'live'));

            -- Emergency stop must be able to cancel an already-queued (not
            -- yet started) live cycle immediately, so 'queued' -> 'skipped'
            -- becomes a second controlled transition alongside the
            -- existing 'queued' -> 'running' one.
            CREATE OR REPLACE FUNCTION protect_scheduler_cycle_run() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'scheduler cycle audit rows cannot be deleted';
                END IF;
                IF NEW.request_id IS DISTINCT FROM OLD.request_id
                   OR NEW.trigger IS DISTINCT FROM OLD.trigger
                   OR NEW.mode_snapshot IS DISTINCT FROM OLD.mode_snapshot
                   OR NEW.policy_snapshot IS DISTINCT FROM OLD.policy_snapshot
                   OR NEW.random_seed IS DISTINCT FROM OLD.random_seed
                   OR NEW.queued_at IS DISTINCT FROM OLD.queued_at THEN
                    RAISE EXCEPTION 'scheduler cycle identity is immutable';
                END IF;
                IF OLD.state = 'queued' AND NEW.state = 'running' THEN RETURN NEW; END IF;
                IF OLD.state = 'queued' AND NEW.state = 'skipped' THEN RETURN NEW; END IF;
                IF OLD.state = 'running'
                   AND NEW.state IN ('completed', 'partial', 'failed', 'skipped') THEN RETURN NEW; END IF;
                RAISE EXCEPTION 'invalid scheduler cycle state transition';
            END;
            $$ LANGUAGE plpgsql;

            -- Live-control state is a singleton row separate from
            -- scheduler_settings.mode. 'armed' only ever becomes TRUE
            -- through the application's arm-confirm path; a bare restart
            -- never sets it, and it always carries a bounded expires_at.
            CREATE TABLE live_control (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                armed BOOLEAN NOT NULL DEFAULT FALSE,
                arm_generation BIGINT NOT NULL DEFAULT 0 CHECK (arm_generation >= 0),
                armed_at TIMESTAMPTZ,
                armed_by VARCHAR(255),
                armed_reason VARCHAR(500),
                expires_at TIMESTAMPTZ,
                emergency_stopped_at TIMESTAMPTZ,
                emergency_stop_reason VARCHAR(500),
                emergency_stop_generation BIGINT NOT NULL DEFAULT 0 CHECK (emergency_stop_generation >= 0),
                max_dispatches_per_cycle INTEGER NOT NULL DEFAULT 1
                    CHECK (max_dispatches_per_cycle BETWEEN 1 AND 5),
                min_delay_seconds_between_dispatches INTEGER NOT NULL DEFAULT 30
                    CHECK (min_delay_seconds_between_dispatches BETWEEN 5 AND 600),
                default_arm_ttl_minutes INTEGER NOT NULL DEFAULT 15
                    CHECK (default_arm_ttl_minutes BETWEEN 1 AND 60),
                max_arm_ttl_minutes INTEGER NOT NULL DEFAULT 60
                    CHECK (max_arm_ttl_minutes BETWEEN 1 AND 60),
                last_dispatch_at TIMESTAMPTZ,
                last_dispatch_summary VARCHAR(1000) NOT NULL DEFAULT '',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CHECK (default_arm_ttl_minutes <= max_arm_ttl_minutes),
                CHECK (
                    (armed = FALSE AND armed_at IS NULL AND armed_by IS NULL
                        AND armed_reason IS NULL AND expires_at IS NULL)
                    OR
                    (armed = TRUE AND armed_at IS NOT NULL AND armed_by IS NOT NULL
                        AND armed_reason IS NOT NULL AND expires_at IS NOT NULL
                        AND arm_generation > 0)
                )
            );
            INSERT INTO live_control (id) VALUES (1);

            -- Short-lived, server-issued challenges for the two-step
            -- enable-live-mode and arm flows. Only a SHA-256 hash of the
            -- opaque token is ever stored - never the raw token.
            CREATE TABLE live_challenges (
                id BIGSERIAL PRIMARY KEY,
                kind TEXT NOT NULL CHECK (kind IN ('enable_mode', 'arm')),
                token_hash VARCHAR(64) NOT NULL,
                policy_digest VARCHAR(64) NOT NULL,
                requested_reason VARCHAR(500),
                requested_ttl_minutes INTEGER CHECK (requested_ttl_minutes IS NULL OR requested_ttl_minutes BETWEEN 1 AND 60),
                state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'confirmed', 'expired', 'consumed')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at TIMESTAMPTZ NOT NULL,
                confirmed_at TIMESTAMPTZ,
                CHECK (kind = 'arm' OR (requested_reason IS NULL AND requested_ttl_minutes IS NULL)),
                CHECK (kind = 'enable_mode' OR (requested_reason IS NOT NULL AND requested_ttl_minutes IS NOT NULL)),
                CHECK ((state = 'confirmed') = (confirmed_at IS NOT NULL))
            );
            CREATE INDEX idx_live_challenges_kind_state_expires
                ON live_challenges (kind, state, expires_at);

            CREATE FUNCTION protect_live_append_only() RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'live control audit rows are append-only';
            END;
            $$ LANGUAGE plpgsql;

            CREATE FUNCTION protect_live_challenge() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'live challenge audit rows cannot be deleted';
                END IF;
                IF NEW.kind IS DISTINCT FROM OLD.kind
                   OR NEW.token_hash IS DISTINCT FROM OLD.token_hash
                   OR NEW.policy_digest IS DISTINCT FROM OLD.policy_digest
                   OR NEW.requested_reason IS DISTINCT FROM OLD.requested_reason
                   OR NEW.requested_ttl_minutes IS DISTINCT FROM OLD.requested_ttl_minutes
                   OR NEW.created_at IS DISTINCT FROM OLD.created_at
                   OR NEW.expires_at IS DISTINCT FROM OLD.expires_at THEN
                    RAISE EXCEPTION 'live challenge identity is immutable';
                END IF;
                IF OLD.state = 'pending' AND NEW.state IN ('confirmed', 'expired', 'consumed') THEN
                    RETURN NEW;
                END IF;
                RAISE EXCEPTION 'invalid live challenge state transition';
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER protect_live_challenge_row
                BEFORE UPDATE OR DELETE ON live_challenges
                FOR EACH ROW EXECUTE FUNCTION protect_live_challenge();

            -- Append-only audit of every mode/arm/disarm/emergency-stop
            -- transition, independent of the mutable live_control row.
            CREATE TABLE live_control_audit (
                id BIGSERIAL PRIMARY KEY,
                event_type TEXT NOT NULL CHECK (event_type IN (
                    'mode_enabled', 'mode_disabled', 'armed', 'disarmed',
                    'emergency_stop', 'arm_expired'
                )),
                previous_mode TEXT,
                new_mode TEXT,
                reason VARCHAR(500),
                actor VARCHAR(255),
                arm_generation BIGINT,
                occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX idx_live_control_audit_occurred
                ON live_control_audit (occurred_at DESC, id DESC);
            CREATE TRIGGER protect_live_control_audit
                BEFORE UPDATE OR DELETE ON live_control_audit
                FOR EACH ROW EXECUTE FUNCTION protect_live_append_only();

            -- Per-cycle live dispatch ledger: exactly one row per candidate
            -- a live cycle actually attempted (or explicitly declined to
            -- attempt), linking the scheduler's planning audit to the
            -- reused manual dispatch ledger row it produced.
            CREATE TABLE live_dispatch_ledger (
                id BIGSERIAL PRIMARY KEY,
                cycle_run_id BIGINT NOT NULL REFERENCES scheduler_cycle_runs (id) ON DELETE RESTRICT,
                library_result_id BIGINT REFERENCES scheduler_library_results (id) ON DELETE RESTRICT,
                candidate_result_id BIGINT REFERENCES scheduler_candidate_results (id) ON DELETE RESTRICT,
                library_id BIGINT REFERENCES arr_libraries (id) ON DELETE SET NULL,
                candidate_id BIGINT REFERENCES scan_candidates (id) ON DELETE RESTRICT,
                dispatch_batch_id BIGINT REFERENCES dispatch_batches (id) ON DELETE RESTRICT,
                dispatch_item_id BIGINT REFERENCES dispatch_batch_items (id) ON DELETE RESTRICT,
                attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt >= 1),
                arm_generation BIGINT NOT NULL CHECK (arm_generation > 0),
                state TEXT NOT NULL CHECK (state IN ('dispatched', 'failed', 'ambiguous', 'blocked', 'skipped')),
                sonarr_command_id INTEGER CHECK (sonarr_command_id IS NULL OR sonarr_command_id > 0),
                sonarr_command_status TEXT,
                terminal_reason VARCHAR(500) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (cycle_run_id, candidate_result_id)
            );
            CREATE INDEX idx_live_dispatch_ledger_cycle
                ON live_dispatch_ledger (cycle_run_id, id);
            CREATE INDEX idx_live_dispatch_ledger_batch
                ON live_dispatch_ledger (dispatch_batch_id);
            CREATE INDEX idx_live_dispatch_ledger_library
                ON live_dispatch_ledger (library_id, created_at DESC);
            CREATE TRIGGER protect_live_dispatch_ledger
                BEFORE UPDATE OR DELETE ON live_dispatch_ledger
                FOR EACH ROW EXECUTE FUNCTION protect_live_append_only();
        """,
    ),
]


def run_migrations(db: Database) -> None:
    with db.connect() as conn:
        # A transaction-scoped lock keeps the bootstrap/read/apply sequence
        # atomic across concurrent app starts and releases automatically on
        # either commit or rollback.
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_MIGRATION_LOCK_KEY,))
        conn.execute(_BOOTSTRAP_SQL)
        applied = {
            row["version"]
            for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
        }
        for migration in MIGRATIONS:
            if migration.version in applied:
                continue
            conn.execute(migration.sql)
            conn.execute(
                "INSERT INTO schema_migrations (version, name) VALUES (%s, %s)",
                (migration.version, migration.name),
            )


def current_version(db: Database) -> int:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations"
        ).fetchone()
    return row["version"] if row else 0

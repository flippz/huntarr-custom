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

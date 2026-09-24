"""Deterministic, transactional schema migrations for Managearr v1.

Each migration is a fixed, ordered SQL script applied inside its own
transaction (commit on success, rollback on any error - see
``Database.connect()``). Applied versions are recorded in
``schema_migrations``, so ``run_migrations`` only ever applies versions
it hasn't seen yet: running it against an already-migrated database, or
concurrently at container startup, is a safe no-op.

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
]


def run_migrations(db: Database) -> None:
    with db.connect() as conn:
        conn.execute(_BOOTSTRAP_SQL)
        applied = {
            row["version"]
            for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
        }

    for migration in MIGRATIONS:
        if migration.version in applied:
            continue
        with db.connect() as conn:
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

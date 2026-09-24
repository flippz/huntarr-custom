# Managearr v1 (Development Preview)

This document describes the `managearr/` application: a standalone rewrite
that lives alongside the legacy Huntarr v1 app (`main.py`, `src/`) without
touching it. v1 continues to run exactly as before - Managearr has its own
dependencies, its own database, its own Docker image(s), and its own port.

**PostgreSQL is the only runtime database.** Earlier previews used a local
SQLite file; this milestone replaces that entirely with PostgreSQL 18,
run as a second Compose service with file-based credentials, deterministic
schema migrations, and a one-time SQLite importer for anyone carrying data
forward from a prior preview - see
[PostgreSQL migration](#postgresql-migration) below.

**Current milestone (M1): read-only Sonarr scan preview.** On top of the
foundation milestone (library/policy/activity CRUD), this milestone adds a
Sonarr adapter and a manual, read-only candidate scan. It does **not**
search, grab, download, import, or send any command to Sonarr. See
[Non-goals](#non-goals--current-limitations) below.

## Architecture

`managearr/` is a small layered Flask app:

```
managearr/
  app/
    domain/       plain dataclasses + validation, zero I/O
      arr_library.py       ArrLibrary, ARR_TYPES, validate_library_input
      automation_policy.py AutomationPolicy, BALANCED_DEFAULTS, validate_policy_input
      activity.py          ActivityJob, JOB_STATES, JOB_TYPES
      scan_candidate.py    ScanCandidate, derive_missing_candidate (pure rule fn)
    services/      use-case orchestration over domain + persistence
      library_service.py, policy_service.py, activity_service.py, status_service.py
      sonarr_scan_service.py   orchestrates connection test + candidate scan
      scan_candidate_service.py read-only candidate listing
    adapters/       boundary transforms + external integrations
      redaction.py       strips api_key before any HTTP response
      sonarr_client.py   read-only Sonarr API v3 client (requests, timeouts)
    persistence/    PostgreSQL schema/migrations + repositories (psycopg 3, no ORM)
      database.py     connection pool (psycopg_pool), startup retry/backoff, health check
      migrations.py   deterministic, transactional schema migrations + schema_migrations table
      library_repository.py, policy_repository.py,
      activity_repository.py, scan_candidate_repository.py
    api/            JSON API blueprint, mounted at /api/v1
    web/            server-rendered UI shell (Jinja2 + vanilla JS/CSS, no build step)
  tools/
    import_sqlite.py  one-time, explicit SQLite -> PostgreSQL importer (see below)
  config.py         env-driven runtime config (incl. file-based DB credentials)
  run.py            entry point (Flask dev server)
  requirements.txt  runtime deps (Flask + CVE-pinned Werkzeug/Jinja2 + requests + psycopg[binary,pool])
  requirements-dev.txt  adds pytest
  tests/            pytest suite (unit + Flask test-client integration + real-PostgreSQL
                     integration tests; only Sonarr network calls are mocked)
```

Data flows one direction: `web`/`api` call into `services`, `services` call
into `persistence`/`adapters` and use `domain` for validation, `adapters` sit
at the HTTP/Sonarr boundary to redact secrets and to wrap the Sonarr HTTP API.
Nothing in `domain` or `persistence` knows about Flask.

### Domain model

- **ArrLibrary** - a configured connection to one of six *Arr ecosystems:
  `sonarr`, `radarr`, `lidarr`, `readarr`, `whisparr`, `eros`. Fields:
  `name`, `type`, `url`, `api_key`, `enabled`. Persisted in PostgreSQL via
  `LibraryRepository`, full CRUD exposed at `/api/v1/libraries`.
  **`api_key` is never present in list/detail API responses** - the
  `redact_library`/`redact_libraries` adapter strips it and exposes a
  `has_api_key` boolean instead. Only `sonarr`-type, enabled libraries with
  a configured key support Test/Scan in this milestone.

- **AutomationPolicy** - a single global policy row describing how a future
  search engine should behave. Unchanged from the foundation milestone; no
  engine consumes it yet.

- **ActivityJob** - a durable row tracking one unit of work. Two
  `job_type` values now exist:
  - `legacy` - the original `planned -> ... -> completed` hunting pipeline
    model. Still never written by anything in this milestone.
  - `sonarr_scan` - written by `SonarrScanService`. Only ever moves
    `searching -> completed` or `searching -> failed`. One row per scan
    run, carrying `candidate_count` and a human-readable `details` summary.

  The read-only API (`GET /api/v1/activity`, `GET /api/v1/activity/<id>`)
  is unchanged; `sonarr_scan` jobs are written internally by
  `SonarrScanService.run_scan`, never by the API layer.

- **ScanCandidate** - one (series, episode) pair identified by a scan as
  monitored, aired, and missing a file. Fields: `job_id`, `library_id`,
  `series_id`, `series_title`, `episode_id`, `season_number`,
  `episode_number`, `air_date`, `reason`, `created_at`. Rows are an
  immutable snapshot tied to the `job_id` that produced them - see
  [Idempotent snapshots](#idempotent-snapshots) below. The selection rule
  itself (`derive_missing_candidate` in `app/domain/scan_candidate.py`) is a
  pure function with no I/O: a candidate requires the **series** to be
  monitored, the **episode** to be monitored, `hasFile` to be false, and a
  valid `airDate` that is today or in the past (future episodes, even if
  monitored, are excluded).

### Sonarr adapter (`app/adapters/sonarr_client.py`)

`SonarrClient` wraps three read-only Sonarr v3 endpoints over `requests`,
with a configurable timeout (`MANAGEARR_SONARR_TIMEOUT_SECONDS`, default 10s)
and safe URL joining (`urljoin` against a fixed, adapter-controlled path -
never a user-supplied one):

- `system_status()` - `GET /api/v3/system/status`, used for the connection
  test. Returns `{"version": ..., "instance_name": ...}`.
- `get_series()` - `GET /api/v3/series`.
- `get_episodes(series_id)` - `GET /api/v3/episode?seriesId=<id>`.

**It never calls any other endpoint and never issues a Sonarr command**
(no `POST /api/v3/command`, no writes of any kind). All failure modes map
to one of four exception types, each with a static, safe message that
never includes the configured URL or API key:

| Exception | Cause |
|---|---|
| `SonarrConnectionError` | timeout, DNS failure, connection refused |
| `SonarrAuthError` | HTTP 401/403 - bad API key |
| `SonarrResponseError` | any other non-2xx HTTP status |
| `SonarrDataError` | non-JSON body, or JSON that doesn't match the expected shape (missing `version`, series/episode payload not a list) |

### Scan orchestration (`app/services/sonarr_scan_service.py`)

`SonarrScanService` is the only thing in this codebase allowed to
construct a `SonarrClient` and call it. Two entry points:

- **`test_connection(library_id)`** - validates the library (exists, type
  `sonarr`, enabled, has an API key), then calls `system_status()`. Returns
  `(status_dict, None)` or `(None, error_message)`. Never writes an
  activity job.

- **`run_scan(library_id)`** - same validation, then:
  1. Creates an `ActivityJob` (`job_type=sonarr_scan`, `state=searching`).
  2. Calls `get_series()`. If this fails, the job is marked `failed` with
     the adapter's safe error message and the scan stops - **this is the
     only case that fails the whole job**.
  3. For each **monitored** series, calls `get_episodes(series_id)`. If
     this fails for one series, that series is skipped (counted in the
     job's `details` summary) and the scan continues with the rest - a
     single series with bad data doesn't fail the whole scan.
  4. Runs `derive_missing_candidate` over every episode, capping total
     candidates at `MAX_CANDIDATES_PER_SCAN` (500). If the cap is hit, the
     job's `details` says so.
  5. Persists all candidates in one batch via
     `ScanCandidateRepository.create_many`, then marks the job `completed`
     with `candidate_count` and a `details` summary.

  Validation failures (library not found / wrong type / disabled / no key)
  return `(None, error_message)` **without creating a job**. Once a job
  exists, `run_scan` always returns `(job, None)` - upstream Sonarr
  failures are recorded on the job, not surfaced as a second class of API
  error.

  **No long-lived DB transaction ever wraps a Sonarr call.** Each step
  above that touches PostgreSQL (job creation, `create_many`,
  `update_state`) opens and closes its own short transaction via
  `Database.connect()`; the Sonarr HTTP calls in between (`get_series`,
  `get_episodes`) always run with no DB transaction open. A slow or
  hanging Sonarr instance therefore never holds a PostgreSQL connection
  or lock.

#### Idempotent snapshots

Every call to `run_scan` creates a brand-new `activity_jobs` row and a
fresh batch of `scan_candidates` rows tied to that row's `id`
(`ON DELETE CASCADE`, but nothing ever deletes a job in this milestone).
Repeated scans of the same library never update or delete a prior scan's
rows - each scan is an independent, permanent snapshot. See
`test_repeat_scans_create_separate_jobs_without_corrupting_prior_snapshot`
in `tests/test_sonarr_scan_service.py`.

### Persistence

PostgreSQL via `psycopg` 3 with a small connection pool (`psycopg_pool`),
dict-row results, and one pooled connection checked out per repository
operation - the same short-lived-connection-per-operation shape the prior
SQLite implementation used, so no repository ever holds a transaction open
across an external network call (see
[Scan orchestration](#scan-orchestration-appservicessonarr_scan_servicepy)
below for why that matters).

Schema is applied by `app/persistence/migrations.py`: a fixed, ordered list
of `Migration(version, name, sql)` entries, each applied inside its own
transaction and recorded in a `schema_migrations` table. `run_migrations()`
only applies versions it hasn't seen yet, so calling it on every app/tool
startup (as `create_app()` and `tools/import_sqlite.py` both do) is always
a safe no-op once the schema is current. All four tables
(`arr_libraries`, `automation_policy`, `activity_jobs`, `scan_candidates`)
keep the same columns, foreign keys, and delete behavior as the previous
SQLite schema (`activity_jobs.library_id` → `SET NULL`,
`scan_candidates.job_id` → `CASCADE`, `scan_candidates.library_id` →
`SET NULL`); `created_at`/`updated_at` are `TIMESTAMPTZ` so every stored
instant is unambiguously UTC, and indexes exist on the columns the app
actually filters/sorts by (`state`, `library_id`, `updated_at`,
`job_id`, `LOWER(name)`).

**Credentials** are never hardcoded or defaulted to a real password:
`Database` is always constructed from `MANAGEARR_DB_*` env vars, and the
password specifically comes from `config.read_secret()`, which prefers
`MANAGEARR_DB_PASSWORD_FILE` (a file-based secret - see
[PostgreSQL migration](#postgresql-migration)) over the plain
`MANAGEARR_DB_PASSWORD` env var.

**Startup** calls `Database.wait_ready(timeout_seconds)`, which opens the
connection pool with a bounded wait (`psycopg_pool`'s own internal retry
loop, capped by `MANAGEARR_DB_STARTUP_TIMEOUT_SECONDS`, default 30s) and
raises `DatabaseUnavailableError` - a static, safe message that never
includes the configured host/port/user/password - if PostgreSQL never
becomes reachable in time.

**`/health` and `/api/v1/status`** report PostgreSQL connectivity
(`database` / `database.connected`) and the applied `schema_version`,
and nothing else about the connection - never the host, user, or
password (verified by
`test_health_reports_schema_version_without_connection_details` and
`test_status_reports_schema_version_without_connection_details`).

### API surface

| Method | Path                        | Purpose                                  |
|--------|-----------------------------|-------------------------------------------|
| GET    | `/health`                   | Liveness + DB connectivity                |
| GET    | `/api/v1/status`             | DB health + configured/enabled library counts |
| GET/POST | `/api/v1/libraries`        | List / create libraries (redacted)        |
| GET/PUT/DELETE | `/api/v1/libraries/<id>` | Read / update / delete one library   |
| **POST** | **`/api/v1/libraries/<id>/test`** | **Read-only Sonarr connection test** |
| **POST** | **`/api/v1/libraries/<id>/scan`** | **Read-only Sonarr candidate scan** |
| GET/PUT | `/api/v1/policy`            | Read / update the automation policy       |
| GET    | `/api/v1/activity`           | List activity jobs (filter by `?state=`)  |
| GET    | `/api/v1/activity/<id>`      | Read one activity job                     |
| **GET** | **`/api/v1/activity/<id>/candidates`** | **List an activity job's candidate snapshot** |

Error status codes for `/test` and `/scan`:

| Condition | Status |
|---|---|
| Library not found | 404 |
| Unsupported library type (not `sonarr`) | 400 |
| Library disabled | 400 |
| Library missing an API key | 400 |
| Sonarr unreachable / timeout / bad auth / malformed data | 502 |

All error responses are `{"errors": ["<safe message>"]}` - never the
library's URL or API key (verified by
`test_test_endpoint_timeout_returns_502_without_leaking_secrets` and
`test_scan_error_details_never_contain_secret_api_key`).

### Web UI

Four pages (Overview, Libraries, Activity, Settings), server-rendered
Jinja2 templates with a small vanilla-JS `fetch()` layer
(`app/web/static/app.js`) calling the JSON API above. Every page carries a
persistent banner: *"Managearr v1 - Development Preview. Read-only Sonarr
scan preview: no searches are sent and nothing in Sonarr is changed.
Automated hunting is not active yet."*

- **Libraries** page: each `sonarr`-type row gets **Test** and **Scan**
  buttons and a status badge (Untested / Testing.../ Reachable (vN) /
  Unreachable / N candidate(s) / Scan failed). A dedicated notice repeats
  the read-only guarantee above the table.
- **Activity** page: `sonarr_scan` rows show a **Details** button that
  opens a modal fetching `GET /api/v1/activity/<id>` +
  `GET /api/v1/activity/<id>/candidates` and rendering the full candidate
  table (series, season, episode, air date, reason).

## Running it

### Locally

Requires a reachable PostgreSQL instance (there is no embedded/file
database anymore):

```bash
cd managearr
python3 -m venv .venv
./.venv/bin/pip install -r requirements-dev.txt   # includes runtime deps + pytest
MANAGEARR_DB_HOST=localhost MANAGEARR_DB_PASSWORD=devpassword ./.venv/bin/python run.py   # serves on :9706
```

Config is entirely environment-driven (see `config.py`):

| Variable | Default | Purpose |
|---|---|---|
| `MANAGEARR_HOST` | `0.0.0.0` | Bind host |
| `MANAGEARR_PORT` | `9706` | Bind port |
| `MANAGEARR_DEBUG` | `false` | Flask debug mode |
| `MANAGEARR_SONARR_TIMEOUT_SECONDS` | `10` | Per-request timeout for Sonarr calls |
| `MANAGEARR_DB_HOST` | `postgres` | PostgreSQL host |
| `MANAGEARR_DB_PORT` | `5432` | PostgreSQL port |
| `MANAGEARR_DB_NAME` | `managearr` | PostgreSQL database name |
| `MANAGEARR_DB_USER` | `managearr` | PostgreSQL user |
| `MANAGEARR_DB_PASSWORD_FILE` | _(unset)_ | Path to a file containing the DB password - **preferred**, see [PostgreSQL migration](#postgresql-migration) |
| `MANAGEARR_DB_PASSWORD` | _(unset)_ | Plain-env-var DB password fallback, only used when `*_FILE` is unset - local dev only |
| `MANAGEARR_DB_SSLMODE` | `prefer` | libpq `sslmode` |
| `MANAGEARR_DB_POOL_MIN_SIZE` / `MANAGEARR_DB_POOL_MAX_SIZE` | `1` / `5` | Connection pool sizing |
| `MANAGEARR_DB_CONNECT_TIMEOUT_SECONDS` | `5` | Per-connection-attempt timeout |
| `MANAGEARR_DB_STARTUP_TIMEOUT_SECONDS` | `30` | Bounded total startup retry/backoff window - see `Database.wait_ready()` |

### Tests

Most of the suite is real integration testing against PostgreSQL (not
mocks) - only the Sonarr HTTP boundary is ever faked. Point
`MANAGEARR_TEST_DB_*` at a disposable database (defaults assume
`127.0.0.1:5432/managearr_test`, user `managearr_test`); the suite
truncates its tables before every test, so use a database dedicated to
testing:

```bash
cd managearr
./.venv/bin/pip install -r requirements-dev.txt
MANAGEARR_TEST_DB_PASSWORD=<your test db password> ./.venv/bin/python -m pytest tests/ -v
```

If no PostgreSQL test database is reachable, every DB-backed test is
skipped with a clear reason (pure-logic tests - `test_scan_candidate_rules.py`,
parts of `test_policy.py`/`test_credential_loading.py` - still run).

**123 tests.** Breakdown:
- 40 carried over from the foundation milestone (health/status, redaction,
  policy, library CRUD, persistence), migrated to `/api/v1` and the
  `managearr` naming with no behavior change.
- `test_sonarr_client.py` (14 tests) - adapter behavior against a fake
  `requests.Session` (never real HTTP): success paths, safe URL joining,
  header-only API key (never in the URL), timeout/connection-error
  mapping, 401/403/500 mapping, non-JSON and malformed-shape handling,
  and a message-never-contains-secrets check.
- `test_scan_candidate_rules.py` (11 tests) - the pure candidate-selection
  rule: monitored/unmonitored series and episodes, has-file exclusion,
  future/today/past air dates, malformed/missing air dates, missing IDs.
- `test_sonarr_scan_service.py` (16 tests) - `SonarrScanService` against a
  stub client: connection-test validation branches (not found/wrong
  type/disabled/missing key/upstream error), scan success + persistence,
  unmonitored-series exclusion, job-fails-on-series-error, one bad series
  is skipped without failing the whole scan, no-secret-leakage in job
  `details`, repeat-scan idempotency (two jobs, two independent
  snapshots), and truncation at `MAX_CANDIDATES_PER_SCAN`.
- `test_sonarr_scan_api.py` (12 tests) - the same scenarios through the
  Flask test client and `/api/v1/*` HTTP surface, including a raw-body
  substring check that a timeout error response never contains the
  library's URL or API key.
- `test_migrations.py` (6 tests) - `run_migrations` idempotence,
  `schema_migrations` bookkeeping, expected columns/indexes, and FK
  `ON DELETE` behavior (`SET NULL` / `CASCADE`) read back from
  PostgreSQL's own catalogs (not assumed).
- `test_db_startup.py` (5 tests) - bounded startup retry/backoff against
  an unreachable host (fails well under the configured timeout, never
  hangs) and a static, safe `DatabaseUnavailableError` message that
  never contains the configured host/user/password.
- `test_credential_loading.py` (7 tests) - `config.read_secret()`:
  file-based secret precedence over a plain env var, whitespace
  stripping, missing-file behavior, and the no-secret-configured case.
- `test_sqlite_import.py` (11 tests) - `tools/import_sqlite.py` against a
  real PostgreSQL target: dry-run counts with no writes, no API key ever
  printed (dry-run or real), ID/timestamp preservation, sequence reset
  after import, refusal on a non-empty target, `--allow-nonempty`
  permitting it, idempotent refusal on repeat runs, transactional
  rollback on a forced re-import's primary-key collision, and importing
  a pre-M1 legacy schema missing `job_type`/`candidate_count`.

### Docker

```bash
./scripts/generate-managearr-db-password.sh   # one-time: writes secrets/managearr_db_password.txt
docker compose -f compose.managearr.yml up --build
```

This builds `Dockerfile.managearr` (context: repo root, copies only
`managearr/`), tags the image `managearr:preview`, and starts two
services: `postgres` (PostgreSQL 18, named volume, no published host
port, healthcheck-gated) and `managearr` (binds host port **9706**,
distinct from v1's 9705, waits for `postgres`'s healthcheck via
`depends_on: condition: service_healthy`). Both read the same
password from `secrets/managearr_db_password.txt`
(`POSTGRES_PASSWORD_FILE` / `MANAGEARR_DB_PASSWORD_FILE`) - see
[PostgreSQL migration](#postgresql-migration). **Managearr never mounts
or reads v1's `./data` directory or database.**

> **Docker was not available in this environment** (no `docker` binary on
> PATH), so the image build and container smoke test above were not
> executed here, same as prior milestones. The commands are exact and
> ready to run. In their place: (1) local test coverage (`pytest`, 123
> tests) against a real PostgreSQL 17 instance installed directly in this
> environment (`apt-get install postgresql`) - the same SQL, schema,
> transaction, and pooling code paths the container will run, just not
> inside a container, and against major version 17 rather than the
> pinned `postgres:18.6-alpine` image; and (2) manual `curl`/browser
> smoke tests against the Flask dev server pointed at that same
> PostgreSQL instance (see below). Compose YAML structure (service
> names, healthchecks, `depends_on` condition, secrets, volumes) was
> reviewed but not exercised end-to-end - verify with a real
> `docker compose up --build` in an environment with Docker before
> deploying.

Manual smoke test performed against the dev server (not the container),
pointed at a real local PostgreSQL instance, confirming: `/health` and
`/api/v1/status` return 200 with `schema_version: 1` and no connection
details in the body; creating a `sonarr` library and calling `/test` and
`/scan` against an intentionally unreachable address returns HTTP 502
with a safe `{"errors": [...]}` body containing neither the configured
URL nor API key; the resulting `sonarr_scan` job is visible (state
`failed`) via `/api/v1/activity` and its (empty) candidate list via
`/api/v1/activity/<id>/candidates`; all four UI pages and both static
assets (`/static/style.css`, `/static/app.js`) return 200; `tools/import_sqlite.py`
was run end-to-end (dry-run, real import, refusal on non-empty target,
forced re-import rollback on collision) against the same PostgreSQL
instance - see `tests/test_sqlite_import.py`.

## Current milestone: PostgreSQL migration

Replaces the SQLite persistence layer used through M1 with PostgreSQL,
end to end:

- `psycopg` 3 + `psycopg_pool` connection pool replace `sqlite3`; every
  repository query is parameterized, uses dict rows, and runs inside a
  short transaction (commit on success / rollback on error) via
  `Database.connect()` - see [Persistence](#persistence).
- Deterministic, transactional schema migrations
  (`app/persistence/migrations.py`) replace the previous ad-hoc
  `CREATE TABLE IF NOT EXISTS` + manual `ALTER TABLE` dance.
- `compose.managearr.yml` is now a two-service stack (`managearr` +
  `postgres`) with file-based credentials, a healthcheck-gated
  `depends_on`, a named PostgreSQL volume with no published host port,
  and healthchecks on both services - see [Docker](#docker).
- Bounded startup retry/backoff (`Database.wait_ready`) and a safe,
  static `DatabaseUnavailableError` if PostgreSQL never becomes
  reachable in time; `/health`/`/api/v1/status` report DB connectivity
  and schema version without ever revealing host/user/password.
- A new, explicit, idempotent one-time importer
  (`tools/import_sqlite.py`) replaces the old manual `sqlite3 ALTER
  TABLE` carry-forward steps - see
  [One-time SQLite → PostgreSQL import](#one-time-sqlite--postgresql-import).
- API behavior, IDs, constraints, and cascade/set-null/redaction
  behavior are unchanged - the pre-existing behavioral suite passes
  against PostgreSQL with no logic changes (only its two SQLite-mechanics
  tests were swapped for equivalent PostgreSQL FK-behavior tests, and two
  schema-version assertions were added to the health/status tests), plus
  29 new tests in four new files (`test_migrations.py`,
  `test_db_startup.py`, `test_credential_loading.py`,
  `test_sqlite_import.py`) covering migrations, startup failure handling,
  credential-file loading, and the importer - 123 tests total, see
  [Tests](#tests).

## Previous milestone (M1)

Adds to the foundation milestone:
- Read-only Sonarr v3 adapter (`SonarrClient`) - connection test, series
  list, episode list. No command endpoint is ever called.
- Candidate derivation rule: monitored series + monitored episode + no
  file + aired (today or past) = candidate. Pure function, unit-tested
  independent of any HTTP call.
- `SonarrScanService`: connection test endpoint + manual scan orchestration,
  with per-series error isolation and a hard cap on candidates per scan.
- Durable `sonarr_scan` activity jobs (`searching -> completed`/`failed`)
  and an immutable `scan_candidates` snapshot table, both queryable via
  the read-only API.
- Idempotent, non-destructive repeat scans - a new job every time, never
  overwriting a prior scan's rows.
- Validation + error handling for: unsupported library type, disabled
  library, missing API key, timeout, unreachable host, and malformed
  Sonarr data - each with its own exception type and a safe, secret-free
  message.
- Library and Activity UI updated with Test/Scan controls, status badges,
  and a candidate detail view, plus a prominent "read-only, no searches
  sent" statement on every page.
- 53 new tests (93 total), all mocking network access - no test in this
  suite makes a real HTTP call.

## Non-goals / current limitations

- **No hunting, still.** Nothing in this milestone sends a search,
  grabs a release, or imports a file. `SonarrClient` cannot call
  `POST /api/v3/command` - the method doesn't exist on the class.
- **Sonarr only.** Radarr/Lidarr/Readarr/Whisparr/Eros libraries can still
  be configured (CRUD is unchanged) but Test/Scan return 400
  (`unsupported library type`) for anything other than `sonarr`.
- **Manual scans only.** No background scheduler, worker process, or
  queue triggers a scan - it only runs when a user clicks Scan or calls
  the API.
- No authentication/authorization on the Managearr API or UI (matches the
  scope of this preview; v1's auth is untouched and unaffected).
- A scan is capped at `MAX_CANDIDATES_PER_SCAN` (500) candidate rows; very
  large libraries will see a "results truncated" note in the job details
  rather than every candidate.
- If fetching episodes for one series fails mid-scan, that series is
  silently skipped (noted in the job's `details`) rather than failing the
  whole scan - by design, but it means a scan can under-report candidates
  for a library with one broken series.
- Docker image build/run was not exercised in this environment (no Docker
  available); commands above are ready but unverified end-to-end - see
  the [Docker](#docker) section.
- The one-time SQLite importer (`tools/import_sqlite.py`) must be run
  explicitly - nothing starts it automatically, and it refuses to touch
  a non-empty PostgreSQL target unless told to.

## Migration safety

- `managearr/` is fully standalone: separate dependencies
  (`managearr/requirements.txt`), separate database (PostgreSQL, its own
  named Compose volume, never v1's `data/`), separate Docker image(s)
  (`managearr:preview` via `Dockerfile.managearr`/`compose.managearr.yml`),
  separate port (9706 vs v1's 9705), separate container/service names
  (`managearr`, `managearr-postgres`).
- No file under `main.py` or `src/` was modified for this or any prior
  milestone.
- Managearr never opens, reads, or writes v1's `./data/config` directory
  or `huntarr.db` file - there is no shared state between v1 and
  Managearr, by design, so both can run side by side safely during
  evaluation.
- **No automatic access to the v1 Huntarr database, or to any prior
  Managearr SQLite preview database, exists anywhere in the running
  application** - the only way old data ever reaches the new PostgreSQL
  database is the explicit, one-time importer below.

## PostgreSQL migration

### Credentials

PostgreSQL is a private, no-public-port service on the Compose network;
Managearr reaches it only as `postgres:5432`. Neither service ever gets
a hardcoded or default password - both read the same generated secret
file via `POSTGRES_PASSWORD_FILE` / `MANAGEARR_DB_PASSWORD_FILE`:

```bash
./scripts/generate-managearr-db-password.sh   # writes secrets/managearr_db_password.txt, never prints it
```

The file is `chmod 600`, gitignored (`secrets/*` except `secrets/README.md`
- see `.gitignore`), and mounted read-only into both containers by
Compose's native `secrets:` block. See `secrets/README.md` for details
and for what to do if the file is lost after the PostgreSQL volume has
already initialized.

### Schema migrations

`app/persistence/migrations.py` applies a fixed, ordered list of
transactional migrations on every startup (app, and
`tools/import_sqlite.py`), tracked in a `schema_migrations` table.
Re-running is always a safe no-op once the schema is current - see
[Persistence](#persistence) above.

### One-time SQLite → PostgreSQL import

If you have data in a prior Managearr SQLite preview database
(`./data-managearr/managearr.db`) that you want to carry forward instead
of re-configuring libraries by hand, use `tools/import_sqlite.py`. It:

- reads the SQLite file **read-only** (opened with SQLite's own
  `mode=ro` URI flag - a write attempt against the source fails
  immediately, see `test_read_source_never_writes_to_sqlite_file`);
- **refuses to write into a non-empty PostgreSQL target** unless you pass
  `--allow-nonempty` - safe by default against accidental double-imports;
- preserves every row's original `id` and timestamps exactly, including
  `arr_libraries`, `automation_policy`, `activity_jobs`, and
  `scan_candidates`, then resets PostgreSQL's identity sequences so the
  next app-created row continues after the highest imported id (never
  collides with imported data);
- runs the entire import as **one PostgreSQL transaction** - any error
  (e.g. an id collision on a forced re-import) rolls back everything the
  run wrote, leaving the target exactly as it was before
  (`test_forced_reimport_id_collision_rolls_back_transactionally`);
- supports `--dry-run`, which reads the source and reports row counts
  per table without writing anything to PostgreSQL;
- **never prints an API key**, in `--dry-run` or normal mode - only row
  counts and non-secret identifiers are logged
  (`test_dry_run_never_prints_api_key`, `test_real_import_never_prints_api_key`);
- transparently defaults `job_type`/`candidate_count` for a pre-M1
  source database that predates those columns, and tolerates a source
  with no `scan_candidates` table at all.

**Usage** (locally):

```bash
cd managearr
./.venv/bin/python tools/import_sqlite.py --sqlite-path ../data-managearr/managearr.db --dry-run
./.venv/bin/python tools/import_sqlite.py --sqlite-path ../data-managearr/managearr.db
```

**Usage** (as a one-shot Compose command, `tools` profile - see
`compose.managearr.yml`):

```bash
docker compose -f compose.managearr.yml --profile tools run --rm \
  managearr-import --sqlite-path /legacy/managearr.db --dry-run
docker compose -f compose.managearr.yml --profile tools run --rm \
  managearr-import --sqlite-path /legacy/managearr.db
```

(`managearr-import` mounts `./data-managearr` read-only at `/legacy` -
put the SQLite file there, or adjust the volume mount.)

**Rollback** (undoing a completed import):

1. Preferred: restore the `managearr-postgres-data` Compose volume from a
   snapshot/backup taken before the import.
2. Manual (irreversible - only if certain no writes have happened in
   PostgreSQL since the import completed): connect to the target
   database and run
   ```sql
   TRUNCATE arr_libraries, automation_policy, activity_jobs,
       scan_candidates RESTART IDENTITY CASCADE;
   ```
   Schema migrations never need to be rolled back for this - the
   `schema_migrations` table is untouched by the importer, and
   re-running migrations on next startup is always a safe no-op.

A **failed** run never needs rollback at all: because the import is one
transaction, PostgreSQL has already discarded every row a failed run
attempted to write by the time the process exits.

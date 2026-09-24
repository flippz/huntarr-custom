# Managearr v1 (Development Preview)

This document describes the `managearr/` application: a standalone rewrite
that lives alongside the legacy Huntarr v1 app (`main.py`, `src/`) without
touching it. v1 continues to run exactly as before - Managearr has its own
dependencies, its own database, its own Docker image, and its own port.

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
    persistence/    SQLite schema + repositories (stdlib sqlite3, no ORM)
      database.py, library_repository.py, policy_repository.py,
      activity_repository.py, scan_candidate_repository.py
    api/            JSON API blueprint, mounted at /api/v1
    web/            server-rendered UI shell (Jinja2 + vanilla JS/CSS, no build step)
  config.py         env-driven runtime config
  run.py            entry point (Flask dev server)
  requirements.txt  runtime deps (Flask + CVE-pinned Werkzeug/Jinja2 + requests)
  requirements-dev.txt  adds pytest
  tests/            pytest suite (unit + Flask test-client integration, all mocked)
```

Data flows one direction: `web`/`api` call into `services`, `services` call
into `persistence`/`adapters` and use `domain` for validation, `adapters` sit
at the HTTP/Sonarr boundary to redact secrets and to wrap the Sonarr HTTP API.
Nothing in `domain` or `persistence` knows about Flask.

### Domain model

- **ArrLibrary** - a configured connection to one of six *Arr ecosystems:
  `sonarr`, `radarr`, `lidarr`, `readarr`, `whisparr`, `eros`. Fields:
  `name`, `type`, `url`, `api_key`, `enabled`. Persisted in SQLite via
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

#### Idempotent snapshots

Every call to `run_scan` creates a brand-new `activity_jobs` row and a
fresh batch of `scan_candidates` rows tied to that row's `id`
(`ON DELETE CASCADE`, but nothing ever deletes a job in this milestone).
Repeated scans of the same library never update or delete a prior scan's
rows - each scan is an independent, permanent snapshot. See
`test_repeat_scans_create_separate_jobs_without_corrupting_prior_snapshot`
in `tests/test_sonarr_scan_service.py`.

### Persistence

Plain `sqlite3` (stdlib), one short-lived connection per operation, schema
created idempotently via `CREATE TABLE IF NOT EXISTS` on startup
(`Database.init_schema`). `activity_jobs` gained `job_type` and
`candidate_count` columns (both `NOT NULL DEFAULT`, so old-shaped inserts
still work); a new `scan_candidates` table was added, foreign-keyed to
`activity_jobs(id)` (cascade delete) and `arr_libraries(id)` (set null).

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

```bash
cd managearr
python3 -m venv .venv
./.venv/bin/pip install -r requirements-dev.txt   # includes runtime deps + pytest
./.venv/bin/python run.py                          # serves on :9706
```

Config is entirely environment-driven (see `config.py`):

| Variable | Default | Purpose |
|---|---|---|
| `MANAGEARR_DB_PATH` | `data-managearr/managearr.db` | SQLite file path |
| `MANAGEARR_HOST` | `0.0.0.0` | Bind host |
| `MANAGEARR_PORT` | `9706` | Bind port |
| `MANAGEARR_DEBUG` | `false` | Flask debug mode |
| `MANAGEARR_SONARR_TIMEOUT_SECONDS` | `10` | Per-request timeout for Sonarr calls |

### Tests

```bash
cd managearr
./.venv/bin/python -m pytest tests/ -v
```

**93 tests, all passing.** Breakdown:
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

### Docker

```bash
docker compose -f compose.managearr.yml up --build
```

This builds `Dockerfile.managearr` (context: repo root, copies only
`managearr/`), tags the image `managearr:preview`, runs container
`managearr`, and binds host port **9706** (distinct from v1's 9705). The
SQLite database lives at `./data-managearr/managearr.db` on the host
(distinct from v1's `./data/config`) - **Managearr never mounts or reads
v1's `./data` directory or database.**

> **Docker was not available in this environment** (no `docker` binary on
> PATH), so the image build and container smoke test above were not
> executed here, same as the foundation milestone. The commands are exact
> and ready to run; local test coverage (`pytest`, 93 tests) and manual
> `curl`/browser smoke tests against the Flask dev server (see below) were
> used instead to verify behavior.

Manual smoke test performed against the dev server (not the container),
confirming: `/health` and `/api/v1/status` return 200; creating a `sonarr`
library and calling `/test` and `/scan` against an intentionally
unreachable address returns HTTP 502 with a safe `{"errors": [...]}` body
containing neither the configured URL nor API key; the resulting
`sonarr_scan` job is visible (state `failed`) via `/api/v1/activity` and
its (empty) candidate list via `/api/v1/activity/<id>/candidates`; all four
UI pages and both static assets (`/static/style.css`, `/static/app.js`)
return 200.

## Current milestone (M1)

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
- No migration tooling beyond idempotent `CREATE TABLE IF NOT EXISTS`.
- Docker image build/run was not exercised in this environment (no Docker
  available); commands above are ready but unverified end-to-end.

## Migration safety

- `managearr/` is fully standalone: separate dependencies
  (`managearr/requirements.txt`), separate database (`data-managearr/`,
  never `data/`), separate Docker image (`managearr:preview` via
  `Dockerfile.managearr`/`compose.managearr.yml`), separate port (9706 vs
  v1's 9705), separate container/service name (`managearr`).
- No file under `main.py` or `src/` was modified for this or the prior
  milestone.
- Managearr never opens, reads, or writes v1's `./data/config` directory
  or `huntarr.db` file - there is no shared state between v1 and
  Managearr, by design, so both can run side by side safely during
  evaluation.
- **No automatic access to the v1 Huntarr database exists anywhere in this
  codebase** - the only way v1 data ever reaches Managearr is the manual,
  one-time copy step below.

### Carrying a tested preview library forward

If you've been running the (pre-Sonarr-scan) foundation preview and want
to keep the one Sonarr library you configured there instead of re-typing
it, copy the SQLite file forward **once**, manually, then run one schema
fixup before starting the new version. `init_schema()` uses
`CREATE TABLE IF NOT EXISTS`, which does **not** add new columns to a
table that already exists - `arr_libraries` and `automation_policy` are
unchanged and open cleanly as-is, but the old `activity_jobs` table
predates the `job_type`/`candidate_count` columns this milestone adds, so
it needs one manual `ALTER TABLE` (verified against a real old-shaped
database while writing this milestone - starting the app against a
copied-forward file without this step fails with
`sqlite3.OperationalError: table activity_jobs has no column named job_type`
the first time a scan runs):

```bash
# Stop any running Managearr container/process first.
cp ./data-v2/huntarr_v2.db ./data-managearr/managearr.db   # one-time copy from the old preview

# activity_jobs was never written to in the foundation milestone (no seed
# data, no write path) - confirm it's empty, then add the two new columns:
sqlite3 ./data-managearr/managearr.db "SELECT COUNT(*) FROM activity_jobs;"   # expect 0
sqlite3 ./data-managearr/managearr.db "ALTER TABLE activity_jobs ADD COLUMN job_type TEXT NOT NULL DEFAULT 'legacy';"
sqlite3 ./data-managearr/managearr.db "ALTER TABLE activity_jobs ADD COLUMN candidate_count INTEGER NOT NULL DEFAULT 0;"

# Now start the new version - init_schema() will add the new
# scan_candidates table on first run (CREATE TABLE IF NOT EXISTS):
cd managearr && ./.venv/bin/python run.py
# or: docker compose -f compose.managearr.yml up --build
```

If the `SELECT COUNT(*)` above returns anything other than `0` (it
shouldn't, by design), stop and inspect those rows before proceeding - the
`ALTER TABLE` statements themselves are safe either way (they only add
columns with defaults), but this milestone's scan feature assumes
`activity_jobs` starts from a known-empty state for the `sonarr_scan`
job_type.

This is a **one-time, manual, explicit copy plus a two-line schema
fixup** - nothing in the application ever reaches into a v1 or
prior-preview database path on its own.

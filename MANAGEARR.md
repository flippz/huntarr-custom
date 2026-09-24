# Managearr v1 (Development Preview)

Managearr is a standalone Flask/PostgreSQL rewrite beside the legacy Huntarr
application (`main.py`, `src/`). It has its own image, database, port, API, and
UI. Nothing in this application changes the legacy runtime.

## Current milestone: controlled manual Sonarr dispatch

A completed Sonarr candidate scan can now be previewed and, only after an
explicit confirmation, sent to Sonarr as one `EpisodeSearch` command. This is
not automated hunting: no scheduler or background worker dispatches searches.

Safety properties:

- scans and previews do not call a Sonarr write endpoint;
- the manual endpoint requires a non-empty `candidate_ids` list and the JSON
  boolean `confirm: true` (strings and truthy values are rejected);
- each request accepts at most 25 candidate IDs and deduplicates both candidate
  IDs and Sonarr episode IDs;
- the candidate must belong to the completed `sonarr_scan` job and its library;
- enabled/type/API-key readiness, cooldown, live reservations, and hourly
  capacity are checked again immediately before reservation;
- reservation is serialized per library with a PostgreSQL advisory transaction
  lock and committed before any network request;
- the Sonarr request runs with no database transaction or lock held;
- success/failure finalization uses a fresh transaction and the same library
  lock, updating the batch and reserved items atomically;
- failed attempts remain audited but release their reservation for retry;
- dry-run rows never count as a dispatch or start cooldown;
- URLs and API keys are never returned in dispatch errors or audit rows.

## Architecture

```
managearr/
  app/
    domain/          dataclasses and pure validation/rules
      dispatch.py    dispatch batch/item model and limits
    adapters/
      sonarr_client.py   Sonarr v3 GETs plus the single allowed POST
      redaction.py
    persistence/
      database.py
      migrations.py      PostgreSQL schema v1 + dispatch-ledger v2
      dispatch_repository.py
      *_repository.py
    services/
      sonarr_scan_service.py
      dispatch_planning_service.py
      dispatch_service.py
      library_readiness.py
    api/routes.py
    web/             Jinja templates and vanilla JS/CSS
  tools/import_sqlite.py
  tests/
```

The API/web layer calls services; services coordinate repositories and the
Sonarr adapter; domain code has no I/O. PostgreSQL is the only runtime database.
Only tests replace the Sonarr HTTP boundary.

## Dispatch model and planning

A `dispatch_batches` row records one preview (`dry_run`) or confirmed attempt
(`manual`). `dispatch_batch_items` snapshots each valid candidate considered by
that attempt. Selected preview items are `planned`; valid candidates rejected by
cooldown, in-flight work, duplicate episode, or capacity are `excluded` with a
reason. Confirmed selected items move `reserved -> dispatched|failed`.

Unknown IDs and candidates owned by another job/library are explained in the
plan response but are not inserted as item rows: there is no valid candidate FK
that an audit item could reference.

Planning applies, in order:

1. job exists, is a completed `sonarr_scan`, and still has a library;
2. library exists, is enabled Sonarr, and has an API key;
3. request shape is a list of 1-25 integer IDs;
4. candidate ownership and candidate/episode deduplication;
5. cooldown from **dispatched items on completed/partial manual batches**;
6. exclusion of another live reservation for the same episode;
7. remaining hourly capacity, counting both completed dispatches in the rolling
   hour and all non-stale live reservations.

The preview persists its plan for audit but makes no Sonarr request. Confirmed
dispatch repeats all planning under the per-library advisory lock; a prior
preview is informative, never trusted as authorization or a reservation.
Reservations older than five minutes are marked failed the next time a dispatch
for that library acquires the lock.

### Audit durability and immutability

Migration v2 adds constrained modes/states/counts, foreign keys, unique item
ownership per batch, and indexes for job/library/state/episode/time lookups.
Database triggers reject deletes, identity-field edits, and invalid state
transitions. Only lifecycle result fields may advance:

- batch: `dispatching -> completed|partial|failed`;
- item: `reserved -> dispatched|failed`.

Dry-run and excluded rows are terminal. Test cleanup uses `TRUNCATE` against a
dedicated test database; the application exposes no audit-delete path.

Migrations run under a PostgreSQL advisory transaction lock. Bootstrap,
pending DDL, and `schema_migrations` rows commit together, so concurrent starts
serialize and any failed pending migration rolls back its DDL and bookkeeping.
Rerunning an up-to-date schema is a no-op.

## Sonarr adapter

Read-only methods remain:

- `GET /api/v3/system/status`
- `GET /api/v3/series`
- `GET /api/v3/episode?seriesId=<id>`

There is exactly one write method:

```http
POST /api/v3/command
X-Api-Key: <header only>
Content-Type: application/json

{"name":"EpisodeSearch","episodeIds":[...]}
```

`search_episodes()` validates 1-25 unique positive integer IDs before sending
and validates the returned positive command ID, command name, and status shape.
Timeouts, connection failures, auth errors, non-2xx responses, and malformed
JSON/command responses map to static safe exceptions that contain neither the
base URL nor API key.

## API

All JSON error responses use `{"errors":["safe message"]}`.

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Liveness, PostgreSQL health, schema version |
| GET | `/api/v1/status` | Database and library summary |
| GET/POST | `/api/v1/libraries` | Redacted library CRUD collection |
| GET/PUT/PATCH/DELETE | `/api/v1/libraries/<id>` | Redacted library CRUD item |
| POST | `/api/v1/libraries/<id>/test` | Read-only Sonarr test |
| POST | `/api/v1/libraries/<id>/scan` | Read-only candidate scan |
| GET/PUT/PATCH | `/api/v1/policy` | Automation policy |
| GET | `/api/v1/activity` | Activity list |
| GET | `/api/v1/activity/<job_id>` | Activity detail |
| GET | `/api/v1/activity/<job_id>/candidates` | Candidate snapshot |
| **POST** | **`/api/v1/activity/<job_id>/dispatch/preview`** | **Persist dry-run audit; never POST to Sonarr** |
| **POST** | **`/api/v1/activity/<job_id>/dispatch`** | **Confirmed manual dispatch** |
| GET | `/api/v1/activity/<job_id>/dispatch-batches` | Audit summaries |
| GET | `/api/v1/dispatch-batches/<batch_id>` | Audit detail with items |

Preview request:

```json
{"candidate_ids": [12, 13]}
```

The `201` response contains both `plan` (selected candidates, explained
exclusions, cap/cooldown values) and the persisted dry-run `batch`.

Confirmed request:

```json
{"candidate_ids": [12, 13], "confirm": true}
```

The `201` response contains the immediate re-plan and final manual batch. A
Sonarr rejection or timeout is represented by a `failed` batch with a safe
`error_summary`; it remains a successfully recorded API attempt rather than
losing the audit in an opaque 5xx response.

## Web UI

The Activity scan-detail modal now provides:

- candidate checkboxes with a visible 25-item maximum;
- **Preview selection**, which writes only a dry-run audit;
- selected/excluded counts and exclusion reasons;
- **Clear preview**, a local UI action that makes no API call;
- an explicit warning checkbox;
- a disabled-until-confirmed red **Send searches to Sonarr** button; and
- a dispatch audit table showing previews and manual attempts.

Changing candidate selection clears the preview and confirmation. The server
still re-plans after confirmation, so stale UI state cannot bypass policy.

## Running locally

A reachable PostgreSQL instance is required:

```bash
cd managearr
python3 -m venv .venv
./.venv/bin/pip install -r requirements-dev.txt
MANAGEARR_DB_HOST=localhost MANAGEARR_DB_PASSWORD=devpassword ./.venv/bin/python run.py
```

Managearr defaults to port `9706`. Configuration remains environment-driven;
important variables include `MANAGEARR_SONARR_TIMEOUT_SECONDS`,
`MANAGEARR_DB_HOST`, `MANAGEARR_DB_PORT`, `MANAGEARR_DB_NAME`,
`MANAGEARR_DB_USER`, `MANAGEARR_DB_PASSWORD_FILE` (preferred),
`MANAGEARR_DB_PASSWORD`, `MANAGEARR_DB_SSLMODE`, pool sizes, and bounded DB
connect/startup timeouts.

### Docker Compose

```bash
./scripts/generate-managearr-db-password.sh
docker compose -f compose.managearr.yml up --build
```

The stack uses separate `managearr` and PostgreSQL services, a named database
volume, healthcheck-gated startup, and a shared file-based password. PostgreSQL
is not published to the host. Managearr does not mount the legacy data folder.

### One-time SQLite import

`managearr/tools/import_sqlite.py` remains the explicit migration tool for an
earlier preview database. It supports dry-run reporting, refuses a non-empty
target unless explicitly overridden, preserves IDs/timestamps, resets
sequences, redacts API keys, and runs writes transactionally. It does not import
or fabricate dispatch audit rows.

## Tests

Tests use a real disposable PostgreSQL database for migrations, constraints,
FKs, transaction behavior, planning, audit, and concurrency. Sonarr is always a
mocked session/client; the suite never calls a live Sonarr instance.

```bash
MANAGEARR_TEST_DB_PASSWORD=<test password> \
  managearr/.venv/bin/python -m pytest managearr/tests -q

# Legacy suite, from an environment with the root requirements installed
# (kept separate because scripts/test_routes_http.py is an executable smoke
# script that pytest must not collect):
python3 -m pytest tests -q
```

Coverage includes migration v2 idempotence/rollback/constraints/immutability,
candidate ownership/dedupe/max selection, cross-job/library rejection, cap,
cooldown, dry-run isolation, exact confirmation, success/failure/retry audit,
concurrent duplicate and cap-overbooking prevention, response redaction, the
single Sonarr POST shape, API contracts, and UI confirmation wiring.

## Current limitations

- Sonarr only; other Arr types remain CRUD-only.
- Dispatch is manual only; no scheduler, worker, automatic search, grab,
  download, or import pipeline exists.
- Managearr has no authentication/authorization yet; protect the service at the
  network/reverse-proxy layer.
- A process crash after Sonarr accepts a command but before local finalization
  is inherently ambiguous because Sonarr's command API offers no idempotency
  key. The reservation remains visible and blocks retry for five minutes, then
  is failed for operator-visible audit; an operator should inspect Sonarr before
  retrying that rare case.
- Candidate scans remain capped at 500 rows and skip an individual series when
  its episode fetch fails, recording that fact in activity details.

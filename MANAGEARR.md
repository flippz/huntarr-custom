# Managearr v1 (Development Preview)

Managearr is a standalone Flask/PostgreSQL rewrite beside the legacy Huntarr
application (`main.py`, `src/`). It has its own image, database, port, API, and
UI. Nothing in this application changes the legacy runtime.

## Current milestone: restart-safe simulation scheduler (M4)

M4 adds a dedicated scheduler worker and durable PostgreSQL planning ledger.
It is **simulation-only**: production defaults to scheduler mode `off`, the only
other accepted mode is `simulate`, and there is no `live` enum value, API
option, UI option, or worker path. **Simulation sends no Sonarr commands.**
The earlier explicitly confirmed manual dispatch endpoint remains available,
but the scheduler neither calls it nor imports its service.

### Architecture and process isolation

`managearr-worker` is a second Compose service using the same image and
PostgreSQL database as the web app. It runs `python -m app.worker`, publishes no
port, and disables the image's HTTP healthcheck. Waitress runs only the Flask
web process; it has no scheduler thread. The worker module does not import
`SonarrClient` or `DispatchService`.

Migration v4 adds:

- singleton scheduler settings constrained to `off|simulate`, with an automatic
  append-only mode-change audit;
- one expiring `cycle-worker` lease using PostgreSQL server time;
- idempotent durable manual simulation requests and atomic claims;
- queued/running/terminal cycle runs with trigger, safe policy snapshot, random
  seed, timestamps, counts, and static safe summaries;
- per-library results tied to the exact completed scan snapshot; and
- immutable per-candidate snapshots recording deterministic position or an
  explicit exclusion reason.

Cycle and request triggers permit only controlled forward transitions. Mode,
library, and candidate audit rows are protected from update/delete. Foreign
keys, count/length checks, partial uniqueness, and query indexes protect the
ledger. It stores normalized IDs, policy values, bounded snapshots, and static
summaries--never library URLs, API keys, exception strings, or raw Sonarr
payloads.

### Operational behavior and restart safety

The worker acquires and heartbeats a database lease before claiming work. A
second worker remains idle until expiry; takeover uses an atomic conditional
update. It sleeps outside transactions and applies bounded exponential backoff
after database errors. SIGTERM stops new work, releases the lease when still
owned, and exits cleanly.

A takeover marks a previous owner's `running` cycle failed with an interrupted,
not-retried summary. Claimed work that never started remains one durable queued
cycle and may be started once by the new owner. Work that did start is never
automatically retried. Scheduled due time is stored in `scheduler_settings` and
advanced atomically with insertion of exactly one scheduled cycle, so restart
does not silently duplicate a due cycle. Mode `off` creates no scheduled work;
an explicit `POST /api/v1/scheduler/run-simulation-now` request may still run a
simulation while off. Concurrent/repeated run-now calls coalesce to the one
queued/claimed request and return promptly.

Enabling `simulate` sets the first due time to current database time plus the
existing `cycle_interval_minutes`; later due cycles advance it the same way.
Disabling scheduling clears next due. Lease staleness is shown in the UI but
does not make the web app health endpoint fail.

### Simulation planning rules

For each enabled Sonarr library, a cycle reads only its latest completed
`sonarr_scan` activity/candidate snapshot. It never launches a scan. Candidate
ordering is deterministic:

- `sequential`: case-insensitive series title, season, episode, candidate ID;
- `oldest_first` / `newest_first`: ISO air date with deterministic tie breaks;
- `random`: a stored cycle seed combined with library ID, making the shuffle
  reproducible from the audit row.

Missing candidates are excluded when `missing_enabled` is false. Every cycle
explicitly records upgrades as `unsupported` when `upgrades_enabled` is true
(or `disabled` otherwise); M4 never pretends to plan upgrades.

The planner conservatively reads the durable manual dispatch/outcome ledger to
exclude duplicate episodes, imported outcomes, live reservations, and episodes
inside cooldown. It calculates each library's effective selection cap as:

```
min(25, hourly capacity remaining, queue slots remaining,
    successful-grab target remaining in the policy cycle window)
```

`25` is the existing `MAX_SELECTION_PER_REQUEST` Sonarr command safety maximum.
Hourly usage includes completed dispatches and live reservations. Queue
occupancy treats dispatched items without terminal release evidence as active.
Recent distinct `grabbed`/`imported` evidence reduces the successful-grab
allowance. Candidates beyond the effective cap receive a cap/queue/target
explanation. The selected rows are only “would dispatch” audit records:
**no dispatch batch, reservation, command, retry, or reconcile is created.**

Libraries explain no-scan, policy-disabled, cap, queue, cooldown, in-flight,
already-imported, duplicate, and safe planning-failure skips. Cycle summaries
contain counts only and never include exception text.

### Scheduler API and UI

| Method | Path | Purpose |
|---|---|---|
| GET/PATCH | `/api/v1/scheduler/settings` | Read settings/health or change mode to `off|simulate` |
| POST | `/api/v1/scheduler/run-simulation-now` | Idempotently queue one manual simulation |
| GET | `/api/v1/scheduler/cycles` | Recent cycle summaries |
| GET | `/api/v1/scheduler/cycles/<id>` | Per-library/candidate audit detail |

`GET/PATCH /api/v1/scheduler` is also supported as a concise settings alias.
Overview and Settings display mode, next due, worker lease health, and latest
cycle, with prominent **Simulation sends no Sonarr commands** text. Enabling
simulate requires a warning checkbox; no live option exists.

### Safety proof and limitations

The scheduler path is proven by real-PostgreSQL tests covering v4 migration
idempotence/transaction rollback/constraints, lease exclusivity and takeover,
heartbeat, off-mode inactivity, due-cycle uniqueness, manual-off execution,
concurrent request/claim idempotence, interrupted-run terminalization,
deterministic ordering/caps/cooldown/in-flight/queue/outcome rules, API/UI
wiring and redaction, and Compose process isolation. Tests monkeypatch every
Sonarr adapter method plus `DispatchService.dispatch` to fail if called during
a simulation.

M4 limitations are intentional: no scheduler auto-scan, no live scheduler
dispatch, no automatic retry, no automatic reconciliation, and no upgrade
planning. A current completed scan must already exist. Queue/success knowledge
is conservative and limited to durable Managearr dispatch/outcome evidence; the
worker does not query live Sonarr state.

### Deployment

Build/start both application services after PostgreSQL is healthy:

```bash
docker compose -f compose.managearr.yml up -d --build managearr managearr-worker
```

After migration, scheduling remains off until an operator explicitly enables
simulate in Settings or via the PATCH API. Existing M3 manual dispatch remains
operator-confirmed and separate from the worker.

## Previous milestone: manual Sonarr outcome reconciliation (M3)

A completed Sonarr candidate scan can be previewed and, only after an explicit
confirmation, sent to Sonarr as one `EpisodeSearch` command. After acceptance,
an operator can manually run read-only reconciliation. The M4 scheduler never
uses either path.

M3 safety properties:

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
- an accepted command ID is committed before local finalization, narrowing the
  crash ambiguity window and allowing later read-only reconciliation;
- failed attempts remain audited but release their reservation for retry;
- dry-run rows never count as a dispatch or start cooldown;
- URLs and API keys are never returned in dispatch errors or audit rows.

Reconciliation safety properties:

- only an existing `manual` dispatch in `completed`, `partial`, or crash-
  `ambiguous` state with a recorded positive Sonarr command ID is eligible;
- dry-runs, ordinary failures, missing-command rows, disabled/wrong-type/
  missing-key libraries, and cross-library ownership ambiguity are rejected
  before any Sonarr request;
- it uses GET only: command status, at most three 50-row history pages per
  episode (with Sonarr's episode filter), and at most three 100-row queue-detail
  pages; dated evidence older than the dispatch is excluded;
- database transactions are not held across those network reads;
- only bounded, normalized identifiers/states and static safe summaries are
  stored--never API keys, configured URLs, arbitrary upstream messages, or raw
  response bodies;
- evidence is deduplicated by a stable batch/source/evidence key while every
  operator reconciliation attempt remains append-only audit history; and
- `command_completed` means only that the search command finished. It never
  fabricates a grab, download, or import outcome.

## Architecture

```
managearr/
  app/
    domain/          dataclasses and pure validation/rules
      dispatch.py    dispatch batch/item model and limits
    adapters/
      sonarr_client.py   bounded Sonarr v3 GETs plus the single allowed POST
      redaction.py
    persistence/
      database.py
      migrations.py      PostgreSQL schema v1 + dispatch v2 + outcomes v3 + scheduler v4
      dispatch_repository.py
      scheduler_repository.py
      outcome_repository.py
      *_repository.py
    services/
      sonarr_scan_service.py
      dispatch_planning_service.py
      dispatch_service.py
      reconciliation_service.py
      scheduler_service.py    simulation-only planner
      library_readiness.py
    worker.py              dedicated lease-owning process
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

Migration v3 adds dedicated reconciliation summary columns plus two append-only
tables. `dispatch_reconciliation_attempts` records when each operator read ran,
which endpoints completed, its explicit `resolved`/`partial`/`unresolved`/
`error` result, and how many new evidence rows were inserted.
`dispatch_outcome_events` records the source endpoint, normalized event type,
terminal/nonterminal/unknown classification, safe summary, command/history/
download identifiers when present, and the dispatch-item/candidate/episode
mapping. Length/check/FK constraints bound the data, a unique evidence key
makes unchanged reruns idempotent, and triggers reject updates/deletes.

The original dispatch identity remains immutable. Reconciliation only updates
its dedicated state/summary/time and observed-command-state columns. A resolved
summary cannot regress. Dispatch lifecycle triggers also allow the narrow
`dispatching -> ambiguous` crash transition while preserving any command ID.

Migrations run under a PostgreSQL advisory transaction lock. Bootstrap,
pending DDL, and `schema_migrations` rows commit together, so concurrent starts
serialize and any failed pending migration rolls back its DDL and bookkeeping.
Rerunning an up-to-date schema is a no-op.

## Sonarr adapter

Read-only methods remain:

- `GET /api/v3/system/status`
- `GET /api/v3/series`
- `GET /api/v3/episode?seriesId=<id>`
- `GET /api/v3/command/<known-command-id>`
- `GET /api/v3/history` with validated page/page-size and optional episode ID
- `GET /api/v3/queue/details` with validated page/page-size

There is exactly one write method:

```http
POST /api/v3/command
X-Api-Key: <header only>
Content-Type: application/json

{"name":"EpisodeSearch","episodeIds":[...]}
```

The reconciliation reads validate endpoint shape and return only whitelisted,
bounded fields. `search_episodes()` validates 1-25 unique positive integer IDs
before sending and validates the returned positive command ID, command name,
and status shape.
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
| **POST** | **`/api/v1/dispatch-batches/<batch_id>/reconcile`** | **Manual GET-only Sonarr reconciliation; sends no search** |
| GET | `/api/v1/dispatch-batches/<batch_id>/outcomes` | Attempts, batch evidence, and per-candidate timelines |

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

The reconciliation response includes the refreshed batch summary, one durable
attempt, count of newly inserted deduplicated events, and an explicit item
result for every dispatched candidate. The result remains `unresolved` or
`partial` when evidence is absent, unknown, or exceeds bounded pagination.
Upstream read failures return a safe 502 and still append an `error` attempt;
they do not store or echo a raw Sonarr payload.

## Web UI

The Activity scan-detail modal now provides:

- candidate checkboxes with a visible 25-item maximum;
- **Preview selection**, which writes only a dry-run audit;
- selected/excluded counts and exclusion reasons;
- **Clear preview**, a local UI action that makes no API call;
- an explicit warning checkbox;
- a disabled-until-confirmed red **Send searches to Sonarr** button; and
- a dispatch audit table showing previews and manual attempts.

The dispatch audit also shows the latest observed command state,
reconciliation state/summary/time, and read-only **Reconcile**/**Evidence**
controls. Evidence detail renders batch command observations and each
candidate's latest outcome plus timeline. The UI repeats that reconciliation
sends no search and command completion is not proof of a grab or import.

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

Coverage includes migration v2/v3 idempotence/rollback/constraints/immutability,
candidate ownership/dedupe/max selection, cross-job/library rejection, cap,
cooldown, dry-run isolation, exact confirmation, success/failure/retry audit,
concurrent duplicate and cap-overbooking prevention, response redaction, the
single Sonarr POST shape, API contracts, and UI confirmation wiring. M3 tests
cover command queued/completed/failed/aborted normalization, no false success,
grab/download/import/failure mapping, unrelated-event exclusion, pagination
bounds, crash ambiguity, cross-library rejection, idempotent reruns, append-only
evidence, safe errors, and no secret/raw-payload leakage. Sonarr remains mocked.

## Current limitations

- Sonarr only; other Arr types remain CRUD-only.
- Dispatch and reconciliation are manual only; no scheduler, worker, polling,
  automatic retry/search, download-client API, grab, download, or import
  pipeline exists.
- Managearr has no authentication/authorization yet; protect the service at the
  network/reverse-proxy layer.
- A process crash after Sonarr accepts a command but before local finalization
  is inherently ambiguous because Sonarr's command API offers no idempotency
  key. The reservation blocks another dispatch for five minutes, then becomes
  operator-visible `ambiguous`; Managearr never retries it. If the accepted
  command ID was durably recorded, the operator can reconcile it. If no command
  ID was recorded, reconciliation is refused and the UI/API instructs the
  operator to inspect Sonarr manually before any retry.
- History and queue reads are deliberately bounded (150 history rows per
  dispatched episode and 300 queue rows). A larger Sonarr result is reported as
  partial; Managearr does not guess beyond the observed evidence.
- Queue state names vary across Sonarr/download-client versions. Unrecognized
  related states are retained as `unknown`, not promoted to success.
- Candidate scans remain capped at 500 rows and skip an individual series when
  its episode fetch fails, recording that fact in activity details.

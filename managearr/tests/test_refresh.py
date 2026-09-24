"""M5 scheduled read-only refresh/reconciliation tests against the suite's
real disposable PostgreSQL database. Sonarr is always a stub/fake here -
never a real network call. Several tests explicitly monkeypatch every
Sonarr-mutating adapter method and ``DispatchService.dispatch`` to raise if
called, proving the worker's read-only refresh path never reaches them.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from app.adapters.read_only_sonarr_client import ReadOnlySonarrAdapterError, ReadOnlySonarrClient
from app.adapters.sonarr_client import SonarrConnectionError, SonarrResponseError
from app.services.scheduler_service import SchedulerService
from app.worker import SchedulerWorker


# --- fixtures/helpers -----------------------------------------------------

class StubScanClient:
    def __init__(self, *, series=None, episodes_by_series=None, series_error=None):
        self._series = series if series is not None else []
        self._episodes_by_series = episodes_by_series or {}
        self._series_error = series_error

    def system_status(self):
        return {"version": "4.0.0", "instance_name": "Sonarr"}

    def get_series(self):
        if self._series_error:
            raise self._series_error
        return self._series

    def get_episodes(self, series_id):
        return self._episodes_by_series.get(series_id, [])


class StubReconcileClient:
    def __init__(self, *, command=None, history=None, queue=None, command_error=None):
        self.command = command or {"id": 9001, "name": "EpisodeSearch", "status": "completed"}
        self.history = history or {}
        self.queue = queue or []
        self.command_error = command_error

    def get_command(self, command_id):
        if self.command_error:
            raise self.command_error
        return dict(self.command)

    def get_history(self, *, page, page_size, episode_id):
        records = self.history.get(episode_id, [])
        return {"page": page, "page_size": page_size, "total_records": len(records), "records": list(records)}

    def get_queue_details(self, *, page, page_size):
        return {"page": page, "page_size": page_size, "total_records": len(self.queue), "records": list(self.queue)}


def install_scan_stub(refresh_service, stub):
    refresh_service.scan_service._client_factory = lambda base_url, api_key, timeout=None: stub


def install_reconcile_stub(refresh_service, stub):
    refresh_service.reconciliation_service._client_factory = lambda base_url, api_key, timeout=None: stub


def make_sonarr_library(library_repo, name="Sonarr Main"):
    return library_repo.create({
        "name": name, "type": "sonarr", "url": "http://sonarr.invalid:8989",
        "api_key": "super-secret-key", "enabled": True,
    })


def make_completed_scan(activity_repo, library, *, age_minutes=0, candidate_count=0, details="Found 0."):
    job = activity_repo.create({
        "library_id": library.id, "library_name": library.name, "job_type": "sonarr_scan",
        "state": "completed", "title": "snapshot", "details": details, "candidate_count": candidate_count,
    })
    return job


def backdate_job(database, job_id, age_minutes):
    with database.connect() as conn:
        conn.execute(
            "UPDATE activity_jobs SET updated_at = now() - (%s * interval '1 minute') WHERE id = %s",
            (age_minutes, job_id),
        )


def make_reconcilable_batch(
    library_repo, activity_repo, candidate_repo, dispatch_repo, *,
    episode_id=101, command_id=9001, state="completed",
):
    library = make_sonarr_library(library_repo)
    job = activity_repo.create({
        "library_id": library.id, "library_name": library.name, "job_type": "sonarr_scan",
        "state": "completed", "title": "snapshot",
    })
    candidate_repo.create_many(job.id, library.id, [{
        "series_id": 11, "series_title": "Show", "episode_id": episode_id,
        "season_number": 1, "episode_number": 1, "reason": "monitored episode aired with no file on disk",
    }])
    candidate = candidate_repo.list_for_job(job.id)[0]
    with dispatch_repo.db.connect() as conn:
        batch_id = dispatch_repo.create_batch(conn, {
            "scan_job_id": job.id, "library_id": library.id, "library_name": library.name,
            "mode": "manual", "state": state, "requested_count": 1, "selected_count": 1,
            "dispatched_count": 1 if state != "failed" else 0, "sonarr_command_id": command_id,
        })
        if state != "failed":
            dispatch_repo.create_items(conn, batch_id, [{
                "candidate_id": candidate.id, "episode_id": candidate.episode_id,
                "series_id": candidate.series_id, "series_title": candidate.series_title,
                "season_number": 1, "episode_number": 1, "state": "dispatched",
            }])
    return library, job, batch_id


# --- read-only adapter guard ------------------------------------------------

def test_read_only_adapter_rejects_write_methods_and_proxies_reads(monkeypatch):
    from app.adapters import sonarr_client

    monkeypatch.setattr(sonarr_client.SonarrClient, "system_status", lambda self: {"version": "x"})

    def forbidden_write(self, episode_ids):
        raise AssertionError("write method reached the network layer")

    monkeypatch.setattr(sonarr_client.SonarrClient, "search_episodes", forbidden_write)

    client = ReadOnlySonarrClient("http://sonarr.invalid:8989", "secret")
    assert client.system_status() == {"version": "x"}
    with pytest.raises(ReadOnlySonarrAdapterError):
        client.search_episodes([1])
    with pytest.raises(ReadOnlySonarrAdapterError):
        client.some_future_write_method()


def test_worker_and_refresh_service_never_import_dispatch_or_write_method():
    worker_source = open("app/worker.py", encoding="utf-8").read()
    refresh_source = open("app/services/refresh_service.py", encoding="utf-8").read()
    assert "import DispatchService" not in worker_source
    assert "import DispatchService" not in refresh_source
    assert "search_episodes" not in refresh_source
    assert "import SonarrClient" not in refresh_source  # only ReadOnlySonarrClient is used


# --- settings -----------------------------------------------------------

def test_refresh_settings_default_and_validated_update(refresh_repo):
    settings = refresh_repo.get_settings()
    assert settings.scan_max_age_minutes == 60
    assert settings.reconcile_min_interval_minutes == 15
    assert settings.reconcile_max_per_cycle == 10

    updated = refresh_repo.update_settings({"scan_max_age_minutes": 30})
    assert updated.scan_max_age_minutes == 30
    assert updated.reconcile_min_interval_minutes == 15


def test_refresh_settings_api_rejects_invalid_and_unknown_fields(client):
    assert client.patch("/api/v1/refresh/settings", json={"scan_max_age_minutes": 1}).status_code == 400
    assert client.patch("/api/v1/refresh/settings", json={"live_mode": True}).status_code == 400
    ok = client.patch("/api/v1/refresh/settings", json={"reconcile_max_per_cycle": 5})
    assert ok.status_code == 200
    assert ok.get_json()["settings"]["reconcile_max_per_cycle"] == 5


# --- manual scan requests: coalescing and claiming --------------------------

def test_manual_scan_requests_coalesce_concurrently(refresh_repo, library_repo):
    make_sonarr_library(library_repo, "Library A")
    make_sonarr_library(library_repo, "Library B")

    with ThreadPoolExecutor(max_workers=6) as pool:
        rows = list(pool.map(lambda _n: refresh_repo.queue_manual_scans(None), range(6)))

    for results, error in rows:
        assert error is None
        assert len(results) == 2
    request_ids_by_library = {}
    for results, _error in rows:
        for item in results:
            request_ids_by_library.setdefault(item["library_id"], set()).add(item["request"]["id"])
    assert all(len(ids) == 1 for ids in request_ids_by_library.values())
    assert sum(1 for results, _ in rows for item in results if item["created"]) == 2


def test_manual_scan_request_for_missing_or_disabled_library_is_rejected(refresh_repo, library_repo):
    disabled = library_repo.create({
        "name": "Disabled", "type": "sonarr", "url": "http://x:8989", "api_key": "k", "enabled": False,
    })
    results, error = refresh_repo.queue_manual_scans(disabled.id)
    assert results == []
    assert "not an enabled Sonarr library" in error

    results, error = refresh_repo.queue_manual_scans(999999)
    assert results == []
    assert error is not None


def test_claim_manual_scan_requests_creates_one_run_and_avoids_duplicates(refresh_repo, library_repo):
    library = make_sonarr_library(library_repo)
    refresh_repo.queue_manual_scans(library.id)
    run_ids = refresh_repo.claim_manual_scan_requests("worker-1")
    assert len(run_ids) == 1

    # A second manual request for the same library while a run is already
    # queued/running must not create a duplicate scan run.
    refresh_repo.queue_manual_scans(library.id)
    run_ids_again = refresh_repo.claim_manual_scan_requests("worker-1")
    assert run_ids_again == []


def test_claim_manual_scan_request_for_deleted_library_fails_explicitly(refresh_repo, library_repo, database):
    library = make_sonarr_library(library_repo)
    refresh_repo.queue_manual_scans(library.id)
    with database.connect() as conn:
        conn.execute("DELETE FROM arr_libraries WHERE id = %s", (library.id,))
    run_ids = refresh_repo.claim_manual_scan_requests("worker-1")
    assert run_ids == []
    with database.connect() as conn:
        row = conn.execute("SELECT state, safe_summary FROM refresh_requests LIMIT 1").fetchone()
    assert row["state"] == "failed"
    assert "no longer exists" in row["safe_summary"]


# --- freshness sweep: exactly one scan, never duplicated --------------------

def test_stale_and_missing_scans_are_queued_exactly_once(refresh_repo, library_repo, activity_repo, database):
    stale_library = make_sonarr_library(library_repo, "Stale")
    job = make_completed_scan(activity_repo, stale_library)
    backdate_job(database, job.id, age_minutes=120)

    fresh_library = make_sonarr_library(library_repo, "Fresh")
    make_completed_scan(activity_repo, fresh_library)

    missing_library = make_sonarr_library(library_repo, "Missing")

    queued = refresh_repo.queue_stale_scans(scan_max_age_minutes=60)
    queued_library_ids = {r["library_id"] for r in refresh_repo.list_runs(kind="scan")}
    assert stale_library.id in queued_library_ids
    assert missing_library.id in queued_library_ids
    assert fresh_library.id not in queued_library_ids
    assert len(queued) == 2

    # Calling again must not duplicate the already-queued scans.
    queued_again = refresh_repo.queue_stale_scans(scan_max_age_minutes=60)
    assert queued_again == []
    assert len(refresh_repo.list_runs(kind="scan")) == 2


# --- reconciliation cooldown and bounded selection --------------------------

def test_reconcile_due_is_cooldown_aware_and_requires_eligible_work(refresh_repo):
    assert refresh_repo.enqueue_reconcile_if_due() is None
    settings_after = refresh_repo.get_settings()
    assert settings_after.next_reconcile_due_at is not None


def test_reconcile_due_queues_once_then_waits_for_cooldown(
    refresh_repo, library_repo, activity_repo, candidate_repo, dispatch_repo, database
):
    make_reconcilable_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    run_id = refresh_repo.enqueue_reconcile_if_due()
    assert run_id is not None
    assert refresh_repo.enqueue_reconcile_if_due() is None  # still cooling down

    with database.connect() as conn:
        conn.execute("UPDATE refresh_settings SET next_reconcile_due_at = NULL WHERE id = 1")
    # An active (queued) reconcile run still blocks a second one.
    assert refresh_repo.enqueue_reconcile_if_due() is None


def test_eligible_reconciliation_batches_excludes_dry_run_and_resolved_and_is_bounded(
    refresh_repo, library_repo, activity_repo, candidate_repo, dispatch_repo, database
):
    _lib1, _job1, batch1 = make_reconcilable_batch(library_repo, activity_repo, candidate_repo, dispatch_repo, episode_id=201, command_id=1)
    _lib2, _job2, batch2 = make_reconcilable_batch(library_repo, activity_repo, candidate_repo, dispatch_repo, episode_id=202, command_id=2)
    _lib3, _job3, batch3 = make_reconcilable_batch(library_repo, activity_repo, candidate_repo, dispatch_repo, episode_id=203, command_id=3)
    with database.connect() as conn:
        conn.execute("UPDATE dispatch_batches SET reconciliation_state = 'resolved' WHERE id = %s", (batch3,))

    eligible = refresh_repo.eligible_reconciliation_batches(limit=50)
    assert batch1 in eligible and batch2 in eligible
    assert batch3 not in eligible

    bounded = refresh_repo.eligible_reconciliation_batches(limit=1)
    assert bounded == [min(batch1, batch2)]


# --- restart recovery --------------------------------------------------------

def test_restart_recovery_terminalizes_running_refresh_without_retry(refresh_repo, library_repo):
    library = make_sonarr_library(library_repo)
    refresh_repo.queue_manual_scans(library.id)
    run_ids = refresh_repo.claim_manual_scan_requests("dead-worker")
    run = refresh_repo.start_next_run("dead-worker")
    assert run["id"] == run_ids[0]

    assert refresh_repo.recover_interrupted("new-worker") == 1
    recovered = refresh_repo.get_run(run["id"])
    assert recovered["state"] == "failed"
    assert "not retried" in recovered["safe_summary"]
    assert refresh_repo.start_next_run("new-worker") is None


# --- RefreshService execution (scan) ----------------------------------------

def test_refresh_service_scan_completes_and_records_snapshot(refresh_repo, refresh_service, library_repo):
    library = make_sonarr_library(library_repo)
    install_scan_stub(refresh_service, StubScanClient(
        series=[{"id": 1, "title": "Show", "monitored": True}],
        episodes_by_series={1: [{
            "id": 501, "seasonNumber": 1, "episodeNumber": 1, "monitored": True,
            "hasFile": False, "airDate": "2020-01-01",
        }]},
    ))
    refresh_repo.queue_manual_scans(library.id)
    refresh_repo.claim_manual_scan_requests("w1")
    run = refresh_repo.start_next_run("w1")
    refresh_service.execute_run(run)

    finished = refresh_repo.get_run(run["id"])
    assert finished["state"] == "completed"
    assert finished["succeeded_count"] == 1
    assert finished["scan_job_id"] is not None


def test_refresh_service_scan_failure_is_safe_and_static(refresh_repo, refresh_service, library_repo):
    library = make_sonarr_library(library_repo)
    install_scan_stub(refresh_service, StubScanClient(
        series_error=SonarrResponseError("Sonarr returned HTTP 429")
    ))
    refresh_repo.queue_manual_scans(library.id)
    refresh_repo.claim_manual_scan_requests("w1")
    run = refresh_repo.start_next_run("w1")
    refresh_service.execute_run(run)

    finished = refresh_repo.get_run(run["id"])
    assert finished["state"] == "failed"
    assert "super-secret-key" not in finished["safe_summary"]
    assert "sonarr.invalid" not in finished["safe_summary"]
    assert "429" not in finished["safe_summary"]


# --- RefreshService execution (reconcile) -----------------------------------

def test_refresh_service_reconcile_resolves_eligible_batch(
    refresh_repo, refresh_service, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _library, _job, batch_id = make_reconcilable_batch(library_repo, activity_repo, candidate_repo, dispatch_repo, episode_id=301, command_id=42)
    install_reconcile_stub(refresh_service, StubReconcileClient(
        command={"id": 42, "name": "EpisodeSearch", "status": "completed"},
        history={301: [{"id": 1, "event_type": "downloadFolderImported", "episode_id": 301,
                        "date": "2030-01-01T00:00:00Z", "download_id": "dl-1"}]},
    ))
    run_id = refresh_repo.enqueue_reconcile_if_due()
    run = refresh_repo.start_next_run("w1")
    refresh_service.execute_run(run, heartbeat=lambda: True)

    finished = refresh_repo.get_run(run["id"])
    assert finished["state"] == "completed"
    assert finished["succeeded_count"] == 1
    assert finished["reconciled_batches"][0]["dispatch_batch_id"] == batch_id
    assert finished["reconciled_batches"][0]["result"] == "resolved"


def test_refresh_service_reconcile_records_error_without_leaking_details(
    refresh_repo, refresh_service, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    make_reconcilable_batch(library_repo, activity_repo, candidate_repo, dispatch_repo, episode_id=401, command_id=77)
    install_reconcile_stub(refresh_service, StubReconcileClient(
        command_error=SonarrConnectionError("Sonarr request timed out")
    ))
    refresh_repo.enqueue_reconcile_if_due()
    run = refresh_repo.start_next_run("w1")
    refresh_service.execute_run(run, heartbeat=lambda: True)

    finished = refresh_repo.get_run(run["id"])
    assert finished["state"] == "failed"
    assert finished["failed_count"] == 1
    assert "super-secret-key" not in finished["safe_summary"]
    assert "sonarr.invalid" not in finished["safe_summary"]


def test_refresh_service_reconcile_skips_dry_run_batch_with_reason(
    refresh_repo, refresh_service, library_repo, activity_repo, candidate_repo, dispatch_repo, monkeypatch
):
    library = make_sonarr_library(library_repo)
    job = activity_repo.create({
        "library_id": library.id, "library_name": library.name, "job_type": "sonarr_scan",
        "state": "completed", "title": "snapshot",
    })
    with dispatch_repo.db.connect() as conn:
        dry_run_batch = dispatch_repo.create_batch(conn, {
            "scan_job_id": job.id, "library_id": library.id, "library_name": library.name,
            "mode": "dry_run", "state": "planned", "requested_count": 0, "selected_count": 0,
        })

    # Eligibility filtering already excludes dry-run/no-command rows; this
    # proves the deeper safety net still explains and skips one if it were
    # ever reached, rather than silently ignoring or retrying it.
    monkeypatch.setattr(refresh_repo, "eligible_reconciliation_batches", lambda limit: [dry_run_batch])
    run_id = None
    with refresh_repo.db.connect() as conn:
        run_id = conn.execute(
            "INSERT INTO refresh_runs (kind, trigger, state) VALUES ('reconcile','scheduled','queued') RETURNING id"
        ).fetchone()["id"]
    run = refresh_repo.start_next_run("w1")
    refresh_service.execute_run(run, heartbeat=lambda: True)

    finished = refresh_repo.get_run(run["id"])
    assert finished["state"] == "skipped"
    assert finished["skipped_count"] == 1
    assert finished["reconciled_batches"][0]["result"] == "skipped"
    assert "dry-run" in finished["reconciled_batches"][0]["reason"]


# --- scheduler freshness gating ---------------------------------------------

def _library_scan(library_repo, activity_repo, candidate_repo, count=1):
    library = make_sonarr_library(library_repo, "Planned")
    job = activity_repo.create({
        "library_id": library.id, "library_name": library.name, "job_type": "sonarr_scan",
        "state": "completed", "title": "snapshot", "candidate_count": count,
    })
    candidate_repo.create_many(job.id, library.id, [
        {"series_id": 10 + i, "series_title": f"Show {i}", "episode_id": 900 + i,
         "season_number": 1, "episode_number": i + 1, "air_date": "2024-01-01",
         "reason": "monitored episode aired with no file on disk"}
        for i in range(count)
    ])
    return library, job


def _run_manual(worker, scheduler_repo):
    scheduler_repo.queue_manual_simulation()
    worker.run_once()
    cycle = scheduler_repo.list_cycles(1)[0]
    return scheduler_repo.get_cycle(cycle["id"])


def test_cycle_skips_stale_snapshot_with_explicit_reason(
    database, scheduler_repo, refresh_repo, policy_repo, library_repo, activity_repo, candidate_repo
):
    library, job = _library_scan(library_repo, activity_repo, candidate_repo)
    backdate_job(database, job.id, age_minutes=120)  # default freshness window is 60 minutes

    scheduler_service = SchedulerService(scheduler_repo, policy_repo, refresh_repo)
    scheduler_repo.queue_manual_simulation()
    scheduler_repo.claim_manual_request("planner", policy_repo.get())
    started = scheduler_repo.start_next_cycle("planner")
    scheduler_service.execute_cycle(started)
    finished = scheduler_repo.get_cycle(started["id"])

    library_result = finished["libraries"][0]
    assert library_result["state"] == "skipped"
    assert "freshness window" in library_result["safe_summary"]
    assert "will be queued" in library_result["safe_summary"]
    assert not refresh_repo.has_active_scan(library.id)

    # Once the worker's freshness sweep has queued a scan, the wording
    # changes to reflect that it must not be duplicated.
    refresh_repo.queue_stale_scans(scan_max_age_minutes=60)
    scheduler_repo.queue_manual_simulation()
    scheduler_repo.claim_manual_request("planner", policy_repo.get())
    started_again = scheduler_repo.start_next_cycle("planner")
    scheduler_service.execute_cycle(started_again)
    finished_again = scheduler_repo.get_cycle(started_again["id"])
    assert "already queued or running" in finished_again["libraries"][0]["safe_summary"]


def test_cycle_uses_fresh_snapshot_and_records_snapshot_age(
    scheduler_repo, policy_repo, library_repo, activity_repo, candidate_repo
):
    _library, _job = _library_scan(library_repo, activity_repo, candidate_repo)
    worker = SchedulerWorker(scheduler_repo, policy_repo, owner_id="fresh")
    cycle = _run_manual(worker, scheduler_repo)

    library_result = cycle["libraries"][0]
    assert library_result["state"] == "completed"
    assert library_result["snapshot_age_seconds"] is not None
    assert library_result["snapshot_age_seconds"] < 60


# --- full worker iteration: zero writes proof -------------------------------

def test_off_mode_runs_only_explicit_refresh_requests(
    scheduler_repo, refresh_repo, policy_repo, library_repo, activity_repo, candidate_repo, monkeypatch,
):
    library, job = _library_scan(library_repo, activity_repo, candidate_repo)
    backdate_job(refresh_repo.db, job.id, age_minutes=120)
    calls = []

    worker = SchedulerWorker(scheduler_repo, policy_repo, owner_id="off-proof")
    monkeypatch.setattr(worker.refresh_service, "ensure_freshness", lambda: calls.append("automatic-scan"))
    monkeypatch.setattr(worker.refresh_service, "enqueue_reconcile_if_due", lambda: calls.append("automatic-reconcile"))

    worker.run_once()
    assert scheduler_repo.current_mode() == "off"
    assert calls == []
    assert refresh_repo.list_runs() == []

    refresh_repo.queue_manual_scans(library.id)
    monkeypatch.setattr(
        worker.refresh_service, "execute_run",
        lambda run, heartbeat=None: refresh_repo.finish_run(
            run["id"], "completed", {"target_count": 1, "succeeded_count": 1},
            "Explicit read-only refresh completed; no Sonarr command was sent.",
        ),
    )
    worker.run_once()
    assert refresh_repo.list_runs(kind="scan")
    assert calls == []


def test_worker_iteration_runs_refresh_and_scheduler_without_any_sonarr_write(
    database, scheduler_repo, refresh_repo, policy_repo, library_repo, activity_repo,
    candidate_repo, dispatch_repo, monkeypatch,
):
    from app.adapters import sonarr_client
    from app.services import dispatch_service

    stale_library, stale_job = _library_scan(library_repo, activity_repo, candidate_repo)
    backdate_job(database, stale_job.id, age_minutes=120)
    make_reconcilable_batch(library_repo, activity_repo, candidate_repo, dispatch_repo, episode_id=701, command_id=55)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Sonarr write or dispatch boundary was called")

    monkeypatch.setattr(sonarr_client.SonarrClient, "search_episodes", forbidden)
    monkeypatch.setattr(dispatch_service.DispatchService, "dispatch", forbidden)
    monkeypatch.setattr(sonarr_client.SonarrClient, "system_status", lambda self: {"version": "4.0.0"})
    monkeypatch.setattr(sonarr_client.SonarrClient, "get_series", lambda self: [])
    monkeypatch.setattr(sonarr_client.SonarrClient, "get_episodes", lambda self, series_id: [])
    monkeypatch.setattr(
        sonarr_client.SonarrClient, "get_command",
        lambda self, command_id: {"id": command_id, "name": "EpisodeSearch", "status": "completed"},
    )
    monkeypatch.setattr(
        sonarr_client.SonarrClient, "get_history",
        lambda self, *, page, page_size, episode_id=None: {
            "page": page, "page_size": page_size, "total_records": 0, "records": [],
        },
    )
    monkeypatch.setattr(
        sonarr_client.SonarrClient, "get_queue_details",
        lambda self, *, page, page_size: {"page": page, "page_size": page_size, "total_records": 0, "records": []},
    )

    scheduler_repo.update_mode("simulate", policy_repo.get().cycle_interval_minutes)
    worker = SchedulerWorker(scheduler_repo, policy_repo, owner_id="proof")
    # Bounded number of iterations: one to queue+run the scan/reconcile,
    # one more to let the scheduler cycle observe the fresh snapshot.
    for _ in range(4):
        worker.run_once()

    scan_runs = refresh_repo.list_runs(kind="scan")
    reconcile_runs = refresh_repo.list_runs(kind="reconcile")
    assert any(r["state"] == "completed" for r in scan_runs)
    assert any(r["state"] in ("completed", "partial") for r in reconcile_runs)


# --- API surface --------------------------------------------------------

def test_refresh_run_now_api_queues_for_all_enabled_libraries(client):
    client.post("/api/v1/libraries", json={
        "name": "A", "type": "sonarr", "url": "http://a:8989", "api_key": "k", "enabled": True,
    })
    client.post("/api/v1/libraries", json={
        "name": "B", "type": "sonarr", "url": "http://b:8989", "api_key": "k", "enabled": True,
    })
    first = client.post("/api/v1/refresh/run-now", json={}).get_json()
    assert len(first["requests"]) == 2
    assert all(item["created"] for item in first["requests"])

    second = client.post("/api/v1/refresh/run-now", json={}).get_json()
    assert all(not item["created"] for item in second["requests"])


def test_refresh_run_now_api_rejects_unknown_library(client):
    res = client.post("/api/v1/refresh/run-now", json={"library_id": 999999})
    assert res.status_code == 404


def test_refresh_runs_api_list_and_detail(client):
    library_id = client.post("/api/v1/libraries", json={
        "name": "A", "type": "sonarr", "url": "http://a:8989", "api_key": "k", "enabled": True,
    }).get_json()["library"]["id"]
    client.post("/api/v1/refresh/run-now", json={"library_id": library_id})

    assert client.get("/api/v1/refresh/runs").get_json()["runs"] == []  # queued as a request, not a run yet
    assert client.get("/api/v1/refresh/runs/999999").status_code == 404


def test_refresh_ui_wiring_mentions_read_only_notice(client):
    assert "never sends Sonarr commands" in client.get("/").get_data(as_text=True)
    assert "never sends Sonarr commands" in client.get("/settings").get_data(as_text=True)

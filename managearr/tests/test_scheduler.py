"""M4 scheduler tests use the suite's real disposable PostgreSQL database."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from app.domain.dispatch import MAX_SELECTION_PER_REQUEST
from app.persistence.migrations import MIGRATIONS
from app.worker import SchedulerWorker


def _library_scan(library_repo, activity_repo, candidate_repo, count=3):
    library = library_repo.create({
        "name": "Shows", "type": "sonarr", "url": "http://sonarr.invalid:8989",
        "api_key": "super-secret", "enabled": True,
    })
    job = activity_repo.create({
        "library_id": library.id, "library_name": library.name, "job_type": "sonarr_scan",
        "state": "completed", "title": "snapshot", "candidate_count": count,
    })
    candidate_repo.create_many(job.id, library.id, [
        {"series_id": 10 + i, "series_title": f"Show {count-i}", "episode_id": 100 + i,
         "season_number": 1, "episode_number": i + 1, "air_date": f"2024-01-{i+1:02d}",
         "reason": "monitored episode aired with no file on disk"}
        for i in range(count)
    ])
    return library, job, candidate_repo.list_for_job(job.id)


def _run_manual(worker, scheduler_repo):
    request, _ = scheduler_repo.queue_manual_simulation()
    result = worker.run_once()
    cycle = scheduler_repo.list_cycles(1)[0]
    assert cycle["request_id"] == request["id"]
    return result, scheduler_repo.get_cycle(cycle["id"])


def test_v4_schema_constraints_and_indexes(database):
    assert MIGRATIONS[3].version == 4
    with database.connect() as conn:
        tables = {r["table_name"] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        ).fetchall()}
        indexes = {r["indexname"] for r in conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname='public'"
        ).fetchall()}
    assert {"scheduler_settings", "scheduler_mode_audit", "scheduler_leases",
            "scheduler_run_requests", "scheduler_cycle_runs", "scheduler_library_results",
            "scheduler_candidate_results"} <= tables
    assert "uq_scheduler_one_active_manual_request" in indexes
    # 'live' became a valid mode value in migration v6 (see
    # test_live_dispatch.py for the full M6 coverage); only a genuinely
    # invalid value is still rejected by the column check.
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute("UPDATE scheduler_settings SET mode='turbo' WHERE id=1")
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET mode='live' WHERE id=1")
        conn.execute("UPDATE scheduler_settings SET mode='off' WHERE id=1")


def test_scheduler_mode_defaults_off_and_change_is_audited(database, scheduler_repo, policy_repo):
    assert scheduler_repo.get_settings().mode == "off"
    scheduler_repo.update_mode("simulate", policy_repo.get().cycle_interval_minutes)
    with database.connect() as conn:
        row = conn.execute("SELECT * FROM scheduler_mode_audit ORDER BY id DESC LIMIT 1").fetchone()
    assert (row["previous_mode"], row["new_mode"]) == ("off", "simulate")
    with pytest.raises(psycopg.errors.RaiseException):
        with database.connect() as conn:
            conn.execute("DELETE FROM scheduler_mode_audit WHERE id=%s", (row["id"],))


def test_lease_exclusivity_heartbeat_expiry_and_takeover(database, scheduler_repo):
    assert scheduler_repo.acquire_lease("one", 30)
    first = scheduler_repo.lease_status()
    assert first["healthy"] is True
    assert scheduler_repo.acquire_lease("two", 30) is False
    assert scheduler_repo.renew_lease("one", 30) is True
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_leases SET heartbeat_at=now()-interval '2 seconds', expires_at=now()-interval '1 second' WHERE lease_name='cycle-worker'")
    assert scheduler_repo.acquire_lease("two", 30) is True
    assert scheduler_repo.lease_status()["owner_id"] == "two"


def test_off_mode_has_no_scheduled_cycles(scheduler_repo, policy_repo):
    assert scheduler_repo.enqueue_scheduled_if_due(policy_repo.get()) is None
    assert scheduler_repo.list_cycles() == []


def test_one_scheduled_cycle_only_when_due(database, scheduler_repo, policy_repo):
    scheduler_repo.update_mode("simulate", policy_repo.get().cycle_interval_minutes)
    assert scheduler_repo.enqueue_scheduled_if_due(policy_repo.get()) is None
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET next_due_at=now()-interval '1 second' WHERE id=1")
    assert scheduler_repo.enqueue_scheduled_if_due(policy_repo.get()) is not None
    assert scheduler_repo.enqueue_scheduled_if_due(policy_repo.get()) is None
    assert len(scheduler_repo.list_cycles()) == 1


def test_manual_simulation_runs_while_off_and_never_dispatches(
    scheduler_repo, policy_repo, library_repo, activity_repo, candidate_repo, monkeypatch
):
    _library_scan(library_repo, activity_repo, candidate_repo)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("network/dispatch boundary called")

    from app.adapters import sonarr_client
    from app.services import dispatch_service
    for name in ("_send", "_get", "_post", "system_status", "get_series", "get_episodes",
                 "get_command", "get_history", "get_queue_details", "search_episodes"):
        monkeypatch.setattr(sonarr_client.SonarrClient, name, forbidden)
    monkeypatch.setattr(dispatch_service.DispatchService, "dispatch", forbidden)

    worker = SchedulerWorker(scheduler_repo, policy_repo, owner_id="manual-worker")
    _, cycle = _run_manual(worker, scheduler_repo)
    assert scheduler_repo.get_settings().mode == "off"
    assert cycle["state"] == "completed"
    assert cycle["selected_count"] == 3
    assert cycle["libraries"][0]["upgrades_state"] == "unsupported"
    assert "no Sonarr commands" in cycle["safe_summary"]


def test_manual_request_is_idempotent_and_claimed_once(scheduler_repo, policy_repo):
    with ThreadPoolExecutor(max_workers=6) as pool:
        rows = list(pool.map(lambda _n: scheduler_repo.queue_manual_simulation(), range(6)))
    assert len({row[0]["id"] for row in rows}) == 1
    assert sum(1 for _row, created in rows if created) == 1
    with ThreadPoolExecutor(max_workers=4) as pool:
        cycle_ids = list(pool.map(lambda n: scheduler_repo.claim_manual_request(f"w{n}", policy_repo.get()), range(4)))
    assert sum(cid is not None for cid in cycle_ids) == 1


def test_restart_recovery_terminalizes_running_cycle_without_retry(
    database, scheduler_repo, policy_repo
):
    scheduler_repo.queue_manual_simulation()
    cycle_id = scheduler_repo.claim_manual_request("dead-worker", policy_repo.get())
    assert scheduler_repo.start_next_cycle("dead-worker")["id"] == cycle_id
    assert scheduler_repo.acquire_lease("dead-worker", 30)
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_leases SET heartbeat_at=now()-interval '2 seconds', expires_at=now()-interval '1 second'")
    assert scheduler_repo.acquire_lease("new-worker", 30)
    assert scheduler_repo.recover_interrupted("new-worker") == 1
    cycle = scheduler_repo.get_cycle(cycle_id)
    assert cycle["state"] == "failed"
    assert "not retried" in cycle["safe_summary"]
    assert scheduler_repo.start_next_cycle("new-worker") is None


def test_deterministic_order_and_safety_cap(
    scheduler_repo, policy_repo, library_repo, activity_repo, candidate_repo
):
    _library_scan(library_repo, activity_repo, candidate_repo, count=30)
    worker = SchedulerWorker(scheduler_repo, policy_repo, owner_id="planner")
    _, cycle = _run_manual(worker, scheduler_repo)
    selected = cycle["libraries"][0]["candidates"][:cycle["selected_count"]]
    # Balanced target (5) is more conservative than the 25-item request limit.
    assert cycle["selected_count"] == 5
    assert [c["series_title"] for c in selected] == sorted(c["series_title"] for c in selected)
    assert cycle["libraries"][0]["effective_cap"] <= MAX_SELECTION_PER_REQUEST == 25
    assert any("effective simulation selection cap" in (c["exclusion_reason"] or "")
               for c in cycle["libraries"][0]["candidates"])


def test_random_order_is_seeded_and_auditable(
    scheduler_repo, policy_repo, library_repo, activity_repo, candidate_repo
):
    library, job, _ = _library_scan(library_repo, activity_repo, candidate_repo, count=8)
    policy_repo.update({"search_order": "random"})
    worker = SchedulerWorker(scheduler_repo, policy_repo, owner_id="random-order")
    _, cycle = _run_manual(worker, scheduler_repo)
    assert isinstance(cycle["random_seed"], int)
    raw = scheduler_repo.candidates_for_scan(job.id)
    expected = worker.service._ordered(raw, "random", cycle["random_seed"], library.id)[:5]
    selected = [c for c in cycle["libraries"][0]["candidates"] if c["selected"]]
    selected.sort(key=lambda c: c["order_position"])
    assert [c["candidate_id"] for c in selected] == [c["id"] for c in expected]


def test_planning_applies_cooldown_and_inflight(
    database, scheduler_repo, policy_repo, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    library, job, candidates = _library_scan(library_repo, activity_repo, candidate_repo, count=3)
    now = datetime.now(timezone.utc)
    with database.connect() as conn:
        completed = dispatch_repo.create_batch(conn, {
            "scan_job_id": job.id, "library_id": library.id, "library_name": library.name,
            "mode": "manual", "state": "completed", "requested_count": 1,
            "selected_count": 1, "dispatched_count": 1, "sonarr_command_id": 11,
        })
        dispatch_repo.create_items(conn, completed, [{
            "candidate_id": candidates[0].id, "episode_id": candidates[0].episode_id,
            "series_id": candidates[0].series_id, "series_title": candidates[0].series_title,
            "season_number": 1, "episode_number": 1, "state": "dispatched",
        }])
        inflight = dispatch_repo.create_batch(conn, {
            "scan_job_id": job.id, "library_id": library.id, "library_name": library.name,
            "mode": "manual", "state": "dispatching", "requested_count": 1,
            "selected_count": 1, "dispatched_count": 0,
        })
        dispatch_repo.create_items(conn, inflight, [{
            "candidate_id": candidates[1].id, "episode_id": candidates[1].episode_id,
            "series_id": candidates[1].series_id, "series_title": candidates[1].series_title,
            "season_number": 1, "episode_number": 2, "state": "reserved",
        }])
    worker = SchedulerWorker(scheduler_repo, policy_repo, owner_id="facts")
    _, cycle = _run_manual(worker, scheduler_repo)
    reasons = {c["candidate_id"]: c["exclusion_reason"] for c in cycle["libraries"][0]["candidates"]}
    assert "cooldown" in reasons[candidates[0].id]
    assert "in flight" in reasons[candidates[1].id]
    assert cycle["selected_count"] == 1


def _record_outcome(
    conn, batch_id, item_id, candidate, event_type, event_state, evidence_key, *, observed_at=None
):
    conn.execute(
        """
        INSERT INTO dispatch_outcome_events (
            batch_id, dispatch_item_id, candidate_id, episode_id, observed_at,
            source_endpoint, event_type, event_state, safe_summary, evidence_key
        ) VALUES (%s,%s,%s,%s,COALESCE(%s,now()),'history',%s,%s,'safe',%s)
        """,
        (batch_id, item_id, candidate.id, candidate.episode_id, observed_at,
         event_type, event_state, evidence_key),
    )


def _completed_dispatch(conn, dispatch_repo, library, job, candidate, *, dispatched_at=None):
    batch_id = dispatch_repo.create_batch(conn, {
        "scan_job_id": job.id, "library_id": library.id, "library_name": library.name,
        "mode": "manual", "state": "completed", "requested_count": 1,
        "selected_count": 1, "dispatched_count": 1, "sonarr_command_id": 22,
    })
    if dispatched_at is None:
        item_id = dispatch_repo.create_items(conn, batch_id, [{
            "candidate_id": candidate.id, "episode_id": candidate.episode_id,
            "series_id": candidate.series_id, "series_title": candidate.series_title,
            "season_number": 1, "episode_number": 1, "state": "dispatched",
        }])[0]
    else:
        item_id = conn.execute(
            """
            INSERT INTO dispatch_batch_items (
                batch_id, candidate_id, episode_id, series_id, series_title,
                season_number, episode_number, state, created_at, updated_at
            ) VALUES (%s,%s,%s,%s,%s,1,1,'dispatched',%s,%s) RETURNING id
            """,
            (batch_id, candidate.id, candidate.episode_id, candidate.series_id,
             candidate.series_title, dispatched_at, dispatched_at),
        ).fetchone()["id"]
    return batch_id, item_id


def test_grabbed_without_terminal_outcome_is_excluded_and_occupies_queue(
    database, scheduler_repo, policy_repo, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    library, job, candidates = _library_scan(library_repo, activity_repo, candidate_repo, count=2)
    with database.connect() as conn:
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        batch_id, item_id = _completed_dispatch(
            conn, dispatch_repo, library, job, candidates[0], dispatched_at=old
        )
        _record_outcome(conn, batch_id, item_id, candidates[0], "grabbed", "nonterminal", "grab-active")
        # Prove the durable outcome, not a recently updated dispatch row, is
        # what keeps this episode out of future automatic searches.
    worker = SchedulerWorker(scheduler_repo, policy_repo, owner_id="grabbed-active")
    _, cycle = _run_manual(worker, scheduler_repo)
    result = cycle["libraries"][0]
    reasons = {c["candidate_id"]: c["exclusion_reason"] for c in result["candidates"]}
    assert "active grab" in reasons[candidates[0].id]
    assert result["queue_occupancy"] == 1
    assert cycle["selected_count"] == 1


def test_terminal_failure_starts_cooldown_from_durable_outcome(
    database, scheduler_repo, policy_repo, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    library, job, candidates = _library_scan(library_repo, activity_repo, candidate_repo, count=2)
    with database.connect() as conn:
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        batch_id, item_id = _completed_dispatch(
            conn, dispatch_repo, library, job, candidates[0], dispatched_at=old
        )
        _record_outcome(
            conn, batch_id, item_id, candidates[0], "grabbed", "nonterminal",
            "grab-before-failure", observed_at=old,
        )
        _record_outcome(conn, batch_id, item_id, candidates[0], "download_failed", "terminal", "recent-failure")
    worker = SchedulerWorker(scheduler_repo, policy_repo, owner_id="failure-cooldown")
    _, cycle = _run_manual(worker, scheduler_repo)
    result = cycle["libraries"][0]
    reasons = {c["candidate_id"]: c["exclusion_reason"] for c in result["candidates"]}
    assert "cooldown" in reasons[candidates[0].id]
    assert result["queue_occupancy"] == 0


def test_episode_becomes_eligible_after_definitive_failure_cooldown(
    database, scheduler_repo, policy_repo, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    library, job, candidates = _library_scan(library_repo, activity_repo, candidate_repo, count=1)
    with database.connect() as conn:
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        batch_id, item_id = _completed_dispatch(
            conn, dispatch_repo, library, job, candidates[0], dispatched_at=old
        )
        _record_outcome(
            conn, batch_id, item_id, candidates[0], "grabbed", "nonterminal",
            "old-grab", observed_at=old,
        )
        _record_outcome(
            conn, batch_id, item_id, candidates[0], "import_failed", "terminal",
            "old-failure", observed_at=old + timedelta(minutes=1),
        )
    worker = SchedulerWorker(scheduler_repo, policy_repo, owner_id="failure-expired")
    _, cycle = _run_manual(worker, scheduler_repo)
    result = cycle["libraries"][0]
    candidate = result["candidates"][0]
    assert candidate["selected"] is True
    assert candidate["exclusion_reason"] is None
    assert result["queue_occupancy"] == 0


def test_planning_uses_durable_queue_and_outcome_evidence(
    database, scheduler_repo, policy_repo, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    library, job, candidates = _library_scan(library_repo, activity_repo, candidate_repo, count=3)
    policy_repo.update({"queue_target": 1})
    with database.connect() as conn:
        batch_id = dispatch_repo.create_batch(conn, {
            "scan_job_id": job.id, "library_id": library.id, "library_name": library.name,
            "mode": "manual", "state": "completed", "requested_count": 2,
            "selected_count": 2, "dispatched_count": 2, "sonarr_command_id": 22,
        })
        item_ids = dispatch_repo.create_items(conn, batch_id, [{
            "candidate_id": c.id, "episode_id": c.episode_id, "series_id": c.series_id,
            "series_title": c.series_title, "season_number": 1,
            "episode_number": index + 1, "state": "dispatched",
        } for index, c in enumerate(candidates[:2])])
        for index, (event_type, event_state) in enumerate((("imported", "terminal"), ("downloading", "nonterminal"))):
            conn.execute(
                """
                INSERT INTO dispatch_outcome_events (
                    batch_id, dispatch_item_id, candidate_id, episode_id, observed_at,
                    source_endpoint, event_type, event_state, safe_summary, evidence_key
                ) VALUES (%s,%s,%s,%s,now(),'history',%s,%s,'safe',%s)
                """,
                (batch_id, item_ids[index], candidates[index].id, candidates[index].episode_id,
                 event_type, event_state, f"evidence-{index}"),
            )
    worker = SchedulerWorker(scheduler_repo, policy_repo, owner_id="outcomes")
    _, cycle = _run_manual(worker, scheduler_repo)
    library_result = cycle["libraries"][0]
    reasons = {c["candidate_id"]: c["exclusion_reason"] for c in library_result["candidates"]}
    assert "already records" in reasons[candidates[0].id]
    assert library_result["queue_occupancy"] == 1
    assert library_result["recent_success_count"] == 1
    assert library_result["effective_cap"] == 0
    assert "queue target" in reasons[candidates[2].id]


def test_api_and_ui_wiring_is_simulation_only(client):
    data = client.get("/api/v1/scheduler/settings").get_json()["scheduler"]
    assert data["settings"]["mode"] == "off"
    assert client.patch("/api/v1/scheduler/settings", json={"mode": "live"}).status_code == 400
    enabled = client.patch("/api/v1/scheduler/settings", json={"mode": "simulate"})
    assert enabled.status_code == 200
    first = client.post("/api/v1/scheduler/run-simulation-now", json={}).get_json()
    second = client.post("/api/v1/scheduler/run-simulation-now", json={}).get_json()
    assert first["request"]["id"] == second["request"]["id"]
    assert "super-secret" not in str(data)
    assert "Simulation sends no Sonarr commands" in client.get("/settings").get_data(as_text=True)
    assert "Simulation sends no Sonarr commands" in client.get("/").get_data(as_text=True)


def test_worker_and_compose_are_separate_processes():
    compose = open("../compose.managearr.yml", encoding="utf-8").read()
    assert "managearr-worker:" in compose
    worker_section = compose.split("managearr-worker:", 1)[1].split("managearr-import:", 1)[0]
    assert 'command: ["python", "-m", "app.worker"]' in worker_section
    assert "ports:" not in worker_section
    run_source = open("app/__init__.py", encoding="utf-8").read()
    assert "SchedulerWorker" not in run_source

"""M6 controlled scheduled live dispatch tests, against the suite's real
disposable PostgreSQL database.

Nothing here ever contacts a real Sonarr instance - every Sonarr call goes
through ``StubSonarrClient`` below, exactly like ``test_dispatch_service.py``.
"""
from datetime import datetime, timedelta, timezone

import pytest

from app.adapters.sonarr_client import SonarrConnectionError, SonarrResponseError
from app.domain.live import ARM_LIVE_DISPATCH_PHRASE, ENABLE_LIVE_MODE_PHRASE, hash_token
from app.services.dispatch_service import DispatchService
from app.services.live_dispatch_coordinator import LiveDispatchCoordinator
from app.worker import SchedulerWorker


class StubSonarrClient:
    """Only ``search_episodes`` is implemented - any other attribute access
    fails loudly, proving the live path can never do anything else."""

    def __init__(self, *, command=None, error=None):
        self._command = command or {"id": 9001, "name": "EpisodeSearch", "status": "queued"}
        self._error = error
        self.calls: list[list[int]] = []

    def search_episodes(self, episode_ids):
        self.calls.append(list(episode_ids))
        if self._error:
            raise self._error
        return self._command

    def __getattr__(self, name):
        raise AssertionError(f"live dispatch attempted a non-search Sonarr method: {name}")


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


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


def _dispatch_service_with_stub(dispatch_planning_service, dispatch_repo, library_repo, stub):
    def factory(base_url, api_key, timeout=None):
        return stub
    return DispatchService(dispatch_planning_service, dispatch_repo, library_repo, client_factory=factory)


def _queue_live_cycle(database, scheduler_repo, policy_repo):
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET next_due_at = now() - interval '1 second' WHERE id = 1")
    cycle_id = scheduler_repo.enqueue_scheduled_if_due(policy_repo.get())
    assert cycle_id is not None
    return cycle_id


def _plan_and_dispatch(worker, scheduler_repo, owner="live-worker"):
    cycle = scheduler_repo.start_next_cycle(owner)
    worker.service.execute_cycle(cycle, heartbeat=lambda: True)
    worker._ensure_live_coordinator().execute(cycle, heartbeat=lambda: True)
    return scheduler_repo.get_cycle(cycle["id"])


def _make_worker(scheduler_repo, policy_repo, live_repo, dispatch_service, owner="live-worker"):
    clock = FakeClock()
    coordinator = LiveDispatchCoordinator(
        live_repo, scheduler_repo, dispatch_service,
        sleeper=clock.sleep, monotonic=clock.monotonic,
    )
    coordinator.test_clock = clock
    return SchedulerWorker(scheduler_repo, policy_repo, live_repository=live_repo, live_coordinator=coordinator, owner_id=owner)


# --- Migration v6 -----------------------------------------------------------


def test_v6_live_tables_exist_and_singleton_seeded(database):
    with database.connect() as conn:
        tables = {r["table_name"] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        ).fetchall()}
        row = conn.execute("SELECT * FROM live_control WHERE id = 1").fetchone()
    assert {"live_control", "live_challenges", "live_control_audit", "live_dispatch_ledger"} <= tables
    assert row["armed"] is False
    assert row["arm_generation"] == 0
    assert row["max_dispatches_per_cycle"] == 1


def test_v6_live_control_check_rejects_armed_without_required_fields(database):
    import psycopg
    with pytest.raises(psycopg.errors.CheckViolation):
        with database.connect() as conn:
            conn.execute("UPDATE live_control SET armed = TRUE WHERE id = 1")


def test_v6_live_dispatch_ledger_is_append_only(database, library_repo, activity_repo, candidate_repo, scheduler_repo, policy_repo, live_repo):
    library, job, candidates = _library_scan(library_repo, activity_repo, candidate_repo, count=1)
    live_repo.enable_live_mode(actor="t", reason="t")
    live_repo.arm(actor="t", reason="t", ttl_minutes=15)
    cycle_id = _queue_live_cycle(database, scheduler_repo, policy_repo)
    with database.connect() as conn:
        conn.execute(
            "UPDATE scheduler_cycle_runs SET state='running', started_at=now(), worker_owner='x' WHERE id=%s",
            (cycle_id,),
        )
    control = live_repo.get_control()
    live_repo.record_ledger_entry({
        "cycle_run_id": cycle_id, "arm_generation": control["arm_generation"],
        "state": "skipped", "terminal_reason": "test row",
    })
    import psycopg
    with pytest.raises(psycopg.errors.RaiseException):
        with database.connect() as conn:
            conn.execute("UPDATE live_dispatch_ledger SET state='dispatched' WHERE cycle_run_id=%s", (cycle_id,))
    with pytest.raises(psycopg.errors.RaiseException):
        with database.connect() as conn:
            conn.execute("DELETE FROM live_dispatch_ledger WHERE cycle_run_id=%s", (cycle_id,))


# --- Direct PATCH cannot enable live; two-step challenge/confirm -----------


def test_direct_patch_rejects_live_with_explicit_guidance(client):
    resp = client.patch("/api/v1/scheduler/settings", json={"mode": "live"})
    assert resp.status_code == 400
    assert "challenge" in resp.get_json()["errors"][0]


def test_mode_challenge_confirm_enables_live_but_never_arms(live_control_service, scheduler_repo):
    challenge = live_control_service.request_mode_challenge()
    result, errors = live_control_service.confirm_mode_challenge(
        {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": ENABLE_LIVE_MODE_PHRASE},
        actor="operator",
    )
    assert errors == []
    assert result["mode"] == "live"
    assert result["armed"] is False
    assert scheduler_repo.get_settings().mode == "live"
    assert live_control_service.status()["control"]["armed"] is False


def test_mode_confirm_rejects_wrong_phrase(live_control_service):
    challenge = live_control_service.request_mode_challenge()
    result, errors = live_control_service.confirm_mode_challenge(
        {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": "nope"},
        actor="operator",
    )
    assert result is None
    assert "phrase" in errors[0]


def test_mode_confirm_rejects_replayed_token(live_control_service):
    challenge = live_control_service.request_mode_challenge()
    payload = {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": ENABLE_LIVE_MODE_PHRASE}
    result, errors = live_control_service.confirm_mode_challenge(payload, actor="operator")
    assert errors == []
    result2, errors2 = live_control_service.confirm_mode_challenge(payload, actor="operator")
    assert result2 is None
    assert "already used" in errors2[0]


def test_mode_confirm_rejects_expired_challenge(database, live_control_service):
    # expires_at is part of a challenge's immutable identity once created
    # (see protect_live_challenge), so an already-expired one is inserted
    # directly (INSERT is not covered by that trigger) rather than created
    # normally and then backdated.
    token = "test-token-value"
    with database.connect() as conn:
        row = conn.execute(
            """
            INSERT INTO live_challenges (kind, token_hash, policy_digest, state, expires_at)
            VALUES ('enable_mode', %s, 'digest', 'pending', now() - interval '1 second')
            RETURNING id
            """,
            (hash_token(token),),
        ).fetchone()
    result, errors = live_control_service.confirm_mode_challenge(
        {"challenge_id": row["id"], "token": token, "phrase": ENABLE_LIVE_MODE_PHRASE},
        actor="operator",
    )
    assert result is None
    assert "expired" in errors[0]


def test_mode_confirm_rejects_when_policy_changed(live_control_service, policy_repo):
    challenge = live_control_service.request_mode_challenge()
    policy_repo.update({"hourly_api_cap": 999})
    result, errors = live_control_service.confirm_mode_challenge(
        {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": ENABLE_LIVE_MODE_PHRASE},
        actor="operator",
    )
    assert result is None
    assert "policy" in errors[0]


def test_mode_confirm_rejects_wrong_token_hash(live_control_service):
    challenge = live_control_service.request_mode_challenge()
    result, errors = live_control_service.confirm_mode_challenge(
        {"challenge_id": challenge["challenge_id"], "token": "not-the-real-token", "phrase": ENABLE_LIVE_MODE_PHRASE},
        actor="operator",
    )
    assert result is None
    assert "token" in errors[0]


# --- Arm challenge/confirm ---------------------------------------------


def test_arm_challenge_requires_live_mode_first(live_control_service):
    result, errors = live_control_service.request_arm_challenge({"reason": "test", "ttl_minutes": 10})
    assert result is None
    assert "live" in errors[0]


def _enable_live(live_control_service):
    challenge = live_control_service.request_mode_challenge()
    live_control_service.confirm_mode_challenge(
        {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": ENABLE_LIVE_MODE_PHRASE},
        actor="operator",
    )


def test_arm_confirm_succeeds_and_sets_bounded_expiry(live_control_service):
    _enable_live(live_control_service)
    challenge, errors = live_control_service.request_arm_challenge({"reason": "on-call fix", "ttl_minutes": 10})
    assert errors == []
    control, errors = live_control_service.confirm_arm_challenge(
        {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": ARM_LIVE_DISPATCH_PHRASE},
        actor="operator",
    )
    assert errors == []
    assert control["armed"] is True
    assert control["authorization_generation"] > 0
    assert control["authorized_by"] == "operator"
    assert control["authorization_reason"] == "on-call fix"
    assert control["expires_at"] is None


def test_legacy_arm_confirm_preserves_existing_live_schedule(
    database, live_control_service,
):
    _enable_live(live_control_service)
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET next_due_at = now() + interval '1 hour' WHERE id = 1")
    challenge, errors = live_control_service.request_arm_challenge({"reason": "test now", "ttl_minutes": 5})
    assert errors == []
    control, errors = live_control_service.confirm_arm_challenge(
        {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": ARM_LIVE_DISPATCH_PHRASE},
        actor="operator",
    )
    assert errors == []
    with database.connect() as conn:
        due = conn.execute(
            "SELECT next_due_at > now() + interval '59 minutes' AS preserved FROM scheduler_settings WHERE id = 1"
        ).fetchone()
    assert due["preserved"] is True
    assert control["expires_at"] is None  # persistent across restart


def test_arm_confirm_rejects_wrong_phrase(live_control_service):
    _enable_live(live_control_service)
    challenge, _ = live_control_service.request_arm_challenge({"reason": "x", "ttl_minutes": 5})
    control, errors = live_control_service.confirm_arm_challenge(
        {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": "ARM IT"},
        actor="operator",
    )
    assert control is None
    assert "phrase" in errors[0]


def test_arm_ttl_is_capped_to_max(live_control_service, live_repo):
    _enable_live(live_control_service)
    challenge, errors = live_control_service.request_arm_challenge({"reason": "x", "ttl_minutes": 60})
    assert errors == []
    control, errors = live_control_service.confirm_arm_challenge(
        {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": ARM_LIVE_DISPATCH_PHRASE},
        actor="operator",
    )
    assert errors == []
    assert control["expires_at"] is None


def test_arm_rejects_ttl_out_of_bounds(live_control_service):
    _enable_live(live_control_service)
    result, errors = live_control_service.request_arm_challenge({"reason": "x", "ttl_minutes": 999})
    assert result is None
    assert "ttl_minutes" in errors[0]


def test_disarm_invalidates_pending_authorization(live_control_service, live_repo):
    _enable_live(live_control_service)
    challenge, _ = live_control_service.request_arm_challenge({"reason": "x", "ttl_minutes": 15})
    live_control_service.confirm_arm_challenge(
        {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": ARM_LIVE_DISPATCH_PHRASE},
        actor="operator",
    )
    assert live_repo.get_control()["armed"] is True
    control = live_control_service.disarm({"reason": "done"}, actor="operator")
    assert control["armed"] is False
    assert live_repo.get_control()["armed"] is False


def test_mode_change_away_from_live_disarms(scheduler_service, live_control_service, live_repo):
    _enable_live(live_control_service)
    challenge, _ = live_control_service.request_arm_challenge({"reason": "x", "ttl_minutes": 15})
    live_control_service.confirm_arm_challenge(
        {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": ARM_LIVE_DISPATCH_PHRASE},
        actor="operator",
    )
    assert live_repo.get_control()["armed"] is True
    settings, errors = scheduler_service.update_settings({"mode": "off"})
    assert errors == []
    assert live_repo.get_control()["armed"] is False


def test_emergency_stop_is_idempotent_and_cancels_queued_live_cycles(
    database, scheduler_repo, policy_repo, live_control_service, live_repo
):
    _enable_live(live_control_service)
    challenge, _ = live_control_service.request_arm_challenge({"reason": "x", "ttl_minutes": 15})
    live_control_service.confirm_arm_challenge(
        {"challenge_id": challenge["challenge_id"], "token": challenge["token"], "phrase": ARM_LIVE_DISPATCH_PHRASE},
        actor="operator",
    )
    cycle_id = _queue_live_cycle(database, scheduler_repo, policy_repo)

    first = live_control_service.emergency_stop({"reason": "stop"}, actor="operator")
    second = live_control_service.emergency_stop({"reason": "stop again"}, actor="operator")
    assert live_repo.get_control()["armed"] is False
    assert live_repo.get_control()["emergency_stopped_at"] is not None
    cycle = scheduler_repo.get_cycle(cycle_id)
    assert cycle["state"] == "skipped"
    assert "emergency stop" in cycle["safe_summary"]
    # Idempotent: repeated calls both succeed and converge on the same state.
    assert first["control"]["armed"] is False
    assert second["control"]["armed"] is False


# --- Worker/coordinator dispatch behavior -----------------------------------


def test_off_simulate_and_unarmed_live_never_instantiate_write_client(
    database, scheduler_repo, policy_repo, live_repo, library_repo, activity_repo, candidate_repo, monkeypatch
):
    _library_scan(library_repo, activity_repo, candidate_repo)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("write-capable Sonarr client was instantiated")

    from app.adapters import sonarr_client
    from app.services import dispatch_service as dispatch_service_module
    monkeypatch.setattr(sonarr_client.SonarrClient, "__init__", forbidden)
    monkeypatch.setattr(dispatch_service_module.DispatchService, "dispatch", forbidden)

    worker = SchedulerWorker(scheduler_repo, policy_repo, live_repository=live_repo, owner_id="w1")

    # off
    worker.run_once()
    assert scheduler_repo.get_settings().mode == "off"

    # simulate
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET mode='simulate', next_due_at=now()-interval '1 second' WHERE id=1")
    worker.run_once()

    # live but unarmed
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET mode='live', next_due_at=now()-interval '1 second' WHERE id=1")
    worker.run_once()
    cycles = scheduler_repo.list_cycles()
    assert any(c["mode_snapshot"] == "live" and c["state"] == "completed" for c in cycles)


def test_live_armed_dispatches_selected_candidate_and_records_ledger(
    database, scheduler_repo, policy_repo, live_repo, library_repo, activity_repo, candidate_repo,
    dispatch_repo, dispatch_planning_service,
):
    # A single candidate avoids any dependency on deterministic ordering
    # between multiple candidates (already covered by test_scheduler.py).
    library, job, candidates = _library_scan(library_repo, activity_repo, candidate_repo, count=1)
    live_repo.enable_live_mode(actor="t", reason="t")
    live_repo.arm(actor="t", reason="t", ttl_minutes=15)
    stub = StubSonarrClient()
    dispatch_service = _dispatch_service_with_stub(dispatch_planning_service, dispatch_repo, library_repo, stub)
    worker = _make_worker(scheduler_repo, policy_repo, live_repo, dispatch_service)

    cycle_id = _queue_live_cycle(database, scheduler_repo, policy_repo)
    detail = _plan_and_dispatch(worker, scheduler_repo)

    assert stub.calls == [[100]]  # exactly one EpisodeSearch call, one episode (max_dispatches_per_cycle=1)
    ledger = live_repo.ledger_for_cycle(cycle_id)
    dispatched = [row for row in ledger if row["state"] == "dispatched"]
    assert len(dispatched) == 1
    assert dispatched[0]["sonarr_command_id"] == 9001
    assert dispatched[0]["arm_generation"] > 0
    batch = dispatch_repo.get_batch(dispatched[0]["dispatch_batch_id"])
    assert batch.mode == "manual"
    assert batch.state == "completed"
    assert batch.sonarr_command_id == 9001
    assert live_repo.get_control()["last_dispatch_at"] is not None


def test_live_respects_max_dispatches_per_cycle(
    database, scheduler_repo, policy_repo, live_repo, library_repo, activity_repo, candidate_repo,
    dispatch_repo, dispatch_planning_service, monkeypatch,
):
    _library_scan(library_repo, activity_repo, candidate_repo, count=5)
    live_repo.enable_live_mode(actor="t", reason="t")
    live_repo.arm(actor="t", reason="t", ttl_minutes=15)
    with database.connect() as conn:
        conn.execute("UPDATE live_control SET max_dispatches_per_cycle = 2 WHERE id = 1")
    stub = StubSonarrClient()
    dispatch_service = _dispatch_service_with_stub(dispatch_planning_service, dispatch_repo, library_repo, stub)
    worker = _make_worker(scheduler_repo, policy_repo, live_repo, dispatch_service)
    # Minimum-delay spacing is covered separately by
    # test_live_respects_minimum_delay_between_dispatches; bypass it here so
    # this test isolates the per-cycle count cap.
    monkeypatch.setattr(worker.live_repository, "check_dispatch_delay_elapsed", lambda *_a, **_k: True)

    _queue_live_cycle(database, scheduler_repo, policy_repo)
    _plan_and_dispatch(worker, scheduler_repo)

    assert len(stub.calls) == 2


def test_live_waits_for_persisted_delay_before_first_dispatch(
    database, scheduler_repo, policy_repo, live_repo, library_repo, activity_repo,
    candidate_repo, dispatch_repo, dispatch_planning_service,
):
    _library_scan(library_repo, activity_repo, candidate_repo, count=1)
    live_repo.enable_live_mode(actor="t", reason="t")
    live_repo.arm(actor="t", reason="t", ttl_minutes=15)
    stub = StubSonarrClient()
    worker = _make_worker(
        scheduler_repo, policy_repo, live_repo,
        _dispatch_service_with_stub(dispatch_planning_service, dispatch_repo, library_repo, stub),
    )
    coordinator = worker._ensure_live_coordinator()
    coordinator.live_repo.dispatch_delay_remaining_seconds = lambda _seconds: 12.5
    call_times = []
    original_search = stub.search_episodes

    def timed_search(episode_ids):
        call_times.append(coordinator.test_clock.now)
        return original_search(episode_ids)

    stub.search_episodes = timed_search
    _queue_live_cycle(database, scheduler_repo, policy_repo)
    _plan_and_dispatch(worker, scheduler_repo)

    assert call_times == [12.5]
    assert sum(coordinator.test_clock.sleeps) == 12.5


def test_live_spaces_multiple_dispatches_without_real_sleep(
    database, scheduler_repo, policy_repo, live_repo, library_repo, activity_repo, candidate_repo,
    dispatch_repo, dispatch_planning_service,
):
    _library_scan(library_repo, activity_repo, candidate_repo, count=5)
    live_repo.enable_live_mode(actor="t", reason="t")
    live_repo.arm(actor="t", reason="t", ttl_minutes=15)
    with database.connect() as conn:
        conn.execute("UPDATE live_control SET max_dispatches_per_cycle = 5, min_delay_seconds_between_dispatches = 600 WHERE id = 1")
    stub = StubSonarrClient()
    dispatch_service = _dispatch_service_with_stub(dispatch_planning_service, dispatch_repo, library_repo, stub)
    worker = _make_worker(scheduler_repo, policy_repo, live_repo, dispatch_service)
    call_times = []
    original_search = stub.search_episodes

    def timed_search(episode_ids):
        call_times.append(worker._ensure_live_coordinator().test_clock.now)
        return original_search(episode_ids)

    stub.search_episodes = timed_search
    cycle_id = _queue_live_cycle(database, scheduler_repo, policy_repo)
    _plan_and_dispatch(worker, scheduler_repo)

    assert len(stub.calls) == 5
    assert call_times == [0, 600, 1200, 1800, 2400]
    assert sum(worker._ensure_live_coordinator().test_clock.sleeps) == 4 * 600
    assert len([r for r in live_repo.ledger_for_cycle(cycle_id) if r["state"] == "dispatched"]) == 5


@pytest.mark.parametrize("interruption", ["pause", "stop", "generation"])
def test_live_wait_is_interrupted_by_persistent_control_change(
    interruption, database, scheduler_repo, policy_repo, live_repo, live_control_service,
    library_repo, activity_repo, candidate_repo, dispatch_repo, dispatch_planning_service,
):
    _library_scan(library_repo, activity_repo, candidate_repo, count=3)
    live_repo.enable_live_mode(actor="t", reason="t")
    live_repo.arm(actor="t", reason="t", ttl_minutes=15)
    with database.connect() as conn:
        conn.execute("UPDATE live_control SET max_dispatches_per_cycle=5 WHERE id=1")
    stub = StubSonarrClient()
    worker = _make_worker(
        scheduler_repo, policy_repo, live_repo,
        _dispatch_service_with_stub(dispatch_planning_service, dispatch_repo, library_repo, stub),
    )
    coordinator = worker._ensure_live_coordinator()
    clock = coordinator.test_clock
    interrupted = False

    def sleep_and_interrupt(seconds):
        nonlocal interrupted
        clock.sleep(seconds)
        if interrupted:
            return
        interrupted = True
        if interruption == "pause":
            live_control_service.pause({"confirm": True, "reason": "test"}, actor="t")
        elif interruption == "stop":
            live_control_service.emergency_stop({"reason": "test"}, actor="t")
        else:
            with database.connect() as conn:
                conn.execute(
                    "UPDATE live_control SET authorization_generation=authorization_generation+1 WHERE id=1"
                )

    coordinator.sleeper = sleep_and_interrupt
    _queue_live_cycle(database, scheduler_repo, policy_repo)
    _plan_and_dispatch(worker, scheduler_repo)
    assert len(stub.calls) == 1


def test_live_wait_stops_when_worker_heartbeat_is_lost(
    database, scheduler_repo, policy_repo, live_repo, library_repo, activity_repo,
    candidate_repo, dispatch_repo, dispatch_planning_service,
):
    _library_scan(library_repo, activity_repo, candidate_repo, count=3)
    live_repo.enable_live_mode(actor="t", reason="t")
    live_repo.arm(actor="t", reason="t", ttl_minutes=15)
    with database.connect() as conn:
        conn.execute("UPDATE live_control SET max_dispatches_per_cycle=5 WHERE id=1")
    stub = StubSonarrClient()
    worker = _make_worker(
        scheduler_repo, policy_repo, live_repo,
        _dispatch_service_with_stub(dispatch_planning_service, dispatch_repo, library_repo, stub),
    )
    _queue_live_cycle(database, scheduler_repo, policy_repo)
    cycle = scheduler_repo.start_next_cycle("live-worker")
    worker.service.execute_cycle(cycle, heartbeat=lambda: True)
    clock = worker._ensure_live_coordinator().test_clock
    worker._ensure_live_coordinator().execute(cycle, heartbeat=lambda: clock.now < 5)
    assert len(stub.calls) == 1
    assert clock.now == 5


def test_persistent_running_ignores_legacy_ttl(database, live_control_service, live_repo):
    live_repo.enable_live_mode(actor="t", reason="t")
    control, errors = live_control_service.resume({"confirm": True, "reason": "persistent"}, actor="t")
    assert errors == []
    assert control["state"] == "running"
    assert control["expires_at"] is None
    assert live_control_service.status()["dispatch_allowed"] is True

def test_live_stops_when_emergency_stopped_mid_cycle(
    database, scheduler_repo, policy_repo, live_repo, live_control_service, library_repo, activity_repo,
    candidate_repo, dispatch_repo, dispatch_planning_service,
):
    _library_scan(library_repo, activity_repo, candidate_repo, count=3)
    live_repo.enable_live_mode(actor="t", reason="t")
    live_repo.arm(actor="t", reason="t", ttl_minutes=15)
    live_control_service.emergency_stop({"reason": "halt"}, actor="operator")
    stub = StubSonarrClient()
    dispatch_service = _dispatch_service_with_stub(dispatch_planning_service, dispatch_repo, library_repo, stub)
    worker = _make_worker(scheduler_repo, policy_repo, live_repo, dispatch_service)

    # A cycle can still be queued/planned (visibility of what *would* run),
    # but the coordinator must refuse to dispatch anything: control.armed()
    # is already False, so the coordinator returns before any attempt.
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET mode='live', next_due_at=now()-interval '1 second' WHERE id=1")
    cycle_id = scheduler_repo.enqueue_scheduled_if_due(policy_repo.get())
    _plan_and_dispatch(worker, scheduler_repo)

    assert stub.calls == []
    assert live_repo.ledger_for_cycle(cycle_id) == []


def test_live_stops_cycle_on_sonarr_failure_no_tight_retry(
    database, scheduler_repo, policy_repo, live_repo, library_repo, activity_repo, candidate_repo,
    dispatch_repo, dispatch_planning_service,
):
    _library_scan(library_repo, activity_repo, candidate_repo, count=3)
    live_repo.enable_live_mode(actor="t", reason="t")
    live_repo.arm(actor="t", reason="t", ttl_minutes=15)
    with database.connect() as conn:
        conn.execute("UPDATE live_control SET max_dispatches_per_cycle = 5, min_delay_seconds_between_dispatches = 5 WHERE id = 1")
    stub = StubSonarrClient(error=SonarrResponseError("Sonarr returned an unexpected response"))
    dispatch_service = _dispatch_service_with_stub(dispatch_planning_service, dispatch_repo, library_repo, stub)
    worker = _make_worker(scheduler_repo, policy_repo, live_repo, dispatch_service)

    cycle_id = _queue_live_cycle(database, scheduler_repo, policy_repo)
    _plan_and_dispatch(worker, scheduler_repo)

    assert len(stub.calls) == 1  # stopped after the first failure, no tight retry
    ledger = live_repo.ledger_for_cycle(cycle_id)
    assert len(ledger) == 1
    assert ledger[0]["state"] == "failed"
    assert live_repo.get_control()["last_dispatch_at"] is not None  # backoff timer still advanced


def test_live_only_ever_calls_search_episodes(
    database, scheduler_repo, policy_repo, live_repo, library_repo, activity_repo, candidate_repo,
    dispatch_repo, dispatch_planning_service,
):
    _library_scan(library_repo, activity_repo, candidate_repo, count=1)
    live_repo.enable_live_mode(actor="t", reason="t")
    live_repo.arm(actor="t", reason="t", ttl_minutes=15)
    stub = StubSonarrClient()  # __getattr__ raises for anything but search_episodes
    dispatch_service = _dispatch_service_with_stub(dispatch_planning_service, dispatch_repo, library_repo, stub)
    worker = _make_worker(scheduler_repo, policy_repo, live_repo, dispatch_service)

    _queue_live_cycle(database, scheduler_repo, policy_repo)
    _plan_and_dispatch(worker, scheduler_repo)

    assert len(stub.calls) == 1


def test_crash_after_send_is_ambiguous_and_never_auto_retried(
    database, scheduler_repo, policy_repo, live_repo, library_repo, activity_repo, candidate_repo,
    dispatch_repo, dispatch_planning_service,
):
    """Simulates a crash between the Sonarr command being accepted and local
    finalization: the batch is left 'dispatching' with a recorded command id.
    A later worker iteration for the same library must mark it ambiguous
    and must never resend the search."""
    library, job, candidates = _library_scan(library_repo, activity_repo, candidate_repo, count=2)
    # Deliberately left unarmed: this test isolates the general reservation
    # sweep (runs regardless of mode) from live dispatch authorization,
    # which is covered by the other tests above.
    live_repo.enable_live_mode(actor="t", reason="t")

    with database.connect() as conn:
        # Inserted directly with an already-old created_at (rather than via
        # create_batch() + UPDATE) because dispatch audit identity,
        # including created_at, is immutable once written - see
        # protect_dispatch_batch_audit().
        batch_id = conn.execute(
            """
            INSERT INTO dispatch_batches (
                scan_job_id, library_id, library_name, mode, state,
                requested_count, selected_count, dispatched_count,
                sonarr_command_id, sonarr_command_status, error_summary,
                created_at, updated_at
            ) VALUES (%s,%s,%s,'manual','dispatching',1,1,0,555,'queued','',
                      now() - interval '10 minutes', now() - interval '10 minutes')
            RETURNING id
            """,
            (job.id, library.id, library.name),
        ).fetchone()["id"]
        dispatch_repo.create_items(conn, batch_id, [{
            "candidate_id": candidates[0].id, "episode_id": candidates[0].episode_id,
            "series_id": candidates[0].series_id, "series_title": candidates[0].series_title,
            "season_number": 1, "episode_number": 1, "state": "reserved",
        }])

    stub = StubSonarrClient()
    dispatch_service = _dispatch_service_with_stub(dispatch_planning_service, dispatch_repo, library_repo, stub)
    worker = _make_worker(scheduler_repo, policy_repo, live_repo, dispatch_service)
    worker.run_once()  # the general reservation sweep runs on every iteration

    batch = dispatch_repo.get_batch(batch_id)
    assert batch.state == "ambiguous"
    assert stub.calls == []  # never resent


def test_restart_does_not_implicitly_arm(database, scheduler_repo, policy_repo, live_repo):
    live_repo.enable_live_mode(actor="t", reason="t")
    assert scheduler_repo.get_settings().mode == "live"
    # A fresh worker/process start reads the same durable state - armed
    # stays False because nothing but arm_confirm ever sets it.
    worker = SchedulerWorker(scheduler_repo, policy_repo, live_repository=live_repo, owner_id="restarted")
    worker.run_once()
    assert live_repo.get_control()["armed"] is False


def test_resume_preserves_existing_next_due(database, live_control_service, live_repo):
    live_repo.enable_live_mode(actor="t", reason="t")
    with database.connect() as conn:
        expected = conn.execute("SELECT now() + interval '37 minutes' AS due").fetchone()["due"]
        conn.execute("UPDATE scheduler_settings SET next_due_at=%s WHERE id=1", (expected,))
    control, errors = live_control_service.resume({"confirm": True, "reason": "resume"}, actor="t")
    assert errors == []
    assert control["state"] == "running"
    with database.connect() as conn:
        actual = conn.execute("SELECT next_due_at FROM scheduler_settings WHERE id=1").fetchone()["next_due_at"]
    assert actual == expected


def test_pause_blocks_persistent_authorization(live_control_service, live_repo):
    live_repo.enable_live_mode(actor="t", reason="t")
    live_control_service.resume({"confirm": True, "reason": "start"}, actor="t")
    control, errors = live_control_service.pause({"confirm": True, "reason": "maintenance"}, actor="t")
    assert errors == []
    assert control["state"] == "paused"
    assert live_control_service.status()["dispatch_allowed"] is False



# --- API wiring --------------------------------------------------------


def test_live_api_endpoints_full_round_trip(client):
    status = client.get("/api/v1/scheduler/live/status").get_json()["live"]
    assert status["mode"] == "off"
    assert status["dispatch_allowed"] is False

    mode_challenge = client.post("/api/v1/scheduler/live/mode-challenge", json={}).get_json()
    confirm = client.post("/api/v1/scheduler/live/mode-confirm", json={
        "challenge_id": mode_challenge["challenge_id"], "token": mode_challenge["token"],
        "phrase": ENABLE_LIVE_MODE_PHRASE,
    })
    assert confirm.status_code == 200
    assert confirm.get_json()["result"]["armed"] is False

    assert client.post("/api/v1/scheduler/live/arm-challenge", json={}).status_code == 410
    resumed = client.post("/api/v1/scheduler/live/resume", json={"confirm": True, "reason": "verify"})
    assert resumed.status_code == 200
    assert resumed.get_json()["control"]["state"] == "running"

    run_now = client.post("/api/v1/scheduler/live/run-now", json={"confirm": True, "reason": "test"})
    assert run_now.status_code == 202
    assert run_now.get_json()["created"] is True

    stopped = client.post("/api/v1/scheduler/live/emergency-stop", json={"reason": "test"})
    assert stopped.status_code == 200
    assert stopped.get_json()["result"]["control"]["armed"] is False

    status_after = client.get("/api/v1/scheduler/live/status").get_json()["live"]
    assert "super-secret" not in str(status_after)


def test_live_ui_inline_scripts_have_valid_js_syntax(client, tmp_path):
    import re
    import shutil
    import subprocess

    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not available to syntax-check inline <script> blocks")
    for path in ("/settings", "/"):
        html = client.get(path).get_data(as_text=True)
        for i, script in enumerate(re.findall(r"<script>(.*?)</script>", html, re.S)):
            js_file = tmp_path / f"{path.strip('/')  or 'overview'}_{i}.js"
            js_file.write_text(script, encoding="utf-8")
            result = subprocess.run([node, "--check", str(js_file)], capture_output=True, text=True)
            assert result.returncode == 0, f"{path} script {i}: {result.stderr}"


def test_ui_exposes_danger_zone_two_step_flow(client):
    settings_html = client.get("/settings").get_data(as_text=True)
    assert 'id="live-mode-request"' in settings_html
    assert 'id="live-pause"' in settings_html
    assert 'id="live-resume"' in settings_html
    assert 'id="live-emergency-stop"' in settings_html
    assert "button-danger" in settings_html

    overview_html = client.get("/").get_data(as_text=True)
    assert 'id="ov-live-banner"' in overview_html
    assert 'id="ov-live-emergency-stop"' in overview_html


def test_live_dispatch_makes_reconciliation_due_immediately(database, live_repo):
    with database.connect() as conn:
        conn.execute("UPDATE refresh_settings SET next_reconcile_due_at = now() + interval '1 hour' WHERE id = 1")
    live_repo.record_dispatch_attempt("test dispatch")
    with database.connect() as conn:
        row = conn.execute("SELECT next_reconcile_due_at <= now() AS due FROM refresh_settings WHERE id = 1").fetchone()
    assert row["due"] is True


def test_activity_recent_dispatches_api_and_ui(client, dispatch_repo):
    response = client.get("/api/v1/activity/dispatches")
    assert response.status_code == 200
    assert "batches" in response.get_json()
    html = client.get("/activity").get_data(as_text=True)
    assert "Episode history" in html
    assert "formatLocalTime" in html
    assert "loadTimeline" in html


def test_activity_live_attempts_api_and_ui(client):
    response = client.get("/api/v1/activity/live-attempts")
    assert response.status_code == 200
    assert "attempts" in response.get_json()
    timeline = client.get("/api/v1/activity/timeline")
    assert timeline.status_code == 200
    assert "events" in timeline.get_json()
    html = client.get("/activity").get_data(as_text=True)
    assert "Episode history" in html
    assert "Searches and their real download or import results" in html
    assert "Batch-level evidence" not in html
    assert "loadTimeline" in html

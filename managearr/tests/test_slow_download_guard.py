"""Integration tests for the Sonarr-only slow-download guard: settings
API, monitoring poll, gated removal, and the safety races that mirror the
season-pack ``authorized_write`` test suite (pause/E-stop vs DELETE,
crash-after-accepted-DELETE, pool max_size=1, duplicate-attempt
prevention)."""
import os
import threading
import time

import pytest

from app.adapters.sonarr_client import SonarrPostAmbiguousError, SonarrPostRejectedError
from app.persistence.database import Database
from app.persistence.slow_download_repository import SlowDownloadRepository
from app.services.slow_download_service import SlowDownloadService

GiB = 1024 * 1024 * 1024
MiB = 1024 * 1024


class FakeQueueClient:
    """Controllable fake Sonarr client: queue contents + delete outcome."""

    records = []
    delete_calls = []
    delete_error = None
    delete_hook = None

    def __init__(self, *args, **kwargs):
        pass

    def get_queue_details(self, *, page=1, page_size=100):
        return {"records": type(self).records, "total_records": len(type(self).records), "page": page, "page_size": page_size}

    def delete_queue_record(self, queue_id, *, remove_from_client, blocklist, skip_redownload=False):
        if type(self).delete_hook:
            type(self).delete_hook()
        type(self).delete_calls.append(
            {"queue_id": queue_id, "remove_from_client": remove_from_client, "blocklist": blocklist, "skip_redownload": skip_redownload}
        )
        if type(self).delete_error:
            raise type(self).delete_error("test outcome")


def queue_record(queue_id=1, status="downloading", size=10 * GiB, sizeleft=5 * GiB, title="Show.S01E01", download_id="dl-1"):
    return {
        "queue_id": queue_id, "episode_ids": [], "status": status, "tracked_state": "downloading",
        "download_id": download_id, "added": None, "title": title, "size": size, "sizeleft": sizeleft,
        "timeleft": None, "error_message": None, "status_messages": [],
    }


def reset_fake(status=None, delete_error=None, delete_hook=None):
    FakeQueueClient.records = []
    FakeQueueClient.delete_calls = []
    FakeQueueClient.delete_error = delete_error
    FakeQueueClient.delete_hook = delete_hook


def make_sonarr_library(library_repo):
    return library_repo.create({"name": "Sonarr", "type": "sonarr", "url": "http://sonarr", "api_key": "secret", "enabled": True})


# --- Settings: defaults + enable confirmation gates -------------------------

def test_settings_default_monitoring_on_auto_removal_off(client, library_repo):
    library = make_sonarr_library(library_repo)
    settings = client.get(f"/api/v1/libraries/{library.id}/slow-download/settings").get_json()["settings"]
    assert settings["monitoring_enabled"] is True
    assert settings["auto_removal_enabled"] is False
    assert "secret" not in str(settings)


def test_enabling_auto_removal_requires_confirm_and_reason(client, library_repo):
    library = make_sonarr_library(library_repo)
    response = client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={"auto_removal_enabled": True})
    assert response.status_code == 400
    assert "confirm" in str(response.get_json()["errors"]).lower()


def test_enabling_auto_removal_requires_live_armed(client, library_repo):
    library = make_sonarr_library(library_repo)
    response = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "testing"},
    )
    assert response.status_code == 400
    assert "armed and running" in str(response.get_json()["errors"])


def test_enabling_auto_removal_succeeds_once_live_armed(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET mode='live', updated_at=now() WHERE id=1")
    app.extensions["managearr"]["live_repo"].set_authorization_state("running", actor="test", reason="guard test")
    response = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "testing"},
    )
    assert response.status_code == 200
    assert response.get_json()["settings"]["auto_removal_enabled"] is True


def test_disabling_auto_removal_never_requires_confirmation(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET mode='live', updated_at=now() WHERE id=1")
    app.extensions["managearr"]["live_repo"].set_authorization_state("running", actor="test", reason="guard test")
    client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={"auto_removal_enabled": True, "confirm": True, "reason": "x"})
    response = client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={"auto_removal_enabled": False})
    assert response.status_code == 200 and response.get_json()["settings"]["auto_removal_enabled"] is False


def test_loosening_thresholds_requires_confirm_only_when_auto_removal_enabled(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    # Monitoring-only (auto_removal disabled): any threshold change is safe
    # to make freely since nothing destructive can happen yet.
    response = client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={"strikes_required": 1})
    assert response.status_code == 200


@pytest.mark.parametrize("field,old,new", [
    ("strikes_required", 2, 1),  # fewer strikes needed
    ("no_progress_window_minutes", 30, 10),  # shorter window
    ("very_slow_min_remaining_bytes", 512 * MiB, 10 * MiB),  # lower floor
    ("progress_epsilon_bytes", 1 * MiB, 64 * MiB),  # bigger epsilon: harder to count as real progress
    ("very_slow_rate_bytes_per_second", 50 * 1024, 500 * 1024),  # higher floor: more downloads count as "too slow"
])
def test_loosening_while_auto_removal_enabled_requires_confirm_and_reason(client, app, database, library_repo, field, old, new):
    library = make_sonarr_library(library_repo)
    _arm_live_for_settings(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "initial enable"},
    )
    response = client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={field: new})
    assert response.status_code == 400
    assert "confirm" in str(response.get_json()["errors"]).lower()
    confirmed = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={field: new, "confirm": True, "reason": f"loosening {field}"},
    )
    assert confirmed.status_code == 200


@pytest.mark.parametrize("field,old,new", [
    ("strikes_required", 2, 5),  # more strikes needed - more protective
    ("no_progress_window_minutes", 30, 120),  # longer window - more protective
])
def test_tightening_while_auto_removal_enabled_never_requires_confirm(client, app, database, library_repo, field, old, new):
    library = make_sonarr_library(library_repo)
    _arm_live_for_settings(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "initial enable"},
    )
    response = client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={field: new})
    assert response.status_code == 200


def _arm_live_for_settings(app, database):
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET mode='live', updated_at=now() WHERE id=1")
    app.extensions["managearr"]["live_repo"].set_authorization_state("running", actor="test", reason="guard test")


def test_settings_optimistic_concurrency_rejects_stale_revision(client, library_repo):
    library = make_sonarr_library(library_repo)
    current = client.get(f"/api/v1/libraries/{library.id}/slow-download/settings").get_json()["settings"]
    client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={"strikes_required": 3})
    stale = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"strikes_required": 4, "revision": current["revision"]},
    )
    assert stale.status_code == 400
    assert "changed by someone else" in str(stale.get_json()["errors"])


# --- Monitoring (read-only) --------------------------------------------------

def test_poll_library_tracks_active_item_and_exempts_paused(client, app, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [
        queue_record(queue_id=1, status="downloading"),
        queue_record(queue_id=2, status="paused"),
    ]
    service.poll_library(library.id)
    items = {i["sonarr_queue_id"]: i for i in slow_download_repo.list_current(library.id)}
    assert items[1]["classification"] == "grace"
    assert items[2]["classification"] == "exempt"


def test_poll_library_clears_disappeared_item_without_treating_as_import(client, app, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=1)]
    service.poll_library(library.id)
    FakeQueueClient.records = []
    service.poll_library(library.id)
    item = slow_download_repo.list_current(library.id)[0]
    assert item["classification"] == "removed"
    assert item["removal_outcome"] is None
    actions = [a["action"] for a in slow_download_repo.recent_actions(library.id)]
    assert "cleared_disappeared" in actions


def test_monitoring_disabled_does_not_poll(client, app, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={"monitoring_enabled": False})
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=1)]
    result = service.poll_library(library.id)
    assert result.get("skipped") == "monitoring disabled"
    assert slow_download_repo.list_current(library.id) == []


def test_status_endpoint_surfaces_exempt_and_healthy_rows(client, app, library_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=1, status="downloading"), queue_record(queue_id=2, status="queued")]
    service.poll_library(library.id)
    payload = client.get("/api/v1/slow-download/status").get_json()
    lib_entry = next(l for l in payload["libraries"] if l["library_id"] == library.id)
    classifications = {i["sonarr_queue_id"]: i["classification"] for i in lib_entry["items"]}
    assert classifications[1] == "grace"
    assert classifications[2] == "exempt"


# --- Gated removal -----------------------------------------------------------

def _arm_live(app, database):
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET mode='live', updated_at=now() WHERE id=1")
    app.extensions["managearr"]["live_repo"].set_authorization_state("running", actor="test", reason="guard test")


def _force_removal_pending(database, library_id, queue_id=1):
    with database.connect() as conn:
        row = conn.execute(
            """
            INSERT INTO slow_download_queue_items (
                library_id, sonarr_queue_id, download_id, title, status,
                classification, reason, stall_strike_count, first_seen_at, last_seen_at
            ) VALUES (%s, %s, 'dl-1', 'Show.S01E01', 'downloading', 'removal_pending', 'test evidence', 2, now(), now())
            RETURNING id
            """,
            (library_id, queue_id),
        ).fetchone()
    return row["id"]


def test_removal_blocked_when_auto_removal_disabled(client, app, database, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    item_id = _force_removal_pending(database, library.id)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    assert FakeQueueClient.delete_calls == []
    item = slow_download_repo.get(item_id)
    assert item["classification"] == "removal_pending"


def test_removal_blocked_when_not_live(client, app, library_repo, database, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": False},
    )
    with database.connect() as conn:
        conn.execute(
            "INSERT INTO slow_download_settings (library_id, auto_removal_enabled) VALUES (%s, TRUE) "
            "ON CONFLICT (library_id) DO UPDATE SET auto_removal_enabled = TRUE",
            (library.id,),
        )
    item_id = _force_removal_pending(database, library.id)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    assert FakeQueueClient.delete_calls == []


def test_removal_completes_when_live_and_auto_removal_enabled(client, app, database, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test"},
    )
    item_id = _force_removal_pending(database, library.id, queue_id=42)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert len(outcomes) == 1 and outcomes[0]["classification"] == "removed"
    assert FakeQueueClient.delete_calls == [
        {"queue_id": 42, "remove_from_client": True, "blocklist": True, "skip_redownload": False}
    ]
    actions = [a["action"] for a in slow_download_repo.recent_actions(library.id)]
    assert "removal_attempt_started" in actions and "removal_completed" in actions


def test_at_most_one_removal_per_call(client, app, database, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test"},
    )
    _force_removal_pending(database, library.id, queue_id=1)
    _force_removal_pending(database, library.id, queue_id=2)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert len(outcomes) == 1
    assert len(FakeQueueClient.delete_calls) == 1


@pytest.mark.parametrize("error,outcome", [(SonarrPostAmbiguousError, "ambiguous"), (SonarrPostRejectedError, "rejected")])
def test_typed_delete_outcomes_persist_safely(client, app, database, library_repo, slow_download_repo, monkeypatch, error, outcome):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test"},
    )
    item_id = _force_removal_pending(database, library.id)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake(delete_error=error)
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes[0]["removal_outcome"] == outcome
    item = slow_download_repo.get(item_id)
    assert item["removal_outcome"] == outcome


def test_ambiguous_outcome_is_never_automatically_retried(client, app, database, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test"},
    )
    item_id = _force_removal_pending(database, library.id)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake(delete_error=SonarrPostAmbiguousError)
    service.attempt_removals(max_removals=1)
    reset_fake()  # a later poll would succeed if retried - it must not be
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    assert FakeQueueClient.delete_calls == []


def test_queue_id_mismatch_cannot_delete_wrong_record(client, app, database, library_repo, slow_download_repo, monkeypatch):
    """A stale/overwritten sonarr_queue_id must never be sent to DELETE
    without a final row-locked read of the current value."""
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test"},
    )
    item_id = _force_removal_pending(database, library.id, queue_id=99)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    service.attempt_removals(max_removals=1)
    assert FakeQueueClient.delete_calls[0]["queue_id"] == 99


# --- Races -------------------------------------------------------------------

class BlockingClient(FakeQueueClient):
    entered = threading.Event()
    release = threading.Event()

    def delete_queue_record(self, queue_id, *, remove_from_client, blocklist, skip_redownload=False):
        type(self).entered.set()
        assert type(self).release.wait(3)
        return super().delete_queue_record(queue_id, remove_from_client=remove_from_client, blocklist=blocklist, skip_redownload=skip_redownload)


def test_pause_waits_for_attempt_boundary_then_delete_proceeds(client, app, database, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test"},
    )
    _force_removal_pending(database, library.id)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", BlockingClient)
    BlockingClient.entered = threading.Event()
    BlockingClient.release = threading.Event()
    reset_fake()
    outcome = {}
    pause_done = threading.Event()

    def remove():
        outcome["result"] = service.attempt_removals(max_removals=1)

    def pause():
        app.extensions["managearr"]["live_repo"].set_authorization_state("paused", actor="test", reason="race")
        pause_done.set()

    remove_thread = threading.Thread(target=remove)
    remove_thread.start()
    assert BlockingClient.entered.wait(3)
    pause_thread = threading.Thread(target=pause)
    pause_thread.start()
    time.sleep(0.1)
    # Pause must wait behind the already-committed attempt marker + bounded
    # DELETE rather than racing in front of it.
    assert not pause_done.is_set()
    BlockingClient.release.set()
    remove_thread.join(3)
    pause_thread.join(3)
    assert not remove_thread.is_alive() and not pause_thread.is_alive()
    assert outcome["result"][0]["classification"] == "removed"
    assert pause_done.is_set()


def test_crash_after_accepted_delete_is_durably_ambiguous_and_never_resent(client, app, database, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test"},
    )
    item_id = _force_removal_pending(database, library.id)

    class SimulatedProcessDeath(BaseException):
        pass

    class CrashAfterAcceptedClient(FakeQueueClient):
        def delete_queue_record(self, *args, **kwargs):
            super().delete_queue_record(*args, **kwargs)
            raise SimulatedProcessDeath("process died after Sonarr accepted the DELETE")

    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", CrashAfterAcceptedClient)
    reset_fake()

    with pytest.raises(SimulatedProcessDeath):
        service._attempt_removal(library_repo.get(library.id), slow_download_repo.get(item_id), app.extensions["managearr"]["live_repo"].get_control()["authorization_generation"])

    marker = None
    with database.connect() as conn:
        marker = conn.execute("SELECT started_at FROM slow_download_removal_attempts WHERE queue_item_id = %s", (item_id,)).fetchone()
    assert marker is not None

    # A fresh service instance models a restart: the committed marker must
    # make this permanently ambiguous, never silently retried.
    restarted = SlowDownloadService(
        SlowDownloadRepository(database), library_repo, app.extensions["managearr"]["live_repo"],
        app.extensions["managearr"]["scheduler_repo"], client_factory=CrashAfterAcceptedClient,
    )
    outcomes = restarted.attempt_removals(max_removals=1)
    assert outcomes == []
    item = slow_download_repo.get(item_id)
    assert item["classification"] == "removal_pending"
    assert len(FakeQueueClient.delete_calls) == 1  # never resent


def test_dedicated_marker_connection_works_with_pool_max_size_one(client, app, database, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test"},
    )
    item_id = _force_removal_pending(database, library.id)
    single = Database(
        host=os.environ.get("MANAGEARR_TEST_DB_HOST", "127.0.0.1"),
        port=int(os.environ.get("MANAGEARR_TEST_DB_PORT", "5432")),
        dbname=os.environ.get("MANAGEARR_TEST_DB_NAME", "managearr_test"),
        user=os.environ.get("MANAGEARR_TEST_DB_USER", "managearr_test"),
        password=os.environ.get("MANAGEARR_TEST_DB_PASSWORD", "testpass123"),
        sslmode="disable", min_size=1, max_size=1, connect_timeout=2,
    )
    single.wait_ready(timeout_seconds=5)
    reset_fake()
    try:
        service = SlowDownloadService(
            SlowDownloadRepository(single), library_repo, app.extensions["managearr"]["live_repo"],
            app.extensions["managearr"]["scheduler_repo"], client_factory=FakeQueueClient,
        )
        outcomes = service.attempt_removals(max_removals=1)
        assert len(outcomes) == 1 and outcomes[0]["classification"] == "removed"
    finally:
        single.close()


def test_concurrent_worker_cycles_never_double_delete(client, app, database, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test"},
    )
    _force_removal_pending(database, library.id)
    reset_fake()
    generation = app.extensions["managearr"]["live_repo"].get_control()["authorization_generation"]

    results = []

    def run():
        service = SlowDownloadService(
            SlowDownloadRepository(database), library_repo, app.extensions["managearr"]["live_repo"],
            app.extensions["managearr"]["scheduler_repo"], client_factory=FakeQueueClient,
        )
        results.append(service.attempt_removals(max_removals=1))

    threads = [threading.Thread(target=run) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
    assert len(FakeQueueClient.delete_calls) == 1

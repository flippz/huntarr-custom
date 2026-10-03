"""Integration tests for the Sonarr-only slow-download guard: settings
API, monitoring poll, gated removal, and the safety races that mirror the
season-pack ``authorized_write`` test suite (pause/E-stop vs DELETE,
crash-after-accepted-DELETE, pool max_size=1, duplicate-attempt
prevention)."""
import os
import threading
import time

import pytest

from app.adapters.sonarr_client import SonarrConnectionError, SonarrPostAmbiguousError, SonarrPostRejectedError
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


def queue_record(
    queue_id=1, status="downloading", size=10 * GiB, sizeleft=5 * GiB, title="Show.S01E01", download_id="dl-1",
    tracked_state="downloading", tracked_status=None,
):
    return {
        "queue_id": queue_id, "episode_ids": [], "status": status, "tracked_state": tracked_state,
        "tracked_status": tracked_status, "download_id": download_id, "added": None, "title": title,
        "size": size, "sizeleft": sizeleft, "timeleft": None, "error_message": None, "status_messages": [],
    }


def reset_fake(delete_error=None, delete_hook=None):
    # Preserve an explicitly prepared live queue record (used by final
    # pre-DELETE revalidation tests); monitoring tests overwrite records
    # directly, and revalidation tests overwrite them after calling this.
    FakeQueueClient.records = list(FakeQueueClient.records)
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
    response = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings", json={"auto_removal_enabled": True, "revision": 0}
    )
    assert response.status_code == 400
    assert "confirm" in str(response.get_json()["errors"]).lower()


def test_enabling_auto_removal_requires_live_armed(client, library_repo):
    library = make_sonarr_library(library_repo)
    response = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "testing", "revision": 0},
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "testing", "revision": 0},
    )
    assert response.status_code == 200
    assert response.get_json()["settings"]["auto_removal_enabled"] is True


def test_disabling_auto_removal_never_requires_confirmation(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET mode='live', updated_at=now() WHERE id=1")
    app.extensions["managearr"]["live_repo"].set_authorization_state("running", actor="test", reason="guard test")
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "x", "revision": 0},
    )
    response = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings", json={"auto_removal_enabled": False, "revision": 1}
    )
    assert response.status_code == 200 and response.get_json()["settings"]["auto_removal_enabled"] is False


def test_loosening_thresholds_requires_confirm_only_when_auto_removal_enabled(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    # Monitoring-only (auto_removal disabled): any threshold change is safe
    # to make freely since nothing destructive can happen yet.
    response = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings", json={"strikes_required": 1, "revision": 0}
    )
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "initial enable", "revision": 0},
    )
    response = client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={field: new, "revision": 1})
    assert response.status_code == 400
    assert "confirm" in str(response.get_json()["errors"]).lower()
    confirmed = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={field: new, "confirm": True, "reason": f"loosening {field}", "revision": 1},
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "initial enable", "revision": 0},
    )
    response = client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={field: new, "revision": 1})
    assert response.status_code == 200


@pytest.mark.parametrize("field", ["remove_from_client", "blocklist", "skip_redownload"])
def test_boolean_destructive_loosening_while_auto_removal_enabled_requires_confirm(
    client, app, database, library_repo, field
):
    library = make_sonarr_library(library_repo)
    _arm_live_for_settings(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, field: False, "confirm": True, "reason": "initial enable", "revision": 0},
    )
    response = client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={field: True, "revision": 1})
    assert response.status_code == 400
    assert "confirm" in str(response.get_json()["errors"]).lower()
    confirmed = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={field: True, "confirm": True, "reason": f"loosening {field}", "revision": 1},
    )
    assert confirmed.status_code == 200
    assert confirmed.get_json()["settings"][field] is True


@pytest.mark.parametrize("field", ["remove_from_client", "blocklist", "skip_redownload"])
def test_boolean_protective_tightening_while_auto_removal_enabled_never_requires_confirm(
    client, app, database, library_repo, field
):
    library = make_sonarr_library(library_repo)
    _arm_live_for_settings(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, field: True, "confirm": True, "reason": "initial enable", "revision": 0},
    )
    response = client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={field: False, "revision": 1})
    assert response.status_code == 200
    assert response.get_json()["settings"][field] is False


def test_disabling_monitoring_while_auto_removal_enabled_safely_disables_removal(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    _arm_live_for_settings(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "initial enable", "revision": 0},
    )
    # Disabling monitoring is itself protective (it stops evidence from ever
    # reaching removal_pending) but leaving auto_removal_enabled=True active
    # with no monitoring would let an already-stale removal_pending item
    # still be auto-deleted with nothing watching it - so it must never
    # require confirm, and must force auto_removal_enabled off too.
    response = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings", json={"monitoring_enabled": False, "revision": 1}
    )
    assert response.status_code == 200
    settings = response.get_json()["settings"]
    assert settings["monitoring_enabled"] is False
    assert settings["auto_removal_enabled"] is False


def test_enabling_both_monitoring_off_and_auto_removal_in_one_request_is_rejected_safely(
    client, app, database, library_repo
):
    library = make_sonarr_library(library_repo)
    _arm_live_for_settings(app, database)
    response = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"monitoring_enabled": False, "auto_removal_enabled": True, "confirm": True, "reason": "x", "revision": 0},
    )
    assert response.status_code == 200
    settings = response.get_json()["settings"]
    assert settings["monitoring_enabled"] is False
    assert settings["auto_removal_enabled"] is False


def _arm_live_for_settings(app, database):
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET mode='live', updated_at=now() WHERE id=1")
    app.extensions["managearr"]["live_repo"].set_authorization_state("running", actor="test", reason="guard test")


# --- Settings-enable race: Pause/E-stop vs. enabling auto-removal ----------

def test_atomic_recheck_rejects_enable_even_if_preliminary_check_is_stale(client, app, database, library_repo, monkeypatch):
    """A stale, non-transactional preliminary 'is Live armed?' read must
    never be the only gate. Here the preliminary check is monkeypatched to
    lie that Live is still allowed (simulating it having read state just
    before a Pause committed); the repository-level atomic recheck inside
    the same transaction as the write must still catch the real, current
    paused state and reject the enable."""
    library = make_sonarr_library(library_repo)
    _arm_live_for_settings(app, database)
    app.extensions["managearr"]["live_repo"].set_authorization_state("paused", actor="test", reason="already paused")
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "_live_status", lambda: {"allowed": True, "reasons": [], "generation": 0})
    updated, errors = service.update_settings(
        library.id, {"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0}
    )
    assert updated is None
    assert any("Live" in e for e in errors)
    assert service.repo.settings(library.id)["auto_removal_enabled"] is False


def test_pause_blocks_on_the_enable_transaction_lock_rather_than_racing_in(
    client, app, database, library_repo, monkeypatch
):
    """Enabling auto-removal locks and rechecks scheduler/live state inside
    its own transaction (see SlowDownloadRepository.update_settings). A
    concurrent Pause must block behind that lock - never commit in the gap
    between the enable's check and its commit - exactly like the existing
    Pause-vs-DELETE race (test_pause_waits_for_attempt_boundary_then_delete_
    proceeds)."""
    library = make_sonarr_library(library_repo)
    _arm_live_for_settings(app, database)
    repo = app.extensions["managearr"]["slow_download_repo"]
    entered = threading.Event()
    release = threading.Event()

    def on_locked():
        entered.set()
        assert release.wait(3)

    original_update_settings = repo.update_settings

    def patched(*args, **kwargs):
        kwargs["_on_locked"] = on_locked
        return original_update_settings(*args, **kwargs)

    monkeypatch.setattr(repo, "update_settings", patched)

    outcome = {}

    def enable():
        response = client.put(
            f"/api/v1/libraries/{library.id}/slow-download/settings",
            json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
        )
        outcome["status"] = response.status_code
        outcome["body"] = response.get_json()

    pause_done = threading.Event()

    def pause():
        app.extensions["managearr"]["live_repo"].set_authorization_state("paused", actor="test", reason="race")
        pause_done.set()

    enable_thread = threading.Thread(target=enable)
    enable_thread.start()
    assert entered.wait(3)
    pause_thread = threading.Thread(target=pause)
    pause_thread.start()
    time.sleep(0.1)
    assert not pause_done.is_set()
    release.set()
    enable_thread.join(3)
    pause_thread.join(3)
    assert not enable_thread.is_alive() and not pause_thread.is_alive()
    assert outcome["status"] == 200
    assert outcome["body"]["settings"]["auto_removal_enabled"] is True
    assert pause_done.is_set()
    # Pause committed strictly after the enable - the Live state is now
    # paused despite auto_removal_enabled being True, so a subsequent
    # removal attempt remains blocked by the authorized_removal recheck.
    assert app.extensions["managearr"]["live_repo"].get_control()["state"] == "paused"


def test_estop_blocks_on_the_enable_transaction_lock_rather_than_racing_in(
    client, app, database, library_repo, monkeypatch
):
    library = make_sonarr_library(library_repo)
    _arm_live_for_settings(app, database)
    repo = app.extensions["managearr"]["slow_download_repo"]
    entered = threading.Event()
    release = threading.Event()

    def on_locked():
        entered.set()
        assert release.wait(3)

    original_update_settings = repo.update_settings

    def patched(*args, **kwargs):
        kwargs["_on_locked"] = on_locked
        return original_update_settings(*args, **kwargs)

    monkeypatch.setattr(repo, "update_settings", patched)

    outcome = {}

    def enable():
        response = client.put(
            f"/api/v1/libraries/{library.id}/slow-download/settings",
            json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
        )
        outcome["status"] = response.status_code
        outcome["body"] = response.get_json()

    estop_done = threading.Event()

    def estop():
        app.extensions["managearr"]["live_repo"].emergency_stop(actor="test", reason="race")
        estop_done.set()

    enable_thread = threading.Thread(target=enable)
    enable_thread.start()
    assert entered.wait(3)
    estop_thread = threading.Thread(target=estop)
    estop_thread.start()
    time.sleep(0.1)
    assert not estop_done.is_set()
    release.set()
    enable_thread.join(3)
    estop_thread.join(3)
    assert not enable_thread.is_alive() and not estop_thread.is_alive()
    assert outcome["status"] == 200
    assert outcome["body"]["settings"]["auto_removal_enabled"] is True
    assert estop_done.is_set()
    assert app.extensions["managearr"]["live_repo"].get_control()["state"] == "emergency_stopped"


def test_resume_after_a_correctly_rejected_enable_does_not_silently_activate_it(
    client, app, database, library_repo
):
    """A Resume must never retroactively "complete" an enable that the
    atomic recheck already rejected - the operator must explicitly re-enable
    after Resume."""
    library = make_sonarr_library(library_repo)
    _arm_live_for_settings(app, database)
    app.extensions["managearr"]["live_repo"].set_authorization_state("paused", actor="test", reason="paused")
    response = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    assert response.status_code == 400
    app.extensions["managearr"]["live_repo"].set_authorization_state("running", actor="test", reason="resume")
    settings = client.get(f"/api/v1/libraries/{library.id}/slow-download/settings").get_json()["settings"]
    assert settings["auto_removal_enabled"] is False


def test_settings_optimistic_concurrency_rejects_stale_revision(client, library_repo):
    library = make_sonarr_library(library_repo)
    current = client.get(f"/api/v1/libraries/{library.id}/slow-download/settings").get_json()["settings"]
    client.put(f"/api/v1/libraries/{library.id}/slow-download/settings", json={"strikes_required": 3, "revision": 0})
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


@pytest.mark.parametrize("tracked_state,tracked_status", [("importblocked", None), (None, "warning"), (None, "error")])
def test_poll_library_hard_exempts_import_blocked_or_warning_error_tracked_queue_items(
    client, app, library_repo, slow_download_repo, monkeypatch, tracked_state, tracked_status
):
    """A queue record can report status "downloading" while Sonarr has
    already flagged it trackedDownloadState=importBlocked or
    trackedDownloadStatus=warning/error - exactly the import-failure
    reason policy's territory (see test_import_failure_guard.py). This
    guard must never accumulate stall/slow-speed evidence against it."""
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [
        queue_record(queue_id=1, status="downloading", tracked_state=tracked_state, tracked_status=tracked_status)
    ]
    service.poll_library(library.id)
    item = slow_download_repo.list_current(library.id)[0]
    assert item["classification"] == "exempt"
    assert item["stall_strike_count"] == 0


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
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings", json={"monitoring_enabled": False, "revision": 0}
    )
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
        existing = conn.execute(
            "SELECT count(*) AS c FROM slow_download_queue_items WHERE library_id=%s", (library_id,)
        ).fetchone()["c"]
        if existing == 0:
            FakeQueueClient.records = []
        FakeQueueClient.records.append(queue_record(queue_id=queue_id))
        row = conn.execute(
            """
            INSERT INTO slow_download_queue_items (
                library_id, sonarr_queue_id, download_id, title, status, size_bytes, sizeleft_bytes,
                classification, reason, stall_strike_count, first_seen_at, last_seen_at
            ) VALUES (%s, %s, 'dl-1', 'Show.S01E01', 'downloading', %s, %s,
                      'removal_pending', 'test evidence', 2, now(), now())
            RETURNING id
            """,
            (library_id, queue_id, 10 * GiB, 5 * GiB),
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
        json={"auto_removal_enabled": False, "revision": 0},
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    _force_removal_pending(database, library.id)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", BlockingClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
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
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
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
        read_only_client_factory=FakeQueueClient,
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
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
            read_only_client_factory=FakeQueueClient,
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
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    _force_removal_pending(database, library.id)
    reset_fake()
    generation = app.extensions["managearr"]["live_repo"].get_control()["authorization_generation"]

    results = []

    def run():
        service = SlowDownloadService(
            SlowDownloadRepository(database), library_repo, app.extensions["managearr"]["live_repo"],
            app.extensions["managearr"]["scheduler_repo"], client_factory=FakeQueueClient,
            read_only_client_factory=FakeQueueClient,
        )
        results.append(service.attempt_removals(max_removals=1))

    threads = [threading.Thread(target=run) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
    assert len(FakeQueueClient.delete_calls) == 1


def test_truncated_bounded_queue_never_clears_unseen_records(app, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["slow_download"]

    class TruncatedQueueClient(FakeQueueClient):
        def get_queue_details(self, *, page=1, page_size=100):
            return {
                "records": [queue_record(queue_id=page)],
                "total_records": 1001,
                "page": page,
                "page_size": page_size,
            }

    existing = slow_download_repo.record_observation(
        library.id, queue_record(queue_id=9999), slow_download_repo.settings(library.id)
    )
    monkeypatch.setattr(service, "read_only_client_factory", TruncatedQueueClient)
    result = service.poll_library(library.id)
    assert "exceeded" in result["error"]
    assert result["cleared"] == 0
    assert slow_download_repo.get(existing["id"])["classification"] != "removed"


def test_delete_uses_settings_bound_under_authorization_lock(client, app, database, library_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    enabled = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    ).get_json()["settings"]
    changed = client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"revision": enabled["revision"], "remove_from_client": False, "blocklist": True},
    )
    assert changed.status_code == 200
    _force_removal_pending(database, library.id, queue_id=77)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    service.attempt_removals(max_removals=1)
    assert FakeQueueClient.delete_calls == [{
        "queue_id": 77,
        "remove_from_client": False,
        "blocklist": True,
        "skip_redownload": False,
    }]


# --- Immediate pre-DELETE revalidation: the stored evidence alone is never
# sufficient. _attempt_removal re-reads the exact Sonarr queue record while
# authorization/settings/library are locked and must abort - safely resetting
# tracking, never inserting the no-retry marker, never calling DELETE -
# whenever that live read no longer proves the stored evidence is current. ---

def _assert_no_marker_and_no_delete(database, item_id):
    with database.connect() as conn:
        marker = conn.execute(
            "SELECT 1 FROM slow_download_removal_attempts WHERE queue_item_id = %s", (item_id,)
        ).fetchone()
    assert marker is None
    assert FakeQueueClient.delete_calls == []


def test_revalidation_aborts_when_queue_record_disappeared(client, app, database, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    item_id = _force_removal_pending(database, library.id, queue_id=201)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = []  # the queue record is gone by the time of the final live read
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)
    item = slow_download_repo.get(item_id)
    assert item["classification"] == "removed"
    assert item["stall_strike_count"] == 0


@pytest.mark.parametrize("status,sizeleft", [("completed", 0), ("paused", 5 * GiB), ("importing", 5 * GiB)])
def test_revalidation_aborts_when_status_no_longer_actively_downloading(
    client, app, database, library_repo, slow_download_repo, monkeypatch, status, sizeleft
):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    item_id = _force_removal_pending(database, library.id, queue_id=202)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=202, status=status, sizeleft=sizeleft)]
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)
    item = slow_download_repo.get(item_id)
    assert item["classification"] == "exempt"


@pytest.mark.parametrize("tracked_state,tracked_status", [("importblocked", None), (None, "warning"), (None, "error")])
def test_revalidation_aborts_when_live_record_is_hard_exempt_tracked(
    client, app, database, library_repo, slow_download_repo, monkeypatch, tracked_state, tracked_status
):
    """Even when status/sizeleft/download_id all still match stored
    evidence exactly, a live record Sonarr has since flagged
    trackedDownloadState=importBlocked or trackedDownloadStatus=warning/
    error must never be deleted by this guard - that is the import-failure
    reason policy's exclusive territory."""
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    item_id = _force_removal_pending(database, library.id, queue_id=209)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [
        queue_record(queue_id=209, tracked_state=tracked_state, tracked_status=tracked_status)
    ]
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)
    item = slow_download_repo.get(item_id)
    assert item["classification"] == "exempt"


def test_revalidation_aborts_when_download_identity_changed(client, app, database, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    item_id = _force_removal_pending(database, library.id, queue_id=203)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    # Same queue slot, but Sonarr has rotated it onto a different client
    # download - the stored evidence no longer describes this download.
    FakeQueueClient.records = [queue_record(queue_id=203, download_id="dl-rotated", sizeleft=5 * GiB)]
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)
    item = slow_download_repo.get(item_id)
    assert item["classification"] == "ambiguous"


@pytest.mark.parametrize("live_sizeleft", [1 * GiB, 6 * GiB])  # material decrease, and an increase
def test_revalidation_aborts_when_progress_changed_materially(
    client, app, database, library_repo, slow_download_repo, monkeypatch, live_sizeleft
):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    item_id = _force_removal_pending(database, library.id, queue_id=204)  # stored sizeleft_bytes = 5 GiB
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=204, sizeleft=live_sizeleft)]
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)
    item = slow_download_repo.get(item_id)
    assert item["classification"] == "healthy"
    assert item["stall_strike_count"] == 0 and item["very_slow_strike_count"] == 0


def test_revalidation_fails_closed_on_sonarr_error(client, app, database, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    item_id = _force_removal_pending(database, library.id, queue_id=205)

    class UnreachableClient(FakeQueueClient):
        def get_queue_details(self, *, page=1, page_size=100):
            raise SonarrConnectionError("connection refused")

    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", UnreachableClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)
    item = slow_download_repo.get(item_id)
    # Network uncertainty must never resolve to "healthy"/"removed" (both
    # would silently clear real evidence) - it stays ambiguous for manual
    # review, same as an uncertain DELETE outcome.
    assert item["classification"] == "ambiguous"


def test_revalidation_passes_when_live_queue_matches_stored_evidence_exactly(
    client, app, database, library_repo, slow_download_repo, monkeypatch
):
    """Sanity check: an exact, unchanged match is the only case that
    reaches the marker + DELETE - confirms the above aborts are each
    caused by the specific field under test, not an overly strict check."""
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    item_id = _force_removal_pending(database, library.id, queue_id=206)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert len(outcomes) == 1 and outcomes[0]["classification"] == "removed"
    assert FakeQueueClient.delete_calls == [
        {"queue_id": 206, "remove_from_client": True, "blocklist": True, "skip_redownload": False}
    ]
    with database.connect() as conn:
        marker = conn.execute(
            "SELECT 1 FROM slow_download_removal_attempts WHERE queue_item_id = %s", (item_id,)
        ).fetchone()
    assert marker is not None


# --- Worker lease renewal / loss during bounded polling + removal ----------

class PagedQueueClient(FakeQueueClient):
    """Serves ``records`` split across real QUEUE_PAGE_SIZE pages, unlike
    FakeQueueClient which always returns everything on page 1 - needed to
    exercise the heartbeat call made between pages."""

    def get_queue_details(self, *, page=1, page_size=100):
        start = (page - 1) * page_size
        chunk = type(self).records[start:start + page_size]
        return {"records": chunk, "total_records": len(type(self).records), "page": page, "page_size": page_size}


def test_poll_library_renews_heartbeat_between_pages(app, library_repo, slow_download_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "read_only_client_factory", PagedQueueClient)
    PagedQueueClient.records = [queue_record(queue_id=i) for i in range(1, 251)]  # 3 full-size pages
    calls = []

    def heartbeat():
        calls.append(1)
        return True

    result = service.poll_library(library.id, heartbeat=heartbeat)
    assert result["polled"] == 250
    assert "cleared" in result
    # Heartbeat fires between pages (after page 1, after page 2) but not
    # again after the final, queue-complete page.
    assert len(calls) == 2


def test_poll_library_stops_and_fails_closed_when_lease_lost_mid_poll(
    app, library_repo, slow_download_repo, monkeypatch
):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "read_only_client_factory", PagedQueueClient)
    PagedQueueClient.records = [queue_record(queue_id=i) for i in range(1, 251)]  # 3 pages
    existing = slow_download_repo.record_observation(
        library.id, queue_record(queue_id=9999), slow_download_repo.settings(library.id)
    )
    result = service.poll_library(library.id, heartbeat=lambda: False)
    assert "lease" in result["error"]
    assert result["cleared"] == 0
    # An incomplete snapshot suppresses *all* classification, not just
    # disappearance reconciliation - the first page's records are never
    # folded into durable tracking state even though they were fetched,
    # since the overall read never proved complete.
    assert result["polled"] == 0
    # No newly-fetched record (queue ids 1..250) was folded into durable
    # tracking state - only the pre-existing, unrelated item (9999)
    # inserted above the poll under test remains.
    assert {item["sonarr_queue_id"] for item in slow_download_repo.list_current(library.id, limit=500)} == {9999}
    # Disappearance reconciliation never ran on the incomplete read.
    assert slow_download_repo.get(existing["id"])["classification"] != "removed"


def test_attempt_removals_never_deletes_when_lease_already_lost(client, app, database, library_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    _force_removal_pending(database, library.id, queue_id=88)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1, heartbeat=lambda: False)
    assert outcomes == []
    assert FakeQueueClient.delete_calls == []


def test_attempt_removals_stops_deleting_once_lease_is_lost_mid_iteration(
    client, app, database, library_repo, monkeypatch
):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/slow-download/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "test", "revision": 0},
    )
    _force_removal_pending(database, library.id, queue_id=1)
    _force_removal_pending(database, library.id, queue_id=2)
    service = app.extensions["managearr"]["slow_download"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    calls = {"n": 0}

    def heartbeat():
        calls["n"] += 1
        # True for the top-of-call check, the pre-attempt check on the
        # first item, and that first item's own destructive-boundary
        # recheck (immediately before its marker/DELETE); False on the
        # pre-attempt check for the second item - the lease is lost
        # between the two removal attempts, so the second item is never
        # even revalidated.
        return calls["n"] <= 3

    outcomes = service.attempt_removals(max_removals=2, heartbeat=heartbeat)
    assert len(outcomes) == 1
    assert len(FakeQueueClient.delete_calls) == 1

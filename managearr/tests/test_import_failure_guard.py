"""Integration tests for the Sonarr-only import-failure reason policy:
settings API (checkbox persistence, revision/audit, confirm gates),
monitoring poll (reason normalization, observed/resolved/disappeared),
gated removal, and the same safety races as the slow-download guard's
``authorized_write`` test suite (pause/E-stop vs DELETE, crash-after-
accepted-DELETE, pool max_size=1, duplicate-attempt prevention, final
pre-DELETE revalidation)."""
import os
import threading
import time

import pytest

from app.adapters.sonarr_client import SonarrConnectionError, SonarrPostAmbiguousError, SonarrPostRejectedError
from app.domain.import_failure import SERIES_MATCHED_BY_ID_MESSAGE, SERIES_MATCHED_BY_ID_REASON_KEY
from app.persistence.database import Database
from app.persistence.import_failure_repository import ImportFailureRepository
from app.services.import_failure_service import ImportFailureService


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
    queue_id=1, status="completed", tracked_state=None, tracked_status=None,
    title="Show.S01E01", download_id="dl-1", messages=("Sample",), error_message=None,
):
    return {
        "queue_id": queue_id, "episode_ids": [], "status": status, "tracked_state": tracked_state,
        "tracked_status": tracked_status, "download_id": download_id, "added": None, "title": title,
        "size": None, "sizeleft": None, "timeleft": None, "error_message": error_message,
        "status_messages": [{"title": None, "messages": list(messages)}] if messages else [],
    }


def reset_fake(delete_error=None, delete_hook=None):
    FakeQueueClient.records = list(FakeQueueClient.records)
    FakeQueueClient.delete_calls = []
    FakeQueueClient.delete_error = delete_error
    FakeQueueClient.delete_hook = delete_hook


def make_sonarr_library(library_repo):
    return library_repo.create({"name": "Sonarr", "type": "sonarr", "url": "http://sonarr", "api_key": "secret", "enabled": True})


def _arm_live(app, database):
    with database.connect() as conn:
        conn.execute("UPDATE scheduler_settings SET mode='live', updated_at=now() WHERE id=1")
    app.extensions["managearr"]["live_repo"].set_authorization_state("running", actor="test", reason="guard test")


# --- Settings defaults -------------------------------------------------------

def test_settings_default_every_reason_leaves_and_auto_removal_off(client, library_repo):
    library = make_sonarr_library(library_repo)
    settings = client.get(f"/api/v1/libraries/{library.id}/import-failure/settings").get_json()["settings"]
    assert settings["monitoring_enabled"] is True
    assert settings["auto_removal_enabled"] is False
    assert settings["removal_reasons"] == []
    assert "secret" not in str(settings)


def test_reasons_api_endpoint_returns_groups_and_special_message(client):
    body = client.get("/api/v1/import-failure/reasons").get_json()
    all_keys = {k for g in body["groups"] for k in g["reasons"]}
    assert SERIES_MATCHED_BY_ID_REASON_KEY in all_keys
    assert "Unknown" not in all_keys
    assert body["always_leave"] == ["Unknown"]
    assert body["series_matched_by_id_message"] == SERIES_MATCHED_BY_ID_MESSAGE


# --- Confirm+reason gating ----------------------------------------------------

def test_enabling_auto_removal_requires_confirm_and_reason(client, library_repo):
    library = make_sonarr_library(library_repo)
    response = client.put(f"/api/v1/libraries/{library.id}/import-failure/settings", json={"auto_removal_enabled": True})
    assert response.status_code == 400
    assert "confirm" in str(response.get_json()["errors"]).lower()


def test_enabling_auto_removal_requires_live_armed(client, library_repo):
    library = make_sonarr_library(library_repo)
    response = client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "testing"},
    )
    assert response.status_code == 400
    assert "armed and running" in str(response.get_json()["errors"])


def test_enabling_auto_removal_succeeds_once_live_armed(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    response = client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"auto_removal_enabled": True, "confirm": True, "reason": "testing"},
    )
    assert response.status_code == 200
    assert response.get_json()["settings"]["auto_removal_enabled"] is True


def test_disabling_auto_removal_never_requires_confirmation(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(f"/api/v1/libraries/{library.id}/import-failure/settings", json={"auto_removal_enabled": True, "confirm": True, "reason": "x"})
    response = client.put(f"/api/v1/libraries/{library.id}/import-failure/settings", json={"auto_removal_enabled": False})
    assert response.status_code == 200 and response.get_json()["settings"]["auto_removal_enabled"] is False


def test_selecting_a_reason_always_requires_confirm_even_with_auto_removal_off(client, library_repo):
    # Newly selecting any removal category always requires confirm+reason,
    # even while automatic removal itself is still off - selection is a
    # material policy change in its own right, not something to leave
    # pre-armed for a later, unconfirmed enable.
    library = make_sonarr_library(library_repo)
    response = client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings", json={"removal_reasons": ["Sample", "Unpacking"]}
    )
    assert response.status_code == 400
    assert "confirm" in str(response.get_json()["errors"]).lower()
    confirmed = client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"removal_reasons": ["Sample", "Unpacking"], "confirm": True, "reason": "preselect"},
    )
    assert confirmed.status_code == 200
    assert sorted(confirmed.get_json()["settings"]["removal_reasons"]) == ["Sample", "Unpacking"]


def test_newly_selecting_a_removal_reason_while_enabled_requires_confirm_and_reason(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"auto_removal_enabled": True, "removal_reasons": ["Sample"], "confirm": True, "reason": "initial"},
    )
    response = client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings", json={"removal_reasons": ["Sample", "Unpacking"]}
    )
    assert response.status_code == 400
    assert "confirm" in str(response.get_json()["errors"]).lower()
    confirmed = client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"removal_reasons": ["Sample", "Unpacking"], "confirm": True, "reason": "expand"},
    )
    assert confirmed.status_code == 200
    assert sorted(confirmed.get_json()["settings"]["removal_reasons"]) == ["Sample", "Unpacking"]


def test_unselecting_a_removal_reason_while_enabled_never_requires_confirm(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"auto_removal_enabled": True, "removal_reasons": ["Sample", "Unpacking"], "confirm": True, "reason": "initial"},
    )
    response = client.put(f"/api/v1/libraries/{library.id}/import-failure/settings", json={"removal_reasons": ["Sample"]})
    assert response.status_code == 200
    assert response.get_json()["settings"]["removal_reasons"] == ["Sample"]


def test_unrecognized_reason_key_is_rejected(client, library_repo):
    library = make_sonarr_library(library_repo)
    response = client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings", json={"removal_reasons": ["TotallyMadeUpReason"]}
    )
    assert response.status_code == 400
    assert "unrecognized" in str(response.get_json()["errors"]).lower()


def test_unknown_reason_key_can_never_be_selected_even_though_it_is_in_the_canonical_catalog(client, library_repo):
    library = make_sonarr_library(library_repo)
    response = client.put(f"/api/v1/libraries/{library.id}/import-failure/settings", json={"removal_reasons": ["Unknown"]})
    assert response.status_code == 400


@pytest.mark.parametrize("field", ["remove_from_client", "blocklist", "skip_redownload"])
def test_boolean_destructive_loosening_while_auto_removal_enabled_requires_confirm(client, app, database, library_repo, field):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"auto_removal_enabled": True, field: False, "confirm": True, "reason": "initial enable"},
    )
    response = client.put(f"/api/v1/libraries/{library.id}/import-failure/settings", json={field: True})
    assert response.status_code == 400
    confirmed = client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings", json={field: True, "confirm": True, "reason": "loosen"}
    )
    assert confirmed.status_code == 200
    assert confirmed.get_json()["settings"][field] is True


def test_disabling_monitoring_while_auto_removal_enabled_safely_disables_removal(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"auto_removal_enabled": True, "removal_reasons": ["Sample"], "confirm": True, "reason": "initial"},
    )
    response = client.put(f"/api/v1/libraries/{library.id}/import-failure/settings", json={"monitoring_enabled": False})
    assert response.status_code == 200
    settings = response.get_json()["settings"]
    assert settings["monitoring_enabled"] is False
    assert settings["auto_removal_enabled"] is False


def test_settings_optimistic_concurrency_rejects_stale_revision(client, library_repo):
    library = make_sonarr_library(library_repo)
    current = client.get(f"/api/v1/libraries/{library.id}/import-failure/settings").get_json()["settings"]
    client.put(f"/api/v1/libraries/{library.id}/import-failure/settings", json={"poll_seconds": 90})
    stale = client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"revision": current["revision"], "poll_seconds": 120},
    )
    assert stale.status_code == 400
    assert "changed by someone else" in str(stale.get_json()["errors"])


def test_atomic_recheck_rejects_enable_even_if_preliminary_check_is_stale(client, app, database, library_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    app.extensions["managearr"]["live_repo"].set_authorization_state("paused", actor="test", reason="already paused")
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "_live_status", lambda: {"allowed": True, "reasons": [], "generation": 0})
    updated, errors = service.update_settings(library.id, {"auto_removal_enabled": True, "confirm": True, "reason": "test"})
    assert updated is None
    assert any("Live" in e for e in errors)
    assert service.repo.settings(library.id)["auto_removal_enabled"] is False


def test_pause_blocks_on_the_enable_transaction_lock_rather_than_racing_in(client, app, database, library_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    repo = app.extensions["managearr"]["import_failure_repo"]
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
            f"/api/v1/libraries/{library.id}/import-failure/settings",
            json={"auto_removal_enabled": True, "confirm": True, "reason": "test"},
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
    assert app.extensions["managearr"]["live_repo"].get_control()["state"] == "paused"


# --- Monitoring: normalization + observed/resolved/disappeared -------------

def test_poll_library_observes_matched_reason_and_decision(client, app, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=1, messages=["Sample"])]
    service.poll_library(library.id)
    item = import_failure_repo.list_current(library.id)[0]
    assert item["matched_reasons"] == ["Sample"]
    assert item["decision"] == "leave"  # auto-removal still disabled by default
    assert item["unmatched_messages"] == []


def test_extract_messages_ignores_status_message_title_to_avoid_false_matches():
    """``status_messages[].title`` is the specific file/release name a
    message applies to - not the rejection reason - and is attacker/
    release-name-influenceable. It must never be fed into normalization,
    or a release named e.g. "Show.S01E01.Error.Special" could be
    misclassified as the 'Error' reason."""
    from app.services.import_failure_service import _extract_messages

    record = {
        "status_messages": [{"title": "Show.S01E01.Error.Sample.Special", "messages": ["some unrelated note"]}],
        "error_message": None,
    }
    assert _extract_messages(record) == ["some unrelated note"]


def test_poll_library_tracks_unmatched_message_separately(client, app, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=1, messages=["complete and utter gibberish"])]
    service.poll_library(library.id)
    item = import_failure_repo.list_current(library.id)[0]
    assert item["matched_reasons"] == []
    assert item["unmatched_messages"] == ["complete and utter gibberish"]
    assert item["decision"] == "leave"


def test_poll_library_ignores_non_watched_statuses(client, app, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=1, status="downloading", tracked_state="downloading", tracked_status="ok")]
    service.poll_library(library.id)
    assert import_failure_repo.list_current(library.id) == []


def test_poll_library_watches_import_blocked_tracked_state(client, app, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=1, status="downloading", tracked_state="importblocked", messages=["Unpacking"])]
    service.poll_library(library.id)
    item = import_failure_repo.list_current(library.id)[0]
    assert item["matched_reasons"] == ["Unpacking"]


def test_poll_library_resolves_item_no_longer_watched(client, app, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=1, messages=["Sample"])]
    service.poll_library(library.id)
    FakeQueueClient.records = [queue_record(queue_id=1, status="downloading", tracked_state="downloading", tracked_status="ok", messages=[])]
    service.poll_library(library.id)
    item = import_failure_repo.list_current(library.id)[0]
    assert item["decision"] == "resolved"


def test_poll_library_clears_disappeared_item_without_treating_as_removal(client, app, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=1, messages=["Sample"])]
    service.poll_library(library.id)
    FakeQueueClient.records = []
    service.poll_library(library.id)
    item = import_failure_repo.list_current(library.id)[0]
    assert item["decision"] == "resolved"
    assert item["removal_outcome"] is None
    actions = [a["action"] for a in import_failure_repo.recent_actions(library.id)]
    assert "cleared_disappeared" in actions


def test_monitoring_disabled_does_not_poll(client, app, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    client.put(f"/api/v1/libraries/{library.id}/import-failure/settings", json={"monitoring_enabled": False})
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=1)]
    result = service.poll_library(library.id)
    assert result.get("skipped") == "monitoring disabled"
    assert import_failure_repo.list_current(library.id) == []


def test_series_matched_by_id_message_is_observed_and_can_be_selected(client, app, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=1, messages=[SERIES_MATCHED_BY_ID_MESSAGE])]
    service.poll_library(library.id)
    item = import_failure_repo.list_current(library.id)[0]
    assert item["matched_reasons"] == [SERIES_MATCHED_BY_ID_REASON_KEY]


# --- Gated removal -----------------------------------------------------------

def _force_remove_eligible(database, library_id, queue_id=1, reasons=("Sample",)):
    with database.connect() as conn:
        existing = conn.execute(
            "SELECT count(*) AS c FROM import_failure_queue_items WHERE library_id=%s", (library_id,)
        ).fetchone()["c"]
        if existing == 0:
            FakeQueueClient.records = []
        FakeQueueClient.records.append(queue_record(queue_id=queue_id, messages=list(reasons)))
        row = conn.execute(
            """
            INSERT INTO import_failure_queue_items (
                library_id, sonarr_queue_id, download_id, title, status,
                matched_reasons, unmatched_messages, decision, decision_reason,
                first_seen_at, last_seen_at
            ) VALUES (%s, %s, 'dl-1', 'Show.S01E01', 'completed', %s, '{}', 'remove_eligible', 'test evidence', now(), now())
            RETURNING id
            """,
            (library_id, queue_id, list(reasons)),
        ).fetchone()
    return row["id"]


def _enable_with_reasons(client, database, app, library_id, reasons):
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library_id}/import-failure/settings",
        json={"auto_removal_enabled": True, "removal_reasons": list(reasons), "confirm": True, "reason": "test"},
    )


def test_removal_blocked_when_auto_removal_disabled(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    item_id = _force_remove_eligible(database, library.id)
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    assert FakeQueueClient.delete_calls == []
    assert import_failure_repo.get(item_id)["decision"] == "remove_eligible"


def test_removal_blocked_when_reason_no_longer_selected(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Unpacking"])  # not "Sample"
    item_id = _force_remove_eligible(database, library.id, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    assert FakeQueueClient.delete_calls == []


def test_removal_completes_when_live_and_reason_selected(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    item_id = _force_remove_eligible(database, library.id, queue_id=42, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert len(outcomes) == 1 and outcomes[0]["decision"] == "removed"
    assert FakeQueueClient.delete_calls == [
        {"queue_id": 42, "remove_from_client": True, "blocklist": True, "skip_redownload": False}
    ]
    actions = [a["action"] for a in import_failure_repo.recent_actions(library.id)]
    assert "removal_attempt_started" in actions and "removal_completed" in actions


def test_at_most_one_removal_per_call(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    _force_remove_eligible(database, library.id, queue_id=1, reasons=["Sample"])
    _force_remove_eligible(database, library.id, queue_id=2, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert len(outcomes) == 1
    assert len(FakeQueueClient.delete_calls) == 1


@pytest.mark.parametrize("error,outcome", [(SonarrPostAmbiguousError, "ambiguous"), (SonarrPostRejectedError, "rejected")])
def test_typed_delete_outcomes_persist_safely(client, app, database, library_repo, import_failure_repo, monkeypatch, error, outcome):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    item_id = _force_remove_eligible(database, library.id, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake(delete_error=error)
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes[0]["removal_outcome"] == outcome
    assert import_failure_repo.get(item_id)["removal_outcome"] == outcome


def test_ambiguous_outcome_is_never_automatically_retried(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    item_id = _force_remove_eligible(database, library.id, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake(delete_error=SonarrPostAmbiguousError)
    service.attempt_removals(max_removals=1)
    assert import_failure_repo.get(item_id)["decision"] == "ambiguous"
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    assert FakeQueueClient.delete_calls == []


# --- Final pre-DELETE revalidation: stored evidence alone is never
# sufficient; abort safely (no marker, no DELETE) on any drift. ------------

def _assert_no_marker_and_no_delete(database, item_id):
    with database.connect() as conn:
        marker = conn.execute(
            "SELECT 1 FROM import_failure_removal_attempts WHERE queue_item_id = %s", (item_id,)
        ).fetchone()
    assert marker is None
    assert FakeQueueClient.delete_calls == []


def test_revalidation_aborts_when_queue_record_disappeared(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    item_id = _force_remove_eligible(database, library.id, queue_id=201, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = []
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)
    assert import_failure_repo.get(item_id)["decision"] == "removed"


def test_revalidation_aborts_when_download_identity_changed(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    item_id = _force_remove_eligible(database, library.id, queue_id=203, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=203, download_id="dl-rotated", messages=["Sample"])]
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)
    assert import_failure_repo.get(item_id)["decision"] == "ambiguous"


def test_revalidation_aborts_when_no_longer_in_watched_state(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    item_id = _force_remove_eligible(database, library.id, queue_id=204, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    # Resolved itself (e.g. an operator manually fixed it) by the time of
    # the final live read - no longer completed/import-blocked/warning.
    FakeQueueClient.records = [queue_record(queue_id=204, status="downloading", tracked_state="downloading", tracked_status="ok", messages=[])]
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)
    assert import_failure_repo.get(item_id)["decision"] == "resolved"


def test_revalidation_aborts_when_observed_reasons_changed(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample", "Unpacking"])
    item_id = _force_remove_eligible(database, library.id, queue_id=205, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    # A fresh additional reason now present on the live record - stored
    # evidence ("Sample" only) no longer matches reality exactly.
    FakeQueueClient.records = [queue_record(queue_id=205, messages=["Sample", "Unpacking"])]
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)
    assert import_failure_repo.get(item_id)["decision"] == "ambiguous"


def test_revalidation_aborts_when_a_fresh_unmatched_message_appears(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    item_id = _force_remove_eligible(database, library.id, queue_id=206, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    FakeQueueClient.records = [queue_record(queue_id=206, messages=["Sample", "unrelated gibberish"])]
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)


def test_revalidation_fails_closed_on_sonarr_error(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    item_id = _force_remove_eligible(database, library.id, queue_id=207, reasons=["Sample"])

    class UnreachableClient(FakeQueueClient):
        def get_queue_details(self, *, page=1, page_size=100):
            raise SonarrConnectionError("connection refused")

    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", UnreachableClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert outcomes == []
    _assert_no_marker_and_no_delete(database, item_id)
    assert import_failure_repo.get(item_id)["decision"] == "ambiguous"


def test_revalidation_passes_when_live_queue_matches_stored_evidence_exactly(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    item_id = _force_remove_eligible(database, library.id, queue_id=208, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1)
    assert len(outcomes) == 1 and outcomes[0]["decision"] == "removed"
    with database.connect() as conn:
        marker = conn.execute(
            "SELECT 1 FROM import_failure_removal_attempts WHERE queue_item_id = %s", (item_id,)
        ).fetchone()
    assert marker is not None


# --- Races: duplicate-attempt prevention, crash safety, pool isolation ------

class BlockingClient(FakeQueueClient):
    entered = threading.Event()
    release = threading.Event()

    def delete_queue_record(self, queue_id, *, remove_from_client, blocklist, skip_redownload=False):
        type(self).entered.set()
        assert type(self).release.wait(3)
        return super().delete_queue_record(queue_id, remove_from_client=remove_from_client, blocklist=blocklist, skip_redownload=skip_redownload)


def test_pause_waits_for_attempt_boundary_then_delete_proceeds(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    _force_remove_eligible(database, library.id, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
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
    assert not pause_done.is_set()
    BlockingClient.release.set()
    remove_thread.join(3)
    pause_thread.join(3)
    assert not remove_thread.is_alive() and not pause_thread.is_alive()
    assert outcome["result"][0]["decision"] == "removed"
    assert pause_done.is_set()


def test_crash_after_accepted_delete_is_durably_ambiguous_and_never_resent(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    item_id = _force_remove_eligible(database, library.id, reasons=["Sample"])

    class SimulatedProcessDeath(BaseException):
        pass

    class CrashAfterAcceptedClient(FakeQueueClient):
        def delete_queue_record(self, *args, **kwargs):
            super().delete_queue_record(*args, **kwargs)
            raise SimulatedProcessDeath("process died after Sonarr accepted the DELETE")

    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", CrashAfterAcceptedClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()

    with pytest.raises(SimulatedProcessDeath):
        service._attempt_removal(
            library_repo.get(library.id), import_failure_repo.get(item_id),
            app.extensions["managearr"]["live_repo"].get_control()["authorization_generation"],
        )

    with database.connect() as conn:
        marker = conn.execute("SELECT started_at FROM import_failure_removal_attempts WHERE queue_item_id = %s", (item_id,)).fetchone()
    assert marker is not None

    restarted = ImportFailureService(
        ImportFailureRepository(database), library_repo, app.extensions["managearr"]["live_repo"],
        app.extensions["managearr"]["scheduler_repo"], client_factory=CrashAfterAcceptedClient,
        read_only_client_factory=FakeQueueClient,
    )
    outcomes = restarted.attempt_removals(max_removals=1)
    assert outcomes == []
    assert import_failure_repo.get(item_id)["decision"] == "remove_eligible"
    assert len(FakeQueueClient.delete_calls) == 1  # never resent


def test_dedicated_marker_connection_works_with_pool_max_size_one(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    item_id = _force_remove_eligible(database, library.id, reasons=["Sample"])
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
        service = ImportFailureService(
            ImportFailureRepository(single), library_repo, app.extensions["managearr"]["live_repo"],
            app.extensions["managearr"]["scheduler_repo"], client_factory=FakeQueueClient,
            read_only_client_factory=FakeQueueClient,
        )
        outcomes = service.attempt_removals(max_removals=1)
        assert len(outcomes) == 1 and outcomes[0]["decision"] == "removed"
    finally:
        single.close()


def test_concurrent_worker_cycles_never_double_delete(client, app, database, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    _force_remove_eligible(database, library.id, reasons=["Sample"])
    reset_fake()

    results = []

    def run():
        service = ImportFailureService(
            ImportFailureRepository(database), library_repo, app.extensions["managearr"]["live_repo"],
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


def test_delete_uses_settings_bound_under_authorization_lock(client, app, database, library_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    enabled = client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"auto_removal_enabled": True, "removal_reasons": ["Sample"], "confirm": True, "reason": "test"},
    ).get_json()["settings"]
    changed = client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"revision": enabled["revision"], "remove_from_client": False, "blocklist": True},
    )
    assert changed.status_code == 200
    _force_remove_eligible(database, library.id, queue_id=77, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    monkeypatch.setattr(service, "read_only_client_factory", FakeQueueClient)
    reset_fake()
    service.attempt_removals(max_removals=1)
    assert FakeQueueClient.delete_calls == [{
        "queue_id": 77, "remove_from_client": False, "blocklist": True, "skip_redownload": False,
    }]


# --- Worker lease renewal / loss ---------------------------------------------

class PagedQueueClient(FakeQueueClient):
    def get_queue_details(self, *, page=1, page_size=100):
        start = (page - 1) * page_size
        chunk = type(self).records[start:start + page_size]
        return {"records": chunk, "total_records": len(type(self).records), "page": page, "page_size": page_size}


def test_poll_library_renews_heartbeat_between_pages(app, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "read_only_client_factory", PagedQueueClient)
    PagedQueueClient.records = [queue_record(queue_id=i, messages=["Sample"]) for i in range(1, 251)]
    calls = []

    def heartbeat():
        calls.append(1)
        return True

    result = service.poll_library(library.id, heartbeat=heartbeat)
    assert result["observed"] == 250
    assert "cleared" in result
    assert len(calls) == 2


def test_poll_library_stops_and_fails_closed_when_lease_lost_mid_poll(app, library_repo, import_failure_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "read_only_client_factory", PagedQueueClient)
    PagedQueueClient.records = [queue_record(queue_id=i, messages=["Sample"]) for i in range(1, 251)]
    result = service.poll_library(library.id, heartbeat=lambda: False)
    assert "lease" in result["error"]
    assert result["cleared"] == 0
    assert result["observed"] == 100


def test_attempt_removals_never_deletes_when_lease_already_lost(client, app, database, library_repo, monkeypatch):
    library = make_sonarr_library(library_repo)
    _enable_with_reasons(client, database, app, library.id, ["Sample"])
    _force_remove_eligible(database, library.id, queue_id=88, reasons=["Sample"])
    service = app.extensions["managearr"]["import_failure"]
    monkeypatch.setattr(service, "client_factory", FakeQueueClient)
    reset_fake()
    outcomes = service.attempt_removals(max_removals=1, heartbeat=lambda: False)
    assert outcomes == []
    assert FakeQueueClient.delete_calls == []


# --- Policy audit trail -------------------------------------------------------

def test_policy_audit_records_added_and_removed_reasons(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"auto_removal_enabled": True, "removal_reasons": ["Sample"], "confirm": True, "reason": "enable"},
    )
    client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"removal_reasons": ["Sample", "Unpacking"], "confirm": True, "reason": "expand"},
    )
    audit = client.get(f"/api/v1/activity/import-failure/policy?library_id={library.id}").get_json()["audit"]
    assert len(audit) == 2
    assert audit[0]["added_reasons"] == ["Unpacking"]
    assert audit[0]["reason"] == "expand"
    assert audit[1]["added_reasons"] == ["Sample"]
    assert audit[1]["auto_removal_enabled_before"] is False
    assert audit[1]["auto_removal_enabled_after"] is True


def test_policy_audit_is_append_only(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    _arm_live(app, database)
    client.put(
        f"/api/v1/libraries/{library.id}/import-failure/settings",
        json={"auto_removal_enabled": True, "removal_reasons": ["Sample"], "confirm": True, "reason": "enable"},
    )
    import psycopg
    with pytest.raises(psycopg.errors.RaiseException):
        with database.connect() as conn:
            conn.execute("UPDATE import_failure_policy_audit SET reason = 'tampered'")


def test_actions_audit_is_append_only(client, app, database, library_repo):
    library = make_sonarr_library(library_repo)
    import psycopg
    with database.connect() as conn:
        conn.execute(
            "INSERT INTO import_failure_queue_items (library_id, sonarr_queue_id, status) VALUES (%s, 1, 'completed') RETURNING id",
            (library.id,),
        )
        item_id = conn.execute("SELECT id FROM import_failure_queue_items WHERE library_id=%s", (library.id,)).fetchone()["id"]
        conn.execute(
            "INSERT INTO import_failure_actions (queue_item_id, library_id, sonarr_queue_id, action, reason) VALUES (%s, %s, 1, 'observed', 'x')",
            (item_id, library.id),
        )
    with pytest.raises(psycopg.errors.RaiseException):
        with database.connect() as conn:
            conn.execute("UPDATE import_failure_actions SET reason = 'tampered'")

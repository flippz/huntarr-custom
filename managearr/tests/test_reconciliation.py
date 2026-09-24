"""M3 command/outcome reconciliation against mocked Sonarr reads."""
from datetime import timedelta

import psycopg
import pytest

from app.adapters.sonarr_client import SonarrAuthError
from app.persistence import dispatch_repository as dispatch_repository_module
from app.services.reconciliation_service import ReconciliationService


class StubReconciliationClient:
    def __init__(self, *, command=None, history=None, queue=None, error=None):
        self.command = command or {"id": 7001, "name": "EpisodeSearch", "status": "completed"}
        self.history = history or {}
        self.queue = queue or []
        self.error = error
        self.calls = []

    def get_command(self, command_id):
        self.calls.append(("command", command_id))
        if self.error:
            raise self.error
        return dict(self.command)

    def get_history(self, *, page, page_size, episode_id):
        self.calls.append(("history", page, page_size, episode_id))
        return {"page": page, "page_size": page_size, "total_records": len(self.history.get(episode_id, [])),
                "records": list(self.history.get(episode_id, []))}

    def get_queue_details(self, *, page, page_size):
        self.calls.append(("queue", page, page_size))
        return {"page": page, "page_size": page_size, "total_records": len(self.queue),
                "records": list(self.queue)}


def make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo, *,
               episode_ids=(101,), mode="manual", state="completed", command_id=7001):
    library = library_repo.create({
        "name": "Sonarr Main", "type": "sonarr", "url": "http://sonarr.invalid:8989",
        "api_key": "super-secret-key", "enabled": True,
    })
    job = activity_repo.create({
        "library_id": library.id, "library_name": library.name, "job_type": "sonarr_scan",
        "state": "completed", "title": "Completed scan",
    })
    candidate_repo.create_many(job.id, library.id, [
        {"series_id": 11, "series_title": "Show", "episode_id": episode_id,
         "season_number": 1, "episode_number": index + 1, "reason": "missing"}
        for index, episode_id in enumerate(episode_ids)
    ])
    candidates = candidate_repo.list_for_job(job.id)
    selected = len(candidates) if mode == "manual" and state != "failed" else 0
    with dispatch_repo.db.connect() as conn:
        batch_id = dispatch_repo.create_batch(conn, {
            "scan_job_id": job.id, "library_id": library.id, "library_name": library.name,
            "mode": mode, "state": state, "requested_count": len(candidates),
            "selected_count": selected, "dispatched_count": selected if state == "completed" else 0,
            "sonarr_command_id": command_id,
            "sonarr_command_status": "queued" if command_id else None,
            "error_summary": "dispatch failed" if state == "failed" else "",
        })
        if mode == "manual" and state != "failed":
            item_state = "dispatched" if state == "completed" else "ambiguous"
            dispatch_repo.create_items(conn, batch_id, [
                {"candidate_id": candidate.id, "episode_id": candidate.episode_id,
                 "series_id": candidate.series_id, "series_title": candidate.series_title,
                 "season_number": candidate.season_number, "episode_number": candidate.episode_number,
                 "state": item_state,
                 "reason": "ambiguous dispatch" if item_state == "ambiguous" else None}
                for candidate in candidates
            ])
    return library, job, dispatch_repo.get_batch(batch_id)


def install_reconciliation(app, stub):
    services = app.extensions["managearr"]

    def factory(base_url, api_key, timeout=None):
        assert base_url == "http://sonarr.invalid:8989"
        assert api_key == "super-secret-key"
        return stub

    services["reconciliation"] = ReconciliationService(
        services["dispatch_repo"], services["outcome_repo"],
        services["reconciliation"].library_repo,
        services["reconciliation"].activity_repo,
        services["reconciliation"].candidate_repo,
        client_factory=factory,
    )


def history_record(event_id, event_type, episode_id, download_id="dl-1"):
    return {"id": event_id, "event_type": event_type, "episode_id": episode_id,
            "date": "2099-09-24T12:00:00Z", "download_id": download_id}


def test_completed_command_without_episode_evidence_is_not_false_success(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, _, batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    stub = StubReconciliationClient()
    install_reconciliation(app, stub)

    response = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile")

    assert response.status_code == 200
    result = response.get_json()["reconciliation"]
    assert result["batch"]["command_observed_state"] == "completed"
    assert result["batch"]["reconciliation_state"] == "unresolved"
    assert result["item_results"][0]["latest_outcome"] == "unresolved"
    assert "not evidence" in result["notice"]
    assert [(call[0], call[1]) for call in stub.calls if call[0] == "history"] == [("history", 1)]
    assert ("queue", 1, 100) in stub.calls


def test_history_before_dispatch_is_excluded_as_unrelated(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, _, batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    old = history_record(1, "downloadFolderImported", 101)
    old["date"] = "2000-01-01T00:00:00Z"
    stub = StubReconciliationClient(history={101: [old]})
    install_reconciliation(app, stub)
    result = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile").get_json()["reconciliation"]
    assert result["batch"]["reconciliation_state"] == "unresolved"
    assert result["item_results"][0]["latest_outcome"] == "unresolved"
    detail = client.get(f"/api/v1/dispatch-batches/{batch.id}/outcomes").get_json()["outcomes"]
    assert detail["items"][0]["outcomes"] == []


def test_command_queued_and_failed_are_normalized_without_claiming_import(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, _, queued_batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    queued = StubReconciliationClient(command={"id": 7001, "name": "EpisodeSearch", "status": "queued"})
    install_reconciliation(app, queued)
    body = client.post(f"/api/v1/dispatch-batches/{queued_batch.id}/reconcile").get_json()["reconciliation"]
    assert body["batch"]["command_observed_state"] == "queued"
    assert body["batch"]["reconciliation_state"] == "unresolved"

    # Fresh database rows, same endpoint contract, terminal command failure is
    # batch evidence only and does not fabricate per-episode failure/import.
    _, _, failed_command_batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    failed = StubReconciliationClient(command={"id": 7001, "name": "EpisodeSearch", "status": "failed"})
    install_reconciliation(app, failed)
    body = client.post(f"/api/v1/dispatch-batches/{failed_command_batch.id}/reconcile").get_json()["reconciliation"]
    assert body["batch"]["command_observed_state"] == "failed"
    assert body["item_results"][0]["latest_outcome"] == "unresolved"


@pytest.mark.parametrize(("status", "observed"), [("started", "running"), ("aborted", "aborted")])
def test_other_command_states_are_normalized(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo,
    status, observed,
):
    _, _, batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    stub = StubReconciliationClient(command={
        "id": 7001, "name": "EpisodeSearch", "status": status,
    })
    install_reconciliation(app, stub)
    result = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile").get_json()["reconciliation"]
    assert result["batch"]["command_observed_state"] == observed


def test_grab_queue_import_and_failure_mapping_excludes_unrelated_events(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, _, batch = make_batch(
        library_repo, activity_repo, candidate_repo, dispatch_repo, episode_ids=(101, 102)
    )
    stub = StubReconciliationClient(
        history={
            101: [history_record(1, "grabbed", 101), history_record(2, "downloadFolderImported", 101)],
            102: [history_record(3, "downloadFailed", 102), history_record(99, "grabbed", 999)],
        },
        queue=[
            {"episode_ids": [101], "status": "downloading", "tracked_state": "downloading", "download_id": "dl-1"},
            {"episode_ids": [999], "status": "downloading", "tracked_state": None, "download_id": "unrelated"},
        ],
    )
    install_reconciliation(app, stub)

    result = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile").get_json()["reconciliation"]
    assert result["batch"]["reconciliation_state"] == "resolved"
    assert {item["episode_id"]: item["latest_outcome"] for item in result["item_results"]} == {
        101: "imported", 102: "download_failed",
    }

    detail = client.get(f"/api/v1/dispatch-batches/{batch.id}/outcomes").get_json()["outcomes"]
    all_events = detail["batch_events"] + [event for item in detail["items"] for event in item["outcomes"]]
    assert {event["event_type"] for event in all_events} >= {
        "command_completed", "grabbed", "downloading", "imported", "download_failed",
    }
    assert all(event["episode_id"] != 999 for event in all_events)


def test_queue_import_failure_mapping_is_explicit(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, _, batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    stub = StubReconciliationClient(queue=[{
        "episode_ids": [101], "status": "warning", "tracked_state": "importFailed",
        "download_id": "dl-1",
    }])
    install_reconciliation(app, stub)
    result = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile").get_json()["reconciliation"]
    assert result["item_results"][0]["latest_outcome"] == "import_failed"
    assert result["item_results"][0]["terminal"] is True


def test_repeated_reconciliation_is_event_idempotent_but_attempts_are_audited(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, _, batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    stub = StubReconciliationClient(history={101: [history_record(1, "grabbed", 101)]})
    install_reconciliation(app, stub)

    first = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile").get_json()["reconciliation"]
    second = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile").get_json()["reconciliation"]
    detail = client.get(f"/api/v1/dispatch-batches/{batch.id}/outcomes").get_json()["outcomes"]

    assert first["inserted_event_count"] > 0
    assert second["inserted_event_count"] == 0
    assert len(detail["attempts"]) == 2
    assert len(detail["batch_events"]) == 1
    assert len(detail["items"][0]["outcomes"]) == 1


def test_reconciliation_summary_does_not_regress_when_later_read_is_empty(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, _, batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    install_reconciliation(
        app, StubReconciliationClient(history={101: [history_record(1, "grabbed", 101)]})
    )
    first = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile").get_json()["reconciliation"]
    assert first["batch"]["reconciliation_state"] == "partial"

    install_reconciliation(app, StubReconciliationClient())
    second = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile").get_json()["reconciliation"]
    assert second["attempt"]["state"] == "unresolved"
    assert second["batch"]["reconciliation_state"] == "partial"


def test_reconciliation_rejects_ineligible_batches_before_network(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, _, dry_run = make_batch(
        library_repo, activity_repo, candidate_repo, dispatch_repo,
        mode="dry_run", state="planned", command_id=None,
    )
    _, _, failed = make_batch(
        library_repo, activity_repo, candidate_repo, dispatch_repo,
        state="failed", command_id=None,
    )
    _, _, no_command = make_batch(
        library_repo, activity_repo, candidate_repo, dispatch_repo,
        state="ambiguous", command_id=None,
    )
    stub = StubReconciliationClient()
    install_reconciliation(app, stub)

    assert client.post(f"/api/v1/dispatch-batches/{dry_run.id}/reconcile").status_code == 400
    assert client.post(f"/api/v1/dispatch-batches/{failed.id}/reconcile").status_code == 400
    no_command_response = client.post(f"/api/v1/dispatch-batches/{no_command.id}/reconcile")
    assert no_command_response.status_code == 400
    assert "inspect Sonarr manually" in no_command_response.get_json()["errors"][0]
    assert client.post("/api/v1/dispatch-batches/999999/reconcile").status_code == 404
    assert stub.calls == []


def test_upstream_error_is_safe_and_durably_audited(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, _, batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    stub = StubReconciliationClient(error=SonarrAuthError("Sonarr rejected the configured API key"))
    install_reconciliation(app, stub)

    response = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile")
    assert response.status_code == 502
    raw = response.get_data(as_text=True)
    assert "super-secret-key" not in raw
    assert "sonarr.invalid" not in raw
    assert "Sonarr rejected the configured API key" in raw
    detail = client.get(f"/api/v1/dispatch-batches/{batch.id}/outcomes").get_json()["outcomes"]
    assert detail["attempts"][0]["state"] == "error"
    assert detail["batch"]["reconciliation_state"] == "error"


def test_outcome_rows_are_append_only_and_dedupe_constraint_is_durable(
    app, client, database, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, _, batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    install_reconciliation(app, StubReconciliationClient())
    client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile")
    with database.connect() as conn:
        event = conn.execute(
            "SELECT * FROM dispatch_outcome_events WHERE batch_id = %s LIMIT 1", (batch.id,)
        ).fetchone()
        attempt = conn.execute(
            "SELECT * FROM dispatch_reconciliation_attempts WHERE batch_id = %s LIMIT 1", (batch.id,)
        ).fetchone()

    with pytest.raises(psycopg.errors.RaiseException):
        with database.connect() as conn:
            conn.execute("UPDATE dispatch_outcome_events SET safe_summary = 'changed' WHERE id = %s", (event["id"],))
    with pytest.raises(psycopg.errors.RaiseException):
        with database.connect() as conn:
            conn.execute("DELETE FROM dispatch_reconciliation_attempts WHERE id = %s", (attempt["id"],))
    with pytest.raises(psycopg.errors.UniqueViolation):
        with database.connect() as conn:
            conn.execute(
                """
                INSERT INTO dispatch_outcome_events (
                    batch_id, observed_at, source_endpoint, event_type, event_state,
                    safe_summary, sonarr_command_id, evidence_key
                ) VALUES (%s, now(), %s, %s, %s, %s, %s, %s)
                """,
                (batch.id, event["source_endpoint"], event["event_type"], event["event_state"],
                 event["safe_summary"], event["sonarr_command_id"], event["evidence_key"]),
            )


def test_stale_dispatch_is_ambiguous_and_never_retried(
    monkeypatch, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    library = library_repo.create({
        "name": "Sonarr Main", "type": "sonarr", "url": "http://sonarr.invalid:8989",
        "api_key": "key", "enabled": True,
    })
    job = activity_repo.create({
        "library_id": library.id, "library_name": library.name, "job_type": "sonarr_scan",
        "state": "completed", "title": "scan",
    })
    candidate_repo.create_many(job.id, library.id, [{
        "series_id": 1, "series_title": "Show", "episode_id": 101,
        "season_number": 1, "episode_number": 1, "reason": "missing",
    }])
    candidate = candidate_repo.list_for_job(job.id)[0]
    with dispatch_repo.db.connect() as conn:
        batch_id = dispatch_repo.create_batch(conn, {
            "scan_job_id": job.id, "library_id": library.id, "library_name": library.name,
            "mode": "manual", "state": "dispatching", "requested_count": 1,
            "selected_count": 1,
        })
        dispatch_repo.create_items(conn, batch_id, [{
            "candidate_id": candidate.id, "episode_id": 101, "series_id": 1,
            "series_title": "Show", "season_number": 1, "episode_number": 1,
            "state": "reserved",
        }])
    assert dispatch_repo.record_command_acceptance(
        batch_id=batch_id, library_id=library.id,
        sonarr_command_id=7001, sonarr_command_status="queued",
    )
    real_now = dispatch_repository_module._now()
    monkeypatch.setattr(
        dispatch_repository_module, "_now",
        lambda: real_now + timedelta(seconds=301),
    )
    with dispatch_repo.db.connect() as conn:
        dispatch_repo.acquire_library_lock(conn, library.id)
        dispatch_repo.expire_stale_reservations(library.id, conn=conn)

    batch = dispatch_repo.get_batch(batch_id)
    assert batch.state == "ambiguous"
    assert batch.sonarr_command_id == 7001
    assert batch.reconciliation_state == "operator_review"
    assert "manual reconciliation" in batch.reconciliation_summary
    assert batch.items[0].state == "ambiguous"


def test_cross_library_ambiguity_is_rejected_before_sonarr_read(
    app, client, database, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, job, batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)
    other = library_repo.create({
        "name": "Other", "type": "sonarr", "url": "http://other.invalid",
        "api_key": "other", "enabled": True,
    })
    with database.connect() as conn:
        conn.execute("UPDATE activity_jobs SET library_id = %s WHERE id = %s", (other.id, job.id))
    stub = StubReconciliationClient()
    install_reconciliation(app, stub)
    response = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile")
    assert response.status_code == 400
    assert "cross-library" in response.get_json()["errors"][0]
    assert stub.calls == []


@pytest.mark.parametrize(
    ("column", "value", "expected"),
    [
        ("enabled", False, "library is disabled"),
        ("type", "radarr", "only sonarr libraries"),
        ("api_key", "", "missing an API key"),
    ],
)
def test_wrong_library_readiness_is_rejected_before_sonarr_read(
    app, client, database, library_repo, activity_repo, candidate_repo, dispatch_repo,
    column, value, expected,
):
    library, _, batch = make_batch(
        library_repo, activity_repo, candidate_repo, dispatch_repo
    )
    with database.connect() as conn:
        conn.execute(f"UPDATE arr_libraries SET {column} = %s WHERE id = %s", (value, library.id))
    stub = StubReconciliationClient()
    install_reconciliation(app, stub)
    response = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile")
    assert response.status_code == 400
    assert expected in response.get_json()["errors"][0]
    assert stub.calls == []


def test_reconciliation_pagination_is_bounded_and_reports_partial(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, _, batch = make_batch(library_repo, activity_repo, candidate_repo, dispatch_repo)

    class LargeResultStub(StubReconciliationClient):
        def get_history(self, *, page, page_size, episode_id):
            self.calls.append(("history", page, page_size, episode_id))
            return {"page": page, "page_size": page_size, "total_records": 999, "records": []}

        def get_queue_details(self, *, page, page_size):
            self.calls.append(("queue", page, page_size))
            return {"page": page, "page_size": page_size, "total_records": 999, "records": []}

    stub = LargeResultStub()
    install_reconciliation(app, stub)
    result = client.post(f"/api/v1/dispatch-batches/{batch.id}/reconcile").get_json()["reconciliation"]
    assert [call[1] for call in stub.calls if call[0] == "history"] == [1, 2, 3]
    assert [call[1] for call in stub.calls if call[0] == "queue"] == [1, 2, 3]
    assert result["batch"]["reconciliation_state"] == "partial"
    assert "bounded read limit" in result["batch"]["reconciliation_summary"]

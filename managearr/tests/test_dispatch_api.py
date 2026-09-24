"""HTTP contract for controlled manual dispatch. Sonarr is always stubbed."""

from app.services.dispatch_service import DispatchService


class StubSonarrClient:
    def __init__(self, *, error=None):
        self.error = error
        self.calls = []

    def search_episodes(self, episode_ids):
        self.calls.append(list(episode_ids))
        if self.error:
            raise self.error
        return {"id": 7001, "name": "EpisodeSearch", "status": "queued"}


def make_scan(library_repo, activity_repo, candidate_repo):
    library = library_repo.create(
        {
            "name": "Sonarr Main",
            "type": "sonarr",
            "url": "http://sonarr.invalid:8989",
            "api_key": "super-secret-key",
            "enabled": True,
        }
    )
    job = activity_repo.create(
        {
            "library_id": library.id,
            "library_name": library.name,
            "job_type": "sonarr_scan",
            "state": "completed",
            "title": "Completed scan",
        }
    )
    candidate_repo.create_many(
        job.id,
        library.id,
        [
            {
                "series_id": 11,
                "series_title": "Show",
                "episode_id": 101,
                "season_number": 1,
                "episode_number": 2,
                "air_date": "2026-01-01",
                "reason": "missing",
            }
        ],
    )
    return library, job, candidate_repo.list_for_job(job.id)[0]


def install_stub_dispatch(app, stub):
    services = app.extensions["managearr"]

    def factory(base_url, api_key, timeout=None):
        assert "super-secret-key" == api_key
        return stub

    services["dispatch"] = DispatchService(
        services["dispatch_planning"],
        services["dispatch_repo"],
        services["dispatch"].library_repo,
        client_factory=factory,
    )


def test_preview_returns_plan_and_durable_dry_run_audit(
    client, library_repo, activity_repo, candidate_repo
):
    _, job, candidate = make_scan(library_repo, activity_repo, candidate_repo)

    response = client.post(
        f"/api/v1/activity/{job.id}/dispatch/preview",
        json={"candidate_ids": [candidate.id]},
    )

    assert response.status_code == 201
    body = response.get_json()
    assert body["plan"]["selected"][0]["id"] == candidate.id
    assert body["plan"]["audit_batch_id"] == body["batch"]["id"]
    assert body["batch"]["mode"] == "dry_run"
    assert body["batch"]["state"] == "planned"
    assert body["batch"]["items"][0]["state"] == "planned"


def test_dispatch_without_exact_confirmation_never_calls_sonarr_or_writes_a_batch(
    app, client, library_repo, activity_repo, candidate_repo, dispatch_repo
):
    _, job, candidate = make_scan(library_repo, activity_repo, candidate_repo)
    stub = StubSonarrClient()
    install_stub_dispatch(app, stub)

    for payload in (
        {"candidate_ids": [candidate.id]},
        {"candidate_ids": [candidate.id], "confirm": False},
        {"candidate_ids": [candidate.id], "confirm": "true"},
    ):
        response = client.post(f"/api/v1/activity/{job.id}/dispatch", json=payload)
        assert response.status_code == 400

    assert stub.calls == []
    assert dispatch_repo.list_for_job(job.id) == []


def test_dispatch_rejects_non_object_json_without_calling_sonarr(
    app, client, library_repo, activity_repo, candidate_repo
):
    _, job, _ = make_scan(library_repo, activity_repo, candidate_repo)
    stub = StubSonarrClient()
    install_stub_dispatch(app, stub)

    response = client.post(f"/api/v1/activity/{job.id}/dispatch", json=[1, 2, 3])

    assert response.status_code == 400
    assert stub.calls == []


def test_confirmed_dispatch_replans_posts_once_and_exposes_audit(
    app, client, library_repo, activity_repo, candidate_repo
):
    _, job, candidate = make_scan(library_repo, activity_repo, candidate_repo)
    stub = StubSonarrClient()
    install_stub_dispatch(app, stub)

    response = client.post(
        f"/api/v1/activity/{job.id}/dispatch",
        json={"candidate_ids": [candidate.id], "confirm": True},
    )

    assert response.status_code == 201
    body = response.get_json()
    assert stub.calls == [[101]]
    assert body["batch"]["state"] == "completed"
    assert body["batch"]["sonarr_command_id"] == 7001
    assert body["plan"]["selected"][0]["id"] == candidate.id

    listed = client.get(f"/api/v1/activity/{job.id}/dispatch-batches").get_json()["batches"]
    assert [batch["id"] for batch in listed] == [body["batch"]["id"]]
    detail = client.get(f"/api/v1/dispatch-batches/{body['batch']['id']}").get_json()["batch"]
    assert detail["items"][0]["candidate_id"] == candidate.id
    assert detail["items"][0]["state"] == "dispatched"

    raw = response.get_data(as_text=True)
    assert "super-secret-key" not in raw
    assert "sonarr.invalid" not in raw


def test_dispatch_audit_routes_return_not_found(client):
    assert client.get("/api/v1/activity/999/dispatch-batches").status_code == 404
    assert client.get("/api/v1/dispatch-batches/999").status_code == 404

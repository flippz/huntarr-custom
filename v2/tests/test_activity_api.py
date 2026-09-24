def test_activity_list_empty(client):
    resp = client.get("/api/v2/activity")
    assert resp.status_code == 200
    assert resp.get_json() == {"jobs": []}


def test_activity_get_not_found(client):
    resp = client.get("/api/v2/activity/999")
    assert resp.status_code == 404


def test_activity_reflects_repository_writes(app, client):
    from app.persistence.activity_repository import ActivityRepository

    repo = ActivityRepository(app.extensions["huntarr"]["db"])
    job = repo.create(
        {"library_id": None, "library_name": "Sonarr", "state": "searching", "title": "Pilot"}
    )

    resp = client.get("/api/v2/activity")
    jobs = resp.get_json()["jobs"]
    assert len(jobs) == 1
    assert jobs[0]["state"] == "searching"

    resp = client.get(f"/api/v2/activity/{job.id}")
    assert resp.status_code == 200
    assert resp.get_json()["job"]["title"] == "Pilot"


def test_activity_filter_by_unknown_state_returns_empty(client):
    resp = client.get("/api/v2/activity?state=not-a-real-state")
    assert resp.get_json() == {"jobs": []}

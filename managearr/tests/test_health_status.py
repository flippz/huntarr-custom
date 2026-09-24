from app.persistence.migrations import MIGRATIONS


def test_health_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["database"] is True


def test_health_reports_schema_version_without_connection_details(client):
    resp = client.get("/health")
    data = resp.get_json()
    assert data["schema_version"] == MIGRATIONS[-1].version
    body = resp.get_data(as_text=True)
    for leaked in ("host", "password", "user="):
        assert leaked not in body.lower()


def test_status_reports_db_and_counts(client):
    resp = client.get("/api/v1/status")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["database"]["connected"] is True
    assert data["libraries"] == {"configured": 0, "enabled": 0}
    assert "version" in data


def test_status_reports_schema_version_without_connection_details(client):
    resp = client.get("/api/v1/status")
    data = resp.get_json()
    assert data["database"]["schema_version"] == MIGRATIONS[-1].version
    body = resp.get_data(as_text=True)
    for leaked in ("host", "password", "user="):
        assert leaked not in body.lower()


def test_status_reflects_created_libraries(client):
    client.post(
        "/api/v1/libraries",
        json={"name": "Sonarr", "type": "sonarr", "url": "http://sonarr:8989", "api_key": "abc123"},
    )
    client.post(
        "/api/v1/libraries",
        json={
            "name": "Radarr",
            "type": "radarr",
            "url": "http://radarr:7878",
            "api_key": "def456",
            "enabled": False,
        },
    )
    resp = client.get("/api/v1/status")
    data = resp.get_json()
    assert data["libraries"] == {"configured": 2, "enabled": 1}

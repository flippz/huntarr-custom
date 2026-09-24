def create_payload(**overrides):
    payload = {
        "name": "Sonarr Main",
        "type": "sonarr",
        "url": "http://sonarr:8989",
        "api_key": "abc123",
    }
    payload.update(overrides)
    return payload


def test_create_library_success(client):
    resp = client.post("/api/v2/libraries", json=create_payload())
    assert resp.status_code == 201
    data = resp.get_json()["library"]
    assert data["name"] == "Sonarr Main"
    assert data["type"] == "sonarr"
    assert data["enabled"] is True
    assert "api_key" not in data


def test_create_library_rejects_bad_type(client):
    resp = client.post("/api/v2/libraries", json=create_payload(type="not-an-arr"))
    assert resp.status_code == 400
    assert "type" in resp.get_json()["errors"][0]


def test_create_library_rejects_missing_fields(client):
    resp = client.post("/api/v2/libraries", json={"name": "Only a name"})
    assert resp.status_code == 400
    errors = resp.get_json()["errors"]
    assert len(errors) >= 3


def test_create_library_rejects_bad_url(client):
    resp = client.post("/api/v2/libraries", json=create_payload(url="not-a-url"))
    assert resp.status_code == 400


def test_list_libraries_empty_initially(client):
    resp = client.get("/api/v2/libraries")
    assert resp.get_json() == {"libraries": []}


def test_get_library_not_found(client):
    resp = client.get("/api/v2/libraries/999")
    assert resp.status_code == 404


def test_update_library_partial(client):
    created = client.post("/api/v2/libraries", json=create_payload()).get_json()["library"]
    resp = client.put(f"/api/v2/libraries/{created['id']}", json={"enabled": False})
    assert resp.status_code == 200
    data = resp.get_json()["library"]
    assert data["enabled"] is False
    assert data["name"] == "Sonarr Main"


def test_update_library_not_found(client):
    resp = client.put("/api/v2/libraries/999", json={"enabled": False})
    assert resp.status_code == 404


def test_update_library_rejects_invalid_value(client):
    created = client.post("/api/v2/libraries", json=create_payload()).get_json()["library"]
    resp = client.put(f"/api/v2/libraries/{created['id']}", json={"type": "bogus"})
    assert resp.status_code == 400


def test_delete_library(client):
    created = client.post("/api/v2/libraries", json=create_payload()).get_json()["library"]
    resp = client.delete(f"/api/v2/libraries/{created['id']}")
    assert resp.status_code == 204
    resp = client.get(f"/api/v2/libraries/{created['id']}")
    assert resp.status_code == 404


def test_delete_library_not_found(client):
    resp = client.delete("/api/v2/libraries/999")
    assert resp.status_code == 404


def test_all_six_arr_types_accepted(client):
    for arr_type in ("sonarr", "radarr", "lidarr", "readarr", "whisparr", "eros"):
        resp = client.post(
            "/api/v2/libraries",
            json=create_payload(name=f"{arr_type}-instance", type=arr_type),
        )
        assert resp.status_code == 201, f"{arr_type} should be accepted"

import json

from app.adapters.redaction import redact_library, redact_libraries
from app.domain.arr_library import ArrLibrary


def make_library(**overrides):
    defaults = dict(
        id=1,
        name="Sonarr",
        type="sonarr",
        url="http://sonarr:8989",
        api_key="super-secret-key",
        enabled=True,
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:00+00:00",
    )
    defaults.update(overrides)
    return ArrLibrary(**defaults)


def test_redact_library_drops_api_key():
    lib = make_library()
    redacted = redact_library(lib)
    assert "api_key" not in redacted
    assert redacted["has_api_key"] is True


def test_redact_library_flags_missing_key():
    lib = make_library(api_key="")
    redacted = redact_library(lib)
    assert redacted["has_api_key"] is False


def test_redact_libraries_list():
    libs = [make_library(id=1), make_library(id=2, api_key="")]
    redacted = redact_libraries(libs)
    assert all("api_key" not in item for item in redacted)
    assert [item["has_api_key"] for item in redacted] == [True, False]


def test_api_list_response_never_contains_secret(client):
    client.post(
        "/api/v1/libraries",
        json={
            "name": "Sonarr",
            "type": "sonarr",
            "url": "http://sonarr:8989",
            "api_key": "totally-secret-value",
        },
    )
    resp = client.get("/api/v1/libraries")
    raw_body = resp.get_data(as_text=True)
    assert "totally-secret-value" not in raw_body
    data = json.loads(raw_body)
    assert data["libraries"][0]["has_api_key"] is True


def test_api_detail_response_never_contains_secret(client):
    created = client.post(
        "/api/v1/libraries",
        json={
            "name": "Radarr",
            "type": "radarr",
            "url": "http://radarr:7878",
            "api_key": "another-secret-value",
        },
    ).get_json()
    lib_id = created["library"]["id"]

    resp = client.get(f"/api/v1/libraries/{lib_id}")
    raw_body = resp.get_data(as_text=True)
    assert "another-secret-value" not in raw_body

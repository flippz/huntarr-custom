"""API-level tests for the Sonarr connection test and scan endpoints.
Uses the Flask test client plus a monkeypatched client_factory on the
already-constructed SonarrScanService - no real network access."""
import json

from app.adapters.sonarr_client import SonarrAuthError, SonarrConnectionError


class StubSonarrClient:
    def __init__(
        self,
        *,
        status=None,
        status_error=None,
        series=None,
        series_error=None,
        episodes_by_series=None,
    ):
        self._status = status or {"version": "4.0.1", "instance_name": "Sonarr"}
        self._status_error = status_error
        self._series = series if series is not None else []
        self._series_error = series_error
        self._episodes_by_series = episodes_by_series or {}

    def system_status(self):
        if self._status_error:
            raise self._status_error
        return self._status

    def get_series(self):
        if self._series_error:
            raise self._series_error
        return self._series

    def get_episodes(self, series_id):
        return self._episodes_by_series.get(series_id, [])


def use_stub(app, stub: StubSonarrClient):
    def factory(base_url, api_key, timeout=None):
        return stub

    app.extensions["managearr"]["sonarr_scan"]._client_factory = factory


def create_sonarr_library(client, **overrides):
    payload = {
        "name": "Sonarr Main",
        "type": "sonarr",
        "url": "http://sonarr:8989",
        "api_key": "abc123",
    }
    payload.update(overrides)
    resp = client.post("/api/v1/libraries", json=payload)
    return resp.get_json()["library"]


# --- /libraries/<id>/test ---------------------------------------------

def test_test_endpoint_success(app, client):
    lib = create_sonarr_library(client)
    use_stub(app, StubSonarrClient())

    resp = client.post(f"/api/v1/libraries/{lib['id']}/test")

    assert resp.status_code == 200
    assert resp.get_json()["status"]["version"] == "4.0.1"


def test_test_endpoint_library_not_found(client):
    resp = client.post("/api/v1/libraries/999/test")
    assert resp.status_code == 404


def test_test_endpoint_unsupported_library_type(client):
    lib = create_sonarr_library(client, name="Radarr Main", type="radarr")
    resp = client.post(f"/api/v1/libraries/{lib['id']}/test")
    assert resp.status_code == 400
    assert "unsupported library type" in resp.get_json()["errors"][0]


def test_test_endpoint_disabled_library(client):
    lib = create_sonarr_library(client)
    client.put(f"/api/v1/libraries/{lib['id']}", json={"enabled": False})
    resp = client.post(f"/api/v1/libraries/{lib['id']}/test")
    assert resp.status_code == 400
    assert "disabled" in resp.get_json()["errors"][0]


def test_test_endpoint_missing_api_key(app, client):
    lib_repo = app.extensions["managearr"]["library"].repository
    lib = lib_repo.create(
        {"name": "Sonarr", "type": "sonarr", "url": "http://sonarr:8989", "api_key": "", "enabled": True}
    )
    resp = client.post(f"/api/v1/libraries/{lib.id}/test")
    assert resp.status_code == 400
    assert "API key" in resp.get_json()["errors"][0]


def test_test_endpoint_timeout_returns_502_without_leaking_secrets(app, client):
    lib = create_sonarr_library(client, api_key="totally-secret-value", url="http://sonarr.internal:8989")
    use_stub(app, StubSonarrClient(status_error=SonarrConnectionError("Sonarr request timed out")))

    resp = client.post(f"/api/v1/libraries/{lib['id']}/test")

    assert resp.status_code == 502
    raw_body = resp.get_data(as_text=True)
    assert "totally-secret-value" not in raw_body
    assert "sonarr.internal" not in raw_body
    assert resp.get_json()["errors"] == ["Sonarr request timed out"]


def test_test_endpoint_auth_error_returns_502(app, client):
    lib = create_sonarr_library(client)
    use_stub(app, StubSonarrClient(status_error=SonarrAuthError("Sonarr rejected the configured API key")))
    resp = client.post(f"/api/v1/libraries/{lib['id']}/test")
    assert resp.status_code == 502


# --- /libraries/<id>/scan -----------------------------------------------

def test_scan_endpoint_success_persists_job_and_candidates(app, client):
    lib = create_sonarr_library(client)
    use_stub(
        app,
        StubSonarrClient(
            series=[{"id": 1, "title": "Show A", "monitored": True}],
            episodes_by_series={
                1: [
                    {
                        "id": 11,
                        "seasonNumber": 1,
                        "episodeNumber": 2,
                        "monitored": True,
                        "hasFile": False,
                        "airDate": "2026-01-01",
                    }
                ]
            },
        ),
    )

    resp = client.post(f"/api/v1/libraries/{lib['id']}/scan")

    assert resp.status_code == 201
    job = resp.get_json()["job"]
    assert job["state"] == "completed"
    assert job["job_type"] == "sonarr_scan"
    assert job["candidate_count"] == 1

    activity_resp = client.get("/api/v1/activity")
    jobs = activity_resp.get_json()["jobs"]
    assert any(j["id"] == job["id"] for j in jobs)

    candidates_resp = client.get(f"/api/v1/activity/{job['id']}/candidates")
    candidates = candidates_resp.get_json()["candidates"]
    assert len(candidates) == 1
    assert candidates[0]["series_title"] == "Show A"
    assert candidates[0]["episode_number"] == 2


def test_scan_endpoint_library_not_found(client):
    resp = client.post("/api/v1/libraries/999/scan")
    assert resp.status_code == 404


def test_scan_endpoint_unsupported_type_never_creates_job(client):
    lib = create_sonarr_library(client, name="Radarr Main", type="radarr")
    resp = client.post(f"/api/v1/libraries/{lib['id']}/scan")
    assert resp.status_code == 400
    assert client.get("/api/v1/activity").get_json()["jobs"] == []


def test_scan_endpoint_records_failed_job_on_upstream_error(app, client):
    lib = create_sonarr_library(client)
    use_stub(app, StubSonarrClient(series_error=SonarrConnectionError("Could not connect to the Sonarr host")))

    resp = client.post(f"/api/v1/libraries/{lib['id']}/scan")

    assert resp.status_code == 201
    job = resp.get_json()["job"]
    assert job["state"] == "failed"
    assert job["details"] == "Could not connect to the Sonarr host"
    assert client.get(f"/api/v1/activity/{job['id']}/candidates").get_json()["candidates"] == []


def test_activity_candidates_not_found_for_unknown_job(client):
    resp = client.get("/api/v1/activity/999/candidates")
    assert resp.status_code == 404


def test_repeat_scan_creates_a_new_job(app, client):
    lib = create_sonarr_library(client)
    use_stub(app, StubSonarrClient(series=[]))

    first = client.post(f"/api/v1/libraries/{lib['id']}/scan").get_json()["job"]
    second = client.post(f"/api/v1/libraries/{lib['id']}/scan").get_json()["job"]

    assert first["id"] != second["id"]
    jobs = client.get("/api/v1/activity").get_json()["jobs"]
    assert len([j for j in jobs if j["library_id"] == lib["id"]]) == 2

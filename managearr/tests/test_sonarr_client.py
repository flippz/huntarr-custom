"""Tests for the read-only Sonarr adapter. All network access is
mocked via a fake ``requests.Session`` - no real HTTP call is ever
made."""
import pytest
import requests

from app.adapters.sonarr_client import (
    SonarrAuthError,
    SonarrClient,
    SonarrConnectionError,
    SonarrDataError,
    SonarrResponseError,
)


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, raise_json_error=False):
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self._json_data = json_data
        self._raise_json_error = raise_json_error

    def json(self):
        if self._raise_json_error:
            raise ValueError("not json")
        return self._json_data


class FakeSession:
    def __init__(self, response=None, exception=None):
        self._response = response
        self._exception = exception
        self.calls = []

    def get(self, url, headers=None, params=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        if self._exception:
            raise self._exception
        return self._response


def make_client(session):
    return SonarrClient("http://sonarr.local:8989", "secret-key", session=session)


def test_system_status_success():
    session = FakeSession(FakeResponse(200, {"version": "4.0.1", "instanceName": "Sonarr"}))
    client = make_client(session)
    status = client.system_status()
    assert status == {"version": "4.0.1", "instance_name": "Sonarr"}


def test_get_series_success():
    session = FakeSession(FakeResponse(200, [{"id": 1, "title": "Show"}]))
    client = make_client(session)
    series = client.get_series()
    assert series == [{"id": 1, "title": "Show"}]


def test_get_episodes_success_passes_series_id_param():
    session = FakeSession(FakeResponse(200, []))
    client = make_client(session)
    client.get_episodes(42)
    assert session.calls[0]["params"] == {"seriesId": 42}


def test_request_never_leaks_api_key_into_url():
    session = FakeSession(FakeResponse(200, {"version": "4.0.1"}))
    client = make_client(session)
    client.system_status()
    call = session.calls[0]
    assert "secret-key" not in call["url"]
    assert call["headers"]["X-Api-Key"] == "secret-key"


def test_base_url_without_trailing_slash_is_joined_safely():
    session = FakeSession(FakeResponse(200, {"version": "4.0.1"}))
    client = make_client(session)
    client.system_status()
    assert session.calls[0]["url"] == "http://sonarr.local:8989/api/v3/system/status"


def test_timeout_raises_connection_error():
    session = FakeSession(exception=requests.exceptions.Timeout())
    client = make_client(session)
    with pytest.raises(SonarrConnectionError):
        client.system_status()


def test_unreachable_host_raises_connection_error():
    session = FakeSession(exception=requests.exceptions.ConnectionError())
    client = make_client(session)
    with pytest.raises(SonarrConnectionError):
        client.get_series()


def test_401_raises_auth_error():
    session = FakeSession(FakeResponse(401))
    client = make_client(session)
    with pytest.raises(SonarrAuthError):
        client.system_status()


def test_403_raises_auth_error():
    session = FakeSession(FakeResponse(403))
    client = make_client(session)
    with pytest.raises(SonarrAuthError):
        client.system_status()


def test_500_raises_response_error():
    session = FakeSession(FakeResponse(500))
    client = make_client(session)
    with pytest.raises(SonarrResponseError):
        client.system_status()


def test_non_json_body_raises_data_error():
    session = FakeSession(FakeResponse(200, raise_json_error=True))
    client = make_client(session)
    with pytest.raises(SonarrDataError):
        client.system_status()


def test_status_missing_version_raises_data_error():
    session = FakeSession(FakeResponse(200, {"instanceName": "Sonarr"}))
    client = make_client(session)
    with pytest.raises(SonarrDataError):
        client.system_status()


def test_series_response_not_a_list_raises_data_error():
    session = FakeSession(FakeResponse(200, {"unexpected": "shape"}))
    client = make_client(session)
    with pytest.raises(SonarrDataError):
        client.get_series()


def test_episodes_response_not_a_list_raises_data_error():
    session = FakeSession(FakeResponse(200, {"unexpected": "shape"}))
    client = make_client(session)
    with pytest.raises(SonarrDataError):
        client.get_episodes(1)


def test_error_messages_never_contain_url_or_api_key():
    session = FakeSession(FakeResponse(401))
    client = SonarrClient("http://user:pass@sonarr.internal:8989", "super-secret", session=session)
    try:
        client.system_status()
        assert False, "expected SonarrAuthError"
    except SonarrAuthError as exc:
        message = str(exc)
        assert "super-secret" not in message
        assert "sonarr.internal" not in message

"""Sonarr API v3 adapter.

Mostly a read-only client (system status, series, episode - all GETs),
plus exactly one write operation: ``search_episodes``, which issues
``POST /api/v3/command`` with command ``EpisodeSearch``. That is the
*only* way anything in this codebase can make Sonarr do something - see
``app/services/dispatch_service.py``, the only caller. No series/season
search, delete, file-change, or override endpoint exists here or
anywhere else in the adapter.

All raised exceptions carry static, safe messages - never the
configured base URL or API key - so callers can surface them directly
to a UI or log line without leaking secrets.
"""
from urllib.parse import urljoin

import requests

from ..domain.dispatch import MAX_SELECTION_PER_REQUEST

DEFAULT_TIMEOUT_SECONDS = 10


class SonarrError(Exception):
    """Base class for all Sonarr adapter errors."""


class SonarrConnectionError(SonarrError):
    """The Sonarr host could not be reached or timed out."""


class SonarrAuthError(SonarrError):
    """Sonarr rejected the configured API key."""


class SonarrResponseError(SonarrError):
    """Sonarr returned a non-2xx response for a reason other than auth."""


class SonarrDataError(SonarrError):
    """Sonarr returned a response that doesn't match the expected shape."""


def _safe_join(base_url: str, path: str) -> str:
    """Join a configured base URL with a fixed, adapter-controlled path.

    ``path`` is always a literal string from this module, never
    user-supplied, so this only needs to guard against a base URL
    missing a trailing slash (which would otherwise cause ``urljoin``
    to drop the last path segment of the base URL).
    """
    base = base_url.rstrip("/") + "/"
    return urljoin(base, path.lstrip("/"))


class SonarrClient:
    """Thin, read-only wrapper around a handful of Sonarr v3 endpoints."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        session: "requests.Session | None" = None,
    ):
        self._base_url = base_url
        self._api_key = api_key
        self._timeout = timeout
        self._session = session or requests.Session()

    def _send(self, request_fn, *args, **kwargs):
        """Shared timeout/connection/auth/status/JSON handling for both
        the read-only GET path and the single write POST path - see
        ``_get``/``_post`` below."""
        try:
            response = request_fn(*args, **kwargs)
        except requests.exceptions.Timeout as exc:
            raise SonarrConnectionError("Sonarr request timed out") from exc
        except requests.exceptions.ConnectionError as exc:
            raise SonarrConnectionError("Could not connect to the Sonarr host") from exc
        except requests.exceptions.RequestException as exc:
            raise SonarrConnectionError("Sonarr request failed") from exc

        if response.status_code in (401, 403):
            raise SonarrAuthError("Sonarr rejected the configured API key")
        if not response.ok:
            raise SonarrResponseError(f"Sonarr returned HTTP {response.status_code}")

        try:
            return response.json()
        except ValueError as exc:
            raise SonarrDataError("Sonarr returned a non-JSON response") from exc

    def _get(self, path: str, *, params: dict | None = None):
        url = _safe_join(self._base_url, path)
        headers = {"X-Api-Key": self._api_key}
        return self._send(self._session.get, url, headers=headers, params=params, timeout=self._timeout)

    def _post(self, path: str, json_body: dict):
        url = _safe_join(self._base_url, path)
        headers = {"X-Api-Key": self._api_key}
        return self._send(self._session.post, url, headers=headers, json=json_body, timeout=self._timeout)

    def system_status(self) -> dict:
        """Read-only connectivity/version check. Never mutates Sonarr."""
        data = self._get("/api/v3/system/status")
        if not isinstance(data, dict) or "version" not in data:
            raise SonarrDataError("Sonarr system status response was missing expected fields")
        return {
            "version": data.get("version"),
            "instance_name": data.get("instanceName"),
        }

    def get_series(self) -> list[dict]:
        """All series known to Sonarr. Read-only."""
        data = self._get("/api/v3/series")
        if not isinstance(data, list):
            raise SonarrDataError("Sonarr series response was not a list")
        return data

    def get_episodes(self, series_id: int) -> list[dict]:
        """All episodes for one series. Read-only."""
        data = self._get("/api/v3/episode", params={"seriesId": series_id})
        if not isinstance(data, list):
            raise SonarrDataError("Sonarr episode response was not a list")
        return data

    def search_episodes(self, episode_ids: list[int]) -> dict:
        """The only write operation this adapter exposes: dispatch one
        Sonarr ``EpisodeSearch`` command for the given episode ids.

        Issues exactly one ``POST /api/v3/command``. Sonarr queues a
        single search job covering every id in ``episode_ids`` and
        returns one command; there is no per-episode success/failure in
        this response, only a command id/name/status to track it by.
        Callers (``DispatchService``) never hold a database transaction
        across this call.
        """
        if (
            not isinstance(episode_ids, list)
            or not episode_ids
            or len(episode_ids) > MAX_SELECTION_PER_REQUEST
            or any(not isinstance(value, int) or isinstance(value, bool) or value <= 0 for value in episode_ids)
            or len(set(episode_ids)) != len(episode_ids)
        ):
            raise ValueError(
                f"episode_ids must contain 1-{MAX_SELECTION_PER_REQUEST} unique positive integers"
            )
        data = self._post("/api/v3/command", {"name": "EpisodeSearch", "episodeIds": list(episode_ids)})
        if (
            not isinstance(data, dict)
            or not isinstance(data.get("id"), int)
            or isinstance(data.get("id"), bool)
            or data["id"] <= 0
            or ("name" in data and data["name"] != "EpisodeSearch")
            or (data.get("status") is not None and not isinstance(data.get("status"), str))
        ):
            raise SonarrDataError("Sonarr command response was missing expected fields")
        return {
            "id": data.get("id"),
            "name": data.get("name", "EpisodeSearch"),
            "status": data.get("status"),
        }

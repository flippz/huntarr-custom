"""Read-only Sonarr API v3 adapter.

This client only ever issues GET requests against read-only endpoints
(system status, series, episode). It never sends a Sonarr command (no
``/api/v3/command`` calls) and never mutates Sonarr state in any way.

All raised exceptions carry static, safe messages - never the
configured base URL or API key - so callers can surface them directly
to a UI or log line without leaking secrets.
"""
from urllib.parse import urljoin

import requests

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

    def _get(self, path: str, *, params: dict | None = None):
        url = _safe_join(self._base_url, path)
        headers = {"X-Api-Key": self._api_key}
        try:
            response = self._session.get(url, headers=headers, params=params, timeout=self._timeout)
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

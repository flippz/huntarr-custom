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
from datetime import datetime
from urllib.parse import urljoin

import requests

from ..domain.dispatch import MAX_SELECTION_PER_REQUEST

DEFAULT_TIMEOUT_SECONDS = 10
MAX_READ_PAGE = 10
MAX_READ_PAGE_SIZE = 100


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

    @staticmethod
    def _positive_int(value, field: str) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise SonarrDataError(f"Sonarr response had an invalid {field}")
        return value

    @staticmethod
    def _bounded_text(value, field: str, *, required: bool = False, limit: int = 255):
        if value is None and not required:
            return None
        if not isinstance(value, str) or (required and not value) or len(value) > limit:
            raise SonarrDataError(f"Sonarr response had an invalid {field}")
        return value

    @classmethod
    def _timestamp_text(cls, value, field: str, *, required: bool = False):
        value = cls._bounded_text(value, field, required=required, limit=64)
        if value is None:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise SonarrDataError(f"Sonarr response had an invalid {field}") from exc
        if parsed.tzinfo is None:
            raise SonarrDataError(f"Sonarr response had an invalid {field}")
        return value

    @staticmethod
    def _page_params(page: int, page_size: int) -> dict:
        if (
            not isinstance(page, int) or isinstance(page, bool) or not 1 <= page <= MAX_READ_PAGE
            or not isinstance(page_size, int) or isinstance(page_size, bool)
            or not 1 <= page_size <= MAX_READ_PAGE_SIZE
        ):
            raise ValueError(
                f"page must be 1-{MAX_READ_PAGE} and page_size must be 1-{MAX_READ_PAGE_SIZE}"
            )
        return {"page": page, "pageSize": page_size}

    def get_command(self, command_id: int) -> dict:
        """Read one already-known Sonarr command; never creates a command."""
        command_id = self._positive_int(command_id, "command id")
        data = self._get(f"/api/v3/command/{command_id}")
        if not isinstance(data, dict):
            raise SonarrDataError("Sonarr command status response was not an object")
        returned_id = self._positive_int(data.get("id"), "command id")
        if returned_id != command_id:
            raise SonarrDataError("Sonarr command status response did not match the requested command")
        name = self._bounded_text(data.get("name"), "command name", required=True, limit=100)
        if name != "EpisodeSearch":
            raise SonarrDataError("Sonarr command status response was not EpisodeSearch")
        status = self._bounded_text(data.get("status"), "command status", required=True, limit=64)
        return {
            "id": returned_id,
            "name": name,
            "status": status.lower(),
        }

    def get_history(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        episode_id: int | None = None,
    ) -> dict:
        """Read one bounded history page, optionally filtered by episode."""
        params = self._page_params(page, page_size)
        params.update({"sortKey": "date", "sortDirection": "descending", "includeEpisode": False})
        if episode_id is not None:
            params["episodeId"] = self._positive_int(episode_id, "episode id")
        data = self._get("/api/v3/history", params=params)
        if not isinstance(data, dict) or not isinstance(data.get("records"), list):
            raise SonarrDataError("Sonarr history response was missing a records list")
        records = data["records"]
        if len(records) > page_size:
            raise SonarrDataError("Sonarr history response exceeded the requested page size")
        safe_records = []
        for record in records:
            if not isinstance(record, dict):
                raise SonarrDataError("Sonarr history record was not an object")
            event_id = self._positive_int(record.get("id"), "history event id")
            event_type = self._bounded_text(
                record.get("eventType"), "history event type", required=True, limit=100
            )
            nested_episode = record.get("episode") if isinstance(record.get("episode"), dict) else {}
            record_episode_id = record.get("episodeId", nested_episode.get("id"))
            if record_episode_id is not None:
                record_episode_id = self._positive_int(record_episode_id, "history episode id")
            date = self._timestamp_text(record.get("date"), "history date", required=True)
            download_id = record.get("downloadId")
            if download_id is not None:
                download_id = self._bounded_text(str(download_id), "history download id", limit=255)
            safe_records.append(
                {
                    "id": event_id,
                    "event_type": event_type,
                    "episode_id": record_episode_id,
                    "date": date,
                    "download_id": download_id,
                }
            )
        total_records = data.get("totalRecords", len(records))
        if not isinstance(total_records, int) or isinstance(total_records, bool) or total_records < 0:
            raise SonarrDataError("Sonarr history response had an invalid total record count")
        return {"page": page, "page_size": page_size, "total_records": total_records, "records": safe_records}

    def get_queue_details(self, *, page: int = 1, page_size: int = 100) -> dict:
        """Read one bounded queue-details page with episode identifiers."""
        params = self._page_params(page, page_size)
        params.update({"includeEpisode": True, "includeSeries": False})
        data = self._get("/api/v3/queue/details", params=params)
        if isinstance(data, list):
            records = data
            total_records = len(records)
        elif isinstance(data, dict) and isinstance(data.get("records"), list):
            records = data["records"]
            total_records = data.get("totalRecords", len(records))
        else:
            raise SonarrDataError("Sonarr queue response was missing a records list")
        if len(records) > page_size:
            raise SonarrDataError("Sonarr queue response exceeded the requested page size")
        if not isinstance(total_records, int) or isinstance(total_records, bool) or total_records < 0:
            raise SonarrDataError("Sonarr queue response had an invalid total record count")
        safe_records = []
        for record in records:
            if not isinstance(record, dict):
                raise SonarrDataError("Sonarr queue record was not an object")
            nested_episode = record.get("episode") if isinstance(record.get("episode"), dict) else {}
            raw_episode_ids = record.get("episodeIds")
            if raw_episode_ids is None:
                one_id = record.get("episodeId", nested_episode.get("id"))
                raw_episode_ids = [] if one_id is None else [one_id]
            if not isinstance(raw_episode_ids, list) or len(raw_episode_ids) > MAX_SELECTION_PER_REQUEST:
                raise SonarrDataError("Sonarr queue record had invalid episode ids")
            episode_ids = [self._positive_int(value, "queue episode id") for value in raw_episode_ids]
            status = self._bounded_text(record.get("status"), "queue status", required=True, limit=64)
            tracked_state = self._bounded_text(record.get("trackedDownloadState"), "queue tracked state", limit=64)
            added = self._timestamp_text(record.get("added"), "queue added date")
            download_id = record.get("downloadId", record.get("id"))
            if download_id is not None:
                download_id = self._bounded_text(str(download_id), "queue download id", limit=255)
            safe_records.append(
                {
                    "episode_ids": episode_ids,
                    "status": status.lower(),
                    "tracked_state": tracked_state.lower() if tracked_state else None,
                    "download_id": download_id,
                    "added": added,
                }
            )
        return {"page": page, "page_size": page_size, "total_records": total_records, "records": safe_records}

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

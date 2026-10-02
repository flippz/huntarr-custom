"""Sonarr API v3 adapter.

Read operations cover status, series, episodes, release search, routing metadata,
history, and queue evidence. Two narrowly scoped writes are exposed:
search_episodes retains the existing confirmed EpisodeSearch command, and
 grab_release posts one exact cached release selected by the manual-only M11
service. No broad SeasonSearch, series search, delete, file mutation, or Sonarr
configuration write exists here.

Raised messages never include the configured base URL or API key. Bounded Sonarr
validation reasons may be surfaced so operators can understand release rejection.
"""
from datetime import datetime
from urllib.parse import urljoin

import requests

from ..domain.dispatch import MAX_SELECTION_PER_REQUEST
from ..domain.season_pack import normalize_protocol, strict_int

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


class SonarrNotFoundError(SonarrResponseError):
    """A previously known Sonarr resource is no longer retained."""


class SonarrDataError(SonarrError):
    """Sonarr returned a response that doesn't match the expected shape."""

class SonarrPostRejectedError(SonarrResponseError):
    """The release POST received a definite 4xx HTTP rejection."""

class SonarrPostAmbiguousError(SonarrError):
    """A release POST was attempted and acceptance cannot be disproved."""


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
        if response.status_code == 404:
            raise SonarrNotFoundError("Sonarr returned HTTP 404")
        if not response.ok:
            reason = ""
            try:
                problem = response.json()
                if isinstance(problem, list):
                    texts = [x.get("errorMessage") for x in problem if isinstance(x, dict)]
                    texts = [x for x in texts if isinstance(x, str) and 0 < len(x) <= 300]
                    reason = "; ".join(texts[:5])
                elif isinstance(problem, dict):
                    text = problem.get("message")
                    if isinstance(text, str) and 0 < len(text) <= 300: reason = text
            except ValueError:
                pass
            suffix = f": {reason}" if reason else ""
            raise SonarrResponseError(f"Sonarr returned HTTP {response.status_code}{suffix}")

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

    def get_cutoff_unmet_episodes(self, *, page: int = 1, page_size: int = 100) -> dict:
        """Read one bounded deterministic page from wanted/cutoff."""
        params = self._page_params(page, page_size)
        params.update({
            "includeSeries": True,
            "monitored": True,
            "sortKey": "airDateUtc",
            "sortDirection": "ascending",
        })
        data = self._get("/api/v3/wanted/cutoff", params=params)
        if not isinstance(data, dict) or not isinstance(data.get("records"), list):
            raise SonarrDataError("Sonarr cutoff response was missing a records list")
        records = data["records"]
        total_records = data.get("totalRecords")
        if (
            len(records) > page_size
            or not isinstance(total_records, int)
            or isinstance(total_records, bool)
            or total_records < 0
        ):
            raise SonarrDataError("Sonarr cutoff response had invalid pagination")
        return {"records": records, "total_records": total_records}

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
            download_id = record.get("downloadId")
            if download_id is not None:
                download_id = self._bounded_text(str(download_id), "queue download id", limit=255)
            # The queue record's own positive integer id is the identity
            # used for the destructive DELETE below. It is deliberately
            # retained separately from downloadId (the client download
            # identity, which Sonarr may reuse/rotate across queue records
            # and is never a valid DELETE target). Fail closed if it is not
            # a strictly positive integer.
            queue_id = self._positive_int(record.get("id"), "queue record id")
            title = self._bounded_text(record.get("title"), "queue title", limit=500) or ""
            size = record.get("size")
            if size is not None and (not isinstance(size, (int, float)) or isinstance(size, bool) or size < 0):
                raise SonarrDataError("Sonarr queue record had an invalid size")
            sizeleft = record.get("sizeleft")
            if sizeleft is not None and (
                not isinstance(sizeleft, (int, float)) or isinstance(sizeleft, bool) or sizeleft < 0
            ):
                raise SonarrDataError("Sonarr queue record had an invalid sizeleft")
            timeleft = self._bounded_text(record.get("timeleft"), "queue timeleft", limit=32)
            error_message = self._bounded_text(record.get("errorMessage"), "queue error message", limit=500)
            status_messages = record.get("statusMessages")
            safe_status_messages = []
            if isinstance(status_messages, list):
                for entry in status_messages[:20]:
                    if isinstance(entry, dict):
                        title_text = entry.get("title")
                        messages = entry.get("messages")
                        safe_status_messages.append(
                            {
                                "title": title_text[:255] if isinstance(title_text, str) else None,
                                "messages": [m[:500] for m in messages if isinstance(m, str)][:10]
                                if isinstance(messages, list)
                                else [],
                            }
                        )
            safe_records.append(
                {
                    "queue_id": queue_id,
                    "episode_ids": episode_ids,
                    "status": status.lower(),
                    "tracked_state": tracked_state.lower() if tracked_state else None,
                    "download_id": download_id,
                    "added": added,
                    "title": title,
                    "size": int(size) if size is not None else None,
                    "sizeleft": int(sizeleft) if sizeleft is not None else None,
                    "timeleft": timeleft,
                    "error_message": error_message,
                    "status_messages": safe_status_messages,
                }
            )
        return {"page": page, "page_size": page_size, "total_records": total_records, "records": safe_records}

    def delete_queue_record(
        self, queue_id: int, *, remove_from_client: bool, blocklist: bool, skip_redownload: bool = False
    ) -> None:
        """Remove one Sonarr queue record via ``DELETE /api/v3/queue/{id}``.

        ``queue_id`` must be the queue record's own positive integer id (see
        ``get_queue_details`` above) - never ``downloadId``. This method
        narrowly distinguishes three outcomes so callers can fail closed on
        uncertainty:

        * returns normally only on a definite HTTP 2xx/404 (404 means the
          record is already gone, which is an acceptable terminal state for
          a removal request - not an error);
        * raises ``SonarrPostRejectedError`` on a definite 4xx (401/403 is
          surfaced as ``SonarrAuthError``, not treated as ambiguous, since
          it is also a definite non-2xx outcome and no DELETE could have
          been accepted);
        * raises ``SonarrPostAmbiguousError`` for anything else where
          acceptance cannot be disproved: connection errors, timeouts, and
          5xx responses. Callers must never automatically retry after this.

        Never includes the configured base URL or API key in any raised
        message.
        """
        if not isinstance(queue_id, int) or isinstance(queue_id, bool) or queue_id <= 0:
            raise ValueError("queue_id must be a positive integer")
        if not isinstance(remove_from_client, bool) or not isinstance(blocklist, bool) or not isinstance(skip_redownload, bool):
            raise ValueError("remove_from_client, blocklist, and skip_redownload must be booleans")
        url = _safe_join(self._base_url, f"/api/v3/queue/{queue_id}")
        headers = {"X-Api-Key": self._api_key}
        params = {
            "removeFromClient": "true" if remove_from_client else "false",
            "blocklist": "true" if blocklist else "false",
            "skipRedownload": "true" if skip_redownload else "false",
        }
        try:
            response = self._session.delete(url, headers=headers, params=params, timeout=self._timeout)
        except requests.exceptions.RequestException as exc:
            raise SonarrPostAmbiguousError("Sonarr queue removal outcome is unknown") from exc
        if response.status_code == 404:
            # Already gone - treat as a successful terminal removal.
            return
        if response.status_code in (401, 403):
            raise SonarrAuthError("Sonarr rejected the configured API key")
        if 400 <= response.status_code < 500:
            reason = ""
            try:
                problem = response.json()
                if isinstance(problem, dict):
                    text = problem.get("message")
                    if isinstance(text, str) and 0 < len(text) <= 300:
                        reason = text
                elif isinstance(problem, list):
                    texts = [x.get("errorMessage") for x in problem if isinstance(x, dict)]
                    texts = [x for x in texts if isinstance(x, str) and 0 < len(x) <= 300]
                    reason = "; ".join(texts[:5])
            except (ValueError, TypeError):
                pass
            suffix = f": {reason}" if reason else ""
            raise SonarrPostRejectedError(f"Sonarr definitely rejected the queue removal with HTTP {response.status_code}{suffix}")
        if not response.ok:
            raise SonarrPostAmbiguousError(f"Sonarr queue removal outcome is unknown after HTTP {response.status_code}")
        return

    def get_series_detail(self, series_id: int) -> dict:
        series_id = self._positive_int(series_id, "series id")
        data = self._get(f"/api/v3/series/{series_id}")
        if not isinstance(data, dict): raise SonarrDataError("Sonarr series detail response was not an object")
        return data

    def get_download_clients(self) -> list[dict]:
        data = self._get("/api/v3/downloadclient")
        if not isinstance(data, list): raise SonarrDataError("Sonarr download-client response was not a list")
        result = []
        for item in data:
            if not isinstance(item, dict) or not strict_int(item.get("id"), positive=True): raise SonarrDataError("Sonarr download-client response was malformed")
            name, protocol = item.get("name"), normalize_protocol(item.get("protocol"))
            if not isinstance(name, str) or not name or len(name) > 255 or protocol is None or not isinstance(item.get("enable"), bool): raise SonarrDataError("Sonarr download-client response was malformed")
            result.append({"id": item["id"], "name": name, "protocol": protocol, "enabled": item["enable"]})
        return result

    def search_season_releases(self, series_id: int, season_number: int) -> list[dict]:
        series_id = self._positive_int(series_id, "series id")
        season_number = self._positive_int(season_number, "season number")
        data = self._get("/api/v3/release", params={"seriesId": series_id, "seasonNumber": season_number})
        if not isinstance(data, list) or len(data) > 1000: raise SonarrDataError("Sonarr release search response was malformed or unbounded")
        return data

    def grab_release(self, body: dict) -> dict:
        allowed = {"guid", "indexerId", "downloadClientId", "shouldOverride", "seriesId", "episodeIds", "quality", "languages"}
        if not isinstance(body, dict) or set(body) - allowed or not isinstance(body.get("guid"), str) or not body["guid"]: raise ValueError("invalid exact release grab body")
        for key in ("indexerId", "downloadClientId"):
            if not strict_int(body.get(key), positive=True): raise ValueError("invalid exact release grab body")
        override_keys={"shouldOverride","seriesId","episodeIds","quality","languages"}
        if body.get("shouldOverride") is True:
            episode_ids=body.get("episodeIds")
            if (not strict_int(body.get("seriesId"),positive=True) or not isinstance(episode_ids,list) or not episode_ids
                    or any(not strict_int(x,positive=True) for x in episode_ids) or len(episode_ids)!=len(set(episode_ids))
                    or not isinstance(body.get("quality"),dict) or not isinstance(body.get("languages"),list)):
                raise ValueError("invalid exact release grab body")
        elif set(body)&override_keys:
            raise ValueError("invalid exact release grab body")
        url = _safe_join(self._base_url, "/api/v3/release")
        headers = {"X-Api-Key": self._api_key}
        try:
            response = self._session.post(url, headers=headers, json=body, timeout=self._timeout)
        except requests.exceptions.RequestException as exc:
            raise SonarrPostAmbiguousError("Sonarr release grab outcome is unknown") from exc
        if not response.ok:
            if not 400 <= response.status_code < 500:
                raise SonarrPostAmbiguousError(f"Sonarr release grab outcome is unknown after HTTP {response.status_code}")
            reason=""
            try:
                problem=response.json()
                if isinstance(problem,list):
                    texts=[x.get("errorMessage") for x in problem if isinstance(x,dict)]
                    texts=[x for x in texts if isinstance(x,str) and 0<len(x)<=300]
                    reason="; ".join(texts[:5])
            except (ValueError,TypeError):pass
            suffix=f": {reason}" if reason else ""
            raise SonarrPostRejectedError(f"Sonarr definitely rejected the release grab with HTTP {response.status_code}{suffix}")
        try:
            data=response.json()
        except (ValueError, TypeError) as exc:
            raise SonarrPostAmbiguousError("Sonarr release grab returned an invalid success response") from exc
        if (not isinstance(data,dict) or not isinstance(data.get("guid"),str) or not data["guid"]
                or not strict_int(data.get("indexerId"),positive=True)
                or data["guid"]!=body["guid"] or data["indexerId"]!=body["indexerId"]):
            raise SonarrPostAmbiguousError("Sonarr release grab returned an invalid success response")
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

"""
Real-Debrid API client for Magnetarr.

Adds discovered magnets to the user's Real-Debrid account so a separate tool
(e.g. Zurg) can mount the cached content and generate .strm files. Real-Debrid
requires an explicit file-selection step after adding a magnet before it will
actually start fetching it — addMagnet alone just parks it in
"waiting_files_selection".
"""

import time
from typing import Callable, Optional, Tuple

import requests

from src.primary.utils.logger import get_logger

magnetarr_logger = get_logger("magnetarr")

RD_BASE_URL = "https://api.real-debrid.com/rest/1.0"


def _headers(api_token: str) -> dict:
    return {"Authorization": f"Bearer {api_token}"}


def _with_backoff(request_fn: Callable[[], requests.Response], max_retries: int = 3) -> Tuple[Optional[requests.Response], str]:
    """Run a request, retrying with backoff on HTTP 429 (Real-Debrid allows 250 req/min,
    but bursts — e.g. bulk-scanning dozens of new magnets at once — can still trip it).
    Returns (response_or_none, error_message)."""
    delay = 2
    for attempt in range(max_retries + 1):
        try:
            resp = request_fn()
        except requests.exceptions.RequestException as e:
            return None, f"Real-Debrid request error: {e}"

        if resp.status_code == 429:
            if attempt >= max_retries:
                return None, f"Real-Debrid rate limited (429) after {max_retries} retries"
            magnetarr_logger.debug(f"Real-Debrid rate limited, backing off {delay}s")
            time.sleep(delay)
            delay *= 2
            continue

        return resp, ""
    return None, "Exhausted retries"


def rd_add_and_select(magnet_uri: str, api_token: str) -> Tuple[Optional[str], str]:
    """Add a magnet to Real-Debrid and select all files so it starts downloading.
    Returns (torrent_id_or_none, error_message)."""
    resp, err = _with_backoff(lambda: requests.post(
        f"{RD_BASE_URL}/torrents/addMagnet",
        headers=_headers(api_token),
        data={"magnet": magnet_uri},
        timeout=20,
    ))
    if resp is None:
        return None, err
    if resp.status_code != 201:
        return None, f"Real-Debrid addMagnet failed (HTTP {resp.status_code}): {resp.text[:200]}"

    try:
        torrent_id = resp.json()["id"]
    except (ValueError, KeyError):
        return None, "Real-Debrid addMagnet returned an unexpected response"

    select_resp, select_err = _with_backoff(lambda: requests.post(
        f"{RD_BASE_URL}/torrents/selectFiles/{torrent_id}",
        headers=_headers(api_token),
        data={"files": "all"},
        timeout=20,
    ))
    if select_resp is None:
        return torrent_id, select_err
    if select_resp.status_code != 204:
        return torrent_id, f"Real-Debrid selectFiles failed (HTTP {select_resp.status_code}): {select_resp.text[:200]}"

    return torrent_id, ""


def rd_get_status(torrent_id: str, api_token: str) -> Tuple[Optional[str], str]:
    """Get the current status string for a Real-Debrid torrent (e.g. 'downloaded',
    'downloading', 'magnet_error'). Returns (status_or_none, error_message)."""
    resp, err = _with_backoff(lambda: requests.get(
        f"{RD_BASE_URL}/torrents/info/{torrent_id}",
        headers=_headers(api_token),
        timeout=20,
    ))
    if resp is None:
        return None, err
    if resp.status_code != 200:
        return None, f"Real-Debrid info failed (HTTP {resp.status_code}): {resp.text[:200]}"

    try:
        return resp.json().get("status"), ""
    except ValueError:
        return None, "Real-Debrid info returned an unexpected response"


def rd_delete_torrent(torrent_id: str, api_token: str) -> str:
    """Delete an existing Real-Debrid torrent entry. Returns an error message, or '' on success."""
    if not torrent_id:
        return ""
    resp, err = _with_backoff(lambda: requests.delete(
        f"{RD_BASE_URL}/torrents/delete/{torrent_id}",
        headers=_headers(api_token),
        timeout=20,
    ))
    if resp is None:
        return err
    if resp.status_code not in (204, 404):
        # 404 just means it's already gone — treat as success either way.
        return f"Real-Debrid delete failed (HTTP {resp.status_code}): {resp.text[:200]}"

    return ""

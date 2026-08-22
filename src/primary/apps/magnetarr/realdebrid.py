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


# Real-Debrid torrent statuses (from /torrents/info). After addMagnet a torrent
# first sits in magnet_conversion/queued while RD resolves the magnet's metadata;
# only once it reaches waiting_files_selection does the file list exist and can
# selectFiles succeed. Calling selectFiles before then returns 404
# "unknown_ressource", which used to mark the whole submission errored.
_RD_STATUS_PENDING = {"magnet_conversion", "queued"}          # keep waiting
_RD_STATUS_READY_TO_SELECT = {"waiting_files_selection"}       # do selectFiles now
_RD_STATUS_ALREADY_SELECTED = {"downloading", "downloaded", "compressing", "uploading"}  # files already chosen
_RD_STATUS_FATAL = {"magnet_error", "error", "virus", "dead"}  # give up

# How long to wait for RD to finish converting a fresh magnet before selecting
# files. Conversion is normally a few seconds for cached content; cap the total
# so a stuck magnet can't block the caller (the recheck loop will retry later).
_RD_CONVERT_MAX_ATTEMPTS = 15
_RD_CONVERT_DELAY_SECONDS = 2


def _rd_select_all_files(torrent_id: str, api_token: str) -> Tuple[bool, str]:
    """Call selectFiles=all for a torrent. Returns (ok, error_message). A 404 here
    means the torrent isn't selectable yet (still converting) — reported as an error
    so the caller can retry rather than treat it as done."""
    select_resp, select_err = _with_backoff(lambda: requests.post(
        f"{RD_BASE_URL}/torrents/selectFiles/{torrent_id}",
        headers=_headers(api_token),
        data={"files": "all"},
        timeout=20,
    ))
    if select_resp is None:
        return False, select_err
    if select_resp.status_code == 204:
        return True, ""
    return False, f"Real-Debrid selectFiles failed (HTTP {select_resp.status_code}): {select_resp.text[:200]}"


def rd_add_and_select(magnet_uri: str, api_token: str) -> Tuple[Optional[str], str]:
    """Add a magnet to Real-Debrid, wait for it to finish magnet conversion, then
    select all files so it starts downloading. Returns (torrent_id_or_none,
    error_message)."""
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

    # Poll until the magnet has converted and is ready for file selection. Once
    # ready we select all files; if RD has already auto-selected (status moved
    # straight to downloading/downloaded) we're done.
    last_status = None
    for attempt in range(_RD_CONVERT_MAX_ATTEMPTS):
        status, status_err = rd_get_status(torrent_id, api_token)
        last_status = status
        if status is None:
            # transient info error — back off briefly and retry
            time.sleep(_RD_CONVERT_DELAY_SECONDS)
            continue
        if status == RD_STATUS_DELETED:
            return torrent_id, "Real-Debrid torrent vanished during magnet conversion"
        if status in _RD_STATUS_FATAL:
            return torrent_id, f"Real-Debrid magnet conversion failed (status '{status}')"
        if status in _RD_STATUS_ALREADY_SELECTED:
            return torrent_id, ""
        if status in _RD_STATUS_READY_TO_SELECT:
            ok, select_err = _rd_select_all_files(torrent_id, api_token)
            if ok:
                return torrent_id, ""
            # Not selectable yet despite the status (rare race) — keep waiting.
            time.sleep(_RD_CONVERT_DELAY_SECONDS)
            continue
        # magnet_conversion / queued / anything unexpected: wait and re-poll
        time.sleep(_RD_CONVERT_DELAY_SECONDS)

    return torrent_id, f"Real-Debrid magnet still converting after {_RD_CONVERT_MAX_ATTEMPTS} checks (last status '{last_status}')"


RD_STATUS_DELETED = "__deleted__"  # sentinel: the torrent no longer exists in the account
# (e.g. the user removed it manually via Real-Debrid's own UI) — distinct from a normal
# lookup failure, since callers need to tell "gone, should resubmit" from "transient error".


def rd_get_status(torrent_id: str, api_token: str) -> Tuple[Optional[str], str]:
    """Get the current status string for a Real-Debrid torrent (e.g. 'downloaded',
    'downloading', 'magnet_error', or RD_STATUS_DELETED if it no longer exists in the
    account). Returns (status_or_none, error_message)."""
    resp, err = _with_backoff(lambda: requests.get(
        f"{RD_BASE_URL}/torrents/info/{torrent_id}",
        headers=_headers(api_token),
        timeout=20,
    ))
    if resp is None:
        return None, err
    if resp.status_code == 404:
        return RD_STATUS_DELETED, ""
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

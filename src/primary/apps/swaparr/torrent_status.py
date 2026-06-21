"""
Best-effort torrent status lookup for the Swaparr Activity view.

The configured "seed_check_torrent_client" (e.g. Decypharr, which emulates qBittorrent's
WebUI API) is queried for a batch of torrent hashes so the Activity view can show what the
actual download client reports for each Sonarr queue item, alongside Sonarr's own status and
Swaparr's strike count. Any failure here must never break the Activity endpoint.
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from src.primary.utils.logger import get_logger

logger = get_logger("swaparr")

_TS_RE = r'^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\|\s*\w+\s*\|'

# Decypharr's own log lines carry the real debrid-level failure reason in a few different
# shapes - none of which Sonarr's history ever sees:
#   ERROR | [manager] Error checking status error="torrent: X has error" name="X"
#   DEBUG | [dfs] download error error="...marked as bad" count=1 entry=X filename=Y
_DECYPHARR_ERROR_LOG_RE = re.compile(
    _TS_RE + r'.*?error="(?P<error>[^"]*)".*?(?:name|entry)=(?:"(?P<name_q>[^"]*)"|(?P<name_bare>\S+))'
)

# INFO | [manager] Creating symlinks for 0 files in /path/to/sonarr/<release name> component=...
# A "0 files" symlink step means the debrid provider's cache had nothing usable for this
# release (commonly a fake/dead torrent) - Sonarr only ever sees this as a generic import
# failure. There's no name=/entry= field on this line - the release name is the final path
# segment of the symlink destination.
_DECYPHARR_ZERO_SYMLINK_RE = re.compile(
    _TS_RE + r'.*?Creating symlinks for (?P<file_count>\d+) files in \S*/(?P<name_bare>[^/\s]+)\s'
)


def _normalize_name(name: str) -> str:
    return re.sub(r'[^a-z0-9]+', '', (name or "").lower())


def get_torrent_statuses(instance_data: Dict[str, Any], download_ids: List[str]) -> Dict[str, Any]:
    """Look up torrent status for the given download IDs (torrent hashes) via the instance's
    configured torrent client. Returns {hash_lower: {state, progress, eta, ...}}, or {} if no
    client is configured, unsupported, or unreachable."""
    download_ids = [d for d in (download_ids or []) if d]
    if not download_ids:
        return {}

    client_config = instance_data.get("seed_check_torrent_client") or {}
    client_type = (client_config.get("type") or "").lower()

    if client_type == "qbittorrent":
        return _get_qbittorrent_statuses(client_config, download_ids)

    return {}


def _get_qbittorrent_statuses(client_config: Dict[str, Any], download_ids: List[str]) -> Dict[str, Any]:
    try:
        from src.primary.apps.tor_hunt.qbittorrent_client import QBittorrentClient

        host = client_config.get("host")
        if not host:
            return {}

        client = QBittorrentClient(
            host=host,
            port=int(client_config.get("port", 8080)),
            username=client_config.get("username", ""),
            password=client_config.get("password", ""),
            use_ssl=bool(client_config.get("use_ssl", False))
        )

        # Decypharr (unlike real qBittorrent) doesn't honor the `hashes` filter param and
        # returns an empty list when it's passed, so fetch the full torrent list and match
        # client-side instead.
        wanted = {d.lower() for d in download_ids}
        torrents = client.get_torrents()
        statuses = {}
        for torrent in torrents:
            torrent_hash = (torrent.get("hash") or "").lower()
            if not torrent_hash or torrent_hash not in wanted:
                continue
            statuses[torrent_hash] = {
                "state": torrent.get("state"),
                "progress": torrent.get("progress"),
                "eta": torrent.get("eta"),
                "num_seeds": torrent.get("num_seeds"),
                "dlspeed": torrent.get("dlspeed"),
            }
        return statuses
    except Exception as e:
        logger.debug(f"Torrent status lookup failed (non-fatal): {e}")
        return {}


def get_decypharr_failure_context(instance_data: Dict[str, Any], names: List[str], hours: int = 24) -> Dict[str, str]:
    """Best-effort: for the given Sonarr release titles, find the most recent matching ERROR
    line in Decypharr's own /logs (debrid-level errors Sonarr never sees, e.g. a magnet that
    failed to resolve or a provider API error). Returns {name: error_message} for matches found
    within the last `hours`. Returns {} on any failure (Decypharr unreachable, unsupported
    client, no matches) - this must never break activity logging."""
    names = [n for n in (names or []) if n]
    if not names:
        return {}

    client_config = instance_data.get("seed_check_torrent_client") or {}
    if (client_config.get("type") or "").lower() != "qbittorrent":
        return {}

    error_logs = _fetch_decypharr_error_logs(client_config, hours=hours)
    if not error_logs:
        return {}

    result = {}
    for name in names:
        match = _find_decypharr_error(error_logs, name)
        if match:
            result[name] = match["error"]
    return result


def _fetch_decypharr_error_logs(client_config: Dict[str, Any], hours: int = 24) -> List[Dict[str, Any]]:
    host = client_config.get("host")
    if not host:
        return []

    scheme = "https" if client_config.get("use_ssl") else "http"
    port = int(client_config.get("port", 8080))
    url = f"{scheme}://{host}:{port}/logs"

    try:
        import requests
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logger.debug(f"Decypharr log fetch failed (non-fatal): {e}")
        return []

    cutoff = datetime.now() - timedelta(hours=hours)
    entries = []
    for line in response.text.splitlines():
        match = _DECYPHARR_ERROR_LOG_RE.match(line)
        if match:
            error_text = match.group("error")
            name = match.group("name_q") or match.group("name_bare") or ""
        else:
            match = _DECYPHARR_ZERO_SYMLINK_RE.match(line)
            if not match or match.group("file_count") != "0":
                continue
            error_text = "Decypharr created 0 symlinks (no usable files found in the debrid cache for this release)"
            name = match.group("name_bare") or ""

        try:
            ts = datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if ts < cutoff:
            continue
        entries.append({"timestamp": ts, "name": name, "error": error_text})
    return entries


def _find_decypharr_error(error_logs: List[Dict[str, Any]], target_name: str) -> Optional[Dict[str, Any]]:
    target_norm = _normalize_name(target_name)
    if not target_norm:
        return None

    best = None
    for entry in error_logs:
        entry_norm = _normalize_name(entry["name"])
        if not entry_norm:
            continue
        if entry_norm in target_norm or target_norm in entry_norm:
            if best is None or entry["timestamp"] > best["timestamp"]:
                best = entry
    return best

"""
Best-effort torrent status lookup for the Swaparr Activity view.

The configured "seed_check_torrent_client" (e.g. Decypharr, which emulates qBittorrent's
WebUI API) is queried for a batch of torrent hashes so the Activity view can show what the
actual download client reports for each Sonarr queue item, alongside Sonarr's own status and
Swaparr's strike count. Any failure here must never break the Activity endpoint.
"""

from typing import Dict, List, Any
from src.primary.utils.logger import get_logger

logger = get_logger("swaparr")


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

        torrents = client.get_torrents(hashes="|".join(download_ids))
        statuses = {}
        for torrent in torrents:
            torrent_hash = (torrent.get("hash") or "").lower()
            if not torrent_hash:
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

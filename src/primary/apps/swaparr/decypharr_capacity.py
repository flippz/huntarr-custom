"""Read-only Decypharr capacity via Swaparr's existing qBittorrent-compatible client."""

from __future__ import annotations

import threading
import time
from typing import Any, Dict

_lock = threading.RLock()
_cache: Dict[tuple, dict] = {}

def _cache_key(config: Dict[str, Any]) -> tuple:
    # Credentials are deliberately excluded from status/log/cache keys.
    return (
        str(config.get("host") or ""), int(config.get("port") or 8080),
        bool(config.get("use_ssl", False)), str(config.get("username") or ""),
    )


def _read_capacity(config: Dict[str, Any]) -> dict:
    from src.primary.apps.tor_hunt.qbittorrent_client import QBittorrentClient

    client = QBittorrentClient(
        host=str(config.get("host") or ""),
        port=int(config.get("port") or 8080),
        username=str(config.get("username") or ""),
        password=str(config.get("password") or ""),
        use_ssl=bool(config.get("use_ssl", False)),
    )
    if not client.login():
        raise RuntimeError("Decypharr/qBittorrent authentication unavailable")
    capacity = client.get_job_capacity(config.get("max_active_jobs", 0))
    return {
        "healthy": True,
        "active": capacity["active"],
        "limit": capacity["limit"],
        "free": capacity["free"],
        "reason": (f"Decypharr {capacity['active']}/{capacity['limit']} active "
                   f"({capacity['source']})"),
    }


def get_capacity(config: Dict[str, Any], monotonic=time.monotonic) -> dict:
    """Return bounded-backoff capacity; unavailable telemetry is explicitly fail-open."""
    if not isinstance(config, dict) or (config.get("type") or "").lower() != "qbittorrent":
        return {"enabled": False, "healthy": False, "free": None,
                "reason": "Decypharr capacity disabled or qBittorrent-compatible client unconfigured",
                "fail_open": True}
    if not str(config.get("host") or "").strip():
        return {"enabled": True, "healthy": False, "free": None,
                "reason": "Decypharr capacity configured without a host; using Starr capacity (fail-open)",
                "fail_open": True}
    key = _cache_key(config)
    now = monotonic()
    with _lock:
        entry = _cache.setdefault(key, {"next_poll": 0.0, "backoff": 15.0, "value": None})
        if now < entry["next_poll"] and entry["value"] is not None:
            return dict(entry["value"])
    try:
        result = _read_capacity(config)
        result.update(enabled=True, fail_open=False, poll_interval=5.0)
        with _lock:
            entry.update(value=result, next_poll=now + 5.0, backoff=15.0)
        return dict(result)
    except Exception as exc:
        with _lock:
            backoff = min(300.0, max(15.0, float(entry.get("backoff", 15.0)) * 2.0))
            result = {
                "enabled": True, "healthy": False, "free": None, "fail_open": True,
                "reason": f"Decypharr capacity unavailable; using Starr capacity (fail-open): {exc}",
                "poll_interval": backoff,
            }
            entry.update(value=result, next_poll=now + backoff, backoff=backoff)
        return dict(result)


def reserve_slot(config: Dict[str, Any]) -> None:
    """Reserve one cached client slot immediately after an accepted search command."""
    if not isinstance(config, dict) or not config.get("host"):
        return
    key = _cache_key(config)
    with _lock:
        entry = _cache.get(key)
        value = entry.get("value") if entry else None
        if not value or not value.get("healthy"):
            return
        value = dict(value)
        value["active"] = int(value.get("active", 0)) + 1
        value["free"] = max(0, int(value.get("free", 0)) - 1)
        value["reason"] = f"Decypharr {value['active']}/{value.get('limit')} active (includes recent Huntarr reservation)"
        entry["value"] = value


def reset_cache() -> None:
    with _lock:
        _cache.clear()

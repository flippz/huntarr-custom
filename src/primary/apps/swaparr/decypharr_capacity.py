"""Read-only Decypharr capacity via Swaparr's existing qBittorrent-compatible client."""

from __future__ import annotations

import threading
import time
from typing import Any, Dict

_lock = threading.RLock()
_cache: Dict[tuple, dict] = {}
_RESERVATION_GRACE_SECONDS = 120.0


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


def _with_reservations(entry: dict, raw: dict, now: float) -> dict:
    raw_active = int(raw.get("active", 0))
    reservations = [
        reservation for reservation in entry.get("reservations", [])
        if reservation["expires"] > now and reservation["threshold"] > raw_active
    ]
    entry["reservations"] = reservations
    entry["raw_active"] = raw_active
    result = dict(raw)
    result["active"] = raw_active + len(reservations)
    result["free"] = max(0, int(result.get("limit", 0)) - result["active"])
    if reservations:
        result["reason"] = (
            f"Decypharr {result['active']}/{result.get('limit')} active "
            f"(includes {len(reservations)} recent Huntarr reservation(s))"
        )
    return result


def get_capacity(config: Dict[str, Any], monotonic=time.monotonic, force: bool = False) -> dict:
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
        entry = _cache.setdefault(key, {
            "next_poll": 0.0, "backoff": 15.0, "value": None,
            "raw_active": 0, "reservations": [],
        })
        if not force and now < entry["next_poll"] and entry["value"] is not None:
            return dict(entry["value"])
    try:
        raw = _read_capacity(config)
        raw.update(enabled=True, fail_open=False, poll_interval=5.0)
        with _lock:
            result = _with_reservations(entry, raw, now)
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


def try_reserve_slot(config: Dict[str, Any], monotonic=time.monotonic):
    """Atomically reserve one healthy cached slot; return a token, False if full, or None if unavailable."""
    if not isinstance(config, dict) or not config.get("host"):
        return None
    key = _cache_key(config)
    now = monotonic()
    with _lock:
        entry = _cache.get(key)
        value = entry.get("value") if entry else None
        if not value or not value.get("healthy"):
            return None
        if int(value.get("free", 0)) <= 0:
            return False
        token = object()
        outstanding = len(entry.get("reservations", []))
        entry.setdefault("reservations", []).append({
            "token": token,
            "expires": now + _RESERVATION_GRACE_SECONDS,
            "threshold": int(entry.get("raw_active", 0)) + outstanding + 1,
        })
        value = dict(value)
        value["active"] = int(value.get("active", 0)) + 1
        value["free"] = max(0, int(value.get("free", 0)) - 1)
        value["reason"] = (
            f"Decypharr {value['active']}/{value.get('limit')} active "
            "(includes recent Huntarr reservation)"
        )
        entry["value"] = value
        return token


def release_reservation(config: Dict[str, Any], token) -> None:
    """Rollback one provisional reservation when no POST was accepted."""
    if token is None or token is False:
        return
    key = _cache_key(config)
    with _lock:
        entry = _cache.get(key)
        if not entry:
            return
        before = len(entry.get("reservations", []))
        entry["reservations"] = [r for r in entry.get("reservations", []) if r["token"] is not token]
        if len(entry["reservations"]) == before:
            return
        value = entry.get("value")
        if value and value.get("healthy"):
            value = dict(value)
            value["active"] = max(0, int(value.get("active", 0)) - 1)
            value["free"] = min(int(value.get("limit", 0)), int(value.get("free", 0)) + 1)
            entry["value"] = value


def reset_cache() -> None:
    with _lock:
        _cache.clear()

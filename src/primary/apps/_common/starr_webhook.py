"""Framework-independent validation and lifecycle handling for Starr webhooks."""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, List, Optional, Tuple

MAX_BODY_BYTES = 256 * 1024
_EVENT_STATES = {
    "grab": ("grabbed", None),
    "download": ("completed", 300),
    "downloadcomplete": ("completed", 300),
    "import": ("completed", 300),
    "importcomplete": ("completed", 300),
    "downloadfailed": ("failed", 300),
    "importfailed": ("failed", 300),
    "manualinteractionrequired": ("failed", 300),
}


class WebhookError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def extract_secret(headers, basic_password: str = "") -> str:
    """Extract a secret without placing it in a URL or loggable query string."""
    direct = headers.get("X-Huntarr-Webhook-Secret", "") if headers else ""
    if direct:
        return str(direct)
    authorization = str(headers.get("Authorization", "")) if headers else ""
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return str(basic_password or "")


def authenticate(expected: str, supplied: str) -> None:
    expected, supplied = str(expected or ""), str(supplied or "")
    if len(expected) < 24 or not hmac.compare_digest(expected, supplied):
        raise WebhookError(401, "unauthorized")


def _positive_id(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _object_id(payload: dict, name: str) -> Optional[int]:
    value = payload.get(name)
    return _positive_id(value.get("id")) if isinstance(value, dict) else None


def sonarr_keys(payload: dict) -> List[str]:
    keys = []
    series_id = _object_id(payload, "series")
    episodes = payload.get("episodes")
    if not isinstance(episodes, list):
        episode = payload.get("episode")
        episodes = [episode] if isinstance(episode, dict) else []
    seasons = set()
    for episode in episodes[:1000]:
        if not isinstance(episode, dict):
            continue
        episode_id = _positive_id(episode.get("id"))
        if episode_id:
            keys.append(f"episodes:{episode_id}")
        try:
            if series_id and episode.get("seasonNumber") is not None:
                seasons.add(int(episode["seasonNumber"]))
        except (TypeError, ValueError):
            pass
    if series_id:
        keys.append(f"series:{series_id}")
        keys.extend(f"season:{series_id}:{season}" for season in sorted(seasons))
    return list(dict.fromkeys(keys))


def radarr_keys(payload: dict) -> List[str]:
    movie_id = _object_id(payload, "movie") or _positive_id(payload.get("movieId"))
    return [f"movies:{movie_id}"] if movie_id else []


def parse_event(app_type: str, payload: dict) -> Tuple[str, Optional[str], Optional[int], List[str]]:
    if not isinstance(payload, dict):
        raise WebhookError(400, "JSON body must be an object")
    event_type = payload.get("eventType")
    if not isinstance(event_type, str) or not event_type.strip() or len(event_type) > 64:
        raise WebhookError(400, "eventType must be a non-empty string of at most 64 characters")
    normalized = "".join(ch for ch in event_type.lower() if ch.isalnum())
    if normalized == "test":
        return normalized, None, None, []
    mapping = _EVENT_STATES.get(normalized)
    if mapping is None:
        raise WebhookError(422, "unrecognized Starr webhook event")
    keys = sonarr_keys(payload) if app_type == "sonarr" else radarr_keys(payload)
    if not keys:
        raise WebhookError(400, "recognized event does not contain a correlatable media ID")
    return normalized, mapping[0], mapping[1], keys


def handle_event(app_type: str, instance_id: str, expected_secret: str,
                 supplied_secret: str, raw_body: bytes, content_type: str,
                 pipeline, wake) -> dict:
    """Validate, deduplicate, correlate, and wake; raises WebhookError on safe rejection."""
    authenticate(expected_secret, supplied_secret)
    if not isinstance(raw_body, bytes) or len(raw_body) > MAX_BODY_BYTES:
        raise WebhookError(413, "payload too large")
    if not str(content_type or "").lower().split(";", 1)[0].strip() == "application/json":
        raise WebhookError(415, "application/json required")
    def _reject_constant(value):
        raise ValueError(f"non-standard JSON constant: {value}")

    try:
        payload = json.loads(raw_body, parse_constant=_reject_constant)
    except (ValueError, TypeError, UnicodeDecodeError, RecursionError) as exc:
        raise WebhookError(400, f"malformed JSON: {exc}") from None
    event_type, state, cooldown, keys = parse_event(app_type, payload)
    canonical_body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(
        str(app_type).encode() + b"\0" + str(instance_id).encode() + b"\0" + canonical_body
    ).hexdigest()
    metadata = json.dumps({
        "source": "starr_webhook", "event_type": event_type,
        "download_id": str(payload.get("downloadId") or "")[:128],
    }, separators=(",", ":"), sort_keys=True)
    result = pipeline.apply_webhook_event(
        app_type, instance_id, digest, keys, state, metadata=metadata,
        cooldown_seconds=cooldown,
    )
    wake_requested = not result["duplicate"] and state is not None
    if wake_requested:
        wake(app_type, instance_id)
    return {
        "accepted": True,
        "event_type": event_type,
        "duplicate": result["duplicate"],
        "transitioned": result["transitioned"],
        "wake_requested": wake_requested,
    }

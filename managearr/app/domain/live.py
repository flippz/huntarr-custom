"""Domain constants and pure validation for M6 controlled live dispatch.

Nothing in this module performs I/O. It defines the exact confirmation
phrases, bounded numeric ranges, and payload validation shared by the API
challenge/confirm/arm/disarm/emergency-stop endpoints (see
``app/services/live_control_service.py``) and the worker-only dispatch
coordinator (see ``app/services/live_dispatch_coordinator.py``).
"""
from __future__ import annotations

import hashlib
import json

# Exact phrases an operator must type back verbatim. Case-sensitive and
# intentionally unambiguous - no truthy/partial match is ever accepted.
ENABLE_LIVE_MODE_PHRASE = "ENABLE LIVE DISPATCH"
ARM_LIVE_DISPATCH_PHRASE = "ARM LIVE DISPATCH"

# Challenges are short-lived on purpose: long enough for an operator to read
# the policy summary and type the phrase, short enough that a stale/leaked
# challenge token is useless a few minutes later.
CHALLENGE_TTL_SECONDS = 120

# Arm duration bounds. The default is conservative; the maximum keeps even a
# forgotten arm window bounded to a single work day at most.
DEFAULT_ARM_TTL_MINUTES = 15
MIN_ARM_TTL_MINUTES = 1
MAX_ARM_TTL_MINUTES = 60

# Per-cycle live dispatch bounds. The default only ever sends one command per
# cycle; the hard cap keeps even a misconfigured deployment from being able
# to fire more than five Sonarr commands in a single worker iteration.
DEFAULT_MAX_DISPATCHES_PER_CYCLE = 1
MAX_DISPATCHES_PER_CYCLE_HARD_CAP = 5

# Minimum wall-clock spacing between live dispatch commands, enforced by
# comparing against live_control.last_dispatch_at rather than sleeping
# inside the worker loop - see LiveDispatchCoordinator.
DEFAULT_MIN_DELAY_SECONDS_BETWEEN_DISPATCHES = 30
MIN_DELAY_SECONDS_FLOOR = 5
MIN_DELAY_SECONDS_CEILING = 600

CHALLENGE_KINDS = ("enable_mode", "arm")
CHALLENGE_STATES = ("pending", "confirmed", "expired", "consumed")
LEDGER_STATES = ("dispatched", "failed", "ambiguous", "blocked", "skipped")
AUDIT_EVENT_TYPES = (
    "mode_enabled", "mode_disabled", "armed", "disarmed",
    "emergency_stop", "arm_expired", "paused", "resumed",
)

def validate_state_change_request(payload) -> tuple[dict, list[str]]:
    if not isinstance(payload, dict):
        return {}, ["request body must be a JSON object"]
    reason = payload.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        return {}, ["reason is required and must be a non-empty string"]
    if len(reason) > 500:
        return {}, ["reason must be at most 500 characters"]
    if payload.get("confirm") is not True:
        return {}, ["confirm must be true"]
    return {"reason": reason.strip()}, []


def compute_policy_digest(policy: dict, live_settings: dict, scheduler_mode: str) -> str:
    """A stable, order-independent digest binding a challenge to the exact
    automation policy, live-dispatch bounds, and scheduler mode in effect
    when the challenge was issued. Confirmation is rejected if any of these
    drift before the operator confirms - see ``LiveControlService``."""
    canonical = {
        "scheduler_mode": scheduler_mode,
        "policy": {
            k: v for k, v in sorted(policy.items()) if k != "updated_at"
        },
        "live_settings": {k: v for k, v in sorted(live_settings.items())},
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def validate_arm_request(payload) -> tuple[dict, list[str]]:
    """Validate an arm-challenge request body. Returns (clean, errors)."""
    if not isinstance(payload, dict):
        return {}, ["request body must be a JSON object"]
    errors = []
    reason = payload.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        errors.append("reason is required and must be a non-empty string")
    elif len(reason) > 500:
        errors.append("reason must be at most 500 characters")
    ttl = payload.get("ttl_minutes", DEFAULT_ARM_TTL_MINUTES)
    if not isinstance(ttl, int) or isinstance(ttl, bool):
        errors.append("ttl_minutes must be an integer")
    elif not (MIN_ARM_TTL_MINUTES <= ttl <= MAX_ARM_TTL_MINUTES):
        errors.append(f"ttl_minutes must be between {MIN_ARM_TTL_MINUTES} and {MAX_ARM_TTL_MINUTES}")
    if errors:
        return {}, errors
    return {"reason": reason.strip(), "ttl_minutes": ttl}, []


def validate_confirm_request(payload) -> tuple[dict, list[str]]:
    """Shared shape for both mode-confirm and arm-confirm bodies."""
    if not isinstance(payload, dict):
        return {}, ["request body must be a JSON object"]
    errors = []
    challenge_id = payload.get("challenge_id")
    if not isinstance(challenge_id, int) or isinstance(challenge_id, bool):
        errors.append("challenge_id must be an integer")
    token = payload.get("token")
    if not isinstance(token, str) or not token:
        errors.append("token is required")
    phrase = payload.get("phrase")
    if not isinstance(phrase, str) or not phrase:
        errors.append("phrase is required")
    if errors:
        return {}, errors
    return {"challenge_id": challenge_id, "token": token, "phrase": phrase}, []


def validate_emergency_stop_request(payload) -> tuple[dict, list[str]]:
    if not isinstance(payload, dict):
        payload = {}
    reason = payload.get("reason") or "operator-triggered emergency stop"
    if not isinstance(reason, str):
        return {}, ["reason must be a string"]
    return {"reason": reason.strip()[:500] or "operator-triggered emergency stop"}, []


def validate_disarm_request(payload) -> tuple[dict, list[str]]:
    if not isinstance(payload, dict):
        payload = {}
    reason = payload.get("reason") or "operator disarm"
    if not isinstance(reason, str):
        return {}, ["reason must be a string"]
    return {"reason": reason.strip()[:500] or "operator disarm"}, []

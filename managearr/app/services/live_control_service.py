"""API-facing orchestration for M6 controlled live dispatch.

This service only ever changes scheduler mode and live_control state
through the two-step challenge/confirm flow (plus dedicated disarm/
emergency-stop actions). It never imports ``DispatchService`` or any
Sonarr adapter - it cannot itself dispatch anything. Only
``LiveDispatchCoordinator`` (worker-only) reads ``live_control`` to decide
whether to attempt a Sonarr command - see that module.
"""
from __future__ import annotations

import secrets

from ..domain.live import (
    ARM_LIVE_DISPATCH_PHRASE,
    CHALLENGE_TTL_SECONDS,
    ENABLE_LIVE_MODE_PHRASE,
    compute_policy_digest,
    hash_token,
    validate_arm_request,
    validate_confirm_request,
    validate_disarm_request,
    validate_emergency_stop_request,
)
from ..persistence.live_repository import LiveRepository
from ..persistence.policy_repository import PolicyRepository
from ..persistence.scheduler_repository import SchedulerRepository


class LiveControlService:
    def __init__(self, live_repo: LiveRepository, scheduler_repo: SchedulerRepository, policy_repo: PolicyRepository):
        self.live_repo = live_repo
        self.scheduler_repo = scheduler_repo
        self.policy_repo = policy_repo

    def _policy_digest(self, scheduler_mode: str) -> str:
        policy = self.policy_repo.get().to_dict()
        control = self.live_repo.get_control()
        live_settings = {
            k: control[k] for k in (
                "max_dispatches_per_cycle", "min_delay_seconds_between_dispatches",
                "default_arm_ttl_minutes", "max_arm_ttl_minutes",
            )
        }
        return compute_policy_digest(policy, live_settings, scheduler_mode)

    # --- status ----------------------------------------------------------

    def status(self) -> dict:
        settings = self.scheduler_repo.get_settings()
        self.live_repo.expire_stale_arm()
        control = self.live_repo.get_control()
        blocked_reasons = []
        if settings.mode != "live":
            blocked_reasons.append(f"scheduler mode is '{settings.mode}', not live")
        if control["emergency_stopped_at"] is not None:
            blocked_reasons.append("emergency stop is active")
        if not control["armed"]:
            blocked_reasons.append("live dispatch is not armed")
        return {
            "mode": settings.mode,
            "control": control,
            "policy_digest": self._policy_digest(settings.mode),
            "pending_live_cycles": self.live_repo.pending_live_cycle_count(),
            "dispatch_allowed": not blocked_reasons,
            "blocked_reasons": blocked_reasons,
            "recent_audit": self.live_repo.recent_audit(limit=20),
        }

    # --- step 1: enable live mode ----------------------------------------

    def request_mode_challenge(self) -> dict:
        settings = self.scheduler_repo.get_settings()
        token = secrets.token_urlsafe(32)
        challenge = self.live_repo.create_challenge(
            "enable_mode", hash_token(token), self._policy_digest(settings.mode)
        )
        return {
            "challenge_id": challenge["id"], "token": token,
            "expires_at": challenge["expires_at"], "ttl_seconds": CHALLENGE_TTL_SECONDS,
            "required_phrase": ENABLE_LIVE_MODE_PHRASE,
            "policy_digest": challenge["policy_digest"],
        }

    def confirm_mode_challenge(self, payload, *, actor: str) -> tuple[dict | None, list[str]]:
        clean, errors = validate_confirm_request(payload)
        if errors:
            return None, errors
        if clean["phrase"] != ENABLE_LIVE_MODE_PHRASE:
            return None, [f"phrase must exactly match: {ENABLE_LIVE_MODE_PHRASE}"]
        challenge, error = self.live_repo.consume_challenge(
            clean["challenge_id"], "enable_mode", hash_token(clean["token"])
        )
        if error:
            return None, [error]
        current_digest = self._policy_digest(self.scheduler_repo.get_settings().mode)
        if challenge["policy_digest"] != current_digest:
            return None, ["policy or scheduler settings changed since the challenge was issued; request a new challenge"]
        result = self.live_repo.enable_live_mode(actor=actor, reason="live mode enabled via confirmed challenge")
        return {"mode": result["mode"], "armed": False}, []

    # --- step 2: arm ---------------------------------------------------

    def request_arm_challenge(self, payload) -> tuple[dict | None, list[str]]:
        clean, errors = validate_arm_request(payload)
        if errors:
            return None, errors
        settings = self.scheduler_repo.get_settings()
        if settings.mode != "live":
            return None, ["scheduler mode must be live before requesting an arm challenge"]
        token = secrets.token_urlsafe(32)
        challenge = self.live_repo.create_challenge(
            "arm", hash_token(token), self._policy_digest(settings.mode),
            requested_reason=clean["reason"], requested_ttl_minutes=clean["ttl_minutes"],
        )
        return {
            "challenge_id": challenge["id"], "token": token,
            "expires_at": challenge["expires_at"], "ttl_seconds": CHALLENGE_TTL_SECONDS,
            "required_phrase": ARM_LIVE_DISPATCH_PHRASE,
            "reason": clean["reason"], "requested_ttl_minutes": clean["ttl_minutes"],
            "policy_digest": challenge["policy_digest"],
        }, []

    def confirm_arm_challenge(self, payload, *, actor: str) -> tuple[dict | None, list[str]]:
        clean, errors = validate_confirm_request(payload)
        if errors:
            return None, errors
        if clean["phrase"] != ARM_LIVE_DISPATCH_PHRASE:
            return None, [f"phrase must exactly match: {ARM_LIVE_DISPATCH_PHRASE}"]
        challenge, error = self.live_repo.consume_challenge(
            clean["challenge_id"], "arm", hash_token(clean["token"])
        )
        if error:
            return None, [error]
        settings = self.scheduler_repo.get_settings()
        if settings.mode != "live":
            return None, ["scheduler mode is no longer live; arming was cancelled"]
        current_digest = self._policy_digest(settings.mode)
        if challenge["policy_digest"] != current_digest:
            return None, ["policy or scheduler settings changed since the challenge was issued; request a new challenge"]
        control, error = self.live_repo.arm(
            actor=actor, reason=challenge["requested_reason"], ttl_minutes=challenge["requested_ttl_minutes"]
        )
        if error:
            return None, [error]
        return control, []

    # --- disarm / emergency stop ------------------------------------------

    def disarm(self, payload, *, actor: str) -> dict:
        clean, _errors = validate_disarm_request(payload)
        return self.live_repo.disarm(actor=actor, reason=clean["reason"])

    def emergency_stop(self, payload, *, actor: str) -> dict:
        clean, _errors = validate_emergency_stop_request(payload)
        return self.live_repo.emergency_stop(actor=actor, reason=clean["reason"])

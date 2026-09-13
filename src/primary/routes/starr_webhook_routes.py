"""Optional authenticated Sonarr/Radarr webhook HTTP endpoint."""

from __future__ import annotations

import hmac
from typing import Optional

from flask import Blueprint, jsonify, request

from src.primary.apps._common.pipeline_state import get_pipeline_state
from src.primary.apps._common.starr_webhook import (
    MAX_BODY_BYTES, WebhookError, authenticate, extract_secret, handle_event,
)
from src.primary.apps._common.wake_registry import request_wake
from src.primary.settings_manager import load_settings
from src.primary.utils.logger import get_logger

starr_webhook_bp = Blueprint("starr_webhooks", __name__, url_prefix="/api/webhooks/starr")
logger = get_logger("starr_webhooks")


def _find_instance(app_type: str, instance_id: str) -> Optional[dict]:
    settings = load_settings(app_type)
    candidates = settings.get("instances", []) if isinstance(settings, dict) else []
    if not candidates and isinstance(settings, dict):
        candidates = [settings]
    for instance in candidates:
        if not isinstance(instance, dict):
            continue
        stable_id = str(instance.get("instance_id") or instance.get("name") or "Default")
        if hmac.compare_digest(stable_id, str(instance_id)):
            return instance
    return None


def _supplied_secret() -> str:
    auth = request.authorization
    return extract_secret(request.headers, auth.password if auth and auth.password else "")


@starr_webhook_bp.route("/<app_type>/<instance_id>", methods=["POST"])
def receive_starr_webhook(app_type: str, instance_id: str):
    app_type = str(app_type).lower()
    if app_type not in {"sonarr", "radarr"} or not instance_id or len(instance_id) > 128:
        return jsonify({"error": "not found"}), 404
    instance = _find_instance(app_type, instance_id)
    if not instance or instance.get("webhook_enabled") is not True:
        return jsonify({"error": "not found"}), 404
    supplied_secret = _supplied_secret()
    try:
        authenticate(str(instance.get("webhook_secret") or ""), supplied_secret)
    except WebhookError as exc:
        return jsonify({"error": exc.message}), exc.status
    if request.content_length is not None and request.content_length > MAX_BODY_BYTES:
        return jsonify({"error": "payload too large"}), 413
    raw = request.stream.read(MAX_BODY_BYTES + 1)
    try:
        result = handle_event(
            app_type, instance_id, str(instance.get("webhook_secret") or ""),
            supplied_secret, raw, request.content_type or "",
            get_pipeline_state(), request_wake,
        )
    except WebhookError as exc:
        return jsonify({"error": exc.message}), exc.status
    if not result["duplicate"]:
        logger.info("Accepted %s webhook for %s/%s; correlated %d lifecycle row(s)",
                    result["event_type"], app_type, instance_id, result["transitioned"])
    result.pop("event_type", None)
    return jsonify(result), 200

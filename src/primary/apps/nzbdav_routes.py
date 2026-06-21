#!/usr/bin/env python3
"""
NZBDav integration - a 3rd party Usenet download client (SABnzbd-compatible API).

NZBDav sits alongside Decypharr in this setup: Decypharr handles torrents/debrid,
NZBDav handles Usenet. Sonarr/Radarr talk to it as a generic "Sabnzbd" download
client, which means Sonarr's own API always masks its API key on read - so this
integration needs its own copy of the key, entered once via Settings, to query
NZBDav's real history/failure details for the Swaparr Activity view.
"""

from flask import Blueprint, request, jsonify
import requests
import socket
from urllib.parse import urlparse
import traceback
from typing import Optional, List, Dict, Any

from src.primary.utils.logger import get_logger
from src.primary.settings_manager import load_settings, save_settings, get_ssl_verify_setting

nzbdav_bp = Blueprint('nzbdav', __name__)
nzbdav_logger = get_logger("nzbdav")


def _base_url(host: str, port: int, use_ssl: bool = False) -> str:
    scheme = "https" if use_ssl else "http"
    return f"{scheme}://{host}:{port}"


def test_connection(host: str, port: int, api_key: str, use_ssl: bool = False, timeout: int = 10) -> Dict[str, Any]:
    """Test connection to NZBDav's SABnzbd-compatible API."""
    if not host or not api_key:
        return {"success": False, "message": "Host and API Key are required"}

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, int(port)))
        sock.close()
        if result != 0:
            return {"success": False, "message": f"Connection refused - unable to connect to {host}:{port}"}
    except socket.gaierror:
        return {"success": False, "message": f"DNS resolution failed - cannot resolve host: {host}"}
    except Exception as e:
        nzbdav_logger.debug(f"Socket pre-check failed, continuing with full request: {e}")

    try:
        url = f"{_base_url(host, port, use_ssl)}/api"
        verify_ssl = get_ssl_verify_setting()
        response = requests.get(url, params={"mode": "version", "apikey": api_key, "output": "json"},
                                 timeout=timeout, verify=verify_ssl)
        response.raise_for_status()
        data = response.json()
        if data.get("status") is False:
            return {"success": False, "message": data.get("error") or "Invalid API key"}
        return {"success": True, "message": "Successfully connected to NZBDav", "version": data.get("version")}
    except requests.exceptions.Timeout:
        return {"success": False, "message": f"Connection timed out after {timeout} seconds"}
    except requests.exceptions.ConnectionError as e:
        return {"success": False, "message": f"Connection error: {str(e)}"}
    except ValueError:
        return {"success": False, "message": "Invalid JSON response - this doesn't appear to be a valid NZBDav server"}
    except Exception as e:
        nzbdav_logger.error(f"Unexpected error testing NZBDav connection: {e}\n{traceback.format_exc()}")
        return {"success": False, "message": f"Unexpected error: {str(e)}"}


def _normalize_name(name: str) -> str:
    import re
    return re.sub(r'[^a-z0-9]+', '', (name or "").lower())


def get_nzbdav_failure_context(names: List[str], limit: int = 200) -> Dict[str, str]:
    """Best-effort: for the given Sonarr release titles, find the matching NZBDav history
    entry and return its real fail_message (e.g. a missing Usenet article), keyed by name.
    Returns {} on any failure (not configured, unreachable, no matches) - must never break
    activity logging."""
    names = [n for n in (names or []) if n]
    if not names:
        return {}

    settings = load_settings("nzbdav") or {}
    if not settings.get("enabled") or not settings.get("host") or not settings.get("api_key"):
        return {}

    try:
        url = f"{_base_url(settings['host'], settings.get('port', 3123), settings.get('use_ssl', False))}/api"
        verify_ssl = get_ssl_verify_setting()
        response = requests.get(url, params={
            "mode": "history", "apikey": settings["api_key"], "output": "json", "limit": limit
        }, timeout=10, verify=verify_ssl)
        response.raise_for_status()
        slots = (response.json() or {}).get("history", {}).get("slots", [])
    except Exception as e:
        nzbdav_logger.debug(f"NZBDav history lookup failed (non-fatal): {e}")
        return {}

    by_norm_name = {}
    for slot in slots:
        fail_message = (slot.get("fail_message") or "").strip()
        if not fail_message:
            continue
        slot_name = slot.get("name") or slot.get("nzb_name") or ""
        norm = _normalize_name(slot_name)
        if norm and norm not in by_norm_name:
            by_norm_name[norm] = fail_message

    result = {}
    for name in names:
        target_norm = _normalize_name(name)
        if not target_norm:
            continue
        for norm, fail_message in by_norm_name.items():
            if norm in target_norm or target_norm in norm:
                result[name] = fail_message
                break
    return result


@nzbdav_bp.route('/status', methods=['GET'])
def get_status():
    """Get the status of the configured NZBDav connection."""
    try:
        settings = load_settings("nzbdav") or {}
        host = (settings.get("host") or "").strip()
        api_key = (settings.get("api_key") or "").strip()
        enabled = settings.get("enabled", False)

        if not host or not api_key:
            return jsonify({"configured": False, "connected": False})

        connected = False
        version = None
        if enabled:
            result = test_connection(host, settings.get("port", 3123), api_key,
                                      settings.get("use_ssl", False), timeout=5)
            connected = result["success"]
            version = result.get("version")

        return jsonify({"configured": True, "connected": connected, "enabled": enabled, "version": version})
    except Exception as e:
        nzbdav_logger.error(f"Error getting NZBDav status: {e}")
        return jsonify({"configured": False, "connected": False, "error": str(e)})


@nzbdav_bp.route('/test-connection', methods=['POST'])
def test_connection_endpoint():
    """Test connection to an NZBDav instance with explicit credentials."""
    data = request.json or {}
    host = data.get('host')
    port = data.get('port', 3123)
    api_key = data.get('api_key')
    use_ssl = data.get('use_ssl', False)

    if not host or not api_key:
        return jsonify({"success": False, "message": "Host and API Key are required"}), 400

    result = test_connection(host, port, api_key, use_ssl)
    return jsonify(result), (200 if result["success"] else 502)

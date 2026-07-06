"""
Route definitions for Magnetarr API endpoints.
Provides CRUD for scrape sources, a manual scan trigger, a recent-discoveries
list, and a Torznab-compatible indexer endpoint for external tools (e.g. Prowlarr).
"""

import secrets
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

from flask import Blueprint, request, jsonify, Response

from src.primary.utils.logger import get_logger
from src.primary.settings_manager import load_settings, save_settings
from src.primary.utils.database import get_database
from src.primary.apps.magnetarr.handler import (
    scan_source,
    get_session_stats,
)

magnetarr_bp = Blueprint('magnetarr', __name__)
magnetarr_logger = get_logger("magnetarr")

TORZNAB_NS = "http://torznab.com/schemas/2015/feed"
CATEGORIES = [
    {"id": "2000", "name": "Movies"},
    {"id": "5000", "name": "TV"},
    {"id": "8000", "name": "Other"},
]


# ── Status ────────────────────────────────────────────────────────────

@magnetarr_bp.route('/status', methods=['GET'])
def get_status():
    """Get Magnetarr status and stats."""
    settings = load_settings("magnetarr")
    enabled = settings.get("enabled", False)
    db = get_database()

    sources = db.get_magnetarr_sources()
    total_magnets = db.get_magnetarr_total_magnets()

    return jsonify({
        "enabled": enabled,
        "configured": bool(sources),
        "sources_count": len(sources),
        "total_magnets": total_magnets,
        "session_stats": get_session_stats(),
    })


# ── Sources CRUD ──────────────────────────────────────────────────────

@magnetarr_bp.route('/sources', methods=['GET'])
def list_sources():
    db = get_database()
    return jsonify({"sources": db.get_magnetarr_sources()})


@magnetarr_bp.route('/sources', methods=['POST'])
def add_source():
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    url = (data.get('url') or '').strip()
    if not url:
        return jsonify({"success": False, "error": "URL is required"}), 400
    if not name:
        name = url.rstrip('/').rsplit('/', 1)[-1] or 'Unnamed'

    db = get_database()
    src_id = db.add_magnetarr_source({
        "name": name,
        "url": url,
        "source_type": data.get("source_type", "reddit"),
        "enabled": data.get("enabled", True),
        "interval_minutes": data.get("interval_minutes", 20),
    })
    return jsonify({"success": True, "id": src_id})


@magnetarr_bp.route('/sources/<src_id>', methods=['PUT'])
def update_source(src_id):
    data = request.get_json() or {}
    db = get_database()
    if not db.get_magnetarr_source(src_id):
        return jsonify({"success": False, "error": "Source not found"}), 404

    updates = {}
    for field in ['name', 'url', 'source_type', 'enabled', 'interval_minutes']:
        if field in data:
            updates[field] = data[field]

    db.update_magnetarr_source(src_id, updates)
    return jsonify({"success": True})


@magnetarr_bp.route('/sources/<src_id>', methods=['DELETE'])
def delete_source(src_id):
    db = get_database()
    if not db.get_magnetarr_source(src_id):
        return jsonify({"success": False, "error": "Source not found"}), 404
    db.delete_magnetarr_source(src_id)
    return jsonify({"success": True})


@magnetarr_bp.route('/sources/<src_id>/scan', methods=['POST'])
def scan_now(src_id):
    db = get_database()
    source = db.get_magnetarr_source(src_id)
    if not source:
        return jsonify({"success": False, "error": "Source not found"}), 404
    try:
        result = scan_source(source)
        return jsonify({"success": not result.get("error"), **result})
    except Exception as e:
        magnetarr_logger.error(f"Manual scan failed for {src_id}: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500


# ── Discoveries ───────────────────────────────────────────────────────

@magnetarr_bp.route('/magnets', methods=['GET'])
def list_magnets():
    db = get_database()
    result = db.get_recent_magnets(
        source_id=request.args.get('source_id'),
        category=request.args.get('category'),
        q=request.args.get('q'),
        page=int(request.args.get('page', 1)),
        page_size=min(200, int(request.args.get('page_size', 50))),
    )
    return jsonify(result)


# ── Torznab endpoint (external — API key query param auth) ───────────

def _get_torznab_api_key():
    settings = load_settings("magnetarr")
    key = settings.get("torznab_api_key", "")
    if not key:
        key = secrets.token_hex(16)
        settings["torznab_api_key"] = key
        save_settings("magnetarr", settings)
    return key


@magnetarr_bp.route('/torznab-key', methods=['GET'])
def torznab_key():
    """Ensure the Torznab API key exists and return it, so the settings page can
    display it immediately instead of waiting for an external tool to query the
    Torznab endpoint first."""
    return jsonify({"api_key": _get_torznab_api_key()})


def _torznab_error(message, code=900):
    root = ET.Element("error", {"code": str(code), "description": message})
    xml_bytes = b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding='utf-8')
    return Response(xml_bytes, mimetype='application/rss+xml', status=401)


def _build_caps_xml():
    caps = ET.Element("caps")
    server = ET.SubElement(caps, "server", {"title": "Huntarr Magnetarr"})
    searching = ET.SubElement(caps, "searching")
    ET.SubElement(searching, "search", {"available": "yes", "supportedParams": "q"})
    ET.SubElement(searching, "tv-search", {"available": "yes", "supportedParams": "q"})
    ET.SubElement(searching, "movie-search", {"available": "yes", "supportedParams": "q"})
    categories_el = ET.SubElement(caps, "categories")
    for cat in CATEGORIES:
        ET.SubElement(categories_el, "category", {"id": cat["id"], "name": cat["name"]})
    xml_bytes = b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(caps, encoding='utf-8')
    return Response(xml_bytes, mimetype='application/rss+xml')


def _build_search_xml(magnets):
    ET.register_namespace('torznab', TORZNAB_NS)
    rss = ET.Element("rss", {"version": "2.0"})
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "Huntarr Magnetarr"

    for m in magnets:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = m.get("title", "Untitled")
        ET.SubElement(item, "guid").text = m["info_hash"]
        ET.SubElement(item, "link").text = m["magnet_uri"]
        ET.SubElement(item, "pubDate").text = str(m.get("discovered_at", ""))
        ET.SubElement(item, "enclosure", {
            "url": m["magnet_uri"],
            "length": str(m.get("size_bytes", 0) or 0),
            "type": "application/x-bittorrent;x-scheme-handler/magnet",
        })
        ET.SubElement(item, f"{{{TORZNAB_NS}}}attr", {"name": "size", "value": str(m.get("size_bytes", 0) or 0)})
        ET.SubElement(item, f"{{{TORZNAB_NS}}}attr", {"name": "infohash", "value": m["info_hash"]})
        ET.SubElement(item, f"{{{TORZNAB_NS}}}attr", {"name": "category", "value": m.get("category", "other")})

    xml_bytes = b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(rss, encoding='utf-8')
    return Response(xml_bytes, mimetype='application/rss+xml')


@magnetarr_bp.route('/torznab', methods=['GET'])
@magnetarr_bp.route('/torznab/api', methods=['GET'])
def torznab_endpoint():
    expected_key = _get_torznab_api_key()
    supplied_key = request.args.get('apikey', '')
    if not supplied_key or not secrets.compare_digest(supplied_key, expected_key):
        return _torznab_error("Invalid API key", code=100)

    t = request.args.get('t', 'caps')
    if t == 'caps':
        return _build_caps_xml()

    if t in ('search', 'tvsearch', 'movie'):
        db = get_database()
        q = request.args.get('q', '')
        magnets = db.search_magnets(q=q, limit=100)
        return _build_search_xml(magnets)

    return _torznab_error(f"Unsupported function: {t}", code=201)

"""
Route definitions for Swaparr API endpoints.
Enhanced with better statistics tracking and status reporting.
"""

from flask import Blueprint, request, jsonify
import os
import json
from src.primary.utils.logger import get_logger
from src.primary.settings_manager import load_settings, save_settings
from src.primary.apps.swaparr.handler import (
    process_stalled_downloads, 
    get_session_stats, 
    reset_session_stats
)
from src.primary.apps.swaparr import get_configured_instances, is_configured
from src.primary.apps.swaparr.stats_manager import get_swaparr_stats, reset_swaparr_stats
from src.primary.utils.database import get_database

# Create the blueprint directly in this file
swaparr_bp = Blueprint('swaparr', __name__)
swaparr_logger = get_logger("swaparr")

@swaparr_bp.route('/status', methods=['GET'])
def get_status():
    """Get Swaparr status and comprehensive statistics"""
    settings = load_settings("swaparr")
    enabled = settings.get("enabled", False)
    configured = is_configured()
    
    # Get strike statistics from database for all configured apps
    app_statistics = {}
    
    # Only read statistics if Swaparr is enabled to avoid unnecessary database errors
    if enabled and configured:
        try:
            db = get_database()
            
            # Get all configured instances to check for state data
            instances = get_configured_instances(quiet=True)
            for app_name, app_instances in instances.items():
                # Only process apps that have Swaparr enabled for at least one instance
                swaparr_enabled_for_app = any(instance.get("swaparr_enabled", False) for instance in app_instances)
                
                if not swaparr_enabled_for_app:
                    continue  # Skip apps that don't have Swaparr enabled
                
                app_stats = {"error": None}
                
                try:
                    # Load strike data from database
                    strike_data = db.get_swaparr_strike_data(app_name)
                    
                    total_items = len(strike_data)
                    removed_items = sum(1 for item in strike_data.values() if item.get("removed", False))
                    striked_items = sum(1 for item in strike_data.values() 
                                      if item.get("strikes", 0) > 0 and not item.get("removed", False))
                    
                    app_stats.update({
                        "total_tracked": total_items,
                        "currently_striked": striked_items,
                        "removed_via_strikes": removed_items
                    })
                    
                    # Load removed items data from database
                    removed_data = db.get_swaparr_removed_items(app_name)
                    
                    app_stats["total_removed"] = len(removed_data)
                    
                    # Get removal reasons breakdown
                    reasons = {}
                    for item in removed_data.values():
                        reason = item.get("reason", "Unknown")
                        reasons[reason] = reasons.get(reason, 0) + 1
                    app_stats["removal_reasons"] = reasons
                        
                except Exception as e:
                    swaparr_logger.error(f"Error reading statistics for {app_name}: {str(e)}")
                    app_stats["error"] = str(e)
                
                app_statistics[app_name] = app_stats
                
        except Exception as e:
            swaparr_logger.error(f"Error accessing database for statistics: {str(e)}")
    
    # Get session statistics
    session_stats = get_session_stats()
    
    # Get persistent statistics
    swaparr_persistent_stats = get_swaparr_stats()
    
    # Get configured instances info
    instances_info = {}
    if configured:
        instances = get_configured_instances(quiet=True)
        for app_name, app_instances in instances.items():
            instances_info[app_name] = [
                {
                    "instance_id": instance.get("instance_id"),
                    "instance_name": instance.get("instance_name", "Unknown"),
                    "api_url": instance.get("api_url", "Not configured"),
                    "enabled": instance.get("enabled", False),
                    "swaparr_enabled": instance.get("swaparr_enabled", False)
                }
                for instance in app_instances
            ]
    
    return jsonify({
        "enabled": enabled,
        "configured": configured,
        "total_instances": sum(len(instances) for instances in instances_info.values()),
        "settings": {
            "max_strikes": settings.get("max_strikes", 3),
            "max_download_time": settings.get("max_download_time", "2h"),
            "ignore_above_size": settings.get("ignore_above_size", "25GB"),
            "remove_from_client": settings.get("remove_from_client", True),
            "dry_run": settings.get("dry_run", False)
        },
        "app_statistics": app_statistics,
        "session_statistics": session_stats,
        "persistent_statistics": swaparr_persistent_stats,
        "configured_instances": instances_info
    })

@swaparr_bp.route('/settings', methods=['GET'])
def get_settings():
    """Get Swaparr settings"""
    settings = load_settings("swaparr")
    return jsonify(settings)

@swaparr_bp.route('/settings', methods=['POST'])
def update_settings():
    """Update Swaparr settings"""
    data = request.json
    
    if not data:
        return jsonify({"success": False, "message": "No data provided"}), 400
    
    # Load current settings
    settings = load_settings("swaparr")
    
    # Update settings with provided data
    for key, value in data.items():
        settings[key] = value
    
    # Save updated settings
    success = save_settings("swaparr", settings)
    
    if success:
        swaparr_logger.info(f"Updated Swaparr settings: {list(data.keys())}")
        return jsonify({"success": True, "message": "Settings updated successfully"})
    else:
        swaparr_logger.error("Failed to save Swaparr settings")
        return jsonify({"success": False, "message": "Failed to save settings"}), 500

@swaparr_bp.route('/reset', methods=['POST'])
def reset_strikes():
    """Reset strikes and optionally removed items for all apps or a specific app"""
    data = request.json or {}
    app_name = data.get('app_name')
    reset_removed = data.get('reset_removed', False)  # Option to also reset removed items
    
    try:
        db = get_database()
        
        if app_name:
            # Reset strikes for a specific app
            files_reset = []
            
            # Reset strikes
            db.set_swaparr_strike_data(app_name, {})
            files_reset.append("strikes")
            
            # Optionally reset removed items
            if reset_removed:
                db.set_swaparr_removed_items(app_name, {})
                files_reset.append("removed_items")
            
            swaparr_logger.info(f"Reset {', '.join(files_reset)} for {app_name}")
            return jsonify({
                "success": True, 
                "message": f"Reset {', '.join(files_reset)} for {app_name}",
                "files_reset": files_reset
            })
        else:
            # Reset strikes for all configured apps
            configured = is_configured()
            if not configured:
                return jsonify({"success": True, "message": "No configured apps to reset"})
            
            instances = get_configured_instances(quiet=True)
            apps_reset = []
            
            for app_name in instances.keys():
                files_reset = []
                
                # Reset strikes
                db.set_swaparr_strike_data(app_name, {})
                files_reset.append("strikes")
                
                # Optionally reset removed items
                if reset_removed:
                    db.set_swaparr_removed_items(app_name, {})
                    files_reset.append("removed_items")
                
                apps_reset.append(f"{app_name} ({', '.join(files_reset)})")
            
            swaparr_logger.info(f"Reset data for apps: {apps_reset}")
            return jsonify({
                "success": True, 
                "message": f"Reset data for {len(apps_reset)} apps",
                "apps_reset": apps_reset
            })
    except Exception as e:
        swaparr_logger.error(f"Error during reset operation: {str(e)}")
        return jsonify({"success": False, "message": f"Error during reset: {str(e)}"}), 500

@swaparr_bp.route('/reset-session', methods=['POST'])
def reset_session_statistics():
    """Reset session statistics"""
    try:
        reset_session_stats()
        swaparr_logger.info("Reset Swaparr session statistics")
        return jsonify({"success": True, "message": "Session statistics reset successfully"})
    except Exception as e:
        swaparr_logger.error(f"Error resetting session statistics: {str(e)}")
        return jsonify({"success": False, "message": f"Error resetting session statistics: {str(e)}"}), 500

@swaparr_bp.route('/reset-stats', methods=['POST'])
def reset_persistent_statistics():
    """Reset persistent statistics (the ones shown on homepage)"""
    try:
        success = reset_swaparr_stats()
        if success:
            swaparr_logger.info("Reset Swaparr persistent statistics")
            return jsonify({"success": True, "message": "Persistent statistics reset successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to reset persistent statistics"}), 500
    except Exception as e:
        swaparr_logger.error(f"Error resetting persistent statistics: {str(e)}")
        return jsonify({"success": False, "message": f"Error resetting persistent statistics: {str(e)}"}), 500

@swaparr_bp.route('/reset-cycle', methods=['POST'])
def reset_cycle_endpoint():
    """Reset Swaparr cycle - forces a new cycle to start immediately"""
    try:
        from src.primary.cycle_tracker import reset_cycle
        
        # Reset the cycle timer for Swaparr
        success = reset_cycle('swaparr')
        
        if success:
            swaparr_logger.info("Reset Swaparr cycle timer - forcing new cycle to start")
            return jsonify({"success": True, "message": "Swaparr cycle reset successfully"})
        else:
            swaparr_logger.error("Failed to reset Swaparr cycle")
            return jsonify({"success": False, "message": "Failed to reset Swaparr cycle"}), 500
    except Exception as e:
        swaparr_logger.error(f"Error resetting Swaparr cycle: {str(e)}")
        return jsonify({"success": False, "message": f"Error resetting Swaparr cycle: {str(e)}"}), 500

@swaparr_bp.route('/test', methods=['POST'])
def test_configuration():
    """Test Swaparr configuration with specific instances"""
    data = request.json or {}
    test_app = data.get('app_name')  # Optional: test specific app
    
    settings = load_settings("swaparr")
    if not settings or not settings.get("enabled", False):
        return jsonify({
            "success": False, 
            "message": "Swaparr is not enabled"
        }), 400
    
    try:
        instances = get_configured_instances(quiet=True)
        
        if not instances or not any(len(app_instances) > 0 for app_instances in instances.values()):
            return jsonify({
                "success": False, 
                "message": "No configured Starr app instances found"
            }), 400
        
        test_results = {}
        
        for app_name, app_instances in instances.items():
            if test_app and app_name != test_app:
                continue
                
            test_results[app_name] = []
            
            for app_settings in app_instances:
                instance_name = app_settings.get("instance_name", "Unknown")
                api_url = app_settings.get("api_url")
                api_key = app_settings.get("api_key")
                
                if not api_url or not api_key:
                    test_results[app_name].append({
                        "instance": instance_name,
                        "success": False,
                        "message": "Missing API URL or API Key"
                    })
                    continue
                
                try:
                    # Test API connectivity by getting queue (dry run)
                    from src.primary.apps.swaparr.handler import get_queue_items
                    
                    queue_items = get_queue_items(app_name, api_url, api_key, 30)  # Short timeout for test
                    
                    test_results[app_name].append({
                        "instance": instance_name,
                        "success": True,
                        "message": f"Successfully connected. Found {len(queue_items)} queue items.",
                        "queue_count": len(queue_items)
                    })
                    
                except Exception as e:
                    test_results[app_name].append({
                        "instance": instance_name,
                        "success": False,
                        "message": f"Connection failed: {str(e)}"
                    })
        
        overall_success = any(
            any(result["success"] for result in app_results) 
            for app_results in test_results.values()
        )
        
        return jsonify({
            "success": overall_success,
            "message": "Configuration test completed",
            "test_results": test_results
        })
        
    except Exception as e:
        swaparr_logger.error(f"Error during configuration test: {str(e)}")
        return jsonify({
            "success": False, 
            "message": f"Test failed with error: {str(e)}"
        }), 500

@swaparr_bp.route('/run', methods=['POST'])
def manual_run():
    """Manually trigger a Swaparr run"""
    try:
        from src.primary.apps.swaparr.handler import run_swaparr
        
        settings = load_settings("swaparr")
        if not settings or not settings.get("enabled", False):
            return jsonify({
                "success": False, 
                "message": "Swaparr is not enabled"
            }), 400
        
        # Run Swaparr
        run_swaparr()
        
        # Get updated session stats
        session_stats = get_session_stats()
        
        return jsonify({
            "success": True,
            "message": "Swaparr run completed successfully",
            "session_stats": session_stats
        })
        
    except Exception as e:
        swaparr_logger.error(f"Error during manual Swaparr run: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Manual run failed: {str(e)}"
        }), 500


def _find_instance(app_name, instance_id):
    """Find a configured instance dict for app_name by instance_id."""
    instances = get_configured_instances(quiet=True)
    for instance in instances.get(app_name, []):
        if instance.get("instance_id") == instance_id:
            return instance
    return None


def _summarize_sonarr_status(record):
    """Reduce a raw Sonarr/Radarr queue record to a short status label + detail message."""
    tracked_status = (record.get("trackedDownloadStatus") or "").strip()
    tracked_state = (record.get("trackedDownloadState") or "").strip()
    messages = record.get("statusMessages") or []
    detail = "; ".join(
        msg for m in messages for msg in [m.get("title") or ""] if msg
    ) or record.get("errorMessage") or ""

    if tracked_status.lower() == "error":
        label = "Error"
    elif tracked_status.lower() == "warning":
        label = "Warning"
    elif tracked_state:
        label = tracked_state.replace("Pending", "Pending ").title()
    else:
        label = (record.get("status") or "Unknown").title()

    return {"label": label, "detail": detail}


@swaparr_bp.route('/activity/<app_name>/<instance_id>', methods=['GET'])
def get_activity(app_name, instance_id):
    """Live merged view of a queue: Sonarr's own queue records + Swaparr strikes + torrent client status."""
    try:
        instance = _find_instance(app_name, instance_id)
        if not instance:
            return jsonify({"success": False, "message": "Instance not found"}), 404

        if app_name != "sonarr":
            return jsonify({"success": False, "message": f"Unsupported app_name: {app_name}"}), 400

        try:
            from src.primary.apps.sonarr.api import get_queue
            records = get_queue(instance["api_url"], instance["api_key"], instance.get("api_timeout", 120)) or []
        except Exception as e:
            swaparr_logger.error(f"Error fetching queue for {app_name}/{instance_id}: {str(e)}")
            records = []

        try:
            strike_data = get_database().get_swaparr_strike_data(app_name)
        except Exception as e:
            swaparr_logger.error(f"Error loading strike data for {app_name}: {str(e)}")
            strike_data = {}

        download_ids = [r.get("downloadId") for r in records if r.get("downloadId")]
        try:
            from src.primary.apps.swaparr.torrent_status import get_torrent_statuses
            torrent_statuses = get_torrent_statuses(instance, download_ids)
        except Exception as e:
            swaparr_logger.debug(f"Torrent status lookup unavailable: {e}")
            torrent_statuses = {}

        settings = load_settings("swaparr")
        max_strikes = settings.get("max_strikes", 3)

        # Sonarr returns one queue record per episode, so a single season-pack torrent shows
        # up as several records sharing the same downloadId. Group those into one row.
        groups = {}
        group_order = []
        for record in records:
            item_id = str(record.get("id"))
            strikes = strike_data.get(item_id, {}).get("strikes", 0)
            group_key = record.get("downloadId") or f"__no_download_id_{item_id}"

            if group_key not in groups:
                groups[group_key] = {
                    "id": item_id,
                    "name": record.get("title") or record.get("name") or "Unknown",
                    "size": record.get("size"),
                    "sizeleft": record.get("sizeleft"),
                    "sonarr_status": _summarize_sonarr_status(record),
                    "strikes": strikes,
                    "max_strikes": max_strikes,
                    "torrent_status": torrent_statuses.get((record.get("downloadId") or "").lower()),
                    "episode_count": 1
                }
                group_order.append(group_key)
            else:
                groups[group_key]["episode_count"] += 1
                groups[group_key]["strikes"] = max(groups[group_key]["strikes"], strikes)

        queue = [groups[key] for key in group_order]

        return jsonify({"success": True, "queue": queue, "max_strikes": max_strikes})
    except Exception as e:
        swaparr_logger.error(f"Error building activity view for {app_name}/{instance_id}: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500


@swaparr_bp.route('/activity/<app_name>/<instance_id>/history', methods=['GET'])
def get_activity_history(app_name, instance_id):
    """Paginated history of completed/removed queue items for an instance."""
    try:
        instance = _find_instance(app_name, instance_id)
        if not instance:
            return jsonify({"success": False, "message": "Instance not found"}), 404

        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        history = get_database().get_swaparr_activity_history(
            app_name, instance.get("instance_name"), page, page_size
        )
        return jsonify({"success": True, **history})
    except Exception as e:
        swaparr_logger.error(f"Error fetching activity history for {app_name}/{instance_id}: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500


@swaparr_bp.route('/activity/<app_name>/<instance_id>/incident', methods=['GET'])
def get_activity_incident(app_name, instance_id):
    """Cross-reference every source Huntarr can reach for one specific item - Sonarr's own
    history, Decypharr's logs, NZBDav's history, and Swaparr's own activity log - merged into
    a single chronological timeline for the incident detail view."""
    try:
        instance = _find_instance(app_name, instance_id)
        if not instance:
            return jsonify({"success": False, "message": "Instance not found"}), 404
        if app_name != "sonarr":
            return jsonify({"success": False, "message": f"Unsupported app_name: {app_name}"}), 400

        name = (request.args.get('name') or '').strip()
        download_id = (request.args.get('item_id') or '').strip()
        if not name:
            return jsonify({"success": False, "message": "Missing 'name' parameter"}), 400

        timeline = []
        additional_context = []

        # Swaparr's own record of what it saw/did for this item
        try:
            swaparr_events = get_database().get_swaparr_activity_for_name(
                app_name, instance.get("instance_name"), name
            )
            for event in swaparr_events:
                label = "completed" if event.get("event_type") == "completed" else (event.get("reason") or event.get("event_type"))
                timeline.append({
                    "timestamp": event.get("occurred_at"),
                    "source": "Swaparr",
                    "message": f"{event.get('event_type')}: {label}" if event.get("event_type") != "completed" else "Completed"
                })
        except Exception as e:
            swaparr_logger.debug(f"Swaparr activity lookup failed for incident view: {e}")

        # Sonarr's full history for this release (every event type, not just grabbed/failed)
        try:
            from src.primary.apps.sonarr.api import arr_request
            response = arr_request(
                instance["api_url"], instance["api_key"], instance.get("api_timeout", 120),
                "history?pageSize=250&sortDirection=descending&sortKey=date", count_api=False
            )
            records = (response or {}).get("records", [])
            name_norm = name.lower().replace(' ', '').replace('.', '')

            from datetime import datetime
            import pytz
            from src.primary.utils.timezone_utils import get_user_timezone
            user_tz = get_user_timezone(prefer_database_for_display=True)

            for record in records:
                source_title = record.get("sourceTitle") or ""
                title_norm = source_title.lower().replace(' ', '').replace('.', '')
                if record.get("downloadId") != download_id and title_norm != name_norm:
                    continue
                event_type = record.get("eventType") or "unknown"
                msg = (record.get("data") or {}).get("message")
                message = f"{event_type}: {msg}" if msg else event_type

                raw_date = record.get("date") or ""
                try:
                    utc_dt = datetime.strptime(raw_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
                    local_ts = utc_dt.astimezone(user_tz).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    local_ts = raw_date

                timeline.append({
                    "timestamp": local_ts,
                    "source": "Sonarr",
                    "message": message
                })
        except Exception as e:
            swaparr_logger.debug(f"Sonarr history lookup failed for incident view: {e}")

        # Decypharr's own logs - full lifecycle, not just errors
        try:
            from src.primary.apps.swaparr.torrent_status import get_decypharr_log_lines
            for entry in get_decypharr_log_lines(instance, name):
                timeline.append({
                    "timestamp": entry["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                    "source": "Decypharr",
                    "message": entry["message"]
                })
        except Exception as e:
            swaparr_logger.debug(f"Decypharr log lookup failed for incident view: {e}")

        # NZBDav - no timestamp field available, so this goes in additional context
        try:
            from src.primary.apps.nzbdav_routes import get_nzbdav_history_entry
            nzbdav_entry = get_nzbdav_history_entry(name)
            if nzbdav_entry:
                status = nzbdav_entry.get("status") or "Unknown"
                fail_message = nzbdav_entry.get("fail_message")
                message = f"{status}" + (f": {fail_message}" if fail_message else "")
                additional_context.append({"source": "NZBDav", "message": message})
        except Exception as e:
            swaparr_logger.debug(f"NZBDav lookup failed for incident view: {e}")

        # Live torrent client status, if the download is still present
        if download_id:
            try:
                from src.primary.apps.swaparr.torrent_status import get_torrent_statuses
                statuses = get_torrent_statuses(instance, [download_id])
                torrent = statuses.get(download_id.lower())
                if torrent:
                    additional_context.append({
                        "source": "Torrent Client",
                        "message": f"state: {torrent.get('state')}, progress: {torrent.get('progress')}, seeds: {torrent.get('num_seeds')}"
                    })
            except Exception as e:
                swaparr_logger.debug(f"Torrent client lookup failed for incident view: {e}")

        timeline.sort(key=lambda t: t.get("timestamp") or "")

        return jsonify({"success": True, "timeline": timeline, "additional_context": additional_context})
    except Exception as e:
        swaparr_logger.error(f"Error building incident view for {app_name}/{instance_id}: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500

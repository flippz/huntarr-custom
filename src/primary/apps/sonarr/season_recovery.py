"""Durable, guarded recovery for opt-in Sonarr exact-season replacement.

The old files (including symlinks themselves) are atomically moved on the same
filesystem before Sonarr's episode-file records are removed.  A SQLite journal is
written first, so any interrupted operation is rolled back on the next cycle.
Successful imports keep the preserved originals in ``.huntarr-recovery``; no media
file is ever deleted by Huntarr.
"""

from __future__ import annotations

import datetime
import json
import os
import time
import uuid
from typing import Callable, Dict, List, Optional, Tuple

from src.primary.apps.sonarr import api as sonarr_api
from src.primary.utils.database import get_database
from src.primary.utils.logger import get_logger

logger = get_logger("sonarr")


def utc_now_iso() -> str:
    """Return a Sonarr-comparable UTC timestamp."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _save(entry: Dict) -> None:
    payload = {
        "files": entry.get("files", []),
        "replacement_files": entry.get("replacement_files", []),
        "expected_episode_ids": entry.get("expected_episode_ids", []),
        "series_path": entry.get("series_path"),
        "recovery_root": entry.get("recovery_root"),
        "expected_release_title": entry.get("expected_release_title"),
        "deadline_at": entry.get("deadline_at"),
        "search_started_at": entry.get("search_started_at"),
        "download_id": entry.get("download_id"),
    }
    db = get_database()
    with db.get_connection() as conn:
        conn.execute(
            """INSERT INTO sonarr_season_recovery_journal
               (id, instance_name, series_id, season_number, state, files_json, error, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(id) DO UPDATE SET state=excluded.state,
                   files_json=excluded.files_json, error=excluded.error,
                   updated_at=CURRENT_TIMESTAMP""",
            (entry["id"], str(entry["instance_name"]), int(entry["series_id"]),
             int(entry["season_number"]), entry["state"],
             json.dumps(payload, separators=(",", ":")), entry.get("error")),
        )


def _pending(instance_name: str) -> List[Dict]:
    db = get_database()
    with db.get_connection() as conn:
        rows = conn.execute(
            """SELECT id, instance_name, series_id, season_number, state, files_json, error
               FROM sonarr_season_recovery_journal
               WHERE instance_name=? AND state NOT IN ('completed', 'imported', 'aborted')
               ORDER BY created_at""", (str(instance_name),),
        ).fetchall()
    entries = []
    for row in rows:
        payload = json.loads(row[5] or "{}")
        # Accept the earliest development journal shape (a bare files list).
        if isinstance(payload, list):
            payload = {"files": payload}
        entries.append({
            "id": row[0], "instance_name": row[1], "series_id": row[2],
            "season_number": row[3], "state": row[4], "error": row[6],
            "files": payload.get("files", []),
            "replacement_files": payload.get("replacement_files", []),
            "expected_episode_ids": payload.get("expected_episode_ids", []),
            "series_path": payload.get("series_path"),
            "recovery_root": payload.get("recovery_root"),
            "expected_release_title": payload.get("expected_release_title"),
            "deadline_at": payload.get("deadline_at"),
            "search_started_at": payload.get("search_started_at"),
            "download_id": payload.get("download_id"),
        })
    return entries


def _import_succeeded(api_url: str, api_key: str, api_timeout: int,
                      download_id: Optional[str]) -> bool:
    if not download_id:
        return False
    records = sonarr_api.get_history_for_download(api_url, api_key, api_timeout, download_id)
    return any(str(record.get("eventType", "")).lower() == "downloadfolderimported"
               for record in records)


def _restore_files(entry: Dict) -> Optional[str]:
    """Restore each staged path idempotently, preserving both sides on conflict."""
    conflicts = []
    for item in entry["files"]:
        original = item["path"]
        backup = item["backup_path"]
        if os.path.lexists(backup):
            if os.path.lexists(original):
                conflicts.append(original)
                continue
            os.makedirs(os.path.dirname(original), exist_ok=True)
            os.replace(backup, original)
        elif not os.path.lexists(original):
            conflicts.append(original)
    if conflicts:
        return "restore conflict or missing backup: " + ", ".join(conflicts)
    return None


def _inventory(api_url: str, api_key: str, api_timeout: int,
               series_id: int) -> Tuple[object, object, object]:
    """Read Sonarr's episode, episode-file, and series inventory."""
    episodes = sonarr_api.arr_request(
        api_url, api_key, api_timeout, f"episode?seriesId={int(series_id)}", count_api=False
    )
    episode_files = sonarr_api.arr_request(
        api_url, api_key, api_timeout, f"episodefile?seriesId={int(series_id)}", count_api=False
    )
    series = sonarr_api.arr_request(
        api_url, api_key, api_timeout, f"series/{int(series_id)}", count_api=False
    )
    return episodes, episode_files, series


def _inside_series(path: str, series_path: str) -> bool:
    try:
        return (
            bool(path) and os.path.isabs(path)
            and os.path.commonpath([
                os.path.realpath(os.path.dirname(path)), os.path.realpath(series_path),
            ]) == os.path.realpath(series_path)
        )
    except (OSError, ValueError, TypeError):
        return False


def _validate_replacement(api_url: str, api_key: str, api_timeout: int,
                          entry: Dict) -> Tuple[bool, str]:
    """Prove every exact-season episode maps to a real, non-original file."""
    episodes, episode_files, series = _inventory(
        api_url, api_key, api_timeout, entry["series_id"]
    )
    series_path = series.get("path") if isinstance(series, dict) else None
    if isinstance(series_path, str):
        series_path = os.path.normpath(series_path)
    if (not isinstance(episodes, list) or not isinstance(episode_files, list)
            or not series_path or not os.path.isabs(series_path)
            or (entry.get("series_path") and os.path.realpath(series_path)
                != os.path.realpath(entry["series_path"]))):
        return False, "replacement inventory or series path is unavailable/changed"

    target = [ep for ep in episodes if ep.get("seasonNumber") == entry["season_number"]]
    target_ids = [ep.get("id") for ep in target]
    expected_ids = entry.get("expected_episode_ids") or target_ids
    if (not target_ids or any(not isinstance(value, int) or isinstance(value, bool)
                              for value in target_ids)
            or len(target_ids) != len(set(target_ids))
            or set(target_ids) != set(expected_ids)):
        return False, "exact-season episode inventory no longer matches the armed journal"

    file_by_id = {item.get("id"): item for item in episode_files
                  if isinstance(item, dict) and isinstance(item.get("id"), int)
                  and not isinstance(item.get("id"), bool)}
    original_ids = {item["episode_file_id"] for item in entry.get("files", [])}
    target_file_ids = []
    for episode in target:
        file_id = episode.get("episodeFileId")
        if (not isinstance(file_id, int) or isinstance(file_id, bool) or file_id <= 0
                or file_id in original_ids or file_id not in file_by_id):
            return False, f"episode {episode.get('id')} lacks a verified replacement file"
        target_file_ids.append(file_id)

    foreign_file_ids = {
        ep.get("episodeFileId") for ep in episodes
        if ep.get("seasonNumber") != entry["season_number"] and ep.get("episodeFileId")
    }
    if set(target_file_ids) & foreign_file_ids:
        return False, "replacement file mapping crosses outside the target season"
    for file_id in set(target_file_ids):
        path = file_by_id[file_id].get("path")
        try:
            usable = (_inside_series(path, series_path) and os.path.lexists(path)
                      and os.path.getsize(path) > 0)
        except OSError:
            usable = False
        if not usable:
            return False, f"replacement file {file_id} is missing or outside the series path"
    return True, ""


def _quarantine_replacements(api_url: str, api_key: str, api_timeout: int,
                             entry: Dict) -> Optional[str]:
    """Preserve partial/new target-season files before restoring originals."""
    episodes, episode_files, series = _inventory(
        api_url, api_key, api_timeout, entry["series_id"]
    )
    series_path = series.get("path") if isinstance(series, dict) else entry.get("series_path")
    if (not isinstance(episodes, list) or not isinstance(episode_files, list)
            or not isinstance(series_path, str) or not os.path.isabs(series_path)):
        return "cannot inventory replacement files for rollback"
    series_path = os.path.normpath(series_path)
    file_by_id = {item.get("id"): item for item in episode_files if isinstance(item, dict)}
    original_ids = {item["episode_file_id"] for item in entry.get("files", [])}
    target_ids = {
        ep.get("episodeFileId") for ep in episodes
        if ep.get("seasonNumber") == entry["season_number"] and ep.get("episodeFileId")
    } - original_ids
    foreign_ids = {
        ep.get("episodeFileId") for ep in episodes
        if ep.get("seasonNumber") != entry["season_number"] and ep.get("episodeFileId")
    }
    if target_ids & foreign_ids:
        return "refusing rollback because a replacement file spans another season"

    recovery_root = entry.get("recovery_root") or (entry.get("files") or [{}])[0].get("recovery_root")
    if target_ids and not recovery_root:
        return "recovery root is unavailable for replacement quarantine"

    # Reconcile previously journaled move intents first. The intent is persisted
    # before os.replace, making a crash on either side of the rename recoverable.
    for intent in entry.get("replacement_files", []):
        path = intent.get("path")
        backup_path = intent.get("backup_path")
        source_exists = bool(path) and os.path.lexists(path)
        backup_exists = bool(backup_path) and os.path.lexists(backup_path)
        if source_exists and backup_exists:
            return f"replacement quarantine conflict at {path}"
        if source_exists:
            try:
                os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                os.replace(path, backup_path)
            except OSError as exc:
                return f"failed to finish replacement quarantine at {path}: {exc}"
        elif not backup_exists:
            return f"replacement quarantine source and backup are both missing: {path}"

    known = {item.get("path") for item in entry.get("replacement_files", [])}
    for file_id in target_ids:
        item = file_by_id.get(file_id)
        path = item.get("path") if isinstance(item, dict) else None
        if path in known:
            continue
        if not _inside_series(path, series_path) or not os.path.lexists(path):
            return f"cannot safely quarantine replacement file {file_id}"
        relative = os.path.relpath(path, series_path)
        backup_path = os.path.join(recovery_root, "failed-replacement", relative)
        intent = {
            "episode_file_id": file_id, "path": path, "backup_path": backup_path,
        }
        entry.setdefault("replacement_files", []).append(intent)
        _save(entry)
        try:
            if os.lstat(path).st_dev != os.stat(os.path.dirname(series_path)).st_dev:
                return f"replacement file {file_id} is not on the recovery filesystem"
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
            os.replace(path, backup_path)
        except OSError as exc:
            return f"failed to quarantine replacement file {file_id}: {exc}"
    return None


def _rescan_series(api_url: str, api_key: str, api_timeout: int, series_id: int) -> bool:
    result = sonarr_api.arr_request(
        api_url, api_key, api_timeout, "command", method="POST",
        data={"name": "RescanSeries", "seriesId": int(series_id)}, count_api=False,
    )
    command_id = result.get("id") if isinstance(result, dict) else None
    if not command_id:
        return False
    for _ in range(30):
        status = sonarr_api.command_status(api_url, api_key, api_timeout, command_id)
        state = str(status.get("status", status.get("state", ""))).lower()
        if state == "completed":
            return True
        if state in {"failed", "aborted", "cancelled"}:
            return False
        time.sleep(1)
    return False


def _download_active(api_url: str, api_key: str, api_timeout: int,
                     download_id: Optional[str]) -> Optional[bool]:
    if not download_id:
        return False
    try:
        response = sonarr_api.arr_request(
            api_url, api_key, api_timeout, "queue?page=1&pageSize=1000", count_api=False,
        )
    except Exception as exc:
        logger.error("Cannot prove download %s inactive before rollback: %s", download_id, exc)
        return None
    if not isinstance(response, dict):
        return None
    for item in response.get("records", []):
        if str(item.get("downloadId")) != str(download_id):
            continue
        status = str(item.get("status", "")).lower()
        tracked_state = str(item.get("trackedDownloadState", "")).lower()
        # Completed downloads that Sonarr has definitively blocked/failed are no
        # longer writing target files and must not prevent the recovery transaction
        # from quarantining a partial import and restoring the originals.
        if tracked_state in {"importblocked", "failedpending", "failed", "warned", "ignored"}:
            return False
        if status in {"failed", "warning"}:
            return False
        return True
    return False


def _rollback(api_url: str, api_key: str, api_timeout: int, entry: Dict,
              reason: str) -> bool:
    active = _download_active(api_url, api_key, api_timeout, entry.get("download_id"))
    if active is not False:
        entry["state"] = "waiting_import"
        entry["error"] = f"rollback deferred while download remains active: {reason}"
        _save(entry)
        logger.warning("Exact-season recovery %s remains armed while its download is active",
                       entry["id"])
        return False
    entry["state"] = "restoring"
    entry["error"] = reason
    _save(entry)
    error = _quarantine_replacements(api_url, api_key, api_timeout, entry)
    if not error:
        error = _restore_files(entry)
    if not error and not _rescan_series(api_url, api_key, api_timeout, entry["series_id"]):
        error = "files restored but Sonarr did not accept RescanSeries"
    entry["state"] = "completed" if not error else "recovery_failed"
    entry["error"] = error
    _save(entry)
    if error:
        logger.error("Exact-season recovery %s needs attention: %s", entry["id"], error)
        return False
    logger.info("Exact-season recovery %s restored files and Sonarr state (%s)",
                entry["id"], reason)
    return True


def recover_pending(api_url: str, api_key: str, api_timeout: int,
                    instance_name: str, recovery_timeout_seconds: int = 600) -> bool:
    """Resolve unfinished journals on startup/cycle entry; safe to call repeatedly."""
    ok = True
    for entry in _pending(instance_name):
        logger.warning("Recovering interrupted exact-season operation %s (series=%s season=%s)",
                       entry["id"], entry["series_id"], entry["season_number"])
        # A crash can happen after submission/import but before the terminal journal
        # update. Recover the download id from timestamped history when possible.
        if entry.get("search_started_at") and not entry.get("download_id"):
            grab = _find_grab(api_url, api_key, api_timeout, entry)
            if grab and grab.get("downloadId"):
                entry["download_id"] = grab["downloadId"]
                entry["state"] = "waiting_import"
                _save(entry)
            else:
                try:
                    deadline = datetime.datetime.fromisoformat(
                        str(entry.get("deadline_at", "")).replace("Z", "+00:00")
                    )
                    before_deadline = datetime.datetime.now(datetime.timezone.utc) < deadline
                except (TypeError, ValueError):
                    before_deadline = False
                if before_deadline:
                    logger.warning(
                        "Exact-season operation %s has no unambiguous grab correlation yet; leaving journal armed",
                        entry["id"],
                    )
                    ok = False
                    continue
        if _import_succeeded(api_url, api_key, api_timeout, entry.get("download_id")):
            valid, validation_error = _validate_replacement(
                api_url, api_key, api_timeout, entry
            )
            if valid:
                entry["state"] = "imported"
                entry["error"] = None
                _save(entry)
                logger.info("Recovered operation %s as a validated complete import; originals remain preserved at %s",
                            entry["id"], entry["files"][0]["recovery_root"] if entry["files"] else "journal")
                continue
            if not _rollback(
                    api_url, api_key, api_timeout, entry,
                    f"import event failed exact-season validation: {validation_error}"):
                ok = False
            continue
        if entry.get("download_id"):
            queue_response = sonarr_api.arr_request(
                api_url, api_key, api_timeout,
                "queue?page=1&pageSize=1000", count_api=False,
            )
            queue = queue_response.get("records", []) if isinstance(queue_response, dict) else []
            still_active = any(str(item.get("downloadId")) == str(entry["download_id"])
                               for item in queue)
            timed_out = True
            try:
                if entry.get("deadline_at"):
                    deadline = datetime.datetime.fromisoformat(
                        str(entry["deadline_at"]).replace("Z", "+00:00")
                    )
                    timed_out = datetime.datetime.now(datetime.timezone.utc) >= deadline
                else:
                    started = datetime.datetime.fromisoformat(
                        str(entry.get("search_started_at", "")).replace("Z", "+00:00")
                    )
                    timed_out = ((datetime.datetime.now(datetime.timezone.utc) - started).total_seconds()
                                 >= max(1, int(recovery_timeout_seconds)))
            except (TypeError, ValueError):
                pass
            if still_active and not timed_out:
                logger.warning("Exact-season operation %s is still downloading/importing; leaving journal armed",
                               entry["id"])
                ok = False
                continue
        if not _rollback(api_url, api_key, api_timeout, entry, "interrupted operation"):
            ok = False
    return ok


def prepare_exact_season(api_url: str, api_key: str, api_timeout: int,
                         instance_name: str, series_id: int,
                         season_number: int,
                         expected_episode_ids: Optional[List[int]] = None,
                         expected_release_title: Optional[str] = None,
                         recovery_timeout_seconds: int = 600) -> Optional[str]:
    """Journal and stage files proven to belong exclusively to one exact season."""
    if not recover_pending(api_url, api_key, api_timeout, instance_name):
        logger.error("Force season replacement blocked: unresolved earlier recovery")
        return None

    episodes, episode_files, series = _inventory(api_url, api_key, api_timeout, series_id)
    series_path = series.get("path") if isinstance(series, dict) else None
    if isinstance(series_path, str):
        series_path = os.path.normpath(series_path)
    if (not isinstance(episodes, list) or not isinstance(episode_files, list)
            or not series_path or not os.path.isabs(series_path)):
        logger.error("Force season replacement blocked: Sonarr series/episode/file inventory unavailable")
        return None

    season_episodes = [ep for ep in episodes if ep.get("seasonNumber") == int(season_number)]
    season_episode_ids = [ep.get("id") for ep in season_episodes]
    if (not season_episode_ids
            or any(not isinstance(value, int) or isinstance(value, bool)
                   for value in season_episode_ids)
            or len(season_episode_ids) != len(set(season_episode_ids))):
        logger.error("Force season replacement blocked: target episode inventory is empty/ambiguous")
        return None
    if expected_episode_ids is not None:
        if (not isinstance(expected_episode_ids, list) or not expected_episode_ids
                or any(not isinstance(value, int) or isinstance(value, bool)
                       for value in expected_episode_ids)
                or len(expected_episode_ids) != len(set(expected_episode_ids))
                or set(expected_episode_ids) != set(season_episode_ids)):
            logger.error("Force season replacement blocked: release is not a complete exact-season mapping")
            return None

    target_ids = {int(ep["episodeFileId"]) for ep in episodes
                  if ep.get("seasonNumber") == int(season_number) and ep.get("episodeFileId")}
    foreign_ids = {int(ep["episodeFileId"]) for ep in episodes
                   if ep.get("seasonNumber") != int(season_number) and ep.get("episodeFileId")}
    if target_ids & foreign_ids:
        logger.error("Force season replacement blocked: an episode file spans outside season %s",
                     season_number)
        return None

    selected = [item for item in episode_files
                if item.get("id") is not None and int(item["id"]) in target_ids]
    if len(selected) != len(target_ids):
        logger.error("Force season replacement blocked: Sonarr returned %d of %d expected files",
                     len(selected), len(target_ids))
        return None
    if not selected and expected_episode_ids is None:
        logger.info("Force season replacement: exact season has no files to stage")
        return "no-files"

    journal_id = uuid.uuid4().hex
    recovery_root = f"{series_path}.huntarr-recovery/{journal_id}"
    files = []
    for episode_file in selected:
        path = episode_file.get("path")
        if not path or not os.path.isabs(path) or not os.path.lexists(path):
            logger.error("Force season replacement blocked: path is unavailable or not absolute: %s", path)
            return None
        try:
            if os.path.commonpath([os.path.realpath(os.path.dirname(path)), os.path.realpath(series_path)]) != os.path.realpath(series_path):
                raise ValueError("outside series path")
            relative_path = os.path.relpath(path, series_path)
        except (OSError, ValueError):
            logger.error("Force season replacement blocked: file is outside Sonarr series path: %s", path)
            return None
        # Keep preserved files outside the configured series directory so a Sonarr
        # rescan cannot accidentally re-import the recovery copy. Refuse cross-device
        # staging because atomic os.replace is part of the recovery guarantee.
        try:
            if os.lstat(path).st_dev != os.stat(os.path.dirname(series_path)).st_dev:
                raise OSError("cross-device recovery path")
        except OSError as exc:
            logger.error("Force season replacement blocked: recovery path is not same-filesystem: %s", exc)
            return None
        files.append({
            "episode_file_id": int(episode_file["id"]), "path": path,
            "backup_path": os.path.join(recovery_root, relative_path),
            "recovery_root": recovery_root, "was_symlink": os.path.islink(path),
        })

    entry = {
        "id": journal_id, "instance_name": str(instance_name),
        "series_id": int(series_id), "season_number": int(season_number),
        "state": "preparing", "files": files, "error": None,
        "replacement_files": [],
        "expected_episode_ids": list(expected_episode_ids or season_episode_ids),
        "series_path": series_path,
        "recovery_root": recovery_root,
        "expected_release_title": expected_release_title,
        "deadline_at": (
            datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(seconds=max(1, int(recovery_timeout_seconds)))
        ).isoformat().replace("+00:00", "Z"),
        "search_started_at": None, "download_id": None,
    }
    _save(entry)
    try:
        # Prove the recovery destination can be created before moving any original.
        # The journal is already durable, so even this preparatory change is tracked.
        os.makedirs(recovery_root, exist_ok=False)
        for item in files:
            os.makedirs(os.path.dirname(item["backup_path"]), exist_ok=True)
            os.replace(item["path"], item["backup_path"])
            item["moved"] = True
            _save(entry)
        for item in files:
            result = sonarr_api.arr_request(
                api_url, api_key, api_timeout,
                f"episodefile/{item['episode_file_id']}", method="DELETE", count_api=False,
            )
            if result is None:
                raise RuntimeError(f"Sonarr rejected DELETE episodefile/{item['episode_file_id']}")
        entry["state"] = "staged"
        _save(entry)
        logger.warning("Force season replacement staged %d file(s)/symlink(s) for exact season %s",
                       len(files), season_number)
        return journal_id
    except Exception as exc:
        logger.error("Force season replacement staging failed; restoring immediately: %s", exc)
        entry["state"] = "recovery_needed"
        entry["error"] = str(exc)
        _save(entry)
        _rollback(api_url, api_key, api_timeout, entry, "staging failure")
        return None


def _entry_by_id(instance_name: str, journal_id: str) -> Optional[Dict]:
    return next((entry for entry in _pending(instance_name) if entry["id"] == journal_id), None)


def mark_search_started(instance_name: str, journal_id: Optional[str],
                        search_started_at: str) -> bool:
    """Persist search timing before POST so restart recovery can identify its grab."""
    if journal_id == "no-files":
        return True
    if not journal_id:
        return False
    entry = _entry_by_id(instance_name, journal_id)
    if not entry:
        return False
    entry["search_started_at"] = search_started_at
    entry["state"] = "searching"
    _save(entry)
    return True


def _find_grab(api_url: str, api_key: str, api_timeout: int, entry: Dict) -> Optional[Dict]:
    response = sonarr_api.arr_request(
        api_url, api_key, api_timeout,
        f"history?pageSize=100&sortDirection=descending&sortKey=date&seriesId={entry['series_id']}",
        count_api=False,
    )
    if not isinstance(response, dict):
        return None
    start = str(entry.get("search_started_at") or "").rstrip("Z")
    matches = []
    expected_title = entry.get("expected_release_title")
    for record in response.get("records", []):
        if str(record.get("eventType", "")).lower() != "grabbed":
            continue
        if start and str(record.get("date", "")).rstrip("Z") < start:
            continue
        episode = record.get("episode") or {}
        if episode.get("seasonNumber") != entry["season_number"]:
            continue
        if expected_title:
            source_title = record.get("sourceTitle") or (record.get("data") or {}).get("sourceTitle")
            if source_title != expected_title:
                continue
        matches.append(record)
    # Never bind destructive recovery to an ambiguous concurrent season grab.
    return matches[0] if len(matches) == 1 else None


def _find_queued_download(api_url: str, api_key: str, api_timeout: int,
                          entry: Dict) -> Optional[str]:
    """Correlate one exact interactive release in Sonarr's queue."""
    expected_title = entry.get("expected_release_title")
    if not expected_title:
        return None
    response = sonarr_api.arr_request(
        api_url, api_key, api_timeout, "queue?page=1&pageSize=1000", count_api=False,
    )
    if not isinstance(response, dict):
        return None
    matches = []
    for item in response.get("records", []):
        title = item.get("title") or item.get("sourceTitle")
        series_id = item.get("seriesId") or (item.get("series") or {}).get("id")
        if title != expected_title or series_id != entry["series_id"]:
            continue
        download_id = item.get("downloadId")
        if download_id:
            matches.append(str(download_id))
    return matches[0] if len(matches) == 1 else None


def finish_operation(api_url: str, api_key: str, api_timeout: int,
                     instance_name: str, journal_id: Optional[str],
                     search_started_at: str, command_completed: bool,
                     wait_delay: int, wait_attempts: int,
                     stop_check: Callable[[], bool]) -> str:
    """Wait for a verified import or roll back. Returns imported/restored/failed/no-files."""
    if journal_id == "no-files":
        return "no-files"
    if not journal_id:
        return "failed"
    entry = _entry_by_id(instance_name, journal_id)
    if not entry:
        return "failed"
    entry["search_started_at"] = search_started_at
    entry["state"] = "search_complete" if command_completed else "recovery_needed"
    _save(entry)
    if not command_completed:
        return "restored" if _rollback(api_url, api_key, api_timeout, entry, "search failure/timeout") else "failed"

    delay = max(1, int(wait_delay or 1))
    attempts = max(1, int(wait_attempts or 1))
    download_id = None
    for attempt in range(attempts):
        grab = _find_grab(api_url, api_key, api_timeout, entry)
        download_id = grab.get("downloadId") if grab else None
        if download_id:
            break
        if stop_check():
            return "restored" if _rollback(api_url, api_key, api_timeout, entry, "stop requested") else "failed"
        if attempt + 1 < attempts:
            time.sleep(delay)
    if not download_id:
        return "restored" if _rollback(api_url, api_key, api_timeout, entry, "no verified grab") else "failed"

    entry["download_id"] = download_id
    entry["state"] = "waiting_import"
    _save(entry)
    for _ in range(attempts):
        records = sonarr_api.get_history_for_download(api_url, api_key, api_timeout, download_id)
        event_types = {str(record.get("eventType", "")).lower() for record in records}
        if "downloadfolderimported" in event_types:
            valid, validation_error = _validate_replacement(
                api_url, api_key, api_timeout, entry
            )
            if valid:
                entry["state"] = "imported"
                entry["error"] = None
                _save(entry)
                logger.warning("Exact-season import validated; original files are preserved at %s",
                               entry["files"][0]["recovery_root"] if entry["files"] else "journal")
                return "imported"
            return ("restored" if _rollback(
                api_url, api_key, api_timeout, entry,
                f"partial/invalid import: {validation_error}",
            ) else "failed")
        if "downloadfailed" in event_types:
            return "restored" if _rollback(api_url, api_key, api_timeout, entry, "download/import failed") else "failed"
        if stop_check():
            return "restored" if _rollback(api_url, api_key, api_timeout, entry, "stop requested") else "failed"
        time.sleep(delay)
    return "restored" if _rollback(api_url, api_key, api_timeout, entry, "import wait timeout") else "failed"

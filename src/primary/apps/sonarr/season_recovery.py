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
        "recovery_timeout_seconds": entry.get("recovery_timeout_seconds"),
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
            "recovery_timeout_seconds": payload.get("recovery_timeout_seconds"),
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


def _same_file_identity(first: str, second: str) -> bool:
    """Return whether two paths are the same retained file or symlink payload."""
    if not first or not second or not os.path.lexists(first) or not os.path.lexists(second):
        return False
    try:
        if os.path.islink(first) and os.path.islink(second):
            return os.readlink(first) == os.readlink(second)
        return os.path.samestat(os.lstat(first), os.lstat(second))
    except OSError:
        return False


def _restore_files(entry: Dict) -> Optional[str]:
    """Restore each staged path while retaining the recovery copy.

    Staging already proved both paths are on one filesystem, so a hard link restores
    regular files and symlinks without copying media or consuming the only backup.
    Keeping that backup protects against a late Sonarr import after an ambiguous POST.
    """
    conflicts = []
    for item in entry["files"]:
        original = item["path"]
        backup = item["backup_path"]
        if os.path.lexists(backup):
            if os.path.lexists(original):
                if _same_file_identity(original, backup):
                    continue
                conflicts.append(original)
                continue
            os.makedirs(os.path.dirname(original), exist_ok=True)
            try:
                os.link(backup, original, follow_symlinks=False)
            except OSError as exc:
                conflicts.append(f"{original} ({exc})")
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
                          entry: Dict, history_records: Optional[List[Dict]] = None
                          ) -> Tuple[bool, str]:
    """Prove every exact-season episode maps to this transaction's imported file."""
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
    imported_file_by_episode = {}
    if entry.get("expected_release_title"):
        if not isinstance(history_records, list) or not entry.get("download_id"):
            return False, "correlated import history is unavailable"
        for record in history_records:
            if (str(record.get("eventType", "")).lower() != "downloadfolderimported"
                    or str(record.get("downloadId")) != str(entry["download_id"])):
                continue
            episode_id = record.get("episodeId")
            file_id = (record.get("data") or {}).get("fileId")
            try:
                episode_id = int(episode_id)
                file_id = int(file_id)
            except (TypeError, ValueError):
                continue
            previous = imported_file_by_episode.setdefault(episode_id, file_id)
            if previous != file_id:
                return False, f"episode {episode_id} has ambiguous imported file provenance"
        if set(imported_file_by_episode) != set(expected_ids):
            return False, "correlated download has not imported every exact-season episode"

    target_file_ids = []
    for episode in target:
        file_id = episode.get("episodeFileId")
        if (not isinstance(file_id, int) or isinstance(file_id, bool) or file_id <= 0
                or file_id in original_ids or file_id not in file_by_id):
            return False, f"episode {episode.get('id')} lacks a verified replacement file"
        if (imported_file_by_episode
                and imported_file_by_episode.get(episode.get("id")) != file_id):
            return False, f"episode {episode.get('id')} is not mapped to its correlated imported file"
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


def _restored_originals_registered(api_url: str, api_key: str, api_timeout: int,
                                   entry: Dict) -> Tuple[bool, str]:
    """Prove every retained original is back on disk and registered in Sonarr."""
    _episodes, episode_files, _series = _inventory(
        api_url, api_key, api_timeout, entry["series_id"]
    )
    if not isinstance(episode_files, list):
        return False, "restored episode-file inventory is unavailable"
    registered_paths = {
        os.path.normpath(item.get("path")) for item in episode_files
        if isinstance(item, dict) and isinstance(item.get("path"), str)
    }
    for item in entry.get("files", []):
        path = item.get("path")
        backup = item.get("backup_path")
        if (not isinstance(path, str) or not _same_file_identity(path, backup)
                or os.path.normpath(path) not in registered_paths):
            return False, f"restored original is not registered in Sonarr: {path}"
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
    original_backups = {
        item.get("path"): item.get("backup_path") for item in entry.get("files", [])
    }
    for intent in entry.get("replacement_files", []):
        path = intent.get("path")
        backup_path = intent.get("backup_path")
        source_exists = bool(path) and os.path.lexists(path)
        backup_exists = bool(backup_path) and os.path.lexists(backup_path)
        if source_exists and backup_exists:
            # A prior recovery pass may already have restored the retained original
            # after quarantining a same-path replacement. Treat that durable state as
            # reconciled rather than repeatedly quarantining the restored original.
            if _same_file_identity(path, original_backups.get(path)):
                continue
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
    if not error:
        rescan_ok = _rescan_series(api_url, api_key, api_timeout, entry["series_id"])
        registered, registration_error = _restored_originals_registered(
            api_url, api_key, api_timeout, entry
        )
        if not registered:
            error = (
                registration_error if rescan_ok
                else f"Sonarr RescanSeries failed and {registration_error}"
            )
    entry["state"] = "completed" if not error else "recovery_failed"
    entry["error"] = error
    _save(entry)
    if error:
        logger.error("Exact-season recovery %s needs attention: %s", entry["id"], error)
        return False
    logger.info("Exact-season recovery %s restored files and Sonarr state (%s)",
                entry["id"], reason)
    return True


def _deadline_remaining(entry: Dict) -> Optional[float]:
    try:
        deadline = datetime.datetime.fromisoformat(
            str(entry.get("deadline_at", "")).replace("Z", "+00:00")
        )
        return (deadline - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
    except (TypeError, ValueError):
        return None


def _before_deadline(entry: Dict, fallback_seconds: int = 600) -> bool:
    if not entry.get("search_started_at"):
        return False
    try:
        started = datetime.datetime.fromisoformat(
            str(entry["search_started_at"]).replace("Z", "+00:00")
        )
        if entry.get("deadline_at"):
            # The durable transaction deadline is authoritative. A restart or
            # later configuration change must never shorten its settle window.
            deadline = datetime.datetime.fromisoformat(
                str(entry["deadline_at"]).replace("Z", "+00:00")
            )
        else:
            deadline = started + datetime.timedelta(
                seconds=max(1, int(fallback_seconds))
            )
        return datetime.datetime.now(datetime.timezone.utc) < deadline
    except (TypeError, ValueError):
        return False


def recover_pending(api_url: str, api_key: str, api_timeout: int,
                    instance_name: str, recovery_timeout_seconds: int = 600) -> bool:
    """Resolve unfinished journals on startup/cycle entry; safe to call repeatedly."""
    ok = True
    for entry in _pending(instance_name):
        logger.warning("Recovering interrupted exact-season operation %s (series=%s season=%s)",
                       entry["id"], entry["series_id"], entry["season_number"])
        # A crash or client timeout can happen after Sonarr accepted the POST. Use
        # exact-title history and queue correlation, and never restore while the
        # transaction is still inside its durable settle window.
        if entry.get("search_started_at") and not entry.get("download_id"):
            grab = _find_grab(api_url, api_key, api_timeout, entry)
            queued_download_id = None if grab else _find_queued_download(
                api_url, api_key, api_timeout, entry
            )
            if (grab and grab.get("downloadId")) or queued_download_id:
                entry["download_id"] = grab.get("downloadId") if grab else queued_download_id
                entry["state"] = "waiting_import"
                _save(entry)

        records = []
        if entry.get("download_id"):
            records = sonarr_api.get_history_for_download(
                api_url, api_key, api_timeout, entry["download_id"]
            )
            event_types = {str(item.get("eventType", "")).lower() for item in records}
            if "downloadfailed" in event_types:
                if not _rollback(api_url, api_key, api_timeout, entry, "download failed"):
                    ok = False
                continue
            if "downloadfolderimported" in event_types:
                valid, validation_error = _validate_replacement(
                    api_url, api_key, api_timeout, entry, records
                )
                if valid:
                    entry["state"] = "imported"
                    entry["error"] = None
                    _save(entry)
                    logger.info(
                        "Recovered operation %s as a validated complete import; originals remain preserved at %s",
                        entry["id"],
                        entry["files"][0]["recovery_root"] if entry["files"] else "journal",
                    )
                    continue
                entry["error"] = f"incomplete correlated import: {validation_error}"
                _save(entry)

        if _before_deadline(entry, recovery_timeout_seconds):
            logger.warning(
                "Exact-season operation %s remains armed while correlation/import settles",
                entry["id"],
            )
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
        "recovery_timeout_seconds": max(1, int(recovery_timeout_seconds)),
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
    entry["deadline_at"] = (
        datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(seconds=max(
            1, int(entry.get("recovery_timeout_seconds") or 600)
        ))
    ).isoformat().replace("+00:00", "Z")
    entry["state"] = "searching"
    _save(entry)
    return True


def mark_submission_indeterminate(instance_name: str, journal_id: Optional[str],
                                  error: str) -> bool:
    """Keep staged originals armed when Sonarr may have accepted a timed-out POST."""
    if not journal_id or journal_id == "no-files":
        return False
    entry = _entry_by_id(instance_name, journal_id)
    if not entry:
        return False
    entry["state"] = "awaiting_correlation"
    entry["error"] = str(error)
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
    if not matches:
        return None
    if not expected_title:
        # Preserve the legacy command-search behavior for the existing optional
        # upgrade path; the new override path always has an exact release title.
        return matches[0]
    download_ids = {str(item.get("downloadId")) for item in matches if item.get("downloadId")}
    if len(download_ids) != 1:
        return None
    return next(item for item in matches if str(item.get("downloadId")) in download_ids)


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
    matches = set()
    for item in response.get("records", []):
        title = item.get("title") or item.get("sourceTitle")
        series_id = item.get("seriesId") or (item.get("series") or {}).get("id")
        if title != expected_title or series_id != entry["series_id"]:
            continue
        seasons = set()
        episode = item.get("episode") or {}
        if episode.get("seasonNumber") is not None:
            seasons.add(episode.get("seasonNumber"))
        for queued_episode in item.get("episodes") or []:
            if isinstance(queued_episode, dict) and queued_episode.get("seasonNumber") is not None:
                seasons.add(queued_episode.get("seasonNumber"))
        if seasons and seasons != {entry["season_number"]}:
            return None
        download_id = item.get("downloadId")
        if download_id:
            matches.add(str(download_id))
    return next(iter(matches)) if len(matches) == 1 else None


def finish_operation(api_url: str, api_key: str, api_timeout: int,
                     instance_name: str, journal_id: Optional[str],
                     search_started_at: str, command_completed: bool,
                     wait_delay: int, wait_attempts: int,
                     stop_check: Callable[[], bool]) -> str:
    """Wait within one budget for correlation plus a provenance-verified import."""
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
    download_id = entry.get("download_id")
    validation_error = "no complete correlated import"
    for attempt in range(attempts):
        remaining = _deadline_remaining(entry)
        if remaining is not None and remaining <= 0:
            break
        if not download_id:
            grab = _find_grab(api_url, api_key, api_timeout, entry)
            download_id = grab.get("downloadId") if grab else _find_queued_download(
                api_url, api_key, api_timeout, entry
            )
            if download_id:
                entry["download_id"] = download_id
                entry["state"] = "waiting_import"
                _save(entry)
        if download_id:
            records = sonarr_api.get_history_for_download(
                api_url, api_key, api_timeout, download_id
            )
            event_types = {str(record.get("eventType", "")).lower() for record in records}
            if "downloadfailed" in event_types:
                return ("restored" if _rollback(
                    api_url, api_key, api_timeout, entry, "download/import failed"
                ) else "failed")
            if "downloadfolderimported" in event_types:
                valid, validation_error = _validate_replacement(
                    api_url, api_key, api_timeout, entry, records
                )
                if valid:
                    entry["state"] = "imported"
                    entry["error"] = None
                    _save(entry)
                    logger.warning(
                        "Exact-season import validated; original files are preserved at %s",
                        entry["files"][0]["recovery_root"] if entry["files"] else "journal",
                    )
                    return "imported"
                entry["error"] = f"incomplete correlated import: {validation_error}"
                _save(entry)
        if stop_check():
            if entry.get("expected_release_title"):
                entry["state"] = "awaiting_correlation"
                entry["error"] = "stop requested after override submission; recovery remains armed"
                _save(entry)
                return "failed"
            return ("restored" if _rollback(
                api_url, api_key, api_timeout, entry, "stop requested"
            ) else "failed")
        if attempt + 1 < attempts:
            remaining = _deadline_remaining(entry)
            if remaining is None:
                time.sleep(delay)
            elif remaining > 0:
                time.sleep(min(delay, remaining))

    remaining = _deadline_remaining(entry)
    if entry.get("expected_release_title") and remaining is not None and remaining > 0:
        entry["state"] = "waiting_import" if download_id else "awaiting_correlation"
        entry["error"] = (
            f"settle deadline has not elapsed ({remaining:.1f}s remain); recovery stays armed"
        )
        _save(entry)
        return "failed"
    return ("restored" if _rollback(
        api_url, api_key, api_timeout, entry,
        f"import wait timeout: {validation_error}",
    ) else "failed")

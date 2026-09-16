"""Safety and restart tests for guarded Sonarr exact-season replacement."""

import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

os.environ.setdefault("HUNTARR_CONFIG_DIR", tempfile.mkdtemp(prefix="huntarr_recovery_import_"))
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.primary.apps.sonarr import season_recovery  # noqa: E402


class _DB:
    def __init__(self, path):
        self.path = str(path)
        with self.get_connection() as conn:
            conn.execute("""CREATE TABLE sonarr_season_recovery_journal (
                id TEXT PRIMARY KEY, instance_name TEXT NOT NULL,
                series_id INTEGER NOT NULL, season_number INTEGER NOT NULL,
                state TEXT NOT NULL, files_json TEXT NOT NULL, error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.path)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


class SeasonRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.db = _DB(root / "journal.db")
        self.media = root / "show" / "Season 01"
        self.media.mkdir(parents=True)
        self.target = root / "storage" / "episode.mkv"
        self.target.parent.mkdir()
        self.target.write_bytes(b"media")
        self.link = self.media / "Show.S01E01.mkv"
        self.link.symlink_to(self.target)
        self.episodes = [
            {"id": 11, "seasonNumber": 1, "episodeFileId": 101},
            {"id": 12, "seasonNumber": 2, "episodeFileId": 202},
        ]
        self.files = [
            {"id": 101, "path": str(self.link)},
            {"id": 202, "path": str(self.media / "Show.S02E01.mkv")},
        ]
        self.db_patch = mock.patch.object(season_recovery, "get_database", return_value=self.db)
        self.status_patch = mock.patch.object(
            season_recovery.sonarr_api, "command_status", return_value={"status": "completed"}
        )
        self.db_patch.start()
        self.status_patch.start()

    def tearDown(self):
        self.status_patch.stop()
        self.db_patch.stop()
        self.tmp.cleanup()

    def _request(self, *args, **kwargs):
        endpoint = args[3]
        if endpoint.startswith("episode?"):
            return self.episodes
        if endpoint.startswith("episodefile?"):
            return self.files
        if endpoint == "series/7":
            return {"id": 7, "path": str(self.media.parent)}
        if endpoint.startswith("episodefile/") and kwargs.get("method") == "DELETE":
            return {}
        if endpoint == "command" and kwargs.get("data", {}).get("name") == "RescanSeries":
            return {"id": 900}
        if endpoint.startswith("history?"):
            return {"records": []}
        if endpoint.startswith("queue?"):
            return {"records": []}
        raise AssertionError(f"unexpected API request: {endpoint} {kwargs}")

    def test_no_grab_restores_same_symlink_and_sonarr_state(self):
        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=self._request):
            journal_id = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1
            )
            self.assertFalse(os.path.lexists(self.link))
            entry = season_recovery._entry_by_id("instance", journal_id)
            backup = Path(entry["files"][0]["backup_path"])
            self.assertTrue(backup.is_symlink())
            outcome = season_recovery.finish_operation(
                "http://sonarr", "key", 10, "instance", journal_id,
                "2026-09-12T10:00:00Z", True, 1, 1, lambda: False,
            )
        self.assertEqual(outcome, "restored")
        self.assertTrue(self.link.is_symlink())
        self.assertEqual(self.link.resolve(), self.target.resolve())
        self.assertFalse(os.path.lexists(backup))
        with self.db.get_connection() as conn:
            state = conn.execute("SELECT state FROM sonarr_season_recovery_journal").fetchone()[0]
        self.assertEqual(state, "completed")

    def test_regular_file_bytes_are_restored_on_no_grab(self):
        self.link.unlink()
        self.link.write_bytes(b"original-regular-file")
        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=self._request):
            journal_id = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1
            )
            outcome = season_recovery.finish_operation(
                "http://sonarr", "key", 10, "instance", journal_id,
                "2026-09-12T10:00:00Z", True, 1, 1, lambda: False,
            )
        self.assertEqual(outcome, "restored")
        self.assertFalse(self.link.is_symlink())
        self.assertEqual(self.link.read_bytes(), b"original-regular-file")

    def test_cross_season_episode_file_is_rejected_before_touching_disk(self):
        self.episodes[1]["episodeFileId"] = 101
        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=self._request):
            journal_id = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1
            )
        self.assertIsNone(journal_id)
        self.assertTrue(self.link.is_symlink())
        with self.db.get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM sonarr_season_recovery_journal").fetchone()[0]
        self.assertEqual(count, 0)

    def test_incomplete_or_duplicate_release_mapping_is_rejected_before_staging(self):
        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=self._request):
            incomplete = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1,
                expected_episode_ids=[11, 99],
            )
            duplicate = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1,
                expected_episode_ids=[11, 11],
            )
        self.assertIsNone(incomplete)
        self.assertIsNone(duplicate)
        self.assertTrue(self.link.is_symlink())
        with self.db.get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM sonarr_season_recovery_journal").fetchone()[0]
        self.assertEqual(count, 0)

    def test_non_target_season_file_is_never_moved(self):
        other = Path(self.files[1]["path"])
        other.parent.mkdir(parents=True, exist_ok=True)
        other.write_bytes(b"season-two")
        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=self._request):
            journal_id = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1,
                expected_episode_ids=[11],
            )
        self.assertIsNotNone(journal_id)
        self.assertEqual(other.read_bytes(), b"season-two")

    def test_interrupted_staging_is_idempotently_restored_next_cycle(self):
        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=self._request):
            journal_id = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1
            )
            self.assertIsNotNone(journal_id)
            self.assertFalse(os.path.lexists(self.link))
            self.assertTrue(season_recovery.recover_pending(
                "http://sonarr", "key", 10, "instance"
            ))
            self.assertTrue(season_recovery.recover_pending(
                "http://sonarr", "key", 10, "instance"
            ))
        self.assertTrue(self.link.is_symlink())

    def test_import_timeout_restores_old_symlink_and_rescans(self):
        def request(*args, **kwargs):
            endpoint = args[3]
            if endpoint.startswith("history?"):
                return {"records": [{
                    "eventType": "grabbed", "date": "2026-09-12T10:00:01Z",
                    "downloadId": "slow", "episode": {"seasonNumber": 1},
                }]}
            if endpoint.startswith("queue?"):
                return {"records": []}
            return self._request(*args, **kwargs)

        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=request), \
             mock.patch.object(season_recovery.sonarr_api, "get_history_for_download", return_value=[]), \
             mock.patch.object(season_recovery.time, "sleep"):
            journal_id = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1
            )
            outcome = season_recovery.finish_operation(
                "http://sonarr", "key", 10, "instance", journal_id,
                "2026-09-12T10:00:00Z", True, 1, 1, lambda: False,
            )
        self.assertEqual(outcome, "restored")
        self.assertTrue(self.link.is_symlink())

    def test_restart_leaves_recent_active_download_armed(self):
        started = season_recovery.utc_now_iso()

        def request(*args, **kwargs):
            endpoint = args[3]
            if endpoint.startswith("history?"):
                return {"records": [{
                    "eventType": "grabbed", "date": started,
                    "downloadId": "active", "episode": {"seasonNumber": 1},
                }]}
            if endpoint.startswith("queue?"):
                return {"records": [{"downloadId": "active"}]}
            return self._request(*args, **kwargs)

        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=request), \
             mock.patch.object(season_recovery.sonarr_api, "get_history_for_download", return_value=[]):
            journal_id = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1
            )
            self.assertTrue(season_recovery.mark_search_started("instance", journal_id, started))
            self.assertFalse(season_recovery.recover_pending(
                "http://sonarr", "key", 10, "instance", recovery_timeout_seconds=600
            ))
        self.assertFalse(os.path.lexists(self.link))
        entry = season_recovery._entry_by_id("instance", journal_id)
        self.assertEqual(entry["state"], "waiting_import")
        self.assertEqual(entry["download_id"], "active")

    def test_timeout_defers_rollback_until_download_is_no_longer_active(self):
        active = [True]

        def request(*args, **kwargs):
            endpoint = args[3]
            if endpoint.startswith("history?"):
                return {"records": [{
                    "eventType": "grabbed", "date": "2026-09-12T10:00:01Z",
                    "downloadId": "still-live", "episode": {"seasonNumber": 1},
                }]}
            if endpoint.startswith("queue?"):
                return {"records": [{"downloadId": "still-live"}]} if active[0] else {"records": []}
            return self._request(*args, **kwargs)

        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=request), \
             mock.patch.object(season_recovery.sonarr_api, "get_history_for_download", return_value=[]), \
             mock.patch.object(season_recovery.time, "sleep"):
            journal_id = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1,
            )
            outcome = season_recovery.finish_operation(
                "http://sonarr", "key", 10, "instance", journal_id,
                "2026-09-12T10:00:00Z", True, 1, 1, lambda: False,
            )
            self.assertEqual(outcome, "failed")
            self.assertFalse(os.path.lexists(self.link))
            active[0] = False
            self.assertTrue(season_recovery.recover_pending(
                "http://sonarr", "key", 10, "instance", recovery_timeout_seconds=1,
            ))
        self.assertTrue(self.link.is_symlink())

    def test_grab_correlation_fails_closed_on_same_release_ambiguity(self):
        entry = {
            "series_id": 7, "season_number": 1,
            "search_started_at": "2026-09-12T10:00:00Z",
            "expected_release_title": "Show.S01.Pack",
        }
        records = [{
            "eventType": "grabbed", "date": "2026-09-12T10:00:01Z",
            "downloadId": value, "sourceTitle": "Show.S01.Pack",
            "episode": {"seasonNumber": 1},
        } for value in ("one", "two")]
        with mock.patch.object(
            season_recovery.sonarr_api, "arr_request", return_value={"records": records}
        ):
            self.assertIsNone(season_recovery._find_grab(
                "http://sonarr", "key", 10, entry,
            ))

    def test_uncorrelated_override_rolls_back_after_deadline(self):
        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=self._request):
            journal_id = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1,
                expected_episode_ids=[11],
                expected_release_title="Show.S01.Pack",
                recovery_timeout_seconds=1,
            )
            self.assertTrue(season_recovery.mark_search_started(
                "instance", journal_id, "2026-09-12T10:00:00Z",
            ))
            entry = season_recovery._entry_by_id("instance", journal_id)
            entry["deadline_at"] = "2026-09-12T10:00:01Z"
            season_recovery._save(entry)
            self.assertTrue(season_recovery.recover_pending(
                "http://sonarr", "key", 10, "instance",
            ))
        self.assertTrue(self.link.is_symlink())
        with self.db.get_connection() as conn:
            state = conn.execute(
                "SELECT state FROM sonarr_season_recovery_journal WHERE id=?", (journal_id,)
            ).fetchone()[0]
        self.assertEqual(state, "completed")

    def test_quarantine_move_intent_recovers_after_rename_crash_window(self):
        replacement = self.media / "Show.S01E01.new.mkv"
        replacement.write_bytes(b"new")
        backup = Path(self.tmp.name) / "recovery" / "failed-replacement" / replacement.name
        backup.parent.mkdir(parents=True)
        os.replace(replacement, backup)  # crash after rename, before any later save
        self.episodes[0]["episodeFileId"] = 303
        self.files.append({"id": 303, "path": str(replacement)})
        entry = {
            "series_id": 7, "season_number": 1,
            "files": [{"episode_file_id": 101}],
            "recovery_root": str(Path(self.tmp.name) / "recovery"),
            "replacement_files": [{
                "episode_file_id": 303, "path": str(replacement),
                "backup_path": str(backup),
            }],
        }
        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=self._request):
            self.assertIsNone(season_recovery._quarantine_replacements(
                "http://sonarr", "key", 10, entry,
            ))
        self.assertFalse(os.path.lexists(replacement))
        self.assertEqual(backup.read_bytes(), b"new")

    def test_verified_import_preserves_old_symlink_in_recovery_area(self):
        def request(*args, **kwargs):
            endpoint = args[3]
            if endpoint.startswith("history?"):
                return {"records": [{
                    "eventType": "grabbed", "date": "2026-09-12T10:00:01Z",
                    "downloadId": "abc", "episode": {"seasonNumber": 1},
                }]}
            return self._request(*args, **kwargs)

        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=request), \
             mock.patch.object(season_recovery.sonarr_api, "get_history_for_download",
                               return_value=[{"eventType": "downloadFolderImported"}]):
            journal_id = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1,
                expected_episode_ids=[11],
            )
            entry = season_recovery._entry_by_id("instance", journal_id)
            backup = Path(entry["files"][0]["backup_path"])
            self.link.write_bytes(b"replacement")
            self.episodes[0]["episodeFileId"] = 303
            self.files.append({"id": 303, "path": str(self.link)})
            outcome = season_recovery.finish_operation(
                "http://sonarr", "key", 10, "instance", journal_id,
                "2026-09-12T10:00:00Z", True, 1, 1, lambda: False,
            )
        self.assertEqual(outcome, "imported")
        self.assertTrue(backup.is_symlink())
        self.assertEqual(self.link.read_bytes(), b"replacement")
        with self.db.get_connection() as conn:
            state = conn.execute("SELECT state FROM sonarr_season_recovery_journal").fetchone()[0]
        self.assertEqual(state, "imported")

    def test_import_blocked_queue_entry_does_not_prevent_rollback(self):
        queue = {"records": [{
            "downloadId": "blocked", "status": "completed",
            "trackedDownloadState": "importBlocked",
        }]}
        with mock.patch.object(
            season_recovery.sonarr_api, "arr_request", return_value=queue,
        ):
            self.assertFalse(season_recovery._download_active(
                "http://sonarr", "key", 10, "blocked",
            ))

    def test_downloading_queue_entry_defers_rollback(self):
        queue = {"records": [{
            "downloadId": "active", "status": "downloading",
            "trackedDownloadState": "downloading",
        }]}
        with mock.patch.object(
            season_recovery.sonarr_api, "arr_request", return_value=queue,
        ):
            self.assertTrue(season_recovery._download_active(
                "http://sonarr", "key", 10, "active",
            ))

    def test_partial_import_is_quarantined_and_original_is_restored(self):
        second = self.media / "Show.S01E02.mkv"
        second_target = self.target.parent / "episode2.mkv"
        second_target.write_bytes(b"media-two")
        second.symlink_to(second_target)
        self.episodes.insert(1, {"id": 13, "seasonNumber": 1, "episodeFileId": 103})
        self.files.insert(1, {"id": 103, "path": str(second)})

        def request(*args, **kwargs):
            endpoint = args[3]
            if endpoint.startswith("history?"):
                return {"records": [{
                    "eventType": "grabbed", "date": "2026-09-12T10:00:01Z",
                    "downloadId": "partial", "episode": {"seasonNumber": 1},
                }]}
            if endpoint.startswith("queue?"):
                return {"records": []}
            return self._request(*args, **kwargs)

        with mock.patch.object(season_recovery.sonarr_api, "arr_request", side_effect=request), \
             mock.patch.object(season_recovery.sonarr_api, "get_history_for_download",
                               return_value=[{"eventType": "downloadFolderImported"}]):
            journal_id = season_recovery.prepare_exact_season(
                "http://sonarr", "key", 10, "instance", 7, 1,
                expected_episode_ids=[11, 13],
            )
            self.link.write_bytes(b"partial-new")
            self.episodes[0]["episodeFileId"] = 303
            self.episodes[1]["episodeFileId"] = 0
            self.files.append({"id": 303, "path": str(self.link)})
            outcome = season_recovery.finish_operation(
                "http://sonarr", "key", 10, "instance", journal_id,
                "2026-09-12T10:00:00Z", True, 1, 1, lambda: False,
            )
            entry = season_recovery._pending("instance")

        self.assertEqual(outcome, "restored")
        self.assertTrue(self.link.is_symlink())
        self.assertEqual(self.link.resolve(), self.target.resolve())
        self.assertTrue(second.is_symlink())
        self.assertEqual(second.resolve(), second_target.resolve())
        self.assertEqual(entry, [])
        with self.db.get_connection() as conn:
            row = conn.execute("SELECT state, files_json FROM sonarr_season_recovery_journal").fetchone()
        self.assertEqual(row[0], "completed")
        self.assertIn("failed-replacement", row[1])


if __name__ == "__main__":
    unittest.main()

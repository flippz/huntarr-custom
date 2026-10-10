"""Focused Magnetarr Real-Debrid submission race regressions."""

import os
import tempfile
import threading
import unittest
from contextlib import contextmanager
from datetime import datetime
from unittest import mock

os.environ.setdefault("HUNTARR_CONFIG_DIR", tempfile.mkdtemp(prefix="magnetarr_test_"))

from src.primary.apps.magnetarr import handler
from src.primary.utils.db_mixins.db_magnetarr import MagnetarrMixin


class _FakeDB:
    def __init__(self, rows=()):
        self.lock = threading.Lock()
        self.rows = {row["info_hash"]: dict(row) for row in rows}
        self.deleted_status = []

    def get_realdebrid_submission(self, info_hash):
        with self.lock:
            row = self.rows.get(info_hash.lower())
            return dict(row) if row else None

    def get_active_realdebrid_submissions(self):
        with self.lock:
            return [dict(row) for row in self.rows.values() if row.get("active")]

    def upsert_realdebrid_submission(self, data):
        with self.lock:
            current = self.rows.get(data["info_hash"])
            row = dict(data)
            if current and current.get("rd_torrent_id") and not data.get("replace_rd_torrent_id"):
                row["rd_torrent_id"] = current["rd_torrent_id"]
            self.rows[data["info_hash"]] = row

    def mark_realdebrid_submission_status(self, info_hash, status, active=True, last_error=""):
        with self.lock:
            self.rows[info_hash].update(status=status, active=active, last_error=last_error)
            self.deleted_status.append((info_hash, status, active))

    def insert_magnet_if_new(self, _record):
        return True

    def record_magnetarr_event(self, *_args):
        pass

    def mark_source_scanned(self, *_args):
        pass


def _old_row(info_hash, torrent_id, permalink="https://reddit/post"):
    return {
        "info_hash": info_hash, "magnet_uri": f"magnet:?xt=urn:btih:{info_hash}",
        "title": "Race", "permalink": permalink, "source_id": "source",
        "rd_torrent_id": torrent_id, "content_hash": "old", "status": "downloaded",
        "active": True, "first_seen_at": datetime.now().isoformat(),
        "last_checked_at": "2000-01-01T00:00:00",
    }


class MagnetarrSubmissionRaceTests(unittest.TestCase):
    def setUp(self):
        handler._REALDEBRID_SUBMISSION_LOCK = threading.RLock()
        self.post = {
            "title": "Race", "permalink": "https://reddit/post", "content_html": "new",
        }
        self.new_hash = "c" * 40
        self.new_magnet = f"magnet:?xt=urn:btih:{self.new_hash}"

    def _settings(self, _name):
        return {"realdebrid_api_token": "token", "realdebrid_watch_days": 4,
                "realdebrid_recheck_minutes": 0}

    def test_scanner_and_recheck_overlap_add_current_hash_once(self):
        db = _FakeDB([_old_row("a" * 40, "old-rd")])
        add_entered = threading.Event()
        release_add = threading.Event()
        adds = []

        def add(magnet, _token):
            adds.append(magnet)
            add_entered.set()
            release_add.wait(2)
            return "canonical-rd", ""

        source = {"id": "source", "name": "feed", "url": "url", "realdebrid_auto_add": True}
        post = dict(self.post, content_html=self.new_magnet)
        patches = [
            mock.patch.object(handler, "get_database", return_value=db),
            mock.patch.object(handler, "load_settings", side_effect=self._settings),
            mock.patch.object(handler, "fetch_reddit_listing", return_value=([post], "")),
            mock.patch.object(handler, "fetch_single_post", return_value=(post, "")),
            mock.patch.object(handler, "rd_add_and_select", side_effect=add),
            mock.patch.object(handler, "rd_get_status", return_value=("downloaded", "")),
            mock.patch.object(handler, "rd_delete_torrent", return_value=""),
            mock.patch.object(handler.time, "sleep", return_value=None),
        ]
        for patch in patches:
            patch.start()
        try:
            scanner = threading.Thread(target=handler.scan_source, args=(source,))
            rechecker = threading.Thread(target=handler.recheck_realdebrid_watched_posts)
            scanner.start()
            self.assertTrue(add_entered.wait(1))
            rechecker.start()
            release_add.set()
            scanner.join(2)
            rechecker.join(2)
        finally:
            for patch in reversed(patches):
                patch.stop()

        self.assertFalse(scanner.is_alive() or rechecker.is_alive())
        self.assertEqual(adds, [self.new_magnet])
        self.assertEqual(db.rows[self.new_hash]["rd_torrent_id"], "canonical-rd")
        self.assertFalse(db.rows["a" * 40]["active"])

    def test_multiple_old_watchers_are_collapsed_and_deleted_once(self):
        db = _FakeDB([_old_row("a" * 40, "old-a"), _old_row("b" * 40, "old-b")])
        deleted = []
        post = dict(self.post, content_html=self.new_magnet)
        with mock.patch.object(handler, "get_database", return_value=db), \
             mock.patch.object(handler, "load_settings", side_effect=self._settings), \
             mock.patch.object(handler, "fetch_single_post", return_value=(post, "")), \
             mock.patch.object(handler, "rd_get_status", return_value=("downloaded", "")), \
             mock.patch.object(handler, "rd_delete_torrent", side_effect=lambda value, _token: deleted.append(value) or ""), \
             mock.patch.object(handler, "rd_add_and_select", return_value=("new-rd", "")) as add:
            handler.recheck_realdebrid_watched_posts()

        add.assert_called_once_with(self.new_magnet, "token")
        self.assertCountEqual(deleted, ["old-a", "old-b"])
        self.assertEqual(len(deleted), 2)
        self.assertFalse(db.rows["a" * 40]["active"])
        self.assertFalse(db.rows["b" * 40]["active"])
        self.assertTrue(db.rows[self.new_hash]["active"])

    def test_losing_new_id_is_deleted_and_canonical_is_retained(self):
        db = _FakeDB()

        def add(_magnet, _token):
            db.rows[self.new_hash] = {"info_hash": self.new_hash, "rd_torrent_id": "canonical", "active": True}
            return "loser", ""

        with mock.patch.object(handler, "get_database", return_value=db), \
             mock.patch.object(handler, "rd_add_and_select", side_effect=add), \
             mock.patch.object(handler, "rd_delete_torrent", return_value="") as delete:
            submitted = handler._submit_to_realdebrid(
                "source", self.post, self.new_magnet, self.new_hash, "Race", "token"
            )

        self.assertTrue(submitted)
        self.assertEqual(db.rows[self.new_hash]["rd_torrent_id"], "canonical")
        delete.assert_called_once_with("loser", "token")


class CanonicalTorrentIdStorageTests(unittest.TestCase):
    class DB(MagnetarrMixin):
        def __init__(self):
            import sqlite3
            self.conn = sqlite3.connect(":memory:")
            self.conn.execute("""CREATE TABLE magnetarr_realdebrid_submissions (
                info_hash TEXT PRIMARY KEY, magnet_uri TEXT, title TEXT, permalink TEXT,
                source_id TEXT, rd_torrent_id TEXT, content_hash TEXT, status TEXT,
                last_error TEXT, last_checked_at TEXT, active INTEGER)""")

        def get_connection(self):
            return self.conn

        @contextmanager
        def _use_row_factory(self, conn):
            old = conn.row_factory
            import sqlite3
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.row_factory = old

    def test_upsert_preserves_canonical_unless_recovery_explicitly_replaces(self):
        db = self.DB()
        self.addCleanup(db.conn.close)
        base = {"info_hash": "d" * 40, "rd_torrent_id": "canonical"}
        db.upsert_realdebrid_submission(base)
        db.upsert_realdebrid_submission(dict(base, rd_torrent_id="loser"))
        self.assertEqual(db.get_realdebrid_submission(base["info_hash"])["rd_torrent_id"], "canonical")
        db.upsert_realdebrid_submission(
            dict(base, rd_torrent_id="recovered", replace_rd_torrent_id=True)
        )
        self.assertEqual(db.get_realdebrid_submission(base["info_hash"])["rd_torrent_id"], "recovered")


if __name__ == "__main__":
    unittest.main()

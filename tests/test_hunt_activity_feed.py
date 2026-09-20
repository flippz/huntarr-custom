"""Coverage for the durable Home Hunt Activity feed."""

import os
import importlib.util
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HUNTARR_CONFIG_DIR", tempfile.mkdtemp(prefix="huntarr_activity_feed_"))

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(REPO_ROOT))

from src.primary.utils.database import HuntarrDatabase  # noqa: E402


def _make_db(db_path: Path) -> HuntarrDatabase:
    db = HuntarrDatabase.__new__(HuntarrDatabase)
    db._thread_local = threading.local()
    db.db_path = db_path
    db.ensure_database_exists()
    return db


class HuntActivityFeedDatabaseTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "huntarr.db"
        self.db = _make_db(self.db_path)

    def tearDown(self):
        self.db.invalidate_connection()
        self.tmpdir.cleanup()

    def test_records_and_filters_append_only_events(self):
        self.db.record_hunt_activity(
            "sonarr", "living-room", "7_2", "Example - Season 2",
            "missing", "search", "searching", "Searching Sonarr",
        )
        self.db.record_hunt_activity(
            "sonarr", "living-room", "7_2", "Example - Season 2",
            "missing", "search", "no_results", "No matching season pack",
        )
        self.db.record_hunt_activity(
            "radarr", "movies", "12", "Example Movie",
            "missing", "download", "downloaded", "Downloaded/grabbed",
        )

        all_events = self.db.get_hunt_activity(page_size=20)
        self.assertEqual(all_events["total_entries"], 3)
        self.assertEqual(all_events["entries"][0]["app_type"], "radarr")
        self.assertEqual(all_events["entries"][0]["status"], "downloaded")

        sonarr = self.db.get_hunt_activity(app_type="sonarr", page_size=20)
        self.assertEqual(sonarr["total_entries"], 2)
        self.assertTrue(all(item["app_type"] == "sonarr" for item in sonarr["entries"]))

        failures = self.db.get_hunt_activity(status="failed", page_size=20)
        self.assertEqual(failures["total_entries"], 0)

    def test_history_transitions_create_readable_activity_events(self):
        entry = self.db.add_hunt_history_entry(
            "sonarr", "living-room", "7_2", "Example - Season 2",
            operation_type="missing", status="sent",
        )
        self.assertTrue(self.db.update_hunt_history_status(entry["id"], "grabbed"))

        events = self.db.get_hunt_activity(page_size=20)["entries"]
        self.assertEqual([event["status"] for event in events], ["downloaded", "searching"])
        self.assertEqual(events[0]["type"], "download")
        self.assertEqual(events[1]["detail"], "Search started")

    def test_activity_table_exists_and_has_indexes(self):
        with sqlite3.connect(self.db_path) as conn:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
        self.assertIn("hunt_activity", tables)
        self.assertIn("idx_hunt_activity_occurred_at", indexes)
        self.assertIn("idx_hunt_activity_filters", indexes)


@unittest.skipUnless(importlib.util.find_spec("flask"), "Flask is not installed in the host test environment")
class HuntActivityFeedRouteTest(unittest.TestCase):
    def test_activity_route_passes_filters_to_manager(self):
        from flask import Flask
        from src.primary.routes.history_routes import history_blueprint

        app = Flask(__name__)
        app.register_blueprint(history_blueprint, url_prefix="/api/hunt-manager")
        client = app.test_client()

        expected = {
            "entries": [],
            "total_entries": 0,
            "total_pages": 1,
            "current_page": 1,
            "page_size": 20,
        }
        with mock.patch("src.primary.routes.history_routes.get_activity", return_value=expected) as get_activity:
            response = client.get("/api/hunt-manager/activity?status=failed&type=search&app=sonarr&since=123")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["filters"]["status"], "failed")
        get_activity.assert_called_once_with(
            status="failed",
            activity_type="search",
            app_type="sonarr",
            instance_name=None,
            since="123",
            page=1,
            page_size=20,
        )


if __name__ == "__main__":
    unittest.main()

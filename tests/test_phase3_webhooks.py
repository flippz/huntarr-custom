"""Focused Phase 3 webhook authentication, validation, lifecycle, and wake tests."""

import json
import sqlite3
import threading
import unittest
from unittest import mock

from src.primary.apps._common.pipeline_state import PipelineState
from src.primary.apps._common import wake_registry
from src.primary.apps._common.starr_webhook import (
    WebhookError, extract_secret, handle_event, parse_event,
)


class _Connection:
    def __init__(self, raw):
        self.raw = raw
        self.lock = threading.RLock()

    def __enter__(self):
        self.lock.acquire()
        self.raw.__enter__()
        return self.raw

    def __exit__(self, *args):
        try:
            return self.raw.__exit__(*args)
        finally:
            self.lock.release()


class _DB:
    def __init__(self):
        raw = sqlite3.connect(":memory:", check_same_thread=False)
        raw.executescript("""
            CREATE TABLE pipeline_items (
                id INTEGER PRIMARY KEY, app_type TEXT, instance_name TEXT, item_key TEXT,
                state TEXT, command_id TEXT, metadata TEXT, cooldown_until_epoch INTEGER DEFAULT 0,
                updated_at_epoch INTEGER, updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(app_type, instance_name, item_key));
            CREATE TABLE pipeline_item_events (
                id INTEGER PRIMARY KEY, app_type TEXT, instance_name TEXT, item_key TEXT,
                state TEXT, command_id TEXT, metadata TEXT, occurred_at_epoch INTEGER,
                occurred_at TEXT DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE starr_webhook_receipts (
                event_id TEXT PRIMARY KEY, app_type TEXT, instance_name TEXT,
                received_at_epoch INTEGER, received_at TEXT DEFAULT CURRENT_TIMESTAMP);
        """)
        self.connection = _Connection(raw)

    def get_connection(self):
        return self.connection

    def close(self):
        self.connection.raw.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class WebhookValidationTests(unittest.TestCase):
    def setUp(self):
        self.secret = "s" * 32
        self.pipeline = PipelineState(db=_DB(), clock=lambda: 1000)
        self.wake = mock.Mock()

    def call(self, payload, secret=None, content_type="application/json"):
        raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        return handle_event("sonarr", "sonarr-main", self.secret,
                            self.secret if secret is None else secret,
                            raw, content_type, self.pipeline, self.wake)

    def assert_webhook_error(self, status, *args, **kwargs):
        with self.assertRaises(WebhookError) as caught:
            self.call(*args, **kwargs)
        self.assertEqual(caught.exception.status, status)

    def test_authentication_required_and_header_bearer_basic_shapes_supported(self):
        self.assert_webhook_error(401, {"eventType": "Test"}, secret="")
        self.assert_webhook_error(401, {"eventType": "Test"}, secret="wrong")
        self.assertTrue(self.call({"eventType": "Test"})["accepted"])
        self.assertEqual(extract_secret({"X-Huntarr-Webhook-Secret": self.secret}), self.secret)
        self.assertEqual(extract_secret({"Authorization": "Bearer " + self.secret}), self.secret)
        self.assertEqual(extract_secret({}, self.secret), self.secret)

    def test_rejects_malformed_unrecognized_wrong_content_type_and_oversized(self):
        self.assert_webhook_error(400, b"{")
        self.assert_webhook_error(422, {"eventType": "HealthIssue"})
        self.assert_webhook_error(415, {"eventType": "Test"}, content_type="text/plain")
        self.assert_webhook_error(413, b"x" * (256 * 1024 + 1))

    def test_duplicate_grab_is_idempotent_and_only_first_delivery_wakes(self):
        self.pipeline.claim_candidate("sonarr", "sonarr-main", "episodes:8", cooldown_seconds=0)
        self.pipeline.transition("sonarr", "sonarr-main", "episodes:8", "search_submitted")
        payload = {"eventType": "Grab", "series": {"id": 3},
                   "episodes": [{"id": 8, "seasonNumber": 1}], "downloadId": "abc"}
        first = self.call(payload)
        second = self.call(payload)
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["transitioned"], 1)
        self.assertEqual(self.wake.call_count, 1)
        with self.pipeline.db.get_connection() as conn:
            state = conn.execute("SELECT state FROM pipeline_items WHERE item_key='episodes:8'").fetchone()[0]
        self.assertEqual(state, "grabbed")


class WakeRegistryTests(unittest.TestCase):
    def tearDown(self):
        wake_registry.reset()

    def test_webhook_wake_interrupts_app_wait_and_is_consumed_once(self):
        with mock.patch.object(wake_registry, "_mark_due") as due, \
             mock.patch("src.primary.apps._common.queue_dispatch.invalidate_dispatch_observation") as invalidate:
            wake_registry.request_wake("sonarr", "sonarr-main")
            self.assertTrue(wake_registry.wait("sonarr", 0))
            self.assertEqual(wake_registry.consume_wakes("sonarr"), {"sonarr-main"})
            self.assertFalse(wake_registry.is_wake_pending("sonarr"))
        invalidate.assert_called_once_with("sonarr", "sonarr-main")
        self.assertGreaterEqual(due.call_count, 1)


class LifecycleCorrelationTests(unittest.TestCase):
    def setUp(self):
        self.state = PipelineState(db=_DB(), clock=lambda: 2000)

    def test_sonarr_download_correlates_episode_season_and_series_without_reopening_terminal(self):
        keys = ["episodes:9", "season:4:2", "series:4"]
        self.assertTrue(self.state.claim_candidates("sonarr", "s", keys, cooldown_seconds=0))
        event = {"eventType": "Download", "series": {"id": 4},
                 "episodes": [{"id": 9, "seasonNumber": 2}]}
        _, state, cooldown, parsed = parse_event("sonarr", event)
        self.assertEqual(set(parsed), set(keys))
        result = self.state.apply_webhook_event("sonarr", "s", "one", parsed, state,
                                                cooldown_seconds=cooldown)
        self.assertEqual(result["transitioned"], 3)
        late = self.state.apply_webhook_event("sonarr", "s", "two", parsed, "failed", cooldown_seconds=300)
        self.assertEqual(late["transitioned"], 0)
        with self.state.db.get_connection() as conn:
            states = {row[0] for row in conn.execute("SELECT state FROM pipeline_items")}
        self.assertEqual(states, {"completed"})

    def test_radarr_failure_correlates_movie(self):
        self.state.claim_candidate("radarr", "r", "movies:22", cooldown_seconds=0)
        event = {"eventType": "DownloadFailed", "movie": {"id": 22}}
        _, state, cooldown, keys = parse_event("radarr", event)
        self.assertEqual((state, keys), ("failed", ["movies:22"]))
        result = self.state.apply_webhook_event("radarr", "r", "r-one", keys, state,
                                                cooldown_seconds=cooldown)
        self.assertEqual(result["transitioned"], 1)


if __name__ == "__main__":
    unittest.main()

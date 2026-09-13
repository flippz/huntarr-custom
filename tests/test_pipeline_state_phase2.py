"""Focused Phase 2 tests for shared queue caching and durable item lifecycle."""

import sqlite3
import threading
import unittest

from src.primary.apps._common.pipeline_state import PipelineState


class _Connection:
    def __init__(self, connection):
        self.connection = connection
        self.lock = threading.RLock()

    def __enter__(self):
        self.lock.acquire()
        self.connection.__enter__()
        return self.connection

    def __exit__(self, *args):
        try:
            return self.connection.__exit__(*args)
        finally:
            self.lock.release()


class _DB:
    def __init__(self):
        raw = sqlite3.connect(":memory:", check_same_thread=False)
        raw.execute("""
            CREATE TABLE pipeline_items (
                id INTEGER PRIMARY KEY, app_type TEXT NOT NULL,
                instance_name TEXT NOT NULL, item_key TEXT NOT NULL,
                state TEXT NOT NULL, command_id TEXT, metadata TEXT,
                cooldown_until_epoch INTEGER DEFAULT 0,
                updated_at_epoch INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(app_type, instance_name, item_key)
            )
        """)
        raw.execute("""
            CREATE TABLE pipeline_item_events (
                id INTEGER PRIMARY KEY, app_type TEXT NOT NULL,
                instance_name TEXT NOT NULL, item_key TEXT NOT NULL,
                state TEXT NOT NULL, command_id TEXT, metadata TEXT,
                occurred_at_epoch INTEGER NOT NULL,
                occurred_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.wrapper = _Connection(raw)

    def get_connection(self):
        return self.wrapper


class PipelineLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.now = 1000
        self.state = PipelineState(db=_DB(), clock=lambda: self.now)

    def test_unresolved_claim_is_atomic_and_blocks_duplicate(self):
        self.assertTrue(self.state.claim_candidate("sonarr", "one", "season:1:2"))
        self.assertFalse(self.state.claim_candidate("sonarr", "one", "season:1:2"))
        self.assertTrue(self.state.transition(
            "sonarr", "one", "season:1:2", "search_submitted", command_id="42"
        ))
        self.assertFalse(self.state.claim_candidate("sonarr", "one", "season:1:2"))

    def test_terminal_cooldown_survives_restart_then_allows_retry(self):
        db = self.state.db
        self.assertTrue(self.state.claim_candidate("radarr", "one", "movies:9", cooldown_seconds=30))
        self.state.transition("radarr", "one", "movies:9", "no_grab", cooldown_seconds=30)
        restarted = PipelineState(db=db, clock=lambda: self.now)
        self.assertFalse(restarted.claim_candidate("radarr", "one", "movies:9"))
        self.now += 31
        self.assertTrue(restarted.claim_candidate("radarr", "one", "movies:9"))

    def test_command_transition_updates_serialized_row(self):
        self.state.claim_candidate("sonarr", "one", "episodes:3")
        self.state.transition("sonarr", "one", "episodes:3", "search_submitted", command_id="7")
        self.assertTrue(self.state.transition_command("sonarr", "7", "command_complete"))
        with self.state.db.get_connection() as conn:
            row = conn.execute("SELECT state FROM pipeline_items WHERE command_id='7'").fetchone()
        self.assertEqual(row[0], "command_complete")


class QueueObservationTests(unittest.TestCase):
    def test_healthy_cache_is_single_flight_and_busy_refreshes_at_five_seconds(self):
        wall = [1000.0]
        mono = [10.0]
        calls = []
        state = PipelineState(db=_DB(), clock=lambda: wall[0], monotonic=lambda: mono[0])

        def fetch():
            calls.append(1)
            return {"queue": 2, "active": 1}

        self.assertEqual(state.observe_queue("sonarr", "one", fetch)["queue"], 2)
        self.assertEqual(state.observe_queue("sonarr", "one", fetch)["queue"], 2)
        self.assertEqual(len(calls), 1)
        mono[0] += 5.1
        state.observe_queue("sonarr", "one", fetch)
        self.assertEqual(len(calls), 2)

    def test_failure_retains_last_good_value_and_uses_unhealthy_backoff(self):
        mono = [0.0]
        state = PipelineState(db=_DB(), monotonic=lambda: mono[0])
        self.assertEqual(state.observe_queue("radarr", "one", lambda: []), [])
        mono[0] += 31
        self.assertEqual(state.observe_queue("radarr", "one", lambda: (_ for _ in ()).throw(RuntimeError("down"))), [])
        # Failure backoff is at least 60 seconds; no second failing fetch occurs here.
        calls = []
        mono[0] += 59
        self.assertEqual(state.observe_queue("radarr", "one", lambda: calls.append(1)), [])
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()

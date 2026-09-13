"""Focused Phase 2 tests for shared queue caching and durable item lifecycle."""

import os
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

os.environ.setdefault("HUNTARR_CONFIG_DIR", tempfile.mkdtemp(prefix="huntarr_phase2_import_"))

from src.primary.apps._common import queue_dispatch
from src.primary.apps._common.pipeline_state import PipelineState
from src.primary.apps.swaparr import handler as swaparr_handler


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

    def close(self):
        self.wrapper.connection.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


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

    def test_batch_claim_and_command_transition_are_per_media_item(self):
        keys = ["movies:1", "movies:2", "movies:3"]
        self.assertTrue(self.state.claim_candidates("radarr", "one", keys))
        self.assertTrue(self.state.transition_items(
            "radarr", "one", keys, "search_submitted", command_id="88",
        ))
        self.assertTrue(self.state.transition_command("radarr", "88", "command_complete"))
        with self.state.db.get_connection() as conn:
            rows = conn.execute(
                "SELECT item_key,state FROM pipeline_items ORDER BY item_key"
            ).fetchall()
        self.assertEqual(rows, [(key, "command_complete") for key in keys])

    def test_blocked_batch_claim_writes_no_partial_rows(self):
        self.assertTrue(self.state.claim_candidate("sonarr", "one", "episodes:2"))
        self.assertFalse(self.state.claim_candidates(
            "sonarr", "one", ["episodes:1", "episodes:2", "episodes:3"],
        ))
        with self.state.db.get_connection() as conn:
            keys = [row[0] for row in conn.execute(
                "SELECT item_key FROM pipeline_items ORDER BY item_key"
            ).fetchall()]
        self.assertEqual(keys, ["episodes:2"])


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

    def test_full_snapshot_is_payload_compatible_in_both_consumer_orders(self):
        for producer_first in ("dispatch", "swaparr"):
            with self.subTest(producer_first=producer_first):
                calls = []
                state = PipelineState(db=_DB(), monotonic=lambda: 10.0)
                records = [{"id": 1}, {"id": 2}]

                if producer_first == "dispatch":
                    first = state.observe_queue(
                        "sonarr", "one",
                        lambda: calls.append("dispatch") or {
                            "records": records, "queue": 2, "active": 1,
                        },
                    )
                    second = state.observe_queue(
                        "sonarr", "one", lambda: calls.append("swaparr") or records,
                        require_records=True,
                    )
                else:
                    first = state.observe_queue(
                        "sonarr", "one", lambda: calls.append("swaparr") or records,
                        require_records=True,
                    )
                    second = state.observe_queue(
                        "sonarr", "one", lambda: calls.append("dispatch") or {
                            "records": records, "queue": 2, "active": 1,
                        },
                    )

                self.assertEqual(first["records"], records)
                self.assertEqual(second["records"], records)
                self.assertEqual(second["queue"], 2)
                self.assertEqual(len(calls), 1)

    def test_summary_only_cache_is_not_returned_to_records_consumer(self):
        state = PipelineState(db=_DB(), monotonic=lambda: 10.0)
        state.observe_queue("radarr", "one", lambda: {"queue": 4, "active": 0})
        calls = []
        snapshot = state.observe_queue(
            "radarr", "one", lambda: calls.append(1) or [{"id": "full"}],
            require_records=True,
        )
        self.assertEqual(calls, [1])
        self.assertEqual(snapshot["records"], [{"id": "full"}])

    def test_healthy_polling_slows_when_stable_and_resets_on_change(self):
        mono = [0.0]
        queue = [{"id": 1}]
        state = PipelineState(db=_DB(), monotonic=lambda: mono[0])
        first = state.observe_queue("sonarr", "one", lambda: list(queue))
        self.assertEqual(first["poll_interval"], 5.0)
        mono[0] += 5.1
        stable = state.observe_queue("sonarr", "one", lambda: list(queue))
        self.assertEqual(stable["poll_interval"], 7.5)
        mono[0] += 7.6
        queue.append({"id": 2})
        changed = state.observe_queue("sonarr", "one", lambda: list(queue))
        self.assertEqual(changed["poll_interval"], 5.0)

        idle = PipelineState(db=_DB(), monotonic=lambda: 0.0)
        self.assertEqual(idle.observe_queue("radarr", "idle", lambda: [])["poll_interval"], 30.0)

    def test_failure_retains_last_good_value_and_uses_unhealthy_backoff(self):
        mono = [0.0]
        state = PipelineState(db=_DB(), monotonic=lambda: mono[0])
        self.assertEqual(state.observe_queue("radarr", "one", lambda: [])["records"], [])
        mono[0] += 31
        failed = state.observe_queue(
            "radarr", "one", lambda: (_ for _ in ()).throw(RuntimeError("down")),
        )
        self.assertEqual(failed["records"], [])
        self.assertFalse(failed["healthy"])
        # Failure backoff is at least 60 seconds; no second failing fetch occurs here.
        calls = []
        mono[0] += 59
        self.assertEqual(state.observe_queue("radarr", "one", lambda: calls.append(1))["records"], [])
        self.assertEqual(calls, [])

    def test_initial_failure_backoff_is_shared_across_consumer_payloads(self):
        mono = [0.0]
        calls = []
        state = PipelineState(db=_DB(), monotonic=lambda: mono[0])
        first = state.observe_queue(
            "sonarr", "one",
            lambda: calls.append("dispatch") or (_ for _ in ()).throw(RuntimeError("down")),
        )
        second = state.observe_queue(
            "sonarr", "one", lambda: calls.append("swaparr") or [],
            require_records=True,
        )
        self.assertFalse(first["healthy"])
        self.assertFalse(second["healthy"])
        self.assertEqual(calls, ["dispatch"])


class BatchDispatchAndSwaparrTests(unittest.TestCase):
    def tearDown(self):
        queue_dispatch.clear_dispatch()

    def test_dispatch_transitions_every_key_in_batch(self):
        pipeline = mock.Mock()
        pipeline.claim_candidates.return_value = True
        queue_dispatch.configure_dispatch(
            "radarr", "one", {}, lambda: 0, lambda: 0, lambda: False,
            mock.Mock(),
        )
        with mock.patch.object(queue_dispatch, "get_pipeline_state", return_value=pipeline):
            self.assertTrue(queue_dispatch.claim_search(["movies:3", "movies:1"]))
            queue_dispatch.finish_search_claim("search_submitted", command_id=9)
        pipeline.claim_candidates.assert_called_once()
        pipeline.transition_items.assert_called_once_with(
            "radarr", "one", ["movies:3", "movies:1"], "search_submitted",
            command_id=9, cooldown_seconds=None,
        )

    def test_dispatch_full_poll_is_reused_by_swaparr_without_queue_refetch(self):
        pipeline = PipelineState(db=_DB(), monotonic=lambda: 10.0)
        queue_calls = []
        records = [{"id": 1, "media_id": 7}]
        with mock.patch.object(queue_dispatch, "get_pipeline_state", return_value=pipeline):
            context = queue_dispatch.configure_dispatch(
                "radarr", "worker-id", {}, lambda: -1, lambda: 0, lambda: False,
                mock.Mock(), queue_cache_name="Display Name",
                queue_items=lambda: queue_calls.append(1) or records,
            )
            self.assertEqual(queue_dispatch._occupancy(context, 10.0)[:3], (1, 1, 0))
            snapshot = pipeline.observe_queue(
                "radarr", "Display Name",
                lambda: queue_calls.append(2) or records,
                require_records=True,
            )
        self.assertEqual(snapshot["records"], records)
        self.assertEqual(queue_calls, [1])

    def test_swaparr_uses_episode_key_and_does_not_transition_failed_claim(self):
        pipeline = mock.Mock()
        pipeline.claim_candidate.return_value = False
        claimed, key = swaparr_handler.record_queue_lifecycle(
            pipeline, "sonarr", "instance-id",
            {"id": 10, "download_id": "download", "media_id": 4,
             "season_number": 2, "episode_id": 99},
        )
        self.assertFalse(claimed)
        self.assertEqual(key, "episodes:99")
        pipeline.transition.assert_not_called()

    def test_swaparr_failed_claim_preserves_unresolved_database_row(self):
        pipeline = PipelineState(db=_DB(), clock=lambda: 1000)
        self.assertTrue(pipeline.claim_candidate("sonarr", "one", "episodes:99"))
        self.assertTrue(pipeline.transition(
            "sonarr", "one", "episodes:99", "search_submitted", command_id="12",
        ))
        claimed, _key = swaparr_handler.record_queue_lifecycle(
            pipeline, "sonarr", "one",
            {"id": 10, "media_id": 4, "season_number": 2, "episode_id": 99},
        )
        self.assertFalse(claimed)
        with pipeline.db.get_connection() as conn:
            state = conn.execute(
                "SELECT state FROM pipeline_items WHERE item_key='episodes:99'"
            ).fetchone()[0]
        self.assertEqual(state, "search_submitted")


if __name__ == "__main__":
    unittest.main()

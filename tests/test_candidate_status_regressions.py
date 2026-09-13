"""Regressions for eligibility-first selection and no-op slot status."""

import os
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

os.environ.setdefault("HUNTARR_CONFIG_DIR", tempfile.mkdtemp(prefix="huntarr_candidate_status_"))

from src.primary.apps._common import queue_dispatch
from src.primary.apps._common.pipeline_state import PipelineState
from src.primary.apps.sonarr import missing as sonarr_missing
from src.primary.apps._common.shared_scheduler import get_shared_scheduler


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
                occurred_at_epoch INTEGER NOT NULL
            )
        """)
        self.wrapper = _Connection(raw)

    def get_connection(self):
        return self.wrapper


class CandidateSelectionRegressionTests(unittest.TestCase):
    def test_pipeline_availability_reports_unresolved_and_cooldown_before_claim(self):
        state = PipelineState(db=_DB(), clock=lambda: 1000)
        state.claim_candidate("sonarr", "stable-id", "episodes:2")
        state.claim_candidate("sonarr", "stable-id", "episodes:3")
        state.transition(
            "sonarr", "stable-id", "episodes:3", "failed", cooldown_seconds=60,
        )
        availability = state.candidate_availability(
            "sonarr", "stable-id", ["episodes:1", "episodes:2", "episodes:3"],
        )
        self.assertEqual(availability["eligible"], ["episodes:1"])
        self.assertEqual(availability["unresolved"], ["episodes:2"])
        self.assertEqual(availability["cooldown"], ["episodes:3"])
        self.assertEqual(availability["next_available_epoch"], 1060)

    def test_processed_candidates_are_filtered_before_final_selection(self):
        episodes = [
            {"id": 1, "seriesId": 7, "seasonNumber": 1, "episodeNumber": 1,
             "title": "processed", "airDateUtc": "2020-01-01T00:00:00Z",
             "monitored": True, "series": {"title": "Show", "monitored": True}},
            {"id": 2, "seriesId": 7, "seasonNumber": 1, "episodeNumber": 2,
             "title": "unresolved", "airDateUtc": "2020-01-02T00:00:00Z",
             "monitored": True, "series": {"title": "Show", "monitored": True}},
            {"id": 3, "seriesId": 7, "seasonNumber": 1, "episodeNumber": 3,
             "title": "eligible", "airDateUtc": "2020-01-03T00:00:00Z",
             "monitored": True, "series": {"title": "Show", "monitored": True}},
        ]
        pipeline = mock.Mock()
        pipeline.candidate_availability.side_effect = lambda _app, _instance, keys: {
            "eligible": [key for key in keys if key == "episodes:3"],
            "unresolved": [key for key in keys if key == "episodes:2"],
            "cooldown": [], "next_available_epoch": 2000,
        }

        def api_selection(*args, candidate_filter, **_kwargs):
            return candidate_filter(episodes)[:args[4]]

        with mock.patch.object(sonarr_missing, "get_processed_ids", return_value={"1"}), \
             mock.patch("src.primary.apps._common.pipeline_state.get_pipeline_state",
                        return_value=pipeline), \
             mock.patch.object(sonarr_missing.sonarr_api, "get_missing_episodes_random_page",
                               side_effect=api_selection), \
             mock.patch.object(sonarr_missing.sonarr_api, "search_episode", return_value=123) as search, \
             mock.patch.object(sonarr_missing, "check_hourly_cap_exceeded", return_value=False), \
             mock.patch.object(sonarr_missing, "add_processed_id", return_value=True), \
             mock.patch.object(sonarr_missing, "log_processed_media", return_value=None), \
             mock.patch.object(sonarr_missing, "increment_media_stat_only"):
            result = sonarr_missing.process_missing_episodes_mode(
                "http://sonarr", "key", "stable-id", 10, True, True, 1, 0,
                0, 0, lambda: False,
            )

        self.assertTrue(result)
        search.assert_called_once()
        self.assertEqual(search.call_args.args[3], [3])

    def test_all_processed_candidates_publish_reset_aware_noop_reason(self):
        episode = {
            "id": 1, "seriesId": 7, "seasonNumber": 1, "episodeNumber": 1,
            "title": "processed", "airDateUtc": "2020-01-01T00:00:00Z",
            "monitored": True, "series": {"title": "Show", "monitored": True},
        }
        pipeline = mock.Mock()
        pipeline.candidate_availability.return_value = {
            "eligible": [], "unresolved": [], "cooldown": [],
            "next_available_epoch": None,
        }

        def api_selection(*_args, candidate_filter, **_kwargs):
            candidate_filter([episode])
            return []

        with mock.patch.object(sonarr_missing, "get_processed_ids", return_value={"1"}), \
             mock.patch("src.primary.apps._common.pipeline_state.get_pipeline_state",
                        return_value=pipeline), \
             mock.patch.object(sonarr_missing.sonarr_api, "get_missing_episodes_random_page",
                               side_effect=api_selection), \
             mock.patch.object(sonarr_missing, "get_state_management_summary", return_value={
                 "next_reset_time": "2026-09-13 20:09:00",
             }), \
             mock.patch.object(queue_dispatch, "publish_noop") as publish, \
             mock.patch.object(sonarr_missing.sonarr_api, "search_episode") as search:
            result = sonarr_missing.process_missing_episodes_mode(
                "http://sonarr", "key", "stable-id", 10, True, True, 1, 0,
                0, 0, lambda: False,
            )

        self.assertFalse(result)
        search.assert_not_called()
        reason = publish.call_args.args[0]
        self.assertIn("1 state-managed processed", reason)
        self.assertIn("state reset 2026-09-13 20:09:00", reason)


class SharedObservationNoopTests(unittest.TestCase):
    def tearDown(self):
        queue_dispatch.clear_dispatch()
        get_shared_scheduler().reset()

    def test_noop_reuses_display_name_observation_for_stable_instance_status(self):
        wall = [1000.0]
        mono = [10.0]
        pipeline = PipelineState(
            db=_DB(), clock=lambda: wall[0], monotonic=lambda: mono[0],
        )
        pipeline.observe_queue("sonarr", "Living Room", lambda: [])
        logger = mock.Mock()
        with mock.patch.object(queue_dispatch, "get_pipeline_state", return_value=pipeline):
            queue_dispatch.configure_dispatch(
                "sonarr", "stable-id", {"target_queue_depth": 3},
                queue_size=lambda: (_ for _ in ()).throw(AssertionError("queue refetched")),
                active_searches=lambda: 0, queue_items=None,
                stop_check=lambda: False, logger=logger,
                queue_cache_name="Living Room",
            )
            queue_dispatch.publish_noop(
                "no eligible missing episodes: 316 state-managed processed; state reset 20:09"
            )

        runtime = pipeline.runtime("sonarr", "stable-id")
        self.assertEqual(runtime["slots_used"], 0)
        self.assertEqual(runtime["slots_target"], 3)
        self.assertEqual(runtime["slots_free"], 3)
        self.assertIn("316 state-managed processed", runtime["pause_reason"])
        self.assertEqual(pipeline.runtime("sonarr", "Living Room"), {})

    def test_unhealthy_shared_observation_does_not_fabricate_zero_slots(self):
        wall = [1000.0]
        mono = [0.0]
        pipeline = PipelineState(
            db=_DB(), clock=lambda: wall[0], monotonic=lambda: mono[0],
        )
        pipeline.observe_queue("sonarr", "Display", lambda: [])
        mono[0] = 31.0
        wall[0] = 1031.0
        pipeline.observe_queue(
            "sonarr", "Display",
            lambda: (_ for _ in ()).throw(RuntimeError("offline")), force=True,
        )
        with mock.patch.object(queue_dispatch, "get_pipeline_state", return_value=pipeline):
            queue_dispatch.configure_dispatch(
                "sonarr", "stable-id", {"target_queue_depth": 3},
                queue_size=lambda: -1, active_searches=lambda: -1,
                stop_check=lambda: False, logger=mock.Mock(),
                queue_cache_name="Display",
            )
            queue_dispatch.publish_noop("no eligible candidates")

        runtime = pipeline.runtime("sonarr", "stable-id")
        self.assertIsNone(runtime["slots_used"])
        self.assertIsNone(runtime["slots_free"])
        self.assertEqual(runtime["pause_reason"], "no eligible candidates")


if __name__ == "__main__":
    unittest.main()

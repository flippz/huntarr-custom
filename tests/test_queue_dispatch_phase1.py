"""Focused Phase 1 tests for queue dispatch and search-cap accounting."""

import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HUNTARR_CONFIG_DIR", tempfile.mkdtemp(prefix="huntarr_phase1_import_"))
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.primary.apps._common import queue_dispatch  # noqa: E402
from src.primary.apps.sonarr import api as sonarr_api  # noqa: E402
from src.primary.apps.radarr import api as radarr_api  # noqa: E402
from src.primary.apps.sonarr import upgrade as sonarr_upgrade  # noqa: E402
from src.primary.default_settings import get_default_instance_config  # noqa: E402
from src.primary import settings_manager  # noqa: E402


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass


class _Response:
    content = b'{"id": 123}'
    text = '{"id": 123}'
    status_code = 201

    def raise_for_status(self):
        return None

    def json(self):
        return {"id": 123}


class QueueDispatchTests(unittest.TestCase):
    def tearDown(self):
        queue_dispatch.clear_dispatch()

    def test_occupancy_combines_queue_active_and_only_unseen_recent(self):
        context = queue_dispatch.DispatchContext(
            "sonarr", "one", 10, -1, 15, 0, lambda: False,
            lambda: 2, lambda: 1, _Logger(), recent_submissions=[95.0, 96.0],
        )
        occupancy, queue, active, unseen = queue_dispatch._occupancy(context, 100.0)
        self.assertEqual((occupancy, queue, active, unseen), (4, 2, 1, 1))

    def test_unknown_status_denies_when_hard_ceiling_enabled(self):
        queue_dispatch.configure_dispatch(
            "radarr", "one",
            {"target_queue_depth": 3, "max_download_queue_size": 5,
             "minimum_dispatch_interval_seconds": 15,
             "queue_redispatch_wait_seconds": 60},
            queue_size=lambda: -1, active_searches=lambda: 0,
            stop_check=lambda: False, logger=_Logger(),
        )
        self.assertFalse(queue_dispatch.acquire_dispatch_slot())

    def test_target_full_with_zero_wait_defers_without_sleeping(self):
        queue_dispatch.configure_dispatch(
            "sonarr", "one",
            {"target_queue_depth": 3, "max_download_queue_size": -1,
             "minimum_dispatch_interval_seconds": 15,
             "queue_redispatch_wait_seconds": 0},
            queue_size=lambda: 2, active_searches=lambda: 1,
            stop_check=lambda: False, logger=_Logger(),
        )
        with mock.patch.object(queue_dispatch.time, "sleep") as sleep:
            self.assertFalse(queue_dispatch.acquire_dispatch_slot())
        sleep.assert_not_called()


class DefaultsAndPlumbingTests(unittest.TestCase):
    def test_defaults_keep_dedup_memory_separate_and_force_off(self):
        sonarr = get_default_instance_config("sonarr")
        self.assertEqual(sonarr["state_management_hours"], 24)
        self.assertEqual(sonarr["target_queue_depth"], 3)
        self.assertEqual(sonarr["minimum_dispatch_interval_seconds"], 15)
        self.assertEqual(sonarr["queue_redispatch_wait_seconds"], 60)
        self.assertFalse(sonarr["force_season_replacement"])
        self.assertFalse(sonarr["webhook_enabled"])
        self.assertEqual(sonarr["webhook_secret"], "")
        self.assertFalse(sonarr["decypharr_capacity_enabled"])
        self.assertEqual(sonarr["shared_capacity_weight"], 1)

    def test_settings_validation_clamps_queue_controls_and_force_boolean(self):
        class FakeDB:
            saved = None

            def migrate_instance_identifier(self, *args):
                return True

            def get_app_config(self, _app):
                return {}

            def save_app_config(self, _app, data):
                self.saved = data

        db = FakeDB()
        payload = {"instances": [{
            "instance_id": "sonarr-test", "enabled": True,
            "target_queue_depth": 0,
            "minimum_dispatch_interval_seconds": "bad",
            "queue_redispatch_wait_seconds": 99999,
            "max_download_queue_size": -50,
            "state_management_hours": 0,
            "shared_capacity_weight": 999,
            "decypharr_max_active_jobs": -5,
            "webhook_enabled": True,
            "webhook_secret": "short",
            "force_season_replacement": "true",
        }]}
        with mock.patch.object(settings_manager, "get_database", return_value=db):
            self.assertTrue(settings_manager.save_settings("sonarr", payload))
        saved = db.saved["instances"][0]
        self.assertEqual(saved["target_queue_depth"], 1)
        self.assertEqual(saved["minimum_dispatch_interval_seconds"], 15)
        self.assertEqual(saved["queue_redispatch_wait_seconds"], 3600)
        self.assertEqual(saved["max_download_queue_size"], -1)
        self.assertEqual(saved["state_management_hours"], 1)
        self.assertEqual(saved["shared_capacity_weight"], 100)
        self.assertEqual(saved["decypharr_max_active_jobs"], 0)
        self.assertTrue(saved["webhook_enabled"])
        self.assertGreaterEqual(len(saved["webhook_secret"]), 24)
        self.assertFalse(saved["force_season_replacement"])

    def test_force_option_is_forwarded_only_to_season_upgrade_mode(self):
        with mock.patch.object(sonarr_upgrade, "process_upgrade_seasons_mode", return_value=True) as process, \
             mock.patch("src.primary.apps.sonarr.season_recovery.recover_pending", return_value=True):
            result = sonarr_upgrade.process_cutoff_upgrades(
                "http://sonarr", "key", "instance", hunt_upgrade_items=1,
                upgrade_mode="seasons_packs", force_season_replacement=True,
            )
        self.assertTrue(result)
        self.assertTrue(process.call_args.kwargs["force_season_replacement"])


class DispatchOwnershipRegressionTests(unittest.TestCase):
    def tearDown(self):
        queue_dispatch.clear_dispatch()

    def _forced_season(self, prepare_result, mark_result=True):
        episode = {
            "id": 9, "seriesId": 7, "seasonNumber": 2,
            "airDateUtc": "2020-01-01T00:00:00Z",
            "series": {"title": "Show"},
        }
        patches = [
            mock.patch.object(sonarr_api, "get_cutoff_unmet_episodes_random_page", return_value=[episode]),
            mock.patch.object(sonarr_upgrade, "is_processed", return_value=False),
            mock.patch.object(sonarr_upgrade, "check_hourly_cap_exceeded", return_value=False),
            mock.patch.object(queue_dispatch, "acquire_dispatch_slot", return_value=True),
            mock.patch.object(queue_dispatch, "cancel_dispatch_slot"),
            mock.patch("src.primary.apps.sonarr.season_recovery.prepare_exact_season", return_value=prepare_result),
            mock.patch("src.primary.apps.sonarr.season_recovery.mark_search_started", return_value=mark_result),
            mock.patch("src.primary.apps.sonarr.season_recovery.recover_pending", return_value=True),
            mock.patch.object(sonarr_api, "search_season"),
        ]
        entered = [patch.start() for patch in patches]
        try:
            result = sonarr_upgrade.process_upgrade_seasons_mode(
                "http://sonarr", "key", "instance", 10, True, 1, 1, 2,
                lambda: False, force_season_replacement=True,
            )
            return result, entered[4], entered[8]
        finally:
            for patch in reversed(patches):
                patch.stop()

    def test_forced_season_prepare_failure_releases_preacquired_grant(self):
        result, cancel, search = self._forced_season(None)
        self.assertFalse(result)
        cancel.assert_called_once_with()
        search.assert_not_called()

    def test_forced_season_journal_arm_failure_releases_preacquired_grant(self):
        result, cancel, search = self._forced_season("journal", mark_result=False)
        self.assertFalse(result)
        cancel.assert_called_once_with()
        search.assert_not_called()

    def test_terminalization_between_claim_and_post_prevents_post(self):
        with mock.patch.object(queue_dispatch, "claim_search", return_value=True), \
             mock.patch.object(queue_dispatch, "acquire_dispatch_slot", return_value=True), \
             mock.patch.object(queue_dispatch, "begin_search_submission", return_value=False), \
             mock.patch.object(queue_dispatch, "cancel_dispatch_slot") as cancel, \
             mock.patch.object(sonarr_api.requests, "post") as post:
            result = sonarr_api.search_episode(
                "http://sonarr", "key", 10, [9], instance_name="instance",
            )
        self.assertIsNone(result)
        post.assert_not_called()
        cancel.assert_called_once_with()


class SearchCapTests(unittest.TestCase):
    def tearDown(self):
        queue_dispatch.clear_dispatch()

    @mock.patch("src.primary.stats_manager.increment_hourly_cap")
    @mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded", return_value=False)
    @mock.patch.object(sonarr_api.requests, "post", return_value=_Response())
    def test_sonarr_season_pack_consumes_exactly_one_slot(self, _post, _check, increment):
        command_id = sonarr_api.search_season(
            "http://sonarr", "key", 10, 7, 2, instance_name="instance-1"
        )
        self.assertEqual(command_id, 123)
        increment.assert_called_once_with("sonarr", 1, instance_name="instance-1")

    @mock.patch("src.primary.stats_manager.increment_hourly_cap")
    @mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded", return_value=False)
    @mock.patch.object(radarr_api, "arr_request", return_value={"id": 456})
    def test_radarr_movie_search_consumes_exactly_one_slot(self, _request, _check, increment):
        fake_logger = types.ModuleType("src.primary.utils.clean_logger")
        fake_logger.get_instance_name_for_cap = lambda: "instance-2"
        with mock.patch.dict(sys.modules, {"src.primary.utils.clean_logger": fake_logger}):
            command_id = radarr_api.movie_search("http://radarr", "key", 10, [1, 2, 3])
        self.assertEqual(command_id, 456)
        increment.assert_called_once_with("radarr", 1, instance_name="instance-2")

    @mock.patch("src.primary.stats_manager.increment_hourly_cap")
    @mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded", return_value=False)
    @mock.patch.object(sonarr_api.requests, "post", return_value=_Response())
    def test_sonarr_episode_batch_claims_each_episode_key(self, _post, _check, _increment):
        with mock.patch.object(queue_dispatch, "claim_search", return_value=True) as claim:
            self.assertEqual(
                sonarr_api.search_episode("http://sonarr", "key", 10, [8, 3, 8]),
                123,
            )
        claim.assert_called_once_with(["episodes:3", "episodes:8"])

    @mock.patch("src.primary.stats_manager.increment_hourly_cap")
    @mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded", return_value=False)
    @mock.patch.object(radarr_api, "arr_request", return_value={"id": 456})
    def test_radarr_batch_claims_each_movie_key(self, _request, _check, _increment):
        fake_logger = types.ModuleType("src.primary.utils.clean_logger")
        fake_logger.get_instance_name_for_cap = lambda: "instance-2"
        with mock.patch.dict(sys.modules, {"src.primary.utils.clean_logger": fake_logger}), \
             mock.patch.object(queue_dispatch, "claim_search", return_value=True) as claim:
            self.assertEqual(radarr_api.movie_search("http://radarr", "key", 10, [9, 2, 9]), 456)
        claim.assert_called_once_with(["movies:2", "movies:9"])

    def test_command_status_polling_is_explicitly_uncounted(self):
        with mock.patch.object(radarr_api, "arr_request", return_value={"state": "completed"}) as request:
            self.assertTrue(radarr_api.wait_for_command("http://radarr", "key", 10, 4, 0, 1))
        self.assertFalse(request.call_args.kwargs["count_api"])

    def test_active_command_count_accepts_paged_response(self):
        response = {"records": [
            {"name": "SeasonSearch", "status": "started"},
            {"name": "EpisodeSearch", "status": "completed"},
            {"name": "RefreshSeries", "status": "started"},
        ]}
        with mock.patch.object(sonarr_api, "arr_request", return_value=response):
            self.assertEqual(sonarr_api.get_active_search_command_count("u", "k", 1), 1)


if __name__ == "__main__":
    unittest.main()

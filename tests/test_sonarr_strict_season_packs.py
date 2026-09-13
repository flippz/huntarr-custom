"""Regression coverage for strict Sonarr missing-season pack dispatch."""

import os
import tempfile
import threading
import unittest
from unittest import mock

os.environ.setdefault("HUNTARR_CONFIG_DIR", tempfile.mkdtemp(prefix="huntarr_strict_pack_"))

from src.primary.apps._common import queue_dispatch
from src.primary.apps._common.pipeline_state import PipelineState
from src.primary.apps.sonarr import api as sonarr_api
from src.primary.apps.sonarr import missing as sonarr_missing
from tests.test_pipeline_state_phase2 import _DB


class _Response:
    def __init__(self, data=None, status=200, error=None):
        self._data = data
        self.status_code = status
        self.content = b"[]" if data is not None else b""
        self._error = error

    def raise_for_status(self):
        if self._error:
            raise self._error

    def json(self):
        return self._data


def _release(guid, weight, **changes):
    release = {
        "guid": guid,
        "indexerId": 5,
        "title": f"Show.S02.{guid}",
        "fullSeason": True,
        "mappedSeriesId": 7,
        "mappedSeasonNumber": 2,
        "approved": True,
        "downloadAllowed": True,
        "rejected": False,
        "temporarilyRejected": False,
        "rejections": [],
        "releaseWeight": weight,
    }
    release.update(changes)
    return release


class StrictSeasonPackApiTests(unittest.TestCase):
    def tearDown(self):
        queue_dispatch.clear_dispatch()

    def _dispatch_patches(self, claim=True, acquire=True):
        return (
            mock.patch.object(queue_dispatch, "claim_search", return_value=claim),
            mock.patch.object(queue_dispatch, "acquire_dispatch_slot", return_value=acquire),
            mock.patch.object(queue_dispatch, "begin_search_submission", return_value=True),
            mock.patch.object(queue_dispatch, "finish_interactive_search", return_value=True),
            mock.patch.object(queue_dispatch, "finish_search_claim", return_value=True),
            mock.patch.object(queue_dispatch, "cancel_dispatch_slot"),
            mock.patch.object(queue_dispatch, "publish_noop"),
            mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded", return_value=False),
        )

    def test_uses_season_interactive_api_and_grabs_sonarr_ranked_best_pack(self):
        individual = _release("episode", 0, fullSeason=False)
        lower_ranked_pack = _release("second", 8)
        best_pack = _release("best", 2, indexerId=9)
        get = mock.patch.object(
            sonarr_api.requests, "get",
            return_value=_Response([individual, lower_ranked_pack, best_pack]),
        )
        post = mock.patch.object(sonarr_api.requests, "post", return_value=_Response({}))
        patches = self._dispatch_patches() + (get, post)
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [12, 11, 12], "main",
        )

        self.assertEqual(result["guid"], "best")
        entered[0].assert_called_once_with(
            ["season:7:2", "episodes:11", "episodes:12"]
        )
        entered[8].assert_called_once_with(
            "http://sonarr/api/v3/release",
            headers={"X-Api-Key": "secret"},
            params={"seriesId": 7, "seasonNumber": 2},
            timeout=10,
            verify=mock.ANY,
        )
        entered[9].assert_called_once_with(
            "http://sonarr/api/v3/release",
            headers={"X-Api-Key": "secret"},
            json={"guid": "best", "indexerId": 9},
            timeout=10,
            verify=mock.ANY,
        )
        entered[3].assert_called_once_with(
            "grabbed", "sonarr", "main", queue_submission=True,
        )

    def test_rejected_wrong_mapped_and_individual_results_are_never_grabbed(self):
        releases = [
            _release("individual", 0, fullSeason=False),
            _release("rejected", 1, approved=False, rejected=True,
                     rejections=["Quality for existing file is of equal or higher preference"]),
            _release("temporary", 2, approved=False, temporarilyRejected=True,
                     rejections=["Not enough seeders"]),
            _release("wrong-season", 3, mappedSeasonNumber=3),
            _release("wrong-series", 4, mappedSeriesId=8),
            _release("allowed", 9),
        ]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response(releases)),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
        )
        self.assertEqual(result["guid"], "allowed")
        self.assertEqual(entered[9].call_args.kwargs["json"]["guid"], "allowed")

    def test_no_acceptable_pack_has_short_cooldown_and_no_grab(self):
        patches = self._dispatch_patches() + (
            mock.patch.object(
                sonarr_api.requests, "get",
                return_value=_Response([_release("episode", 0, fullSeason=False)]),
            ),
            mock.patch.object(sonarr_api.requests, "post"),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
        )
        self.assertIsNone(result)
        entered[9].assert_not_called()
        entered[6].assert_called_once_with("no acceptable season pack")
        entered[3].assert_called_once_with(
            "no_grab", "sonarr", "main", cooldown_seconds=300,
            queue_submission=False,
        )

    def test_permission_denial_prevents_interactive_search(self):
        patches = self._dispatch_patches(acquire=False) + (
            mock.patch.object(sonarr_api.requests, "get"),
            mock.patch.object(sonarr_api.requests, "post"),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        self.assertIsNone(sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
        ))
        entered[8].assert_not_called()
        entered[9].assert_not_called()
        entered[4].assert_called_once_with("timed_out", cooldown_seconds=60)

    def test_failed_grab_marks_failure_and_unwinds_queue_reservation(self):
        error = sonarr_api.requests.exceptions.HTTPError("grab failed")
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get",
                              return_value=_Response([_release("pack", 0)])),
            mock.patch.object(sonarr_api.requests, "post",
                              return_value=_Response({}, status=500, error=error)),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        self.assertIsNone(sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
        ))
        entered[3].assert_called_once_with(
            "failed", "sonarr", "main", cooldown_seconds=300,
            queue_submission=False,
        )

    def test_duplicate_claim_prevents_all_sonarr_requests(self):
        patches = self._dispatch_patches(claim=False) + (
            mock.patch.object(sonarr_api.requests, "get"),
            mock.patch.object(sonarr_api.requests, "post"),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        self.assertIsNone(sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
        ))
        entered[1].assert_not_called()
        entered[8].assert_not_called()
        entered[9].assert_not_called()

    def test_search_plus_grab_consumes_one_hourly_search_slot(self):
        queue_dispatch.clear_dispatch()
        with mock.patch.object(
                sonarr_api.requests, "get",
                return_value=_Response([_release("pack", 0)])), \
             mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})), \
             mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded",
                        return_value=False), \
             mock.patch("src.primary.stats_manager.increment_hourly_cap") as increment:
            result = sonarr_api.grab_best_season_pack(
                "http://sonarr", "secret", 10, 7, 2, [11], "main",
            )
        self.assertEqual(result["guid"], "pack")
        increment.assert_called_once_with("sonarr", 1, instance_name="main")

    def test_concurrent_duplicate_claim_allows_only_one_interactive_search(self):
        state = PipelineState(db=_DB())
        barrier = threading.Barrier(3)
        results = []

        def atomic_claim(keys):
            return state.claim_candidates("sonarr", "main", keys)

        def worker():
            barrier.wait()
            results.append(sonarr_api.grab_best_season_pack(
                "http://sonarr", "secret", 10, 7, 2, [11], "main",
            ))

        with mock.patch.object(queue_dispatch, "claim_search", side_effect=atomic_claim), \
             mock.patch.object(queue_dispatch, "acquire_dispatch_slot", return_value=True), \
             mock.patch.object(queue_dispatch, "begin_search_submission", return_value=True), \
             mock.patch.object(queue_dispatch, "finish_interactive_search", return_value=True), \
             mock.patch.object(queue_dispatch, "finish_search_claim", return_value=True), \
             mock.patch.object(queue_dispatch, "cancel_dispatch_slot"), \
             mock.patch.object(queue_dispatch, "publish_noop"), \
             mock.patch.object(sonarr_api.requests, "get", return_value=_Response([])) as get, \
             mock.patch.object(sonarr_api.requests, "post") as post, \
             mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded",
                        return_value=False):
            threads = [threading.Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            barrier.wait()
            for thread in threads:
                thread.join()

        self.assertEqual(results, [None, None])
        get.assert_called_once()
        post.assert_not_called()


class InteractiveDispatchAccountingTests(unittest.TestCase):
    def setUp(self):
        self.context = queue_dispatch.DispatchContext(
            "sonarr", "main", 3, 10, 15, 60, lambda: False,
            lambda: 0, lambda: 0, mock.Mock(),
        )
        self.context.scheduler_held = True
        self.context.budget_reserved = True
        self.context.decypharr_reservation = object()
        self.context.current_item_keys = ["season:7:2", "episodes:11"]
        queue_dispatch._local.context = self.context

    def tearDown(self):
        queue_dispatch._local.context = None

    def test_accepted_grab_commits_one_search_and_retains_queue_capacity(self):
        pipeline = mock.Mock()
        pipeline.transition_items.return_value = True
        scheduler = mock.Mock()
        with mock.patch.object(queue_dispatch, "get_pipeline_state", return_value=pipeline), \
             mock.patch(
                 "src.primary.apps._common.shared_scheduler.get_shared_scheduler",
                 return_value=scheduler,
             ), \
             mock.patch("src.primary.stats_manager.increment_hourly_cap") as increment, \
             mock.patch("src.primary.stats_manager.release_hourly_cap_reservation") as release_budget, \
             mock.patch("src.primary.apps.swaparr.decypharr_capacity.release_reservation") as release_decy:
            self.assertTrue(queue_dispatch.finish_interactive_search(
                "grabbed", "sonarr", "main", queue_submission=True,
            ))

        self.assertFalse(self.context.budget_reserved)
        self.assertIsNone(self.context.decypharr_reservation)
        self.assertEqual(len(self.context.recent_submissions), 1)
        scheduler.release.assert_called_once_with("sonarr", "main")
        increment.assert_not_called()
        release_budget.assert_not_called()
        release_decy.assert_not_called()

    def test_failed_grab_commits_search_but_releases_queue_reservation(self):
        reservation = self.context.decypharr_reservation
        pipeline = mock.Mock()
        pipeline.transition_items.return_value = True
        scheduler = mock.Mock()
        with mock.patch.object(queue_dispatch, "get_pipeline_state", return_value=pipeline), \
             mock.patch(
                 "src.primary.apps._common.shared_scheduler.get_shared_scheduler",
                 return_value=scheduler,
             ), \
             mock.patch("src.primary.stats_manager.increment_hourly_cap") as increment, \
             mock.patch("src.primary.stats_manager.release_hourly_cap_reservation") as release_budget, \
             mock.patch("src.primary.apps.swaparr.decypharr_capacity.release_reservation") as release_decy:
            self.assertTrue(queue_dispatch.finish_interactive_search(
                "failed", "sonarr", "main", cooldown_seconds=300,
                queue_submission=False,
            ))

        self.assertFalse(self.context.budget_reserved)
        self.assertIsNone(self.context.decypharr_reservation)
        self.assertEqual(self.context.recent_submissions, [])
        scheduler.release.assert_called_once_with("sonarr", "main")
        release_decy.assert_called_once_with({}, reservation)
        increment.assert_not_called()
        release_budget.assert_not_called()


class MissingSeasonPackIntegrationTests(unittest.TestCase):
    episode = {
        "id": 11,
        "seriesId": 7,
        "seasonNumber": 2,
        "monitored": True,
        "airDateUtc": "2020-01-01T00:00:00Z",
        "series": {"title": "Show"},
    }

    def _run(self, grab_result):
        with mock.patch.object(
                sonarr_api, "get_missing_episodes_random_page",
                return_value=[self.episode]), \
             mock.patch.object(sonarr_missing.random, "shuffle"), \
             mock.patch.object(sonarr_missing, "is_processed", return_value=False), \
             mock.patch.object(sonarr_missing, "check_hourly_cap_exceeded", return_value=False), \
             mock.patch.object(sonarr_api, "grab_best_season_pack",
                               return_value=grab_result) as grab, \
             mock.patch.object(sonarr_missing, "add_processed_id") as processed, \
             mock.patch.object(sonarr_missing, "try_tag_item") as tag, \
             mock.patch.object(sonarr_missing, "log_processed_media") as history, \
             mock.patch.object(sonarr_missing, "increment_media_stat_only") as stats, \
             mock.patch.object(sonarr_api, "search_season") as automatic, \
             mock.patch.object(sonarr_api, "search_episode") as episodes:
            result = sonarr_missing.process_missing_seasons_packs_mode(
                "http://sonarr", "secret", "main", 10, True, True,
                1, 0, 1, 2, lambda: False,
            )
        return result, grab, processed, tag, history, stats, automatic, episodes

    def test_accepted_grab_marks_processed_tags_history_and_media_stats(self):
        result, grab, processed, tag, history, stats, automatic, episodes = self._run(
            _release("pack", 0)
        )
        self.assertTrue(result)
        grab.assert_called_once_with(
            "http://sonarr", "secret", 10, 7, 2, [11], instance_name="main",
        )
        processed.assert_called_once_with("sonarr", "main", "7_2")
        tag.assert_called_once()
        self.assertEqual(history.call_args.kwargs["status"], "grabbed")
        stats.assert_called_once_with("sonarr", "hunted", 1, "main")
        automatic.assert_not_called()
        episodes.assert_not_called()

    def test_no_pack_is_not_processed_or_tagged_and_never_falls_back(self):
        result, grab, processed, tag, history, stats, automatic, episodes = self._run(None)
        self.assertFalse(result)
        grab.assert_called_once()
        processed.assert_not_called()
        tag.assert_not_called()
        history.assert_not_called()
        stats.assert_not_called()
        automatic.assert_not_called()
        episodes.assert_not_called()

    def test_episodes_mode_dispatch_is_unchanged(self):
        with mock.patch.object(sonarr_missing, "process_missing_episodes_mode",
                               return_value=True) as episode_mode, \
             mock.patch.object(sonarr_missing, "process_missing_seasons_packs_mode") as pack_mode:
            result = sonarr_missing.process_missing_episodes(
                "http://sonarr", "secret", "main", hunt_missing_items=1,
                hunt_missing_mode="episodes",
            )
        self.assertTrue(result)
        episode_mode.assert_called_once()
        pack_mode.assert_not_called()

    def test_upgrade_and_exact_season_recovery_still_use_command_search(self):
        # Strict interactive grabbing is intentionally scoped to missing mode only.
        source_call = mock.patch.object(sonarr_api, "grab_best_season_pack")
        with source_call as strict, \
             mock.patch.object(sonarr_api.requests, "post",
                               return_value=_Response({"id": 123})), \
             mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded",
                        return_value=False), \
             mock.patch("src.primary.stats_manager.increment_hourly_cap"):
            command_id = sonarr_api.search_season(
                "http://sonarr", "secret", 10, 7, 2, instance_name="main",
            )
        self.assertEqual(command_id, 123)
        strict.assert_not_called()


if __name__ == "__main__":
    unittest.main()

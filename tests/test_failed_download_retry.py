"""Focused retry-ownership and failed-download lifecycle tests."""

import json
import sys
import types
import unittest
from unittest import mock

import requests

from src.primary.apps._common.pipeline_state import PipelineState
from src.primary.apps.swaparr import handler
from tests.test_pipeline_state_phase2 import _DB


class _Response:
    def __init__(self, command_id=None, error=None):
        self.command_id = command_id
        self.error = error

    def raise_for_status(self):
        if self.error:
            raise self.error

    def json(self):
        return {"id": self.command_id} if self.command_id is not None else {}


class RetryOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.now = [1000]
        self.pipeline = PipelineState(db=_DB(), clock=lambda: self.now[0])
        self.item = {
            "id": "queue-1", "name": "Show.S04E10", "source_title": "Show.S04E10",
            "download_id": "failed-download", "series_id": 7, "media_id": 7,
            "season_number": 4, "episode_id": 4080, "episode_ids": [4080],
        }
        self.pipeline.claim_candidate("sonarr", "instance", "episodes:4080", cooldown_seconds=0)
        self.pipeline.transition(
            "sonarr", "instance", "episodes:4080", "grabbed",
            metadata=handler._pipeline_metadata(self.item),
        )
        self.state_db = mock.Mock()
        self.state_db.remove_processed_ids.return_value = 1

    def _delete(self, research):
        delete_response = _Response()
        post_response = _Response(command_id=3721937)
        with mock.patch.object(handler.requests, "delete", return_value=delete_response) as delete, \
             mock.patch.object(handler.requests, "post", return_value=post_response) as post, \
             mock.patch.object(handler, "get_database", return_value=self.state_db), \
             mock.patch("src.primary.settings_manager.get_ssl_verify_setting", return_value=True), \
             mock.patch("src.primary.stats_manager.increment_hourly_cap") as hourly, \
             mock.patch.object(handler, "increment_swaparr_stat"):
            result = handler.delete_download(
                "sonarr", "http://sonarr", "secret", "queue-1", True, self.item,
                research, 30, instance_name="instance", pipeline=self.pipeline,
                retry_cooldown_seconds=600,
            )
        return result, delete, post, hourly

    def test_research_on_suppresses_starr_and_issues_exactly_one_accounted_search(self):
        result, delete, post, hourly = self._delete(True)
        self.assertTrue(result)
        self.assertEqual(delete.call_args.kwargs["params"]["skipRedownload"], "true")
        post.assert_called_once()
        self.assertEqual(post.call_args.kwargs["json"], {
            "name": "EpisodeSearch", "seriesId": 7, "episodeIds": [4080],
        })
        hourly.assert_called_once_with("sonarr", 1, instance_name="instance")
        with self.pipeline.db.get_connection() as conn:
            state, command_id, metadata = conn.execute(
                "SELECT state,command_id,metadata FROM pipeline_items WHERE item_key='episodes:4080'"
            ).fetchone()
        self.assertEqual((state, command_id), ("search_submitted", "3721937"))
        self.assertEqual(json.loads(metadata)["retry_owner"], "swaparr")
        self.state_db.remove_processed_ids.assert_called_once_with(
            "sonarr", "instance", ["4080"]
        )

    def test_research_off_leaves_starr_retry_enabled_and_issues_no_search(self):
        result, delete, post, hourly = self._delete(False)
        self.assertTrue(result)
        self.assertEqual(delete.call_args.kwargs["params"]["skipRedownload"], "false")
        post.assert_not_called()
        hourly.assert_not_called()
        with self.pipeline.db.get_connection() as conn:
            state, cooldown, metadata = conn.execute(
                "SELECT state,cooldown_until_epoch,metadata FROM pipeline_items "
                "WHERE item_key='episodes:4080'"
            ).fetchone()
        self.assertEqual((state, cooldown), ("failed", 1600))
        self.assertEqual(json.loads(metadata)["retry_owner"], "starr")

    def test_failed_delete_never_searches_or_changes_lifecycle(self):
        error = requests.exceptions.HTTPError("delete failed")
        with mock.patch.object(handler.requests, "delete", return_value=_Response(error=error)), \
             mock.patch.object(handler.requests, "post") as post, \
             mock.patch("src.primary.settings_manager.get_ssl_verify_setting", return_value=True), \
             mock.patch.object(handler, "increment_swaparr_stat"):
            result = handler.delete_download(
                "sonarr", "http://sonarr", "secret", "queue-1", True, self.item,
                True, 30, instance_name="instance", pipeline=self.pipeline,
            )
        self.assertFalse(result)
        post.assert_not_called()
        with self.pipeline.db.get_connection() as conn:
            state = conn.execute(
                "SELECT state FROM pipeline_items WHERE item_key='episodes:4080'"
            ).fetchone()[0]
        self.assertEqual(state, "grabbed")

    def test_unsupported_arr_delete_does_not_invent_skip_redownload(self):
        with mock.patch.object(handler.requests, "delete", return_value=_Response()) as delete, \
             mock.patch("src.primary.settings_manager.get_ssl_verify_setting", return_value=True), \
             mock.patch.object(handler, "increment_swaparr_stat"):
            self.assertTrue(handler.delete_download(
                "lidarr", "http://lidarr", "secret", 9, True, None, False, 30,
            ))
        self.assertNotIn("skipRedownload", delete.call_args.kwargs["params"])

    def test_radarr_uses_same_supported_retry_ownership_semantics(self):
        movie = {
            "id": 12, "name": "Movie", "source_title": "Movie.Release",
            "download_id": "movie-download", "movie_id": 42,
        }
        with mock.patch.object(handler.requests, "delete", return_value=_Response()) as delete, \
             mock.patch.object(handler.requests, "post", return_value=_Response(99)) as post, \
             mock.patch("src.primary.settings_manager.get_ssl_verify_setting", return_value=True), \
             mock.patch("src.primary.stats_manager.increment_hourly_cap") as hourly, \
             mock.patch.object(handler, "increment_swaparr_stat"):
            self.assertTrue(handler.delete_download(
                "radarr", "http://radarr", "secret", 12, True, movie, True, 30,
                instance_name="radarr-instance",
            ))
        self.assertEqual(delete.call_args.kwargs["params"]["skipRedownload"], "true")
        post.assert_called_once()
        self.assertEqual(post.call_args.kwargs["json"], {
            "name": "MoviesSearch", "movieIds": [42],
        })
        hourly.assert_called_once_with("radarr", 1, instance_name="radarr-instance")


class LifecycleCorrelationTests(unittest.TestCase):
    def setUp(self):
        self.now = [1000]
        self.pipeline = PipelineState(db=_DB(), clock=lambda: self.now[0])

    def _row(self, key):
        with self.pipeline.db.get_connection() as conn:
            return conn.execute(
                "SELECT state,cooldown_until_epoch,metadata FROM pipeline_items WHERE item_key=?",
                (key,),
            ).fetchone()

    def test_queue_ids_correlate_episode_season_and_movie(self):
        for key in ("episodes:4080", "season:7:4"):
            self.pipeline.claim_candidate("sonarr", "one", key, cooldown_seconds=0)
            self.pipeline.transition("sonarr", "one", key, "grabbed")
        keys = self.pipeline.fail_correlated_items(
            "sonarr", "one", ["episodes:4080", "season:7:4", "series:7"],
            "download-a", "Show.S04E10", "starr", cooldown_seconds=600,
        )
        self.assertEqual(keys, ["episodes:4080", "season:7:4"])
        self.assertEqual(self._row("episodes:4080")[:2], ("failed", 1600))

        for key in ("episodes:11", "episodes:12"):
            self.pipeline.claim_candidate(
                "sonarr", "pack", key, cooldown_seconds=0,
                metadata=json.dumps({"download_id": "shared-pack", "title": "Pack"}),
            )
            self.pipeline.transition("sonarr", "pack", key, "downloading")
        self.assertEqual(self.pipeline.fail_correlated_items(
            "sonarr", "pack", ["episodes:11"], "SHARED-PACK", "Pack", "starr"
        ), ["episodes:11", "episodes:12"])

        self.pipeline.claim_candidate("radarr", "one", "movies:42", cooldown_seconds=0)
        self.pipeline.transition("radarr", "one", "movies:42", "downloading")
        self.assertEqual(self.pipeline.fail_correlated_items(
            "radarr", "one", ["movies:42"], "movie-dl", "Movie", "starr"
        ), ["movies:42"])
        self.assertEqual(self._row("movies:42")[0], "failed")

    def test_download_id_fallback_and_completed_protection(self):
        old = {"download_id": "abc", "title": "Exact.Title"}
        self.pipeline.claim_candidate(
            "sonarr", "one", "episodes:5", cooldown_seconds=0,
            metadata=json.dumps(old),
        )
        self.pipeline.transition("sonarr", "one", "episodes:5", "downloading")
        self.assertEqual(self.pipeline.fail_correlated_items(
            "sonarr", "one", [], "ABC", "different", "starr"
        ), ["episodes:5"])

        self.pipeline.claim_candidate("sonarr", "one", "episodes:6", cooldown_seconds=0)
        self.pipeline.transition("sonarr", "one", "episodes:6", "completed")
        self.pipeline.fail_correlated_items(
            "sonarr", "one", ["episodes:6"], "done", "Done", "starr"
        )
        self.assertEqual(self._row("episodes:6")[0], "completed")

        self.pipeline.claim_candidate("sonarr", "one", "episodes:7", cooldown_seconds=0)
        self.pipeline.transition("sonarr", "one", "episodes:7", "imported")
        self.pipeline.fail_correlated_items(
            "sonarr", "one", ["episodes:7"], "imported", "Imported", "starr"
        )
        self.assertEqual(self._row("episodes:7")[0], "imported")

    def test_cooldown_then_fallback_only_without_active_replacement(self):
        self.pipeline.claim_candidate("sonarr", "one", "episodes:8", cooldown_seconds=0)
        self.pipeline.transition(
            "sonarr", "one", "episodes:8", "downloading",
            metadata=json.dumps({"download_id": "old"}),
        )
        self.pipeline.fail_correlated_items(
            "sonarr", "one", ["episodes:8"], "old", "Title", "starr",
            cooldown_seconds=600,
        )
        self.assertEqual(
            self.pipeline.candidate_availability("sonarr", "one", ["episodes:8"])["cooldown"],
            ["episodes:8"],
        )
        self.now[0] = 1601
        self.assertEqual(
            self.pipeline.candidate_availability("sonarr", "one", ["episodes:8"])["eligible"],
            ["episodes:8"],
        )
        self.assertTrue(self.pipeline.claim_candidate("sonarr", "one", "episodes:8"))
        self.assertEqual(json.loads(self._row("episodes:8")[2])["retry_owner"], "huntarr-fallback")

        # A new Starr replacement queue item is unresolved even after the old cooldown.
        self.pipeline.transition("sonarr", "one", "episodes:8", "failed", cooldown_seconds=0)
        changed = self.pipeline.observe_active_items(
            "sonarr", "one", ["episodes:8"], download_id="new", title="Title",
        )
        self.assertEqual(changed, ["episodes:8"])
        self.assertEqual(
            self.pipeline.candidate_availability("sonarr", "one", ["episodes:8"])["unresolved"],
            ["episodes:8"],
        )

    def test_processed_memory_release_maps_exact_media_ids(self):
        db = mock.Mock()
        db.remove_processed_ids.return_value = 3
        with mock.patch.object(handler, "get_database", return_value=db):
            released = handler.release_processed_memory(
                "sonarr", "instance",
                ["episodes:4080", "season:7:4", "series:7", "queue:abc"],
            )
        self.assertEqual(released, 3)
        db.remove_processed_ids.assert_called_once_with(
            "sonarr", "instance", ["4080", "7_4", "7"]
        )

    def test_completed_replacement_releases_unresolved_guard_but_preserves_cooldown(self):
        self.pipeline.claim_candidate("sonarr", "one", "episodes:9", cooldown_seconds=0)
        self.pipeline.transition("sonarr", "one", "episodes:9", "downloading")
        keys = self.pipeline.fail_correlated_items(
            "sonarr", "one", ["episodes:9"], "old", "Title", "swaparr",
            cooldown_seconds=600,
        )
        self.pipeline.mark_retry_requested("sonarr", "one", keys, "55")
        # An ordinary Huntarr command must never be reconciled as a Swaparr retry.
        self.pipeline.claim_candidate("sonarr", "one", "episodes:10", cooldown_seconds=0)
        self.pipeline.transition(
            "sonarr", "one", "episodes:10", "search_submitted", command_id="66"
        )
        with mock.patch("src.primary.apps.sonarr.api.get_command_status", return_value={"status": "completed"}) as status:
            handler.reconcile_replacement_commands(
                self.pipeline, "sonarr", "one",
                {"api_url": "http://sonarr", "api_key": "secret", "api_timeout": 30},
            )
        status.assert_called_once()
        self.assertEqual(self._row("episodes:9")[:2], ("no_grab", 1600))
        self.assertEqual(self._row("episodes:10")[0], "search_submitted")
        self.assertEqual(
            self.pipeline.candidate_availability("sonarr", "one", ["episodes:9"])["cooldown"],
            ["episodes:9"],
        )
        self.now[0] = 1601
        self.assertEqual(
            self.pipeline.candidate_availability("sonarr", "one", ["episodes:9"])["eligible"],
            ["episodes:9"],
        )
        self.assertTrue(self.pipeline.claim_candidate("sonarr", "one", "episodes:9"))
        self.assertEqual(json.loads(self._row("episodes:9")[2])["retry_owner"], "huntarr-fallback")

    def test_natural_sonarr_failure_is_starr_owned_without_delete_or_search(self):
        item = {
            "id": "q", "name": "Show.S04E10", "source_title": "Show.S04E10",
            "download_id": "natural", "series_id": 7, "season_number": 4,
            "episode_id": 4080, "episode_ids": [4080],
        }
        self.pipeline.claim_candidate(
            "sonarr", "instance", "episodes:4080", cooldown_seconds=0,
            metadata=handler._pipeline_metadata(item),
        )
        self.pipeline.transition("sonarr", "instance", "episodes:4080", "grabbed")
        history = {"records": [{
            "id": 1, "date": "2026-09-13T17:00:00Z", "eventType": "downloadFailed",
            "downloadId": "natural", "sourceTitle": "Show.S04E10", "seriesId": 7,
            "episode": {"id": 4080, "seasonNumber": 4},
            "data": {"message": "partial availability"},
        }]}
        activity_db = mock.Mock()
        activity_db.get_swaparr_state_data.return_value = {}
        activity_db.has_recent_swaparr_activity.return_value = False
        activity_db.remove_processed_ids.return_value = 1
        instance = {"api_url": "http://sonarr", "api_key": "secret", "instance_id": "instance"}
        nzbdav = types.ModuleType("src.primary.apps.nzbdav_routes")
        nzbdav.get_nzbdav_failure_context = mock.Mock(return_value={})
        with mock.patch.object(handler, "get_database", return_value=activity_db), \
             mock.patch("src.primary.apps.sonarr.api.arr_request", return_value=history), \
             mock.patch("src.primary.apps._common.pipeline_state.get_pipeline_state", return_value=self.pipeline), \
             mock.patch("src.primary.apps.swaparr.torrent_status.get_torrent_statuses", return_value={}), \
             mock.patch("src.primary.apps.swaparr.torrent_status.get_decypharr_failure_context", return_value={}), \
             mock.patch.dict(sys.modules, {"src.primary.apps.nzbdav_routes": nzbdav}), \
             mock.patch.object(handler, "delete_download") as delete, \
             mock.patch.object(handler, "trigger_search_for_item") as search:
            handler.scan_sonarr_history_for_activity(
                "sonarr", "Display", instance, retry_cooldown_seconds=600,
            )
        delete.assert_not_called()
        search.assert_not_called()
        state, cooldown, metadata = self._row("episodes:4080")
        self.assertEqual((state, cooldown), ("failed", 1600))
        self.assertEqual(json.loads(metadata)["retry_owner"], "starr")


if __name__ == "__main__":
    unittest.main()

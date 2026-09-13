"""Focused Phase 3 Decypharr capacity, weighted fairness, and status tests."""

import os
import tempfile
import threading
import time
import unittest
from unittest import mock

os.environ.setdefault("HUNTARR_CONFIG_DIR", tempfile.mkdtemp(prefix="huntarr_phase3_capacity_"))

from src.primary.apps._common import queue_dispatch
from src.primary.apps._common.shared_scheduler import WeightedDispatchScheduler, get_shared_scheduler
from src.primary.apps.swaparr import decypharr_capacity
from src.primary.apps.tor_hunt.qbittorrent_client import QBittorrentClient


class _Response:
    def __init__(self, data, status=200):
        self.data = data
        self.status_code = status

    def json(self):
        return self.data


class QBittorrentCapacityExtensionTests(unittest.TestCase):
    def test_reads_active_jobs_and_reported_limit_without_treating_failure_as_empty(self):
        client = object.__new__(QBittorrentClient)
        client._get = mock.Mock(side_effect=[
            _Response([{"state": "downloading"}, {"state": "uploading"}, {"state": "queuedDL"}]),
            _Response({"max_active_downloads": 4}),
        ])
        self.assertEqual(client.get_job_capacity(), {
            "active": 2, "limit": 4, "free": 2,
            "source": "client-reported max_active_downloads",
        })
        client._get = mock.Mock(return_value=None)
        with self.assertRaisesRegex(RuntimeError, "unreachable"):
            client.get_job_capacity()


class DecypharrCapacityTests(unittest.TestCase):
    def setUp(self):
        decypharr_capacity.reset_cache()
        self.config = {"type": "qbittorrent", "host": "decypharr", "port": 8282}

    def test_available_and_full_capacity(self):
        with mock.patch.object(decypharr_capacity, "_read_capacity", return_value={
            "healthy": True, "active": 1, "limit": 3, "free": 2, "reason": "Decypharr 1/3 active",
        }):
            available = decypharr_capacity.get_capacity(self.config, monotonic=lambda: 0)
        self.assertTrue(available["healthy"])
        self.assertEqual(available["free"], 2)
        decypharr_capacity.reset_cache()
        with mock.patch.object(decypharr_capacity, "_read_capacity", return_value={
            "healthy": True, "active": 3, "limit": 3, "free": 0, "reason": "Decypharr 3/3 active",
        }):
            full = decypharr_capacity.get_capacity(self.config, monotonic=lambda: 0)
        self.assertEqual(full["free"], 0)

    def test_unavailable_capacity_fails_open_with_bounded_backoff_and_precise_reason(self):
        clock = [0.0]
        with mock.patch.object(decypharr_capacity, "_read_capacity", side_effect=RuntimeError("offline")) as read:
            first = decypharr_capacity.get_capacity(self.config, monotonic=lambda: clock[0])
            clock[0] = 1
            cached = decypharr_capacity.get_capacity(self.config, monotonic=lambda: clock[0])
            clock[0] = 31
            second = decypharr_capacity.get_capacity(self.config, monotonic=lambda: clock[0])
        self.assertTrue(first["fail_open"])
        self.assertIn("using Starr capacity (fail-open): offline", first["reason"])
        self.assertEqual(first["poll_interval"], 30.0)
        self.assertEqual(cached, first)
        self.assertEqual(second["poll_interval"], 60.0)
        self.assertLessEqual(second["poll_interval"], 300.0)
        self.assertEqual(read.call_count, 2)

    def test_disabled_or_unconfigured_is_non_blocking(self):
        result = decypharr_capacity.get_capacity({})
        self.assertFalse(result["enabled"])
        self.assertTrue(result["fail_open"])

    def test_only_one_concurrent_waiter_can_reserve_final_decypharr_slot(self):
        with mock.patch.object(decypharr_capacity, "_read_capacity", return_value={
            "healthy": True, "active": 1, "limit": 2, "free": 1,
            "reason": "Decypharr 1/2 active",
        }):
            decypharr_capacity.get_capacity(self.config, monotonic=lambda: 0)
        barrier = threading.Barrier(3)
        results = []

        def reserve():
            barrier.wait()
            results.append(decypharr_capacity.try_reserve_slot(
                self.config, monotonic=lambda: 1,
            ))

        threads = [threading.Thread(target=reserve) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(2)
        self.assertEqual(sum(result is False for result in results), 1)
        self.assertEqual(sum(result not in (False, None) for result in results), 1)


class WeightedSchedulerTests(unittest.TestCase):
    def test_equal_weight_contenders_alternate_and_never_overlap(self):
        scheduler = WeightedDispatchScheduler()
        scheduler.configure("sonarr", "s", 1)
        scheduler.configure("radarr", "r", 1)
        barrier = threading.Barrier(3)
        order = []
        active = [0]
        overlap = []
        lock = threading.Lock()

        def worker(app, instance):
            barrier.wait()
            for _ in range(5):
                self.assertTrue(scheduler.acquire(app, instance, 2, lambda: False))
                with lock:
                    active[0] += 1
                    overlap.append(active[0])
                    order.append(app)
                time.sleep(0.002)
                with lock:
                    active[0] -= 1
                scheduler.release(app, instance)

        threads = [threading.Thread(target=worker, args=("sonarr", "s")),
                   threading.Thread(target=worker, args=("radarr", "r"))]
        for thread in threads: thread.start()
        barrier.wait()
        for thread in threads: thread.join(5)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(max(overlap), 1)
        # Once both have entered contention, equal weights prevent a run longer than one.
        middle = order[1:-1]
        self.assertTrue(all(a != b for a, b in zip(middle, middle[1:])), order)

    def test_weight_two_gets_two_turns_per_other_turn_under_contention(self):
        scheduler = WeightedDispatchScheduler()
        scheduler.configure("sonarr", "s", 2)
        scheduler.configure("radarr", "r", 1)
        # Exercise deterministic weighted selection while both keys remain waiting.
        with scheduler._condition:
            scheduler._waiting = {("sonarr", "s"): 1, ("radarr", "r"): 1}
            sequence = []
            for _ in range(6):
                key = scheduler._next_waiter()
                sequence.append(key[0])
                scheduler._cursor = (scheduler._cursor + 1) % len(scheduler._ring)
        self.assertEqual(sequence, ["sonarr", "sonarr", "radarr", "sonarr", "sonarr", "radarr"])


class _Pipeline:
    def __init__(self):
        self.runtime_values = {}

    def observe_queue(self, app, name, fetch, **kwargs):
        value = fetch()
        value.update(healthy=True)
        return value

    def merge_queue(self, *args, **kwargs):
        pass

    def set_runtime(self, app, instance, **values):
        self.runtime_values[(app, instance)] = values

    def invalidate_queue(self, *args):
        pass

    def claim_candidates(self, *args, **kwargs):
        return True

    def transition_items(self, *args, **kwargs):
        return True


class DispatchCapacityStatusTests(unittest.TestCase):
    def setUp(self):
        get_shared_scheduler().reset()
        self.pipeline = _Pipeline()
        self.logger = mock.Mock()
        self.settings = {
            "target_queue_depth": 3,
            "minimum_dispatch_interval_seconds": 1,
            "queue_redispatch_wait_seconds": 0,
            "decypharr_capacity_enabled": True,
            "decypharr_max_active_jobs": 2,
            "seed_check_torrent_client": {"type": "qbittorrent", "host": "decypharr"},
        }
        patcher = mock.patch.object(queue_dispatch, "get_pipeline_state", return_value=self.pipeline)
        patcher.start(); self.addCleanup(patcher.stop)
        budget = mock.patch("src.primary.stats_manager.get_hourly_cap_status", return_value={
            "remaining": 4, "limit": 5, "current_usage": 1,
        })
        budget.start(); self.addCleanup(budget.stop)

    def configure(self):
        return queue_dispatch.configure_dispatch(
            "sonarr", "s", self.settings,
            queue_size=lambda: 0, active_searches=lambda: 0,
            queue_items=lambda: [], stop_check=lambda: False, logger=self.logger,
        )

    def test_full_decypharr_blocks_with_status_reason(self):
        self.configure()
        with mock.patch.object(queue_dispatch, "_decypharr_capacity", return_value={
            "enabled": True, "healthy": True, "free": 0, "fail_open": False,
            "reason": "Decypharr 2/2 active",
        }):
            self.assertFalse(queue_dispatch.acquire_dispatch_slot())
        runtime = self.pipeline.runtime_values[("sonarr", "s")]
        self.assertIn("Decypharr capacity full", runtime["pause_reason"])
        self.assertEqual(runtime["slots_free"], 0)

    def test_unavailable_decypharr_fails_open_to_starr_and_budget(self):
        self.configure()
        unavailable = {"enabled": True, "healthy": False, "free": None, "fail_open": True,
                       "reason": "Decypharr capacity unavailable; using Starr capacity (fail-open): offline"}
        with mock.patch.object(queue_dispatch, "_decypharr_capacity", return_value=unavailable):
            self.assertTrue(queue_dispatch.acquire_dispatch_slot())
            queue_dispatch.record_submission()
        runtime = self.pipeline.runtime_values[("sonarr", "s")]
        self.assertEqual(runtime["decypharr"], unavailable)
        self.assertIsNone(runtime["pause_reason"])
        self.assertIn("fail-open", runtime["decypharr"]["reason"])

    def test_exception_after_scheduler_grant_cannot_leak_grant(self):
        context = self.configure()
        with mock.patch.object(queue_dispatch, "_occupancy", side_effect=[
                (0, 0, 0, 0), RuntimeError("final Starr refresh failed")]), \
             mock.patch.object(queue_dispatch, "_decypharr_capacity", return_value={
                 "enabled": True, "healthy": True, "free": 2, "fail_open": False,
                 "reason": "Decypharr 0/2 active",
             }):
            with self.assertRaisesRegex(RuntimeError, "final Starr refresh failed"):
                queue_dispatch.acquire_dispatch_slot()
        self.assertFalse(context.scheduler_held)
        self.assertIsNone(get_shared_scheduler()._holder)

    def test_cancel_unwinds_scheduler_budget_and_decypharr_reservations_once(self):
        context = self.configure()
        context.scheduler_held = True
        context.budget_reserved = True
        context.decypharr_reservation = object()
        with mock.patch.object(get_shared_scheduler(), "release") as release_grant, \
             mock.patch("src.primary.stats_manager.release_hourly_cap_reservation") as release_budget, \
             mock.patch("src.primary.apps.swaparr.decypharr_capacity.release_reservation") as release_decy:
            queue_dispatch.cancel_dispatch_slot()
            queue_dispatch.cancel_dispatch_slot()
        release_grant.assert_called_once_with("sonarr", "s")
        release_budget.assert_called_once_with("sonarr", "s")
        release_decy.assert_called_once()
        self.assertFalse(context.scheduler_held)
        self.assertFalse(context.budget_reserved)
        self.assertIsNone(context.decypharr_reservation)

    def test_final_grant_check_rejects_newly_full_decypharr_capacity(self):
        self.configure()
        available = {"enabled": True, "healthy": True, "free": 1, "fail_open": False,
                     "reason": "Decypharr 1/2 active"}
        full = {"enabled": True, "healthy": True, "free": 0, "fail_open": False,
                "reason": "Decypharr 2/2 active"}
        with mock.patch.object(queue_dispatch, "_decypharr_capacity",
                               side_effect=[available, full]) as capacity, \
             mock.patch("src.primary.stats_manager.try_reserve_hourly_cap") as reserve_budget:
            self.assertFalse(queue_dispatch.acquire_dispatch_slot())
        self.assertEqual(capacity.call_args_list[-1].kwargs, {"force": True})
        reserve_budget.assert_not_called()
        self.assertIn("Decypharr capacity full after grant",
                      self.pipeline.runtime_values[("sonarr", "s")]["pause_reason"])

    def test_final_grant_check_rejects_consumed_last_hourly_token(self):
        self.configure()
        decy = {"enabled": True, "healthy": True, "free": 2, "fail_open": False,
                "reason": "Decypharr 0/2 active"}
        with mock.patch.object(queue_dispatch, "_decypharr_capacity", return_value=decy), \
             mock.patch.object(queue_dispatch, "_search_budget", side_effect=[
                 {"remaining": 1, "limit": 5, "used": 4},
                 {"remaining": 0, "limit": 5, "used": 5},
             ]), \
             mock.patch("src.primary.stats_manager.try_reserve_hourly_cap") as reserve_budget:
            self.assertFalse(queue_dispatch.acquire_dispatch_slot())
        reserve_budget.assert_not_called()
        self.assertIn("search budget exhausted after grant",
                      self.pipeline.runtime_values[("sonarr", "s")]["pause_reason"])

    def test_atomic_hourly_reservation_allows_only_final_token(self):
        from src.primary import stats_manager

        class BudgetDB:
            def __init__(self):
                self.used = 4

            def get_hourly_caps_per_instance(self, _app):
                return {"s": {"api_hits": self.used}}

            def increment_hourly_cap_per_instance(self, _app, _instance, count):
                self.used += count

        database = BudgetDB()
        barrier = threading.Barrier(3)
        results = []

        def reserve():
            barrier.wait()
            results.append(stats_manager.try_reserve_hourly_cap("sonarr", "s"))

        with mock.patch.object(stats_manager, "get_database", return_value=database), \
             mock.patch.object(stats_manager, "_get_instance_hourly_cap_limit", return_value=5), \
             mock.patch.object(stats_manager, "check_hourly_reset"):
            threads = [threading.Thread(target=reserve) for _ in range(2)]
            for thread in threads:
                thread.start()
            barrier.wait()
            for thread in threads:
                thread.join(2)
        self.assertCountEqual(results, [True, False])
        self.assertEqual(database.used, 5)

    def test_search_budget_is_part_of_minimum_gate(self):
        self.configure()
        with mock.patch.object(queue_dispatch, "_decypharr_capacity", return_value={
                "enabled": True, "healthy": True, "free": 2, "fail_open": False,
                "reason": "Decypharr 0/2 active"}), \
             mock.patch.object(queue_dispatch, "_search_budget", return_value={
                "remaining": 0, "limit": 5, "used": 5}):
            self.assertFalse(queue_dispatch.acquire_dispatch_slot())
        self.assertIn("search budget exhausted", self.pipeline.runtime_values[("sonarr", "s")]["pause_reason"])


if __name__ == "__main__":
    unittest.main()

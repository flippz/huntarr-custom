"""Queue-aware search dispatch for Sonarr and Radarr.

The dispatcher is configured once per instance worker thread.  Search API helpers use
it immediately before POSTing a command, so every missing/upgrade mode shares the
same queue and pacing rules without changing stateful processed-item retention.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, Optional, Union

from src.primary.apps._common.pipeline_state import get_pipeline_state


@dataclass
class DispatchContext:
    app_type: str
    instance_name: str
    target_depth: int
    max_queue_size: int
    minimum_interval: float
    redispatch_wait: float
    stop_check: Callable[[], bool]
    queue_size: Callable[[], int]
    active_searches: Callable[[], int]
    logger: object
    queue_cache_name: Optional[str] = None
    recent_submissions: list = field(default_factory=list)
    last_submission: float = 0.0
    queue_status: Optional[Callable[[], tuple]] = None
    poll_interval: float = 5.0
    last_capacity: Optional[tuple] = None
    current_item_keys: list = field(default_factory=list)
    shared_weight: int = 1
    decypharr_capacity_enabled: bool = False
    decypharr_config: Optional[dict] = None
    scheduler_held: bool = False


_local = threading.local()
_contexts: Dict[tuple, DispatchContext] = {}
_lock = threading.RLock()
_RECENT_GRACE_SECONDS = 120.0


def configure_dispatch(app_type: str, instance_name: str, settings: dict,
                       queue_size: Callable[[], int], active_searches: Callable[[], int],
                       stop_check: Callable[[], bool], logger,
                       queue_cache_name: Optional[str] = None,
                       queue_items: Optional[Callable[[], list]] = None) -> DispatchContext:
    """Configure queue dispatch for the current instance worker thread."""
    def _integer(name, default, minimum):
        try:
            return max(minimum, int(settings.get(name, default)))
        except (TypeError, ValueError):
            return default

    target = _integer("target_queue_depth", 3, 1)
    maximum = _integer("max_download_queue_size", -1, -1)
    interval = float(_integer("minimum_dispatch_interval_seconds", 15, 1))
    redispatch = float(_integer("queue_redispatch_wait_seconds", 60, 0))
    shared_weight = _integer("shared_capacity_weight", 1, 1)
    decypharr_enabled = settings.get("decypharr_capacity_enabled") is True
    decypharr_config = dict(settings.get("seed_check_torrent_client") or {})
    decypharr_config["max_active_jobs"] = _integer("decypharr_max_active_jobs", 0, 0)
    key = (app_type, str(instance_name))
    with _lock:
        previous = _contexts.get(key)
        if previous and previous.scheduler_held:
            _release_shared(previous)
        recent = list(previous.recent_submissions) if previous else []
        last = previous.last_submission if previous else 0.0
        cache_name = str(queue_cache_name or instance_name)

        def _fetch_status():
            records = queue_items() if queue_items is not None else None
            if queue_items is not None and records is None:
                raise RuntimeError("queue records unavailable")
            queue = len(records) if records is not None else queue_size()
            active = active_searches()
            if queue < 0 or active < 0:
                raise RuntimeError("queue/command status unavailable")
            return {"records": records, "queue": queue, "active": active}

        def _queue_status():
            value = get_pipeline_state().observe_queue(app_type, cache_name, _fetch_status)
            if not value.get("healthy"):
                return -1, -1
            queue = value.get("queue")
            active = value.get("active")
            # Swaparr may have populated the shared cache first. Reuse its full queue
            # observation and fetch only the independent command-capacity endpoint.
            if active is None:
                active = active_searches()
                if active >= 0:
                    get_pipeline_state().merge_queue(app_type, cache_name, active=active)
            return (int(queue) if queue is not None else -1,
                    int(active) if active is not None else -1)

        context = DispatchContext(app_type, str(instance_name), target, maximum,
                                  interval, redispatch, stop_check, queue_size,
                                  active_searches, logger, cache_name, recent, last,
                                  queue_status=_queue_status, shared_weight=shared_weight,
                                  decypharr_capacity_enabled=decypharr_enabled,
                                  decypharr_config=decypharr_config)
        _contexts[key] = context
        from src.primary.apps._common.shared_scheduler import get_shared_scheduler
        get_shared_scheduler().configure(app_type, str(instance_name), shared_weight)
    _local.context = context
    logger.info(
        "Queue dispatch enabled for %s: target=%d, hard ceiling=%s, minimum interval=%ds, "
        "shared weight=%d, Decypharr capacity=%s",
        instance_name, target, "disabled" if maximum < 0 else maximum, int(interval),
        shared_weight, "enabled" if decypharr_enabled else "disabled"
    )
    return context


def clear_dispatch() -> None:
    context = getattr(_local, "context", None)
    if context is not None:
        _release_shared(context)
    _local.context = None


def invalidate_dispatch_observation(app_type: str, instance_name: str) -> None:
    """Force the matching stable instance's next Starr observation after a webhook."""
    with _lock:
        context = _contexts.get((str(app_type), str(instance_name)))
        cache_name = context.queue_cache_name if context else str(instance_name)
    get_pipeline_state().invalidate_queue(app_type, cache_name)


def _occupancy(context: DispatchContext, now: float):
    context.recent_submissions[:] = [t for t in context.recent_submissions
                                     if now - t < _RECENT_GRACE_SECONDS]
    if context.queue_status:
        queue, active = context.queue_status()
    else:
        queue = context.queue_size()
        active = context.active_searches()
    if queue < 0 or active < 0:
        return None, queue, active, len(context.recent_submissions)
    # Recent submissions reserve slots until the command endpoint catches up. Only
    # active searches can satisfy those reservations; unrelated download-queue rows
    # must not mask a just-submitted search.
    unseen_recent = max(0, len(context.recent_submissions) - active)
    return queue + active + unseen_recent, queue, active, unseen_recent


def _release_shared(context: DispatchContext) -> None:
    if not context.scheduler_held:
        return
    from src.primary.apps._common.shared_scheduler import get_shared_scheduler
    get_shared_scheduler().release(context.app_type, context.instance_name)
    context.scheduler_held = False


def _decypharr_capacity(context: DispatchContext) -> dict:
    if not context.decypharr_capacity_enabled:
        return {"enabled": False, "healthy": False, "free": None, "fail_open": True,
                "reason": "Decypharr capacity disabled"}
    from src.primary.apps.swaparr.decypharr_capacity import get_capacity
    return get_capacity(context.decypharr_config or {})


def _search_budget(context: DispatchContext) -> dict:
    try:
        from src.primary.stats_manager import get_hourly_cap_status
        status = get_hourly_cap_status(context.app_type, instance_name=context.instance_name)
        if not isinstance(status, dict) or status.get("error") or status.get("remaining") is None:
            raise RuntimeError("search budget status unavailable")
        return {"remaining": max(0, int(status["remaining"])),
                "limit": status.get("limit"), "used": status.get("current_usage", 0)}
    except Exception:
        # Existing call-site cap checks still apply; telemetry failure must not introduce deadlock.
        return {"remaining": None, "limit": None, "used": None}


def acquire_dispatch_slot() -> bool:
    """Wait responsively for min(Starr, Decypharr, budget) and a weighted turn."""
    context: Optional[DispatchContext] = getattr(_local, "context", None)
    if context is None:
        return True
    deadline = time.monotonic() + context.redispatch_wait
    last_reason = None
    while True:
        if context.stop_check():
            context.logger.info("Queue dispatch stopped by request for %s", context.instance_name)
            return False
        now = time.monotonic()
        occupancy, queue, active, recent = _occupancy(context, now)
        decypharr = _decypharr_capacity(context)
        budget = _search_budget(context)
        starr_free = None if occupancy is None else max(0, context.target_depth - occupancy)
        decypharr_free = decypharr.get("free") if decypharr.get("healthy") else None
        budget_free = budget.get("remaining")
        effective_free = min(
            value for value in (starr_free, decypharr_free, budget_free)
            if value is not None
        ) if any(value is not None for value in (starr_free, decypharr_free, budget_free)) else None
        if occupancy is None:
            # A configured hard ceiling must never be bypassed when queue state is unknown.
            if context.max_queue_size >= 0:
                get_pipeline_state().set_runtime(
                    context.app_type, context.instance_name, slots_used=None,
                    slots_target=context.target_depth, slots_free=0, queue=queue,
                    active_searches=active, decypharr=decypharr,
                    pause_reason="queue/command status unavailable while hard ceiling is enabled",
                )
                context.logger.warning(
                    "Queue dispatch deferred for %s: queue/command status unavailable and hard ceiling is enabled",
                    context.instance_name,
                )
                return False
            last_reason = "queue/command status unavailable"
            # The shared cache owns unhealthy 30..300s backoff. Avoid forcing a fresh
            # request here; this loop merely waits before consulting that cache again.
            context.poll_interval = min(300.0, max(30.0, context.poll_interval * 2.0))
        elif budget_free is not None and budget_free <= 0:
            reason = f"search budget exhausted ({budget.get('used')}/{budget.get('limit')})"
            get_pipeline_state().set_runtime(
                context.app_type, context.instance_name, slots_used=occupancy,
                slots_target=context.target_depth, slots_free=0, queue=queue,
                active_searches=active, decypharr=decypharr, pause_reason=reason,
            )
            context.logger.info("Queue dispatch deferred for %s: %s", context.instance_name, reason)
            return False
        elif context.max_queue_size >= 0 and queue >= context.max_queue_size:
            reason = f"download queue {queue} reached hard ceiling {context.max_queue_size}"
            get_pipeline_state().set_runtime(
                context.app_type, context.instance_name, slots_used=occupancy,
                slots_target=context.target_depth, slots_free=0, queue=queue,
                active_searches=active, decypharr=decypharr, pause_reason=reason,
            )
            context.logger.info(
                "Queue dispatch deferred for %s: queue=%d reached hard ceiling=%d",
                context.instance_name, queue, context.max_queue_size,
            )
            return False
        elif occupancy >= context.target_depth:
            last_reason = (f"Starr capacity full: effective occupancy {occupancy}/{context.target_depth} "
                           f"(queue={queue}, active searches={active}, recent reservations={recent})")
        elif decypharr.get("healthy") and decypharr_free <= 0:
            last_reason = f"Decypharr capacity full: {decypharr.get('reason')}"
        else:
            interval_left = context.minimum_interval - (now - context.last_submission)
            if interval_left <= 0:
                from src.primary.apps._common.shared_scheduler import get_shared_scheduler
                scheduler = get_shared_scheduler()
                remaining = max(0.0, deadline - now)
                waiting_reason = scheduler.waiting_reason(context.app_type, context.instance_name)
                if waiting_reason:
                    get_pipeline_state().set_runtime(
                        context.app_type, context.instance_name, slots_used=occupancy,
                        slots_target=context.target_depth, slots_free=effective_free,
                        queue=queue, active_searches=active, decypharr=decypharr,
                        pause_reason=waiting_reason,
                    )
                if not scheduler.acquire(
                    context.app_type, context.instance_name, remaining, context.stop_check,
                ):
                    last_reason = waiting_reason or "waiting for weighted shared capacity turn"
                else:
                    context.scheduler_held = True
                    # Re-observe Starr capacity after the serialized grant. Recent reservations
                    # from the previous holder are now visible before this caller can POST.
                    occupancy2, queue2, active2, recent2 = _occupancy(context, time.monotonic())
                    if occupancy2 is None or occupancy2 >= context.target_depth:
                        _release_shared(context)
                        last_reason = "shared capacity changed before dispatch; reconciling"
                    else:
                        get_pipeline_state().set_runtime(
                            context.app_type, context.instance_name, slots_used=occupancy2,
                            slots_target=context.target_depth, slots_free=max(0, effective_free or 0),
                            queue=queue2, active_searches=active2, decypharr=decypharr,
                            pause_reason=None,
                        )
                        context.logger.info(
                            "Queue dispatch slot available for %s: Starr free=%d, Decypharr free=%s, "
                            "search budget=%s (weighted shared grant)",
                            context.instance_name, context.target_depth - occupancy2,
                            decypharr_free if decypharr_free is not None else "fail-open",
                            budget_free if budget_free is not None else "unknown",
                        )
                        return True
            else:
                last_reason = f"minimum dispatch interval ({interval_left:.1f}s remaining)"

        if occupancy is not None:
            capacity = (queue, active, recent, occupancy >= context.target_depth,
                        context.max_queue_size >= 0 and queue >= context.max_queue_size)
            if context.last_capacity is None or capacity != context.last_capacity:
                context.poll_interval = 5.0
            else:
                context.poll_interval = min(30.0, max(5.0, context.poll_interval * 1.5))
            context.last_capacity = capacity

        remaining = deadline - now
        get_pipeline_state().set_runtime(
            context.app_type, context.instance_name,
            slots_used=occupancy, slots_target=context.target_depth, slots_free=effective_free,
            queue=queue, active_searches=active, decypharr=decypharr,
            pause_reason=last_reason,
        )
        if remaining <= 0:
            context.logger.info("Queue dispatch paused for %s after %s; next cycle will retry",
                                context.instance_name, last_reason)
            return False
        # Bounded polling: never faster than once per second and normally at the configured interval.
        sleep_for = min(remaining, context.poll_interval)
        context.logger.debug("Queue dispatch waiting %.1fs for %s: %s",
                             sleep_for, context.instance_name, last_reason)
        end = time.monotonic() + sleep_for
        while time.monotonic() < end:
            if context.stop_check():
                _release_shared(context)
                return False
            from src.primary.apps._common.wake_registry import is_wake_pending
            if is_wake_pending(context.app_type):
                context.poll_interval = 1.0
                break
            time.sleep(min(0.5, end - time.monotonic()))


def record_submission() -> None:
    """Reserve capacity immediately after a successful command submission."""
    context: Optional[DispatchContext] = getattr(_local, "context", None)
    if context is None:
        return
    now = time.monotonic()
    with _lock:
        context.last_submission = now
        context.recent_submissions.append(now)
    if context.decypharr_capacity_enabled:
        try:
            from src.primary.apps.swaparr.decypharr_capacity import reserve_slot
            reserve_slot(context.decypharr_config or {})
        except Exception:
            pass
    _release_shared(context)


def claim_search(item_keys: Union[str, Iterable[str]]) -> bool:
    """Atomically reserve every media item in a command, blocking partial batches."""
    context: Optional[DispatchContext] = getattr(_local, "context", None)
    if context is None:
        return True
    keys = [item_keys] if isinstance(item_keys, str) else list(item_keys)
    keys = list(dict.fromkeys(str(item_key) for item_key in keys))
    claimed = get_pipeline_state().claim_candidates(
        context.app_type, context.instance_name, keys,
        cooldown_seconds=max(300, int(context.redispatch_wait)),
    )
    context.current_item_keys = keys if claimed else []
    if not claimed:
        context.logger.info("Search deferred for %s: one or more items already unresolved or cooling down: %s",
                            context.instance_name, ", ".join(keys))
    return claimed


def finish_search_claim(state: str, command_id=None, cooldown_seconds: Optional[int] = None) -> None:
    context: Optional[DispatchContext] = getattr(_local, "context", None)
    if context is None:
        return
    if not context.current_item_keys:
        _release_shared(context)
        return
    get_pipeline_state().transition_items(
        context.app_type, context.instance_name, context.current_item_keys, state,
        command_id=command_id, cooldown_seconds=cooldown_seconds,
    )
    if state in {"completed", "no_grab", "failed", "timed_out"}:
        context.current_item_keys = []
    _release_shared(context)


def mark_command(command_id, state: str, cooldown_seconds: Optional[int] = None) -> bool:
    context: Optional[DispatchContext] = getattr(_local, "context", None)
    if context is None:
        return False
    return get_pipeline_state().transition_command(
        context.app_type, str(command_id), state, cooldown_seconds=cooldown_seconds,
    )

"""Queue-aware search dispatch for Sonarr and Radarr.

The dispatcher is configured once per instance worker thread.  Search API helpers use
it immediately before POSTing a command, so every missing/upgrade mode shares the
same queue and pacing rules without changing stateful processed-item retention.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

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
    recent_submissions: list = field(default_factory=list)
    last_submission: float = 0.0
    queue_status: Optional[Callable[[], tuple]] = None
    poll_interval: float = 5.0
    current_item_key: Optional[str] = None


_local = threading.local()
_contexts: Dict[tuple, DispatchContext] = {}
_lock = threading.RLock()
_RECENT_GRACE_SECONDS = 120.0


def configure_dispatch(app_type: str, instance_name: str, settings: dict,
                       queue_size: Callable[[], int], active_searches: Callable[[], int],
                       stop_check: Callable[[], bool], logger,
                       queue_cache_name: Optional[str] = None) -> DispatchContext:
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
    key = (app_type, str(instance_name))
    with _lock:
        previous = _contexts.get(key)
        recent = list(previous.recent_submissions) if previous else []
        last = previous.last_submission if previous else 0.0
        cache_name = str(queue_cache_name or instance_name)

        def _fetch_status():
            queue = queue_size()
            active = active_searches()
            if queue < 0 or active < 0:
                raise RuntimeError("queue/command status unavailable")
            return {"queue": queue, "active": active}

        def _queue_status():
            value = get_pipeline_state().observe_queue(app_type, cache_name, _fetch_status)
            # Swaparr may have just populated the shared cache with normalized queue
            # records. Reuse that queue observation and only poll the command endpoint.
            if isinstance(value, list):
                active = active_searches()
                return (len(value), active if active >= 0 else -1)
            if not isinstance(value, dict):
                return -1, -1
            return int(value.get("queue", -1)), int(value.get("active", -1))

        context = DispatchContext(app_type, str(instance_name), target, maximum,
                                  interval, redispatch, stop_check, queue_size,
                                  active_searches, logger, recent, last,
                                  queue_status=_queue_status)
        _contexts[key] = context
    _local.context = context
    logger.info(
        "Queue dispatch enabled for %s: target=%d, hard ceiling=%s, minimum interval=%ds",
        instance_name, target, "disabled" if maximum < 0 else maximum, int(interval)
    )
    return context


def clear_dispatch() -> None:
    _local.context = None


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


def acquire_dispatch_slot() -> bool:
    """Wait responsively for one dispatch slot; return False when safely deferred."""
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
        if occupancy is None:
            # A configured hard ceiling must never be bypassed when queue state is unknown.
            if context.max_queue_size >= 0:
                get_pipeline_state().set_runtime(
                    context.app_type, context.instance_name, slots_used=None,
                    slots_target=context.target_depth, queue=queue, active_searches=active,
                    pause_reason="queue/command status unavailable while hard ceiling is enabled",
                )
                context.logger.warning(
                    "Queue dispatch deferred for %s: queue/command status unavailable and hard ceiling is enabled",
                    context.instance_name,
                )
                return False
            last_reason = "queue/command status unavailable"
            context.poll_interval = min(300.0, max(30.0, context.poll_interval * 2.0))
        elif context.max_queue_size >= 0 and queue >= context.max_queue_size:
            reason = f"download queue {queue} reached hard ceiling {context.max_queue_size}"
            get_pipeline_state().set_runtime(
                context.app_type, context.instance_name, slots_used=occupancy,
                slots_target=context.target_depth, queue=queue, active_searches=active,
                pause_reason=reason,
            )
            context.logger.info(
                "Queue dispatch deferred for %s: queue=%d reached hard ceiling=%d",
                context.instance_name, queue, context.max_queue_size,
            )
            return False
        elif occupancy >= context.target_depth:
            last_reason = (f"effective occupancy {occupancy}/{context.target_depth} "
                           f"(queue={queue}, active searches={active}, recent reservations={recent})")
            context.poll_interval = 5.0
        else:
            context.poll_interval = min(30.0, max(5.0, context.poll_interval * 1.5))
            interval_left = context.minimum_interval - (now - context.last_submission)
            if interval_left <= 0:
                get_pipeline_state().set_runtime(
                    context.app_type, context.instance_name, slots_used=occupancy,
                    slots_target=context.target_depth, queue=queue, active_searches=active,
                    pause_reason=None,
                )
                context.logger.info(
                    "Queue dispatch slot available for %s: occupancy=%d/%d "
                    "(queue=%d, active searches=%d, recent reservations=%d)",
                    context.instance_name, occupancy, context.target_depth,
                    queue, active, recent,
                )
                return True
            last_reason = f"minimum dispatch interval ({interval_left:.1f}s remaining)"

        remaining = deadline - now
        if remaining <= 0:
            context.logger.info("Queue dispatch paused for %s after %s; next cycle will retry",
                                context.instance_name, last_reason)
            return False
        # Bounded polling: never faster than once per second and normally at the configured interval.
        sleep_for = min(remaining, context.poll_interval)
        get_pipeline_state().set_runtime(
            context.app_type, context.instance_name,
            slots_used=occupancy, slots_target=context.target_depth,
            queue=queue, active_searches=active, pause_reason=last_reason,
        )
        context.logger.debug("Queue dispatch waiting %.1fs for %s: %s",
                             sleep_for, context.instance_name, last_reason)
        end = time.monotonic() + sleep_for
        while time.monotonic() < end:
            if context.stop_check():
                return False
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


def claim_search(item_key: str) -> bool:
    """Atomically reserve an item before dispatch, blocking unresolved duplicates."""
    context: Optional[DispatchContext] = getattr(_local, "context", None)
    if context is None:
        return True
    claimed = get_pipeline_state().claim_candidate(
        context.app_type, context.instance_name, str(item_key),
        cooldown_seconds=max(300, int(context.redispatch_wait)),
    )
    context.current_item_key = str(item_key) if claimed else None
    if not claimed:
        context.logger.info("Search deferred for %s: item %s already unresolved or cooling down",
                            context.instance_name, item_key)
    return claimed


def finish_search_claim(state: str, command_id=None, cooldown_seconds: Optional[int] = None) -> None:
    context: Optional[DispatchContext] = getattr(_local, "context", None)
    if context is None or not context.current_item_key:
        return
    get_pipeline_state().transition(
        context.app_type, context.instance_name, context.current_item_key, state,
        command_id=command_id, cooldown_seconds=cooldown_seconds,
    )
    if state in {"completed", "no_grab", "failed", "timed_out"}:
        context.current_item_key = None


def mark_command(command_id, state: str, cooldown_seconds: Optional[int] = None) -> bool:
    context: Optional[DispatchContext] = getattr(_local, "context", None)
    if context is None:
        return False
    return get_pipeline_state().transition_command(
        context.app_type, str(command_id), state, cooldown_seconds=cooldown_seconds,
    )

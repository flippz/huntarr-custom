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


_local = threading.local()
_contexts: Dict[tuple, DispatchContext] = {}
_lock = threading.RLock()
_RECENT_GRACE_SECONDS = 120.0


def configure_dispatch(app_type: str, instance_name: str, settings: dict,
                       queue_size: Callable[[], int], active_searches: Callable[[], int],
                       stop_check: Callable[[], bool], logger) -> DispatchContext:
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
        context = DispatchContext(app_type, str(instance_name), target, maximum,
                                  interval, redispatch, stop_check, queue_size,
                                  active_searches, logger, recent, last)
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
                context.logger.warning(
                    "Queue dispatch deferred for %s: queue/command status unavailable and hard ceiling is enabled",
                    context.instance_name,
                )
                return False
            last_reason = "queue/command status unavailable"
        elif context.max_queue_size >= 0 and queue >= context.max_queue_size:
            context.logger.info(
                "Queue dispatch deferred for %s: queue=%d reached hard ceiling=%d",
                context.instance_name, queue, context.max_queue_size,
            )
            return False
        elif occupancy >= context.target_depth:
            last_reason = (f"effective occupancy {occupancy}/{context.target_depth} "
                           f"(queue={queue}, active searches={active}, recent reservations={recent})")
        else:
            interval_left = context.minimum_interval - (now - context.last_submission)
            if interval_left <= 0:
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
        sleep_for = min(remaining, max(1.0, min(context.minimum_interval, 10.0)))
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

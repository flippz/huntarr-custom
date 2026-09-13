"""Process-local prompt wake signals for optional Starr webhooks."""

from __future__ import annotations

import datetime
import threading
from collections import defaultdict
from typing import Optional, Set

_lock = threading.RLock()
_events = defaultdict(threading.Event)
_pending = defaultdict(set)


def request_wake(app_type: str, instance_name: str) -> None:
    """Mark one instance due and wake its app loop without changing processed-item state."""
    app_type, instance_name = str(app_type), str(instance_name)
    with _lock:
        _pending[app_type].add(instance_name)
        _events[app_type].set()
    try:
        from src.primary.apps._common.queue_dispatch import invalidate_dispatch_observation
        invalidate_dispatch_observation(app_type, instance_name)
    except Exception:
        pass
    _mark_due(app_type, {instance_name})


def _mark_due(app_type: str, instances: Set[str]) -> None:
    try:
        from src.primary.utils.database import get_database
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        db = get_database()
        for instance_name in instances:
            db.set_sleep_data_per_instance(
                app_type, instance_name, next_cycle_time=now, cycle_lock=False,
            )
    except Exception:
        # The process-local event still wakes dispatch/sleep. Reconciliation polling remains.
        pass


def is_wake_pending(app_type: str) -> bool:
    return _events[str(app_type)].is_set()


def consume_wakes(app_type: str) -> Set[str]:
    app_type = str(app_type)
    with _lock:
        pending = set(_pending.pop(app_type, set()))
        _events[app_type].clear()
    if pending:
        _mark_due(app_type, pending)
    return pending


def wait(app_type: str, timeout: float) -> bool:
    return _events[str(app_type)].wait(max(0.0, float(timeout)))


def reset() -> None:
    with _lock:
        for event in _events.values():
            event.clear()
        _pending.clear()

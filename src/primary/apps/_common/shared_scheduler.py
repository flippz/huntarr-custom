"""Small thread-safe weighted scheduler shared only by Sonarr and Radarr dispatchers."""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Callable, Dict, Optional, Tuple


class WeightedDispatchScheduler:
    """Serialize contending Starr submissions with weighted round-robin fairness.

    A grant remains held until the caller reports submission success/failure. This closes
    the telemetry race where two workers see the same final shared client slot.
    """

    def __init__(self, monotonic=time.monotonic):
        self._clock = monotonic
        self._condition = threading.Condition(threading.RLock())
        self._weights: "OrderedDict[Tuple[str, str], int]" = OrderedDict()
        self._waiting: Dict[Tuple[str, str], int] = {}
        self._ring = []
        self._cursor = 0
        self._holder: Optional[Tuple[str, str]] = None

    @staticmethod
    def _key(app_type: str, instance_name: str) -> Tuple[str, str]:
        return str(app_type), str(instance_name)

    def configure(self, app_type: str, instance_name: str, weight: int = 1) -> None:
        key = self._key(app_type, instance_name)
        weight = min(100, max(1, int(weight or 1)))
        with self._condition:
            if self._weights.get(key) != weight:
                self._weights[key] = weight
                self._rebuild_ring()
            self._condition.notify_all()

    def _rebuild_ring(self) -> None:
        old_next = self._ring[self._cursor % len(self._ring)] if self._ring else None
        self._ring = [key for key, weight in self._weights.items() for _ in range(weight)]
        if old_next in self._ring:
            self._cursor = self._ring.index(old_next)
        elif self._ring:
            self._cursor %= len(self._ring)
        else:
            self._cursor = 0

    def _next_waiter(self) -> Optional[Tuple[str, str]]:
        if not self._ring:
            return None
        for offset in range(len(self._ring)):
            index = (self._cursor + offset) % len(self._ring)
            key = self._ring[index]
            if self._waiting.get(key, 0) > 0:
                self._cursor = index
                return key
        return None

    def acquire(self, app_type: str, instance_name: str, timeout: float,
                stop_check: Callable[[], bool]) -> bool:
        key = self._key(app_type, instance_name)
        deadline = self._clock() + max(0.0, float(timeout))
        with self._condition:
            if key not in self._weights:
                self._weights[key] = 1
                self._rebuild_ring()
            self._waiting[key] = self._waiting.get(key, 0) + 1
            try:
                while True:
                    if stop_check():
                        return False
                    if self._holder is None and self._next_waiter() == key:
                        self._holder = key
                        return True
                    remaining = deadline - self._clock()
                    if remaining <= 0:
                        return False
                    self._condition.wait(min(0.5, remaining))
            finally:
                self._waiting[key] -= 1
                if self._waiting[key] <= 0:
                    self._waiting.pop(key, None)

    def release(self, app_type: str, instance_name: str) -> None:
        key = self._key(app_type, instance_name)
        with self._condition:
            if self._holder != key:
                return
            if self._ring:
                # Consume exactly one weighted turn. Repeated ring entries implement weights.
                self._cursor = (self._cursor + 1) % len(self._ring)
            self._holder = None
            self._condition.notify_all()

    def waiting_reason(self, app_type: str, instance_name: str) -> Optional[str]:
        key = self._key(app_type, instance_name)
        with self._condition:
            if self._holder is not None and self._holder != key:
                owner = f"{self._holder[0]}/{self._holder[1]}"
                return f"waiting for shared Sonarr/Radarr capacity (current grant: {owner})"
            next_key = self._next_waiter()
            if next_key is not None and next_key != key:
                return f"waiting for weighted shared capacity turn (next: {next_key[0]}/{next_key[1]})"
            return None

    def reset(self) -> None:
        """Test/support reset; does not affect durable lifecycle state."""
        with self._condition:
            self._weights.clear()
            self._waiting.clear()
            self._ring.clear()
            self._cursor = 0
            self._holder = None
            self._condition.notify_all()


_scheduler = WeightedDispatchScheduler()


def get_shared_scheduler() -> WeightedDispatchScheduler:
    return _scheduler

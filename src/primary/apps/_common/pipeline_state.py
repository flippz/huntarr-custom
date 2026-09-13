"""Shared, crash-safe queue/search pipeline state for Huntarr and Swaparr."""

from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, Optional

LIFECYCLE_STATES = {
    "candidate", "search_submitted", "command_complete", "grabbed",
    "downloading", "imported", "completed", "no_grab", "failed", "timed_out",
}
UNRESOLVED_STATES = {
    "candidate", "search_submitted", "command_complete", "grabbed",
    "downloading", "imported",
}
TERMINAL_STATES = LIFECYCLE_STATES - UNRESOLVED_STATES


class PipelineState:
    """Coordinates queue observations and serialized lifecycle mutations.

    Queue observations are process-local and single-flight. Lifecycle rows are durable in
    SQLite; claiming a candidate and checking for unresolved/cooldown rows happen in one
    transaction, so concurrent workers and restarts cannot submit the same item twice.
    """

    def __init__(self, db=None, clock: Callable[[], float] = time.time,
                 monotonic: Callable[[], float] = time.monotonic):
        self._db = db
        self._clock = clock
        self._monotonic = monotonic
        self._lock = threading.RLock()
        self._queue: Dict[tuple, dict] = {}
        self._runtime: Dict[tuple, dict] = {}

    @property
    def db(self):
        if self._db is None:
            from src.primary.utils.database import get_database
            self._db = get_database()
        return self._db

    def observe_queue(self, app_type: str, instance_name: str, fetch: Callable[[], Any],
                      force: bool = False) -> Any:
        """Return a cached queue observation or fetch it once for all local consumers.

        Healthy observations use an adaptive 5..30 second interval (busy queues refresh
        fastest, idle queues progressively relax). Failures retain the last good value and
        back off from 30 seconds up to five minutes.
        """
        key = (str(app_type), str(instance_name))
        now = self._monotonic()
        with self._lock:
            entry = self._queue.setdefault(key, {
                "value": None, "next_poll": 0.0, "healthy": False,
                "healthy_interval": 5.0, "unhealthy_interval": 30.0,
            })
            if not force and entry["value"] is not None and now < entry["next_poll"]:
                return entry["value"]
            try:
                value = fetch()
                if value is None:
                    raise RuntimeError("queue status unavailable")
                busy = self._is_busy(value)
                interval = 5.0 if busy else min(30.0, max(5.0, entry["healthy_interval"] * 1.5))
                entry.update(value=value, healthy=True, healthy_interval=interval,
                             unhealthy_interval=30.0, next_poll=now + interval,
                             error=None, observed_at=self._clock())
                return value
            except Exception as exc:
                interval = min(300.0, max(30.0, entry["unhealthy_interval"] * 2.0))
                entry.update(healthy=False, unhealthy_interval=interval,
                             next_poll=now + interval, error=str(exc))
                return entry["value"]

    @staticmethod
    def _is_busy(value: Any) -> bool:
        if isinstance(value, dict):
            if "queue" in value or "active" in value:
                return int(value.get("queue", 0) or 0) + int(value.get("active", 0) or 0) > 0
            records = value.get("records")
            return bool(records)
        if isinstance(value, (list, tuple, set)):
            return bool(value)
        if isinstance(value, (int, float)):
            return value > 0
        return False

    def publish_queue(self, app_type: str, instance_name: str, value: Any) -> None:
        """Publish an already-fetched Swaparr observation for Huntarr reuse."""
        key = (str(app_type), str(instance_name))
        now = self._monotonic()
        interval = 5.0 if self._is_busy(value) else 30.0
        with self._lock:
            self._queue[key] = {
                "value": value, "next_poll": now + interval, "healthy": True,
                "healthy_interval": interval, "unhealthy_interval": 30.0,
                "observed_at": self._clock(), "error": None,
            }

    def set_runtime(self, app_type: str, instance_name: str, **values) -> None:
        with self._lock:
            state = self._runtime.setdefault((str(app_type), str(instance_name)), {})
            state.update(values)
            state["updated_at"] = self._clock()

    def runtime(self, app_type: str, instance_name: str) -> dict:
        with self._lock:
            return dict(self._runtime.get((str(app_type), str(instance_name)), {}))

    def claim_candidate(self, app_type: str, instance_name: str, item_key: str,
                        cooldown_seconds: int = 300, unresolved_timeout: int = 21600,
                        metadata: Optional[str] = None) -> bool:
        now = int(self._clock())
        with self.db.get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT state, updated_at_epoch, cooldown_until_epoch FROM pipeline_items "
                "WHERE app_type=? AND instance_name=? AND item_key=?",
                (str(app_type), str(instance_name), str(item_key)),
            ).fetchone()
            if row:
                state, updated_at, cooldown_until = row
                if state in UNRESOLVED_STATES and now - int(updated_at or now) < unresolved_timeout:
                    return False
                if int(cooldown_until or 0) > now:
                    return False
                conn.execute(
                    "UPDATE pipeline_items SET state='candidate', command_id=NULL, metadata=?, "
                    "cooldown_until_epoch=?, updated_at_epoch=?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE app_type=? AND instance_name=? AND item_key=?",
                    (metadata, now + max(0, int(cooldown_seconds)), now,
                     str(app_type), str(instance_name), str(item_key)),
                )
            else:
                conn.execute(
                    "INSERT INTO pipeline_items(app_type,instance_name,item_key,state,metadata,"
                    "cooldown_until_epoch,updated_at_epoch) VALUES(?,?,?,'candidate',?,?,?)",
                    (str(app_type), str(instance_name), str(item_key), metadata,
                     now + max(0, int(cooldown_seconds)), now),
                )
            conn.execute(
                "INSERT INTO pipeline_item_events(app_type,instance_name,item_key,state,metadata,occurred_at_epoch) "
                "VALUES(?,?,?,'candidate',?,?)",
                (str(app_type), str(instance_name), str(item_key), metadata, now),
            )
        return True

    def transition(self, app_type: str, instance_name: str, item_key: str, state: str,
                   command_id: Optional[str] = None, cooldown_seconds: Optional[int] = None,
                   metadata: Optional[str] = None) -> bool:
        if state not in LIFECYCLE_STATES:
            raise ValueError(f"invalid pipeline lifecycle state: {state}")
        now = int(self._clock())
        cooldown = now + max(0, int(cooldown_seconds or 0)) if cooldown_seconds is not None else None
        with self.db.get_connection() as conn:
            cursor = conn.execute(
                "UPDATE pipeline_items SET state=?, command_id=COALESCE(?,command_id), "
                "metadata=COALESCE(?,metadata), cooldown_until_epoch=COALESCE(?,cooldown_until_epoch), "
                "updated_at_epoch=?, updated_at=CURRENT_TIMESTAMP "
                "WHERE app_type=? AND instance_name=? AND item_key=?",
                (state, None if command_id is None else str(command_id), metadata, cooldown, now,
                 str(app_type), str(instance_name), str(item_key)),
            )
            if cursor.rowcount == 1:
                conn.execute(
                    "INSERT INTO pipeline_item_events(app_type,instance_name,item_key,state,command_id,metadata,occurred_at_epoch) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (str(app_type), str(instance_name), str(item_key), state,
                     None if command_id is None else str(command_id), metadata, now),
                )
            return cursor.rowcount == 1

    def transition_command(self, app_type: str, command_id: str, state: str,
                           cooldown_seconds: Optional[int] = None) -> bool:
        if state not in LIFECYCLE_STATES:
            raise ValueError(f"invalid pipeline lifecycle state: {state}")
        now = int(self._clock())
        cooldown = now + max(0, int(cooldown_seconds or 0)) if cooldown_seconds is not None else None
        with self.db.get_connection() as conn:
            cursor = conn.execute(
                "UPDATE pipeline_items SET state=?, cooldown_until_epoch=COALESCE(?,cooldown_until_epoch), "
                "updated_at_epoch=?, updated_at=CURRENT_TIMESTAMP WHERE app_type=? AND command_id=?",
                (state, cooldown, now, str(app_type), str(command_id)),
            )
            if cursor.rowcount:
                conn.execute(
                    "INSERT INTO pipeline_item_events(app_type,instance_name,item_key,state,command_id,occurred_at_epoch) "
                    "SELECT app_type,instance_name,item_key,?,command_id,? FROM pipeline_items "
                    "WHERE app_type=? AND command_id=?",
                    (state, now, str(app_type), str(command_id)),
                )
            return cursor.rowcount > 0


_pipeline_state = PipelineState()


def get_pipeline_state() -> PipelineState:
    return _pipeline_state

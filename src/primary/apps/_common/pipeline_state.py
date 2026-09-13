"""Shared, crash-safe queue/search pipeline state for Huntarr and Swaparr."""

from __future__ import annotations

import copy
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

    @staticmethod
    def _normalize_queue(value: Any) -> dict:
        """Convert every producer payload to the cache's stable snapshot contract."""
        if isinstance(value, (list, tuple)):
            records = list(value)
            return {"records": records, "queue": len(records), "active": None}
        if isinstance(value, dict):
            records = value.get("records")
            records = list(records) if isinstance(records, (list, tuple)) else None
            queue = value.get("queue", value.get("totalRecords"))
            if queue is None and records is not None:
                queue = len(records)
            try:
                queue = int(queue) if queue is not None else None
            except (TypeError, ValueError):
                queue = None
            active = value.get("active")
            try:
                active = int(active) if active is not None else None
            except (TypeError, ValueError):
                active = None
            return {"records": records, "queue": queue, "active": active}
        if isinstance(value, (int, float)):
            return {"records": None, "queue": int(value), "active": None}
        raise RuntimeError("invalid queue observation payload")

    @staticmethod
    def _queue_signature(snapshot: dict) -> tuple:
        records = snapshot.get("records")
        record_ids = None
        if records is not None:
            record_ids = tuple(
                str(record.get("id", record.get("download_id", index)))
                if isinstance(record, dict) else str(record)
                for index, record in enumerate(records)
            )
        return snapshot.get("queue"), snapshot.get("active"), record_ids

    def observe_queue(self, app_type: str, instance_name: str, fetch: Callable[[], Any],
                      force: bool = False, require_records: bool = False) -> dict:
        """Return one canonical queue snapshot shared by dispatch and Swaparr.

        Producers may return normalized records or a queue/active summary; consumers always
        receive ``records``, ``queue``, ``active`` and health metadata. A records consumer
        never receives an incompatible summary-only hit. Healthy stable observations relax
        from 5 to 30 seconds, while a queue/capacity change resets polling to five seconds.
        Failures retain the last value but mark it unhealthy and preserve 30..300s backoff.
        """
        key = (str(app_type), str(instance_name))
        now = self._monotonic()
        with self._lock:
            entry = self._queue.setdefault(key, {
                "value": None, "next_poll": 0.0, "healthy": False,
                "healthy_interval": 5.0, "unhealthy_interval": 30.0,
            })
            cached = entry["value"]
            compatible = cached is not None and (not require_records or cached.get("records") is not None)
            if (not force and now < entry["next_poll"]
                    and (compatible or not entry["healthy"])):
                result = copy.deepcopy(cached) if cached is not None else {
                    "records": None, "queue": None, "active": None,
                }
                result.update(healthy=entry["healthy"], observed_at=entry.get("observed_at"),
                              error=entry.get("error"),
                              poll_interval=(entry["healthy_interval"] if entry["healthy"]
                                             else entry["unhealthy_interval"]))
                return result
            try:
                fetched = fetch()
                if fetched is None:
                    raise RuntimeError("queue status unavailable")
                value = self._normalize_queue(fetched)
                if require_records and value["records"] is None:
                    raise RuntimeError("queue records unavailable")
                previous_signature = (self._queue_signature(cached) if cached is not None else None)
                changed = previous_signature is not None and previous_signature != self._queue_signature(value)
                busy = self._is_busy(value)
                if changed:
                    interval = 5.0
                elif previous_signature is None:
                    interval = 5.0 if busy else 30.0
                else:
                    interval = min(30.0, max(5.0, entry["healthy_interval"] * 1.5))
                entry.update(value=value, healthy=True, healthy_interval=interval,
                             unhealthy_interval=30.0, next_poll=now + interval,
                             error=None, observed_at=self._clock())
                result = copy.deepcopy(value)
                result.update(healthy=True, observed_at=entry["observed_at"], error=None,
                              poll_interval=interval)
                return result
            except Exception as exc:
                interval = min(300.0, max(30.0, entry["unhealthy_interval"] * 2.0))
                entry.update(healthy=False, unhealthy_interval=interval,
                             next_poll=now + interval, error=str(exc))
                result = copy.deepcopy(entry["value"]) if entry["value"] is not None else {
                    "records": None, "queue": None, "active": None,
                }
                result.update(healthy=False, observed_at=entry.get("observed_at"),
                              error=str(exc), poll_interval=interval)
                return result

    @staticmethod
    def _is_busy(value: Any) -> bool:
        if isinstance(value, dict):
            if value.get("queue") is not None or value.get("active") is not None:
                return int(value.get("queue") or 0) + int(value.get("active") or 0) > 0
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
        with self._lock:
            value = self._normalize_queue(value)
            previous = self._queue.get(key)
            previous_value = previous.get("value") if previous else None
            changed = (previous_value is not None and
                       self._queue_signature(previous_value) != self._queue_signature(value))
            if changed:
                interval = 5.0
            elif previous:
                interval = min(30.0, max(5.0, previous["healthy_interval"] * 1.5))
            else:
                interval = 5.0 if self._is_busy(value) else 30.0
            self._queue[key] = {
                "value": value, "next_poll": now + interval, "healthy": True,
                "healthy_interval": interval, "unhealthy_interval": 30.0,
                "observed_at": self._clock(), "error": None,
            }

    def merge_queue(self, app_type: str, instance_name: str, **values) -> None:
        """Fill compatible fields (normally command capacity) without another queue poll."""
        key = (str(app_type), str(instance_name))
        with self._lock:
            entry = self._queue.get(key)
            if not entry or entry.get("value") is None:
                return
            for name in ("queue", "active", "records"):
                if name in values:
                    entry["value"][name] = copy.deepcopy(values[name])

    def invalidate_queue(self, app_type: str, instance_name: str) -> None:
        """Make the next reconciliation observation prompt without discarding safe cache data."""
        key = (str(app_type), str(instance_name))
        with self._lock:
            entry = self._queue.get(key)
            if entry:
                entry["next_poll"] = 0.0

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
        return self.claim_candidates(
            app_type, instance_name, [item_key], cooldown_seconds=cooldown_seconds,
            unresolved_timeout=unresolved_timeout, metadata=metadata,
        )

    def claim_candidates(self, app_type: str, instance_name: str, item_keys,
                         cooldown_seconds: int = 300, unresolved_timeout: int = 21600,
                         metadata: Optional[str] = None) -> bool:
        """Atomically claim every distinct media item in a batch, or claim none."""
        keys = list(dict.fromkeys(str(item_key) for item_key in item_keys))
        if not keys:
            return False
        now = int(self._clock())
        with self.db.get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = {}
            for item_key in keys:
                row = conn.execute(
                    "SELECT state, updated_at_epoch, cooldown_until_epoch FROM pipeline_items "
                    "WHERE app_type=? AND instance_name=? AND item_key=?",
                    (str(app_type), str(instance_name), item_key),
                ).fetchone()
                rows[item_key] = row
                if row:
                    state, updated_at, cooldown_until = row
                    if state in UNRESOLVED_STATES and now - int(updated_at or now) < unresolved_timeout:
                        return False
                    if int(cooldown_until or 0) > now:
                        return False
            for item_key in keys:
                if rows[item_key]:
                    conn.execute(
                        "UPDATE pipeline_items SET state='candidate', command_id=NULL, metadata=?, "
                        "cooldown_until_epoch=?, updated_at_epoch=?, updated_at=CURRENT_TIMESTAMP "
                        "WHERE app_type=? AND instance_name=? AND item_key=?",
                        (metadata, now + max(0, int(cooldown_seconds)), now,
                         str(app_type), str(instance_name), item_key),
                    )
                else:
                    conn.execute(
                        "INSERT INTO pipeline_items(app_type,instance_name,item_key,state,metadata,"
                        "cooldown_until_epoch,updated_at_epoch) VALUES(?,?,?,'candidate',?,?,?)",
                        (str(app_type), str(instance_name), item_key, metadata,
                         now + max(0, int(cooldown_seconds)), now),
                    )
                conn.execute(
                    "INSERT INTO pipeline_item_events(app_type,instance_name,item_key,state,metadata,occurred_at_epoch) "
                    "VALUES(?,?,?,'candidate',?,?)",
                    (str(app_type), str(instance_name), item_key, metadata, now),
                )
        return True

    def transition(self, app_type: str, instance_name: str, item_key: str, state: str,
                   command_id: Optional[str] = None, cooldown_seconds: Optional[int] = None,
                   metadata: Optional[str] = None) -> bool:
        return self.transition_items(
            app_type, instance_name, [item_key], state, command_id=command_id,
            cooldown_seconds=cooldown_seconds, metadata=metadata,
        )

    def transition_items(self, app_type: str, instance_name: str, item_keys, state: str,
                         command_id: Optional[str] = None,
                         cooldown_seconds: Optional[int] = None,
                         metadata: Optional[str] = None) -> bool:
        """Transition every existing row in a media batch in one transaction."""
        if state not in LIFECYCLE_STATES:
            raise ValueError(f"invalid pipeline lifecycle state: {state}")
        keys = list(dict.fromkeys(str(item_key) for item_key in item_keys))
        if not keys:
            return False
        now = int(self._clock())
        cooldown = now + max(0, int(cooldown_seconds or 0)) if cooldown_seconds is not None else None
        with self.db.get_connection() as conn:
            updated = 0
            for item_key in keys:
                cursor = conn.execute(
                    "UPDATE pipeline_items SET state=?, command_id=COALESCE(?,command_id), "
                    "metadata=COALESCE(?,metadata), cooldown_until_epoch=COALESCE(?,cooldown_until_epoch), "
                    "updated_at_epoch=?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE app_type=? AND instance_name=? AND item_key=?",
                    (state, None if command_id is None else str(command_id), metadata, cooldown, now,
                     str(app_type), str(instance_name), item_key),
                )
                if cursor.rowcount == 1:
                    updated += 1
                    conn.execute(
                        "INSERT INTO pipeline_item_events(app_type,instance_name,item_key,state,command_id,metadata,occurred_at_epoch) "
                        "VALUES(?,?,?,?,?,?,?)",
                        (str(app_type), str(instance_name), item_key, state,
                         None if command_id is None else str(command_id), metadata, now),
                    )
            return updated == len(keys)

    def observe_transition(self, app_type: str, instance_name: str, item_key: str,
                           state: str, cooldown_seconds: Optional[int] = None,
                           metadata: Optional[str] = None) -> bool:
        """Advance an existing unresolved item from external queue/history evidence.

        This deliberately cannot revive or overwrite a terminal row. It lets Swaparr
        advance Huntarr's unresolved claim without treating the observation as a new
        search candidate, preserving duplicate-search protection.
        """
        if state not in LIFECYCLE_STATES:
            raise ValueError(f"invalid pipeline lifecycle state: {state}")
        now = int(self._clock())
        cooldown = now + max(0, int(cooldown_seconds or 0)) if cooldown_seconds is not None else None
        unresolved = tuple(sorted(UNRESOLVED_STATES))
        placeholders = ",".join("?" for _ in unresolved)
        with self.db.get_connection() as conn:
            cursor = conn.execute(
                "UPDATE pipeline_items SET state=?, metadata=COALESCE(?,metadata), "
                "cooldown_until_epoch=COALESCE(?,cooldown_until_epoch), updated_at_epoch=?, "
                "updated_at=CURRENT_TIMESTAMP WHERE app_type=? AND instance_name=? AND item_key=? "
                f"AND state IN ({placeholders})",
                (state, metadata, cooldown, now, str(app_type), str(instance_name),
                 str(item_key), *unresolved),
            )
            if cursor.rowcount == 1:
                conn.execute(
                    "INSERT INTO pipeline_item_events(app_type,instance_name,item_key,state,metadata,occurred_at_epoch) "
                    "VALUES(?,?,?,?,?,?)",
                    (str(app_type), str(instance_name), str(item_key), state, metadata, now),
                )
            return cursor.rowcount == 1

    def apply_webhook_event(self, app_type: str, instance_name: str, event_id: str,
                            item_keys, state: Optional[str], metadata: Optional[str] = None,
                            cooldown_seconds: Optional[int] = None) -> dict:
        """Durably deduplicate a webhook and advance only existing unresolved rows.

        Terminal rows are intentionally immutable here. A repeated or late event therefore
        cannot reopen completed/failed/no-grab work or shorten its cooldown.
        """
        if state is not None and state not in LIFECYCLE_STATES:
            raise ValueError(f"invalid pipeline lifecycle state: {state}")
        keys = list(dict.fromkeys(str(key) for key in item_keys if key))
        now = int(self._clock())
        cooldown = now + max(0, int(cooldown_seconds or 0)) if cooldown_seconds is not None else None
        unresolved = tuple(sorted(UNRESOLVED_STATES))
        placeholders = ",".join("?" for _ in unresolved)
        with self.db.get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            receipt = conn.execute(
                "INSERT OR IGNORE INTO starr_webhook_receipts"
                "(event_id,app_type,instance_name,received_at_epoch) VALUES(?,?,?,?)",
                (str(event_id), str(app_type), str(instance_name), now),
            )
            if receipt.rowcount != 1:
                return {"duplicate": True, "transitioned": 0}
            transitioned = 0
            if state is not None:
                for item_key in keys:
                    cursor = conn.execute(
                        "UPDATE pipeline_items SET state=?, metadata=COALESCE(?,metadata), "
                        "cooldown_until_epoch=COALESCE(?,cooldown_until_epoch), updated_at_epoch=?, "
                        "updated_at=CURRENT_TIMESTAMP WHERE app_type=? AND instance_name=? AND item_key=? "
                        f"AND state IN ({placeholders})",
                        (state, metadata, cooldown, now, str(app_type), str(instance_name),
                         item_key, *unresolved),
                    )
                    if cursor.rowcount == 1:
                        transitioned += 1
                        conn.execute(
                            "INSERT INTO pipeline_item_events"
                            "(app_type,instance_name,item_key,state,metadata,occurred_at_epoch) "
                            "VALUES(?,?,?,?,?,?)",
                            (str(app_type), str(instance_name), item_key, state, metadata, now),
                        )
            return {"duplicate": False, "transitioned": transitioned}

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

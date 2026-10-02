"""Persistence for the Sonarr-only slow-download guard.

Follows the same safety shape as ``SeasonPackRepository``: a durable
attempt-start marker is committed through ``Database.dedicated_transaction``
independently, just before the one bounded Sonarr DELETE, while the calling
transaction still holds row locks on the queue item and shared locks on the
scheduler/Live-authorization singleton rows. If that marker exists for a
queue item, this repository never again lets the worker automatically
retry removal of that exact queue record - see ``claim_removal``.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

from ..domain.slow_download import (
    Observation,
    QueueItemState,
    SlowDownloadSettings,
    classify,
)
from .database import Database

DEFAULT_SETTINGS = {
    "monitoring_enabled": True,
    "auto_removal_enabled": False,
    "poll_seconds": 60,
    "initial_grace_minutes": 30,
    "no_progress_window_minutes": 30,
    "no_progress_min_observations": 3,
    "progress_epsilon_bytes": 1 * 1024 * 1024,
    "very_slow_rate_bytes_per_second": 50 * 1024,
    "very_slow_window_minutes": 90,
    "very_slow_min_remaining_bytes": 512 * 1024 * 1024,
    "very_slow_min_observations": 4,
    "strikes_required": 2,
    "remove_from_client": True,
    "blocklist": True,
    "skip_redownload": False,
}

_SETTINGS_COLUMNS = (
    "monitoring_enabled", "auto_removal_enabled", "poll_seconds",
    "initial_grace_minutes", "no_progress_window_minutes", "no_progress_min_observations",
    "progress_epsilon_bytes", "very_slow_rate_bytes_per_second", "very_slow_window_minutes",
    "very_slow_min_remaining_bytes", "very_slow_min_observations", "strikes_required",
    "remove_from_client", "blocklist", "skip_redownload",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value):
    return value.isoformat() if value is not None else None


def settings_to_domain(settings: dict) -> SlowDownloadSettings:
    return SlowDownloadSettings(
        initial_grace_minutes=settings["initial_grace_minutes"],
        no_progress_window_minutes=settings["no_progress_window_minutes"],
        no_progress_min_observations=settings["no_progress_min_observations"],
        progress_epsilon_bytes=settings["progress_epsilon_bytes"],
        very_slow_rate_bytes_per_second=settings["very_slow_rate_bytes_per_second"],
        very_slow_window_minutes=settings["very_slow_window_minutes"],
        very_slow_min_remaining_bytes=settings["very_slow_min_remaining_bytes"],
        very_slow_min_observations=settings["very_slow_min_observations"],
        strikes_required=settings["strikes_required"],
    )


def _state_from_row(row) -> QueueItemState:
    return QueueItemState(
        first_seen_at=row["first_seen_at"],
        last_observed_at=row["last_observed_at"],
        last_sizeleft_bytes=row["last_sizeleft_bytes"],
        last_meaningful_progress_at=row["last_meaningful_progress_at"],
        no_progress_window_started_at=row["no_progress_window_started_at"],
        no_progress_window_start_sizeleft=row["no_progress_window_start_sizeleft"],
        no_progress_window_observations=row["no_progress_window_observations"],
        stall_strike_count=row["stall_strike_count"],
        very_slow_window_started_at=row["very_slow_window_started_at"],
        very_slow_window_start_sizeleft=row["very_slow_window_start_sizeleft"],
        very_slow_window_observations=row["very_slow_window_observations"],
        very_slow_strike_count=row["very_slow_strike_count"],
        classification=row["classification"],
        reason=row["reason"],
    )


def _measured_rate(row):
    """Best-effort current-window throughput for display only - never used
    by classification itself (see app.domain.slow_download)."""
    started = row["very_slow_window_started_at"]
    start_sizeleft = row["very_slow_window_start_sizeleft"]
    if started is None or start_sizeleft is None or row["sizeleft_bytes"] is None:
        return None
    elapsed = (_now() - started).total_seconds()
    if elapsed <= 0:
        return None
    return max(0.0, (start_sizeleft - row["sizeleft_bytes"]) / elapsed)


def _queue_item_dict(row) -> dict:
    progress_percent = None
    if row["size_bytes"] and row["size_bytes"] > 0 and row["sizeleft_bytes"] is not None:
        progress_percent = round(100.0 * (row["size_bytes"] - row["sizeleft_bytes"]) / row["size_bytes"], 1)
    return {
        "id": row["id"], "library_id": row["library_id"], "sonarr_queue_id": row["sonarr_queue_id"],
        "download_id": row["download_id"], "title": row["title"], "status": row["status"],
        "tracked_state": row["tracked_state"], "size_bytes": row["size_bytes"],
        "sizeleft_bytes": row["sizeleft_bytes"], "progress_percent": progress_percent,
        "measured_rate_bytes_per_second": _measured_rate(row),
        "first_seen_at": _iso(row["first_seen_at"]), "last_seen_at": _iso(row["last_seen_at"]),
        "last_observed_at": _iso(row["last_observed_at"]),
        "last_meaningful_progress_at": _iso(row["last_meaningful_progress_at"]),
        "stall_strike_count": row["stall_strike_count"], "very_slow_strike_count": row["very_slow_strike_count"],
        "strike_count": max(row["stall_strike_count"], row["very_slow_strike_count"]),
        "classification": row["classification"], "reason": row["reason"],
        "removed_at": _iso(row["removed_at"]), "removal_outcome": row["removal_outcome"],
        "updated_at": _iso(row["updated_at"]),
    }


class SlowDownloadRepository:
    LOCK_CLASS = 91_743  # Namespace, distinct from SeasonPackRepository.LOCK_CLASS.

    def __init__(self, db: Database):
        self.db = db

    # --- settings ---------------------------------------------------------

    def settings(self, library_id: int) -> dict:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM slow_download_settings WHERE library_id = %s", (library_id,)
            ).fetchone()
        if not row:
            return {"library_id": library_id, "revision": 0, **DEFAULT_SETTINGS}
        return {"library_id": library_id, "revision": row["revision"], **{k: row[k] for k in _SETTINGS_COLUMNS}}

    def update_settings(self, library_id: int, data: dict, *, expected_revision: int | None) -> tuple[dict | None, str | None]:
        current = self.settings(library_id)
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT revision FROM slow_download_settings WHERE library_id = %s FOR UPDATE", (library_id,)
            ).fetchone()
            current_revision = row["revision"] if row else 0
            if expected_revision is not None and expected_revision != current_revision:
                return None, "settings were changed by someone else; reload and try again"
            merged = {**current, **data}
            values = [merged[k] for k in _SETTINGS_COLUMNS]
            conn.execute(
                f"""
                INSERT INTO slow_download_settings (library_id, {', '.join(_SETTINGS_COLUMNS)}, revision, updated_at)
                VALUES (%s, {', '.join(['%s'] * len(_SETTINGS_COLUMNS))}, 1, now())
                ON CONFLICT (library_id) DO UPDATE SET
                    {', '.join(f'{c} = EXCLUDED.{c}' for c in _SETTINGS_COLUMNS)},
                    revision = slow_download_settings.revision + 1,
                    updated_at = now()
                """,
                (library_id, *values),
            )
        return self.settings(library_id), None

    def all_sonarr_settings(self) -> list[dict]:
        """One row per enabled Sonarr library, defaulted if never configured."""
        with self.db.connect() as conn:
            libraries = conn.execute(
                "SELECT id FROM arr_libraries WHERE enabled = TRUE AND type = 'sonarr' ORDER BY id"
            ).fetchall()
        return [self.settings(row["id"]) for row in libraries]

    def last_polled_at(self, library_id: int) -> datetime | None:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT last_polled_at FROM slow_download_settings WHERE library_id = %s", (library_id,)
            ).fetchone()
        return row["last_polled_at"] if row else None

    def record_poll_attempt(self, library_id: int) -> None:
        """Upserts a settings row (with schema defaults) if none exists yet,
        so polling cadence can be tracked before any operator has ever
        opened Settings for this library."""
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO slow_download_settings (library_id, last_polled_at) VALUES (%s, now())
                ON CONFLICT (library_id) DO UPDATE SET last_polled_at = now()
                """,
                (library_id,),
            )

    # --- current queue item tracking --------------------------------------

    def record_observation(self, library_id: int, record: dict, settings: dict, *, observed_at: datetime | None = None) -> dict:
        """Fold one Sonarr queue-details record into durable tracking state.

        Returns the updated, API-safe queue item dict. ``record`` must come
        from ``SonarrClient.get_queue_details`` (already adapter-validated).
        """
        observed_at = observed_at or _now()
        domain_settings = settings_to_domain(settings)
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM slow_download_queue_items
                WHERE library_id = %s AND sonarr_queue_id = %s FOR UPDATE
                """,
                (library_id, record["queue_id"]),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    INSERT INTO slow_download_queue_items (
                        library_id, sonarr_queue_id, download_id, title, status, tracked_state,
                        size_bytes, sizeleft_bytes, first_seen_at, last_seen_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        library_id, record["queue_id"], record.get("download_id"), record.get("title", ""),
                        record.get("status", ""), record.get("tracked_state"), record.get("size"),
                        record.get("sizeleft"), observed_at, observed_at,
                    ),
                ).fetchone()

            state = _state_from_row(row)
            observation = Observation(
                observed_at=observed_at, status=record.get("status", ""),
                size_bytes=record.get("size"), sizeleft_bytes=record.get("sizeleft"),
            )
            last_sizeleft_before = state.last_sizeleft_bytes
            result = classify(domain_settings, state, observation)
            new_state = result.state

            updated = conn.execute(
                """
                UPDATE slow_download_queue_items SET
                    download_id = %s, title = %s, status = %s, tracked_state = %s,
                    size_bytes = %s, sizeleft_bytes = %s, last_seen_at = %s,
                    last_observed_at = %s, last_sizeleft_bytes = %s, last_meaningful_progress_at = %s,
                    no_progress_window_started_at = %s, no_progress_window_start_sizeleft = %s,
                    no_progress_window_observations = %s, stall_strike_count = %s,
                    very_slow_window_started_at = %s, very_slow_window_start_sizeleft = %s,
                    very_slow_window_observations = %s, very_slow_strike_count = %s,
                    classification = %s, reason = %s, updated_at = now()
                WHERE id = %s
                RETURNING *
                """,
                (
                    record.get("download_id"), record.get("title", ""), record.get("status", ""),
                    record.get("tracked_state"), record.get("size"), record.get("sizeleft"), observed_at,
                    new_state.last_observed_at, new_state.last_sizeleft_bytes, new_state.last_meaningful_progress_at,
                    new_state.no_progress_window_started_at, new_state.no_progress_window_start_sizeleft,
                    new_state.no_progress_window_observations, new_state.stall_strike_count,
                    new_state.very_slow_window_started_at, new_state.very_slow_window_start_sizeleft,
                    new_state.very_slow_window_observations, new_state.very_slow_strike_count,
                    new_state.classification, new_state.reason[:1000], row["id"],
                ),
            ).fetchone()

            delta_bytes = None if last_sizeleft_before is None or record.get("sizeleft") is None else (
                last_sizeleft_before - record["sizeleft"]
            )
            elapsed_seconds = None
            if state.last_observed_at is not None:
                elapsed_seconds = (observed_at - state.last_observed_at).total_seconds()
            conn.execute(
                """
                INSERT INTO slow_download_observations (
                    queue_item_id, observed_at, status, size_bytes, sizeleft_bytes,
                    delta_bytes, elapsed_seconds, classification, reason
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["id"], observed_at, record.get("status", ""), record.get("size"), record.get("sizeleft"),
                    delta_bytes, elapsed_seconds, new_state.classification, new_state.reason[:1000],
                ),
            )
            if result.strike_recorded:
                self._record_action(conn, row["id"], library_id, record["queue_id"], "strike", new_state.reason)
            if result.reset_recorded:
                self._record_action(conn, row["id"], library_id, record["queue_id"], "reset", new_state.reason)
            if new_state.classification == "exempt" and state.classification != "exempt":
                self._record_action(conn, row["id"], library_id, record["queue_id"], "exempted", new_state.reason)
        return _queue_item_dict(updated)

    @staticmethod
    def _record_action(conn, queue_item_id: int, library_id: int, sonarr_queue_id: int, action: str, reason: str):
        conn.execute(
            """
            INSERT INTO slow_download_actions (queue_item_id, library_id, sonarr_queue_id, action, reason)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (queue_item_id, library_id, sonarr_queue_id, action, reason[:1000]),
        )

    def clear_disappeared(self, library_id: int, present_queue_ids: set[int]) -> int:
        """Clear tracking for any item no longer present in the live Sonarr
        queue. Disappearance is recorded as its own outcome - never
        conflated with a successful import, which this layer never
        observes."""
        with self.db.connect() as conn:
            query = """
                SELECT id, sonarr_queue_id FROM slow_download_queue_items
                WHERE library_id = %s AND classification NOT IN ('removed', 'ambiguous')
            """
            rows = conn.execute(query, (library_id,)).fetchall()
            stale = [r for r in rows if r["sonarr_queue_id"] not in present_queue_ids]
            for r in stale:
                conn.execute(
                    """
                    UPDATE slow_download_queue_items
                    SET classification = 'removed', reason = 'queue record no longer present in Sonarr queue',
                        removed_at = now(), updated_at = now()
                    WHERE id = %s
                    """,
                    (r["id"],),
                )
                self._record_action(
                    conn, r["id"], library_id, r["sonarr_queue_id"], "cleared_disappeared",
                    "queue record disappeared from Sonarr queue; outcome unknown, not assumed successful",
                )
        return len(stale)

    def list_current(self, library_id: int | None = None, *, limit: int = 200) -> list[dict]:
        limit = max(1, min(int(limit), 500))
        with self.db.connect() as conn:
            if library_id is not None:
                rows = conn.execute(
                    """
                    SELECT * FROM slow_download_queue_items WHERE library_id = %s
                    ORDER BY updated_at DESC LIMIT %s
                    """,
                    (library_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM slow_download_queue_items ORDER BY updated_at DESC LIMIT %s", (limit,)
                ).fetchall()
        return [_queue_item_dict(r) for r in rows]

    def get(self, queue_item_id: int, *, conn=None) -> dict | None:
        if conn is not None:
            row = conn.execute("SELECT * FROM slow_download_queue_items WHERE id = %s", (queue_item_id,)).fetchone()
            return _queue_item_dict(row) if row else None
        with self.db.connect() as owned:
            row = owned.execute("SELECT * FROM slow_download_queue_items WHERE id = %s", (queue_item_id,)).fetchone()
        return _queue_item_dict(row) if row else None

    def recent_actions(self, library_id: int | None = None, *, limit: int = 100) -> list[dict]:
        limit = max(1, min(int(limit), 500))
        with self.db.connect() as conn:
            if library_id is not None:
                rows = conn.execute(
                    """
                    SELECT a.*, q.title FROM slow_download_actions a
                    LEFT JOIN slow_download_queue_items q ON q.id = a.queue_item_id
                    WHERE a.library_id = %s ORDER BY a.occurred_at DESC, a.id DESC LIMIT %s
                    """,
                    (library_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT a.*, q.title FROM slow_download_actions a
                    LEFT JOIN slow_download_queue_items q ON q.id = a.queue_item_id
                    ORDER BY a.occurred_at DESC, a.id DESC LIMIT %s
                    """,
                    (limit,),
                ).fetchall()
        return [
            {
                "id": r["id"], "queue_item_id": r["queue_item_id"], "library_id": r["library_id"],
                "sonarr_queue_id": r["sonarr_queue_id"], "action": r["action"], "reason": r["reason"],
                "title": r["title"], "occurred_at": _iso(r["occurred_at"]),
            }
            for r in rows
        ]

    # --- gated removal: mirrors SeasonPackRepository.authorized_write ------

    @contextmanager
    def authorized_removal(self, queue_item_id: int, expected_generation: int):
        """Linearize the last authorization/settings check with the DELETE.

        Row/shared locks held here keep settings, Live authorization, and
        library identity from changing between the final check and the
        write. Immediately before the caller's DELETE, a separate
        transaction commits the durable attempt marker through
        ``dedicated_transaction`` so a crash after an accepted DELETE is
        provably ambiguous on restart rather than silently retried.
        """
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM slow_download_queue_items WHERE id = %s FOR NO KEY UPDATE", (queue_item_id,)
            ).fetchone()
            if not row or row["classification"] not in ("removal_pending",):
                yield conn, row, "queue item is not in a removal-pending state"
                return
            if conn.execute(
                "SELECT 1 FROM slow_download_removal_attempts WHERE queue_item_id = %s", (queue_item_id,)
            ).fetchone():
                yield conn, dict(row), "a removal attempt was already started for this queue record; manual review required"
                return
            row = dict(row)
            conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", (self.LOCK_CLASS, row["library_id"]))
            scheduler = conn.execute("SELECT mode FROM scheduler_settings WHERE id = 1 FOR SHARE").fetchone()
            live = conn.execute(
                "SELECT authorization_state, authorization_generation FROM live_control WHERE id = 1 FOR SHARE"
            ).fetchone()
            library = conn.execute(
                "SELECT * FROM arr_libraries WHERE id = %s FOR SHARE", (row["library_id"],)
            ).fetchone()
            settings_row = conn.execute(
                "SELECT * FROM slow_download_settings WHERE library_id = %s FOR SHARE", (row["library_id"],)
            ).fetchone()
            error = None
            if scheduler is None or scheduler["mode"] != "live":
                error = "Live mode is not active"
            elif live is None or live["authorization_state"] != "running" or live["authorization_generation"] != expected_generation:
                error = "Live authorization changed before removal"
            elif not library or library["type"] != "sonarr" or library["enabled"] is not True:
                error = "library routing identity changed since evaluation"
            elif not settings_row or not settings_row["auto_removal_enabled"]:
                error = "automatic removal is disabled for this library"
            elif row["classification"] != "removal_pending":
                error = "queue item is no longer removal-pending"
            if error is None:
                with self.db.dedicated_transaction(lock_timeout_seconds=5) as marker_conn:
                    marker_conn.execute(
                        "INSERT INTO slow_download_removal_attempts (queue_item_id, started_at) VALUES (%s, now())",
                        (queue_item_id,),
                    )
                self._record_action(
                    conn, queue_item_id, row["library_id"], row["sonarr_queue_id"],
                    "removal_attempt_started", "durable attempt marker committed before DELETE",
                )
            yield conn, row, error

    def finalize_removal(self, queue_item_id: int, outcome: str, reason: str, *, conn=None) -> dict | None:
        """``outcome`` is one of 'completed', 'rejected', 'ambiguous'."""
        action = {"completed": "removal_completed", "rejected": "removal_rejected", "ambiguous": "removal_ambiguous"}[outcome]
        classification = "removed" if outcome == "completed" else "ambiguous" if outcome == "ambiguous" else "removal_pending"
        if conn is not None:
            row = conn.execute(
                """
                UPDATE slow_download_queue_items
                SET classification = %s, reason = %s, removal_outcome = %s,
                    removed_at = CASE WHEN %s = 'completed' THEN now() ELSE removed_at END, updated_at = now()
                WHERE id = %s RETURNING *
                """,
                (classification, reason[:1000], outcome, outcome, queue_item_id),
            ).fetchone()
            self._record_action(conn, queue_item_id, row["library_id"], row["sonarr_queue_id"], action, reason)
            return _queue_item_dict(row)
        with self.db.connect() as owned:
            row = owned.execute(
                """
                UPDATE slow_download_queue_items
                SET classification = %s, reason = %s, removal_outcome = %s,
                    removed_at = CASE WHEN %s = 'completed' THEN now() ELSE removed_at END, updated_at = now()
                WHERE id = %s RETURNING *
                """,
                (classification, reason[:1000], outcome, outcome, queue_item_id),
            ).fetchone()
            self._record_action(owned, queue_item_id, row["library_id"], row["sonarr_queue_id"], action, reason)
        return _queue_item_dict(row)

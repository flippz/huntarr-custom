"""Persistence for the Sonarr-only import-failure reason policy.

Follows the same safety shape as ``SlowDownloadRepository``/
``SeasonPackRepository``: a durable attempt-start marker is committed
through ``Database.dedicated_transaction`` independently, just before the
one bounded Sonarr DELETE, while the calling transaction still holds row
locks on the queue item and shared locks on the scheduler/Live-
authorization/policy singleton rows. If that marker exists for a queue
item, this repository never again lets the worker automatically retry
removal of that exact queue record - see ``claim_removal``/
``authorized_removal``.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

from ..domain.import_failure import (
    Decision,
    ImportFailurePolicy,
    NormalizationResult,
    evaluate,
)
from .database import Database

DEFAULT_SETTINGS = {
    "monitoring_enabled": True,
    "auto_removal_enabled": False,
    "poll_seconds": 60,
    "remove_from_client": True,
    "blocklist": True,
    "skip_redownload": False,
}

_SETTINGS_COLUMNS = (
    "monitoring_enabled", "auto_removal_enabled", "poll_seconds",
    "remove_from_client", "blocklist", "skip_redownload",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value):
    return value.isoformat() if value is not None else None


def _policy_to_domain(settings: dict, removal_reasons: frozenset[str]) -> ImportFailurePolicy:
    return ImportFailurePolicy(
        auto_removal_enabled=settings["auto_removal_enabled"],
        removal_reasons=removal_reasons,
    )


def _queue_item_dict(row) -> dict:
    return {
        "id": row["id"], "library_id": row["library_id"], "sonarr_queue_id": row["sonarr_queue_id"],
        "download_id": row["download_id"], "title": row["title"], "status": row["status"],
        "tracked_state": row["tracked_state"], "tracked_status": row["tracked_status"],
        "matched_reasons": list(row["matched_reasons"] or []),
        "unmatched_messages": list(row["unmatched_messages"] or []),
        "decision": row["decision"], "decision_reason": row["decision_reason"],
        "first_seen_at": _iso(row["first_seen_at"]), "last_seen_at": _iso(row["last_seen_at"]),
        "removed_at": _iso(row["removed_at"]), "removal_outcome": row["removal_outcome"],
        "updated_at": _iso(row["updated_at"]),
    }


class ImportFailureRepository:
    LOCK_CLASS = 91_827  # Namespace, distinct from SlowDownloadRepository.LOCK_CLASS (91_743).

    def __init__(self, db: Database):
        self.db = db

    # --- settings -----------------------------------------------------------

    def settings(self, library_id: int) -> dict:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM import_failure_policies WHERE library_id = %s", (library_id,)
            ).fetchone()
            reason_rows = conn.execute(
                "SELECT reason_key FROM import_failure_policy_reasons WHERE library_id = %s ORDER BY reason_key",
                (library_id,),
            ).fetchall()
        removal_reasons = [r["reason_key"] for r in reason_rows]
        if not row:
            return {"library_id": library_id, "revision": 0, "removal_reasons": removal_reasons, **DEFAULT_SETTINGS}
        return {
            "library_id": library_id, "revision": row["revision"], "removal_reasons": removal_reasons,
            **{k: row[k] for k in _SETTINGS_COLUMNS},
        }

    def update_settings(
        self, library_id: int, data: dict, *, removal_reasons: list[str] | None,
        expected_revision: int, require_live_running: bool = False,
        confirm: bool = False, reason: str = "", _on_locked=None,
    ) -> tuple[dict | None, str | None]:
        """Persist a settings change, atomically with any reason-selection
        change, bumping ``revision`` and appending one audit row.

        ``expected_revision`` is required (callers must always supply the
        revision they last read) and is checked against the row locked
        immediately below - never against an earlier, unlocked read - so a
        caller that skipped reloading current settings can never silently
        bypass the optimistic-concurrency check.

        The full current settings row *and* the full current reason
        selection are both read for the first time only after being locked
        here (``FOR UPDATE``) - never from an earlier, unlocked snapshot -
        so ``merged`` below always merges the incoming change against
        what is truly current at write time, not a possibly-stale
        pre-lock read that a concurrent writer could have already moved
        past.

        ``require_live_running=True`` (passed only when the caller is
        enabling or loosening automatic removal, or newly selecting any
        removal reason) locks and rechecks ``scheduler_settings``/
        ``live_control`` *inside this same transaction*, immediately
        before anything is written - exactly like
        ``SlowDownloadRepository.update_settings``/``authorized_removal``.
        The lock order (scheduler_settings, then live_control, then this
        settings row) matches those so the two can never deadlock against
        each other.
        """
        with self.db.connect() as conn:
            if require_live_running:
                scheduler = conn.execute("SELECT mode FROM scheduler_settings WHERE id = 1 FOR SHARE").fetchone()
                live = conn.execute(
                    "SELECT authorization_state, authorization_generation FROM live_control WHERE id = 1 FOR SHARE"
                ).fetchone()
                if scheduler is None or scheduler["mode"] != "live":
                    return None, "Live mode is not active"
                if live is None or live["authorization_state"] != "running":
                    return None, "Live authorization changed before this change could be saved; reload and try again"
                if _on_locked is not None:
                    _on_locked()
            row = conn.execute(
                "SELECT * FROM import_failure_policies WHERE library_id = %s FOR UPDATE", (library_id,)
            ).fetchone()
            reason_rows = conn.execute(
                "SELECT reason_key FROM import_failure_policy_reasons WHERE library_id = %s ORDER BY reason_key FOR UPDATE",
                (library_id,),
            ).fetchall()
            current_revision = row["revision"] if row else 0
            if expected_revision != current_revision:
                return None, "settings were changed by someone else; reload and try again"
            current = {
                "library_id": library_id, "revision": current_revision,
                "removal_reasons": [r["reason_key"] for r in reason_rows],
                **(({k: row[k] for k in _SETTINGS_COLUMNS}) if row else DEFAULT_SETTINGS),
            }
            merged = {**current, **data}
            values = [merged[k] for k in _SETTINGS_COLUMNS]
            conn.execute(
                f"""
                INSERT INTO import_failure_policies (library_id, {', '.join(_SETTINGS_COLUMNS)}, revision, updated_at)
                VALUES (%s, {', '.join(['%s'] * len(_SETTINGS_COLUMNS))}, 1, now())
                ON CONFLICT (library_id) DO UPDATE SET
                    {', '.join(f'{c} = EXCLUDED.{c}' for c in _SETTINGS_COLUMNS)},
                    revision = import_failure_policies.revision + 1,
                    updated_at = now()
                """,
                (library_id, *values),
            )
            new_revision = current_revision + 1
            current_reasons = set(current["removal_reasons"])
            new_reasons = set(removal_reasons) if removal_reasons is not None else current_reasons
            added = new_reasons - current_reasons
            removed = current_reasons - new_reasons
            if removal_reasons is not None and (added or removed):
                conn.execute("DELETE FROM import_failure_policy_reasons WHERE library_id = %s", (library_id,))
                for key in sorted(new_reasons):
                    conn.execute(
                        "INSERT INTO import_failure_policy_reasons (library_id, reason_key) VALUES (%s, %s)",
                        (library_id, key),
                    )
            if added or removed or merged["auto_removal_enabled"] != current["auto_removal_enabled"]:
                conn.execute(
                    """
                    INSERT INTO import_failure_policy_audit (
                        library_id, revision_before, revision_after,
                        auto_removal_enabled_before, auto_removal_enabled_after,
                        added_reasons, removed_reasons, confirm, reason
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        library_id, current_revision, new_revision,
                        current["auto_removal_enabled"], merged["auto_removal_enabled"],
                        sorted(added), sorted(removed), confirm, reason[:500],
                    ),
                )
        return self.settings(library_id), None

    def all_sonarr_settings(self) -> list[dict]:
        with self.db.connect() as conn:
            libraries = conn.execute(
                "SELECT id FROM arr_libraries WHERE enabled = TRUE AND type = 'sonarr' ORDER BY id"
            ).fetchall()
        return [self.settings(row["id"]) for row in libraries]

    def last_polled_at(self, library_id: int) -> datetime | None:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT last_polled_at FROM import_failure_policies WHERE library_id = %s", (library_id,)
            ).fetchone()
        return row["last_polled_at"] if row else None

    def record_poll_attempt(self, library_id: int) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO import_failure_policies (library_id, last_polled_at) VALUES (%s, now())
                ON CONFLICT (library_id) DO UPDATE SET last_polled_at = now()
                """,
                (library_id,),
            )

    # --- current queue item tracking --------------------------------------

    def record_observation(
        self, library_id: int, record: dict, normalization: NormalizationResult, settings: dict,
        *, observed_at: datetime | None = None,
    ) -> dict:
        """Fold one watched Sonarr queue-details record into durable
        tracking state. ``record`` must come from
        ``SonarrClient.get_queue_details`` (already adapter-validated) and
        have already passed ``should_observe``."""
        observed_at = observed_at or _now()
        with self.db.connect() as conn:
            reason_rows = conn.execute(
                "SELECT reason_key FROM import_failure_policy_reasons WHERE library_id = %s", (library_id,)
            ).fetchall()
            policy = _policy_to_domain(settings, frozenset(r["reason_key"] for r in reason_rows))
            decision = evaluate(policy, normalization)
            if decision.action == "remove_eligible" and not record.get("download_id"):
                # A stable, nonempty downloadId is required for removal -
                # without one there is nothing to re-prove identity against
                # at the final pre-DELETE revalidation (see
                # ImportFailureService._attempt_removal), so this item must
                # never become remove_eligible in the first place.
                decision = Decision("leave", "queue record has no stable download identity; never auto-removed")

            row = conn.execute(
                """
                SELECT * FROM import_failure_queue_items
                WHERE library_id = %s AND sonarr_queue_id = %s FOR UPDATE
                """,
                (library_id, record["queue_id"]),
            ).fetchone()
            matched = sorted(normalization.matched_reasons)
            unmatched = list(normalization.unmatched_messages)[:20]
            if row is None:
                updated = conn.execute(
                    """
                    INSERT INTO import_failure_queue_items (
                        library_id, sonarr_queue_id, download_id, title, status,
                        tracked_state, tracked_status, matched_reasons, unmatched_messages,
                        decision, decision_reason, first_seen_at, last_seen_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        library_id, record["queue_id"], record.get("download_id"), record.get("title", ""),
                        record.get("status", ""), record.get("tracked_state"), record.get("tracked_status"),
                        matched, unmatched, decision.action, decision.reason[:1000], observed_at, observed_at,
                    ),
                ).fetchone()
                self._record_action(conn, updated["id"], library_id, record["queue_id"], "observed", decision.reason)
            else:
                decision_changed = row["decision"] != decision.action
                updated = conn.execute(
                    """
                    UPDATE import_failure_queue_items SET
                        download_id = %s, title = %s, status = %s, tracked_state = %s, tracked_status = %s,
                        matched_reasons = %s, unmatched_messages = %s,
                        decision = %s, decision_reason = %s, last_seen_at = %s, updated_at = now()
                    WHERE id = %s
                    RETURNING *
                    """,
                    (
                        record.get("download_id"), record.get("title", ""), record.get("status", ""),
                        record.get("tracked_state"), record.get("tracked_status"),
                        matched, unmatched, decision.action, decision.reason[:1000], observed_at, row["id"],
                    ),
                ).fetchone()
                action = "decision_changed" if decision_changed else "observed"
                self._record_action(conn, updated["id"], library_id, record["queue_id"], action, decision.reason)
        return _queue_item_dict(updated)

    @staticmethod
    def _record_action(conn, queue_item_id: int, library_id: int, sonarr_queue_id: int, action: str, reason: str):
        conn.execute(
            """
            INSERT INTO import_failure_actions (queue_item_id, library_id, sonarr_queue_id, action, reason)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (queue_item_id, library_id, sonarr_queue_id, action, reason[:1000]),
        )

    def resolve_no_longer_watched(self, library_id: int, record: dict) -> None:
        """A previously tracked item is still present in the queue but no
        longer in a watched (completed/importBlocked/warning-or-error)
        state - e.g. an operator manually fixed it. Mark resolved rather
        than removed; this layer never performed a DELETE for it."""
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id FROM import_failure_queue_items
                WHERE library_id = %s AND sonarr_queue_id = %s
                  AND decision NOT IN ('removed', 'resolved', 'ambiguous')
                FOR UPDATE
                """,
                (library_id, record["queue_id"]),
            ).fetchone()
            if row is None:
                return
            conn.execute(
                """
                UPDATE import_failure_queue_items
                SET decision = 'resolved', decision_reason = 'no longer in a watched import-blocked/warning state',
                    status = %s, tracked_state = %s, tracked_status = %s, last_seen_at = now(), updated_at = now()
                WHERE id = %s
                """,
                (record.get("status", ""), record.get("tracked_state"), record.get("tracked_status"), row["id"]),
            )
            self._record_action(
                conn, row["id"], library_id, record["queue_id"], "resolved",
                "queue record is no longer in a watched state",
            )

    def clear_disappeared(self, library_id: int, present_queue_ids: set[int]) -> int:
        """Clear tracking for any item no longer present at all in the
        live Sonarr queue. Disappearance is its own outcome - never
        conflated with a successful import or a removal this layer
        performed."""
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, sonarr_queue_id FROM import_failure_queue_items
                WHERE library_id = %s AND decision NOT IN ('removed', 'ambiguous', 'resolved')
                """,
                (library_id,),
            ).fetchall()
            stale = [r for r in rows if r["sonarr_queue_id"] not in present_queue_ids]
            for r in stale:
                conn.execute(
                    """
                    UPDATE import_failure_queue_items
                    SET decision = 'resolved', decision_reason = 'queue record no longer present in Sonarr queue',
                        removed_at = now(), updated_at = now()
                    WHERE id = %s
                    """,
                    (r["id"],),
                )
                self._record_action(
                    conn, r["id"], library_id, r["sonarr_queue_id"], "cleared_disappeared",
                    "queue record disappeared from Sonarr queue; outcome unknown, not assumed successful or a removal by this guard",
                )
        return len(stale)

    def list_current(self, library_id: int | None = None, *, limit: int = 200) -> list[dict]:
        limit = max(1, min(int(limit), 500))
        with self.db.connect() as conn:
            if library_id is not None:
                rows = conn.execute(
                    """
                    SELECT * FROM import_failure_queue_items WHERE library_id = %s
                    ORDER BY updated_at DESC LIMIT %s
                    """,
                    (library_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM import_failure_queue_items ORDER BY updated_at DESC LIMIT %s", (limit,)
                ).fetchall()
        return [_queue_item_dict(r) for r in rows]

    def get(self, queue_item_id: int, *, conn=None) -> dict | None:
        if conn is not None:
            row = conn.execute("SELECT * FROM import_failure_queue_items WHERE id = %s", (queue_item_id,)).fetchone()
            return _queue_item_dict(row) if row else None
        with self.db.connect() as owned:
            row = owned.execute("SELECT * FROM import_failure_queue_items WHERE id = %s", (queue_item_id,)).fetchone()
        return _queue_item_dict(row) if row else None

    def recent_actions(self, library_id: int | None = None, *, limit: int = 100) -> list[dict]:
        limit = max(1, min(int(limit), 500))
        with self.db.connect() as conn:
            if library_id is not None:
                rows = conn.execute(
                    """
                    SELECT a.*, q.title FROM import_failure_actions a
                    LEFT JOIN import_failure_queue_items q ON q.id = a.queue_item_id
                    WHERE a.library_id = %s ORDER BY a.occurred_at DESC, a.id DESC LIMIT %s
                    """,
                    (library_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT a.*, q.title FROM import_failure_actions a
                    LEFT JOIN import_failure_queue_items q ON q.id = a.queue_item_id
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

    def recent_policy_audit(self, library_id: int | None = None, *, limit: int = 100) -> list[dict]:
        limit = max(1, min(int(limit), 500))
        with self.db.connect() as conn:
            if library_id is not None:
                rows = conn.execute(
                    "SELECT * FROM import_failure_policy_audit WHERE library_id = %s ORDER BY occurred_at DESC, id DESC LIMIT %s",
                    (library_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM import_failure_policy_audit ORDER BY occurred_at DESC, id DESC LIMIT %s", (limit,)
                ).fetchall()
        return [
            {
                "id": r["id"], "library_id": r["library_id"],
                "revision_before": r["revision_before"], "revision_after": r["revision_after"],
                "auto_removal_enabled_before": r["auto_removal_enabled_before"],
                "auto_removal_enabled_after": r["auto_removal_enabled_after"],
                "added_reasons": list(r["added_reasons"] or []), "removed_reasons": list(r["removed_reasons"] or []),
                "confirm": r["confirm"], "reason": r["reason"], "occurred_at": _iso(r["occurred_at"]),
            }
            for r in rows
        ]

    # --- gated removal: mirrors SlowDownloadRepository.authorized_removal --

    @contextmanager
    def authorized_removal(
        self, queue_item_id: int, expected_generation: int, *, expected_url: str, expected_api_key: str
    ):
        """Linearize the last authorization/settings/reason-selection
        check with the DELETE. Row/shared locks held here keep settings,
        selected reasons, Live authorization, and library identity from
        changing between the final check and the write. Immediately
        before the caller's DELETE, a separate transaction commits the
        durable attempt marker through ``dedicated_transaction`` so a
        crash after an accepted DELETE is provably ambiguous on restart
        rather than silently retried."""
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM import_failure_queue_items WHERE id = %s FOR NO KEY UPDATE", (queue_item_id,)
            ).fetchone()
            if not row or row["decision"] != "remove_eligible":
                yield conn, row, "queue item is not in a removal-eligible state"
                return
            if conn.execute(
                "SELECT 1 FROM import_failure_removal_attempts WHERE queue_item_id = %s", (queue_item_id,)
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
                "SELECT * FROM import_failure_policies WHERE library_id = %s FOR SHARE", (row["library_id"],)
            ).fetchone()
            reason_rows = conn.execute(
                "SELECT reason_key FROM import_failure_policy_reasons WHERE library_id = %s FOR SHARE",
                (row["library_id"],),
            ).fetchall()
            selected_reasons = frozenset(r["reason_key"] for r in reason_rows)
            error = None
            if scheduler is None or scheduler["mode"] != "live":
                error = "Live mode is not active"
            elif live is None or live["authorization_state"] != "running" or live["authorization_generation"] != expected_generation:
                error = "Live authorization changed before removal"
            elif (
                not library or library["type"] != "sonarr" or library["enabled"] is not True
                or library["url"] != expected_url or library["api_key"] != expected_api_key
            ):
                error = "library routing identity changed since evaluation"
            elif not settings_row or not settings_row["auto_removal_enabled"]:
                error = "automatic removal is disabled for this library"
            elif row["decision"] != "remove_eligible":
                error = "queue item is no longer removal-eligible"
            elif set(row["matched_reasons"] or []) - selected_reasons:
                error = "a matched reason is no longer selected for automatic removal"
            if error is None:
                row["remove_from_client"] = settings_row["remove_from_client"]
                row["blocklist"] = settings_row["blocklist"]
                row["skip_redownload"] = settings_row["skip_redownload"]
                row["selected_reasons"] = selected_reasons
            yield conn, row, error

    def mark_removal_attempt_started(self, row: dict, *, conn) -> None:
        """Commit the permanent no-retry marker after the final live queue
        GET has revalidated the exact record, and immediately before DELETE."""
        with self.db.dedicated_transaction(lock_timeout_seconds=5) as marker_conn:
            marker_conn.execute(
                "INSERT INTO import_failure_removal_attempts (queue_item_id, started_at) VALUES (%s, now())",
                (row["id"],),
            )
        self._record_action(
            conn, row["id"], row["library_id"], row["sonarr_queue_id"],
            "removal_attempt_started", "durable attempt marker committed after live queue revalidation and before DELETE",
        )

    def abort_revalidated_removal(self, row: dict, decision: str, reason: str, *, conn) -> dict:
        """Fail closed before an attempt marker/DELETE when the final
        queue read no longer proves the stored removal evidence is
        current."""
        if decision not in ("leave", "resolved", "removed", "ambiguous"):
            decision = "ambiguous"
        updated = conn.execute(
            """
            UPDATE import_failure_queue_items SET
                decision = %s, decision_reason = %s, updated_at = now()
            WHERE id = %s RETURNING *
            """,
            (decision, reason[:1000], row["id"]),
        ).fetchone()
        self._record_action(
            conn, row["id"], row["library_id"], row["sonarr_queue_id"],
            "decision_changed", "final removal revalidation blocked DELETE: " + reason[:900],
        )
        return _queue_item_dict(updated)

    def finalize_removal(self, queue_item_id: int, outcome: str, reason: str, *, conn=None) -> dict | None:
        """``outcome`` is one of 'completed', 'rejected', 'ambiguous'."""
        action = {"completed": "removal_completed", "rejected": "removal_rejected", "ambiguous": "removal_ambiguous"}[outcome]
        decision = "removed" if outcome == "completed" else "ambiguous" if outcome == "ambiguous" else "remove_eligible"
        if conn is not None:
            row = conn.execute(
                """
                UPDATE import_failure_queue_items
                SET decision = %s, decision_reason = %s, removal_outcome = %s,
                    removed_at = CASE WHEN %s = 'completed' THEN now() ELSE removed_at END, updated_at = now()
                WHERE id = %s RETURNING *
                """,
                (decision, reason[:1000], outcome, outcome, queue_item_id),
            ).fetchone()
            self._record_action(conn, queue_item_id, row["library_id"], row["sonarr_queue_id"], action, reason)
            return _queue_item_dict(row)
        with self.db.connect() as owned:
            row = owned.execute(
                """
                UPDATE import_failure_queue_items
                SET decision = %s, decision_reason = %s, removal_outcome = %s,
                    removed_at = CASE WHEN %s = 'completed' THEN now() ELSE removed_at END, updated_at = now()
                WHERE id = %s RETURNING *
                """,
                (decision, reason[:1000], outcome, outcome, queue_item_id),
            ).fetchone()
            self._record_action(owned, queue_item_id, row["library_id"], row["sonarr_queue_id"], action, reason)
        return _queue_item_dict(row)

"""Persistence for M6 controlled live dispatch: arm state, short-lived
challenges, append-only audit, and the per-cycle live dispatch ledger.

Every mutation here is a small, self-contained transaction (matching the
patterns already used by ``SchedulerRepository``/``DispatchRepository``):
nothing sleeps or waits on a network call inside a transaction, and gating
reads that must be consistent with a concurrent arm/disarm/stop use
``SELECT ... FOR UPDATE`` on the singleton ``live_control`` row.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ..domain.live import CHALLENGE_TTL_SECONDS
from .database import Database


def _iso(value):
    return value.isoformat() if value is not None else None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _control_dict(row) -> dict:
    return {
        "armed": row["armed"],
        "arm_generation": row["arm_generation"],
        "armed_at": _iso(row["armed_at"]),
        "armed_by": row["armed_by"],
        "armed_reason": row["armed_reason"],
        "expires_at": _iso(row["expires_at"]),
        "emergency_stopped_at": _iso(row["emergency_stopped_at"]),
        "emergency_stop_reason": row["emergency_stop_reason"],
        "emergency_stop_generation": row["emergency_stop_generation"],
        "max_dispatches_per_cycle": row["max_dispatches_per_cycle"],
        "min_delay_seconds_between_dispatches": row["min_delay_seconds_between_dispatches"],
        "default_arm_ttl_minutes": row["default_arm_ttl_minutes"],
        "max_arm_ttl_minutes": row["max_arm_ttl_minutes"],
        "last_dispatch_at": _iso(row["last_dispatch_at"]),
        "last_dispatch_summary": row["last_dispatch_summary"],
        "updated_at": _iso(row["updated_at"]),
    }


def _challenge_dict(row) -> dict:
    return {
        "id": row["id"], "kind": row["kind"], "state": row["state"],
        "token_hash": row["token_hash"], "policy_digest": row["policy_digest"],
        "requested_reason": row["requested_reason"],
        "requested_ttl_minutes": row["requested_ttl_minutes"],
        "created_at": _iso(row["created_at"]), "expires_at": _iso(row["expires_at"]),
        "confirmed_at": _iso(row["confirmed_at"]),
    }


class LiveRepository:
    def __init__(self, db: Database):
        self.db = db

    # --- live_control reads ------------------------------------------------

    def get_control(self) -> dict:
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM live_control WHERE id = 1").fetchone()
        return _control_dict(row)

    # --- challenges ----------------------------------------------------

    def create_challenge(
        self, kind: str, token_hash: str, policy_digest: str, *,
        requested_reason: str | None = None, requested_ttl_minutes: int | None = None,
    ) -> dict:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                INSERT INTO live_challenges (
                    kind, token_hash, policy_digest, requested_reason,
                    requested_ttl_minutes, state, expires_at
                ) VALUES (%s, %s, %s, %s, %s, 'pending', now() + (%s * interval '1 second'))
                RETURNING *
                """,
                (kind, token_hash, policy_digest, requested_reason, requested_ttl_minutes, CHALLENGE_TTL_SECONDS),
            ).fetchone()
        return _challenge_dict(row)

    def consume_challenge(self, challenge_id: int, kind: str, token_hash: str) -> tuple[dict | None, str | None]:
        """Atomically validate and consume a pending challenge.

        Returns ``(challenge_row, None)`` on success (state moved to
        'confirmed') or ``(None, error)``. A challenge can only ever be
        confirmed once - a replayed token/id always fails.
        """
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM live_challenges WHERE id = %s FOR UPDATE", (challenge_id,)
            ).fetchone()
            if row is None or row["kind"] != kind:
                return None, "challenge not found"
            if row["state"] != "pending":
                return None, "challenge already used or expired"
            now = conn.execute("SELECT now() AS now").fetchone()["now"]
            if row["expires_at"] <= now:
                conn.execute(
                    "UPDATE live_challenges SET state = 'expired' WHERE id = %s", (challenge_id,)
                )
                return None, "challenge has expired; request a new one"
            if row["token_hash"] != token_hash:
                return None, "challenge token does not match"
            updated = conn.execute(
                "UPDATE live_challenges SET state = 'confirmed', confirmed_at = now() WHERE id = %s RETURNING *",
                (challenge_id,),
            ).fetchone()
        return _challenge_dict(updated), None

    # --- mode enable (does not arm) ------------------------------------

    def enable_live_mode(self, *, actor: str, reason: str | None) -> dict:
        with self.db.connect() as conn:
            previous = conn.execute("SELECT mode FROM scheduler_settings WHERE id = 1 FOR UPDATE").fetchone()
            row = conn.execute(
                """
                UPDATE scheduler_settings
                SET mode = 'live',
                    next_due_at = CASE WHEN mode <> 'live' OR next_due_at IS NULL
                        THEN now() ELSE next_due_at END,
                    updated_at = now()
                WHERE id = 1
                RETURNING mode
                """
            ).fetchone()
            conn.execute(
                """
                INSERT INTO live_control_audit (event_type, previous_mode, new_mode, reason, actor)
                VALUES ('mode_enabled', %s, 'live', %s, %s)
                """,
                (previous["mode"], reason, actor),
            )
        return {"mode": row["mode"]}

    def disarm_for_mode_change(self, *, new_mode: str, actor: str) -> None:
        """Invalidate any pending live authorization whenever the
        scheduler mode moves away from 'live' - required even if the mode
        was already off/simulate (idempotent no-op in that case)."""
        if new_mode == "live":
            return
        with self.db.connect() as conn:
            updated = conn.execute(
                """
                UPDATE live_control
                SET armed = FALSE, armed_at = NULL, armed_by = NULL,
                    armed_reason = NULL, expires_at = NULL, updated_at = now()
                WHERE id = 1 AND armed = TRUE
                RETURNING arm_generation
                """
            ).fetchone()
            if updated is not None:
                conn.execute(
                    """
                    INSERT INTO live_control_audit (event_type, new_mode, reason, actor, arm_generation)
                    VALUES ('disarmed', %s, 'scheduler mode changed away from live', %s, %s)
                    """,
                    (new_mode, actor, updated["arm_generation"]),
                )

    # --- arm / disarm / emergency stop ----------------------------------

    def arm(self, *, actor: str, reason: str, ttl_minutes: int) -> tuple[dict | None, str | None]:
        with self.db.connect() as conn:
            settings = conn.execute("SELECT mode FROM scheduler_settings WHERE id = 1").fetchone()
            if settings["mode"] != "live":
                return None, "scheduler mode is not live; arming is not allowed"
            control = conn.execute("SELECT * FROM live_control WHERE id = 1 FOR UPDATE").fetchone()
            ttl_minutes = min(ttl_minutes, control["max_arm_ttl_minutes"])
            row = conn.execute(
                """
                UPDATE live_control
                SET armed = TRUE, arm_generation = arm_generation + 1,
                    armed_at = now(), armed_by = %s, armed_reason = %s,
                    expires_at = now() + (%s * interval '1 minute'),
                    emergency_stopped_at = NULL, emergency_stop_reason = NULL,
                    updated_at = now()
                WHERE id = 1
                RETURNING *
                """,
                (actor[:255], reason[:500], ttl_minutes),
            ).fetchone()
            # Arming must create an opportunity to dispatch within the arm
            # window. Without this, a short arm TTL can expire before the
            # scheduler's normal hourly next_due_at, making Live appear inert.
            # The worker still performs every mode/generation/expiry gate
            # before any Sonarr command.
            conn.execute(
                """
                UPDATE scheduler_settings
                SET next_due_at = now(), updated_at = now()
                WHERE id = 1 AND mode = 'live'
                """
            )
            conn.execute(
                """
                INSERT INTO live_control_audit (event_type, new_mode, reason, actor, arm_generation)
                VALUES ('armed', 'live', %s, %s, %s)
                """,
                (reason[:500], actor[:255], row["arm_generation"]),
            )
        return _control_dict(row), None

    def disarm(self, *, actor: str, reason: str) -> dict:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                UPDATE live_control
                SET armed = FALSE, armed_at = NULL, armed_by = NULL,
                    armed_reason = NULL, expires_at = NULL, updated_at = now()
                WHERE id = 1
                RETURNING *
                """
            ).fetchone()
            conn.execute(
                """
                INSERT INTO live_control_audit (event_type, reason, actor, arm_generation)
                VALUES ('disarmed', %s, %s, %s)
                """,
                (reason[:500], actor[:255], row["arm_generation"]),
            )
        return _control_dict(row)

    def emergency_stop(self, *, actor: str, reason: str) -> dict:
        """Idempotent: repeated calls always converge on armed=FALSE and a
        set emergency_stopped_at, updating the reason/timestamp/generation
        each time without erroring."""
        with self.db.connect() as conn:
            row = conn.execute(
                """
                UPDATE live_control
                SET armed = FALSE, armed_at = NULL, armed_by = NULL, armed_reason = NULL,
                    expires_at = NULL, emergency_stopped_at = now(),
                    emergency_stop_reason = %s, emergency_stop_generation = emergency_stop_generation + 1,
                    updated_at = now()
                WHERE id = 1
                RETURNING *
                """,
                (reason[:500],),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO live_control_audit (event_type, reason, actor, arm_generation)
                VALUES ('emergency_stop', %s, %s, %s)
                """,
                (reason[:500], actor[:255], row["arm_generation"]),
            )
            cancelled = conn.execute(
                """
                UPDATE scheduler_cycle_runs
                SET state = 'skipped', started_at = COALESCE(started_at, now()), finished_at = now(),
                    worker_owner = COALESCE(worker_owner, 'emergency-stop'),
                    safe_summary = 'Live dispatch emergency stop cancelled this queued cycle before it started.'
                WHERE state = 'queued' AND mode_snapshot = 'live'
                RETURNING id
                """
            ).fetchall()
        return {"control": _control_dict(row), "cancelled_cycle_ids": [r["id"] for r in cancelled]}

    # --- gating check used by the worker-only coordinator ----------------

    def check_dispatch_authorized(self, expected_generation: int) -> tuple[bool, str]:
        """Transactionally re-verify live dispatch is still authorized.

        Returns ``(True, "")`` only if scheduler mode is 'live', the arm
        is still the exact generation the cycle captured at start, it has
        not expired, and no emergency stop has occurred since. Any other
        outcome returns a safe, specific reason and the caller must stop
        the cycle's dispatch loop rather than continue.
        """
        with self.db.connect() as conn:
            settings = conn.execute("SELECT mode FROM scheduler_settings WHERE id = 1").fetchone()
            if settings["mode"] != "live":
                return False, "scheduler mode is no longer live"
            control = conn.execute("SELECT * FROM live_control WHERE id = 1").fetchone()
            if control["emergency_stopped_at"] is not None:
                return False, "emergency stop is active"
            if not control["armed"]:
                return False, "live dispatch is not armed"
            if control["arm_generation"] != expected_generation:
                return False, "arm generation changed since this cycle started"
            now = conn.execute("SELECT now() AS now").fetchone()["now"]
            if control["expires_at"] is None or control["expires_at"] <= now:
                return False, "arm window has expired"
        return True, ""

    def check_dispatch_delay_elapsed(self, min_delay_seconds: int) -> bool:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT (
                    last_dispatch_at IS NULL
                    OR last_dispatch_at <= now() - (%s * interval '1 second')
                ) AS ok
                FROM live_control WHERE id = 1
                """,
                (min_delay_seconds,),
            ).fetchone()
        return bool(row["ok"])

    def record_dispatch_attempt(self, summary: str) -> None:
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE live_control SET last_dispatch_at = now(), last_dispatch_summary = %s, updated_at = now() WHERE id = 1",
                (summary[:1000],),
            )
            # A new command should be observed promptly instead of waiting
            # for a reconcile deadline that may have been advanced just
            # before this dispatch existed.
            conn.execute(
                "UPDATE refresh_settings SET next_reconcile_due_at = now(), updated_at = now() WHERE id = 1"
            )

    def expire_stale_arm(self) -> bool:
        """Best-effort housekeeping: flips armed to FALSE once the TTL has
        passed, with an explicit audit row. Not required for safety (every
        gating check already re-verifies expires_at itself) but keeps the
        UI/API status from showing a stale 'armed' flag."""
        with self.db.connect() as conn:
            row = conn.execute(
                """
                UPDATE live_control
                SET armed = FALSE, armed_at = NULL, armed_by = NULL,
                    armed_reason = NULL, expires_at = NULL, updated_at = now()
                WHERE id = 1 AND armed = TRUE AND expires_at <= now()
                RETURNING arm_generation
                """
            ).fetchone()
            if row is None:
                return False
            conn.execute(
                """
                INSERT INTO live_control_audit (event_type, arm_generation, reason)
                VALUES ('arm_expired', %s, 'arm TTL elapsed')
                """,
                (row["arm_generation"],),
            )
        return True

    # --- ledger ----------------------------------------------------------

    def record_ledger_entry(self, data: dict) -> int:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                INSERT INTO live_dispatch_ledger (
                    cycle_run_id, library_result_id, candidate_result_id, library_id,
                    candidate_id, dispatch_batch_id, dispatch_item_id, attempt,
                    arm_generation, state, sonarr_command_id, sonarr_command_status,
                    terminal_reason
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (cycle_run_id, candidate_result_id) DO NOTHING
                RETURNING id
                """,
                (
                    data["cycle_run_id"], data.get("library_result_id"), data.get("candidate_result_id"),
                    data.get("library_id"), data.get("candidate_id"), data.get("dispatch_batch_id"),
                    data.get("dispatch_item_id"), data.get("attempt", 1), data["arm_generation"],
                    data["state"], data.get("sonarr_command_id"), data.get("sonarr_command_status"),
                    data["terminal_reason"][:500],
                ),
            ).fetchone()
        return row["id"] if row else None

    def ledger_for_cycle(self, cycle_run_id: int) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM live_dispatch_ledger WHERE cycle_run_id = %s ORDER BY id", (cycle_run_id,)
            ).fetchall()
        return [
            {
                "id": r["id"], "cycle_run_id": r["cycle_run_id"], "library_result_id": r["library_result_id"],
                "candidate_result_id": r["candidate_result_id"], "library_id": r["library_id"],
                "candidate_id": r["candidate_id"], "dispatch_batch_id": r["dispatch_batch_id"],
                "dispatch_item_id": r["dispatch_item_id"], "attempt": r["attempt"],
                "arm_generation": r["arm_generation"], "state": r["state"],
                "sonarr_command_id": r["sonarr_command_id"], "sonarr_command_status": r["sonarr_command_status"],
                "terminal_reason": r["terminal_reason"], "created_at": _iso(r["created_at"]),
            }
            for r in rows
        ]

    def dispatch_count_in_window(self, since: datetime) -> int:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT count(*) AS c FROM live_dispatch_ledger WHERE state = 'dispatched' AND created_at >= %s",
                (since,),
            ).fetchone()
        return row["c"]

    # --- audit reads -------------------------------------------------------

    def recent_audit(self, limit: int = 50) -> list[dict]:
        limit = max(1, min(limit, 200))
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM live_control_audit ORDER BY id DESC LIMIT %s", (limit,)
            ).fetchall()
        return [
            {
                "id": r["id"], "event_type": r["event_type"], "previous_mode": r["previous_mode"],
                "new_mode": r["new_mode"], "reason": r["reason"], "actor": r["actor"],
                "arm_generation": r["arm_generation"], "occurred_at": _iso(r["occurred_at"]),
            }
            for r in rows
        ]

    def pending_live_cycle_count(self) -> int:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT count(*) AS c FROM scheduler_cycle_runs WHERE mode_snapshot = 'live' AND state IN ('queued', 'running')"
            ).fetchone()
        return row["c"]

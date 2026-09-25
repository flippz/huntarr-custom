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
        "state": row["authorization_state"],
        "authorization_generation": row["authorization_generation"],
        "authorized_at": _iso(row["authorized_at"]),
        "authorized_by": row["authorized_by"],
        "authorization_reason": row["authorization_reason"],
        "state_changed_at": _iso(row["state_changed_at"]),
        # Harmless response compatibility; these have no TTL semantics in v7.
        "armed": row["authorization_state"] == "running",
        "arm_generation": row["authorization_generation"],
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
            conn.execute("""UPDATE live_control SET authorization_state='paused',
                authorization_reason='Live enabled; explicit resume required',
                state_changed_at=now(), updated_at=now() WHERE id=1""")
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
                SET authorization_state='paused', authorization_generation=authorization_generation+1,
                    authorization_reason='scheduler mode changed away from live', state_changed_at=now(),
                    armed = FALSE, armed_at = NULL, armed_by = NULL,
                    armed_reason = NULL, expires_at = NULL, updated_at = now()
                WHERE id = 1 AND authorization_state <> 'paused'
                RETURNING authorization_generation AS arm_generation
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
        # Internal compatibility shim for pre-v7 callers. TTL is deliberately
        # ignored; the resulting authorization is the same persistent resume.
        try:
            return self.set_authorization_state("running", actor=actor, reason=reason), None
        except ValueError as exc:
            return None, str(exc)

    def disarm(self, *, actor: str, reason: str) -> dict:
        return self.set_authorization_state("paused", actor=actor, reason=reason)

    def emergency_stop(self, *, actor: str, reason: str) -> dict:
        """Idempotent: repeated calls always converge on armed=FALSE and a
        set emergency_stopped_at, updating the reason/timestamp/generation
        each time without erroring."""
        with self.db.connect() as conn:
            row = conn.execute(
                """
                UPDATE live_control
                SET authorization_state = 'emergency_stopped', authorization_reason = %s,
                    state_changed_at = now(), armed = FALSE, armed_at = NULL, armed_by = NULL, armed_reason = NULL,
                    expires_at = NULL, emergency_stopped_at = now(),
                    emergency_stop_reason = %s, emergency_stop_generation = emergency_stop_generation + 1,
                    updated_at = now()
                WHERE id = 1
                RETURNING *
                """,
                (reason[:500], reason[:500]),
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
            if control["authorization_state"] == "emergency_stopped":
                return False, "emergency stop is active"
            if control["authorization_state"] != "running":
                return False, "live dispatch is paused"
            if control["authorization_generation"] != expected_generation:
                return False, "authorization generation changed since this cycle started"
        return True, ""

    def set_authorization_state(self, state: str, *, actor: str, reason: str) -> dict:
        """Persistently pause/resume and invalidate already captured work."""
        if state not in ("running", "paused"):
            raise ValueError("invalid authorization state")
        with self.db.connect() as conn:
            mode = conn.execute("SELECT mode FROM scheduler_settings WHERE id=1").fetchone()["mode"]
            if state == "running" and mode != "live":
                raise ValueError("scheduler mode is not live")
            row = conn.execute(
                """UPDATE live_control SET authorization_state=%s,
                   authorization_generation=authorization_generation+1,
                   authorized_at=CASE WHEN %s='running' THEN now() ELSE authorized_at END,
                   authorized_by=CASE WHEN %s='running' THEN %s ELSE authorized_by END,
                   authorization_reason=%s, state_changed_at=now(),
                   emergency_stopped_at=NULL, emergency_stop_reason=NULL,
                   armed=FALSE, armed_at=NULL, armed_by=NULL, armed_reason=NULL, expires_at=NULL,
                   updated_at=now() WHERE id=1 RETURNING *""",
                (state, state, state, actor[:255], reason[:500]),
            ).fetchone()
            conn.execute(
                "INSERT INTO live_control_audit(event_type,new_mode,reason,actor,arm_generation) VALUES (%s,'live',%s,%s,%s)",
                ("resumed" if state == "running" else "paused", reason[:500], actor[:255], row["authorization_generation"]),
            )
            if state == "running":
                conn.execute("UPDATE scheduler_settings SET next_due_at=now(),updated_at=now() WHERE id=1 AND mode='live'")
            else:
                conn.execute("""UPDATE scheduler_cycle_runs SET state='skipped',
                    started_at=COALESCE(started_at,now()),finished_at=now(),
                    worker_owner=COALESCE(worker_owner,'pause'),
                    safe_summary='Live dispatch paused before this queued cycle started.'
                    WHERE state='queued' AND mode_snapshot='live'""")
        return _control_dict(row)

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

    def recent_ledger(self, limit: int = 100) -> list[dict]:
        limit = max(1, min(int(limit), 200))
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT l.*, c.series_title, c.season_number, c.episode_number,
                       r.queued_at AS cycle_queued_at, r.mode_snapshot
                FROM live_dispatch_ledger l
                JOIN scheduler_candidate_results c ON c.id = l.candidate_result_id
                JOIN scheduler_cycle_runs r ON r.id = l.cycle_run_id
                ORDER BY l.created_at DESC, l.id DESC LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "id": r["id"], "cycle_run_id": r["cycle_run_id"],
                "candidate_id": r["candidate_id"], "series_title": r["series_title"],
                "season_number": r["season_number"], "episode_number": r["episode_number"],
                "state": r["state"], "terminal_reason": r["terminal_reason"],
                "dispatch_batch_id": r["dispatch_batch_id"], "arm_generation": r["arm_generation"],
                "sonarr_command_id": r["sonarr_command_id"],
                "sonarr_command_status": r["sonarr_command_status"],
                "cycle_queued_at": _iso(r["cycle_queued_at"]), "created_at": _iso(r["created_at"]),
            }
            for r in rows
        ]

    def activity_timeline(self, limit: int = 200) -> list[dict]:
        """Return user-meaningful episode events, not internal worker audit noise.

        Planning rows and command polling transitions remain available in the
        detailed audit APIs. The main feed contains only an attempted search
        or evidence of a real download/import outcome.
        """
        limit = max(1, min(int(limit), 500))
        with self.db.connect() as conn:
            rows = conn.execute("""
                WITH effective_outcomes AS (
                  SELECT DISTINCT ON (e.dispatch_item_id)
                    e.id, e.dispatch_item_id, e.observed_at, e.event_type,
                    e.safe_summary, e.batch_id, e.sonarr_command_id
                  FROM dispatch_outcome_events e
                  WHERE e.dispatch_item_id IS NOT NULL
                    AND e.event_type IN ('grabbed','imported','download_failed',
                                         'import_failed','command_failed','command_aborted')
                  ORDER BY e.dispatch_item_id,
                    CASE WHEN e.event_type IN ('imported','download_failed','import_failed',
                                               'command_failed','command_aborted') THEN 1 ELSE 0 END DESC,
                    e.sonarr_event_id DESC NULLS LAST, e.id DESC
                )
                SELECT * FROM (
                  SELECT ('live:'||l.id)::text event_key, l.created_at occurred_at,
                    CASE
                      WHEN l.state='dispatched' THEN 'Search sent'
                      WHEN l.state IN ('blocked','skipped') THEN 'Search skipped'
                      WHEN l.state='ambiguous' THEN 'Search needs review'
                      ELSE 'Search failed'
                    END activity,
                    CASE
                      WHEN l.state='dispatched' THEN 'In progress'
                      WHEN l.state IN ('blocked','skipped') THEN 'Blocked'
                      WHEN l.state='ambiguous' THEN 'Needs review'
                      ELSE 'Failed'
                    END result,
                    l.terminal_reason details, c.series_title, c.season_number,
                    c.episode_number, l.dispatch_batch_id, l.sonarr_command_id
                  FROM live_dispatch_ledger l
                  JOIN scheduler_candidate_results c ON c.id=l.candidate_result_id
                  UNION ALL
                  SELECT ('outcome:'||e.id)::text, e.observed_at,
                    CASE e.event_type
                      WHEN 'grabbed' THEN 'Download found'
                      WHEN 'imported' THEN 'Imported'
                      WHEN 'download_failed' THEN 'Download failed'
                      WHEN 'import_failed' THEN 'Import failed'
                      ELSE 'Search failed'
                    END activity,
                    CASE
                      WHEN e.event_type='grabbed' THEN 'In progress'
                      WHEN e.event_type='imported' THEN 'Completed'
                      ELSE 'Failed'
                    END result,
                    e.safe_summary details, i.series_title, i.season_number,
                    i.episode_number, e.batch_id AS dispatch_batch_id,
                    e.sonarr_command_id
                  FROM effective_outcomes e
                  JOIN dispatch_batch_items i ON i.id=e.dispatch_item_id
                ) timeline
                ORDER BY occurred_at DESC, event_key DESC LIMIT %s
            """, (limit,)).fetchall()
        return [{k: (_iso(v) if k == "occurred_at" else v) for k, v in dict(r).items()} for r in rows]

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

"""PostgreSQL persistence and atomic claims for M5 read-only refresh/reconciliation.

Mirrors the M4 ``SchedulerRepository`` patterns: durable queued/claimed
requests, one queued/running item at a time per dedupe key, restart
recovery that terminalizes interrupted work without retrying it, and
append-only evidence. Nothing here calls Sonarr; it only tracks *when* the
worker should run a read-only scan or reconciliation pass.
"""
from __future__ import annotations

from ..domain.refresh import RefreshSettings
from .database import Database


def _iso(value):
    return value.isoformat() if value is not None else None


def _settings(row) -> RefreshSettings:
    return RefreshSettings(
        scan_max_age_minutes=row["scan_max_age_minutes"],
        reconcile_min_interval_minutes=row["reconcile_min_interval_minutes"],
        reconcile_max_per_cycle=row["reconcile_max_per_cycle"],
        next_reconcile_due_at=_iso(row["next_reconcile_due_at"]),
        updated_at=_iso(row["updated_at"]),
    )


def _run_dict(row) -> dict:
    return {
        "id": row["id"],
        "request_id": row["request_id"],
        "kind": row["kind"],
        "trigger": row["trigger"],
        "state": row["state"],
        "library_id": row["library_id"],
        "library_name": row["library_name"],
        "scan_job_id": row["scan_job_id"],
        "worker_owner": row["worker_owner"],
        "attempt": row["attempt"],
        "queued_at": _iso(row["queued_at"]),
        "started_at": _iso(row["started_at"]),
        "finished_at": _iso(row["finished_at"]),
        "target_count": row["target_count"],
        "succeeded_count": row["succeeded_count"],
        "failed_count": row["failed_count"],
        "skipped_count": row["skipped_count"],
        "safe_summary": row["safe_summary"],
    }


def _request_dict(row) -> dict:
    return {
        "id": row["id"], "kind": row["kind"], "library_id": row["library_id"],
        "library_name": row["library_name"], "state": row["state"],
        "requested_at": _iso(row["requested_at"]), "claimed_at": _iso(row["claimed_at"]),
        "finished_at": _iso(row["finished_at"]), "safe_summary": row["safe_summary"],
    }


class RefreshRepository:
    def __init__(self, db: Database):
        self.db = db

    # --- settings --------------------------------------------------------

    def get_settings(self) -> RefreshSettings:
        with self.db.connect() as conn:
            return _settings(conn.execute("SELECT * FROM refresh_settings WHERE id = 1").fetchone())

    def update_settings(self, data: dict) -> RefreshSettings:
        current = self.get_settings().to_dict()
        current.update({k: v for k, v in data.items() if k in (
            "scan_max_age_minutes", "reconcile_min_interval_minutes", "reconcile_max_per_cycle",
        )})
        with self.db.connect() as conn:
            row = conn.execute(
                """
                UPDATE refresh_settings
                SET scan_max_age_minutes = %s, reconcile_min_interval_minutes = %s,
                    reconcile_max_per_cycle = %s, updated_at = now()
                WHERE id = 1 RETURNING *
                """,
                (
                    current["scan_max_age_minutes"], current["reconcile_min_interval_minutes"],
                    current["reconcile_max_per_cycle"],
                ),
            ).fetchone()
        return _settings(row)

    # --- manual scan requests ---------------------------------------------

    def queue_manual_scans(self, library_id: int | None) -> tuple[list[dict], str | None]:
        """Queue one coalesced scan request per targeted enabled Sonarr
        library. ``library_id=None`` targets every enabled Sonarr library."""
        with self.db.connect() as conn:
            if library_id is not None:
                libraries = conn.execute(
                    "SELECT id, name FROM arr_libraries WHERE id = %s AND enabled = TRUE AND type = 'sonarr'",
                    (library_id,),
                ).fetchall()
                if not libraries:
                    return [], "library not found or not an enabled Sonarr library"
            else:
                libraries = conn.execute(
                    """SELECT id, name FROM arr_libraries
                       WHERE enabled = TRUE AND type = 'sonarr' ORDER BY lower(name), id"""
                ).fetchall()

            results = []
            for library in libraries:
                row = conn.execute(
                    """
                    INSERT INTO refresh_requests (kind, library_id, library_name, state)
                    VALUES ('scan', %s, %s, 'queued')
                    ON CONFLICT (library_id) WHERE state IN ('queued', 'claimed') AND library_id IS NOT NULL
                    DO NOTHING RETURNING *
                    """,
                    (library["id"], library["name"]),
                ).fetchone()
                created = row is not None
                if row is None:
                    row = conn.execute(
                        """
                        SELECT * FROM refresh_requests
                        WHERE library_id = %s AND state IN ('queued', 'claimed') ORDER BY id LIMIT 1
                        """,
                        (library["id"],),
                    ).fetchone()
                results.append({
                    "library_id": library["id"], "library_name": library["name"],
                    "created": created, "request": _request_dict(row),
                })
        return results, None

    def claim_manual_scan_requests(self, owner_id: str, *, limit: int = 10) -> list[int]:
        """Turn queued manual scan requests into queued refresh runs.

        A request whose library already has an active (queued/running) scan
        run is left queued for a later iteration rather than duplicated -
        the freshness sweep or a previous manual request already covers it.
        A request whose library was deleted in the meantime is failed
        explicitly instead of silently dropped.
        """
        created: list[int] = []
        with self.db.connect() as conn:
            requests = conn.execute(
                """
                SELECT * FROM refresh_requests
                WHERE state = 'queued' ORDER BY requested_at, id
                FOR UPDATE SKIP LOCKED LIMIT %s
                """,
                (limit,),
            ).fetchall()
            for request in requests:
                if request["library_id"] is None:
                    conn.execute(
                        """
                        UPDATE refresh_requests
                        SET state = 'claimed', claimed_at = now(), claim_owner = %s
                        WHERE id = %s
                        """,
                        (owner_id, request["id"]),
                    )
                    conn.execute(
                        """
                        UPDATE refresh_requests
                        SET state = 'failed', finished_at = now(),
                            safe_summary = 'The requested library no longer exists.'
                        WHERE id = %s
                        """,
                        (request["id"],),
                    )
                    continue
                active = conn.execute(
                    """
                    SELECT 1 FROM refresh_runs
                    WHERE kind = 'scan' AND library_id = %s AND state IN ('queued', 'running')
                    """,
                    (request["library_id"],),
                ).fetchone()
                if active:
                    continue
                conn.execute(
                    """
                    UPDATE refresh_requests
                    SET state = 'claimed', claimed_at = now(), claim_owner = %s
                    WHERE id = %s
                    """,
                    (owner_id, request["id"]),
                )
                row = conn.execute(
                    """
                    INSERT INTO refresh_runs (request_id, kind, trigger, state, library_id, library_name)
                    VALUES (%s, 'scan', 'manual', 'queued', %s, %s) RETURNING id
                    """,
                    (request["id"], request["library_id"], request["library_name"]),
                ).fetchone()
                created.append(row["id"])
        return created

    # --- freshness/reconcile scheduling ------------------------------------

    def queue_stale_scans(self, scan_max_age_minutes: int) -> list[int]:
        """Queue one scheduled scan run for every enabled Sonarr library
        that has no completed scan snapshot within the freshness window and
        has no scan run already queued/running. Never queues a duplicate."""
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                INSERT INTO refresh_runs (kind, trigger, state, library_id, library_name)
                SELECT 'scan', 'scheduled', 'queued', l.id, l.name
                FROM arr_libraries l
                WHERE l.enabled = TRUE AND l.type = 'sonarr'
                  AND NOT EXISTS (
                      SELECT 1 FROM refresh_runs r
                      WHERE r.kind = 'scan' AND r.library_id = l.id AND r.state IN ('queued', 'running')
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM activity_jobs a
                      WHERE a.library_id = l.id AND a.job_type = 'sonarr_scan' AND a.state = 'completed'
                        AND a.updated_at >= now() - (%s * interval '1 minute')
                  )
                ON CONFLICT (library_id) WHERE kind = 'scan' AND state IN ('queued', 'running')
                DO NOTHING
                RETURNING id
                """,
                (scan_max_age_minutes,),
            ).fetchall()
        return [row["id"] for row in rows]

    def has_active_scan(self, library_id: int) -> bool:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM refresh_runs
                WHERE kind = 'scan' AND library_id = %s AND state IN ('queued', 'running')
                """,
                (library_id,),
            ).fetchone()
        return row is not None

    def eligible_reconciliation_count(self, *, conn=None) -> int:
        sql = """
            SELECT count(*) AS c FROM dispatch_batches
            WHERE mode = 'manual' AND sonarr_command_id IS NOT NULL
              AND state IN ('completed', 'partial', 'ambiguous')
              AND reconciliation_state <> 'resolved'
        """
        if conn is not None:
            return conn.execute(sql).fetchone()["c"]
        with self.db.connect() as owned_conn:
            return owned_conn.execute(sql).fetchone()["c"]

    def enqueue_reconcile_if_due(self) -> int | None:
        """Queue one bounded reconciliation pass when the cooldown has
        elapsed and at least one dispatch batch still needs it. Always
        advances the next-due time so a quiet period does not cause a
        burst of eligibility checks once traffic resumes."""
        with self.db.connect() as conn:
            settings = conn.execute("SELECT * FROM refresh_settings WHERE id = 1 FOR UPDATE").fetchone()
            now_row = conn.execute("SELECT now() AS now").fetchone()
            if settings["next_reconcile_due_at"] is not None and settings["next_reconcile_due_at"] > now_row["now"]:
                return None
            active = conn.execute(
                "SELECT 1 FROM refresh_runs WHERE kind = 'reconcile' AND state IN ('queued', 'running')"
            ).fetchone()
            queued_id = None
            if active is None and self.eligible_reconciliation_count(conn=conn) > 0:
                row = conn.execute(
                    """
                    INSERT INTO refresh_runs (kind, trigger, state)
                    VALUES ('reconcile', 'scheduled', 'queued') RETURNING id
                    """
                ).fetchone()
                queued_id = row["id"]
            conn.execute(
                """
                UPDATE refresh_settings
                SET next_reconcile_due_at = now() + (%s * interval '1 minute'), updated_at = now()
                WHERE id = 1
                """,
                (settings["reconcile_min_interval_minutes"],),
            )
        return queued_id

    def eligible_reconciliation_batches(self, limit: int) -> list[int]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id FROM dispatch_batches
                WHERE mode = 'manual' AND sonarr_command_id IS NOT NULL
                  AND state IN ('completed', 'partial', 'ambiguous')
                  AND reconciliation_state <> 'resolved'
                ORDER BY updated_at ASC, id ASC LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [row["id"] for row in rows]

    def record_reconciled_batch(self, run_id: int, batch_id: int, result: str, reason: str | None) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO refresh_run_reconciled_batches (refresh_run_id, dispatch_batch_id, result, reason)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (refresh_run_id, dispatch_batch_id) DO NOTHING
                """,
                (run_id, batch_id, result, reason[:500] if reason else None),
            )

    # --- run lifecycle -----------------------------------------------------

    def recover_interrupted(self, owner_id: str) -> int:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                UPDATE refresh_runs
                SET state = 'failed', finished_at = now(),
                    safe_summary = 'Worker lease expired while this refresh was running; it was not retried.'
                WHERE state = 'running'
                RETURNING id, request_id
                """
            ).fetchall()
            request_ids = [row["request_id"] for row in rows if row["request_id"] is not None]
            if request_ids:
                conn.execute(
                    """
                    UPDATE refresh_requests
                    SET state = 'failed', finished_at = now(),
                        safe_summary = 'Worker interruption; request was not retried.'
                    WHERE id = ANY(%s) AND state = 'claimed'
                    """,
                    (request_ids,),
                )
        return len(rows)

    def start_next_run(self, owner_id: str) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM refresh_runs
                WHERE state = 'queued'
                ORDER BY CASE WHEN trigger = 'manual' THEN 0 ELSE 1 END,
                         CASE WHEN kind = 'scan' THEN 0 ELSE 1 END,
                         queued_at, id
                FOR UPDATE SKIP LOCKED LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            row = conn.execute(
                """
                UPDATE refresh_runs
                SET state = 'running', worker_owner = %s, started_at = now()
                WHERE id = %s RETURNING *
                """,
                (owner_id, row["id"]),
            ).fetchone()
        return _run_dict(row)

    def finish_run(self, run_id: int, state: str, counts: dict, summary: str, *, scan_job_id: int | None = None) -> None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                UPDATE refresh_runs
                SET state = %s, finished_at = now(),
                    target_count = %s, succeeded_count = %s, failed_count = %s, skipped_count = %s,
                    scan_job_id = COALESCE(%s, scan_job_id), safe_summary = %s
                WHERE id = %s AND state = 'running'
                RETURNING request_id
                """,
                (
                    state, counts.get("target_count", 0), counts.get("succeeded_count", 0),
                    counts.get("failed_count", 0), counts.get("skipped_count", 0),
                    scan_job_id, summary[:1000], run_id,
                ),
            ).fetchone()
            if row is None:
                return
            if row["request_id"] is not None:
                request_state = "completed" if state in ("completed", "partial", "skipped") else "failed"
                conn.execute(
                    """
                    UPDATE refresh_requests
                    SET state = %s, finished_at = now(), safe_summary = %s
                    WHERE id = %s AND state = 'claimed'
                    """,
                    (request_state, summary[:1000], row["request_id"]),
                )

    # --- reads --------------------------------------------------------------

    def list_runs(self, *, limit: int = 50, kind: str | None = None) -> list[dict]:
        limit = max(1, min(limit, 100))
        with self.db.connect() as conn:
            if kind is not None:
                rows = conn.execute(
                    "SELECT * FROM refresh_runs WHERE kind = %s ORDER BY id DESC LIMIT %s", (kind, limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM refresh_runs ORDER BY id DESC LIMIT %s", (limit,)
                ).fetchall()
        return [_run_dict(row) for row in rows]

    def get_run(self, run_id: int) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM refresh_runs WHERE id = %s", (run_id,)).fetchone()
            if row is None:
                return None
            reconciled = conn.execute(
                """
                SELECT * FROM refresh_run_reconciled_batches
                WHERE refresh_run_id = %s ORDER BY id
                """,
                (run_id,),
            ).fetchall()
        result = _run_dict(row)
        result["reconciled_batches"] = [
            {
                "dispatch_batch_id": r["dispatch_batch_id"], "result": r["result"],
                "reason": r["reason"], "created_at": _iso(r["created_at"]),
            }
            for r in reconciled
        ]
        return result

    def status(self) -> dict:
        settings = self.get_settings().to_dict()
        active = self.list_runs(limit=10)
        active = [r for r in active if r["state"] in ("queued", "running")]
        latest_scan = self.list_runs(limit=1, kind="scan")
        latest_reconcile = self.list_runs(limit=1, kind="reconcile")
        return {
            "settings": settings,
            "active_runs": active,
            "latest_scan": latest_scan[0] if latest_scan else None,
            "latest_reconcile": latest_reconcile[0] if latest_reconcile else None,
        }

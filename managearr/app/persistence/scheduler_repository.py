"""PostgreSQL persistence and atomic claims for the simulation scheduler."""
from __future__ import annotations

from datetime import timedelta
from random import SystemRandom

from psycopg.types.json import Jsonb

from ..domain.scheduler import SchedulerSettings
from .database import Database

LEASE_NAME = "cycle-worker"


def _iso(value):
    return value.isoformat() if value is not None else None


def _settings(row) -> SchedulerSettings:
    return SchedulerSettings(row["mode"], _iso(row["next_due_at"]), _iso(row["updated_at"]))


def _policy_snapshot(policy) -> dict:
    data = policy.to_dict()
    data.pop("updated_at", None)
    return data


def _cycle_dict(row) -> dict:
    return {
        "id": row["id"],
        "request_id": row["request_id"],
        "trigger": row["trigger"],
        "state": row["state"],
        "mode_snapshot": row["mode_snapshot"],
        "policy_snapshot": row["policy_snapshot"],
        "random_seed": row["random_seed"],
        "worker_owner": row["worker_owner"],
        "queued_at": _iso(row["queued_at"]),
        "started_at": _iso(row["started_at"]),
        "finished_at": _iso(row["finished_at"]),
        "library_count": row["library_count"],
        "completed_library_count": row["completed_library_count"],
        "failed_library_count": row["failed_library_count"],
        "considered_count": row["considered_count"],
        "selected_count": row["selected_count"],
        "excluded_count": row["excluded_count"],
        "safe_summary": row["safe_summary"],
    }


class SchedulerRepository:
    def __init__(self, db: Database):
        self.db = db

    # Settings and operator request -------------------------------------
    def get_settings(self) -> SchedulerSettings:
        with self.db.connect() as conn:
            return _settings(conn.execute("SELECT * FROM scheduler_settings WHERE id = 1").fetchone())

    def update_mode(self, mode: str, cycle_interval_minutes: int) -> SchedulerSettings:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                UPDATE scheduler_settings
                SET mode = %s,
                    next_due_at = CASE
                        WHEN %s = 'off' THEN NULL
                        WHEN mode <> %s OR next_due_at IS NULL
                            THEN now() + (%s * interval '1 minute')
                        ELSE next_due_at
                    END,
                    updated_at = CASE WHEN mode <> %s THEN now() ELSE updated_at END
                WHERE id = 1
                RETURNING *
                """,
                (mode, mode, mode, cycle_interval_minutes, mode),
            ).fetchone()
        return _settings(row)

    def queue_manual_simulation(self) -> tuple[dict, bool]:
        """Return the sole queued/claimed request; concurrent calls coalesce."""
        with self.db.connect() as conn:
            row = conn.execute(
                """
                INSERT INTO scheduler_run_requests (request_kind, state)
                VALUES ('simulation', 'queued')
                ON CONFLICT (request_kind) WHERE state IN ('queued', 'claimed')
                DO NOTHING
                RETURNING *
                """
            ).fetchone()
            created = row is not None
            if row is None:
                row = conn.execute(
                    """
                    SELECT * FROM scheduler_run_requests
                    WHERE request_kind = 'simulation' AND state IN ('queued', 'claimed')
                    ORDER BY id LIMIT 1
                    """
                ).fetchone()
        return self._request_dict(row), created

    @staticmethod
    def _request_dict(row) -> dict:
        return {
            "id": row["id"], "request_kind": row["request_kind"], "state": row["state"],
            "requested_at": _iso(row["requested_at"]), "claimed_at": _iso(row["claimed_at"]),
            "finished_at": _iso(row["finished_at"]), "safe_summary": row["safe_summary"],
        }

    # Lease -------------------------------------------------------------
    def acquire_lease(self, owner_id: str, lease_seconds: int) -> bool:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                UPDATE scheduler_leases
                SET owner_id = %s,
                    acquired_at = CASE WHEN owner_id = %s THEN acquired_at ELSE now() END,
                    heartbeat_at = now(),
                    expires_at = now() + (%s * interval '1 second')
                WHERE lease_name = %s
                  AND (owner_id = %s OR expires_at IS NULL OR expires_at <= now())
                RETURNING owner_id
                """,
                (owner_id, owner_id, lease_seconds, LEASE_NAME, owner_id),
            ).fetchone()
        return row is not None

    def renew_lease(self, owner_id: str, lease_seconds: int) -> bool:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                UPDATE scheduler_leases
                SET heartbeat_at = now(), expires_at = now() + (%s * interval '1 second')
                WHERE lease_name = %s AND owner_id = %s AND expires_at > now()
                RETURNING owner_id
                """,
                (lease_seconds, LEASE_NAME, owner_id),
            ).fetchone()
        return row is not None

    def release_lease(self, owner_id: str) -> bool:
        with self.db.connect() as conn:
            return conn.execute(
                """
                UPDATE scheduler_leases
                SET owner_id = NULL, acquired_at = NULL, heartbeat_at = NULL, expires_at = NULL
                WHERE lease_name = %s AND owner_id = %s
                """,
                (LEASE_NAME, owner_id),
            ).rowcount == 1

    def lease_status(self) -> dict:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT owner_id, acquired_at, heartbeat_at, expires_at,
                       owner_id IS NOT NULL AND expires_at > now() AS healthy
                FROM scheduler_leases WHERE lease_name = %s
                """,
                (LEASE_NAME,),
            ).fetchone()
        return {
            "healthy": bool(row["healthy"]),
            "owner_id": row["owner_id"],
            "acquired_at": _iso(row["acquired_at"]),
            "heartbeat_at": _iso(row["heartbeat_at"]),
            "expires_at": _iso(row["expires_at"]),
        }

    # Durable work claiming --------------------------------------------
    def recover_interrupted(self, owner_id: str) -> int:
        """Terminalize work left running by the expired previous lease owner."""
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                UPDATE scheduler_cycle_runs
                SET state = 'failed', finished_at = now(),
                    safe_summary = 'Worker lease expired while this simulation was running; it was not retried.'
                WHERE state = 'running'
                RETURNING id, request_id
                """
            ).fetchall()
            request_ids = [row["request_id"] for row in rows if row["request_id"] is not None]
            if request_ids:
                conn.execute(
                    """
                    UPDATE scheduler_run_requests
                    SET state = 'failed', finished_at = now(),
                        safe_summary = 'Worker interruption; request was not retried.'
                    WHERE id = ANY(%s) AND state = 'claimed'
                    """,
                    (request_ids,),
                )
        return len(rows)

    def claim_manual_request(self, owner_id: str, policy) -> int | None:
        with self.db.connect() as conn:
            request = conn.execute(
                """
                SELECT * FROM scheduler_run_requests
                WHERE state = 'queued' ORDER BY requested_at, id
                FOR UPDATE SKIP LOCKED LIMIT 1
                """
            ).fetchone()
            if request is None:
                return None
            conn.execute(
                """
                UPDATE scheduler_run_requests
                SET state = 'claimed', claimed_at = now(), claim_owner = %s
                WHERE id = %s
                """,
                (owner_id, request["id"]),
            )
            row = conn.execute(
                """
                INSERT INTO scheduler_cycle_runs (
                    request_id, trigger, state, mode_snapshot, policy_snapshot, random_seed
                ) VALUES (%s, 'manual', 'queued', 'simulate', %s, %s)
                RETURNING id
                """,
                (request["id"], Jsonb(_policy_snapshot(policy)), SystemRandom().randrange(1, 2**63 - 1)),
            ).fetchone()
        return row["id"]

    def enqueue_scheduled_if_due(self, policy) -> int | None:
        with self.db.connect() as conn:
            settings = conn.execute(
                "SELECT * FROM scheduler_settings WHERE id = 1 FOR UPDATE"
            ).fetchone()
            if (settings["mode"] not in ("simulate", "live") or settings["next_due_at"] is None
                    or settings["next_due_at"] > conn.execute("SELECT now() AS now").fetchone()["now"]):
                return None
            row = conn.execute(
                """
                INSERT INTO scheduler_cycle_runs (
                    trigger, state, mode_snapshot, policy_snapshot, random_seed
                ) VALUES ('scheduled', 'queued', %s, %s, %s)
                RETURNING id
                """,
                (settings["mode"], Jsonb(_policy_snapshot(policy)), SystemRandom().randrange(1, 2**63 - 1)),
            ).fetchone()
            conn.execute(
                """
                UPDATE scheduler_settings
                SET next_due_at = now() + (%s * interval '1 minute'), updated_at = now()
                WHERE id = 1
                """,
                (policy.cycle_interval_minutes,),
            )
        return row["id"]

    def start_next_cycle(self, owner_id: str) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM scheduler_cycle_runs
                WHERE state = 'queued'
                ORDER BY CASE WHEN trigger = 'manual' THEN 0 ELSE 1 END, queued_at, id
                FOR UPDATE SKIP LOCKED LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            row = conn.execute(
                """
                UPDATE scheduler_cycle_runs
                SET state = 'running', worker_owner = %s, started_at = now()
                WHERE id = %s RETURNING *
                """,
                (owner_id, row["id"]),
            ).fetchone()
        return _cycle_dict(row)

    def finish_cycle(self, cycle_id: int, state: str, counts: dict, summary: str) -> None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                UPDATE scheduler_cycle_runs
                SET state = %s, finished_at = now(), library_count = %s,
                    completed_library_count = %s, failed_library_count = %s,
                    considered_count = %s, selected_count = %s, excluded_count = %s,
                    safe_summary = %s
                WHERE id = %s AND state = 'running'
                RETURNING request_id
                """,
                (
                    state, counts.get("library_count", 0), counts.get("completed_library_count", 0),
                    counts.get("failed_library_count", 0), counts.get("considered_count", 0),
                    counts.get("selected_count", 0), counts.get("excluded_count", 0),
                    summary[:1000], cycle_id,
                ),
            ).fetchone()
            if row is None:
                return
            if row["request_id"] is not None:
                request_state = "completed" if state in ("completed", "partial", "skipped") else "failed"
                conn.execute(
                    """
                    UPDATE scheduler_run_requests
                    SET state = %s, finished_at = now(), safe_summary = %s
                    WHERE id = %s AND state = 'claimed'
                    """,
                    (request_state, summary[:1000], row["request_id"]),
                )

    # Simulation reads and append-only results --------------------------
    def current_mode(self) -> str:
        return self.get_settings().mode

    def enabled_sonarr_libraries(self) -> list[dict]:
        with self.db.connect() as conn:
            return conn.execute(
                """SELECT id, name FROM arr_libraries
                   WHERE enabled = TRUE AND type = 'sonarr' ORDER BY lower(name), id"""
            ).fetchall()

    def latest_completed_scan(self, library_id: int) -> dict | None:
        with self.db.connect() as conn:
            return conn.execute(
                """
                SELECT * FROM activity_jobs
                WHERE library_id = %s AND job_type = 'sonarr_scan' AND state = 'completed'
                ORDER BY updated_at DESC, id DESC LIMIT 1
                """,
                (library_id,),
            ).fetchone()

    def candidates_for_scan(self, scan_job_id: int) -> list[dict]:
        with self.db.connect() as conn:
            return conn.execute(
                "SELECT * FROM scan_candidates WHERE job_id = %s ORDER BY id",
                (scan_job_id,),
            ).fetchall()

    def planning_facts(self, library_id: int, episode_ids: list[int], policy: dict) -> dict:
        """Read conservative durable dispatch/outcome facts; never contacts Sonarr."""
        if not episode_ids:
            return {"cooldown": set(), "in_flight": set(), "stale": set(), "imported": set(),
                    "active_grabbed": set(), "capacity_used": 0, "queue_occupancy": 0,
                    "recent_success_count": 0}
        cooldown = policy["cooldown_minutes"]
        cycle_minutes = policy["cycle_interval_minutes"]
        with self.db.connect() as conn:
            cooled = conn.execute(
                """
                SELECT DISTINCT i.episode_id FROM dispatch_batch_items i
                JOIN dispatch_batches b ON b.id = i.batch_id
                WHERE b.library_id = %s AND b.mode = 'manual'
                  AND ((b.state IN ('completed','partial') AND i.state = 'dispatched')
                       OR (b.state = 'failed' AND i.state = 'failed'))
                  AND i.episode_id = ANY(%s)
                  AND i.updated_at >= now() - (%s * interval '1 minute')
                """, (library_id, episode_ids, cooldown),
            ).fetchall()
            inflight = conn.execute(
                """
                SELECT DISTINCT i.episode_id FROM dispatch_batch_items i
                JOIN dispatch_batches b ON b.id = i.batch_id
                WHERE b.library_id = %s AND b.mode = 'manual' AND b.state = 'dispatching'
                  AND i.state = 'reserved' AND i.episode_id = ANY(%s)
                  AND i.created_at >= now() - interval '5 minutes'
                """, (library_id, episode_ids),
            ).fetchall()
            stale = conn.execute(
                """
                SELECT DISTINCT i.episode_id FROM dispatch_batch_items i
                JOIN dispatch_batches b ON b.id = i.batch_id
                WHERE b.library_id = %s AND i.episode_id = ANY(%s) AND (
                    (b.mode = 'manual' AND b.state = 'ambiguous' AND i.state = 'ambiguous')
                    OR (b.mode = 'manual' AND b.state = 'dispatching' AND i.state = 'reserved'
                        AND i.created_at < now() - interval '5 minutes'))
                """, (library_id, episode_ids),
            ).fetchall()
            capacity = conn.execute(
                """
                SELECT count(*) AS c FROM dispatch_batch_items i
                JOIN dispatch_batches b ON b.id = i.batch_id
                WHERE b.library_id = %s AND b.mode = 'manual' AND (
                    (b.state IN ('completed','partial') AND i.state = 'dispatched'
                     AND i.updated_at >= now() - interval '1 hour')
                    OR (b.state = 'failed' AND i.state = 'failed'
                        AND i.updated_at >= now() - interval '1 hour')
                    OR (b.state = 'dispatching' AND i.state = 'reserved'
                        AND i.created_at >= now() - interval '5 minutes'))
                """, (library_id,),
            ).fetchone()["c"]
            imported = conn.execute(
                """
                SELECT DISTINCT e.episode_id FROM dispatch_outcome_events e
                JOIN dispatch_batches b ON b.id = e.batch_id
                WHERE b.library_id = %s AND e.episode_id = ANY(%s)
                  AND e.event_type = 'imported'
                """, (library_id, episode_ids),
            ).fetchall()
            active_grabbed = conn.execute(
                """
                SELECT DISTINCT grabbed.episode_id
                FROM dispatch_outcome_events grabbed
                JOIN dispatch_batches b ON b.id = grabbed.batch_id
                WHERE b.library_id = %s AND grabbed.episode_id = ANY(%s)
                  AND grabbed.event_type = 'grabbed'
                  AND NOT EXISTS (
                      SELECT 1 FROM dispatch_outcome_events terminal
                      WHERE terminal.dispatch_item_id = grabbed.dispatch_item_id
                        AND terminal.event_type IN (
                            'imported','download_failed','import_failed',
                            'command_failed','command_aborted'
                        )
                  )
                """, (library_id, episode_ids),
            ).fetchall()
            outcome_cooldown = conn.execute(
                """
                SELECT DISTINCT e.episode_id FROM dispatch_outcome_events e
                JOIN dispatch_batches b ON b.id = e.batch_id
                WHERE b.library_id = %s AND e.episode_id = ANY(%s)
                  AND e.event_type IN (
                      'download_failed','import_failed','command_failed','command_aborted'
                  )
                  AND e.observed_at >= now() - (%s * interval '1 minute')
                """, (library_id, episode_ids, cooldown),
            ).fetchall()
            queue = conn.execute(
                """
                WITH dispatched AS (
                    SELECT i.id, i.episode_id, i.created_at
                    FROM dispatch_batch_items i
                    JOIN dispatch_batches b ON b.id = i.batch_id
                    WHERE b.library_id = %s AND b.mode = 'manual'
                      AND b.state IN ('completed','partial') AND i.state = 'dispatched'
                ), active_episodes AS (
                    SELECT DISTINCT d.episode_id
                    FROM dispatched d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dispatch_outcome_events terminal
                        WHERE terminal.dispatch_item_id = d.id
                          AND terminal.event_type IN (
                              'imported','download_failed','import_failed',
                              'command_failed','command_aborted'
                          )
                    ) AND (
                        d.created_at >= now() - interval '5 minutes'
                        OR EXISTS (
                            SELECT 1 FROM dispatch_outcome_events active
                            WHERE active.dispatch_item_id = d.id
                              AND active.event_type IN ('grabbed','downloading')
                        )
                    )
                )
                SELECT count(*) AS c FROM active_episodes
                """, (library_id,),
            ).fetchone()["c"]
            successes = conn.execute(
                """
                SELECT count(DISTINCT e.episode_id) AS c FROM dispatch_outcome_events e
                JOIN dispatch_batches b ON b.id = e.batch_id
                WHERE b.library_id = %s AND e.event_type IN ('grabbed','imported')
                  AND e.observed_at >= now() - (%s * interval '1 minute')
                """, (library_id, cycle_minutes),
            ).fetchone()["c"]
        return {
            "cooldown": ({r["episode_id"] for r in cooled}
                         | {r["episode_id"] for r in outcome_cooldown}),
            "in_flight": {r["episode_id"] for r in inflight},
            "stale": {r["episode_id"] for r in stale},
            "imported": {r["episode_id"] for r in imported},
            "active_grabbed": {r["episode_id"] for r in active_grabbed},
            "capacity_used": capacity,
            "queue_occupancy": queue,
            "recent_success_count": successes,
        }

    def add_library_result(self, cycle_id: int, data: dict, candidates: list[dict]) -> int:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                INSERT INTO scheduler_library_results (
                    cycle_run_id, library_id, library_name, scan_job_id, state,
                    considered_count, selected_count, excluded_count, effective_cap,
                    queue_occupancy, recent_success_count, upgrades_state, safe_summary,
                    snapshot_taken_at, snapshot_age_seconds
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
                """,
                (
                    cycle_id, data.get("library_id"), data["library_name"], data.get("scan_job_id"),
                    data["state"], data.get("considered_count", 0), data.get("selected_count", 0),
                    data.get("excluded_count", 0), data.get("effective_cap", 0),
                    data.get("queue_occupancy", 0), data.get("recent_success_count", 0),
                    data.get("upgrades_state", "unsupported"), data["safe_summary"][:1000],
                    data.get("snapshot_taken_at"), data.get("snapshot_age_seconds"),
                ),
            ).fetchone()
            result_id = row["id"]
            for item in candidates:
                conn.execute(
                    """
                    INSERT INTO scheduler_candidate_results (
                        cycle_run_id, library_result_id, candidate_id, episode_id, series_id,
                        series_title, season_number, episode_number, air_date, candidate_reason,
                        selected, exclusion_reason, order_position
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        cycle_id, result_id, item["candidate_id"], item["episode_id"], item["series_id"],
                        item["series_title"][:500], item["season_number"], item["episode_number"],
                        item.get("air_date"), item["candidate_reason"][:100], item["selected"],
                        item.get("exclusion_reason"), item.get("order_position"),
                    ),
                )
        return result_id

    # API reads ---------------------------------------------------------
    def list_cycles(self, limit: int = 50) -> list[dict]:
        limit = max(1, min(limit, 100))
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM scheduler_cycle_runs ORDER BY id DESC LIMIT %s", (limit,)
            ).fetchall()
        return [_cycle_dict(r) for r in rows]

    def get_cycle(self, cycle_id: int) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM scheduler_cycle_runs WHERE id = %s", (cycle_id,)).fetchone()
            if row is None:
                return None
            libraries = conn.execute(
                "SELECT * FROM scheduler_library_results WHERE cycle_run_id = %s ORDER BY id", (cycle_id,)
            ).fetchall()
            candidate_rows = conn.execute(
                """SELECT * FROM scheduler_candidate_results
                   WHERE cycle_run_id = %s ORDER BY library_result_id, selected DESC,
                       order_position NULLS LAST, id""", (cycle_id,),
            ).fetchall()
        result = _cycle_dict(row)
        by_library = {}
        result["libraries"] = []
        for lib in libraries:
            item = dict(lib)
            item["created_at"] = _iso(item["created_at"])
            item["snapshot_taken_at"] = _iso(item["snapshot_taken_at"])
            item["candidates"] = []
            by_library[item["id"]] = item
            result["libraries"].append(item)
        for candidate in candidate_rows:
            item = dict(candidate)
            item["created_at"] = _iso(item["created_at"])
            by_library[item["library_result_id"]]["candidates"].append(item)
        return result

    def status(self) -> dict:
        settings = self.get_settings().to_dict()
        latest = self.list_cycles(limit=1)
        return {"settings": settings, "lease": self.lease_status(), "latest_cycle": latest[0] if latest else None}

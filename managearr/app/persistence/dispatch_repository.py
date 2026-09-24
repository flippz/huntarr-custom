"""Persistence for the manual Sonarr dispatch ledger.

Two write paths exist:

1. A **reservation** transaction (``acquire_library_lock`` +
   ``expire_stale_reservations`` + the cap/cooldown read helpers +
   ``create_batch``/``create_items``) that all run inside one caller-
   supplied ``conn`` so a whole "re-plan, then reserve" sequence is
   atomic and serialized per library via ``pg_advisory_xact_lock`` - see
   ``app/services/dispatch_service.py``. The lock is automatically
   released when that transaction commits or rolls back.
2. A **finalize** step (``finalize_attempt``) that runs in its own fresh
   transaction *after* the Sonarr HTTP call returns. It takes the same
   per-library lock and advances the batch and reserved items atomically,
   so planning cannot observe a half-finalized attempt. No transaction is
   held across the network call.

Only ``state = 'dispatched'`` items on a completed/partial manual batch
count as real dispatches for cooldown. Hourly capacity additionally counts
live reservations, preventing two concurrent requests for different
episodes from overbooking the cap. Failed attempts release their capacity
while their protected audit rows remain.
"""
from datetime import datetime, timedelta, timezone

from ..domain.dispatch import DispatchBatch, DispatchBatchItem, RESERVATION_STALE_SECONDS
from .database import Database

# Advisory lock "class" id namespacing dispatch-reservation locks from
# any other advisory lock usage. The lock key is the library id.
_LOCK_CLASSID = 87234


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_batch(row, items: list[DispatchBatchItem] | None = None) -> DispatchBatch:
    return DispatchBatch(
        id=row["id"],
        scan_job_id=row["scan_job_id"],
        library_id=row["library_id"],
        library_name=row["library_name"],
        mode=row["mode"],
        state=row["state"],
        requested_count=row["requested_count"],
        selected_count=row["selected_count"],
        dispatched_count=row["dispatched_count"],
        sonarr_command_id=row["sonarr_command_id"],
        sonarr_command_status=row["sonarr_command_status"],
        error_summary=row["error_summary"],
        reconciliation_state=row["reconciliation_state"],
        reconciliation_summary=row["reconciliation_summary"],
        last_reconciled_at=(
            row["last_reconciled_at"].isoformat() if row["last_reconciled_at"] else None
        ),
        command_observed_state=row["command_observed_state"],
        created_at=row["created_at"].isoformat(),
        updated_at=row["updated_at"].isoformat(),
        items=items,
    )


def _row_to_item(row) -> DispatchBatchItem:
    return DispatchBatchItem(
        id=row["id"],
        batch_id=row["batch_id"],
        candidate_id=row["candidate_id"],
        episode_id=row["episode_id"],
        series_id=row["series_id"],
        series_title=row["series_title"],
        season_number=row["season_number"],
        episode_number=row["episode_number"],
        state=row["state"],
        reason=row["reason"],
        created_at=row["created_at"].isoformat(),
        updated_at=row["updated_at"].isoformat(),
    )


class DispatchRepository:
    def __init__(self, db: Database):
        self.db = db

    def _run(self, conn, sql, params):
        if conn is not None:
            return conn.execute(sql, params)
        with self.db.connect() as owned_conn:
            return owned_conn.execute(sql, params)

    # --- concurrency -----------------------------------------------------

    def acquire_library_lock(self, conn, library_id: int) -> None:
        """Serialize dispatch reservations for one library. Must be
        called inside the transaction that also performs the re-plan and
        reservation insert - the lock releases automatically at
        commit/rollback of ``conn``'s transaction."""
        conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", (_LOCK_CLASSID, library_id))

    def expire_stale_reservations(self, library_id: int, *, conn=None) -> None:
        """Mark any stale dispatch reservation as operator-visible ambiguity.

        Managearr never retries it: Sonarr may have accepted the command before
        the process stopped. A recorded command id can be reconciled; without
        one the operator must inspect Sonarr manually.

        Mark any 'dispatching' batch (and its 'reserved' items) whose
        reservation is older than ``RESERVATION_STALE_SECONDS`` - e.g. the
        process crashed between reserving and calling Sonarr. There is no
        background sweep; this only ever runs inline while handling a new
        request for the same library."""
        cutoff = _now() - timedelta(seconds=RESERVATION_STALE_SECONDS)
        now = _now()
        stale_rows = self._run(
            conn,
            """
            UPDATE dispatch_batches
            SET state = 'ambiguous',
                error_summary = CASE
                    WHEN sonarr_command_id IS NULL THEN
                        'dispatch outcome is ambiguous; inspect Sonarr manually before any retry'
                    ELSE
                        'Sonarr accepted the command but local finalization did not complete'
                END,
                reconciliation_state = 'operator_review',
                reconciliation_summary = CASE
                    WHEN sonarr_command_id IS NULL THEN
                        'No Sonarr command id was recorded; inspect Sonarr manually. Managearr will not retry.'
                    ELSE
                        'A Sonarr command id is available for manual reconciliation. Managearr will not retry.'
                END,
                updated_at = %s
            WHERE library_id = %s AND mode = 'manual' AND state = 'dispatching'
              AND created_at < %s
            RETURNING id
            """,
            (now, library_id, cutoff),
        ).fetchall()
        stale_batch_ids = [row["id"] for row in stale_rows]
        if not stale_batch_ids:
            return
        self._run(
            conn,
            """
            UPDATE dispatch_batch_items
            SET state = 'ambiguous',
                reason = 'dispatch reservation expired with an unknown local finalization outcome',
                updated_at = %s
            WHERE batch_id = ANY(%s) AND state = 'reserved'
            """,
            (now, stale_batch_ids),
        )

    def active_reservation_episode_ids(self, library_id: int, episode_ids: list[int], *, conn=None) -> set[int]:
        """Episode ids among ``episode_ids`` that currently have a live
        (non-stale) in-flight reservation for this library - used to stop
        two concurrent requests from dispatching the same episode."""
        if not episode_ids:
            return set()
        cutoff = _now() - timedelta(seconds=RESERVATION_STALE_SECONDS)
        rows = self._run(
            conn,
            """
            SELECT DISTINCT i.episode_id
            FROM dispatch_batch_items i
            JOIN dispatch_batches b ON b.id = i.batch_id
            WHERE b.library_id = %s AND b.mode = 'manual' AND b.state = 'dispatching'
              AND i.state = 'reserved' AND i.episode_id = ANY(%s)
              AND i.created_at >= %s
            """,
            (library_id, list(episode_ids), cutoff),
        ).fetchall()
        return {row["episode_id"] for row in rows}

    # --- planning reads ----------------------------------------------------

    def count_dispatched_since(self, library_id: int, since: datetime, *, conn=None) -> int:
        """Number of episodes successfully dispatched (real Sonarr
        searches, never dry-run) for this library since ``since`` -
        the basis for the remaining hourly-cap calculation."""
        row = self._run(
            conn,
            """
            SELECT COUNT(*) AS c
            FROM dispatch_batch_items i
            JOIN dispatch_batches b ON b.id = i.batch_id
            WHERE b.library_id = %s AND b.mode = 'manual'
              AND b.state IN ('completed', 'partial') AND i.state = 'dispatched'
              AND i.updated_at >= %s
            """,
            (library_id, since),
        ).fetchone()
        return row["c"]

    def count_capacity_used_since(self, library_id: int, since: datetime, *, conn=None) -> int:
        """Count completed dispatches in the rolling window plus every
        live reservation. One statement gives planning a consistent
        snapshot and cannot double-count an item while it is finalized."""
        reservation_cutoff = _now() - timedelta(seconds=RESERVATION_STALE_SECONDS)
        row = self._run(
            conn,
            """
            SELECT COUNT(*) AS c
            FROM dispatch_batch_items i
            JOIN dispatch_batches b ON b.id = i.batch_id
            WHERE b.library_id = %s AND b.mode = 'manual' AND (
                (b.state IN ('completed', 'partial') AND i.state = 'dispatched'
                    AND i.updated_at >= %s)
                OR
                (b.state = 'dispatching' AND i.state = 'reserved'
                    AND i.created_at >= %s)
            )
            """,
            (library_id, since, reservation_cutoff),
        ).fetchone()
        return row["c"]

    def dispatched_episode_ids_since(self, library_id: int, episode_ids: list[int], since: datetime, *, conn=None) -> set[int]:
        """Episode ids among ``episode_ids`` that were successfully
        dispatched for this library since ``since`` - drives the
        cooldown exclusion."""
        if not episode_ids:
            return set()
        rows = self._run(
            conn,
            """
            SELECT DISTINCT i.episode_id
            FROM dispatch_batch_items i
            JOIN dispatch_batches b ON b.id = i.batch_id
            WHERE b.library_id = %s AND b.mode = 'manual'
              AND b.state IN ('completed', 'partial') AND i.state = 'dispatched'
              AND i.episode_id = ANY(%s) AND i.updated_at >= %s
            """,
            (library_id, list(episode_ids), since),
        ).fetchall()
        return {row["episode_id"] for row in rows}

    # --- writes --------------------------------------------------------

    def create_batch(self, conn, data: dict) -> int:
        now = _now()
        row = self._run(
            conn,
            """
            INSERT INTO dispatch_batches (
                scan_job_id, library_id, library_name, mode, state,
                requested_count, selected_count, dispatched_count,
                sonarr_command_id, sonarr_command_status, error_summary,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                data["scan_job_id"],
                data.get("library_id"),
                data["library_name"],
                data["mode"],
                data["state"],
                data.get("requested_count", 0),
                data.get("selected_count", 0),
                data.get("dispatched_count", 0),
                data.get("sonarr_command_id"),
                data.get("sonarr_command_status"),
                data.get("error_summary", ""),
                now,
                now,
            ),
        ).fetchone()
        return row["id"]

    def create_items(self, conn, batch_id: int, items: list[dict]) -> list[int]:
        if not items:
            return []
        now = _now()
        ids = []
        with conn.cursor() as cur:
            for item in items:
                cur.execute(
                    """
                    INSERT INTO dispatch_batch_items (
                        batch_id, candidate_id, episode_id, series_id, series_title,
                        season_number, episode_number, state, reason, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        batch_id,
                        item["candidate_id"],
                        item["episode_id"],
                        item["series_id"],
                        item["series_title"],
                        item["season_number"],
                        item["episode_number"],
                        item["state"],
                        item.get("reason"),
                        now,
                        now,
                    ),
                )
                ids.append(cur.fetchone()["id"])
        return ids

    def finalize_attempt(
        self,
        *,
        batch_id: int,
        item_ids: list[int],
        library_id: int,
        batch_state: str,
        item_state: str,
        dispatched_count: int,
        sonarr_command_id: int | None = None,
        sonarr_command_status: str | None = None,
        error_summary: str = "",
        item_reason: str | None = None,
    ) -> bool:
        """Atomically finish one reserved attempt.

        Returns ``False`` if the reservation is no longer live (for
        example, another request expired it after a process stall). The
        existing terminal audit record is left untouched in that case.
        """
        now = _now()
        with self.db.connect() as conn:
            self.acquire_library_lock(conn, library_id)
            batch_row = conn.execute(
                "SELECT state FROM dispatch_batches WHERE id = %s FOR UPDATE",
                (batch_id,),
            ).fetchone()
            if batch_row is None or batch_row["state"] != "dispatching":
                return False

            updated_items = conn.execute(
                """
                UPDATE dispatch_batch_items
                SET state = %s, reason = %s, updated_at = %s
                WHERE id = ANY(%s) AND batch_id = %s AND state = 'reserved'
                """,
                (item_state, item_reason, now, item_ids, batch_id),
            ).rowcount
            if updated_items != len(item_ids):
                raise RuntimeError("dispatch reservation items were not all live")

            updated_batch = conn.execute(
                """
                UPDATE dispatch_batches
                SET state = %s, dispatched_count = %s, sonarr_command_id = %s,
                    sonarr_command_status = %s, error_summary = %s, updated_at = %s
                WHERE id = %s AND state = 'dispatching'
                """,
                (
                    batch_state,
                    dispatched_count,
                    sonarr_command_id,
                    sonarr_command_status,
                    error_summary,
                    now,
                    batch_id,
                ),
            ).rowcount
            if updated_batch != 1:
                raise RuntimeError("dispatch reservation batch was not live")
        return True

    def record_command_acceptance(
        self,
        *,
        batch_id: int,
        library_id: int,
        sonarr_command_id: int,
        sonarr_command_status: str | None,
    ) -> bool:
        """Durably record Sonarr acceptance before finalizing item states.

        This narrows the crash window: if the process stops after this commit,
        stale-reservation handling exposes an ambiguous batch that can still be
        reconciled by its known command id. It never resends the search.
        """
        now = _now()
        with self.db.connect() as conn:
            self.acquire_library_lock(conn, library_id)
            updated = conn.execute(
                """
                UPDATE dispatch_batches
                SET sonarr_command_id = %s, sonarr_command_status = %s, updated_at = %s
                WHERE id = %s AND library_id = %s AND state = 'dispatching'
                  AND sonarr_command_id IS NULL
                """,
                (sonarr_command_id, sonarr_command_status, now, batch_id, library_id),
            ).rowcount
        return updated == 1

    # --- reads -----------------------------------------------------------

    def get_batch(self, batch_id: int, *, with_items: bool = True) -> DispatchBatch | None:
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM dispatch_batches WHERE id = %s", (batch_id,)).fetchone()
            if row is None:
                return None
            items = None
            if with_items:
                item_rows = conn.execute(
                    "SELECT * FROM dispatch_batch_items WHERE batch_id = %s ORDER BY id", (batch_id,)
                ).fetchall()
                items = [_row_to_item(r) for r in item_rows]
        return _row_to_batch(row, items)

    def list_for_job(self, scan_job_id: int, *, limit: int = 50) -> list[DispatchBatch]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM dispatch_batches WHERE scan_job_id = %s
                ORDER BY created_at DESC LIMIT %s
                """,
                (scan_job_id, limit),
            ).fetchall()
        return [_row_to_batch(row) for row in rows]

    def list_recent(self, *, limit: int = 50) -> list[DispatchBatch]:
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT id FROM dispatch_batches ORDER BY created_at DESC LIMIT %s", (limit,)
            ).fetchall()
        return [self.get_batch(row["id"], with_items=True) for row in rows]

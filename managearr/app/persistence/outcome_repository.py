"""Transactional persistence for append-only reconciliation evidence."""
from datetime import datetime, timezone

from ..domain.outcome import OutcomeEvent, ReconciliationAttempt
from .database import Database


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _event(row) -> OutcomeEvent:
    return OutcomeEvent(
        id=row["id"], batch_id=row["batch_id"], dispatch_item_id=row["dispatch_item_id"],
        candidate_id=row["candidate_id"], episode_id=row["episode_id"],
        observed_at=row["observed_at"].isoformat(), source_endpoint=row["source_endpoint"],
        event_type=row["event_type"], event_state=row["event_state"],
        safe_summary=row["safe_summary"], sonarr_command_id=row["sonarr_command_id"],
        sonarr_event_id=row["sonarr_event_id"], download_id=row["download_id"],
    )


def _attempt(row) -> ReconciliationAttempt:
    return ReconciliationAttempt(
        id=row["id"], batch_id=row["batch_id"], observed_at=row["observed_at"].isoformat(),
        state=row["state"], safe_summary=row["safe_summary"],
        command_endpoint_read=row["command_endpoint_read"],
        history_endpoint_read=row["history_endpoint_read"],
        queue_endpoint_read=row["queue_endpoint_read"],
        inserted_event_count=row["inserted_event_count"],
    )


class OutcomeRepository:
    def __init__(self, db: Database):
        self.db = db

    def record_reconciliation(
        self,
        *,
        batch_id: int,
        state: str,
        summary: str,
        command_state: str,
        events: list[dict],
        endpoint_reads: dict[str, bool],
    ) -> tuple[ReconciliationAttempt, int]:
        """Dedupe evidence and append the attempt in one short transaction."""
        observed_at = _now()
        inserted = 0
        with self.db.connect() as conn:
            locked = conn.execute(
                """
                SELECT reconciliation_state, reconciliation_summary, command_observed_state
                FROM dispatch_batches WHERE id = %s FOR UPDATE
                """,
                (batch_id,),
            ).fetchone()
            if locked is None:
                raise LookupError("dispatch batch not found")
            aggregate_state = state
            aggregate_summary = summary
            aggregate_command_state = (
                locked["command_observed_state"]
                if command_state == "unknown" and locked["command_observed_state"]
                else command_state
            )
            rank = {
                "not_reconciled": 0,
                "operator_review": 0,
                "error": 0,
                "unresolved": 1,
                "partial": 2,
                "resolved": 3,
            }
            if rank[locked["reconciliation_state"]] > rank[state]:
                aggregate_state = locked["reconciliation_state"]
                aggregate_summary = locked["reconciliation_summary"]

            for event in events:
                result = conn.execute(
                    """
                    INSERT INTO dispatch_outcome_events (
                        batch_id, dispatch_item_id, candidate_id, episode_id, observed_at,
                        source_endpoint, event_type, event_state, safe_summary,
                        sonarr_command_id, sonarr_event_id, download_id, evidence_key
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (batch_id, source_endpoint, evidence_key) DO NOTHING
                    RETURNING id
                    """,
                    (
                        batch_id, event.get("dispatch_item_id"), event.get("candidate_id"),
                        event.get("episode_id"), observed_at, event["source_endpoint"],
                        event["event_type"], event["event_state"], event["safe_summary"],
                        event.get("sonarr_command_id"), event.get("sonarr_event_id"),
                        event.get("download_id"), event["evidence_key"],
                    ),
                ).fetchone()
                inserted += int(result is not None)

            attempt_row = conn.execute(
                """
                INSERT INTO dispatch_reconciliation_attempts (
                    batch_id, observed_at, state, safe_summary,
                    command_endpoint_read, history_endpoint_read, queue_endpoint_read,
                    inserted_event_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    batch_id, observed_at, state, summary,
                    bool(endpoint_reads.get("command")), bool(endpoint_reads.get("history")),
                    bool(endpoint_reads.get("queue")), inserted,
                ),
            ).fetchone()
            conn.execute(
                """
                UPDATE dispatch_batches
                SET reconciliation_state = %s, reconciliation_summary = %s,
                    last_reconciled_at = %s, command_observed_state = %s, updated_at = %s
                WHERE id = %s
                """,
                (
                    aggregate_state, aggregate_summary, observed_at,
                    aggregate_command_state, observed_at, batch_id,
                ),
            )
        return _attempt(attempt_row), inserted

    def list_events(self, batch_id: int, *, limit: int = 500) -> list[OutcomeEvent]:
        limit = max(1, min(int(limit), 500))
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM dispatch_outcome_events
                WHERE batch_id = %s ORDER BY observed_at, id LIMIT %s
                """,
                (batch_id, limit),
            ).fetchall()
        return [_event(row) for row in rows]

    def list_attempts(self, batch_id: int, *, limit: int = 100) -> list[ReconciliationAttempt]:
        limit = max(1, min(int(limit), 100))
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM dispatch_reconciliation_attempts
                WHERE batch_id = %s ORDER BY observed_at DESC, id DESC LIMIT %s
                """,
                (batch_id, limit),
            ).fetchall()
        return [_attempt(row) for row in rows]

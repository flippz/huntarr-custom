"""Persistence for the singleton AutomationPolicy row."""
from datetime import datetime, timezone

from ..domain.automation_policy import AutomationPolicy, BALANCED_DEFAULTS
from .database import Database


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_policy(row) -> AutomationPolicy:
    return AutomationPolicy(
        missing_enabled=bool(row["missing_enabled"]),
        upgrades_enabled=bool(row["upgrades_enabled"]),
        cycle_interval_minutes=row["cycle_interval_minutes"],
        hourly_api_cap=row["hourly_api_cap"],
        successful_grab_target=row["successful_grab_target"],
        dispatch_interval_seconds=row["dispatch_interval_seconds"],
        queue_target=row["queue_target"],
        cooldown_minutes=row["cooldown_minutes"],
        search_order=row["search_order"],
        updated_at=row["updated_at"],
    )


class PolicyRepository:
    def __init__(self, db: Database):
        self.db = db

    def get(self) -> AutomationPolicy:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM automation_policy WHERE id = 1"
            ).fetchone()
            if row is None:
                now = _now()
                defaults = BALANCED_DEFAULTS
                conn.execute(
                    """
                    INSERT INTO automation_policy (
                        id, missing_enabled, upgrades_enabled, cycle_interval_minutes,
                        hourly_api_cap, successful_grab_target, dispatch_interval_seconds,
                        queue_target, cooldown_minutes, search_order, updated_at
                    ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(defaults["missing_enabled"]),
                        int(defaults["upgrades_enabled"]),
                        defaults["cycle_interval_minutes"],
                        defaults["hourly_api_cap"],
                        defaults["successful_grab_target"],
                        defaults["dispatch_interval_seconds"],
                        defaults["queue_target"],
                        defaults["cooldown_minutes"],
                        defaults["search_order"],
                        now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM automation_policy WHERE id = 1"
                ).fetchone()
        return _row_to_policy(row)

    def update(self, data: dict) -> AutomationPolicy:
        current = self.get().to_dict()
        current.pop("updated_at", None)
        current.update(data)
        now = _now()

        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE automation_policy
                SET missing_enabled = ?, upgrades_enabled = ?, cycle_interval_minutes = ?,
                    hourly_api_cap = ?, successful_grab_target = ?, dispatch_interval_seconds = ?,
                    queue_target = ?, cooldown_minutes = ?, search_order = ?, updated_at = ?
                WHERE id = 1
                """,
                (
                    int(current["missing_enabled"]),
                    int(current["upgrades_enabled"]),
                    current["cycle_interval_minutes"],
                    current["hourly_api_cap"],
                    current["successful_grab_target"],
                    current["dispatch_interval_seconds"],
                    current["queue_target"],
                    current["cooldown_minutes"],
                    current["search_order"],
                    now,
                ),
            )
        return self.get()

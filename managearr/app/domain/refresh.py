"""Domain values for M5 scheduled read-only refresh/reconciliation.

Two kinds of durable, read-only worker work exist:

- ``scan``: one read-only Sonarr candidate scan for one library, run by
  ``SonarrScanService`` (never a Sonarr command).
- ``reconcile``: one bounded pass of ``ReconciliationService`` over already
  dispatched batches that still have a recorded Sonarr command id and a
  nonterminal/incomplete outcome (also read-only; sends no search).

There is no ``live`` mode here, mirroring the M4 scheduler: this module only
ever governs when to run read-only refresh work, never whether to dispatch.
"""
from dataclasses import dataclass

REFRESH_KINDS = ("scan", "reconcile")
REFRESH_TRIGGERS = ("scheduled", "manual")
REFRESH_RUN_STATES = ("queued", "running", "completed", "partial", "failed", "skipped")
REFRESH_REQUEST_STATES = ("queued", "claimed", "completed", "failed")

_LIMITS = {
    "scan_max_age_minutes": (5, 10080),
    "reconcile_min_interval_minutes": (5, 1440),
    "reconcile_max_per_cycle": (1, 50),
}


@dataclass(frozen=True)
class RefreshSettings:
    scan_max_age_minutes: int
    reconcile_min_interval_minutes: int
    reconcile_max_per_cycle: int
    next_reconcile_due_at: str | None
    updated_at: str

    def to_dict(self) -> dict:
        return {
            "scan_max_age_minutes": self.scan_max_age_minutes,
            "reconcile_min_interval_minutes": self.reconcile_min_interval_minutes,
            "reconcile_max_per_cycle": self.reconcile_max_per_cycle,
            "next_reconcile_due_at": self.next_reconcile_due_at,
            "updated_at": self.updated_at,
        }


def validate_refresh_settings(data) -> list[str]:
    if not isinstance(data, dict):
        return ["request body must be a JSON object"]
    errors: list[str] = []
    extra = set(data) - set(_LIMITS)
    if extra:
        errors.append(
            "only scan_max_age_minutes, reconcile_min_interval_minutes, "
            "reconcile_max_per_cycle may be changed"
        )
    if not data:
        errors.append("at least one setting is required")
    for field, (lo, hi) in _LIMITS.items():
        if field not in data:
            continue
        value = data[field]
        if not isinstance(value, int) or isinstance(value, bool):
            errors.append(f"{field} must be an integer")
        elif not (lo <= value <= hi):
            errors.append(f"{field} must be between {lo} and {hi}")
    return errors

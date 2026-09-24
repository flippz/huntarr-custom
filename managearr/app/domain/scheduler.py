"""Safe scheduler domain values for the simulation-only M4 worker."""
from dataclasses import dataclass

SCHEDULER_MODES = ("off", "simulate")
CYCLE_STATES = ("queued", "running", "completed", "partial", "failed", "skipped")
CYCLE_TRIGGERS = ("scheduled", "manual")
MAX_SIMULATION_SELECTION = 25


@dataclass(frozen=True)
class SchedulerSettings:
    mode: str
    next_due_at: str | None
    updated_at: str

    def to_dict(self) -> dict:
        return {
            "mode": self.mode,
            "next_due_at": self.next_due_at,
            "updated_at": self.updated_at,
        }


def validate_scheduler_settings(data) -> list[str]:
    if not isinstance(data, dict):
        return ["request body must be a JSON object"]
    extra = set(data) - {"mode"}
    errors = []
    if extra:
        errors.append("only mode may be changed")
    if "mode" not in data:
        errors.append("mode is required")
    elif data["mode"] not in SCHEDULER_MODES:
        errors.append("mode must be one of: off, simulate")
    return errors

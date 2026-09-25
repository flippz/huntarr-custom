"""Domain model for the automation policy that will eventually govern
Managearr v1's search/grab/import cycle.

This milestone only models, validates and summarizes the policy. No
engine consumes it yet - see V2.md non-goals.
"""
from dataclasses import dataclass

SEARCH_ORDERS = ("sequential", "random", "oldest_first", "newest_first")

# "Balanced" preset - a middle ground between an aggressive hourly hammer
# and an overly conservative once-a-day trickle.
BALANCED_DEFAULTS = {
    "missing_enabled": True,
    "upgrades_enabled": True,
    "cycle_interval_minutes": 60,
    "hourly_api_cap": 20,
    "successful_grab_target": 5,
    "dispatch_interval_seconds": 30,
    "queue_target": 10,
    "cooldown_minutes": 15,
    "search_order": "sequential",
}

_LIMITS = {
    "cycle_interval_minutes": (5, 1440),
    "hourly_api_cap": (1, 1000),
    "successful_grab_target": (1, 100),
    "dispatch_interval_seconds": (5, 3600),
    "queue_target": (1, 100),
    "cooldown_minutes": (0, 1440),
}


@dataclass
class AutomationPolicy:
    missing_enabled: bool
    upgrades_enabled: bool
    cycle_interval_minutes: int
    hourly_api_cap: int
    successful_grab_target: int
    dispatch_interval_seconds: int
    queue_target: int
    cooldown_minutes: int
    search_order: str
    updated_at: str = ""

    @classmethod
    def balanced_defaults(cls) -> "AutomationPolicy":
        return cls(updated_at="", **BALANCED_DEFAULTS)

    def to_dict(self) -> dict:
        return {
            "missing_enabled": self.missing_enabled,
            "upgrades_enabled": self.upgrades_enabled,
            "cycle_interval_minutes": self.cycle_interval_minutes,
            "hourly_api_cap": self.hourly_api_cap,
            "successful_grab_target": self.successful_grab_target,
            "dispatch_interval_seconds": self.dispatch_interval_seconds,
            "queue_target": self.queue_target,
            "cooldown_minutes": self.cooldown_minutes,
            "search_order": self.search_order,
            "updated_at": self.updated_at,
        }

    def summary(self) -> str:
        """A plain-language description of the current policy."""
        modes = []
        if self.missing_enabled:
            modes.append("missing items")
        if self.upgrades_enabled:
            modes.append("upgrades")
        modes_text = " and ".join(modes) if modes else "nothing (missing and upgrade search are both disabled)"

        order_text = self.search_order.replace("_", " ")

        return (
            f"Searches for {modes_text}, running a full cycle every "
            f"{self.cycle_interval_minutes} minute(s) with a cap of "
            f"{self.hourly_api_cap} API call(s) per hour. Aims for "
            f"up to {min(self.successful_grab_target, 5)} eligible search(es) per cycle "
            f"(hard maximum 5), at least {self.dispatch_interval_seconds} "
            f"second(s) apart, while keeping the queue at up to {self.queue_target} "
            f"item(s). Waits {self.cooldown_minutes} minute(s) of cooldown "
            f"before retrying the same item, processing items in "
            f"{order_text} order."
        )


def validate_policy_input(data: dict, *, partial: bool = False) -> list[str]:
    errors: list[str] = []

    def required(field: str) -> bool:
        if partial:
            return field in data
        return True

    for bool_field in ("missing_enabled", "upgrades_enabled"):
        if required(bool_field):
            if not isinstance(data.get(bool_field), bool):
                errors.append(f"{bool_field} must be a boolean")

    for int_field, (lo, hi) in _LIMITS.items():
        if required(int_field):
            value = data.get(int_field)
            if not isinstance(value, int) or isinstance(value, bool):
                errors.append(f"{int_field} must be an integer")
            elif not (lo <= value <= hi):
                errors.append(f"{int_field} must be between {lo} and {hi}")

    if required("search_order"):
        if data.get("search_order") not in SEARCH_ORDERS:
            errors.append(f"search_order must be one of: {', '.join(SEARCH_ORDERS)}")

    return errors

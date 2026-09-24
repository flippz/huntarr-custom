"""Domain model for durable activity jobs.

A job tracks one unit of hunting work (a single wanted item moving
through search -> grab -> import) as it progresses through the
pipeline. No engine writes real jobs yet in this milestone - the table
starts empty and stays empty until a future milestone wires up actual
searching. This module intentionally contains no seed/fake data.
"""
from dataclasses import dataclass
from typing import Optional

# Ordered lifecycle. "deferred", "failed" and "rolled_back" are terminal-ish
# states that a future engine may re-queue from, but that is out of scope
# here - this module only models the states, not transitions between them.
JOB_STATES = (
    "planned",
    "searching",
    "selected",
    "sent",
    "downloading",
    "importing",
    "verifying",
    "completed",
    "deferred",
    "failed",
    "rolled_back",
)


@dataclass
class ActivityJob:
    id: Optional[int]
    library_id: Optional[int]
    library_name: str
    state: str
    title: str
    details: str
    created_at: str
    updated_at: str

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "library_id": self.library_id,
            "library_name": self.library_name,
            "state": self.state,
            "title": self.title,
            "details": self.details,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

"""Domain model for durable activity jobs.

A job tracks one unit of work moving through a pipeline. The
"legacy" job_type ("planned -> ... -> completed") models a future
hunting engine and is not written by anything in this milestone. The
"sonarr_scan" job_type is written by ``SonarrScanService`` and only
ever moves ``searching -> completed`` or ``searching -> failed`` - it
represents one read-only candidate scan of one library and never
issues a Sonarr command.
"""
from dataclasses import dataclass
from typing import Optional

# Ordered lifecycle. "deferred", "failed" and "rolled_back" are terminal-ish
# states that a future engine may re-queue from, but that is out of scope
# here - this module only models the states, not transitions between them.
# "searching", "completed" and "failed" double as the (simpler) lifecycle
# used by "sonarr_scan" jobs.
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

JOB_TYPES = ("legacy", "sonarr_scan")


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
    job_type: str = "legacy"
    candidate_count: int = 0

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "library_id": self.library_id,
            "library_name": self.library_name,
            "job_type": self.job_type,
            "state": self.state,
            "title": self.title,
            "details": self.details,
            "candidate_count": self.candidate_count,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

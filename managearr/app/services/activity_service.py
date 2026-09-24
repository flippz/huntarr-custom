"""Read-only application service for activity jobs.

Deliberately exposes no create/update methods to the API layer - jobs
are only ever written by the (future) search engine or by tests
exercising the repository directly.
"""
from ..domain.activity import ActivityJob, JOB_STATES
from ..persistence.activity_repository import ActivityRepository


class ActivityService:
    def __init__(self, repository: ActivityRepository):
        self.repository = repository

    def list_jobs(self, *, state: str | None = None, limit: int = 100) -> list[ActivityJob]:
        if state is not None and state not in JOB_STATES:
            return []
        return self.repository.list_all(state=state, limit=limit)

    def get_job(self, job_id: int) -> ActivityJob | None:
        return self.repository.get(job_id)

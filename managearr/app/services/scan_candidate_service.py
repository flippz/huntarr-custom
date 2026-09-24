"""Read-only application service for scan candidate snapshot rows."""
from ..domain.scan_candidate import ScanCandidate
from ..persistence.scan_candidate_repository import ScanCandidateRepository


class ScanCandidateService:
    def __init__(self, repository: ScanCandidateRepository):
        self.repository = repository

    def list_for_job(self, job_id: int) -> list[ScanCandidate]:
        return self.repository.list_for_job(job_id)

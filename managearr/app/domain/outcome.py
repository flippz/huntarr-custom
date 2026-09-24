"""Safe, bounded reconciliation audit models."""
from dataclasses import dataclass
from typing import Optional


@dataclass
class OutcomeEvent:
    id: int
    batch_id: int
    dispatch_item_id: Optional[int]
    candidate_id: Optional[int]
    episode_id: Optional[int]
    observed_at: str
    source_endpoint: str
    event_type: str
    event_state: str
    safe_summary: str
    sonarr_command_id: Optional[int]
    sonarr_event_id: Optional[int]
    download_id: Optional[str]

    def to_dict(self) -> dict:
        return self.__dict__.copy()


@dataclass
class ReconciliationAttempt:
    id: int
    batch_id: int
    observed_at: str
    state: str
    safe_summary: str
    command_endpoint_read: bool
    history_endpoint_read: bool
    queue_endpoint_read: bool
    inserted_event_count: int

    def to_dict(self) -> dict:
        return self.__dict__.copy()

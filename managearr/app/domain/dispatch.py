"""Domain model for the manual Sonarr search dispatch ledger.

A ``DispatchBatch`` is one *attempt* to search for a set of candidates
from one completed ``sonarr_scan`` activity job - either a ``dry_run``
(preview, never touches Sonarr) or a ``manual`` dispatch (issues exactly
one ``POST /api/v3/command`` EpisodeSearch call, gated by an explicit
``confirm: true`` in the API request - see
``app/services/dispatch_service.py``). Each batch owns a set of
``DispatchBatchItem`` rows, one per candidate that was actually found
and validated against the batch's scan job - candidates that don't
exist or belong to a different job are reported in the API response's
``excluded`` list but never get a ledger row (there's nothing
meaningful to reference).

The ledger is append-only/audit-style: rows are never deleted, and a
batch/item's ``state`` only ever moves forward (see the transitions
documented on ``BATCH_STATES``/``ITEM_STATES`` below). A failed dispatch
still leaves its batch and item rows in place (state ``failed``) so the
attempt is auditable - it just stops counting toward the hourly cap or
cooldown window, since only ``dispatched`` items do (see
``DispatchRepository``).
"""
from dataclasses import dataclass
from typing import Optional

# Safe, documented cap on how many candidates one manual dispatch (or
# preview) request may select. Keeps a single request from being able to
# trigger an unbounded number of Sonarr searches in one call.
MAX_SELECTION_PER_REQUEST = 25

# A 'reserved' item (created by DispatchService right before the Sonarr
# call) that is still 'reserved' after this many seconds is treated as
# abandoned - e.g. the process crashed between reserving and calling
# Sonarr - and is excluded from concurrency/cooldown checks and expired
# to 'failed' the next time planning touches its library. There is no
# background sweep; expiry is only ever evaluated inline while handling
# a request for the same library (see
# ``DispatchRepository.expire_stale_reservations``).
RESERVATION_STALE_SECONDS = 300

# 'dry_run' batches are preview-only audit snapshots and never progress
# past 'planned'. 'manual' batches move 'dispatching' -> one of
# 'completed' (every requested candidate was eligible and dispatched),
# 'partial' (some requested candidates were excluded by planning but the
# eligible subset dispatched), or 'failed' (nothing was eligible, or the
# Sonarr call itself failed).
DISPATCH_MODES = ("dry_run", "manual")
BATCH_STATES = ("planned", "dispatching", "completed", "partial", "failed", "ambiguous")
ITEM_STATES = ("planned", "reserved", "dispatched", "failed", "excluded", "ambiguous")


@dataclass
class DispatchBatchItem:
    id: Optional[int]
    batch_id: int
    candidate_id: int
    episode_id: int
    series_id: int
    series_title: str
    season_number: int
    episode_number: int
    state: str
    reason: Optional[str]
    created_at: str
    updated_at: str

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "batch_id": self.batch_id,
            "candidate_id": self.candidate_id,
            "episode_id": self.episode_id,
            "series_id": self.series_id,
            "series_title": self.series_title,
            "season_number": self.season_number,
            "episode_number": self.episode_number,
            "state": self.state,
            "reason": self.reason,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass
class DispatchBatch:
    id: Optional[int]
    scan_job_id: int
    library_id: Optional[int]
    library_name: str
    mode: str
    state: str
    requested_count: int
    selected_count: int
    dispatched_count: int
    sonarr_command_id: Optional[int]
    sonarr_command_status: Optional[str]
    error_summary: str
    reconciliation_state: str
    reconciliation_summary: str
    last_reconciled_at: Optional[str]
    command_observed_state: Optional[str]
    created_at: str
    updated_at: str
    items: list[DispatchBatchItem] | None = None

    def to_dict(self) -> dict:
        data = {
            "id": self.id,
            "scan_job_id": self.scan_job_id,
            "library_id": self.library_id,
            "library_name": self.library_name,
            "mode": self.mode,
            "state": self.state,
            "requested_count": self.requested_count,
            "selected_count": self.selected_count,
            "dispatched_count": self.dispatched_count,
            "sonarr_command_id": self.sonarr_command_id,
            "sonarr_command_status": self.sonarr_command_status,
            "error_summary": self.error_summary,
            "reconciliation_state": self.reconciliation_state,
            "reconciliation_summary": self.reconciliation_summary,
            "last_reconciled_at": self.last_reconciled_at,
            "command_observed_state": self.command_observed_state,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        if self.items is not None:
            data["items"] = [item.to_dict() for item in self.items]
        return data

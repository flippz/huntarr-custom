"""Pure, deterministic classification for the Sonarr-only slow-download guard.

No I/O, no wall-clock reads - every function takes ``now`` explicitly so
tests can drive classification with a fake clock. This module only ever
*observes* and *classifies*; it never decides to call Sonarr. The worker/
service layer is responsible for turning a ``removal_pending`` classification
into an actual gated DELETE.

Design intent (see MANAGEARR.md "Slow-download guard" for the full
rationale): a normal slow download must be protected. Evidence accumulates
across whole, completed observation windows, never a single sample.

Two independent evidence tracks are kept, each with its own strike counter:

* "stall" evidence - no meaningful (>= ``progress_epsilon_bytes``) decrease in
  remaining bytes across a whole ``no_progress_window_minutes`` window. A
  single-sample decrease at or above the epsilon conclusively proves the
  download is not frozen, so it immediately resets *only* this track.
* "very slow" evidence - rolling throughput below
  ``very_slow_rate_bytes_per_second`` sustained across a whole
  ``very_slow_window_minutes`` window. This track is deliberately *not*
  reset by the stall epsilon check: a very-slow download is, by definition,
  still making normal per-poll byte progress (just too slowly in aggregate),
  so treating every per-sample decrease as "meaningful progress" would make
  this classification unreachable.

Either track reaching ``strikes_required`` strikes makes the item
removal-eligible. A counter reset/replace (sizeleft increases) or an
untrustworthy observation gap restarts both tracks from a fresh baseline
without blaming the item.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta

# Sonarr queue "status" values (already lower-cased by the adapter) that are
# never evaluated for stall/slow-speed evidence. Only an actively
# downloading-like item with bytes remaining is ever evaluated.
EXEMPT_STATUSES = frozenset({
    "queued", "delayed", "paused",
    "completed", "importpending", "importing",
    "warning", "manualintervention",
})
ACTIVE_STATUSES = frozenset({"downloading"})

# Hard-exempt regardless of ``status`` - Sonarr has already classified this
# download as import-blocked or carrying a warning/error, which is the
# import-failure reason policy's exclusive territory (see
# app/domain/import_failure.py). A queue record can legitimately still
# report status "downloading" while trackedDownloadState is "importBlocked"
# or trackedDownloadStatus is "warning"/"error" - this guard must never
# accumulate stall/slow-speed evidence against it, or revalidate it as
# removal-eligible, while that is true, since it is not a measured-
# progress problem this guard is meant to police.
EXEMPT_TRACKED_STATES = frozenset({"importblocked"})
EXEMPT_TRACKED_STATUSES = frozenset({"warning", "error"})

CLASSIFICATIONS = frozenset({
    "exempt", "grace", "healthy", "slow_watch",
    "stalled_evidence", "very_slow_evidence", "removal_pending",
})

# If the gap since the previous observation is at least this fraction of the
# no-progress window, the elapsed time cannot be trusted as continuous
# evidence (the worker may have been down, the lease may have moved, Sonarr
# may have been unreachable). Both windows are restarted from this
# observation instead of being allowed to silently "complete" on a gap.
GAP_RESET_FRACTION = 1.0


@dataclass(frozen=True)
class SlowDownloadSettings:
    initial_grace_minutes: int = 30
    no_progress_window_minutes: int = 30
    no_progress_min_observations: int = 3
    progress_epsilon_bytes: int = 1 * 1024 * 1024  # 1 MiB
    very_slow_rate_bytes_per_second: float = 50 * 1024  # 50 KiB/s
    very_slow_window_minutes: int = 90
    very_slow_min_remaining_bytes: int = 512 * 1024 * 1024  # 512 MiB
    very_slow_min_observations: int = 4
    strikes_required: int = 2


@dataclass(frozen=True)
class Observation:
    observed_at: datetime
    status: str  # already lower-cased Sonarr queue status
    size_bytes: int | None
    sizeleft_bytes: int | None
    tracked_state: str | None = None  # already lower-cased trackedDownloadState
    tracked_status: str | None = None  # already lower-cased trackedDownloadStatus


@dataclass(frozen=True)
class QueueItemState:
    """Durable per-queue-item tracking state. Mirrors the persisted row in
    ``slow_download_queue_items`` (see migrations.py v14)."""

    first_seen_at: datetime
    last_observed_at: datetime | None = None
    last_sizeleft_bytes: int | None = None
    last_meaningful_progress_at: datetime | None = None

    no_progress_window_started_at: datetime | None = None
    no_progress_window_start_sizeleft: int | None = None
    no_progress_window_observations: int = 0
    stall_strike_count: int = 0

    very_slow_window_started_at: datetime | None = None
    very_slow_window_start_sizeleft: int | None = None
    very_slow_window_observations: int = 0
    very_slow_strike_count: int = 0

    classification: str = "grace"
    reason: str = "awaiting first observation"

    @property
    def strike_count(self) -> int:
        """Combined strikes for display; removal eligibility is computed
        per-track, never by summing these two unrelated evidence types."""
        return max(self.stall_strike_count, self.very_slow_strike_count)


@dataclass(frozen=True)
class ClassificationResult:
    state: QueueItemState
    strike_recorded: bool
    reset_recorded: bool
    removal_eligible: bool


def is_exempt_status(status: str) -> bool:
    return (status or "").lower() in EXEMPT_STATUSES


def is_active_status(status: str) -> bool:
    return (status or "").lower() in ACTIVE_STATUSES


def is_exempt_tracked(tracked_state: str | None, tracked_status: str | None) -> bool:
    return (tracked_state or "").lower() in EXEMPT_TRACKED_STATES or (tracked_status or "").lower() in EXEMPT_TRACKED_STATUSES


def _minutes(delta: timedelta) -> float:
    return delta.total_seconds() / 60.0


def classify(
    settings: SlowDownloadSettings,
    state: QueueItemState,
    observation: Observation,
) -> ClassificationResult:
    """Fold one new observation into ``state`` and return the updated state.

    Callers (the service layer) are responsible for only invoking this with
    already-validated adapter output; this function is total over any
    ``Observation`` built from valid Sonarr fields.
    """
    now = observation.observed_at

    gap_minutes = None
    if state.last_observed_at is not None:
        gap_minutes = _minutes(now - state.last_observed_at)

    def _gap_too_long(window_minutes: int) -> bool:
        return gap_minutes is not None and gap_minutes >= window_minutes * GAP_RESET_FRACTION

    status = (observation.status or "").lower()
    remaining = observation.sizeleft_bytes
    tracked_exempt = is_exempt_tracked(observation.tracked_state, observation.tracked_status)
    exempt = is_exempt_status(status) or not is_active_status(status) or tracked_exempt
    no_remaining = remaining is None or remaining <= 0
    if exempt or no_remaining:
        # Freeze both windows while exempt/unknown so paused/queued time is
        # never read back as "no progress for N minutes" once the item
        # resumes. Strikes are preserved: pausing is not itself evidence of
        # anything, good or bad.
        if tracked_exempt:
            reason = (
                f"trackedDownloadState '{observation.tracked_state}'/trackedDownloadStatus "
                f"'{observation.tracked_status}' is hard-exempt from slow-download evaluation"
            )
        else:
            reason = f"status '{status}' is exempt from slow-download evaluation"
        new_state = replace(
            state,
            last_observed_at=now,
            no_progress_window_started_at=None,
            no_progress_window_start_sizeleft=None,
            no_progress_window_observations=0,
            very_slow_window_started_at=None,
            very_slow_window_start_sizeleft=None,
            very_slow_window_observations=0,
            classification="exempt",
            reason=reason,
        )
        return ClassificationResult(new_state, strike_recorded=False, reset_recorded=False, removal_eligible=False)

    in_grace = _minutes(now - state.first_seen_at) < settings.initial_grace_minutes

    if in_grace:
        # No evidence window may accumulate during the initial grace period:
        # a window that happened to open before grace expired and complete
        # exactly as grace ends would otherwise bank a strike for free,
        # letting removal fire right at grace expiry instead of after grace
        # plus full evidence windows measured from a clean post-grace
        # baseline. Freeze both tracks entirely (mirrors the exempt/no-
        # remaining branch above) so the first post-grace observation always
        # starts a fresh window.
        new_state = replace(
            state,
            last_observed_at=now,
            last_sizeleft_bytes=remaining,
            no_progress_window_started_at=None,
            no_progress_window_start_sizeleft=None,
            no_progress_window_observations=0,
            very_slow_window_started_at=None,
            very_slow_window_start_sizeleft=None,
            very_slow_window_observations=0,
            classification="grace",
            reason="within initial grace period",
        )
        return ClassificationResult(new_state, strike_recorded=False, reset_recorded=False, removal_eligible=False)

    last_sizeleft = state.last_sizeleft_bytes
    delta_bytes = None if last_sizeleft is None else last_sizeleft - remaining

    # A negative delta (sizeleft increased) means the queue record's
    # underlying download was replaced/restarted; an untrustworthy gap means
    # elapsed time can't be used as continuous evidence either way (the gap
    # is measured against whichever active window is shorter, since either
    # one completing on an untrustworthy gap would be wrong). Both start a
    # brand-new baseline on both tracks, clearing all strikes - the old
    # evidence no longer describes the current download.
    reset_needed = (
        delta_bytes is not None and delta_bytes < 0
    ) or _gap_too_long(min(settings.no_progress_window_minutes, settings.very_slow_window_minutes))

    if reset_needed:
        new_state = replace(
            state,
            last_observed_at=now,
            last_sizeleft_bytes=remaining,
            no_progress_window_started_at=now,
            no_progress_window_start_sizeleft=remaining,
            no_progress_window_observations=1,
            stall_strike_count=0,
            very_slow_window_started_at=now,
            very_slow_window_start_sizeleft=remaining,
            very_slow_window_observations=1,
            very_slow_strike_count=0,
            classification="healthy",
            reason=(
                "remaining bytes increased; starting a new baseline"
                if delta_bytes is not None and delta_bytes < 0
                else "observation gap too long to trust as continuous evidence; starting a new baseline"
            ),
        )
        reset_recorded = state.stall_strike_count > 0 or state.very_slow_strike_count > 0
        return ClassificationResult(new_state, strike_recorded=False, reset_recorded=reset_recorded, removal_eligible=False)

    meaningful_progress = delta_bytes is not None and delta_bytes >= settings.progress_epsilon_bytes

    # --- stall track: reset immediately on any single-sample epsilon decrease
    if meaningful_progress:
        no_progress_started, no_progress_start_sizeleft, no_progress_observations = now, remaining, 1
        stall_strike_count = 0
        stall_reset_now = state.stall_strike_count > 0
    else:
        no_progress_started = state.no_progress_window_started_at or now
        no_progress_start_sizeleft = (
            state.no_progress_window_start_sizeleft if state.no_progress_window_started_at is not None else remaining
        )
        no_progress_observations = (
            state.no_progress_window_observations + 1 if state.no_progress_window_started_at is not None else 1
        )
        stall_strike_count = state.stall_strike_count
        stall_reset_now = False

    # --- very-slow track: accumulates across normal per-poll decreases -----
    very_slow_started = state.very_slow_window_started_at or now
    very_slow_start_sizeleft = (
        state.very_slow_window_start_sizeleft if state.very_slow_window_started_at is not None else remaining
    )
    very_slow_observations = (
        state.very_slow_window_observations + 1 if state.very_slow_window_started_at is not None else 1
    )
    very_slow_strike_count = state.very_slow_strike_count

    no_progress_elapsed_minutes = _minutes(now - no_progress_started)
    very_slow_elapsed_minutes = _minutes(now - very_slow_started)

    stalled_window_complete = (
        not meaningful_progress
        and no_progress_elapsed_minutes >= settings.no_progress_window_minutes
        and no_progress_observations >= settings.no_progress_min_observations
    )
    stalled_window_decrease = no_progress_start_sizeleft - remaining if stalled_window_complete else None

    very_slow_window_complete = (
        very_slow_elapsed_minutes >= settings.very_slow_window_minutes
        and very_slow_observations >= settings.very_slow_min_observations
        and remaining >= settings.very_slow_min_remaining_bytes
    )
    very_slow_throughput = None
    if very_slow_window_complete:
        elapsed_seconds = (now - very_slow_started).total_seconds()
        decrease = very_slow_start_sizeleft - remaining
        very_slow_throughput = (decrease / elapsed_seconds) if elapsed_seconds > 0 else 0.0

    stalled_evidence = (
        stalled_window_complete
        and stalled_window_decrease is not None
        and stalled_window_decrease < settings.progress_epsilon_bytes
    )
    very_slow_evidence = (
        very_slow_window_complete
        and very_slow_throughput is not None
        and very_slow_throughput < settings.very_slow_rate_bytes_per_second
    )

    strike_recorded = False
    reset_recorded = stall_reset_now

    if stalled_evidence:
        stall_strike_count += 1
        strike_recorded = True
        no_progress_started, no_progress_start_sizeleft, no_progress_observations = now, remaining, 1
    elif stalled_window_complete:
        # Window completed showing real (if small) progress above the
        # epsilon - resets without a strike.
        no_progress_started, no_progress_start_sizeleft, no_progress_observations = now, remaining, 1
        if stall_strike_count > 0:
            reset_recorded = True
        stall_strike_count = 0

    if very_slow_evidence:
        very_slow_strike_count += 1
        strike_recorded = True
        very_slow_started, very_slow_start_sizeleft, very_slow_observations = now, remaining, 1
    elif very_slow_window_complete:
        very_slow_started, very_slow_start_sizeleft, very_slow_observations = now, remaining, 1
        if very_slow_strike_count > 0:
            reset_recorded = True
        very_slow_strike_count = 0

    removal_eligible = (
        stall_strike_count >= settings.strikes_required
        or very_slow_strike_count >= settings.strikes_required
    )

    if removal_eligible:
        classification = "removal_pending"
        parts = []
        if stall_strike_count >= settings.strikes_required:
            parts.append(
                f"no meaningful decrease in remaining bytes across {settings.strikes_required} "
                f"full {settings.no_progress_window_minutes}-minute windows"
            )
        if very_slow_strike_count >= settings.strikes_required:
            parts.append(
                f"throughput below {settings.very_slow_rate_bytes_per_second:.0f} B/s across "
                f"{settings.strikes_required} full {settings.very_slow_window_minutes}-minute windows"
            )
        reason = "; ".join(parts) + f" (strikes required: {settings.strikes_required})"
    elif stall_strike_count > 0:
        classification = "stalled_evidence"
        reason = (
            f"no meaningful decrease in remaining bytes for a full {settings.no_progress_window_minutes}-minute "
            f"window (strike {stall_strike_count}/{settings.strikes_required})"
        )
    elif very_slow_strike_count > 0:
        classification = "very_slow_evidence"
        reason = (
            f"sustained throughput below {settings.very_slow_rate_bytes_per_second:.0f} B/s for a full "
            f"{settings.very_slow_window_minutes}-minute window (strike {very_slow_strike_count}/{settings.strikes_required})"
        )
    elif not meaningful_progress and (no_progress_observations > 1 or very_slow_observations > 1):
        classification = "slow_watch"
        reason = "below-epsilon progress observed this window; watching for a completed evidence window"
    else:
        classification = "healthy"
        reason = "meaningful progress observed" if meaningful_progress else "actively downloading"

    new_state = replace(
        state,
        last_observed_at=now,
        last_sizeleft_bytes=remaining,
        last_meaningful_progress_at=now if meaningful_progress else state.last_meaningful_progress_at,
        no_progress_window_started_at=no_progress_started,
        no_progress_window_start_sizeleft=no_progress_start_sizeleft,
        no_progress_window_observations=no_progress_observations,
        stall_strike_count=stall_strike_count,
        very_slow_window_started_at=very_slow_started,
        very_slow_window_start_sizeleft=very_slow_start_sizeleft,
        very_slow_window_observations=very_slow_observations,
        very_slow_strike_count=very_slow_strike_count,
        classification=classification,
        reason=reason,
    )
    return ClassificationResult(
        new_state, strike_recorded=strike_recorded, reset_recorded=reset_recorded, removal_eligible=removal_eligible,
    )

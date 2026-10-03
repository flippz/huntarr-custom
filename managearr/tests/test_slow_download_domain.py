"""Deterministic, fake-clock classification tests for the slow-download guard.

No database, no I/O - every test drives app.domain.slow_download.classify()
with an explicit, advancing ``datetime`` so scenarios are fully reproducible.
"""
from datetime import datetime, timedelta, timezone

import pytest

from app.domain.slow_download import (
    Observation,
    QueueItemState,
    SlowDownloadSettings,
    classify,
    is_exempt_tracked,
)

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
GiB = 1024 * 1024 * 1024
MiB = 1024 * 1024
KiB = 1024


def settings(**overrides):
    return SlowDownloadSettings(**overrides)


def fresh_state(first_seen_at=T0):
    return QueueItemState(first_seen_at=first_seen_at)


def obs(t, status="downloading", size=10 * GiB, sizeleft=None, tracked_state=None, tracked_status=None):
    return Observation(
        observed_at=t, status=status, size_bytes=size, sizeleft_bytes=sizeleft,
        tracked_state=tracked_state, tracked_status=tracked_status,
    )


def feed(settings_, state, observations):
    """Run a sequence of observations through classify(), returning the
    list of ClassificationResult in order."""
    results = []
    for o in observations:
        result = classify(settings_, state, o)
        state = result.state
        results.append(result)
    return results


# --- Exemptions --------------------------------------------------------

@pytest.mark.parametrize("status", ["queued", "paused", "delayed", "completed", "importpending", "importing", "warning", "manualintervention"])
def test_exempt_statuses_never_accumulate_evidence(status):
    s = settings()
    state = fresh_state()
    results = feed(s, state, [obs(T0 + timedelta(minutes=i * 10), status=status, sizeleft=5 * GiB) for i in range(10)])
    assert all(r.state.classification == "exempt" for r in results)
    assert all(r.state.strike_count == 0 for r in results)


def test_unknown_status_is_exempt_not_stalled():
    s = settings()
    state = fresh_state()
    result = classify(s, state, obs(T0, status="unknowngarbage", sizeleft=5 * GiB))
    assert result.state.classification == "exempt"


def test_zero_remaining_bytes_is_exempt():
    s = settings()
    state = fresh_state()
    result = classify(s, state, obs(T0, status="downloading", sizeleft=0))
    assert result.state.classification == "exempt"


# --- Hard exemption: trackedDownloadState/trackedDownloadStatus is the
# import-failure reason policy's exclusive territory, regardless of the
# plain Sonarr queue ``status`` -----------------------------------------

@pytest.mark.parametrize("tracked_state,tracked_status", [
    ("importblocked", None), ("ImportBlocked", None),  # case-insensitive
    (None, "warning"), (None, "Warning"),
    (None, "error"), (None, "Error"),
])
def test_hard_exempt_tracked_state_or_status_overrides_active_downloading_status(tracked_state, tracked_status):
    assert is_exempt_tracked(tracked_state, tracked_status) is True
    s = settings()
    state = fresh_state()
    # "downloading" with bytes remaining would otherwise be fully active -
    # the hard exemption must still win.
    result = classify(s, state, obs(T0, status="downloading", sizeleft=5 * GiB, tracked_state=tracked_state, tracked_status=tracked_status))
    assert result.state.classification == "exempt"
    assert "hard-exempt" in result.state.reason


def test_hard_exempt_tracked_state_never_accumulates_evidence_across_many_observations():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=2)
    state = fresh_state()
    observations = [
        obs(T0 + timedelta(minutes=i * 10), status="downloading", sizeleft=5 * GiB, tracked_state="importblocked")
        for i in range(10)
    ]
    results = feed(s, state, observations)
    assert all(r.state.classification == "exempt" for r in results)
    assert all(r.state.strike_count == 0 for r in results)
    assert all(not r.strike_recorded for r in results)


def test_not_exempt_when_tracked_state_and_status_are_both_absent_or_benign():
    assert is_exempt_tracked(None, None) is False
    assert is_exempt_tracked("downloading", "ok") is False


def test_tracked_hard_exempt_freezes_existing_evidence_like_other_exemptions():
    s = settings(
        initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3,
        strikes_required=5,
    )
    state = fresh_state()
    sizeleft = 5 * GiB
    # Accumulate one stall strike while actively downloading with no
    # tracked exemption (window opens at minute 10, completes at minute 40
    # with no meaningful decrease).
    observations = [obs(T0 + timedelta(minutes=10 * i), sizeleft=sizeleft) for i in range(1, 5)]
    results = feed(s, state, observations)
    state = results[-1].state
    assert state.stall_strike_count == 1
    # ...then Sonarr reports it import-blocked: existing strikes are
    # preserved (not wiped), but the window is frozen, exactly like any
    # other exempt transition.
    r_exempt = classify(s, state, obs(T0 + timedelta(minutes=50), sizeleft=sizeleft, tracked_state="importblocked"))
    assert r_exempt.state.classification == "exempt"
    assert r_exempt.state.stall_strike_count == 1
    assert r_exempt.state.no_progress_window_started_at is None


def test_queue_paused_then_resumes_does_not_count_pause_duration():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=2)
    state = fresh_state()
    # First active observation past grace establishes a baseline.
    r1 = classify(s, state, obs(T0, sizeleft=5 * GiB))
    # Paused for a long time - must not be read back as stall evidence.
    r2 = classify(s, r1.state, obs(T0 + timedelta(hours=5), status="paused", sizeleft=5 * GiB))
    assert r2.state.classification == "exempt"
    # Resumes; window restarts fresh from here rather than "no progress for 5 hours".
    r3 = classify(s, r2.state, obs(T0 + timedelta(hours=5, minutes=1), sizeleft=5 * GiB))
    assert r3.state.classification in ("healthy", "slow_watch")
    assert r3.state.strike_count == 0


# --- Initial grace -------------------------------------------------------

def test_initial_grace_suppresses_strikes_even_with_no_progress():
    s = settings(initial_grace_minutes=30, no_progress_window_minutes=10, no_progress_min_observations=2)
    state = fresh_state(first_seen_at=T0)
    observations = [obs(T0 + timedelta(minutes=m), sizeleft=5 * GiB) for m in (0, 15, 29)]
    results = feed(s, state, observations)
    assert all(r.state.classification == "grace" for r in results)
    assert all(r.state.strike_count == 0 for r in results)


# --- Normal slow-but-progressing download must be protected ---------------

def test_normal_slow_but_progressing_download_never_strikes():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3)
    state = fresh_state()
    sizeleft = 10 * GiB
    observations = []
    t = T0
    for _ in range(12):
        sizeleft -= 50 * MiB  # steady meaningful progress each poll
        t += timedelta(minutes=10)
        observations.append(obs(t, sizeleft=sizeleft))
    results = feed(s, state, observations)
    assert all(r.state.strike_count == 0 for r in results)
    assert all(r.state.classification == "healthy" for r in results)


# --- Progress resets evidence/strikes -------------------------------------

def test_meaningful_progress_resets_strikes_and_window():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3, strikes_required=5)
    state = fresh_state()
    sizeleft = 10 * GiB
    t = T0
    # Build up a stall strike first: window opens at minute 10 (first obs),
    # completes once 30 minutes have elapsed from that start (minute 40).
    observations = [obs(t + timedelta(minutes=10 * i), sizeleft=sizeleft) for i in range(1, 5)]
    results = feed(s, state, observations)
    state = results[-1].state
    assert state.stall_strike_count == 1
    # Now real progress happens.
    t_next = observations[-1].observed_at + timedelta(minutes=10)
    result = classify(s, state, obs(t_next, sizeleft=sizeleft - 5 * MiB))
    assert result.state.stall_strike_count == 0
    assert result.state.classification == "healthy"
    assert result.reset_recorded is True


# --- True stall ------------------------------------------------------------

def test_true_stall_accumulates_strikes_and_eventually_removal_pending():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3, strikes_required=2)
    state = fresh_state()
    sizeleft = 10 * GiB
    t = T0
    # Window 1 opens at minute 10 (first obs), completes at minute 40
    # (30 min elapsed, 4 observations). Window 2 opens at minute 40,
    # completes at minute 70.
    observations = [obs(t + timedelta(minutes=10 * i), sizeleft=sizeleft) for i in range(1, 8)]
    results = feed(s, state, observations)
    classifications = [r.state.classification for r in results]
    strike_counts = [r.state.stall_strike_count for r in results]
    assert strike_counts[-1] == 2
    assert classifications[-1] == "removal_pending"
    assert results[-1].removal_eligible is True
    # No strike before the first full window completes.
    assert strike_counts[0] == 0
    assert strike_counts[1] == 0


def test_single_strike_is_not_enough_by_default():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3)
    state = fresh_state()
    sizeleft = 10 * GiB
    observations = [obs(T0 + timedelta(minutes=10 * i), sizeleft=sizeleft) for i in range(1, 5)]
    results = feed(s, state, observations)
    assert results[-1].state.stall_strike_count == 1
    assert results[-1].state.classification == "stalled_evidence"
    assert results[-1].removal_eligible is False


def test_strike_does_not_repeat_every_poll_within_same_window():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3)
    state = fresh_state()
    sizeleft = 10 * GiB
    # Window opens at minute 10 (first obs) and completes at minute 40
    # (30 min elapsed, 4 observations) -> strike 1. Keep polling without
    # letting a whole new window elapse - strike count must stay at 1.
    observations = [
        obs(T0 + timedelta(minutes=10), sizeleft=sizeleft),
        obs(T0 + timedelta(minutes=20), sizeleft=sizeleft),
        obs(T0 + timedelta(minutes=30), sizeleft=sizeleft),
        obs(T0 + timedelta(minutes=40), sizeleft=sizeleft),  # strike 1
        obs(T0 + timedelta(minutes=45), sizeleft=sizeleft),  # new window, not yet 30 min
    ]
    results = feed(s, state, observations)
    assert results[3].state.stall_strike_count == 1
    assert results[4].state.stall_strike_count == 1


# --- Sustained very-slow throughput ----------------------------------------

def test_sustained_very_slow_throughput_strikes():
    s = settings(
        initial_grace_minutes=0, very_slow_window_minutes=90, very_slow_min_observations=4,
        very_slow_rate_bytes_per_second=50 * KiB, very_slow_min_remaining_bytes=512 * MiB,
        no_progress_window_minutes=10_000,  # effectively disable the stall window for this test
        no_progress_min_observations=1000,
    )
    state = fresh_state()
    sizeleft = 2 * GiB
    # 30 KiB/s average over 90 minutes. Each step's decrease is well above
    # the 1 MiB stall epsilon (as expected for a download that is slow, not
    # frozen) - the very-slow track must still accumulate independently of
    # the stall epsilon check.
    t = T0
    observations = []
    for i in range(1, 8):
        t = T0 + timedelta(minutes=15 * i)
        sizeleft -= int(30 * KiB * 15 * 60)  # 30 KiB/s for 15 minutes
        observations.append(obs(t, sizeleft=sizeleft))
    results = feed(s, state, observations)
    assert results[-1].state.very_slow_strike_count >= 1
    assert results[-1].state.classification in ("very_slow_evidence", "removal_pending")


def test_very_slow_not_flagged_when_remaining_below_minimum():
    s = settings(
        initial_grace_minutes=0, very_slow_window_minutes=90, very_slow_min_observations=4,
        very_slow_rate_bytes_per_second=50 * KiB, very_slow_min_remaining_bytes=512 * MiB,
        no_progress_window_minutes=10_000, no_progress_min_observations=1000,
    )
    state = fresh_state()
    sizeleft = 100 * MiB  # below the 512 MiB floor - must never be flagged very-slow
    t = T0
    observations = []
    for i in range(1, 8):
        t = T0 + timedelta(minutes=15 * i)
        sizeleft = max(0, sizeleft - int(1 * KiB * 15 * 60))
        observations.append(obs(t, sizeleft=sizeleft))
    results = feed(s, state, observations)
    assert all(r.state.classification != "very_slow_evidence" for r in results)
    assert all(r.state.very_slow_strike_count == 0 for r in results)


def test_short_low_speed_period_does_not_strike():
    """A brief slow patch shorter than the full window must not strike."""
    s = settings(
        initial_grace_minutes=0, very_slow_window_minutes=90, very_slow_min_observations=4,
        very_slow_rate_bytes_per_second=50 * KiB, very_slow_min_remaining_bytes=512 * MiB,
        no_progress_window_minutes=10_000, no_progress_min_observations=1000,
    )
    state = fresh_state()
    sizeleft = 2 * GiB
    observations = [
        obs(T0 + timedelta(minutes=20), sizeleft=sizeleft - int(10 * KiB * 20 * 60)),
    ]
    sizeleft = observations[0].sizeleft_bytes
    # Then it speeds back up before the 90-minute window/4-observation floor is reached.
    observations.append(obs(T0 + timedelta(minutes=30), sizeleft=sizeleft - 5 * MiB))
    results = feed(s, state, observations)
    assert all(r.state.very_slow_strike_count == 0 for r in results)
    assert all(r.state.stall_strike_count == 0 for r in results)


# --- Tiny jitter must not look like progress -------------------------------

def test_tiny_jitter_does_not_reset_or_strike_prematurely():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3, progress_epsilon_bytes=1 * MiB)
    state = fresh_state()
    sizeleft = 10 * GiB
    observations = [
        obs(T0 + timedelta(minutes=10), sizeleft=sizeleft - 100 * KiB),  # jitter < epsilon
        obs(T0 + timedelta(minutes=20), sizeleft=sizeleft - 150 * KiB),
        obs(T0 + timedelta(minutes=30), sizeleft=sizeleft - 170 * KiB),
        obs(T0 + timedelta(minutes=40), sizeleft=sizeleft - 180 * KiB),
    ]
    results = feed(s, state, observations)
    # Cumulative decrease (180 KiB) is still below the 1 MiB epsilon, so this
    # is genuine stall evidence, not disguised progress.
    assert results[-1].state.stall_strike_count == 1


def test_cumulative_small_progress_above_epsilon_clears_window_without_strike():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3, progress_epsilon_bytes=1 * MiB)
    state = fresh_state()
    sizeleft = 10 * GiB
    observations = [
        obs(T0 + timedelta(minutes=10), sizeleft=sizeleft - 400 * KiB),
        obs(T0 + timedelta(minutes=20), sizeleft=sizeleft - 800 * KiB),
        obs(T0 + timedelta(minutes=30), sizeleft=sizeleft - 1200 * KiB),
        # Window started at the first observation (sizeleft - 400 KiB); the
        # cumulative decrease since then (1200 KiB) exceeds the 1 MiB epsilon.
        obs(T0 + timedelta(minutes=40), sizeleft=sizeleft - 1600 * KiB),
    ]
    results = feed(s, state, observations)
    assert results[-1].state.stall_strike_count == 0


# --- Counter reset / replaced item ----------------------------------------

def test_sizeleft_increase_starts_new_baseline_without_strike():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3)
    state = fresh_state()
    sizeleft = 5 * GiB
    observations = [
        obs(T0 + timedelta(minutes=10), sizeleft=sizeleft),
        obs(T0 + timedelta(minutes=20), sizeleft=sizeleft),
        # Sonarr's queue record now reports more remaining than before -
        # the underlying download was replaced/restarted.
        obs(T0 + timedelta(minutes=25), sizeleft=sizeleft + 2 * GiB),
    ]
    results = feed(s, state, observations)
    assert results[-1].state.strike_count == 0
    assert "new baseline" in results[-1].state.reason


# --- Long poll gap ----------------------------------------------------------

def test_long_poll_gap_does_not_count_as_stall_evidence():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3)
    state = fresh_state()
    sizeleft = 5 * GiB
    r1 = classify(s, state, obs(T0, sizeleft=sizeleft))
    # Worker was down for hours; gap >= window means we cannot trust it as
    # continuous no-progress evidence.
    r2 = classify(s, r1.state, obs(T0 + timedelta(hours=6), sizeleft=sizeleft))
    assert r2.state.strike_count == 0
    assert "gap" in r2.state.reason


# --- Restart durability (state reconstructed from persisted fields) --------

def test_classification_is_a_pure_function_of_persisted_state():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3)
    state = fresh_state()
    sizeleft = 5 * GiB
    observations = [obs(T0 + timedelta(minutes=10 * i), sizeleft=sizeleft) for i in range(1, 3)]
    results = feed(s, state, observations)
    mid_state = results[-1].state
    # Simulate a process restart: rebuild the same state from scratch and
    # feed the remaining observations - outcome must be identical to an
    # uninterrupted run.
    continued = classify(s, mid_state, obs(T0 + timedelta(minutes=30), sizeleft=sizeleft))
    rebuilt_state = QueueItemState(**{**mid_state.__dict__})
    continued_rebuilt = classify(s, rebuilt_state, obs(T0 + timedelta(minutes=30), sizeleft=sizeleft))
    assert continued.state == continued_rebuilt.state


# --- Two-strike windowing with default settings ----------------------------

def test_two_strikes_required_by_default():
    s = settings(initial_grace_minutes=0, no_progress_window_minutes=30, no_progress_min_observations=3)
    assert s.strikes_required == 2
    state = fresh_state()
    sizeleft = 5 * GiB
    observations = [obs(T0 + timedelta(minutes=10 * i), sizeleft=sizeleft) for i in range(1, 8)]
    results = feed(s, state, observations)
    pending = [r for r in results if r.removal_eligible]
    assert len(pending) >= 1
    first_pending_index = next(i for i, r in enumerate(results) if r.removal_eligible)
    # Must take at least two full windows to reach removal_eligible, never
    # a single bad sample.
    assert first_pending_index >= 5


# --- Grace must not let evidence windows bank during grace -----------------

def test_grace_expiry_starts_a_fresh_window_not_banked_evidence():
    """A window that would have completed the instant grace ends (because it
    silently started accumulating from the very first, still-in-grace
    observation) must not produce a strike at grace expiry. The first
    post-grace observation must start a brand-new window baseline."""
    s = settings(initial_grace_minutes=30, no_progress_window_minutes=30, no_progress_min_observations=2, strikes_required=1)
    state = fresh_state()
    sizeleft = 5 * GiB
    observations = [
        obs(T0, sizeleft=sizeleft),
        obs(T0 + timedelta(minutes=15), sizeleft=sizeleft),
        obs(T0 + timedelta(minutes=30), sizeleft=sizeleft),  # grace just expired; fresh baseline starts here
        obs(T0 + timedelta(minutes=45), sizeleft=sizeleft),
        obs(T0 + timedelta(minutes=60), sizeleft=sizeleft),  # one full post-grace window later
    ]
    results = feed(s, state, observations)
    assert all(r.state.classification != "removal_pending" for r in results[:-1])
    assert results[-1].state.classification == "removal_pending"
    assert results[-1].state.stall_strike_count == 1


def test_default_strikes_required_needs_two_full_windows_after_grace():
    s = settings(initial_grace_minutes=30, no_progress_window_minutes=30, no_progress_min_observations=2)
    assert s.strikes_required == 2
    state = fresh_state()
    sizeleft = 5 * GiB
    observations = [obs(T0 + timedelta(minutes=m), sizeleft=sizeleft) for m in (0, 15, 30, 45, 60, 75, 90)]
    results = feed(s, state, observations)
    first_pending_index = next((i for i, r in enumerate(results) if r.removal_eligible), None)
    assert first_pending_index is not None
    # Earliest possible removal is grace (30m) + two full 30m windows = 90m.
    assert observations[first_pending_index].observed_at == T0 + timedelta(minutes=90)


# --- Gap reset must use the shorter of the two active windows --------------

def test_gap_reset_uses_the_shorter_of_the_two_active_windows():
    """A gap long enough to make the (short) very-slow window untrustworthy,
    but not the (long) no-progress window, must still reset both tracks -
    otherwise the very-slow window could silently "complete" across a gap
    during which the worker was simply not polling."""
    s = settings(
        initial_grace_minutes=0, no_progress_window_minutes=120, no_progress_min_observations=2,
        very_slow_window_minutes=20, very_slow_min_observations=2,
        very_slow_rate_bytes_per_second=50 * KiB, very_slow_min_remaining_bytes=512 * MiB,
    )
    state = fresh_state()
    sizeleft = 5 * GiB
    r1 = classify(s, state, obs(T0, sizeleft=sizeleft))
    # Gap of 25 minutes exceeds the 20-minute very-slow window but not the
    # 120-minute no-progress window.
    r2 = classify(s, r1.state, obs(T0 + timedelta(minutes=25), sizeleft=sizeleft))
    assert r2.state.strike_count == 0
    assert "gap" in r2.state.reason
    assert r2.state.no_progress_window_started_at == T0 + timedelta(minutes=25)
    assert r2.state.very_slow_window_started_at == T0 + timedelta(minutes=25)

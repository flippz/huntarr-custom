"""Pure, deterministic reason classification for the Sonarr-only import-
failure reason policy.

No I/O. This module only ever *observes and classifies* queue-level import
rejection evidence and decides whether a decision is ``remove_eligible`` or
``leave`` under an operator-selected policy; it never decides to call
Sonarr itself (see ``app/services/import_failure_service.py`` for the
worker/service layer that turns a ``remove_eligible`` decision into an
actual gated DELETE - reusing the exact same ``delete_queue_record`` write
the slow-download guard already uses, never a new Sonarr write).

Scope is deliberately narrow: **reason-aware observation and reason-gated
removal only**. There is no force-import and no new Sonarr write path here.

Canonical catalog (see MANAGEARR.md): the 34 Sonarr v4 ``ImportRejectionReason``
values, exactly as published, plus one synthetic key for a documented
queue-only message that is not itself part of that enum. ``Unknown`` is
part of the canonical catalog but is deliberately never selectable for
automatic removal - see ``REMOVAL_SELECTABLE_REASON_KEYS`` - and any raw
message that cannot be matched to *any* known key is treated the same way:
always left in the queue, never guessed into a bucket. Every reason
defaults to "leave" (not removal-eligible) until an operator explicitly
selects it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# --- Canonical catalog -----------------------------------------------------

# Exact Sonarr v4 ImportRejectionReason catalog, in the order given in the
# product requirement. "Unknown" is included for recognition purposes (a
# raw message can be classified into it) but is excluded from
# REMOVAL_SELECTABLE_REASON_KEYS below - it is never a removal decision.
IMPORT_REJECTION_REASONS: tuple[str, ...] = (
    "Unknown", "FileLocked", "UnknownSeries", "DangerousFile", "ExecutableFile",
    "ArchiveFile", "SeriesFolder", "InvalidFilePath", "UnsupportedExtension",
    "PartialSeason", "SeasonExtra", "InvalidSeasonOrEpisode", "UnableToParse",
    "Error", "DecisionError", "NoEpisodes", "MissingAbsoluteEpisodeNumber",
    "EpisodeAlreadyImported", "TitleMissing", "TitleTba", "MinimumFreeSpace",
    "FullSeason", "NoAudio", "EpisodeUnexpected", "EpisodeNotFoundInRelease",
    "Sample", "SampleIndeterminate", "Unpacking", "ExistingFileHasMoreEpisodes",
    "SplitEpisode", "UnverifiedSceneMapping", "NotQualityUpgrade",
    "NotRevisionUpgrade", "NotCustomFormatUpgrade",
)

# A documented, exact queue-only message that is not part of the
# ImportRejectionReason enum itself - Sonarr emits this literal sentence
# when a release was grabbed via history matching but bound to a series by
# numeric id rather than a confirmed title/path match, and automatic import
# cannot proceed. Matched by an exact (trimmed) string comparison only -
# never a fuzzy/substring match - since it is a single fixed sentence, not
# a family of related messages.
SERIES_MATCHED_BY_ID_MESSAGE = (
    "Found matching series via grab history, but release was matched to "
    "series by ID. Automatic import is not possible."
)
SERIES_MATCHED_BY_ID_REASON_KEY = "SeriesMatchedByIdOnly"

# Every key this module can ever assign to an observed message - the
# canonical catalog plus the one synthetic queue-only key above.
ALL_REASON_KEYS: tuple[str, ...] = IMPORT_REJECTION_REASONS + (SERIES_MATCHED_BY_ID_REASON_KEY,)

_UNKNOWN_REASON_KEY = "Unknown"

# Reasons an operator may ever select for automatic removal. "Unknown" is
# deliberately excluded - it is Sonarr/this module's own fallback bucket for
# "a rejection happened but we can't say more", which is exactly the case
# that must always be left for manual review, never auto-removed.
REMOVAL_SELECTABLE_REASON_KEYS: frozenset[str] = frozenset(ALL_REASON_KEYS) - {_UNKNOWN_REASON_KEY}

# Grouped for UI presentation only - purely cosmetic, has no effect on
# normalization or decisions. Every REMOVAL_SELECTABLE_REASON_KEYS entry
# appears in exactly one group; this is asserted below at import time so
# the grouping can never silently drift out of sync with the catalog.
REASON_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Parsing & identification", (
        "UnknownSeries", "UnableToParse", "InvalidSeasonOrEpisode",
        "MissingAbsoluteEpisodeNumber", "TitleMissing", "TitleTba", "NoEpisodes",
        "EpisodeNotFoundInRelease", "EpisodeUnexpected", SERIES_MATCHED_BY_ID_REASON_KEY,
    )),
    ("File safety", (
        "FileLocked", "DangerousFile", "ExecutableFile", "ArchiveFile",
        "InvalidFilePath", "UnsupportedExtension", "SeriesFolder",
    )),
    ("Season / episode shape", (
        "PartialSeason", "SeasonExtra", "FullSeason", "SplitEpisode",
        "ExistingFileHasMoreEpisodes", "EpisodeAlreadyImported", "UnverifiedSceneMapping",
    )),
    ("Media content", (
        "NoAudio", "Sample", "SampleIndeterminate", "Unpacking",
    )),
    ("Capacity & upgrade gates", (
        "MinimumFreeSpace", "NotQualityUpgrade", "NotRevisionUpgrade", "NotCustomFormatUpgrade",
    )),
    ("Errors", (
        "Error", "DecisionError",
    )),
)
_grouped = frozenset(key for _, keys in REASON_GROUPS for key in keys)
assert _grouped == REMOVAL_SELECTABLE_REASON_KEYS, (
    "REASON_GROUPS must exactly partition REMOVAL_SELECTABLE_REASON_KEYS with no gaps or overlaps"
)
assert sum(len(keys) for _, keys in REASON_GROUPS) == len(REMOVAL_SELECTABLE_REASON_KEYS), (
    "REASON_GROUPS must not list any reason key more than once"
)


def _humanize(token: str) -> str:
    """"NotQualityUpgrade" -> "not quality upgrade". Used as a best-effort
    word-bounded pattern for matching Sonarr's human-readable queue status
    messages; the literal PascalCase token is also matched directly so a
    message or title that already carries the raw reason name (as some
    callers/tests do) is recognized without relying on phrasing at all."""
    return re.sub(r"(?<!^)(?=[A-Z])", " ", token).lower()


# Complete-message allowlist. These are deliberately values, not search
# fragments: no recognized prefix or embedded token can authorize removal.
_EXACT_MESSAGE_KEYS: dict[str, str] = {
    **{key.casefold(): key for key in IMPORT_REJECTION_REASONS},
    **{_humanize(key).casefold(): key for key in IMPORT_REJECTION_REASONS},
    "this appears to be a sample file": "Sample",
    "sample indeterminate - unable to verify": "SampleIndeterminate",
    "unable to parse the episode information": "UnableToParse",
    "minimum free space not available on disk": "MinimumFreeSpace",
    "not quality upgrade for this release": "NotQualityUpgrade",
    "file locked - unable to import": "FileLocked",
    SERIES_MATCHED_BY_ID_MESSAGE.casefold(): SERIES_MATCHED_BY_ID_REASON_KEY,
}

# Only explicitly supported separators may combine complete messages. Commas
# are excluded because the synthetic queue-only message itself contains one.
_SUPPORTED_MESSAGE_DELIMITER = re.compile(r"\s*(?:;|\||\r?\n)\s*")


@dataclass(frozen=True)
class NormalizationResult:
    matched_reasons: frozenset[str]
    unmatched_messages: tuple[str, ...]

    @property
    def has_any_evidence(self) -> bool:
        return bool(self.matched_reasons) or bool(self.unmatched_messages)


def _match_keys(trimmed: str) -> frozenset[str]:
    """Every reason key safely recognizable in one trimmed message.

    The complete message must be allowlisted, or be an explicitly
    delimiter-separated sequence whose every complete segment is allowlisted.
    Any residual text invalidates the whole message. Multiple valid segments
    return every distinct key so policy evaluation still requires all of them
    to be selected."""
    segments = _SUPPORTED_MESSAGE_DELIMITER.split(trimmed)
    if not segments or any(not segment.strip() for segment in segments):
        return frozenset()
    keys: set[str] = set()
    for segment in segments:
        key = _EXACT_MESSAGE_KEYS.get(segment.strip().casefold())
        if key is None:
            # Residual/unrecognized text invalidates the complete message.
            return frozenset()
        keys.add(key)
    return frozenset(keys)


def normalize_messages(messages: list[str]) -> NormalizationResult:
    """Classify a bounded list of raw Sonarr queue status message strings.

    Pure/no I/O. Each message is matched independently against the
    allowlisted catalog patterns (see ``_match_keys``); the result is the
    union of every matched canonical/synthetic reason key across every
    message, plus every message that could not be safely matched at all
    (always "leave" - see ``evaluate`` below, which never guesses an
    unmatched message into any removal-eligible bucket, so removal
    requires every message to map safely, not just some of them)."""
    matched: set[str] = set()
    unmatched: list[str] = []
    for raw in messages:
        if not isinstance(raw, str) or not raw.strip():
            continue
        keys = _match_keys(raw.strip())
        if keys:
            matched.update(keys)
        else:
            unmatched.append(raw)
    return NormalizationResult(matched_reasons=frozenset(matched), unmatched_messages=tuple(unmatched))


# --- Watched queue states ---------------------------------------------------

# Sonarr queue records worth evaluating for an import-failure reason: a
# completed download still sitting in the queue, one Sonarr has explicitly
# marked import-blocked, or one carrying a warning/error tracked download
# status. Every other status (queued/downloading/delayed/paused/importing/
# importpending with no warning) is exempt - this module never evaluates a
# download that is simply still in progress, which is the slow-download
# guard's exclusive territory (see MANAGEARR.md; this module adds no logic
# there and the slow-download evidence path is untouched).
WATCHED_STATUSES = frozenset({"completed"})
WATCHED_TRACKED_STATES = frozenset({"importblocked"})
WATCHED_TRACKED_STATUSES = frozenset({"warning", "error"})


def should_observe(*, status: str | None, tracked_state: str | None, tracked_status: str | None) -> bool:
    status = (status or "").lower()
    tracked_state = (tracked_state or "").lower()
    tracked_status = (tracked_status or "").lower()
    return (
        status in WATCHED_STATUSES
        or tracked_state in WATCHED_TRACKED_STATES
        or tracked_status in WATCHED_TRACKED_STATUSES
    )


# --- Policy and decision -----------------------------------------------------

@dataclass(frozen=True)
class ImportFailurePolicy:
    auto_removal_enabled: bool = False
    removal_reasons: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class Decision:
    action: str  # "remove_eligible" or "leave"
    reason: str


def evaluate(policy: ImportFailurePolicy, normalization: NormalizationResult) -> Decision:
    """Decide "remove_eligible" vs "leave" for one observed queue item.

    Conservative by construction: removal requires *every* observed reason
    on the item to be an operator-selected removal reason. A single
    unmatched message or a single non-selected (or ``Unknown``) reason
    forces "leave", even if other reasons on the same item are selected."""
    if not normalization.has_any_evidence:
        return Decision("leave", "no import-failure reason observed")
    if not policy.auto_removal_enabled:
        return Decision("leave", "automatic removal is disabled for this library")
    if normalization.unmatched_messages:
        return Decision(
            "leave",
            "observed an unrecognized message that cannot be matched to any known reason; "
            "unmatched messages are never auto-removed",
        )
    if _UNKNOWN_REASON_KEY in normalization.matched_reasons:
        return Decision("leave", "'Unknown' is never eligible for automatic removal")
    not_selected = normalization.matched_reasons - policy.removal_reasons
    if not_selected:
        return Decision(
            "leave",
            "reason(s) not selected for automatic removal: " + ", ".join(sorted(not_selected)),
        )
    return Decision(
        "remove_eligible",
        "every observed reason is selected for automatic removal: " + ", ".join(sorted(normalization.matched_reasons)),
    )

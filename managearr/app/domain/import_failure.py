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


# "Error" (humanized: the single word "error") is excluded from the
# word-boundary pattern set below. Every *other* reason's token/phrase is
# distinctive enough that a whole-word match anywhere in a message is safe
# signal, but "error" alone is an ordinary, highly generic English word -
# matching it anywhere would misclassify unrelated text (a download
# client's own "filesystem error" message is not Sonarr's "Error" import-
# rejection reason). "Unknown" has the same problem and is handled the same
# way. Both are only ever matched against the *complete* (trimmed,
# case-insensitive) message - see ``_EXACT_MATCH_KEYS`` below - never as a
# substring of a longer sentence.
_EXACT_ONLY_REASON_KEYS: frozenset[str] = frozenset({"Error"})
_EXACT_MATCH_KEYS: dict[str, str] = {"error": "Error", _UNKNOWN_REASON_KEY.lower(): _UNKNOWN_REASON_KEY}


def _boundary_pattern(phrase: str) -> re.Pattern[str]:
    # (?<!\w)...(?!\w) rather than \b on both ends: \b alone still fires
    # between two different "word" boundaries fine for our ASCII catalog,
    # but spelling it as a non-word lookaround either side is unambiguous
    # about intent - the phrase must stand on its own, not be glued to
    # other letters/digits on either side (so "Sample" never matches
    # inside "sampler", and "SampleIndeterminate" never lets its own
    # "Sample" prefix also register separately - see the overlap
    # suppression in ``_match_keys``).
    return re.compile(r"(?<!\w)" + re.escape(phrase) + r"(?!\w)", re.IGNORECASE)


# Every (key, compiled pattern) considered for word-boundary matching - both
# the literal PascalCase token and the humanized phrasing, for every
# canonical key except the exact-only generic words above. A key
# contributes one pattern per distinct phrase (token vs. humanized); either
# one matching counts as that key matching.
_BOUNDARY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (key, _boundary_pattern(phrase))
    for key in IMPORT_REJECTION_REASONS
    if key != _UNKNOWN_REASON_KEY and key not in _EXACT_ONLY_REASON_KEYS
    for phrase in {key, _humanize(key)}
)


@dataclass(frozen=True)
class NormalizationResult:
    matched_reasons: frozenset[str]
    unmatched_messages: tuple[str, ...]

    @property
    def has_any_evidence(self) -> bool:
        return bool(self.matched_reasons) or bool(self.unmatched_messages)


def _match_keys(trimmed: str) -> frozenset[str]:
    """Every reason key safely recognizable in one trimmed message.

    Exact allowlisted phrase/pattern boundaries only: a word-bounded
    token/phrase match for every ordinary reason, or a complete-message
    match for the two generic single-word keys. When two matches overlap
    (e.g. "Sample" inside "SampleIndeterminate"), only the longest/most
    specific span wins - the shorter one is suppressed, never both.
    Matches at genuinely different, non-overlapping spans are all kept,
    so a message naming more than one reason returns every one of them.
    Zero matches - including any residual text no pattern explains - is
    reported as unmatched (see ``normalize_messages``), never guessed."""
    if trimmed == SERIES_MATCHED_BY_ID_MESSAGE:
        return frozenset({SERIES_MATCHED_BY_ID_REASON_KEY})
    spans: list[tuple[int, int, str]] = [
        (m.start(), m.end(), key) for key, pattern in _BOUNDARY_PATTERNS for m in pattern.finditer(trimmed)
    ]
    exact_key = _EXACT_MATCH_KEYS.get(trimmed.lower())
    if exact_key is not None:
        spans.append((0, len(trimmed), exact_key))
    if not spans:
        return frozenset()
    spans.sort(key=lambda s: (-(s[1] - s[0]), s[0]))
    selected_spans: list[tuple[int, int]] = []
    keys: set[str] = set()
    for start, end, key in spans:
        if any(start < s_end and end > s_start for s_start, s_end in selected_spans):
            continue
        selected_spans.append((start, end))
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

"""Pure, no-I/O tests for app.domain.import_failure: reason catalog
integrity, message normalization, and the reason-gated removal decision."""
import pytest

from app.domain.import_failure import (
    ALL_REASON_KEYS,
    IMPORT_REJECTION_REASONS,
    REASON_GROUPS,
    REMOVAL_SELECTABLE_REASON_KEYS,
    SERIES_MATCHED_BY_ID_MESSAGE,
    SERIES_MATCHED_BY_ID_FAQ_MESSAGE,
    SERIES_MATCHED_BY_ID_REASON_KEY,
    Decision,
    ImportFailurePolicy,
    evaluate,
    normalize_messages,
    should_observe,
)


# --- Catalog integrity -------------------------------------------------------

def test_canonical_catalog_matches_exact_published_list():
    assert IMPORT_REJECTION_REASONS == (
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
    assert len(IMPORT_REJECTION_REASONS) == 34
    assert len(set(IMPORT_REJECTION_REASONS)) == 34  # no duplicates


def test_unknown_is_never_removal_selectable():
    assert "Unknown" not in REMOVAL_SELECTABLE_REASON_KEYS


def test_series_matched_by_id_is_removal_selectable_but_not_in_canonical_catalog():
    assert SERIES_MATCHED_BY_ID_REASON_KEY not in IMPORT_REJECTION_REASONS
    assert SERIES_MATCHED_BY_ID_REASON_KEY in REMOVAL_SELECTABLE_REASON_KEYS
    assert SERIES_MATCHED_BY_ID_REASON_KEY in ALL_REASON_KEYS


def test_removal_selectable_keys_are_every_canonical_reason_except_unknown_plus_special():
    expected = (set(IMPORT_REJECTION_REASONS) - {"Unknown"}) | {SERIES_MATCHED_BY_ID_REASON_KEY}
    assert REMOVAL_SELECTABLE_REASON_KEYS == expected
    assert len(REMOVAL_SELECTABLE_REASON_KEYS) == 34


def test_reason_groups_exactly_partition_removal_selectable_keys_with_no_gaps_or_overlaps():
    seen = []
    for _, keys in REASON_GROUPS:
        seen.extend(keys)
    assert len(seen) == len(set(seen)), "a reason key appears in more than one group"
    assert set(seen) == REMOVAL_SELECTABLE_REASON_KEYS


def test_every_group_has_a_name_and_at_least_one_reason():
    for name, keys in REASON_GROUPS:
        assert isinstance(name, str) and name
        assert len(keys) >= 1


# --- Message normalization ---------------------------------------------------

@pytest.mark.parametrize("message,expected_key", [
    ("Sample", "Sample"),
    ("This appears to be a sample file", "Sample"),
    ("SampleIndeterminate", "SampleIndeterminate"),
    ("Sample indeterminate - unable to verify", "SampleIndeterminate"),
    ("Unpacking", "Unpacking"),
    ("UnableToParse", "UnableToParse"),
    ("Unable to parse the episode information", "UnableToParse"),
    ("DecisionError", "DecisionError"),
    ("MinimumFreeSpace", "MinimumFreeSpace"),
    ("Minimum free space not available on disk", "MinimumFreeSpace"),
    ("NotQualityUpgrade", "NotQualityUpgrade"),
    ("Not quality upgrade for this release", "NotQualityUpgrade"),
    ("NotRevisionUpgrade", "NotRevisionUpgrade"),
    ("NotCustomFormatUpgrade", "NotCustomFormatUpgrade"),
    ("FileLocked", "FileLocked"),
    ("File locked - unable to import", "FileLocked"),
    ("DangerousFile", "DangerousFile"),
    ("ExecutableFile", "ExecutableFile"),
    ("ArchiveFile", "ArchiveFile"),
    ("EpisodeAlreadyImported", "EpisodeAlreadyImported"),
    ("EpisodeNotFoundInRelease", "EpisodeNotFoundInRelease"),
    ("EpisodeUnexpected", "EpisodeUnexpected"),
    ("ExistingFileHasMoreEpisodes", "ExistingFileHasMoreEpisodes"),
    ("SplitEpisode", "SplitEpisode"),
    ("UnverifiedSceneMapping", "UnverifiedSceneMapping"),
    ("PartialSeason", "PartialSeason"),
    ("SeasonExtra", "SeasonExtra"),
    ("FullSeason", "FullSeason"),
    ("NoAudio", "NoAudio"),
    ("NoEpisodes", "NoEpisodes"),
    ("MissingAbsoluteEpisodeNumber", "MissingAbsoluteEpisodeNumber"),
    ("TitleMissing", "TitleMissing"),
    ("TitleTba", "TitleTba"),
    ("InvalidSeasonOrEpisode", "InvalidSeasonOrEpisode"),
    ("InvalidFilePath", "InvalidFilePath"),
    ("UnsupportedExtension", "UnsupportedExtension"),
    ("SeriesFolder", "SeriesFolder"),
    ("UnknownSeries", "UnknownSeries"),
    ("Error", "Error"),
])
def test_normalize_recognizes_literal_token_and_humanized_phrasing(message, expected_key):
    result = normalize_messages([message])
    assert result.matched_reasons == {expected_key}
    assert result.unmatched_messages == ()


def test_normalize_exact_series_matched_by_id_message():
    for message in (SERIES_MATCHED_BY_ID_MESSAGE, SERIES_MATCHED_BY_ID_FAQ_MESSAGE):
        result = normalize_messages([message])
        assert result.matched_reasons == {SERIES_MATCHED_BY_ID_REASON_KEY}
        assert result.unmatched_messages == ()


def test_normalize_series_matched_by_id_message_requires_exact_match_not_fuzzy():
    # A similar but not identical sentence must never be matched - this is
    # a single fixed queue-only message, not a family of related phrasings.
    result = normalize_messages(["Found matching series via grab history."])
    assert SERIES_MATCHED_BY_ID_REASON_KEY not in result.matched_reasons


def test_normalize_series_matched_by_id_message_tolerates_surrounding_whitespace():
    result = normalize_messages(["  " + SERIES_MATCHED_BY_ID_MESSAGE + "  "])
    assert result.matched_reasons == {SERIES_MATCHED_BY_ID_REASON_KEY}


def test_normalize_unmatched_message_is_never_guessed():
    result = normalize_messages(["some totally unrelated gibberish that matches nothing"])
    assert result.matched_reasons == frozenset()
    assert result.unmatched_messages == ("some totally unrelated gibberish that matches nothing",)


def test_normalize_literal_unknown_text_maps_to_unknown_reason():
    result = normalize_messages(["Unknown"])
    assert result.matched_reasons == {"Unknown"}


def test_normalize_ignores_blank_and_non_string_entries():
    result = normalize_messages(["", "   ", None, 123, "Sample"])
    assert result.matched_reasons == {"Sample"}
    assert result.unmatched_messages == ()


def test_normalize_multiple_messages_unions_matches_and_unmatched():
    result = normalize_messages(["Sample", "gibberish one", "Unpacking", "gibberish two"])
    assert result.matched_reasons == {"Sample", "Unpacking"}
    assert set(result.unmatched_messages) == {"gibberish one", "gibberish two"}


def test_normalize_empty_list_has_no_evidence():
    result = normalize_messages([])
    assert not result.has_any_evidence


def test_normalize_prefers_more_specific_longer_token_over_shorter_substring():
    # "DecisionError" contains "Error" as a substring; the longer, more
    # specific token must win rather than collapsing to the generic one.
    result = normalize_messages(["DecisionError"])
    assert result.matched_reasons == {"DecisionError"}
    assert "Error" not in result.matched_reasons


def test_normalize_sample_indeterminate_not_collapsed_to_sample():
    result = normalize_messages(["SampleIndeterminate"])
    assert result.matched_reasons == {"SampleIndeterminate"}


def test_normalize_sample_indeterminate_sentence_not_collapsed_to_sample():
    # Same overlap-suppression rule as the literal-token case above, but
    # for the humanized sentence form: "Sample" is a word-bounded substring
    # of "Sample indeterminate", yet only the longer, more specific match
    # must survive.
    result = normalize_messages(["Sample indeterminate - unable to verify"])
    assert result.matched_reasons == {"SampleIndeterminate"}
    assert "Sample" not in result.matched_reasons


def test_normalize_generic_word_error_requires_exact_whole_message_match():
    # "error" is an ordinary, highly generic English word - matching it as
    # a mere word-bounded substring would misclassify unrelated text (e.g.
    # a download client's own "filesystem error" message, which is not
    # Sonarr's "Error" import-rejection reason at all). Only the complete
    # trimmed message being exactly "error" is safe signal.
    result = normalize_messages(["filesystem error"])
    assert "Error" not in result.matched_reasons
    assert result.matched_reasons == frozenset()
    assert result.unmatched_messages == ("filesystem error",)


def test_normalize_sampler_is_not_collapsed_to_sample():
    # "sampler" is a different word entirely - "Sample" must never match
    # as a substring glued to other letters.
    result = normalize_messages(["sampler"])
    assert "Sample" not in result.matched_reasons
    assert result.matched_reasons == frozenset()
    assert result.unmatched_messages == ("sampler",)


def test_normalize_single_message_returns_every_distinct_matched_category():
    # Two genuinely separate, non-overlapping reason mentions in one
    # message must both be reported - "return all matched categories",
    # not just the first/longest one found.
    result = normalize_messages(["Sample; Unpacking"])
    assert result.matched_reasons == {"Sample", "Unpacking"}
    assert result.unmatched_messages == ()


def test_normalize_rejects_recognized_prefix_with_residual_unknown_text():
    result = normalize_messages(["Sample xyz-unrecognized-tail"])
    assert result.matched_reasons == frozenset()
    assert result.unmatched_messages == ("Sample xyz-unrecognized-tail",)


def test_evaluate_combined_single_message_reasons_require_every_category_selected():
    normalization = normalize_messages(["Sample; Unpacking"])
    only_one_selected = ImportFailurePolicy(auto_removal_enabled=True, removal_reasons=frozenset({"Sample"}))
    decision = evaluate(only_one_selected, normalization)
    assert decision.action == "leave"
    assert "Unpacking" in decision.reason
    both_selected = ImportFailurePolicy(auto_removal_enabled=True, removal_reasons=frozenset({"Sample", "Unpacking"}))
    decision = evaluate(both_selected, normalization)
    assert decision.action == "remove_eligible"


def test_all_reason_keys_catalog_is_exactly_35():
    # 34 current upstream Sonarr v4 ImportRejectionReason enum values
    # (including "Unknown") plus the one synthetic queue-only key.
    assert len(ALL_REASON_KEYS) == 35
    assert len(set(ALL_REASON_KEYS)) == 35


# --- should_observe ---------------------------------------------------------

@pytest.mark.parametrize("status,tracked_state,tracked_status,expected", [
    ("completed", None, None, True),
    ("downloading", "importblocked", None, True),
    ("downloading", None, "warning", True),
    ("downloading", None, "error", True),
    ("downloading", "downloading", "ok", False),
    ("queued", None, None, False),
    ("paused", None, None, False),
    (None, None, None, False),
])
def test_should_observe_watches_only_completed_importblocked_or_warning_error(
    status, tracked_state, tracked_status, expected
):
    assert should_observe(status=status, tracked_state=tracked_state, tracked_status=tracked_status) is expected


def test_should_observe_is_case_insensitive():
    assert should_observe(status="COMPLETED", tracked_state=None, tracked_status=None) is True
    assert should_observe(status=None, tracked_state="ImportBlocked", tracked_status=None) is True
    assert should_observe(status=None, tracked_state=None, tracked_status="WARNING") is True


# --- evaluate: the reason-gated removal decision -----------------------------

def test_evaluate_leaves_when_no_evidence():
    policy = ImportFailurePolicy(auto_removal_enabled=True, removal_reasons=frozenset({"Sample"}))
    decision = evaluate(policy, normalize_messages([]))
    assert decision.action == "leave"


def test_evaluate_leaves_when_auto_removal_disabled_even_if_reason_selected():
    policy = ImportFailurePolicy(auto_removal_enabled=False, removal_reasons=frozenset({"Sample"}))
    decision = evaluate(policy, normalize_messages(["Sample"]))
    assert decision.action == "leave"
    assert "disabled" in decision.reason


def test_evaluate_leaves_when_reason_not_selected():
    policy = ImportFailurePolicy(auto_removal_enabled=True, removal_reasons=frozenset({"Unpacking"}))
    decision = evaluate(policy, normalize_messages(["Sample"]))
    assert decision.action == "leave"
    assert "Sample" in decision.reason


def test_evaluate_removes_when_every_observed_reason_is_selected():
    policy = ImportFailurePolicy(auto_removal_enabled=True, removal_reasons=frozenset({"Sample", "Unpacking"}))
    decision = evaluate(policy, normalize_messages(["Sample"]))
    assert decision.action == "remove_eligible"


def test_evaluate_leaves_when_only_some_observed_reasons_are_selected():
    # Conservative-by-construction: every reason present must be selected.
    policy = ImportFailurePolicy(auto_removal_enabled=True, removal_reasons=frozenset({"Sample"}))
    decision = evaluate(policy, normalize_messages(["Sample", "Unpacking"]))
    assert decision.action == "leave"
    assert "Unpacking" in decision.reason


def test_evaluate_leaves_when_unknown_matched_even_if_literally_selected_would_be_impossible():
    policy = ImportFailurePolicy(auto_removal_enabled=True, removal_reasons=frozenset(REMOVAL_SELECTABLE_REASON_KEYS))
    decision = evaluate(policy, normalize_messages(["Unknown"]))
    assert decision.action == "leave"
    assert "Unknown" in decision.reason


def test_evaluate_leaves_when_any_message_is_unmatched_even_with_full_policy():
    policy = ImportFailurePolicy(auto_removal_enabled=True, removal_reasons=frozenset(REMOVAL_SELECTABLE_REASON_KEYS))
    decision = evaluate(policy, normalize_messages(["Sample", "total gibberish"]))
    assert decision.action == "leave"
    assert "unrecognized" in decision.reason


def test_evaluate_removes_for_the_series_matched_by_id_special_message():
    policy = ImportFailurePolicy(auto_removal_enabled=True, removal_reasons=frozenset({SERIES_MATCHED_BY_ID_REASON_KEY}))
    decision = evaluate(policy, normalize_messages([SERIES_MATCHED_BY_ID_MESSAGE]))
    assert decision.action == "remove_eligible"


def test_default_policy_has_every_reason_leave():
    policy = ImportFailurePolicy()
    assert policy.auto_removal_enabled is False
    assert policy.removal_reasons == frozenset()
    for key in REMOVAL_SELECTABLE_REASON_KEYS:
        decision = evaluate(policy, normalize_messages([key]))
        assert decision.action == "leave"


def test_decision_is_immutable_dataclass():
    d = Decision(action="leave", reason="x")
    with pytest.raises(Exception):
        d.action = "remove_eligible"

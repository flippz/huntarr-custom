"""Regression coverage for strict Sonarr missing-season pack dispatch."""

import os
import tempfile
import threading
import unittest
from unittest import mock

os.environ.setdefault("HUNTARR_CONFIG_DIR", tempfile.mkdtemp(prefix="huntarr_strict_pack_"))

from src.primary.apps._common import queue_dispatch
from src.primary.apps._common.pipeline_state import PipelineState
from src.primary.apps.sonarr import api as sonarr_api
from src.primary.apps.sonarr import missing as sonarr_missing
from tests.test_pipeline_state_phase2 import _DB


class _Response:
    def __init__(self, data=None, status=200, error=None):
        self._data = data
        self.status_code = status
        self.content = b"[]" if data is not None else b""
        self._error = error

    def raise_for_status(self):
        if self._error:
            raise self._error

    def json(self):
        return self._data


def _release(guid, weight, **changes):
    release = {
        "guid": guid,
        "indexerId": 5,
        "title": f"Show.S02.{guid}",
        "fullSeason": True,
        "mappedSeriesId": 7,
        "mappedSeasonNumber": 2,
        "mappedEpisodeInfo": [
            {"id": 101, "seasonNumber": 2, "episodeNumber": 1, "title": "Ep 1"},
            {"id": 102, "seasonNumber": 2, "episodeNumber": 2, "title": "Ep 2"},
        ],
        "quality": {
            "quality": {"id": 4, "name": "HDTV-720p", "source": "television", "resolution": 720},
            "revision": {"version": 1, "real": 0, "isRepack": False},
        },
        "languages": [{"id": 1, "name": "English"}],
        "approved": True,
        "downloadAllowed": True,
        "rejected": False,
        "temporarilyRejected": False,
        "rejections": [],
        "releaseWeight": weight,
    }
    release.update(changes)
    return release


def _rejected_release(guid, weight, rejections, **changes):
    """A release Sonarr rejected (approved=False) with the given rejections list."""
    return _release(
        guid, weight, approved=False, rejected=True,
        rejections=rejections, **changes,
    )


class CutoffOnlyRejectionTextTests(unittest.TestCase):
    """Unit coverage for the single-entry matcher (_cutoff_only_rejection_text)."""

    def test_exact_valid_examples_match(self):
        valid = [
            "Existing file meets cutoff: WEB DL-1080p",
            "Existing file meets cutoff: X",
            "Existing file meets cutoff: HDTV-720p",
            "Existing file meets cutoff: Multi Word Quality Name",
            "Existing file meets cutoff: :",  # colon is a valid non-whitespace suffix char
        ]
        for text in valid:
            with self.subTest(text=text):
                self.assertTrue(sonarr_api._cutoff_only_rejection_text(text))

    def test_bare_prefix_with_no_suffix_is_rejected(self):
        self.assertFalse(sonarr_api._cutoff_only_rejection_text("Existing file meets cutoff:"))

    def test_no_space_after_colon_is_rejected(self):
        self.assertFalse(sonarr_api._cutoff_only_rejection_text("Existing file meets cutoff:WEB DL-1080p"))

    def test_empty_suffix_after_required_space_is_rejected(self):
        self.assertFalse(sonarr_api._cutoff_only_rejection_text("Existing file meets cutoff: "))

    def test_whitespace_only_suffix_is_rejected(self):
        for suffix in (" ", "  ", "   ", "\t"):
            with self.subTest(suffix=repr(suffix)):
                self.assertFalse(sonarr_api._cutoff_only_rejection_text(
                    f"Existing file meets cutoff:{suffix}"
                ))

    def test_double_space_after_colon_is_rejected(self):
        # Exactly one space is Sonarr's literal format string; two spaces means the
        # first suffix character is itself whitespace, which is not tolerated.
        self.assertFalse(sonarr_api._cutoff_only_rejection_text("Existing file meets cutoff:  WEB DL-1080p"))

    def test_leading_whitespace_on_whole_string_is_rejected(self):
        self.assertFalse(sonarr_api._cutoff_only_rejection_text(" Existing file meets cutoff: WEB DL-1080p"))

    def test_trailing_whitespace_on_whole_string_is_rejected(self):
        for text in (
            "Existing file meets cutoff: WEB DL-1080p ",
            "Existing file meets cutoff: WEB DL-1080p\t",
            "Existing file meets cutoff: WEB DL-1080p\n",
        ):
            with self.subTest(text=repr(text)):
                self.assertFalse(sonarr_api._cutoff_only_rejection_text(text))

    def test_not_stripped_before_matching(self):
        # A caller must never see this normalized to valid via .strip() - the raw
        # string is matched exactly as Sonarr sent it.
        self.assertFalse(sonarr_api._cutoff_only_rejection_text("  Existing file meets cutoff: WEB DL-1080p  "))

    def test_wrong_case_is_rejected(self):
        self.assertFalse(sonarr_api._cutoff_only_rejection_text("existing file meets cutoff: WEB DL-1080p"))

    def test_extra_leading_text_is_rejected(self):
        self.assertFalse(sonarr_api._cutoff_only_rejection_text("Foo: Existing file meets cutoff: WEB DL-1080p"))

    def test_similar_but_distinct_sonarr_reasons_are_rejected(self):
        # Real, distinct Sonarr UpgradeDiskSpecification rejection texts that must
        # never be treated as the narrow cutoff-only condition.
        distinct_reasons = [
            "Existing file on disk is of equal or higher preference: WEBDL-1080p",
            "Existing file on disk is of equal or higher revision: v2",
            "Existing file on disk meets quality cutoff: WEB DL-1080p",
            "Existing file on disk meets Custom Format cutoff: 100",
            "Existing file on disk has a equal or higher Custom Format score: 50",
            "Existing file on disk has Custom Format score within Custom Format score increment: 10",
            "Existing file on disk and Quality Profile 'HD' does not allow upgrades",
        ]
        for reason in distinct_reasons:
            with self.subTest(reason=reason):
                self.assertFalse(sonarr_api._cutoff_only_rejection_text(reason))

    def test_non_string_entries_are_always_rejected(self):
        # Object/dict entries are not a shape Sonarr's ReleaseResource ever emits and
        # must never be defensively coerced or accepted, regardless of their keys.
        non_string_cases = [
            None, "", 42, 3.14, True, False,
            ["nested", "list"],
            {"reason": "Existing file meets cutoff: WEB DL-1080p"},
            {"message": "Existing file meets cutoff: WEB DL-1080p"},
            {"unexpectedKey": "Existing file meets cutoff: WEB DL-1080p"},
            {},
        ]
        for entry in non_string_cases:
            with self.subTest(entry=entry):
                self.assertFalse(sonarr_api._cutoff_only_rejection_text(entry))


class CutoffOnlyRejectionMatchingTests(unittest.TestCase):
    """Unit coverage for the whole-release allowlist matcher (_cutoff_only_rejections)."""

    def test_single_exact_cutoff_message_matches(self):
        release = _rejected_release("r1", 0, ["Existing file meets cutoff: WEB DL-1080p"])
        self.assertTrue(sonarr_api._cutoff_only_rejections(release))

    def test_multiple_cutoff_only_reasons_match(self):
        # A season pack maps multiple episode files; Sonarr emits one rejection per
        # file that already meets cutoff, so several instances of the same-shaped
        # message must still be treated as cutoff-only.
        release = _rejected_release("r1", 0, [
            "Existing file meets cutoff: WEB DL-1080p",
            "Existing file meets cutoff: HDTV-720p",
            "Existing file meets cutoff: Bluray-1080p",
        ])
        self.assertTrue(sonarr_api._cutoff_only_rejections(release))

    def test_mixed_cutoff_and_other_reason_fails_closed(self):
        release = _rejected_release("r1", 0, [
            "Existing file meets cutoff: WEB DL-1080p",
            "Not enough seeders",
        ])
        self.assertFalse(sonarr_api._cutoff_only_rejections(release))

    def test_mixed_cutoff_and_malformed_entry_fails_closed(self):
        release = _rejected_release("r1", 0, [
            "Existing file meets cutoff: WEB DL-1080p",
            "Existing file meets cutoff: ",
        ])
        self.assertFalse(sonarr_api._cutoff_only_rejections(release))

    def test_mixed_cutoff_and_object_entry_fails_closed(self):
        release = _rejected_release("r1", 0, [
            "Existing file meets cutoff: WEB DL-1080p",
            {"reason": "Existing file meets cutoff: WEB DL-1080p"},
        ])
        self.assertFalse(sonarr_api._cutoff_only_rejections(release))

    def test_similar_but_distinct_reasons_do_not_match(self):
        distinct_reasons = [
            "Existing file on disk is of equal or higher preference: WEBDL-1080p",
            "Existing file on disk is of equal or higher revision: v2",
            "Existing file on disk meets quality cutoff: WEB DL-1080p",
            "Existing file on disk meets Custom Format cutoff: 100",
            "Existing file on disk has a equal or higher Custom Format score: 50",
            "Existing file on disk has Custom Format score within Custom Format score increment: 10",
            "Existing file on disk and Quality Profile 'HD' does not allow upgrades",
        ]
        for reason in distinct_reasons:
            with self.subTest(reason=reason):
                release = _rejected_release("r1", 0, [reason])
                self.assertFalse(sonarr_api._cutoff_only_rejections(release))

    def test_empty_rejections_fails_closed(self):
        release = _rejected_release("r1", 0, [])
        self.assertFalse(sonarr_api._cutoff_only_rejections(release))

    def test_unknown_or_malformed_rejection_entries_fail_closed(self):
        malformed_cases = [
            ["Existing file meets cutoff: WEB DL-1080p", None],
            ["Existing file meets cutoff: WEB DL-1080p", ""],
            ["Existing file meets cutoff: WEB DL-1080p", 42],
            ["Existing file meets cutoff: WEB DL-1080p", ["nested", "list"]],
            ["Existing file meets cutoff: WEB DL-1080p", " Existing file meets cutoff: HDTV-720p"],
            ["Existing file meets cutoff: WEB DL-1080p", "Existing file meets cutoff: HDTV-720p "],
            ["Existing file meets cutoff: WEB DL-1080p", "Existing file meets cutoff:"],
            [{"unexpectedKey": "Existing file meets cutoff: WEB DL-1080p"}],
            [{"reason": 123}],
            [{"reason": "Existing file meets cutoff: WEB DL-1080p"}],
        ]
        for rejections in malformed_cases:
            with self.subTest(rejections=rejections):
                release = _rejected_release("r1", 0, rejections)
                self.assertFalse(sonarr_api._cutoff_only_rejections(release))

    def test_non_list_rejections_fails_closed(self):
        release = _rejected_release("r1", 0, [])
        release["rejections"] = "Existing file meets cutoff: WEB DL-1080p"
        self.assertFalse(sonarr_api._cutoff_only_rejections(release))

    def test_none_rejections_fails_closed(self):
        release = _rejected_release("r1", 0, [])
        release["rejections"] = None
        self.assertFalse(sonarr_api._cutoff_only_rejections(release))


class AcceptableSeasonPackOverrideTests(unittest.TestCase):
    """Coverage for _acceptable_season_pack's allow_cutoff_override behavior."""

    def test_disabled_override_rejects_cutoff_only_release_unchanged(self):
        release = _rejected_release("r1", 0, ["Existing file meets cutoff: WEB DL-1080p"])
        self.assertFalse(sonarr_api._acceptable_season_pack(
            release, 7, 2, "sonarr_default", allow_cutoff_override=False,
        ))

    def test_enabled_override_accepts_sole_cutoff_reason(self):
        release = _rejected_release("r1", 0, ["Existing file meets cutoff: WEB DL-1080p"])
        self.assertTrue(sonarr_api._acceptable_season_pack(
            release, 7, 2, "sonarr_default", allow_cutoff_override=True,
        ))

    def test_enabled_override_accepts_multiple_cutoff_only_reasons(self):
        release = _rejected_release("r1", 0, [
            "Existing file meets cutoff: WEB DL-1080p",
            "Existing file meets cutoff: HDTV-720p",
        ])
        self.assertTrue(sonarr_api._acceptable_season_pack(
            release, 7, 2, "sonarr_default", allow_cutoff_override=True,
        ))

    def test_enabled_override_never_accepts_mixed_reason(self):
        release = _rejected_release("r1", 0, [
            "Existing file meets cutoff: WEB DL-1080p",
            "Quality for existing file is of equal or higher preference",
        ])
        self.assertFalse(sonarr_api._acceptable_season_pack(
            release, 7, 2, "sonarr_default", allow_cutoff_override=True,
        ))

    def test_enabled_override_never_accepts_unknown_reason(self):
        release = _rejected_release("r1", 0, ["Some new Sonarr rejection text"])
        self.assertFalse(sonarr_api._acceptable_season_pack(
            release, 7, 2, "sonarr_default", allow_cutoff_override=True,
        ))

    def test_enabled_override_never_accepts_empty_rejections_with_approved_false(self):
        # approved=False with no rejections at all is malformed/unexplained - fail closed.
        release = _rejected_release("r1", 0, [])
        self.assertFalse(sonarr_api._acceptable_season_pack(
            release, 7, 2, "sonarr_default", allow_cutoff_override=True,
        ))

    def test_enabled_override_never_accepts_temporarily_rejected(self):
        release = _rejected_release(
            "r1", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            temporarilyRejected=True,
        )
        self.assertFalse(sonarr_api._acceptable_season_pack(
            release, 7, 2, "sonarr_default", allow_cutoff_override=True,
        ))

    def test_enabled_override_requires_exact_decision_state_booleans(self):
        # Every field in the override decision-state gate (approved, rejected,
        # temporarilyRejected) must compare with `is`, not truthiness. A missing,
        # None, or otherwise-typed value must fail closed exactly like the disabled
        # path, never falling through to the cutoff-only rejection check.
        base_rejections = ["Existing file meets cutoff: WEB DL-1080p"]
        cases = {
            "approved_none": {"approved": None},
            "approved_missing": {"approved": "__DELETE__"},
            "approved_string_false": {"approved": "false"},
            "approved_zero": {"approved": 0},
            "rejected_none": {"rejected": None},
            "rejected_missing": {"rejected": "__DELETE__"},
            "rejected_false": {"rejected": False},
            "rejected_string_true": {"rejected": "true"},
            "temporarily_rejected_none": {"temporarilyRejected": None},
            "temporarily_rejected_missing": {"temporarilyRejected": "__DELETE__"},
            "temporarily_rejected_true": {"temporarilyRejected": True},
            "temporarily_rejected_string_false": {"temporarilyRejected": "false"},
        }
        for name, overrides in cases.items():
            with self.subTest(case=name):
                release = _rejected_release("r1", 0, base_rejections)
                for key, value in overrides.items():
                    if value == "__DELETE__":
                        release.pop(key, None)
                    else:
                        release[key] = value
                self.assertFalse(sonarr_api._acceptable_season_pack(
                    release, 7, 2, "sonarr_default", allow_cutoff_override=True,
                ))

    def test_enabled_override_exact_valid_decision_state_is_accepted(self):
        # The positive control for the matrix above: approved is False, rejected is
        # True, temporarilyRejected is False, exactly (all _rejected_release
        # defaults) - this must still qualify.
        release = _rejected_release(
            "r1", 0, ["Existing file meets cutoff: WEB DL-1080p"],
        )
        self.assertIs(release["approved"], False)
        self.assertIs(release["rejected"], True)
        self.assertIs(release["temporarilyRejected"], False)
        self.assertTrue(sonarr_api._acceptable_season_pack(
            release, 7, 2, "sonarr_default", allow_cutoff_override=True,
        ))

    def test_enabled_override_still_enforces_full_season_mapping_and_download_allowed(self):
        base_rejections = ["Existing file meets cutoff: WEB DL-1080p"]
        wrong_season = _rejected_release("r1", 0, base_rejections, mappedSeasonNumber=3)
        wrong_series = _rejected_release("r2", 0, base_rejections, mappedSeriesId=8)
        not_full_season = _rejected_release("r3", 0, base_rejections, fullSeason=False)
        no_download_allowed = _rejected_release("r4", 0, base_rejections, downloadAllowed=False)
        for release in (wrong_season, wrong_series, not_full_season, no_download_allowed):
            with self.subTest(guid=release["guid"]):
                self.assertFalse(sonarr_api._acceptable_season_pack(
                    release, 7, 2, "sonarr_default", allow_cutoff_override=True,
                ))

    def test_enabled_override_still_enforces_protocol_filter(self):
        release = _rejected_release(
            "r1", 0, ["Existing file meets cutoff: WEB DL-1080p"], protocol="torrent",
        )
        self.assertFalse(sonarr_api._acceptable_season_pack(
            release, 7, 2, "usenet", allow_cutoff_override=True,
        ))

    def test_approved_normal_candidate_unaffected_by_override_flag(self):
        # A normal approved=True, rejections=[] release must remain acceptable
        # identically regardless of the override flag - no behavior change for the
        # already-passing path.
        release = _release("r1", 0)
        self.assertTrue(sonarr_api._acceptable_season_pack(
            release, 7, 2, "sonarr_default", allow_cutoff_override=False,
        ))
        self.assertTrue(sonarr_api._acceptable_season_pack(
            release, 7, 2, "sonarr_default", allow_cutoff_override=True,
        ))

    def test_approved_true_with_stray_rejections_is_rejected_defensively(self):
        # Sonarr should never emit approved=True together with populated rejections,
        # but if it did, treat it as unapproved rather than trusting the approved flag.
        release = _release("r1", 0, rejections=["Existing file meets cutoff: WEB DL-1080p"])
        self.assertFalse(sonarr_api._acceptable_season_pack(
            release, 7, 2, "sonarr_default", allow_cutoff_override=True,
        ))


class StrictSeasonPackApiTests(unittest.TestCase):
    def tearDown(self):
        queue_dispatch.clear_dispatch()

    def _dispatch_patches(self, claim=True, acquire=True):
        return (
            mock.patch.object(queue_dispatch, "claim_search", return_value=claim),
            mock.patch.object(queue_dispatch, "acquire_dispatch_slot", return_value=acquire),
            mock.patch.object(queue_dispatch, "begin_search_submission", return_value=True),
            mock.patch.object(queue_dispatch, "finish_interactive_search", return_value=True),
            mock.patch.object(queue_dispatch, "finish_search_claim", return_value=True),
            mock.patch.object(queue_dispatch, "cancel_dispatch_slot"),
            mock.patch.object(queue_dispatch, "publish_noop"),
            mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded", return_value=False),
        )

    def test_uses_season_interactive_api_and_grabs_sonarr_ranked_best_pack(self):
        individual = _release("episode", 0, fullSeason=False)
        lower_ranked_pack = _release("second", 8)
        best_pack = _release("best", 2, indexerId=9)
        get = mock.patch.object(
            sonarr_api.requests, "get",
            return_value=_Response([individual, lower_ranked_pack, best_pack]),
        )
        post = mock.patch.object(sonarr_api.requests, "post", return_value=_Response({}))
        patches = self._dispatch_patches() + (get, post)
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [12, 11, 12], "main",
        )

        self.assertEqual(result["guid"], "best")
        entered[0].assert_called_once_with(
            ["season:7:2", "episodes:11", "episodes:12"]
        )
        entered[8].assert_called_once_with(
            "http://sonarr/api/v3/release",
            headers={"X-Api-Key": "secret"},
            params={"seriesId": 7, "seasonNumber": 2},
            timeout=10,
            verify=mock.ANY,
        )
        entered[9].assert_called_once_with(
            "http://sonarr/api/v3/release",
            headers={"X-Api-Key": "secret"},
            json={"guid": "best", "indexerId": 9},
            timeout=10,
            verify=mock.ANY,
        )
        entered[3].assert_called_once_with(
            "grabbed", "sonarr", "main", queue_submission=True,
        )

    def test_rejected_wrong_mapped_and_individual_results_are_never_grabbed(self):
        releases = [
            _release("individual", 0, fullSeason=False),
            _release("rejected", 1, approved=False, rejected=True,
                     rejections=["Quality for existing file is of equal or higher preference"]),
            _release("temporary", 2, approved=False, temporarilyRejected=True,
                     rejections=["Not enough seeders"]),
            _release("wrong-season", 3, mappedSeasonNumber=3),
            _release("wrong-series", 4, mappedSeriesId=8),
            _release("allowed", 9),
        ]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response(releases)),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
        )
        self.assertEqual(result["guid"], "allowed")
        self.assertEqual(entered[9].call_args.kwargs["json"]["guid"], "allowed")

    def test_no_acceptable_pack_has_short_cooldown_and_no_grab(self):
        patches = self._dispatch_patches() + (
            mock.patch.object(
                sonarr_api.requests, "get",
                return_value=_Response([_release("episode", 0, fullSeason=False)]),
            ),
            mock.patch.object(sonarr_api.requests, "post"),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
        )
        self.assertIsNone(result)
        entered[9].assert_not_called()
        entered[6].assert_called_once_with("no acceptable season pack")
        entered[3].assert_called_once_with(
            "no_grab", "sonarr", "main", cooldown_seconds=300,
            queue_submission=False,
        )

    def test_permission_denial_prevents_interactive_search(self):
        patches = self._dispatch_patches(acquire=False) + (
            mock.patch.object(sonarr_api.requests, "get"),
            mock.patch.object(sonarr_api.requests, "post"),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        self.assertIsNone(sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
        ))
        entered[8].assert_not_called()
        entered[9].assert_not_called()
        entered[4].assert_called_once_with("timed_out", cooldown_seconds=60)

    def test_failed_grab_marks_failure_and_unwinds_queue_reservation(self):
        error = sonarr_api.requests.exceptions.HTTPError("grab failed")
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get",
                              return_value=_Response([_release("pack", 0)])),
            mock.patch.object(sonarr_api.requests, "post",
                              return_value=_Response({}, status=500, error=error)),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        self.assertIsNone(sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
        ))
        entered[3].assert_called_once_with(
            "failed", "sonarr", "main", cooldown_seconds=300,
            queue_submission=False,
        )

    def test_duplicate_claim_prevents_all_sonarr_requests(self):
        patches = self._dispatch_patches(claim=False) + (
            mock.patch.object(sonarr_api.requests, "get"),
            mock.patch.object(sonarr_api.requests, "post"),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        self.assertIsNone(sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
        ))
        entered[1].assert_not_called()
        entered[8].assert_not_called()
        entered[9].assert_not_called()

    def test_search_plus_grab_consumes_one_hourly_search_slot(self):
        queue_dispatch.clear_dispatch()
        with mock.patch.object(
                sonarr_api.requests, "get",
                return_value=_Response([_release("pack", 0)])), \
             mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})), \
             mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded",
                        return_value=False), \
             mock.patch("src.primary.stats_manager.increment_hourly_cap") as increment:
            result = sonarr_api.grab_best_season_pack(
                "http://sonarr", "secret", 10, 7, 2, [11], "main",
            )
        self.assertEqual(result["guid"], "pack")
        increment.assert_called_once_with("sonarr", 1, instance_name="main")

    def test_concurrent_duplicate_claim_allows_only_one_interactive_search(self):
        state = PipelineState(db=_DB())
        barrier = threading.Barrier(3)
        results = []

        def atomic_claim(keys):
            return state.claim_candidates("sonarr", "main", keys)

        def worker():
            barrier.wait()
            results.append(sonarr_api.grab_best_season_pack(
                "http://sonarr", "secret", 10, 7, 2, [11], "main",
            ))

        with mock.patch.object(queue_dispatch, "claim_search", side_effect=atomic_claim), \
             mock.patch.object(queue_dispatch, "acquire_dispatch_slot", return_value=True), \
             mock.patch.object(queue_dispatch, "begin_search_submission", return_value=True), \
             mock.patch.object(queue_dispatch, "finish_interactive_search", return_value=True), \
             mock.patch.object(queue_dispatch, "finish_search_claim", return_value=True), \
             mock.patch.object(queue_dispatch, "cancel_dispatch_slot"), \
             mock.patch.object(queue_dispatch, "publish_noop"), \
             mock.patch.object(sonarr_api.requests, "get", return_value=_Response([])) as get, \
             mock.patch.object(sonarr_api.requests, "post") as post, \
             mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded",
                        return_value=False):
            threads = [threading.Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            barrier.wait()
            for thread in threads:
                thread.join()

        self.assertEqual(results, [None, None])
        get.assert_called_once()
        post.assert_not_called()


class CutoffOverrideGrabTests(unittest.TestCase):
    """End-to-end grab_best_season_pack coverage for the cutoff-only override path."""

    def setUp(self):
        # API-unit coverage isolates the durable filesystem transaction; its full
        # validation/rollback behavior is covered in test_sonarr_season_recovery.
        patches = (
            mock.patch("src.primary.apps.sonarr.season_recovery.prepare_exact_season",
                       return_value="journal"),
            mock.patch("src.primary.apps.sonarr.season_recovery.mark_search_started",
                       return_value=True),
            mock.patch("src.primary.apps.sonarr.season_recovery.utc_now_iso",
                       return_value="2026-09-16T06:00:00Z"),
            mock.patch("src.primary.apps.sonarr.season_recovery.finish_operation",
                       return_value="imported"),
        )
        started = []
        for patcher in patches:
            started.append(patcher.start())
            self.addCleanup(patcher.stop)
        self.prepare_recovery, self.mark_recovery, _, self.finish_recovery = started

    def tearDown(self):
        queue_dispatch.clear_dispatch()

    def _dispatch_patches(self, claim=True, acquire=True):
        return (
            mock.patch.object(queue_dispatch, "claim_search", return_value=claim),
            mock.patch.object(queue_dispatch, "acquire_dispatch_slot", return_value=acquire),
            mock.patch.object(queue_dispatch, "begin_search_submission", return_value=True),
            mock.patch.object(queue_dispatch, "finish_interactive_search", return_value=True),
            mock.patch.object(queue_dispatch, "finish_search_claim", return_value=True),
            mock.patch.object(queue_dispatch, "cancel_dispatch_slot"),
            mock.patch.object(queue_dispatch, "publish_noop"),
            mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded", return_value=False),
        )

    def test_disabled_by_default_omits_should_override_and_rejects_cutoff_only_pack(self):
        cutoff_only = _rejected_release("cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"])
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response([cutoff_only])),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
        )
        self.assertIsNone(result)
        entered[9].assert_not_called()

    def test_enabled_override_grabs_cutoff_only_pack_with_should_override_true(self):
        cutoff_only = _rejected_release("cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"])
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response([cutoff_only])),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            allow_cutoff_override=True,
        )
        self.assertEqual(result["guid"], "cutoff-only")
        post_kwargs = entered[9].call_args.kwargs
        self.assertEqual(post_kwargs["json"], {
            "guid": "cutoff-only", "indexerId": 5, "shouldOverride": True,
            "seriesId": 7, "episodeIds": [101, 102],
            "quality": cutoff_only["quality"], "languages": cutoff_only["languages"],
        })
        self.prepare_recovery.assert_called_once_with(
            "http://sonarr", "secret", 10, "main", 7, 2,
            expected_episode_ids=[101, 102],
            expected_release_title="Show.S02.cutoff-only",
            recovery_timeout_seconds=600,
        )
        self.mark_recovery.assert_called_once_with(
            "main", "journal", "2026-09-16T06:00:00Z",
        )
        self.finish_recovery.assert_called_once()

    def test_enabled_override_normal_approved_candidate_omits_should_override(self):
        # An approved, non-rejected candidate must never carry shouldOverride even
        # when the setting is enabled - the override only applies when it was actually
        # needed to qualify the selected release.
        normal = _release("normal", 0)
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response([normal])),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            allow_cutoff_override=True,
        )
        self.assertEqual(result["guid"], "normal")
        post_kwargs = entered[9].call_args.kwargs
        self.assertEqual(post_kwargs["json"], {"guid": "normal", "indexerId": 5})
        for key in ("shouldOverride", "seriesId", "episodeIds", "quality", "languages"):
            self.assertNotIn(key, post_kwargs["json"])

    def test_enabled_override_is_not_returned_successful_until_atomic_commit(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
        )
        self.finish_recovery.return_value = "restored"
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response([cutoff_only])),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            allow_cutoff_override=True,
        )
        self.assertIsNone(result)
        entered[9].assert_called_once()
        self.finish_recovery.assert_called_once()

    def test_enabled_override_skips_earlier_mixed_rejection_selects_later_cutoff_only(self):
        # Sonarr ranking order must be preserved: an earlier-ranked candidate with an
        # unsafe/mixed rejection is skipped, and a later-ranked cutoff-only candidate
        # is selected instead - never the reverse, and never the unsafe one.
        earlier_mixed = _rejected_release(
            "earlier-mixed", 0,
            ["Existing file meets cutoff: WEB DL-1080p", "Not enough seeders"],
        )
        # episode_ids passed to grab_best_season_pack is only [11] (Huntarr's missing
        # list); mappedEpisodeInfo here carries the full pack (both 101 and 102) to
        # prove episodeIds in the POST body comes from the release, not the arg.
        later_cutoff_only = _rejected_release(
            "later-cutoff-only", 1, ["Existing file meets cutoff: HDTV-720p"],
        )
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get",
                              return_value=_Response([earlier_mixed, later_cutoff_only])),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            allow_cutoff_override=True,
        )
        self.assertEqual(result["guid"], "later-cutoff-only")
        post_kwargs = entered[9].call_args.kwargs
        self.assertTrue(post_kwargs["json"]["shouldOverride"])
        self.assertEqual(post_kwargs["json"]["episodeIds"], [101, 102])
        self.assertEqual(post_kwargs["json"]["seriesId"], 7)

    def test_enabled_override_preserves_download_client_id_alongside_should_override(self):
        cutoff_only = _rejected_release("cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
                                         protocol="usenet")
        clients = [{"id": 18, "name": "Decypharr Usenet", "protocol": "usenet", "enable": True}]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api, "get_download_clients", return_value=clients),
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response([cutoff_only])),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="usenet", download_client_id=18,
            allow_cutoff_override=True,
        )
        self.assertEqual(result["guid"], "cutoff-only")
        post_kwargs = entered[10].call_args.kwargs
        self.assertEqual(post_kwargs["json"], {
            "guid": "cutoff-only", "indexerId": 5,
            "downloadClientId": 18, "shouldOverride": True,
            "seriesId": 7, "episodeIds": [101, 102],
            "quality": cutoff_only["quality"], "languages": cutoff_only["languages"],
        })

    def test_enabled_override_no_qualifying_pack_still_no_grab(self):
        mixed = _rejected_release("mixed", 0, ["Not enough seeders"])
        unknown = _rejected_release("unknown", 1, ["Some brand new rejection text"])
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response([mixed, unknown])),
            mock.patch.object(sonarr_api.requests, "post"),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            allow_cutoff_override=True,
        )
        self.assertIsNone(result)
        entered[9].assert_not_called()
        entered[3].assert_called_once_with(
            "no_grab", "sonarr", "main", cooldown_seconds=300,
            queue_submission=False,
        )

    def test_enabled_override_post_failure_keeps_recovery_armed(self):
        cutoff_only = _rejected_release("cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"])
        error = sonarr_api.requests.exceptions.Timeout("client timed out after Sonarr may have accepted")
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response([cutoff_only])),
            mock.patch.object(sonarr_api.requests, "post", side_effect=error),
            mock.patch("src.primary.apps.sonarr.season_recovery.mark_submission_indeterminate",
                       return_value=True),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            allow_cutoff_override=True,
        )
        self.assertIsNone(result)
        entered[10].assert_called_once_with("main", "journal", str(error))
        self.finish_recovery.assert_not_called()
        entered[3].assert_called_once_with(
            "failed", "sonarr", "main", cooldown_seconds=300,
            queue_submission=False,
        )

    def _assert_override_fails_closed_no_post(self, cutoff_only):
        """Shared assertion for the override-field fail-closed tests below: no POST is
        ever sent, the grab reports no_grab (never a partial/malformed override), and
        the queue reservation unwinds exactly like the no-acceptable-pack path."""
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response([cutoff_only])),
            mock.patch.object(sonarr_api.requests, "post"),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            allow_cutoff_override=True,
        )
        self.assertIsNone(result)
        entered[9].assert_not_called()
        entered[3].assert_called_once_with(
            "no_grab", "sonarr", "main", cooldown_seconds=300,
            queue_submission=False,
        )

    def test_enabled_override_missing_mapped_episode_info_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            mappedEpisodeInfo=None,
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_empty_mapped_episode_info_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            mappedEpisodeInfo=[],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_non_integer_episode_id_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            mappedEpisodeInfo=[{"id": "101", "seasonNumber": 2, "episodeNumber": 1}],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_bool_episode_id_fails_closed(self):
        # bool is an int subclass in Python - explicitly excluded so a stray
        # True/False id can never be sent to Sonarr as an episode id.
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            mappedEpisodeInfo=[{"id": True, "seasonNumber": 2, "episodeNumber": 1}],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_duplicate_episode_id_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            mappedEpisodeInfo=[
                {"id": 101, "seasonNumber": 2, "episodeNumber": 1},
                {"id": 101, "seasonNumber": 2, "episodeNumber": 2},
            ],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_malformed_episode_entry_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            mappedEpisodeInfo=["not-a-dict"],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_missing_quality_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            quality=None,
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_missing_languages_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            languages=None,
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_non_list_languages_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            languages={"id": 1, "name": "English"},
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_mapped_series_id_mismatch_fails_closed(self):
        # Even though _acceptable_season_pack already enforces mappedSeriesId ==
        # series_id before a release is ever "selected", _build_override_fields
        # independently re-checks it and never broadens to a different series.
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            mappedSeriesId=7,
        )
        # Simulate a corrupted/inconsistent payload by calling the helper directly
        # with a mismatched requested series id.
        self.assertIsNone(sonarr_api._build_override_fields(cutoff_only, 999, 2))

    def test_enabled_override_bool_mapped_series_id_fails_closed(self):
        # bool is an int subclass in Python - a stray True/False mappedSeriesId that
        # happens to equal series_id via Python's bool==int comparison must never
        # pass; only a genuine int is a proven Sonarr shape.
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            mappedSeriesId=True,
        )
        self.assertIsNone(sonarr_api._build_override_fields(cutoff_only, 1, 2))

    def test_enabled_override_cross_season_episode_entry_fails_closed(self):
        # mappedEpisodeInfo containing an entry from a different season must never
        # be included or silently accepted - the override must never broaden onto
        # another season's episodes.
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            mappedEpisodeInfo=[
                {"id": 101, "seasonNumber": 2, "episodeNumber": 1},
                {"id": 201, "seasonNumber": 3, "episodeNumber": 1},
            ],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_bool_episode_season_number_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            mappedEpisodeInfo=[{"id": 101, "seasonNumber": True, "episodeNumber": 1}],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_missing_episode_season_number_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            mappedEpisodeInfo=[{"id": 101, "episodeNumber": 1}],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_scalar_quality_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            quality="HDTV-720p",
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_empty_dict_quality_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            quality={},
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_quality_missing_inner_quality_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            quality={"revision": {"version": 1, "real": 0, "isRepack": False}},
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_quality_missing_revision_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            quality={"quality": {"id": 4, "name": "HDTV-720p", "source": "television", "resolution": 720}},
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_quality_bool_id_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            quality={
                "quality": {"id": True, "name": "HDTV-720p", "source": "television", "resolution": 720},
                "revision": {"version": 1, "real": 0, "isRepack": False},
            },
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_quality_empty_name_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            quality={
                "quality": {"id": 4, "name": "", "source": "television", "resolution": 720},
                "revision": {"version": 1, "real": 0, "isRepack": False},
            },
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_quality_non_bool_is_repack_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            quality={
                "quality": {"id": 4, "name": "HDTV-720p", "source": "television", "resolution": 720},
                "revision": {"version": 1, "real": 0, "isRepack": "false"},
            },
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_scalar_language_entry_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            languages=["English"],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_bool_language_id_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            languages=[{"id": True, "name": "English"}],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_language_missing_name_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            languages=[{"id": 1, "name": ""}],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_mixed_valid_and_malformed_language_entries_fails_closed(self):
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            languages=[{"id": 1, "name": "English"}, "French"],
        )
        self._assert_override_fails_closed_no_post(cutoff_only)

    def test_enabled_override_accepts_proven_valid_quality_and_language_shapes(self):
        # Real Sonarr shapes, including a legitimately non-positive language id
        # (Original == -2, Unknown == 0 per NzbDrone.Core.Languages.Language) and a
        # multi-entry language list, must be accepted and passed through untouched.
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
            quality={
                "quality": {"id": 7, "name": "Bluray-1080p", "source": "bluray", "resolution": 1080},
                "revision": {"version": 2, "real": 1, "isRepack": True},
            },
            languages=[{"id": -2, "name": "Original"}, {"id": 1, "name": "English"}],
        )
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response([cutoff_only])),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [patch.start() for patch in patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            allow_cutoff_override=True,
        )
        self.assertEqual(result["guid"], "cutoff-only")
        post_kwargs = entered[9].call_args.kwargs
        self.assertEqual(post_kwargs["json"]["quality"], cutoff_only["quality"])
        self.assertEqual(post_kwargs["json"]["languages"], cutoff_only["languages"])

    def test_override_fields_deep_copies_quality_and_languages(self):
        # P2: the returned quality/languages must be independent copies - mutating
        # the POST payload (as callers commonly do) must never alter the selected
        # release dict that Huntarr continues to hold/log/tag from.
        cutoff_only = _rejected_release(
            "cutoff-only", 0, ["Existing file meets cutoff: WEB DL-1080p"],
        )
        fields = sonarr_api._build_override_fields(cutoff_only, 7, 2)
        self.assertIsNotNone(fields)
        self.assertIsNot(fields["quality"], cutoff_only["quality"])
        self.assertIsNot(fields["quality"]["quality"], cutoff_only["quality"]["quality"])
        self.assertIsNot(fields["quality"]["revision"], cutoff_only["quality"]["revision"])
        self.assertIsNot(fields["languages"], cutoff_only["languages"])
        self.assertIsNot(fields["languages"][0], cutoff_only["languages"][0])

        fields["quality"]["quality"]["name"] = "Tampered"
        fields["quality"]["revision"]["version"] = 999
        fields["languages"][0]["name"] = "Tampered"
        fields["languages"].append({"id": 99, "name": "Injected"})

        self.assertEqual(cutoff_only["quality"]["quality"]["name"], "HDTV-720p")
        self.assertEqual(cutoff_only["quality"]["revision"]["version"], 1)
        self.assertEqual(cutoff_only["languages"][0]["name"], "English")
        self.assertEqual(len(cutoff_only["languages"]), 1)


class InteractiveDispatchAccountingTests(unittest.TestCase):
    def setUp(self):
        self.context = queue_dispatch.DispatchContext(
            "sonarr", "main", 3, 10, 15, 60, lambda: False,
            lambda: 0, lambda: 0, mock.Mock(),
        )
        self.context.scheduler_held = True
        self.context.budget_reserved = True
        self.context.decypharr_reservation = object()
        self.context.current_item_keys = ["season:7:2", "episodes:11"]
        queue_dispatch._local.context = self.context

    def tearDown(self):
        queue_dispatch._local.context = None

    def test_accepted_grab_commits_one_search_and_retains_queue_capacity(self):
        pipeline = mock.Mock()
        pipeline.transition_items.return_value = True
        scheduler = mock.Mock()
        with mock.patch.object(queue_dispatch, "get_pipeline_state", return_value=pipeline), \
             mock.patch(
                 "src.primary.apps._common.shared_scheduler.get_shared_scheduler",
                 return_value=scheduler,
             ), \
             mock.patch("src.primary.stats_manager.increment_hourly_cap") as increment, \
             mock.patch("src.primary.stats_manager.release_hourly_cap_reservation") as release_budget, \
             mock.patch("src.primary.apps.swaparr.decypharr_capacity.release_reservation") as release_decy:
            self.assertTrue(queue_dispatch.finish_interactive_search(
                "grabbed", "sonarr", "main", queue_submission=True,
            ))

        self.assertFalse(self.context.budget_reserved)
        self.assertIsNone(self.context.decypharr_reservation)
        self.assertEqual(len(self.context.recent_submissions), 1)
        scheduler.release.assert_called_once_with("sonarr", "main")
        increment.assert_not_called()
        release_budget.assert_not_called()
        release_decy.assert_not_called()

    def test_failed_grab_commits_search_but_releases_queue_reservation(self):
        reservation = self.context.decypharr_reservation
        pipeline = mock.Mock()
        pipeline.transition_items.return_value = True
        scheduler = mock.Mock()
        with mock.patch.object(queue_dispatch, "get_pipeline_state", return_value=pipeline), \
             mock.patch(
                 "src.primary.apps._common.shared_scheduler.get_shared_scheduler",
                 return_value=scheduler,
             ), \
             mock.patch("src.primary.stats_manager.increment_hourly_cap") as increment, \
             mock.patch("src.primary.stats_manager.release_hourly_cap_reservation") as release_budget, \
             mock.patch("src.primary.apps.swaparr.decypharr_capacity.release_reservation") as release_decy:
            self.assertTrue(queue_dispatch.finish_interactive_search(
                "failed", "sonarr", "main", cooldown_seconds=300,
                queue_submission=False,
            ))

        self.assertFalse(self.context.budget_reserved)
        self.assertIsNone(self.context.decypharr_reservation)
        self.assertEqual(self.context.recent_submissions, [])
        scheduler.release.assert_called_once_with("sonarr", "main")
        release_decy.assert_called_once_with({}, reservation)
        increment.assert_not_called()
        release_budget.assert_not_called()


class MissingSeasonPackIntegrationTests(unittest.TestCase):
    episode = {
        "id": 11,
        "seriesId": 7,
        "seasonNumber": 2,
        "monitored": True,
        "airDateUtc": "2020-01-01T00:00:00Z",
        "series": {"title": "Show"},
    }

    def _run(self, grab_result, **kwargs):
        with mock.patch.object(
                sonarr_api, "get_missing_episodes_random_page",
                return_value=[self.episode]), \
             mock.patch.object(sonarr_missing.random, "shuffle"), \
             mock.patch.object(sonarr_missing, "is_processed", return_value=False), \
             mock.patch.object(sonarr_missing, "check_hourly_cap_exceeded", return_value=False), \
             mock.patch.object(sonarr_api, "grab_best_season_pack",
                               return_value=grab_result) as grab, \
             mock.patch.object(sonarr_missing, "add_processed_id") as processed, \
             mock.patch.object(sonarr_missing, "try_tag_item") as tag, \
             mock.patch.object(sonarr_missing, "log_processed_media") as history, \
             mock.patch.object(sonarr_missing, "increment_media_stat_only") as stats, \
             mock.patch.object(sonarr_api, "search_season") as automatic, \
             mock.patch.object(sonarr_api, "search_episode") as episodes:
            result = sonarr_missing.process_missing_seasons_packs_mode(
                "http://sonarr", "secret", "main", 10, True, True,
                1, 0, 1, 2, lambda: False,
                **kwargs,
            )
        return result, grab, processed, tag, history, stats, automatic, episodes

    def test_accepted_grab_marks_processed_tags_history_and_media_stats(self):
        result, grab, processed, tag, history, stats, automatic, episodes = self._run(
            _release("pack", 0)
        )
        self.assertTrue(result)
        grab.assert_called_once_with(
            "http://sonarr", "secret", 10, 7, 2, [11], instance_name="main",
            download_protocol="sonarr_default", download_client_id=None,
            allow_cutoff_override=False,
        )
        processed.assert_called_once_with("sonarr", "main", "7_2")
        tag.assert_called_once()
        self.assertEqual(history.call_args.kwargs["status"], "grabbed")
        stats.assert_called_once_with("sonarr", "hunted", 1, "main")
        automatic.assert_not_called()
        episodes.assert_not_called()

    def test_missing_pack_allow_cutoff_override_defaults_false_and_forwards_when_set(self):
        result, grab, _processed, _tag, _history, _stats, _automatic, _episodes = self._run(
            _release("pack", 0)
        )
        self.assertTrue(result)
        self.assertFalse(grab.call_args.kwargs["allow_cutoff_override"])

        result, grab, _processed, _tag, _history, _stats, _automatic, _episodes = self._run(
            _release("pack", 0), missing_pack_allow_cutoff_override=True,
        )
        self.assertTrue(result)
        self.assertTrue(grab.call_args.kwargs["allow_cutoff_override"])

    def test_no_pack_is_not_processed_or_tagged_and_never_falls_back(self):
        result, grab, processed, tag, history, stats, automatic, episodes = self._run(None)
        self.assertFalse(result)
        grab.assert_called_once()
        processed.assert_not_called()
        tag.assert_not_called()
        history.assert_not_called()
        stats.assert_not_called()
        automatic.assert_not_called()
        episodes.assert_not_called()

    def test_episodes_mode_dispatch_is_unchanged(self):
        with mock.patch.object(sonarr_missing, "process_missing_episodes_mode",
                               return_value=True) as episode_mode, \
             mock.patch.object(sonarr_missing, "process_missing_seasons_packs_mode") as pack_mode:
            result = sonarr_missing.process_missing_episodes(
                "http://sonarr", "secret", "main", hunt_missing_items=1,
                hunt_missing_mode="episodes",
            )
        self.assertTrue(result)
        episode_mode.assert_called_once()
        pack_mode.assert_not_called()

    def test_upgrade_and_exact_season_recovery_still_use_command_search(self):
        # Strict interactive grabbing is intentionally scoped to missing mode only.
        source_call = mock.patch.object(sonarr_api, "grab_best_season_pack")
        with source_call as strict, \
             mock.patch.object(sonarr_api.requests, "post",
                               return_value=_Response({"id": 123})), \
             mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded",
                        return_value=False), \
             mock.patch("src.primary.stats_manager.increment_hourly_cap"):
            command_id = sonarr_api.search_season(
                "http://sonarr", "secret", 10, 7, 2, instance_name="main",
            )
        self.assertEqual(command_id, 123)
        strict.assert_not_called()


if __name__ == "__main__":
    unittest.main()

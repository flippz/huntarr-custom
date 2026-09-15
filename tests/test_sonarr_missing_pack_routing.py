"""Coverage for strict missing season-pack download protocol/client routing.

Verifies: defaults/migration for existing installs, settings validation/clamping,
the download-clients listing route, protocol normalization against Sonarr's proven
API shapes, strict filtering (usenet excludes torrent and vice versa, Sonarr default
retains both), exact downloadClientId passthrough, Automatic omission, fail-closed
behavior for stale/disabled/mismatched clients, and that no-acceptable-protocol packs
never grab/fallback/lock processed state.
"""

import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("HUNTARR_CONFIG_DIR", tempfile.mkdtemp(prefix="huntarr_pack_routing_"))

from src.primary.apps._common import queue_dispatch
from src.primary.apps.sonarr import api as sonarr_api
from src.primary.apps.sonarr import missing as sonarr_missing
from src.primary import default_settings
from src.primary import settings_manager
from src.primary.utils.database import get_database


def _release(guid, weight, **changes):
    release = {
        "guid": guid,
        "indexerId": 5,
        "title": f"Show.S02.{guid}",
        "fullSeason": True,
        "mappedSeriesId": 7,
        "mappedSeasonNumber": 2,
        "approved": True,
        "downloadAllowed": True,
        "rejected": False,
        "temporarilyRejected": False,
        "rejections": [],
        "releaseWeight": weight,
        "protocol": "usenet",
    }
    release.update(changes)
    return release


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


# ---------------------------------------------------------------------------
# Defaults / migration for existing installs
# ---------------------------------------------------------------------------

class DefaultsAndMigrationTests(unittest.TestCase):
    def test_new_default_instance_config_is_sonarr_default_automatic(self):
        cfg = default_settings.get_default_instance_config("sonarr")
        self.assertEqual(cfg["missing_pack_download_protocol"], "sonarr_default")
        self.assertIsNone(cfg["missing_pack_download_client_id"])

    def test_existing_instance_without_new_keys_defaults_to_sonarr_behavior(self):
        """An old DB record predating this feature must behave exactly as before."""
        legacy_instance = {
            "name": "Legacy", "api_url": "http://sonarr", "api_key": "k",
            "enabled": True,
        }
        settings = {"instances": [legacy_instance]}
        import src.primary.apps.sonarr as sonarr_pkg
        with mock.patch.object(sonarr_pkg, "load_settings", return_value=settings):
            instances = sonarr_pkg.get_configured_instances(quiet=True)
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]["missing_pack_download_protocol"], "sonarr_default")
        self.assertIsNone(instances[0]["missing_pack_download_client_id"])


# ---------------------------------------------------------------------------
# Settings validation (save_settings)
# ---------------------------------------------------------------------------

class SaveSettingsValidationTests(unittest.TestCase):
    def setUp(self):
        self.db = get_database()

    def test_invalid_protocol_falls_back_to_sonarr_default_top_level(self):
        data = {"missing_pack_download_protocol": "bogus"}
        settings_manager.save_settings("sonarr", data)
        self.assertEqual(data["missing_pack_download_protocol"], "sonarr_default")

    def test_valid_protocols_pass_through(self):
        for proto in ("sonarr_default", "usenet", "torrent"):
            data = {"missing_pack_download_protocol": proto}
            settings_manager.save_settings("sonarr", data)
            self.assertEqual(data["missing_pack_download_protocol"], proto)

    def test_client_id_automatic_sentinel_normalizes_to_none(self):
        for sentinel in ("", "automatic", "null", None):
            data = {"missing_pack_download_client_id": sentinel}
            settings_manager.save_settings("sonarr", data)
            self.assertIsNone(data["missing_pack_download_client_id"])

    def test_client_id_numeric_string_coerced_to_int(self):
        data = {"missing_pack_download_client_id": "42"}
        settings_manager.save_settings("sonarr", data)
        self.assertEqual(data["missing_pack_download_client_id"], 42)

    def test_client_id_garbage_normalizes_to_none(self):
        data = {"missing_pack_download_client_id": "not-a-number"}
        settings_manager.save_settings("sonarr", data)
        self.assertIsNone(data["missing_pack_download_client_id"])

    def test_per_instance_validation_matches_top_level(self):
        data = {"instances": [{
            "name": "A", "missing_pack_download_protocol": "TORRENT-ish",
            "missing_pack_download_client_id": "7",
        }]}
        settings_manager.save_settings("sonarr", data)
        inst = data["instances"][0]
        self.assertEqual(inst["missing_pack_download_protocol"], "sonarr_default")
        self.assertEqual(inst["missing_pack_download_client_id"], 7)

    def test_upgrade_mode_and_episodes_mode_untouched_by_this_setting(self):
        """This feature is scoped to missing strict season packs only."""
        data = {
            "missing_pack_download_protocol": "usenet",
            "upgrade_mode": "episodes",
            "hunt_missing_mode": "episodes",
        }
        settings_manager.save_settings("sonarr", data)
        self.assertEqual(data["upgrade_mode"], "episodes")
        self.assertEqual(data["hunt_missing_mode"], "episodes")


# ---------------------------------------------------------------------------
# Protocol normalization against proven Sonarr API shapes
# ---------------------------------------------------------------------------

class ProtocolNormalizationTests(unittest.TestCase):
    def test_camel_case_strings_from_sonarr_string_enum_converter(self):
        self.assertEqual(sonarr_api.normalize_release_protocol("usenet"), "usenet")
        self.assertEqual(sonarr_api.normalize_release_protocol("torrent"), "torrent")
        self.assertEqual(sonarr_api.normalize_release_protocol("unknown"), "unknown")

    def test_case_insensitive(self):
        self.assertEqual(sonarr_api.normalize_release_protocol("Usenet"), "usenet")
        self.assertEqual(sonarr_api.normalize_release_protocol("TORRENT"), "torrent")

    def test_numeric_enum_fallback(self):
        self.assertEqual(sonarr_api.normalize_release_protocol(1), "usenet")
        self.assertEqual(sonarr_api.normalize_release_protocol(2), "torrent")
        self.assertEqual(sonarr_api.normalize_release_protocol(0), "unknown")

    def test_unrecognized_shapes_return_none(self):
        self.assertIsNone(sonarr_api.normalize_release_protocol(None))
        self.assertIsNone(sonarr_api.normalize_release_protocol("ftp"))
        self.assertIsNone(sonarr_api.normalize_release_protocol(99))
        self.assertIsNone(sonarr_api.normalize_release_protocol(True))


# ---------------------------------------------------------------------------
# get_download_clients() - credential-safe listing
# ---------------------------------------------------------------------------

class GetDownloadClientsTests(unittest.TestCase):
    def test_maps_id_name_protocol_enable_and_normalizes_protocol(self):
        clients = [
            {"id": 1, "name": "Decypharr Usenet", "protocol": "usenet", "enable": True},
            {"id": 2, "name": "qBit", "protocol": "torrent", "enable": False},
        ]
        with mock.patch.object(sonarr_api.requests, "get", return_value=_Response(clients)):
            result = sonarr_api.get_download_clients("http://sonarr", "key", 10)
        self.assertEqual(result, [
            {"id": 1, "name": "Decypharr Usenet", "protocol": "usenet", "enable": True},
            {"id": 2, "name": "qBit", "protocol": "torrent", "enable": False},
        ])

    def test_never_includes_api_key_in_result(self):
        clients = [{"id": 1, "name": "X", "protocol": "usenet", "enable": True, "apiKey": "leak"}]
        with mock.patch.object(sonarr_api.requests, "get", return_value=_Response(clients)):
            result = sonarr_api.get_download_clients("http://sonarr", "secret-key", 10)
        self.assertNotIn("apiKey", result[0])
        self.assertNotIn("api_key", result[0])

    def test_true_empty_client_list_returns_empty_list_not_error(self):
        """Sonarr legitimately having zero download clients configured is not a failure."""
        with mock.patch.object(sonarr_api.requests, "get", return_value=_Response([])):
            result = sonarr_api.get_download_clients("http://sonarr", "key", 10)
        self.assertEqual(result, [])

    def test_network_error_raises_distinct_from_empty_list(self):
        """An upstream fetch failure must be distinguishable from a true empty list (P2 fix)."""
        with mock.patch.object(sonarr_api.requests, "get",
                               side_effect=sonarr_api.requests.exceptions.ConnectionError("down")):
            with self.assertRaises(sonarr_api.SonarrDownloadClientsError):
                sonarr_api.get_download_clients("http://sonarr", "key", 10)

    def test_http_error_status_raises(self):
        error = sonarr_api.requests.exceptions.HTTPError("500 server error")
        with mock.patch.object(sonarr_api.requests, "get",
                               return_value=_Response({}, status=500, error=error)):
            with self.assertRaises(sonarr_api.SonarrDownloadClientsError):
                sonarr_api.get_download_clients("http://sonarr", "key", 10)

    def test_non_list_response_raises(self):
        with mock.patch.object(sonarr_api.requests, "get", return_value=_Response({"not": "a list"})):
            with self.assertRaises(sonarr_api.SonarrDownloadClientsError):
                sonarr_api.get_download_clients("http://sonarr", "key", 10)


# ---------------------------------------------------------------------------
# download-clients route: auth/error/redaction
# ---------------------------------------------------------------------------

try:
    import flask  # noqa: F401
    _FLASK_AVAILABLE = True
except ImportError:
    _FLASK_AVAILABLE = False


@unittest.skipUnless(_FLASK_AVAILABLE, "flask is not installed in this environment")
class DownloadClientsRouteTests(unittest.TestCase):
    def setUp(self):
        from flask import Flask
        from src.primary.apps.sonarr_routes import sonarr_bp
        app = Flask(__name__)
        app.config["TESTING"] = True
        app.register_blueprint(sonarr_bp, url_prefix='/api/sonarr')
        self.client = app.test_client()

    def test_missing_credentials_returns_400(self):
        resp = self.client.post("/api/sonarr/download-clients", json={})
        self.assertEqual(resp.status_code, 400)

    def test_success_returns_clients_without_key(self):
        clients = [{"id": 1, "name": "Decypharr Usenet", "protocol": "usenet", "enable": True}]
        with mock.patch(
                "src.primary.apps.sonarr_routes.sonarr_api.get_download_clients",
                return_value=clients) as fetch:
            resp = self.client.post("/api/sonarr/download-clients",
                                    json={"api_url": "http://sonarr", "api_key": "secret"})
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["success"])
        self.assertEqual(body["clients"], clients)
        self.assertNotIn("secret", str(body))
        fetch.assert_called_once_with("http://sonarr", "secret", 30)

    def test_upstream_error_returns_502_not_crash(self):
        with mock.patch(
                "src.primary.apps.sonarr_routes.sonarr_api.get_download_clients",
                side_effect=RuntimeError("boom")):
            resp = self.client.post("/api/sonarr/download-clients",
                                    json={"api_url": "http://sonarr", "api_key": "secret"})
        self.assertEqual(resp.status_code, 502)
        self.assertNotIn("boom", str(resp.get_json()))

    def test_sonarr_download_clients_error_returns_502_with_failure_status(self):
        """P2 fix: a real upstream fetch failure must be distinguishable from an
        empty client list - success:false + 502, never success:true + []."""
        from src.primary.apps.sonarr import api as real_sonarr_api
        with mock.patch(
                "src.primary.apps.sonarr_routes.sonarr_api.get_download_clients",
                side_effect=real_sonarr_api.SonarrDownloadClientsError("Sonarr unreachable")):
            resp = self.client.post("/api/sonarr/download-clients",
                                    json={"api_url": "http://sonarr", "api_key": "secret"})
        self.assertEqual(resp.status_code, 502)
        body = resp.get_json()
        self.assertFalse(body["success"])
        self.assertNotIn("clients", body)

    def test_true_empty_client_list_returns_200_success_with_empty_array(self):
        """Distinguish "Sonarr has zero clients" (200/success) from an error (502)."""
        with mock.patch(
                "src.primary.apps.sonarr_routes.sonarr_api.get_download_clients",
                return_value=[]):
            resp = self.client.post("/api/sonarr/download-clients",
                                    json={"api_url": "http://sonarr", "api_key": "secret"})
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["success"])
        self.assertEqual(body["clients"], [])


# ---------------------------------------------------------------------------
# grab_best_season_pack: strict protocol filtering + client passthrough
# ---------------------------------------------------------------------------

class StrictProtocolFilteringTests(unittest.TestCase):
    def tearDown(self):
        queue_dispatch.clear_dispatch()

    def _dispatch_patches(self):
        return (
            mock.patch.object(queue_dispatch, "claim_search", return_value=True),
            mock.patch.object(queue_dispatch, "acquire_dispatch_slot", return_value=True),
            mock.patch.object(queue_dispatch, "begin_search_submission", return_value=True),
            mock.patch.object(queue_dispatch, "finish_interactive_search", return_value=True),
            mock.patch.object(queue_dispatch, "finish_search_claim", return_value=True),
            mock.patch.object(queue_dispatch, "cancel_dispatch_slot"),
            mock.patch.object(queue_dispatch, "publish_noop"),
            mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded", return_value=False),
        )

    def test_usenet_only_excludes_torrent_releases(self):
        releases = [
            _release("torrent-pack", 0, protocol="torrent"),
            _release("usenet-pack", 1, protocol="usenet"),
        ]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response(releases)),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="usenet",
        )
        self.assertEqual(result["guid"], "usenet-pack")
        self.assertEqual(entered[9].call_args.kwargs["json"]["guid"], "usenet-pack")

    def test_torrent_only_excludes_usenet_releases(self):
        releases = [
            _release("usenet-pack", 0, protocol="usenet"),
            _release("torrent-pack", 1, protocol="torrent"),
        ]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response(releases)),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="torrent",
        )
        self.assertEqual(result["guid"], "torrent-pack")

    def test_sonarr_default_retains_both_protocols(self):
        releases = [
            _release("torrent-pack", 0, protocol="torrent"),
            _release("usenet-pack", 1, protocol="usenet"),
        ]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response(releases)),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="sonarr_default",
        )
        # Best-weighted (torrent-pack, weight 0) wins; both protocols were eligible.
        self.assertEqual(result["guid"], "torrent-pack")

    def test_no_acceptable_protocol_pack_reports_precise_reason_and_grabs_nothing(self):
        releases = [_release("torrent-pack", 0, protocol="torrent")]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response(releases)),
            mock.patch.object(sonarr_api.requests, "post"),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="usenet",
        )
        self.assertIsNone(result)
        entered[9].assert_not_called()
        entered[6].assert_called_once_with("no acceptable usenet season pack")
        entered[3].assert_called_once_with(
            "no_grab", "sonarr", "main", cooldown_seconds=300, queue_submission=False,
        )

    def test_rejected_release_stays_rejected_even_with_client_override_configured(self):
        """A rejected release must never be grabbed, regardless of client override."""
        releases = [
            _release("rejected", 0, protocol="usenet", approved=False, rejected=True,
                     rejections=["Release rejected"]),
        ]
        clients = [{"id": 3, "name": "Decypharr Usenet", "protocol": "usenet", "enable": True}]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response(releases)),
            mock.patch.object(sonarr_api.requests, "post"),
            mock.patch.object(sonarr_api, "get_download_clients", return_value=clients),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="usenet", download_client_id=3,
        )
        self.assertIsNone(result)
        entered[9].assert_not_called()


class ExactClientIdPassthroughTests(unittest.TestCase):
    def tearDown(self):
        queue_dispatch.clear_dispatch()

    def _dispatch_patches(self):
        return (
            mock.patch.object(queue_dispatch, "claim_search", return_value=True),
            mock.patch.object(queue_dispatch, "acquire_dispatch_slot", return_value=True),
            mock.patch.object(queue_dispatch, "begin_search_submission", return_value=True),
            mock.patch.object(queue_dispatch, "finish_interactive_search", return_value=True),
            mock.patch.object(queue_dispatch, "finish_search_claim", return_value=True),
            mock.patch.object(queue_dispatch, "cancel_dispatch_slot"),
            mock.patch.object(queue_dispatch, "publish_noop"),
            mock.patch("src.primary.stats_manager.check_hourly_cap_exceeded", return_value=False),
        )

    def test_valid_enabled_matching_client_sends_exact_download_client_id(self):
        releases = [_release("pack", 0, protocol="usenet")]
        clients = [{"id": 42, "name": "Decypharr Usenet", "protocol": "usenet", "enable": True}]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response(releases)),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
            mock.patch.object(sonarr_api, "get_download_clients", return_value=clients),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="usenet", download_client_id=42,
        )
        self.assertEqual(result["guid"], "pack")
        sent_body = entered[9].call_args.kwargs["json"]
        self.assertEqual(sent_body["downloadClientId"], 42)
        self.assertEqual(sent_body["guid"], "pack")
        self.assertEqual(sent_body["indexerId"], 5)

    def test_automatic_omits_download_client_id_from_body(self):
        releases = [_release("pack", 0, protocol="usenet")]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response(releases)),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="usenet", download_client_id=None,
        )
        sent_body = entered[9].call_args.kwargs["json"]
        self.assertNotIn("downloadClientId", sent_body)

    def test_sonarr_default_omits_download_client_id_even_if_configured(self):
        """download_client_id is only meaningful when a protocol filter is active."""
        releases = [_release("pack", 0, protocol="usenet")]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get", return_value=_Response(releases)),
            mock.patch.object(sonarr_api.requests, "post", return_value=_Response({})),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="sonarr_default", download_client_id=42,
        )
        sent_body = entered[9].call_args.kwargs["json"]
        self.assertNotIn("downloadClientId", sent_body)

    def test_stale_client_id_fails_closed_no_search_dispatched(self):
        clients = [{"id": 1, "name": "Other", "protocol": "usenet", "enable": True}]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get"),
            mock.patch.object(sonarr_api.requests, "post"),
            mock.patch.object(sonarr_api, "get_download_clients", return_value=clients),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="usenet", download_client_id=999,
        )
        self.assertIsNone(result)
        entered[8].assert_not_called()  # GET /release never dispatched
        entered[9].assert_not_called()
        entered[0].assert_not_called()  # claim_search never reached: no cap/queue slot consumed

    def test_disabled_client_fails_closed(self):
        clients = [{"id": 42, "name": "Decypharr Usenet", "protocol": "usenet", "enable": False}]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get"),
            mock.patch.object(sonarr_api.requests, "post"),
            mock.patch.object(sonarr_api, "get_download_clients", return_value=clients),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="usenet", download_client_id=42,
        )
        self.assertIsNone(result)
        entered[9].assert_not_called()

    def test_protocol_mismatched_client_fails_closed(self):
        clients = [{"id": 42, "name": "qBit", "protocol": "torrent", "enable": True}]
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get"),
            mock.patch.object(sonarr_api.requests, "post"),
            mock.patch.object(sonarr_api, "get_download_clients", return_value=clients),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="usenet", download_client_id=42,
        )
        self.assertIsNone(result)
        entered[9].assert_not_called()

    def test_no_download_clients_available_fails_closed(self):
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get"),
            mock.patch.object(sonarr_api.requests, "post"),
            mock.patch.object(sonarr_api, "get_download_clients", return_value=[]),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="usenet", download_client_id=42,
        )
        self.assertIsNone(result)
        entered[9].assert_not_called()

    def test_download_clients_fetch_error_fails_closed_same_as_empty_list(self):
        """P2 fix: get_download_clients now raises on fetch failure instead of
        returning []; grab-time validation must still fail closed identically."""
        patches = self._dispatch_patches() + (
            mock.patch.object(sonarr_api.requests, "get"),
            mock.patch.object(sonarr_api.requests, "post"),
            mock.patch.object(sonarr_api, "get_download_clients",
                              side_effect=sonarr_api.SonarrDownloadClientsError("Sonarr unreachable")),
        )
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        result = sonarr_api.grab_best_season_pack(
            "http://sonarr", "secret", 10, 7, 2, [11], "main",
            download_protocol="usenet", download_client_id=42,
        )
        self.assertIsNone(result)
        entered[8].assert_not_called()  # GET /release never dispatched
        entered[9].assert_not_called()
        entered[0].assert_not_called()  # claim_search never reached: no cap/queue slot consumed


# ---------------------------------------------------------------------------
# Episodes/upgrade paths unaffected
# ---------------------------------------------------------------------------

class UnaffectedPathsTests(unittest.TestCase):
    def test_episodes_mode_never_calls_grab_best_season_pack(self):
        with mock.patch.object(sonarr_missing, "process_missing_episodes_mode",
                               return_value=True) as episode_mode, \
             mock.patch.object(sonarr_missing, "process_missing_seasons_packs_mode") as pack_mode:
            result = sonarr_missing.process_missing_episodes(
                "http://sonarr", "secret", "main", hunt_missing_items=1,
                hunt_missing_mode="episodes",
                missing_pack_download_protocol="usenet",
                missing_pack_download_client_id=42,
            )
        self.assertTrue(result)
        episode_mode.assert_called_once()
        pack_mode.assert_not_called()

    def test_shows_mode_never_calls_grab_best_season_pack(self):
        with mock.patch.object(sonarr_missing, "process_missing_shows_mode",
                               return_value=True) as shows_mode, \
             mock.patch.object(sonarr_missing, "process_missing_seasons_packs_mode") as pack_mode:
            result = sonarr_missing.process_missing_episodes(
                "http://sonarr", "secret", "main", hunt_missing_items=1,
                hunt_missing_mode="shows",
                missing_pack_download_protocol="torrent",
                missing_pack_download_client_id=7,
            )
        self.assertTrue(result)
        shows_mode.assert_called_once()
        pack_mode.assert_not_called()


# ---------------------------------------------------------------------------
# Full missing-mode integration: no acceptable protocol pack -> no grab/lock
# ---------------------------------------------------------------------------

class MissingModeProtocolIntegrationTests(unittest.TestCase):
    episode = {
        "id": 11,
        "seriesId": 7,
        "seasonNumber": 2,
        "monitored": True,
        "airDateUtc": "2020-01-01T00:00:00Z",
        "series": {"title": "Show"},
    }

    def test_protocol_and_client_settings_threaded_to_grab_call(self):
        with mock.patch.object(
                sonarr_api, "get_missing_episodes_random_page",
                return_value=[self.episode]), \
             mock.patch.object(sonarr_missing.random, "shuffle"), \
             mock.patch.object(sonarr_missing, "is_processed", return_value=False), \
             mock.patch.object(sonarr_missing, "check_hourly_cap_exceeded", return_value=False), \
             mock.patch.object(sonarr_api, "grab_best_season_pack",
                               return_value=None) as grab, \
             mock.patch.object(sonarr_missing, "add_processed_id") as processed, \
             mock.patch.object(sonarr_missing, "log_processed_media") as history:
            result = sonarr_missing.process_missing_seasons_packs_mode(
                "http://sonarr", "secret", "main", 10, True, True,
                1, 0, 1, 2, lambda: False,
                missing_pack_download_protocol="usenet",
                missing_pack_download_client_id=42,
            )
        self.assertFalse(result)
        grab.assert_called_once_with(
            "http://sonarr", "secret", 10, 7, 2, [11], instance_name="main",
            download_protocol="usenet", download_client_id=42,
        )
        # No grab -> season is never marked processed, never locked, never logged.
        processed.assert_not_called()
        history.assert_not_called()


if __name__ == "__main__":
    unittest.main()

"""Regression tests for informational/warning box copy across Managearr pages.

These lock in wording that reflects the currently implemented product
(through M11, plus the Huntarr-aligned navigation) so a stale "not
available"/"not active" claim about a feature that has since shipped
(persistent Live controls, multi-dispatch pacing, M10 missing/upgrade
distinction, M11 manual season packs) cannot silently return, and so the
sidebar navigation shell cannot silently regress back to a full-page
"Development Preview" warning banner.
"""

PAGES = ("/", "/sonarr", "/activity", "/settings")


def test_no_global_development_preview_banner(client):
    for path in PAGES:
        html = client.get(path).get_data(as_text=True)
        assert "Development Preview" not in html
        assert "preview-banner" not in html
        assert "automated hunting is not active" not in html


def test_settings_no_longer_claims_live_automation_is_unavailable(client):
    html = client.get("/settings").get_data(as_text=True)
    assert "Live automation is not available" not in html
    assert "Simulation sends no Sonarr commands" in html
    # The Live automation controls this notice references must actually be present.
    assert '<h2 id="live-automation">Live automation</h2>' in html
    assert 'id="live-mode-request"' in html


def test_sonarr_instances_tab_describes_current_missing_and_upgrade_refresh(client):
    html = client.get("/sonarr").get_data(as_text=True)
    assert "reads Sonarr library and wanted data" in html
    assert "refresh missing and upgrade candidates" in html
    assert "only reads series/episode data" not in html


def test_activity_copy_describes_automatic_and_manual_reconciliation(client):
    html = client.get("/activity").get_data(as_text=True)
    assert "Live dispatches are reconciled automatically" in html
    assert "manual reconciliation remains available" in html
    assert "Outcome reconciliation is manual" not in html


def test_hunting_policy_tab_points_to_season_packs_tab(client):
    html = client.get("/sonarr").get_data(as_text=True)
    assert "does not dispatch season packs" in html
    assert "Season packs tab" in html
    assert "No season packs, file deletion, or replacement" not in html


def test_home_live_card_is_not_described_as_unavailable(client):
    html = client.get("/").get_data(as_text=True)
    assert "not available" not in html
    assert "<h2>Live hunting</h2>" in html
    assert 'id="home-live-emergency-stop"' in html


def test_sonarr_season_packs_tab_states_manual_only_m11_scope_without_overclaiming(client):
    html = client.get("/sonarr").get_data(as_text=True)
    # M11 scope: manual-only, disabled by default, no automatic/broad search.
    assert "Manual only" in html
    assert "never sends broad SeasonSearch" in html
    assert "disabled by default" in html
    # M12 destructive-replacement/rollback capability must not be implied here.
    assert "rollback" not in html.lower()
    assert "quarantine" not in html.lower()
    assert "import verification" not in html.lower()


def test_no_page_claims_unimplemented_m12_capabilities(client):
    m12_only_phrases = (
        "automatic season-pack",
        "destructive replacement",
        "rollback",
        "quarantine",
        "import verification",
    )
    for path in PAGES:
        html = client.get(path).get_data(as_text=True).lower()
        for phrase in m12_only_phrases:
            assert phrase not in html, f"{path} unexpectedly references M12-only capability {phrase!r}"


def test_no_dropped_arr_names_appear_in_rendered_ui(client):
    dropped_names = ("radarr", "lidarr", "readarr", "whisparr", "eros")
    for path in PAGES:
        html = client.get(path).get_data(as_text=True).lower()
        for name in dropped_names:
            assert name not in html, f"{path} unexpectedly renders dropped Arr name {name!r}"


def test_old_bookmarks_redirect_into_the_sonarr_workspace(client):
    libraries_response = client.get("/libraries")
    assert libraries_response.status_code == 302
    assert libraries_response.headers["Location"] == "/sonarr"

    season_packs_response = client.get("/season-packs")
    assert season_packs_response.status_code == 302
    assert season_packs_response.headers["Location"] == "/sonarr?tab=season-packs"


def test_sidebar_navigation_has_exactly_the_four_primary_items(client):
    html = client.get("/").get_data(as_text=True)
    nav = html.split('<nav class="sidebar-nav"', 1)[1].split("</nav>", 1)[0]
    assert nav.count("<a ") == 4
    assert "Home</a>" in nav
    assert "Sonarr</a>" in nav
    assert "Activity</a>" in nav
    assert "Settings</a>" in nav
    assert "Season Packs" not in nav
    assert "Libraries" not in nav

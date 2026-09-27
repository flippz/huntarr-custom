"""Regression tests for informational/warning box copy across Managearr pages.

These lock in wording that reflects the currently implemented product
(through M11) so a stale "not available"/"not active" claim about a feature
that has since shipped (persistent Live controls, multi-dispatch pacing,
M10 missing/upgrade distinction, M11 manual season packs) cannot silently
return.
"""

PAGES = ("/", "/libraries", "/season-packs", "/activity", "/settings")


def test_preview_banner_does_not_claim_automated_hunting_is_categorically_inactive(client):
    for path in PAGES:
        html = client.get(path).get_data(as_text=True)
        assert "automated hunting is not active" not in html
        assert "Manual searches require preview and confirmation" in html
        assert "scheduled Live episode automation can send searches after explicit enablement" in html


def test_settings_no_longer_claims_live_automation_is_unavailable(client):
    html = client.get("/settings").get_data(as_text=True)
    assert "Live automation is not available" not in html
    assert "Simulation sends no Sonarr commands and remains read-only" in html
    assert "Live automation below can send EpisodeSearch commands after explicit enablement" in html
    # The Live automation controls this notice references must actually be present.
    assert "<h2>Live automation</h2>" in html
    assert 'id="live-mode-request"' in html


def test_library_scan_copy_describes_current_missing_and_upgrade_refresh(client):
    html = client.get("/libraries").get_data(as_text=True)
    assert "reads Sonarr library and wanted data" in html
    assert "refresh missing and upgrade candidates" in html
    assert "only reads series/episode data" not in html


def test_activity_copy_describes_automatic_and_manual_reconciliation(client):
    html = client.get("/activity").get_data(as_text=True)
    assert "Live dispatches are reconciled automatically" in html
    assert "manual reconciliation remains available" in html
    assert "Outcome reconciliation is manual" not in html


def test_episode_policy_points_to_separate_manual_season_packs(client):
    html = client.get("/settings").get_data(as_text=True)
    assert "This episode policy does not dispatch season packs" in html
    assert "manual Season Packs page" in html
    assert "No season packs, file deletion, or replacement" not in html


def test_overview_live_dispatch_card_is_not_described_as_unavailable(client):
    html = client.get("/").get_data(as_text=True)
    assert "not available" not in html
    assert "<h2>Live dispatch</h2>" in html
    assert 'id="ov-live-emergency-stop"' in html


def test_season_packs_page_states_manual_only_m11_scope_without_overclaiming(client):
    html = client.get("/season-packs").get_data(as_text=True)
    # M11 scope: manual-only, disabled by default, no automatic/broad search.
    assert "Manual only" in html
    assert "never sends broad SeasonSearch" in html
    assert "Defaults are off" in html
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

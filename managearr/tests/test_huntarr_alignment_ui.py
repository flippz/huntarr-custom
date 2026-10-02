"""Regression tests for the Huntarr-aligned information architecture:
sidebar navigation shell, theme toggle, Sonarr-only instance creation,
Home's hunting-first summary/empty state, and Activity's human-readable-
first audit table with raw IDs pushed into expandable detail rows.
"""

PAGES = ("/", "/sonarr", "/activity", "/settings")


def test_every_page_uses_the_sidebar_shell_and_theme_toggle(client):
    for path in PAGES:
        html = client.get(path).get_data(as_text=True)
        assert 'class="sidebar"' in html
        assert 'class="mobile-header"' in html
        assert 'id="nav-open"' in html
        assert 'id="theme-toggle"' in html
        assert "data-theme" in html


def test_theme_preference_is_persisted_client_side_not_server_side(client):
    html = client.get("/").get_data(as_text=True)
    assert "localStorage" in html
    assert "managearr-theme" in html


def test_sonarr_instances_tab_offers_no_generic_arr_type_choice(client):
    html = client.get("/sonarr").get_data(as_text=True)
    assert '<select id="lib-type"' not in html
    assert 'id="lib-type" value="sonarr"' in html
    assert "Add Sonarr instance" in html


def test_sonarr_workspace_has_three_tabs(client):
    html = client.get("/sonarr").get_data(as_text=True)
    assert 'data-tab="instances"' in html
    assert 'data-tab="hunting-policy"' in html
    assert 'data-tab="season-packs"' in html


def test_home_has_empty_state_guidance_and_hunting_first_sections(client):
    html = client.get("/").get_data(as_text=True)
    assert 'id="home-empty"' in html
    assert "Add a Sonarr instance" in html
    assert "Home requirements" not in html
    assert "<h2>Live hunting</h2>" in html
    assert "<h2>Recent activity</h2>" in html


def test_activity_audit_table_hides_raw_ids_behind_detail_toggle(client):
    html = client.get("/activity").get_data(as_text=True)
    audit_section = html.split('id="dispatch-audit"', 1)[1].split("</table>", 1)[0]
    assert "<th>ID</th>" not in audit_section
    assert "<th>Command</th>" not in audit_section
    assert 'data-action="toggle-detail"' in html
    assert 'class="detail-row"' in html or "detail-row" in html


def test_settings_has_advanced_system_disclosure_with_technical_details(client):
    html = client.get("/settings").get_data(as_text=True)
    assert '<details class="advanced-disclosure">' in html
    advanced = html.split('<details class="advanced-disclosure">', 1)[1]
    assert "Schema version" in advanced
    assert "Worker lease" in advanced
    assert "Policy digest" in advanced
    assert "Authorization generation" in advanced


def test_settings_routine_controls_appear_before_advanced_disclosure(client):
    html = client.get("/settings").get_data(as_text=True)
    scheduler_index = html.index("Simulation scheduler")
    advanced_index = html.index('<details class="advanced-disclosure">')
    assert scheduler_index < advanced_index


def test_hunting_policy_moved_out_of_settings_onto_sonarr_page(client):
    settings_html = client.get("/settings").get_data(as_text=True)
    sonarr_html = client.get("/sonarr").get_data(as_text=True)
    assert 'id="p-missing"' not in settings_html
    assert 'id="p-upgrades"' not in settings_html
    assert 'id="p-missing"' in sonarr_html
    assert 'id="p-upgrades"' in sonarr_html


def test_home_guides_first_time_live_enablement_and_refreshes_status(client):
    html = client.get("/").get_data(as_text=True)
    assert "home-live-enable-hint" in html
    assert "#live-automation" in html
    assert "refreshHomeLive" in html
    assert "setInterval(() => refreshHomeLive()" in html
    assert "home-live-resume').disabled = liveInfo.mode !== 'live'" in html
    assert "home-live-run-now').disabled = liveInfo.mode !== 'live'" in html


def test_sonarr_tabs_have_complete_aria_relationships(client):
    html = client.get("/sonarr").get_data(as_text=True)
    for name in ("instances", "hunting-policy", "season-packs", "slow-download"):
        assert f'id="tab-{name}"' in html
        assert f'aria-controls="panel-{name}"' in html
        assert f'id="panel-{name}"' in html
        assert f'aria-labelledby="tab-{name}"' in html
    assert html.count('role="tabpanel"') == 4
    assert "aria-selected" in html


def test_version_badge_is_consistent_across_breakpoints(client):
    html = client.get("/").get_data(as_text=True)
    assert "v1 preview" not in html
    assert html.count('<span class="status-badge"') == 2

"""Activity UI exposes a preview-first, explicit-confirmation dispatch flow."""


def test_activity_page_contains_confirmation_gated_dispatch_controls(client):
    response = client.get("/activity")
    assert response.status_code == 200
    html = response.get_data(as_text=True)

    assert 'id="preview-dispatch"' in html
    assert 'id="clear-dispatch-preview"' in html
    assert 'id="dispatch-confirm"' in html
    assert 'id="send-dispatch"' in html
    assert 'class="button-danger"' in html
    assert 'id="dispatch-audit"' in html
    assert "maximum 25" in html
    assert "confirm: true" in html


def test_activity_ui_clear_preview_is_local_and_send_uses_exact_confirmation(client):
    html = client.get("/activity").get_data(as_text=True)

    clear_handler = html.split(
        "document.getElementById('clear-dispatch-preview').addEventListener('click',", 1
    )[1].split(";", 1)[0]
    assert "clearPreview" in clear_handler
    assert "api(" not in clear_handler
    assert "body: {candidate_ids: previewCandidateIds, confirm: true}" in html
    assert "Any selection change clears the preview" in html


def test_global_preview_banner_no_longer_claims_no_search_can_be_sent(client):
    html = client.get("/activity").get_data(as_text=True)
    assert "Sonarr searches require an explicit preview and confirmation" in html
    assert "no searches are sent and nothing in Sonarr is changed" not in html

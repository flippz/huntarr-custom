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


def test_no_global_preview_banner_claims_no_search_can_be_sent(client):
    html = client.get("/activity").get_data(as_text=True)
    assert "preview-banner" not in html
    assert "Development Preview" not in html
    assert "no searches are sent and nothing in Sonarr is changed" not in html
    assert "automated hunting is not active" not in html


def test_activity_ui_exposes_read_only_reconciliation_and_evidence_timeline(client):
    html = client.get("/activity").get_data(as_text=True)
    assert "Outcome reconciliation is read-only and sends no search" in html
    assert "Live dispatches are reconciled automatically" in html
    assert "manual reconciliation remains available" in html
    assert "not proof that Sonarr grabbed, downloaded, or imported" in html
    assert 'id="outcome-detail"' in html
    assert 'data-action="reconcile"' in html
    assert "/reconcile`, {method: 'POST'}" in html
    assert "/outcomes`" in html
    assert "item.human_status" in html
    assert "status.label" in html
    assert "status.explanation" in html


def test_activity_ui_labels_missing_and_upgrade_candidate_kinds(client):
    html = client.get("/activity").get_data(as_text=True)
    assert "<th>Search type</th>" in html
    assert "<th>Kind</th>" in html
    assert "'Upgrade' : 'Missing'" in html
    assert "kind-pill kind-${escapeHtml(event.candidate_kind)}" in html
    assert "kind-pill kind-${escapeHtml(c.candidate_kind)}" in html


def test_sonarr_hunting_policy_explains_safe_individual_upgrade_search(client):
    html = client.get("/sonarr").get_data(as_text=True)
    assert "Hunt individual episode quality upgrades" in html
    assert "does not dispatch season packs" in html
    assert "Season packs tab" in html
    assert "roughly 20% of shared capacity is reserved for upgrades" in html

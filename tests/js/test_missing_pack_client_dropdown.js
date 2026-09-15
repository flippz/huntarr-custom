'use strict';
/**
 * Coverage for the strict missing-season-pack client dropdown fixes:
 *  - P1: a stale/disabled/mismatched configured client id must be preserved
 *    as its own selected "Unavailable" option, never silently dropped to
 *    Automatic by an unrelated save or a background fetch.
 *  - P2 (race): an older in-flight download-clients response must never
 *    overwrite options rendered for a newer protocol/request.
 *  - P2 (fetch error): a failed/erroring fetch must never touch the
 *    existing options/selection; only the help text may change.
 *
 * Run with: node tests/js/test_missing_pack_client_dropdown.js
 * Uses Node's built-in test runner (node:test) - no npm dependencies,
 * consistent with this repo having no existing JS test framework.
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const { createSandbox } = require('./helpers/load_settings_forms');

function flush() {
    // Let the queued microtasks (fetch .then chains) settle.
    return new Promise((resolve) => setTimeout(resolve, 10));
}

test('stale configured client id is preserved as its own selected option', async () => {
    const s = createSandbox();
    const { clientEl, protocolEl } = s.elements;
    clientEl.setAttribute('data-selected-id', '42');
    protocolEl.value = 'usenet';
    s.setFetchResponses([{
        ok: true,
        json: { success: true, clients: [{ id: 1, name: 'Other Client', protocol: 'usenet', enable: true }] },
    }]);

    s.SettingsForms.loadMissingPackDownloadClients('usenet');
    await flush();

    const options = clientEl.options();
    const staleOption = options.find(o => o.value === '42');
    assert.ok(staleOption, 'stale id 42 must still be present as an option');
    assert.equal(staleOption.selected, true, 'stale option must remain selected');
    assert.equal(clientEl.value, '42', 'select value must still be the configured id');
});

test('disabled configured client is preserved as its own selected option', async () => {
    const s = createSandbox();
    const { clientEl, protocolEl } = s.elements;
    clientEl.setAttribute('data-selected-id', '7');
    protocolEl.value = 'usenet';
    s.setFetchResponses([{
        ok: true,
        json: { success: true, clients: [{ id: 7, name: 'Decypharr Usenet', protocol: 'usenet', enable: false }] },
    }]);

    s.SettingsForms.loadMissingPackDownloadClients('usenet');
    await flush();

    assert.equal(clientEl.value, '7');
    const staleOption = clientEl.options().find(o => o.value === '7');
    assert.ok(staleOption.text.toLowerCase().includes('unavailable'));
});

test('protocol-mismatched configured client is preserved as its own selected option', async () => {
    const s = createSandbox();
    const { clientEl, protocolEl } = s.elements;
    clientEl.setAttribute('data-selected-id', '9');
    protocolEl.value = 'usenet';
    s.setFetchResponses([{
        ok: true,
        json: { success: true, clients: [{ id: 9, name: 'qBit', protocol: 'torrent', enable: true }] },
    }]);

    s.SettingsForms.loadMissingPackDownloadClients('usenet');
    await flush();

    assert.equal(clientEl.value, '9');
});

test('unrelated save after a background fetch does not clear a compatible configured client', async () => {
    const s = createSandbox();
    const { clientEl, protocolEl } = s.elements;
    clientEl.setAttribute('data-selected-id', '3');
    protocolEl.value = 'usenet';
    s.setFetchResponses([{
        ok: true,
        json: { success: true, clients: [{ id: 3, name: 'Decypharr Usenet', protocol: 'usenet', enable: true }] },
    }]);

    s.SettingsForms.loadMissingPackDownloadClients('usenet');
    await flush();

    // Simulate the save-time collection reading .value directly (matches
    // instance-editor.js's collect logic).
    assert.equal(clientEl.value, '3');
});

test('fetch error leaves existing options/selection untouched (fail closed, no weakening)', async () => {
    const s = createSandbox();
    const { clientEl, helpEl, protocolEl } = s.elements;
    // Pre-populate as if a prior successful load already selected client 5.
    clientEl.innerHTML = '<option value="">Automatic (Sonarr chooses)</option>'
        + '<option value="5" selected>Decypharr Usenet</option>';
    clientEl.setAttribute('data-selected-id', '5');
    protocolEl.value = 'usenet';

    s.setFetchResponses([{ ok: false, json: { success: false, message: 'Failed to fetch download clients from Sonarr' } }]);
    s.SettingsForms.loadMissingPackDownloadClients('usenet');
    await flush();

    assert.equal(clientEl.value, '5', 'selection must be unchanged after an upstream error');
    assert.equal(clientEl.options().length, 2, 'options list must be unchanged after an upstream error');
    assert.match(helpEl.textContent, /not changed/i);
});

test('network-level rejection (fetch throws) also leaves selection untouched', async () => {
    const s = createSandbox();
    const { clientEl, helpEl, protocolEl } = s.elements;
    clientEl.innerHTML = '<option value="">Automatic (Sonarr chooses)</option>'
        + '<option value="5" selected>Decypharr Usenet</option>';
    clientEl.setAttribute('data-selected-id', '5');
    protocolEl.value = 'usenet';

    s.setFetchResponses([{ reject: true, error: new Error('network down') }]);
    s.SettingsForms.loadMissingPackDownloadClients('usenet');
    await flush();

    assert.equal(clientEl.value, '5');
    assert.equal(clientEl.options().length, 2);
    assert.match(helpEl.textContent, /not changed/i);
});

test('race: older in-flight response for a superseded protocol never overwrites newer options', async () => {
    const s = createSandbox();
    const { clientEl, protocolEl } = s.elements;
    clientEl.setAttribute('data-selected-id', '');
    protocolEl.value = 'usenet';

    // First call (usenet) will resolve AFTER the second call (torrent) because
    // we control resolution order via the queue, simulating an out-of-order
    // network response to a rapid protocol flip.
    s.setFetchResponses([
        { ok: true, json: { success: true, clients: [{ id: 1, name: 'Usenet Client', protocol: 'usenet', enable: true }] } },
        { ok: true, json: { success: true, clients: [{ id: 2, name: 'Torrent Client', protocol: 'torrent', enable: true }] } },
    ]);

    s.SettingsForms.loadMissingPackDownloadClients('usenet');
    // User flips to torrent before the usenet response resolves.
    protocolEl.value = 'torrent';
    s.SettingsForms.loadMissingPackDownloadClients('torrent');
    await flush();

    const options = clientEl.options();
    assert.ok(options.some(o => o.text === 'Torrent Client'), 'newer (torrent) response must be rendered');
    assert.ok(!options.some(o => o.text === 'Usenet Client'), 'older (usenet) response must be discarded');
});

test('race: response for a request superseded by a later request with the same protocol is discarded', async () => {
    const s = createSandbox();
    const { clientEl, protocolEl } = s.elements;
    clientEl.setAttribute('data-selected-id', '');
    protocolEl.value = 'usenet';

    s.setFetchResponses([
        { ok: true, json: { success: true, clients: [{ id: 1, name: 'Stale Result', protocol: 'usenet', enable: true }] } },
        { ok: true, json: { success: true, clients: [{ id: 2, name: 'Fresh Result', protocol: 'usenet', enable: true }] } },
    ]);

    s.SettingsForms.loadMissingPackDownloadClients('usenet');
    s.SettingsForms.loadMissingPackDownloadClients('usenet');
    await flush();

    const options = clientEl.options();
    assert.ok(options.some(o => o.text === 'Fresh Result'));
    assert.ok(!options.some(o => o.text === 'Stale Result'), 'first (superseded) request result must not apply');
});

test('switching to Sonarr default resets the client selection intentionally', () => {
    const s = createSandbox();
    const { clientEl } = s.elements;
    clientEl.setAttribute('data-selected-id', '42');
    clientEl.innerHTML = '<option value="">Automatic (Sonarr chooses)</option>'
        + '<option value="42" selected>Unavailable/stale (id 42)</option>';

    s.SettingsForms.onMissingPackProtocolChange({ value: 'sonarr_default' });

    assert.equal(clientEl.disabled, true);
    assert.equal(clientEl.getAttribute('data-selected-id'), '');
    assert.equal(clientEl.value, '');
});

test('explicit user selection updates data-selected-id so it survives a later re-fetch', async () => {
    const s = createSandbox();
    const { clientEl, protocolEl } = s.elements;
    clientEl.setAttribute('data-selected-id', '42');
    protocolEl.value = 'usenet';
    s.setFetchResponses([{
        ok: true,
        json: { success: true, clients: [{ id: 1, name: 'A', protocol: 'usenet', enable: true }, { id: 2, name: 'B', protocol: 'usenet', enable: true }] },
    }]);
    s.SettingsForms.loadMissingPackDownloadClients('usenet');
    await flush();

    // User explicitly picks client 2 via the change handler wired in the
    // rendered HTML (this.setAttribute('data-selected-id', this.value)).
    clientEl.value = '2';
    clientEl.setAttribute('data-selected-id', clientEl.value);

    // A later reload/re-fetch must now track the user's new choice, not the
    // original stale id 42.
    s.setFetchResponses([{
        ok: true,
        json: { success: true, clients: [{ id: 1, name: 'A', protocol: 'usenet', enable: true }, { id: 2, name: 'B', protocol: 'usenet', enable: true }] },
    }]);
    s.SettingsForms.loadMissingPackDownloadClients('usenet');
    await flush();

    assert.equal(clientEl.value, '2');
});

test('missing URL/API key preserves existing selection and does not wipe options', () => {
    const s = createSandbox();
    const { clientEl, urlEl, helpEl, protocolEl } = s.elements;
    urlEl.value = '';
    clientEl.innerHTML = '<option value="">Automatic (Sonarr chooses)</option>'
        + '<option value="5" selected>Decypharr Usenet</option>';
    clientEl.setAttribute('data-selected-id', '5');
    protocolEl.value = 'usenet';

    s.SettingsForms.loadMissingPackDownloadClients('usenet');

    assert.equal(clientEl.value, '5');
    assert.match(helpEl.textContent, /Enter URL and API Key/i);
});

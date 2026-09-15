'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const { FakeElement, FakeDocument } = require('./dom_stub');

const CORE_JS_PATH = path.join(__dirname, '..', '..', '..', 'frontend', 'static', 'js',
    'modules', 'features', 'settings', 'core.js');

/**
 * Build a fresh sandbox with document/window stubs and a mockable
 * HuntarrUtils.fetchWithTimeout, load core.js into it, and return
 * { window, document, elements, setFetchResponse, fetchCalls }.
 *
 * elements pre-registers the specific DOM nodes the missing-pack routing
 * functions look up by id, matching the real editor's rendered markup.
 */
function createSandbox() {
    const document = new FakeDocument();

    const protocolEl = new FakeElement('editor-missing-pack-protocol');
    const clientEl = new FakeElement('editor-missing-pack-client');
    const helpEl = new FakeElement('editor-missing-pack-client-help');
    const urlEl = new FakeElement('editor-url');
    const keyEl = new FakeElement('editor-key');
    const modeEl = new FakeElement('editor-missing-mode');
    const routingGroup = new FakeElement('__routing_group__');

    document.registerElement('editor-missing-pack-protocol', protocolEl);
    document.registerElement('editor-missing-pack-client', clientEl);
    document.registerElement('editor-missing-pack-client-help', helpEl);
    document.registerElement('editor-url', urlEl);
    document.registerElement('editor-key', keyEl);
    document.registerElement('editor-missing-mode', modeEl);
    document.registerElement('__routing_group__', routingGroup);

    urlEl.value = 'http://sonarr.example';
    keyEl.value = 'test-api-key';

    let fetchQueue = [];
    let fetchCalls = [];

    function setFetchResponses(responses) {
        // responses: array of {ok, json} or {reject: true} consumed in order,
        // one per successive fetchWithTimeout call - lets tests control which
        // in-flight request resolves first to exercise the race guard.
        fetchQueue = responses.slice();
    }

    const HuntarrUtils = {
        fetchWithTimeout: function (url, options) {
            fetchCalls.push({ url, options, body: options && options.body ? JSON.parse(options.body) : null });
            const spec = fetchQueue.shift() || { ok: true, json: { success: true, clients: [] } };
            return new Promise((resolve, reject) => {
                // Resolve asynchronously (microtask) like a real fetch, but let
                // the test control ordering via the queue rather than real timing.
                Promise.resolve().then(() => {
                    if (spec.reject) {
                        reject(spec.error || new Error('network error'));
                        return;
                    }
                    resolve({
                        ok: spec.ok !== false,
                        json: () => Promise.resolve(spec.json),
                    });
                });
            });
        },
    };

    const windowObj = {};
    const sandbox = {
        window: windowObj,
        document,
        HuntarrUtils,
        console,
        setTimeout,
        clearTimeout,
        Promise,
    };
    vm.createContext(sandbox);
    const code = fs.readFileSync(CORE_JS_PATH, 'utf8');
    vm.runInContext(code, sandbox, { filename: CORE_JS_PATH });

    return {
        SettingsForms: sandbox.window.SettingsForms,
        document,
        elements: { protocolEl, clientEl, helpEl, urlEl, keyEl, modeEl, routingGroup },
        setFetchResponses,
        fetchCalls,
    };
}

module.exports = { createSandbox };

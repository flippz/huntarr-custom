'use strict';
/**
 * Minimal, dependency-free DOM stub sufficient to load and exercise
 * frontend/static/js/modules/features/settings/core.js's missing-pack
 * client/protocol dropdown logic in plain Node (no jsdom/npm required,
 * consistent with this repo having no existing JS test framework).
 *
 * Only implements the small surface actually touched by the functions under
 * test: getElementById/querySelector, element.value/innerHTML/disabled/
 * getAttribute/setAttribute/style.display, and a synchronous <option> parser
 * good enough to inspect rendered dropdown state.
 */

function parseOptions(html) {
    const options = [];
    const re = /<option\s+value="([^"]*)"\s*(selected)?\s*>([^<]*)<\/option>/g;
    let m;
    while ((m = re.exec(html)) !== null) {
        options.push({ value: m[1], selected: !!m[2], text: m[3] });
    }
    return options;
}

class FakeElement {
    constructor(id) {
        this.id = id;
        this._attrs = {};
        this._innerHTML = '';
        this.disabled = false;
        this.style = { display: '' };
    }

    get innerHTML() { return this._innerHTML; }
    set innerHTML(html) {
        this._innerHTML = html;
        // Selecting a <select>'s .value mimics the browser: the last option
        // marked selected="selected" wins; otherwise the first option.
        const options = parseOptions(html);
        const selected = options.find(o => o.selected) || options[0];
        this._value = selected ? selected.value : '';
    }

    get value() { return this._value !== undefined ? this._value : ''; }
    set value(v) { this._value = v; }

    get textContent() { return this._text || ''; }
    set textContent(v) { this._text = v; }

    getAttribute(name) {
        return Object.prototype.hasOwnProperty.call(this._attrs, name) ? this._attrs[name] : null;
    }

    setAttribute(name, value) {
        this._attrs[name] = String(value);
    }

    options() {
        return parseOptions(this._innerHTML);
    }
}

class FakeDocument {
    constructor() {
        this._byId = new Map();
        // core.js appends a one-off <style> tag at module load time (unrelated
        // to the logic under test); these no-ops just let that line execute.
        this.head = { appendChild: function () {} };
    }

    registerElement(id, el) {
        this._byId.set(id, el);
    }

    getElementById(id) {
        return this._byId.get(id) || null;
    }

    querySelector(selector) {
        // Only supports the single class selector this module actually uses.
        if (selector === '.editor-missing-pack-routing-group') {
            return this._byId.get('__routing_group__') || null;
        }
        return null;
    }

    createElement() {
        return new FakeElement('__anonymous__');
    }
}

module.exports = { FakeElement, FakeDocument, parseOptions };

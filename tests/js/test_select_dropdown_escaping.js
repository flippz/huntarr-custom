'use strict';
/**
 * Regression test for stored-XSS safety in the library/download-client
 * <select> dropdowns on the Sonarr page (managearr/app/web/templates/
 * sonarr.html: season-pack, slow-download-guard, and import-failure-policy
 * tabs). These dropdowns used to build their <option> list by string-
 * concatenating an operator-entered name (a library name, a download
 * client name) straight into innerHTML, unescaped - a stored-XSS path
 * distinct from (and missed by) the existing escapeHtml-disciplined queue/
 * activity tables covered by the other tests in this directory.
 *
 * The fix (managearr/app/web/static/app.js::setSelectOptions) builds
 * options via the DOM Option constructor and element.textContent
 * exclusively, never innerHTML. This extracts the *actual* function out of
 * the shipped source (not a hand-copied duplicate that could silently
 * drift from it) and proves a malicious name is never HTML-parsed.
 *
 * Run with: node tests/js/test_select_dropdown_escaping.js
 */
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const appJsPath = path.join(__dirname, '..', '..', 'managearr', 'app', 'web', 'static', 'app.js');
const source = fs.readFileSync(appJsPath, 'utf8');

const fnMatch = source.match(/function setSelectOptions\(select, items, labelOf, valueOf\) \{[\s\S]*?\n\}/);
assert(fnMatch, 'could not locate setSelectOptions() in app.js');

// A faithful-enough fake of the real DOM primitives setSelectOptions uses:
// the Option constructor never parses its arguments as HTML, and
// textContent/appendChild never route through innerHTML either.
class FakeOption {
  constructor(text, value) {
    this.text = text;
    this.label = text;
    this.value = value;
  }
}

class FakeSelect {
  constructor() {
    this._options = [];
  }
  set textContent(_v) {
    this._options = [];
  }
  appendChild(option) {
    this._options.push(option);
    return option;
  }
}

global.Option = FakeOption;
const setSelectOptions = new Function(`'use strict'; ${fnMatch[0]}; return setSelectOptions;`)();

const maliciousItems = [
  { id: 1, name: '<img src=x onerror=alert(1)>' },
  { id: 2, name: '<script>alert(2)</script>' },
];

const select = new FakeSelect();
setSelectOptions(select, maliciousItems, (x) => x.name, (x) => x.id);

assert.equal(select._options.length, 2, 'expected one <option> per item');
for (const [i, item] of maliciousItems.entries()) {
  const option = select._options[i];
  // The malicious string must survive verbatim as plain text data - never
  // re-parsed, never silently stripped - proving it went through
  // Option/textContent rather than any HTML-parsing path.
  assert.equal(option.text, item.name, 'option text must equal the raw name verbatim (plain-text property, not HTML)');
  assert.equal(option.value, String(item.id));
}

// A second call must fully replace the previous options (textContent = ''
// clears), never append on top of stale/malicious entries from a prior
// render.
setSelectOptions(select, [{ id: 3, name: 'Clean Library' }], (x) => x.name, (x) => x.id);
assert.equal(select._options.length, 1, 'stale options must be cleared before re-rendering');
assert.equal(select._options[0].text, 'Clean Library');

console.log('OK: library/download-client <select> dropdowns build options via Option/textContent, never innerHTML');

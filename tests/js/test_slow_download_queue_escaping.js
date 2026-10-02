'use strict';
/**
 * Regression test for the stored-XSS fix in the Sonarr page's slow-download
 * guard queue table (managearr/app/web/templates/sonarr.html,
 * loadSdgQueue()). Every value in a Sonarr queue record - especially
 * title/status - is attacker-influenceable (an attacker who controls a
 * release/download name controls what gets rendered) and must never reach
 * innerHTML unescaped.
 *
 * This extracts the *actual* row-rendering expression out of the shipped
 * template (not a hand-copied duplicate that could silently drift from it)
 * and executes it with a malicious record, asserting the output contains
 * no live HTML tags.
 */
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const sonarrHtmlPath = path.join(__dirname, '..', '..', 'managearr', 'app', 'web', 'templates', 'sonarr.html');
const html = fs.readFileSync(sonarrHtmlPath, 'utf8');

const rowExprPattern = new RegExp(
  "\\$\\('sdg-rows'\\)\\.innerHTML = rows\\.map\\(r => \\(([\\s\\S]*?)\\)\\)\\.join\\('" + "'" + "\\)"
);
const match = html.match(rowExprPattern);
assert(match, 'could not locate the slow-download queue row-rendering expression in sonarr.html');
const rowExprSource = match[1];

// Mirrors app.js's escapeHtml (document.createElement('div').textContent ->
// .innerHTML), which escapes exactly &, < and > - the characters that
// matter for breaking out of a text node.
function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

const buildRow = new Function('r', 'escapeHtml', `return (${rowExprSource});`);

const malicious = {
  title: '<img src=x onerror=alert(1)>',
  status: '<script>alert(2)</script>',
  classification: 'removal_pending',
  strike_count: 2,
  reason: '<svg onload=alert(3)>',
};

const rendered = buildRow(malicious, escapeHtml);

assert(!rendered.includes('<img'), `title was rendered as a live tag: ${rendered}`);
assert(!rendered.includes('<script>'), `status was rendered as a live tag: ${rendered}`);
assert(!rendered.includes('<svg'), `reason was rendered as a live tag: ${rendered}`);
assert(rendered.includes('&lt;img'), 'expected the title to be escaped into an entity');
assert(rendered.includes('&lt;script&gt;'), 'expected the status to be escaped into an entity');
assert(rendered.includes('&lt;svg'), 'expected the reason to be escaped into an entity');

// classification is a closed internal enum, not Sonarr-derived, but must
// still go through escapeHtml for defense in depth per the fix.
assert(rowExprSource.includes('escapeHtml(r.classification)'), 'classification must be escaped too');

console.log('OK: slow-download queue row rendering escapes Sonarr-derived title/status/reason');

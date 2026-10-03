'use strict';
/**
 * Regression test for stored-XSS safety in the Sonarr page's import-
 * failure reason policy section (managearr/app/web/templates/sonarr.html,
 * loadIfpQueue()). Title/status/matched-reasons/decision/decision_reason
 * and every unmatched message are Sonarr-derived - an attacker who
 * controls a release/download name or a crafted queue status message
 * controls what gets rendered - and must never reach innerHTML unescaped.
 *
 * This extracts the *actual* rendering expressions out of the shipped
 * template (not a hand-copied duplicate that could silently drift from
 * it) and executes them with malicious input, asserting the output
 * contains no live HTML tags.
 */
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const sonarrHtmlPath = path.join(__dirname, '..', '..', 'managearr', 'app', 'web', 'templates', 'sonarr.html');
const html = fs.readFileSync(sonarrHtmlPath, 'utf8');

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// --- Tracked import-failure items table --------------------------------

const rowExprPattern = new RegExp(
  "\\$\\('ifp-rows'\\)\\.innerHTML = rows\\.map\\(r => \\(([\\s\\S]*?)\\)\\)\\.join\\('" + "'" + "\\);"
);
const rowMatch = html.match(rowExprPattern);
assert(rowMatch, 'could not locate the import-failure row-rendering expression in sonarr.html');
const buildRow = new Function('r', 'escapeHtml', `return (${rowMatch[1]});`);

const maliciousRow = {
  title: '<img src=x onerror=alert(1)>',
  status: '<script>alert(2)</script>',
  matched_reasons: ['<svg onload=alert(3)>'],
  decision: 'leave',
  decision_reason: '<svg onload=alert(4)>',
};
const renderedRow = buildRow(maliciousRow, escapeHtml);
assert(!renderedRow.includes('<img'), `title was rendered as a live tag: ${renderedRow}`);
assert(!renderedRow.includes('<script>'), `status was rendered as a live tag: ${renderedRow}`);
assert(!renderedRow.includes('<svg'), `matched_reasons/decision_reason were rendered as a live tag: ${renderedRow}`);
assert(renderedRow.includes('&lt;img'), 'expected the title to be escaped into an entity');
assert(renderedRow.includes('&lt;script&gt;'), 'expected the status to be escaped into an entity');
assert(rowMatch[1].includes('escapeHtml(r.decision)'), 'decision must be escaped too (internal enum, defense in depth)');

// --- Observed unmatched messages list ------------------------------------

const unmatchedExprPattern = /unmatched\.map\(m => '<li>' \+ escapeHtml\(m\) \+ '<\/li>'\)\.join\(''\)/;
assert(
  unmatchedExprPattern.test(html.replace(/\s+/g, ' ')),
  'could not locate the unmatched-messages row-rendering expression in sonarr.html'
);
const maliciousMessage = '<img src=x onerror=alert(5)>';
const renderedMessage = '<li>' + escapeHtml(maliciousMessage) + '</li>';
assert(!renderedMessage.includes('<img'), `unmatched message was rendered as a live tag: ${renderedMessage}`);
assert(renderedMessage.includes('&lt;img'), 'expected the unmatched message to be escaped into an entity');

// --- Reason-group checkbox labels (server-controlled catalog, but still
// escaped for defense in depth - see loadReasonGroups()) -----------------

const groupExprPattern = /\$\('ifp-reason-groups'\)\.innerHTML = reasonGroups\.map\(group => \(([\s\S]*?)\)\)\.join\(''\);/;
const groupMatch = html.match(groupExprPattern);
assert(groupMatch, 'could not locate the reason-group rendering expression in sonarr.html');
const buildGroups = new Function('reasonGroups', 'escapeHtml', 'reasonCheckboxId', `
  return reasonGroups.map(group => (${groupMatch[1]})).join('');
`);
function reasonCheckboxId(key) { return 'ifp-reason-' + key.replace(/[^A-Za-z0-9]/g, '-'); }
const renderedGroups = buildGroups(
  [{ name: '<script>alert(6)</script>', reasons: ['<img src=x onerror=alert(7)>'] }],
  escapeHtml,
  reasonCheckboxId,
);
assert(!renderedGroups.includes('<script>alert(6)'), `group name was rendered as a live tag: ${renderedGroups}`);
assert(!renderedGroups.includes('<img src=x onerror'), `reason key was rendered as a live tag: ${renderedGroups}`);

console.log('OK: import-failure reason policy rendering escapes Sonarr-derived and catalog-derived values');

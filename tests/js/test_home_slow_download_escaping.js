'use strict';
/**
 * Regression test for the stored-XSS fix in Home's slow-download guard card
 * (managearr/app/web/templates/home.html, loadSlowDownloadCard()). Every
 * Sonarr-derived value rendered there - title, health/classification label,
 * reason - is attacker-influenceable and must never reach innerHTML
 * unescaped.
 *
 * This extracts the *actual* row-rendering template literal out of the
 * shipped template (not a hand-copied duplicate that could silently drift
 * from it) and executes it with a malicious record, asserting the output
 * contains no live HTML tags.
 */
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const homeHtmlPath = path.join(__dirname, '..', '..', 'managearr', 'app', 'web', 'templates', 'home.html');
const html = fs.readFileSync(homeHtmlPath, 'utf8');

const rowExprPattern = /tr\.innerHTML = `([\s\S]*?)`;/;
const match = html.match(rowExprPattern);
assert(match, 'could not locate the slow-download card row-rendering template literal in home.html');
const templateBody = match[1];

// Mirrors app.js's escapeHtml (document.createElement('div').textContent ->
// .innerHTML), which escapes exactly &, < and > - the characters that
// matter for breaking out of a text node.
function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

const buildRow = new Function(
  'item', 'progress', 'downloaded', 'speed', 'health', 'ageText', 'escapeHtml',
  `return \`${templateBody}\`;`
);

const malicious = {
  title: '<img src=x onerror=alert(1)>',
  reason: '<script>alert(2)</script>',
  strike_count: 2,
};
const maliciousHealth = '<svg onload=alert(3)>'; // simulates an unmapped classification value reaching `health` raw

const rendered = buildRow(malicious, '50%', '5 GiB / 5 GiB left', '1 MiB/s', maliciousHealth, () => '1h', escapeHtml);

assert(!rendered.includes('<img'), `title was rendered as a live tag: ${rendered}`);
assert(!rendered.includes('<svg'), `health was rendered as a live tag: ${rendered}`);
assert(!rendered.includes('<script>'), `reason was rendered as a live tag: ${rendered}`);
assert(rendered.includes('&lt;img'), 'expected the title to be escaped into an entity');
assert(rendered.includes('&lt;svg'), 'expected the health label to be escaped into an entity');
assert(rendered.includes('&lt;script&gt;'), 'expected the reason to be escaped into an entity');

console.log('OK: Home slow-download card row rendering escapes Sonarr-derived title/health/reason');

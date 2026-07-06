"""
Magnetarr scraping logic.

Fetches Reddit's Atom RSS feed (appending `.rss` to any listing/comments URL) —
Reddit blocks the unauthenticated `.json` API for most callers since its 2023
API changes, but its RSS endpoints remain openly accessible and require no
developer app/approval. Extracts magnet: URIs (including text-obfuscated ones,
since some communities mask links to dodge Reddit's spam filter) from post
bodies, dedupes them by BTIH info-hash, and stores them in the database for
the Torznab endpoint to serve.
"""

import re
import time
import hashlib
import html as html_module
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import requests

from src.primary.utils.logger import get_logger
from src.primary.utils.database import get_database
from src.primary.settings_manager import load_settings
from src.primary.apps.magnetarr.realdebrid import rd_add_and_select, rd_get_status, rd_delete_torrent

magnetarr_logger = get_logger("magnetarr")

USER_AGENT = "Mozilla/5.0 (compatible; Huntarr-Magnetarr/1.0; +https://github.com/plexguide/Huntarr)"

ATOM_NS = "http://www.w3.org/2005/Atom"

MAGNET_RE = re.compile(r'magnet:\?xt=urn:btih:[A-Za-z0-9]+[^\s"\'<>\\]*')
INFO_HASH_RE = re.compile(r'xt=urn:btih:([A-Za-z0-9]+)')
SIZE_RE = re.compile(r'(\d+(?:\.\d+)?)\s*(GB|MB|TB)', re.IGNORECASE)
HTML_TAG_RE = re.compile(r'<[^>]+>')

SIZE_MULTIPLIERS = {'MB': 1024 ** 2, 'GB': 1024 ** 3, 'TB': 1024 ** 4}

# Some communities mask links (e.g. "magnet (colon) ?xt=urn (colon) btih (colon) ...",
# "mega (dot) nz") to dodge Reddit's spam/domain filters. Undo the common patterns
# before scanning for magnet URIs so masked links are still caught.
_DEOBFUSCATE_PATTERNS = [
    (re.compile(r'\s*[\(\[]\s*dot\s*[\)\]]\s*', re.IGNORECASE), '.'),
    (re.compile(r'\s+dot\s+', re.IGNORECASE), '.'),
    (re.compile(r'\s*[\(\[]\s*colon\s*[\)\]]\s*', re.IGNORECASE), ':'),
    (re.compile(r'\s*[\(\[]\s*at\s*[\)\]]\s*', re.IGNORECASE), '@'),
]

# Simple in-memory session stats, reset on process restart
MAGNETARR_STATS = {
    'total_scans': 0,
    'total_found': 0,
    'total_errors': 0,
    'last_run_time': None,
}


def get_session_stats() -> Dict[str, Any]:
    return dict(MAGNETARR_STATS)


def reset_session_stats() -> None:
    MAGNETARR_STATS.update({
        'total_scans': 0,
        'total_found': 0,
        'total_errors': 0,
        'last_run_time': None,
    })


def extract_info_hash(magnet_uri: str) -> Optional[str]:
    """Pull the BTIH info-hash out of a magnet URI."""
    m = INFO_HASH_RE.search(magnet_uri)
    return m.group(1).lower() if m else None


def parse_size_from_title(title: str) -> int:
    """Best-effort size parse from a post title (e.g. '... [4.2GB]'). Returns bytes, 0 if unparseable."""
    m = SIZE_RE.search(title or '')
    if not m:
        return 0
    value = float(m.group(1))
    unit = m.group(2).upper()
    return int(value * SIZE_MULTIPLIERS.get(unit, 0))


def deobfuscate_text(text: str) -> str:
    """Undo common link-masking patterns (e.g. 'mega (dot) nz') so masked magnet
    links posted to dodge spam filters are still detected."""
    for pattern, replacement in _DEOBFUSCATE_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def find_magnets_in_text(text: str) -> List[str]:
    """Find magnet URIs in raw text, including common obfuscated forms."""
    if not text:
        return []
    found = list(MAGNET_RE.findall(text))
    deobfuscated = deobfuscate_text(text)
    if deobfuscated != text:
        for candidate in MAGNET_RE.findall(deobfuscated):
            if candidate not in found:
                found.append(candidate)
    return found


def _request_with_backoff(url: str, timeout: int = 15, max_retries: int = 3) -> Tuple[Optional[requests.Response], str]:
    """GET a URL with a descriptive User-Agent and exponential backoff on HTTP 429.
    Returns (response_or_none, error_message) — error_message is '' on success."""
    headers = {'User-Agent': USER_AGENT}
    delay = 2
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
        except requests.exceptions.RequestException as e:
            magnetarr_logger.warning(f"Request error fetching {url}: {e}")
            return None, f"Request error: {e}"

        if resp.status_code == 429:
            if attempt >= max_retries:
                return None, f"Rate limited (429) after {max_retries} retries"
            magnetarr_logger.debug(f"Rate limited fetching {url}, backing off {delay}s")
            time.sleep(delay)
            delay *= 2
            continue

        if resp.status_code == 403:
            return None, "Blocked by Reddit (HTTP 403)"

        if resp.status_code != 200:
            return None, f"Unexpected HTTP {resp.status_code} from Reddit"

        return resp, ""
    return None, "Exhausted retries"


def _to_reddit_path(url: str) -> str:
    """Strip the reddit.com host prefix from a configured source URL, leaving just the path."""
    path = url.rstrip('/')
    for prefix in ("https://www.reddit.com", "https://old.reddit.com", "https://reddit.com"):
        if path.startswith(prefix):
            return path[len(prefix):]
    return path


def _parse_atom_entries(xml_text: str) -> List[Dict[str, Any]]:
    """Parse a Reddit Atom RSS feed into a list of post dicts:
    {id, name (fullname), title, permalink, content_html}."""
    root = ET.fromstring(xml_text)
    posts = []
    for entry in root.findall(f'{{{ATOM_NS}}}entry'):
        entry_id = (entry.findtext(f'{{{ATOM_NS}}}id') or '').strip()  # e.g. "t3_abc123"
        title = (entry.findtext(f'{{{ATOM_NS}}}title') or 'Untitled').strip()
        content_el = entry.find(f'{{{ATOM_NS}}}content')
        content_html = content_el.text if content_el is not None and content_el.text else ''

        permalink = ''
        for link_el in entry.findall(f'{{{ATOM_NS}}}link'):
            href = link_el.get('href', '')
            if href:
                permalink = href
                break

        posts.append({
            'id': entry_id,
            'title': title,
            'permalink': permalink,
            'content_html': html_module.unescape(content_html),
        })
    return posts


def fetch_reddit_listing(url: str, after: str = None, limit: int = 100) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """Fetch a page of a Reddit listing (subreddit) via its Atom RSS feed.
    Returns (posts_or_none, error_message)."""
    path = _to_reddit_path(url)
    query = f"?limit={limit}"
    if after:
        query += f"&after={after}"

    resp, err = _request_with_backoff(f"https://www.reddit.com{path}/.rss{query}")
    if resp is None:
        return None, err
    try:
        return _parse_atom_entries(resp.text), ""
    except ET.ParseError as e:
        return None, f"Invalid RSS/XML response from Reddit: {e}"


def fetch_reddit_comments_text(permalink: str) -> Tuple[str, str]:
    """Fetch a post's comments feed as raw text (used only when no magnet is found
    in the post body). Returns (raw_text, error_message)."""
    resp, err = _request_with_backoff(f"https://www.reddit.com{_to_reddit_path(permalink)}/.rss")
    if resp is None:
        return '', err
    return resp.text, ""


def guess_category(title: str) -> str:
    """Best-effort category guess — RSS doesn't expose link flair, so this is a light heuristic."""
    return 'other'


def _hash_post_content(content_html: str) -> str:
    """Hash a post's raw body so we can detect when the uploader edits it
    (e.g. adds a new session to a 'live' torrent's Contains: list)."""
    return hashlib.sha256((content_html or '').encode('utf-8', errors='ignore')).hexdigest()


def matches_realdebrid_keywords(title: str, keywords_csv: str) -> bool:
    """True if the title contains at least one of the comma-separated keywords
    (case-insensitive). An empty/blank keyword list matches everything —
    sources are only filtered when keywords are explicitly configured."""
    keywords = [k.strip() for k in (keywords_csv or '').split(',') if k.strip()]
    if not keywords:
        return True
    title_lower = (title or '').lower()
    return any(k.lower() in title_lower for k in keywords)


def _submit_to_realdebrid(source_id: str, post: Dict[str, Any], magnet_uri: str, info_hash: str, title: str) -> None:
    """Send a newly discovered magnet to Real-Debrid and start tracking its post
    for content changes (see recheck_realdebrid_watched_posts)."""
    settings = load_settings("magnetarr")
    api_token = (settings.get("realdebrid_api_token") or "").strip()
    if not api_token:
        magnetarr_logger.warning("Real-Debrid auto-add is enabled for a source but no API token is configured")
        return

    db = get_database()
    torrent_id, err = rd_add_and_select(magnet_uri, api_token)
    if err:
        magnetarr_logger.warning(f"Real-Debrid submission failed for '{title}': {err}")

    db.upsert_realdebrid_submission({
        'info_hash': info_hash,
        'magnet_uri': magnet_uri,
        'title': title,
        'permalink': post.get('permalink', ''),
        'source_id': source_id,
        'rd_torrent_id': torrent_id or '',
        'content_hash': _hash_post_content(post.get('content_html', '')),
        'status': 'error' if err else 'submitted',
        'last_error': err,
        'active': True,
    })


def extract_magnets_from_post(post: Dict[str, Any]) -> Tuple[List[str], bool]:
    """Find magnet URIs in a post's body/content (including obfuscated ones).
    Falls back to fetching the comments feed if none found there.
    Returns (magnet_uris, fetched_comments) — callers use the second value to pace requests."""
    content_html = post.get('content_html') or ''
    content_text = HTML_TAG_RE.sub(' ', content_html)
    candidates = find_magnets_in_text(content_text)
    if candidates:
        return candidates, False

    permalink = post.get('permalink')
    if not permalink:
        return [], False
    comments_text, _err = fetch_reddit_comments_text(permalink)
    if not comments_text:
        return [], True
    return find_magnets_in_text(HTML_TAG_RE.sub(' ', html_module.unescape(comments_text))), True


def scan_source(source: Dict[str, Any]) -> Dict[str, int]:
    """Scan a single configured source for new magnets. Returns {'scanned': n, 'found': n}."""
    db = get_database()
    source_id = source['id']
    source_name = source.get('name', '')
    source_url = source.get('url', '')
    after = source.get('last_after_token') or None

    scanned = 0
    found = 0
    last_after = after
    last_error = ''

    try:
        posts, fetch_err = fetch_reddit_listing(source_url, after=after)
        if posts is None:
            last_error = fetch_err or 'Unknown fetch error'
            magnetarr_logger.warning(f"Scan failed for source {source_name} ({source_url}): {last_error}")
            db.record_magnetarr_event(source_id, 'errors')
            return {'scanned': 0, 'found': 0, 'error': last_error}

        for post in posts:
            scanned += 1
            magnets, fetched_comments = extract_magnets_from_post(post)
            for magnet_uri in magnets:
                info_hash = extract_info_hash(magnet_uri)
                if not info_hash:
                    continue
                title = post.get('title', 'Untitled')
                record = {
                    'info_hash': info_hash,
                    'title': title,
                    'magnet_uri': magnet_uri,
                    'source_id': source_id,
                    'source_name': source_name,
                    'source_url': source_url,
                    'category': guess_category(title),
                    'size_bytes': parse_size_from_title(title),
                }
                if db.insert_magnet_if_new(record):
                    found += 1
                    if source.get('realdebrid_auto_add') and matches_realdebrid_keywords(title, source.get('realdebrid_keywords', '')):
                        _submit_to_realdebrid(source_id, post, magnet_uri, info_hash, title)
                        time.sleep(0.5)  # pace RD calls so a big backfill scan doesn't burst past its rate limit
            if fetched_comments:
                time.sleep(1.1)  # pace the comments-fallback request we just made

        if posts:
            last_after = posts[-1].get('id') or ''
        db.record_magnetarr_event(source_id, 'scans')
        if found:
            db.record_magnetarr_event(source_id, 'found', found)
    except Exception as e:
        magnetarr_logger.error(f"Error scanning source {source_name} ({source_url}): {e}", exc_info=True)
        db.record_magnetarr_event(source_id, 'errors')
        last_error = str(e)
    finally:
        db.mark_source_scanned(source_id, last_after or '', last_error)

    result = {'scanned': scanned, 'found': found}
    if last_error:
        result['error'] = last_error
    return result


def run_magnetarr() -> None:
    """Scan all enabled sources whose interval has elapsed since their last scan."""
    db = get_database()
    MAGNETARR_STATS['last_run_time'] = datetime.utcnow().isoformat()

    sources = db.get_magnetarr_sources()
    now = datetime.utcnow()

    for source in sources:
        if not source.get('enabled', True):
            continue

        last_scanned = source.get('last_scanned_at')
        interval_minutes = source.get('interval_minutes', 20)
        if last_scanned:
            try:
                last_dt = datetime.fromisoformat(str(last_scanned).replace('Z', ''))
                elapsed_minutes = (now - last_dt).total_seconds() / 60
                if elapsed_minutes < interval_minutes:
                    continue
            except (ValueError, TypeError):
                pass  # malformed timestamp — treat as due

        try:
            result = scan_source(source)
            MAGNETARR_STATS['total_scans'] += 1
            MAGNETARR_STATS['total_found'] += result.get('found', 0)
        except Exception as e:
            magnetarr_logger.error(f"Unhandled error scanning source {source.get('name')}: {e}", exc_info=True)
            MAGNETARR_STATS['total_errors'] += 1


def fetch_single_post(permalink: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """Fetch a single post's own entry (title/content) via its permalink RSS feed —
    the same feed also carries the post's comments, but the post itself is always
    the first entry."""
    resp, err = _request_with_backoff(f"https://www.reddit.com{_to_reddit_path(permalink)}/.rss")
    if resp is None:
        return None, err
    try:
        posts = _parse_atom_entries(resp.text)
    except ET.ParseError as e:
        return None, f"Invalid RSS/XML response from Reddit: {e}"
    if not posts:
        return None, "Post not found (may have been deleted)"
    return posts[0], ""


def recheck_realdebrid_watched_posts() -> None:
    """Re-check posts we've already submitted to Real-Debrid for content changes
    (e.g. egortech adding a new session to a 'live' torrent's Contains: list).
    If the post's content changed, delete the old Real-Debrid entry and re-add
    the same magnet fresh so Real-Debrid re-fetches from the swarm. If nothing
    changed, no Real-Debrid calls are made."""
    settings = load_settings("magnetarr")
    api_token = (settings.get("realdebrid_api_token") or "").strip()
    if not api_token:
        return

    watch_days = settings.get("realdebrid_watch_days", 4)
    recheck_minutes = settings.get("realdebrid_recheck_minutes", 20)
    db = get_database()
    now = datetime.utcnow()

    for sub in db.get_active_realdebrid_submissions():
        info_hash = sub['info_hash']
        first_seen = sub.get('first_seen_at')
        try:
            first_seen_dt = datetime.fromisoformat(str(first_seen).replace('Z', ''))
            age_days = (now - first_seen_dt).total_seconds() / 86400
        except (ValueError, TypeError):
            age_days = 0

        if age_days > watch_days:
            db.mark_realdebrid_submission_status(info_hash, 'expired', active=False)
            continue

        last_checked = sub.get('last_checked_at')
        try:
            last_checked_dt = datetime.fromisoformat(str(last_checked).replace('Z', ''))
            if (now - last_checked_dt).total_seconds() / 60 < recheck_minutes:
                continue
        except (ValueError, TypeError):
            pass  # never checked — go ahead

        post, err = fetch_single_post(sub['permalink'])
        if post is None:
            magnetarr_logger.debug(f"Real-Debrid recheck: couldn't refetch post for '{sub.get('title')}': {err}")
            continue

        new_hash = _hash_post_content(post.get('content_html', ''))
        if new_hash == sub.get('content_hash'):
            # Nothing changed — just note we checked, without touching Real-Debrid at all.
            db.mark_realdebrid_submission_status(info_hash, sub.get('status', 'submitted'), active=True)
            status, _ = rd_get_status(sub['rd_torrent_id'], api_token) if sub.get('rd_torrent_id') else (None, '')
            if status == 'downloaded':
                db.mark_realdebrid_submission_status(info_hash, 'downloaded', active=False)
            continue

        # Post content changed — force Real-Debrid to re-fetch the same magnet.
        magnetarr_logger.info(f"Post content changed for '{sub.get('title')}', re-adding to Real-Debrid")
        del_err = rd_delete_torrent(sub.get('rd_torrent_id', ''), api_token)
        if del_err:
            magnetarr_logger.debug(f"Real-Debrid delete before re-add: {del_err}")

        torrent_id, add_err = rd_add_and_select(sub['magnet_uri'], api_token)
        db.upsert_realdebrid_submission({
            'info_hash': info_hash,
            'magnet_uri': sub['magnet_uri'],
            'title': sub.get('title', ''),
            'permalink': sub['permalink'],
            'source_id': sub.get('source_id', ''),
            'rd_torrent_id': torrent_id or '',
            'content_hash': new_hash,
            'status': 'error' if add_err else 'submitted',
            'last_error': add_err,
            'active': True,
        })

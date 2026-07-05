"""
Magnetarr scraping logic.

Fetches Reddit listing JSON (no HTML parsing needed — appending `.json` to any
reddit listing/comments URL returns structured data), extracts magnet: URIs
from post bodies and linked URLs, dedupes them by BTIH info-hash, and stores
them in the database for the Torznab endpoint to serve.
"""

import re
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import requests

from src.primary.utils.logger import get_logger
from src.primary.utils.database import get_database
from src.primary.settings_manager import load_settings

magnetarr_logger = get_logger("magnetarr")

USER_AGENT = "Mozilla/5.0 (compatible; Huntarr-Magnetarr/1.0; +https://github.com/plexguide/Huntarr)"

MAGNET_RE = re.compile(r'magnet:\?xt=urn:btih:[A-Za-z0-9]+[^\s"\'<>\\]*')
INFO_HASH_RE = re.compile(r'xt=urn:btih:([A-Za-z0-9]+)')
SIZE_RE = re.compile(r'(\d+(?:\.\d+)?)\s*(GB|MB|TB)', re.IGNORECASE)

SIZE_MULTIPLIERS = {'MB': 1024 ** 2, 'GB': 1024 ** 3, 'TB': 1024 ** 4}

# Reddit now blocks unauthenticated .json scraping from most IPs (post-2023 API
# changes) regardless of headers, so we go through the official OAuth API
# (oauth.reddit.com) whenever credentials are configured. In-memory token cache
# — one process-wide app-only token, refreshed on expiry or a 401.
_REDDIT_TOKEN_CACHE = {'access_token': None, 'expires_at': 0}

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


def guess_category(post_data: Dict[str, Any]) -> str:
    """Best-effort category guess from link flair; defaults to 'other'."""
    flair = (post_data.get('link_flair_text') or '').strip().lower()
    if not flair:
        return 'other'
    return flair


def _get_reddit_credentials() -> Tuple[str, str]:
    settings = load_settings("magnetarr")
    return (settings.get("reddit_client_id", "") or "").strip(), \
           (settings.get("reddit_client_secret", "") or "").strip()


def _get_reddit_oauth_token(force_refresh: bool = False) -> Optional[str]:
    """Get a cached app-only OAuth token, fetching/refreshing one if needed.
    Returns None if no credentials are configured or the token request fails."""
    client_id, client_secret = _get_reddit_credentials()
    if not client_id or not client_secret:
        return None

    if not force_refresh and _REDDIT_TOKEN_CACHE['access_token'] and time.time() < _REDDIT_TOKEN_CACHE['expires_at']:
        return _REDDIT_TOKEN_CACHE['access_token']

    try:
        resp = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(client_id, client_secret),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
    except requests.exceptions.RequestException as e:
        magnetarr_logger.warning(f"Reddit OAuth token request failed: {e}")
        return None

    if resp.status_code != 200:
        magnetarr_logger.warning(f"Reddit OAuth token request returned HTTP {resp.status_code}: {resp.text[:200]}")
        return None

    try:
        payload = resp.json()
        token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
    except (ValueError, KeyError):
        magnetarr_logger.warning("Reddit OAuth token response was not valid JSON")
        return None

    _REDDIT_TOKEN_CACHE['access_token'] = token
    _REDDIT_TOKEN_CACHE['expires_at'] = time.time() + expires_in - 60  # refresh a minute early
    return token


def _request_with_backoff(url: str, timeout: int = 15, max_retries: int = 3,
                           use_oauth: bool = False) -> Tuple[Optional[requests.Response], str]:
    """GET a URL with a descriptive User-Agent and exponential backoff on HTTP 429.
    Returns (response_or_none, error_message) — error_message is '' on success."""
    delay = 2
    token = _get_reddit_oauth_token() if use_oauth else None
    for attempt in range(max_retries + 1):
        headers = {'User-Agent': USER_AGENT}
        if token:
            headers['Authorization'] = f'Bearer {token}'
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

        if resp.status_code == 401 and use_oauth and token:
            # Token may have been revoked/expired early — refresh once and retry.
            token = _get_reddit_oauth_token(force_refresh=True)
            if token:
                continue
            return None, "Reddit OAuth token invalid/expired and refresh failed"

        if resp.status_code == 403:
            hint = "" if use_oauth else " - configure a Reddit client ID/secret in Magnetarr settings to fix this"
            return None, f"Blocked by Reddit (HTTP 403){hint}"

        if resp.status_code != 200:
            return None, f"Unexpected HTTP {resp.status_code} from Reddit"

        return resp, ""
    return None, "Exhausted retries"


def fetch_reddit_listing(url: str, after: str = None, limit: int = 100) -> Tuple[Optional[Dict[str, Any]], str]:
    """Fetch a page of a Reddit listing (subreddit) as JSON. Returns (data_or_none, error_message)."""
    client_id, client_secret = _get_reddit_credentials()
    use_oauth = bool(client_id and client_secret)
    base_host = "https://oauth.reddit.com" if use_oauth else "https://www.reddit.com"

    parsed_path = url.rstrip('/')
    for prefix in ("https://www.reddit.com", "https://old.reddit.com", "https://reddit.com"):
        if parsed_path.startswith(prefix):
            parsed_path = parsed_path[len(prefix):]
            break
    query = f"?limit={limit}"
    if after:
        query += f"&after={after}"

    resp, err = _request_with_backoff(f"{base_host}{parsed_path}/.json{query}", use_oauth=use_oauth)
    if resp is None:
        return None, err
    try:
        return resp.json(), ""
    except ValueError:
        return None, "Invalid JSON response from Reddit"


def fetch_reddit_comments(permalink: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """Fetch a post's comments JSON (used only when no magnet is found in the post body/link)."""
    client_id, client_secret = _get_reddit_credentials()
    use_oauth = bool(client_id and client_secret)
    base_host = "https://oauth.reddit.com" if use_oauth else "https://www.reddit.com"

    resp, err = _request_with_backoff(f"{base_host}{permalink.rstrip('/')}.json", use_oauth=use_oauth)
    if resp is None:
        return None, err
    try:
        return resp.json(), ""
    except ValueError:
        return None, "Invalid JSON response from Reddit"


def extract_magnets_from_post(post_data: Dict[str, Any]) -> Tuple[List[str], bool]:
    """Find magnet URIs in a post's selftext or linked URL.
    Falls back to fetching comments if none found there.
    Returns (magnet_uris, fetched_comments) — callers use the second value to pace requests."""
    candidates = []
    selftext = post_data.get('selftext') or ''
    candidates.extend(MAGNET_RE.findall(selftext))

    link_url = post_data.get('url_overridden_by_dest') or post_data.get('url') or ''
    if link_url.startswith('magnet:'):
        candidates.extend(MAGNET_RE.findall(link_url))

    if candidates:
        return candidates, False

    permalink = post_data.get('permalink')
    if not permalink:
        return [], False
    comments_data, _err = fetch_reddit_comments(permalink)
    if not comments_data:
        return [], True
    try:
        comments_text = str(comments_data)
        return MAGNET_RE.findall(comments_text), True
    except Exception:
        return [], True


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
        listing, fetch_err = fetch_reddit_listing(source_url, after=after)
        if not listing:
            last_error = fetch_err or 'Unknown fetch error'
            magnetarr_logger.warning(f"Scan failed for source {source_name} ({source_url}): {last_error}")
            db.record_magnetarr_event(source_id, 'errors')
            return {'scanned': 0, 'found': 0, 'error': last_error}

        children = listing.get('data', {}).get('children', [])
        for child in children:
            post_data = child.get('data', {})
            scanned += 1
            magnets, fetched_comments = extract_magnets_from_post(post_data)
            for magnet_uri in magnets:
                info_hash = extract_info_hash(magnet_uri)
                if not info_hash:
                    continue
                title = post_data.get('title', 'Untitled')
                record = {
                    'info_hash': info_hash,
                    'title': title,
                    'magnet_uri': magnet_uri,
                    'source_id': source_id,
                    'source_name': source_name,
                    'source_url': source_url,
                    'category': guess_category(post_data),
                    'size_bytes': parse_size_from_title(title),
                }
                if db.insert_magnet_if_new(record):
                    found += 1
            if fetched_comments:
                time.sleep(1.1)  # pace the comments-fallback request we just made

        last_after = listing.get('data', {}).get('after') or ''
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

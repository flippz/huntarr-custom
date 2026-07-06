"""Magnetarr — URL/subreddit magnet scraper storage and Torznab search backing."""
import json
import sqlite3
import uuid
from typing import Dict, List, Any, Optional


class MagnetarrMixin:
    """Scrape sources, discovered magnets, and per-source stats for Magnetarr."""

    # ── Sources ───────────────────────────────────────────────────────

    def add_magnetarr_source(self, data: Dict[str, Any]) -> str:
        """Add a new scrape source. Returns the source id."""
        src_id = data.get('id') or str(uuid.uuid4())
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO magnetarr_sources
                    (id, name, url, source_type, enabled, interval_minutes)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                src_id,
                data.get('name', 'Unnamed'),
                data.get('url', ''),
                data.get('source_type', 'reddit'),
                1 if data.get('enabled', True) else 0,
                max(5, int(data.get('interval_minutes', 20) or 20)),
            ))
            conn.commit()
        return src_id

    def get_magnetarr_sources(self) -> List[Dict[str, Any]]:
        """Return all scrape sources."""
        with self.get_connection() as conn:
            with self._use_row_factory(conn) as c:
                rows = c.execute('SELECT * FROM magnetarr_sources ORDER BY name ASC').fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d['enabled'] = bool(d.get('enabled', 1))
                out.append(d)
            return out

    def get_magnetarr_source(self, src_id: str) -> Optional[Dict[str, Any]]:
        """Return a single scrape source by id."""
        with self.get_connection() as conn:
            with self._use_row_factory(conn) as c:
                row = c.execute('SELECT * FROM magnetarr_sources WHERE id = ?', (src_id,)).fetchone()
            if not row:
                return None
            d = dict(row)
            d['enabled'] = bool(d.get('enabled', 1))
            return d

    def update_magnetarr_source(self, src_id: str, updates: Dict[str, Any]) -> bool:
        """Update fields on a scrape source. Returns True if a row was updated."""
        allowed = ['name', 'url', 'source_type', 'enabled', 'interval_minutes',
                   'last_scanned_at', 'last_after_token', 'last_error']
        sets = []
        vals = []
        for k in allowed:
            if k in updates:
                v = updates[k]
                if k == 'enabled':
                    v = 1 if v else 0
                if k == 'interval_minutes':
                    try:
                        v = max(5, int(v))
                    except (TypeError, ValueError):
                        continue
                sets.append(f'{k} = ?')
                vals.append(v)
        if not sets:
            return False
        sets.append('updated_at = CURRENT_TIMESTAMP')
        vals.append(src_id)
        with self.get_connection() as conn:
            cur = conn.execute(f'UPDATE magnetarr_sources SET {", ".join(sets)} WHERE id = ?', vals)
            conn.commit()
            return cur.rowcount > 0

    def delete_magnetarr_source(self, src_id: str) -> bool:
        """Delete a scrape source and its stats."""
        with self.get_connection() as conn:
            conn.execute('DELETE FROM magnetarr_stats WHERE source_id = ?', (src_id,))
            cur = conn.execute('DELETE FROM magnetarr_sources WHERE id = ?', (src_id,))
            conn.commit()
            return cur.rowcount > 0

    def mark_source_scanned(self, src_id: str, after_token: str = '', last_error: str = '') -> None:
        """Update last_scanned_at (now), the pagination cursor, and last error (if any) for a source."""
        with self.get_connection() as conn:
            conn.execute('''
                UPDATE magnetarr_sources
                SET last_scanned_at = CURRENT_TIMESTAMP, last_after_token = ?, last_error = ?
                WHERE id = ?
            ''', (after_token or '', last_error or '', src_id))
            conn.commit()

    # ── Magnets ───────────────────────────────────────────────────────

    def insert_magnet_if_new(self, magnet: Dict[str, Any]) -> bool:
        """Insert a discovered magnet if its info_hash isn't already known.
        Returns True if it was newly inserted."""
        with self.get_connection() as conn:
            cur = conn.execute('''
                INSERT INTO magnetarr_magnets
                    (info_hash, title, magnet_uri, source_id, source_name, source_url, category, size_bytes, seeders)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(info_hash) DO NOTHING
            ''', (
                magnet['info_hash'].lower(),
                magnet.get('title', ''),
                magnet['magnet_uri'],
                magnet.get('source_id', ''),
                magnet.get('source_name', ''),
                magnet.get('source_url', ''),
                magnet.get('category', 'other'),
                magnet.get('size_bytes', 0),
                magnet.get('seeders', 1),
            ))
            conn.commit()
            return cur.rowcount > 0

    def backfill_magnetarr_seeders(self, seeders: int = 1) -> int:
        """Set seeders to a nominal value on any existing rows missing it
        (e.g. rows inserted before the seeders column existed). Returns rows updated."""
        with self.get_connection() as conn:
            cur = conn.execute(
                'UPDATE magnetarr_magnets SET seeders = ? WHERE seeders IS NULL OR seeders <= 0',
                (seeders,)
            )
            conn.commit()
            return cur.rowcount

    def get_recent_magnets(self, source_id: str = None, category: str = None,
                            q: str = None, page: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """Paginated recent discoveries, newest first."""
        conditions = []
        params = []
        if source_id:
            conditions.append('source_id = ?')
            params.append(source_id)
        if category:
            conditions.append('category = ?')
            params.append(category)
        if q:
            conditions.append('title LIKE ?')
            params.append(f'%{q}%')
        where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
        offset = (max(1, page) - 1) * page_size
        with self.get_connection() as conn:
            with self._use_row_factory(conn) as c:
                count_row = c.execute(f'SELECT COUNT(*) as cnt FROM magnetarr_magnets {where}', params).fetchone()
                total = count_row['cnt'] if count_row else 0
                rows = c.execute(
                    f'SELECT * FROM magnetarr_magnets {where} ORDER BY discovered_at DESC LIMIT ? OFFSET ?',
                    params + [page_size, offset]
                ).fetchall()
            return {
                'items': [dict(r) for r in rows],
                'total': total,
                'page': page,
                'page_size': page_size,
                'total_pages': max(1, (total + page_size - 1) // page_size),
            }

    def search_magnets(self, q: str = '', category: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Free-text title search over discovered magnets, for the Torznab endpoint."""
        conditions = []
        params = []
        if q:
            conditions.append('title LIKE ?')
            params.append(f'%{q}%')
        if category:
            conditions.append('category = ?')
            params.append(category)
        where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
        with self.get_connection() as conn:
            with self._use_row_factory(conn) as c:
                rows = c.execute(
                    f'SELECT * FROM magnetarr_magnets {where} ORDER BY discovered_at DESC LIMIT ?',
                    params + [limit]
                ).fetchall()
            return [dict(r) for r in rows]

    # ── Stats ─────────────────────────────────────────────────────────

    def record_magnetarr_event(self, source_id: str, stat_type: str, increment: float = 1) -> None:
        """Increment a per-source counter (e.g. 'scans', 'found', 'errors')."""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO magnetarr_stats (source_id, stat_type, stat_value)
                VALUES (?, ?, ?)
                ON CONFLICT(source_id, stat_type) DO UPDATE SET
                    stat_value = stat_value + excluded.stat_value,
                    recorded_at = CURRENT_TIMESTAMP
            ''', (source_id, stat_type, increment))
            conn.commit()

    def get_magnetarr_stats(self, source_id: str = None) -> List[Dict[str, Any]]:
        """Get Magnetarr stats, optionally filtered by source_id."""
        with self.get_connection() as conn:
            with self._use_row_factory(conn) as c:
                if source_id:
                    rows = c.execute('SELECT * FROM magnetarr_stats WHERE source_id = ?', (source_id,)).fetchall()
                else:
                    rows = c.execute('SELECT * FROM magnetarr_stats ORDER BY source_id').fetchall()
            return [dict(r) for r in rows]

    def get_magnetarr_total_magnets(self) -> int:
        """Total number of distinct magnets discovered across all sources."""
        with self.get_connection() as conn:
            row = conn.execute('SELECT COUNT(*) FROM magnetarr_magnets').fetchone()
            return row[0] if row else 0

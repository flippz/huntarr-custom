"""
SQLite Database Manager for Huntarr
Replaces all JSON file operations with SQLite database for better performance and reliability.
Handles both app configurations, general settings, and stateful management data.
"""

import os
import json
import sqlite3
import threading
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
import logging
import time
import shutil

logger = logging.getLogger(__name__)

from src.primary.utils.db_mixins import ConfigMixin, StateMixin, UsersMixin, RequestarrMixin, ExtrasMixin, MagnetarrMixin

class _SerializedConnection:
    """Thin wrapper around a thread-local sqlite3.Connection that serializes ALL
    access through a single process-wide lock.

    Why: Huntarr runs 16+ independent app background loops plus a ~32-thread web
    server, each historically opening its own thread-local connection and writing
    to the same SQLite file with no coordination beyond SQLite's own busy_timeout
    retry. That's far more concurrent write pressure than a single-purpose app
    (Sonarr/Radarr/Prowlarr) ever generates against its own database — those apps
    serialize writes through an internal command pipeline by design, which is the
    standard safe way to use SQLite under any real concurrency. More concurrent
    writers means SQLite's WAL auto-checkpoint (the operation that rewrites the
    actual bytes of the main .db file — the only part of a WAL-mode write that
    touches the main file directly) fires far more often. Every extra checkpoint
    is one more moment where an external interruption (host freeze, OOM, container
    restart) can catch the file mid-write and produce the "file is not a database"
    corruption Huntarr has been hitting. Serializing here removes that exposure
    without changing the database engine, the schema, or any call site's SQL —
    every one of the ~177 `with self.get_connection() as conn:` sites across the
    codebase is covered automatically since they all go through get_connection().

    Uses a re-entrant lock (RLock) because some methods call other self.*
    methods that also acquire a connection from the same thread — a plain Lock
    would deadlock a thread against itself in that case.
    """
    _write_lock = threading.RLock()

    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        self._write_lock.acquire()
        try:
            self._conn.__enter__()
        except Exception:
            self._write_lock.release()
            raise
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            return self._conn.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self._write_lock.release()

    def __getattr__(self, name):
        return getattr(self._conn, name)


class HuntarrDatabase(ConfigMixin, StateMixin, UsersMixin, RequestarrMixin, ExtrasMixin, MagnetarrMixin):
    """Database manager for all Huntarr configurations and settings"""

    # Class-level corruption recovery lock — ensures only one thread recovers at a time
    _corruption_lock = threading.Lock()
    _corruption_recovering = False
    _corruption_recovered_at = 0  # timestamp of last recovery
    
    def __init__(self):
        self._thread_local = threading.local()
        self.db_path = self._get_database_path()
        self.ensure_database_exists()
    
    @staticmethod
    def _use_row_factory(conn):
        """Context manager that temporarily sets row_factory = sqlite3.Row on a connection,
        then resets it to None on exit. Prevents row_factory leaking to other callers
        sharing the same thread-local connection."""
        import contextlib
        @contextlib.contextmanager
        def _ctx():
            old = conn.row_factory
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.row_factory = old
        return _ctx()

    def _configure_connection(self, conn):
        """Configure SQLite connection with NAS-safe settings.
        
        Memory note: cache_size is PER CONNECTION and Huntarr uses thread-local
        connections.  With 32 Waitress threads + background threads (~42 total),
        the old 16 MB setting meant 42 × 16 MB ≈ 672 MB of RAM just for SQLite
        page cache.  Reduced to 2 MB (still generous for a small config DB).
        
        NAS safety: mmap_size is disabled (set to 0) because memory-mapped I/O
        is unreliable on network filesystems (Unraid, Synology, NFS, CIFS).
        This is a known cause of "database disk image is malformed" errors.
        """
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA journal_mode = WAL')
        # FULL (not NORMAL) — with writes now serialized through _SerializedConnection,
        # the write volume this trades against is much lower than before, and FULL gives
        # stronger protection against exactly the kind of interrupted-write corruption
        # (host freeze, OOM, container restart) Huntarr has been hitting.
        conn.execute('PRAGMA synchronous = FULL')
        conn.execute('PRAGMA cache_size = -2000')   # 2 MB per connection (was 16 MB — caused 600 MB+ RAM usage)
        conn.execute('PRAGMA temp_store = MEMORY')
        conn.execute('PRAGMA mmap_size = 0')         # DISABLED — mmap causes corruption on NAS/network storage
        conn.execute('PRAGMA wal_autocheckpoint = 1000')
        conn.execute('PRAGMA busy_timeout = 30000')
        conn.execute('PRAGMA auto_vacuum = INCREMENTAL')
    
    def get_connection(self):
        """Get a configured SQLite connection with Synology NAS compatibility.
        
        Uses thread-local connection caching to avoid repeatedly creating new
        connections and running PRAGMA configuration on every database operation.
        Falls back to retry logic with WAL recovery before resorting to corruption handling.
        This prevents false-positive corruption detection after unclean shutdowns.
        """
        # Try to reuse thread-local cached connection (avoids ~5-10ms overhead per operation)
        cached_conn = getattr(self._thread_local, 'conn', None)
        if cached_conn is not None:
            try:
                # Use a query that touches sqlite_master to detect corruption
                # (SELECT 1 succeeds even on a malformed database)
                cached_conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1").fetchone()
                # Reset row_factory to prevent leaking Row mode across callers
                cached_conn.row_factory = None
                return _SerializedConnection(cached_conn)
            except Exception as e:
                # Connection is stale/broken — check if it's corruption
                if self._is_corruption_error(e):
                    logger.warning(f"Corruption detected on cached connection: {e}")
                    try:
                        cached_conn.close()
                    except Exception:
                        pass
                    self._thread_local.conn = None
                    self._trigger_corruption_recovery(error=e)
                    # Fall through to create a new connection below
                else:
                    try:
                        cached_conn.close()
                    except Exception:
                        pass
                    self._thread_local.conn = None
        
        # No valid cached connection — create a new one with retry logic
        max_retries = 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self.db_path, timeout=30)
                self._configure_connection(conn)
                # Test the connection by running a simple query
                conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1").fetchone()
                # Cache for this thread
                self._thread_local.conn = conn
                return _SerializedConnection(conn)
            except (sqlite3.DatabaseError, sqlite3.OperationalError) as e:
                last_error = e
                error_str = str(e).lower()
                
                if "file is not a database" in error_str or "database disk image is malformed" in error_str:
                    if attempt == 0:
                        # First attempt: try WAL recovery before anything destructive
                        logger.warning(f"Database error on attempt {attempt + 1}: {e}. Attempting WAL recovery...")
                        if self._attempt_wal_recovery():
                            logger.info("WAL recovery succeeded, retrying connection")
                            continue
                        logger.warning("WAL recovery did not help, will retry...")
                        time.sleep(1)
                        continue
                    elif attempt == 1:
                        # Second attempt: try a simple reconnect after a brief wait
                        logger.warning(f"Database error on attempt {attempt + 1}: {e}. Waiting and retrying...")
                        time.sleep(2)
                        continue
                    else:
                        # Final attempt: corruption is real, handle it
                        logger.error(f"Database corruption confirmed after {max_retries} attempts: {e}")
                        self._handle_database_corruption()
                        # Invalidate any cached connections across threads
                        self._thread_local.conn = None
                        # Try connecting again after recovery
                        conn = sqlite3.connect(self.db_path, timeout=30)
                        self._configure_connection(conn)
                        self._thread_local.conn = conn
                        return _SerializedConnection(conn)
                elif "database is locked" in error_str:
                    logger.warning(f"Database locked on attempt {attempt + 1}, waiting...")
                    time.sleep(2)
                    continue
                elif self._is_transient_io_error(e):
                    if attempt < max_retries - 1:
                        backoff = 0.5 * (2 ** attempt)
                        logger.warning(
                            f"Transient disk I/O error on attempt {attempt + 1}/{max_retries}, "
                            f"retrying in {backoff}s (not treated as corruption): {e}"
                        )
                        time.sleep(backoff)
                        continue
                    else:
                        logger.error(
                            f"Disk I/O error persisted after {max_retries} attempts, giving up "
                            f"(not treated as corruption, no recovery triggered): {e}"
                        )
                        raise
                else:
                    raise
        
        # Should not reach here, but just in case
        raise last_error if last_error else sqlite3.OperationalError("Failed to connect to database")
    
    def invalidate_connection(self):
        """Invalidate the thread-local cached connection (call after database reset/delete)."""
        cached_conn = getattr(self._thread_local, 'conn', None)
        if cached_conn is not None:
            try:
                cached_conn.close()
            except Exception:
                pass
            self._thread_local.conn = None
    
    @staticmethod
    def _is_corruption_error(error):
        """Check if an exception indicates database corruption.

        Note: "disk i/o error" is deliberately NOT included here. It's usually a
        transient host/filesystem hiccup (NAS blip, brief ENOSPC) on an otherwise
        healthy database, not corruption — treating it as corruption meant a
        transient I/O error alone could trigger the full destructive recovery
        path against a perfectly fine database. It's handled as a bounded,
        backed-off retry in get_connection() instead (see _is_transient_io_error).
        """
        err_str = str(error).lower()
        return ("database disk image is malformed" in err_str or
                "file is not a database" in err_str)

    @staticmethod
    def _is_transient_io_error(error) -> bool:
        """Check if an exception is a transient I/O error that should be
        retried with backoff rather than treated as corruption."""
        return "disk i/o error" in str(error).lower()
    
    def _trigger_corruption_recovery(self, error=None):
        """Thread-safe corruption recovery. Only one thread performs recovery;
        others wait for it to complete, then get fresh connections.

        `error` is the exception that made the caller *suspect* corruption —
        it is only used for logging. The decision to actually run the
        destructive recovery path is always based on PRAGMA integrity_check
        (see _run_integrity_diagnostics), never on the exception string alone:
        trusting a single exception string previously meant e.g. a transient
        "disk i/o error" could trigger a full destructive rebuild of a
        perfectly healthy database.

        Returns True if recovery was performed (or already done recently, or
        the integrity check found nothing wrong), False on failure.
        """
        # If we recovered very recently (within 30s), don't do it again — just invalidate connection
        if time.time() - HuntarrDatabase._corruption_recovered_at < 30:
            self.invalidate_connection()
            return True

        acquired = HuntarrDatabase._corruption_lock.acquire(timeout=60)
        if not acquired:
            logger.warning("Timed out waiting for corruption recovery lock")
            self.invalidate_connection()
            return False

        try:
            # Double-check: another thread may have already recovered while we waited
            if time.time() - HuntarrDatabase._corruption_recovered_at < 30:
                self.invalidate_connection()
                return True

            HuntarrDatabase._corruption_recovering = True
            logger.error(f"=== POSSIBLE DATABASE CORRUPTION ({error}) — verifying before recovery ===")

            # Invalidate this thread's connection
            self.invalidate_connection()

            # Attempt WAL recovery first (non-destructive) — folds any committed-
            # but-uncheckpointed WAL data into the main file before we even ask
            # whether anything is actually wrong.
            self._attempt_wal_recovery()

            # Require proof before doing anything destructive: a single caught
            # exception string is not enough (see _is_corruption_error notes).
            diagnostics = self._run_integrity_diagnostics()
            if diagnostics['ok']:
                logger.error(
                    f"integrity_check reports ok after WAL recovery — the triggering error "
                    f"was not real corruption ({error}). Declining destructive recovery."
                )
                HuntarrDatabase._corruption_recovered_at = time.time()
                return True

            logger.error(
                f"Corruption CONFIRMED by integrity_check: {diagnostics['integrity_check']} "
                f"(quick_check={diagnostics['quick_check']})"
            )

            # Full corruption handling (backup + rebuild) — _handle_database_corruption
            # re-verifies integrity itself before touching anything, so this is safe
            # even if called from other paths that didn't already check.
            self._handle_database_corruption()

            # Recreate tables on the fresh database
            self.ensure_database_exists()

            HuntarrDatabase._corruption_recovered_at = time.time()
            logger.info("=== DATABASE CORRUPTION RECOVERY COMPLETE ===")
            return True

        except Exception as e:
            logger.error(f"Database corruption recovery failed: {e}")
            return False
        finally:
            HuntarrDatabase._corruption_recovering = False
            HuntarrDatabase._corruption_lock.release()

    def _check_and_recover_corruption(self, error):
        """Check if an error is corruption-related and trigger recovery if so.

        Call this from any except block that catches database errors.
        Returns True if corruption was detected and recovery was triggered,
        meaning the caller should retry or return a safe default.
        """
        if self._is_corruption_error(error):
            logger.error(f"Database corruption suspected during operation: {error}")
            self._trigger_corruption_recovery(error=error)
            return True
        return False
    
    def _get_database_path(self) -> Path:
        """Get database path - use /config for Docker, Windows AppData, or local data directory"""
        # Check if running in Docker (config directory exists)
        config_dir = Path("/config")
        if config_dir.exists() and config_dir.is_dir():
            # Running in Docker - use persistent config directory
            return config_dir / "huntarr.db"
        
        # Check if we have a Windows-specific config directory set
        windows_config = os.environ.get("HUNTARR_CONFIG_DIR")
        if windows_config:
            config_path = Path(windows_config)
            config_path.mkdir(parents=True, exist_ok=True)
            return config_path / "huntarr.db"
        
        # Check if we're on Windows and use AppData
        import platform
        if platform.system() == "Windows":
            appdata = os.environ.get("APPDATA", os.path.expanduser("~"))
            windows_config_dir = Path(appdata) / "Huntarr"
            windows_config_dir.mkdir(parents=True, exist_ok=True)
            return windows_config_dir / "huntarr.db"
        
        # For local development on non-Windows, use data directory in project root
        project_root = Path(__file__).parent.parent.parent.parent
        data_dir = project_root / "data"
        
        # Ensure directory exists
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir / "huntarr.db"



    def _attempt_wal_recovery(self) -> bool:
        """Attempt to recover database by checkpointing WAL files.
        
        After an unclean shutdown (Docker stop, crash, etc.), the WAL file may contain
        uncommitted data. This method tries to recover that data before declaring corruption.
        Returns True if recovery succeeded, False otherwise.
        """
        try:
            wal_path = Path(str(self.db_path) + "-wal")
            shm_path = Path(str(self.db_path) + "-shm")
            
            # If WAL file exists, try to checkpoint it
            if wal_path.exists():
                logger.info(f"WAL file found ({wal_path.stat().st_size} bytes), attempting recovery checkpoint...")
                try:
                    # Open with a fresh connection, set WAL mode, and force checkpoint
                    conn = sqlite3.connect(self.db_path, timeout=30)
                    conn.execute('PRAGMA journal_mode = WAL')
                    conn.execute('PRAGMA busy_timeout = 30000')
                    result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                    conn.close()
                    logger.info(f"WAL checkpoint result: blocked={result[0]}, pages_written={result[1]}, pages_checkpointed={result[2]}")
                    return True
                except Exception as wal_error:
                    logger.warning(f"WAL checkpoint failed: {wal_error}")
            
            # If no WAL but SHM exists, remove orphaned SHM
            if shm_path.exists() and not wal_path.exists():
                logger.info("Removing orphaned SHM file...")
                try:
                    shm_path.unlink()
                except Exception:
                    pass
            
            return False
        except Exception as e:
            logger.warning(f"WAL recovery attempt failed: {e}")
            return False
    
    # Number of safe snapshots kept: huntarr_safe_snapshot.db (newest) plus
    # this many numbered history files. A single-file snapshot meant a bad
    # write (e.g. snapshotting an already-emptied live database) permanently
    # destroyed the only known-good copy — see write_safe_snapshot.
    _SAFE_SNAPSHOT_HISTORY_COUNT = 5

    def _get_safe_snapshot_path(self) -> Path:
        """Path to the newest periodically-refreshed, guaranteed-consistent
        database snapshot (see write_safe_snapshot). Lives alongside the live
        db so it's covered by whatever the host already backs up, without
        needing any external backup config changes."""
        return self.db_path.parent / "huntarr_safe_snapshot.db"

    def _get_safe_snapshot_history_path(self, n: int) -> Path:
        """Path to the n-th oldest snapshot history slot (1 = most recently
        rotated out of the newest slot)."""
        return self.db_path.parent / f"huntarr_safe_snapshot.{n}.db"

    def _iter_safe_snapshot_paths(self):
        """Yield existing snapshot paths, newest first."""
        newest = self._get_safe_snapshot_path()
        if newest.exists():
            yield newest
        for n in range(1, self._SAFE_SNAPSHOT_HISTORY_COUNT + 1):
            p = self._get_safe_snapshot_history_path(n)
            if p.exists():
                yield p

    def _rotate_safe_snapshots(self):
        """Shift the numbered snapshot history up by one slot, dropping the
        oldest beyond the retention count, and move the current newest
        snapshot into slot 1. Call immediately before writing a new snapshot
        into huntarr_safe_snapshot.db."""
        newest = self._get_safe_snapshot_path()
        if not newest.exists():
            return
        oldest = self._get_safe_snapshot_history_path(self._SAFE_SNAPSHOT_HISTORY_COUNT)
        try:
            if oldest.exists():
                oldest.unlink()
        except Exception as e:
            logger.warning(f"Failed to prune oldest safe snapshot history slot: {e}")
        for n in range(self._SAFE_SNAPSHOT_HISTORY_COUNT - 1, 0, -1):
            src = self._get_safe_snapshot_history_path(n)
            if src.exists():
                try:
                    src.replace(self._get_safe_snapshot_history_path(n + 1))
                except Exception as e:
                    logger.warning(f"Failed to rotate safe snapshot history slot {n}: {e}")
        try:
            newest.replace(self._get_safe_snapshot_history_path(1))
        except Exception as e:
            logger.warning(f"Failed to rotate current safe snapshot into history: {e}")

    @staticmethod
    def _get_table_row_counts(db_path) -> Dict[str, int]:
        """Best-effort per-table row counts for a database file. Used to
        compare before/after state around recovery and snapshot writes."""
        counts: Dict[str, int] = {}
        conn = None
        try:
            conn = sqlite3.connect(str(db_path), timeout=10)
            conn.execute('PRAGMA busy_timeout = 5000')
            tables = [
                row[0] for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            ]
            for table in tables:
                try:
                    counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        return counts

    @staticmethod
    def _detect_row_count_collapse(before: Dict[str, int], after: Dict[str, int]) -> List[str]:
        """Return descriptions of tables whose row count collapsed by more
        than 90% (or went non-zero -> zero) from `before` to `after`. This is
        exactly the failure mode that let a recovery run silently empty
        magnetarr_sources/magnetarr_realdebrid_submissions/magnetarr_stats and
        then have write_safe_snapshot faithfully overwrite the last good
        snapshot with the empties six hours later."""
        collapsed = []
        for table, before_count in before.items():
            if before_count <= 0:
                continue
            after_count = after.get(table, 0)
            if after_count == 0 or after_count <= before_count * 0.10:
                collapsed.append(f"{table} ({before_count} -> {after_count})")
        return collapsed

    def write_safe_snapshot(self) -> bool:
        """Write a guaranteed-consistent snapshot of the live database to a separate
        file using SQLite's online backup API, then atomically replace any previous
        snapshot (temp file + os.replace, so a reader/backup tool always sees either
        the old complete snapshot or the new one, never a partial write).

        This exists because an external file-level backup tool can catch huntarr.db
        itself mid-write (e.g. mid-checkpoint) and copy a torn, corrupted snapshot —
        that's a structural risk of copying any actively-written SQLite file, not
        something fixable by timing alone. This snapshot file is never open for
        writes except during this method's own atomic swap, so it's always safe for
        an external tool to copy, and also serves as a better recovery source than
        row-by-row salvage of an actually-corrupted file (see _handle_database_corruption).
        Returns True on success.
        """
        snapshot_path = self._get_safe_snapshot_path()
        tmp_path = snapshot_path.with_suffix(snapshot_path.suffix + '.tmp')
        source_conn = None
        dest_conn = None
        try:
            if tmp_path.exists():
                tmp_path.unlink()
            source_conn = sqlite3.connect(str(self.db_path), timeout=30)
            dest_conn = sqlite3.connect(str(tmp_path))
            source_conn.backup(dest_conn)
            dest_conn.close()
            dest_conn = None
            source_conn.close()
            source_conn = None

            # Refuse to overwrite a healthy snapshot with a collapsed one. This
            # is exactly what let a previous recovery bug go unnoticed for two
            # weeks: the live database got silently emptied of three tables,
            # and the maintenance loop kept snapshotting that emptied database
            # over the last good snapshot every 6 hours with no check at all.
            if snapshot_path.exists():
                before_counts = self._get_table_row_counts(snapshot_path)
                after_counts = self._get_table_row_counts(tmp_path)
                collapsed = self._detect_row_count_collapse(before_counts, after_counts)
                if collapsed:
                    logger.error(
                        "Refusing to overwrite safe snapshot: row count collapsed in "
                        f"table(s): {', '.join(collapsed)}. Keeping previous snapshot "
                        f"at {snapshot_path} untouched."
                    )
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass
                    return False

            self._rotate_safe_snapshots()
            os.replace(str(tmp_path), str(snapshot_path))
            logger.debug(f"Wrote safe database snapshot to {snapshot_path}")
            return True
        except Exception as e:
            logger.warning(f"Failed to write safe database snapshot: {e}")
            return False
        finally:
            if dest_conn is not None:
                try:
                    dest_conn.close()
                except Exception:
                    pass
            if source_conn is not None:
                try:
                    source_conn.close()
                except Exception:
                    pass
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

    def _run_integrity_diagnostics(self, db_path=None) -> dict:
        """Run PRAGMA quick_check (cheap pre-filter) then PRAGMA integrity_check
        against a database file and return both results. This is the proof
        gate for the destructive recovery path — nothing in this module should
        rebuild/quarantine live data based on a caught exception string alone;
        a prior version trusted "disk i/o error" as proof of corruption and
        that alone could trigger destroying a perfectly healthy database.

        Always logs the outcome at ERROR level: that evidence never existed
        before, which is why a past bad recovery run went unnoticed for two
        weeks — there was no log record of what integrity_check actually said.
        """
        path = db_path or self.db_path
        diag = {'quick_check': None, 'integrity_check': None, 'ok': False, 'error': None}
        conn = None
        try:
            conn = sqlite3.connect(str(path), timeout=10)
            conn.execute('PRAGMA busy_timeout = 30000')
            quick = conn.execute('PRAGMA quick_check').fetchall()
            diag['quick_check'] = [row[0] for row in quick]
            quick_ok = len(quick) == 1 and quick[0][0] == 'ok'
            full = conn.execute('PRAGMA integrity_check').fetchall()
            diag['integrity_check'] = [row[0] for row in full]
            diag['ok'] = quick_ok and len(full) == 1 and full[0][0] == 'ok'
        except Exception as e:
            diag['error'] = str(e)
            diag['ok'] = False
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        logger.error(
            f"Integrity diagnostics for {path}: ok={diag['ok']} "
            f"quick_check={diag['quick_check']} integrity_check={diag['integrity_check']} "
            f"error={diag['error']}"
        )
        return diag

    def _fill_missing_tables_from_snapshots(self, recovered_tables: Dict[str, Any]) -> None:
        """Fill in tables that row-by-row salvage couldn't fully recover using
        the safe snapshot rotation (see write_safe_snapshot), newest first.
        A table already salvaged with more rows than a given snapshot has is
        left alone, since the corrupted file's own data is more recent than
        any periodic snapshot; a snapshot is only used when it has strictly
        more rows than what's already in `recovered_tables`. Mutates
        `recovered_tables` in place."""
        found_any = False
        for snapshot_path in self._iter_safe_snapshot_paths():
            found_any = True
            try:
                snap_conn = sqlite3.connect(str(snapshot_path), timeout=10)
            except Exception as e:
                logger.warning(f"Could not open safe snapshot {snapshot_path} for recovery: {e}")
                continue
            try:
                snap_tables = [
                    row[0] for row in snap_conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                    ).fetchall()
                ]
                for table in snap_tables:
                    existing_rows = recovered_tables.get(table, (None, []))[1]
                    try:
                        cursor = snap_conn.execute(f"SELECT * FROM {table}")
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                    except Exception as e:
                        logger.debug(f"Could not read table '{table}' from safe snapshot {snapshot_path}: {e}")
                        continue
                    if len(rows) > len(existing_rows):
                        logger.info(
                            f"Using safe snapshot {snapshot_path.name} for table '{table}' "
                            f"({len(rows)} row(s) vs {len(existing_rows)} previously recovered)"
                        )
                        recovered_tables[table] = (columns, rows)
            finally:
                snap_conn.close()
        if not found_any:
            logger.debug("No safe snapshot available for corruption recovery fallback")

    def _prune_quarantined_databases(self, keep_days: int = 14):
        """Delete quarantined database files (see _handle_database_corruption)
        older than keep_days. Quarantining instead of deleting the suspect
        live database means a bad recovery decision is still recoverable
        from; this just stops the quarantine directory from growing forever."""
        cutoff = time.time() - keep_days * 86400
        try:
            for p in self.db_path.parent.glob("huntarr_quarantined_*.db*"):
                try:
                    if p.stat().st_mtime < cutoff:
                        p.unlink()
                        logger.info(f"Pruned old quarantined database file: {p}")
                except Exception as e:
                    logger.debug(f"Could not prune quarantined file {p}: {e}")
        except Exception as e:
            logger.debug(f"Quarantine pruning skipped: {e}")

    def _log_recovery_summary(self, before_counts: Dict[str, int], after_counts: Dict[str, int],
                               backup_path, quarantine_path) -> None:
        """Log a clear, ERROR-level, per-table before/after row count summary
        for a recovery run. This evidence never existed before, which is why a
        past recovery run that silently emptied three tables went unnoticed
        for two weeks with no error anywhere."""
        all_tables = sorted(set(before_counts) | set(after_counts))
        lines = [f"  {t}: {before_counts.get(t, 0)} -> {after_counts.get(t, 0)}" for t in all_tables]
        logger.error(
            "=== DATABASE RECOVERY SUMMARY ===\n"
            f"Backup: {backup_path}\n"
            f"Quarantined original: {quarantine_path}\n"
            "Per-table row counts (before -> after):\n" + "\n".join(lines)
        )
        # TODO: wire this into a notification channel (e.g. via
        # src.primary.notification_manager.send_notification). Not done here
        # because notification connection config lives in this same database —
        # reading it back out mid-recovery, before we know the rebuild even
        # succeeded, risks re-entering DB access from inside the recovery path
        # itself. Left as a single well-marked hook point instead of restructuring.

    def _handle_database_corruption(self):
        """Handle suspected database corruption with recovery-first approach.

        Strategy: verify corruption is real via PRAGMA integrity_check (never
        trust just a caught exception string), back up the FULL file set
        (db + wal + shm — a WAL routinely holds multi-megabyte committed but
        not yet checkpointed data, and copying only the .db file made that
        data unrecoverable), then copy every table's rows over to a freshly
        created database, generically (not a hardcoded list of "important"
        tables). Only quarantine (rename, never delete) the suspect file after
        a backup exists.
        """
        logger.error(f"Handling suspected database corruption for: {self.db_path}")

        if not self.db_path.exists():
            logger.info("Database file does not exist, nothing to recover")
            return

        wal_path = Path(str(self.db_path) + "-wal")
        shm_path = Path(str(self.db_path) + "-shm")

        # Fold any committed-but-uncheckpointed WAL data into the main file
        # before doing anything else. If this fails we still fall back to
        # copying the WAL alongside the main file below, so that data isn't
        # lost even if it can't be folded in right now.
        try:
            ckpt_conn = sqlite3.connect(str(self.db_path), timeout=10)
            try:
                ckpt_conn.execute('PRAGMA busy_timeout = 30000')
                result = ckpt_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                logger.info(
                    f"Pre-recovery WAL checkpoint: blocked={result[0]}, "
                    f"pages_written={result[1]}, pages_checkpointed={result[2]}"
                )
            finally:
                ckpt_conn.close()
        except Exception as e:
            logger.warning(f"Pre-recovery WAL checkpoint failed (continuing anyway): {e}")

        # Require proof before doing anything destructive. This method is
        # reachable from several places (get_connection retries, startup
        # checks, _trigger_corruption_recovery); re-verifying here means every
        # one of those callers is safe even if they didn't already check.
        diagnostics = self._run_integrity_diagnostics()
        if diagnostics['ok']:
            logger.error(
                "integrity_check reports ok after WAL checkpoint — declining to run "
                f"destructive recovery. quick_check={diagnostics['quick_check']}"
            )
            return

        logger.error(
            f"Corruption CONFIRMED before destructive recovery: "
            f"quick_check={diagnostics['quick_check']} integrity_check={diagnostics['integrity_check']}"
        )

        before_counts = self._get_table_row_counts(self.db_path)

        # Always create a backup first - NEVER quarantine/delete without one.
        # Copy the WHOLE file set (db + wal + shm), keeping sidecar names
        # consistent with the backup .db name so SQLite associates them —
        # copying only huntarr.db silently dropped everything still sitting
        # in the WAL.
        timestamp = int(time.time())
        backup_path = self.db_path.parent / f"huntarr_corrupted_backup_{timestamp}.db"
        backup_wal = Path(str(backup_path) + "-wal")
        backup_shm = Path(str(backup_path) + "-shm")
        try:
            shutil.copy2(self.db_path, backup_path)
            if wal_path.exists():
                shutil.copy2(wal_path, backup_wal)
            if shm_path.exists():
                shutil.copy2(shm_path, backup_shm)
            logger.warning(
                f"Suspect database backed up to: {backup_path} "
                f"(wal={wal_path.exists()}, shm={shm_path.exists()})"
            )
        except Exception as backup_error:
            logger.error(f"Failed to create backup copy: {backup_error}")

        # Strategy 1: Salvage every table's rows from the backup copy,
        # generically. WAL-aware: if a WAL sidecar was copied, open the backup
        # in normal WAL mode and checkpoint it so rows only present in the WAL
        # are visible, rather than forcing journal_mode=OFF (which would
        # silently ignore them). Only fall back to a WAL-less read if the
        # WAL-aware open genuinely fails.
        recovered_tables: Dict[str, Any] = {}
        salvage_conn = None
        try:
            try:
                salvage_conn = sqlite3.connect(str(backup_path), timeout=10)
                salvage_conn.execute('PRAGMA busy_timeout = 30000')
                if backup_wal.exists():
                    salvage_conn.execute('PRAGMA journal_mode = WAL')
                    salvage_conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                else:
                    salvage_conn.execute('PRAGMA journal_mode = OFF')
                salvage_conn.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone()
            except Exception as wal_open_error:
                logger.warning(
                    f"WAL-aware open of backup copy failed, falling back to "
                    f"journal_mode=OFF: {wal_open_error}"
                )
                try:
                    if salvage_conn is not None:
                        salvage_conn.close()
                except Exception:
                    pass
                salvage_conn = sqlite3.connect(str(backup_path), timeout=10)
                salvage_conn.execute('PRAGMA journal_mode = OFF')

            try:
                table_names = [
                    row[0] for row in salvage_conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                    ).fetchall()
                ]
            except Exception as e:
                logger.warning(f"Could not list tables in backup copy: {e}")
                table_names = []

            for table in table_names:
                try:
                    cursor = salvage_conn.execute(f"SELECT * FROM {table}")
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    recovered_tables[table] = (columns, rows)
                    logger.info(f"Recovered {len(rows)} row(s) from table '{table}' in backup copy")
                except Exception as e:
                    logger.warning(f"Could not recover table '{table}': {e}")
        except Exception as e:
            logger.warning(f"Could not open backup copy for recovery: {e}")
        finally:
            if salvage_conn is not None:
                try:
                    salvage_conn.close()
                except Exception:
                    pass

        # Strategy 1b: fill in anything still missing/partial from the safe
        # snapshot rotation (see write_safe_snapshot).
        self._fill_missing_tables_from_snapshots(recovered_tables)

        # Quarantine (rename, NEVER unlink) the suspect database and its
        # sidecars now that a backup exists. A previous version of this
        # method deleted the live file outright, which meant a bad salvage
        # (e.g. one that missed WAL-only data) destroyed the only copy.
        quarantine_path = self.db_path.parent / f"huntarr_quarantined_{timestamp}.db"
        try:
            self.db_path.rename(quarantine_path)
            if wal_path.exists():
                wal_path.rename(Path(str(quarantine_path) + "-wal"))
            if shm_path.exists():
                shm_path.rename(Path(str(quarantine_path) + "-shm"))
            logger.warning(f"Quarantined suspect database to: {quarantine_path}")
        except Exception as rm_error:
            logger.error(f"Error quarantining suspect database: {rm_error}")

        self._prune_quarantined_databases()

        # Strategy 2: Recreate database (with the current schema) and restore every
        # recovered table's rows into it.
        if recovered_tables:
            try:
                self.ensure_database_exists()

                conn = sqlite3.connect(self.db_path, timeout=30)
                self._configure_connection(conn)

                for table, (columns, rows) in recovered_tables.items():
                    if not rows:
                        continue
                    # The fresh database may not have this table (e.g. it was dropped in
                    # a newer version) - skip it rather than failing the whole restore.
                    try:
                        existing_cols = {
                            row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
                        }
                    except Exception:
                        existing_cols = set()
                    if not existing_cols:
                        logger.warning(f"Skipping restore of table '{table}': table no longer exists in current schema")
                        continue

                    # Only restore columns that still exist, in case the schema changed
                    usable_columns = [c for c in columns if c in existing_cols]
                    if not usable_columns:
                        logger.warning(f"Skipping restore of table '{table}': no matching columns in current schema")
                        continue
                    col_indices = [columns.index(c) for c in usable_columns]

                    # setup_progress is intentionally excluded from general_settings restores
                    # to avoid resurrecting a stale "setup in progress" state
                    skip_key_col = None
                    if table == 'general_settings' and 'setting_key' in usable_columns:
                        skip_key_col = usable_columns.index('setting_key')

                    placeholders = ", ".join(["?"] * len(usable_columns))
                    col_list = ", ".join(usable_columns)
                    restored = 0
                    for row in rows:
                        if skip_key_col is not None and row[col_indices[skip_key_col]] == 'setup_progress':
                            continue
                        try:
                            values = [row[i] for i in col_indices]
                            conn.execute(
                                f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})",
                                values
                            )
                            restored += 1
                        except Exception as e:
                            logger.warning(f"Failed to restore a row in table '{table}': {e}")
                    logger.info(f"Restored {restored} row(s) into table '{table}'")

                conn.commit()
                conn.close()
                logger.info("Successfully restored critical data from corrupted database")

            except Exception as restore_error:
                logger.error(f"Failed to restore data to fresh database: {restore_error}")
        else:
            logger.warning("No data could be recovered from corrupted database. Starting fresh.")

        after_counts = self._get_table_row_counts(self.db_path)
        self._log_recovery_summary(before_counts, after_counts, backup_path, quarantine_path)

    def _check_database_integrity(self) -> bool:
        """Check if database integrity is intact"""
        try:
            with self.get_connection() as conn:
                # Run SQLite integrity check
                result = conn.execute("PRAGMA integrity_check").fetchone()
                if result and result[0] == "ok":
                    return True
                else:
                    logger.error(f"Database integrity check failed: {result}")
                    return False
        except Exception as e:
            logger.error(f"Database integrity check failed with error: {e}")
            return False
    
    def perform_integrity_check(self, repair: bool = False) -> dict:
        """Perform comprehensive integrity check with optional repair"""
        results = {
            'status': 'ok',
            'errors': [],
            'warnings': [],
            'repaired': False
        }
        
        try:
            with self.get_connection() as conn:
                # Full integrity check
                integrity_results = conn.execute("PRAGMA integrity_check").fetchall()
                
                if len(integrity_results) == 1 and integrity_results[0][0] == 'ok':
                    logger.info("Database integrity check passed")
                else:
                    results['status'] = 'error'
                    for result in integrity_results:
                        results['errors'].append(result[0])
                    
                    if repair:
                        logger.warning("Attempting to repair database corruption")
                        try:
                            # Attempt repair by forcing checkpoint and vacuum
                            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                            conn.execute("VACUUM")
                            
                            # Re-check integrity after repair
                            post_repair = conn.execute("PRAGMA integrity_check").fetchall()
                            if len(post_repair) == 1 and post_repair[0][0] == 'ok':
                                results['status'] = 'repaired'
                                results['repaired'] = True
                                logger.info("Database integrity restored after repair")
                            else:
                                logger.error("Database repair failed, corruption persists")
                        except Exception as repair_error:
                            logger.error(f"Database repair attempt failed: {repair_error}")
                
                # Check foreign key constraints
                fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
                if fk_violations:
                    results['warnings'].append(f"Foreign key violations found: {len(fk_violations)}")
                    for violation in fk_violations[:5]:  # Limit to first 5
                        results['warnings'].append(f"FK violation: {violation}")
                
                # Check index consistency
                for table_info in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
                    table_name = table_info[0]
                    try:
                        index_check = conn.execute(f"PRAGMA integrity_check('{table_name}')").fetchall()
                        if len(index_check) > 1 or (len(index_check) == 1 and index_check[0][0] != 'ok'):
                            results['warnings'].append(f"Index issues in table {table_name}")
                    except Exception:
                        pass  # Skip if table doesn't exist or other issues
                        
        except Exception as e:
            results['status'] = 'error'
            results['errors'].append(f"Integrity check failed: {e}")
            logger.error(f"Failed to perform integrity check: {e}")
        
        return results
    
    def create_backup(self, backup_path: str = None) -> str:
        """Create a backup of the database using SQLite backup API"""
        import time
        import shutil
        from pathlib import Path
        
        if not backup_path:
            timestamp = int(time.time())
            backup_filename = f"huntarr_backup_{timestamp}.db"
            backup_path = self.db_path.parent / backup_filename
        else:
            backup_path = Path(backup_path)
        
        try:
            # Ensure backup directory exists
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Force WAL checkpoint before backup
            with self.get_connection() as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            
            # Create backup using file copy (simple but effective)
            shutil.copy2(self.db_path, backup_path)
            
            # Verify backup integrity
            backup_db = HuntarrDatabase()
            backup_db.db_path = backup_path
            
            if backup_db._check_database_integrity():
                logger.info(f"Database backup created successfully: {backup_path}")
                return str(backup_path)
            else:
                logger.error("Backup verification failed, removing corrupt backup")
                backup_path.unlink(missing_ok=True)
                raise Exception("Backup verification failed")
                
        except Exception as e:
            logger.error(f"Failed to create database backup: {e}")
            raise
    
    def schedule_maintenance(self):
        """Schedule regular maintenance tasks"""
        import threading
        import time
        
        def maintenance_worker():
            # Write an initial safe snapshot right away so one exists even before
            # the first 6-hour maintenance cycle completes (e.g. right after a
            # fresh deploy, well before the next external backup window).
            try:
                self.write_safe_snapshot()
            except Exception as e:
                logger.warning(f"Initial safe snapshot failed: {e}")

            while True:
                try:
                    # Wait 6 hours between maintenance cycles
                    time.sleep(6 * 60 * 60)
                    
                    logger.info("Starting scheduled database maintenance")
                    
                    # Perform integrity check
                    integrity_results = self.perform_integrity_check(repair=True)
                    if integrity_results['status'] == 'error':
                        logger.error("Database integrity issues detected during maintenance")
                    
                    # Clean up expired rate limit entries
                    self.cleanup_expired_rate_limits()
                    
                    # Clean up old hunt history entries (keep last 90 days)
                    try:
                        cutoff = int(time.time()) - (90 * 24 * 60 * 60)
                        with self.get_connection() as conn:
                            cursor = conn.execute(
                                "DELETE FROM hunt_history WHERE date_time < ?", (cutoff,)
                            )
                            if cursor.rowcount > 0:
                                conn.commit()
                                logger.info(f"Cleaned up {cursor.rowcount} old hunt history entries")
                    except Exception as e:
                        logger.warning(f"Hunt history cleanup failed: {e}")

                    # Clean up old Swaparr activity history (keep last 15 days)
                    try:
                        self.cleanup_swaparr_activity_history(15)
                    except Exception as e:
                        logger.warning(f"Swaparr activity history cleanup failed: {e}")

                    # Clean up old processed reset requests
                    try:
                        with self.get_connection() as conn:
                            for table in ('reset_requests', 'reset_requests_per_instance'):
                                cursor = conn.execute(
                                    f"DELETE FROM {table} WHERE processed = 1 AND processed_at < datetime('now', '-30 days')"
                                )
                                if cursor.rowcount > 0:
                                    logger.info(f"Cleaned up {cursor.rowcount} old processed entries from {table}")
                            conn.commit()
                    except Exception as e:
                        logger.warning(f"Reset requests cleanup failed: {e}")
                    
                    # Optimize database
                    with self.get_connection() as conn:
                        conn.execute("PRAGMA optimize")
                        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                        conn.execute("PRAGMA incremental_vacuum(100)")

                    # Refresh the safe snapshot used as a corruption-recovery fallback
                    # and as a stable copy for external backup tools to read instead
                    # of the live, actively-written database file.
                    self.write_safe_snapshot()

                    logger.info("Scheduled database maintenance completed")
                    
                except Exception as e:
                    logger.error(f"Database maintenance failed: {e}")
        
        # Start maintenance thread
        maintenance_thread = threading.Thread(target=maintenance_worker, daemon=True)
        maintenance_thread.start()
        logger.info("Database maintenance scheduler started")
    
    def ensure_database_exists(self):
        """Create database and all tables if they don't exist"""
        try:
            # Ensure the database directory exists and is writable
            db_dir = self.db_path.parent
            db_dir.mkdir(parents=True, exist_ok=True)
            
            # Test write permissions
            test_file = db_dir / f"db_test_{int(time.time())}.tmp"
            try:
                test_file.write_text("test")
                test_file.unlink()
            except Exception as perm_error:
                logger.error(f"Database directory not writable: {db_dir} - {perm_error}")
                # On Windows, try an alternative location
                import platform
                if platform.system() == "Windows":
                    alt_dir = Path(os.path.expanduser("~")) / "Documents" / "Huntarr"
                    alt_dir.mkdir(parents=True, exist_ok=True)
                    self.db_path = alt_dir / "huntarr.db"
                    logger.info(f"Using alternative database location: {self.db_path}")
                else:
                    raise perm_error
            
        except Exception as e:
            logger.error(f"Error setting up database directory: {e}")
            raise
            
        # Attempt WAL recovery on startup if WAL file exists
        # This prevents data loss after unclean shutdowns (Docker stop, crash, etc.)
        wal_path = Path(str(self.db_path) + "-wal")
        if self.db_path.exists() and wal_path.exists():
            logger.info("Database WAL file detected on startup — performing recovery checkpoint...")
            self._attempt_wal_recovery()
        
        # Quick integrity check on startup to catch corruption early
        if self.db_path.exists():
            try:
                conn = sqlite3.connect(self.db_path, timeout=30)
                conn.execute('PRAGMA busy_timeout = 30000')
                result = conn.execute("PRAGMA quick_check").fetchone()
                if result and result[0] != 'ok':
                    conn.close()
                    logger.error(f"Database quick_check failed on startup: {result[0]}. Attempting recovery...")
                    self._handle_database_corruption()
                else:
                    # quick_check passed, but also test actual data reads
                    # quick_check only validates B-tree structure, not data pages
                    try:
                        conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
                        # Try reading from a known table if it exists
                        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
                        if 'app_configs' in tables:
                            conn.execute("SELECT app_type FROM app_configs LIMIT 1").fetchone()
                        if 'general_settings' in tables:
                            conn.execute("SELECT setting_key FROM general_settings LIMIT 1").fetchone()
                    except (sqlite3.DatabaseError, sqlite3.OperationalError) as data_err:
                        error_str = str(data_err).lower()
                        if "malformed" in error_str or "not a database" in error_str or "corrupt" in error_str:
                            logger.error(f"Database data corruption detected on startup: {data_err}. Attempting recovery...")
                            conn.close()
                            self._handle_database_corruption()
                            return  # ensure_database_exists will be called again via _trigger_corruption_recovery
                        else:
                            logger.warning(f"Database data read warning on startup: {data_err}")
                    finally:
                        try:
                            conn.close()
                        except Exception:
                            pass
            except (sqlite3.DatabaseError, sqlite3.OperationalError) as e:
                error_str = str(e).lower()
                if "malformed" in error_str or "not a database" in error_str:
                    logger.error(f"Database corruption detected on startup: {e}. Attempting recovery...")
                    self._handle_database_corruption()
                else:
                    logger.warning(f"Database startup check warning: {e}")
        
        # Create all tables with corruption recovery
        try:
            self._create_all_tables()
        except (sqlite3.DatabaseError, sqlite3.OperationalError) as e:
            error_str = str(e).lower()
            if "file is not a database" in error_str or "database disk image is malformed" in error_str:
                logger.error(f"Database corruption detected during table creation: {e}")
                # Try WAL recovery one more time before nuclear option
                if self._attempt_wal_recovery():
                    try:
                        self._create_all_tables()
                        logger.info("WAL recovery succeeded during table creation")
                        return
                    except Exception:
                        pass
                # WAL recovery didn't help, handle corruption with data salvage
                self._handle_database_corruption()
                # Try creating tables again after recovery
                self._create_all_tables()
            else:
                raise

        # Ensure the main owner account is synced into requestarr_users
        try:
            self.ensure_owner_in_requestarr_users()
        except Exception as e:
            logger.debug(f"Owner sync to requestarr_users deferred: {e}")
                
    def _create_all_tables(self):
        """Create all database tables"""
        with self.get_connection() as conn:
            
            # Create app_configs table for all app settings
            conn.execute('''
                CREATE TABLE IF NOT EXISTS app_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL UNIQUE,
                    config_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create general_settings table for general/global settings
            conn.execute('''
                CREATE TABLE IF NOT EXISTS general_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    setting_key TEXT NOT NULL UNIQUE,
                    setting_value TEXT NOT NULL,
                    setting_type TEXT DEFAULT 'string',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create stateful_lock table for stateful management lock info
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stateful_lock (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create stateful_processed_ids table for processed media IDs
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stateful_processed_ids (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    media_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(app_type, instance_name, media_id)
                )
            ''')
            
            # Create stateful_instance_locks table for per-instance state management
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stateful_instance_locks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    expiration_hours INTEGER NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(app_type, instance_name)
                )
            ''')
            
            # Create media_stats table for tracking hunted/upgraded media statistics (app-level aggregate)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS media_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    stat_type TEXT NOT NULL,
                    stat_value INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(app_type, stat_type)
                )
            ''')
            # Per-instance stats for Home dashboard (instance name + hunted/upgraded)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS media_stats_per_instance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    stat_type TEXT NOT NULL,
                    stat_value INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(app_type, instance_name, stat_type)
                )
            ''')
            
            # Create hourly_caps table for API usage tracking (app-level fallback)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS hourly_caps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL UNIQUE,
                    api_hits INTEGER DEFAULT 0,
                    last_reset_hour INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Per-instance API usage for *arr apps
            conn.execute('''
                CREATE TABLE IF NOT EXISTS hourly_caps_per_instance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    api_hits INTEGER DEFAULT 0,
                    last_reset_hour INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(app_type, instance_name)
                )
            ''')
            
            # Durable journal for guarded Sonarr exact-season replacement recovery.
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sonarr_season_recovery_journal (
                    id TEXT PRIMARY KEY,
                    instance_name TEXT NOT NULL,
                    series_id INTEGER NOT NULL,
                    season_number INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    files_json TEXT NOT NULL,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Shared Huntarr/Swaparr per-item pipeline journal. Additive and idempotent:
            # existing installations retain all data and gain this table on startup.
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pipeline_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    item_key TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN (
                        'candidate','search_submitted','command_complete','grabbed',
                        'downloading','imported','completed','no_grab','failed','timed_out'
                    )),
                    command_id TEXT,
                    metadata TEXT,
                    cooldown_until_epoch INTEGER DEFAULT 0,
                    updated_at_epoch INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(app_type, instance_name, item_key)
                )
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_pipeline_items_command
                ON pipeline_items (app_type, command_id)
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_pipeline_items_unresolved
                ON pipeline_items (app_type, instance_name, state, cooldown_until_epoch)
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pipeline_item_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    item_key TEXT NOT NULL,
                    state TEXT NOT NULL,
                    command_id TEXT,
                    metadata TEXT,
                    occurred_at_epoch INTEGER NOT NULL,
                    occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_pipeline_item_events_lookup
                ON pipeline_item_events (app_type, instance_name, item_key, occurred_at_epoch)
            ''')

            # Optional Sonarr/Radarr webhook receipts. The globally unique digest makes
            # retries idempotent across restarts; pruning bounds long-term storage.
            conn.execute('''
                CREATE TABLE IF NOT EXISTS starr_webhook_receipts (
                    event_id TEXT PRIMARY KEY,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    received_at_epoch INTEGER NOT NULL,
                    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_starr_webhook_receipts_received
                ON starr_webhook_receipts (received_at_epoch)
            ''')
            conn.execute(
                "DELETE FROM starr_webhook_receipts WHERE received_at_epoch < ?",
                (int(time.time()) - 7 * 24 * 60 * 60,),
            )

            # Create sleep_data table for cycle tracking (single-app e.g. swaparr)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sleep_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL UNIQUE,
                    next_cycle_time TEXT,
                    cycle_lock BOOLEAN DEFAULT FALSE,
                    last_cycle_start TEXT,
                    last_cycle_end TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Per-instance sleep/cycle data for *arr apps (one next_cycle per instance)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sleep_data_per_instance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    next_cycle_time TEXT,
                    cycle_lock BOOLEAN DEFAULT FALSE,
                    last_cycle_start TEXT,
                    last_cycle_end TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(app_type, instance_name)
                )
            ''')
            
            # Create swaparr_stats table for Swaparr-specific statistics
            conn.execute('''
                CREATE TABLE IF NOT EXISTS swaparr_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stat_key TEXT NOT NULL UNIQUE,
                    stat_value INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # History table moved to manager.db - remove this table if it exists
            conn.execute('DROP TABLE IF EXISTS history')
            
            # Create schedules table for storing scheduled actions
            conn.execute('''
                CREATE TABLE IF NOT EXISTS schedules (
                    id TEXT PRIMARY KEY,
                    app_type TEXT NOT NULL,
                    action TEXT NOT NULL,
                    time_hour INTEGER NOT NULL,
                    time_minute INTEGER NOT NULL,
                    days TEXT NOT NULL,
                    app_instance TEXT NOT NULL,
                    enabled BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create state_data table for state management (processed IDs and reset times)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS state_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    state_type TEXT NOT NULL,
                    state_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(app_type, state_type)
                )
            ''')
            
            # Create swaparr_state table for Swaparr-specific state management
            conn.execute('''
                CREATE TABLE IF NOT EXISTS swaparr_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_name TEXT NOT NULL,
                    state_type TEXT NOT NULL,
                    state_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(app_name, state_type)
                )
            ''')

            # Create swaparr_activity_history table - durable record of queue items
            # that completed normally vs were removed by Swaparr (and why)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS swaparr_activity_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_name TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    item_name TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    reason TEXT,
                    strikes_at_event INTEGER DEFAULT 0,
                    occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_swaparr_activity_history_lookup
                ON swaparr_activity_history (app_name, instance_name, occurred_at)
            ''')

            # Create users table for authentication and user management
            conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    two_fa_enabled BOOLEAN DEFAULT FALSE,
                    two_fa_secret TEXT,
                    temp_2fa_secret TEXT,
                    plex_token TEXT,
                    plex_user_data TEXT,
                    recovery_key TEXT
                )
            ''')
            
            # Logs table moved to separate logs.db - remove if it exists
            conn.execute('DROP TABLE IF EXISTS logs')
            
            # Create recovery_key_rate_limit table for tracking failed recovery key attempts
            conn.execute('''
                CREATE TABLE IF NOT EXISTS recovery_key_rate_limit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ip_address TEXT NOT NULL,
                    username TEXT,
                    failed_attempts INTEGER DEFAULT 0,
                    locked_until TIMESTAMP,
                    last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ip_address)
                )
            ''')

            # Movie Hunt multi-instance: one row per tenant (ID never reused)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS movie_hunt_instances (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_movie_hunt_instances_id ON movie_hunt_instances(id)')

            # TV Hunt multi-instance: one row per tenant (ID never reused)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS tv_hunt_instances (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create requestarr_requests table for tracking media requests
            # NOTE: No UNIQUE constraint — multiple users can request the same media
            conn.execute('''
                CREATE TABLE IF NOT EXISTS requestarr_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tmdb_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    year INTEGER,
                    overview TEXT,
                    poster_path TEXT,
                    backdrop_path TEXT,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create hunt_history table for tracking processed media history
            conn.execute('''
                CREATE TABLE IF NOT EXISTS hunt_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    media_id TEXT NOT NULL,
                    processed_info TEXT NOT NULL,
                    operation_type TEXT DEFAULT 'missing',
                    discovered BOOLEAN DEFAULT FALSE,
                    status TEXT DEFAULT 'sent',
                    date_time INTEGER NOT NULL,
                    date_time_readable TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Add status column if it doesn't exist (for existing databases)
            try:
                conn.execute("ALTER TABLE hunt_history ADD COLUMN status TEXT DEFAULT 'sent'")
                logger.info("Added status column to hunt_history table")
            except sqlite3.OperationalError:
                pass  # Column already exists

            # Create chat_messages table for lightweight in-app chat
            conn.execute('''
                CREATE TABLE IF NOT EXISTS chat_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    username TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'user',
                    message TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_chat_messages_created ON chat_messages(created_at)')
            
            # Create requestarr_hidden_media table for permanently hidden media
            conn.execute('''
                CREATE TABLE IF NOT EXISTS requestarr_hidden_media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tmdb_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    poster_path TEXT,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    hidden_at INTEGER NOT NULL,
                    hidden_at_readable TEXT NOT NULL,
                    UNIQUE(tmdb_id, media_type, app_type, instance_name)
                )
            ''')
            
            # Add app_type and instance_name columns if they don't exist (for existing databases)
            try:
                conn.execute('ALTER TABLE requestarr_hidden_media ADD COLUMN app_type TEXT')
                logger.info("Added app_type column to requestarr_hidden_media table")
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            try:
                conn.execute('ALTER TABLE requestarr_hidden_media ADD COLUMN instance_name TEXT')
                logger.info("Added instance_name column to requestarr_hidden_media table")
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            # Add user_id column for per-user hidden media (NULL = global/owner, user_id = personal)
            try:
                conn.execute('ALTER TABLE requestarr_hidden_media ADD COLUMN user_id INTEGER')
                logger.info("Added user_id column to requestarr_hidden_media table")
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            # Add username column for simplified cross-instance personal blacklist
            try:
                conn.execute('ALTER TABLE requestarr_hidden_media ADD COLUMN username TEXT')
                logger.info("Added username column to requestarr_hidden_media table")
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            # Create requestarr_global_blacklist table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS requestarr_global_blacklist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tmdb_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    year TEXT,
                    poster_path TEXT,
                    blacklisted_by TEXT,
                    blacklisted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notes TEXT,
                    UNIQUE(tmdb_id, media_type)
                )
            ''')
            try:
                conn.execute('CREATE INDEX IF NOT EXISTS idx_global_blacklist_media ON requestarr_global_blacklist(media_type, tmdb_id)')
            except Exception:
                pass
            
            # Add temp_2fa_secret column if it doesn't exist (for existing databases)
            try:
                conn.execute('ALTER TABLE users ADD COLUMN temp_2fa_secret TEXT')
                logger.info("Added temp_2fa_secret column to users table")
            except sqlite3.OperationalError:
                # Column already exists
                pass
            
            # Add recovery_key column if it doesn't exist (for existing databases)
            try:
                conn.execute('ALTER TABLE users ADD COLUMN recovery_key TEXT')
                logger.info("Added recovery_key column to users table")
            except sqlite3.OperationalError:
                # Column already exists
                pass
            
            # Add plex_linked_at column if it doesn't exist (for existing databases)
            try:
                conn.execute('ALTER TABLE users ADD COLUMN plex_linked_at INTEGER')
                logger.info("Added plex_linked_at column to users table")
            except sqlite3.OperationalError:
                # Column already exists
                pass
            
            # Create reset_requests table for reset request management (app-level e.g. swaparr)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS reset_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    processed INTEGER NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Per-instance reset requests for *arr apps (multiple rows per instance over time)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS reset_requests_per_instance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    processed INTEGER NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Notification connections table — multi-provider notification system
            conn.execute('''
                CREATE TABLE IF NOT EXISTS notification_connections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    settings TEXT NOT NULL DEFAULT '{}',
                    triggers TEXT NOT NULL DEFAULT '{}',
                    include_app_name INTEGER DEFAULT 1,
                    include_instance_name INTEGER DEFAULT 1,
                    app_scope TEXT NOT NULL DEFAULT 'all',
                    instance_scope TEXT NOT NULL DEFAULT 'all',
                    category TEXT NOT NULL DEFAULT 'instance',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Migration: add app_scope/instance_scope if table already existed
            try:
                conn.execute('SELECT app_scope FROM notification_connections LIMIT 1')
            except sqlite3.OperationalError:
                conn.execute("ALTER TABLE notification_connections ADD COLUMN app_scope TEXT NOT NULL DEFAULT 'all'")
                conn.execute("ALTER TABLE notification_connections ADD COLUMN instance_scope TEXT NOT NULL DEFAULT 'all'")
            # Migration: add category column
            try:
                conn.execute('SELECT category FROM notification_connections LIMIT 1')
            except sqlite3.OperationalError:
                conn.execute("ALTER TABLE notification_connections ADD COLUMN category TEXT NOT NULL DEFAULT 'instance'")

            # User notification connections — per-user, connection-based (mirrors admin notifications)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS user_notification_connections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    name TEXT NOT NULL DEFAULT 'Unnamed',
                    provider TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    settings TEXT NOT NULL DEFAULT '{}',
                    triggers TEXT NOT NULL DEFAULT '{}',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Migrate old user_notification_settings to new table if it exists
            try:
                old_rows = conn.execute('SELECT * FROM user_notification_settings').fetchall()
                if old_rows:
                    for row in old_rows:
                        r = dict(zip([d[0] for d in conn.execute('SELECT * FROM user_notification_settings LIMIT 0').description], row)) if not hasattr(row, 'keys') else dict(row)
                        conn.execute('''
                            INSERT OR IGNORE INTO user_notification_connections (username, name, provider, enabled, settings, triggers)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (r.get('username', ''), r.get('provider', 'Unknown'), r.get('provider', ''),
                              r.get('enabled', 1), r.get('settings', '{}'), r.get('types', '{}')))
                    conn.commit()
            except Exception:
                pass
            
            # Create indexes for better performance
            # Note: indexes on UNIQUE columns (app_configs.app_type, general_settings.setting_key,
            # swaparr_stats.stat_key, users.username) and PRIMARY KEY columns
            # (movie_hunt_instances.id) are redundant — SQLite auto-creates implicit indexes for these.
            conn.execute('CREATE INDEX IF NOT EXISTS idx_stateful_processed_app_instance ON stateful_processed_ids(app_type, instance_name)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_stateful_processed_media_id ON stateful_processed_ids(media_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_stateful_instance_locks_app_instance ON stateful_instance_locks(app_type, instance_name)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_media_stats_app_type ON media_stats(app_type, stat_type)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_hourly_caps_app_type ON hourly_caps(app_type)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_hourly_caps_per_instance_app ON hourly_caps_per_instance(app_type, instance_name)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_sonarr_recovery_instance_state ON sonarr_season_recovery_journal(instance_name, state)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_sleep_data_app_type ON sleep_data(app_type)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_sleep_data_per_instance_app ON sleep_data_per_instance(app_type, instance_name)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_schedules_app_type ON schedules(app_type)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_schedules_enabled ON schedules(enabled)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_schedules_time ON schedules(time_hour, time_minute)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_state_data_app_type ON state_data(app_type, state_type)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_swaparr_state_app_name ON swaparr_state(app_name, state_type)')
            # Logs indexes moved to logs.db
            conn.execute('CREATE INDEX IF NOT EXISTS idx_hunt_history_app_instance ON hunt_history(app_type, instance_name)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_hunt_history_date_time ON hunt_history(date_time)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_hunt_history_media_id ON hunt_history(media_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_hunt_history_operation_type ON hunt_history(operation_type)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_hunt_history_processed_info ON hunt_history(processed_info)')
            # Reset request indexes for pending lookups (queried by app_type + processed)
            conn.execute('CREATE INDEX IF NOT EXISTS idx_reset_requests_app_processed ON reset_requests(app_type, processed)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_reset_requests_per_instance_app ON reset_requests_per_instance(app_type, instance_name, processed)')
            
            # Hidden media filter index for requestarr (cross-instance: filtered by media_type + username)
            conn.execute('CREATE INDEX IF NOT EXISTS idx_hidden_media_filter ON requestarr_hidden_media(media_type, app_type, instance_name, hidden_at)')
            try:
                conn.execute('CREATE INDEX IF NOT EXISTS idx_hidden_media_username ON requestarr_hidden_media(tmdb_id, media_type, username)')
            except Exception:
                pass
            
            # ── Indexer Hunt tables ─────────────────────────────────────
            # Centralized indexer storage (global, not per-instance)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS indexer_hunt_indexers (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    display_name TEXT DEFAULT '',
                    preset TEXT DEFAULT 'manual',
                    protocol TEXT DEFAULT 'usenet',
                    url TEXT DEFAULT '',
                    api_path TEXT DEFAULT '/api',
                    api_key TEXT DEFAULT '',
                    enabled INTEGER DEFAULT 1,
                    priority INTEGER DEFAULT 50,
                    categories TEXT DEFAULT '[]',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Stats tracking per indexer (aggregate counters)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS indexer_hunt_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    indexer_id TEXT NOT NULL,
                    stat_type TEXT NOT NULL,
                    stat_value REAL DEFAULT 0,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(indexer_id, stat_type)
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_ih_stats_indexer ON indexer_hunt_stats(indexer_id)')

            # Event history (searches, grabs, failures)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS indexer_hunt_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    indexer_id TEXT NOT NULL,
                    indexer_name TEXT DEFAULT '',
                    event_type TEXT NOT NULL,
                    query TEXT DEFAULT '',
                    result_title TEXT DEFAULT '',
                    response_time_ms INTEGER DEFAULT 0,
                    success INTEGER DEFAULT 1,
                    error_message TEXT DEFAULT '',
                    instance_id INTEGER DEFAULT NULL,
                    instance_name TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_ih_history_indexer ON indexer_hunt_history(indexer_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_ih_history_type ON indexer_hunt_history(event_type)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_ih_history_date ON indexer_hunt_history(created_at)')

            # ── Magnetarr tables (URL/subreddit magnet scraper) ──────────
            conn.execute('''
                CREATE TABLE IF NOT EXISTS magnetarr_sources (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    source_type TEXT DEFAULT 'reddit',
                    enabled INTEGER DEFAULT 1,
                    interval_minutes INTEGER DEFAULT 20,
                    last_scanned_at TIMESTAMP,
                    last_after_token TEXT DEFAULT '',
                    last_error TEXT DEFAULT '',
                    realdebrid_auto_add INTEGER DEFAULT 0,
                    realdebrid_keywords TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            try:
                conn.execute("ALTER TABLE magnetarr_sources ADD COLUMN last_error TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass  # Column already exists
            try:
                conn.execute("ALTER TABLE magnetarr_sources ADD COLUMN realdebrid_auto_add INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass  # Column already exists
            try:
                conn.execute("ALTER TABLE magnetarr_sources ADD COLUMN realdebrid_keywords TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass  # Column already exists

            conn.execute('''
                CREATE TABLE IF NOT EXISTS magnetarr_magnets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    info_hash TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    magnet_uri TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    source_name TEXT DEFAULT '',
                    source_url TEXT DEFAULT '',
                    category TEXT DEFAULT 'other',
                    size_bytes INTEGER DEFAULT 0,
                    seeders INTEGER DEFAULT 1,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            try:
                # Magnet URIs carry no real seeder/peer count, and some *arr clients
                # filter out or deprioritize anything reporting 0 seeders, so we report
                # a nominal 1. Adding the column backfills existing rows to 1 too
                # (SQLite applies ADD COLUMN...DEFAULT to pre-existing rows).
                conn.execute('ALTER TABLE magnetarr_magnets ADD COLUMN seeders INTEGER DEFAULT 1')
            except sqlite3.OperationalError:
                pass  # Column already exists
            try:
                conn.execute('UPDATE magnetarr_magnets SET seeders = 1 WHERE seeders IS NULL OR seeders <= 0')
            except sqlite3.OperationalError:
                pass  # Table doesn't exist yet on a brand-new database
            conn.execute('CREATE INDEX IF NOT EXISTS idx_magnetarr_magnets_hash ON magnetarr_magnets(info_hash)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_magnetarr_magnets_discovered ON magnetarr_magnets(discovered_at)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_magnetarr_magnets_source ON magnetarr_magnets(source_id)')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS magnetarr_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT NOT NULL,
                    stat_type TEXT NOT NULL,
                    stat_value REAL DEFAULT 0,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_id, stat_type)
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_magnetarr_stats_source ON magnetarr_stats(source_id)')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS magnetarr_realdebrid_submissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    info_hash TEXT NOT NULL UNIQUE,
                    magnet_uri TEXT NOT NULL,
                    title TEXT DEFAULT '',
                    permalink TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    rd_torrent_id TEXT DEFAULT '',
                    content_hash TEXT DEFAULT '',
                    status TEXT DEFAULT 'pending',
                    last_error TEXT DEFAULT '',
                    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_checked_at TIMESTAMP,
                    active INTEGER DEFAULT 1
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_magnetarr_rd_active ON magnetarr_realdebrid_submissions(active)')

            # ── Requestarr Users (multi-user request system) ────────────
            conn.execute('''
                CREATE TABLE IF NOT EXISTS requestarr_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password TEXT NOT NULL,
                    email TEXT DEFAULT '',
                    role TEXT NOT NULL DEFAULT 'user',
                    permissions TEXT NOT NULL DEFAULT '{}',
                    plex_user_data TEXT,
                    avatar_url TEXT,
                    request_count INTEGER DEFAULT 0,
                    tv_category TEXT DEFAULT '',
                    movie_category TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_requestarr_users_role ON requestarr_users(role)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_requestarr_users_username ON requestarr_users(username)')

            # ── Migration: add tv_category / movie_category columns if missing ──
            try:
                cols = [r[1] for r in conn.execute('PRAGMA table_info(requestarr_users)').fetchall()]
                if 'tv_category' not in cols:
                    conn.execute("ALTER TABLE requestarr_users ADD COLUMN tv_category TEXT DEFAULT ''")
                    logger.info("Added tv_category column to requestarr_users")
                if 'movie_category' not in cols:
                    conn.execute("ALTER TABLE requestarr_users ADD COLUMN movie_category TEXT DEFAULT ''")
                    logger.info("Added movie_category column to requestarr_users")
                # 2FA support for non-owner users
                if 'two_fa_enabled' not in cols:
                    conn.execute("ALTER TABLE requestarr_users ADD COLUMN two_fa_enabled BOOLEAN DEFAULT 0")
                    logger.info("Added two_fa_enabled column to requestarr_users")
                if 'two_fa_secret' not in cols:
                    conn.execute("ALTER TABLE requestarr_users ADD COLUMN two_fa_secret TEXT")
                    logger.info("Added two_fa_secret column to requestarr_users")
                if 'temp_2fa_secret' not in cols:
                    conn.execute("ALTER TABLE requestarr_users ADD COLUMN temp_2fa_secret TEXT")
                    logger.info("Added temp_2fa_secret column to requestarr_users")
            except Exception as mig_err:
                logger.warning(f"requestarr_users migration note: {mig_err}")

            # ── Requestarr Services (which instances are enabled for requests) ──
            conn.execute('''
                CREATE TABLE IF NOT EXISTS requestarr_services (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    service_type TEXT NOT NULL,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    instance_id INTEGER,
                    is_default INTEGER DEFAULT 0,
                    is_4k INTEGER DEFAULT 0,
                    enabled INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(app_type, instance_name)
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_requestarr_services_type ON requestarr_services(service_type)')

            # ── Requestarr Requests (media request tracking) ──
            conn.execute('''
                CREATE TABLE IF NOT EXISTS requestarr_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL DEFAULT 0,
                    username TEXT NOT NULL DEFAULT '',
                    media_type TEXT NOT NULL DEFAULT 'movie',
                    tmdb_id INTEGER NOT NULL DEFAULT 0,
                    tvdb_id INTEGER,
                    title TEXT NOT NULL DEFAULT '',
                    year TEXT,
                    poster_path TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    instance_name TEXT,
                    app_type TEXT NOT NULL DEFAULT '',
                    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    responded_at TIMESTAMP,
                    responded_by TEXT,
                    notes TEXT
                )
            ''')
            # Add columns if they don't exist (migration for existing DBs)
            for col_def in [
                ('user_id', 'INTEGER NOT NULL DEFAULT 0'),
                ('username', "TEXT NOT NULL DEFAULT ''"),
                ('media_type', "TEXT NOT NULL DEFAULT 'movie'"),
                ('tmdb_id', 'INTEGER NOT NULL DEFAULT 0'),
                ('tvdb_id', 'INTEGER'),
                ('title', "TEXT NOT NULL DEFAULT ''"),
                ('year', 'TEXT'),
                ('poster_path', 'TEXT'),
                ('status', "TEXT NOT NULL DEFAULT 'pending'"),
                ('instance_name', 'TEXT'),
                ('requested_at', 'TIMESTAMP'),
                ('responded_at', 'TIMESTAMP'),
                ('responded_by', 'TEXT'),
                ('notes', 'TEXT'),
                ('app_type', "TEXT NOT NULL DEFAULT ''"),
            ]:
                try:
                    conn.execute(f'ALTER TABLE requestarr_requests ADD COLUMN {col_def[0]} {col_def[1]}')
                except Exception:
                    pass  # Column already exists
            try:
                conn.execute('CREATE INDEX IF NOT EXISTS idx_requestarr_requests_user ON requestarr_requests(user_id)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_requestarr_requests_status ON requestarr_requests(status)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_requestarr_requests_media ON requestarr_requests(media_type, tmdb_id)')
            except Exception:
                pass  # Indexes may already exist or columns missing

            # ── Migration: drop old UNIQUE(tmdb_id, media_type, app_type, instance_name) ──
            # Multiple users can now request the same media item, so the old
            # per-media uniqueness constraint must go.
            try:
                row = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='requestarr_requests'"
                ).fetchone()
                if row and row[0] and 'UNIQUE' in row[0]:
                    logger.info("Migrating requestarr_requests: dropping UNIQUE constraint")
                    conn.execute('''
                        CREATE TABLE IF NOT EXISTS requestarr_requests_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id INTEGER NOT NULL DEFAULT 0,
                            username TEXT NOT NULL DEFAULT '',
                            media_type TEXT NOT NULL DEFAULT 'movie',
                            tmdb_id INTEGER NOT NULL DEFAULT 0,
                            tvdb_id INTEGER,
                            title TEXT NOT NULL DEFAULT '',
                            year TEXT,
                            poster_path TEXT,
                            status TEXT NOT NULL DEFAULT 'pending',
                            instance_name TEXT,
                            app_type TEXT NOT NULL DEFAULT '',
                            requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            responded_at TIMESTAMP,
                            responded_by TEXT,
                            notes TEXT
                        )
                    ''')
                    # Copy existing data — map old columns to new where they overlap
                    conn.execute('''
                        INSERT INTO requestarr_requests_new
                            (id, user_id, username, media_type, tmdb_id, tvdb_id, title, year,
                             poster_path, status, instance_name, app_type, requested_at,
                             responded_at, responded_by, notes)
                        SELECT id,
                               COALESCE(user_id, 0),
                               COALESCE(username, ''),
                               COALESCE(media_type, 'movie'),
                               COALESCE(tmdb_id, 0),
                               tvdb_id,
                               COALESCE(title, ''),
                               year,
                               poster_path,
                               COALESCE(status, 'pending'),
                               instance_name,
                               COALESCE(app_type, ''),
                               COALESCE(requested_at, CURRENT_TIMESTAMP),
                               responded_at,
                               responded_by,
                               notes
                        FROM requestarr_requests
                    ''')
                    conn.execute('DROP TABLE requestarr_requests')
                    conn.execute('ALTER TABLE requestarr_requests_new RENAME TO requestarr_requests')
                    # Recreate indexes on the new table
                    conn.execute('CREATE INDEX IF NOT EXISTS idx_requestarr_requests_user ON requestarr_requests(user_id)')
                    conn.execute('CREATE INDEX IF NOT EXISTS idx_requestarr_requests_status ON requestarr_requests(status)')
                    conn.execute('CREATE INDEX IF NOT EXISTS idx_requestarr_requests_media ON requestarr_requests(media_type, tmdb_id)')
                    logger.info("Migration complete: UNIQUE constraint removed from requestarr_requests")
            except Exception as e:
                logger.error(f"Migration error (requestarr_requests UNIQUE drop): {e}")

            # ── Instance Bundles (v2: references app_type+instance_name directly) ──
            # Migrate from v1 (service_id based) to v2 (app_type+instance_name based)
            try:
                cols = [r[1] for r in conn.execute('PRAGMA table_info(requestarr_bundles)').fetchall()]
                if 'primary_service_id' in cols and 'primary_app_type' not in cols:
                    logger.info("Migrating requestarr_bundles from v1 (service_id) to v2 (app_type+instance_name)")
                    # Read old data with service lookups
                    old_bundles = conn.execute('''
                        SELECT b.id, b.name, b.service_type, s.app_type, s.instance_name
                        FROM requestarr_bundles b
                        LEFT JOIN requestarr_services s ON s.id = b.primary_service_id
                    ''').fetchall()
                    old_members = conn.execute('''
                        SELECT bm.bundle_id, s.app_type, s.instance_name
                        FROM requestarr_bundle_members bm
                        LEFT JOIN requestarr_services s ON s.id = bm.service_id
                    ''').fetchall()
                    conn.execute('DROP TABLE IF EXISTS requestarr_bundle_members')
                    conn.execute('DROP TABLE IF EXISTS requestarr_bundles')
                    # Create v2 tables (below) then re-insert
                    conn.execute('''
                        CREATE TABLE IF NOT EXISTS requestarr_bundles (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL UNIQUE,
                            service_type TEXT NOT NULL,
                            primary_app_type TEXT NOT NULL,
                            primary_instance_name TEXT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    conn.execute('''
                        CREATE TABLE IF NOT EXISTS requestarr_bundle_members (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            bundle_id INTEGER NOT NULL,
                            app_type TEXT NOT NULL,
                            instance_name TEXT NOT NULL,
                            UNIQUE(bundle_id, app_type, instance_name),
                            FOREIGN KEY (bundle_id) REFERENCES requestarr_bundles(id) ON DELETE CASCADE
                        )
                    ''')
                    for ob in old_bundles:
                        if ob[3] and ob[4]:  # app_type and instance_name resolved
                            conn.execute('''
                                INSERT OR IGNORE INTO requestarr_bundles (id, name, service_type, primary_app_type, primary_instance_name)
                                VALUES (?, ?, ?, ?, ?)
                            ''', (ob[0], ob[1], ob[2], ob[3], ob[4]))
                    for om in old_members:
                        if om[1] and om[2]:
                            conn.execute('''
                                INSERT OR IGNORE INTO requestarr_bundle_members (bundle_id, app_type, instance_name)
                                VALUES (?, ?, ?)
                            ''', (om[0], om[1], om[2]))
                    conn.commit()
                    logger.info("Migration complete: requestarr_bundles v2")
            except Exception as e:
                logger.error(f"Migration error (requestarr_bundles v2): {e}")

            conn.execute('''
                CREATE TABLE IF NOT EXISTS requestarr_bundles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    service_type TEXT NOT NULL,
                    primary_app_type TEXT NOT NULL,
                    primary_instance_name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS requestarr_bundle_members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bundle_id INTEGER NOT NULL,
                    app_type TEXT NOT NULL,
                    instance_name TEXT NOT NULL,
                    UNIQUE(bundle_id, app_type, instance_name),
                    FOREIGN KEY (bundle_id) REFERENCES requestarr_bundles(id) ON DELETE CASCADE
                )
            ''')

            conn.commit()
            logger.info(f"Database initialized at: {self.db_path}")
    

    # ── CRUD methods are in db_mixins/ ──────────────────────────────
    # ConfigMixin:     db_mixins/db_config.py     (app config, instances, settings)
    # StateMixin:      db_mixins/db_state.py      (locks, stats, caps, sleep, schedules)
    # UsersMixin:      db_mixins/db_users.py      (auth, requestarr users, recovery)
    # RequestarrMixin: db_mixins/db_requestarr.py (services, requests, blacklist, hidden)
    # ExtrasMixin:     db_mixins/db_extras.py     (notifications, indexer, history)


# Separate LogsDatabase class for logs.db
class LogsDatabase:
    """Separate database class specifically for logs to keep logs.db separate from huntarr.db"""
    
    def __init__(self):
        self._thread_local = threading.local()
        self.db_path = self._get_logs_database_path()
        self.ensure_logs_database_exists()
    
    def _get_logs_database_path(self) -> Path:
        """Get logs database path - same directory as main database but separate file"""
        # Check if running in Docker
        config_dir = Path("/config")
        if config_dir.exists() and config_dir.is_dir():
            return config_dir / "logs.db"
        
        # Check for Windows config directory
        windows_config = os.environ.get("HUNTARR_CONFIG_DIR")
        if windows_config:
            config_path = Path(windows_config)
            config_path.mkdir(parents=True, exist_ok=True)
            return config_path / "logs.db"
        
        # Check for Windows AppData
        import platform
        if platform.system() == "Windows":
            appdata = os.environ.get("APPDATA", os.path.expanduser("~"))
            windows_config_dir = Path(appdata) / "Huntarr"
            windows_config_dir.mkdir(parents=True, exist_ok=True)
            return windows_config_dir / "logs.db"
        
        # Local development
        project_root = Path(__file__).parent.parent.parent.parent
        data_dir = project_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir / "logs.db"
    
    def _configure_logs_connection(self, conn):
        """Configure SQLite connection optimized for high-volume log writes"""
        try:
            conn.execute('PRAGMA foreign_keys = ON')
            
            # WAL mode is particularly beneficial for logs (write-heavy workload)
            try:
                conn.execute('PRAGMA journal_mode = WAL')
            except Exception as wal_error:
                logger.warning(f"WAL mode failed for logs.db, using DELETE mode: {wal_error}")
                conn.execute('PRAGMA journal_mode = DELETE')
            
            # Optimized settings for log writing
            conn.execute('PRAGMA synchronous = NORMAL')     # Balance between speed and safety for logs
            conn.execute('PRAGMA cache_size = -2000')       # 2 MB per connection (was 16 MB — per-connection cost adds up)
            conn.execute('PRAGMA temp_store = MEMORY')
            conn.execute('PRAGMA busy_timeout = 30000')     # 30 seconds for log operations
            conn.execute('PRAGMA auto_vacuum = INCREMENTAL')
            
            # WAL-specific optimizations for logs
            result = conn.execute('PRAGMA journal_mode').fetchone()
            if result and result[0] == 'wal':
                conn.execute('PRAGMA wal_autocheckpoint = 2000')    # Less frequent checkpoints for logs
                conn.execute('PRAGMA journal_size_limit = 134217728') # 128MB journal size for logs
                
        except Exception as e:
            logger.error(f"Error configuring logs database connection: {e}")
            pass
    
    def get_logs_connection(self):
        """Get a configured SQLite connection for logs database with thread-local caching and retry logic"""
        # Try to reuse thread-local cached connection
        cached_conn = getattr(self._thread_local, 'conn', None)
        if cached_conn is not None:
            try:
                cached_conn.execute("SELECT 1")
                return cached_conn
            except Exception:
                try:
                    cached_conn.close()
                except Exception:
                    pass
                self._thread_local.conn = None
        
        max_retries = 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self.db_path, timeout=30)
                self._configure_logs_connection(conn)
                # Test connection
                conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1").fetchone()
                self._thread_local.conn = conn
                return conn
            except (sqlite3.DatabaseError, sqlite3.OperationalError) as e:
                last_error = e
                error_str = str(e).lower()
                if "file is not a database" in error_str or "database disk image is malformed" in error_str:
                    if attempt < max_retries - 1:
                        # Try WAL recovery first
                        logger.warning(f"Logs database error on attempt {attempt + 1}: {e}. Trying WAL recovery...")
                        wal_path = Path(str(self.db_path) + "-wal")
                        if wal_path.exists():
                            try:
                                recovery_conn = sqlite3.connect(self.db_path, timeout=30)
                                recovery_conn.execute('PRAGMA journal_mode = WAL')
                                recovery_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                                recovery_conn.close()
                            except Exception:
                                pass
                        time.sleep(1)
                        continue
                    else:
                        logger.error(f"Logs database corruption confirmed: {e}")
                        self._handle_logs_database_corruption()
                        conn = sqlite3.connect(self.db_path, timeout=30)
                        self._configure_logs_connection(conn)
                        self._thread_local.conn = conn
                        return conn
                elif "database is locked" in error_str:
                    time.sleep(2)
                    continue
                else:
                    raise
        raise last_error if last_error else sqlite3.OperationalError("Failed to connect to logs database")
    
    def _handle_logs_database_corruption(self):
        """Handle logs database corruption — logs are non-critical so deletion is acceptable"""
        logger.error(f"Handling logs database corruption for: {self.db_path}")
        
        try:
            if self.db_path.exists():
                backup_path = self.db_path.parent / f"logs_corrupted_backup_{int(time.time())}.db"
                try:
                    shutil.copy2(self.db_path, backup_path)
                    logger.warning(f"Corrupted logs database backed up to: {backup_path}")
                except Exception:
                    pass
                
                self.db_path.unlink()
                # Clean up WAL/SHM files
                for suffix in ["-wal", "-shm"]:
                    p = Path(str(self.db_path) + suffix)
                    if p.exists():
                        p.unlink()
                logger.warning("Starting with fresh logs database - log history will be lost")
                
        except Exception as backup_error:
            logger.error(f"Error during logs database corruption recovery: {backup_error}")
            try:
                if self.db_path.exists():
                    self.db_path.unlink()
            except OSError:
                pass
    
    def ensure_logs_database_exists(self):
        """Create logs database and tables if they don't exist"""
        # Quick integrity check on startup
        if self.db_path.exists():
            try:
                conn = sqlite3.connect(self.db_path, timeout=30)
                conn.execute('PRAGMA busy_timeout = 30000')
                result = conn.execute("PRAGMA quick_check").fetchone()
                if result and result[0] != 'ok':
                    conn.close()
                    logger.error(f"Logs database quick_check failed on startup: {result[0]}. Recovering...")
                    self._handle_logs_database_corruption()
                else:
                    # Also test actual data read
                    try:
                        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
                        if 'logs' in tables:
                            conn.execute("SELECT id FROM logs LIMIT 1").fetchone()
                    except (sqlite3.DatabaseError, sqlite3.OperationalError) as data_err:
                        err_str = str(data_err).lower()
                        if "malformed" in err_str or "not a database" in err_str:
                            logger.error(f"Logs database data corruption on startup: {data_err}. Recovering...")
                            conn.close()
                            self._handle_logs_database_corruption()
                        else:
                            conn.close()
                    else:
                        conn.close()
            except (sqlite3.DatabaseError, sqlite3.OperationalError) as e:
                err_str = str(e).lower()
                if "malformed" in err_str or "not a database" in err_str:
                    logger.error(f"Logs database corruption on startup: {e}. Recovering...")
                    self._handle_logs_database_corruption()
        
        try:
            with self.get_logs_connection() as conn:
                # Create logs table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        level TEXT NOT NULL,
                        level_num INTEGER DEFAULT 20,
                        app_type TEXT NOT NULL,
                        message TEXT NOT NULL,
                        logger_name TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Add level_num column if it doesn't exist (for existing databases)
                try:
                    conn.execute('ALTER TABLE logs ADD COLUMN level_num INTEGER DEFAULT 20')
                    logger.info("Added level_num column to logs table")
                except sqlite3.OperationalError:
                    pass  # Column already exists
                
                # Create indexes for logs performance
                conn.execute('CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_logs_app_type ON logs(app_type)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_logs_level_num ON logs(level_num)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_logs_app_level ON logs(app_type, level)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_logs_app_level_num ON logs(app_type, level_num)')
                # Composite index for sorted pagination (most common query: filter by app + sort by time)
                conn.execute('CREATE INDEX IF NOT EXISTS idx_logs_app_timestamp ON logs(app_type, timestamp DESC)')
                
                conn.commit()
                logger.info(f"Logs database initialized at: {self.db_path}")
                
        except (sqlite3.DatabaseError, sqlite3.OperationalError) as e:
            if "file is not a database" in str(e) or "database disk image is malformed" in str(e):
                logger.error(f"Logs database corruption detected during table creation: {e}")
                self._handle_logs_database_corruption()
                # Try creating tables again after recovery
                self.ensure_logs_database_exists()
            else:
                raise
    
    def _get_level_num(self, level: str) -> int:
        """Map log level string to numeric value for inclusive filtering"""
        level_map = {
            'DEBUG': 10,
            'INFO': 20,
            'INFORMATION': 20,
            'WARNING': 30,
            'WARN': 30,
            'ERROR': 40,
            'CRITICAL': 50,
            'FATAL': 50
        }
        return level_map.get(level.upper(), 20)

    def insert_log(self, timestamp: datetime, level: str, app_type: str, message: str, logger_name: str = None):
        """Insert a log entry into the logs database. Skips insert if an identical entry (same second, app_type, level, message) already exists."""
        try:
            level_num = self._get_level_num(level)
            with self.get_logs_connection() as conn:
                # Normalize timestamp to second for duplicate check
                ts_str = timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp)
                ts_sec = (ts_str[:19].replace('T', ' ') if len(ts_str) >= 19 else ts_str)
                cur = conn.execute('''
                    SELECT 1 FROM logs
                    WHERE strftime('%Y-%m-%d %H:%M:%S', timestamp) = ?
                    AND app_type = ? AND level = ? AND message = ?
                    LIMIT 1
                ''', (ts_sec, app_type, level, message))
                if cur.fetchone() is not None:
                    return  # duplicate within same second, skip insert
                conn.execute('''
                    INSERT INTO logs (timestamp, level, level_num, app_type, message, logger_name)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (timestamp, level, level_num, app_type, message, logger_name))
                conn.commit()
        except sqlite3.DatabaseError as db_err:
            err_str = str(db_err).lower()
            if "malformed" in err_str or "not a database" in err_str or "disk i/o error" in err_str:
                # Trigger one-time recovery for corrupted logs DB
                if not getattr(self, '_logs_recovery_attempted', False):
                    self._logs_recovery_attempted = True
                    print(f"Logs database corruption detected, recovering: {db_err}")
                    try:
                        self._handle_logs_database_corruption()
                        self.ensure_logs_database_exists()
                        # Invalidate thread-local connection
                        if hasattr(self, '_thread_local') and hasattr(self._thread_local, 'conn'):
                            try:
                                self._thread_local.conn.close()
                            except Exception:
                                pass
                            self._thread_local.conn = None
                        print("Logs database recovered successfully")
                    except Exception as recovery_err:
                        print(f"Logs database recovery failed: {recovery_err}")
            else:
                print(f"Error inserting log: {db_err}")
        except Exception as e:
            # Don't let log insertion failures crash the app
            print(f"Error inserting log: {e}")
    
    def get_logs(self, app_type: str = None, level: str = None, limit: int = 100, offset: int = 0, search: str = None, exclude_app_types: List[str] = None) -> List[Dict[str, Any]]:
        """Get logs with filtering and pagination.
        exclude_app_types: when app_type is None (all), exclude these app types (e.g. ['movie_hunt'] for main logs).
        """
        try:
            with self.get_logs_connection() as conn:
                conn.row_factory = sqlite3.Row
                
                where_conditions = []
                params = []
                
                if app_type and app_type != "all":
                    base_apps = ["sonarr", "radarr", "lidarr", "readarr", "whisparr", "eros", "swaparr"]
                    app_lower = (app_type or "").lower()
                    # media_hunt = Movie Hunt + TV Hunt combined (default for Media Hunt Logs)
                    if app_lower == "media_hunt":
                        where_conditions.append("(app_type IN (?, ?))")
                        params.extend(["movie_hunt", "tv_hunt"])
                    # For cyclical apps, include per-instance logs (e.g. Sonarr-test, Sonarr-beta9)
                    elif app_lower in base_apps:
                        app_prefix = app_type.strip()[0:1].upper() + (app_type.strip()[1:].lower() if len(app_type) > 1 else "")
                        where_conditions.append("(app_type = ? OR app_type LIKE ?)")
                        params.extend([app_type, app_prefix + "-%"])
                    else:
                        where_conditions.append("app_type = ?")
                        params.append(app_type)
                elif exclude_app_types:
                    # Main logs "all" view: exclude independent modules (e.g. movie_hunt has its own Activity → Logs)
                    placeholders = ",".join("?" * len(exclude_app_types))
                    where_conditions.append(f"app_type NOT IN ({placeholders})")
                    params.extend(exclude_app_types)
                
                if level and level != "all":
                    # Use inclusive filtering: show selected level and above
                    level_num = self._get_level_num(level)
                    where_conditions.append("level_num >= ?")
                    params.append(level_num)
                
                if search:
                    where_conditions.append("message LIKE ?")
                    params.append(f"%{search}%")
                
                where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
                
                query = f"""
                    SELECT * FROM logs {where_clause}
                    ORDER BY timestamp DESC
                    LIMIT ? OFFSET ?
                """
                
                cursor = conn.execute(query, params + [limit, offset])
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting logs: {e}")
            return []
    
    def get_log_count(self, app_type: str = None, level: str = None, search: str = None, exclude_app_types: List[str] = None) -> int:
        """Get total count of logs matching filters.
        exclude_app_types: when app_type is None (all), exclude these app types (e.g. ['movie_hunt'] for main logs).
        """
        try:
            with self.get_logs_connection() as conn:
                where_conditions = []
                params = []
                
                if app_type and app_type != "all":
                    base_apps = ["sonarr", "radarr", "lidarr", "readarr", "whisparr", "eros", "swaparr"]
                    app_lower = (app_type or "").lower()
                    # media_hunt = Movie Hunt + TV Hunt combined (default for Media Hunt Logs)
                    if app_lower == "media_hunt":
                        where_conditions.append("(app_type IN (?, ?))")
                        params.extend(["movie_hunt", "tv_hunt"])
                    # For cyclical apps, include per-instance logs (e.g. Sonarr-test, Sonarr-beta9)
                    elif app_lower in base_apps:
                        app_prefix = app_type.strip()[0:1].upper() + (app_type.strip()[1:].lower() if len(app_type) > 1 else "")
                        where_conditions.append("(app_type = ? OR app_type LIKE ?)")
                        params.extend([app_type, app_prefix + "-%"])
                    else:
                        where_conditions.append("app_type = ?")
                        params.append(app_type)
                elif exclude_app_types:
                    # Main logs "all" view: exclude independent modules (e.g. movie_hunt has its own Activity → Logs)
                    placeholders = ",".join("?" * len(exclude_app_types))
                    where_conditions.append(f"app_type NOT IN ({placeholders})")
                    params.extend(exclude_app_types)
                
                if level and level != "all":
                    # Use inclusive filtering: show selected level and above
                    level_num = self._get_level_num(level)
                    where_conditions.append("level_num >= ?")
                    params.append(level_num)
                
                if search:
                    where_conditions.append("message LIKE ?")
                    params.append(f"%{search}%")
                
                where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
                
                query = f"SELECT COUNT(*) FROM logs {where_clause}"
                cursor = conn.execute(query, params)
                return cursor.fetchone()[0]
                
        except Exception as e:
            logger.error(f"Error getting log count: {e}")
            return 0
    
    def cleanup_old_logs(self, days_to_keep: int = 30, max_entries_per_app: int = 10000):
        """Clean up old logs to prevent database bloat"""
        try:
            with self.get_logs_connection() as conn:
                # Delete logs older than specified days
                cutoff_date = datetime.now() - timedelta(days=days_to_keep)
                cursor = conn.execute("DELETE FROM logs WHERE timestamp < ?", (cutoff_date,))
                deleted_by_age = cursor.rowcount
                
                # Keep only the most recent entries per app
                apps_cursor = conn.execute("SELECT DISTINCT app_type FROM logs")
                total_deleted_by_count = 0
                
                for (app_type,) in apps_cursor.fetchall():
                    # Get count for this app
                    count_cursor = conn.execute("SELECT COUNT(*) FROM logs WHERE app_type = ?", (app_type,))
                    count = count_cursor.fetchone()[0]
                    
                    if count > max_entries_per_app:
                        # Delete oldest entries beyond the limit
                        excess_count = count - max_entries_per_app
                        delete_cursor = conn.execute("""
                            DELETE FROM logs 
                            WHERE app_type = ? 
                            AND id IN (
                                SELECT id FROM logs 
                                WHERE app_type = ? 
                                ORDER BY timestamp ASC 
                                LIMIT ?
                            )
                        """, (app_type, app_type, excess_count))
                        total_deleted_by_count += delete_cursor.rowcount
                
                conn.commit()
                return deleted_by_age + total_deleted_by_count
                
        except Exception as e:
            logger.error(f"Error cleaning up logs: {e}")
            return 0
    
    def get_app_types_from_logs(self) -> List[str]:
        """Get list of all raw app types that have logs (e.g. Sonarr-test, sonarr, system)."""
        try:
            with self.get_logs_connection() as conn:
                cursor = conn.execute("SELECT DISTINCT app_type FROM logs ORDER BY app_type")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting app types from logs: {e}")
            return []

    def get_app_types(self) -> List[str]:
        """Get base app types for log filter dropdown (all, system, sonarr, radarr, ...). Normalizes Sonarr-test -> sonarr."""
        try:
            raw = self.get_app_types_from_logs()
            base = set()
            for at in raw:
                part = (at or "").split("-")[0].strip().lower()
                if part:
                    base.add(part)
            order = ["all", "system", "sonarr", "radarr", "lidarr", "readarr", "whisparr", "eros", "swaparr", "movie_hunt", "tv_hunt"]
            result = [a for a in order if a in base or (a == "all" and base)]
            if "all" not in result and base:
                result.insert(0, "all")
            for a in sorted(base):
                if a not in result:
                    result.append(a)
            return result if result else ["all"]
        except Exception as e:
            logger.error(f"Error getting app types: {e}")
            return ["all", "system"]

    def get_log_levels(self) -> List[str]:
        """Get list of all log levels that exist"""
        try:
            with self.get_logs_connection() as conn:
                cursor = conn.execute("SELECT DISTINCT level FROM logs ORDER BY level")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting log levels: {e}")
            return []
    
    def clear_logs(self, app_type: str = None, exclude_app_types: List[str] = None):
        """Clear logs for a specific app type or all logs. For cyclical apps, clears base and per-instance (e.g. sonarr, Sonarr-test).
        exclude_app_types: when clearing all (app_type None), do not delete these (e.g. ['movie_hunt'] for main logs clear).
        """
        try:
            with self.get_logs_connection() as conn:
                if app_type:
                    base_apps = ["sonarr", "radarr", "lidarr", "readarr", "whisparr", "eros", "swaparr"]
                    app_lower = (app_type or "").lower()
                    if app_lower == "media_hunt":
                        cursor = conn.execute("DELETE FROM logs WHERE app_type IN (?, ?)", ("movie_hunt", "tv_hunt"))
                    elif app_lower in base_apps:
                        app_prefix = app_type.strip()[0:1].upper() + (app_type.strip()[1:].lower() if len(app_type) > 1 else "")
                        cursor = conn.execute("DELETE FROM logs WHERE app_type = ? OR app_type LIKE ?", (app_type, app_prefix + "-%"))
                    else:
                        cursor = conn.execute("DELETE FROM logs WHERE app_type = ?", (app_type,))
                else:
                    if exclude_app_types:
                        placeholders = ",".join("?" * len(exclude_app_types))
                        cursor = conn.execute(f"DELETE FROM logs WHERE app_type NOT IN ({placeholders})", exclude_app_types)
                    else:
                        cursor = conn.execute("DELETE FROM logs")

                deleted_count = cursor.rowcount
                conn.commit()

                logger.info(f"Cleared {deleted_count} logs" + (f" for {app_type}" if app_type else " (main apps only)" if exclude_app_types else ""))
                return deleted_count
        except Exception as e:
            logger.error(f"Error clearing logs: {e}")
            return 0

# Global database instances (thread-safe initialization)
_database_instance = None
_logs_database_instance = None
_db_init_lock = threading.Lock()

def get_database() -> HuntarrDatabase:
    """Get the global database instance (thread-safe)"""
    global _database_instance
    if _database_instance is None:
        with _db_init_lock:
            if _database_instance is None:  # Double-check locking
                _database_instance = HuntarrDatabase()
    return _database_instance

def _reset_database_instances():
    """Reset global database singletons.
    
    Called after database deletion so the next get_database() call
    creates a fresh instance that will properly initialize a new database.
    """
    global _database_instance, _logs_database_instance
    with _db_init_lock:
        # Invalidate any cached connections on the current thread
        if _database_instance is not None:
            _database_instance.invalidate_connection()
        _database_instance = None
        _logs_database_instance = None
    logger.info("Database singleton instances reset")

# Logs Database Functions (consolidated from logs_database.py)
def get_logs_database() -> LogsDatabase:
    """Get the logs database instance for logs operations (thread-safe)"""
    global _logs_database_instance
    if _logs_database_instance is None:
        with _db_init_lock:
            if _logs_database_instance is None:  # Double-check locking
                _logs_database_instance = LogsDatabase()
    return _logs_database_instance

def schedule_log_cleanup():
    """Schedule periodic log cleanup - call this from background tasks"""
    import threading
    import time
    
    def cleanup_worker():
        """Background worker to clean up logs periodically"""
        while True:
            try:
                time.sleep(3600)  # Run every hour
                # Read user settings instead of using hardcoded values
                try:
                    import src.primary.settings_manager as sm
                    settings = sm.load_settings('general')
                    days = int(settings.get('log_retention_days', 30))
                    max_entries = int(settings.get('log_max_entries_per_app', 10000))
                    auto_cleanup = settings.get('log_auto_cleanup', True)
                except Exception:
                    days = 30
                    max_entries = 10000
                    auto_cleanup = True

                if not auto_cleanup:
                    continue

                logs_db = get_logs_database()
                deleted_count = logs_db.cleanup_old_logs(days_to_keep=days, max_entries_per_app=max_entries)
                if deleted_count > 0:
                    logger.info(f"Scheduled cleanup removed {deleted_count} old log entries (retention={days}d, max_per_app={max_entries})")
            except Exception as e:
                logger.error(f"Error in scheduled log cleanup: {e}")
    
    # Start cleanup thread
    cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
    cleanup_thread.start()
    logger.info("Scheduled log cleanup thread started")

# Manager Database Functions (consolidated from manager_database.py)
def get_manager_database() -> HuntarrDatabase:
    """Get the database instance for manager operations"""
    return get_database() 
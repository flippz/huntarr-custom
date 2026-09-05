"""
Regression tests for the SQLite corruption-recovery path in
src/primary/utils/database.py.

These guard against a real data-loss bug: recovery used to copy only the
main .db file (never the -wal sidecar) and then force journal_mode=OFF on
the backup copy, so any committed-but-not-yet-checkpointed WAL data was
silently dropped during "recovery". They also guard against treating a
transient "disk i/o error" as proof of corruption, and against a recovery
bug (row-count collapse) being masked by write_safe_snapshot silently
overwriting the last known-good snapshot.
"""

import os
import sys
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

# Importing src.primary.* has a side effect (unrelated to this diff): the
# package's logger module eagerly loads settings via get_database(), which
# would otherwise create a real HuntarrDatabase() against the user's actual
# config directory just by importing this test module. Point that at a
# throwaway temp directory before the first import so nothing ever touches
# a real /config or ~/Huntarr path.
_IMPORT_SIDE_EFFECT_DIR = tempfile.mkdtemp(prefix="huntarr_test_import_sideeffect_")
os.environ.setdefault("HUNTARR_CONFIG_DIR", _IMPORT_SIDE_EFFECT_DIR)

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.primary.utils.database import HuntarrDatabase  # noqa: E402


def _make_db(db_path: Path) -> HuntarrDatabase:
    """Build a HuntarrDatabase instance pointed at an isolated temp path,
    bypassing __init__'s real _get_database_path() resolution entirely."""
    db = HuntarrDatabase.__new__(HuntarrDatabase)
    db._thread_local = threading.local()
    db.db_path = db_path
    db.ensure_database_exists()
    return db


class WalCommittedDataSurvivesRecoveryTest(unittest.TestCase):
    """A database with committed-but-uncheckpointed WAL data must not lose
    that data when the destructive recovery path runs."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "huntarr.db"
        self.db = _make_db(self.db_path)
        self.wal_path = Path(str(self.db_path) + "-wal")

    def tearDown(self):
        self.db.invalidate_connection()
        self.tmpdir.cleanup()

    def test_wal_only_rows_survive_forced_recovery(self):
        # Pin a reader snapshot from before the inserts below. This is what
        # makes the upcoming PRAGMA wal_checkpoint(TRUNCATE) inside
        # _handle_database_corruption unable to fully flush the WAL into the
        # main .db file before the backup copy is made -- exactly the state
        # a real interrupted-checkpoint / concurrent-reader scenario leaves
        # behind, and the scenario the old code silently lost data in.
        reader = sqlite3.connect(str(self.db_path))
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM app_configs").fetchall()

        try:
            with self.db.get_connection() as conn:
                for i in range(5):
                    conn.execute(
                        "INSERT INTO app_configs (app_type, config_data) VALUES (?, ?)",
                        (f"app_{i}", f'{{"n": {i}}}'),
                    )

            self.assertTrue(self.wal_path.exists(), "expected a -wal sidecar to exist")
            self.assertGreater(
                self.wal_path.stat().st_size, 0,
                "expected committed rows to still be sitting in the WAL, unchecked",
            )

            # Mirror real call sites (_trigger_corruption_recovery, and the
            # final-attempt branch in get_connection()): invalidate the
            # thread-local connection before calling _handle_database_corruption
            # directly, so table creation on the rebuilt database can't
            # accidentally reuse a stale connection still bound to the
            # about-to-be-quarantined file.
            self.db.invalidate_connection()

            # Force the destructive path to run even though the data is
            # actually fine -- proves the WAL-aware backup+salvage logic
            # itself, independent of corruption *detection*.
            fake_diagnostics = {
                "ok": False,
                "quick_check": ["simulated"],
                "integrity_check": ["simulated corruption"],
                "error": None,
            }
            with mock.patch.object(
                self.db, "_run_integrity_diagnostics", return_value=fake_diagnostics
            ):
                self.db._handle_database_corruption()
        finally:
            reader.close()

        quarantined = list(Path(self.tmpdir.name).glob("huntarr_quarantined_*.db"))
        self.assertEqual(
            len(quarantined), 1,
            "expected the suspect database to be quarantined (renamed), not deleted",
        )

        backups = list(Path(self.tmpdir.name).glob("huntarr_corrupted_backup_*.db"))
        self.assertEqual(len(backups), 1, "expected a backup copy to be created")

        # The rebuilt live database must contain every row, including the
        # ones that only ever existed in the WAL sidecar.
        fresh_conn = sqlite3.connect(str(self.db_path))
        try:
            rows = fresh_conn.execute(
                "SELECT app_type FROM app_configs ORDER BY app_type"
            ).fetchall()
        finally:
            fresh_conn.close()
        self.assertEqual([r[0] for r in rows], [f"app_{i}" for i in range(5)])


class TransientDiskIoErrorDoesNotTriggerRecoveryTest(unittest.TestCase):
    """A healthy database hit with a transient 'disk i/o error' must never
    be quarantined or rebuilt -- that's a NAS/filesystem hiccup, not proof
    of corruption."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "huntarr.db"
        self.db = _make_db(self.db_path)

        with self.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO app_configs (app_type, config_data) VALUES (?, ?)",
                ("sonarr", '{"enabled": true}'),
            )
        self.db.invalidate_connection()

        self.original_bytes = self.db_path.read_bytes()

    def tearDown(self):
        self.db.invalidate_connection()
        self.tmpdir.cleanup()

    def test_disk_io_error_is_retried_and_raised_without_recovery(self):
        with mock.patch(
            "src.primary.utils.database.sqlite3.connect",
            side_effect=sqlite3.OperationalError("disk i/o error"),
        ), mock.patch.object(
            self.db, "_handle_database_corruption"
        ) as mock_handle_corruption, mock.patch.object(
            self.db, "_trigger_corruption_recovery"
        ) as mock_trigger_recovery:
            with self.assertRaises(sqlite3.OperationalError):
                self.db.get_connection()

        mock_handle_corruption.assert_not_called()
        mock_trigger_recovery.assert_not_called()

        quarantined = list(Path(self.tmpdir.name).glob("huntarr_quarantined_*.db*"))
        self.assertEqual(quarantined, [], "no quarantine file should ever be created")
        backups = list(Path(self.tmpdir.name).glob("huntarr_corrupted_backup_*.db*"))
        self.assertEqual(backups, [], "no backup/rebuild should ever be triggered")

        # The live database file itself must be byte-for-byte untouched.
        self.assertEqual(self.db_path.read_bytes(), self.original_bytes)

        # And the data is still readable/intact through a fresh connection.
        conn = sqlite3.connect(str(self.db_path))
        try:
            row = conn.execute(
                "SELECT app_type, config_data FROM app_configs WHERE app_type = ?",
                ("sonarr",),
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row, ("sonarr", '{"enabled": true}'))


class SafeSnapshotRefusesRowCountCollapseTest(unittest.TestCase):
    """write_safe_snapshot() must refuse to overwrite a healthy snapshot
    with one whose data has collapsed, so a bad recovery run can't quietly
    destroy the last known-good backup."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "huntarr.db"
        self.db = _make_db(self.db_path)

    def tearDown(self):
        self.db.invalidate_connection()
        self.tmpdir.cleanup()

    def test_refuses_to_overwrite_snapshot_on_collapse(self):
        with self.db.get_connection() as conn:
            for i in range(3):
                conn.execute(
                    "INSERT INTO app_configs (app_type, config_data) VALUES (?, ?)",
                    (f"app_{i}", "{}"),
                )

        self.assertTrue(self.db.write_safe_snapshot())
        snapshot_path = self.db._get_safe_snapshot_path()
        self.assertTrue(snapshot_path.exists())
        snapshot_bytes_before = snapshot_path.read_bytes()

        # Simulate the exact failure mode this guards against: the live
        # database gets emptied (e.g. by a bad recovery run).
        with self.db.get_connection() as conn:
            conn.execute("DELETE FROM app_configs")

        result = self.db.write_safe_snapshot()
        self.assertFalse(result, "write_safe_snapshot should refuse a collapsed write")

        self.assertEqual(
            snapshot_path.read_bytes(), snapshot_bytes_before,
            "the previous good snapshot must be left byte-identical",
        )

        # No stray .tmp file should be left behind either.
        tmp_leftovers = list(Path(self.tmpdir.name).glob("*.tmp"))
        self.assertEqual(tmp_leftovers, [])


if __name__ == "__main__":
    unittest.main()

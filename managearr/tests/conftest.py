import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

from app import create_app
from app.persistence.database import Database
from app.persistence.migrations import run_migrations
from app.persistence.library_repository import LibraryRepository
from app.persistence.policy_repository import PolicyRepository
from app.persistence.activity_repository import ActivityRepository
from app.persistence.scan_candidate_repository import ScanCandidateRepository

# Tests run against a real PostgreSQL instance - no SQLite/mock DB layer.
# Point MANAGEARR_TEST_DB_* at a disposable database; the suite truncates
# its tables before every test, so nothing else should use this database.
TEST_DB_CONFIG = dict(
    host=os.environ.get("MANAGEARR_TEST_DB_HOST", "127.0.0.1"),
    port=int(os.environ.get("MANAGEARR_TEST_DB_PORT", "5432")),
    dbname=os.environ.get("MANAGEARR_TEST_DB_NAME", "managearr_test"),
    user=os.environ.get("MANAGEARR_TEST_DB_USER", "managearr_test"),
    password=os.environ.get("MANAGEARR_TEST_DB_PASSWORD", "testpass123"),
)

_DATA_TABLES = "arr_libraries, automation_policy, activity_jobs, scan_candidates"


@pytest.fixture(scope="session")
def _migrated_db():
    db = Database(**TEST_DB_CONFIG, sslmode="disable", min_size=1, max_size=5, connect_timeout=5)
    try:
        db.wait_ready(timeout_seconds=10)
    except Exception as exc:
        db.close()
        pytest.skip(
            "No reachable PostgreSQL test database "
            f"({TEST_DB_CONFIG['host']}:{TEST_DB_CONFIG['port']}/{TEST_DB_CONFIG['dbname']}): {exc}"
        )
    run_migrations(db)
    yield db
    db.close()


@pytest.fixture(scope="session")
def _app(_migrated_db):
    application = create_app(
        {
            "DB_HOST": TEST_DB_CONFIG["host"],
            "DB_PORT": TEST_DB_CONFIG["port"],
            "DB_NAME": TEST_DB_CONFIG["dbname"],
            "DB_USER": TEST_DB_CONFIG["user"],
            "DB_PASSWORD": TEST_DB_CONFIG["password"],
            "DB_SSLMODE": "disable",
            "DB_STARTUP_TIMEOUT_SECONDS": 10,
            "TESTING": True,
        }
    )
    yield application
    application.extensions["managearr"]["db"].close()


@pytest.fixture
def _reset_tables(_migrated_db):
    with _migrated_db.connect() as conn:
        conn.execute(f"TRUNCATE {_DATA_TABLES} RESTART IDENTITY CASCADE")


@pytest.fixture
def app(_app, _reset_tables):
    return _app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def database(_migrated_db, _reset_tables):
    return _migrated_db


@pytest.fixture
def library_repo(database):
    return LibraryRepository(database)


@pytest.fixture
def policy_repo(database):
    return PolicyRepository(database)


@pytest.fixture
def activity_repo(database):
    return ActivityRepository(database)


@pytest.fixture
def candidate_repo(database):
    return ScanCandidateRepository(database)

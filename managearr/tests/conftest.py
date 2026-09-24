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
from app.persistence.dispatch_repository import DispatchRepository
from app.persistence.outcome_repository import OutcomeRepository
from app.persistence.scheduler_repository import SchedulerRepository
from app.persistence.refresh_repository import RefreshRepository
from app.persistence.live_repository import LiveRepository
from app.services.dispatch_planning_service import DispatchPlanningService
from app.services.dispatch_service import DispatchService
from app.services.reconciliation_service import ReconciliationService
from app.services.scheduler_service import SchedulerService
from app.services.refresh_service import RefreshService
from app.services.refresh_settings_service import RefreshSettingsService
from app.services.live_control_service import LiveControlService
from app.services.live_dispatch_coordinator import LiveDispatchCoordinator

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

_DATA_TABLES = (
    "arr_libraries, automation_policy, activity_jobs, scan_candidates, "
    "dispatch_batches, dispatch_batch_items, dispatch_reconciliation_attempts, dispatch_outcome_events, "
    "scheduler_candidate_results, scheduler_library_results, scheduler_cycle_runs, "
    "scheduler_run_requests, scheduler_mode_audit, "
    "refresh_run_reconciled_batches, refresh_runs, refresh_requests, "
    "live_dispatch_ledger, live_control_audit, live_challenges"
)


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
        conn.execute("UPDATE scheduler_settings SET mode = 'off', next_due_at = NULL, updated_at = now()")
        conn.execute(
            "UPDATE refresh_settings SET scan_max_age_minutes = 60, "
            "reconcile_min_interval_minutes = 15, reconcile_max_per_cycle = 10, "
            "next_reconcile_due_at = NULL, updated_at = now()"
        )
        conn.execute(f"TRUNCATE {_DATA_TABLES} RESTART IDENTITY CASCADE")
        conn.execute(
            "UPDATE live_control SET armed = FALSE, arm_generation = 0, armed_at = NULL, "
            "armed_by = NULL, armed_reason = NULL, expires_at = NULL, emergency_stopped_at = NULL, "
            "emergency_stop_reason = NULL, emergency_stop_generation = 0, max_dispatches_per_cycle = 1, "
            "min_delay_seconds_between_dispatches = 30, default_arm_ttl_minutes = 15, "
            "max_arm_ttl_minutes = 60, last_dispatch_at = NULL, last_dispatch_summary = '', updated_at = now()"
        )
        conn.execute(
            "UPDATE scheduler_leases SET owner_id = NULL, acquired_at = NULL, "
            "heartbeat_at = NULL, expires_at = NULL"
        )


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


@pytest.fixture
def dispatch_repo(database):
    return DispatchRepository(database)


@pytest.fixture
def outcome_repo(database):
    return OutcomeRepository(database)


@pytest.fixture
def dispatch_planning_service(activity_repo, candidate_repo, library_repo, policy_repo, dispatch_repo):
    return DispatchPlanningService(activity_repo, candidate_repo, library_repo, policy_repo, dispatch_repo)


@pytest.fixture
def dispatch_service(dispatch_planning_service, dispatch_repo, library_repo):
    return DispatchService(dispatch_planning_service, dispatch_repo, library_repo)


@pytest.fixture
def scheduler_repo(database):
    return SchedulerRepository(database)


@pytest.fixture
def refresh_repo(database):
    return RefreshRepository(database)


@pytest.fixture
def scheduler_service(scheduler_repo, policy_repo, refresh_repo):
    return SchedulerService(scheduler_repo, policy_repo, refresh_repo)


@pytest.fixture
def reconciliation_service(dispatch_repo, outcome_repo, library_repo, activity_repo, candidate_repo):
    return ReconciliationService(
        dispatch_repo, outcome_repo, library_repo, activity_repo, candidate_repo
    )


@pytest.fixture
def refresh_settings_service(refresh_repo):
    return RefreshSettingsService(refresh_repo)


@pytest.fixture
def refresh_service(refresh_repo, library_repo, activity_repo, candidate_repo, dispatch_repo, outcome_repo):
    return RefreshService(refresh_repo, library_repo, activity_repo, candidate_repo, dispatch_repo, outcome_repo)


@pytest.fixture
def live_repo(database):
    return LiveRepository(database)


@pytest.fixture
def live_control_service(live_repo, scheduler_repo, policy_repo):
    return LiveControlService(live_repo, scheduler_repo, policy_repo)


@pytest.fixture
def live_coordinator(live_repo, scheduler_repo, dispatch_service):
    return LiveDispatchCoordinator(live_repo, scheduler_repo, dispatch_service)

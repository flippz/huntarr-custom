"""Managearr v1 application factory.

Layering:
  domain/      - plain dataclasses + validation, no I/O
  services/    - use-case orchestration over domain + persistence
  adapters/    - boundary transforms (e.g. secret redaction)
  persistence/ - PostgreSQL schema/migrations + repositories
  api/         - JSON API blueprint (/api/v1/*)
  web/         - server-rendered UI shell
"""
from flask import Flask, jsonify

from .persistence.database import Database
from .persistence.migrations import run_migrations
from .persistence.library_repository import LibraryRepository
from .persistence.policy_repository import PolicyRepository
from .persistence.activity_repository import ActivityRepository
from .persistence.scan_candidate_repository import ScanCandidateRepository
from .persistence.dispatch_repository import DispatchRepository
from .persistence.outcome_repository import OutcomeRepository
from .persistence.scheduler_repository import SchedulerRepository
from .services.library_service import LibraryService
from .services.policy_service import PolicyService
from .services.activity_service import ActivityService
from .services.status_service import StatusService
from .services.sonarr_scan_service import SonarrScanService
from .services.scan_candidate_service import ScanCandidateService
from .services.dispatch_planning_service import DispatchPlanningService
from .services.dispatch_service import DispatchService
from .services.reconciliation_service import ReconciliationService
from .services.scheduler_service import SchedulerService
from .api import api_bp
from .web import web_bp


def create_app(config: dict | None = None) -> Flask:
    # Static assets are served by the web blueprint (app/web/static); the
    # app itself has no top-level static folder to avoid a route clash.
    app = Flask(__name__, static_folder=None)
    app.config.update(config or {})

    db = Database(
        host=app.config.get("DB_HOST", "postgres"),
        port=app.config.get("DB_PORT", 5432),
        dbname=app.config.get("DB_NAME", "managearr"),
        user=app.config.get("DB_USER", "managearr"),
        password=app.config.get("DB_PASSWORD"),
        sslmode=app.config.get("DB_SSLMODE", "prefer"),
        min_size=app.config.get("DB_POOL_MIN_SIZE", 1),
        max_size=app.config.get("DB_POOL_MAX_SIZE", 5),
        connect_timeout=app.config.get("DB_CONNECT_TIMEOUT_SECONDS", 5),
    )
    # Bounded startup retry/backoff - raises DatabaseUnavailableError (a
    # safe, static message) if PostgreSQL never becomes reachable.
    db.wait_ready(timeout_seconds=app.config.get("DB_STARTUP_TIMEOUT_SECONDS", 30))
    run_migrations(db)

    library_repo = LibraryRepository(db)
    policy_repo = PolicyRepository(db)
    activity_repo = ActivityRepository(db)
    candidate_repo = ScanCandidateRepository(db)
    dispatch_repo = DispatchRepository(db)
    outcome_repo = OutcomeRepository(db)
    scheduler_repo = SchedulerRepository(db)

    sonarr_timeout = app.config.get("SONARR_TIMEOUT_SECONDS")

    dispatch_planning = DispatchPlanningService(
        activity_repo, candidate_repo, library_repo, policy_repo, dispatch_repo
    )

    app.extensions["managearr"] = {
        "db": db,
        "library": LibraryService(library_repo),
        "policy": PolicyService(policy_repo),
        "activity": ActivityService(activity_repo),
        "status": StatusService(db, library_repo),
        "sonarr_scan": SonarrScanService(
            library_repo, activity_repo, candidate_repo, timeout=sonarr_timeout
        ),
        "scan_candidate": ScanCandidateService(candidate_repo),
        "dispatch_repo": dispatch_repo,
        "outcome_repo": outcome_repo,
        "scheduler_repo": scheduler_repo,
        "scheduler": SchedulerService(scheduler_repo, policy_repo),
        "dispatch_planning": dispatch_planning,
        "dispatch": DispatchService(
            dispatch_planning, dispatch_repo, library_repo, timeout=sonarr_timeout
        ),
        "reconciliation": ReconciliationService(
            dispatch_repo,
            outcome_repo,
            library_repo,
            activity_repo,
            candidate_repo,
            timeout=sonarr_timeout,
        ),
    }

    app.register_blueprint(api_bp)
    app.register_blueprint(web_bp)

    @app.get("/health")
    def health():
        result = app.extensions["managearr"]["status"].health()
        status_code = 200 if result["status"] == "ok" else 503
        return jsonify(result), status_code

    return app

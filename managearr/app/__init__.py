"""Managearr v1 application factory.

Layering:
  domain/      - plain dataclasses + validation, no I/O
  services/    - use-case orchestration over domain + persistence
  adapters/    - boundary transforms (e.g. secret redaction)
  persistence/ - SQLite schema + repositories
  api/         - JSON API blueprint (/api/v1/*)
  web/         - server-rendered UI shell
"""
from flask import Flask, jsonify

from .persistence.database import Database
from .persistence.library_repository import LibraryRepository
from .persistence.policy_repository import PolicyRepository
from .persistence.activity_repository import ActivityRepository
from .persistence.scan_candidate_repository import ScanCandidateRepository
from .services.library_service import LibraryService
from .services.policy_service import PolicyService
from .services.activity_service import ActivityService
from .services.status_service import StatusService
from .services.sonarr_scan_service import SonarrScanService
from .services.scan_candidate_service import ScanCandidateService
from .api import api_bp
from .web import web_bp


def create_app(config: dict | None = None) -> Flask:
    # Static assets are served by the web blueprint (app/web/static); the
    # app itself has no top-level static folder to avoid a route clash.
    app = Flask(__name__, static_folder=None)
    app.config.update(config or {})

    db_path = app.config.get("DB_PATH", "data-managearr/managearr.db")
    db = Database(db_path)
    db.init_schema()

    library_repo = LibraryRepository(db)
    policy_repo = PolicyRepository(db)
    activity_repo = ActivityRepository(db)
    candidate_repo = ScanCandidateRepository(db)

    sonarr_timeout = app.config.get("SONARR_TIMEOUT_SECONDS")

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
    }

    app.register_blueprint(api_bp)
    app.register_blueprint(web_bp)

    @app.get("/health")
    def health():
        result = app.extensions["managearr"]["status"].health()
        status_code = 200 if result["status"] == "ok" else 503
        return jsonify(result), status_code

    return app

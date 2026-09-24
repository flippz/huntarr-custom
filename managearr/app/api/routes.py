"""JSON API blueprint: /api/v1/*"""
from flask import Blueprint, current_app, jsonify, request

from ..adapters.redaction import redact_libraries, redact_library
from ..services.sonarr_scan_service import VALIDATION_ERRORS
from ..services.dispatch_planning_service import JOB_NOT_FOUND_ERROR
from ..services.reconciliation_service import BATCH_NOT_FOUND

api_bp = Blueprint("api_v1", __name__, url_prefix="/api/v1")


def _services():
    return current_app.extensions["managearr"]


def _sonarr_error_status(error: str) -> int:
    if error == "library not found":
        return 404
    if error in VALIDATION_ERRORS:
        return 400
    # Anything else is an upstream Sonarr failure (unreachable, timeout,
    # auth rejected, malformed data) - not the caller's fault.
    return 502


def _dispatch_error_status(error: str) -> int:
    if error in (JOB_NOT_FOUND_ERROR, "library not found"):
        return 404
    # Every other error this layer can return (bad job state, bad
    # selection shape, missing confirm, cap exceeded) is a caller
    # mistake, not an upstream failure - dispatch/preview never call
    # Sonarr except via the one confirmed EpisodeSearch POST, whose
    # failures are recorded on the batch, not raised as an API error.
    return 400


@api_bp.get("/status")
def status():
    return jsonify(_services()["status"].status())


# --- Libraries ---------------------------------------------------------

@api_bp.get("/libraries")
def list_libraries():
    libraries = _services()["library"].list_libraries()
    return jsonify({"libraries": redact_libraries(libraries)})


@api_bp.post("/libraries")
def create_library():
    payload = request.get_json(silent=True) or {}
    library, errors = _services()["library"].create_library(payload)
    if errors:
        return jsonify({"errors": errors}), 400
    return jsonify({"library": redact_library(library)}), 201


@api_bp.get("/libraries/<int:library_id>")
def get_library(library_id: int):
    library = _services()["library"].get_library(library_id)
    if library is None:
        return jsonify({"errors": ["library not found"]}), 404
    return jsonify({"library": redact_library(library)})


@api_bp.put("/libraries/<int:library_id>")
@api_bp.patch("/libraries/<int:library_id>")
def update_library(library_id: int):
    payload = request.get_json(silent=True) or {}
    library, errors = _services()["library"].update_library(library_id, payload)
    if errors:
        status_code = 404 if errors == ["library not found"] else 400
        return jsonify({"errors": errors}), status_code
    return jsonify({"library": redact_library(library)})


@api_bp.delete("/libraries/<int:library_id>")
def delete_library(library_id: int):
    deleted = _services()["library"].delete_library(library_id)
    if not deleted:
        return jsonify({"errors": ["library not found"]}), 404
    return "", 204


# --- Automation policy ---------------------------------------------------

@api_bp.get("/policy")
def get_policy():
    policy_service = _services()["policy"]
    policy = policy_service.get_policy()
    return jsonify({"policy": policy.to_dict(), "summary": policy_service.summary(policy)})


@api_bp.put("/policy")
@api_bp.patch("/policy")
def update_policy():
    payload = request.get_json(silent=True) or {}
    policy_service = _services()["policy"]
    policy, errors = policy_service.update_policy(payload)
    if errors:
        return jsonify({"errors": errors}), 400
    return jsonify({"policy": policy.to_dict(), "summary": policy_service.summary(policy)})


# --- Activity (read-only) ------------------------------------------------

@api_bp.get("/activity")
def list_activity():
    state = request.args.get("state")
    limit = request.args.get("limit", default=100, type=int)
    jobs = _services()["activity"].list_jobs(state=state, limit=limit)
    return jsonify({"jobs": [job.to_dict() for job in jobs]})


@api_bp.get("/activity/<int:job_id>")
def get_activity(job_id: int):
    job = _services()["activity"].get_job(job_id)
    if job is None:
        return jsonify({"errors": ["activity job not found"]}), 404
    return jsonify({"job": job.to_dict()})


@api_bp.get("/activity/<int:job_id>/candidates")
def get_activity_candidates(job_id: int):
    job = _services()["activity"].get_job(job_id)
    if job is None:
        return jsonify({"errors": ["activity job not found"]}), 404
    candidates = _services()["scan_candidate"].list_for_job(job_id)
    return jsonify({"candidates": [c.to_dict() for c in candidates]})


# --- Sonarr connection test + read-only candidate scan --------------------
#
# Both endpoints only ever issue GET requests against Sonarr (via
# SonarrScanService / SonarrClient). Neither sends a Sonarr command or
# mutates Sonarr state - see app/adapters/sonarr_client.py.

@api_bp.post("/libraries/<int:library_id>/test")
def test_library_connection(library_id: int):
    status_info, error = _services()["sonarr_scan"].test_connection(library_id)
    if error:
        return jsonify({"errors": [error]}), _sonarr_error_status(error)
    return jsonify({"status": status_info})


@api_bp.post("/libraries/<int:library_id>/scan")
def scan_library(library_id: int):
    job, error = _services()["sonarr_scan"].run_scan(library_id)
    if error:
        return jsonify({"errors": [error]}), _sonarr_error_status(error)
    return jsonify({"job": job.to_dict()}), 201


# --- Manual Sonarr search dispatch ----------------------------------------
#
# preview/plan never calls Sonarr - see DispatchPlanningService. dispatch
# is the only endpoint in this codebase that can make Sonarr do
# something, and only when the request body explicitly sets
# confirm: true with a non-empty candidate_ids list - see DispatchService.

@api_bp.post("/activity/<int:job_id>/dispatch/preview")
def preview_dispatch(job_id: int):
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        payload = {}
    candidate_ids = payload.get("candidate_ids")
    result, error = _services()["dispatch_planning"].preview(job_id, candidate_ids)
    if error:
        return jsonify({"errors": [error]}), _dispatch_error_status(error)
    batch = _services()["dispatch_repo"].get_batch(result.audit_batch_id)
    return jsonify({"plan": result.to_dict(), "batch": batch.to_dict()}), 201


@api_bp.post("/activity/<int:job_id>/dispatch")
def dispatch_searches(job_id: int):
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        payload = {}
    candidate_ids = payload.get("candidate_ids")
    confirm = payload.get("confirm")
    outcome, error = _services()["dispatch"].dispatch(job_id, candidate_ids, confirm)
    if error:
        return jsonify({"errors": [error]}), _dispatch_error_status(error)
    response = {"batch": outcome.batch.to_dict()}
    if outcome.plan is not None:
        response["plan"] = outcome.plan.to_dict()
    return jsonify(response), 201


@api_bp.get("/activity/<int:job_id>/dispatch-batches")
def list_dispatch_batches(job_id: int):
    job = _services()["activity"].get_job(job_id)
    if job is None:
        return jsonify({"errors": ["activity job not found"]}), 404
    batches = _services()["dispatch_repo"].list_for_job(job_id)
    return jsonify({"batches": [b.to_dict() for b in batches]})


@api_bp.get("/dispatch-batches/<int:batch_id>")
def get_dispatch_batch(batch_id: int):
    batch = _services()["dispatch_repo"].get_batch(batch_id)
    if batch is None:
        return jsonify({"errors": ["dispatch batch not found"]}), 404
    return jsonify({"batch": batch.to_dict()})


# --- Manual, read-only outcome reconciliation ----------------------------

@api_bp.post("/dispatch-batches/<int:batch_id>/reconcile")
def reconcile_dispatch_batch(batch_id: int):
    result, error = _services()["reconciliation"].reconcile(batch_id)
    if error:
        if result is not None:
            # The safe upstream failure is itself durably audited.
            return jsonify({"errors": [error], "reconciliation": result.to_dict()}), 502
        if error == BATCH_NOT_FOUND:
            return jsonify({"errors": [error]}), 404
        return jsonify({"errors": [error]}), 400
    return jsonify({"reconciliation": result.to_dict()})


@api_bp.get("/dispatch-batches/<int:batch_id>/outcomes")
def get_dispatch_outcomes(batch_id: int):
    detail = _services()["reconciliation"].detail(batch_id)
    if detail is None:
        return jsonify({"errors": [BATCH_NOT_FOUND]}), 404
    return jsonify({"outcomes": detail})

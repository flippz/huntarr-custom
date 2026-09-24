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


# --- Simulation scheduler -----------------------------------------------

@api_bp.get("/scheduler")
@api_bp.get("/scheduler/settings")
def get_scheduler_settings():
    return jsonify({"scheduler": _services()["scheduler"].settings()})


@api_bp.patch("/scheduler")
@api_bp.patch("/scheduler/settings")
def update_scheduler_settings():
    payload = request.get_json(silent=True)
    settings, errors = _services()["scheduler"].update_settings(payload)
    if errors:
        return jsonify({"errors": errors}), 400
    return jsonify({"settings": settings, "scheduler": _services()["scheduler"].settings()})


@api_bp.post("/scheduler/run-simulation-now")
def run_simulation_now():
    queued, created = _services()["scheduler"].queue_manual()
    return jsonify({"request": queued, "created": created}), 202


@api_bp.get("/scheduler/cycles")
def list_scheduler_cycles():
    limit = request.args.get("limit", default=50, type=int)
    return jsonify({"cycles": _services()["scheduler"].list_cycles(limit)})


@api_bp.get("/scheduler/cycles/<int:cycle_id>")
def get_scheduler_cycle(cycle_id: int):
    cycle = _services()["scheduler"].cycle_detail(cycle_id)
    if cycle is None:
        return jsonify({"errors": ["scheduler cycle not found"]}), 404
    ledger = _services()["live_repo"].ledger_for_cycle(cycle_id)
    if ledger:
        cycle["live_dispatch_ledger"] = ledger
    return jsonify({"cycle": cycle})


# --- Controlled live dispatch (M6) ---------------------------------------
#
# Every endpoint below only ever changes scheduler mode or live_control
# state (armed/disarmed/emergency-stopped) - none of them import
# DispatchService or a Sonarr adapter, and none of them can themselves send
# a Sonarr command. Scheduled live dispatch only ever executes from
# app/worker.py's LiveDispatchCoordinator, gated on mode=live AND a valid
# unexpired arm - see LiveControlService/LiveRepository.

@api_bp.get("/scheduler/live/status")
def live_status():
    return jsonify({"live": _services()["live_control"].status()})


@api_bp.post("/scheduler/live/mode-challenge")
def live_mode_challenge():
    return jsonify(_services()["live_control"].request_mode_challenge()), 201


@api_bp.post("/scheduler/live/mode-confirm")
def live_mode_confirm():
    payload = request.get_json(silent=True) or {}
    actor = request.headers.get("X-Managearr-Actor", "operator")[:255]
    result, errors = _services()["live_control"].confirm_mode_challenge(payload, actor=actor)
    if errors:
        return jsonify({"errors": errors}), 400
    return jsonify({"result": result, "live": _services()["live_control"].status()})


@api_bp.post("/scheduler/live/arm-challenge")
def live_arm_challenge():
    payload = request.get_json(silent=True) or {}
    result, errors = _services()["live_control"].request_arm_challenge(payload)
    if errors:
        return jsonify({"errors": errors}), 400
    return jsonify(result), 201


@api_bp.post("/scheduler/live/arm-confirm")
def live_arm_confirm():
    payload = request.get_json(silent=True) or {}
    actor = request.headers.get("X-Managearr-Actor", "operator")[:255]
    result, errors = _services()["live_control"].confirm_arm_challenge(payload, actor=actor)
    if errors:
        return jsonify({"errors": errors}), 400
    return jsonify({"control": result, "live": _services()["live_control"].status()})


@api_bp.post("/scheduler/live/disarm")
def live_disarm():
    payload = request.get_json(silent=True) or {}
    actor = request.headers.get("X-Managearr-Actor", "operator")[:255]
    control = _services()["live_control"].disarm(payload, actor=actor)
    return jsonify({"control": control, "live": _services()["live_control"].status()})


@api_bp.post("/scheduler/live/emergency-stop")
def live_emergency_stop():
    payload = request.get_json(silent=True) or {}
    actor = request.headers.get("X-Managearr-Actor", "operator")[:255]
    result = _services()["live_control"].emergency_stop(payload, actor=actor)
    return jsonify({"result": result, "live": _services()["live_control"].status()})


# --- Read-only scheduled refresh/reconciliation (M5) ---------------------
#
# These endpoints only ever queue durable work for the worker in
# app/worker.py; nothing here calls Sonarr. See RefreshService for the
# read-only scan/reconciliation execution and its ReadOnlySonarrClient guard.

@api_bp.get("/refresh")
@api_bp.get("/refresh/settings")
def get_refresh_settings():
    return jsonify({"refresh": _services()["refresh"].status()})


@api_bp.patch("/refresh")
@api_bp.patch("/refresh/settings")
def update_refresh_settings():
    payload = request.get_json(silent=True)
    settings, errors = _services()["refresh"].update_settings(payload)
    if errors:
        return jsonify({"errors": errors}), 400
    return jsonify({"settings": settings, "refresh": _services()["refresh"].status()})


@api_bp.post("/refresh/run-now")
def run_refresh_now():
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        payload = {}
    library_id = payload.get("library_id")
    if library_id is not None and (not isinstance(library_id, int) or isinstance(library_id, bool)):
        return jsonify({"errors": ["library_id must be an integer"]}), 400
    results, error = _services()["refresh"].queue_manual_scan(library_id)
    if error:
        return jsonify({"errors": [error]}), 404
    return jsonify({"requests": results}), 202


@api_bp.get("/refresh/runs")
def list_refresh_runs():
    limit = request.args.get("limit", default=50, type=int)
    return jsonify({"runs": _services()["refresh"].list_runs(limit)})


@api_bp.get("/refresh/runs/<int:run_id>")
def get_refresh_run(run_id: int):
    run = _services()["refresh"].run_detail(run_id)
    if run is None:
        return jsonify({"errors": ["refresh run not found"]}), 404
    return jsonify({"run": run})


# --- Activity (read-only) ------------------------------------------------

@api_bp.get("/activity")
def list_activity():
    state = request.args.get("state")
    limit = request.args.get("limit", default=100, type=int)
    jobs = _services()["activity"].list_jobs(state=state, limit=limit)
    return jsonify({"jobs": [job.to_dict() for job in jobs]})


@api_bp.get("/activity/dispatches")
def list_recent_dispatches():
    limit = max(1, min(request.args.get("limit", default=50, type=int), 100))
    batches = _services()["dispatch_repo"].list_recent(limit=limit)
    return jsonify({"batches": [batch.to_dict() for batch in batches]})


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

"""JSON API blueprint: /api/v2/*"""
from flask import Blueprint, current_app, jsonify, request

from ..adapters.redaction import redact_libraries, redact_library

api_bp = Blueprint("api_v2", __name__, url_prefix="/api/v2")


def _services():
    return current_app.extensions["huntarr"]


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

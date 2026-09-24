"""Server-rendered UI shell. Pages fetch live data from /api/v2/* via
static/app.js - these views just render the page frame.
"""
from flask import Blueprint, render_template

from ..domain.arr_library import ARR_TYPES
from ..domain.automation_policy import SEARCH_ORDERS
from ..domain.activity import JOB_STATES

web_bp = Blueprint("web", __name__, template_folder="templates", static_folder="static")


@web_bp.get("/")
def overview():
    return render_template("overview.html", active="overview")


@web_bp.get("/libraries")
def libraries():
    return render_template(
        "libraries.html", active="libraries", arr_types=ARR_TYPES
    )


@web_bp.get("/activity")
def activity():
    return render_template(
        "activity.html", active="activity", job_states=JOB_STATES
    )


@web_bp.get("/settings")
def settings():
    return render_template(
        "settings.html", active="settings", search_orders=SEARCH_ORDERS
    )

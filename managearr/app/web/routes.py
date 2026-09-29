"""Server-rendered UI shell. Pages fetch live data from /api/v1/* via
static/app.js - these views just render the page frame.
"""
from flask import Blueprint, redirect, render_template, url_for

from ..domain.automation_policy import SEARCH_ORDERS
from ..domain.activity import JOB_STATES

web_bp = Blueprint("web", __name__, template_folder="templates", static_folder="static")


@web_bp.get("/")
def overview():
    return render_template("home.html", active="home")


@web_bp.get("/sonarr")
def sonarr():
    return render_template("sonarr.html", active="sonarr", search_orders=SEARCH_ORDERS)


@web_bp.get("/libraries")
def libraries():
    # Pre-Huntarr-alignment bookmark: the generic Libraries page is now the
    # Sonarr workspace's Instances tab.
    return redirect(url_for("web.sonarr"), code=302)


@web_bp.get("/season-packs")
def season_packs():
    # Season packs are no longer a standalone top-level page - they are a
    # tab inside the Sonarr workspace.
    return redirect(url_for("web.sonarr", tab="season-packs"), code=302)


@web_bp.get("/activity")
def activity():
    return render_template(
        "activity.html", active="activity", job_states=JOB_STATES
    )


@web_bp.get("/settings")
def settings():
    return render_template("settings.html", active="settings")

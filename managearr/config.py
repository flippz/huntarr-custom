"""Runtime configuration for Managearr v1, sourced entirely from the
environment so the container/compose file is the single source of
truth for paths and ports.
"""
import os


class Config:
    DB_PATH = os.environ.get("MANAGEARR_DB_PATH", "data-managearr/managearr.db")
    HOST = os.environ.get("MANAGEARR_HOST", "0.0.0.0")
    PORT = int(os.environ.get("MANAGEARR_PORT", "9706"))
    DEBUG = os.environ.get("MANAGEARR_DEBUG", "false").lower() == "true"
    SONARR_TIMEOUT_SECONDS = int(os.environ.get("MANAGEARR_SONARR_TIMEOUT_SECONDS", "10"))

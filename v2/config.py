"""Runtime configuration for Huntarr v2, sourced entirely from the
environment so the container/compose file is the single source of
truth for paths and ports.
"""
import os


class Config:
    DB_PATH = os.environ.get("HUNTARR_V2_DB_PATH", "data-v2/huntarr_v2.db")
    HOST = os.environ.get("HUNTARR_V2_HOST", "0.0.0.0")
    PORT = int(os.environ.get("HUNTARR_V2_PORT", "9706"))
    DEBUG = os.environ.get("HUNTARR_V2_DEBUG", "false").lower() == "true"

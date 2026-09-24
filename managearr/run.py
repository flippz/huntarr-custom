#!/usr/bin/env python3
"""Entry point for Managearr v1. Dev-server only for this preview
milestone - see MANAGEARR.md."""
from app import create_app
from config import Config

app = create_app({"DB_PATH": Config.DB_PATH, "SONARR_TIMEOUT_SECONDS": Config.SONARR_TIMEOUT_SECONDS})

if __name__ == "__main__":
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)

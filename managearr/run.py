#!/usr/bin/env python3
"""Entry point for Managearr v1. Dev-server only for this preview
milestone - see MANAGEARR.md."""
from app import create_app
from config import Config

app = create_app(
    {
        "DB_HOST": Config.DB_HOST,
        "DB_PORT": Config.DB_PORT,
        "DB_NAME": Config.DB_NAME,
        "DB_USER": Config.DB_USER,
        "DB_PASSWORD": Config.DB_PASSWORD,
        "DB_SSLMODE": Config.DB_SSLMODE,
        "DB_POOL_MIN_SIZE": Config.DB_POOL_MIN_SIZE,
        "DB_POOL_MAX_SIZE": Config.DB_POOL_MAX_SIZE,
        "DB_CONNECT_TIMEOUT_SECONDS": Config.DB_CONNECT_TIMEOUT_SECONDS,
        "DB_STARTUP_TIMEOUT_SECONDS": Config.DB_STARTUP_TIMEOUT_SECONDS,
        "SONARR_TIMEOUT_SECONDS": Config.SONARR_TIMEOUT_SECONDS,
    }
)

if __name__ == "__main__":
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)

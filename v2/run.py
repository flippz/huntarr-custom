#!/usr/bin/env python3
"""Entry point for Huntarr v2. Dev-server only for this preview
milestone - see V2.md."""
from app import create_app
from config import Config

app = create_app({"DB_PATH": Config.DB_PATH})

if __name__ == "__main__":
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)

"""Runtime configuration for Managearr v1, sourced entirely from the
environment so the container/compose file is the single source of
truth for connection details, credentials, and ports.
"""
import os


def read_secret(value_env: str, file_env: str, *, default: str | None = None) -> str | None:
    """Resolve a secret from a ``*_FILE`` path (preferred) or a plain env var.

    A file-based secret always wins over a plain env var of the same
    purpose, so a Compose ``*_PASSWORD_FILE`` mount is never silently
    shadowed by a leftover plain value. The file contents are stripped of
    surrounding whitespace/newlines (as written by ``docker secret`` /
    Compose file-based secrets and by our password generator script).
    """
    file_path = os.environ.get(file_env)
    if file_path:
        with open(file_path, "r", encoding="utf-8") as fh:
            return fh.read().strip()
    return os.environ.get(value_env, default)


class Config:
    HOST = os.environ.get("MANAGEARR_HOST", "0.0.0.0")
    PORT = int(os.environ.get("MANAGEARR_PORT", "9706"))
    DEBUG = os.environ.get("MANAGEARR_DEBUG", "false").lower() == "true"
    SONARR_TIMEOUT_SECONDS = int(os.environ.get("MANAGEARR_SONARR_TIMEOUT_SECONDS", "10"))
    SCHEDULER_LEASE_SECONDS = int(os.environ.get("MANAGEARR_SCHEDULER_LEASE_SECONDS", "30"))
    SCHEDULER_POLL_SECONDS = float(os.environ.get("MANAGEARR_SCHEDULER_POLL_SECONDS", "5"))

    # PostgreSQL is the only runtime database - see app/persistence/database.py.
    DB_HOST = os.environ.get("MANAGEARR_DB_HOST", "postgres")
    DB_PORT = int(os.environ.get("MANAGEARR_DB_PORT", "5432"))
    DB_NAME = os.environ.get("MANAGEARR_DB_NAME", "managearr")
    DB_USER = os.environ.get("MANAGEARR_DB_USER", "managearr")
    DB_PASSWORD = read_secret("MANAGEARR_DB_PASSWORD", "MANAGEARR_DB_PASSWORD_FILE")
    DB_SSLMODE = os.environ.get("MANAGEARR_DB_SSLMODE", "prefer")
    DB_POOL_MIN_SIZE = int(os.environ.get("MANAGEARR_DB_POOL_MIN_SIZE", "1"))
    DB_POOL_MAX_SIZE = int(os.environ.get("MANAGEARR_DB_POOL_MAX_SIZE", "5"))
    DB_CONNECT_TIMEOUT_SECONDS = int(os.environ.get("MANAGEARR_DB_CONNECT_TIMEOUT_SECONDS", "5"))
    # Bounded startup retry/backoff window - see Database.wait_ready().
    DB_STARTUP_TIMEOUT_SECONDS = int(os.environ.get("MANAGEARR_DB_STARTUP_TIMEOUT_SECONDS", "30"))

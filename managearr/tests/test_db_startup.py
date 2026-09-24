"""Tests for bounded startup retry/backoff and safe failure handling.

An unreachable database must fail fast (bounded by a small timeout,
never hang indefinitely) with a static, safe error message - never the
configured host, port, user, or password.
"""
import time

import pytest

from app.persistence.database import Database, DatabaseUnavailableError


def test_wait_ready_raises_on_unreachable_host():
    # Port 1 is a reserved/unreachable port that will fail fast.
    db = Database(
        host="127.0.0.1",
        port=1,
        dbname="managearr",
        user="managearr",
        password="whatever-this-is-never-used",
        sslmode="disable",
        connect_timeout=1,
    )
    try:
        start = time.monotonic()
        with pytest.raises(DatabaseUnavailableError) as exc_info:
            db.wait_ready(timeout_seconds=2)
        elapsed = time.monotonic() - start
    finally:
        db.close()

    # Bounded: must not hang well past the configured timeout.
    assert elapsed < 10

    message = str(exc_info.value)
    for leaked in ("127.0.0.1", "whatever-this-is-never-used", "managearr_pw", "port=1"):
        assert leaked not in message


def test_wait_ready_error_message_is_static_and_safe():
    db = Database(
        host="does-not-resolve.invalid",
        port=5432,
        dbname="managearr",
        user="secret-user-name",
        password="super-secret-password",
        sslmode="disable",
        connect_timeout=1,
    )
    try:
        with pytest.raises(DatabaseUnavailableError) as exc_info:
            db.wait_ready(timeout_seconds=2)
    finally:
        db.close()

    message = str(exc_info.value)
    assert message == "Could not connect to the database within the configured startup timeout"
    assert "secret-user-name" not in message
    assert "super-secret-password" not in message
    assert "does-not-resolve.invalid" not in message


def test_health_check_returns_false_when_unreachable():
    db = Database(
        host="127.0.0.1",
        port=1,
        dbname="managearr",
        user="managearr",
        password="x",
        sslmode="disable",
        connect_timeout=1,
    )
    try:
        assert db.health_check() is False
    finally:
        db.close()


def test_health_check_returns_true_once_connected(database):
    assert database.health_check() is True

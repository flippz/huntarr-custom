"""PostgreSQL connection management for Managearr v1.

Uses psycopg 3 with a small connection pool (``psycopg_pool``). Every
``connect()`` call checks out one pooled connection, opens a real
transaction for the block (psycopg connections default to
``autocommit=False``), and commits on clean exit / rolls back on
exception - the same short-lived-connection-per-operation shape the
previous SQLite implementation used.

Callers must never hold a connection open (i.e. stay inside one
``with db.connect()`` block) across an external network call such as a
Sonarr request - see ``app/services/sonarr_scan_service.py``, which
always closes its DB transaction before calling out to Sonarr and opens
a fresh one afterward to persist results.
"""
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout


class DatabaseUnavailableError(RuntimeError):
    """Raised when PostgreSQL could not be reached within the configured
    startup timeout. The message is always static and safe - it never
    includes the configured host, port, user, or password."""


class Database:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str | None,
        sslmode: str = "prefer",
        min_size: int = 1,
        max_size: int = 5,
        connect_timeout: int = 5,
    ):
        conninfo = psycopg.conninfo.make_conninfo(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password or "",
            sslmode=sslmode,
            connect_timeout=connect_timeout,
        )
        # open=False: the pool is constructed but makes no connection
        # attempt until wait_ready() (or first use) - lets startup retry
        # be explicit and bounded rather than failing at import time.
        self._pool = ConnectionPool(
            conninfo,
            min_size=min_size,
            max_size=max_size,
            open=False,
            kwargs={"row_factory": dict_row, "autocommit": False},
        )

    def wait_ready(self, timeout_seconds: int) -> None:
        """Block until at least one pooled connection is established, or
        raise ``DatabaseUnavailableError`` once ``timeout_seconds`` has
        elapsed. The pool retries with its own internal backoff while
        waiting - this just bounds the total wait."""
        try:
            self._pool.open(wait=True, timeout=timeout_seconds)
        except PoolTimeout as exc:
            raise DatabaseUnavailableError(
                "Could not connect to the database within the configured startup timeout"
            ) from exc

    def close(self) -> None:
        self._pool.close()

    @contextmanager
    def connect(self):
        with self._pool.connection() as conn:
            yield conn

    def health_check(self) -> bool:
        try:
            with self.connect() as conn:
                conn.execute("SELECT 1").fetchone()
            return True
        except psycopg.Error:
            return False

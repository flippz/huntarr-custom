"""Dedicated Managearr simulation scheduler worker process.

It never imports SonarrClient or DispatchService. Run with ``python -m app.worker``.
"""
from __future__ import annotations

import logging
import signal
import threading
import time
import uuid

from config import Config
from .persistence.database import Database
from .persistence.migrations import run_migrations
from .persistence.policy_repository import PolicyRepository
from .persistence.scheduler_repository import SchedulerRepository
from .services.scheduler_service import SchedulerService

LOG = logging.getLogger("managearr.worker")


class SchedulerWorker:
    def __init__(
        self,
        repository: SchedulerRepository,
        policy_repository: PolicyRepository,
        *,
        owner_id: str | None = None,
        lease_seconds: int = 30,
        poll_seconds: float = 5.0,
    ):
        self.repository = repository
        self.policy_repository = policy_repository
        self.service = SchedulerService(repository, policy_repository)
        self.owner_id = owner_id or f"worker-{uuid.uuid4()}"
        self.lease_seconds = max(10, lease_seconds)
        self.poll_seconds = max(0.5, poll_seconds)
        self.stop_event = threading.Event()
        self._owns_lease = False

    def stop(self, *_args) -> None:
        self.stop_event.set()

    def _heartbeat(self) -> bool:
        if self.stop_event.is_set():
            return False
        self._owns_lease = self.repository.renew_lease(self.owner_id, self.lease_seconds)
        return self._owns_lease

    def run_once(self) -> dict:
        self._owns_lease = self.repository.acquire_lease(self.owner_id, self.lease_seconds)
        if not self._owns_lease:
            return {"lease": False, "cycle_id": None}

        self.repository.recover_interrupted(self.owner_id)
        policy = self.policy_repository.get()
        self.repository.claim_manual_request(self.owner_id, policy)
        self.repository.enqueue_scheduled_if_due(policy)
        cycle = self.repository.start_next_cycle(self.owner_id)
        if cycle is None:
            self._heartbeat()
            return {"lease": True, "cycle_id": None}

        if not self._heartbeat():
            return {"lease": False, "cycle_id": cycle["id"]}
        self.service.execute_cycle(cycle, heartbeat=self._heartbeat)
        return {"lease": self._owns_lease, "cycle_id": cycle["id"]}

    def run_forever(self) -> None:
        failures = 0
        try:
            while not self.stop_event.is_set():
                try:
                    self.run_once()
                    failures = 0
                    delay = self.poll_seconds
                except Exception:
                    # Static logging only: DSNs, SQL parameters, and exception
                    # strings may contain deployment details and are omitted.
                    LOG.error("scheduler worker iteration failed safely")
                    failures += 1
                    delay = min(30.0, self.poll_seconds * (2 ** min(failures, 4)))
                self.stop_event.wait(delay)
        finally:
            if self._owns_lease:
                try:
                    self.repository.release_lease(self.owner_id)
                except Exception:
                    LOG.warning("scheduler lease release failed during shutdown")


def _database() -> Database:
    return Database(
        host=Config.DB_HOST, port=Config.DB_PORT, dbname=Config.DB_NAME,
        user=Config.DB_USER, password=Config.DB_PASSWORD, sslmode=Config.DB_SSLMODE,
        min_size=Config.DB_POOL_MIN_SIZE, max_size=Config.DB_POOL_MAX_SIZE,
        connect_timeout=Config.DB_CONNECT_TIMEOUT_SECONDS,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    db = _database()
    db.wait_ready(timeout_seconds=Config.DB_STARTUP_TIMEOUT_SECONDS)
    run_migrations(db)
    repository = SchedulerRepository(db)
    worker = SchedulerWorker(
        repository, PolicyRepository(db),
        lease_seconds=Config.SCHEDULER_LEASE_SECONDS,
        poll_seconds=Config.SCHEDULER_POLL_SECONDS,
    )
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    try:
        worker.run_forever()
    finally:
        db.close()


if __name__ == "__main__":
    main()

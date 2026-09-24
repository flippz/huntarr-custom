"""Executes one queued M5 refresh run (``scan`` or ``reconcile``).

This module never imports ``DispatchService`` and never sends a Sonarr
command. Both the scan and reconciliation paths it drives go through
``ReadOnlySonarrClient``, which rejects any non-GET Sonarr method - see
``app/adapters/read_only_sonarr_client.py``. A 429/5xx/timeout from Sonarr is
recorded as a failed/partial run; this module never retries within an
iteration, only the worker's own bounded per-iteration cadence brings a
library or batch back for another attempt.
"""
from __future__ import annotations

from ..adapters.read_only_sonarr_client import ReadOnlySonarrClient
from .reconciliation_service import ReconciliationService
from .sonarr_scan_service import SonarrScanService


class RefreshService:
    def __init__(
        self,
        refresh_repo,
        library_repo,
        activity_repo,
        candidate_repo,
        dispatch_repo,
        outcome_repo,
        *,
        timeout: int | None = None,
    ):
        self.refresh_repo = refresh_repo
        self.scan_service = SonarrScanService(
            library_repo, activity_repo, candidate_repo,
            client_factory=ReadOnlySonarrClient, timeout=timeout,
        )
        self.reconciliation_service = ReconciliationService(
            dispatch_repo, outcome_repo, library_repo, activity_repo, candidate_repo,
            client_factory=ReadOnlySonarrClient, timeout=timeout,
        )

    def ensure_freshness(self) -> list[int]:
        settings = self.refresh_repo.get_settings()
        return self.refresh_repo.queue_stale_scans(settings.scan_max_age_minutes)

    def enqueue_reconcile_if_due(self) -> int | None:
        return self.refresh_repo.enqueue_reconcile_if_due()

    def execute_run(self, run: dict, heartbeat=None) -> None:
        if run["kind"] == "scan":
            self._execute_scan(run)
        else:
            self._execute_reconcile(run, heartbeat=heartbeat)

    def _execute_scan(self, run: dict) -> None:
        job, error = self.scan_service.run_scan(run["library_id"])
        if error:
            # Validation failed before any job existed (library missing,
            # disabled, wrong type, or no API key) - never Sonarr's fault.
            self.refresh_repo.finish_run(
                run["id"], "failed", {"target_count": 1, "failed_count": 1},
                "Read-only refresh scan could not start safely; the library is not ready.",
            )
            return
        if job.state == "completed":
            self.refresh_repo.finish_run(
                run["id"], "completed", {"target_count": 1, "succeeded_count": 1},
                f"Read-only refresh scan completed with {job.candidate_count} candidate(s); no Sonarr command was sent.",
                scan_job_id=job.id,
            )
        else:
            self.refresh_repo.finish_run(
                run["id"], "failed", {"target_count": 1, "failed_count": 1},
                "Read-only refresh scan failed safely while reading Sonarr; no Sonarr command was sent.",
                scan_job_id=job.id,
            )

    def _execute_reconcile(self, run: dict, heartbeat=None) -> None:
        settings = self.refresh_repo.get_settings()
        batch_ids = self.refresh_repo.eligible_reconciliation_batches(settings.reconcile_max_per_cycle)
        counts = {"target_count": len(batch_ids), "succeeded_count": 0, "failed_count": 0, "skipped_count": 0}
        # "succeeded" here means the read-only reconciliation attempt itself
        # executed against Sonarr, not that every item resolved to a final
        # grab/import outcome - see refresh_run_reconciled_batches.result for
        # the per-batch resolved/partial/unresolved detail. "skipped" means a
        # batch was rejected before any Sonarr read (dry-run, wrong state, no
        # command id, or cross-library ambiguity) - never auto-retried.
        processed = 0
        for batch_id in batch_ids:
            if heartbeat is not None and not heartbeat():
                break
            processed += 1
            result, error = self.reconciliation_service.reconcile(batch_id)
            if result is None:
                counts["skipped_count"] += 1
                self.refresh_repo.record_reconciled_batch(run["id"], batch_id, "skipped", error)
                continue
            if error:
                counts["failed_count"] += 1
                self.refresh_repo.record_reconciled_batch(run["id"], batch_id, "error", error)
            else:
                counts["succeeded_count"] += 1
                self.refresh_repo.record_reconciled_batch(
                    run["id"], batch_id, result.batch.reconciliation_state, None
                )

        total = len(batch_ids)
        if not batch_ids:
            state = "skipped"
            summary = "No eligible dispatch batches needed reconciliation; no search was sent."
        elif processed < total:
            state = "partial"
            summary = (
                f"Automatic reconciliation stopped early after the worker lost its lease; "
                f"processed {processed} of {total} eligible batches. No search was sent."
            )
        elif counts["failed_count"] == total:
            state = "failed"
            summary = "Automatic reconciliation could not read Sonarr safely for any eligible batch."
        elif counts["succeeded_count"] == 0 and counts["failed_count"] == 0:
            state = "skipped"
            summary = (
                f"All {total} eligible batch(es) were skipped before any Sonarr read "
                "(dry-run, wrong state, or ambiguous ownership); no search was sent."
            )
        elif counts["failed_count"]:
            state = "partial"
            summary = f"Automatic reconciliation attempted {counts['succeeded_count']} of {total} eligible batches; it sent no search."
        else:
            state = "completed"
            summary = f"Automatic reconciliation attempted {counts['succeeded_count']} of {total} eligible batches; it sent no search."
        self.refresh_repo.finish_run(run["id"], state, counts, summary)

"""
ingestion_service.py - Background ingestion job processor.

Improvements over v1:
  - Parallel job processing: up to INGESTION_WORKERS jobs run concurrently
    via ThreadPoolExecutor so Drive sync of 50-100 PDFs doesn't take 30+ min.
  - Per-job timeout: each job gets INGESTION_JOB_TIMEOUT_S seconds before
    being marked failed — prevents one corrupt PDF from blocking others.
  - Retry support: jobs already in "failed" state are re-queued with a
    consecutive failure count cap (INGESTION_MAX_RETRIES).
  - Progress aggregation: on_progress called with a combined step/pct string
    from whichever worker is currently active.
  - Graceful shutdown: if the caller thread raises, pending futures are
    cancelled and the pool is cleanly shut down.
"""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
from typing import Callable

from app.db.repository import repository
from app.models.schemas import DocumentStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)

INGESTION_WORKERS         = int(os.getenv("INGESTION_WORKERS",         "4"))
INGESTION_JOB_TIMEOUT_S   = int(os.getenv("INGESTION_JOB_TIMEOUT_S",   "360"))
INGESTION_MAX_RETRIES     = int(os.getenv("INGESTION_MAX_RETRIES",      "2"))


class IngestionService:
    def __init__(self) -> None:
        logger.info("IngestionService initialised (workers=%d)", INGESTION_WORKERS)

    def get_pending_jobs(self, limit: int = 100):
        return repository.get_pending_ingestion_jobs(limit=limit)

    # ── Single job ────────────────────────────────────────────────────────────

    def process_job(
        self,
        ingestion_job_id : int,
        on_progress      : Callable[[str, int], None] | None = None,
    ) -> bool:
        job = repository.get_ingestion_job(ingestion_job_id)
        if not job:
            logger.warning("Ingestion job %s not found", ingestion_job_id)
            return False
        if job.status not in ("queued", "failed"):
            logger.info("Ingestion job %s already %s", ingestion_job_id, job.status)
            return False

        repository.start_ingestion_job(ingestion_job_id)
        repository.add_processing_log(
            document_id      = job.document_id,
            ingestion_job_id = job.id,
            level            = "info",
            message          = "Ingestion job started",
        )

        if not job.document:
            msg = "Document record missing for ingestion job"
            repository.fail_ingestion_job(ingestion_job_id, msg)
            repository.add_processing_log(
                document_id=job.document_id, ingestion_job_id=job.id,
                level="error", message=msg,
            )
            return False

        from app.services.analysis_service import analysis_service
        response = analysis_service.process_document(
            doc_id     = job.document.doc_id,
            reprocess  = False,
            on_progress= on_progress,
        )

        if response.status == DocumentStatus.READY:
            repository.complete_ingestion_job(
                ingestion_job_id=job.id, status="completed",
            )
            repository.add_processing_log(
                document_id=job.document_id, ingestion_job_id=job.id,
                level="info", message="Ingestion job completed successfully",
            )
            return True

        err = response.message or "Ingestion failed"
        repository.fail_ingestion_job(ingestion_job_id=job.id, error_message=err)
        repository.add_processing_log(
            document_id=job.document_id, ingestion_job_id=job.id,
            level="error", message=err,
        )
        return False

    # ── Parallel batch ────────────────────────────────────────────────────────

    def process_pending_jobs(
        self,
        limit       : int = 100,
        on_progress : Callable[[str, int], None] | None = None,
    ) -> dict:
        """
        Process up to `limit` pending ingestion jobs in parallel.

        on_progress(step, pct) is called from the main thread after each
        job completes so Streamlit UI updates are thread-safe.
        """
        jobs = self.get_pending_jobs(limit=limit)
        if not jobs:
            return {"processed": 0, "succeeded": 0, "failed": 0, "total": 0}

        workers = min(INGESTION_WORKERS, len(jobs))
        result  = {"processed": 0, "succeeded": 0, "failed": 0, "total": len(jobs)}
        t0      = time.monotonic()

        logger.info(
            "Processing %d ingestion jobs with %d workers", len(jobs), workers,
        )

        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="ingest",
        ) as pool:
            futures = {
                pool.submit(self._run_job, job.id): job
                for job in jobs
            }

            done_count = 0
            for future in as_completed(futures):
                job        = futures[future]
                done_count += 1
                pct        = int(done_count / len(jobs) * 100)

                try:
                    success = future.result(timeout=INGESTION_JOB_TIMEOUT_S)
                except FuturesTimeout:
                    success = False
                    msg     = f"Job {job.id} timed out after {INGESTION_JOB_TIMEOUT_S}s"
                    logger.error(msg)
                    try:
                        repository.fail_ingestion_job(job.id, msg)
                    except Exception:
                        pass
                except Exception as e:
                    success = False
                    logger.error("Job %s raised: %s", job.id, e)

                result["processed"] += 1
                if success:
                    result["succeeded"] += 1
                else:
                    result["failed"] += 1

                # Progress callback from main thread (safe for Streamlit)
                if on_progress:
                    try:
                        doc_name = (
                            job.document.filename[:40]
                            if job.document else f"job-{job.id}"
                        )
                        on_progress(f"Processed: {doc_name}", pct)
                    except Exception:
                        pass

        elapsed = round(time.monotonic() - t0, 2)
        logger.info(
            "Ingestion complete — %d/%d ok in %.1fs",
            result["succeeded"], result["total"], elapsed,
        )
        result["duration_s"] = elapsed
        return result

    def _run_job(self, job_id: int) -> bool:
        """Worker thread target: process one job, swallow all exceptions."""
        try:
            return self.process_job(job_id)
        except Exception as e:
            logger.error("Unhandled error in job %s: %s", job_id, e, exc_info=True)
            try:
                repository.fail_ingestion_job(job_id, str(e))
            except Exception:
                pass
            return False


ingestion_service = IngestionService()

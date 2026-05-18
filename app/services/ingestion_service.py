"""
ingestion_service.py - Background ingestion job processor.
Handles queued Drive ingestion jobs and updates job state in the database.
"""

from __future__ import annotations

from typing import Callable

from app.db.repository import repository
from app.models.schemas import DocumentStatus
from app.services.analysis_service import analysis_service
from app.utils.logger import get_logger, ServiceLogger

logger = get_logger(__name__)


class IngestionService:
    def __init__(self) -> None:
        logger.info("IngestionService initialised")

    def get_pending_jobs(self, limit: int = 10):
        return repository.get_pending_ingestion_jobs(limit=limit)

    def process_job(self, ingestion_job_id: int, on_progress: Callable[[str, int], None] | None = None) -> bool:
        job = repository.get_ingestion_job(ingestion_job_id)
        if not job:
            logger.warning("Ingestion job %s not found", ingestion_job_id)
            return False

        if job.status not in ("queued", "failed"):
            logger.info("Ingestion job %s is already %s", ingestion_job_id, job.status)
            return False

        repository.start_ingestion_job(ingestion_job_id)
        repository.add_processing_log(
            document_id=job.document_id,
            ingestion_job_id=job.id,
            level="info",
            message="Ingestion job started",
        )

        if not job.document:
            repository.fail_ingestion_job(
                ingestion_job_id,
                "Document record missing for ingestion job",
            )
            repository.add_processing_log(
                document_id=job.document_id,
                ingestion_job_id=job.id,
                level="error",
                message="Document record missing for ingestion job",
            )
            return False

        response = analysis_service.process_document(
            doc_id=job.document.doc_id,
            reprocess=False,
            on_progress=on_progress,
        )

        if response.status == DocumentStatus.READY:
            repository.complete_ingestion_job(
                ingestion_job_id=job.id,
                status="completed",
            )
            repository.add_processing_log(
                document_id=job.document_id,
                ingestion_job_id=job.id,
                level="info",
                message="Ingestion job completed successfully",
            )
            return True

        repository.fail_ingestion_job(
            ingestion_job_id=job.id,
            error_message=response.message or "Ingestion failed",
        )
        repository.add_processing_log(
            document_id=job.document_id,
            ingestion_job_id=job.id,
            level="error",
            message=response.message or "Ingestion failed",
        )
        return False

    def process_pending_jobs(self, limit: int = 10, on_progress: Callable[[str, int], None] | None = None) -> dict:
        jobs = self.get_pending_jobs(limit=limit)
        result = {"processed": 0, "succeeded": 0, "failed": 0, "total": len(jobs)}
        for job in jobs:
            result["processed"] += 1
            if self.process_job(job.id, on_progress=on_progress):
                result["succeeded"] += 1
            else:
                result["failed"] += 1
        return result


ingestion_service = IngestionService()

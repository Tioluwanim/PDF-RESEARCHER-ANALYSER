"""
batch_service.py - Multi-PDF batch processing (up to 100 PDFs).

Safer version:
  - Uses process isolation by default to reduce crashes from native PDF libs.
  - Parallel processing is still supported with BATCH_WORKERS.
  - Memory-safe sub-batches via BATCH_CHUNK_SIZE.
  - Real per-item timeout handling (the old future.result(timeout=...) inside
    as_completed() could never actually time out).
  - Progress callback runs on the main thread.
  - Each item failure is isolated from the rest of the batch.
  - Configurable via env vars / Streamlit secrets.
"""

from __future__ import annotations

import os
import time
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    wait,
    FIRST_COMPLETED,
)
from dataclasses import dataclass, field
from typing import Callable, Optional

from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Tuning ────────────────────────────────────────────────────────────────────
BATCH_EXECUTOR = os.getenv("BATCH_EXECUTOR", "process").strip().lower()
BATCH_WORKERS = int(os.getenv("BATCH_WORKERS", "4"))
BATCH_WORKER_HARD_CAP = int(os.getenv("BATCH_WORKER_HARD_CAP", "2"))
BATCH_CHUNK_SIZE = int(os.getenv("BATCH_CHUNK_SIZE", "20"))
BATCH_ITEM_TIMEOUT_S = int(os.getenv("BATCH_ITEM_TIMEOUT_S", "300"))


@dataclass
class BatchItem:
    doc_id: str
    filename: str
    status: str = "queued"   # queued | processing | ready | failed
    error: str = ""
    pages: int = 0
    words: int = 0
    chunks: int = 0
    sections: list[str] = field(default_factory=list)
    duration_s: float = 0.0


@dataclass
class BatchResult:
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    items: list[BatchItem] = field(default_factory=list)
    duration_s: float = 0.0


def _process_one_worker(file_bytes: bytes, filename: str) -> BatchItem:
    """
    Worker entry point.

    Runs the full pipeline inside an isolated process (default) or worker.
    Importing the services here helps avoid sharing native-library state
    across workers.
    """
    from app.models.schemas import DocumentStatus
    from app.services.pdf_service import pdf_service
    from app.services.extraction_service import extraction_service
    from app.services.rag_service import rag_service

    item = BatchItem(doc_id="", filename=filename)
    t0 = time.monotonic()

    try:
        item.status = "processing"

        # 1 — Upload / persist to disk
        doc, err = pdf_service.save_upload(
            file_bytes=file_bytes,
            filename=filename,
        )
        if err or not doc:
            raise RuntimeError(str(err) if err else "Upload returned no document")
        item.doc_id = doc.doc_id

        # 2 — Extract text, sections, chunks
        doc = extraction_service.process(doc)
        pdf_service.save_document(doc)

        if doc.status == DocumentStatus.FAILED:
            raise RuntimeError(doc.error_message or "Extraction failed")

        # 3 — Embed + build vector index
        doc = rag_service.build_index(doc)
        pdf_service.save_document(doc)

        if doc.status == DocumentStatus.FAILED:
            raise RuntimeError(doc.error_message or "Indexing failed")

        item.status = "ready"
        item.pages = getattr(doc.metadata, "page_count", 0) or 0
        item.words = getattr(doc.metadata, "word_count", 0) or 0
        item.chunks = getattr(doc, "chunk_count", 0) or 0
        item.sections = [
            s.section_type.value
            for s in getattr(doc, "sections", [])
            if getattr(s, "section_type", None) is not None
        ]

    except Exception as e:
        item.status = "failed"
        item.error = str(e)
        logger.error("Batch item '%s' failed: %s", filename, e, exc_info=True)

    item.duration_s = round(time.monotonic() - t0, 2)
    return item


class BatchService:
    """
    Processes up to 100 PDFs in a single batch.

    Sub-batches are processed sequentially. Within a sub-batch, files are
    processed in parallel.
    """

    def _make_executor(self, max_workers: int):
        """
        Choose executor type.

        Default is process-based isolation for safety.
        Use BATCH_EXECUTOR=thread if you prefer threads.
        """
        if BATCH_EXECUTOR == "thread":
            return ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="batch_proc",
            )
        return ProcessPoolExecutor(max_workers=max_workers)

    def process_batch(
        self,
        files: list[tuple[bytes, str]],
        on_item_start: Optional[Callable[[int, int, str], None]] = None,
        on_item_done: Optional[Callable[[BatchItem], None]] = None,
    ) -> BatchResult:
        """
        Process a list of (file_bytes, filename) tuples.

        Args:
            files: List of (file_bytes, filename).
            on_item_start: Called on the main thread before each item is submitted:
                           (current, total, filename)
            on_item_done: Called on the main thread when each item finishes.

        Returns:
            BatchResult with per-document details.
        """
        if not files:
            return BatchResult()

        workers = min(BATCH_WORKERS, BATCH_WORKER_HARD_CAP, len(files))
        workers = max(1, workers)
        chunk = max(1, BATCH_CHUNK_SIZE)

        result = BatchResult(total=len(files))
        batch_t0 = time.monotonic()
        overall_index = 0

        logger.info(
            "Batch start — %d files, %d workers, chunk=%d, executor=%s",
            len(files), workers, chunk, BATCH_EXECUTOR,
        )

        # Split into sub-batches to control peak memory usage.
        for sub_start in range(0, len(files), chunk):
            sub = files[sub_start: sub_start + chunk]

            try:
                with self._make_executor(workers) as pool:
                    pending: dict = {}
                    submitted_meta: dict = {}

                    # Submit all items in this sub-batch.
                    for local_idx, (fb, fn) in enumerate(sub, start=1):
                        overall_index += 1
                        if on_item_start:
                            try:
                                on_item_start(overall_index, len(files), fn)
                            except Exception:
                                pass

                        fut = pool.submit(_process_one_worker, fb, fn)
                        pending[fut] = True
                        submitted_meta[fut] = {
                            "index": overall_index,
                            "filename": fn,
                            "submitted_at": time.monotonic(),
                        }

                    # Track completion and real deadlines.
                    while pending:
                        done, _ = wait(
                            pending.keys(),
                            timeout=0.5,
                            return_when=FIRST_COMPLETED,
                        )

                        now = time.monotonic()

                        # Handle completed futures.
                        for future in done:
                            if future not in pending:
                                continue
                            pending.pop(future, None)
                            meta = submitted_meta.pop(future, {})
                            filename = meta.get("filename", "")

                            try:
                                item = future.result()
                            except Exception as e:
                                item = BatchItem(
                                    doc_id="",
                                    filename=filename,
                                    status="failed",
                                    error=str(e),
                                )

                            self._record_item(result, item, on_item_done)

                            logger.info(
                                "Batch [%d/%d] %-40s → %s (%.1fs)",
                                meta.get("index", 0),
                                len(files),
                                filename[:40],
                                item.status,
                                item.duration_s,
                            )

                        # Mark timed-out tasks. This is best-effort:
                        # we mark the item failed in the UI/result and try to cancel
                        # the future if it hasn't started yet.
                        timed_out = []
                        for future in list(pending.keys()):
                            meta = submitted_meta.get(future, {})
                            started = meta.get("submitted_at", now)
                            if now - started > BATCH_ITEM_TIMEOUT_S:
                                timed_out.append(future)

                        for future in timed_out:
                            pending.pop(future, None)
                            meta = submitted_meta.pop(future, {})
                            filename = meta.get("filename", "")
                            future.cancel()

                            item = BatchItem(
                                doc_id="",
                                filename=filename,
                                status="failed",
                                error=f"Timed out after {BATCH_ITEM_TIMEOUT_S}s",
                            )
                            item.duration_s = float(BATCH_ITEM_TIMEOUT_S)

                            self._record_item(result, item, on_item_done)

                            logger.warning(
                                "Batch [%d/%d] %-40s → failed (timeout after %ss)",
                                meta.get("index", 0),
                                len(files),
                                filename[:40],
                                BATCH_ITEM_TIMEOUT_S,
                            )

            except Exception as e:
                logger.error("Batch sub-batch failed: %s", e, exc_info=True)

        result.duration_s = round(time.monotonic() - batch_t0, 2)
        logger.info(
            "Batch complete — %d/%d succeeded in %.1fs",
            result.succeeded, result.total, result.duration_s,
        )
        return result

    @staticmethod
    def _record_item(
        result: BatchResult,
        item: BatchItem,
        on_item_done: Optional[Callable[[BatchItem], None]] = None,
    ) -> None:
        result.items.append(item)
        if item.status == "ready":
            result.succeeded += 1
        else:
            result.failed += 1

        if on_item_done:
            try:
                on_item_done(item)
            except Exception:
                pass


# ── Singleton ─────────────────────────────────────────────────────────────────
batch_service = BatchService()

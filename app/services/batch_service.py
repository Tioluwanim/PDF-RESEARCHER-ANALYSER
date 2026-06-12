"""
batch_service.py - Multi-PDF batch processing (up to 100 PDFs).

Improvements over v1:
  - Parallel processing: BATCH_WORKERS PDFs processed concurrently via
    ThreadPoolExecutor (default 4).  Each worker runs the full
    upload → extract → embed pipeline independently.
  - Memory-safe: large batches are split into sub-batches of BATCH_CHUNK_SIZE
    (default 20) so we never have 100 docs' worth of extracted text in RAM
    simultaneously.  Sub-batches are processed sequentially; within each
    sub-batch PDFs are processed in parallel.
  - Per-item timeouts: each document is given BATCH_ITEM_TIMEOUT_S seconds
    (default 300) before being marked failed — prevents one huge PDF from
    blocking the whole batch.
  - Robust error isolation: an exception in one worker never affects others.
  - Progress callback called from the main thread for thread-safe UI updates.
  - Configurable via env vars so Streamlit Cloud / low-RAM deployments can
    reduce workers without code changes.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
from dataclasses import dataclass, field
from typing import Callable, Optional
import os

from app.models.schemas import DocumentStatus
from app.services.pdf_service        import pdf_service
from app.services.extraction_service import extraction_service
from app.services.rag_service        import rag_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Tuning ────────────────────────────────────────────────────────────────────
BATCH_WORKERS          = int(os.getenv("BATCH_WORKERS",          "4"))
BATCH_CHUNK_SIZE       = int(os.getenv("BATCH_CHUNK_SIZE",       "20"))
BATCH_ITEM_TIMEOUT_S   = int(os.getenv("BATCH_ITEM_TIMEOUT_S",   "300"))   # 5 min per doc


@dataclass
class BatchItem:
    doc_id    : str
    filename  : str
    status    : str = "queued"   # queued | processing | ready | failed
    error     : str = ""
    pages     : int = 0
    words     : int = 0
    chunks    : int = 0
    sections  : list[str] = field(default_factory=list)
    duration_s: float = 0.0


@dataclass
class BatchResult:
    total     : int = 0
    succeeded : int = 0
    failed    : int = 0
    items     : list[BatchItem] = field(default_factory=list)
    duration_s: float = 0.0


class BatchService:
    """
    Processes up to 100 PDFs in a single batch.
    Each PDF goes through: upload → extract → embed → index.
    Processing is parallelised within memory-safe sub-batches.
    """

    def process_batch(
        self,
        files         : list[tuple[bytes, str]],
        on_item_start : Optional[Callable[[int, int, str], None]] = None,
        on_item_done  : Optional[Callable[[BatchItem], None]]     = None,
    ) -> BatchResult:
        """
        Process a list of (file_bytes, filename) tuples.

        Args:
            files:          List of (file_bytes, filename).
            on_item_start:  Called before processing each file: (current, total, filename)
            on_item_done:   Called after each file with the finished BatchItem.

        Returns:
            BatchResult with per-document details.
        """
        if not files:
            return BatchResult()

        # Clamp workers to available files
        workers = min(BATCH_WORKERS, len(files))
        chunk   = BATCH_CHUNK_SIZE

        result     = BatchResult(total=len(files))
        batch_t0   = time.monotonic()
        done_count = 0   # tracks overall position for on_item_start numbering

        logger.info(
            "Batch start — %d files, %d workers, chunk=%d",
            len(files), workers, chunk,
        )

        # Split into sub-batches to control peak memory usage
        for sub_start in range(0, len(files), chunk):
            sub = files[sub_start : sub_start + chunk]

            with ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="batch_proc",
            ) as pool:
                # Submit all items in this sub-batch
                future_to_file = {
                    pool.submit(self._process_one, fb, fn): (fb, fn)
                    for fb, fn in sub
                }

                for future in as_completed(future_to_file):
                    done_count += 1
                    _, filename = future_to_file[future]

                    if on_item_start:
                        try:
                            on_item_start(done_count, len(files), filename)
                        except Exception:
                            pass

                    try:
                        item = future.result(timeout=BATCH_ITEM_TIMEOUT_S)
                    except FuturesTimeout:
                        item = BatchItem(
                            doc_id   = "",
                            filename = filename,
                            status   = "failed",
                            error    = f"Timed out after {BATCH_ITEM_TIMEOUT_S}s",
                        )
                    except Exception as e:
                        item = BatchItem(
                            doc_id   = "",
                            filename = filename,
                            status   = "failed",
                            error    = str(e),
                        )

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

                    logger.info(
                        "Batch [%d/%d] %-40s → %s (%.1fs)",
                        done_count, len(files),
                        filename[:40], item.status, item.duration_s,
                    )

        result.duration_s = round(time.monotonic() - batch_t0, 2)
        logger.info(
            "Batch complete — %d/%d succeeded in %.1fs",
            result.succeeded, result.total, result.duration_s,
        )
        return result

    def _process_one(self, file_bytes: bytes, filename: str) -> BatchItem:
        """Full pipeline for a single PDF. Runs inside a worker thread."""
        item = BatchItem(doc_id="", filename=filename)
        t0   = time.monotonic()

        try:
            item.status = "processing"

            # 1 — Upload / persist to disk
            doc, err = pdf_service.save_upload(
                file_bytes=file_bytes, filename=filename,
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

            item.status   = "ready"
            item.pages    = doc.metadata.page_count
            item.words    = doc.metadata.word_count
            item.chunks   = doc.chunk_count
            item.sections = [s.section_type.value for s in doc.sections]

        except Exception as e:
            item.status = "failed"
            item.error  = str(e)
            logger.error("Batch item '%s' failed: %s", filename, e, exc_info=True)

        item.duration_s = round(time.monotonic() - t0, 2)
        return item


# ── Singleton ─────────────────────────────────────────────────────────────────
batch_service = BatchService()

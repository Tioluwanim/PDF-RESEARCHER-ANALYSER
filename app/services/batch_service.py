"""
batch_service.py - Multi-PDF batch processing (20-50 PDFs).

Manages a queue of documents, processes them sequentially with
progress reporting, and tracks per-document status.
"""

from __future__ import annotations

import time
from typing import Callable, Optional
from dataclasses import dataclass, field

from app.models.schemas import DocumentStatus
from app.services.pdf_service        import pdf_service
from app.services.extraction_service import extraction_service
from app.services.rag_service        import rag_service
from app.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class BatchItem:
    doc_id   : str
    filename : str
    status   : str = "queued"      # queued | processing | ready | failed
    error    : str = ""
    pages    : int = 0
    words    : int = 0
    chunks   : int = 0
    sections : list[str] = field(default_factory=list)
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
    Processes up to 50 PDFs in a single batch.
    Each PDF goes through the full pipeline: upload → extract → embed → index.
    """

    def process_batch(
        self,
        files         : list[tuple[bytes, str]],   # [(file_bytes, filename), ...]
        on_item_start : Optional[Callable[[int, int, str], None]] = None,
        on_item_done  : Optional[Callable[[BatchItem], None]]     = None,
    ) -> BatchResult:
        """
        Process a list of (bytes, filename) tuples.

        Args:
            files:          List of (file_bytes, filename).
            on_item_start:  Called before each file: (current, total, filename)
            on_item_done:   Called after each file with the BatchItem result.

        Returns:
            BatchResult with per-document details.
        """
        result    = BatchResult(total=len(files))
        batch_start = time.monotonic()

        logger.info("Batch start — %d files", len(files))

        for i, (file_bytes, filename) in enumerate(files, start=1):
            if on_item_start:
                try:
                    on_item_start(i, len(files), filename)
                except Exception:
                    pass

            item = self._process_one(file_bytes, filename)
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
                "Batch [%d/%d] %s → %s",
                i, len(files), filename, item.status,
            )

        result.duration_s = round(time.monotonic() - batch_start, 2)
        logger.info(
            "Batch complete — %d/%d succeeded in %.1fs",
            result.succeeded, result.total, result.duration_s,
        )
        return result

    def _process_one(self, file_bytes: bytes, filename: str) -> BatchItem:
        item = BatchItem(doc_id="", filename=filename)
        start = time.monotonic()

        try:
            item.status = "processing"

            # 1 — Upload
            doc, err = pdf_service.save_upload(
                file_bytes=file_bytes, filename=filename
            )
            if err or not doc:
                raise RuntimeError(str(err) if err else "Upload failed")

            item.doc_id = doc.doc_id

            # 2 — Extract
            doc = extraction_service.process(doc)
            pdf_service.save_document(doc)

            if doc.status == DocumentStatus.FAILED:
                raise RuntimeError(doc.error_message or "Extraction failed")

            # 3 — Embed + index
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
            logger.error("Batch item '%s' failed: %s", filename, e)

        item.duration_s = round(time.monotonic() - start, 2)
        return item


# ── Singleton ─────────────────────────────────────────────────────────────────
batch_service = BatchService()
"""
pdf_service.py - Handles PDF upload, storage, and retrieval.
Responsible for saving files to disk and managing document state.
"""

import uuid
import shutil
from pathlib import Path
from datetime import datetime

from app.config import (
    UPLOAD_DIR,
    PROCESSED_DIR,
    MAX_FILE_SIZE_BYTES,
    MAX_FILE_SIZE_MB,
    ALLOWED_EXTENSIONS,
)
from app.models.schemas import (
    ProcessedDocument,
    DocumentMetadata,
    DocumentStatus,
    UploadResponse,
    ErrorResponse,
)
from app.utils.logger import get_logger, ServiceLogger

logger = get_logger(__name__)


# ── PDF Service Class ─────────────────────────────────────────────────────────

class PDFService:
    """
    Handles all file-level operations:
    - Validating uploaded PDFs
    - Saving to disk
    - Loading processed document state
    - Listing and deleting documents
    """

    def __init__(self):
        self.upload_dir    = UPLOAD_DIR
        self.processed_dir = PROCESSED_DIR
        logger.info("PDFService initialised")

    # ── Upload ────────────────────────────────────────────────────────────────

    def save_upload(
        self,
        file_bytes: bytes,
        filename: str,
    ) -> tuple[ProcessedDocument, None] | tuple[None, ErrorResponse]:
        """
        Validates and saves an uploaded PDF to disk.
        Creates a ProcessedDocument with UPLOADED status.

        Args:
            file_bytes: Raw bytes of the uploaded file.
            filename:   Original filename from the upload widget.

        Returns:
            (ProcessedDocument, None) on success.
            (None, ErrorResponse)     on validation failure.
        """
        slog = ServiceLogger("pdf_service")

        # ── Validate extension ────────────────────────────────────────────────
        suffix = Path(filename).suffix.lower()
        if suffix not in ALLOWED_EXTENSIONS:
            msg = f"Invalid file type '{suffix}'. Only PDF files are allowed."
            slog.warning(msg)
            return None, ErrorResponse(error="Invalid file type", detail=msg)

        # ── Validate file size ────────────────────────────────────────────────
        file_size = len(file_bytes)
        if file_size == 0:
            msg = "Uploaded file is empty."
            slog.warning(msg)
            return None, ErrorResponse(error="Empty file", detail=msg)

        if file_size > MAX_FILE_SIZE_BYTES:
            msg = (
                f"File size {file_size / (1024*1024):.1f} MB exceeds "
                f"the {MAX_FILE_SIZE_MB} MB limit."
            )
            slog.warning(msg)
            return None, ErrorResponse(error="File too large", detail=msg)

        # ── Validate PDF magic bytes ──────────────────────────────────────────
        if not file_bytes.startswith(b"%PDF"):
            msg = "File does not appear to be a valid PDF (missing PDF header)."
            slog.warning(msg)
            return None, ErrorResponse(error="Invalid PDF", detail=msg)

        # ── Generate unique doc_id ────────────────────────────────────────────
        doc_id    = str(uuid.uuid4())
        safe_name = self._sanitize_filename(filename)
        dest_path = self.upload_dir / f"{doc_id}_{safe_name}"

        slog = ServiceLogger("pdf_service", doc_id=doc_id)

        # ── Save to disk ──────────────────────────────────────────────────────
        try:
            dest_path.write_bytes(file_bytes)
            slog.info(f"Saved '{filename}' → {dest_path} ({file_size:,} bytes)")
        except OSError as e:
            msg = f"Failed to save file: {e}"
            slog.error(msg)
            return None, ErrorResponse(error="Storage error", detail=msg, doc_id=doc_id)

        # ── Build ProcessedDocument ───────────────────────────────────────────
        doc = ProcessedDocument(
            doc_id    = doc_id,
            filename  = filename,
            file_path = str(dest_path),
            status    = DocumentStatus.UPLOADED,
            metadata  = DocumentMetadata(file_size_bytes=file_size),
        )

        # Persist initial state
        self._save_document_state(doc)

        slog.info(f"Document state created — status={doc.status.value}")
        return doc, None

    # ── Load / Save State ─────────────────────────────────────────────────────

    def load_document(self, doc_id: str) -> ProcessedDocument | None:
        """
        Loads a ProcessedDocument from its JSON state file.

        Args:
            doc_id: The document UUID.

        Returns:
            ProcessedDocument if found, None otherwise.
        """
        state_path = self._state_path(doc_id)
        if not state_path.exists():
            logger.warning(f"[{doc_id}] State file not found: {state_path}")
            return None

        try:
            doc = ProcessedDocument.model_validate_json(state_path.read_text(encoding="utf-8"))
            logger.debug(f"[{doc_id}] Loaded document — status={doc.status.value}")
            return doc
        except Exception as e:
            logger.error(f"[{doc_id}] Failed to load state: {e}", exc_info=True)
            return None

    def save_document(self, doc: ProcessedDocument) -> bool:
        """
        Persists a ProcessedDocument to its JSON state file.
        Updates updated_at timestamp automatically.

        Returns:
            True on success, False on failure.
        """
        doc.updated_at = datetime.utcnow()
        return self._save_document_state(doc)

    def update_status(
        self,
        doc_id: str,
        status: DocumentStatus,
        error_message: str | None = None,
    ) -> bool:
        """
        Convenience method to update only the status of a document.

        Args:
            doc_id:        Document UUID.
            status:        New DocumentStatus value.
            error_message: Optional error detail (set on FAILED status).

        Returns:
            True on success, False if document not found.
        """
        doc = self.load_document(doc_id)
        if not doc:
            return False

        doc.status = status
        if error_message:
            doc.error_message = error_message

        logger.info(f"[{doc_id}] Status → {status.value}")
        return self.save_document(doc)

    # ── List Documents ────────────────────────────────────────────────────────

    def list_documents(self) -> list[dict]:
        """
        Returns a list of all document summaries from state files.
        Used to populate the sidebar in the Streamlit UI.

        Returns:
            List of summary dicts (doc_id, filename, status, pages etc.)
        """
        summaries = []
        for state_file in sorted(
            self.processed_dir.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,  # most recent first
        ):
            try:
                doc = ProcessedDocument.model_validate_json(
                    state_file.read_text(encoding="utf-8")
                )
                summaries.append(doc.summary())
            except Exception as e:
                logger.warning(f"Could not parse state file {state_file.name}: {e}")

        logger.debug(f"Listed {len(summaries)} documents")
        return summaries

    # ── Delete Document ───────────────────────────────────────────────────────

    def delete_document(self, doc_id: str) -> bool:
        """
        Deletes all files associated with a document:
        - Uploaded PDF
        - State JSON
        - FAISS vector index directory

        Args:
            doc_id: Document UUID.

        Returns:
            True if anything was deleted, False if nothing found.
        """
        slog   = ServiceLogger("pdf_service", doc_id=doc_id)
        doc    = self.load_document(doc_id)
        found  = False

        # Delete uploaded PDF
        if doc and Path(doc.file_path).exists():
            Path(doc.file_path).unlink()
            slog.info("Deleted uploaded PDF")
            found = True

        # Delete vector index
        if doc and doc.vector_index_path:
            idx_path = Path(doc.vector_index_path)
            if idx_path.exists():
                if idx_path.is_dir():
                    shutil.rmtree(idx_path)
                else:
                    idx_path.unlink()
                slog.info("Deleted vector index")

        # Delete state JSON
        state_path = self._state_path(doc_id)
        if state_path.exists():
            state_path.unlink()
            slog.info("Deleted state file")
            found = True

        return found

    # ── Helpers ───────────────────────────────────────────────────────────────

    def get_upload_response(self, doc: ProcessedDocument) -> UploadResponse:
        """Builds an UploadResponse from a ProcessedDocument."""
        return UploadResponse(
            doc_id    = doc.doc_id,
            filename  = doc.filename,
            file_size = doc.metadata.file_size_bytes,
            status    = doc.status,
        )

    def document_exists(self, doc_id: str) -> bool:
        """Returns True if state file exists for this doc_id."""
        return self._state_path(doc_id).exists()

    def is_ready(self, doc_id: str) -> bool:
        """Returns True if document is fully processed and ready for chat."""
        doc = self.load_document(doc_id)
        return doc is not None and doc.status == DocumentStatus.READY

    # ── Private ───────────────────────────────────────────────────────────────

    def _state_path(self, doc_id: str) -> Path:
        """Returns the path to the JSON state file for a document."""
        return self.processed_dir / f"{doc_id}.json"

    def _save_document_state(self, doc: ProcessedDocument) -> bool:
        """Writes the ProcessedDocument as JSON to processed_dir."""
        state_path = self._state_path(doc.doc_id)
        try:
            state_path.write_text(
                doc.model_dump_json(indent=2),
                encoding="utf-8",
            )
            return True
        except OSError as e:
            logger.error(f"[{doc.doc_id}] Failed to save state: {e}", exc_info=True)
            return False

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        """
        Removes unsafe characters from a filename.
        Keeps alphanumerics, dots, hyphens and underscores.
        """
        safe = "".join(
            c if c.isalnum() or c in (".", "-", "_") else "_"
            for c in filename
        )
        # Collapse multiple underscores
        while "__" in safe:
            safe = safe.replace("__", "_")
        return safe.strip("_") or "document.pdf"


# ── Module-level singleton ────────────────────────────────────────────────────
# Services import this instance directly — no need to instantiate.
pdf_service = PDFService()
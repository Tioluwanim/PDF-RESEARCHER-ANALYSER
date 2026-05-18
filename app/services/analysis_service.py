"""
analysis_service.py - Orchestrates the full PDF processing pipeline.
Single point of contact for the Streamlit UI.
"""

from __future__ import annotations

import time
from typing import Generator, Optional, Callable

from app.models.schemas import (
    ProcessedDocument,
    AnalysisResponse,
    ChatMessage,
    ChatResponse,
    SearchResponse,
    DocumentStatus,
    SectionType,
)
from app.services.pdf_service        import pdf_service
from app.services.extraction_service import extraction_service
from app.services.rag_service        import rag_service
from app.services.ai_router          import ai_router
from app.services.drive_service      import drive_service
from app.db.repository import repository
from app.config import TOP_K_RESULTS, SIMILARITY_THRESHOLD
from app.utils.logger import get_logger, ServiceLogger

logger = get_logger(__name__)


class AnalysisService:

    def __init__(self) -> None:
        logger.info("AnalysisService initialised")

    # ── Full pipeline ─────────────────────────────────────────────────────────

    def process_document(
        self,
        doc_id      : str,
        reprocess   : bool                       = False,
        on_progress : Optional[Callable]         = None,
    ) -> AnalysisResponse:

        slog       = ServiceLogger("analysis_service", doc_id=doc_id)
        start_time = time.time()

        def progress(step: str, pct: int) -> None:
            slog.info("[%d%%] %s", pct, step)
            if on_progress:
                try:
                    on_progress(step, pct)
                except Exception:
                    pass

        try:
            progress("Loading document …", 5)
            doc = pdf_service.load_document(doc_id)

            if not doc:
                return AnalysisResponse(
                    doc_id=doc_id,
                    status=DocumentStatus.FAILED,
                    message="Document not found",
                )

            if doc.status == DocumentStatus.READY and not reprocess:
                slog.info("Already READY — skipping reprocess")
                return self._build_analysis_response(doc, start_time, "Already processed")

            # ── Step 1: extract ───────────────────────────────────────────────
            progress("Extracting text and detecting sections …", 25)
            doc = extraction_service.process(doc)
            pdf_service.save_document(doc)

            if doc.status == DocumentStatus.FAILED:
                raise RuntimeError(doc.error_message or "Extraction failed")

            progress(
                f"Extracted {doc.metadata.page_count}p · "
                f"{len(doc.sections)} sections · "
                f"{doc.chunk_count} chunks",
                55,
            )

            # ── Step 2: embed + index ─────────────────────────────────────────
            progress("Generating embeddings and building vector index …", 70)
            doc = rag_service.build_index(doc)
            pdf_service.save_document(doc)

            if doc.status == DocumentStatus.FAILED:
                raise RuntimeError(doc.error_message or "Indexing failed")

            progress("Document ready for chat ✓", 100)
            slog.info(
                "Pipeline complete — %dp · %d sections · %d chunks",
                doc.metadata.page_count, len(doc.sections), doc.chunk_count,
            )
            return self._build_analysis_response(doc, start_time, "Processing complete")

        except Exception as e:
            slog.error("Pipeline failed: %s", e)
            try:
                pdf_service.update_status(doc_id, DocumentStatus.FAILED, str(e))
            except Exception:
                pass
            return AnalysisResponse(
                doc_id=doc_id,
                status=DocumentStatus.FAILED,
                message=str(e),
            )

    # ── Chat (streaming) ──────────────────────────────────────────────────────

    def chat_stream(
        self,
        doc_id    : str,
        question  : str,
        history   : list[ChatMessage],
        top_k     : int   = TOP_K_RESULTS,
        threshold : float = SIMILARITY_THRESHOLD,
    ) -> Generator[str, None, None]:

        slog = ServiceLogger("analysis_service", doc_id=doc_id)
        slog.info("Chat stream — '%s'", question[:60])

        try:
            doc = pdf_service.load_document(doc_id)
            if not doc:
                yield "⚠️ Document not found."
                return
            if doc.status != DocumentStatus.READY:
                yield f"⚠️ Document is not ready (status: {doc.status.value}). Please process it first."
                return

            context, _ = rag_service.get_context(
                doc_id=doc_id, query=question,
                top_k=top_k, threshold=threshold,
            )

            # ai_router.chat() takes (question, context, history, doc_id, stream)
            yield from ai_router.chat(
                question = question,
                context  = context,
                history  = history,
                doc_id   = doc_id,
                stream   = True,
            )

        except Exception as e:
            slog.error("chat_stream failed: %s", e)
            yield f"⚠️ Chat error: {e}"

    # ── Chat (complete) ───────────────────────────────────────────────────────

    def chat_complete(
        self,
        doc_id    : str,
        question  : str,
        history   : list[ChatMessage],
        top_k     : int   = TOP_K_RESULTS,
        threshold : float = SIMILARITY_THRESHOLD,
    ) -> ChatResponse:

        slog = ServiceLogger("analysis_service", doc_id=doc_id)

        try:
            doc = pdf_service.load_document(doc_id)
            if not doc or doc.status != DocumentStatus.READY:
                return ChatResponse(
                    answer="⚠️ Document not ready.",
                    doc_id=doc_id, question=question,
                )

            context, sources = rag_service.get_context(
                doc_id=doc_id, query=question,
                top_k=top_k, threshold=threshold,
            )

            response = ai_router.chat(
                question = question,
                context  = context,
                history  = history,
                doc_id   = doc_id,
                stream   = False,
            )
            response.sources = sources
            return response

        except Exception as e:
            slog.error("chat_complete failed: %s", e)
            return ChatResponse(
                answer=f"⚠️ Error: {e}",
                doc_id=doc_id, question=question,
            )

    # ── Document info (UI-compatible) ─────────────────────────────────────────

    def get_document_info(self, doc_id: str) -> dict:
        try:
            doc = pdf_service.load_document(doc_id)
            if not doc:
                return {"error": f"Document '{doc_id}' not found"}

            index_stats: dict = {}
            if doc.status == DocumentStatus.READY:
                try:
                    index_stats = rag_service.get_index_stats(doc_id)
                except Exception:
                    pass

            m = doc.metadata
            return {
                "doc_id"   : doc.doc_id,
                "filename" : doc.filename,
                "status"   : doc.status.value,
                "metadata" : {
                    "title"    : getattr(m, "title",            ""),
                    "authors"  : getattr(m, "authors",          []),
                    "abstract" : getattr(m, "abstract",         ""),
                    "keywords" : getattr(m, "keywords",         []),
                    "doi"      : getattr(m, "doi",              ""),
                    "issn"     : getattr(m, "issn",             ""),
                    "publisher": getattr(m, "publisher",        ""),
                    "journal"  : getattr(m, "journal",          ""),
                    "volume"   : getattr(m, "volume",           ""),
                    "issue"    : getattr(m, "issue",            ""),
                    "pages"    : getattr(m, "page_count",        0),
                    "words"    : getattr(m, "word_count",        0),
                    "file_size": f"{getattr(m, 'file_size_bytes', 0) / 1024:.1f} KB",
                    "language" : getattr(m, "language",         "en"),
                },
                "sections" : [
                    {
                        "type"      : s.section_type.value,
                        "title"     : s.title,
                        "word_count": s.word_count,
                        "page_start": s.page_start,
                    }
                    for s in getattr(doc, "sections", [])
                ],
                "chunks"   : {
                    "total"  : getattr(doc, "chunk_count", 0),
                    "indexed": index_stats.get("total_vectors", 0),
                },
                "created_at": doc.created_at.isoformat() if doc.created_at else "",
                "updated_at": doc.updated_at.isoformat() if doc.updated_at else "",
            }

        except Exception as e:
            logger.error("get_document_info failed: %s", e)
            return {"error": str(e)}

    def get_section_content(
        self, doc_id: str, section_type: SectionType
    ) -> str:
        try:
            doc = pdf_service.load_document(doc_id)
            return doc.get_section_text(section_type) if doc else ""
        except Exception:
            return ""

    def get_abstract(self, doc_id: str) -> str:
        return self.get_section_content(doc_id, SectionType.ABSTRACT)

    # ── Search ────────────────────────────────────────────────────────────────

    def semantic_search(
        self,
        doc_id    : str | None,
        query     : str,
        top_k     : int   = TOP_K_RESULTS,
        threshold : float = SIMILARITY_THRESHOLD,
        author    : str | None = None,
        year      : str | None = None,
        section_type: SectionType | None = None,
    ) -> SearchResponse:
        if doc_id:
            return rag_service.search(doc_id, query, top_k, threshold)
        return rag_service.search_library(
            query=query,
            top_k=top_k,
            threshold=threshold,
            author=author,
            year=year,
            section_type=section_type,
        )

    def library_search(
        self,
        query     : str,
        top_k     : int   = TOP_K_RESULTS,
        threshold : float = SIMILARITY_THRESHOLD,
        doc_ids   : list[str] | None = None,
        author    : str | None = None,
        year      : str | None = None,
        section_type: SectionType | None = None,
        page_number: int | None = None,
    ) -> SearchResponse:
        return rag_service.search_library(
            query=query,
            top_k=top_k,
            threshold=threshold,
            doc_ids=doc_ids,
            author=author,
            year=year,
            section_type=section_type,
            page_number=page_number,
        )

    def library_chat_stream(
        self,
        question: str,
        history: list[ChatMessage],
        top_k: int = TOP_K_RESULTS,
        threshold: float = SIMILARITY_THRESHOLD,
        doc_ids: list[str] | None = None,
        author: str | None = None,
        year: str | None = None,
        section_type: SectionType | None = None,
    ) -> Generator[str, None, None]:
        try:
            repository.add_document_log(doc_id, "info", "Processing started")
            context, sources = rag_service.get_library_context(
                query=question,
                top_k=top_k,
                threshold=threshold,
                doc_ids=doc_ids,
                author=author,
                year=year,
                section_type=section_type,
            )
            if not context:
                yield "No indexed library context was found. Sync or process documents, then rebuild the index."
                return
            source_lines = "\n".join(
                f"- {r.chunk.doc_id}, page {r.chunk.page_number}, section {r.chunk.section_type.value}, score {r.score:.3f}"
                for r in sources
            )
            prompt_context = f"{context}\n\nSource index:\n{source_lines}"
            yield from ai_router.chat(
                question=question,
                context=prompt_context,
                history=history,
                doc_id="library",
                stream=True,
            )
        except Exception as e:
            logger.error("library_chat_stream failed: %s", e)
            yield f"Chat error: {e}"

    # ── Document management ───────────────────────────────────────────────────

    def save_upload(
        self, file_bytes: bytes, filename: str
    ) -> tuple:
        try:
            return pdf_service.save_upload(file_bytes=file_bytes, filename=filename)
        except Exception as e:
            logger.error("save_upload failed: %s", e)
            from app.models.schemas import ErrorResponse
            return None, ErrorResponse(error="Upload failed", detail=str(e))

    def list_documents(self) -> list:
        try:
            return pdf_service.list_documents()
        except Exception as e:
            logger.error("list_documents failed: %s", e)
            return []

    def delete_document(self, doc_id: str) -> bool:
        try:
            rag_service.delete_index(doc_id)
            return pdf_service.delete_document(doc_id)
        except Exception as e:
            logger.error("delete_document failed: %s", e)
            return False

    def get_provider_status(self) -> dict:
        try:
            return ai_router.get_provider_status()
        except Exception:
            return {
                "openrouter" : {"configured": bool(__import__("os").getenv("OPENROUTER_API_KEY")), "model": "unknown"},
                "huggingface": {"configured": bool(__import__("os").getenv("HUGGINGFACE_API_KEY")), "model": "unknown"},
            }

    def get_library_stats(self) -> dict:
        try:
            return repository.get_library_stats()
        except Exception as e:
            logger.error("get_library_stats failed: %s", e)
            return {}

    def get_recent_sync_runs(self, limit: int = 10) -> list:
        try:
            return repository.get_recent_sync_runs(limit=limit)
        except Exception as e:
            logger.error("get_recent_sync_runs failed: %s", e)
            return []

    def get_recent_logs(self, limit: int = 50) -> list[dict]:
        try:
            return repository.get_recent_processing_logs(limit=limit)
        except Exception as e:
            logger.error("get_recent_logs failed: %s", e)
            return []

    def sync_drive(self, on_file_found=None, on_file_done=None) -> dict:
        return drive_service.sync(on_file_found=on_file_found, on_file_done=on_file_done)

    def process_pending_ingestion_jobs(self, limit: int = 10, on_progress=None) -> dict:
        from app.services.ingestion_service import ingestion_service
        return ingestion_service.process_pending_jobs(limit=limit, on_progress=on_progress)

    def rebuild_library_index(self) -> bool:
        try:
            index, chunks = rag_service.build_library_index(force=True)
            return index is not None and bool(chunks)
        except Exception as e:
            logger.error("rebuild_library_index failed: %s", e)
            return False

    # ── Private ───────────────────────────────────────────────────────────────

    @staticmethod
    def _build_analysis_response(
        doc        : ProcessedDocument,
        start_time : float,
        message    : str,
    ) -> AnalysisResponse:
        return AnalysisResponse(
            doc_id             = doc.doc_id,
            status             = doc.status,
            message            = message,
            sections_found     = [s.section_type.value for s in doc.sections],
            chunk_count        = doc.chunk_count,
            page_count         = doc.metadata.page_count,
            word_count         = doc.metadata.word_count,
            processing_time_ms = round((time.time() - start_time) * 1000, 2),
        )


# ── Singleton ─────────────────────────────────────────────────────────────────
analysis_service = AnalysisService()

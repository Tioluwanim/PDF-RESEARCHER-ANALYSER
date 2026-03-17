"""
rag_service.py - FAISS vector store management and semantic search (RAG).
Stores, persists, and queries embeddings for each document.
"""

import json
import time
import numpy as np
from pathlib import Path
from typing import Optional

import faiss

from app.config import (
    VECTORSTORE_DIR,
    TOP_K_RESULTS,
    SIMILARITY_THRESHOLD,
    EMBEDDING_DIMENSION,
)
from app.models.schemas import (
    ProcessedDocument,
    TextChunk,
    SearchResult,
    SearchResponse,
    DocumentStatus,
)
from app.services.embedding_service import embedding_service
from app.utils.logger import get_logger, ServiceLogger

logger = get_logger(__name__)


# ── RAG Service ───────────────────────────────────────────────────────────────

class RAGService:
    """
    Manages FAISS vector indexes per document.

    Each document gets its own FAISS index stored in:
        data/vectorstore/<doc_id>/
            index.faiss   ← the FAISS index binary
            chunks.json   ← chunk metadata (text + section info)

    This separation means documents are independently searchable
    and deletable without affecting each other.
    """

    def __init__(self):
        self.vectorstore_dir = VECTORSTORE_DIR
        # In-memory cache: doc_id → (faiss.Index, list[TextChunk])
        self._index_cache: dict[str, tuple[faiss.Index, list[TextChunk]]] = {}
        logger.info("RAGService initialised")

    # ── Index Building ────────────────────────────────────────────────────────

    def build_index(self, doc: ProcessedDocument) -> ProcessedDocument:
        """
        Generates embeddings for all chunks and builds a FAISS index.
        Persists the index to disk and updates the document's
        vector_index_path and status.

        Args:
            doc: ProcessedDocument with status EXTRACTED and chunks populated.

        Returns:
            Updated ProcessedDocument with status READY or FAILED.
        """
        slog = ServiceLogger("rag_service", doc_id=doc.doc_id)
        slog.info(f"Building FAISS index for '{doc.filename}' "
                  f"({len(doc.chunks)} chunks)")

        try:
            doc.status = DocumentStatus.EMBEDDING

            if not doc.chunks:
                raise ValueError("Document has no chunks to index")

            # Step 1 — Generate embeddings
            slog.info("Generating embeddings ...")
            embeddings = embedding_service.embed_chunks(
                chunks  = doc.chunks,
                doc_id  = doc.doc_id,
                show_progress = False,
            )

            # Step 2 — Build FAISS index (IndexFlatIP = inner product on L2-normalized = cosine)
            dimension = embeddings.shape[1]
            index     = faiss.IndexFlatIP(dimension)
            index.add(embeddings)

            slog.info(f"FAISS index built — {index.ntotal} vectors, dim={dimension}")

            # Step 3 — Persist to disk
            index_dir = self._index_dir(doc.doc_id)
            index_dir.mkdir(parents=True, exist_ok=True)

            index_path  = index_dir / "index.faiss"
            chunks_path = index_dir / "chunks.json"

            faiss.write_index(index, str(index_path))
            chunks_path.write_text(
                json.dumps(
                    [c.model_dump() for c in doc.chunks],
                    default=str,
                    indent=2,
                ),
                encoding="utf-8",
            )

            slog.info(f"Index persisted → {index_dir}")

            # Step 4 — Cache in memory
            self._index_cache[doc.doc_id] = (index, doc.chunks)

            # Step 5 — Update document
            doc.vector_index_path = str(index_dir)
            doc.status            = DocumentStatus.READY

            slog.info("FAISS index ready ✓")

        except Exception as e:
            doc.status        = DocumentStatus.FAILED
            doc.error_message = str(e)
            slog.error(f"Index build failed: {e}", exc_info=True)

        return doc

    # ── Search ────────────────────────────────────────────────────────────────

    def search(
        self,
        doc_id  : str,
        query   : str,
        top_k   : int   = TOP_K_RESULTS,
        threshold: float = SIMILARITY_THRESHOLD,
    ) -> SearchResponse:
        """
        Performs semantic search against a document's FAISS index.

        Steps:
        1. Embed the query
        2. Search FAISS for nearest neighbours
        3. Filter by similarity threshold
        4. Return ranked SearchResult list

        Args:
            doc_id:    Document to search.
            query:     User's question or search string.
            top_k:     Number of results to return.
            threshold: Minimum similarity score (0.0–1.0).

        Returns:
            SearchResponse with ranked results and timing info.
        """
        slog       = ServiceLogger("rag_service", doc_id=doc_id)
        start_time = time.time()

        slog.debug(f"Searching: '{query[:80]}'")

        # Load index (from cache or disk)
        index, chunks = self._load_index(doc_id, slog)

        if index is None or not chunks:
            slog.warning("Index not available — returning empty results")
            return SearchResponse(
                query      = query,
                doc_id     = doc_id,
                results    = [],
                total_found= 0,
            )

        # Embed query
        query_vec = embedding_service.embed_query(query)  # shape (1, dim)

        # FAISS search
        actual_k  = min(top_k, index.ntotal)
        scores, indices = index.search(query_vec, actual_k)

        # Build results
        results: list[SearchResult] = []
        for rank, (score, idx) in enumerate(zip(scores[0], indices[0]), start=1):
            if idx == -1:
                continue  # FAISS returns -1 for empty slots
            if float(score) < threshold:
                continue  # below similarity threshold

            chunk = chunks[idx]
            results.append(
                SearchResult(
                    chunk = chunk,
                    score = round(float(score), 4),
                    rank  = rank,
                )
            )

        elapsed_ms = round((time.time() - start_time) * 1000, 2)

        slog.info(
            f"Search complete — {len(results)}/{actual_k} results "
            f"above threshold={threshold} in {elapsed_ms}ms"
        )

        return SearchResponse(
            query         = query,
            doc_id        = doc_id,
            results       = results,
            total_found   = len(results),
            search_time_ms= elapsed_ms,
        )

    # ── Context Builder ───────────────────────────────────────────────────────

    def get_context(
        self,
        doc_id   : str,
        query    : str,
        top_k    : int   = TOP_K_RESULTS,
        threshold: float = SIMILARITY_THRESHOLD,
        max_chars: int   = 3000,
    ) -> tuple[str, list[SearchResult]]:
        """
        Retrieves and formats relevant chunks as a context string
        for the LLM prompt.

        Args:
            doc_id:    Document to search.
            query:     User's question.
            top_k:     Max chunks to retrieve.
            threshold: Minimum similarity score.
            max_chars: Hard cap on total context characters.

        Returns:
            (context_string, list[SearchResult])
            context_string is ready to insert into an LLM prompt.
        """
        response = self.search(
            doc_id    = doc_id,
            query     = query,
            top_k     = top_k,
            threshold = threshold,
        )

        if not response.results:
            return "", []

        # Build context string — most relevant chunks first
        context_parts: list[str] = []
        total_chars   = 0

        for result in response.results:
            chunk_text = (
                f"[{result.chunk.section_type.value.upper()} | "
                f"Chunk {result.chunk.chunk_index + 1} | "
                f"Score: {result.score:.3f}]\n"
                f"{result.chunk.content}"
            )

            if total_chars + len(chunk_text) > max_chars:
                # Add partial chunk up to the limit
                remaining = max_chars - total_chars
                if remaining > 100:
                    context_parts.append(chunk_text[:remaining] + "...")
                break

            context_parts.append(chunk_text)
            total_chars += len(chunk_text)

        context = "\n\n---\n\n".join(context_parts)
        return context, response.results

    # ── Index Management ──────────────────────────────────────────────────────

    def index_exists(self, doc_id: str) -> bool:
        """Returns True if a FAISS index exists on disk for this document."""
        return (self._index_dir(doc_id) / "index.faiss").exists()

    def delete_index(self, doc_id: str) -> bool:
        """
        Removes the FAISS index from disk and memory cache.

        Returns:
            True if index was found and deleted, False otherwise.
        """
        import shutil
        index_dir = self._index_dir(doc_id)

        # Remove from cache
        self._index_cache.pop(doc_id, None)

        if index_dir.exists():
            shutil.rmtree(index_dir)
            logger.info(f"[{doc_id}] FAISS index deleted")
            return True

        return False

    def get_index_stats(self, doc_id: str) -> dict:
        """
        Returns stats about a loaded/persisted index.
        Useful for debugging and the Streamlit sidebar.
        """
        index, chunks = self._load_index(doc_id, ServiceLogger("rag_service", doc_id))

        if index is None:
            return {"status": "not_found", "doc_id": doc_id}

        return {
            "doc_id"      : doc_id,
            "status"      : "loaded",
            "total_vectors": index.ntotal,
            "dimension"   : index.d,
            "total_chunks": len(chunks),
            "cached"      : doc_id in self._index_cache,
        }

    # ── Private ───────────────────────────────────────────────────────────────

    def _load_index(
        self,
        doc_id: str,
        slog  : ServiceLogger,
    ) -> tuple[Optional[faiss.Index], list[TextChunk]]:
        """
        Returns (faiss.Index, chunks) from memory cache or disk.
        Returns (None, []) if not found.
        """
        # Check memory cache first
        if doc_id in self._index_cache:
            slog.debug("Index loaded from memory cache")
            return self._index_cache[doc_id]

        # Try loading from disk
        index_path  = self._index_dir(doc_id) / "index.faiss"
        chunks_path = self._index_dir(doc_id) / "chunks.json"

        if not index_path.exists() or not chunks_path.exists():
            slog.warning(f"Index files not found at {self._index_dir(doc_id)}")
            return None, []

        try:
            index  = faiss.read_index(str(index_path))
            raw    = json.loads(chunks_path.read_text(encoding="utf-8"))
            chunks = [TextChunk.model_validate(c) for c in raw]

            # Populate cache
            self._index_cache[doc_id] = (index, chunks)
            slog.info(
                f"Index loaded from disk — {index.ntotal} vectors, "
                f"{len(chunks)} chunks"
            )
            return index, chunks

        except Exception as e:
            slog.error(f"Failed to load index from disk: {e}", exc_info=True)
            return None, []

    def _index_dir(self, doc_id: str) -> Path:
        """Returns the directory path for a document's FAISS index."""
        return self.vectorstore_dir / doc_id


# ── Module-level singleton ────────────────────────────────────────────────────
rag_service = RAGService()
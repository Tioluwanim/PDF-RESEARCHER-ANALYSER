"""
rag_service.py - FAISS vector store with multi-query retrieval.

Key improvements:
- Multi-query RAG: runs 3 query variants and merges/deduplicates results
  so generic questions like "what is this about?" always find context
- Raised max_chars from 3,000 to 6,000 so the LLM gets enough context
- Graceful fallback: if threshold finds 0 results, retry with 0.0 threshold
  (returns top-k regardless of score) so the LLM always gets something
- FAISS IndexFlatIP (cosine on L2-normalised vectors) unchanged — correct
"""

from __future__ import annotations

import json
import re
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


class RAGService:
    """
    Manages per-document FAISS indexes.

    Storage layout:
        data/vectorstore/<doc_id>/
            index.faiss   — FAISS binary
            chunks.json   — serialised TextChunk list
    """

    def __init__(self) -> None:
        self.vectorstore_dir = VECTORSTORE_DIR
        self._index_cache: dict[str, tuple[faiss.Index, list[TextChunk]]] = {}
        logger.info("RAGService initialised")

    # ── Index building ────────────────────────────────────────────────────────

    def build_index(self, doc: ProcessedDocument) -> ProcessedDocument:
        slog = ServiceLogger("rag_service", doc_id=doc.doc_id)
        slog.info(
            "Building FAISS index for '%s' (%d chunks)",
            doc.filename, len(doc.chunks),
        )

        try:
            doc.status = DocumentStatus.EMBEDDING

            if not doc.chunks:
                raise ValueError("Document has no chunks to index")

            embeddings = embedding_service.embed_chunks(
                chunks=doc.chunks, doc_id=doc.doc_id
            )

            dimension = embeddings.shape[1]
            index     = faiss.IndexFlatIP(dimension)
            index.add(embeddings)

            slog.info(
                "FAISS index built — %d vectors, dim=%d",
                index.ntotal, dimension,
            )

            index_dir   = self._index_dir(doc.doc_id)
            index_dir.mkdir(parents=True, exist_ok=True)
            index_path  = index_dir / "index.faiss"
            chunks_path = index_dir / "chunks.json"

            faiss.write_index(index, str(index_path))
            chunks_path.write_text(
                json.dumps(
                    [c.model_dump() for c in doc.chunks],
                    default=str, indent=2,
                ),
                encoding="utf-8",
            )

            self._index_cache[doc.doc_id] = (index, doc.chunks)
            doc.vector_index_path = str(index_dir)
            doc.status            = DocumentStatus.READY
            slog.info("FAISS index ready ✓")

        except Exception as e:
            doc.status        = DocumentStatus.FAILED
            doc.error_message = str(e)
            slog.error("Index build failed: %s", e)

        return doc

    # ── Search ────────────────────────────────────────────────────────────────

    def search(
        self,
        doc_id   : str,
        query    : str,
        top_k    : int   = TOP_K_RESULTS,
        threshold: float = SIMILARITY_THRESHOLD,
    ) -> SearchResponse:
        """Single-query semantic search."""
        slog       = ServiceLogger("rag_service", doc_id=doc_id)
        start_time = time.time()

        index, chunks = self._load_index(doc_id, slog)
        if index is None or not chunks:
            return SearchResponse(
                query=query, doc_id=doc_id, results=[], total_found=0
            )

        query_vec        = embedding_service.embed_query(query)
        actual_k         = min(top_k, index.ntotal)
        scores, indices  = index.search(query_vec, actual_k)

        results: list[SearchResult] = []
        for rank, (score, idx) in enumerate(
            zip(scores[0], indices[0]), start=1
        ):
            if idx == -1 or float(score) < threshold:
                continue
            results.append(
                SearchResult(
                    chunk = chunks[idx],
                    score = round(float(score), 4),
                    rank  = rank,
                )
            )

        elapsed_ms = round((time.time() - start_time) * 1000, 2)
        slog.info(
            "Search complete — %d/%d results above threshold=%.2f in %.1fms",
            len(results), actual_k, threshold, elapsed_ms,
        )

        return SearchResponse(
            query          = query,
            doc_id         = doc_id,
            results        = results,
            total_found    = len(results),
            search_time_ms = elapsed_ms,
        )

    # ── Context builder (multi-query) ─────────────────────────────────────────

    def get_context(
        self,
        doc_id   : str,
        query    : str,
        top_k    : int   = TOP_K_RESULTS,
        threshold: float = SIMILARITY_THRESHOLD,
        max_chars: int   = 6000,    # raised from 3000 — LLM needs more context
    ) -> tuple[str, list[SearchResult]]:
        """
        Multi-query context retrieval.

        Runs up to 3 query variants:
          1. Original question
          2. Keywords extracted from the question
          3. A short imperative form ("explain X / describe Y")

        Merges and deduplicates by chunk_id, keeps top results by score.
        Falls back to threshold=0 if nothing passes the threshold,
        so the LLM always receives some context.

        Returns:
            (context_string, list[SearchResult])
        """
        slog = ServiceLogger("rag_service", doc_id=doc_id)

        index, chunks = self._load_index(doc_id, slog)
        if index is None or not chunks:
            return "", []

        # Build query variants
        queries = self._expand_query(query)
        slog.debug("Multi-query variants: %s", queries)

        # Run all variants and collect unique results
        seen_ids: set[str] = set()
        all_results: list[SearchResult] = []

        for q in queries:
            try:
                qvec            = embedding_service.embed_query(q)
                actual_k        = min(top_k * 2, index.ntotal)
                scores, indices = index.search(qvec, actual_k)

                for score, idx in zip(scores[0], indices[0]):
                    if idx == -1:
                        continue
                    chunk = chunks[idx]
                    if chunk.chunk_id in seen_ids:
                        continue
                    if float(score) >= threshold:
                        seen_ids.add(chunk.chunk_id)
                        all_results.append(
                            SearchResult(
                                chunk = chunk,
                                score = round(float(score), 4),
                                rank  = 0,
                            )
                        )
            except Exception as e:
                slog.warning("Multi-query variant failed: %s", e)

        # Fallback: if still empty, return top-k regardless of threshold
        if not all_results:
            slog.warning(
                "No results above threshold=%.2f — "
                "returning top-%d without threshold filter",
                threshold, top_k,
            )
            qvec            = embedding_service.embed_query(queries[0])
            actual_k        = min(top_k, index.ntotal)
            scores, indices = index.search(qvec, actual_k)
            for score, idx in zip(scores[0], indices[0]):
                if idx == -1:
                    continue
                chunk = chunks[idx]
                if chunk.chunk_id not in seen_ids:
                    seen_ids.add(chunk.chunk_id)
                    all_results.append(
                        SearchResult(
                            chunk = chunk,
                            score = round(float(score), 4),
                            rank  = 0,
                        )
                    )

        # Sort by score descending, assign ranks
        all_results.sort(key=lambda r: r.score, reverse=True)
        for i, r in enumerate(all_results):
            r.rank = i + 1

        # Build context string within max_chars budget
        context_parts : list[str] = []
        total_chars   = 0
        used_results  : list[SearchResult] = []

        for result in all_results[:top_k]:
            header     = (
                f"[{result.chunk.section_type.value.upper()} | "
                f"Score: {result.score:.3f}]"
            )
            chunk_text = f"{header}\n{result.chunk.content}"

            if total_chars + len(chunk_text) > max_chars:
                remaining = max_chars - total_chars
                if remaining > 200:
                    context_parts.append(chunk_text[:remaining] + "…")
                    used_results.append(result)
                break

            context_parts.append(chunk_text)
            used_results.append(result)
            total_chars += len(chunk_text)

        context = "\n\n---\n\n".join(context_parts)
        slog.info(
            "Context built — %d chunks, %d chars",
            len(used_results), len(context),
        )
        return context, used_results

    # ── Index management ──────────────────────────────────────────────────────

    def index_exists(self, doc_id: str) -> bool:
        return (self._index_dir(doc_id) / "index.faiss").exists()

    def delete_index(self, doc_id: str) -> bool:
        import shutil
        index_dir = self._index_dir(doc_id)
        self._index_cache.pop(doc_id, None)
        if index_dir.exists():
            shutil.rmtree(index_dir)
            logger.info("[%s] FAISS index deleted", doc_id)
            return True
        return False

    def get_index_stats(self, doc_id: str) -> dict:
        index, chunks = self._load_index(
            doc_id, ServiceLogger("rag_service", doc_id)
        )
        if index is None:
            return {"status": "not_found", "doc_id": doc_id}
        return {
            "doc_id"       : doc_id,
            "status"       : "loaded",
            "total_vectors": index.ntotal,
            "dimension"    : index.d,
            "total_chunks" : len(chunks),
            "cached"       : doc_id in self._index_cache,
        }

    # ── Private ───────────────────────────────────────────────────────────────

    def _load_index(
        self,
        doc_id: str,
        slog  : ServiceLogger,
    ) -> tuple[Optional[faiss.Index], list[TextChunk]]:
        if doc_id in self._index_cache:
            return self._index_cache[doc_id]

        index_path  = self._index_dir(doc_id) / "index.faiss"
        chunks_path = self._index_dir(doc_id) / "chunks.json"

        if not index_path.exists() or not chunks_path.exists():
            slog.warning("Index files not found at %s", self._index_dir(doc_id))
            return None, []

        try:
            index  = faiss.read_index(str(index_path))
            raw    = json.loads(chunks_path.read_text(encoding="utf-8"))
            chunks = [TextChunk.model_validate(c) for c in raw]
            self._index_cache[doc_id] = (index, chunks)
            slog.info(
                "Index loaded from disk — %d vectors, %d chunks",
                index.ntotal, len(chunks),
            )
            return index, chunks
        except Exception as e:
            slog.error("Failed to load index: %s", e)
            return None, []

    def _index_dir(self, doc_id: str) -> Path:
        return self.vectorstore_dir / doc_id

    @staticmethod
    def _expand_query(question: str) -> list[str]:
        """
        Generates up to 3 query variants from the user's question
        to improve recall in multi-query RAG.

          1. Original question (always included)
          2. Keywords: strip stop words, keep nouns/verbs
          3. Imperative form: "describe / explain / what is"
        """
        q = question.strip()
        variants = [q]

        # Variant 2 — keywords only (remove question words and stop words)
        stop = {
            "what", "who", "when", "where", "why", "how",
            "is", "are", "was", "were", "the", "a", "an",
            "this", "that", "these", "those", "it", "its",
            "of", "in", "on", "at", "to", "for", "with",
            "and", "or", "but", "about", "does", "do",
            "did", "can", "could", "would", "should",
            "tell", "me", "please", "paper", "study",
        }
        words    = re.findall(r"\b\w{3,}\b", q.lower())
        keywords = [w for w in words if w not in stop]
        if keywords and " ".join(keywords) != q.lower():
            variants.append(" ".join(keywords))

        # Variant 3 — imperative / descriptive form
        lower = q.lower()
        if lower.startswith(("what is", "what are")):
            imperative = re.sub(r"^what (?:is|are)\s+", "describe ", lower)
            variants.append(imperative)
        elif lower.startswith(("how does", "how do")):
            imperative = re.sub(r"^how (?:does|do)\s+", "explain how ", lower)
            variants.append(imperative)
        elif lower.startswith("who"):
            imperative = re.sub(r"^who\s+", "identify the person who ", lower)
            variants.append(imperative)

        # Return unique variants only
        seen: set[str] = set()
        result: list[str] = []
        for v in variants:
            if v and v not in seen:
                seen.add(v)
                result.append(v)
        return result[:3]


# ── Singleton ─────────────────────────────────────────────────────────────────
rag_service = RAGService()
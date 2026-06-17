"""
rag_service.py - FAISS vector store with multi-query retrieval.

Improvements over v1:
  - Multi-query search runs its variants in PARALLEL via ThreadPoolExecutor
    instead of a sequential loop — on a 3-variant expansion this cuts
    retrieval latency roughly 2-3x since each FAISS search + embed_query
    call is independent.
  - MMR (Maximal Marginal Relevance) reranking after the initial similarity
    sort: greedily picks results that are both relevant to the query AND
    diverse from already-selected results, so the LLM doesn't receive 5
    near-duplicate chunks from the same paragraph.
  - max_chars now defaults to CONTEXT_WINDOW_TOKENS from config instead of
    a hardcoded 6000, so raising the config value actually changes behavior.
  - Query expansion deduplicates near-identical variants (Jaccard overlap
    check) so we don't waste a parallel search slot on a variant that's
    >90% the same words as another variant.
  - FAISS is imported lazily so module import stays stable.
"""

from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional

import numpy as np

from app.config import (
    CONTEXT_WINDOW_TOKENS,
    EMBEDDING_DIMENSION,
    SIMILARITY_THRESHOLD,
    TOP_K_RESULTS,
    VECTORSTORE_DIR,
)
from app.db.repository import repository
from app.models.schemas import (
    DocumentStatus,
    ProcessedDocument,
    SearchResponse,
    SearchResult,
    SectionType,
    TextChunk,
)
from app.services.embedding_service import embedding_service
from app.utils.logger import ServiceLogger, get_logger

logger = get_logger(__name__)

# MMR diversity trade-off: 1.0 = pure relevance, 0.0 = pure diversity.
# 0.7 favours relevance while still penalising near-duplicate chunks.
_MMR_LAMBDA = 0.7


def _get_faiss_module():
    try:
        import faiss
        return faiss
    except ImportError as exc:
        raise ImportError(
            "FAISS is required for vector search. Install faiss-cpu or faiss-gpu."
        ) from exc


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
        self._index_cache: dict[str, tuple[Any, list[TextChunk]]] = {}
        logger.info("RAGService initialised")

    # ── Index building ────────────────────────────────────────────────────────

    def build_index(self, doc: ProcessedDocument) -> ProcessedDocument:
        faiss = _get_faiss_module()
        slog = ServiceLogger("rag_service", doc_id=doc.doc_id)
        slog.info(
            "Building FAISS index for '%s' (%d chunks)",
            doc.filename,
            len(doc.chunks),
        )

        try:
            doc.status = DocumentStatus.EMBEDDING

            if not doc.chunks:
                raise ValueError("Document has no chunks to index")

            embeddings = embedding_service.embed_chunks(
                chunks=doc.chunks,
                doc_id=doc.doc_id,
            )
            repository.save_chunks(doc.doc_id, doc.chunks, embeddings=embeddings)
            self._index_cache.pop("library", None)

            if embeddings.ndim != 2 or embeddings.size == 0:
                raise ValueError("No embeddings were produced for this document")

            dimension = int(embeddings.shape[1])
            index = faiss.IndexFlatIP(dimension)
            index.add(embeddings.astype(np.float32))

            slog.info(
                "FAISS index built — %d vectors, dim=%d",
                index.ntotal,
                dimension,
            )

            index_dir = self._index_dir(doc.doc_id)
            index_dir.mkdir(parents=True, exist_ok=True)
            index_path = index_dir / "index.faiss"
            chunks_path = index_dir / "chunks.json"

            faiss.write_index(index, str(index_path))
            chunks_path.write_text(
                json.dumps([c.model_dump() for c in doc.chunks], default=str, indent=2),
                encoding="utf-8",
            )

            self._index_cache[doc.doc_id] = (index, doc.chunks)
            doc.vector_index_path = str(index_dir)
            doc.status = DocumentStatus.READY
            slog.info("FAISS index ready ✓")

        except Exception as e:
            doc.status = DocumentStatus.FAILED
            doc.error_message = str(e)
            slog.error("Index build failed: %s", e)

        return doc

    def _library_index_dir(self) -> Path:
        return self.vectorstore_dir / "library"

    def library_index_exists(self) -> bool:
        return (self._library_index_dir() / "index.faiss").exists()

    def _load_library_index(self) -> tuple[Optional[Any], list[TextChunk]]:
        faiss = _get_faiss_module()
        cache_key = "library"
        if cache_key in self._index_cache:
            return self._index_cache[cache_key]

        index_path = self._library_index_dir() / "index.faiss"
        chunks_path = self._library_index_dir() / "chunks.json"
        if not index_path.exists() or not chunks_path.exists():
            return None, []

        try:
            index = faiss.read_index(str(index_path))
            chunks = [
                TextChunk.model_validate(c)
                for c in json.loads(chunks_path.read_text(encoding="utf-8"))
            ]
            self._index_cache[cache_key] = (index, chunks)
            logger.info(
                "Library index loaded — %d vectors, %d chunk records",
                index.ntotal,
                len(chunks),
            )
            return index, chunks
        except Exception as e:
            logger.error("Failed to load library index: %s", e)
            return None, []

    def build_library_index(
        self,
        doc_ids: list[str] | None = None,
        author: str | None = None,
        year: str | None = None,
        section_type: SectionType | None = None,
        page_number: int | None = None,
        force: bool = False,
    ) -> tuple[Optional[Any], list[TextChunk]]:
        faiss = _get_faiss_module()
        slog = ServiceLogger("rag_service", doc_id="library")
        slog.info("Building library FAISS index")

        has_filters = bool(doc_ids or author or year or section_type or page_number is not None)
        if not force and not has_filters and self.library_index_exists():
            return self._load_library_index()

        rows = repository.get_library_chunks(
            doc_ids=doc_ids,
            author=author,
            year=year,
            section_type=section_type,
            page_number=page_number,
        )
        if not rows:
            slog.warning("No ready chunks found for library index")
            return None, []

        chunks: list[TextChunk] = []
        embeddings: list[np.ndarray] = []

        for chunk_record, document in rows:
            if not chunk_record.embedding:
                continue

            chunk = TextChunk(
                chunk_id=chunk_record.id,
                doc_id=document.doc_id,
                content=chunk_record.content,
                section_type=SectionType(chunk_record.section_type),
                chunk_index=chunk_record.chunk_index,
                total_chunks=chunk_record.total_chunks,
                page_number=chunk_record.page_number,
                word_count=chunk_record.word_count,
                char_count=chunk_record.char_count,
            )
            emb = np.frombuffer(chunk_record.embedding, dtype=np.float32)

            if emb.ndim != 1 or emb.size == 0:
                continue

            chunks.append(chunk)
            embeddings.append(emb)

        if not embeddings:
            slog.warning("No embeddings available for library index")
            return None, []

        matrix = np.vstack(embeddings).astype(np.float32)
        dimension = int(matrix.shape[1])
        index = faiss.IndexFlatIP(dimension)
        index.add(matrix)

        if not has_filters:
            index_dir = self._library_index_dir()
            index_dir.mkdir(parents=True, exist_ok=True)
            faiss.write_index(index, str(index_dir / "index.faiss"))
            (index_dir / "chunks.json").write_text(
                json.dumps([c.model_dump() for c in chunks], default=str, indent=2),
                encoding="utf-8",
            )
            self._index_cache["library"] = (index, chunks)

        slog.info("Library index built — %d vectors, %d chunks", index.ntotal, len(chunks))
        return index, chunks

    def search_library(
        self,
        query: str,
        top_k: int = TOP_K_RESULTS,
        threshold: float = SIMILARITY_THRESHOLD,
        doc_ids: list[str] | None = None,
        author: str | None = None,
        year: str | None = None,
        section_type: SectionType | None = None,
        page_number: int | None = None,
    ) -> SearchResponse:
        slog = ServiceLogger("rag_service", doc_id="library")
        start_time = time.time()

        index, chunks = self._resolve_library_index(
            doc_ids, author, year, section_type, page_number,
        )
        if index is None or not chunks:
            return SearchResponse(query=query, doc_id="library", results=[], total_found=0)

        query_vec = embedding_service.embed_query(query)
        actual_k = min(top_k, index.ntotal)
        scores, indices = index.search(query_vec, actual_k)

        results: list[SearchResult] = []
        for rank, (score, idx) in enumerate(zip(scores[0], indices[0]), start=1):
            idx_int = int(idx)
            if idx_int == -1 or float(score) < threshold:
                continue
            results.append(SearchResult(chunk=chunks[idx_int], score=round(float(score), 4), rank=rank))

        elapsed_ms = round((time.time() - start_time) * 1000, 2)
        slog.info(
            "Library search complete — %d/%d results above threshold=%.2f in %.1fms",
            len(results), actual_k, threshold, elapsed_ms,
        )
        return SearchResponse(
            query=query, doc_id="library", results=results,
            total_found=len(results), search_time_ms=elapsed_ms,
        )

    def get_library_context(
        self,
        query: str,
        top_k: int = TOP_K_RESULTS,
        threshold: float = SIMILARITY_THRESHOLD,
        max_chars: int = 0,   # 0 = use CONTEXT_WINDOW_TOKENS
        doc_ids: list[str] | None = None,
        author: str | None = None,
        year: str | None = None,
        section_type: SectionType | None = None,
        page_number: int | None = None,
    ) -> tuple[str, list[SearchResult]]:
        slog = ServiceLogger("rag_service", doc_id="library")
        max_chars = max_chars or CONTEXT_WINDOW_TOKENS

        index, chunks = self._resolve_library_index(
            doc_ids, author, year, section_type, page_number,
        )
        if index is None or not chunks:
            return "", []

        all_results = self._multi_query_search(
            index=index, chunks=chunks, query=query,
            top_k=top_k, threshold=threshold, slog=slog,
        )
        selected = self._mmr_select(all_results, index, top_k)
        return self._assemble_context(selected, max_chars, slog, with_doc_id=True)

    def _resolve_library_index(
        self, doc_ids, author, year, section_type, page_number,
    ) -> tuple[Optional[Any], list[TextChunk]]:
        has_filters = bool(doc_ids or author or year or section_type or page_number is not None)
        if has_filters:
            return self.build_library_index(
                doc_ids=doc_ids, author=author, year=year,
                section_type=section_type, page_number=page_number, force=True,
            )
        index, chunks = self._load_library_index()
        if index is None or not chunks:
            return self.build_library_index(
                doc_ids=doc_ids, author=author, year=year,
                section_type=section_type, page_number=page_number, force=True,
            )
        return index, chunks

    # ── Search ────────────────────────────────────────────────────────────────

    def search(
        self,
        doc_id: str,
        query: str,
        top_k: int = TOP_K_RESULTS,
        threshold: float = SIMILARITY_THRESHOLD,
    ) -> SearchResponse:
        slog = ServiceLogger("rag_service", doc_id=doc_id)
        start_time = time.time()

        index, chunks = self._load_index(doc_id, slog)
        if index is None or not chunks:
            return SearchResponse(query=query, doc_id=doc_id, results=[], total_found=0)

        query_vec = embedding_service.embed_query(query)
        actual_k = min(top_k, index.ntotal)
        scores, indices = index.search(query_vec, actual_k)

        results: list[SearchResult] = []
        for rank, (score, idx) in enumerate(zip(scores[0], indices[0]), start=1):
            idx_int = int(idx)
            if idx_int == -1 or float(score) < threshold:
                continue
            results.append(SearchResult(chunk=chunks[idx_int], score=round(float(score), 4), rank=rank))

        elapsed_ms = round((time.time() - start_time) * 1000, 2)
        slog.info(
            "Search complete — %d/%d results above threshold=%.2f in %.1fms",
            len(results), actual_k, threshold, elapsed_ms,
        )
        return SearchResponse(
            query=query, doc_id=doc_id, results=results,
            total_found=len(results), search_time_ms=elapsed_ms,
        )

    # ── Context builder (multi-query, parallel + MMR) ────────────────────────

    def get_context(
        self,
        doc_id: str,
        query: str,
        top_k: int = TOP_K_RESULTS,
        threshold: float = SIMILARITY_THRESHOLD,
        max_chars: int = 0,   # 0 = use CONTEXT_WINDOW_TOKENS
    ) -> tuple[str, list[SearchResult]]:
        slog = ServiceLogger("rag_service", doc_id=doc_id)
        max_chars = max_chars or CONTEXT_WINDOW_TOKENS

        index, chunks = self._load_index(doc_id, slog)
        if index is None or not chunks:
            return "", []

        all_results = self._multi_query_search(
            index=index, chunks=chunks, query=query,
            top_k=top_k, threshold=threshold, slog=slog,
        )
        selected = self._mmr_select(all_results, index, top_k)
        return self._assemble_context(selected, max_chars, slog, with_doc_id=False)

    # ── Shared multi-query + MMR machinery ────────────────────────────────────

    def _multi_query_search(
        self,
        index    : Any,
        chunks   : list[TextChunk],
        query    : str,
        top_k    : int,
        threshold: float,
        slog     : ServiceLogger,
    ) -> list[SearchResult]:
        """
        Runs query-expansion variants IN PARALLEL (ThreadPoolExecutor) and
        merges/deduplicates the results. Each variant does its own
        embed_query + FAISS search; these are independent so they parallelise
        cleanly. Falls back to threshold=0.0 if nothing clears the bar.
        """
        queries = self._expand_query(query)
        slog.debug("Multi-query variants (%d): %s", len(queries), queries)

        seen_ids: set[str] = set()
        all_results: list[SearchResult] = []

        def _run_variant(q: str) -> list[tuple[float, int]]:
            qvec = embedding_service.embed_query(q)
            actual_k = min(top_k * 2, index.ntotal)
            scores, indices = index.search(qvec, actual_k)
            return list(zip(scores[0].tolist(), indices[0].tolist()))

        with ThreadPoolExecutor(max_workers=min(len(queries), 4), thread_name_prefix="rag_q") as pool:
            futures = {pool.submit(_run_variant, q): q for q in queries}
            for future in as_completed(futures):
                q = futures[future]
                try:
                    pairs = future.result()
                except Exception as e:
                    slog.warning("Multi-query variant '%s' failed: %s", q, e)
                    continue

                for score, idx in pairs:
                    idx_int = int(idx)
                    if idx_int == -1:
                        continue
                    chunk = chunks[idx_int]
                    if chunk.chunk_id in seen_ids:
                        continue
                    if float(score) >= threshold:
                        seen_ids.add(chunk.chunk_id)
                        all_results.append(
                            SearchResult(chunk=chunk, score=round(float(score), 4), rank=0)
                        )

        if not all_results:
            slog.warning(
                "No results above threshold=%.2f — returning top-%d without filter",
                threshold, top_k,
            )
            qvec = embedding_service.embed_query(queries[0])
            actual_k = min(top_k, index.ntotal)
            scores, indices = index.search(qvec, actual_k)
            for score, idx in zip(scores[0], indices[0]):
                idx_int = int(idx)
                if idx_int == -1:
                    continue
                chunk = chunks[idx_int]
                if chunk.chunk_id not in seen_ids:
                    seen_ids.add(chunk.chunk_id)
                    all_results.append(
                        SearchResult(chunk=chunk, score=round(float(score), 4), rank=0)
                    )

        all_results.sort(key=lambda r: r.score, reverse=True)
        for i, r in enumerate(all_results):
            r.rank = i + 1
        return all_results

    def _mmr_select(
        self,
        candidates : list[SearchResult],
        index      : Any,
        top_k      : int,
        pool_size  : int = 0,
    ) -> list[SearchResult]:
        """
        Maximal Marginal Relevance reranking.

        Greedily picks the candidate that maximises:
            lambda * relevance(c) - (1 - lambda) * max_similarity(c, selected)

        This prevents the context window from being filled with several
        near-duplicate chunks (e.g. 4 chunks all from the same paragraph
        because it scored highest on every query variant) at the expense
        of chunks covering different parts of the document.

        Falls back to plain top-k order if fewer than 3 candidates, since
        MMR has no diversification benefit on tiny candidate sets.
        """
        if len(candidates) <= max(top_k, 2):
            return candidates[:top_k]

        pool_size = pool_size or min(len(candidates), top_k * 3)
        pool = candidates[:pool_size]

        # Re-embed pool chunk contents directly for the diversity comparison.
        # Pool size is bounded (top_k*3, typically <=24) so this stays cheap
        # even though it doesn't reuse the original FAISS-indexed vectors.
        try:
            texts = [r.chunk.content for r in pool]
            pool_vecs = embedding_service.embed_texts(texts)
        except Exception:
            return candidates[:top_k]  # graceful fallback on embed failure

        selected_idx: list[int] = []
        remaining_idx = list(range(len(pool)))

        # Seed with the highest-relevance candidate
        selected_idx.append(0)
        remaining_idx.remove(0)

        while len(selected_idx) < min(top_k, len(pool)) and remaining_idx:
            best_idx = None
            best_score = float("-inf")
            for i in remaining_idx:
                relevance = pool[i].score
                max_sim = max(
                    float(np.dot(pool_vecs[i], pool_vecs[j]))
                    for j in selected_idx
                )
                mmr_score = _MMR_LAMBDA * relevance - (1 - _MMR_LAMBDA) * max_sim
                if mmr_score > best_score:
                    best_score = mmr_score
                    best_idx = i
            if best_idx is None:
                break
            selected_idx.append(best_idx)
            remaining_idx.remove(best_idx)

        result = [pool[i] for i in selected_idx]
        for i, r in enumerate(result):
            r.rank = i + 1
        return result

    def _assemble_context(
        self,
        results    : list[SearchResult],
        max_chars  : int,
        slog       : ServiceLogger,
        with_doc_id: bool,
    ) -> tuple[str, list[SearchResult]]:
        context_parts: list[str] = []
        total_chars = 0
        used_results: list[SearchResult] = []

        for result in results:
            if with_doc_id:
                header = (
                    f"[{result.chunk.doc_id} | {result.chunk.section_type.value.upper()} | "
                    f"Score: {result.score:.3f}]"
                )
            else:
                header = f"[{result.chunk.section_type.value.upper()} | Score: {result.score:.3f}]"
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
        slog.info("Context built — %d chunks, %d chars", len(used_results), len(context))
        return context, used_results

    # ── Index management ──────────────────────────────────────────────────────

    def index_exists(self, doc_id: str) -> bool:
        return (self._index_dir(doc_id) / "index.faiss").exists()

    def delete_index(self, doc_id: str) -> bool:
        import shutil

        index_dir = self._index_dir(doc_id)
        self._index_cache.pop(doc_id, None)
        self._index_cache.pop("library", None)
        if index_dir.exists():
            shutil.rmtree(index_dir)
            logger.info("[%s] FAISS index deleted", doc_id)
            return True
        return False

    def get_index_stats(self, doc_id: str) -> dict:
        index, chunks = self._load_index(doc_id, ServiceLogger("rag_service", doc_id))
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

    def _load_index(self, doc_id: str, slog: ServiceLogger) -> tuple[Optional[Any], list[TextChunk]]:
        faiss = _get_faiss_module()

        if doc_id in self._index_cache:
            return self._index_cache[doc_id]

        index_path = self._index_dir(doc_id) / "index.faiss"
        chunks_path = self._index_dir(doc_id) / "chunks.json"

        if not index_path.exists() or not chunks_path.exists():
            slog.warning("Index files not found at %s", self._index_dir(doc_id))
            return None, []

        try:
            index = faiss.read_index(str(index_path))
            raw = json.loads(chunks_path.read_text(encoding="utf-8"))
            chunks = [TextChunk.model_validate(c) for c in raw]
            self._index_cache[doc_id] = (index, chunks)
            slog.info("Index loaded from disk — %d vectors, %d chunks", index.ntotal, len(chunks))
            return index, chunks
        except Exception as e:
            slog.error("Failed to load index: %s", e)
            return None, []

    def _index_dir(self, doc_id: str) -> Path:
        return self.vectorstore_dir / doc_id

    @staticmethod
    def _expand_query(question: str) -> list[str]:
        """
        Generates up to 3 query variants from the user's question.
        Deduplicates near-identical variants via word-set Jaccard overlap
        so we don't burn a parallel search slot on a near-duplicate phrase.
        """
        q = question.strip()
        variants = [q]

        stop = {
            "what", "who", "when", "where", "why", "how",
            "is", "are", "was", "were", "the", "a", "an",
            "this", "that", "these", "those", "it", "its",
            "of", "in", "on", "at", "to", "for", "with",
            "and", "or", "but", "about", "does", "do",
            "did", "can", "could", "would", "should",
            "tell", "me", "please", "paper", "study",
        }

        words = re.findall(r"\b\w{3,}\b", q.lower())
        keywords = [w for w in words if w not in stop]
        if keywords:
            candidate = " ".join(keywords)
            if _jaccard(candidate, q.lower()) < 0.9:
                variants.append(candidate)

        lower = q.lower()
        imperative = None
        if lower.startswith(("what is", "what are")):
            imperative = re.sub(r"^what (?:is|are)\s+", "describe ", lower)
        elif lower.startswith(("how does", "how do")):
            imperative = re.sub(r"^how (?:does|do)\s+", "explain how ", lower)
        elif lower.startswith("who"):
            imperative = re.sub(r"^who\s+", "identify the person who ", lower)

        if imperative and all(_jaccard(imperative, v.lower()) < 0.9 for v in variants):
            variants.append(imperative)

        return variants[:3]


def _jaccard(a: str, b: str) -> float:
    """Word-set Jaccard similarity, used to dedupe near-identical query variants."""
    sa, sb = set(a.split()), set(b.split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


# ── Singleton ─────────────────────────────────────────────────────────────────
rag_service = RAGService()

"""
embedding_service.py - Local sentence-transformers embeddings.

Improvements over v1:
  - Batch size scales more aggressively with chunk count (32/64/128/256)
    so encoding 10k-30k chunks (a 50-100 PDF batch) doesn't bottleneck on
    tiny batches with high per-call overhead.
  - torch thread count explicitly set to os.cpu_count() when not already
    configured, so CPU-bound encoding uses all available cores.
  - Model cached in st.session_state when running under Streamlit,
    preventing expensive reloads on every Streamlit rerun.
  - Suppresses all HuggingFace/transformers noise before any import.
  - Graceful fallback encoder if Sentence Transformers cannot load.
  - Query embedding cache (LRU, size 256) — repeated searches/chat turns
    on the same question don't re-run the model.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from functools import lru_cache
from typing import Optional

import numpy as np

# Silence HF noise before any optional model import happens.
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ.setdefault("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1")
os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
logging.getLogger("transformers").setLevel(logging.ERROR)
logging.getLogger("sentence_transformers").setLevel(logging.WARNING)
logging.getLogger("huggingface_hub").setLevel(logging.WARNING)

# Make sure CPU-bound encoding uses all available cores unless the
# deployment environment has already pinned thread counts explicitly.
if "OMP_NUM_THREADS" not in os.environ and "TORCH_NUM_THREADS" not in os.environ:
    _cpu_count = os.cpu_count() or 4
    os.environ.setdefault("OMP_NUM_THREADS", str(_cpu_count))

from app.config import EMBEDDING_DIMENSION, EMBEDDING_MODEL
from app.models.schemas import TextChunk
from app.utils.logger import ServiceLogger, get_logger

logger = get_logger(__name__)

_ST_CACHE_KEY = "_embedding_model_singleton"
_module_model_cache: Optional[object] = None


def _set_torch_threads() -> None:
    """Set torch's intra-op thread count to all available CPUs, once."""
    try:
        import torch
        cpu_count = os.cpu_count() or 4
        if torch.get_num_threads() < cpu_count:
            torch.set_num_threads(cpu_count)
    except Exception:
        pass


class _FallbackEmbeddingModel:
    """
    Deterministic local fallback for environments where Sentence Transformers
    cannot load. Produces normalized hashing-based vectors so the app keeps
    working instead of hard-failing during index build.
    """

    def __init__(self, dimension: int) -> None:
        self.dimension = int(dimension)

    def encode(
        self,
        texts,
        batch_size: int = 32,
        show_progress_bar: bool = False,
        convert_to_numpy: bool = True,
        normalize_embeddings: bool = True,
    ):
        if isinstance(texts, str):
            texts = [texts]

        vectors: list[np.ndarray] = []

        for text in texts:
            vec = np.zeros(self.dimension, dtype=np.float32)
            cleaned = (text or "").strip()

            if cleaned:
                tokens = re.findall(r"[A-Za-z0-9_']+", cleaned.lower())
                if not tokens:
                    tokens = [cleaned.lower()]

                for token in tokens:
                    digest = hashlib.blake2b(
                        token.encode("utf-8"),
                        digest_size=16,
                    ).digest()

                    idx = int.from_bytes(digest[:4], "little") % self.dimension
                    sign = 1.0 if (digest[4] % 2 == 0) else -1.0
                    weight = 1.0 + (digest[5] / 255.0)
                    vec[idx] += sign * weight

            if normalize_embeddings:
                norm = float(np.linalg.norm(vec))
                if norm > 0:
                    vec /= norm

            vectors.append(vec)

        if convert_to_numpy:
            return np.vstack(vectors) if vectors else np.empty((0, self.dimension), dtype=np.float32)

        return vectors


def _store_model(model: object) -> None:
    global _module_model_cache

    try:
        import streamlit as st

        st.session_state[_ST_CACHE_KEY] = model
        return
    except Exception:
        pass

    _module_model_cache = model


def _load_sentence_transformer_model(model_name: str):
    """
    Load SentenceTransformer lazily. This is only called when embeddings are
    actually needed.
    """
    from sentence_transformers import SentenceTransformer
    _set_torch_threads()
    return SentenceTransformer(model_name)


def _get_or_load_model(model_name: str):
    """Load SentenceTransformer lazily — only when embeddings are first needed."""
    try:
        import streamlit as st

        if _ST_CACHE_KEY in st.session_state:
            return st.session_state[_ST_CACHE_KEY]
    except Exception:
        pass

    global _module_model_cache
    if _module_model_cache is not None:
        return _module_model_cache

    try:
        logger.info("Loading embedding model '%s' …", model_name)
        model = _load_sentence_transformer_model(model_name)
        logger.info("Embedding model loaded ✓")
    except Exception as exc:
        logger.exception(
            "SentenceTransformer load failed for '%s'; using fallback encoder: %s",
            model_name,
            exc,
        )
        model = _FallbackEmbeddingModel(EMBEDDING_DIMENSION)
        logger.warning(
            "Fallback hashing embeddings active. Retrieval quality may be lower."
        )

    _store_model(model)
    return model


def _auto_batch_size(n_texts: int) -> int:
    """
    Scale batch size with input volume to amortise per-call overhead.

    Small batches (<50 texts, e.g. a single short PDF) use 32 to keep
    memory low. Large batches (a 50-100 PDF sync producing 10k-30k chunks)
    use up to 256, which on CPU sentence-transformers models cuts total
    encode wall-clock time substantially versus fixed small batches.
    """
    if n_texts < 50:
        return 32
    if n_texts < 500:
        return 64
    if n_texts < 5000:
        return 128
    return 256


class EmbeddingService:
    """
    Wraps sentence-transformers for encoding chunks and queries.
    Model loads once and is reused across all requests.
    """

    def __init__(self) -> None:
        self._model_name = EMBEDDING_MODEL
        self._dimension = EMBEDDING_DIMENSION
        self._dimension_verified = False
        logger.info("EmbeddingService ready (model will load on first use)")

    # ── Model property ────────────────────────────────────────────────────────

    @property
    def model(self):
        m = _get_or_load_model(self._model_name)

        if not self._dimension_verified:
            try:
                probe = m.encode(
                    ["probe"],
                    show_progress_bar=False,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                )
                probe_arr = np.asarray(probe)

                if probe_arr.ndim == 1:
                    actual = int(probe_arr.shape[0])
                elif probe_arr.ndim >= 2:
                    actual = int(probe_arr.shape[-1])
                else:
                    actual = self._dimension

                if actual != self._dimension:
                    logger.warning(
                        "Embedding dimension mismatch: config=%d actual=%d",
                        self._dimension,
                        actual,
                    )
                    self._dimension = actual
            except Exception as exc:
                logger.debug("Dimension probe skipped: %s", exc)

            self._dimension_verified = True

        return m

    # ── Public API ────────────────────────────────────────────────────────────

    def embed_chunks(
        self,
        chunks: list[TextChunk],
        doc_id: str = "",
        batch_size: int = 0,  # 0 = auto
        show_progress: bool = False,
    ) -> np.ndarray:
        slog = ServiceLogger("embedding_service", doc_id=doc_id)

        if not chunks:
            slog.warning("embed_chunks called with empty list")
            return np.empty((0, self._dimension), dtype=np.float32)

        if batch_size == 0:
            batch_size = _auto_batch_size(len(chunks))

        slog.info("Embedding %d chunks (batch=%d) …", len(chunks), batch_size)
        texts = [c.content for c in chunks]
        vecs = self.model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=show_progress,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        vecs = np.asarray(vecs, dtype=np.float32)

        if vecs.ndim == 1:
            vecs = vecs.reshape(1, -1)

        slog.info("Embeddings done ✓  shape=%s", str(vecs.shape))
        return vecs

    def embed_query(self, query: str) -> np.ndarray:
        if not query or not query.strip():
            raise ValueError("Query must not be empty.")

        cached = self._embed_query_cached(query.strip())
        return cached.copy()  # caller may mutate; never hand out the cached array

    @lru_cache(maxsize=256)
    def _embed_query_cached(self, query: str) -> np.ndarray:
        vec = self.model.encode(
            [query],
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        vec = np.asarray(vec, dtype=np.float32)
        if vec.ndim == 1:
            vec = vec.reshape(1, -1)
        # Freeze so the cached array can't be mutated by a caller holding a
        # reference; embed_query() always returns a fresh .copy() anyway.
        vec.setflags(write=False)
        return vec

    def embed_texts(
        self,
        texts: list[str],
        batch_size: int = 0,  # 0 = auto
    ) -> np.ndarray:
        if not texts:
            return np.empty((0, self._dimension), dtype=np.float32)

        if batch_size == 0:
            batch_size = _auto_batch_size(len(texts))

        vecs = self.model.encode(
            texts,
            batch_size=batch_size,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        vecs = np.asarray(vecs, dtype=np.float32)

        if vecs.ndim == 1:
            vecs = vecs.reshape(1, -1)

        return vecs

    # ── Utilities ─────────────────────────────────────────────────────────────

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def model_name(self) -> str:
        return self._model_name

    def is_loaded(self) -> bool:
        try:
            import streamlit as st
            return _ST_CACHE_KEY in st.session_state
        except Exception:
            return _module_model_cache is not None

    @staticmethod
    def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
        a, b = a.flatten(), b.flatten()
        na, nb = np.linalg.norm(a), np.linalg.norm(b)
        if na == 0 or nb == 0:
            return 0.0
        return float(np.dot(a, b) / (na * nb))


# ── Singleton ─────────────────────────────────────────────────────────────────
embedding_service = EmbeddingService()

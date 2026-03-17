"""
embedding_service.py - Local sentence-transformers embeddings.

Key improvements:
- Model cached in st.session_state when running under Streamlit,
  preventing expensive reloads on every Streamlit rerun
- Suppresses all HuggingFace/transformers noise before any import
- Batch size auto-tuned based on chunk count
"""

from __future__ import annotations

import os
import logging
from typing import Optional

import numpy as np

# Silence all HF noise before imports
os.environ.setdefault("TOKENIZERS_PARALLELISM",        "false")
os.environ.setdefault("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1")
os.environ.setdefault("TRANSFORMERS_VERBOSITY",        "error")
logging.getLogger("transformers").setLevel(logging.ERROR)
logging.getLogger("sentence_transformers").setLevel(logging.WARNING)
logging.getLogger("huggingface_hub").setLevel(logging.WARNING)

from sentence_transformers import SentenceTransformer  # noqa: E402

from app.config import EMBEDDING_MODEL, EMBEDDING_DIMENSION
from app.models.schemas import TextChunk
from app.utils.logger import get_logger, ServiceLogger

logger = get_logger(__name__)

# Streamlit session-state key for the cached model
_ST_CACHE_KEY = "_embedding_model_singleton"


def _get_or_load_model(model_name: str) -> SentenceTransformer:
    """
    Returns the SentenceTransformer model from Streamlit session state cache
    if available, otherwise loads it and caches it.

    Falls back to a plain module-level variable if not running under Streamlit.
    """
    # Try Streamlit session state first (avoids reload on every rerun)
    try:
        import streamlit as st
        if _ST_CACHE_KEY not in st.session_state:
            logger.info("Loading embedding model '%s' …", model_name)
            st.session_state[_ST_CACHE_KEY] = SentenceTransformer(model_name)
            logger.info("Embedding model loaded and cached in session state ✓")
        return st.session_state[_ST_CACHE_KEY]
    except Exception:
        pass

    # Fallback: module-level cache
    global _module_model_cache
    if _module_model_cache is None:
        logger.info("Loading embedding model '%s' …", model_name)
        _module_model_cache = SentenceTransformer(model_name)
        logger.info("Embedding model loaded ✓")
    return _module_model_cache


_module_model_cache: Optional[SentenceTransformer] = None


class EmbeddingService:
    """
    Wraps sentence-transformers for encoding chunks and queries.
    Model loads once and is reused across all requests.
    """

    def __init__(self) -> None:
        self._model_name = EMBEDDING_MODEL
        self._dimension  = EMBEDDING_DIMENSION
        logger.info("EmbeddingService ready (model will load on first use)")

    # ── Model property ────────────────────────────────────────────────────────

    @property
    def model(self) -> SentenceTransformer:
        m = _get_or_load_model(self._model_name)
        # Sync dimension on first load
        if self._dimension == EMBEDDING_DIMENSION:
            probe  = m.encode(["probe"], show_progress_bar=False)
            actual = int(probe.shape[1])
            if actual != self._dimension:
                logger.warning(
                    "Embedding dimension mismatch: config=%d actual=%d",
                    self._dimension, actual,
                )
                self._dimension = actual
        return m

    # ── Public API ────────────────────────────────────────────────────────────

    def embed_chunks(
        self,
        chunks       : list[TextChunk],
        doc_id       : str  = "",
        batch_size   : int  = 0,   # 0 = auto
        show_progress: bool = False,
    ) -> np.ndarray:
        slog = ServiceLogger("embedding_service", doc_id=doc_id)

        if not chunks:
            slog.warning("embed_chunks called with empty list")
            return np.empty((0, self._dimension), dtype=np.float32)

        # Auto batch size: smaller for tiny chunk sets to avoid overhead
        if batch_size == 0:
            batch_size = 32 if len(chunks) < 50 else 64

        slog.info("Embedding %d chunks (batch=%d) …", len(chunks), batch_size)
        texts = [c.content for c in chunks]
        vecs  = self.model.encode(
            texts,
            batch_size           = batch_size,
            show_progress_bar    = show_progress,
            convert_to_numpy     = True,
            normalize_embeddings = True,
        ).astype(np.float32)
        slog.info("Embeddings done ✓  shape=%s", str(vecs.shape))
        return vecs

    def embed_query(self, query: str) -> np.ndarray:
        if not query or not query.strip():
            raise ValueError("Query must not be empty.")
        return self.model.encode(
            [query.strip()],
            convert_to_numpy     = True,
            normalize_embeddings = True,
        ).astype(np.float32)

    def embed_texts(
        self,
        texts      : list[str],
        batch_size : int = 64,
    ) -> np.ndarray:
        if not texts:
            return np.empty((0, self._dimension), dtype=np.float32)
        return self.model.encode(
            texts,
            batch_size           = batch_size,
            convert_to_numpy     = True,
            normalize_embeddings = True,
        ).astype(np.float32)

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
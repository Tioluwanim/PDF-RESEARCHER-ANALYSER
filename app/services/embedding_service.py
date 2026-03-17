"""
embedding_service.py - Local sentence-transformers embeddings.
Lazy-loads the model on first use; suppresses all noisy HF/transformers logs.
"""

from __future__ import annotations

import os
import logging

import numpy as np
from typing import Optional

# ── Silence HuggingFace / transformers noise before any import ────────────────
os.environ.setdefault("TOKENIZERS_PARALLELISM",      "false")
os.environ.setdefault("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1")
os.environ.setdefault("TRANSFORMERS_VERBOSITY",      "error")
logging.getLogger("transformers").setLevel(logging.ERROR)
logging.getLogger("sentence_transformers").setLevel(logging.WARNING)
logging.getLogger("huggingface_hub").setLevel(logging.WARNING)

from sentence_transformers import SentenceTransformer  # noqa: E402

from app.config import EMBEDDING_MODEL, EMBEDDING_DIMENSION
from app.models.schemas import TextChunk
from app.utils.logger import get_logger, ServiceLogger

logger = get_logger(__name__)


class EmbeddingService:
    """
    Wraps sentence-transformers.  Lazy-loads the model on first access.
    Thread-safe for read (encode) calls; model is loaded exactly once.
    """

    def __init__(self) -> None:
        self._model    : Optional[SentenceTransformer] = None
        self._model_name = EMBEDDING_MODEL
        self._dimension  = EMBEDDING_DIMENSION
        logger.info("EmbeddingService ready (model will load on first use)")

    # ── Model access ──────────────────────────────────────────────────────────

    @property
    def model(self) -> SentenceTransformer:
        if self._model is None:
            logger.info("Loading embedding model '%s' …", self._model_name)
            self._model = SentenceTransformer(self._model_name)
            # Verify actual dimension matches config
            probe = self._model.encode(["probe"], show_progress_bar=False)
            actual = int(probe.shape[1])
            if actual != self._dimension:
                logger.warning(
                    "Embedding dimension mismatch: config=%d, actual=%d — "
                    "updating runtime value.",
                    self._dimension, actual,
                )
                self._dimension = actual
            logger.info(
                "Embedding model loaded ✓  dim=%d", self._dimension
            )
        return self._model

    # ── Public encode API ─────────────────────────────────────────────────────

    def embed_chunks(
        self,
        chunks       : list[TextChunk],
        doc_id       : str  = "",
        batch_size   : int  = 64,
        show_progress: bool = False,
    ) -> np.ndarray:
        """
        Encode a list of TextChunk objects.
        Returns float32 array of shape (N, dim), L2-normalised.
        """
        slog = ServiceLogger("embedding_service", doc_id=doc_id)
        if not chunks:
            slog.warning("embed_chunks called with empty list")
            return np.empty((0, self._dimension), dtype=np.float32)

        slog.info("Embedding %d chunks (batch=%d) …", len(chunks), batch_size)
        texts = [c.content for c in chunks]
        vecs  = self.model.encode(
            texts,
            batch_size           = batch_size,
            show_progress_bar    = show_progress,
            convert_to_numpy     = True,
            normalize_embeddings = True,
        ).astype(np.float32)
        slog.info("Embeddings done — shape=%s", vecs.shape)
        return vecs

    def embed_query(self, query: str) -> np.ndarray:
        """
        Encode a single query string.
        Returns float32 array of shape (1, dim), L2-normalised.
        """
        if not query or not query.strip():
            raise ValueError("Query text must not be empty.")
        vecs = self.model.encode(
            [query.strip()],
            convert_to_numpy     = True,
            normalize_embeddings = True,
        ).astype(np.float32)
        return vecs

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
        _ = self.model          # ensure loaded
        return self._dimension

    @property
    def model_name(self) -> str:
        return self._model_name

    def is_loaded(self) -> bool:
        return self._model is not None

    def unload(self) -> None:
        """Release the model from RAM (will reload on next access)."""
        if self._model is not None:
            del self._model
            self._model = None
            logger.info("Embedding model unloaded from memory")

    @staticmethod
    def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
        a, b = a.flatten(), b.flatten()
        na, nb = np.linalg.norm(a), np.linalg.norm(b)
        if na == 0 or nb == 0:
            return 0.0
        return float(np.dot(a, b) / (na * nb))


# ── Singleton ─────────────────────────────────────────────────────────────────
embedding_service = EmbeddingService()
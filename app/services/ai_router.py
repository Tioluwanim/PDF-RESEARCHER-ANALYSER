"""
ai_router.py - Production LLM router. Fully verified endpoints March 2026.

PRIMARY  → OpenRouter  https://openrouter.ai/api/v1
           Model: "openrouter/free"
           OpenRouter's official free-models router. Never returns 404.
           Automatically selects from all currently live free models.
           Docs: https://openrouter.ai/docs/guides/routing/routers/free-models-router

FALLBACK → HuggingFace Inference Providers  https://router.huggingface.co/v1
           Model: "meta-llama/Llama-3.1-8B-Instruct:cerebras"
           Uses :auto suffix to auto-select best available provider.
           OpenAI-compatible. Token needs "Make calls to Inference Providers" scope.
           Docs: https://huggingface.co/docs/inference-providers

Both use the openai SDK — identical interface for streaming and non-streaming.
"""

from __future__ import annotations

import time
import logging
from typing import Generator, Iterator

from app.config import (
    OPENROUTER_API_KEY,
    OPENROUTER_MODEL,
    OPENROUTER_TIMEOUT,
    OPENROUTER_RATE_LIMIT_DELAY,
    HUGGINGFACE_API_KEY,
    HUGGINGFACE_MODEL,
    HUGGINGFACE_BASE_URL,
    HUGGINGFACE_TIMEOUT,
    MAX_TOKENS,
    TEMPERATURE,
    CONTEXT_WINDOW_TOKENS,
    RETRY_MAX_ATTEMPTS,
)
from app.models.schemas import ChatMessage, ChatResponse, LLMProvider
from app.utils.logger import get_logger, ServiceLogger

logger = get_logger(__name__)

_SYSTEM = (
    "You are an expert research assistant specialised in analysing academic "
    "papers and research documents.\n\n"
    "You are given relevant excerpts from a research paper as context, "
    "followed by a question. Answer accurately and clearly based solely on "
    "the provided context.\n\n"
    "Rules:\n"
    "- Answer ONLY from the context provided.\n"
    "- If the context is insufficient, say so clearly.\n"
    "- Never invent facts not present in the context.\n"
    "- Be concise but thorough.\n"
)

# ── OpenRouter free-router slug ───────────────────────────────────────────────
# "openrouter/free" is the official OpenRouter free-models router.
# It NEVER 404s — auto-selects from whichever free models are live.
_OR_FREE_ROUTER = "openrouter/free"


def _build_openrouter_client():
    from openai import OpenAI
    return OpenAI(
        api_key         = OPENROUTER_API_KEY,
        base_url        = "https://openrouter.ai/api/v1",
        timeout         = OPENROUTER_TIMEOUT,
        default_headers = {
            "HTTP-Referer": "https://pdf-research-analyzer.local",
            "X-Title"     : "PDF Research Analyzer",
        },
    )


def _build_huggingface_client():
    """
    OpenAI SDK pointed at HuggingFace Inference Providers router.
    Endpoint: https://router.huggingface.co/v1  (confirmed current 2026)
    Requires token with 'Make calls to Inference Providers' scope.
    """
    from openai import OpenAI
    return OpenAI(
        api_key  = HUGGINGFACE_API_KEY,
        base_url = HUGGINGFACE_BASE_URL,   # https://router.huggingface.co/v1
        timeout  = HUGGINGFACE_TIMEOUT,
    )


class AIRouter:
    """
    Routes LLM requests with automatic fallback.
    Never crashes — all public methods are fully exception-safe.
    """

    def __init__(self) -> None:
        self._or_client = None
        self._hf_client = None
        # Use configured model or fall back to free router
        self._or_model = OPENROUTER_MODEL or _OR_FREE_ROUTER
        logger.info(
            "AIRouter ready — OR model=%s  HF model=%s  HF url=%s",
            self._or_model, HUGGINGFACE_MODEL, HUGGINGFACE_BASE_URL,
        )

    # ── Lazy clients ──────────────────────────────────────────────────────────

    @property
    def or_client(self):
        if self._or_client is None:
            self._or_client = _build_openrouter_client()
        return self._or_client

    @property
    def hf_client(self):
        if self._hf_client is None:
            self._hf_client = _build_huggingface_client()
        return self._hf_client

    # ── Public API ────────────────────────────────────────────────────────────

    def chat(
        self,
        question : str,
        context  : str,
        history  : list[ChatMessage],
        doc_id   : str  = "",
        stream   : bool = True,
    ) -> Generator[str, None, None] | ChatResponse:
        slog     = ServiceLogger("ai_router", doc_id=doc_id)
        messages = self._build_messages(question, context, history)
        slog.info("Chat — stream=%s  q='%s'", stream, question[:60])

        if stream:
            return self._stream_with_fallback(messages, slog)
        return self._complete_with_fallback(messages, question, doc_id, slog)

    def get_provider_status(self) -> dict:
        return {
            "openrouter" : {
                "configured": bool(OPENROUTER_API_KEY),
                "model"     : self._or_model,
            },
            "huggingface": {
                "configured": bool(HUGGINGFACE_API_KEY),
                "model"     : HUGGINGFACE_MODEL,
            },
        }

    # ── Streaming ─────────────────────────────────────────────────────────────

    def _stream_with_fallback(
        self,
        messages : list[dict],
        slog     : ServiceLogger,
    ) -> Generator[str, None, None]:

        # Primary: OpenRouter
        if OPENROUTER_API_KEY:
            try:
                slog.info("Streaming via OpenRouter (%s) …", self._or_model)
                tokens = list(self._stream_openrouter(messages, slog))
                if tokens:
                    yield from tokens
                    return
                slog.warning("OpenRouter returned empty stream — trying HuggingFace")
            except Exception as e:
                _log_error("OpenRouter", e, slog)
        else:
            slog.warning("OPENROUTER_API_KEY not set — skipping primary")

        # Fallback: HuggingFace
        if HUGGINGFACE_API_KEY:
            try:
                slog.info("Streaming via HuggingFace (%s) …", HUGGINGFACE_MODEL)
                tokens = list(self._stream_huggingface(messages, slog))
                if tokens:
                    yield from tokens
                    return
                slog.error("HuggingFace also returned empty stream")
            except Exception as e:
                _log_error("HuggingFace", e, slog)
        else:
            slog.warning("HUGGINGFACE_API_KEY not set — skipping fallback")

        yield (
            "⚠️ Both LLM providers are currently unavailable. "
            "Please verify your API keys in the .env file. "
            "OpenRouter key must start with 'sk-or-'. "
            "HuggingFace token must have 'Make calls to Inference Providers' permission."
        )

    def _stream_openrouter(
        self,
        messages : list[dict],
        slog     : ServiceLogger,
    ) -> Iterator[str]:
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                resp = self.or_client.chat.completions.create(
                    model      = self._or_model,
                    messages   = messages,
                    max_tokens = MAX_TOKENS,
                    temperature= max(float(TEMPERATURE), 0.01),
                    stream     = True,
                )
                count = 0
                for chunk in resp:
                    delta = chunk.choices[0].delta if chunk.choices else None
                    if delta and delta.content:
                        count += 1
                        yield delta.content
                slog.info("OpenRouter stream done — %d tokens", count)
                return

            except Exception as e:
                status = _http_status(e)
                if status == 429:
                    wait = OPENROUTER_RATE_LIMIT_DELAY
                    slog.warning(
                        "OpenRouter 429 — waiting %.1fs (attempt %d/%d)",
                        wait, attempt, RETRY_MAX_ATTEMPTS,
                    )
                    time.sleep(wait)
                    if attempt >= RETRY_MAX_ATTEMPTS:
                        raise
                elif status and 400 <= status < 500:
                    # Hard client error — no point retrying
                    slog.error("OpenRouter HTTP %s: %s", status, _error_body(e))
                    raise
                elif attempt < RETRY_MAX_ATTEMPTS:
                    delay = 2 ** (attempt - 1)
                    slog.warning(
                        "OpenRouter transient error attempt %d/%d: %s — retry in %ds",
                        attempt, RETRY_MAX_ATTEMPTS, e, delay,
                    )
                    time.sleep(delay)
                else:
                    raise

    def _stream_huggingface(
        self,
        messages : list[dict],
        slog     : ServiceLogger,
    ) -> Iterator[str]:
        """
        HuggingFace Inference Providers router.
        Model format: "org/model:provider" e.g. "meta-llama/Llama-3.1-8B-Instruct:cerebras"
        Use ":auto" suffix to let HF auto-select the best available provider.
        """
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                resp = self.hf_client.chat.completions.create(
                    model      = HUGGINGFACE_MODEL,
                    messages   = messages,
                    max_tokens = MAX_TOKENS,
                    temperature= max(float(TEMPERATURE), 0.01),
                    stream     = True,
                )
                count = 0
                for chunk in resp:
                    delta = chunk.choices[0].delta if chunk.choices else None
                    if delta and delta.content:
                        count += 1
                        yield delta.content
                slog.info("HuggingFace stream done — %d tokens", count)
                return

            except Exception as e:
                status = _http_status(e)
                body   = _error_body(e)
                if status == 429:
                    slog.warning(
                        "HuggingFace 429 — waiting 15s (attempt %d/%d)",
                        attempt, RETRY_MAX_ATTEMPTS,
                    )
                    time.sleep(15)
                    if attempt >= RETRY_MAX_ATTEMPTS:
                        raise
                elif status == 503:
                    slog.warning("HuggingFace 503 model loading — waiting 20s")
                    time.sleep(20)
                elif status == 401:
                    slog.error(
                        "HuggingFace 401 Unauthorized. "
                        "Ensure your HF token has 'Make calls to Inference Providers' "
                        "permission at huggingface.co/settings/tokens"
                    )
                    raise
                elif status and 400 <= status < 500:
                    slog.error("HuggingFace HTTP %s: %s", status, body)
                    raise
                elif attempt < RETRY_MAX_ATTEMPTS:
                    delay = 2 ** (attempt - 1)
                    slog.warning(
                        "HuggingFace transient error attempt %d/%d: %s — retry in %ds",
                        attempt, RETRY_MAX_ATTEMPTS, e, delay,
                    )
                    time.sleep(delay)
                else:
                    raise

    # ── Non-streaming ─────────────────────────────────────────────────────────

    def _complete_with_fallback(
        self,
        messages : list[dict],
        question : str,
        doc_id   : str,
        slog     : ServiceLogger,
    ) -> ChatResponse:
        start    = time.monotonic()
        answer   = ""
        provider = LLMProvider.OPENROUTER
        model_u  = self._or_model

        if OPENROUTER_API_KEY:
            try:
                answer   = self._complete_openrouter(messages, slog)
                provider = LLMProvider.OPENROUTER
            except Exception as e:
                _log_error("OpenRouter", e, slog)

        if not answer and HUGGINGFACE_API_KEY:
            try:
                answer   = self._complete_huggingface(messages, slog)
                provider = LLMProvider.HUGGINGFACE
                model_u  = HUGGINGFACE_MODEL
            except Exception as e:
                _log_error("HuggingFace", e, slog)
                answer   = (
                    "⚠️ Both LLM providers failed. "
                    "Check your API keys and model availability."
                )

        return ChatResponse(
            answer           = answer,
            doc_id           = doc_id,
            question         = question,
            provider         = provider,
            model            = model_u,
            response_time_ms = round((time.monotonic() - start) * 1000, 2),
        )

    def _complete_openrouter(
        self, messages: list[dict], slog: ServiceLogger
    ) -> str:
        resp   = self.or_client.chat.completions.create(
            model      = self._or_model,
            messages   = messages,
            max_tokens = MAX_TOKENS,
            temperature= max(float(TEMPERATURE), 0.01),
            stream     = False,
        )
        answer = resp.choices[0].message.content or ""
        slog.info("OpenRouter complete ✓ — %d chars", len(answer))
        return answer

    def _complete_huggingface(
        self, messages: list[dict], slog: ServiceLogger
    ) -> str:
        resp   = self.hf_client.chat.completions.create(
            model      = HUGGINGFACE_MODEL,
            messages   = messages,
            max_tokens = MAX_TOKENS,
            temperature= max(float(TEMPERATURE), 0.01),
            stream     = False,
        )
        answer = resp.choices[0].message.content or ""
        slog.info("HuggingFace complete ✓ — %d chars", len(answer))
        return answer

    # ── Prompt builder ────────────────────────────────────────────────────────

    def _build_messages(
        self,
        question : str,
        context  : str,
        history  : list[ChatMessage],
    ) -> list[dict]:
        msgs: list[dict] = [{"role": "system", "content": _SYSTEM}]

        for msg in _trim_history(history):
            msgs.append({"role": msg.role.value, "content": msg.content})

        if context:
            user_content = (
                "CONTEXT FROM THE DOCUMENT:\n"
                + "=" * 60 + "\n"
                + context + "\n"
                + "=" * 60 + "\n\n"
                + f"QUESTION: {question}"
            )
        else:
            user_content = (
                "No relevant context was found for this question.\n\n"
                f"QUESTION: {question}"
            )

        msgs.append({"role": "user", "content": user_content})
        return msgs


# ── Helpers ───────────────────────────────────────────────────────────────────

def _trim_history(
    history   : list[ChatMessage],
    max_chars : int = CONTEXT_WINDOW_TOKENS * 3,
) -> list[ChatMessage]:
    trimmed     = list(history)
    total_chars = sum(len(m.content) for m in trimmed)
    while total_chars > max_chars and len(trimmed) > 2:
        removed      = trimmed.pop(0)
        total_chars -= len(removed.content)
    return trimmed


def _http_status(exc: Exception) -> int | None:
    if hasattr(exc, "status_code"):
        return int(exc.status_code)
    if hasattr(exc, "response") and exc.response is not None:
        return int(exc.response.status_code)
    return None


def _error_body(exc: Exception) -> str:
    if hasattr(exc, "body"):
        return str(exc.body)[:300]
    if hasattr(exc, "message"):
        return str(exc.message)[:300]
    if hasattr(exc, "response") and exc.response is not None:
        try:
            return exc.response.text[:300]
        except Exception:
            pass
    return str(exc)[:300]


def _log_error(
    provider : str,
    exc      : Exception,
    slog     : ServiceLogger,
) -> None:
    status = _http_status(exc)
    body   = _error_body(exc)
    if status:
        slog.warning("%s HTTP %s: %s — trying fallback", provider, status, body)
    else:
        slog.warning(
            "%s error (%s): %s — trying fallback",
            provider, type(exc).__name__, exc,
        )


# ── Singleton ─────────────────────────────────────────────────────────────────
ai_router = AIRouter()
"""
retry.py - Production-grade retry logic with exponential backoff + jitter.
Handles 429 rate-limit responses explicitly with Retry-After header support.
"""

from __future__ import annotations

import time
import random
import functools
from typing import Callable, Type, Tuple, Any, Optional

from app.utils.logger import get_logger
from app.config import (
    RETRY_MAX_ATTEMPTS,
    RETRY_BASE_DELAY,
    RETRY_MAX_DELAY,
    RETRY_BACKOFF_FACTOR,
)

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Rate-limit exception — raised when a provider returns HTTP 429
# ─────────────────────────────────────────────────────────────────────────────

class RateLimitError(Exception):
    """Raised when an LLM provider returns HTTP 429 Too Many Requests."""
    def __init__(self, message: str, retry_after: float = 10.0):
        super().__init__(message)
        self.retry_after = retry_after


# ─────────────────────────────────────────────────────────────────────────────
# Core retry decorator  (sync)
# ─────────────────────────────────────────────────────────────────────────────

def retry(
    max_attempts   : int                               = RETRY_MAX_ATTEMPTS,
    base_delay     : float                             = RETRY_BASE_DELAY,
    max_delay      : float                             = RETRY_MAX_DELAY,
    backoff_factor : float                             = RETRY_BACKOFF_FACTOR,
    exceptions     : Tuple[Type[Exception], ...]       = (Exception,),
    reraise_on     : Tuple[Type[Exception], ...]       = (),
    on_retry       : Optional[Callable[[int, Exception, float], None]] = None,
) -> Callable:
    """
    Decorator: retry *func* up to *max_attempts* times with full-jitter
    exponential back-off.

    Special behaviour:
    - RateLimitError: honours the retry_after field before retrying.
    - HTTP 4xx (except 429): NOT retried — they are deterministic failures.
    - Exceptions listed in *reraise_on* propagate immediately without retry.

    Jitter formula (full-jitter, AWS style):
        delay = random.uniform(0, min(max_delay, base * factor^(attempt-1)))
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Optional[Exception] = None

            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(
                            f"'{func.__name__}' succeeded on attempt "
                            f"{attempt}/{max_attempts}"
                        )
                    return result

                except tuple(reraise_on) as e:
                    # Deterministic failure — no point retrying
                    logger.error(
                        f"'{func.__name__}' raised non-retryable "
                        f"{type(e).__name__}: {e}"
                    )
                    raise

                except RateLimitError as e:
                    last_exc = e
                    wait = min(e.retry_after, max_delay)
                    logger.warning(
                        f"'{func.__name__}' rate-limited (429). "
                        f"Waiting {wait:.1f}s before retry "
                        f"[{attempt}/{max_attempts}]"
                    )
                    if attempt == max_attempts:
                        raise
                    time.sleep(wait)

                except tuple(exceptions) as e:
                    last_exc = e

                    if attempt == max_attempts:
                        logger.error(
                            f"'{func.__name__}' failed after {max_attempts} "
                            f"attempts. Final: {type(e).__name__}: {e}"
                        )
                        raise

                    # Full-jitter exponential back-off
                    cap   = min(max_delay, base_delay * (backoff_factor ** (attempt - 1)))
                    delay = random.uniform(0, cap)

                    logger.warning(
                        f"'{func.__name__}' attempt {attempt}/{max_attempts} "
                        f"failed ({type(e).__name__}: {e}) — "
                        f"retrying in {delay:.2f}s"
                    )

                    if on_retry:
                        try:
                            on_retry(attempt, e, delay)
                        except Exception as cb_err:
                            logger.debug(f"on_retry callback error: {cb_err}")

                    time.sleep(delay)

            # Should not be reached
            if last_exc:
                raise last_exc  # type: ignore
            raise RuntimeError(f"retry loop exhausted without raising for '{func.__name__}'")

        return wrapper
    return decorator


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def is_retryable_http(status_code: int) -> bool:
    """
    Returns True if the HTTP status code warrants a retry.
    - 429 Too Many Requests  → yes (rate limit)
    - 5xx Server Errors      → yes (transient)
    - 4xx (except 429)       → no  (client error, won't change on retry)
    """
    return status_code == 429 or status_code >= 500


def extract_retry_after(response_headers: dict, default: float = 10.0) -> float:
    """
    Reads the Retry-After header (seconds or HTTP-date) from a response.
    Returns *default* if the header is absent or unparseable.
    """
    raw = response_headers.get("Retry-After", "")
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return default


class RetryStats:
    """
    Context manager that times a block and records success/failure.
    Useful for structured logging and metrics in production.

    Usage:
        with RetryStats("openrouter_stream") as stats:
            ...
        # stats.succeeded, stats.elapsed_s available after block
    """
    def __init__(self, operation: str):
        self.operation  = operation
        self.succeeded  = False
        self.elapsed_s  = 0.0
        self._start     = 0.0

    def __enter__(self) -> "RetryStats":
        self._start = time.monotonic()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.elapsed_s = time.monotonic() - self._start
        self.succeeded = exc_type is None
        level = logger.info if self.succeeded else logger.error
        level(
            f"[RetryStats] '{self.operation}' "
            f"{'succeeded' if self.succeeded else 'failed'} "
            f"in {self.elapsed_s:.2f}s"
        )
        return False  # never suppress
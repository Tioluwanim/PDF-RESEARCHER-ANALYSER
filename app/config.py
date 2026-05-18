"""
config.py - Central configuration for PDF Research Analyzer.
All settings loaded from environment variables with validated defaults.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

from dotenv import load_dotenv

# =============================================================================
# PATHS
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
PROCESSED_DIR = DATA_DIR / "processed"
VECTORSTORE_DIR = DATA_DIR / "vectorstore"

LOGS_DIR = BASE_DIR / "logs"

# =============================================================================
# HELPERS
# =============================================================================


def _env_str(name: str, default: str = "") -> str:
    value = os.getenv(name, default)

    if isinstance(value, str):
        return value.strip()

    return default


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)

    if value is None or not value.strip():
        return default

    try:
        return int(value)

    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)

    if value is None or not value.strip():
        return default

    try:
        return float(value)

    except (TypeError, ValueError):
        return default


def _resolved_path(path_value: str) -> Path:
    path = Path(path_value)

    if path.is_absolute():
        return path

    return BASE_DIR / path


# =============================================================================
# STREAMLIT SECRETS SUPPORT
# =============================================================================


def _load_streamlit_secrets_into_env() -> None:
    """
    Safely load Streamlit secrets into environment variables.
    """

    try:
        import streamlit as st
    except Exception:
        return

    try:
        secrets = getattr(st, "secrets", None)

        if secrets is None:
            return

        for key in secrets.keys():
            value = secrets[key]

            if isinstance(value, str):
                os.environ.setdefault(key, value)

    except Exception:
        return


def _get_streamlit_secret_section(
    name: str,
) -> dict[str, Any] | None:
    """
    Safely get nested Streamlit secrets section.
    """

    try:
        import streamlit as st
    except Exception:
        return None

    secrets = getattr(st, "secrets", None)

    if secrets is None:
        return None

    try:
        if hasattr(secrets, "get"):
            section = secrets.get(name)
        else:
            section = secrets[name]

        if isinstance(section, Mapping):
            return dict(section)

    except (KeyError, Exception):
        return None

    return None


def _write_json_file(
    path: Path,
    data: Mapping[str, Any],
) -> bool:
    """
    Write JSON credentials safely.
    """

    try:
        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with path.open(
            "w",
            encoding="utf-8",
        ) as file:

            json.dump(
                dict(data),
                file,
                indent=2,
            )

        return True

    except Exception:
        return False


# =============================================================================
# LOAD ENVIRONMENT
# =============================================================================

_env_path = BASE_DIR / ".env"

load_dotenv(
    dotenv_path=_env_path,
    override=False,
)

_load_streamlit_secrets_into_env()

# =============================================================================
# CREATE REQUIRED DIRECTORIES
# =============================================================================

for directory in (
    DATA_DIR,
    UPLOAD_DIR,
    PROCESSED_DIR,
    VECTORSTORE_DIR,
    LOGS_DIR,
):
    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

# =============================================================================
# APP SETTINGS
# =============================================================================

APP_TITLE = _env_str(
    "APP_TITLE",
    "PDF Research Analyzer",
)

APP_VERSION = _env_str(
    "APP_VERSION",
    "1.0.0",
)

DEBUG = _env_bool(
    "DEBUG",
    False,
)

LOG_LEVEL = _env_str(
    "LOG_LEVEL",
    "INFO",
).upper()

# =============================================================================
# STREAMLIT SETTINGS
# =============================================================================

STREAMLIT_PAGE_TITLE = _env_str(
    "STREAMLIT_PAGE_TITLE",
    APP_TITLE,
)

STREAMLIT_PAGE_ICON = _env_str(
    "STREAMLIT_PAGE_ICON",
    "📚",
)

STREAMLIT_LAYOUT = _env_str(
    "STREAMLIT_LAYOUT",
    "wide",
)

STREAMLIT_SIDEBAR_STATE = _env_str(
    "STREAMLIT_SIDEBAR_STATE",
    "expanded",
)

MAX_CHAT_HISTORY = _env_int(
    "MAX_CHAT_HISTORY",
    20,
)

# =============================================================================
# DATABASE
# =============================================================================

DATABASE_URL = _env_str(
    "DATABASE_URL",
    f"sqlite:///{BASE_DIR / 'data' / 'pdf_analyzer.db'}",
)

SQLALCHEMY_ECHO = _env_bool(
    "SQLALCHEMY_ECHO",
    False,
)

# =============================================================================
# GOOGLE DRIVE
# =============================================================================

GOOGLE_DRIVE_FOLDER_ID = _env_str(
    "GOOGLE_DRIVE_FOLDER_ID"
)

GOOGLE_CREDENTIALS_PATH = _env_str(
    "GOOGLE_CREDENTIALS_PATH",
    "credentials.json",
)

GOOGLE_CREDENTIALS_SECRET_SECTION = _env_str(
    "GOOGLE_CREDENTIALS_SECRET_SECTION",
    "gcp_service_account",
)

GOOGLE_CREDENTIALS_JSON = _env_str(
    "GOOGLE_CREDENTIALS_JSON"
)


def _materialize_google_credentials_file() -> None:
    """
    Create Google credentials file from:
    1. Existing credentials file
    2. GOOGLE_CREDENTIALS_JSON env var
    3. Streamlit secrets section
    """

    cred_path = _resolved_path(
        GOOGLE_CREDENTIALS_PATH
    )

    # Already exists
    if cred_path.exists():
        return

    # Try JSON env variable
    raw_json = GOOGLE_CREDENTIALS_JSON

    if raw_json:
        try:
            parsed = json.loads(raw_json)

            if isinstance(parsed, dict):

                _write_json_file(
                    cred_path,
                    parsed,
                )

                return

        except Exception:
            pass

    # Try Streamlit secrets
    secret_section = _get_streamlit_secret_section(
        GOOGLE_CREDENTIALS_SECRET_SECTION
    )

    if secret_section:

        _write_json_file(
            cred_path,
            secret_section,
        )


_materialize_google_credentials_file()

# =============================================================================
# OPENROUTER
# =============================================================================

OPENROUTER_API_KEY = _env_str(
    "OPENROUTER_API_KEY"
)

OPENROUTER_BASE_URL = _env_str(
    "OPENROUTER_BASE_URL",
    "https://openrouter.ai/api/v1",
)

OPENROUTER_MODEL = _env_str(
    "OPENROUTER_MODEL",
    "openrouter/free",
)

OPENROUTER_TIMEOUT = _env_int(
    "OPENROUTER_TIMEOUT",
    90,
)

OPENROUTER_RATE_LIMIT_DELAY = _env_float(
    "OPENROUTER_RATE_LIMIT_DELAY",
    10.0,
)

# =============================================================================
# HUGGINGFACE
# =============================================================================

HUGGINGFACE_API_KEY = _env_str(
    "HUGGINGFACE_API_KEY"
)

HUGGINGFACE_BASE_URL = _env_str(
    "HUGGINGFACE_BASE_URL",
    "https://router.huggingface.co/v1",
)

HUGGINGFACE_MODEL = _env_str(
    "HUGGINGFACE_MODEL",
    "meta-llama/Llama-3.1-8B-Instruct:cerebras",
)

HUGGINGFACE_TIMEOUT = _env_int(
    "HUGGINGFACE_TIMEOUT",
    90,
)

# =============================================================================
# EMBEDDINGS
# =============================================================================

EMBEDDING_MODEL = _env_str(
    "EMBEDDING_MODEL",
    "sentence-transformers/all-MiniLM-L6-v2",
)

EMBEDDING_DIMENSION = _env_int(
    "EMBEDDING_DIMENSION",
    384,
)

# =============================================================================
# CHUNKING
# =============================================================================

CHUNK_SIZE = _env_int(
    "CHUNK_SIZE",
    500,
)

CHUNK_OVERLAP = _env_int(
    "CHUNK_OVERLAP",
    50,
)

MIN_CHUNK_LENGTH = _env_int(
    "MIN_CHUNK_LENGTH",
    50,
)

# =============================================================================
# RETRIEVAL
# =============================================================================

TOP_K_RESULTS = _env_int(
    "TOP_K_RESULTS",
    8,
)

SIMILARITY_THRESHOLD = _env_float(
    "SIMILARITY_THRESHOLD",
    0.05,
)

# =============================================================================
# GENERATION
# =============================================================================

MAX_TOKENS = _env_int(
    "MAX_TOKENS",
    2048,
)

TEMPERATURE = _env_float(
    "TEMPERATURE",
    0.3,
)

CONTEXT_WINDOW_TOKENS = _env_int(
    "CONTEXT_WINDOW_TOKENS",
    6000,
)

# =============================================================================
# RETRY
# =============================================================================

RETRY_MAX_ATTEMPTS = _env_int(
    "RETRY_MAX_ATTEMPTS",
    3,
)

RETRY_BASE_DELAY = _env_float(
    "RETRY_BASE_DELAY",
    1.0,
)

RETRY_MAX_DELAY = _env_float(
    "RETRY_MAX_DELAY",
    60.0,
)

RETRY_BACKOFF_FACTOR = _env_float(
    "RETRY_BACKOFF_FACTOR",
    2.0,
)

# =============================================================================
# UPLOAD SETTINGS
# =============================================================================

MAX_FILE_SIZE_MB = _env_int(
    "MAX_FILE_SIZE_MB",
    50,
)

MAX_FILE_SIZE_BYTES = (
    MAX_FILE_SIZE_MB * 1024 * 1024
)

ALLOWED_EXTENSIONS = {
    ".pdf",
    ".docx",
    ".doc",
    ".txt",
    ".xlsx",
    ".xls",
    ".csv",
}

# =============================================================================
# VECTORSTORE SETTINGS
# =============================================================================

VECTORSTORE_COLLECTION_NAME = _env_str(
    "VECTORSTORE_COLLECTION_NAME",
    "pdf_research_chunks",
)

# =============================================================================
# CACHE SETTINGS
# =============================================================================

ENABLE_CACHE = _env_bool(
    "ENABLE_CACHE",
    True,
)

CACHE_TTL_SECONDS = _env_int(
    "CACHE_TTL_SECONDS",
    3600,
)

# =============================================================================
# UI SETTINGS
# =============================================================================

DEFAULT_THEME = _env_str(
    "DEFAULT_THEME",
    "light",
)

SHOW_DEBUG_INFO = _env_bool(
    "SHOW_DEBUG_INFO",
    False,
)

# =============================================================================
# VALIDATION
# =============================================================================


def validate_config() -> list[str]:
    """
    Validate configuration values.
    """

    issues: list[str] = []

    if not OPENROUTER_API_KEY:
        issues.append(
            "OPENROUTER_API_KEY missing"
        )

    if CHUNK_OVERLAP >= CHUNK_SIZE:
        issues.append(
            f"CHUNK_OVERLAP ({CHUNK_OVERLAP}) "
            f"must be less than CHUNK_SIZE "
            f"({CHUNK_SIZE})"
        )

    if MAX_FILE_SIZE_MB <= 0:
        issues.append(
            "MAX_FILE_SIZE_MB must be greater than 0"
        )

    if TOP_K_RESULTS <= 0:
        issues.append(
            "TOP_K_RESULTS must be greater than 0"
        )

    return issues


def get_config_summary() -> dict[str, str | int | bool]:
    """
    Return a sanitized app configuration summary for startup logging.
    """

    return {
        "app_title": APP_TITLE,
        "version": APP_VERSION,
        "debug": DEBUG,
        "log_level": LOG_LEVEL,
        "streamlit_layout": STREAMLIT_LAYOUT,
        "streamlit_sidebar_state": STREAMLIT_SIDEBAR_STATE,
        "max_chat_history": MAX_CHAT_HISTORY,
        "database_url": DATABASE_URL,
        "openrouter_model": OPENROUTER_MODEL,
        "huggingface_model": HUGGINGFACE_MODEL,
        "embedding_model": EMBEDDING_MODEL,
        "chunk_size": CHUNK_SIZE,
        "chunk_overlap": CHUNK_OVERLAP,
        "top_k_results": TOP_K_RESULTS,
        "max_tokens": MAX_TOKENS,
        "temperature": TEMPERATURE,
        "max_file_size_mb": MAX_FILE_SIZE_MB,
    }


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Paths
    "BASE_DIR",
    "DATA_DIR",
    "UPLOAD_DIR",
    "PROCESSED_DIR",
    "VECTORSTORE_DIR",
    "LOGS_DIR",

    # App
    "APP_TITLE",
    "APP_VERSION",
    "DEBUG",
    "LOG_LEVEL",

    # Streamlit
    "STREAMLIT_PAGE_TITLE",
    "STREAMLIT_PAGE_ICON",
    "STREAMLIT_LAYOUT",
    "STREAMLIT_SIDEBAR_STATE",
    "MAX_CHAT_HISTORY",

    # Database
    "DATABASE_URL",
    "SQLALCHEMY_ECHO",

    # Google
    "GOOGLE_DRIVE_FOLDER_ID",
    "GOOGLE_CREDENTIALS_PATH",

    # OpenRouter
    "OPENROUTER_API_KEY",
    "OPENROUTER_BASE_URL",
    "OPENROUTER_MODEL",
    "OPENROUTER_TIMEOUT",
    "OPENROUTER_RATE_LIMIT_DELAY",

    # HuggingFace
    "HUGGINGFACE_API_KEY",
    "HUGGINGFACE_BASE_URL",
    "HUGGINGFACE_MODEL",
    "HUGGINGFACE_TIMEOUT",

    # Embeddings
    "EMBEDDING_MODEL",
    "EMBEDDING_DIMENSION",

    # Chunking
    "CHUNK_SIZE",
    "CHUNK_OVERLAP",
    "MIN_CHUNK_LENGTH",

    # Retrieval
    "TOP_K_RESULTS",
    "SIMILARITY_THRESHOLD",

    # Generation
    "MAX_TOKENS",
    "TEMPERATURE",
    "CONTEXT_WINDOW_TOKENS",

    # Retry
    "RETRY_MAX_ATTEMPTS",
    "RETRY_BASE_DELAY",
    "RETRY_MAX_DELAY",
    "RETRY_BACKOFF_FACTOR",

    # Upload
    "MAX_FILE_SIZE_MB",
    "MAX_FILE_SIZE_BYTES",
    "ALLOWED_EXTENSIONS",

    # Vectorstore
    "VECTORSTORE_COLLECTION_NAME",

    # Cache
    "ENABLE_CACHE",
    "CACHE_TTL_SECONDS",

    # UI
    "DEFAULT_THEME",
    "SHOW_DEBUG_INFO",

    # Validation
    "validate_config",
    "get_config_summary",
]
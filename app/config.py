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

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
PROCESSED_DIR = DATA_DIR / "processed"
VECTORSTORE_DIR = DATA_DIR / "vectorstore"
LOGS_DIR = BASE_DIR / "logs"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _env_str(name: str, default: str = "") -> str:
    value = os.getenv(name, default)

    if value is None:
        return default

    return str(value).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return str(value).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)

    if value is None:
        return default

    value = str(value).strip()

    if not value:
        return default

    try:
        return int(value)

    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)

    if value is None:
        return default

    value = str(value).strip()

    if not value:
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


# ─────────────────────────────────────────────────────────────────────────────
# Streamlit secrets support
# ─────────────────────────────────────────────────────────────────────────────
def _load_streamlit_secrets_into_env() -> None:
    """
    Safely load Streamlit secrets into environment variables.

    Compatible with:
    - Streamlit Cloud
    - Local Streamlit
    - CLI scripts
    - Alembic
    - Unit tests
    """

    try:
        import streamlit as st
    except Exception:
        return

    try:
        for key in st.secrets:
            value = st.secrets[key]

            if isinstance(value, str):
                os.environ.setdefault(key, value)

    except Exception:
        # Never crash config loading because of Streamlit
        return


def _get_streamlit_secret_section(
    name: str,
) -> dict[str, Any] | None:
    """
    Example:

    [gcp_service_account]
    type="service_account"
    project_id="..."
    """

    try:
        import streamlit as st
    except Exception:
        return None

    try:
        section = st.secrets.get(name)

        if isinstance(section, Mapping):
            return dict(section)

    except Exception:
        return None

    return None


def _write_json_file(
    path: Path,
    data: Mapping[str, Any],
) -> bool:
    try:
        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with path.open(
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(
                dict(data),
                f,
                indent=2,
            )

        return True

    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Load environment variables
# ─────────────────────────────────────────────────────────────────────────────
ENV_PATH = BASE_DIR / ".env"

load_dotenv(
    dotenv_path=ENV_PATH,
    override=False,
)

_load_streamlit_secrets_into_env()


# ─────────────────────────────────────────────────────────────────────────────
# Ensure required directories exist
# ─────────────────────────────────────────────────────────────────────────────
for directory in [
    DATA_DIR,
    UPLOAD_DIR,
    PROCESSED_DIR,
    VECTORSTORE_DIR,
    LOGS_DIR,
]:
    directory.mkdir(
        parents=True,
        exist_ok=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# App Config
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────────────────
DATABASE_URL = _env_str(
    "DATABASE_URL",
    f"sqlite:///{DATA_DIR / 'pdf_analyzer.db'}",
)

SQLALCHEMY_ECHO = _env_bool(
    "SQLALCHEMY_ECHO",
    False,
)


# ─────────────────────────────────────────────────────────────────────────────
# Google Drive
# ─────────────────────────────────────────────────────────────────────────────
GOOGLE_DRIVE_FOLDER_ID = _env_str(
    "GOOGLE_DRIVE_FOLDER_ID",
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
    "GOOGLE_CREDENTIALS_JSON",
)


def _materialize_google_credentials_file() -> None:
    """
    Creates credentials.json automatically from:
    1. GOOGLE_CREDENTIALS_JSON
    2. Streamlit secrets section
    """

    try:
        cred_path = _resolved_path(
            GOOGLE_CREDENTIALS_PATH
        )

        # If file already exists, do nothing
        if cred_path.exists():
            return

        # Option 1: JSON string from env
        if GOOGLE_CREDENTIALS_JSON:
            try:
                parsed = json.loads(
                    GOOGLE_CREDENTIALS_JSON
                )

                if isinstance(parsed, dict):
                    _write_json_file(
                        cred_path,
                        parsed,
                    )
                    return

            except json.JSONDecodeError:
                pass

        # Option 2: Streamlit secrets
        secret_section = _get_streamlit_secret_section(
            GOOGLE_CREDENTIALS_SECRET_SECTION
        )

        if secret_section:
            _write_json_file(
                cred_path,
                secret_section,
            )

    except Exception:
        # Never crash app startup because of credentials
        pass


_materialize_google_credentials_file()


# ─────────────────────────────────────────────────────────────────────────────
# OpenRouter
# ─────────────────────────────────────────────────────────────────────────────
OPENROUTER_API_KEY = _env_str(
    "OPENROUTER_API_KEY",
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


# ─────────────────────────────────────────────────────────────────────────────
# HuggingFace
# ─────────────────────────────────────────────────────────────────────────────
HUGGINGFACE_API_KEY = _env_str(
    "HUGGINGFACE_API_KEY",
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


# ─────────────────────────────────────────────────────────────────────────────
# Embeddings
# ─────────────────────────────────────────────────────────────────────────────
EMBEDDING_MODEL = _env_str(
    "EMBEDDING_MODEL",
    "sentence-transformers/all-MiniLM-L6-v2",
)

EMBEDDING_DIMENSION = _env_int(
    "EMBEDDING_DIMENSION",
    384,
)


# ─────────────────────────────────────────────────────────────────────────────
# Chunking
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# Retrieval
# ─────────────────────────────────────────────────────────────────────────────
TOP_K_RESULTS = _env_int(
    "TOP_K_RESULTS",
    8,
)

SIMILARITY_THRESHOLD = _env_float(
    "SIMILARITY_THRESHOLD",
    0.05,
)


# ─────────────────────────────────────────────────────────────────────────────
# Generation
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# Retry Settings
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# Upload Settings
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────
def validate_config() -> list[str]:
    issues: list[str] = []

    # Optional checks
    if not OPENROUTER_API_KEY:
        issues.append(
            "OPENROUTER_API_KEY is missing"
        )

    if not HUGGINGFACE_API_KEY:
        issues.append(
            "HUGGINGFACE_API_KEY is missing"
        )

    if CHUNK_OVERLAP >= CHUNK_SIZE:
        issues.append(
            f"CHUNK_OVERLAP ({CHUNK_OVERLAP}) "
            f"must be less than CHUNK_SIZE "
            f"({CHUNK_SIZE})"
        )

    return issues


# ─────────────────────────────────────────────────────────────────────────────
# Exported names
# ─────────────────────────────────────────────────────────────────────────────
__all__ = [
    "APP_TITLE",
    "APP_VERSION",
    "DEBUG",
    "LOG_LEVEL",
    "DATABASE_URL",
    "SQLALCHEMY_ECHO",
    "GOOGLE_DRIVE_FOLDER_ID",
    "GOOGLE_CREDENTIALS_PATH",
    "OPENROUTER_API_KEY",
    "OPENROUTER_BASE_URL",
    "OPENROUTER_MODEL",
    "OPENROUTER_TIMEOUT",
    "OPENROUTER_RATE_LIMIT_DELAY",
    "HUGGINGFACE_API_KEY",
    "HUGGINGFACE_BASE_URL",
    "HUGGINGFACE_MODEL",
    "HUGGINGFACE_TIMEOUT",
    "EMBEDDING_MODEL",
    "EMBEDDING_DIMENSION",
    "CHUNK_SIZE",
    "CHUNK_OVERLAP",
    "MIN_CHUNK_LENGTH",
    "TOP_K_RESULTS",
    "SIMILARITY_THRESHOLD",
    "MAX_TOKENS",
    "TEMPERATURE",
    "CONTEXT_WINDOW_TOKENS",
    "RETRY_MAX_ATTEMPTS",
    "RETRY_BASE_DELAY",
    "RETRY_MAX_DELAY",
    "RETRY_BACKOFF_FACTOR",
    "MAX_FILE_SIZE_MB",
    "MAX_FILE_SIZE_BYTES",
    "ALLOWED_EXTENSIONS",
    "BASE_DIR",
    "DATA_DIR",
    "UPLOAD_DIR",
    "PROCESSED_DIR",
    "VECTORSTORE_DIR",
    "LOGS_DIR",
    "validate_config",
]
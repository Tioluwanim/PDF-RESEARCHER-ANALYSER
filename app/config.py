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

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
PROCESSED_DIR = DATA_DIR / "processed"
VECTORSTORE_DIR = DATA_DIR / "vectorstore"
LOGS_DIR = BASE_DIR / "logs"

# ── Helpers ──────────────────────────────────────────────────────────────────
def _env_str(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip() if isinstance(value, str) else default


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


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
    return path if path.is_absolute() else BASE_DIR / path


def _load_streamlit_secrets_into_env() -> None:
    """
    Load flat Streamlit secrets into environment variables.
    Nested sections are ignored here and handled separately.
    """
    try:
        import streamlit as st  # type: ignore
    except Exception:
        return

    secrets_obj: Any = getattr(st, "secrets", None)
    if not secrets_obj:
        return

    try:
        for key in secrets_obj.keys():
            value = secrets_obj[key]
            if isinstance(value, str) and key not in os.environ:
                os.environ[key] = value
    except Exception:
        return


def _get_streamlit_secret_section(name: str) -> dict[str, Any] | None:
    """
    Return a nested Streamlit secrets section as a plain dict.
    Example: [gcp_service_account] in secrets.toml
    """
    try:
        import streamlit as st  # type: ignore
    except Exception:
        return None

    secrets_obj: Any = getattr(st, "secrets", None)
    if not secrets_obj:
        return None

    try:
        section = secrets_obj[name]
    except Exception:
        return None

    if isinstance(section, Mapping):
        return dict(section)

    return None


def _write_json_file(path: Path, data: Mapping[str, Any]) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(dict(data), f, indent=2)
        return True
    except Exception:
        return False


def _materialize_google_credentials_file() -> None:
    """
    Recreate credentials.json from Streamlit secrets if it does not exist.

    Supported sources:
    1. GOOGLE_CREDENTIALS_JSON env var containing the full JSON string
    2. A Streamlit secrets section, default name: [gcp_service_account]
    """
    cred_path = _resolved_path(GOOGLE_CREDENTIALS_PATH)
    if cred_path.exists():
        return

    raw_json = _env_str("GOOGLE_CREDENTIALS_JSON", "")
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            parsed = None

        if isinstance(parsed, dict):
            _write_json_file(cred_path, parsed)
            return

    secret_section = _get_streamlit_secret_section(GOOGLE_CREDENTIALS_SECRET_SECTION)
    if secret_section:
        _write_json_file(cred_path, secret_section)


# ── Load .env ────────────────────────────────────────────────────────────────
_env_path = BASE_DIR / ".env"
load_dotenv(dotenv_path=_env_path, override=False)

# ── Load Streamlit secrets (Cloud deployment) ────────────────────────────────
_load_streamlit_secrets_into_env()

# ── Ensure directories exist ────────────────────────────────────────────────
for _dir in (UPLOAD_DIR, PROCESSED_DIR, VECTORSTORE_DIR, LOGS_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────
APP_TITLE = _env_str("APP_TITLE", "PDF Research Analyzer")
APP_VERSION = _env_str("APP_VERSION", "1.0.0")
DEBUG = _env_bool("DEBUG", False)
LOG_LEVEL = _env_str("LOG_LEVEL", "INFO").upper()

# ─────────────────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────────────────
DATABASE_URL = _env_str(
    "DATABASE_URL",
    f"sqlite:///{BASE_DIR / 'data' / 'pdf_analyzer.db'}",
)
SQLALCHEMY_ECHO = _env_bool("SQLALCHEMY_ECHO", False)

# ─────────────────────────────────────────────────────────────────────────────
# Google Drive
# ─────────────────────────────────────────────────────────────────────────────
GOOGLE_DRIVE_FOLDER_ID = _env_str("GOOGLE_DRIVE_FOLDER_ID", "")
GOOGLE_CREDENTIALS_PATH = _env_str("GOOGLE_CREDENTIALS_PATH", "credentials.json")
GOOGLE_CREDENTIALS_SECRET_SECTION = _env_str(
    "GOOGLE_CREDENTIALS_SECRET_SECTION",
    "gcp_service_account",
)
GOOGLE_CREDENTIALS_JSON = _env_str("GOOGLE_CREDENTIALS_JSON", "")

# Recreate the credentials file locally/cloud if possible
_materialize_google_credentials_file()

# ─────────────────────────────────────────────────────────────────────────────
# OpenRouter (Primary LLM)
# ─────────────────────────────────────────────────────────────────────────────
OPENROUTER_API_KEY = _env_str("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = _env_str("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL = _env_str("OPENROUTER_MODEL", "openrouter/free")
OPENROUTER_TIMEOUT = _env_int("OPENROUTER_TIMEOUT", 90)
OPENROUTER_RATE_LIMIT_DELAY = _env_float("OPENROUTER_RATE_LIMIT_DELAY", 10.0)

# ─────────────────────────────────────────────────────────────────────────────
# HuggingFace (Fallback LLM)
# ─────────────────────────────────────────────────────────────────────────────
HUGGINGFACE_API_KEY = _env_str("HUGGINGFACE_API_KEY", "")
HUGGINGFACE_BASE_URL = _env_str("HUGGINGFACE_BASE_URL", "https://router.huggingface.co/v1")
HUGGINGFACE_MODEL = _env_str(
    "HUGGINGFACE_MODEL",
    "meta-llama/Llama-3.1-8B-Instruct:cerebras",
)
HUGGINGFACE_TIMEOUT = _env_int("HUGGINGFACE_TIMEOUT", 90)

# ─────────────────────────────────────────────────────────────────────────────
# Embeddings
# ─────────────────────────────────────────────────────────────────────────────
EMBEDDING_MODEL = _env_str(
    "EMBEDDING_MODEL",
    "sentence-transformers/all-MiniLM-L6-v2",
)
EMBEDDING_DIMENSION = _env_int("EMBEDDING_DIMENSION", 384)

# ─────────────────────────────────────────────────────────────────────────────
# Chunking
# ─────────────────────────────────────────────────────────────────────────────
CHUNK_SIZE = _env_int("CHUNK_SIZE", 500)
CHUNK_OVERLAP = _env_int("CHUNK_OVERLAP", 50)
MIN_CHUNK_LENGTH = _env_int("MIN_CHUNK_LENGTH", 50)

# ─────────────────────────────────────────────────────────────────────────────
# Retrieval
# ─────────────────────────────────────────────────────────────────────────────
TOP_K_RESULTS = _env_int("TOP_K_RESULTS", 8)
SIMILARITY_THRESHOLD = _env_float("SIMILARITY_THRESHOLD", 0.05)

# ─────────────────────────────────────────────────────────────────────────────
# LLM Generation
# ─────────────────────────────────────────────────────────────────────────────
MAX_TOKENS = _env_int("MAX_TOKENS", 2048)
TEMPERATURE = _env_float("TEMPERATURE", 0.3)
CONTEXT_WINDOW_TOKENS = _env_int("CONTEXT_WINDOW_TOKENS", 6000)

# ─────────────────────────────────────────────────────────────────────────────
# Retry / Back-off
# ─────────────────────────────────────────────────────────────────────────────
RETRY_MAX_ATTEMPTS = _env_int("RETRY_MAX_ATTEMPTS", 3)
RETRY_BASE_DELAY = _env_float("RETRY_BASE_DELAY", 1.0)
RETRY_MAX_DELAY = _env_float("RETRY_MAX_DELAY", 60.0)
RETRY_BACKOFF_FACTOR = _env_float("RETRY_BACKOFF_FACTOR", 2.0)

# ─────────────────────────────────────────────────────────────────────────────
# Upload
# ─────────────────────────────────────────────────────────────────────────────
MAX_FILE_SIZE_MB = _env_int("MAX_FILE_SIZE_MB", 50)
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
ALLOWED_EXTENSIONS = {".pdf", ".docx", ".doc", ".txt", ".xlsx", ".xls", ".csv"}

# ─────────────────────────────────────────────────────────────────────────────
# Streamlit UI
# ─────────────────────────────────────────────────────────────────────────────
STREAMLIT_PAGE_TITLE = APP_TITLE
STREAMLIT_PAGE_ICON = "📄"
STREAMLIT_LAYOUT = _env_str("STREAMLIT_LAYOUT", "wide")
MAX_CHAT_HISTORY = _env_int("MAX_CHAT_HISTORY", 50)

# ─────────────────────────────────────────────────────────────────────────────
# Section Detection Keywords
# ─────────────────────────────────────────────────────────────────────────────
SECTION_KEYWORDS: dict[str, list[str]] = {
    "abstract": ["abstract", "summary", "overview", "synopsis"],
    "introduction": [
        "introduction",
        "background",
        "motivation",
        "problem statement",
        "1. introduction",
        "1 introduction",
    ],
    "methods": [
        "methods",
        "methodology",
        "approach",
        "materials and methods",
        "experimental setup",
        "proposed method",
        "2. methods",
        "3. methods",
    ],
    "results": [
        "results",
        "experiments",
        "evaluation",
        "findings",
        "experimental results",
        "4. results",
        "5. results",
    ],
    "discussion": ["discussion", "analysis", "interpretation"],
    "conclusion": [
        "conclusion",
        "conclusions",
        "concluding remarks",
        "future work",
        "summary and conclusion",
    ],
    "references": ["references", "bibliography", "works cited"],
}

# ─────────────────────────────────────────────────────────────────────────────
# Validation & Summary
# ─────────────────────────────────────────────────────────────────────────────
def validate_config() -> list[str]:
    issues: list[str] = []

    if not OPENROUTER_API_KEY:
        issues.append("OPENROUTER_API_KEY not set — OpenRouter will be unavailable.")
    elif not OPENROUTER_API_KEY.startswith("sk-or-"):
        issues.append(
            "OPENROUTER_API_KEY doesn't look valid (expected prefix 'sk-or-'). "
            "Get your key at https://openrouter.ai/keys"
        )

    if not HUGGINGFACE_API_KEY:
        issues.append(
            "HUGGINGFACE_API_KEY not set — HuggingFace fallback will be unavailable."
        )
    elif not HUGGINGFACE_API_KEY.startswith("hf_"):
        issues.append(
            "HUGGINGFACE_API_KEY doesn't look valid (expected prefix 'hf_'). "
            "Get your token at https://huggingface.co/settings/tokens — "
            "token must have 'Make calls to Inference Providers' permission."
        )

    if not OPENROUTER_API_KEY and not HUGGINGFACE_API_KEY:
        issues.append("CRITICAL: No LLM provider configured. Chat will not work.")

    if CHUNK_OVERLAP >= CHUNK_SIZE:
        issues.append(
            f"CHUNK_OVERLAP ({CHUNK_OVERLAP}) must be less than CHUNK_SIZE ({CHUNK_SIZE})."
        )

    if GOOGLE_DRIVE_FOLDER_ID:
        cred_path = _resolved_path(GOOGLE_CREDENTIALS_PATH)
        if not cred_path.exists():
            issues.append(
                "GOOGLE_DRIVE_FOLDER_ID is set, but GOOGLE_CREDENTIALS_PATH does not point to a file "
                "and no Streamlit secret section was materialized."
            )

    return issues


def get_config_summary() -> dict:
    return {
        "app_title": APP_TITLE,
        "version": APP_VERSION,
        "debug": DEBUG,
        "log_level": LOG_LEVEL,
        "openrouter_model": OPENROUTER_MODEL,
        "openrouter_configured": bool(OPENROUTER_API_KEY),
        "huggingface_model": HUGGINGFACE_MODEL,
        "huggingface_configured": bool(HUGGINGFACE_API_KEY),
        "huggingface_base_url": HUGGINGFACE_BASE_URL,
        "embedding_model": EMBEDDING_MODEL,
        "embedding_dimension": EMBEDDING_DIMENSION,
        "chunk_size": CHUNK_SIZE,
        "chunk_overlap": CHUNK_OVERLAP,
        "similarity_threshold": SIMILARITY_THRESHOLD,
        "top_k_results": TOP_K_RESULTS,
        "max_file_size_mb": MAX_FILE_SIZE_MB,
        "upload_dir": str(UPLOAD_DIR),
        "processed_dir": str(PROCESSED_DIR),
        "vectorstore_dir": str(VECTORSTORE_DIR),
        "database_url_configured": bool(DATABASE_URL),
        "google_drive_configured": bool(GOOGLE_DRIVE_FOLDER_ID),
        "google_credentials_path": str(_resolved_path(GOOGLE_CREDENTIALS_PATH)),
    }
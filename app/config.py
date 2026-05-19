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

    Handles:
    - flat keys like OPENROUTER_API_KEY, GOOGLE_DRIVE_FOLDER_ID
    - nested OAuth section like [google_oauth_client]
    - nested GCP service-account section like [gcp_service_account]
    """

    try:
        import streamlit as st
    except Exception:
        return

    try:
        secrets = getattr(st, "secrets", None)
        if secrets is None:
            return

        # Pass 1: load all flat string keys into os.environ
        for key in secrets.keys():
            try:
                value = secrets[key]
                if isinstance(value, str) and value.strip():
                    existing = os.getenv(key)
                    if not existing or not existing.strip():
                        os.environ[key] = value
            except Exception:
                pass

        # Pass 1b: scan nested sections for known provider keys
        known_secret_keys = {
            "OPENROUTER_API_KEY",
            "HUGGINGFACE_API_KEY",
            "HUGGINGFACE_BASE_URL",
            "HUGGINGFACE_MODEL",
            "OPENROUTER_BASE_URL",
            "OPENROUTER_MODEL",
            "OPENROUTER_TIMEOUT",
            "HUGGINGFACE_TIMEOUT",
            "OPENROUTER_RATE_LIMIT_DELAY",
            "GOOGLE_DRIVE_FOLDER_ID",
            "GOOGLE_CREDENTIALS_PATH",
            "GOOGLE_OAUTH_CLIENT_ID",
            "GOOGLE_OAUTH_CLIENT_SECRET",
            "GOOGLE_OAUTH_PROJECT_ID",
            "GOOGLE_OAUTH_REDIRECT_URI",
            "GOOGLE_OAUTH_CLIENT_PATH",
            "GOOGLE_OAUTH_TOKEN_PATH",
        }

        def _load_known_secret_keys(item: Any) -> None:
            if not isinstance(item, Mapping):
                return

            for key, value in item.items():
                if isinstance(value, str) and key in known_secret_keys and value.strip():
                    existing = os.getenv(key)
                    if not existing or not existing.strip():
                        os.environ[key] = value
                elif isinstance(value, Mapping):
                    _load_known_secret_keys(value)

        _load_known_secret_keys(secrets)

        # Pass 2: look for a Google service-account section and materialize JSON
        gcp_sections = (
            "gcp_service_account",
            "google_service_account",
            "GOOGLE_SERVICE_ACCOUNT",
            "GCP_SERVICE_ACCOUNT",
        )
        for sec in gcp_sections:
            try:
                section = secrets.get(sec) if hasattr(secrets, "get") else secrets[sec]
                if section is None or not isinstance(section, Mapping):
                    continue

                cred_path = BASE_DIR / "credentials.json"
                if not cred_path.exists():
                    _write_json_file(cred_path, dict(section))

                existing = os.getenv("GOOGLE_CREDENTIALS_PATH")
                if not existing or not existing.strip():
                    os.environ["GOOGLE_CREDENTIALS_PATH"] = str(cred_path)

                break
            except (KeyError, Exception):
                continue

        # Pass 3: surface GOOGLE_DRIVE_FOLDER_ID from optional sub-section
        drive_key = "GOOGLE_DRIVE_FOLDER_ID"
        if not os.getenv(drive_key) or not os.getenv(drive_key).strip():
            for sec in ("google_drive", "GOOGLE_DRIVE", "drive"):
                try:
                    section = secrets.get(sec) if hasattr(secrets, "get") else secrets[sec]
                    if section is None or not isinstance(section, Mapping):
                        continue

                    fid = section.get(drive_key) or section.get("folder_id")
                    if fid and isinstance(fid, str) and fid.strip():
                        os.environ[drive_key] = fid.strip()
                        break
                except (KeyError, Exception):
                    continue

        # Pass 4: OAuth client section from secrets, if present
        oauth_sections = (
            "google_oauth_client",
            "GOOGLE_OAUTH_CLIENT",
            "oauth_client",
            "google_oauth",
            "gcp_oauth_client",
        )
        for sec in oauth_sections:
            try:
                section = secrets.get(sec) if hasattr(secrets, "get") else secrets[sec]
                if section is None or not isinstance(section, Mapping):
                    continue

                # Allow either a flat section or nested {"web": {...}}
                if "web" in section and isinstance(section["web"], Mapping):
                    os.environ.setdefault("GOOGLE_OAUTH_CLIENT_JSON", json.dumps(dict(section)))
                    break

                if "installed" in section and isinstance(section["installed"], Mapping):
                    os.environ.setdefault("GOOGLE_OAUTH_CLIENT_JSON", json.dumps(dict(section)))
                    break

                # Treat plain mapping as the "web" body
                os.environ.setdefault("GOOGLE_OAUTH_CLIENT_JSON", json.dumps({"web": dict(section)}))
                break

            except (KeyError, Exception):
                continue

    except Exception:
        return


def _get_streamlit_secret_section(name: str) -> dict[str, Any] | None:
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
        section = secrets.get(name) if hasattr(secrets, "get") else secrets[name]
        if isinstance(section, Mapping):
            return dict(section)
    except (KeyError, Exception):
        return None

    return None


def _write_json_file(path: Path, data: Mapping[str, Any]) -> bool:
    """
    Write JSON data safely.
    """

    try:
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w", encoding="utf-8") as file:
            json.dump(dict(data), file, indent=2)

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
    directory.mkdir(parents=True, exist_ok=True)

# =============================================================================
# APP SETTINGS
# =============================================================================

APP_TITLE = _env_str("APP_TITLE", "PDF Research Analyzer")
APP_VERSION = _env_str("APP_VERSION", "1.0.0")
DEBUG = _env_bool("DEBUG", False)
LOG_LEVEL = _env_str("LOG_LEVEL", "INFO").upper()

# =============================================================================
# STREAMLIT SETTINGS
# =============================================================================

STREAMLIT_PAGE_TITLE = _env_str("STREAMLIT_PAGE_TITLE", APP_TITLE)
STREAMLIT_PAGE_ICON = _env_str("STREAMLIT_PAGE_ICON", "📚")
STREAMLIT_LAYOUT = _env_str("STREAMLIT_LAYOUT", "wide")
STREAMLIT_SIDEBAR_STATE = _env_str("STREAMLIT_SIDEBAR_STATE", "expanded")
MAX_CHAT_HISTORY = _env_int("MAX_CHAT_HISTORY", 20)

# =============================================================================
# DATABASE
# =============================================================================

DATABASE_URL = _env_str(
    "DATABASE_URL",
    f"sqlite:///{BASE_DIR / 'data' / 'pdf_analyzer.db'}",
)

SQLALCHEMY_ECHO = _env_bool("SQLALCHEMY_ECHO", False)

# =============================================================================
# GOOGLE DRIVE / OAUTH
# =============================================================================

_raw_folder_id = _env_str("GOOGLE_DRIVE_FOLDER_ID")
GOOGLE_DRIVE_FOLDER_ID = (
    _raw_folder_id.rstrip("/").split("/")[-1]
    if _raw_folder_id.startswith("http")
    else _raw_folder_id
)

# Service account fallback (kept for compatibility)
GOOGLE_CREDENTIALS_PATH = _env_str("GOOGLE_CREDENTIALS_PATH", "credentials.json")
GOOGLE_CREDENTIALS_SECRET_SECTION = _env_str("GOOGLE_CREDENTIALS_SECRET_SECTION", "gcp_service_account")
GOOGLE_CREDENTIALS_JSON = _env_str("GOOGLE_CREDENTIALS_JSON")

# OAuth web app settings
GOOGLE_OAUTH_CLIENT_SECTION = _env_str("GOOGLE_OAUTH_CLIENT_SECTION", "google_oauth_client")
GOOGLE_OAUTH_CLIENT_PATH = _env_str("GOOGLE_OAUTH_CLIENT_PATH", str(DATA_DIR / "google_oauth_client.json"))
GOOGLE_OAUTH_TOKEN_PATH = _env_str("GOOGLE_OAUTH_TOKEN_PATH", str(DATA_DIR / "google_oauth_token.json"))
GOOGLE_OAUTH_REDIRECT_URI = _env_str("GOOGLE_OAUTH_REDIRECT_URI", "http://localhost:8501/")
GOOGLE_OAUTH_CLIENT_ID = _env_str("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = _env_str("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_PROJECT_ID = _env_str("GOOGLE_OAUTH_PROJECT_ID")
GOOGLE_OAUTH_AUTH_URI = _env_str("GOOGLE_OAUTH_AUTH_URI", "https://accounts.google.com/o/oauth2/auth")
GOOGLE_OAUTH_TOKEN_URI = _env_str("GOOGLE_OAUTH_TOKEN_URI", "https://oauth2.googleapis.com/token")
GOOGLE_OAUTH_AUTH_PROVIDER_X509_CERT_URL = _env_str(
    "GOOGLE_OAUTH_AUTH_PROVIDER_X509_CERT_URL",
    "https://www.googleapis.com/oauth2/v1/certs",
)
GOOGLE_OAUTH_CLIENT_X509_CERT_URL = _env_str("GOOGLE_OAUTH_CLIENT_X509_CERT_URL")
GOOGLE_OAUTH_CLIENT_JSON = _env_str("GOOGLE_OAUTH_CLIENT_JSON")


def _build_google_oauth_client_json() -> str:
    """
    Assemble OAuth client JSON from env vars or secrets when a raw JSON blob is
    not already provided.
    """
    raw = GOOGLE_OAUTH_CLIENT_JSON.strip()
    if raw:
        return raw

    if GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET:
        payload: dict[str, Any] = {
            "web": {
                "client_id": GOOGLE_OAUTH_CLIENT_ID,
                "client_secret": GOOGLE_OAUTH_CLIENT_SECRET,
                "auth_uri": GOOGLE_OAUTH_AUTH_URI,
                "token_uri": GOOGLE_OAUTH_TOKEN_URI,
                "auth_provider_x509_cert_url": GOOGLE_OAUTH_AUTH_PROVIDER_X509_CERT_URL,
                "redirect_uris": [GOOGLE_OAUTH_REDIRECT_URI],
            }
        }
        if GOOGLE_OAUTH_PROJECT_ID:
            payload["web"]["project_id"] = GOOGLE_OAUTH_PROJECT_ID  # type: ignore[index]
        if GOOGLE_OAUTH_CLIENT_X509_CERT_URL:
            payload["web"]["client_x509_cert_url"] = GOOGLE_OAUTH_CLIENT_X509_CERT_URL  # type: ignore[index]
        return json.dumps(payload)

    section = _get_streamlit_secret_section(GOOGLE_OAUTH_CLIENT_SECTION)
    if section:
        if "web" in section and isinstance(section["web"], Mapping):
            return json.dumps(section)
        if "installed" in section and isinstance(section["installed"], Mapping):
            return json.dumps(section)

        payload = {"web": dict(section)}
        if not payload["web"].get("redirect_uris"):
            payload["web"]["redirect_uris"] = [GOOGLE_OAUTH_REDIRECT_URI]
        return json.dumps(payload)

    return ""


def _build_google_credentials_json() -> str:
    """
    Assemble Google service-account JSON from env vars or secrets.
    """
    gc_keys = [
        "type",
        "project_id",
        "private_key_id",
        "private_key",
        "client_email",
        "client_id",
        "auth_uri",
        "token_uri",
        "auth_provider_x509_cert_url",
        "client_x509_cert_url",
        "universe_domain",
    ]

    creds_dict: dict[str, str] = {}

    for key in gc_keys:
        val = _env_str(key)
        if val:
            if key == "private_key":
                val = val.replace("\\n", "\n")
            creds_dict[key] = val

    if creds_dict.get("private_key") and creds_dict.get("client_email"):
        return json.dumps(creds_dict)

    section = _get_streamlit_secret_section(GOOGLE_CREDENTIALS_SECRET_SECTION)
    if section:
        return json.dumps(section)

    return ""


def _materialize_google_oauth_client_file() -> None:
    """
    Create the OAuth client JSON file if enough info is available.
    """
    cred_path = _resolved_path(GOOGLE_OAUTH_CLIENT_PATH)

    if cred_path.exists():
        return

    raw_json = _build_google_oauth_client_json()
    if not raw_json:
        return

    try:
        parsed = json.loads(raw_json)
        if isinstance(parsed, dict):
            _write_json_file(cred_path, parsed)
            os.environ.setdefault("GOOGLE_OAUTH_CLIENT_PATH", str(cred_path))
    except Exception:
        return


def _materialize_google_credentials_file() -> None:
    """
    Create the service-account credentials file if enough info is available.
    """
    cred_path = _resolved_path(GOOGLE_CREDENTIALS_PATH)

    if cred_path.exists():
        return

    raw_json = GOOGLE_CREDENTIALS_JSON or _build_google_credentials_json()
    if not raw_json:
        return

    try:
        parsed = json.loads(raw_json)
        if isinstance(parsed, dict):
            _write_json_file(cred_path, parsed)
            os.environ.setdefault("GOOGLE_CREDENTIALS_PATH", str(cred_path))
    except Exception:
        return


_materialize_google_oauth_client_file()
_materialize_google_credentials_file()


def is_drive_sync_configured() -> bool:
    """
    Returns True when GOOGLE_DRIVE_FOLDER_ID is set and at least one valid
    auth artifact is available.

    For OAuth, this is enough to show the Drive UI as configured even before
    the first token exchange.
    """
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        return False

    oauth_client_raw = os.getenv("GOOGLE_OAUTH_CLIENT_PATH", GOOGLE_OAUTH_CLIENT_PATH).strip()
    oauth_token_raw = os.getenv("GOOGLE_OAUTH_TOKEN_PATH", GOOGLE_OAUTH_TOKEN_PATH).strip()
    sa_raw = os.getenv("GOOGLE_CREDENTIALS_PATH", GOOGLE_CREDENTIALS_PATH).strip()

    oauth_client_path = Path(oauth_client_raw) if os.path.isabs(oauth_client_raw) else BASE_DIR / oauth_client_raw
    oauth_token_path = Path(oauth_token_raw) if os.path.isabs(oauth_token_raw) else BASE_DIR / oauth_token_raw
    sa_path = Path(sa_raw) if os.path.isabs(sa_raw) else BASE_DIR / sa_raw

    return any(
        path.exists()
        for path in (oauth_client_path, oauth_token_path, sa_path)
    )


# =============================================================================
# OPENROUTER
# =============================================================================

OPENROUTER_API_KEY = _env_str("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = _env_str("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL = _env_str("OPENROUTER_MODEL", "openrouter/free")
OPENROUTER_TIMEOUT = _env_int("OPENROUTER_TIMEOUT", 90)
OPENROUTER_RATE_LIMIT_DELAY = _env_float("OPENROUTER_RATE_LIMIT_DELAY", 10.0)

# =============================================================================
# HUGGINGFACE
# =============================================================================

HUGGINGFACE_API_KEY = _env_str("HUGGINGFACE_API_KEY")
HUGGINGFACE_BASE_URL = _env_str("HUGGINGFACE_BASE_URL", "https://router.huggingface.co/v1")
HUGGINGFACE_MODEL = _env_str("HUGGINGFACE_MODEL", "meta-llama/Llama-3.1-8B-Instruct:cerebras")
HUGGINGFACE_TIMEOUT = _env_int("HUGGINGFACE_TIMEOUT", 90)

# =============================================================================
# EMBEDDINGS
# =============================================================================

EMBEDDING_MODEL = _env_str("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIMENSION = _env_int("EMBEDDING_DIMENSION", 384)

# =============================================================================
# CHUNKING
# =============================================================================

CHUNK_SIZE = _env_int("CHUNK_SIZE", 500)
CHUNK_OVERLAP = _env_int("CHUNK_OVERLAP", 50)

SECTION_KEYWORDS: dict[str, list[str]] = {
    "abstract": [
        "abstract",
        "summary",
        "executive summary",
        "overview",
    ],
    "introduction": [
        "introduction",
        "background",
        "background and motivation",
        "background and related work",
    ],
    "methods": [
        "methods",
        "materials and methods",
        "methodology",
        "experimental",
        "experimental setup",
    ],
    "results": [
        "results",
        "findings",
        "outcomes",
        "experimental results",
        "clinical results",
    ],
    "discussion": [
        "discussion",
        "analysis",
        "interpretation",
        "discussion and conclusion",
    ],
    "conclusion": [
        "conclusion",
        "conclusions",
        "summary and conclusion",
        "future work",
        "limitations",
    ],
    "references": [
        "references",
        "bibliography",
        "works cited",
        "citations",
        "sources",
    ],
}

MIN_CHUNK_LENGTH = _env_int("MIN_CHUNK_LENGTH", 50)

# =============================================================================
# RETRIEVAL
# =============================================================================

TOP_K_RESULTS = _env_int("TOP_K_RESULTS", 8)
SIMILARITY_THRESHOLD = _env_float("SIMILARITY_THRESHOLD", 0.05)

# =============================================================================
# GENERATION
# =============================================================================

MAX_TOKENS = _env_int("MAX_TOKENS", 2048)
TEMPERATURE = _env_float("TEMPERATURE", 0.3)
CONTEXT_WINDOW_TOKENS = _env_int("CONTEXT_WINDOW_TOKENS", 6000)

# =============================================================================
# RETRY
# =============================================================================

RETRY_MAX_ATTEMPTS = _env_int("RETRY_MAX_ATTEMPTS", 3)
RETRY_BASE_DELAY = _env_float("RETRY_BASE_DELAY", 1.0)
RETRY_MAX_DELAY = _env_float("RETRY_MAX_DELAY", 60.0)
RETRY_BACKOFF_FACTOR = _env_float("RETRY_BACKOFF_FACTOR", 2.0)

# =============================================================================
# UPLOAD SETTINGS
# =============================================================================

MAX_FILE_SIZE_MB = _env_int("MAX_FILE_SIZE_MB", 50)
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

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

VECTORSTORE_COLLECTION_NAME = _env_str("VECTORSTORE_COLLECTION_NAME", "pdf_research_chunks")

# =============================================================================
# CACHE SETTINGS
# =============================================================================

ENABLE_CACHE = _env_bool("ENABLE_CACHE", True)
CACHE_TTL_SECONDS = _env_int("CACHE_TTL_SECONDS", 3600)

# =============================================================================
# UI SETTINGS
# =============================================================================

DEFAULT_THEME = _env_str("DEFAULT_THEME", "light")
SHOW_DEBUG_INFO = _env_bool("SHOW_DEBUG_INFO", False)

# =============================================================================
# VALIDATION
# =============================================================================


def validate_config() -> list[str]:
    """
    Validate configuration values.
    """

    issues: list[str] = []

    if not OPENROUTER_API_KEY and not HUGGINGFACE_API_KEY:
        issues.append(
            "No LLM provider configured. Set OPENROUTER_API_KEY or HUGGINGFACE_API_KEY."
        )

    if CHUNK_OVERLAP >= CHUNK_SIZE:
        issues.append(
            f"CHUNK_OVERLAP ({CHUNK_OVERLAP}) must be less than CHUNK_SIZE ({CHUNK_SIZE})"
        )

    if MAX_FILE_SIZE_MB <= 0:
        issues.append("MAX_FILE_SIZE_MB must be greater than 0")

    if TOP_K_RESULTS <= 0:
        issues.append("TOP_K_RESULTS must be greater than 0")

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
        "drive_sync_configured": is_drive_sync_configured(),
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
    "GOOGLE_CREDENTIALS_SECRET_SECTION",
    "GOOGLE_CREDENTIALS_JSON",
    "GOOGLE_OAUTH_CLIENT_SECTION",
    "GOOGLE_OAUTH_CLIENT_PATH",
    "GOOGLE_OAUTH_TOKEN_PATH",
    "GOOGLE_OAUTH_REDIRECT_URI",
    "GOOGLE_OAUTH_CLIENT_ID",
    "GOOGLE_OAUTH_CLIENT_SECRET",
    "GOOGLE_OAUTH_PROJECT_ID",
    "GOOGLE_OAUTH_AUTH_URI",
    "GOOGLE_OAUTH_TOKEN_URI",
    "GOOGLE_OAUTH_AUTH_PROVIDER_X509_CERT_URL",
    "GOOGLE_OAUTH_CLIENT_X509_CERT_URL",
    "GOOGLE_OAUTH_CLIENT_JSON",
    "is_drive_sync_configured",

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
    "SECTION_KEYWORDS",
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
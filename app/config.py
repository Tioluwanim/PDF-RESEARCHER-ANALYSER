"""
config.py - Central configuration for PDF Research Analyzer.
All settings loaded from environment variables with validated defaults.
Endpoints verified March 2026.
"""

from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Paths (resolved before .env load) ────────────────────────────────────────
BASE_DIR        = Path(__file__).resolve().parent.parent
DATA_DIR        = BASE_DIR / "data"
UPLOAD_DIR      = DATA_DIR / "uploads"
PROCESSED_DIR   = DATA_DIR / "processed"
VECTORSTORE_DIR = DATA_DIR / "vectorstore"
LOGS_DIR        = BASE_DIR / "logs"

# ── Load .env with absolute path ─────────────────────────────────────────────
_env_path = BASE_DIR / ".env"
load_dotenv(dotenv_path=_env_path, override=True)

# ── Ensure directories exist ──────────────────────────────────────────────────
for _d in [UPLOAD_DIR, PROCESSED_DIR, VECTORSTORE_DIR, LOGS_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────
APP_TITLE   = os.getenv("APP_TITLE",   "PDF Research Analyzer")
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
DEBUG       = os.getenv("DEBUG", "false").lower() == "true"
LOG_LEVEL   = os.getenv("LOG_LEVEL",   "INFO").upper()

# ─────────────────────────────────────────────────────────────────────────────
# OpenRouter  (Primary LLM)
# Docs: https://openrouter.ai/docs
# Get key: https://openrouter.ai/keys
# ─────────────────────────────────────────────────────────────────────────────
OPENROUTER_API_KEY  = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# "openrouter/free" = official OpenRouter free-models router.
# Auto-selects from all currently live free models. Never returns 404.
# Docs: https://openrouter.ai/docs/guides/routing/routers/free-models-router
OPENROUTER_MODEL    = os.getenv("OPENROUTER_MODEL", "openrouter/free")
OPENROUTER_TIMEOUT  = int(os.getenv("OPENROUTER_TIMEOUT", "90"))
OPENROUTER_RATE_LIMIT_DELAY = float(os.getenv("OPENROUTER_RATE_LIMIT_DELAY", "10.0"))

# ─────────────────────────────────────────────────────────────────────────────
# HuggingFace  (Fallback LLM)
# Uses OpenAI-compatible Inference Providers router (verified March 2026)
# Docs: https://huggingface.co/docs/inference-providers
# Get token: https://huggingface.co/settings/tokens
#   → Token MUST have "Make calls to Inference Providers" permission
# ─────────────────────────────────────────────────────────────────────────────
HUGGINGFACE_API_KEY  = os.getenv("HUGGINGFACE_API_KEY", "")

# Verified current endpoint (March 2026)
HUGGINGFACE_BASE_URL = "https://router.huggingface.co/v1"

# Model format: "org/model:provider" — ":auto" lets HF pick best provider
# Docs: https://huggingface.co/docs/inference-providers/en/tasks/chat-completion
HUGGINGFACE_MODEL    = os.getenv(
    "HUGGINGFACE_MODEL",
    "meta-llama/Llama-3.1-8B-Instruct:cerebras",
)
HUGGINGFACE_TIMEOUT  = int(os.getenv("HUGGINGFACE_TIMEOUT", "90"))

# ─────────────────────────────────────────────────────────────────────────────
# Embeddings  (local — no API cost)
# ─────────────────────────────────────────────────────────────────────────────
EMBEDDING_MODEL     = os.getenv("EMBEDDING_MODEL",
                                "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIMENSION = int(os.getenv("EMBEDDING_DIMENSION", "384"))

# ─────────────────────────────────────────────────────────────────────────────
# Chunking
# ─────────────────────────────────────────────────────────────────────────────
CHUNK_SIZE       = int(os.getenv("CHUNK_SIZE",      "500"))
CHUNK_OVERLAP    = int(os.getenv("CHUNK_OVERLAP",    "50"))
MIN_CHUNK_LENGTH = int(os.getenv("MIN_CHUNK_LENGTH", "50"))

# ─────────────────────────────────────────────────────────────────────────────
# RAG / Retrieval
# ─────────────────────────────────────────────────────────────────────────────
TOP_K_RESULTS        = int(os.getenv("TOP_K_RESULTS",       "5"))
SIMILARITY_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", "0.10"))

# ─────────────────────────────────────────────────────────────────────────────
# LLM Generation
# ─────────────────────────────────────────────────────────────────────────────
MAX_TOKENS            = int(os.getenv("MAX_TOKENS",            "1024"))
TEMPERATURE           = float(os.getenv("TEMPERATURE",          "0.7"))
CONTEXT_WINDOW_TOKENS = int(os.getenv("CONTEXT_WINDOW_TOKENS", "4000"))

# ─────────────────────────────────────────────────────────────────────────────
# Retry / Back-off
# ─────────────────────────────────────────────────────────────────────────────
RETRY_MAX_ATTEMPTS   = int(os.getenv("RETRY_MAX_ATTEMPTS",   "3"))
RETRY_BASE_DELAY     = float(os.getenv("RETRY_BASE_DELAY",   "1.0"))
RETRY_MAX_DELAY      = float(os.getenv("RETRY_MAX_DELAY",   "60.0"))
RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", "2.0"))

# ─────────────────────────────────────────────────────────────────────────────
# Upload
# ─────────────────────────────────────────────────────────────────────────────
MAX_FILE_SIZE_MB    = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
ALLOWED_EXTENSIONS  = {".pdf"}

# ─────────────────────────────────────────────────────────────────────────────
# Streamlit UI
# ─────────────────────────────────────────────────────────────────────────────
STREAMLIT_PAGE_TITLE = APP_TITLE
STREAMLIT_PAGE_ICON  = "📄"
STREAMLIT_LAYOUT     = "wide"
MAX_CHAT_HISTORY     = int(os.getenv("MAX_CHAT_HISTORY", "50"))

# ─────────────────────────────────────────────────────────────────────────────
# Section Detection Keywords
# ─────────────────────────────────────────────────────────────────────────────
SECTION_KEYWORDS: dict[str, list[str]] = {
    "abstract"    : ["abstract", "summary", "overview", "synopsis"],
    "introduction": ["introduction", "background", "motivation",
                     "problem statement", "1. introduction", "1 introduction"],
    "methods"     : ["methods", "methodology", "approach",
                     "materials and methods", "experimental setup",
                     "proposed method", "2. methods", "3. methods"],
    "results"     : ["results", "experiments", "evaluation", "findings",
                     "experimental results", "4. results", "5. results"],
    "discussion"  : ["discussion", "analysis", "interpretation"],
    "conclusion"  : ["conclusion", "conclusions", "concluding remarks",
                     "future work", "summary and conclusion"],
    "references"  : ["references", "bibliography", "works cited"],
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
        issues.append("HUGGINGFACE_API_KEY not set — HuggingFace fallback will be unavailable.")
    elif not HUGGINGFACE_API_KEY.startswith("hf_"):
        issues.append(
            "HUGGINGFACE_API_KEY doesn't look valid (expected prefix 'hf_'). "
            "Get your token at https://huggingface.co/settings/tokens — "
            "token MUST have 'Make calls to Inference Providers' permission."
        )

    if not OPENROUTER_API_KEY and not HUGGINGFACE_API_KEY:
        issues.append("CRITICAL: No LLM provider configured. Chat will not work.")

    if CHUNK_OVERLAP >= CHUNK_SIZE:
        issues.append(
            f"CHUNK_OVERLAP ({CHUNK_OVERLAP}) must be less than CHUNK_SIZE ({CHUNK_SIZE})."
        )

    return issues


def get_config_summary() -> dict:
    return {
        "app_title"             : APP_TITLE,
        "version"               : APP_VERSION,
        "debug"                 : DEBUG,
        "log_level"             : LOG_LEVEL,
        "openrouter_model"      : OPENROUTER_MODEL,
        "openrouter_configured" : bool(OPENROUTER_API_KEY),
        "huggingface_model"     : HUGGINGFACE_MODEL,
        "huggingface_configured": bool(HUGGINGFACE_API_KEY),
        "huggingface_base_url"  : HUGGINGFACE_BASE_URL,
        "embedding_model"       : EMBEDDING_MODEL,
        "embedding_dimension"   : EMBEDDING_DIMENSION,
        "chunk_size"            : CHUNK_SIZE,
        "chunk_overlap"         : CHUNK_OVERLAP,
        "similarity_threshold"  : SIMILARITY_THRESHOLD,
        "top_k_results"         : TOP_K_RESULTS,
        "max_file_size_mb"      : MAX_FILE_SIZE_MB,
        "upload_dir"            : str(UPLOAD_DIR),
        "processed_dir"         : str(PROCESSED_DIR),
        "vectorstore_dir"       : str(VECTORSTORE_DIR),
    }
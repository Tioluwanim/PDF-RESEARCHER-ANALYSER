"""
run.py - Entry point for PDF Research Analyzer.
Loads .env, validates config, then launches Streamlit.
"""

from __future__ import annotations

import sys
import subprocess
from pathlib import Path

# Load .env before anything else (including app imports)
from dotenv import load_dotenv
_env = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_env, override=True)

import os
import logging

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("startup")


def _banner() -> None:
    from app.config import (
        APP_TITLE, APP_VERSION,
        OPENROUTER_API_KEY, OPENROUTER_MODEL,
        HUGGINGFACE_API_KEY, HUGGINGFACE_MODEL,
        HUGGINGFACE_BASE_URL,
        SIMILARITY_THRESHOLD,
        validate_config,
    )

    print("\n" + "=" * 60)
    print(f"  📄  {APP_TITLE}  v{APP_VERSION}")
    print("=" * 60)

    # Key readiness
    def _key_status(key: str, prefix: str, label: str) -> None:
        if not key:
            print(f"  ✗  {label:<22} NOT SET")
        elif not key.startswith(prefix):
            print(f"  ?  {label:<22} SET (unexpected format)")
        else:
            masked = key[:8] + "…" + key[-4:]
            print(f"  ✓  {label:<22} {masked}")

    _key_status(OPENROUTER_API_KEY,  "sk-or-", "OpenRouter key")
    _key_status(HUGGINGFACE_API_KEY, "hf_",    "HuggingFace key")
    print(f"     {'OpenRouter model':<22} {OPENROUTER_MODEL}")
    print(f"     {'HuggingFace model':<22} {HUGGINGFACE_MODEL}")
    print(f"     {'HF base URL':<22} {HUGGINGFACE_BASE_URL}")
    print(f"     {'Similarity threshold':<22} {SIMILARITY_THRESHOLD}")
    print(f"     {'.env path':<22} {_env}  ({'found' if _env.exists() else 'MISSING'})")

    issues = validate_config()
    print("-" * 60)
    if issues:
        for w in issues:
            print(f"  ⚠   {w}")
    else:
        print("  ✓  All configuration checks passed")
    print("=" * 60)
    print("  Starting at → http://localhost:8501")
    print("=" * 60 + "\n")


def main() -> None:
    _banner()

    app_path = Path(__file__).parent / "app" / "main.py"
    cmd = [
        sys.executable, "-m", "streamlit", "run", str(app_path),
        "--server.headless",          "true",
        "--server.address",           "0.0.0.0",
        "--browser.gatherUsageStats", "false",
        "--theme.base",               "light",
    ]
    subprocess.run(cmd)


if __name__ == "__main__":
    main()
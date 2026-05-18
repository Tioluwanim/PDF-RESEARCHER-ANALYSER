"""
streamlit_app.py - Entrypoint for Streamlit Cloud deployment.

Sets critical env vars BEFORE any transformers/torch import to prevent
Streamlit's file watcher from flooding logs with torchvision errors.
"""
import os
import sys
from pathlib import Path

# ── Silence noisy libraries before any import ─────────────────────────────────
os.environ.setdefault("TOKENIZERS_PARALLELISM",        "false")
os.environ.setdefault("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1")
os.environ.setdefault("TRANSFORMERS_VERBOSITY",        "error")
os.environ.setdefault("TRANSFORMERS_NO_ADVISORY_WARNINGS", "1")
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
# Tell transformers not to scan vision models (prevents torchvision import)
os.environ.setdefault("TRANSFORMERS_OFFLINE", "0")
# Provide a lightweight dummy for torchvision if it's not installed so
# Streamlit's file-watcher or transformers optional imports won't crash startup.
try:
    import torchvision  # type: ignore
except Exception:
    import types
    _tv = types.ModuleType("torchvision")
    # common submodules referenced by transformers/vision
    for sub in ("transforms", "models", "io", "ops", "datasets"):
        setattr(_tv, sub, types.ModuleType(f"torchvision.{sub}"))
        # also register in sys.modules so `import torchvision.transforms` works
        sys.modules[f"torchvision.{sub}"] = getattr(_tv, sub)
    sys.modules["torchvision"] = _tv

ROOT = Path(__file__).parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import main  # noqa: E402

main()
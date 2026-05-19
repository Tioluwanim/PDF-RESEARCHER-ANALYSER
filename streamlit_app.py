"""
streamlit_app.py - Entrypoint for Streamlit Cloud deployment.

Sets critical env vars BEFORE any transformers/torch import to prevent
Streamlit's file watcher from flooding logs with torchvision errors.
"""
import os
import sys
import types
from pathlib import Path

# ── Silence noisy libraries before any import ─────────────────────────────────
os.environ.setdefault("TOKENIZERS_PARALLELISM",            "false")
os.environ.setdefault("HF_HUB_DISABLE_IMPLICIT_TOKEN",    "1")
os.environ.setdefault("TRANSFORMERS_VERBOSITY",            "error")
os.environ.setdefault("TRANSFORMERS_NO_ADVISORY_WARNINGS", "1")
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS",     "1")


def _build_torchvision_shim() -> None:
    """
    Install a lightweight dummy for torchvision when it is not installed.

    The previous shim created plain ModuleType objects, which Python does not
    treat as packages (they have no __path__).  Any code that does:
        from torchvision.transforms.v2 import functional as tvF
        from torchvision.transforms import functional as F
    would raise:
        ModuleNotFoundError: 'torchvision.transforms' is not a package

    Fix: set __path__ = [] on every stub module so Python recognises them as
    namespace packages and allows sub-attribute access without crashing.
    """
    try:
        import torchvision  # type: ignore  # noqa: F401
        return  # real torchvision is available — nothing to do
    except Exception:
        pass

    def _make_pkg(name: str) -> types.ModuleType:
        mod = types.ModuleType(name)
        mod.__path__ = []       # marks it as a package
        mod.__package__ = name
        mod.__spec__ = None
        return mod

    # Root
    _tv = _make_pkg("torchvision")

    # First-level submodules
    first_level = ("transforms", "models", "io", "ops", "datasets")
    for sub in first_level:
        full = f"torchvision.{sub}"
        pkg = _make_pkg(full)
        setattr(_tv, sub, pkg)
        sys.modules[full] = pkg

    # torchvision.transforms needs .v2 and .functional as sub-packages
    _transforms = sys.modules["torchvision.transforms"]

    for sub2 in ("v2", "functional"):
        full2 = f"torchvision.transforms.{sub2}"
        pkg2 = _make_pkg(full2)
        setattr(_transforms, sub2, pkg2)
        sys.modules[full2] = pkg2

    # torchvision.transforms.v2 needs .functional too
    _v2 = sys.modules["torchvision.transforms.v2"]
    _v2_func = _make_pkg("torchvision.transforms.v2.functional")
    _v2.functional = _v2_func
    sys.modules["torchvision.transforms.v2.functional"] = _v2_func

    sys.modules["torchvision"] = _tv


_build_torchvision_shim()

ROOT = Path(__file__).parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import main  # noqa: E402

main()
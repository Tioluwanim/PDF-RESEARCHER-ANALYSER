"""
streamlit_app.py - Entrypoint for Streamlit Cloud deployment.

Sets critical env vars and installs torchvision shims BEFORE any
transformers/torch import to prevent Streamlit's file watcher from
flooding logs with ModuleNotFoundError spam.
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
    Install a comprehensive dummy for torchvision when it is not installed.

    Streamlit's file-watcher introspects every module that transformers exposes,
    which triggers deep imports of vision models.  Without torchvision those
    imports raise:
        ModuleNotFoundError: 'torchvision.transforms' is not a package
        ImportError: cannot import name 'read_image' from 'torchvision.io'

    Fix:
    - Every stub gets __path__ = [] so Python treats it as a package.
    - Attribute stubs (read_image, functional, etc.) are installed as
      no-op callables so that `from torchvision.io import read_image` works
      without crashing.
    """
    try:
        import torchvision  # type: ignore  # noqa: F401
        return  # Real torchvision is present — nothing to do.
    except Exception:
        pass

    def _make_pkg(name: str) -> types.ModuleType:
        mod = types.ModuleType(name)
        mod.__path__ = []        # marks it as a package
        mod.__package__ = name
        mod.__spec__ = None
        return mod

    def _noop(*_a, **_kw):
        return None

    # ── Root ──────────────────────────────────────────────────────────────────
    _tv = _make_pkg("torchvision")

    # ── First-level sub-packages ──────────────────────────────────────────────
    for sub in ("transforms", "models", "io", "ops", "datasets", "utils"):
        full = f"torchvision.{sub}"
        pkg = _make_pkg(full)
        setattr(_tv, sub, pkg)
        sys.modules[full] = pkg

    # ── torchvision.io — stub out all commonly-imported symbols ───────────────
    _io = sys.modules["torchvision.io"]
    for sym in ("read_image", "write_jpeg", "write_png", "decode_image",
                "encode_jpeg", "encode_png", "read_video", "write_video",
                "ImageReadMode"):
        setattr(_io, sym, _noop)

    # ── torchvision.transforms — needs .v2 and .functional ───────────────────
    _transforms = sys.modules["torchvision.transforms"]
    for sub2 in ("v2", "functional"):
        full2 = f"torchvision.transforms.{sub2}"
        pkg2 = _make_pkg(full2)
        setattr(_transforms, sub2, pkg2)
        sys.modules[full2] = pkg2

    # ── torchvision.transforms.v2 — needs its own .functional ────────────────
    _v2 = sys.modules["torchvision.transforms.v2"]
    _v2_func = _make_pkg("torchvision.transforms.v2.functional")
    _v2.functional = _v2_func
    sys.modules["torchvision.transforms.v2.functional"] = _v2_func

    # ── torchvision.ops — stub common symbols ─────────────────────────────────
    _ops = sys.modules["torchvision.ops"]
    for sym in ("nms", "roi_align", "roi_pool", "box_iou",
                "generalized_box_iou", "clip_boxes_to_image"):
        setattr(_ops, sym, _noop)

    sys.modules["torchvision"] = _tv


_build_torchvision_shim()

ROOT = Path(__file__).parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import main  # noqa: E402

main()

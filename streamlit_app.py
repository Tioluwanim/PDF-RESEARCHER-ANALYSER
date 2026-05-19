"""
streamlit_app.py - Entrypoint for Streamlit Cloud deployment.

Installs a sys.meta_path shim for torchvision BEFORE any other import
so that Streamlit's file-watcher thread never sees a real import failure,
regardless of when it scans sys.modules.

The previous approach (pre-populating sys.modules) was insufficient because:
  - Streamlit's file-watcher runs in a background thread.
  - It calls hasattr() on transformers lazy-loaded modules, which triggers
    importlib.import_module() inside transformers.__getattr__.
  - Those real-module files do e.g.:
        from torchvision.transforms.v2 import functional as tvF
        from torchvision.io import read_image
  - If the sys.modules entry for 'torchvision.transforms' exists but lacks
    a proper __spec__.submodule_search_locations, CPython's _bootstrap raises:
        "torchvision.transforms is not a package"
  - A sys.meta_path finder intercepts at the import-machinery level, so it
    works in every thread and survives any sys.modules eviction by Streamlit.
"""
import os
import sys
import types
import importlib.abc
import importlib.machinery
from pathlib import Path

# ── Silence noisy libraries ────────────────────────────────────────────────────
os.environ.setdefault("TOKENIZERS_PARALLELISM",            "false")
os.environ.setdefault("HF_HUB_DISABLE_IMPLICIT_TOKEN",    "1")
os.environ.setdefault("TRANSFORMERS_VERBOSITY",            "error")
os.environ.setdefault("TRANSFORMERS_NO_ADVISORY_WARNINGS", "1")
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS",     "1")


class _TorchvisionShimFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    """
    A sys.meta_path finder+loader that intercepts every import of
    ``torchvision`` or any ``torchvision.*`` sub-package and returns a
    lightweight stub module instead of raising ModuleNotFoundError.

    Design:
    - Acts as both a Finder (find_spec) and a Loader (create_module/exec_module)
      so no real file is ever touched.
    - Every stub has __path__ = [] and a proper ModuleSpec with
      submodule_search_locations = [], which satisfies CPython's _bootstrap
      check that the parent "is a package".
    - Known imported names (read_image, functional, etc.) are pre-installed
      as no-op callables so `from torchvision.io import read_image` resolves
      to an attribute rather than triggering another sub-import.
    - Thread-safe: create_module checks sys.modules first under the GIL.
    """

    # Symbols that transformers (and other libs) import FROM these packages.
    # Pre-installing them as attrs prevents a second finder lookup.
    _KNOWN_ATTRS: dict[str, list[str]] = {
        "torchvision.io": [
            "read_image", "decode_image", "encode_jpeg", "encode_png",
            "write_jpeg", "write_png", "read_video", "write_video",
            "ImageReadMode",
        ],
        "torchvision.ops": [
            "nms", "roi_align", "roi_pool", "box_iou",
            "generalized_box_iou", "clip_boxes_to_image",
            "batched_nms", "remove_small_boxes",
        ],
        "torchvision.transforms.functional": [
            "normalize", "resize", "crop", "center_crop", "to_tensor",
            "to_pil_image", "hflip", "vflip", "rotate", "pad",
        ],
        "torchvision.transforms.v2.functional": [
            "normalize", "resize", "crop", "center_crop", "to_image",
            "to_dtype", "hflip", "vflip", "rotate", "pad",
        ],
    }

    # No-op sentinel used for every stubbed callable
    @staticmethod
    def _noop(*_a, **_kw):
        return None

    def find_spec(
        self,
        fullname: str,
        path,
        target=None,
    ) -> "importlib.machinery.ModuleSpec | None":
        if fullname == "torchvision" or fullname.startswith("torchvision."):
            spec = importlib.machinery.ModuleSpec(
                fullname,
                self,
                is_package=True,
            )
            # submodule_search_locations is what CPython checks to decide
            # "is this a package" — must be a list (even empty).
            spec.submodule_search_locations = []
            return spec
        return None

    def create_module(
        self, spec: "importlib.machinery.ModuleSpec"
    ) -> "types.ModuleType":
        # Return cached module if already built (thread-safe via GIL dict lookup)
        if spec.name in sys.modules:
            return sys.modules[spec.name]  # type: ignore[return-value]

        mod = types.ModuleType(spec.name)
        mod.__path__ = []                                    # package marker
        mod.__package__ = spec.name.rpartition(".")[0] or spec.name
        mod.__spec__ = spec
        mod.__loader__ = self  # type: ignore[assignment]
        mod.__file__ = None    # type: ignore[assignment]   # no real file

        # Pre-install known symbols so `from x import y` resolves as attr
        for attr in self._KNOWN_ATTRS.get(spec.name, []):
            setattr(mod, attr, self._noop)

        sys.modules[spec.name] = mod
        return mod

    def exec_module(self, module: "types.ModuleType") -> None:
        pass  # nothing to execute — the stub is fully built in create_module


def _install_torchvision_shim() -> None:
    """Install the shim only when torchvision is not already available."""
    try:
        import torchvision  # type: ignore  # noqa: F401
        return  # Real torchvision is installed — nothing to do.
    except Exception:
        pass

    # Guard: don't double-install if the shim is already in sys.meta_path
    for finder in sys.meta_path:
        if type(finder).__name__ == "_TorchvisionShimFinder":
            return

    sys.meta_path.insert(0, _TorchvisionShimFinder())


_install_torchvision_shim()

# ── Ensure project root is on sys.path ────────────────────────────────────────
ROOT = Path(__file__).parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import main  # noqa: E402

main()

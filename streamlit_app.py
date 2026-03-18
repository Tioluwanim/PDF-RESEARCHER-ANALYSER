"""
streamlit_app.py - Entrypoint for Streamlit Cloud deployment.

Sits at the repo root so that 'app' is importable as a package.
Streamlit Cloud should point to THIS file, not app/main.py.
"""

import sys
from pathlib import Path

# ── Ensure repo root is on sys.path ───────────────────────────────────────────
ROOT = Path(__file__).parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── Clear any stale 'app' entries from sys.modules ────────────────────────────
# Streamlit reruns the script on every interaction. If a previous run left
# partial module entries (e.g. 'app' but not 'app.services'), Python raises
# KeyError: 'app.services' when trying to import submodules.
# Removing all 'app.*' keys forces a clean reimport each time.
stale = [k for k in sys.modules if k == "app" or k.startswith("app.")]
for k in stale:
    del sys.modules[k]

# ── Import and run ────────────────────────────────────────────────────────────
from app.main import main  # noqa: E402

main()
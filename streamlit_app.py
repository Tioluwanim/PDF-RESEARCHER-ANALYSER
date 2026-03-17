"""
streamlit_app.py - Entrypoint for Streamlit Cloud deployment.

Sits at the repo root so that 'app' is importable as a package.
Streamlit Cloud should point to THIS file, not app/main.py.
"""

import sys
from pathlib import Path

# Ensure repo root is on sys.path so 'app' package is found
ROOT = Path(__file__).parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Now import and run the app
from app.main import main  # noqa: E402

main()
from __future__ import annotations

"""
app/db/base.py — Single SQLAlchemy declarative Base.

On Streamlit Cloud the script is re-executed on every rerun, which causes
`declarative_base()` to produce a new MetaData object each time.  If the
engine's connection pool is still alive from the previous run, SQLAlchemy
sees the tables already registered and raises:

    InvalidRequestError: Table 'documents' is already defined for this
    MetaData instance.

Fix: store the Base (and its MetaData) in sys.modules so it is truly
created only once per Python process lifetime, regardless of how many
times Streamlit re-executes the script.
"""

import sys

_SENTINEL = "__libraryiq_sa_base__"

if _SENTINEL not in sys.modules:
    from sqlalchemy.orm import declarative_base

    _base = declarative_base()
    # Stash in sys.modules under a private key so Streamlit reruns reuse it
    sys.modules[_SENTINEL] = _base  # type: ignore[assignment]

Base = sys.modules[_SENTINEL]  # type: ignore[assignment]
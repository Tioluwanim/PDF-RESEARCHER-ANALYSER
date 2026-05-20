"""
app/db/base.py — Single SQLAlchemy declarative Base.

Simple, no sentinel tricks. The mapper re-registration problem is
solved in models.py via extend_existing=True on every table and by
importing models before any query is executed.
"""
from __future__ import annotations

from sqlalchemy.orm import declarative_base

Base = declarative_base()
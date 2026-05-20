"""merge multiple heads

Revision ID: d4c742e572d9
Revises: 20260520_0002, 91f4f29d0f4a
Create Date: 2026-05-20 11:44:20.584403+00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = 'd4c742e572d9'
down_revision = ('20260520_0002', '91f4f29d0f4a')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

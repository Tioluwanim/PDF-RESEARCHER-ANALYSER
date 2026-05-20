"""add updated_files to sync_runs

Revision ID: 20260520_0002
Revises: 1ccffca356d9
Create Date: 2026-05-20
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "20260520_0002"
down_revision = "1ccffca356d9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)
    columns = [column["name"] for column in inspector.get_columns("sync_runs")]
    if "updated_files" in columns:
        return

    if conn.dialect.name == "postgresql":
        op.execute(
            "ALTER TABLE sync_runs ADD COLUMN IF NOT EXISTS updated_files INTEGER DEFAULT 0 NOT NULL"
        )
    else:
        op.add_column(
            "sync_runs",
            sa.Column("updated_files", sa.Integer(), nullable=False, server_default="0"),
        )
        op.alter_column("sync_runs", "updated_files", server_default=None)


def downgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)
    columns = [column["name"] for column in inspector.get_columns("sync_runs")]
    if "updated_files" in columns:
        op.drop_column("sync_runs", "updated_files")

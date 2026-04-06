"""add sync_watermark column to service_connections

Revision ID: e2f3a4b5c6d7
Revises: d1a2b3c4e5f6
Create Date: 2026-04-06

"""

from __future__ import annotations

from alembic import op

import sqlalchemy as sa

revision: str = "e2f3a4b5c6d7"
down_revision: str | None = "d1a2b3c4e5f6"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    op.add_column(
        "service_connections",
        sa.Column("sync_watermark", sa.JSON, nullable=False, server_default="{}"),
    )


def downgrade() -> None:
    op.drop_column("service_connections", "sync_watermark")

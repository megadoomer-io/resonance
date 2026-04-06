"""add timezone column to users table

Revision ID: d1a2b3c4e5f6
Revises: c4d5e6f7a890
Create Date: 2026-04-06

"""

from __future__ import annotations

from alembic import op

import sqlalchemy as sa

revision: str = "d1a2b3c4e5f6"
down_revision: str | None = "c4d5e6f7a890"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    op.add_column("users", sa.Column("timezone", sa.String(63), nullable=True))


def downgrade() -> None:
    op.drop_column("users", "timezone")

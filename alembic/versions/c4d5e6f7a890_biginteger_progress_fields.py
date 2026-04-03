"""change progress fields to biginteger

Revision ID: c4d5e6f7a890
Revises: b8e2f4a1c307
Create Date: 2026-04-03

"""

from __future__ import annotations

from alembic import op

import sqlalchemy as sa

revision: str = "c4d5e6f7a890"
down_revision: str | None = "b8e2f4a1c307"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.alter_column(
        "sync_tasks",
        "progress_current",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=False,
    )
    op.alter_column(
        "sync_tasks",
        "progress_total",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "sync_tasks",
        "progress_total",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=True,
    )
    op.alter_column(
        "sync_tasks",
        "progress_current",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=False,
    )

"""add deferred status, description and deferred_until columns

Revision ID: a3f7c1d9e204
Revises: 6ac8ba60ca95
Create Date: 2026-04-03

"""

from __future__ import annotations

from alembic import op

import sqlalchemy as sa

revision: str = "a3f7c1d9e204"
down_revision: str | None = "6ac8ba60ca95"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column("sync_tasks", sa.Column("description", sa.Text, nullable=True))
    op.add_column(
        "sync_tasks",
        sa.Column("deferred_until", sa.DateTime(timezone=True), nullable=True),
    )

    # Add DEFERRED to the status CHECK constraint.
    # SQLAlchemy stores StrEnum .name (uppercase), not .value (lowercase).
    op.drop_constraint("ck_sync_tasks_status", "sync_tasks", type_="check")
    op.create_check_constraint(
        "ck_sync_tasks_status",
        "sync_tasks",
        "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'DEFERRED')",
    )


def downgrade() -> None:
    # Restore status constraint without DEFERRED
    op.drop_constraint("ck_sync_tasks_status", "sync_tasks", type_="check")
    op.create_check_constraint(
        "ck_sync_tasks_status",
        "sync_tasks",
        "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')",
    )

    op.drop_column("sync_tasks", "deferred_until")
    op.drop_column("sync_tasks", "description")

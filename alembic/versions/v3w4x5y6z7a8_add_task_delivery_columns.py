"""add enqueued_at and arq_job_id to sync_tasks

Records each task's current arq delivery so the reaper can tell a lost job
from one arq still holds, and so cancel can abort the exact job.

Revision ID: v3w4x5y6z7a8
Revises: u2v3w4x5y6z7
Create Date: 2026-11-05

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "v3w4x5y6z7a8"
down_revision: str = "u2v3w4x5y6z7"
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    op.add_column(
        "sync_tasks",
        sa.Column("enqueued_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "sync_tasks",
        sa.Column("arq_job_id", sa.String(255), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("sync_tasks", "arq_job_id")
    op.drop_column("sync_tasks", "enqueued_at")

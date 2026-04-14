"""make user_id/service_connection_id nullable, add bulk_job task type

Revision ID: i6d7e8f9g0h1
Revises: h5c6d7e8f9g0
Create Date: 2026-04-14

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "i6d7e8f9g0h1"
down_revision: str | None = "h5c6d7e8f9g0"


def upgrade() -> None:
    # Make user_id and service_connection_id nullable for bulk tasks
    op.alter_column("sync_tasks", "user_id", existing_type=sa.Uuid(), nullable=True)
    op.alter_column(
        "sync_tasks", "service_connection_id", existing_type=sa.Uuid(), nullable=True
    )

    # Update the task_type CHECK constraint to include bulk_job.
    # SQLAlchemy uses native_enum=False so it's a VARCHAR with a CHECK.
    op.execute(
        "ALTER TABLE sync_tasks DROP CONSTRAINT IF EXISTS "
        '"ck_sync_tasks_task_type"'
    )
    op.execute(
        "ALTER TABLE sync_tasks ADD CONSTRAINT "
        '"ck_sync_tasks_task_type" '
        "CHECK (task_type IN ('sync_job', 'time_range', 'page_fetch', 'bulk_job'))"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE sync_tasks DROP CONSTRAINT IF EXISTS "
        '"ck_sync_tasks_task_type"'
    )
    op.execute(
        "ALTER TABLE sync_tasks ADD CONSTRAINT "
        '"ck_sync_tasks_task_type" '
        "CHECK (task_type IN ('sync_job', 'time_range', 'page_fetch'))"
    )
    op.alter_column(
        "sync_tasks", "service_connection_id", existing_type=sa.Uuid(), nullable=False
    )
    op.alter_column("sync_tasks", "user_id", existing_type=sa.Uuid(), nullable=False)

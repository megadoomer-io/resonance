"""add sync_tasks enum check constraints

Revision ID: 6ac8ba60ca95
Revises: 9c0e85015110
Create Date: 2026-04-02

"""

from __future__ import annotations

from alembic import op

revision: str = "6ac8ba60ca95"
down_revision: str | None = "9c0e85015110"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_sync_tasks_status",
        "sync_tasks",
        "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')",
    )
    op.create_check_constraint(
        "ck_sync_tasks_task_type",
        "sync_tasks",
        "task_type IN ('SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_sync_tasks_task_type", "sync_tasks", type_="check")
    op.drop_constraint("ck_sync_tasks_status", "sync_tasks", type_="check")

"""cancel stale running/pending sync tasks

Mark any CALENDAR_SYNC tasks stuck in running or pending as failed
so the UI no longer shows a stale "Syncing..." spinner.

Revision ID: m0h1i2j3k4l5
Revises: l9g0h1i2j3k4
Create Date: 2026-04-22

"""

from __future__ import annotations

from alembic import op

revision: str = "m0h1i2j3k4l5"
down_revision: str | None = "l9g0h1i2j3k4"


def upgrade() -> None:
    op.execute(
        "UPDATE sync_tasks "
        "SET status = 'failed', "
        "    error_message = 'Cancelled: stale task after data purge', "
        "    completed_at = NOW() "
        "WHERE status IN ('running', 'pending') "
        "AND task_type = 'calendar_sync'"
    )


def downgrade() -> None:
    # Data-only migration — cannot restore previous state
    pass

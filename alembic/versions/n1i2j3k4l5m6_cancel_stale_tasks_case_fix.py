"""cancel stale sync tasks (case-insensitive)

Previous migration used lowercase enum values but the DB CHECK constraint
and stored values use uppercase names. Use UPPER() to match regardless of
case convention.

Revision ID: n1i2j3k4l5m6
Revises: m0h1i2j3k4l5
Create Date: 2026-04-22

"""

from __future__ import annotations

from alembic import op

revision: str = "n1i2j3k4l5m6"
down_revision: str | None = "m0h1i2j3k4l5"


def upgrade() -> None:
    op.execute(
        "UPDATE sync_tasks "
        "SET status = 'FAILED', "
        "    error_message = 'Cancelled: stale task after data purge', "
        "    completed_at = NOW() "
        "WHERE status IN ('RUNNING', 'PENDING') "
        "AND task_type = 'CALENDAR_SYNC'"
    )


def downgrade() -> None:
    # Data-only migration — cannot restore previous state
    pass

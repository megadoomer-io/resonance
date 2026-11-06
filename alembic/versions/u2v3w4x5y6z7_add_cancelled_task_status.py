"""add CANCELLED to the sync_tasks status CHECK and migrate legacy cancel rows

Cancellation used to be stored as status FAILED with "Cancelled" in
error_message (user cancels: "Cancelled by user"; stale-task purges:
"Cancelled: stale task after data purge"). Children failed because their
parent was cancelled carry "Parent task was cancelled". All of these become
the real CANCELLED status.

The CHECK is widened before any row is rewritten. ``status`` is VARCHAR(9),
which already fits 'CANCELLED'.

Revision ID: u2v3w4x5y6z7
Revises: t1u2v3w4x5y6
Create Date: 2026-11-05

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "u2v3w4x5y6z7"
down_revision: str = "t1u2v3w4x5y6"
branch_labels: None = None
depends_on: None = None

_STATUSES = "'PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'DEFERRED', 'CANCELLED'"
_STATUSES_WITHOUT_CANCELLED = "'PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'DEFERRED'"

# Matches every message the legacy convention wrote: "Cancelled ..." and
# "Parent task was cancelled".
_LEGACY_CANCEL_PREDICATE = (
    "status = 'FAILED' AND error_message IS NOT NULL "
    "AND (error_message LIKE '%Cancelled%' "
    "OR error_message = 'Parent task was cancelled')"
)


def _replace_status_check(statuses: str) -> None:
    op.execute(
        sa.text('ALTER TABLE sync_tasks DROP CONSTRAINT IF EXISTS "ck_sync_tasks_status"')
    )
    op.execute(
        sa.text(
            'ALTER TABLE sync_tasks ADD CONSTRAINT "ck_sync_tasks_status" '
            f"CHECK (status IN ({statuses}))"
        )
    )


def upgrade() -> None:
    _replace_status_check(_STATUSES)
    op.execute(
        sa.text(
            f"UPDATE sync_tasks SET status = 'CANCELLED' WHERE {_LEGACY_CANCEL_PREDICATE}"
        )
    )


def downgrade() -> None:
    # The legacy convention needs "Cancelled" in error_message for is_cancelled
    # to see it; keep an existing message that already says so.
    op.execute(
        sa.text(
            "UPDATE sync_tasks SET status = 'FAILED', "
            "error_message = CASE "
            "WHEN error_message LIKE '%Cancelled%' "
            "OR error_message = 'Parent task was cancelled' THEN error_message "
            "ELSE 'Cancelled: ' || COALESCE(error_message, 'no reason recorded') END "
            "WHERE status = 'CANCELLED'"
        )
    )
    _replace_status_check(_STATUSES_WITHOUT_CANCELLED)

"""add a partial index on sync_tasks for the reaper scan

Built CONCURRENTLY so the init-container migration doesn't block task writes.
CONCURRENTLY waits for every older transaction, and an old worker can hold one
for a whole job during a rolling deploy, so the wait is bounded by
lock_timeout. A deploy may then fail the init container and retry once; old
pods keep serving meanwhile. A failed build leaves an INVALID index that IF
NOT EXISTS would skip on the retry, so an invalid index of the same name is
dropped first.

Revision ID: w4x5y6z7a8b9
Revises: v3w4x5y6z7a8
Create Date: 2026-11-05

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "w4x5y6z7a8b9"
down_revision: str = "v3w4x5y6z7a8"
branch_labels: None = None
depends_on: None = None

_INDEX = "ix_sync_tasks_reaper_candidates"
# The build itself takes well under a second at this table's size; the time is
# spent waiting on older transactions. Long enough to ride out ordinary
# requests, short enough that a retry is cheap.
_LOCK_TIMEOUT = "30s"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f"SET lock_timeout = '{_LOCK_TIMEOUT}'"))
        try:
            _build_index()
        finally:
            op.execute(sa.text("RESET lock_timeout"))


def _build_index() -> None:
    invalid = op.get_bind().execute(
        sa.text(
            "SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE c.relname = :name AND NOT i.indisvalid"
        ),
        {"name": _INDEX},
    )
    if invalid.first() is not None:
        op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}"))
    op.execute(
        sa.text(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_INDEX} "
            "ON sync_tasks (status) "
            "WHERE status IN ('PENDING', 'DEFERRED', 'RUNNING')"
        )
    )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}"))

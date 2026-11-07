"""add attempt to sync_tasks

A delivery claims a task by bumping ``attempt``; task writes are applied only
while the row still carries the claimer's attempt. The server default keeps
inserts from code that predates the column working during a rolling deploy.

Adding a column with a constant default is a catalog-only change, but it takes
an ACCESS EXCLUSIVE lock that queues behind any open transaction (an old worker
can hold one for a whole job) and blocks every other task query while it
waits. lock_timeout bounds that; on timeout the init container fails and is
retried while old pods keep serving.

Revision ID: x5y6z7a8b9c0
Revises: w4x5y6z7a8b9
Create Date: 2026-11-06

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "x5y6z7a8b9c0"
down_revision: str = "w4x5y6z7a8b9"
branch_labels: None = None
depends_on: None = None

# Short: every second spent waiting blocks task reads and writes too.
_LOCK_TIMEOUT = "5s"


def upgrade() -> None:
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'"))
    op.add_column(
        "sync_tasks",
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="0"),
    )
    # SET LOCAL lasts until the shared migration transaction ends.
    op.execute(sa.text("RESET lock_timeout"))


def downgrade() -> None:
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'"))
    op.drop_column("sync_tasks", "attempt")
    op.execute(sa.text("RESET lock_timeout"))

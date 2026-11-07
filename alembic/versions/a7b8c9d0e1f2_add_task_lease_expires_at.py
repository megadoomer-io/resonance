"""add lease_expires_at to sync_tasks

A claimed delivery holds a lease on its row that it renews while it runs; the
reaper recovers RUNNING rows whose lease has expired. Nullable with no
default: rows claimed by code that predates the column keep NULL, which the
reaper never recovers.

A nullable column with no default is a catalog-only change, but it takes an
ACCESS EXCLUSIVE lock that queues behind any open transaction and blocks every
other task query while it waits. lock_timeout bounds that, as in the attempt
migration.

Revision ID: a7b8c9d0e1f2
Revises: x5y6z7a8b9c0
Create Date: 2026-11-07

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "a7b8c9d0e1f2"
down_revision: str = "x5y6z7a8b9c0"
branch_labels: None = None
depends_on: None = None

_LOCK_TIMEOUT = "5s"


def upgrade() -> None:
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'"))
    op.add_column(
        "sync_tasks",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(sa.text("RESET lock_timeout"))


def downgrade() -> None:
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'"))
    op.drop_column("sync_tasks", "lease_expires_at")
    op.execute(sa.text("RESET lock_timeout"))

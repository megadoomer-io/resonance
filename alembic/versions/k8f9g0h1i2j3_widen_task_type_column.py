"""widen task_type column for CALENDAR_SYNC value

The task_type column was VARCHAR(10), but CALENDAR_SYNC is 13 chars.

Revision ID: k8f9g0h1i2j3
Revises: j7e8f9g0h1i2
Create Date: 2026-04-21

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "k8f9g0h1i2j3"
down_revision: str | None = "j7e8f9g0h1i2"


def upgrade() -> None:
    op.alter_column(
        "sync_tasks",
        "task_type",
        existing_type=sa.String(10),
        type_=sa.String(20),
    )


def downgrade() -> None:
    op.alter_column(
        "sync_tasks",
        "task_type",
        existing_type=sa.String(20),
        type_=sa.String(10),
    )

"""add sync_tasks table

Revision ID: 1de39a06ff65
Revises: 60444bc8434c
Create Date: 2026-04-01

"""

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "1de39a06ff65"
down_revision: str | Sequence[str] | None = "60444bc8434c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sync_tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("service_connection_id", sa.Uuid(), nullable=False),
        sa.Column("parent_id", sa.Uuid(), nullable=True),
        sa.Column(
            "task_type",
            sa.Enum(
                "SYNC_JOB",
                "TIME_RANGE",
                "PAGE_FETCH",
                name="synctasktype",
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Enum(
                "PENDING",
                "RUNNING",
                "COMPLETED",
                "FAILED",
                name="syncstatus",
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.Column("params", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("progress_current", sa.Integer(), nullable=False),
        sa.Column("progress_total", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["parent_id"], ["sync_tasks.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["service_connection_id"],
            ["service_connections.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_sync_tasks_user_status",
        "sync_tasks",
        ["user_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_sync_tasks_parent_status",
        "sync_tasks",
        ["parent_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_sync_tasks_connection_type_status",
        "sync_tasks",
        ["service_connection_id", "task_type", "status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_sync_tasks_connection_type_status", table_name="sync_tasks"
    )
    op.drop_index("ix_sync_tasks_parent_status", table_name="sync_tasks")
    op.drop_index("ix_sync_tasks_user_status", table_name="sync_tasks")
    op.drop_table("sync_tasks")

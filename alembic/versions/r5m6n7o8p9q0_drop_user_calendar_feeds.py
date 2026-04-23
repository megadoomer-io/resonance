"""drop user_calendar_feeds table and last_used_at column

The user_calendar_feeds table has been fully replaced by the unified
service_connections model.  The last_used_at column on service_connections
is replaced by last_synced_at.

Revision ID: r5m6n7o8p9q0
Revises: q4l5m6n7o8p9
Create Date: 2026-04-22

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "r5m6n7o8p9q0"
down_revision: str = "q4l5m6n7o8p9"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.drop_table("user_calendar_feeds")
    op.drop_column("service_connections", "last_used_at")


def downgrade() -> None:
    # Recreate last_used_at
    op.add_column(
        "service_connections",
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Recreate user_calendar_feeds table (simplified — data is lost)
    op.create_table(
        "user_calendar_feeds",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("feed_type", sa.String(255), nullable=False),
        sa.Column("url", sa.String(2048), nullable=False),
        sa.Column("label", sa.String(256), nullable=True),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "url", name="uq_user_calendar_feeds_user_url"),
    )

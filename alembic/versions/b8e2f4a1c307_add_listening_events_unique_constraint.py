"""add unique constraint on listening_events (user_id, track_id, listened_at)

Deduplicates existing rows (keeping the oldest by id per group) then adds the
constraint so the application can use INSERT ... ON CONFLICT DO NOTHING.

Revision ID: b8e2f4a1c307
Revises: a3f7c1d9e204
Create Date: 2026-04-03

"""

from __future__ import annotations

from alembic import op

revision: str = "b8e2f4a1c307"
down_revision: str | None = "a3f7c1d9e204"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # Delete duplicate listening_events, keeping the row with the smallest id
    # per (user_id, track_id, listened_at) group.
    op.execute(
        """
        DELETE FROM listening_events
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM listening_events
            GROUP BY user_id, track_id, listened_at
        )
        """
    )

    op.create_unique_constraint(
        "uq_listening_events_user_track_time",
        "listening_events",
        ["user_id", "track_id", "listened_at"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_listening_events_user_track_time",
        "listening_events",
        type_="unique",
    )

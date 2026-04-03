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
    # Delete duplicate listening_events, keeping one row per
    # (user_id, track_id, listened_at) group using DISTINCT ON.
    # PostgreSQL UUIDs don't support MIN(), so we use ctid as the tiebreaker.
    op.execute(
        """
        DELETE FROM listening_events
        WHERE ctid NOT IN (
            SELECT DISTINCT ON (user_id, track_id, listened_at) ctid
            FROM listening_events
            ORDER BY user_id, track_id, listened_at, created_at
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

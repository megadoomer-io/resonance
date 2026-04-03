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
    # Delete duplicate listening_events, keeping the earliest-created row
    # per (user_id, track_id, listened_at) group.
    # Uses a targeted CTE to only touch actual duplicate groups, avoiding
    # a full-table scan on what is typically a very large table with few dupes.
    op.execute(
        """
        WITH dupes AS (
            SELECT user_id, track_id, listened_at
            FROM listening_events
            GROUP BY user_id, track_id, listened_at
            HAVING count(*) > 1
        ),
        keep AS (
            SELECT DISTINCT ON (le.user_id, le.track_id, le.listened_at) le.id
            FROM listening_events le
            JOIN dupes d USING (user_id, track_id, listened_at)
            ORDER BY le.user_id, le.track_id, le.listened_at, le.created_at
        )
        DELETE FROM listening_events
        WHERE (user_id, track_id, listened_at) IN (
            SELECT user_id, track_id, listened_at FROM dupes
        )
        AND id NOT IN (SELECT id FROM keep)
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

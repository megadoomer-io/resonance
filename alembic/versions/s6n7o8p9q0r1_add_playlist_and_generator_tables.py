"""add playlist and generator tables

Revision ID: s6n7o8p9q0r1
Revises: r5m6n7o8p9q0
Create Date: 2026-04-27

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "s6n7o8p9q0r1"
down_revision: str = "r5m6n7o8p9q0"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # -- playlists --
    op.create_table(
        "playlists",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=512), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "track_count", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column(
            "is_pinned", sa.Boolean(), nullable=False, server_default="false"
        ),
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
    )

    # -- playlist_tracks --
    op.create_table(
        "playlist_tracks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("playlist_id", sa.Uuid(), nullable=False),
        sa.Column("track_id", sa.Uuid(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
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
        sa.ForeignKeyConstraint(
            ["playlist_id"], ["playlists.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["track_id"], ["tracks.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # -- generator_profiles --
    op.create_table(
        "generator_profiles",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=512), nullable=False),
        sa.Column("generator_type", sa.String(length=50), nullable=False),
        sa.Column("input_references", sa.JSON(), nullable=False),
        sa.Column("parameter_values", sa.JSON(), nullable=False),
        sa.Column("auto_sync_targets", sa.JSON(), nullable=True),
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
        sa.CheckConstraint(
            "generator_type IN ("
            "'CONCERT_PREP', 'ARTIST_DEEP_DIVE', 'REDISCOVERY', "
            "'DISCOGRAPHY', 'PLAYLIST_REFRESH', 'CURATED_MIX')",
            name="ck_generator_profiles_generator_type",
        ),
    )

    # -- generation_records --
    op.create_table(
        "generation_records",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("profile_id", sa.Uuid(), nullable=False),
        sa.Column("playlist_id", sa.Uuid(), nullable=False),
        sa.Column("parameter_snapshot", sa.JSON(), nullable=False),
        sa.Column("freshness_target", sa.Integer(), nullable=True),
        sa.Column("freshness_actual", sa.Float(), nullable=True),
        sa.Column("generation_duration_ms", sa.Integer(), nullable=True),
        sa.Column("track_sources_summary", sa.JSON(), nullable=True),
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
        sa.ForeignKeyConstraint(
            ["profile_id"], ["generator_profiles.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["playlist_id"], ["playlists.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Update sync_tasks task_type CHECK to include playlist generation types
    op.execute(
        sa.text(
            'ALTER TABLE sync_tasks '
            'DROP CONSTRAINT IF EXISTS "ck_sync_tasks_task_type"'
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks ADD CONSTRAINT "
            '"ck_sync_tasks_task_type" '
            "CHECK (task_type IN ("
            "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
            "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
            "'TRACK_DISCOVERY', 'TRACK_SCORING'))"
        )
    )


def downgrade() -> None:
    # Revert task_type CHECK to previous values
    op.execute(
        sa.text(
            'ALTER TABLE sync_tasks '
            'DROP CONSTRAINT IF EXISTS "ck_sync_tasks_task_type"'
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks ADD CONSTRAINT "
            '"ck_sync_tasks_task_type" '
            "CHECK (task_type IN ("
            "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
            "'CALENDAR_SYNC'))"
        )
    )

    # Drop tables in reverse dependency order
    op.drop_table("generation_records")
    op.drop_table("generator_profiles")
    op.drop_table("playlist_tracks")
    op.drop_table("playlists")

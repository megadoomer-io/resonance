"""add indexes to playlist and generator tables

Revision ID: t7o8p9q0r1s2
Revises: s6n7o8p9q0r1
Create Date: 2026-04-28

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "t7o8p9q0r1s2"
down_revision: str = "s6n7o8p9q0r1"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_index("ix_playlists_user_id", "playlists", ["user_id"])
    op.create_index(
        "ix_playlist_tracks_playlist_id", "playlist_tracks", ["playlist_id"]
    )
    op.create_index(
        "ix_generator_profiles_user_id", "generator_profiles", ["user_id"]
    )
    op.create_index(
        "ix_generation_records_profile_id", "generation_records", ["profile_id"]
    )
    op.create_index(
        "ix_generation_records_profile_id_created_at",
        "generation_records",
        ["profile_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_generation_records_profile_id_created_at",
        table_name="generation_records",
    )
    op.drop_index(
        "ix_generation_records_profile_id", table_name="generation_records"
    )
    op.drop_index(
        "ix_generator_profiles_user_id", table_name="generator_profiles"
    )
    op.drop_index(
        "ix_playlist_tracks_playlist_id", table_name="playlist_tracks"
    )
    op.drop_index("ix_playlists_user_id", table_name="playlists")

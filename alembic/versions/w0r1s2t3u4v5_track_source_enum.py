"""convert playlist_tracks.source from String to TrackSource enum

Revision ID: w0r1s2t3u4v5
Revises: v9q0r1s2t3u4
Create Date: 2026-05-07

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "w0r1s2t3u4v5"
down_revision: str = "v9q0r1s2t3u4"
branch_labels: str | None = None
depends_on: str | None = None

_TRACK_SOURCE_VALUES = "'LIBRARY', 'DISCOVERY', 'MANUAL'"


def upgrade() -> None:
    # 1. Convert existing lowercase values to uppercase (enum .name format)
    op.execute(
        sa.text(
            "UPDATE playlist_tracks SET source = UPPER(source) "
            "WHERE source != UPPER(source)"
        )
    )

    # 2. Change column type from VARCHAR(64) to VARCHAR(7) to match enum storage
    #    (longest value is "DISCOVERY" = 9 chars, but sa.Enum computes length
    #    automatically; we use alter_column to change the type)
    op.alter_column(
        "playlist_tracks",
        "source",
        existing_type=sa.String(64),
        type_=sa.String(9),
        existing_nullable=False,
    )

    # 3. Add CHECK constraint for valid TrackSource values
    op.create_check_constraint(
        "ck_playlist_tracks_source_tracksource",
        "playlist_tracks",
        f"source IN ({_TRACK_SOURCE_VALUES})",
    )


def downgrade() -> None:
    # 1. Drop the CHECK constraint
    op.drop_constraint(
        "ck_playlist_tracks_source_tracksource",
        "playlist_tracks",
        type_="check",
    )

    # 2. Restore column type to VARCHAR(64)
    op.alter_column(
        "playlist_tracks",
        "source",
        existing_type=sa.String(9),
        type_=sa.String(64),
        existing_nullable=False,
    )

    # 3. Convert uppercase values back to lowercase
    op.execute(
        sa.text(
            "UPDATE playlist_tracks SET source = LOWER(source) "
            "WHERE source != LOWER(source)"
        )
    )

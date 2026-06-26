"""add generation_records.track_snapshot and playlist_tracks.spotify_synced_at

Two additive, nullable columns for the round-robin + refine cluster:

- ``generation_records.track_snapshot`` (JSON): the ordered track-id list a
  generation produced. Persists per-version track history (the live PlaylistTrack
  rows are replaced in place on regenerate) and is the freshness baseline for the
  next regenerate. Browse/restore UI is deferred -- this is the data model only.
- ``playlist_tracks.spotify_synced_at`` (timestamptz): when the track was last
  confirmed on Spotify by an export; null = not synced. Drives the per-track sync
  badge and the "exclude unsynced" action.

Both nullable with no backfill: existing generations have no track snapshot, and
existing playlist tracks are treated as not-yet-synced until the next export.

Revision ID: q8r9s0t1u2v3
Revises: p7q8r9s0t1u2
Create Date: 2026-06-26

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "q8r9s0t1u2v3"
down_revision: str = "p7q8r9s0t1u2"
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    op.add_column(
        "generation_records",
        sa.Column("track_snapshot", sa.JSON(), nullable=True),
    )
    op.add_column(
        "playlist_tracks",
        sa.Column("spotify_synced_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("playlist_tracks", "spotify_synced_at")
    op.drop_column("generation_records", "track_snapshot")

"""add artist_tags table and artists.genre_attempted_at

#136 genre model, Arc 1. Stores artist genre/folksonomy tags as durable domain
data (NOT a cache), mirroring artist_similarities. Each row records that
``source`` (default MusicBrainz, fetched via the ListenBrainz artist metadata
endpoint) reports ``tag`` with folksonomy ``count``. ``genre_mbid`` is non-NULL
only for canonical MusicBrainz genres, so genre-vs-noise filtering is data-driven
rather than a hand-maintained stoplist. ``fetched_at`` drives refresh-if-old.

Also adds ``artists.genre_attempted_at``: the GENRE_BACKFILL resume key,
mirroring ``mb_attempted_at``. NULL = not yet attempted; NOT NULL with zero
artist_tags rows = attempted, no tags found.

Additive and non-breaking: a new table plus a nullable column. Safe to deploy
ahead of the code that reads them.

Revision ID: r9s0t1u2v3w4
Revises: q8r9s0t1u2v3
Create Date: 2026-07-01

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "r9s0t1u2v3w4"
down_revision: str = "q8r9s0t1u2v3"
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    op.create_table(
        "artist_tags",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("artist_id", sa.Uuid(), nullable=False),
        sa.Column("tag", sa.String(length=256), nullable=False),
        sa.Column("genre_mbid", sa.String(length=64), nullable=True),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
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
        sa.ForeignKeyConstraint(["artist_id"], ["artists.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("artist_id", "tag", name="uq_artist_tags_artist_tag"),
        sa.CheckConstraint("count >= 0", name="ck_artist_tags_count_nonneg"),
    )
    op.add_column(
        "artists",
        sa.Column("genre_attempted_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("artists", "genre_attempted_at")
    op.drop_table("artist_tags")

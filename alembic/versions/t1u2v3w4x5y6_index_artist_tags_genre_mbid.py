"""index artist_tags.genre_mbid for genre browse/filter/stats

#154 Arc 2, Phase 1 (genre-aware discovery/taste). Every P1 query filters or
groups by ``genre_mbid`` (browse the library by genre, top-genres aggregation,
"more like this genre"). The existing ``uq_artist_tags_artist_tag`` composite
btree serves ``artist_id`` lookups via its leftmost prefix but does NOT cover a
bare ``genre_mbid`` predicate, so those P1 queries would seq-scan.

Partial index (``WHERE genre_mbid IS NOT NULL``): only canonical-genre rows carry
a ``genre_mbid``; folksonomy tags are NULL and never queried by it, so the partial
predicate keeps the index small and skips the NULL rows entirely.

Additive and non-breaking: an index add on an existing column. Safe to deploy
ahead of the code that reads it.

Revision ID: t1u2v3w4x5y6
Revises: s0t1u2v3w4x5
Create Date: 2026-07-02

"""

from __future__ import annotations

from alembic import op

revision: str = "t1u2v3w4x5y6"
down_revision: str = "s0t1u2v3w4x5"
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    op.create_index(
        "ix_artist_tags_genre_mbid",
        "artist_tags",
        ["genre_mbid"],
        unique=False,
        postgresql_where="genre_mbid IS NOT NULL",
    )


def downgrade() -> None:
    op.drop_index("ix_artist_tags_genre_mbid", table_name="artist_tags")

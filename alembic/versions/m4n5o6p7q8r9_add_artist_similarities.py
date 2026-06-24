"""add artist_similarities table

#133 stores artist->neighbor similarity edges as durable domain data (NOT a
cache). Each row records that ``connector`` reported ``neighbor_name`` (with
``neighbor_mbid`` when known) as the rank-``rank`` similar artist of
``source_artist_id``. The enrich worker reads stored edges first and falls back
to a live get_similar_artists fetch, recording the result; ``fetched_at`` drives
refresh-if-old (re-fetch + replace), not eviction.

Neighbors are stored by name + MBID (not a FK) because a neighbor may not be an
imported Artist yet. Edges for a (source_artist, connector) pair are replaced
wholesale on refresh; the unique constraint guards against in-batch duplicates.

native_enum=False stores the ServiceType .name (UPPERCASE).

Revision ID: m4n5o6p7q8r9
Revises: l3m4n5o6p7q8
Create Date: 2026-06-24

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "m4n5o6p7q8r9"
down_revision: str = "l3m4n5o6p7q8"
branch_labels: None = None
depends_on: None = None

_connector_enum = sa.Enum(
    "SPOTIFY",
    "LASTFM",
    "LISTENBRAINZ",
    "SONGKICK",
    "BANDSINTOWN",
    "BANDCAMP",
    "SOUNDCLOUD",
    "ICAL",
    "CONCERT_ARCHIVES",
    "GITHUB",
    "MANUAL",
    "TEST",
    name="servicetype",
    native_enum=False,
    # Emit the DB-level CHECK (project convention for native_enum=False enum
    # columns); yields ck_artist_similarities_connector_servicetype.
    create_constraint=True,
)


def upgrade() -> None:
    op.create_table(
        "artist_similarities",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_artist_id", sa.Uuid(), nullable=False),
        sa.Column("connector", _connector_enum, nullable=False),
        sa.Column("neighbor_name", sa.String(length=512), nullable=False),
        sa.Column("neighbor_mbid", sa.String(length=64), nullable=True),
        sa.Column("rank", sa.Integer(), nullable=False),
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
        sa.ForeignKeyConstraint(
            ["source_artist_id"], ["artists.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_artist_id",
            "connector",
            "neighbor_name",
            name="uq_artist_similarities_source_connector_neighbor",
        ),
    )
    op.create_index(
        "ix_artist_similarities_source_connector",
        "artist_similarities",
        ["source_artist_id", "connector"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_artist_similarities_source_connector",
        table_name="artist_similarities",
    )
    op.drop_table("artist_similarities")

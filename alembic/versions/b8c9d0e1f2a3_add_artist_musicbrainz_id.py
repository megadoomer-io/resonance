"""add artists.musicbrainz_id, backfilled from service_links

The normalized artist MBID: service_links.musicbrainz.id, else the legacy flat
service_links.listenbrainz string, trimmed and lowercased (the same rule as
resonance.services.artist_utils.normalized_mbid). The app keeps it in sync on
every service_links write. Not unique yet: the unique index is a separate
migration, applied once no MBID is shared by two artists.

Revision ID: b8c9d0e1f2a3
Revises: a7b8c9d0e1f2
Create Date: 2026-11-09

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "b8c9d0e1f2a3"
down_revision: str = "a7b8c9d0e1f2"
branch_labels: None = None
depends_on: None = None

# json_typeof guards a non-string legacy value ({"id": ...} or null).
_NORMALIZED_MBID_SQL = """
    COALESCE(
        NULLIF(LOWER(TRIM(CASE
            WHEN json_typeof(service_links -> 'musicbrainz' -> 'id') = 'string'
            THEN service_links -> 'musicbrainz' ->> 'id' END)), ''),
        NULLIF(LOWER(TRIM(CASE
            WHEN json_typeof(service_links -> 'listenbrainz') = 'string'
            THEN service_links ->> 'listenbrainz' END)), '')
    )
"""

_BACKFILL_SQL = f"""
    UPDATE artists
    SET musicbrainz_id = {_NORMALIZED_MBID_SQL}
    WHERE {_NORMALIZED_MBID_SQL} IS NOT NULL
"""


def upgrade() -> None:
    # ADD COLUMN needs a brief ACCESS EXCLUSIVE lock; don't queue every artist
    # read behind a long transaction while waiting for it.
    op.execute(sa.text("SET LOCAL lock_timeout = '30s'"))
    op.add_column(
        "artists", sa.Column("musicbrainz_id", sa.String(length=64), nullable=True)
    )
    op.execute(sa.text(_BACKFILL_SQL))


def downgrade() -> None:
    op.drop_column("artists", "musicbrainz_id")

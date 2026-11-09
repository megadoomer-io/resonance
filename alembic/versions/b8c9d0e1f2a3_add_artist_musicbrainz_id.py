"""add artists.musicbrainz_id, generated from service_links

The normalized artist MBID: service_links.musicbrainz.id, else a flat
service_links.musicbrainz string, else the legacy flat service_links.listenbrainz
string, trimmed and lowercased (resonance.services.artist_utils.normalized_mbid).
A STORED generated column, so the database keeps it in sync on every write path.
Not unique yet: the unique index is the next migration, shipped separately.

Adding a stored generated column rewrites the table under ACCESS EXCLUSIVE,
blocking artist reads and writes for the rewrite (well under a second at ~7k
rows). lock_timeout bounds the wait for the lock instead of queueing every
artist query behind a long transaction.

Rolling the app image back past this revision needs `alembic downgrade` first:
the startup schema check refuses a database ahead of the code.

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

# Frozen copy of resonance.models.mbid_sql.postgresql_sql(); json_typeof guards
# non-string values ({"id": ...} or null).
_NORMALIZED_MBID_SQL = """
    COALESCE(
        NULLIF(LOWER(BTRIM(CASE
            WHEN json_typeof(service_links -> 'musicbrainz' -> 'id') = 'string'
            THEN service_links -> 'musicbrainz' ->> 'id' END)), ''),
        NULLIF(LOWER(BTRIM(CASE
            WHEN json_typeof(service_links -> 'musicbrainz') = 'string'
            THEN service_links ->> 'musicbrainz' END)), ''),
        NULLIF(LOWER(BTRIM(CASE
            WHEN json_typeof(service_links -> 'listenbrainz') = 'string'
            THEN service_links ->> 'listenbrainz' END)), '')
    )
"""


def upgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '30s'"))
    op.add_column(
        "artists",
        sa.Column(
            "musicbrainz_id",
            sa.String(length=64),
            sa.Computed(sa.text(_NORMALIZED_MBID_SQL), persisted=True),
        ),
    )


def downgrade() -> None:
    op.drop_column("artists", "musicbrainz_id")

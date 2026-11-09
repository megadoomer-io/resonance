"""make artists.musicbrainz_id unique

Ships only after `resonance-api artists mbid-duplicates` reports none in prod.
Re-syncs the column first (rows written by pods older than the column), then
refuses to start if any MBID is still shared, naming the MBIDs, so nothing is
half-applied. The index is built CONCURRENTLY under lock_timeout, like
w4x5y6z7a8b9; an INVALID index left by a failed build is dropped first.

Revision ID: c9d0e1f2a3b4
Revises: b8c9d0e1f2a3
Create Date: 2026-11-09

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "c9d0e1f2a3b4"
down_revision: str = "b8c9d0e1f2a3"
branch_labels: None = None
depends_on: None = None

INDEX = "uq_artists_musicbrainz_id"
_LOCK_TIMEOUT = "30s"

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
_RESYNC_SQL = f"""
    UPDATE artists
    SET musicbrainz_id = {_NORMALIZED_MBID_SQL}
    WHERE musicbrainz_id IS DISTINCT FROM {_NORMALIZED_MBID_SQL}
"""


class DuplicateMbidError(RuntimeError):
    """Two or more artists share an MBID; merge them before this migration."""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f"SET lock_timeout = '{_LOCK_TIMEOUT}'"))
        try:
            op.execute(sa.text(_RESYNC_SQL))
            _refuse_duplicates()
            _build_index()
        finally:
            op.execute(sa.text("RESET lock_timeout"))


def _refuse_duplicates() -> None:
    rows = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT musicbrainz_id, COUNT(*) FROM artists "
                "WHERE musicbrainz_id IS NOT NULL "
                "GROUP BY musicbrainz_id HAVING COUNT(*) > 1 "
                "ORDER BY musicbrainz_id LIMIT 20"
            )
        )
        .all()
    )
    if rows:
        shared = ", ".join(f"{mbid} ({count})" for mbid, count in rows)
        raise DuplicateMbidError(
            f"artists share a MusicBrainz MBID: {shared}. Run "
            "'resonance-api dedup artists', then retry the deploy."
        )


def _build_index() -> None:
    invalid = op.get_bind().execute(
        sa.text(
            "SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE c.relname = :name AND NOT i.indisvalid"
        ),
        {"name": INDEX},
    )
    if invalid.first() is not None:
        op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX}"))
    op.execute(
        sa.text(
            f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {INDEX} "
            "ON artists (musicbrainz_id) WHERE musicbrainz_id IS NOT NULL"
        )
    )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX}"))

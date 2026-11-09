"""make artists.musicbrainz_id unique

Ships in its own deploy, after the column migration and only once
`resonance-api artists mbid-duplicates` reports no groups in prod. Refuses to
start if any MBID is shared, naming the MBIDs, so nothing is half-applied. The
index is built CONCURRENTLY under lock_timeout, like w4x5y6z7a8b9; an INVALID
index left by a failed build is dropped first.

If it refuses, the init container fails and the previous pods keep serving
(they match the column revision), so dedup can still run through the API.

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


class DuplicateMbidError(RuntimeError):
    """Two or more artists share an MBID; merge them before this migration."""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f"SET lock_timeout = '{_LOCK_TIMEOUT}'"))
        try:
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

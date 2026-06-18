"""add MusicBrainz MBID-backfill bookkeeping columns to artists and tracks

Adds mb_attempted_at (resume key; NULL = not yet attempted) and mb_match_status
(matched / no_match / below_similarity) to both tables for #71. The MBID itself
continues to live in service_links["musicbrainz"]["id"]; these columns only track
the outcome of a backfill attempt.

mb_match_status mirrors the MatchStatus StrEnum stored as native_enum=False
(VARCHAR + CHECK). SQLAlchemy persists the enum .name (UPPERCASE), so the CHECK
uses 'MATCHED' / 'NO_MATCH' / 'BELOW_SIMILARITY'.

Revision ID: e6f7g8h9i0j1
Revises: d5e6f7g8h9i0
Create Date: 2026-06-18

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "e6f7g8h9i0j1"
down_revision: str | None = "d5e6f7g8h9i0"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

# Longest value "BELOW_SIMILARITY" is 16 chars.
_MATCH_STATUS_LEN = 16
_MATCH_STATUS_VALUES = "'MATCHED', 'NO_MATCH', 'BELOW_SIMILARITY'"


def upgrade() -> None:
    for table in ("artists", "tracks"):
        op.add_column(
            table,
            sa.Column("mb_attempted_at", sa.DateTime(timezone=True), nullable=True),
        )
        op.add_column(
            table,
            sa.Column("mb_match_status", sa.String(_MATCH_STATUS_LEN), nullable=True),
        )
        op.create_check_constraint(
            f"ck_{table}_mb_match_status",
            table,
            f"mb_match_status IN ({_MATCH_STATUS_VALUES})",
        )


def downgrade() -> None:
    for table in ("artists", "tracks"):
        op.drop_constraint(
            f"ck_{table}_mb_match_status",
            table,
            type_="check",
        )
        op.drop_column(table, "mb_match_status")
        op.drop_column(table, "mb_attempted_at")

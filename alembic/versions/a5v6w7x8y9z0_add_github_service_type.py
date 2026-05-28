"""add GITHUB to ServiceType enum values

Revision ID: a5v6w7x8y9z0
Revises: b2c3d4e5f6g7
Create Date: 2026-05-28

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "a5v6w7x8y9z0"
down_revision: str = "b2c3d4e5f6g7"
branch_labels: str | None = None
depends_on: str | None = None

_TABLES_AND_COLUMNS = [
    ("listening_events", "source_service"),
    ("events", "source_service"),
    ("event_artist_candidates", "source_service"),
    ("event_candidates", "source_service"),
    ("entity_exclusions", "source_service"),
    ("venue_candidates", "source_service"),
    ("user_event_attendance", "source_service"),
    ("user_artist_relations", "source_service"),
    ("user_track_relations", "source_service"),
    ("service_connections", "service_type"),
]

_OLD_VALUES = (
    "'SPOTIFY', 'LASTFM', 'LISTENBRAINZ', 'SONGKICK', "
    "'BANDSINTOWN', 'BANDCAMP', 'SOUNDCLOUD', 'ICAL', "
    "'CONCERT_ARCHIVES', 'MANUAL', 'TEST'"
)
_NEW_VALUES = (
    "'SPOTIFY', 'LASTFM', 'LISTENBRAINZ', 'SONGKICK', "
    "'BANDSINTOWN', 'BANDCAMP', 'SOUNDCLOUD', 'ICAL', "
    "'CONCERT_ARCHIVES', 'GITHUB', 'MANUAL', 'TEST'"
)

_FIND_CONSTRAINTS_SQL = sa.text("""
    SELECT con.conname
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    JOIN pg_attribute att ON att.attrelid = rel.oid
    WHERE con.contype = 'c'
      AND rel.relname = :table_name
      AND nsp.nspname = 'public'
      AND pg_get_constraintdef(con.oid) LIKE '%' || :column_name || '%'
""")


def _replace_constraints(values: str) -> None:
    conn = op.get_bind()
    for table, column in _TABLES_AND_COLUMNS:
        result = conn.execute(
            _FIND_CONSTRAINTS_SQL,
            {"table_name": table, "column_name": column},
        )
        for (constraint_name,) in result:
            conn.execute(
                sa.text(
                    f'ALTER TABLE "{table}" DROP CONSTRAINT'
                    f' IF EXISTS "{constraint_name}"'
                )
            )

        op.create_check_constraint(
            f"ck_{table}_{column}_servicetype",
            table,
            f"{column} IN ({values})",
        )


def upgrade() -> None:
    _replace_constraints(_NEW_VALUES)


def downgrade() -> None:
    _replace_constraints(_OLD_VALUES)

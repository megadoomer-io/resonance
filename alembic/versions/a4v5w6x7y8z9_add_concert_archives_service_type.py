"""add CONCERT_ARCHIVES to ServiceType and CONCERT_ARCHIVES_IMPORT to TaskType

Revision ID: a4v5w6x7y8z9
Revises: a4u5v6w7x8y9
Create Date: 2026-05-19

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "a4v5w6x7y8z9"
down_revision: str = "a4u5v6w7x8y9"
branch_labels: str | None = None
depends_on: str | None = None

# --- ServiceType tables/columns ---

_SERVICE_TABLES_AND_COLUMNS = [
    ("listening_events", "source_service"),
    ("events", "source_service"),
    ("user_event_attendance", "source_service"),
    ("user_artist_relations", "source_service"),
    ("user_track_relations", "source_service"),
    ("service_connections", "service_type"),
]

_OLD_SERVICE_VALUES = (
    "'SPOTIFY', 'LASTFM', 'LISTENBRAINZ', 'SONGKICK', "
    "'BANDSINTOWN', 'BANDCAMP', 'SOUNDCLOUD', 'ICAL', 'MANUAL', 'TEST'"
)
_NEW_SERVICE_VALUES = (
    "'SPOTIFY', 'LASTFM', 'LISTENBRAINZ', 'SONGKICK', "
    "'BANDSINTOWN', 'BANDCAMP', 'SOUNDCLOUD', 'ICAL', "
    "'CONCERT_ARCHIVES', 'MANUAL', 'TEST'"
)

# --- TaskType table/column ---

_TASK_TABLES_AND_COLUMNS = [
    ("sync_tasks", "task_type"),
]

_OLD_TASK_VALUES = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', 'PLAYLIST_EXPORT'"
)
_NEW_TASK_VALUES = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', 'CONCERT_ARCHIVES_IMPORT', "
    "'PLAYLIST_EXPORT'"
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


def _replace_constraints(
    tables_and_columns: list[tuple[str, str]],
    values: str,
    constraint_suffix: str,
) -> None:
    conn = op.get_bind()
    for table, column in tables_and_columns:
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
            f"ck_{table}_{column}_{constraint_suffix}",
            table,
            f"{column} IN ({values})",
        )


def upgrade() -> None:
    # Widen varchar columns to fit CONCERT_ARCHIVES (17 chars) and
    # CONCERT_ARCHIVES_IMPORT (24 chars). Increasing varchar length
    # is a metadata-only change in PostgreSQL — no table rewrite.
    for table, column in _SERVICE_TABLES_AND_COLUMNS:
        op.alter_column(table, column, type_=sa.String(17))
    op.alter_column("sync_tasks", "task_type", type_=sa.String(24))

    _replace_constraints(
        _SERVICE_TABLES_AND_COLUMNS, _NEW_SERVICE_VALUES, "servicetype"
    )
    _replace_constraints(
        _TASK_TABLES_AND_COLUMNS, _NEW_TASK_VALUES, "tasktype"
    )


def downgrade() -> None:
    _replace_constraints(
        _SERVICE_TABLES_AND_COLUMNS, _OLD_SERVICE_VALUES, "servicetype"
    )
    _replace_constraints(
        _TASK_TABLES_AND_COLUMNS, _OLD_TASK_VALUES, "tasktype"
    )

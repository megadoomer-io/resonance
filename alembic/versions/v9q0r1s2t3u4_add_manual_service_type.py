"""add MANUAL to ServiceType enum values

Revision ID: v9q0r1s2t3u4
Revises: u8p9q0r1s2t3
Create Date: 2026-04-29

"""

from __future__ import annotations

from alembic import op

revision: str = "v9q0r1s2t3u4"
down_revision: str = "u8p9q0r1s2t3"
branch_labels: str | None = None
depends_on: str | None = None

_TABLES_AND_COLUMNS = [
    ("listening_events", "source_service"),
    ("events", "source_service"),
    ("user_event_attendance", "source_service"),
    ("user_artist_relations", "source_service"),
    ("user_track_relations", "source_service"),
    ("service_connections", "service_type"),
]

_OLD_VALUES = (
    "'SPOTIFY', 'LASTFM', 'LISTENBRAINZ', 'SONGKICK', "
    "'BANDSINTOWN', 'BANDCAMP', 'SOUNDCLOUD', 'ICAL', 'TEST'"
)
_NEW_VALUES = (
    "'SPOTIFY', 'LASTFM', 'LISTENBRAINZ', 'SONGKICK', "
    "'BANDSINTOWN', 'BANDCAMP', 'SOUNDCLOUD', 'ICAL', 'MANUAL', 'TEST'"
)


def upgrade() -> None:
    for table, column in _TABLES_AND_COLUMNS:
        constraint_name = f"ck_{table}_{column}"
        op.drop_constraint(constraint_name, table, type_="check")
        op.create_check_constraint(
            constraint_name,
            table,
            f"{column} IN ({_NEW_VALUES})",
        )


def downgrade() -> None:
    for table, column in _TABLES_AND_COLUMNS:
        constraint_name = f"ck_{table}_{column}"
        op.drop_constraint(constraint_name, table, type_="check")
        op.create_check_constraint(
            constraint_name,
            table,
            f"{column} IN ({_OLD_VALUES})",
        )

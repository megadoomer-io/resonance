"""add concert tables (venues, events, candidates, attendance, feeds)

Revision ID: j7e8f9g0h1i2
Revises: i6d7e8f9g0h1
Create Date: 2026-04-20

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "j7e8f9g0h1i2"
down_revision: str | None = "i6d7e8f9g0h1"


# ServiceType values including the new ICAL member.
_SERVICE_TYPE_VALUES = (
    "'SPOTIFY', 'LASTFM', 'LISTENBRAINZ', 'SONGKICK', "
    "'BANDSINTOWN', 'BANDCAMP', 'SOUNDCLOUD', 'ICAL', 'TEST'"
)


def upgrade() -> None:
    # -- venues --
    op.create_table(
        "venues",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=512), nullable=False),
        sa.Column("address", sa.String(length=512), nullable=True),
        sa.Column("city", sa.String(length=256), nullable=True),
        sa.Column("state", sa.String(length=256), nullable=True),
        sa.Column("postal_code", sa.String(length=32), nullable=True),
        sa.Column("country", sa.String(length=2), nullable=True),
        sa.Column("service_links", sa.JSON(), nullable=True),
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
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "name", "city", "state", "country", name="uq_venues_name_location"
        ),
    )

    # -- events --
    op.create_table(
        "events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=1024), nullable=False),
        sa.Column("event_date", sa.Date(), nullable=False),
        sa.Column("venue_id", sa.Uuid(), nullable=True),
        sa.Column(
            "source_service",
            sa.Enum(
                "SPOTIFY",
                "LASTFM",
                "LISTENBRAINZ",
                "SONGKICK",
                "BANDSINTOWN",
                "BANDCAMP",
                "SOUNDCLOUD",
                "ICAL",
                "TEST",
                name="servicetype",
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.Column("external_id", sa.String(length=512), nullable=False),
        sa.Column("external_url", sa.String(length=1024), nullable=True),
        sa.Column("service_links", sa.JSON(), nullable=True),
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
        sa.ForeignKeyConstraint(["venue_id"], ["venues.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_service", "external_id", name="uq_events_source_external"
        ),
    )
    op.create_index("ix_events_event_date", "events", ["event_date"])
    op.create_index("ix_events_venue_id", "events", ["venue_id"])

    # -- event_artist_candidates --
    op.create_table(
        "event_artist_candidates",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("raw_name", sa.String(length=512), nullable=False),
        sa.Column("matched_artist_id", sa.Uuid(), nullable=True),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("confidence_score", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "PENDING",
                "ACCEPTED",
                "REJECTED",
                name="candidatestatus",
                native_enum=False,
            ),
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
        sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["matched_artist_id"], ["artists.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "event_id", "raw_name", name="uq_event_artist_candidates_event_name"
        ),
    )

    # -- event_artists --
    op.create_table(
        "event_artists",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("artist_id", sa.Uuid(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("raw_name", sa.String(length=512), nullable=False),
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
        sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["artist_id"], ["artists.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "event_id", "artist_id", name="uq_event_artists_event_artist"
        ),
    )

    # -- user_event_attendance --
    op.create_table(
        "user_event_attendance",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "GOING",
                "INTERESTED",
                "NONE",
                name="attendancestatus",
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.Column(
            "source_service",
            sa.Enum(
                "SPOTIFY",
                "LASTFM",
                "LISTENBRAINZ",
                "SONGKICK",
                "BANDSINTOWN",
                "BANDCAMP",
                "SOUNDCLOUD",
                "ICAL",
                "TEST",
                name="servicetype",
                native_enum=False,
            ),
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
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id", "event_id", name="uq_user_event_attendance_user_event"
        ),
    )

    # -- user_calendar_feeds --
    op.create_table(
        "user_calendar_feeds",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column(
            "feed_type",
            sa.Enum(
                "SONGKICK_ATTENDANCE",
                "SONGKICK_TRACKED_ARTIST",
                "ICAL_GENERIC",
                name="feedtype",
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.Column("url", sa.String(length=2048), nullable=False),
        sa.Column("label", sa.String(length=256), nullable=True),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False),
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
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "url", name="uq_user_calendar_feeds_user_url"),
    )

    # Update sync_tasks task_type CHECK to include CALENDAR_SYNC
    op.execute(
        'ALTER TABLE sync_tasks DROP CONSTRAINT IF EXISTS "ck_sync_tasks_task_type"'
    )
    op.execute(
        "ALTER TABLE sync_tasks ADD CONSTRAINT "
        '"ck_sync_tasks_task_type" '
        "CHECK (task_type IN "
        "('SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', 'CALENDAR_SYNC'))"
    )


def downgrade() -> None:
    # Revert task_type CHECK
    op.execute(
        'ALTER TABLE sync_tasks DROP CONSTRAINT IF EXISTS "ck_sync_tasks_task_type"'
    )
    op.execute(
        "ALTER TABLE sync_tasks ADD CONSTRAINT "
        '"ck_sync_tasks_task_type" '
        "CHECK (task_type IN "
        "('SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB'))"
    )

    op.drop_table("user_calendar_feeds")
    op.drop_table("user_event_attendance")
    op.drop_table("event_artists")
    op.drop_table("event_artist_candidates")
    op.drop_index("ix_events_venue_id", table_name="events")
    op.drop_index("ix_events_event_date", table_name="events")
    op.drop_table("events")
    op.drop_table("venues")

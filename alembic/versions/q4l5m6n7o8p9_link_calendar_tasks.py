"""link calendar sync tasks to unified connection rows

Update sync_tasks.service_connection_id for existing CALENDAR_SYNC tasks
to point to the service_connections rows created in the previous migration.

Revision ID: q4l5m6n7o8p9
Revises: p3k4l5m6n7o8
Create Date: 2026-04-23

"""

from __future__ import annotations

from alembic import op

revision: str = "q4l5m6n7o8p9"
down_revision: str | None = "p3k4l5m6n7o8"


def upgrade() -> None:
    # Link Songkick CALENDAR_SYNC tasks to service_connections.
    # Each task has params->>'feed_id' referencing a user_calendar_feeds row.
    # Extract the Songkick username from the feed URL, then match the
    # service_connections row by (user_id, service_type, external_user_id).
    op.execute("""
        UPDATE sync_tasks st
        SET service_connection_id = sc.id
        FROM user_calendar_feeds ucf
        JOIN service_connections sc ON (
            sc.user_id = ucf.user_id
            AND sc.service_type = 'SONGKICK'
            AND sc.external_user_id = split_part(
                split_part(ucf.url, '/users/', 2), '/', 1
            )
        )
        WHERE st.task_type = 'CALENDAR_SYNC'
        AND st.service_connection_id IS NULL
        AND st.params->>'feed_id' = ucf.id::text
        AND ucf.feed_type IN ('SONGKICK_ATTENDANCE', 'SONGKICK_TRACKED_ARTIST')
    """)

    # Link iCal CALENDAR_SYNC tasks to service_connections.
    # Match by (user_id, service_type='ICAL', url).
    op.execute("""
        UPDATE sync_tasks st
        SET service_connection_id = sc.id
        FROM user_calendar_feeds ucf
        JOIN service_connections sc ON (
            sc.user_id = ucf.user_id
            AND sc.service_type = 'ICAL'
            AND sc.url = ucf.url
        )
        WHERE st.task_type = 'CALENDAR_SYNC'
        AND st.service_connection_id IS NULL
        AND st.params->>'feed_id' = ucf.id::text
        AND ucf.feed_type = 'ICAL_GENERIC'
    """)


def downgrade() -> None:
    # Unlink calendar sync tasks from service_connections.
    op.execute(
        "UPDATE sync_tasks SET service_connection_id = NULL "
        "WHERE task_type = 'CALENDAR_SYNC'"
    )

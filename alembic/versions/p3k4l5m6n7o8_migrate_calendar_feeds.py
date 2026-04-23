"""migrate calendar feed data into service_connections

Copy UserCalendarFeed rows into service_connections:
- Songkick feeds: one row per username (collapse attendance + tracked_artist)
- iCal feeds: one row per URL

Revision ID: p3k4l5m6n7o8
Revises: o2j3k4l5m6n7
Create Date: 2026-04-23

"""

from __future__ import annotations

from alembic import op

revision: str = "p3k4l5m6n7o8"
down_revision: str | None = "o2j3k4l5m6n7"


def upgrade() -> None:
    # Migrate Songkick feeds: one connection per distinct username.
    # Extract username from URL pattern:
    #   https://www.songkick.com/users/{username}/calendars.ics?filter=...
    # Use the earliest created_at and latest last_synced_at from the pair.
    op.execute("""
        INSERT INTO service_connections (
            id, user_id, service_type, external_user_id,
            url, enabled, last_synced_at,
            connected_at, created_at, updated_at, sync_watermark
        )
        SELECT
            gen_random_uuid(),
            user_id,
            'SONGKICK',
            split_part(split_part(url, '/users/', 2), '/', 1),
            NULL,
            bool_and(enabled),
            MAX(last_synced_at),
            MIN(created_at),
            MIN(created_at),
            NOW(),
            '{}'
        FROM user_calendar_feeds
        WHERE feed_type IN ('SONGKICK_ATTENDANCE', 'SONGKICK_TRACKED_ARTIST')
        GROUP BY user_id, split_part(split_part(url, '/users/', 2), '/', 1)
    """)

    # Migrate generic iCal feeds: one connection per URL.
    op.execute("""
        INSERT INTO service_connections (
            id, user_id, service_type, external_user_id,
            url, label, enabled, last_synced_at,
            connected_at, created_at, updated_at, sync_watermark
        )
        SELECT
            gen_random_uuid(),
            user_id,
            'ICAL',
            NULL,
            url,
            label,
            enabled,
            last_synced_at,
            created_at,
            created_at,
            NOW(),
            '{}'
        FROM user_calendar_feeds
        WHERE feed_type = 'ICAL_GENERIC'
    """)


def downgrade() -> None:
    # Remove migrated feed connections (identifiable by having no OAuth token).
    op.execute(
        "DELETE FROM service_connections "
        "WHERE service_type IN ('SONGKICK', 'ICAL') "
        "AND encrypted_access_token IS NULL"
    )

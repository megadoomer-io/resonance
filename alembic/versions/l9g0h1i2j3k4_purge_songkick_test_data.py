"""purge all songkick concert data for fresh sync

Remove all events, artist candidates, event artists, attendance records,
and orphaned venues. Reset last_synced_at on remaining calendar feeds so
the next sync performs a full import.

Revision ID: l9g0h1i2j3k4
Revises: k8f9g0h1i2j3
Create Date: 2026-04-22

"""

from __future__ import annotations

from alembic import op

revision: str = "l9g0h1i2j3k4"
down_revision: str | None = "k8f9g0h1i2j3"


def upgrade() -> None:
    # Child tables cascade on event deletion, but delete attendance
    # explicitly first to be safe across DB engines.
    op.execute("DELETE FROM user_event_attendance")
    op.execute("DELETE FROM event_artists")
    op.execute("DELETE FROM event_artist_candidates")
    op.execute("DELETE FROM events")
    op.execute(
        "DELETE FROM venues WHERE id NOT IN "
        "(SELECT DISTINCT venue_id FROM events WHERE venue_id IS NOT NULL)"
    )
    # Reset sync timestamps so next sync does a full import
    op.execute(
        "UPDATE user_calendar_feeds SET last_synced_at = NULL "
        "WHERE feed_type IN ('songkick_attendance', 'songkick_tracked_artist')"
    )


def downgrade() -> None:
    # Data-only migration — cannot restore deleted rows
    pass

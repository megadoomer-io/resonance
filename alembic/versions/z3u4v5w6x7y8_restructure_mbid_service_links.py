"""restructure MBID in artist service_links

For artists whose service_links.listenbrainz value is a MusicBrainz ID
(UUID format), copy that ID into a new service_links.musicbrainz.id key.
This gives MBIDs their own top-level namespace independent of the
ListenBrainz connector that originally discovered them.

Revision ID: z3u4v5w6x7y8
Revises: y2t3u4v5w6x7
Create Date: 2026-05-11

"""

from __future__ import annotations

from alembic import op

revision: str = "z3u4v5w6x7y8"
down_revision: str | None = "y2t3u4v5w6x7"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    # The service_links column is json (not jsonb). Cast to jsonb for the
    # concatenation operator (||), then cast back to json.
    #
    # Conditions:
    #   - service_links is not NULL
    #   - service_links->>'listenbrainz' is not NULL and not empty
    #   - The value looks like a UUID (length 36, contains hyphens)
    #   - service_links->'musicbrainz' does not already exist (idempotent)
    op.execute(
        """
        UPDATE artists
        SET service_links = (
            service_links::jsonb
            || jsonb_build_object('musicbrainz', jsonb_build_object('id', service_links->>'listenbrainz'))
        )::json
        WHERE service_links IS NOT NULL
          AND service_links->>'listenbrainz' IS NOT NULL
          AND service_links->>'listenbrainz' != ''
          AND length(service_links->>'listenbrainz') = 36
          AND service_links->>'listenbrainz' LIKE '%-%'
          AND service_links->'musicbrainz' IS NULL
        """
    )


def downgrade() -> None:
    # Remove the musicbrainz key from service_links where it exists.
    op.execute(
        """
        UPDATE artists
        SET service_links = (
            service_links::jsonb - 'musicbrainz'
        )::json
        WHERE service_links IS NOT NULL
          AND service_links->'musicbrainz' IS NOT NULL
        """
    )

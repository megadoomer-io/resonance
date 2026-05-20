"""Backfill venue_candidates and event_candidates from existing data.

Every existing Venue gets a VenueCandidate, every existing Event gets an
EventCandidate, all with status AUTO_ACCEPTED. This ensures the system
starts in a consistent state where every entity has provenance.

Revision ID: b2c3d4e5f6g7
Revises: b1a2b3c4d5e6
Create Date: 2026-05-20
"""

from __future__ import annotations

from alembic import op

revision: str = "b2c3d4e5f6g7"
down_revision: str = "b1a2b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Backfill VenueCandidates from existing Venues.
    # Use the venue's first event's source_service, falling back to 'MANUAL'.
    # External ID is the venue's UUID (no external ID exists for venues).
    op.execute(
        """
        INSERT INTO venue_candidates (
            id, source_service, external_id,
            name, city, state, country, address, postal_code,
            resolved_venue_id, status, confidence_score,
            created_at, updated_at
        )
        SELECT
            gen_random_uuid(),
            COALESCE(
                (SELECT e.source_service FROM events e
                 WHERE e.venue_id = v.id LIMIT 1),
                'MANUAL'
            ),
            v.id::text,
            v.name, v.city, v.state, v.country, v.address, v.postal_code,
            v.id,
            'AUTO_ACCEPTED',
            100,
            v.created_at,
            v.updated_at
        FROM venues v
        """
    )

    # Backfill EventCandidates from existing Events.
    # Link to the venue's backfilled VenueCandidate via external_id match.
    op.execute(
        """
        INSERT INTO event_candidates (
            id, source_service, external_id, external_url,
            title, event_date,
            venue_candidate_id, attendance_status,
            resolved_event_id, status, confidence_score,
            created_at, updated_at
        )
        SELECT
            gen_random_uuid(),
            e.source_service,
            e.external_id,
            e.external_url,
            e.title,
            e.event_date,
            (SELECT vc.id FROM venue_candidates vc
             WHERE vc.external_id = e.venue_id::text LIMIT 1),
            (SELECT
                CASE WHEN uea.status = 'GOING' THEN 'going'
                     WHEN uea.status = 'INTERESTED' THEN 'interested'
                     ELSE NULL
                END
             FROM user_event_attendance uea
             WHERE uea.event_id = e.id LIMIT 1),
            e.id,
            'AUTO_ACCEPTED',
            100,
            e.created_at,
            e.updated_at
        FROM events e
        """
    )


def downgrade() -> None:
    op.execute("DELETE FROM event_candidates")
    op.execute("DELETE FROM venue_candidates")

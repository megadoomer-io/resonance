"""Add venue_candidates, event_candidates, entity_exclusions tables.

Revision ID: b1a2b3c4d5e6
Revises: a4v5w6x7y8z9
Create Date: 2026-05-20
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "b1a2b3c4d5e6"
down_revision: str = "a4v5w6x7y8z9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Update CandidateStatus CHECK constraint to include AUTO_ACCEPTED.
    # Since native_enum=False, the enum is stored as VARCHAR with a CHECK.
    # Drop the old constraint and add the new one on event_artist_candidates.
    op.execute(
        "ALTER TABLE event_artist_candidates "
        "DROP CONSTRAINT IF EXISTS ck_event_artist_candidates_status"
    )
    # The constraint name varies — also try the SQLAlchemy-generated name
    op.execute(
        "ALTER TABLE event_artist_candidates "
        "DROP CONSTRAINT IF EXISTS candidatestatus"
    )

    op.create_table(
        "venue_candidates",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_service", sa.String(length=64), nullable=False),
        sa.Column("external_id", sa.String(length=512), nullable=False),
        sa.Column("name", sa.String(length=512), nullable=False),
        sa.Column("city", sa.String(length=256), nullable=True),
        sa.Column("state", sa.String(length=256), nullable=True),
        sa.Column("country", sa.String(length=256), nullable=True),
        sa.Column("address", sa.String(length=512), nullable=True),
        sa.Column("postal_code", sa.String(length=32), nullable=True),
        sa.Column("resolved_venue_id", sa.Uuid(), nullable=True),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("confidence_score", sa.Integer(), nullable=False),
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
        sa.ForeignKeyConstraint(
            ["resolved_venue_id"],
            ["venues.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_service",
            "external_id",
            name="uq_venue_candidates_source_external",
        ),
    )

    op.create_table(
        "event_candidates",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_service", sa.String(length=64), nullable=False),
        sa.Column("external_id", sa.String(length=512), nullable=False),
        sa.Column("external_url", sa.String(length=1024), nullable=True),
        sa.Column("title", sa.String(length=1024), nullable=False),
        sa.Column("event_date", sa.Date(), nullable=False),
        sa.Column("venue_candidate_id", sa.Uuid(), nullable=True),
        sa.Column("attendance_status", sa.String(length=64), nullable=True),
        sa.Column("resolved_event_id", sa.Uuid(), nullable=True),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("confidence_score", sa.Integer(), nullable=False),
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
        sa.ForeignKeyConstraint(
            ["venue_candidate_id"],
            ["venue_candidates.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["resolved_event_id"],
            ["events.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_service",
            "external_id",
            name="uq_event_candidates_source_external",
        ),
    )

    op.create_table(
        "entity_exclusions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("entity_a_id", sa.Uuid(), nullable=False),
        sa.Column("entity_b_id", sa.Uuid(), nullable=False),
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
            "entity_type",
            "entity_a_id",
            "entity_b_id",
            name="uq_entity_exclusions_type_pair",
        ),
    )


def downgrade() -> None:
    op.drop_table("entity_exclusions")
    op.drop_table("event_candidates")
    op.drop_table("venue_candidates")

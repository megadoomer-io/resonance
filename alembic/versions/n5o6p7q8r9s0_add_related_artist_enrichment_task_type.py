"""add RELATED_ARTIST_ENRICHMENT to the sync_tasks task_type CHECK constraint

TaskType.RELATED_ARTIST_ENRICHMENT (#133) was added in code; the sync_tasks
task_type CHECK constraint must list it or inserts of enrich tasks fail with a
check violation (same class of bug fixed for MBID_BACKFILL / POPULARITY_BACKFILL).

SQLAlchemy stores the enum .name (UPPERCASE) for native_enum=False columns, so the
constraint lists 'RELATED_ARTIST_ENRICHMENT'. The test_task_type_constraint guard
asserts the head-most CHECK definer lists exactly the TaskType enum names.

Revision ID: n5o6p7q8r9s0
Revises: m4n5o6p7q8r9
Create Date: 2026-06-24

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "n5o6p7q8r9s0"
down_revision: str = "m4n5o6p7q8r9"
branch_labels: None = None
depends_on: None = None

_ALL_TYPES = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', "
    "'CONCERT_ARCHIVES_IMPORT', 'CONCERT_ARCHIVES_CHUNK', "
    "'PLAYLIST_EXPORT', 'MBID_BACKFILL', 'POPULARITY_BACKFILL', "
    "'RELATED_ARTIST_ENRICHMENT'"
)

_ALL_TYPES_WITHOUT_ENRICHMENT = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', "
    "'CONCERT_ARCHIVES_IMPORT', 'CONCERT_ARCHIVES_CHUNK', "
    "'PLAYLIST_EXPORT', 'MBID_BACKFILL', 'POPULARITY_BACKFILL'"
)


def upgrade() -> None:
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks DROP CONSTRAINT IF EXISTS "
            '"ck_sync_tasks_task_type_tasktype"'
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks ADD CONSTRAINT "
            '"ck_sync_tasks_task_type_tasktype" '
            f"CHECK (task_type IN ({_ALL_TYPES}))"
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks DROP CONSTRAINT IF EXISTS "
            '"ck_sync_tasks_task_type_tasktype"'
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks ADD CONSTRAINT "
            '"ck_sync_tasks_task_type_tasktype" '
            f"CHECK (task_type IN ({_ALL_TYPES_WITHOUT_ENRICHMENT}))"
        )
    )

"""add GENRE_BACKFILL to the sync_tasks task_type CHECK constraint

TaskType.GENRE_BACKFILL (#136 genre model) was added in code; the sync_tasks
task_type CHECK constraint must list it or inserts of genre-backfill tasks fail
with a check violation (same class of bug fixed for MBID_BACKFILL /
POPULARITY_BACKFILL / RELATED_ARTIST_ENRICHMENT).

SQLAlchemy stores the enum .name (UPPERCASE) for native_enum=False columns, so the
constraint lists 'GENRE_BACKFILL'. The test_task_type_constraint guard asserts the
head-most CHECK definer lists exactly the TaskType enum names.

Revision ID: s0t1u2v3w4x5
Revises: r9s0t1u2v3w4
Create Date: 2026-07-01

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "s0t1u2v3w4x5"
down_revision: str = "r9s0t1u2v3w4"
branch_labels: None = None
depends_on: None = None

_ALL_TYPES = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', "
    "'CONCERT_ARCHIVES_IMPORT', 'CONCERT_ARCHIVES_CHUNK', "
    "'PLAYLIST_EXPORT', 'MBID_BACKFILL', 'POPULARITY_BACKFILL', "
    "'RELATED_ARTIST_ENRICHMENT', 'GENRE_BACKFILL'"
)

_ALL_TYPES_WITHOUT_GENRE = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', "
    "'CONCERT_ARCHIVES_IMPORT', 'CONCERT_ARCHIVES_CHUNK', "
    "'PLAYLIST_EXPORT', 'MBID_BACKFILL', 'POPULARITY_BACKFILL', "
    "'RELATED_ARTIST_ENRICHMENT'"
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
            f"CHECK (task_type IN ({_ALL_TYPES_WITHOUT_GENRE}))"
        )
    )

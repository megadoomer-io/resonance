"""add POPULARITY_BACKFILL to the sync_tasks task_type CHECK constraint

TaskType.POPULARITY_BACKFILL (#117) was added in code; the sync_tasks.task_type
CHECK constraint must list it or inserts of popularity-backfill tasks fail with a
check violation (same class of bug fixed for MBID_BACKFILL in f7g8h9i0j1k2).

SQLAlchemy stores the enum .name (UPPERCASE) for native_enum=False columns, so the
constraint lists 'POPULARITY_BACKFILL', not the lowercase value.

Revision ID: h9i0j1k2l3m4
Revises: g8h9i0j1k2l3
Create Date: 2026-06-20

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "h9i0j1k2l3m4"
down_revision: str = "g8h9i0j1k2l3"
branch_labels: None = None
depends_on: None = None

_ALL_TYPES = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', "
    "'CONCERT_ARCHIVES_IMPORT', 'CONCERT_ARCHIVES_CHUNK', "
    "'PLAYLIST_EXPORT', 'MBID_BACKFILL', 'POPULARITY_BACKFILL'"
)

_ALL_TYPES_WITHOUT_POPULARITY = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', "
    "'CONCERT_ARCHIVES_IMPORT', 'CONCERT_ARCHIVES_CHUNK', "
    "'PLAYLIST_EXPORT', 'MBID_BACKFILL'"
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
            f"CHECK (task_type IN ({_ALL_TYPES_WITHOUT_POPULARITY}))"
        )
    )

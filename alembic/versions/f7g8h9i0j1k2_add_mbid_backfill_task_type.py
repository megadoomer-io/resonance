"""add MBID_BACKFILL to the sync_tasks task_type CHECK constraint

TaskType.MBID_BACKFILL (#71) was added in code; the sync_tasks.task_type CHECK
constraint must list it or inserts of backfill tasks fail with a check violation.

Revision ID: f7g8h9i0j1k2
Revises: e6f7g8h9i0j1
Create Date: 2026-06-18

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "f7g8h9i0j1k2"
down_revision: str = "e6f7g8h9i0j1"
branch_labels: None = None
depends_on: None = None

_ALL_TYPES = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', "
    "'CONCERT_ARCHIVES_IMPORT', 'CONCERT_ARCHIVES_CHUNK', "
    "'PLAYLIST_EXPORT', 'MBID_BACKFILL'"
)

_ALL_TYPES_WITHOUT_BACKFILL = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', "
    "'CONCERT_ARCHIVES_IMPORT', 'CONCERT_ARCHIVES_CHUNK', "
    "'PLAYLIST_EXPORT'"
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
            f"CHECK (task_type IN ({_ALL_TYPES_WITHOUT_BACKFILL}))"
        )
    )

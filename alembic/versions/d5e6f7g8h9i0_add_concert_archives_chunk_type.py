"""add CONCERT_ARCHIVES_CHUNK to TaskType enum values

Revision ID: d5e6f7g8h9i0
Revises: c4d5e6f7g8h9
Create Date: 2026-06-11

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "d5e6f7g8h9i0"
down_revision: str = "c4d5e6f7g8h9"
branch_labels: None = None
depends_on: None = None

_ALL_TYPES = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', "
    "'CONCERT_ARCHIVES_IMPORT', 'CONCERT_ARCHIVES_CHUNK', "
    "'PLAYLIST_EXPORT'"
)

_ALL_TYPES_WITHOUT_CHUNK = (
    "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
    "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
    "'TRACK_DISCOVERY', 'TRACK_SCORING', "
    "'CONCERT_ARCHIVES_IMPORT', 'PLAYLIST_EXPORT'"
)


def upgrade() -> None:
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks "
            "DROP CONSTRAINT IF EXISTS "
            '"ck_sync_tasks_task_type"'
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks "
            "DROP CONSTRAINT IF EXISTS "
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
            "ALTER TABLE sync_tasks "
            "DROP CONSTRAINT IF EXISTS "
            '"ck_sync_tasks_task_type_tasktype"'
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks ADD CONSTRAINT "
            '"ck_sync_tasks_task_type_tasktype" '
            f"CHECK (task_type IN ({_ALL_TYPES_WITHOUT_CHUNK}))"
        )
    )

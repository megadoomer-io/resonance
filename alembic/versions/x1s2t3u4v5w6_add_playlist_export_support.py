"""add playlist export support

Revision ID: x1s2t3u4v5w6
Revises: w0r1s2t3u4v5
Create Date: 2026-05-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "x1s2t3u4v5w6"
down_revision: str = "w0r1s2t3u4v5"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "playlists",
        sa.Column("service_links", sa.JSON(), nullable=True),
    )

    op.execute(
        sa.text(
            'ALTER TABLE sync_tasks DROP CONSTRAINT IF EXISTS "ck_sync_tasks_task_type"'
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks ADD CONSTRAINT "
            '"ck_sync_tasks_task_type" '
            "CHECK (task_type IN ("
            "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
            "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
            "'TRACK_DISCOVERY', 'TRACK_SCORING', 'PLAYLIST_EXPORT'))"
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            'ALTER TABLE sync_tasks DROP CONSTRAINT IF EXISTS "ck_sync_tasks_task_type"'
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks ADD CONSTRAINT "
            '"ck_sync_tasks_task_type" '
            "CHECK (task_type IN ("
            "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
            "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
            "'TRACK_DISCOVERY', 'TRACK_SCORING'))"
        )
    )

    op.drop_column("playlists", "service_links")

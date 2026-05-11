"""rename attendance status NONE to NOT_GOING

Revision ID: a4u5v6w7x8y9
Revises: z3u4v5w6x7y8
Create Date: 2026-05-11

"""

from __future__ import annotations

from alembic import op

revision: str = "a4u5v6w7x8y9"
down_revision: str | None = "z3u4v5w6x7y8"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    # Update existing records from NONE to NOT_GOING
    op.execute(
        """
        UPDATE user_event_attendance
        SET status = 'NOT_GOING'
        WHERE status = 'NONE'
        """
    )

    # Update the CHECK constraint to use NOT_GOING instead of NONE
    # native_enum=False means varchar with CHECK constraint
    op.execute(
        """
        ALTER TABLE user_event_attendance
        DROP CONSTRAINT IF EXISTS "ck_user_event_attendance_status"
        """
    )
    op.execute(
        """
        ALTER TABLE user_event_attendance
        ADD CONSTRAINT "ck_user_event_attendance_status"
        CHECK (status IN ('GOING', 'INTERESTED', 'NOT_GOING'))
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE user_event_attendance
        SET status = 'NONE'
        WHERE status = 'NOT_GOING'
        """
    )
    op.execute(
        """
        ALTER TABLE user_event_attendance
        DROP CONSTRAINT IF EXISTS "ck_user_event_attendance_status"
        """
    )
    op.execute(
        """
        ALTER TABLE user_event_attendance
        ADD CONSTRAINT "ck_user_event_attendance_status"
        CHECK (status IN ('GOING', 'INTERESTED', 'NONE'))
        """
    )

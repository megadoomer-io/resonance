"""add role column to users table

Revision ID: f3a4b5c6d7e8
Revises: e2f3a4b5c6d7
Create Date: 2026-04-09

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "f3a4b5c6d7e8"
down_revision: str | None = "e2f3a4b5c6d7"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column(
            "role",
            sa.Enum("user", "admin", "owner", name="userrole", native_enum=False),
            nullable=False,
            server_default="user",
        ),
    )
    # Promote the earliest user to owner
    op.execute(
        """
        UPDATE users SET role = 'owner'
        WHERE id = (SELECT id FROM users ORDER BY created_at ASC LIMIT 1)
        """
    )


def downgrade() -> None:
    op.drop_column("users", "role")

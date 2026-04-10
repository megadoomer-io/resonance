"""fix role column values to uppercase

Revision ID: g4b5c6d7e8f9
Revises: f3a4b5c6d7e8
Create Date: 2026-04-09

"""

from __future__ import annotations

from alembic import op

revision: str = "g4b5c6d7e8f9"
down_revision: str | None = "f3a4b5c6d7e8"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    # Fix any lowercase role values to uppercase (matching SQLAlchemy enum name convention)
    op.execute("UPDATE users SET role = UPPER(role) WHERE role != UPPER(role)")
    # Fix the server default
    op.alter_column("users", "role", server_default="USER")


def downgrade() -> None:
    op.execute("UPDATE users SET role = LOWER(role) WHERE role != LOWER(role)")
    op.alter_column("users", "role", server_default="user")

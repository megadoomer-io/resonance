"""add origin field to artists table

Revision ID: u8p9q0r1s2t3
Revises: t7o8p9q0r1s2
Create Date: 2026-04-28

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "u8p9q0r1s2t3"
down_revision: str = "t7o8p9q0r1s2"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column("artists", sa.Column("origin", sa.String(256), nullable=True))


def downgrade() -> None:
    op.drop_column("artists", "origin")

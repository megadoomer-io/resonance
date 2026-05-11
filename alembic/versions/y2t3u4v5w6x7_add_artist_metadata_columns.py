"""add artist metadata columns

Revision ID: y2t3u4v5w6x7
Revises: x1s2t3u4v5w6
Create Date: 2026-05-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "y2t3u4v5w6x7"
down_revision: str = "x1s2t3u4v5w6"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "artists",
        sa.Column("disambiguation", sa.String(512), nullable=True),
    )
    op.add_column(
        "artists",
        sa.Column("artist_type", sa.String(64), nullable=True),
    )
    op.add_column(
        "artists",
        sa.Column("area", sa.String(256), nullable=True),
    )
    op.add_column(
        "artists",
        sa.Column("begin_year", sa.Integer(), nullable=True),
    )
    op.add_column(
        "artists",
        sa.Column("end_year", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("artists", "end_year")
    op.drop_column("artists", "begin_year")
    op.drop_column("artists", "area")
    op.drop_column("artists", "artist_type")
    op.drop_column("artists", "disambiguation")

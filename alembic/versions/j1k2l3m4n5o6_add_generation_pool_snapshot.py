"""add pool_snapshot column to generation_records

#128: live source re-resolution (events re-read each run, related artists hit
nondeterministic external connectors) means two runs of the same profile can
produce different pools. parameter_snapshot already records the params; add a
pool_snapshot JSON column so the resolved artist pool (ids + provenance) is also
captured, making a generation auditable and reproducible.

Nullable: rows written before this migration have no snapshot.

Revision ID: j1k2l3m4n5o6
Revises: i0j1k2l3m4n5
Create Date: 2026-06-23

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "j1k2l3m4n5o6"
down_revision: str = "i0j1k2l3m4n5"
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    op.add_column(
        "generation_records",
        sa.Column("pool_snapshot", sa.JSON, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("generation_records", "pool_snapshot")

"""add version column to generator_profiles

#133 optimistic concurrency: the builder (PATCH), the CLI/agent, and the enrich
worker all write generator_profiles.input_references. Without a guard, a worker
that read the row, then committed after the editor's PATCH, would clobber the
edit (lost update). An integer version that every writer assert-and-bumps turns
that race into a 409 / retry.

This migration only adds the column (default 1, backfilling existing rows to 1).
The mapper version_id_col wiring and the 409/retry handling land with the
concurrency task.

Revision ID: l3m4n5o6p7q8
Revises: k2l3m4n5o6p7
Create Date: 2026-06-24

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "l3m4n5o6p7q8"
down_revision: str = "k2l3m4n5o6p7"
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    op.add_column(
        "generator_profiles",
        sa.Column(
            "version",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
    )


def downgrade() -> None:
    op.drop_column("generator_profiles", "version")

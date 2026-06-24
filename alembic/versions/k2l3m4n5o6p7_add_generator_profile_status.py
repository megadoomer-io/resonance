"""add status column to generator_profiles

#133 makes the lineup builder a server-backed profile editor: opening "new
playlist" eagerly creates a DRAFT profile so every edit persists, and the first
successful generation flips it to ACTIVE. The profile list shows only ACTIVE
profiles, so half-built drafts stay hidden.

Existing profiles predate the draft concept -- they are real, in-use recipes, so
they are backfilled to ACTIVE (not DRAFT, which would hide them from the list).
The column server_default is DRAFT so new rows created outside the ORM still land
as drafts.

native_enum=False stores the enum .name (UPPERCASE), so values are 'DRAFT' /
'ACTIVE' (and the CHECK lists those), per the convention used across this schema.

Revision ID: k2l3m4n5o6p7
Revises: j1k2l3m4n5o6
Create Date: 2026-06-24

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "k2l3m4n5o6p7"
down_revision: str = "j1k2l3m4n5o6"
branch_labels: None = None
depends_on: None = None

_status_enum = sa.Enum("DRAFT", "ACTIVE", name="profilestatus", native_enum=False)


def upgrade() -> None:
    op.add_column(
        "generator_profiles",
        sa.Column(
            "status",
            _status_enum,
            nullable=False,
            server_default="DRAFT",
        ),
    )
    # Existing profiles are established, in-use recipes -> ACTIVE, so they keep
    # showing in the (active-filtered) list. New rows keep the DRAFT default.
    op.execute(sa.text("UPDATE generator_profiles SET status = 'ACTIVE'"))
    # Enforce valid values at the DB level (project convention for
    # native_enum=False columns; mirrors ck_generator_profiles_generator_type).
    # Values are the enum .name (UPPERCASE).
    op.create_check_constraint(
        "ck_generator_profiles_status_profilestatus",
        "generator_profiles",
        "status IN ('DRAFT', 'ACTIVE')",
    )


def downgrade() -> None:
    op.drop_column("generator_profiles", "status")

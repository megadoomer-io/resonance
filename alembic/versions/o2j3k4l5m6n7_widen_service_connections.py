"""widen service_connections for unified connection model

Add columns (url, label, enabled, last_synced_at) so the table can hold
both OAuth connections and calendar-feed connections.  Make
encrypted_access_token and external_user_id nullable for feed rows that
have no tokens.  Copy last_used_at → last_synced_at for existing OAuth
rows.  Add a partial unique index on (user_id, url) WHERE url IS NOT NULL.

Revision ID: o2j3k4l5m6n7
Revises: n1i2j3k4l5m6
Create Date: 2026-04-22

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "o2j3k4l5m6n7"
down_revision: str | None = "n1i2j3k4l5m6"


def upgrade() -> None:
    # -- new columns --------------------------------------------------------
    op.add_column(
        "service_connections",
        sa.Column("url", sa.String(2048), nullable=True),
    )
    op.add_column(
        "service_connections",
        sa.Column("label", sa.String(256), nullable=True),
    )
    op.add_column(
        "service_connections",
        sa.Column(
            "enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
    )
    op.add_column(
        "service_connections",
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
    )

    # -- copy last_used_at → last_synced_at for existing rows ---------------
    op.execute(
        "UPDATE service_connections "
        "SET last_synced_at = last_used_at "
        "WHERE last_used_at IS NOT NULL"
    )

    # -- relax NOT NULL on columns that feed rows won't populate ------------
    op.alter_column(
        "service_connections",
        "encrypted_access_token",
        existing_type=sa.Text(),
        nullable=True,
    )
    op.alter_column(
        "service_connections",
        "external_user_id",
        existing_type=sa.String(255),
        nullable=True,
    )

    # -- partial unique index: one URL per user -----------------------------
    op.create_index(
        "ix_service_connections_user_url",
        "service_connections",
        ["user_id", "url"],
        unique=True,
        postgresql_where=sa.text("url IS NOT NULL"),
    )


def downgrade() -> None:
    # -- drop partial unique index ------------------------------------------
    op.drop_index(
        "ix_service_connections_user_url",
        table_name="service_connections",
    )

    # -- restore NOT NULL on columns ----------------------------------------
    op.alter_column(
        "service_connections",
        "external_user_id",
        existing_type=sa.String(255),
        nullable=False,
    )
    op.alter_column(
        "service_connections",
        "encrypted_access_token",
        existing_type=sa.Text(),
        nullable=False,
    )

    # -- drop new columns ---------------------------------------------------
    op.drop_column("service_connections", "last_synced_at")
    op.drop_column("service_connections", "enabled")
    op.drop_column("service_connections", "label")
    op.drop_column("service_connections", "url")

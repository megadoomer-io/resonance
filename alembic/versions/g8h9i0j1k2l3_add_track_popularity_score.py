"""add popularity_score column to tracks

Track popularity (0-100) feeds the generator's hit_depth parameter via
scoring.popularity_signal. Until now the column did not exist and
score_and_build_playlist hardcoded 0, so hit_depth had no effect (#114).

Nullable int, NULL = unknown popularity (treated as 0 at scoring time). Populated
going forward by track discovery (ListenBrainz synthetic rank) and, in a later
phase, by Spotify's authoritative 0-100 popularity.

Revision ID: g8h9i0j1k2l3
Revises: f7g8h9i0j1k2
Create Date: 2026-06-19

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "g8h9i0j1k2l3"
down_revision: str | None = "f7g8h9i0j1k2"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    op.add_column(
        "tracks",
        sa.Column("popularity_score", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("tracks", "popularity_score")

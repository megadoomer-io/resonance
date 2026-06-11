# ruff: noqa: RUF001 — ambiguous unicode characters are intentional (quote map)
"""add normalized_raw_name to event_artist_candidates

Adds a normalized_raw_name column for unicode-safe deduplication.
Backfills from existing raw_name values, then swaps the unique
constraint from (event_id, raw_name) to (event_id, normalized_raw_name).

Revision ID: c4d5e6f7g8h9
Revises: a5v6w7x8y9z0
Create Date: 2026-06-10

"""

from __future__ import annotations

import re
import unicodedata

import sqlalchemy as sa
from alembic import op

revision: str = "c4d5e6f7g8h9"
down_revision: str = "a5v6w7x8y9z0"
branch_labels: None = None
depends_on: None = None

# Inline normalization — mirrors resonance.normalize.normalize_name()
# so the migration doesn't depend on application code.
_QUOTE_MAP = str.maketrans(
    {
        "‘": "'",
        "’": "'",
        "‚": "'",
        "′": "'",
        "“": '"',
        "”": '"',
        "„": '"',
        "″": '"',
    }
)
_WHITESPACE = re.compile(r"\s+")


def _normalize_name(value: str) -> str:
    if not value:
        return ""
    result = unicodedata.normalize("NFC", value)
    result = result.translate(_QUOTE_MAP)
    decomposed = unicodedata.normalize("NFD", result)
    result = "".join(c for c in decomposed if unicodedata.category(c) != "Mn")
    result = unicodedata.normalize("NFC", result)
    result = result.casefold()
    result = _WHITESPACE.sub(" ", result).strip()
    return result


def upgrade() -> None:
    # Step 1: Add column as nullable first
    op.add_column(
        "event_artist_candidates",
        sa.Column("normalized_raw_name", sa.String(512), nullable=True),
    )

    # Step 2: Backfill existing rows
    conn = op.get_bind()
    rows = conn.execute(
        sa.text("SELECT id, raw_name FROM event_artist_candidates")
    ).fetchall()
    for row_id, raw_name in rows:
        normalized = _normalize_name(raw_name)
        conn.execute(
            sa.text(
                "UPDATE event_artist_candidates "
                "SET normalized_raw_name = :normalized "
                "WHERE id = :id"
            ),
            {"normalized": normalized, "id": row_id},
        )

    # Step 3: Deduplicate rows that now share (event_id, normalized_raw_name).
    # Keep the row with the highest confidence (prefer matched over unmatched).
    conn.execute(
        sa.text("""
            DELETE FROM event_artist_candidates
            WHERE id IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY event_id, normalized_raw_name
                        ORDER BY
                            CASE WHEN matched_artist_id IS NOT NULL
                                 THEN 0 ELSE 1 END,
                            confidence_score DESC,
                            created_at ASC
                    ) AS rn
                    FROM event_artist_candidates
                ) ranked
                WHERE rn > 1
            )
        """)
    )

    # Step 4: Make column non-nullable after backfill and dedup
    op.alter_column(
        "event_artist_candidates",
        "normalized_raw_name",
        nullable=False,
    )

    # Step 5: Drop old constraint, add new one
    op.drop_constraint(
        "uq_event_artist_candidates_event_name",
        "event_artist_candidates",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_event_artist_candidates_event_normalized_name",
        "event_artist_candidates",
        ["event_id", "normalized_raw_name"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_event_artist_candidates_event_normalized_name",
        "event_artist_candidates",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_event_artist_candidates_event_name",
        "event_artist_candidates",
        ["event_id", "raw_name"],
    )
    op.drop_column("event_artist_candidates", "normalized_raw_name")

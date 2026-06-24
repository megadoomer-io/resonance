"""migrate generator_profiles.input_references {event_id} -> {sources}

#128 generalizes a profile's pool from a single event to a layered list of
sources. Existing concert_prep profiles store the legacy single-event shape
``{"event_id": "<uuid>"}``; rewrite each to the layered shape
``{"sources": [{"kind": "event", "event_id": "<uuid>", "enabled": true}],
"exclude_artist_ids": []}``.

The resolver (generators.pool.normalize_sources) already tolerates the legacy
shape, so this migration and the worker cutover need not be perfectly atomic --
a profile still resolves whether or not this has run. The migration only
normalizes stored data to the new canonical shape.

input_references is a sa.JSON column, so a lightweight table with the JSON type
handles per-dialect (de)serialization for both Postgres and SQLite.

Revision ID: i0j1k2l3m4n5
Revises: h9i0j1k2l3m4
Create Date: 2026-06-23

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "i0j1k2l3m4n5"
down_revision: str = "h9i0j1k2l3m4"
branch_labels: None = None
depends_on: None = None


_profiles = sa.table(
    "generator_profiles",
    sa.column("id", sa.Uuid),
    sa.column("input_references", sa.JSON),
)


def _legacy_to_layered(refs: object) -> dict[str, object] | None:
    """Return the layered shape for a legacy {event_id} dict, else None."""
    if not isinstance(refs, dict):
        return None
    if "sources" in refs:
        return None  # already layered
    event_id = refs.get("event_id")
    if not event_id:
        return None  # nothing to migrate (no event_id)
    return {
        "sources": [
            {"kind": "event", "event_id": str(event_id), "enabled": True}
        ],
        "exclude_artist_ids": [],
    }


def _layered_to_legacy(refs: object) -> dict[str, object] | None:
    """Reverse only the exact single-event shape this migration produced."""
    if not isinstance(refs, dict):
        return None
    sources = refs.get("sources")
    excludes = refs.get("exclude_artist_ids")
    if not isinstance(sources, list) or len(sources) != 1:
        return None
    if excludes:  # had a non-empty exclude set -> not a clean legacy reversal
        return None
    source = sources[0]
    if not isinstance(source, dict):
        return None
    if source.get("kind") != "event" or not source.get("enabled", True):
        return None
    event_id = source.get("event_id")
    if not event_id:
        return None
    return {"event_id": str(event_id)}


def upgrade() -> None:
    bind = op.get_bind()
    rows = bind.execute(
        sa.select(_profiles.c.id, _profiles.c.input_references)
    ).all()
    for row in rows:
        new_refs = _legacy_to_layered(row.input_references)
        if new_refs is not None:
            bind.execute(
                sa.update(_profiles)
                .where(_profiles.c.id == row.id)
                .values(input_references=new_refs)
            )


def downgrade() -> None:
    bind = op.get_bind()
    rows = bind.execute(
        sa.select(_profiles.c.id, _profiles.c.input_references)
    ).all()
    for row in rows:
        old_refs = _layered_to_legacy(row.input_references)
        if old_refs is not None:
            bind.execute(
                sa.update(_profiles)
                .where(_profiles.c.id == row.id)
                .values(input_references=old_refs)
            )

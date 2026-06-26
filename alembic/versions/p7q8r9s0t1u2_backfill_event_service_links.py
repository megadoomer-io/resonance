"""backfill events.service_links from primary + resolved candidate sources

The per-source event link audit (#event-link-audit) populates ``events.service_links``
with one ``{service: url}`` entry per contributing source, so the detail view's
per-source label and href can never desync. New writes populate it going forward;
this DATA migration backfills existing rows.

For each event we assemble links from two places, never overwriting a value
already present:
  1. the event's own denormalized ``(source_service, external_url)`` pair -- the
     primary source, which wins for its own key, and
  2. every ``event_candidates`` row resolved to this event, which fills links for
     the *other* sources that matched it across (date, venue).

``service_links`` keys are ``ServiceType`` values (lowercase). The
``source_service`` column stores the StrEnum *name* (uppercase, ``native_enum=False``);
since every member's value is exactly ``name.lower()`` we lowercase the stored
name to get the key without importing the enum into the migration.

Downgrade is a no-op: we cannot tell backfilled keys from any that predated the
migration, and the per-source links are recoverable from candidates anyway.

Revision ID: p7q8r9s0t1u2
Revises: o6p7q8r9s0t1
Create Date: 2026-06-26

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "p7q8r9s0t1u2"
down_revision: str = "o6p7q8r9s0t1"
branch_labels: None = None
depends_on: None = None

_events = sa.table(
    "events",
    sa.column("id", sa.Uuid),
    sa.column("source_service", sa.String),
    sa.column("external_url", sa.String),
    sa.column("service_links", sa.JSON),
)

_event_candidates = sa.table(
    "event_candidates",
    sa.column("resolved_event_id", sa.Uuid),
    sa.column("source_service", sa.String),
    sa.column("external_url", sa.String),
)


def upgrade() -> None:
    bind = op.get_bind()

    # Group resolved candidate links by the event they resolved to.
    cand_rows = bind.execute(
        sa.select(
            _event_candidates.c.resolved_event_id,
            _event_candidates.c.source_service,
            _event_candidates.c.external_url,
        ).where(_event_candidates.c.resolved_event_id.is_not(None))
    ).all()
    by_event: dict[object, list[tuple[str, str]]] = {}
    for row in cand_rows:
        if row.external_url:
            by_event.setdefault(row.resolved_event_id, []).append(
                (row.source_service, row.external_url)
            )

    event_rows = bind.execute(
        sa.select(
            _events.c.id,
            _events.c.source_service,
            _events.c.external_url,
            _events.c.service_links,
        )
    ).all()
    for row in event_rows:
        existing = dict(row.service_links or {})
        links = dict(existing)
        # The event's own primary source wins for its own key.
        if row.external_url:
            links.setdefault(row.source_service.lower(), row.external_url)
        # Candidates fill links for the other sources matched to this event.
        for src, url in by_event.get(row.id, []):
            links.setdefault(src.lower(), url)
        if links != existing:
            bind.execute(
                sa.update(_events)
                .where(_events.c.id == row.id)
                .values(service_links=links)
            )


def downgrade() -> None:
    # Irreversible by design: backfilled keys are indistinguishable from any that
    # predated the migration, and per-source links are recoverable from the
    # resolved event_candidates rows.
    pass

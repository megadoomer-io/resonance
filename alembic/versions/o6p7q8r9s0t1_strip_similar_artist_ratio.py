"""strip similar_artist_ratio from generator_profiles.parameter_values

#133 removes the generation-time related-artist path: the ``similar_artist_ratio``
parameter is deleted from the registry and replaced by explicit, persisted
enrichment. ``apply_defaults`` raises on any unknown parameter name, so a stored
profile that still carries ``similar_artist_ratio`` would fail to load once the
registry no longer knows the key.

This DATA migration removes the key from every existing profile's
``parameter_values`` so they keep loading after the registry removal. It is
sequenced as the head migration, AFTER the schema changes and paired (in the same
PR) with the code that stops reading the parameter -- migrations run before app
code on deploy, so the strip lands first.

Downgrade is a no-op: the stripped value is not recoverable, and once the
parameter is restored to the registry, ``apply_defaults`` re-supplies its default
(0) for any profile missing the key. No data is lost that defaults can't refill.

Revision ID: o6p7q8r9s0t1
Revises: n5o6p7q8r9s0
Create Date: 2026-06-24

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "o6p7q8r9s0t1"
down_revision: str = "n5o6p7q8r9s0"
branch_labels: None = None
depends_on: None = None

_PARAM = "similar_artist_ratio"

_profiles = sa.table(
    "generator_profiles",
    sa.column("id", sa.Uuid),
    sa.column("parameter_values", sa.JSON),
)


def _strip_similar_ratio(params: object) -> dict[str, object] | None:
    """Return parameter_values without similar_artist_ratio, or None to skip.

    None means "no change needed" (not a dict, or the key is absent) so the
    migration only writes rows that actually carry the dead parameter.
    """
    if not isinstance(params, dict) or _PARAM not in params:
        return None
    return {k: v for k, v in params.items() if k != _PARAM}


def upgrade() -> None:
    bind = op.get_bind()
    rows = bind.execute(sa.select(_profiles.c.id, _profiles.c.parameter_values)).all()
    for row in rows:
        stripped = _strip_similar_ratio(row.parameter_values)
        if stripped is not None:
            bind.execute(
                sa.update(_profiles)
                .where(_profiles.c.id == row.id)
                .values(parameter_values=stripped)
            )


def downgrade() -> None:
    # Irreversible by design: the stripped value is gone, and the parameter's
    # registry default refills it on read once the parameter is restored.
    pass

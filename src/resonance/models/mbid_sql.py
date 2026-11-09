"""SQL for an artist's normalized MBID, the generated ``artists.musicbrainz_id``.

The same rule as ``services.artist_utils.normalized_mbid``: ``musicbrainz.id``,
else a flat ``musicbrainz`` string, else the legacy flat ``listenbrainz``
string, trimmed and lowercased; non-string or blank values are skipped. Postgres
is the real target; the SQLite form keeps the unit-test schema buildable.
Every function used is immutable, as a generated column requires.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy.sql.expression as sql_expr
from sqlalchemy.ext.compiler import compiles

_PATHS = (("musicbrainz", "id"), ("musicbrainz",), ("listenbrainz",))


def postgresql_sql(column: str = "service_links") -> str:
    parts = []
    for path in _PATHS:
        node = column + "".join(f" -> '{key}'" for key in path)
        text = column + "".join(f" -> '{key}'" for key in path[:-1])
        text += f" ->> '{path[-1]}'"
        parts.append(
            f"NULLIF(LOWER(BTRIM(CASE WHEN json_typeof({node}) = 'string'"
            f" THEN {text} END)), '')"
        )
    return f"COALESCE({', '.join(parts)})"


def _sqlite_sql(column: str = "service_links") -> str:
    parts = []
    for path in _PATHS:
        json_path = "$." + ".".join(path)
        parts.append(
            f"NULLIF(LOWER(TRIM(CASE WHEN json_type({column}, '{json_path}') = 'text'"
            f" THEN json_extract({column}, '{json_path}') END)), '')"
        )
    return f"COALESCE({', '.join(parts)})"


class NormalizedMbid(sql_expr.ColumnElement[str]):
    """The normalized-MBID expression over ``service_links``, per dialect."""

    inherit_cache = True


@compiles(NormalizedMbid)
def _compile_default(element: NormalizedMbid, compiler: Any, **kw: Any) -> str:
    return postgresql_sql()


@compiles(NormalizedMbid, "sqlite")
def _compile_sqlite(element: NormalizedMbid, compiler: Any, **kw: Any) -> str:
    return _sqlite_sql()

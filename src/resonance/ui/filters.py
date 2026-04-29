"""Shared filter framework for UI list views.

Defines filter field types and the ``apply_filters`` engine that builds
SQLAlchemy ``.where()`` clauses from Starlette query parameters.
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
from typing import Any

import sqlalchemy as sa

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _escape_ilike(value: str) -> str:
    """Escape ``%`` and ``_`` for safe use in ILIKE patterns."""
    return value.replace("%", r"\%").replace("_", r"\_")


# ---------------------------------------------------------------------------
# Filter field types
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class TextField:
    """ILIKE text search on a single column."""

    name: str
    column: sa.Column[Any]
    join: Any | None = None

    def parse(self, params: dict[str, str]) -> str | None:
        """Return the stripped search term, or ``None`` if absent/blank."""
        raw = params.get(self.name, "")
        stripped = raw.strip()
        return stripped if stripped else None

    def apply(self, query: sa.Select[Any], value: str) -> sa.Select[Any]:
        """Add an ILIKE WHERE clause for *value*."""
        pattern = f"%{_escape_ilike(value)}%"
        return query.where(self.column.ilike(pattern))

    def apply_quick_search(self, query: sa.Select[Any], value: str) -> sa.Select[Any]:
        """Same as :meth:`apply` -- provided for interface consistency."""
        return self.apply(query, value)

    def get_ilike_clause(self, value: str) -> sa.ColumnElement[bool]:
        """Return an ILIKE clause element (for OR composition in quick search)."""
        pattern = f"%{_escape_ilike(value)}%"
        clause: sa.ColumnElement[bool] = self.column.ilike(pattern)
        return clause

    @staticmethod
    def is_text() -> bool:
        return True


@dataclasses.dataclass
class MultiSelectField:
    """OR-match across a set of allowed option values."""

    name: str
    column: sa.Column[Any]
    options: list[str]

    def parse(self, params: dict[str, str]) -> list[str] | None:
        """Parse a single query-param value; validates against *options*."""
        raw = params.get(self.name)
        if raw is None:
            return None
        return [raw] if raw in self.options else None

    def parse_multi(self, values: list[str]) -> list[str] | None:
        """Parse repeated query-param values; filters to valid options."""
        valid = [v for v in values if v in self.options]
        return valid if valid else None

    def apply(self, query: sa.Select[Any], value: list[str]) -> sa.Select[Any]:
        """Add an IN clause for the selected values."""
        return query.where(self.column.in_(value))

    @staticmethod
    def is_text() -> bool:
        return False


@dataclasses.dataclass
class DateRangeField:
    """From/to date range filter using ``{name}_from`` and ``{name}_to``."""

    name: str
    column: sa.Column[Any]

    def parse(self, params: dict[str, str]) -> dict[str, datetime.date | None] | None:
        """Parse ISO date strings; returns ``None`` if both are missing/invalid."""
        from_str = params.get(f"{self.name}_from", "")
        to_str = params.get(f"{self.name}_to", "")

        from_date: datetime.date | None = None
        to_date: datetime.date | None = None

        if from_str:
            with contextlib.suppress(ValueError):
                from_date = datetime.date.fromisoformat(from_str)

        if to_str:
            with contextlib.suppress(ValueError):
                to_date = datetime.date.fromisoformat(to_str)

        if from_date is None and to_date is None:
            return None

        return {f"{self.name}_from": from_date, f"{self.name}_to": to_date}

    def apply(
        self, query: sa.Select[Any], value: dict[str, datetime.date | None]
    ) -> sa.Select[Any]:
        """Add >= and/or <= WHERE clauses for the date range."""
        if value.get(f"{self.name}_from") is not None:
            query = query.where(self.column >= value[f"{self.name}_from"])
        if value.get(f"{self.name}_to") is not None:
            query = query.where(self.column <= value[f"{self.name}_to"])
        return query

    @staticmethod
    def is_text() -> bool:
        return False


@dataclasses.dataclass
class NumericRangeField:
    """Min/max numeric range using ``{name}_min`` and ``{name}_max``."""

    name: str
    column: sa.Column[Any]

    def parse(self, params: dict[str, str]) -> dict[str, int | None] | None:
        """Parse integer strings; returns ``None`` if both are missing/invalid."""
        min_str = params.get(f"{self.name}_min", "")
        max_str = params.get(f"{self.name}_max", "")

        min_val: int | None = None
        max_val: int | None = None

        if min_str:
            with contextlib.suppress(ValueError):
                min_val = int(min_str)

        if max_str:
            with contextlib.suppress(ValueError):
                max_val = int(max_str)

        if min_val is None and max_val is None:
            return None

        return {f"{self.name}_min": min_val, f"{self.name}_max": max_val}

    def apply(
        self, query: sa.Select[Any], value: dict[str, int | None]
    ) -> sa.Select[Any]:
        """Add >= and/or <= WHERE clauses for the numeric range."""
        if value.get(f"{self.name}_min") is not None:
            query = query.where(self.column >= value[f"{self.name}_min"])
        if value.get(f"{self.name}_max") is not None:
            query = query.where(self.column <= value[f"{self.name}_max"])
        return query

    @staticmethod
    def is_text() -> bool:
        return False


@dataclasses.dataclass
class ExistsField:
    """Boolean EXISTS / NOT EXISTS subquery filter."""

    name: str
    exists_query: sa.Select[Any]

    def parse(self, params: dict[str, str]) -> bool | None:
        """Parse ``true``/``false`` string; returns ``None`` for other values."""
        raw = params.get(self.name)
        if raw == "true":
            return True
        if raw == "false":
            return False
        return None

    def apply(self, query: sa.Select[Any], value: bool) -> sa.Select[Any]:
        """Add EXISTS or NOT EXISTS clause."""
        exists_clause = self.exists_query.exists()
        condition = exists_clause if value else ~exists_clause
        return query.where(condition)

    @staticmethod
    def is_text() -> bool:
        return False


# ---------------------------------------------------------------------------
# Union type for annotations
# ---------------------------------------------------------------------------

AnyFilterField = (
    TextField | MultiSelectField | DateRangeField | NumericRangeField | ExistsField
)


# ---------------------------------------------------------------------------
# AppliedFilters
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class AppliedFilters:
    """Container for parsed filter values keyed by field name."""

    active_filters: dict[str, Any] = dataclasses.field(default_factory=dict)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


def parse_filter_params(
    fields: list[AnyFilterField],
    params: dict[str, str],
    *,
    multi_params: dict[str, list[str]] | None = None,
) -> AppliedFilters:
    """Parse query parameters into an :class:`AppliedFilters` instance.

    Args:
        fields: Filter field definitions for this list view.
        params: Single-value query params (e.g. ``request.query_params``).
        multi_params: Multi-value params for :class:`MultiSelectField`
            (e.g. ``{name: request.query_params.getlist(name)}``).

    Returns:
        An :class:`AppliedFilters` with only the fields that had valid input.
    """
    result = AppliedFilters()
    for field in fields:
        if isinstance(field, MultiSelectField) and multi_params:
            values = multi_params.get(field.name)
            if values:
                parsed = field.parse_multi(values)
                if parsed is not None:
                    result.active_filters[field.name] = parsed
        else:
            parsed_value = field.parse(params)
            if parsed_value is not None:
                result.active_filters[field.name] = parsed_value
    return result


def apply_filters(
    query: sa.Select[Any],
    fields: list[AnyFilterField],
    params: dict[str, str],
    *,
    multi_params: dict[str, list[str]] | None = None,
) -> sa.Select[Any]:
    """Apply per-field column filters and quick search to *query*.

    The ``q`` query parameter triggers quick search: an OR across all
    :class:`TextField` columns.  Per-field filters are ANDed together.

    Args:
        query: The base SQLAlchemy SELECT statement.
        fields: Filter field definitions for this list view.
        params: Single-value query params.
        multi_params: Multi-value params for :class:`MultiSelectField`.

    Returns:
        The query with WHERE clauses added for active filters.
    """
    applied = parse_filter_params(fields, params, multi_params=multi_params)

    # Apply per-field filters (ANDed)
    for field in fields:
        value = applied.active_filters.get(field.name)
        if value is not None:
            query = field.apply(query, value)

    # Quick search: OR across all text fields
    q = params.get("q", "").strip()
    if q:
        text_fields = [f for f in fields if isinstance(f, TextField)]
        if text_fields:
            clauses = [f.get_ilike_clause(q) for f in text_fields]
            if clauses:
                query = query.where(sa.or_(*clauses))

    return query

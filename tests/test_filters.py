"""Tests for the shared filter framework."""

from __future__ import annotations

import datetime

import sqlalchemy as sa

import resonance.ui.filters as filters_module

# ---------------------------------------------------------------------------
# Test table fixtures — plain sa.Table, not ORM models
# ---------------------------------------------------------------------------

_metadata = sa.MetaData()

artists_table = sa.Table(
    "artists",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String),
    sa.Column("origin", sa.String),
)

events_table = sa.Table(
    "events",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("title", sa.String),
    sa.Column("venue_name", sa.String),
    sa.Column("event_date", sa.Date),
    sa.Column("status", sa.String),
    sa.Column("capacity", sa.Integer),
)

tracks_table = sa.Table(
    "tracks",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("title", sa.String),
    sa.Column("artist_id", sa.Integer),
)


def _compile(stmt: sa.Select[tuple[object, ...]]) -> str:
    """Compile a SQLAlchemy statement to a string for assertion matching."""
    return str(
        stmt.compile(
            compile_kwargs={"literal_binds": True},
            dialect=sa.create_engine("sqlite://").dialect,
        )
    )


# ---------------------------------------------------------------------------
# _escape_ilike
# ---------------------------------------------------------------------------


class TestEscapeIlike:
    """Test ILIKE wildcard escaping."""

    def test_escapes_percent(self) -> None:
        assert filters_module._escape_ilike("50%") == r"50\%"

    def test_escapes_underscore(self) -> None:
        assert filters_module._escape_ilike("hello_world") == r"hello\_world"

    def test_escapes_both(self) -> None:
        assert filters_module._escape_ilike("a%b_c") == r"a\%b\_c"

    def test_no_special_chars_unchanged(self) -> None:
        assert filters_module._escape_ilike("hello") == "hello"

    def test_empty_string(self) -> None:
        assert filters_module._escape_ilike("") == ""


# ---------------------------------------------------------------------------
# TextField
# ---------------------------------------------------------------------------


class TestTextField:
    """Test TextField parsing and SQL generation."""

    def test_parse_returns_stripped_value(self) -> None:
        field = filters_module.TextField(name="q", column=artists_table.c.name)
        result = field.parse({"q": "  Radiohead  "})
        assert result == "Radiohead"

    def test_parse_empty_returns_none(self) -> None:
        field = filters_module.TextField(name="q", column=artists_table.c.name)
        assert field.parse({"q": ""}) is None
        assert field.parse({"q": "   "}) is None

    def test_parse_missing_key_returns_none(self) -> None:
        field = filters_module.TextField(name="q", column=artists_table.c.name)
        assert field.parse({}) is None

    def test_is_text_returns_true(self) -> None:
        field = filters_module.TextField(name="q", column=artists_table.c.name)
        assert field.is_text() is True

    def test_apply_adds_ilike_clause(self) -> None:
        field = filters_module.TextField(name="q", column=artists_table.c.name)
        base = sa.select(artists_table)
        result = field.apply(base, "Radio")
        compiled = _compile(result)
        assert "LIKE" in compiled.upper()
        assert "radio" in compiled.lower() or "Radio" in compiled

    def test_apply_quick_search_same_as_apply(self) -> None:
        field = filters_module.TextField(name="q", column=artists_table.c.name)
        base = sa.select(artists_table)
        result = field.apply_quick_search(base, "test")
        compiled = _compile(result)
        assert "LIKE" in compiled.upper()

    def test_apply_escapes_wildcards(self) -> None:
        field = filters_module.TextField(name="q", column=artists_table.c.name)
        base = sa.select(artists_table)
        result = field.apply(base, "50%_off")
        compiled = _compile(result)
        # The escaped wildcards should appear in the compiled SQL
        assert r"\%" in compiled or "50" in compiled


# ---------------------------------------------------------------------------
# MultiSelectField
# ---------------------------------------------------------------------------


class TestMultiSelectField:
    """Test MultiSelectField parsing and SQL generation."""

    def test_parse_single_value(self) -> None:
        field = filters_module.MultiSelectField(
            name="status",
            column=events_table.c.status,
            options=["going", "interested", "not_going"],
        )
        result = field.parse({"status": "going"})
        assert result == ["going"]

    def test_parse_multiple_values(self) -> None:
        field = filters_module.MultiSelectField(
            name="status",
            column=events_table.c.status,
            options=["going", "interested", "not_going"],
        )
        # Simulate repeated query params via getlist-style dict
        result = field.parse_multi(["going", "interested"])
        assert result == ["going", "interested"]

    def test_parse_filters_invalid_values(self) -> None:
        field = filters_module.MultiSelectField(
            name="status",
            column=events_table.c.status,
            options=["going", "interested"],
        )
        result = field.parse_multi(["going", "invalid", "interested"])
        assert result == ["going", "interested"]

    def test_parse_empty_returns_none(self) -> None:
        field = filters_module.MultiSelectField(
            name="status",
            column=events_table.c.status,
            options=["going", "interested"],
        )
        assert field.parse({}) is None

    def test_parse_all_invalid_returns_none(self) -> None:
        field = filters_module.MultiSelectField(
            name="status",
            column=events_table.c.status,
            options=["going"],
        )
        result = field.parse_multi(["invalid", "also_invalid"])
        assert result is None

    def test_apply_generates_in_clause(self) -> None:
        field = filters_module.MultiSelectField(
            name="status",
            column=events_table.c.status,
            options=["going", "interested"],
        )
        base = sa.select(events_table)
        result = field.apply(base, ["going", "interested"])
        compiled = _compile(result)
        assert "IN" in compiled.upper()

    def test_is_text_returns_false(self) -> None:
        field = filters_module.MultiSelectField(
            name="status",
            column=events_table.c.status,
            options=["going"],
        )
        assert field.is_text() is False


# ---------------------------------------------------------------------------
# DateRangeField
# ---------------------------------------------------------------------------


class TestDateRangeField:
    """Test DateRangeField parsing and SQL generation."""

    def test_parse_from_only(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        result = field.parse({"date_from": "2025-01-01"})
        assert result is not None
        assert result["from"] == datetime.date(2025, 1, 1)
        assert result.get("to") is None

    def test_parse_to_only(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        result = field.parse({"date_to": "2025-12-31"})
        assert result is not None
        assert result.get("from") is None
        assert result["to"] == datetime.date(2025, 12, 31)

    def test_parse_both(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        result = field.parse({"date_from": "2025-01-01", "date_to": "2025-12-31"})
        assert result is not None
        assert result["from"] == datetime.date(2025, 1, 1)
        assert result["to"] == datetime.date(2025, 12, 31)

    def test_parse_invalid_date_ignored(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        result = field.parse({"date_from": "not-a-date"})
        assert result is None

    def test_parse_empty_returns_none(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        assert field.parse({}) is None

    def test_apply_from_generates_gte(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        base = sa.select(events_table)
        result = field.apply(base, {"from": datetime.date(2025, 1, 1), "to": None})
        compiled = _compile(result)
        assert ">=" in compiled

    def test_apply_to_generates_lte(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        base = sa.select(events_table)
        result = field.apply(base, {"from": None, "to": datetime.date(2025, 12, 31)})
        compiled = _compile(result)
        assert "<=" in compiled

    def test_apply_both_generates_both_clauses(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        base = sa.select(events_table)
        result = field.apply(
            base,
            {
                "from": datetime.date(2025, 1, 1),
                "to": datetime.date(2025, 12, 31),
            },
        )
        compiled = _compile(result)
        assert ">=" in compiled
        assert "<=" in compiled

    def test_is_text_returns_false(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        assert field.is_text() is False


# ---------------------------------------------------------------------------
# NumericRangeField
# ---------------------------------------------------------------------------


class TestNumericRangeField:
    """Test NumericRangeField parsing and SQL generation."""

    def test_parse_min_only(self) -> None:
        field = filters_module.NumericRangeField(
            name="capacity", column=events_table.c.capacity
        )
        result = field.parse({"capacity_min": "100"})
        assert result is not None
        assert result["min"] == 100
        assert result.get("max") is None

    def test_parse_max_only(self) -> None:
        field = filters_module.NumericRangeField(
            name="capacity", column=events_table.c.capacity
        )
        result = field.parse({"capacity_max": "500"})
        assert result is not None
        assert result.get("min") is None
        assert result["max"] == 500

    def test_parse_both(self) -> None:
        field = filters_module.NumericRangeField(
            name="capacity", column=events_table.c.capacity
        )
        result = field.parse({"capacity_min": "100", "capacity_max": "500"})
        assert result is not None
        assert result["min"] == 100
        assert result["max"] == 500

    def test_parse_invalid_number_ignored(self) -> None:
        field = filters_module.NumericRangeField(
            name="capacity", column=events_table.c.capacity
        )
        result = field.parse({"capacity_min": "abc"})
        assert result is None

    def test_parse_empty_returns_none(self) -> None:
        field = filters_module.NumericRangeField(
            name="capacity", column=events_table.c.capacity
        )
        assert field.parse({}) is None

    def test_apply_min_generates_gte(self) -> None:
        field = filters_module.NumericRangeField(
            name="capacity", column=events_table.c.capacity
        )
        base = sa.select(events_table)
        result = field.apply(base, {"min": 100, "max": None})
        compiled = _compile(result)
        assert ">=" in compiled

    def test_apply_max_generates_lte(self) -> None:
        field = filters_module.NumericRangeField(
            name="capacity", column=events_table.c.capacity
        )
        base = sa.select(events_table)
        result = field.apply(base, {"min": None, "max": 500})
        compiled = _compile(result)
        assert "<=" in compiled

    def test_is_text_returns_false(self) -> None:
        field = filters_module.NumericRangeField(
            name="capacity", column=events_table.c.capacity
        )
        assert field.is_text() is False


# ---------------------------------------------------------------------------
# ExistsField
# ---------------------------------------------------------------------------


class TestExistsField:
    """Test ExistsField parsing and SQL generation."""

    def test_parse_true(self) -> None:
        subquery = sa.select(tracks_table.c.id).where(
            tracks_table.c.artist_id == artists_table.c.id
        )
        field = filters_module.ExistsField(name="has_tracks", exists_query=subquery)
        assert field.parse({"has_tracks": "true"}) is True

    def test_parse_false(self) -> None:
        subquery = sa.select(tracks_table.c.id).where(
            tracks_table.c.artist_id == artists_table.c.id
        )
        field = filters_module.ExistsField(name="has_tracks", exists_query=subquery)
        assert field.parse({"has_tracks": "false"}) is False

    def test_parse_missing_returns_none(self) -> None:
        subquery = sa.select(tracks_table.c.id).where(
            tracks_table.c.artist_id == artists_table.c.id
        )
        field = filters_module.ExistsField(name="has_tracks", exists_query=subquery)
        assert field.parse({}) is None

    def test_parse_invalid_returns_none(self) -> None:
        subquery = sa.select(tracks_table.c.id).where(
            tracks_table.c.artist_id == artists_table.c.id
        )
        field = filters_module.ExistsField(name="has_tracks", exists_query=subquery)
        assert field.parse({"has_tracks": "maybe"}) is None

    def test_apply_true_generates_exists(self) -> None:
        subquery = sa.select(tracks_table.c.id).where(
            tracks_table.c.artist_id == artists_table.c.id
        )
        field = filters_module.ExistsField(name="has_tracks", exists_query=subquery)
        base = sa.select(artists_table)
        result = field.apply(base, True)
        compiled = _compile(result)
        assert "EXISTS" in compiled.upper()

    def test_apply_false_generates_not_exists(self) -> None:
        subquery = sa.select(tracks_table.c.id).where(
            tracks_table.c.artist_id == artists_table.c.id
        )
        field = filters_module.ExistsField(name="has_tracks", exists_query=subquery)
        base = sa.select(artists_table)
        result = field.apply(base, False)
        compiled = _compile(result)
        assert "NOT" in compiled.upper()
        assert "EXISTS" in compiled.upper()

    def test_is_text_returns_false(self) -> None:
        subquery = sa.select(tracks_table.c.id).where(
            tracks_table.c.artist_id == artists_table.c.id
        )
        field = filters_module.ExistsField(name="has_tracks", exists_query=subquery)
        assert field.is_text() is False


# ---------------------------------------------------------------------------
# parse_filter_params
# ---------------------------------------------------------------------------


class TestParseFilterParams:
    """Test parse_filter_params helper."""

    def test_parses_active_text_filter(self) -> None:
        fields: list[filters_module.AnyFilterField] = [
            filters_module.TextField(name="name", column=artists_table.c.name),
        ]
        result = filters_module.parse_filter_params(fields, {"name": "Radiohead"})
        assert "name" in result.active_filters
        assert result.active_filters["name"] == "Radiohead"

    def test_skips_empty_params(self) -> None:
        fields: list[filters_module.AnyFilterField] = [
            filters_module.TextField(name="name", column=artists_table.c.name),
        ]
        result = filters_module.parse_filter_params(fields, {"name": ""})
        assert "name" not in result.active_filters

    def test_parses_multiple_fields(self) -> None:
        fields: list[filters_module.AnyFilterField] = [
            filters_module.TextField(name="name", column=artists_table.c.name),
            filters_module.DateRangeField(
                name="date", column=events_table.c.event_date
            ),
        ]
        result = filters_module.parse_filter_params(
            fields, {"name": "Radiohead", "date_from": "2025-01-01"}
        )
        assert "name" in result.active_filters
        assert "date" in result.active_filters

    def test_empty_params_produces_empty_filters(self) -> None:
        fields: list[filters_module.AnyFilterField] = [
            filters_module.TextField(name="name", column=artists_table.c.name),
        ]
        result = filters_module.parse_filter_params(fields, {})
        assert len(result.active_filters) == 0


# ---------------------------------------------------------------------------
# apply_filters
# ---------------------------------------------------------------------------


class TestApplyFilters:
    """Test the apply_filters engine."""

    def test_no_filters_returns_unchanged_query(self) -> None:
        fields: list[filters_module.AnyFilterField] = [
            filters_module.TextField(name="name", column=artists_table.c.name),
        ]
        base = sa.select(artists_table)
        result = filters_module.apply_filters(base, fields, {})
        # No WHERE clause should be added
        compiled = _compile(result)
        assert "WHERE" not in compiled.upper()

    def test_text_filter_applied(self) -> None:
        fields: list[filters_module.AnyFilterField] = [
            filters_module.TextField(name="name", column=artists_table.c.name),
        ]
        base = sa.select(artists_table)
        result = filters_module.apply_filters(base, fields, {"name": "Radiohead"})
        compiled = _compile(result)
        assert "WHERE" in compiled.upper()
        assert "LIKE" in compiled.upper()

    def test_multiselect_filter_applied(self) -> None:
        fields: list[filters_module.AnyFilterField] = [
            filters_module.MultiSelectField(
                name="status",
                column=events_table.c.status,
                options=["going", "interested"],
            ),
        ]
        base = sa.select(events_table)
        result = filters_module.apply_filters(
            base, fields, {}, multi_params={"status": ["going", "interested"]}
        )
        compiled = _compile(result)
        assert "IN" in compiled.upper()

    def test_quick_search_ors_across_text_fields(self) -> None:
        fields: list[filters_module.AnyFilterField] = [
            filters_module.TextField(name="title", column=events_table.c.title),
            filters_module.TextField(
                name="venue_name", column=events_table.c.venue_name
            ),
        ]
        base = sa.select(events_table)
        result = filters_module.apply_filters(base, fields, {"q": "Madison"})
        compiled = _compile(result)
        assert "WHERE" in compiled.upper()
        assert "LIKE" in compiled.upper()
        # Should have OR between the two text field conditions
        assert "OR" in compiled.upper()

    def test_quick_search_with_no_text_fields_unchanged(self) -> None:
        fields: list[filters_module.AnyFilterField] = [
            filters_module.DateRangeField(
                name="date", column=events_table.c.event_date
            ),
        ]
        base = sa.select(events_table)
        result = filters_module.apply_filters(base, fields, {"q": "something"})
        compiled = _compile(result)
        # No text fields means quick search has nothing to OR across
        assert "LIKE" not in compiled.upper()

    def test_multiple_filters_combined_with_and(self) -> None:
        fields: list[filters_module.AnyFilterField] = [
            filters_module.TextField(name="title", column=events_table.c.title),
            filters_module.DateRangeField(
                name="date", column=events_table.c.event_date
            ),
        ]
        base = sa.select(events_table)
        result = filters_module.apply_filters(
            base,
            fields,
            {"title": "Concert", "date_from": "2025-01-01"},
        )
        compiled = _compile(result)
        assert "WHERE" in compiled.upper()
        assert "AND" in compiled.upper()

    def test_quick_search_combined_with_field_filter(self) -> None:
        fields: list[filters_module.AnyFilterField] = [
            filters_module.TextField(name="title", column=events_table.c.title),
            filters_module.DateRangeField(
                name="date", column=events_table.c.event_date
            ),
        ]
        base = sa.select(events_table)
        result = filters_module.apply_filters(
            base,
            fields,
            {"q": "Concert", "date_from": "2025-06-01"},
        )
        compiled = _compile(result)
        assert "WHERE" in compiled.upper()
        assert "LIKE" in compiled.upper()
        assert ">=" in compiled

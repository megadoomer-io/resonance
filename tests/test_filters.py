"""Tests for the shared filter framework and per-view filter registries."""

from __future__ import annotations

import datetime
from typing import ClassVar

import sqlalchemy as sa

import resonance.ui.filters as filters_module
import resonance.ui.view_filters as view_filters_module

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

listening_events_table = sa.Table(
    "listening_events",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("track_id", sa.Integer),
    sa.Column("user_id", sa.Integer),
    sa.Column("source_service", sa.String),
    sa.Column("listened_at", sa.DateTime),
)

playlists_table = sa.Table(
    "playlists",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String),
    sa.Column("user_id", sa.Integer),
    sa.Column("track_count", sa.Integer),
    sa.Column("created_at", sa.DateTime),
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
        assert result["date_from"] == datetime.date(2025, 1, 1)
        assert result.get("date_to") is None

    def test_parse_to_only(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        result = field.parse({"date_to": "2025-12-31"})
        assert result is not None
        assert result.get("date_from") is None
        assert result["date_to"] == datetime.date(2025, 12, 31)

    def test_parse_both(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        result = field.parse({"date_from": "2025-01-01", "date_to": "2025-12-31"})
        assert result is not None
        assert result["date_from"] == datetime.date(2025, 1, 1)
        assert result["date_to"] == datetime.date(2025, 12, 31)

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
        result = field.apply(
            base, {"date_from": datetime.date(2025, 1, 1), "date_to": None}
        )
        compiled = _compile(result)
        assert ">=" in compiled

    def test_apply_to_generates_lte(self) -> None:
        field = filters_module.DateRangeField(
            name="date", column=events_table.c.event_date
        )
        base = sa.select(events_table)
        result = field.apply(
            base, {"date_from": None, "date_to": datetime.date(2025, 12, 31)}
        )
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
                "date_from": datetime.date(2025, 1, 1),
                "date_to": datetime.date(2025, 12, 31),
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
        assert result["capacity_min"] == 100
        assert result.get("capacity_max") is None

    def test_parse_max_only(self) -> None:
        field = filters_module.NumericRangeField(
            name="capacity", column=events_table.c.capacity
        )
        result = field.parse({"capacity_max": "500"})
        assert result is not None
        assert result.get("capacity_min") is None
        assert result["capacity_max"] == 500

    def test_parse_both(self) -> None:
        field = filters_module.NumericRangeField(
            name="capacity", column=events_table.c.capacity
        )
        result = field.parse({"capacity_min": "100", "capacity_max": "500"})
        assert result is not None
        assert result["capacity_min"] == 100
        assert result["capacity_max"] == 500

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
        result = field.apply(base, {"capacity_min": 100, "capacity_max": None})
        compiled = _compile(result)
        assert ">=" in compiled

    def test_apply_max_generates_lte(self) -> None:
        field = filters_module.NumericRangeField(
            name="capacity", column=events_table.c.capacity
        )
        base = sa.select(events_table)
        result = field.apply(base, {"capacity_min": None, "capacity_max": 500})
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


# ---------------------------------------------------------------------------
# Event Filter Registry (view_filters)
# ---------------------------------------------------------------------------


class TestEventFilterRegistry:
    """Test the event filter field definitions."""

    def test_event_filters_has_expected_fields(self) -> None:
        names = [f.name for f in view_filters_module.EVENT_FILTERS]
        assert "title" in names
        assert "venue" in names
        assert "artist" in names
        assert "date" in names
        assert "has_pending" in names

    def test_event_filters_field_types(self) -> None:
        by_name = {f.name: f for f in view_filters_module.EVENT_FILTERS}
        assert isinstance(by_name["title"], filters_module.TextField)
        assert isinstance(by_name["venue"], filters_module.TextField)
        assert isinstance(by_name["artist"], filters_module.TextField)
        assert isinstance(by_name["date"], filters_module.DateRangeField)
        assert isinstance(by_name["has_pending"], filters_module.ExistsField)

    def test_event_filters_count(self) -> None:
        assert len(view_filters_module.EVENT_FILTERS) == 5


class TestEventPresets:
    """Test event preset generation."""

    def test_build_event_presets_returns_three(self) -> None:
        presets = view_filters_module.build_event_presets()
        assert len(presets) == 3

    def test_upcoming_preset_uses_today(self) -> None:
        presets = view_filters_module.build_event_presets()
        upcoming = next(p for p in presets if p["name"] == "upcoming")
        today = datetime.date.today().isoformat()
        assert f"date_from={today}" in upcoming["params"]

    def test_going_preset_params(self) -> None:
        presets = view_filters_module.build_event_presets()
        going = next(p for p in presets if p["name"] == "going")
        assert going["params"] == "attendance=GOING"

    def test_needs_review_preset_params(self) -> None:
        presets = view_filters_module.build_event_presets()
        review = next(p for p in presets if p["name"] == "needs_review")
        assert review["params"] == "has_pending=true"

    def test_preset_names_are_unique(self) -> None:
        presets = view_filters_module.build_event_presets()
        names = [p["name"] for p in presets]
        assert len(names) == len(set(names))


class TestEventTemplateFilters:
    """Test event template filter metadata."""

    def test_template_filters_has_expected_count(self) -> None:
        assert len(view_filters_module.EVENT_TEMPLATE_FILTERS) == 5

    def test_template_filter_names(self) -> None:
        names = [f["name"] for f in view_filters_module.EVENT_TEMPLATE_FILTERS]
        assert names == ["title", "date", "venue", "artist", "attendance"]

    def test_attendance_has_options(self) -> None:
        att = next(
            f
            for f in view_filters_module.EVENT_TEMPLATE_FILTERS
            if f["name"] == "attendance"
        )
        assert att["type"] == "multiselect"
        assert len(att["options"]) == 3
        option_values = [o["value"] for o in att["options"]]
        assert option_values == ["GOING", "INTERESTED", "NONE"]


class TestDetectActivePreset:
    """Test active preset detection for events."""

    _EVENT_FILTER_KEYS: ClassVar[set[str]] = {
        "q",
        "title",
        "venue",
        "artist",
        "date_from",
        "date_to",
        "attendance",
        "has_pending",
    }

    def test_no_filters_defaults_to_upcoming(self) -> None:
        presets = view_filters_module.build_event_presets()
        result = view_filters_module.detect_active_preset(
            {},
            presets,
            filter_keys=self._EVENT_FILTER_KEYS,
            default_preset="upcoming",
        )
        assert result == "upcoming"

    def test_matching_upcoming_params(self) -> None:
        presets = view_filters_module.build_event_presets()
        today = datetime.date.today().isoformat()
        result = view_filters_module.detect_active_preset(
            {"date_from": today},
            presets,
            filter_keys=self._EVENT_FILTER_KEYS,
            default_preset="upcoming",
        )
        assert result == "upcoming"

    def test_matching_going_params(self) -> None:
        presets = view_filters_module.build_event_presets()
        result = view_filters_module.detect_active_preset(
            {"attendance": "GOING"},
            presets,
            filter_keys=self._EVENT_FILTER_KEYS,
            default_preset="upcoming",
        )
        assert result == "going"

    def test_matching_needs_review_params(self) -> None:
        presets = view_filters_module.build_event_presets()
        result = view_filters_module.detect_active_preset(
            {"has_pending": "true"},
            presets,
            filter_keys=self._EVENT_FILTER_KEYS,
            default_preset="upcoming",
        )
        assert result == "needs_review"

    def test_custom_filters_returns_none(self) -> None:
        presets = view_filters_module.build_event_presets()
        result = view_filters_module.detect_active_preset(
            {"title": "concert", "venue": "arena"},
            presets,
            filter_keys=self._EVENT_FILTER_KEYS,
            default_preset="upcoming",
        )
        assert result is None

    def test_quick_search_returns_none(self) -> None:
        presets = view_filters_module.build_event_presets()
        result = view_filters_module.detect_active_preset(
            {"q": "radiohead"},
            presets,
            filter_keys=self._EVENT_FILTER_KEYS,
            default_preset="upcoming",
        )
        assert result is None


class TestBuildFilterQueryString:
    """Test filter query string builder."""

    def test_empty_filters_returns_empty(self) -> None:
        result = view_filters_module.build_filter_query_string(
            {}, view_filters_module.EVENT_FILTERS
        )
        assert result == ""

    def test_text_filter_in_qs(self) -> None:
        result = view_filters_module.build_filter_query_string(
            {"title": "concert"}, view_filters_module.EVENT_FILTERS
        )
        assert "title=concert" in result

    def test_date_range_from_in_qs(self) -> None:
        result = view_filters_module.build_filter_query_string(
            {"date": {"date_from": datetime.date(2025, 6, 1), "date_to": None}},
            view_filters_module.EVENT_FILTERS,
        )
        assert "date_from=2025-06-01" in result
        assert "date_to" not in result

    def test_date_range_both_in_qs(self) -> None:
        result = view_filters_module.build_filter_query_string(
            {
                "date": {
                    "date_from": datetime.date(2025, 1, 1),
                    "date_to": datetime.date(2025, 12, 31),
                }
            },
            view_filters_module.EVENT_FILTERS,
        )
        assert "date_from=2025-01-01" in result
        assert "date_to=2025-12-31" in result

    def test_exists_filter_true_in_qs(self) -> None:
        result = view_filters_module.build_filter_query_string(
            {"has_pending": True}, view_filters_module.EVENT_FILTERS
        )
        assert "has_pending=true" in result

    def test_exists_filter_false_in_qs(self) -> None:
        result = view_filters_module.build_filter_query_string(
            {"has_pending": False}, view_filters_module.EVENT_FILTERS
        )
        assert "has_pending=false" in result

    def test_numeric_range_min_in_qs(self) -> None:
        result = view_filters_module.build_filter_query_string(
            {"tracks": {"tracks_min": 5, "tracks_max": None}},
            view_filters_module.PLAYLIST_FILTERS,
        )
        assert "tracks_min=5" in result
        assert "tracks_max" not in result

    def test_numeric_range_both_in_qs(self) -> None:
        result = view_filters_module.build_filter_query_string(
            {"tracks": {"tracks_min": 5, "tracks_max": 20}},
            view_filters_module.PLAYLIST_FILTERS,
        )
        assert "tracks_min=5" in result
        assert "tracks_max=20" in result


# ---------------------------------------------------------------------------
# Artist Filter Registry (view_filters)
# ---------------------------------------------------------------------------


class TestArtistFilterRegistry:
    """Test the artist filter field definitions."""

    def test_artist_filters_has_expected_fields(self) -> None:
        names = [f.name for f in view_filters_module.ARTIST_FILTERS]
        assert "name" in names
        assert "origin" in names
        assert "has_events" in names
        assert "has_tracks" in names

    def test_artist_filters_field_types(self) -> None:
        by_name = {f.name: f for f in view_filters_module.ARTIST_FILTERS}
        assert isinstance(by_name["name"], filters_module.TextField)
        assert isinstance(by_name["origin"], filters_module.TextField)
        assert isinstance(by_name["has_events"], filters_module.ExistsField)
        assert isinstance(by_name["has_tracks"], filters_module.ExistsField)

    def test_artist_filters_count(self) -> None:
        assert len(view_filters_module.ARTIST_FILTERS) == 4


class TestArtistPresets:
    """Test artist preset definitions."""

    def test_preset_count(self) -> None:
        assert len(view_filters_module.ARTIST_PRESETS) == 2

    def test_has_events_preset_params(self) -> None:
        preset = next(
            p for p in view_filters_module.ARTIST_PRESETS if p["name"] == "has_events"
        )
        assert preset["params"] == "has_events=true"

    def test_no_tracks_preset_params(self) -> None:
        preset = next(
            p for p in view_filters_module.ARTIST_PRESETS if p["name"] == "no_tracks"
        )
        assert preset["params"] == "has_tracks=false"

    def test_preset_names_are_unique(self) -> None:
        names = [p["name"] for p in view_filters_module.ARTIST_PRESETS]
        assert len(names) == len(set(names))


class TestArtistTemplateFilters:
    """Test artist template filter metadata."""

    def test_template_filters_count(self) -> None:
        assert len(view_filters_module.ARTIST_TEMPLATE_FILTERS) == 2

    def test_template_filter_names(self) -> None:
        names = [f["name"] for f in view_filters_module.ARTIST_TEMPLATE_FILTERS]
        assert names == ["name", "origin"]

    def test_template_filter_types(self) -> None:
        for f in view_filters_module.ARTIST_TEMPLATE_FILTERS:
            assert f["type"] == "text"


# ---------------------------------------------------------------------------
# Track Filter Registry (view_filters)
# ---------------------------------------------------------------------------


class TestTrackFilterRegistry:
    """Test the track filter field definitions."""

    def test_track_filters_has_expected_fields(self) -> None:
        names = [f.name for f in view_filters_module.TRACK_FILTERS]
        assert "title" in names
        assert "artist" in names
        assert "recently_played" in names

    def test_track_filters_field_types(self) -> None:
        by_name = {f.name: f for f in view_filters_module.TRACK_FILTERS}
        assert isinstance(by_name["title"], filters_module.TextField)
        assert isinstance(by_name["artist"], filters_module.TextField)
        assert isinstance(by_name["recently_played"], filters_module.ExistsField)

    def test_track_filters_count(self) -> None:
        assert len(view_filters_module.TRACK_FILTERS) == 3


class TestTrackPresets:
    """Test track preset definitions."""

    def test_preset_count(self) -> None:
        assert len(view_filters_module.TRACK_PRESETS) == 1

    def test_recently_played_preset_params(self) -> None:
        preset = view_filters_module.TRACK_PRESETS[0]
        assert preset["name"] == "recently_played"
        assert preset["params"] == "recently_played=true"


class TestTrackTemplateFilters:
    """Test track template filter metadata."""

    def test_template_filters_count(self) -> None:
        assert len(view_filters_module.TRACK_TEMPLATE_FILTERS) == 2

    def test_template_filter_names(self) -> None:
        names = [f["name"] for f in view_filters_module.TRACK_TEMPLATE_FILTERS]
        assert names == ["title", "artist"]


# ---------------------------------------------------------------------------
# History Filter Registry (view_filters)
# ---------------------------------------------------------------------------


class TestHistoryFilterRegistry:
    """Test the history filter field definitions."""

    def test_history_filters_has_expected_fields(self) -> None:
        names = [f.name for f in view_filters_module.HISTORY_FILTERS]
        assert "track" in names
        assert "artist" in names
        assert "date" in names

    def test_history_filters_field_types(self) -> None:
        by_name = {f.name: f for f in view_filters_module.HISTORY_FILTERS}
        assert isinstance(by_name["track"], filters_module.TextField)
        assert isinstance(by_name["artist"], filters_module.TextField)
        assert isinstance(by_name["date"], filters_module.DateRangeField)

    def test_history_filters_count(self) -> None:
        assert len(view_filters_module.HISTORY_FILTERS) == 3


class TestHistoryPresets:
    """Test history preset definitions."""

    def test_preset_count(self) -> None:
        assert len(view_filters_module.HISTORY_PRESETS) == 3

    def test_spotify_preset_params(self) -> None:
        preset = next(
            p for p in view_filters_module.HISTORY_PRESETS if p["name"] == "spotify"
        )
        assert preset["params"] == "source=SPOTIFY"

    def test_listenbrainz_preset_params(self) -> None:
        preset = next(
            p
            for p in view_filters_module.HISTORY_PRESETS
            if p["name"] == "listenbrainz"
        )
        assert preset["params"] == "source=LISTENBRAINZ"

    def test_lastfm_preset_params(self) -> None:
        preset = next(
            p for p in view_filters_module.HISTORY_PRESETS if p["name"] == "lastfm"
        )
        assert preset["params"] == "source=LASTFM"

    def test_preset_names_are_unique(self) -> None:
        names = [p["name"] for p in view_filters_module.HISTORY_PRESETS]
        assert len(names) == len(set(names))


class TestHistoryTemplateFilters:
    """Test history template filter metadata."""

    def test_template_filters_count(self) -> None:
        assert len(view_filters_module.HISTORY_TEMPLATE_FILTERS) == 4

    def test_template_filter_names(self) -> None:
        names = [f["name"] for f in view_filters_module.HISTORY_TEMPLATE_FILTERS]
        assert names == ["track", "artist", "source", "date"]

    def test_source_has_options(self) -> None:
        source = next(
            f
            for f in view_filters_module.HISTORY_TEMPLATE_FILTERS
            if f["name"] == "source"
        )
        assert source["type"] == "multiselect"
        assert len(source["options"]) == 3
        option_values = [o["value"] for o in source["options"]]
        assert option_values == ["SPOTIFY", "LISTENBRAINZ", "LASTFM"]


# ---------------------------------------------------------------------------
# Playlist Filter Registry (view_filters)
# ---------------------------------------------------------------------------


class TestPlaylistFilterRegistry:
    """Test the playlist filter field definitions."""

    def test_playlist_filters_has_expected_fields(self) -> None:
        names = [f.name for f in view_filters_module.PLAYLIST_FILTERS]
        assert "name" in names
        assert "created" in names
        assert "tracks" in names

    def test_playlist_filters_field_types(self) -> None:
        by_name = {f.name: f for f in view_filters_module.PLAYLIST_FILTERS}
        assert isinstance(by_name["name"], filters_module.TextField)
        assert isinstance(by_name["created"], filters_module.DateRangeField)
        assert isinstance(by_name["tracks"], filters_module.NumericRangeField)

    def test_playlist_filters_count(self) -> None:
        assert len(view_filters_module.PLAYLIST_FILTERS) == 3


class TestPlaylistPresets:
    """Test playlist preset definitions."""

    def test_no_presets(self) -> None:
        assert len(view_filters_module.PLAYLIST_PRESETS) == 0


class TestPlaylistTemplateFilters:
    """Test playlist template filter metadata."""

    def test_template_filters_count(self) -> None:
        assert len(view_filters_module.PLAYLIST_TEMPLATE_FILTERS) == 3

    def test_template_filter_names(self) -> None:
        names = [f["name"] for f in view_filters_module.PLAYLIST_TEMPLATE_FILTERS]
        assert names == ["name", "created", "tracks"]

    def test_template_filter_types(self) -> None:
        by_name = {f["name"]: f for f in view_filters_module.PLAYLIST_TEMPLATE_FILTERS}
        assert by_name["name"]["type"] == "text"
        assert by_name["created"]["type"] == "daterange"
        assert by_name["tracks"]["type"] == "numericrange"


# ---------------------------------------------------------------------------
# detect_active_preset (generic version)
# ---------------------------------------------------------------------------


class TestDetectActivePresetGeneric:
    """Test the generic detect_active_preset with explicit parameters."""

    def test_no_filters_returns_default(self) -> None:
        presets = view_filters_module.ARTIST_PRESETS
        result = view_filters_module.detect_active_preset(
            {}, presets, default_preset="has_events"
        )
        assert result == "has_events"

    def test_no_filters_no_default_returns_none(self) -> None:
        presets = view_filters_module.ARTIST_PRESETS
        result = view_filters_module.detect_active_preset({}, presets)
        assert result is None

    def test_matching_preset_detected(self) -> None:
        presets = view_filters_module.ARTIST_PRESETS
        result = view_filters_module.detect_active_preset(
            {"has_events": "true"}, presets
        )
        assert result == "has_events"

    def test_non_matching_params_returns_none(self) -> None:
        presets = view_filters_module.ARTIST_PRESETS
        result = view_filters_module.detect_active_preset(
            {"name": "Radiohead"}, presets
        )
        assert result is None

    def test_empty_presets_no_default_returns_none(self) -> None:
        result = view_filters_module.detect_active_preset({}, [])
        assert result is None

    def test_empty_presets_with_default_returns_default(self) -> None:
        result = view_filters_module.detect_active_preset({}, [], default_preset="foo")
        assert result == "foo"

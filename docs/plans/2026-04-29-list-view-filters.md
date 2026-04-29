# List View Search & Filtering Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add search, per-column filtering, and preset filter sets to all five list views.

**Architecture:** A shared filter framework defines field types (text, multi-select, date range, numeric range, exists) and an `apply_filters` engine that builds SQLAlchemy `.where()` clauses from query params. Each view declares its filterable fields as a registry. Templates use shared filter bar partials with HTMX for interactivity.

**Tech Stack:** Python/FastAPI, SQLAlchemy 2.0 async, Jinja2, HTMX, Pico CSS.

**Design doc:** `docs/plans/2026-04-29-list-view-filters-design.md`

---

## Task 1: Filter Framework — Field Type Definitions

Create the filter framework module with field type dataclasses.

**Files:**
- Create: `src/resonance/ui/filters.py`
- Test: `tests/test_filters.py`

**Step 1: Write the failing test**

```python
# tests/test_filters.py
"""Tests for the UI filter framework."""

from __future__ import annotations

import datetime
import uuid
from typing import Any

import pytest
import sqlalchemy as sa

from resonance.ui.filters import (
    AppliedFilters,
    DateRangeField,
    ExistsField,
    FilterField,
    MultiSelectField,
    NumericRangeField,
    TextField,
    apply_filters,
    parse_filter_params,
)


class FakeQueryParams:
    """Simulates Starlette QueryParams for testing."""

    def __init__(self, data: dict[str, str | list[str]]) -> None:
        self._data = data

    def get(self, key: str, default: str | None = None) -> str | None:
        val = self._data.get(key, default)
        if isinstance(val, list):
            return val[0] if val else default
        return val

    def getlist(self, key: str) -> list[str]:
        val = self._data.get(key, [])
        if isinstance(val, list):
            return val
        return [val]


# -- Test models for filter tests --

metadata = sa.MetaData()

test_artists = sa.Table(
    "test_artists",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("name", sa.String(512)),
    sa.Column("origin", sa.String(256)),
)

test_events = sa.Table(
    "test_events",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("title", sa.String(512)),
    sa.Column("event_date", sa.Date),
)

test_event_artists = sa.Table(
    "test_event_artists",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("event_id", sa.Uuid, sa.ForeignKey("test_events.id")),
    sa.Column("artist_id", sa.Uuid, sa.ForeignKey("test_artists.id")),
)


class TestTextField:
    def test_applies_ilike_filter(self) -> None:
        field = TextField("name", test_artists.c.name)
        params = FakeQueryParams({"name": "red pears"})
        base = sa.select(test_artists)

        result = parse_filter_params([field], params)
        assert result.active_filters["name"] == "red pears"

    def test_escapes_ilike_wildcards(self) -> None:
        field = TextField("name", test_artists.c.name)
        params = FakeQueryParams({"name": "100%"})

        result = parse_filter_params([field], params)
        assert result.active_filters["name"] == "100%"

    def test_ignores_empty_string(self) -> None:
        field = TextField("name", test_artists.c.name)
        params = FakeQueryParams({"name": ""})

        result = parse_filter_params([field], params)
        assert "name" not in result.active_filters


class TestMultiSelectField:
    def test_parses_multiple_values(self) -> None:
        field = MultiSelectField(
            "status", test_artists.c.name, options=["going", "interested", "none"]
        )
        params = FakeQueryParams({"status": ["going", "interested"]})

        result = parse_filter_params([field], params)
        assert result.active_filters["status"] == ["going", "interested"]

    def test_rejects_invalid_options(self) -> None:
        field = MultiSelectField(
            "status", test_artists.c.name, options=["going", "interested"]
        )
        params = FakeQueryParams({"status": ["going", "hacked"]})

        result = parse_filter_params([field], params)
        assert result.active_filters["status"] == ["going"]

    def test_ignores_when_no_values(self) -> None:
        field = MultiSelectField(
            "status", test_artists.c.name, options=["going", "interested"]
        )
        params = FakeQueryParams({})

        result = parse_filter_params([field], params)
        assert "status" not in result.active_filters


class TestDateRangeField:
    def test_parses_from_and_to(self) -> None:
        field = DateRangeField("date", test_events.c.event_date)
        params = FakeQueryParams(
            {"date_from": "2026-05-01", "date_to": "2026-06-01"}
        )

        result = parse_filter_params([field], params)
        assert result.active_filters["date_from"] == "2026-05-01"
        assert result.active_filters["date_to"] == "2026-06-01"

    def test_parses_from_only(self) -> None:
        field = DateRangeField("date", test_events.c.event_date)
        params = FakeQueryParams({"date_from": "2026-05-01"})

        result = parse_filter_params([field], params)
        assert result.active_filters["date_from"] == "2026-05-01"
        assert "date_to" not in result.active_filters

    def test_ignores_invalid_dates(self) -> None:
        field = DateRangeField("date", test_events.c.event_date)
        params = FakeQueryParams({"date_from": "not-a-date"})

        result = parse_filter_params([field], params)
        assert "date_from" not in result.active_filters


class TestApplyFilters:
    def test_text_filter_adds_where_clause(self) -> None:
        field = TextField("name", test_artists.c.name)
        params = FakeQueryParams({"name": "pears"})
        base = sa.select(test_artists)

        filtered = apply_filters(base, [field], params)
        compiled = filtered.compile(compile_kwargs={"literal_binds": True})
        sql = str(compiled)
        assert "LIKE" in sql.upper()

    def test_no_params_returns_unchanged_query(self) -> None:
        field = TextField("name", test_artists.c.name)
        params = FakeQueryParams({})
        base = sa.select(test_artists)

        filtered = apply_filters(base, [field], params)
        assert str(filtered.compile()) == str(base.compile())

    def test_quick_search_matches_across_text_fields(self) -> None:
        fields: list[FilterField] = [
            TextField("name", test_artists.c.name),
            TextField("origin", test_artists.c.origin),
        ]
        params = FakeQueryParams({"q": "california"})
        base = sa.select(test_artists)

        filtered = apply_filters(base, fields, params)
        compiled = str(filtered.compile(compile_kwargs={"literal_binds": True}))
        # q should OR across text fields
        assert "OR" in compiled.upper()

    def test_column_filters_and_together(self) -> None:
        fields: list[FilterField] = [
            TextField("name", test_artists.c.name),
            TextField("origin", test_artists.c.origin),
        ]
        params = FakeQueryParams({"name": "pears", "origin": "LA"})
        base = sa.select(test_artists)

        filtered = apply_filters(base, fields, params)
        compiled = str(filtered.compile(compile_kwargs={"literal_binds": True}))
        assert "AND" in compiled.upper()
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_filters.py -v`
Expected: ImportError — `resonance.ui.filters` does not exist

**Step 3: Write minimal implementation**

```python
# src/resonance/ui/filters.py
"""Shared filter framework for list views.

Each view declares filterable fields as a registry. The apply_filters
engine builds SQLAlchemy .where() clauses from query params.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, Union

import sqlalchemy as sa

if TYPE_CHECKING:
    from starlette.datastructures import QueryParams as StarletteQueryParams


def _escape_ilike(value: str) -> str:
    return value.replace("%", r"\%").replace("_", r"\_")


@dataclass
class AppliedFilters:
    """Parsed filter state from query params."""

    active_filters: dict[str, Any] = field(default_factory=dict)


class FilterField(Protocol):
    name: str

    def parse(self, params: Any) -> dict[str, Any]: ...

    def apply(self, query: sa.Select[Any], values: dict[str, Any]) -> sa.Select[Any]: ...

    def is_text(self) -> bool: ...


@dataclass
class TextField:
    name: str
    column: sa.ColumnElement[Any]
    join: Any | None = None

    def parse(self, params: Any) -> dict[str, Any]:
        val = params.get(self.name)
        if val and val.strip():
            return {self.name: val.strip()}
        return {}

    def apply(
        self, query: sa.Select[Any], values: dict[str, Any]
    ) -> sa.Select[Any]:
        val = values.get(self.name)
        if val:
            escaped = _escape_ilike(val)
            query = query.where(self.column.ilike(f"%{escaped}%"))
        return query

    def apply_quick_search(
        self, value: str
    ) -> sa.ColumnElement[Any]:
        escaped = _escape_ilike(value)
        return self.column.ilike(f"%{escaped}%")

    def is_text(self) -> bool:
        return True


@dataclass
class MultiSelectField:
    name: str
    column: sa.ColumnElement[Any]
    options: list[str] = field(default_factory=list)

    def parse(self, params: Any) -> dict[str, Any]:
        raw = params.getlist(self.name)
        valid = [v for v in raw if v in self.options]
        if valid:
            return {self.name: valid}
        return {}

    def apply(
        self, query: sa.Select[Any], values: dict[str, Any]
    ) -> sa.Select[Any]:
        selected = values.get(self.name)
        if selected:
            query = query.where(self.column.in_(selected))
        return query

    def is_text(self) -> bool:
        return False


@dataclass
class DateRangeField:
    name: str
    column: sa.ColumnElement[Any]

    def parse(self, params: Any) -> dict[str, Any]:
        result: dict[str, Any] = {}
        from_val = params.get(f"{self.name}_from")
        to_val = params.get(f"{self.name}_to")
        if from_val:
            try:
                datetime.date.fromisoformat(from_val)
                result[f"{self.name}_from"] = from_val
            except ValueError:
                pass
        if to_val:
            try:
                datetime.date.fromisoformat(to_val)
                result[f"{self.name}_to"] = to_val
            except ValueError:
                pass
        return result

    def apply(
        self, query: sa.Select[Any], values: dict[str, Any]
    ) -> sa.Select[Any]:
        from_val = values.get(f"{self.name}_from")
        to_val = values.get(f"{self.name}_to")
        if from_val:
            query = query.where(
                self.column >= datetime.date.fromisoformat(from_val)
            )
        if to_val:
            query = query.where(
                self.column <= datetime.date.fromisoformat(to_val)
            )
        return query

    def is_text(self) -> bool:
        return False


@dataclass
class NumericRangeField:
    name: str
    column: sa.ColumnElement[Any]

    def parse(self, params: Any) -> dict[str, Any]:
        result: dict[str, Any] = {}
        min_val = params.get(f"{self.name}_min")
        max_val = params.get(f"{self.name}_max")
        if min_val:
            try:
                result[f"{self.name}_min"] = str(int(min_val))
            except ValueError:
                pass
        if max_val:
            try:
                result[f"{self.name}_max"] = str(int(max_val))
            except ValueError:
                pass
        return result

    def apply(
        self, query: sa.Select[Any], values: dict[str, Any]
    ) -> sa.Select[Any]:
        min_val = values.get(f"{self.name}_min")
        max_val = values.get(f"{self.name}_max")
        if min_val:
            query = query.where(self.column >= int(min_val))
        if max_val:
            query = query.where(self.column <= int(max_val))
        return query

    def is_text(self) -> bool:
        return False


@dataclass
class ExistsField:
    name: str
    subquery: sa.Exists

    def parse(self, params: Any) -> dict[str, Any]:
        val = params.get(self.name)
        if val in ("true", "false"):
            return {self.name: val}
        return {}

    def apply(
        self, query: sa.Select[Any], values: dict[str, Any]
    ) -> sa.Select[Any]:
        val = values.get(self.name)
        if val == "true":
            query = query.where(self.subquery)
        elif val == "false":
            query = query.where(~self.subquery)
        return query

    def is_text(self) -> bool:
        return False


AnyFilterField = Union[
    TextField, MultiSelectField, DateRangeField, NumericRangeField, ExistsField
]


def parse_filter_params(
    fields: list[AnyFilterField],
    params: Any,
) -> AppliedFilters:
    active: dict[str, Any] = {}
    for f in fields:
        active.update(f.parse(params))
    return AppliedFilters(active_filters=active)


def apply_filters(
    query: sa.Select[Any],
    fields: list[AnyFilterField],
    params: Any,
) -> sa.Select[Any]:
    parsed = parse_filter_params(fields, params)

    # Apply per-field column filters
    for f in fields:
        query = f.apply(query, parsed.active_filters)

    # Apply quick search (q param) — OR across all text fields
    q = params.get("q")
    if q and q.strip():
        q = q.strip()
        text_conditions = [
            f.apply_quick_search(q)
            for f in fields
            if isinstance(f, TextField)
        ]
        if text_conditions:
            query = query.where(sa.or_(*text_conditions))

    return query
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_filters.py -v`
Expected: All PASS

**Step 5: Run linters**

Run: `uv run ruff check src/resonance/ui/filters.py tests/test_filters.py && uv run mypy src/resonance/ui/filters.py`

**Step 6: Commit**

```bash
git add src/resonance/ui/filters.py tests/test_filters.py
git commit -m "feat: add shared filter framework for list views"
```

---

## Task 2: Filter Bar Template Partials

Create shared Jinja2 partials for the filter bar UI and supporting CSS.

**Files:**
- Create: `src/resonance/ui/templates/partials/filter_bar.html`
- Create: `src/resonance/ui/static/filters.css`
- Modify: `src/resonance/ui/templates/base.html` — add CSS link

**Step 1: Create the filter bar partial**

This partial is included by each list page template. It receives `presets`, `filters`, `active_filters`, `list_url`, and `list_target` as template variables.

```html
{# partials/filter_bar.html
   Shared filter bar: presets + quick search + collapsible column filters.

   Required vars:
     list_url     — base URL for the list (e.g., "/events")
     list_target  — HTMX target ID (e.g., "#event-list")
     presets      — list of {name, label, params} dicts
     filters      — list of {name, label, type, options?} dicts
     active_filters — dict of currently active filter values
     active_preset — name of active preset or None
#}

{# -- Presets + Quick Search Row -- #}
<div class="filter-bar">
  <div class="filter-presets">
    {% for preset in presets %}
    <button
      type="button"
      class="preset-btn{% if active_preset == preset.name %} preset-active{% endif %}"
      hx-get="{{ list_url }}?{{ preset.params }}&page=1"
      hx-target="{{ list_target }}"
      hx-swap="innerHTML"
      hx-push-url="true"
    >{{ preset.label }}</button>
    {% endfor %}
    {% if active_preset or active_filters|length > 0 %}
    <button
      type="button"
      class="preset-btn preset-clear"
      hx-get="{{ list_url }}?page=1"
      hx-target="{{ list_target }}"
      hx-swap="innerHTML"
      hx-push-url="true"
    >Clear</button>
    {% endif %}
  </div>
  <div class="filter-search">
    <input
      type="search"
      name="q"
      placeholder="Search..."
      value="{{ active_filters.get('q', '') }}"
      hx-get="{{ list_url }}"
      hx-target="{{ list_target }}"
      hx-swap="innerHTML"
      hx-trigger="input changed delay:300ms"
      hx-push-url="true"
      hx-include="[data-filter-input]"
      hx-vals='{"page": "1"}'
    />
  </div>
</div>

{# -- Collapsible Column Filters -- #}
{% if filters %}
<details class="filter-panel"{% if active_filters|length > (1 if active_filters.get('q') else 0) %} open{% endif %}>
  <summary>Filters</summary>
  <div class="filter-fields">
    {% for f in filters %}
    <div class="filter-field">
      <label for="filter-{{ f.name }}">{{ f.label }}</label>
      {% if f.type == "text" %}
      <input
        id="filter-{{ f.name }}"
        type="text"
        name="{{ f.name }}"
        value="{{ active_filters.get(f.name, '') }}"
        data-filter-input
        hx-get="{{ list_url }}"
        hx-target="{{ list_target }}"
        hx-swap="innerHTML"
        hx-trigger="input changed delay:300ms"
        hx-push-url="true"
        hx-include="[data-filter-input], .filter-search input"
        hx-vals='{"page": "1"}'
      />
      {% elif f.type == "multiselect" %}
      <details class="multiselect-dropdown" id="filter-{{ f.name }}">
        <summary>
          {% set selected = active_filters.get(f.name, []) %}
          {% if selected %}{{ selected|length }} selected{% else %}All{% endif %}
        </summary>
        <div class="multiselect-options">
          {% for opt in f.options %}
          <label>
            <input
              type="checkbox"
              name="{{ f.name }}"
              value="{{ opt.value }}"
              data-filter-input
              {% if opt.value in active_filters.get(f.name, []) %}checked{% endif %}
              hx-get="{{ list_url }}"
              hx-target="{{ list_target }}"
              hx-swap="innerHTML"
              hx-trigger="change"
              hx-push-url="true"
              hx-include="[data-filter-input], .filter-search input"
              hx-vals='{"page": "1"}'
            />
            {{ opt.label }}
          </label>
          {% endfor %}
        </div>
      </details>
      {% elif f.type == "daterange" %}
      <div class="daterange-inputs">
        <input
          type="date"
          name="{{ f.name }}_from"
          value="{{ active_filters.get(f.name ~ '_from', '') }}"
          data-filter-input
          hx-get="{{ list_url }}"
          hx-target="{{ list_target }}"
          hx-swap="innerHTML"
          hx-trigger="change"
          hx-push-url="true"
          hx-include="[data-filter-input], .filter-search input"
          hx-vals='{"page": "1"}'
        />
        <span>to</span>
        <input
          type="date"
          name="{{ f.name }}_to"
          value="{{ active_filters.get(f.name ~ '_to', '') }}"
          data-filter-input
          hx-get="{{ list_url }}"
          hx-target="{{ list_target }}"
          hx-swap="innerHTML"
          hx-trigger="change"
          hx-push-url="true"
          hx-include="[data-filter-input], .filter-search input"
          hx-vals='{"page": "1"}'
        />
      </div>
      {% elif f.type == "numericrange" %}
      <div class="numrange-inputs">
        <input
          type="number"
          name="{{ f.name }}_min"
          placeholder="Min"
          value="{{ active_filters.get(f.name ~ '_min', '') }}"
          data-filter-input
          hx-get="{{ list_url }}"
          hx-target="{{ list_target }}"
          hx-swap="innerHTML"
          hx-trigger="input changed delay:300ms"
          hx-push-url="true"
          hx-include="[data-filter-input], .filter-search input"
          hx-vals='{"page": "1"}'
        />
        <span>to</span>
        <input
          type="number"
          name="{{ f.name }}_max"
          placeholder="Max"
          value="{{ active_filters.get(f.name ~ '_max', '') }}"
          data-filter-input
          hx-get="{{ list_url }}"
          hx-target="{{ list_target }}"
          hx-swap="innerHTML"
          hx-trigger="input changed delay:300ms"
          hx-push-url="true"
          hx-include="[data-filter-input], .filter-search input"
          hx-vals='{"page": "1"}'
        />
      </div>
      {% endif %}
    </div>
    {% endfor %}
  </div>
</details>
{% endif %}
```

**Step 2: Create filter CSS**

```css
/* src/resonance/ui/static/filters.css */
.filter-bar {
  display: flex;
  align-items: center;
  gap: 1rem;
  margin-bottom: 0.5rem;
  flex-wrap: wrap;
}
.filter-presets {
  display: flex;
  gap: 0.25rem;
  flex-wrap: wrap;
}
.preset-btn {
  padding: 0.25rem 0.75rem;
  border: 1px solid var(--pico-primary);
  border-radius: 2rem;
  background: transparent;
  color: var(--pico-primary);
  cursor: pointer;
  font-size: 0.875rem;
}
.preset-btn.preset-active {
  background: var(--pico-primary);
  color: var(--pico-primary-inverse);
}
.preset-btn.preset-clear {
  border-color: var(--pico-muted-border-color);
  color: var(--pico-muted-color);
}
.filter-search {
  flex: 1;
  min-width: 12rem;
}
.filter-search input {
  margin-bottom: 0;
}
.filter-panel {
  margin-bottom: 1rem;
}
.filter-panel summary {
  cursor: pointer;
  font-size: 0.875rem;
  color: var(--pico-muted-color);
}
.filter-fields {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(14rem, 1fr));
  gap: 0.5rem;
  padding: 0.5rem 0;
}
.filter-field label {
  font-size: 0.8rem;
  margin-bottom: 0.125rem;
}
.filter-field input[type="text"],
.filter-field input[type="search"],
.filter-field input[type="number"],
.filter-field input[type="date"] {
  margin-bottom: 0;
  padding: 0.25rem 0.5rem;
  font-size: 0.875rem;
}
.daterange-inputs,
.numrange-inputs {
  display: flex;
  align-items: center;
  gap: 0.25rem;
}
.daterange-inputs span,
.numrange-inputs span {
  font-size: 0.8rem;
  color: var(--pico-muted-color);
}
.multiselect-dropdown {
  position: relative;
}
.multiselect-dropdown summary {
  padding: 0.25rem 0.5rem;
  border: 1px solid var(--pico-form-element-border-color);
  border-radius: var(--pico-border-radius);
  font-size: 0.875rem;
  cursor: pointer;
  list-style: none;
}
.multiselect-options {
  position: absolute;
  z-index: 10;
  background: var(--pico-background-color);
  border: 1px solid var(--pico-form-element-border-color);
  border-radius: var(--pico-border-radius);
  padding: 0.25rem;
  min-width: 100%;
}
.multiselect-options label {
  display: flex;
  align-items: center;
  gap: 0.25rem;
  padding: 0.125rem 0.25rem;
  cursor: pointer;
  white-space: nowrap;
}
.multiselect-options input[type="checkbox"] {
  margin: 0;
}
```

**Step 3: Add CSS link to base.html**

In `src/resonance/ui/templates/base.html`, add after the Pico CSS link:
```html
<link rel="stylesheet" href="/static/filters.css" />
```

**Step 4: Verify templates render without errors**

Run the dev server: `uv run uvicorn resonance.app:create_app --factory --reload`
Navigate to any list page — no visual changes yet, but no errors.

**Step 5: Commit**

```bash
git add src/resonance/ui/templates/partials/filter_bar.html src/resonance/ui/static/filters.css src/resonance/ui/templates/base.html
git commit -m "feat: add shared filter bar template partials and CSS"
```

---

## Task 3: Events List — Filter Registry and API

Wire up the filter framework to the events list. This is the most complex view and serves as the template for the others.

**Files:**
- Create: `src/resonance/ui/view_filters.py` — per-view filter registries
- Modify: `src/resonance/ui/routes.py` — events_page route
- Modify: `src/resonance/ui/templates/events.html` — add filter bar
- Modify: `src/resonance/ui/templates/partials/event_list.html` — preserve filter params in pagination

**Step 1: Write failing test for events filter registry**

```python
# Add to tests/test_filters.py

import resonance.ui.view_filters as view_filters_module


class TestEventFilters:
    def test_registry_exists(self) -> None:
        filters = view_filters_module.EVENT_FILTERS
        assert len(filters) > 0

    def test_has_text_fields(self) -> None:
        filters = view_filters_module.EVENT_FILTERS
        names = [f.name for f in filters if isinstance(f, TextField)]
        assert "title" in names
        assert "venue" in names
        assert "artist" in names

    def test_has_date_range(self) -> None:
        filters = view_filters_module.EVENT_FILTERS
        names = [f.name for f in filters if isinstance(f, DateRangeField)]
        assert "date" in names

    def test_presets_defined(self) -> None:
        presets = view_filters_module.EVENT_PRESETS
        names = [p["name"] for p in presets]
        assert "upcoming" in names
        assert "going" in names
        assert "needs_review" in names
```

**Step 2: Run test — fails (module doesn't exist)**

Run: `uv run pytest tests/test_filters.py::TestEventFilters -v`

**Step 3: Create view_filters.py with event registry**

```python
# src/resonance/ui/view_filters.py
"""Per-view filter field registries and preset definitions."""

from __future__ import annotations

import datetime

import sqlalchemy as sa

import resonance.models as models

from resonance.ui.filters import (
    DateRangeField,
    ExistsField,
    MultiSelectField,
    NumericRangeField,
    TextField,
)

# -- Events --

EVENT_FILTERS = [
    TextField("title", models.Event.title),
    TextField("venue", models.Venue.name),
    TextField("artist", models.Artist.name),
    DateRangeField("date", models.Event.event_date),
    MultiSelectField(
        "attendance",
        models.UserEventAttendance.status,
        options=["GOING", "INTERESTED", "NONE"],
    ),
    ExistsField(
        "has_pending",
        sa.exists(
            sa.select(models.EventArtistCandidate.id).where(
                models.EventArtistCandidate.event_id == models.Event.id,
                models.EventArtistCandidate.status == "PENDING",
            )
        ),
    ),
]

EVENT_PRESETS = [
    {
        "name": "upcoming",
        "label": "Upcoming",
        "params": f"date_from={datetime.date.today().isoformat()}",
    },
    {
        "name": "going",
        "label": "Going",
        "params": "attendance=GOING",
    },
    {
        "name": "needs_review",
        "label": "Needs Review",
        "params": "has_pending=true",
    },
]

EVENT_TEMPLATE_FILTERS = [
    {"name": "title", "label": "Title", "type": "text"},
    {"name": "date", "label": "Date", "type": "daterange"},
    {"name": "venue", "label": "Venue", "type": "text"},
    {"name": "artist", "label": "Artists", "type": "text"},
    {
        "name": "attendance",
        "label": "Attendance",
        "type": "multiselect",
        "options": [
            {"value": "GOING", "label": "Going"},
            {"value": "INTERESTED", "label": "Interested"},
            {"value": "NONE", "label": "None"},
        ],
    },
]
```

Note: The ExistsField, attendance MultiSelectField, and artist/venue TextFields require
joins that are view-specific. The filter framework's `apply_filters` handles TextField
and DateRangeField directly. For fields requiring joins (venue, artist, attendance,
has_pending), the events_page route must add the necessary joins before calling
apply_filters, or the filter fields must carry join information. Decide during
implementation whether to add `join` support to the field types or handle it in the route.

**Step 4: Run tests — should pass**

Run: `uv run pytest tests/test_filters.py -v`

**Step 5: Commit**

```bash
git add src/resonance/ui/view_filters.py tests/test_filters.py
git commit -m "feat: add event filter registry and presets"
```

---

## Task 4: Events List — UI Route Integration

Wire the filter framework into the events_page route.

**Files:**
- Modify: `src/resonance/ui/routes.py` — events_page (around line 604)

**Step 1: Write failing test**

```python
# Add to tests/test_ui.py or tests/test_filters.py

class TestEventsPageFilters:
    """Events page route passes filter state to templates."""

    @pytest.mark.asyncio
    async def test_events_page_accepts_q_param(self) -> None:
        # Test that the events_page route accepts a q query param
        # and passes active_filters to the template context
        # (Exact test depends on the test infrastructure — may use
        #  the same FakeAsyncSession pattern as test_api_matching.py)
        pass  # Implement with project's test patterns
```

Note: The existing test patterns in `tests/test_ui.py` test routes at a high level
(auth redirects, page loads). For filter integration, the most practical approach is
to test via the API endpoints (already testable) and do a manual browser check for
the UI. The filter framework itself is tested in `tests/test_filters.py`.

**Step 2: Modify events_page route**

In `src/resonance/ui/routes.py`, modify the `events_page` function to:

1. Parse all filter query params from the request
2. Apply filters to the base query
3. Pass `active_filters`, `presets`, `filters`, and `active_preset` to the template context

Key changes to the events_page route (around line 604):
- Import `view_filters` and `filters` modules
- Parse `request.query_params` through the filter framework
- Add necessary joins for venue, artist, attendance filtering
- Apply the `upcoming` preset as default when no filters are active
- Pass filter state to template context

The route should handle these joins for cross-entity search:
- Venue: `outerjoin(Venue, Event.venue_id == Venue.id)` (for venue text search)
- Artist: `outerjoin(EventArtist, ...).outerjoin(Artist, ...)` (for artist text search)
- Attendance: subquery or join on UserEventAttendance (for attendance filter)

**Step 3: Modify events.html template**

```html
{% extends "base.html" %}
{% block title %}Events — resonance{% endblock %}
{% block content %}
<h1>Events</h1>
{% include "partials/filter_bar.html" %}
<div id="event-list">
    {% include "partials/event_list.html" %}
</div>
{% endblock %}
```

**Step 4: Modify event_list.html pagination links**

Update the Previous/Next links to preserve active filter params:

```html
{# Build filter query string from active_filters #}
{% set filter_qs = active_filters | urlencode if active_filters else "" %}

<a href="/events?page={{ page - 1 }}&{{ filter_qs }}"
   hx-get="/events?page={{ page - 1 }}&{{ filter_qs }}"
   hx-target="#event-list"
   hx-swap="innerHTML"
   hx-push-url="true"
   role="button"
   class="secondary">Previous</a>
```

Note: Jinja2's `urlencode` filter works on dicts. For list values (multi-select),
you may need a custom filter or build the query string in the route and pass it
as a template variable.

**Step 5: Test in browser**

Run: `uv run uvicorn resonance.app:create_app --factory --reload`
Navigate to `/events`:
- Verify presets appear (Upcoming active by default)
- Type in search box — list updates via HTMX
- Click "Filters" — column filters expand
- Type in a column filter — list updates
- Click a preset — filters populate and list updates
- Pagination preserves active filters

**Step 6: Commit**

```bash
git add src/resonance/ui/routes.py src/resonance/ui/templates/events.html src/resonance/ui/templates/partials/event_list.html
git commit -m "feat: add search and filtering to events list view"
```

---

## Task 5: Artists List — Filters

Follow the same pattern established in Task 3-4 for artists.

**Files:**
- Modify: `src/resonance/ui/view_filters.py` — add ARTIST_FILTERS, ARTIST_PRESETS
- Modify: `src/resonance/ui/routes.py` — artists_page
- Modify: `src/resonance/ui/templates/artists.html`
- Modify: `src/resonance/ui/templates/partials/artist_list.html`

**Filter registry:**

```python
ARTIST_FILTERS = [
    TextField("name", models.Artist.name),
    TextField("origin", models.Artist.origin),
    # Services requires JSON field search — use TextField on
    # cast(service_links, String) or handle specially in the route
]

ARTIST_PRESETS = [
    {
        "name": "has_events",
        "label": "Has Events",
        "params": "has_events=true",
    },
    {
        "name": "no_tracks",
        "label": "No Tracks",
        "params": "has_tracks=false",
    },
]

ARTIST_TEMPLATE_FILTERS = [
    {"name": "name", "label": "Name", "type": "text"},
    {"name": "origin", "label": "Origin", "type": "text"},
]
```

**Route changes:**
- Add `q`, `name`, `origin`, `has_events`, `has_tracks` query params
- Apply filters using the framework
- Pass filter state to template context
- Handle `has_events` with EXISTS subquery on EventArtist
- Handle `has_tracks` with EXISTS subquery on Track

**Template changes:**
- Same pattern as events: add `{% include "partials/filter_bar.html" %}` before the list div
- Update pagination links to preserve filter params

**Step: Commit**

```bash
git commit -m "feat: add search and filtering to artists list view"
```

---

## Task 6: Tracks List — Filters

**Files:**
- Modify: `src/resonance/ui/view_filters.py` — add TRACK_FILTERS, TRACK_PRESETS
- Modify: `src/resonance/ui/routes.py` — tracks_page
- Modify: `src/resonance/api/v1/tracks.py` — fix ILIKE escaping bug (line 42)
- Modify: `src/resonance/ui/templates/tracks.html`
- Modify: `src/resonance/ui/templates/partials/track_list.html`

**Filter registry:**

```python
TRACK_FILTERS = [
    TextField("title", models.Track.title),
    TextField("artist", models.Artist.name),
]

TRACK_PRESETS = [
    {
        "name": "recently_played",
        "label": "Recently Played",
        "params": "recently_played=true",
    },
    {
        "name": "no_links",
        "label": "No Service Links",
        "params": "has_links=false",
    },
]

TRACK_TEMPLATE_FILTERS = [
    {"name": "title", "label": "Title", "type": "text"},
    {"name": "artist", "label": "Artist", "type": "text"},
]
```

**IMPORTANT: Fix ILIKE escaping bug** in `src/resonance/api/v1/tracks.py` line 42.
Current code: `.where(music_models.Track.title.ilike(f"%{q}%"))`
Fixed: `.where(music_models.Track.title.ilike(f"%{_escape_ilike(q)}%"))`
Import `_escape_ilike` from the artists module or the new filters module.

**Route changes:**
- Add `q`, `title`, `artist`, `recently_played`, `has_links` query params
- Handle `recently_played` with EXISTS subquery on ListeningEvent (last 30 days)
- Handle `has_links` by checking `service_links IS NOT NULL AND service_links != '{}'`
- Artist search joins through the existing Track.artist relationship

**Step: Commit**

```bash
git commit -m "feat: add search and filtering to tracks list view

Also fixes ILIKE wildcard escaping in tracks API endpoint."
```

---

## Task 7: Listening History — Filters

**Files:**
- Modify: `src/resonance/ui/view_filters.py` — add HISTORY_FILTERS, HISTORY_PRESETS
- Modify: `src/resonance/ui/routes.py` — history_page
- Modify: `src/resonance/ui/templates/history.html`
- Modify: `src/resonance/ui/templates/partials/history_list.html`

**Filter registry:**

```python
HISTORY_FILTERS = [
    TextField("track", models.Track.title),
    TextField("artist", models.Artist.name),
    MultiSelectField(
        "source",
        models.ListeningEvent.source_service,
        options=["SPOTIFY", "LISTENBRAINZ", "LASTFM"],
    ),
    DateRangeField("date", models.ListeningEvent.listened_at),
]

HISTORY_PRESETS = [
    {"name": "spotify", "label": "Spotify", "params": "source=SPOTIFY"},
    {"name": "listenbrainz", "label": "ListenBrainz", "params": "source=LISTENBRAINZ"},
    {"name": "lastfm", "label": "Last.fm", "params": "source=LASTFM"},
]

HISTORY_TEMPLATE_FILTERS = [
    {"name": "track", "label": "Track", "type": "text"},
    {"name": "artist", "label": "Artist", "type": "text"},
    {
        "name": "source",
        "label": "Source",
        "type": "multiselect",
        "options": [
            {"value": "SPOTIFY", "label": "Spotify"},
            {"value": "LISTENBRAINZ", "label": "ListenBrainz"},
            {"value": "LASTFM", "label": "Last.fm"},
        ],
    },
    {"name": "date", "label": "Date", "type": "daterange"},
]
```

**Route changes:**
- Add filter params to history_page
- Join through Track → Artist for text search
- Apply source filter on ListeningEvent.source_service

**Step: Commit**

```bash
git commit -m "feat: add search and filtering to listening history list view"
```

---

## Task 8: Playlists List — Filters

**Files:**
- Modify: `src/resonance/ui/view_filters.py` — add PLAYLIST_FILTERS
- Modify: `src/resonance/ui/routes.py` — playlists_page
- Modify: `src/resonance/ui/templates/playlists.html`
- Modify: `src/resonance/ui/templates/partials/playlist_list.html`

**Filter registry:**

```python
PLAYLIST_FILTERS = [
    TextField("name", models.Playlist.name),
    DateRangeField("created", models.Playlist.created_at),
    NumericRangeField("tracks", models.Playlist.track_count),
]

PLAYLIST_PRESETS: list[dict[str, str]] = []  # none needed

PLAYLIST_TEMPLATE_FILTERS = [
    {"name": "name", "label": "Name", "type": "text"},
    {"name": "created", "label": "Created", "type": "daterange"},
    {"name": "tracks", "label": "Tracks", "type": "numericrange"},
]
```

**Route changes:**
- Add filter params to playlists_page
- Simple — no cross-entity joins needed

**Step: Commit**

```bash
git commit -m "feat: add search and filtering to playlists list view"
```

---

## Task 9: Manual Testing & Polish

Test all five views in the browser. For each view:

1. Verify presets appear and toggle correctly
2. Verify quick search filters results with debounce
3. Verify "Filters" toggle expands/collapses
4. Verify column filters work independently and with quick search
5. Verify pagination preserves all active filters
6. Verify URL updates on filter changes (bookmark-friendly)
7. Verify browser back button restores previous filter state
8. Verify "Clear" button resets all filters
9. Verify preset clicking auto-expands filter panel when needed
10. Verify empty results show appropriate message

Fix any issues found during testing.

**Step: Commit**

```bash
git commit -m "fix: polish filter bar interactions and edge cases"
```

---

## Task 10: Final Cleanup and Type Checking

1. Run full test suite: `uv run pytest`
2. Run type checker: `uv run mypy src/`
3. Run linter: `uv run ruff check . && uv run ruff format --check .`
4. Fix any issues
5. Update `docs/plans/2026-04-29-list-view-filters-design.md` with any design changes made during implementation

**Step: Final commit**

```bash
git commit -m "chore: type checking and lint fixes for list view filters"
```

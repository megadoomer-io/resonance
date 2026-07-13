"""Template-render tests for the rediscovery editor panel (#rediscovery-ui).

Renders ``playlists_new.html`` directly (no app/DB) to prove the conditional
panel swaps by generator_type: rediscovery gets the window panel, concert_prep
keeps the lineup add-controls. The seed-preview endpoint and full create flow are
covered by live QA (there is no async client/DB fixture in this suite).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import resonance.generators.parameters as params_module
import resonance.types as types_module
import resonance.ui.common as common_ui


def _render(generator_type: str, *, similar_available: bool = True) -> str:
    """Render the editor template with a minimal, real-shaped context."""
    config = params_module.GENERATOR_TYPE_CONFIG[
        types_module.GeneratorType(generator_type)
    ]
    reg = params_module.PARAMETER_REGISTRY
    param_sections = [
        {"params": [(n, reg[n]) for n in config.ordered_lead()]},
        {"params": [(n, reg[n]) for n in config.advanced_parameters]},
    ]
    ctx: dict[str, Any] = {
        "request": SimpleNamespace(url=SimpleNamespace(path="/playlists/x/edit")),
        "user_id": "u",
        "user_tz": "UTC",
        "user_role": "owner",
        "actual_role": "owner",
        "viewing_as": None,
        "profile_id": "x",
        "profile_name": "My Playlist",
        "parameter_values": {},
        "similar_available": similar_available,
        "events": [],
        "parameters": {},
        "param_sections": param_sections,
        "generator_type": generator_type,
        "lineup": {"version": 0, "groups": []},
    }
    return common_ui.templates.get_template("playlists_new.html").render(ctx)


class TestRediscoveryPanel:
    def test_renders_window_pills_and_seed_preview(self) -> None:
        html = _render("rediscovery")
        assert 'data-window-preset="last_2_weeks"' in html
        assert 'data-window-preset="a_month_ago"' in html
        assert 'data-window-preset="this_time_last_year"' in html
        assert 'data-window-preset="custom"' in html
        assert 'id="seed-preview"' in html
        assert 'id="seed-count"' in html
        assert 'id="custom-window"' in html

    def test_uses_rediscover_header_not_lineup_copy(self) -> None:
        html = _render("rediscovery")
        assert "Rediscover" in html
        # The event dropdown (lineup add-control) must not appear for rediscovery.
        assert 'id="add-event-select"' not in html

    def test_leads_with_rediscovery_dials_advanced_hides_the_rest(self) -> None:
        html = _render("rediscovery")
        # Lead dials render; familiarity/hit_depth are behind the Advanced <details>.
        assert 'data-param="new_ratio"' in html
        assert 'data-param="less_heard_percentile"' in html
        assert "<summary>Advanced</summary>" in html
        assert 'data-param="familiarity"' in html  # present, but inside Advanced

    def test_find_related_button_when_similar_available(self) -> None:
        html = _render("rediscovery", similar_available=True)
        assert "Find related artists" in html
        assert 'id="new-stream-hint"' in html


class TestConcertPrepPanelUnchanged:
    def test_renders_lineup_controls_not_window_panel(self) -> None:
        html = _render("concert_prep")
        assert 'id="add-event-select"' in html
        assert 'id="artist-search"' in html
        # No rediscovery window panel for concert_prep.
        assert "data-window-preset" not in html
        assert 'id="seed-preview"' not in html

    def test_shows_only_featured_dials(self) -> None:
        html = _render("concert_prep")
        assert 'data-param="familiarity"' in html
        assert 'data-param="hit_depth"' in html
        # The inert rediscovery dials must not render for concert_prep.
        assert 'data-param="new_ratio"' not in html
        assert 'data-param="less_heard_percentile"' not in html
        # No Advanced disclosure (concert_prep has no advanced params).
        assert "<summary>Advanced</summary>" not in html


class TestSeedPreviewPartial:
    """Directly render the seed-preview partial to catch Jinja/format errors
    (strftime, the date-range echo, the data-empty flag) without an app/DB."""

    def _render_preview(self, ctx_extra: dict[str, Any]) -> str:
        ctx: dict[str, Any] = {
            "request": SimpleNamespace(url=SimpleNamespace(path="/x")),
            "user_id": "u",
            "user_tz": "UTC",
            "user_role": "owner",
            "actual_role": "owner",
            "viewing_as": None,
        }
        ctx.update(ctx_extra)
        return common_ui.templates.get_template(
            "partials/rediscovery_seed_preview.html"
        ).render(ctx)

    def test_populated_window_lists_artists_and_echoes_dates(self) -> None:
        import datetime

        html = self._render_preview(
            {
                "artists": [
                    {"id": "a", "name": "Boards of Canada", "meta": "idm, ambient"},
                    {"id": "b", "name": "Aphex Twin", "meta": ""},
                ],
                "start": datetime.datetime(2025, 7, 6, tzinfo=datetime.UTC),
                "end": datetime.datetime(2025, 8, 3, tzinfo=datetime.UTC),
                "total": 2,
                "more": 0,
                "empty": False,
            }
        )
        assert 'data-empty="false"' in html
        assert "Boards of Canada" in html
        assert "Aphex Twin" in html
        assert "Jul" in html and "Aug" in html  # date-range echo rendered

    def test_more_suffix_when_truncated(self) -> None:
        import datetime

        html = self._render_preview(
            {
                "artists": [{"id": "a", "name": "One", "meta": ""}],
                "start": datetime.datetime(2026, 6, 6, tzinfo=datetime.UTC),
                "end": datetime.datetime(2026, 7, 4, tzinfo=datetime.UTC),
                "total": 31,
                "more": 30,
                "empty": False,
            }
        )
        assert "+30 more" in html
        # Same-year range collapses to one year label.
        assert html.count("2026") == 1

    def test_empty_window_marks_data_empty(self) -> None:
        import datetime

        html = self._render_preview(
            {
                "artists": [],
                "start": datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC),
                "end": datetime.datetime(2020, 1, 14, tzinfo=datetime.UTC),
                "total": 0,
                "more": 0,
                "empty": True,
            }
        )
        assert 'data-empty="true"' in html
        assert "No listening data" in html

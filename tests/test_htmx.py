"""Tests for ui/htmx.py — HTMX request detection and response helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import fastapi

import resonance.ui.htmx as htmx_module


def _make_request(htmx: bool = False) -> fastapi.Request:
    """Build a fake Request with optional HX-Request header."""
    request = MagicMock(spec=fastapi.Request)
    headers: dict[str, str] = {}
    if htmx:
        headers["HX-Request"] = "true"
    request.headers = headers
    return request


# ---------------------------------------------------------------------------
# is_htmx_request
# ---------------------------------------------------------------------------


class TestIsHtmxRequest:
    def test_true_when_header_present(self) -> None:
        request = _make_request(htmx=True)
        assert htmx_module.is_htmx_request(request) is True

    def test_false_when_header_absent(self) -> None:
        request = _make_request(htmx=False)
        assert htmx_module.is_htmx_request(request) is False


# ---------------------------------------------------------------------------
# trigger_event
# ---------------------------------------------------------------------------


class TestTriggerEvent:
    def test_simple_event(self) -> None:
        response = fastapi.responses.HTMLResponse(content="ok")
        result = htmx_module.trigger_event(response, "artistsChanged")
        assert result.headers["HX-Trigger"] == "artistsChanged"
        assert result is response

    def test_event_with_detail(self) -> None:
        response = fastapi.responses.HTMLResponse(content="ok")
        htmx_module.trigger_event(response, "itemDeleted", {"id": "abc123"})
        header = response.headers["HX-Trigger"]
        assert '"itemDeleted"' in header
        assert '"abc123"' in header


# ---------------------------------------------------------------------------
# render_fragment
# ---------------------------------------------------------------------------


class TestRenderFragment:
    def test_returns_partial_for_htmx(self) -> None:
        request = _make_request(htmx=True)
        templates = MagicMock()
        templates.TemplateResponse.return_value = fastapi.responses.HTMLResponse(
            content="partial"
        )
        result = htmx_module.render_fragment(
            request,
            templates,
            partial_template="partials/event_list.html",
            full_template="events.html",
            context={"request": request},
        )
        templates.TemplateResponse.assert_called_once_with(
            request, "partials/event_list.html", {"request": request}
        )
        assert result.body == b"partial"

    def test_returns_full_page_for_direct(self) -> None:
        request = _make_request(htmx=False)
        templates = MagicMock()
        templates.TemplateResponse.return_value = fastapi.responses.HTMLResponse(
            content="full"
        )
        result = htmx_module.render_fragment(
            request,
            templates,
            partial_template="partials/event_list.html",
            full_template="events.html",
            context={"request": request},
        )
        templates.TemplateResponse.assert_called_once_with(
            request, "events.html", {"request": request}
        )
        assert result.body == b"full"

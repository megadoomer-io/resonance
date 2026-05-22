"""HTMX helper functions for consistent request/response handling."""

from __future__ import annotations

import json
from typing import Any

import fastapi
import fastapi.templating


def is_htmx_request(request: fastapi.Request) -> bool:
    """Return True if the request was initiated by HTMX."""
    return request.headers.get("HX-Request") == "true"


def render_fragment(
    request: fastapi.Request,
    templates: fastapi.templating.Jinja2Templates,
    *,
    partial_template: str,
    full_template: str,
    context: dict[str, Any],
) -> fastapi.responses.HTMLResponse:
    """Return partial for HTMX requests, full page for direct navigation.

    Args:
        request: The incoming request.
        templates: Jinja2Templates instance.
        partial_template: Template path for the HTMX fragment.
        full_template: Template path for the full page.
        context: Template context dict (must include "request").
    """
    template = partial_template if is_htmx_request(request) else full_template
    return templates.TemplateResponse(request, template, context)


def trigger_event[R: fastapi.responses.Response](
    response: R,
    event: str,
    detail: dict[str, Any] | None = None,
) -> R:
    """Add HX-Trigger header for cross-component updates.

    Args:
        response: The response to add the header to.
        event: The event name (e.g., "artistsChanged").
        detail: Optional detail payload for the event.
    """
    if detail is not None:
        response.headers["HX-Trigger"] = json.dumps({event: detail})
    else:
        response.headers["HX-Trigger"] = event
    return response

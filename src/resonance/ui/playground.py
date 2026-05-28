"""Component playground: renders every macro in every state."""

from __future__ import annotations

import uuid
from typing import Annotated

import fastapi
import fastapi.responses

import resonance.ui.common as common

router = fastapi.APIRouter(tags=["ui"])

COMPONENT_ENTRIES = [
    {
        "name": "entity_list",
        "description": "List shell with table wrapper, pagination, empty state",
        "states": ["populated", "empty", "paginated"],
    },
    {
        "name": "action_button",
        "description": "Button with HTMX attrs, loading state, confirm",
        "states": ["default", "danger", "with_confirm"],
    },
    {
        "name": "confirm_action",
        "description": "Confirmation before destructive actions",
        "states": ["default"],
    },
    {
        "name": "empty_state",
        "description": "Consistent empty state messaging",
        "states": ["default", "with_action"],
    },
    {
        "name": "flash_message",
        "description": "Action feedback with severity and auto-fade",
        "states": ["success", "error", "warning"],
    },
    {
        "name": "error_state",
        "description": "Validation failure / server error display",
        "states": ["message_only", "with_errors"],
    },
    {
        "name": "filter_bar",
        "description": "Presets, search, column filters, persistence",
        "states": ["default"],
    },
    {
        "name": "detail_section",
        "description": "Detail page layout block",
        "states": ["default"],
    },
    {
        "name": "view_as_pill",
        "description": "Floating pill for role impersonation",
        "states": ["inactive", "active"],
    },
]


@router.get("/dev/components", response_model=None)
async def component_playground(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
) -> fastapi.responses.HTMLResponse:
    """Render the component playground (admin only)."""
    ctx = common.base_context(request)
    ctx["components"] = COMPONENT_ENTRIES

    return common.templates.TemplateResponse(request, "dev/components.html", ctx)

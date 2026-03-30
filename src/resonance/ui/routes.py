import pathlib

import fastapi
import fastapi.requests
import fastapi.responses
import fastapi.templating

_TEMPLATE_DIR = pathlib.Path(__file__).resolve().parent.parent / "templates"
templates = fastapi.templating.Jinja2Templates(directory=str(_TEMPLATE_DIR))

router = fastapi.APIRouter(tags=["ui"])


@router.get("/login", response_class=fastapi.responses.HTMLResponse)
async def login(request: fastapi.Request) -> fastapi.responses.HTMLResponse:
    """Render the login page."""
    return templates.TemplateResponse(request, "login.html")


@router.get("/", response_class=fastapi.responses.HTMLResponse, response_model=None)
async def root(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render dashboard or redirect to login if unauthenticated."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    return templates.TemplateResponse(
        request, "dashboard.html", {"user_id": user_id}
    )

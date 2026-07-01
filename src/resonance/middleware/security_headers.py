"""Security-header middleware (security review #141, finding #8).

Adds a Content-Security-Policy plus the standard hardening headers to every
response. The CSP is defense-in-depth on top of the output-escaping fix (#6):
it restricts where scripts/styles/images may load from, and blocks framing and
MIME sniffing.

The policy allows ``'unsafe-inline'`` for scripts because the templates rely on
inline ``<script>`` blocks (theme bootstrap, timezone capture, per-page
``window.LINEUP_*`` config). That means the CSP does not by itself stop an
injected inline script — the breakout hole that enabled that is closed by the
``tojson`` escaping in #6. A future hardening could move to nonce-based inline
scripts and drop ``'unsafe-inline'``.
"""

from __future__ import annotations

import typing

import starlette.middleware.base as base_middleware

if typing.TYPE_CHECKING:
    import starlette.requests as starlette_requests
    import starlette.responses as starlette_responses
    import starlette.types as starlette_types

# Allowed sources mirror what base.html actually loads:
# - scripts: unpkg (htmx, lucide) + jsdelivr (Swagger UI in dev) + inline blocks
# - styles: jsdelivr (Pico CSS) + /static + inline
# Everything else is same-origin only; framing and plugins are denied.
DEFAULT_CSP = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net; "
    "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
    "img-src 'self' data:; "
    "font-src 'self' data:; "
    "connect-src 'self'; "
    "frame-ancestors 'none'; "
    "base-uri 'self'; "
    "form-action 'self'; "
    "object-src 'none'"
)


class SecurityHeadersMiddleware(base_middleware.BaseHTTPMiddleware):
    """Attach CSP + hardening headers to every response.

    Uses ``setdefault`` so a route that sets its own value (rare) is not
    clobbered.
    """

    def __init__(
        self,
        app: starlette_types.ASGIApp,
        csp: str = DEFAULT_CSP,
    ) -> None:
        super().__init__(app)
        self.csp = csp

    async def dispatch(
        self,
        request: starlette_requests.Request,
        call_next: base_middleware.RequestResponseEndpoint,
    ) -> starlette_responses.Response:
        response = await call_next(request)
        headers = response.headers
        headers.setdefault("Content-Security-Policy", self.csp)
        headers.setdefault("X-Content-Type-Options", "nosniff")
        headers.setdefault("X-Frame-Options", "DENY")
        headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        return response

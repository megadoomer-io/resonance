# Resonance Security Review — 2026-06-26

First comprehensive security review of the resonance codebase (~28k LoC, 92 source
files). Driven by the pending follow-up "full-codebase security review (planned, not
started)."

## Scope & Methodology

Static review prioritizing the highest-risk surfaces called out in the work item:
auth flows (OAuth, bearer-token admin, the X-Assume-User identity assumption), token
handling (Fernet), input validation, and external API integrations. Plus a
dangerous-pattern sweep across all of `src/`.

**Reviewed in depth:** `config.py`, `crypto.py`, `dependencies.py` (identity/authz),
`middleware/session.py`, `app.py`, `api/v1/auth.py` (OAuth), `connectors/ical.py` +
`concerts/worker.py` (feed fetch), `dedup.py` (raw SQL), the `lineup_json` template
path. Swept the whole tree for `eval`/`exec`/`subprocess`/`pickle`/`yaml.load`,
`verify=False`, raw SQL interpolation, `| safe`, and outbound URL fetches.

**Not yet covered (recommended next pass):** per-endpoint authz on every API/UI route
(admin endpoints, playlist generation, account, matching, sync), the playlist
generation input surface, CSV/concert-archives parsing, and a dependency-vulnerability
scan of `uv.lock`. This review establishes the baseline; it is not exhaustive.

## Findings

| # | Severity | Finding | Location |
|---|----------|---------|----------|
| 1 | High | SSRF: user-supplied iCal feed URL fetched with no validation | `concerts/worker.py:216` |
| 2 | Medium | OAuth state/CSRF check bypassable with an empty `state` | `api/v1/auth.py:127` |
| 3 | Medium | First user to complete OAuth silently becomes OWNER | `api/v1/auth.py:252` |
| 4 | Medium | Weak secret defaults, no startup guard | `config.py:24,28` |
| 5 | Medium | Session cookie missing `Secure` flag | `middleware/session.py:133` |
| 6 | Medium | XSS: `json.dumps` rendered with `| safe` into `<script>` | `templates/playlists_new.html:96` |
| 7 | Low-Med | Admin bearer token compared with `==` (timing side channel) | `dependencies.py:145,181` |
| 8 | Low | No CSP / security-header middleware (compounds #6) | `app.py` |
| 9 | Low | Swagger `/docs` exposed unconditionally | `app.py:101` |
| 10 | Low | No rate limiting on auth/admin (compounds #7) | `app.py` |

---

### 1. SSRF in iCal calendar feed fetch — High

`concerts/worker.py:216-219` fetches `connection.url` (a user-supplied iCal feed URL)
with `httpx.AsyncClient().get(url)` and no validation: no scheme allowlist, no
private/loopback/link-local/metadata IP blocking, no redirect guard, and **no
timeout**. An authenticated user (the USER role exists; this is not owner-only) can add
a feed URL pointing at `http://169.254.169.254/...` (DO/cloud metadata),
`http://localhost:<port>`, or cluster-internal HTTP services. The response is parsed as
iCal (mostly blind SSRF), but the fetch still reaches the internal target, and parse
errors/fetched data can surface in task results.

**Fix:** validate feed URLs before fetch — require `http`/`https`, resolve the host and
reject private/loopback/link-local/ULA + `169.254.169.254`, guard against DNS rebinding
(re-resolve and pin, or block on the resolved IP), set an explicit timeout, keep
`follow_redirects=False` (httpx default) or validate redirect targets, and cap response
size.

### 2. OAuth state check bypassable with empty state — Medium

`api/v1/auth.py:127`: `if state and (stored_state is None or stored_state != state):`.
The state/CSRF check is skipped entirely when `state` is empty. The intent is to
accommodate Last.fm (which doesn't echo `state`), but the escape hatch applies to
**every** service on `/auth/{service}/callback`. An attacker can defeat OAuth-CSRF
protection for Spotify/Dex/MusicBrainz by simply omitting `state` in the callback.

**Fix:** scope the "skip state" exception to services that genuinely don't return state
(Last.fm only, keyed off the connector/service type), and require a matching state for
all standard-OAuth2 services. Compare with `secrets.compare_digest` for good measure.

### 3. First-user-becomes-OWNER bootstrapping — Medium

`api/v1/auth.py:252-264`: the first user to complete OAuth (`is_first_user`) is created
with `UserRole.OWNER`. If the app is internet-reachable before the legitimate owner
logs in, whoever authenticates first claims owner. Race/landrush risk on any public
deployment.

**Fix:** bootstrap the owner explicitly (env/config naming the owner's external id, or
a one-time setup token), or restrict who can self-register, rather than implicitly
trusting arrival order.

### 4. Weak secret defaults, no startup guard — Medium

`config.py:24,28`: `session_secret_key` and `token_encryption_key` both default to
`"change-me-in-production"`. Nothing fails startup if they are not overridden. If
deployed unset, the session signer key and the Fernet token-encryption key are
publicly known — session forgery and decryption of all stored OAuth tokens. (Prod sets
these via sealed secrets, so this is defense-in-depth against a deploy-time slip, not a
confirmed live exposure.)

**Fix:** a `model_validator` that refuses to start when these equal the placeholder (or
are empty) outside an explicit dev mode. Same treatment for the default `PGPASSWORD`.

### 5. Session cookie missing Secure flag — Medium

`middleware/session.py:133-139`: the session cookie sets `httponly=True`,
`samesite="lax"`, but **not** `secure=True`, so a browser will send it over plain HTTP.
On any non-HTTPS hop (or a downgrade) the session id is exposed.

**Fix:** set `secure=True` (gate on a config flag for local-http dev if needed). Same
for the `last_auth_service` cookie in `auth.py`.

### 6. XSS via `json.dumps` + `| safe` in a script block — Medium

`templates/playlists_new.html:96`:
`<script id="lineup-data" type="application/json">{{ lineup_json | safe }}</script>`
where `lineup_json = json.dumps(lineup)` (`ui/playlists.py:492`). `json.dumps` does not
escape `<`/`>`/`/`, so any lineup value containing `</script>` (artist/event names come
from external APIs and manual entry) breaks out of the tag → stored/reflected XSS.

**Fix:** use Jinja's `tojson` filter (markupsafe-aware: escapes `<`, `>`, `&` to
`<` etc.) instead of `json.dumps` + `| safe`. Add a CSP (see #8) as defense in
depth.

### 7. Non-constant-time admin token comparison — Low-Medium

`dependencies.py:145` (`token != settings.admin_api_token`), `:181`
(`token == settings.admin_api_token`), and `verify_admin_access` compare the admin
bearer token with `==`/`!=`. Timing side channel on a long-lived omnipotent token.
Practical exploitability is low but the fix is trivial.

**Fix:** `secrets.compare_digest(token, settings.admin_api_token)`.

### 8. No CSP / security-header middleware — Low

`app.py` adds no Content-Security-Policy, `X-Frame-Options`/`frame-ancestors`,
`X-Content-Type-Options`, or `Referrer-Policy`. A CSP would blunt #6 and clickjacking.

**Fix:** a small middleware setting CSP (script-src self), `X-Content-Type-Options:
nosniff`, `frame-ancestors 'none'`, `Referrer-Policy`.

### 9. Swagger `/docs` exposed unconditionally — Low

`app.py:101`: `docs_url="/docs"` regardless of environment. Endpoints still require
auth, so this is enumeration/info-disclosure, not direct access.

**Fix:** gate `docs_url`/`openapi_url` on `settings.debug` (or behind admin auth).

### 10. No rate limiting on auth/admin — Low

No rate limiting on login/callback or admin-token endpoints. Combined with #7, the
admin token is brute-forceable given enough requests (mitigated by token length).

**Fix:** rate-limit auth + admin paths (per-IP / per-session).

---

## What's solid

- OAuth tokens encrypted at rest with Fernet before DB storage (`crypto.py`, `auth.py`).
- OAuth `state` is generated with `secrets.token_urlsafe(32)` and verified (when present — see #2).
- SQL is parameterized throughout: SQLAlchemy Core/ORM, and the raw `sa.text()` calls in `dedup.py` are static strings with no interpolation. No SQLi found.
- No `eval`/`exec`/`subprocess`/`os.system`/`pickle`/`yaml.load`/`verify=False` anywhere in `src/`.
- Session cookies are `httponly` + `samesite=lax` (just missing `secure` — #5).
- The X-Assume-User feature is correctly gated behind a valid admin token (not a privilege escalation), is toggle-able, and every assumption is audit-logged (`dependencies.py:85`).
- Sessions are invalidated on role change (`auth.py:317`), and a per-user reverse index supports bulk invalidation.
- No permissive CORS (no CORS middleware → same-origin only).

## Suggested remediation order

1. **#1 SSRF** and **#6 XSS** — the two with a remote-exploit path.
2. **#2 OAuth state** and **#3 owner-landrush** — auth-integrity.
3. **#4 secret guard**, **#5 Secure cookie**, **#7 constant-time compare** — quick, high-value hardening.
4. **#8–#10** — defense-in-depth.

Then a second pass for the not-yet-covered surfaces (per-endpoint authz, playlist
generation inputs, dependency scan).

"""Tests for the lineup builder: create_playlist POST, event lineup + search.

Covers the #128 UI cutover from the legacy single-event ``{"event_id": ...}``
shape to the layered ``{"sources": [...], "exclude_artist_ids": [...]}`` spec.
"""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import fastapi
import httpx
import pytest

import resonance.config as config_module
import resonance.middleware.session as session_middleware
import resonance.types as types_module
import resonance.ui.playlists as playlists_ui

# --- shared fakes (self-contained, mirrors test_api_artist_search) ---


def _make_settings() -> config_module.Settings:
    return config_module.Settings(
        spotify_client_id="test-id",
        spotify_client_secret="test-secret",
        token_encryption_key="y4s2fMagCz79NWhqQfaAPbTBl9vnamqcvlGM6GRH2cQ=",
    )


class FakeResult:
    def __init__(self, items: list[Any] | None = None) -> None:
        self._items = items or []

    def unique(self) -> FakeResult:
        return self

    def scalars(self) -> FakeResult:
        return self

    def all(self) -> list[Any]:
        return self._items

    def scalar_one_or_none(self) -> Any:
        return self._items[0] if self._items else None


class FakeAsyncSession:
    def __init__(self) -> None:
        self._results: list[Any] = []
        self._call_count = 0
        self.added: list[Any] = []
        self._committed = False
        self._get_returns: list[Any] = []
        self._get_count = 0

    def set_results(self, results: list[Any]) -> None:
        self._results = results

    def set_get_returns(self, values: list[Any]) -> None:
        self._get_returns = values

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        if self._call_count < len(self._results):
            result = self._results[self._call_count]
            self._call_count += 1
            return result
        return FakeResult()

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        if self._get_count < len(self._get_returns):
            value = self._get_returns[self._get_count]
            self._get_count += 1
            return value
        return None

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def flush(self) -> None:
        pass

    async def commit(self) -> None:
        self._committed = True

    async def __aenter__(self) -> FakeAsyncSession:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


class FakeSessionFactory:
    def __init__(self, session: FakeAsyncSession) -> None:
        self._session = session

    def __call__(self) -> FakeAsyncSession:
        return self._session


class FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = value

    async def aclose(self) -> None:
        pass

    def inject_session(self, session_id: str, data: dict[str, Any]) -> None:
        self._store[f"session:{session_id}"] = json.dumps(data)


def _session_cookie(secret_key: str) -> str:
    import itsdangerous

    return itsdangerous.TimestampSigner(secret_key).sign("test-session-id").decode()


def _api_app(user_id: uuid.UUID, db: FakeAsyncSession) -> Any:
    import resonance.api.v1 as api_v1_module

    settings = _make_settings()
    fake_redis = FakeRedis()
    fake_redis.inject_session(
        "test-session-id", {"user_id": str(user_id), "user_role": "owner"}
    )
    app = fastapi.FastAPI(title="test", lifespan=None)
    app.state.settings = settings
    app.state.session_factory = FakeSessionFactory(db)
    app.add_middleware(
        session_middleware.SessionMiddleware,
        redis=fake_redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )
    app.include_router(api_v1_module.router)
    return app


def _artist(name: str, **over: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "id": uuid.uuid4(),
        "name": name,
        "origin": None,
        "disambiguation": "",
        "artist_type": "Group",
        "area": "US",
        "begin_year": 2010,
        "end_year": None,
        "service_links": {},
    }
    base.update(over)
    return SimpleNamespace(**base)


@pytest.fixture
def user_id() -> uuid.UUID:
    return uuid.uuid4()


# --- server-backed editor: new (create-draft) + edit (#133) ---


class TestNewPlaylistCreatesDraft:
    async def test_creates_empty_draft_and_redirects_to_editor(
        self, user_id: uuid.UUID
    ) -> None:
        request = MagicMock(spec=fastapi.Request)
        db = FakeAsyncSession()

        resp = await playlists_ui.new_playlist_page(request, user_id, db, "", "")

        assert resp.status_code == 303
        assert resp.headers["location"].endswith("/edit")
        profiles = [a for a in db.added if hasattr(a, "input_references")]
        assert len(profiles) == 1
        profile = profiles[0]
        assert profile.status == types_module.ProfileStatus.DRAFT
        assert profile.input_references == {"sources": [], "exclude_artist_ids": []}
        # No generation is triggered on open.
        assert not any(hasattr(a, "task_type") for a in db.added)

    async def test_preseeds_event_source(self, user_id: uuid.UUID) -> None:
        event_id = str(uuid.uuid4())
        db = FakeAsyncSession()
        # _default_playlist_name resolves the event via db.get (no venue).
        db.set_get_returns([SimpleNamespace(title="Big Show", venue_id=None)])
        request = MagicMock(spec=fastapi.Request)

        resp = await playlists_ui.new_playlist_page(request, user_id, db, event_id, "")

        assert resp.status_code == 303
        profile = next(a for a in db.added if hasattr(a, "input_references"))
        assert profile.input_references["sources"] == [
            {"kind": "event", "event_id": event_id, "enabled": True}
        ]
        assert profile.name == "Concert Prep: Big Show"

    async def test_ignores_bad_event_id(self, user_id: uuid.UUID) -> None:
        db = FakeAsyncSession()
        request = MagicMock(spec=fastapi.Request)

        resp = await playlists_ui.new_playlist_page(
            request, user_id, db, "not-a-uuid", ""
        )

        assert resp.status_code == 303
        profile = next(a for a in db.added if hasattr(a, "input_references"))
        assert profile.input_references["sources"] == []


class TestEditPlaylistPage:
    async def test_404_for_missing_profile(self, user_id: uuid.UUID) -> None:
        db = FakeAsyncSession()
        db.set_results([FakeResult([])])  # profile lookup -> none
        request = MagicMock(spec=fastapi.Request)

        with pytest.raises(fastapi.HTTPException) as exc:
            await playlists_ui.edit_playlist_page(request, user_id, db, uuid.uuid4())
        assert exc.value.status_code == 404


# --- event lineup endpoint ---


class TestEventLineup:
    async def test_resolves_lineup_with_in_library(self, user_id: uuid.UUID) -> None:
        event_id = uuid.uuid4()
        headliner = _artist("Wolves in the Throne Room")
        opener = _artist("Opening DJ")
        db = FakeAsyncSession()
        db.set_get_returns([SimpleNamespace(id=event_id, title="WITTR")])
        db.set_results(
            [
                FakeResult([headliner.id, opener.id]),  # EventArtist ids
                FakeResult([]),  # accepted candidates
                FakeResult([headliner, opener]),  # Artist objects
                FakeResult([headliner.id]),  # in-library subset (only headliner)
            ]
        )

        app = _api_app(user_id, db)
        cookie = _session_cookie(_make_settings().session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            resp = await c.get(f"/api/v1/events/{event_id}/lineup")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["artists"]) == 2
        by_name = {a["name"]: a for a in data["artists"]}
        assert by_name["Wolves in the Throne Room"]["in_library"] is True
        assert by_name["Opening DJ"]["in_library"] is False

    async def test_missing_event_returns_404(self, user_id: uuid.UUID) -> None:
        db = FakeAsyncSession()
        db.set_get_returns([None])
        app = _api_app(user_id, db)
        cookie = _session_cookie(_make_settings().session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            resp = await c.get(f"/api/v1/events/{uuid.uuid4()}/lineup")
        assert resp.status_code == 404


# --- artist search in_library flag ---


class TestArtistSearchInLibrary:
    async def test_search_includes_in_library_flag(self, user_id: uuid.UUID) -> None:
        nite_metal = _artist("Nite", disambiguation="heavy metal")
        nite_electronic = _artist("Nite", disambiguation="electronic")
        db = FakeAsyncSession()
        db.set_results(
            [
                FakeResult([nite_metal, nite_electronic]),  # name search
                FakeResult([nite_metal.id]),  # in-library subset
            ]
        )
        app = _api_app(user_id, db)
        cookie = _session_cookie(_make_settings().session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            resp = await c.get("/api/v1/artists/search?q=nite")

        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 2
        flags = {(i["disambiguation"], i["in_library"]) for i in items}
        assert ("heavy metal", True) in flags
        assert ("electronic", False) in flags


# --- lineup-data XSS (security review #141, finding #6) ---


class TestLineupDataXSS:
    """The lineup JSON embedded in the editor page must not break out of its
    <script> block. Artist/event names come from external APIs and manual entry,
    so a name containing ``</script>`` must be escaped, not rendered raw."""

    def test_script_breakout_is_escaped(self) -> None:
        """``playlists_new.html`` serializes the lineup with the markupsafe-aware
        ``tojson`` filter, so a ``</script>`` payload can't escape the tag."""
        import resonance.ui.common as common_ui

        payload = "</script><script>alert('xss')</script>"
        lineup = {
            "version": 0,
            "groups": [
                {
                    "label": "Added artists",
                    "rows": [{"artist_id": "x", "name": payload, "included": True}],
                }
            ],
        }
        ctx = {
            "request": SimpleNamespace(url=SimpleNamespace(path="/playlists/x/edit")),
            "user_id": "u",
            "user_tz": "UTC",
            "user_role": "owner",
            "actual_role": "owner",
            "viewing_as": None,
            "profile_id": "x",
            "profile_name": "My Playlist",
            "parameter_values": {},
            "similar_available": False,
            "events": [],
            "parameters": {},
            "lineup": lineup,
        }
        html = common_ui.templates.get_template("playlists_new.html").render(ctx)

        # No breakout: the raw payload must never appear verbatim in the page.
        assert payload not in html
        # The name is still present, escaped (tojson encodes "<" as <), which
        # also proves the template actually serialized `lineup` (not a no-op).
        assert "\\u003c/script\\u003e" in html

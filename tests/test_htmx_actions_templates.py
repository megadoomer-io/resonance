"""Templates use htmx-actions.js data attributes instead of hx-on.

The CSP has no 'unsafe-eval', so hx-on handlers (compiled with Function()) never
run in the browser. These render the affected templates and assert each element
carries the declarative attribute that replaces its old handler.
"""

from __future__ import annotations

import dataclasses
import datetime
import html.parser
import pathlib
import re
import uuid
from types import SimpleNamespace
from typing import Any

import pytest

import resonance.types as types_module
import resonance.ui.common as common_ui

_TEMPLATE_DIR = pathlib.Path(common_ui.__file__).parent.parent / "templates"
_NOW = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
_CANONICAL = uuid.UUID("11111111-1111-1111-1111-111111111111")


def _base_ctx(**extra: Any) -> dict[str, Any]:
    ctx: dict[str, Any] = {
        "request": SimpleNamespace(url=SimpleNamespace(path="/x")),
        "user_id": "u",
        "user_tz": "UTC",
        "user_role": "owner",
        "actual_role": "owner",
        "viewing_as": None,
    }
    ctx.update(extra)
    return ctx


@dataclasses.dataclass
class _Tag:
    name: str
    attrs: dict[str, str | None]
    text: str = ""

    def has(self, attr: str) -> bool:
        return attr in self.attrs


class _Collector(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tags: list[_Tag] = []
        self._open: list[_Tag] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        node = _Tag(tag, dict(attrs))
        self.tags.append(node)
        self._open.append(node)

    def handle_endtag(self, tag: str) -> None:
        while self._open:
            if self._open.pop().name == tag:
                break

    def handle_data(self, data: str) -> None:
        for node in self._open:
            node.text += data


def _render(template: str, **extra: Any) -> list[_Tag]:
    collector = _Collector()
    collector.feed(
        common_ui.templates.get_template(template).render(_base_ctx(**extra))
    )
    return collector.tags


def _one(tags: list[_Tag], **attrs: str) -> _Tag:
    matches = [t for t in tags if all(t.attrs.get(k) == v for k, v in attrs.items())]
    assert len(matches) == 1, attrs
    return matches[0]


def _conn(service: types_module.ServiceType) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        service_type=service,
        external_user_id="ext",
        label=None,
        url=None,
        last_synced_at=None,
        connected_at=_NOW,
    )


def test_no_template_uses_hx_on() -> None:
    offenders = [
        str(p.relative_to(_TEMPLATE_DIR))
        for p in _TEMPLATE_DIR.rglob("*.html")
        if re.search(r"\bhx-on", p.read_text())
    ]
    assert offenders == []


def test_base_loads_htmx_actions_after_htmx() -> None:
    soup = _render("base.html")
    srcs = [t.attrs["src"] for t in soup if t.name == "script" and t.attrs.get("src")]
    assert "/static/htmx-actions.js" in srcs
    assert srcs.index("/static/htmx-actions.js") > next(
        i for i, s in enumerate(srcs) if "htmx.org" in str(s)
    )


class TestDashboard:
    def test_cancel_reloads_after_request(self) -> None:
        conn = _conn(types_module.ServiceType.SPOTIFY)
        task = SimpleNamespace(
            id=uuid.uuid4(),
            status=types_module.SyncStatus.RUNNING,
            deferred_until=None,
        )
        soup = _render(
            "dashboard.html",
            connections=[conn],
            active_syncs={str(conn.id): task},
            syncable_services=set(),
        )
        btn = _one(soup, **{"hx-post": f"/api/v1/sync/cancel/{task.id}"})
        assert btn.has("data-after-request-reload")

    def test_sync_now_refreshes_status_and_disables(self) -> None:
        conn = _conn(types_module.ServiceType.SPOTIFY)
        soup = _render(
            "dashboard.html",
            connections=[conn],
            active_syncs={},
            syncable_services={types_module.ServiceType.SPOTIFY},
        )
        btn = _one(soup, **{"hx-post": f"/api/v1/sync/connection/{conn.id}"})
        assert btn.attrs["data-after-request-refresh"] == "#sync-status"
        assert btn.attrs["data-after-request-disable"] == "Syncing..."
        target = _one(soup, id="sync-status")
        assert target.attrs["hx-get"] == "/partials/sync-status"


class TestAccount:
    @pytest.fixture
    def soup(self) -> list[_Tag]:
        self.conn = _conn(types_module.ServiceType.SPOTIFY)
        return _render(
            "account.html",
            user=SimpleNamespace(
                display_name="Me", email=None, created_at=_NOW, timezone=None
            ),
            connections=[self.conn],
            authn_connectors=[],
            connected_services=set(),
            state="button",
        )

    def test_disconnect_reloads_after_request(self, soup: list[_Tag]) -> None:
        btn = _one(soup, **{"hx-delete": f"/api/v1/account/connections/{self.conn.id}"})
        assert btn.has("data-after-request-reload")

    def test_connect_test_service_reloads_after_request(self, soup: list[_Tag]) -> None:
        btn = _one(soup, **{"hx-post": "/api/v1/admin/test/connect"})
        assert btn.has("data-after-request-reload")


def test_playlist_delete_redirects_on_success() -> None:
    playlist = SimpleNamespace(
        id=uuid.uuid4(),
        name="P",
        description=None,
        track_count=0,
        created_at=_NOW,
        updated_at=_NOW,
        service_links={},
    )
    soup = _render(
        "playlist_detail.html",
        playlist=playlist,
        generation=None,
        tracks=[],
        spotify_connections=[],
        export_in_progress=False,
    )
    btn = _one(soup, **{"hx-delete": f"/api/v1/playlists/{playlist.id}"})
    assert btn.attrs["data-after-success-redirect"] == "/playlists"
    assert not btn.has("data-after-request-reload")


@pytest.mark.parametrize(
    ("template", "path"),
    [
        ("partials/merge_preview.html", "artists"),
        ("partials/track_merge_preview.html", "tracks"),
    ],
)
def test_merge_confirm_redirects_to_canonical_on_success(
    template: str, path: str
) -> None:
    canonical = SimpleNamespace(id=_CANONICAL, name="C", title="C")
    duplicate = SimpleNamespace(id=uuid.uuid4(), name="D", title="D")
    soup = _render(
        template,
        canonical=canonical,
        duplicate=duplicate,
        tracks_to_repoint=1,
        events_to_repoint=1,
        relations_to_repoint=1,
        playlist_appearances=0,
        merged_links={},
        duration_backfill=None,
    )
    [btn] = [
        t for t in soup if t.name == "button" and t.text.strip() == "Confirm Merge"
    ]
    assert btn.attrs["data-after-success-redirect"] == f"/{path}/{_CANONICAL}"


def test_concert_archives_upload_reloads_on_success_and_shows_errors() -> None:
    soup = _render("partials/concert_archives_connect.html", state="form")
    form = _one(soup, **{"hx-post": "/api/v1/connections/concert-archives/upload"})
    assert form.has("data-after-success-reload")
    assert form.attrs["data-after-error-show"] == "#ca-upload-error"
    assert form.attrs["data-after-error-fallback"] == "Upload failed."
    error_idx = soup.index(_one(soup, id="ca-upload-error"))
    assert soup[error_idx + 1].name == "mark"

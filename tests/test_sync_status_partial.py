"""Rendering of the dashboard's sync history partial: Cancel and cancelled rows."""

from __future__ import annotations

import datetime
import uuid
from types import SimpleNamespace

import resonance.types as types_module
import resonance.ui.common as common_ui


def _job(status: types_module.SyncStatus, error: str | None = None) -> SimpleNamespace:
    now = datetime.datetime.now(datetime.UTC)
    return SimpleNamespace(
        id=uuid.uuid4(),
        status=status,
        task_type=types_module.TaskType.SYNC_JOB,
        service_connection=SimpleNamespace(
            service_type=types_module.ServiceType.SPOTIFY
        ),
        children=[],
        description="Syncing",
        result=None,
        error_message=error,
        deferred_until=None,
        progress_current=0,
        progress_total=None,
        started_at=None,
        completed_at=None,
        created_at=now,
    )


def _render(*jobs: SimpleNamespace) -> str:
    return common_ui.templates.get_template("partials/sync_status.html").render(
        sync_jobs=list(jobs),
        has_active_sync=any(j.status in types_module.ACTIVE_STATUSES for j in jobs),
        user_tz=None,
        now=datetime.datetime.now(datetime.UTC),
    )


class TestSyncStatusCancel:
    def test_cancel_does_not_swap_json_into_the_status(self) -> None:
        job = _job(types_module.SyncStatus.PENDING)
        html = _render(job)
        assert f'hx-post="/api/v1/sync/cancel/{job.id}"' in html
        assert 'hx-swap="none"' in html
        assert 'hx-target="#sync-status"' not in html
        assert 'hx-disabled-elt="this"' in html

    def test_cancel_reloads_on_success_and_shows_error_otherwise(self) -> None:
        job = _job(types_module.SyncStatus.RUNNING)
        html = _render(job)
        assert "hx-on" not in html
        assert 'data-after-success-refresh="#sync-status"' in html
        assert "data-after-request-refresh" not in html
        assert f'data-after-error-show="#sync-cancel-error-{job.id}"' in html
        assert 'data-after-error-fallback="Couldn\'t cancel. Try again."' in html
        assert (
            f'<div id="sync-cancel-error-{job.id}" class="sync-cancel-error" hidden>'
            in html
        )
        assert "data-error-message>Couldn&#39;t cancel. Try again.</small>" in html

    def test_cancelled_job_has_its_own_display(self) -> None:
        html = _render(_job(types_module.SyncStatus.CANCELLED, "Cancelled by user"))
        assert '<span class="text-muted">Cancelled</span>' in html
        assert '<small class="text-muted">Cancelled by user</small>' in html
        assert "\n                    --\n" not in html
        assert ">Cancel</button>" not in html

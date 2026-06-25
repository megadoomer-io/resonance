"""Tests for in-flight playlist export status helpers (#dedup + UI indicator)."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import resonance.models.task as task_models
import resonance.services.playlist_export as playlist_export_module
import resonance.types as types_module


def _export_task(
    *,
    playlist_id: uuid.UUID,
    connection_id: uuid.UUID | None,
    status: types_module.SyncStatus = types_module.SyncStatus.RUNNING,
) -> task_models.Task:
    params: dict[str, str] = {"playlist_id": str(playlist_id)}
    if connection_id is not None:
        params["connection_id"] = str(connection_id)
    return task_models.Task(
        id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        task_type=types_module.TaskType.PLAYLIST_EXPORT,
        status=status,
        params=params,
    )


def _session_returning(tasks: list[task_models.Task]) -> AsyncMock:
    """Mock an AsyncSession whose execute().scalars().all() yields ``tasks``."""
    scalars = MagicMock()
    scalars.all.return_value = tasks
    result_obj = MagicMock()
    result_obj.scalars.return_value = scalars
    session = AsyncMock()
    session.execute = AsyncMock(return_value=result_obj)
    return session


class TestInProgressExportTasks:
    async def test_keeps_only_matching_playlist(self) -> None:
        pid = uuid.uuid4()
        other = uuid.uuid4()
        mine = _export_task(playlist_id=pid, connection_id=uuid.uuid4())
        theirs = _export_task(playlist_id=other, connection_id=uuid.uuid4())
        session = _session_returning([mine, theirs])

        result = await playlist_export_module.in_progress_export_tasks(
            session, uuid.uuid4(), pid
        )

        assert result == [mine]

    async def test_empty_when_no_active_export(self) -> None:
        session = _session_returning([])
        result = await playlist_export_module.in_progress_export_tasks(
            session, uuid.uuid4(), uuid.uuid4()
        )
        assert result == []

    async def test_handles_missing_params(self) -> None:
        pid = uuid.uuid4()
        broken = _export_task(playlist_id=pid, connection_id=None)
        broken.params = {}  # defensive: a task with no playlist_id must not match
        session = _session_returning([broken])
        result = await playlist_export_module.in_progress_export_tasks(
            session, uuid.uuid4(), pid
        )
        assert result == []


class TestExportConnectionIds:
    def test_extracts_and_dedupes(self) -> None:
        pid = uuid.uuid4()
        c1 = uuid.uuid4()
        c2 = uuid.uuid4()
        tasks = [
            _export_task(playlist_id=pid, connection_id=c1),
            _export_task(playlist_id=pid, connection_id=c2),
            _export_task(playlist_id=pid, connection_id=c1),  # duplicate connection
        ]
        assert playlist_export_module.export_connection_ids(tasks) == {c1, c2}

    def test_ignores_tasks_without_connection(self) -> None:
        pid = uuid.uuid4()
        tasks = [_export_task(playlist_id=pid, connection_id=None)]
        assert playlist_export_module.export_connection_ids(tasks) == set()

    def test_empty_list(self) -> None:
        assert playlist_export_module.export_connection_ids([]) == set()

"""Tests for sync strategy base types."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pydantic
import pytest

import resonance.sync.base as sync_base
import resonance.types as types_module

if TYPE_CHECKING:
    from unittest import mock


# ---------------------------------------------------------------------------
# SyncTaskDescriptor
# ---------------------------------------------------------------------------


class TestSyncTaskDescriptor:
    """Tests for the SyncTaskDescriptor Pydantic model."""

    def test_required_fields(self) -> None:
        desc = sync_base.SyncTaskDescriptor(
            task_type=types_module.SyncTaskType.TIME_RANGE,
            params={"start": "2024-01-01"},
        )
        assert desc.task_type == types_module.SyncTaskType.TIME_RANGE
        assert desc.params == {"start": "2024-01-01"}

    def test_optional_defaults(self) -> None:
        desc = sync_base.SyncTaskDescriptor(
            task_type=types_module.SyncTaskType.PAGE_FETCH,
            params={},
        )
        assert desc.progress_total is None
        assert desc.description == ""

    def test_optional_fields_set(self) -> None:
        desc = sync_base.SyncTaskDescriptor(
            task_type=types_module.SyncTaskType.SYNC_JOB,
            params={"key": "value"},
            progress_total=42,
            description="Fetch pages",
        )
        assert desc.progress_total == 42
        assert desc.description == "Fetch pages"

    def test_missing_required_field_raises(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            sync_base.SyncTaskDescriptor(task_type=types_module.SyncTaskType.TIME_RANGE)  # type: ignore[call-arg]

        with pytest.raises(pydantic.ValidationError):
            sync_base.SyncTaskDescriptor(params={"a": 1})  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# DeferRequest
# ---------------------------------------------------------------------------


class TestDeferRequest:
    """Tests for the DeferRequest exception."""

    def test_stores_attributes(self) -> None:
        dr = sync_base.DeferRequest(retry_after=300.0, resume_params={"offset": 100})
        assert dr.retry_after == 300.0
        assert dr.resume_params == {"offset": 100}

    def test_is_exception(self) -> None:
        dr = sync_base.DeferRequest(retry_after=60.0, resume_params={})
        assert isinstance(dr, Exception)

    def test_message_includes_retry_after(self) -> None:
        dr = sync_base.DeferRequest(retry_after=120.5, resume_params={})
        assert "120" in str(dr)
        assert "deferred" in str(dr).lower()

    def test_can_be_raised_and_caught(self) -> None:
        with pytest.raises(sync_base.DeferRequest) as exc_info:
            raise sync_base.DeferRequest(retry_after=45.0, resume_params={"page": 3})
        assert exc_info.value.retry_after == 45.0
        assert exc_info.value.resume_params == {"page": 3}


# ---------------------------------------------------------------------------
# SyncStrategy
# ---------------------------------------------------------------------------


class TestSyncStrategy:
    """Tests for the SyncStrategy abstract base class."""

    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            sync_base.SyncStrategy()  # type: ignore[abstract]

    def test_incomplete_subclass_cannot_instantiate(self) -> None:
        class PartialStrategy(sync_base.SyncStrategy):
            concurrency = "sequential"

            async def plan(
                self,
                session: mock.ANY,
                connection: mock.ANY,
                connector: mock.ANY,
            ) -> list[sync_base.SyncTaskDescriptor]:
                return []

            # execute intentionally missing

        with pytest.raises(TypeError):
            PartialStrategy()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_complete_subclass_can_instantiate(self) -> None:
        class FullStrategy(sync_base.SyncStrategy):
            concurrency = "parallel"

            async def plan(
                self,
                session: mock.ANY,
                connection: mock.ANY,
                connector: mock.ANY,
            ) -> list[sync_base.SyncTaskDescriptor]:
                return []

            async def execute(
                self,
                session: mock.ANY,
                task: mock.ANY,
                connector: mock.ANY,
            ) -> dict[str, object]:
                return {}

        strategy = FullStrategy()
        assert strategy.concurrency == "parallel"

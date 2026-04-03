"""Base classes for sync strategies."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Literal

import pydantic

import resonance.types as types_module  # noqa: TC001 — Pydantic needs at runtime

if TYPE_CHECKING:
    import sqlalchemy.ext.asyncio as sa_async

    import resonance.connectors.base as connector_base
    import resonance.models.task as task_module
    import resonance.models.user as user_models


class SyncTaskDescriptor(pydantic.BaseModel):
    """Lightweight description of a child task to create."""

    task_type: types_module.SyncTaskType
    params: dict[str, object]
    progress_total: int | None = None
    description: str = ""


class DeferRequest(Exception):  # noqa: N818 — not an error; a control-flow signal
    """Raised by execute() when a rate limit exceeds acceptable wait time."""

    def __init__(self, retry_after: float, resume_params: dict[str, object]) -> None:
        self.retry_after = retry_after
        self.resume_params = resume_params
        super().__init__(f"Sync deferred for {retry_after:.0f}s")


class SyncStrategy(abc.ABC):
    """Defines how a service plans and executes sync tasks."""

    concurrency: Literal["sequential", "parallel"]

    @abc.abstractmethod
    async def plan(
        self,
        session: sa_async.AsyncSession,
        connection: user_models.ServiceConnection,
        connector: connector_base.BaseConnector,
    ) -> list[SyncTaskDescriptor]:
        """Return child task descriptors for a sync job."""
        ...

    @abc.abstractmethod
    async def execute(
        self,
        session: sa_async.AsyncSession,
        task: task_module.SyncTask,
        connector: connector_base.BaseConnector,
    ) -> dict[str, object]:
        """Execute a single child task. May raise DeferRequest."""
        ...

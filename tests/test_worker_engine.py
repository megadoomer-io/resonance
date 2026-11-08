"""Worker connections carry an idle-in-transaction timeout and a name (#197)."""

from __future__ import annotations

import os
import socket
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.app as app_module
import resonance.config as config_module
import resonance.database as database_module
import resonance.worker as worker_module


class _EngineSpy:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(self, url: str, **kwargs: Any) -> MagicMock:
        self.calls.append(kwargs)
        return MagicMock(dispose=AsyncMock())


@pytest.fixture
def engine_spy(monkeypatch: pytest.MonkeyPatch) -> _EngineSpy:
    spy = _EngineSpy()
    monkeypatch.setattr(database_module.sa_async, "create_async_engine", spy)
    return spy


def test_default_timeout_is_thirty_minutes() -> None:
    assert config_module.Settings().worker_idle_tx_timeout_seconds == 1800


def test_timeout_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_IDLE_TX_TIMEOUT_SECONDS", "90")
    assert config_module.Settings().worker_idle_tx_timeout_seconds == 90


def test_worker_server_settings() -> None:
    settings = config_module.Settings(worker_idle_tx_timeout_seconds=90)

    assert worker_module.worker_server_settings(settings) == {
        "idle_in_transaction_session_timeout": "90000",
        "application_name": f"resonance-worker:{socket.gethostname()}:{os.getpid()}",
    }


def test_engine_passes_server_settings(engine_spy: _EngineSpy) -> None:
    database_module.create_async_engine(
        config_module.Settings(), server_settings={"application_name": "x"}
    )

    (kwargs,) = engine_spy.calls
    assert kwargs["connect_args"] == {"server_settings": {"application_name": "x"}}
    assert kwargs["pool_pre_ping"] is True


def test_engine_default_has_no_server_settings(engine_spy: _EngineSpy) -> None:
    database_module.create_async_engine(config_module.Settings())

    (kwargs,) = engine_spy.calls
    assert kwargs["connect_args"] == {}


async def test_worker_startup_engine_gets_server_settings(
    engine_spy: _EngineSpy, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _StopError(Exception):
        pass

    monkeypatch.setattr(
        worker_module.migrations_module,
        "assert_schema_current",
        AsyncMock(side_effect=_StopError),
    )

    with pytest.raises(_StopError):
        await worker_module.startup({"redis": AsyncMock()})

    (kwargs,) = engine_spy.calls
    server_settings = kwargs["connect_args"]["server_settings"]
    assert server_settings["idle_in_transaction_session_timeout"] == "1800000"
    assert server_settings["application_name"].startswith("resonance-worker:")


async def test_api_engine_has_no_server_settings(
    engine_spy: _EngineSpy, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _StopError(Exception):
        pass

    monkeypatch.setattr(
        app_module.migrations_module,
        "assert_schema_current",
        AsyncMock(side_effect=_StopError),
    )
    application = app_module.create_app(config_module.Settings(debug=True))

    with pytest.raises(_StopError):
        async with application.router.lifespan_context(application):
            pass

    (kwargs,) = engine_spy.calls
    assert kwargs["connect_args"] == {}

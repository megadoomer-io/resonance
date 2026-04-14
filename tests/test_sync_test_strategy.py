"""Tests for the test sync strategy."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.sync.test as test_sync
import resonance.types as types_module


class TestTestSyncStrategy:
    """Tests for TestSyncStrategy."""

    def test_concurrency(self) -> None:
        strategy = test_sync.TestSyncStrategy()
        assert strategy.concurrency == "sequential"

    @pytest.mark.asyncio
    async def test_plan_returns_one_descriptor(self) -> None:
        strategy = test_sync.TestSyncStrategy()
        session = AsyncMock()
        connection = MagicMock()
        connection.user_id = uuid.uuid4()
        connector = MagicMock()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 1
        desc = descriptors[0]
        assert desc.task_type == types_module.TaskType.TIME_RANGE
        assert desc.progress_total == 1
        assert desc.description == "Generate test data"
        assert "artists" in desc.params
        assert "tracks" in desc.params
        assert "listens" in desc.params
        assert "seed" in desc.params

    @pytest.mark.asyncio
    async def test_plan_uses_user_id_as_seed(self) -> None:
        strategy = test_sync.TestSyncStrategy()
        session = AsyncMock()
        user_id = uuid.uuid4()
        connection = MagicMock()
        connection.user_id = user_id
        connector = MagicMock()

        descriptors = await strategy.plan(session, connection, connector)
        assert descriptors[0].params["seed"] == str(user_id)


class TestSeededHex:
    """Tests for the _seeded_hex helper."""

    def test_deterministic(self) -> None:
        result1 = test_sync._seeded_hex("seed", "artist", 0)
        result2 = test_sync._seeded_hex("seed", "artist", 0)
        assert result1 == result2

    def test_different_inputs(self) -> None:
        result1 = test_sync._seeded_hex("seed", "artist", 0)
        result2 = test_sync._seeded_hex("seed", "artist", 1)
        assert result1 != result2

    def test_different_seeds(self) -> None:
        result1 = test_sync._seeded_hex("seed_a", "artist", 0)
        result2 = test_sync._seeded_hex("seed_b", "artist", 0)
        assert result1 != result2

    def test_returns_8_char_hex(self) -> None:
        result = test_sync._seeded_hex("seed", "artist", 0)
        assert len(result) == 8
        int(result, 16)  # should not raise — valid hex

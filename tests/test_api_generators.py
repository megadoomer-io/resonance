"""Tests for generator profile API endpoints."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import fastapi
import pytest

import resonance.api.v1.generators as generators_module
import resonance.types as types_module


class TestCreateProfileRequest:
    """Tests for CreateProfileRequest Pydantic model."""

    def test_valid_concert_prep(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {"event_id": str(uuid.uuid4())},
            "parameter_values": {"hit_depth": 75},
        }
        request = generators_module.CreateProfileRequest(**body)
        assert request.generator_type == types_module.GeneratorType.CONCERT_PREP
        assert request.name == "Show Prep"
        assert request.parameter_values == {"hit_depth": 75}

    def test_default_parameter_values(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {"event_id": str(uuid.uuid4())},
        }
        request = generators_module.CreateProfileRequest(**body)
        assert request.parameter_values == {}

    def test_empty_pool_raises(self) -> None:
        # No sources at all -> structurally empty pool -> clear error (#128).
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        with pytest.raises(ValueError, match="empty pool"):
            generators_module.validate_profile_inputs(
                request.input_references, request.generator_type
            )

    def test_valid_legacy_inputs_pass_validation(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {"event_id": str(uuid.uuid4())},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        # Legacy single-event shape resolves to one enabled source -> passes.
        generators_module.validate_profile_inputs(
            request.input_references, request.generator_type
        )

    def test_valid_layered_sources_pass_validation(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {
                "sources": [
                    {"kind": "event", "event_id": str(uuid.uuid4()), "enabled": True}
                ]
            },
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        generators_module.validate_profile_inputs(
            request.input_references, request.generator_type
        )

    def test_unknown_generator_type_still_requires_pool(self) -> None:
        """No type config skips the legacy key-check but still needs a pool."""
        valid = {
            "name": "Deep Dive",
            "generator_type": "artist_deep_dive",
            "input_references": {
                "sources": [
                    {"kind": "artist", "artist_id": str(uuid.uuid4()), "enabled": True}
                ]
            },
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**valid)
        # A valid source passes even with no type config.
        generators_module.validate_profile_inputs(
            request.input_references, request.generator_type
        )
        # But an empty pool still raises, config or not.
        empty = generators_module.CreateProfileRequest(
            name="Deep Dive",
            generator_type="artist_deep_dive",
            input_references={},
        )
        with pytest.raises(ValueError, match="empty pool"):
            generators_module.validate_profile_inputs(
                empty.input_references, empty.generator_type
            )

    def test_malformed_source_raises(self) -> None:
        body = {
            "name": "Bad",
            "generator_type": "concert_prep",
            "input_references": {"sources": [{"kind": "nonsense"}]},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        with pytest.raises(ValueError, match="Invalid input_references"):
            generators_module.validate_profile_inputs(
                request.input_references, request.generator_type
            )


class TestUpdateProfileRequest:
    """Tests for UpdateProfileRequest Pydantic model."""

    def test_partial_update(self) -> None:
        body = {"parameter_values": {"hit_depth": 25}}
        request = generators_module.UpdateProfileRequest(**body)
        assert request.name is None
        assert request.parameter_values == {"hit_depth": 25}
        assert request.input_references is None

    def test_name_only_update(self) -> None:
        body = {"name": "New Name"}
        request = generators_module.UpdateProfileRequest(**body)
        assert request.name == "New Name"
        assert request.parameter_values is None

    def test_empty_update(self) -> None:
        request = generators_module.UpdateProfileRequest()
        assert request.name is None
        assert request.parameter_values is None
        assert request.input_references is None


class TestGenerateRequest:
    """Tests for GenerateRequest Pydantic model."""

    def test_freshness_target(self) -> None:
        body = {"freshness_target": 50}
        request = generators_module.GenerateRequest(**body)
        assert request.freshness_target == 50

    def test_default_no_freshness(self) -> None:
        request = generators_module.GenerateRequest()
        assert request.freshness_target is None

    def test_default_max_tracks(self) -> None:
        request = generators_module.GenerateRequest()
        assert request.max_tracks == 30

    def test_custom_max_tracks(self) -> None:
        body = {"max_tracks": 50, "freshness_target": 75}
        request = generators_module.GenerateRequest(**body)
        assert request.max_tracks == 50
        assert request.freshness_target == 75


class TestValidateProfileInputs:
    """Tests for validate_profile_inputs function."""

    def test_empty_input_references_raise_empty_pool(self) -> None:
        """An input spec with no sources reports a clear empty-pool error."""
        body = {
            "name": "Test",
            "generator_type": "concert_prep",
            "input_references": {},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        with pytest.raises(ValueError, match="empty pool"):
            generators_module.validate_profile_inputs(
                request.input_references, request.generator_type
            )

    def test_disabled_only_sources_raise_empty_pool(self) -> None:
        """All-disabled sources resolve to an empty pool."""
        body = {
            "name": "Test",
            "generator_type": "concert_prep",
            "input_references": {
                "sources": [
                    {"kind": "event", "event_id": str(uuid.uuid4()), "enabled": False}
                ]
            },
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        with pytest.raises(ValueError, match="empty pool"):
            generators_module.validate_profile_inputs(
                request.input_references, request.generator_type
            )

    def test_extra_legacy_keys_are_allowed(self) -> None:
        """Extra keys alongside a legacy event_id are ignored, not rejected."""
        body = {
            "name": "Test",
            "generator_type": "concert_prep",
            "input_references": {
                "event_id": str(uuid.uuid4()),
                "extra_ref": "some-value",
            },
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        # Should not raise
        generators_module.validate_profile_inputs(
            request.input_references, request.generator_type
        )


# --- enrich endpoint (#133) ---


class _FakeResult:
    def __init__(self, items: list[Any] | None = None) -> None:
        self._items = items or []

    def scalar_one_or_none(self) -> Any:
        return self._items[0] if self._items else None


class _FakeSession:
    """Returns preset execute() results in order; records added objects."""

    def __init__(self, results: list[Any]) -> None:
        self._results = results
        self._i = 0
        self.added: list[Any] = []
        self.committed = False

    async def execute(self, *_a: Any, **_k: Any) -> Any:
        result = (
            self._results[self._i] if self._i < len(self._results) else _FakeResult()
        )
        self._i += 1
        return result

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def commit(self) -> None:
        self.committed = True


def _fake_request() -> Any:
    req = MagicMock(spec=fastapi.Request)
    req.app.state.arq_redis.enqueue_job = AsyncMock()
    return req


class TestEnrichRequest:
    """EnrichRequest validation."""

    def test_lineup_literal_valid(self) -> None:
        body = generators_module.EnrichRequest(seed_artist_ids="lineup", n=5)
        assert body.seed_artist_ids == "lineup"
        assert body.n == 5

    def test_seed_list_valid(self) -> None:
        aid = uuid.uuid4()
        body = generators_module.EnrichRequest(seed_artist_ids=[aid])
        assert body.seed_artist_ids == [aid]
        assert body.n == 10  # default

    def test_empty_list_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            generators_module.EnrichRequest(seed_artist_ids=[])

    def test_too_many_seeds_rejected(self) -> None:
        seeds = [uuid.uuid4() for _ in range(51)]
        with pytest.raises(ValueError, match="at most"):
            generators_module.EnrichRequest(seed_artist_ids=seeds)

    def test_n_zero_rejected(self) -> None:
        with pytest.raises(ValueError):
            generators_module.EnrichRequest(seed_artist_ids="lineup", n=0)

    def test_n_over_cap_rejected(self) -> None:
        with pytest.raises(ValueError):
            generators_module.EnrichRequest(seed_artist_ids="lineup", n=51)


class TestEnrichEndpoint:
    """enrich_profile: 404 / 409 / success."""

    async def test_404_when_profile_missing(self) -> None:
        db = _FakeSession([_FakeResult([])])  # profile lookup -> none
        body = generators_module.EnrichRequest(seed_artist_ids="lineup")
        with pytest.raises(fastapi.HTTPException) as exc:
            await generators_module.enrich_profile(
                uuid.uuid4(), body, _fake_request(), uuid.uuid4(), db
            )
        assert exc.value.status_code == 404

    async def test_success_lineup(self) -> None:
        profile = SimpleNamespace(id=uuid.uuid4())
        db = _FakeSession([_FakeResult([profile]), _FakeResult([])])
        req = _fake_request()
        body = generators_module.EnrichRequest(seed_artist_ids="lineup", n=7)
        result = await generators_module.enrich_profile(
            profile.id, body, req, uuid.uuid4(), db
        )
        assert result["status"] == "started"
        tasks = [a for a in db.added if hasattr(a, "task_type")]
        assert len(tasks) == 1
        task = tasks[0]
        assert task.task_type == types_module.TaskType.RELATED_ARTIST_ENRICHMENT
        assert task.params["profile_id"] == str(profile.id)
        assert task.params["seed_artist_ids"] == "lineup"
        assert task.params["n"] == 7
        assert result["task_id"] == str(task.id)
        req.app.state.arq_redis.enqueue_job.assert_awaited_once()
        assert req.app.state.arq_redis.enqueue_job.await_args.args[0] == (
            "enrich_related_artists"
        )

    async def test_success_per_seed_stores_str_ids(self) -> None:
        profile = SimpleNamespace(id=uuid.uuid4())
        seed = uuid.uuid4()
        db = _FakeSession([_FakeResult([profile]), _FakeResult([])])
        body = generators_module.EnrichRequest(seed_artist_ids=[seed], n=3)
        await generators_module.enrich_profile(
            profile.id, body, _fake_request(), uuid.uuid4(), db
        )
        task = next(a for a in db.added if hasattr(a, "task_type"))
        assert task.params["seed_artist_ids"] == [str(seed)]

    async def test_409_when_task_running(self) -> None:
        profile = SimpleNamespace(id=uuid.uuid4())
        running = SimpleNamespace(id=uuid.uuid4())
        db = _FakeSession([_FakeResult([profile]), _FakeResult([running])])
        body = generators_module.EnrichRequest(seed_artist_ids="lineup")
        with pytest.raises(fastapi.HTTPException) as exc:
            await generators_module.enrich_profile(
                profile.id, body, _fake_request(), uuid.uuid4(), db
            )
        assert exc.value.status_code == 409


class TestUpdateProfileConcurrency:
    """update_profile surfaces an optimistic-version conflict as a 409 (#133)."""

    async def test_stale_data_returns_409(self) -> None:
        import sqlalchemy.orm.exc as orm_exc

        profile = SimpleNamespace(
            id=uuid.uuid4(),
            name="old",
            generator_type=types_module.GeneratorType.CONCERT_PREP,
            input_references={},
            parameter_values={},
        )

        class _StaleSession:
            def __init__(self) -> None:
                self.rolled_back = False

            async def execute(self, *_a: Any, **_k: Any) -> Any:
                return _FakeResult([profile])

            async def commit(self) -> None:
                raise orm_exc.StaleDataError("conflict")

            async def rollback(self) -> None:
                self.rolled_back = True

        db = _StaleSession()
        body = generators_module.UpdateProfileRequest(name="new")
        with pytest.raises(fastapi.HTTPException) as exc:
            await generators_module.update_profile(profile.id, body, uuid.uuid4(), db)
        assert exc.value.status_code == 409
        assert db.rolled_back is True


class TestUpdateProfileExpectedVersion:
    """update_profile honors the client's optimistic-version token (#133)."""

    async def test_stale_expected_version_returns_409(self) -> None:
        profile = SimpleNamespace(
            id=uuid.uuid4(),
            name="old",
            version=3,
            status=types_module.ProfileStatus.ACTIVE,
            generator_type=types_module.GeneratorType.CONCERT_PREP,
            input_references={},
            parameter_values={},
        )
        db = _FakeSession([_FakeResult([profile])])
        body = generators_module.UpdateProfileRequest(name="new", expected_version=1)
        with pytest.raises(fastapi.HTTPException) as exc:
            await generators_module.update_profile(profile.id, body, uuid.uuid4(), db)
        assert exc.value.status_code == 409

    async def test_matching_expected_version_updates(self) -> None:
        profile = SimpleNamespace(
            id=uuid.uuid4(),
            name="old",
            version=3,
            status=types_module.ProfileStatus.ACTIVE,
            generator_type=types_module.GeneratorType.CONCERT_PREP,
            input_references={},
            parameter_values={},
        )
        db = _FakeSession([_FakeResult([profile])])
        body = generators_module.UpdateProfileRequest(name="new", expected_version=3)
        result = await generators_module.update_profile(
            profile.id, body, uuid.uuid4(), db
        )
        assert result["status"] == "updated"
        assert profile.name == "new"


class TestGenerateFlipsDraftActive:
    """First generate flips a draft profile to active (#133)."""

    async def test_draft_becomes_active(self) -> None:
        profile = SimpleNamespace(
            id=uuid.uuid4(), status=types_module.ProfileStatus.DRAFT
        )
        db = _FakeSession([_FakeResult([profile]), _FakeResult([])])
        result = await generators_module.trigger_generation(
            profile.id, _fake_request(), uuid.uuid4(), db, None
        )
        assert result["status"] == "started"
        assert profile.status == types_module.ProfileStatus.ACTIVE


class TestMutualExclusion:
    """Generation and enrichment block each other via the shared helper."""

    async def test_enrich_blocked_by_running_generation(self) -> None:
        profile = SimpleNamespace(id=uuid.uuid4())
        running_gen = SimpleNamespace(
            id=uuid.uuid4(), task_type=types_module.TaskType.PLAYLIST_GENERATION
        )
        db = _FakeSession([_FakeResult([profile]), _FakeResult([running_gen])])
        body = generators_module.EnrichRequest(seed_artist_ids="lineup")
        with pytest.raises(fastapi.HTTPException) as exc:
            await generators_module.enrich_profile(
                profile.id, body, _fake_request(), uuid.uuid4(), db
            )
        assert exc.value.status_code == 409

    async def test_generate_blocked_by_running_enrichment(self) -> None:
        profile = SimpleNamespace(
            id=uuid.uuid4(), status=types_module.ProfileStatus.ACTIVE
        )
        running_enrich = SimpleNamespace(
            id=uuid.uuid4(),
            task_type=types_module.TaskType.RELATED_ARTIST_ENRICHMENT,
        )
        db = _FakeSession([_FakeResult([profile]), _FakeResult([running_enrich])])
        with pytest.raises(fastapi.HTTPException) as exc:
            await generators_module.trigger_generation(
                profile.id, _fake_request(), uuid.uuid4(), db, None
            )
        assert exc.value.status_code == 409

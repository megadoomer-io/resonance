"""CRITICAL regression: optimistic concurrency on GeneratorProfile (#133 T5).

The enrich worker and the builder PATCH both write
``GeneratorProfile.input_references``. Without a guard, the worker reading the
row and committing after a concurrent PATCH would silently clobber the edit
(lost update). ``version_id_col`` turns that race into a ``StaleDataError`` the
callers handle (PATCH -> 409, worker -> reload + re-apply).

These tests exercise the real ORM behavior against an in-memory SQLite engine
(version_id_col is dialect-agnostic), so the mapper config is verified end to end
rather than mocked.
"""

from __future__ import annotations

import uuid

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy.orm.exc as orm_exc

import resonance.models as models
import resonance.models.generator as generator_models
import resonance.models.user as user_models
import resonance.types as types_module


def _seeded_engine() -> tuple[sa.Engine, uuid.UUID]:
    engine = sa.create_engine("sqlite://")
    models.Base.metadata.create_all(engine)
    maker = orm.sessionmaker(engine)
    with maker() as session:
        user = user_models.User(display_name="Versioning User")
        session.add(user)
        session.flush()
        profile = generator_models.GeneratorProfile(
            user_id=user.id,
            name="P",
            generator_type=types_module.GeneratorType.CONCERT_PREP,
            input_references={"sources": [], "exclude_artist_ids": []},
            parameter_values={},
        )
        session.add(profile)
        session.commit()
        return engine, profile.id


def _artist_refs() -> dict[str, object]:
    return {
        "sources": [
            {"kind": "artist", "artist_id": str(uuid.uuid4()), "enabled": True}
        ],
        "exclude_artist_ids": [],
    }


class TestOptimisticVersioning:
    def test_new_profile_starts_at_version_1(self) -> None:
        engine, pid = _seeded_engine()
        with orm.sessionmaker(engine)() as session:
            profile = session.get(generator_models.GeneratorProfile, pid)
            assert profile is not None
            assert profile.version == 1

    def test_update_increments_version(self) -> None:
        engine, pid = _seeded_engine()
        maker = orm.sessionmaker(engine)
        with maker() as session:
            profile = session.get(generator_models.GeneratorProfile, pid)
            assert profile is not None
            profile.input_references = _artist_refs()
            session.commit()
            assert profile.version == 2

    def test_concurrent_update_raises_stale_data(self) -> None:
        """Two writers load v1; the second to commit must not clobber."""
        engine, pid = _seeded_engine()
        maker = orm.sessionmaker(engine)
        session_a = maker()
        session_b = maker()
        try:
            prof_a = session_a.get(generator_models.GeneratorProfile, pid)
            prof_b = session_b.get(generator_models.GeneratorProfile, pid)
            assert prof_a is not None and prof_b is not None
            assert prof_a.version == prof_b.version == 1

            # Writer A commits first -> version becomes 2.
            prof_a.input_references = _artist_refs()
            session_a.commit()

            # Writer B still thinks it's v1: its UPDATE matches 0 rows -> raises,
            # rather than overwriting A's change.
            prof_b.input_references = _artist_refs()
            with pytest.raises(orm_exc.StaleDataError):
                session_b.commit()
        finally:
            session_a.close()
            session_b.close()

    def test_refresh_then_reapply_succeeds(self) -> None:
        """The worker's recovery path: refresh to the new version, re-commit."""
        engine, pid = _seeded_engine()
        maker = orm.sessionmaker(engine)
        session_a = maker()
        session_b = maker()
        try:
            prof_a = session_a.get(generator_models.GeneratorProfile, pid)
            prof_b = session_b.get(generator_models.GeneratorProfile, pid)
            assert prof_a is not None and prof_b is not None

            prof_a.input_references = _artist_refs()
            session_a.commit()  # version -> 2

            prof_b.input_references = _artist_refs()
            with pytest.raises(orm_exc.StaleDataError):
                session_b.commit()
            session_b.rollback()

            # Reload fresh (version 2), re-apply, commit -> succeeds at version 3.
            session_b.refresh(prof_b)
            assert prof_b.version == 2
            prof_b.input_references = _artist_refs()
            session_b.commit()
            assert prof_b.version == 3
        finally:
            session_a.close()
            session_b.close()

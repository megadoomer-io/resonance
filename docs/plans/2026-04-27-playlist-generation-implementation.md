# Playlist Generation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a playlist generation system starting with the "concert prep" generator — given an upcoming concert event, generate a playlist from the user's library and external discovery sources.

**Architecture:** Generator profiles store recipes (type + inputs + parameters). Running a profile spawns a hierarchical task tree (parent generation → discovery children → scoring child) that produces an immutable Playlist linked back via a GenerationRecord. Parameters use a code-defined registry with bipolar/unipolar scale types.

**Tech Stack:** Python 3.14, FastAPI, SQLAlchemy 2.0 async, PostgreSQL, arq, MusicBrainz/ListenBrainz APIs, pytest, structlog.

**Design doc:** `docs/plans/2026-04-27-playlist-generation-design.md`

---

### Task 1: Add new enums to types.py

**Files:**
- Modify: `src/resonance/types.py`
- Test: `tests/test_types.py` (create)

**Step 1: Write tests for new enum values**

```python
# tests/test_types.py
"""Tests for type enumerations."""

import resonance.types as types_module


class TestGeneratorType:
    def test_concert_prep_value(self) -> None:
        assert types_module.GeneratorType.CONCERT_PREP == "concert_prep"

    def test_all_values_exist(self) -> None:
        expected = {"concert_prep", "artist_deep_dive", "rediscovery",
                    "discography", "playlist_refresh", "curated_mix"}
        actual = {g.value for g in types_module.GeneratorType}
        assert expected == actual


class TestParameterScaleType:
    def test_bipolar_value(self) -> None:
        assert types_module.ParameterScaleType.BIPOLAR == "bipolar"

    def test_unipolar_value(self) -> None:
        assert types_module.ParameterScaleType.UNIPOLAR == "unipolar"


class TestTrackSource:
    def test_values(self) -> None:
        expected = {"library", "discovery", "manual"}
        actual = {s.value for s in types_module.TrackSource}
        assert expected == actual


class TestNewTaskTypes:
    def test_playlist_generation(self) -> None:
        assert types_module.TaskType.PLAYLIST_GENERATION == "playlist_generation"

    def test_track_discovery(self) -> None:
        assert types_module.TaskType.TRACK_DISCOVERY == "track_discovery"

    def test_track_scoring(self) -> None:
        assert types_module.TaskType.TRACK_SCORING == "track_scoring"
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_types.py -v`
Expected: FAIL — GeneratorType, ParameterScaleType, TrackSource not defined

**Step 3: Add enums to types.py**

Add after the existing `CandidateStatus` enum (line 91):

```python
class GeneratorType(enum.StrEnum):
    """Types of playlist generators."""

    CONCERT_PREP = "concert_prep"
    ARTIST_DEEP_DIVE = "artist_deep_dive"
    REDISCOVERY = "rediscovery"
    DISCOGRAPHY = "discography"
    PLAYLIST_REFRESH = "playlist_refresh"
    CURATED_MIX = "curated_mix"


class ParameterScaleType(enum.StrEnum):
    """Scale types for generator parameters."""

    BIPOLAR = "bipolar"
    UNIPOLAR = "unipolar"


class TrackSource(enum.StrEnum):
    """How a track was sourced for a playlist."""

    LIBRARY = "library"
    DISCOVERY = "discovery"
    MANUAL = "manual"
```

Add new TaskType values to the existing `TaskType` enum (after `CALENDAR_SYNC`):

```python
    PLAYLIST_GENERATION = "playlist_generation"
    TRACK_DISCOVERY = "track_discovery"
    TRACK_SCORING = "track_scoring"
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_types.py -v`
Expected: PASS

**Step 5: Run full suite + type checks**

Run: `uv run pytest && uv run mypy src/ && uv run ruff check .`
Expected: All pass. The new TaskType values are additive and don't affect existing code.

**Step 6: Commit**

```bash
git add src/resonance/types.py tests/test_types.py
git commit -m "feat: add generator, parameter, and track source enums"
```

---

### Task 2: Add TRACK_DISCOVERY capability and DiscoveredTrack model

**Files:**
- Modify: `src/resonance/connectors/base.py` (add capability + dataclass)
- Test: `tests/test_connector_config.py` (add test)

**Step 1: Write test for new capability**

Add to `tests/test_connector_config.py`:

```python
class TestTrackDiscoveryCapability:
    def test_track_discovery_exists(self) -> None:
        assert base_module.ConnectorCapability.TRACK_DISCOVERY == "track_discovery"

    def test_discovered_track_fields(self) -> None:
        track = base_module.DiscoveredTrack(
            external_id="abc123",
            title="Test Song",
            artist_name="Test Artist",
            artist_external_id="artist123",
            service=types_module.ServiceType.LISTENBRAINZ,
            popularity_score=75,
        )
        assert track.title == "Test Song"
        assert track.popularity_score == 75
        assert track.duration_ms is None
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_connector_config.py::TestTrackDiscoveryCapability -v`
Expected: FAIL — DiscoveredTrack not defined

**Step 3: Add capability and dataclass**

In `src/resonance/connectors/base.py`, add `TRACK_DISCOVERY` to `ConnectorCapability` enum (after `NEW_RELEASES`):

```python
    TRACK_DISCOVERY = "track_discovery"
```

Add `DiscoveredTrack` Pydantic model after `TrackData`:

```python
class DiscoveredTrack(pydantic.BaseModel):
    """Track discovered from an external service for playlist generation."""

    external_id: str
    title: str
    artist_name: str
    artist_external_id: str
    service: types_module.ServiceType
    popularity_score: int = 0
    duration_ms: int | None = None
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_connector_config.py -v && uv run mypy src/`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/connectors/base.py tests/test_connector_config.py
git commit -m "feat: add TRACK_DISCOVERY capability and DiscoveredTrack model"
```

---

### Task 3: Create Playlist and PlaylistTrack models

**Files:**
- Create: `src/resonance/models/playlist.py`
- Modify: `src/resonance/models/__init__.py`
- Test: `tests/test_models.py` (add tests)

**Step 1: Write tests**

Add to `tests/test_models.py`:

```python
class TestPlaylistModel:
    def test_playlist_fields(self) -> None:
        playlist = models_module.Playlist(
            user_id=uuid.uuid4(),
            name="Concert Prep",
        )
        assert playlist.name == "Concert Prep"
        assert playlist.track_count == 0
        assert playlist.is_pinned is False
        assert playlist.description is None

    def test_playlist_track_fields(self) -> None:
        track = models_module.PlaylistTrack(
            playlist_id=uuid.uuid4(),
            track_id=uuid.uuid4(),
            position=1,
            source="library",
        )
        assert track.position == 1
        assert track.source == "library"
        assert track.score is None
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_models.py::TestPlaylistModel -v`
Expected: FAIL — Playlist, PlaylistTrack not defined

**Step 3: Create the models**

Create `src/resonance/models/playlist.py`:

```python
"""Playlist models for generated and curated track lists."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module

if TYPE_CHECKING:
    import resonance.models.music as music_module


class Playlist(base_module.TimestampMixin, base_module.Base):
    """An ordered collection of tracks."""

    __tablename__ = "playlists"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)
    description: orm.Mapped[str | None] = orm.mapped_column(sa.Text, nullable=True)
    track_count: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=0
    )
    is_pinned: orm.Mapped[bool] = orm.mapped_column(
        sa.Boolean, nullable=False, default=False
    )

    tracks: orm.Mapped[list[PlaylistTrack]] = orm.relationship(
        back_populates="playlist",
        cascade="all, delete-orphan",
        order_by="PlaylistTrack.position",
    )


class PlaylistTrack(base_module.TimestampMixin, base_module.Base):
    """A track's position and metadata within a playlist."""

    __tablename__ = "playlist_tracks"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    playlist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, sa.ForeignKey("playlists.id", ondelete="CASCADE"), nullable=False
    )
    track_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, sa.ForeignKey("tracks.id", ondelete="CASCADE"), nullable=False
    )
    position: orm.Mapped[int] = orm.mapped_column(sa.Integer, nullable=False)
    score: orm.Mapped[float | None] = orm.mapped_column(sa.Float, nullable=True)
    source: orm.Mapped[str] = orm.mapped_column(sa.String(64), nullable=False)

    playlist: orm.Mapped[Playlist] = orm.relationship(back_populates="tracks")
    track: orm.Mapped[music_module.Track] = orm.relationship()
```

Update `src/resonance/models/__init__.py` to export:

```python
from resonance.models.playlist import Playlist, PlaylistTrack
```

Add to `__all__`:

```python
    "Playlist",
    "PlaylistTrack",
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_models.py::TestPlaylistModel -v && uv run mypy src/`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/models/playlist.py src/resonance/models/__init__.py tests/test_models.py
git commit -m "feat: add Playlist and PlaylistTrack models"
```

---

### Task 4: Create GeneratorProfile and GenerationRecord models

**Files:**
- Create: `src/resonance/models/generator.py`
- Modify: `src/resonance/models/__init__.py`
- Test: `tests/test_models.py` (add tests)

**Step 1: Write tests**

Add to `tests/test_models.py`:

```python
class TestGeneratorModels:
    def test_generator_profile_fields(self) -> None:
        profile = models_module.GeneratorProfile(
            user_id=uuid.uuid4(),
            name="Show Prep",
            generator_type=types_module.GeneratorType.CONCERT_PREP,
            input_references={"event_id": str(uuid.uuid4())},
            parameter_values={"hit_depth": 75, "familiarity": 40},
        )
        assert profile.name == "Show Prep"
        assert profile.generator_type == types_module.GeneratorType.CONCERT_PREP
        assert profile.parameter_values["hit_depth"] == 75
        assert profile.auto_sync_targets is None

    def test_generation_record_fields(self) -> None:
        record = models_module.GenerationRecord(
            profile_id=uuid.uuid4(),
            playlist_id=uuid.uuid4(),
            parameter_snapshot={"hit_depth": 75},
            freshness_target=50,
        )
        assert record.freshness_target == 50
        assert record.freshness_actual is None
        assert record.generation_duration_ms is None
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_models.py::TestGeneratorModels -v`
Expected: FAIL — GeneratorProfile, GenerationRecord not defined

**Step 3: Create the models**

Create `src/resonance/models/generator.py`:

```python
"""Generator profile and generation record models."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module

if TYPE_CHECKING:
    import resonance.models.playlist as playlist_module


class GeneratorProfile(base_module.TimestampMixin, base_module.Base):
    """A saved playlist generation recipe."""

    __tablename__ = "generator_profiles"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)
    generator_type: orm.Mapped[types_module.GeneratorType] = orm.mapped_column(
        sa.Enum(types_module.GeneratorType, native_enum=False), nullable=False
    )
    input_references: orm.Mapped[dict[str, object]] = orm.mapped_column(
        sa.JSON, nullable=False, default=dict
    )
    parameter_values: orm.Mapped[dict[str, object]] = orm.mapped_column(
        sa.JSON, nullable=False, default=dict
    )
    auto_sync_targets: orm.Mapped[list[dict[str, str]] | None] = orm.mapped_column(
        sa.JSON, nullable=True
    )

    generations: orm.Mapped[list[GenerationRecord]] = orm.relationship(
        back_populates="profile",
        cascade="all, delete-orphan",
        order_by="GenerationRecord.created_at.desc()",
    )

    @sa.event.listens_for(sa.orm.Session, "before_flush")
    @staticmethod
    def _set_defaults(
        session: sa.orm.Session,
        flush_context: object,
        instances: object,
    ) -> None:
        for obj in session.new:
            if isinstance(obj, GeneratorProfile):
                if obj.input_references is None:
                    obj.input_references = {}
                if obj.parameter_values is None:
                    obj.parameter_values = {}


class GenerationRecord(base_module.TimestampMixin, base_module.Base):
    """Links a Playlist to the GeneratorProfile run that created it."""

    __tablename__ = "generation_records"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    profile_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid,
        sa.ForeignKey("generator_profiles.id", ondelete="CASCADE"),
        nullable=False,
    )
    playlist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid,
        sa.ForeignKey("playlists.id", ondelete="CASCADE"),
        nullable=False,
    )
    parameter_snapshot: orm.Mapped[dict[str, object]] = orm.mapped_column(
        sa.JSON, nullable=False
    )
    freshness_target: orm.Mapped[int | None] = orm.mapped_column(
        sa.Integer, nullable=True
    )
    freshness_actual: orm.Mapped[float | None] = orm.mapped_column(
        sa.Float, nullable=True
    )
    generation_duration_ms: orm.Mapped[int | None] = orm.mapped_column(
        sa.Integer, nullable=True
    )
    track_sources_summary: orm.Mapped[dict[str, int] | None] = orm.mapped_column(
        sa.JSON, nullable=True
    )

    profile: orm.Mapped[GeneratorProfile] = orm.relationship(
        back_populates="generations"
    )
    playlist: orm.Mapped[playlist_module.Playlist] = orm.relationship()
```

Update `src/resonance/models/__init__.py` to export:

```python
from resonance.models.generator import GenerationRecord, GeneratorProfile
```

Add to `__all__` (alphabetical):

```python
    "GenerationRecord",
    "GeneratorProfile",
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_models.py::TestGeneratorModels -v && uv run mypy src/`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/models/generator.py src/resonance/models/__init__.py tests/test_models.py
git commit -m "feat: add GeneratorProfile and GenerationRecord models"
```

---

### Task 5: Create Alembic migration for new tables

**Files:**
- Create: `alembic/versions/s6n7o8p9q0r1_add_playlist_and_generator_tables.py`

**Step 1: Write the migration manually**

Since we don't have a live database for autogenerate, write manually. The migration creates four tables: `playlists`, `playlist_tracks`, `generator_profiles`, `generation_records`.

```python
"""Add playlist and generator tables.

Revision ID: s6n7o8p9q0r1
Revises: r5m6n7o8p9q0
Create Date: 2026-04-27
"""

from alembic import op
import sqlalchemy as sa

revision = "s6n7o8p9q0r1"
down_revision = "r5m6n7o8p9q0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "playlists",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(512), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("track_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_pinned", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "playlist_tracks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("playlist_id", sa.Uuid(), nullable=False),
        sa.Column("track_id", sa.Uuid(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("source", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["playlist_id"], ["playlists.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["track_id"], ["tracks.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "generator_profiles",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(512), nullable=False),
        sa.Column(
            "generator_type",
            sa.String(50),
            nullable=False,
        ),
        sa.Column("input_references", sa.JSON(), nullable=False),
        sa.Column("parameter_values", sa.JSON(), nullable=False),
        sa.Column("auto_sync_targets", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "generator_type IN ('CONCERT_PREP', 'ARTIST_DEEP_DIVE', 'REDISCOVERY', "
            "'DISCOGRAPHY', 'PLAYLIST_REFRESH', 'CURATED_MIX')",
            name="ck_generator_profiles_generator_type",
        ),
    )

    op.create_table(
        "generation_records",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("profile_id", sa.Uuid(), nullable=False),
        sa.Column("playlist_id", sa.Uuid(), nullable=False),
        sa.Column("parameter_snapshot", sa.JSON(), nullable=False),
        sa.Column("freshness_target", sa.Integer(), nullable=True),
        sa.Column("freshness_actual", sa.Float(), nullable=True),
        sa.Column("generation_duration_ms", sa.Integer(), nullable=True),
        sa.Column("track_sources_summary", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["profile_id"], ["generator_profiles.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["playlist_id"], ["playlists.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Add CHECK constraint for new TaskType values
    op.execute(sa.text(
        "ALTER TABLE sync_tasks DROP CONSTRAINT IF EXISTS ck_sync_tasks_task_type"
    ))
    op.create_check_constraint(
        "ck_sync_tasks_task_type",
        "sync_tasks",
        "task_type IN ('SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
        "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', 'TRACK_DISCOVERY', 'TRACK_SCORING')",
    )


def downgrade() -> None:
    op.drop_table("generation_records")
    op.drop_table("generator_profiles")
    op.drop_table("playlist_tracks")
    op.drop_table("playlists")

    op.execute(sa.text(
        "ALTER TABLE sync_tasks DROP CONSTRAINT IF EXISTS ck_sync_tasks_task_type"
    ))
    op.create_check_constraint(
        "ck_sync_tasks_task_type",
        "sync_tasks",
        "task_type IN ('SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
        "'CALENDAR_SYNC')",
    )
```

**Step 2: Verify migration is valid Python**

Run: `uv run python -c "import alembic.versions.s6n7o8p9q0r1_add_playlist_and_generator_tables" 2>&1 || echo "Import check — migration will be applied at deploy time"`

**Step 3: Run full test suite**

Run: `uv run pytest && uv run mypy src/`
Expected: PASS (migration doesn't run during tests since there's no live DB)

**Step 4: Commit**

```bash
git add alembic/versions/s6n7o8p9q0r1_add_playlist_and_generator_tables.py
git commit -m "feat: add migration for playlist and generator tables"
```

---

### Task 6: Create the generator parameter registry

**Files:**
- Create: `src/resonance/generators/__init__.py`
- Create: `src/resonance/generators/parameters.py`
- Test: `tests/test_parameters.py` (create)

**Step 1: Write tests**

```python
# tests/test_parameters.py
"""Tests for the generator parameter registry."""

import resonance.generators.parameters as params_module
import resonance.types as types_module


class TestParameterDefinition:
    def test_bipolar_parameter(self) -> None:
        param = params_module.PARAMETER_REGISTRY["hit_depth"]
        assert param.scale_type == types_module.ParameterScaleType.BIPOLAR
        assert param.default_value == 50
        assert param.labels == ("Deep Cuts", "Big Hits")

    def test_unipolar_parameter(self) -> None:
        param = params_module.PARAMETER_REGISTRY["similar_artist_ratio"]
        assert param.scale_type == types_module.ParameterScaleType.UNIPOLAR
        assert param.default_value == 0

    def test_familiarity_parameter(self) -> None:
        param = params_module.PARAMETER_REGISTRY["familiarity"]
        assert param.scale_type == types_module.ParameterScaleType.BIPOLAR
        assert param.default_value == 50
        assert param.labels == ("All Discovery", "All Known Tracks")


class TestGeneratorTypeConfig:
    def test_concert_prep_featured_params(self) -> None:
        config = params_module.GENERATOR_TYPE_CONFIG[
            types_module.GeneratorType.CONCERT_PREP
        ]
        assert "familiarity" in config.featured_parameters
        assert "hit_depth" in config.featured_parameters

    def test_concert_prep_required_inputs(self) -> None:
        config = params_module.GENERATOR_TYPE_CONFIG[
            types_module.GeneratorType.CONCERT_PREP
        ]
        assert "event_id" in config.required_inputs


class TestApplyDefaults:
    def test_fills_missing_with_defaults(self) -> None:
        result = params_module.apply_defaults({"hit_depth": 75})
        assert result["hit_depth"] == 75
        assert result["familiarity"] == 50
        assert result["similar_artist_ratio"] == 0

    def test_preserves_all_provided(self) -> None:
        provided = {"hit_depth": 25, "familiarity": 80, "similar_artist_ratio": 30}
        result = params_module.apply_defaults(provided)
        assert result == provided
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_parameters.py -v`
Expected: FAIL — module not found

**Step 3: Create the parameter registry**

Create `src/resonance/generators/__init__.py`:

```python
"""Playlist generation system."""
```

Create `src/resonance/generators/parameters.py`:

```python
"""Generator parameter registry and type configurations."""

from __future__ import annotations

import dataclasses

import resonance.types as types_module


@dataclasses.dataclass(frozen=True)
class ParameterDefinition:
    """A named parameter with display and scale metadata."""

    name: str
    display_name: str
    description: str
    scale_type: types_module.ParameterScaleType
    default_value: int
    labels: tuple[str, str]


@dataclasses.dataclass(frozen=True)
class GeneratorTypeConfig:
    """Configuration for a generator type."""

    featured_parameters: frozenset[str]
    required_inputs: frozenset[str]
    description: str


PARAMETER_REGISTRY: dict[str, ParameterDefinition] = {
    "familiarity": ParameterDefinition(
        name="familiarity",
        display_name="Familiarity",
        description="Balance between tracks you know and new discovery",
        scale_type=types_module.ParameterScaleType.BIPOLAR,
        default_value=50,
        labels=("All Discovery", "All Known Tracks"),
    ),
    "hit_depth": ParameterDefinition(
        name="hit_depth",
        display_name="Hit Depth",
        description="Balance between deep cuts and popular tracks",
        scale_type=types_module.ParameterScaleType.BIPOLAR,
        default_value=50,
        labels=("Deep Cuts", "Big Hits"),
    ),
    "similar_artist_ratio": ParameterDefinition(
        name="similar_artist_ratio",
        display_name="Similar Artists",
        description="How much to include tracks from adjacent/similar artists",
        scale_type=types_module.ParameterScaleType.UNIPOLAR,
        default_value=0,
        labels=("Target Artists Only", "Heavy Adjacent Artists"),
    ),
}

GENERATOR_TYPE_CONFIG: dict[types_module.GeneratorType, GeneratorTypeConfig] = {
    types_module.GeneratorType.CONCERT_PREP: GeneratorTypeConfig(
        featured_parameters=frozenset({"familiarity", "hit_depth"}),
        required_inputs=frozenset({"event_id"}),
        description="Generate a playlist to prepare for a concert",
    ),
}


def apply_defaults(
    provided: dict[str, object],
) -> dict[str, int]:
    """Fill in missing parameter values with registry defaults."""
    result: dict[str, int] = {}
    for name, defn in PARAMETER_REGISTRY.items():
        value = provided.get(name)
        result[name] = int(value) if value is not None else defn.default_value
    return result
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_parameters.py -v && uv run mypy src/`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/generators/__init__.py src/resonance/generators/parameters.py tests/test_parameters.py
git commit -m "feat: add generator parameter registry with concert prep config"
```

---

### Task 7: Build the scoring engine

**Files:**
- Create: `src/resonance/generators/scoring.py`
- Test: `tests/test_scoring.py` (create)

**Step 1: Write tests**

```python
# tests/test_scoring.py
"""Tests for the playlist scoring engine."""

import resonance.generators.scoring as scoring_module


class TestFamiliaritySignal:
    def test_never_heard_returns_zero(self) -> None:
        assert scoring_module.familiarity_signal(listen_count=0, in_library=False) == 0.0

    def test_high_listen_count(self) -> None:
        score = scoring_module.familiarity_signal(listen_count=100, in_library=True)
        assert score > 0.8

    def test_in_library_low_listens(self) -> None:
        score = scoring_module.familiarity_signal(listen_count=1, in_library=True)
        assert 0.0 < score < 0.5


class TestPopularitySignal:
    def test_zero_popularity(self) -> None:
        assert scoring_module.popularity_signal(popularity_score=0) == 0.0

    def test_max_popularity(self) -> None:
        assert scoring_module.popularity_signal(popularity_score=100) == 1.0

    def test_mid_popularity(self) -> None:
        score = scoring_module.popularity_signal(popularity_score=50)
        assert 0.4 <= score <= 0.6


class TestArtistRelevanceSignal:
    def test_target_artist(self) -> None:
        assert scoring_module.artist_relevance_signal(is_target_artist=True) == 1.0

    def test_adjacent_artist(self) -> None:
        assert scoring_module.artist_relevance_signal(is_target_artist=False) == 0.0


class TestBipolarWeight:
    def test_neutral_returns_zero(self) -> None:
        assert scoring_module.bipolar_weight(50) == 0.0

    def test_max_returns_positive(self) -> None:
        assert scoring_module.bipolar_weight(100) == 1.0

    def test_min_returns_negative(self) -> None:
        assert scoring_module.bipolar_weight(0) == -1.0

    def test_seventy_five(self) -> None:
        assert scoring_module.bipolar_weight(75) == 0.5


class TestCompositeScore:
    def test_neutral_params_score_from_relevance(self) -> None:
        score = scoring_module.composite_score(
            familiarity_val=0.5,
            popularity_val=0.5,
            is_target_artist=True,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
        )
        assert 0.0 <= score <= 1.0

    def test_high_familiarity_boosts_known_tracks(self) -> None:
        known = scoring_module.composite_score(
            familiarity_val=0.9,
            popularity_val=0.5,
            is_target_artist=True,
            params={"familiarity": 90, "hit_depth": 50, "similar_artist_ratio": 0},
        )
        unknown = scoring_module.composite_score(
            familiarity_val=0.1,
            popularity_val=0.5,
            is_target_artist=True,
            params={"familiarity": 90, "hit_depth": 50, "similar_artist_ratio": 0},
        )
        assert known > unknown
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_scoring.py -v`
Expected: FAIL — module not found

**Step 3: Implement the scoring engine**

Create `src/resonance/generators/scoring.py`:

```python
"""Scoring engine for playlist track selection."""

from __future__ import annotations

import math


def familiarity_signal(*, listen_count: int, in_library: bool) -> float:
    """Score from 0.0 (never heard) to 1.0 (most played).

    Uses a logarithmic curve so early listens matter more than
    the difference between 80 and 100 listens.
    """
    if not in_library and listen_count == 0:
        return 0.0
    return min(1.0, math.log1p(listen_count) / math.log1p(100))


def popularity_signal(*, popularity_score: int) -> float:
    """Score from 0.0 (obscure) to 1.0 (biggest hit).

    Linear mapping from the 0-100 external popularity score.
    """
    return max(0.0, min(1.0, popularity_score / 100.0))


def artist_relevance_signal(*, is_target_artist: bool) -> float:
    """1.0 for target artists, 0.0 for adjacent artists."""
    return 1.0 if is_target_artist else 0.0


def bipolar_weight(param_value: int) -> float:
    """Convert a 0-100 bipolar parameter to a -1.0 to 1.0 weight.

    50 = neutral (0.0), 0 = full negative (-1.0), 100 = full positive (1.0).
    """
    return (param_value - 50) / 50.0


def composite_score(
    *,
    familiarity_val: float,
    popularity_val: float,
    is_target_artist: bool,
    params: dict[str, int],
) -> float:
    """Compute composite score for a candidate track.

    Bipolar parameters shift the score up or down based on the signal.
    The similar_artist_ratio parameter gates adjacent artist inclusion
    (handled by the caller during track selection, not here).

    Returns a value clamped to [0.0, 1.0].
    """
    base = 0.5

    fam_weight = bipolar_weight(params.get("familiarity", 50))
    hit_weight = bipolar_weight(params.get("hit_depth", 50))

    relevance = artist_relevance_signal(is_target_artist=is_target_artist)

    score = base
    score += fam_weight * (familiarity_val - 0.5)
    score += hit_weight * (popularity_val - 0.5)
    score *= 0.5 + 0.5 * relevance

    return max(0.0, min(1.0, score))
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_scoring.py -v && uv run mypy src/`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/generators/scoring.py tests/test_scoring.py
git commit -m "feat: add scoring engine with signal functions and composite scoring"
```

---

### Task 8: Build the concert prep generator core logic

**Files:**
- Create: `src/resonance/generators/concert_prep.py`
- Test: `tests/test_concert_prep_generator.py` (create)

This is the main generator logic — given an event, resolve artists, source tracks
from library and discovery, score them, and produce an ordered track list. This
module is pure logic (no database access) — it takes pre-fetched data as input.

**Step 1: Write tests**

```python
# tests/test_concert_prep_generator.py
"""Tests for the concert prep generator."""

import uuid

import resonance.generators.concert_prep as concert_prep_module


class TestBuildCandidateList:
    def test_library_tracks_included(self) -> None:
        artist_id = uuid.uuid4()
        track_id = uuid.uuid4()
        library_tracks = [
            concert_prep_module.CandidateTrack(
                track_id=track_id,
                title="Known Song",
                artist_name="Band A",
                artist_id=artist_id,
                is_target_artist=True,
                listen_count=50,
                in_library=True,
                popularity_score=0,
                source="library",
            )
        ]
        result = concert_prep_module.score_and_select(
            candidates=library_tracks,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert len(result.tracks) == 1
        assert result.tracks[0].track_id == track_id

    def test_respects_max_tracks(self) -> None:
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=uuid.uuid4(),
                title=f"Song {i}",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=i,
                in_library=True,
                popularity_score=50,
                source="library",
            )
            for i in range(50)
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=20,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert len(result.tracks) == 20


class TestFreshnessFilter:
    def test_full_freshness_excludes_previous(self) -> None:
        prev_id = uuid.uuid4()
        new_id = uuid.uuid4()
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=prev_id,
                title="Old Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=100,
                in_library=True,
                popularity_score=90,
                source="library",
            ),
            concert_prep_module.CandidateTrack(
                track_id=new_id,
                title="New Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=10,
                in_library=True,
                popularity_score=50,
                source="library",
            ),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids={prev_id},
            freshness_target=100,
        )
        track_ids = {t.track_id for t in result.tracks}
        assert prev_id not in track_ids
        assert new_id in track_ids

    def test_zero_freshness_allows_all(self) -> None:
        prev_id = uuid.uuid4()
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=prev_id,
                title="Old Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=100,
                in_library=True,
                popularity_score=90,
                source="library",
            ),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids={prev_id},
            freshness_target=0,
        )
        assert len(result.tracks) == 1


class TestSelectionResult:
    def test_tracks_ordered_by_position(self) -> None:
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=uuid.uuid4(),
                title=f"Song {i}",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=i * 10,
                in_library=True,
                popularity_score=50,
                source="library",
            )
            for i in range(5)
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 80, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids=set(),
            freshness_target=None,
        )
        positions = [t.position for t in result.tracks]
        assert positions == list(range(1, len(result.tracks) + 1))

    def test_source_summary_computed(self) -> None:
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=uuid.uuid4(),
                title="Lib Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=10,
                in_library=True,
                popularity_score=50,
                source="library",
            ),
            concert_prep_module.CandidateTrack(
                track_id=uuid.uuid4(),
                title="Disc Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=0,
                in_library=False,
                popularity_score=60,
                source="discovery",
            ),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert result.sources_summary["library"] == 1
        assert result.sources_summary["discovery"] == 1
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_concert_prep_generator.py -v`
Expected: FAIL — module not found

**Step 3: Implement**

Create `src/resonance/generators/concert_prep.py`:

```python
"""Concert prep playlist generator — pure scoring and selection logic."""

from __future__ import annotations

import dataclasses
import uuid
from collections import Counter

import resonance.generators.scoring as scoring_module


@dataclasses.dataclass(frozen=True)
class CandidateTrack:
    """A track candidate for playlist inclusion."""

    track_id: uuid.UUID
    title: str
    artist_name: str
    artist_id: uuid.UUID
    is_target_artist: bool
    listen_count: int
    in_library: bool
    popularity_score: int
    source: str  # "library" or "discovery"


@dataclasses.dataclass(frozen=True)
class ScoredTrack:
    """A track with its composite score and position."""

    track_id: uuid.UUID
    title: str
    artist_name: str
    position: int
    score: float
    source: str


@dataclasses.dataclass(frozen=True)
class SelectionResult:
    """The output of score_and_select."""

    tracks: list[ScoredTrack]
    sources_summary: dict[str, int]
    freshness_actual: float | None


def score_and_select(
    *,
    candidates: list[CandidateTrack],
    params: dict[str, int],
    max_tracks: int,
    previous_track_ids: set[uuid.UUID],
    freshness_target: int | None,
) -> SelectionResult:
    """Score candidates and select the top tracks for the playlist."""
    scored: list[tuple[float, CandidateTrack]] = []

    for candidate in candidates:
        fam_val = scoring_module.familiarity_signal(
            listen_count=candidate.listen_count,
            in_library=candidate.in_library,
        )
        pop_val = scoring_module.popularity_signal(
            popularity_score=candidate.popularity_score,
        )
        score = scoring_module.composite_score(
            familiarity_val=fam_val,
            popularity_val=pop_val,
            is_target_artist=candidate.is_target_artist,
            params=params,
        )
        scored.append((score, candidate))

    scored.sort(key=lambda x: x[0], reverse=True)

    # Apply freshness filter
    if freshness_target is not None and freshness_target > 0 and previous_track_ids:
        max_repeats = max(
            0,
            int(max_tracks * (1 - freshness_target / 100.0)),
        )
        selected: list[tuple[float, CandidateTrack]] = []
        repeat_count = 0

        for score, candidate in scored:
            if len(selected) >= max_tracks:
                break
            if candidate.track_id in previous_track_ids:
                if repeat_count >= max_repeats:
                    continue
                repeat_count += 1
            selected.append((score, candidate))
    else:
        selected = scored[:max_tracks]

    # Build result
    tracks: list[ScoredTrack] = []
    for i, (score, candidate) in enumerate(selected):
        tracks.append(
            ScoredTrack(
                track_id=candidate.track_id,
                title=candidate.title,
                artist_name=candidate.artist_name,
                position=i + 1,
                score=score,
                source=candidate.source,
            )
        )

    source_counts = Counter(t.source for t in tracks)
    sources_summary = dict(source_counts)

    freshness_actual: float | None = None
    if previous_track_ids and tracks:
        new_tracks = sum(
            1 for t in tracks if t.track_id not in previous_track_ids
        )
        freshness_actual = new_tracks / len(tracks) * 100.0

    return SelectionResult(
        tracks=tracks,
        sources_summary=sources_summary,
        freshness_actual=freshness_actual,
    )
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_concert_prep_generator.py -v && uv run mypy src/`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/generators/concert_prep.py tests/test_concert_prep_generator.py
git commit -m "feat: add concert prep generator with scoring and freshness filtering"
```

---

### Task 9: Add ListenBrainz/MusicBrainz track discovery

**Files:**
- Modify: `src/resonance/connectors/listenbrainz.py`
- Test: `tests/test_listenbrainz_connector.py` (add tests)

This adds the `TRACK_DISCOVERY` capability to the ListenBrainz connector.
It fetches an artist's top recordings from MusicBrainz and optionally enriches
with ListenBrainz popularity data.

**Step 1: Write tests**

Add to `tests/test_listenbrainz_connector.py`:

```python
class TestDiscoverTracks:
    def test_discover_by_mbid(self, connector: listenbrainz_module.ListenBrainzConnector) -> None:
        """Discovers tracks when service_links contains a MusicBrainz ID."""
        mb_response = httpx.Response(
            200,
            json={
                "recordings": [
                    {
                        "id": "rec-1",
                        "title": "Song One",
                        "length": 240000,
                    },
                    {
                        "id": "rec-2",
                        "title": "Song Two",
                        "length": 180000,
                    },
                ]
            },
        )
        with unittest.mock.patch.object(
            connector, "_request", return_value=mb_response
        ) as mock_req:
            result = asyncio.run(
                connector.discover_tracks(
                    artist_name="Test Artist",
                    service_links={"listenbrainz": "artist-mbid-123"},
                    limit=10,
                )
            )
        assert len(result) == 2
        assert result[0].title == "Song One"
        assert result[0].artist_name == "Test Artist"
        assert result[0].external_id == "rec-1"
        assert result[0].service == types_module.ServiceType.LISTENBRAINZ

    def test_discover_by_name_search(self, connector: listenbrainz_module.ListenBrainzConnector) -> None:
        """Falls back to MusicBrainz name search when no MBID in service_links."""
        search_response = httpx.Response(
            200,
            json={
                "artists": [
                    {"id": "found-mbid", "name": "Test Artist", "score": 100}
                ]
            },
        )
        recordings_response = httpx.Response(
            200,
            json={
                "recordings": [
                    {"id": "rec-1", "title": "Found Song", "length": 200000}
                ]
            },
        )
        with unittest.mock.patch.object(
            connector, "_request", side_effect=[search_response, recordings_response]
        ):
            result = asyncio.run(
                connector.discover_tracks(
                    artist_name="Test Artist",
                    service_links=None,
                    limit=5,
                )
            )
        assert len(result) == 1
        assert result[0].title == "Found Song"

    def test_discover_returns_empty_on_no_match(self, connector: listenbrainz_module.ListenBrainzConnector) -> None:
        """Returns empty list when artist not found in MusicBrainz."""
        search_response = httpx.Response(
            200,
            json={"artists": []},
        )
        with unittest.mock.patch.object(
            connector, "_request", return_value=search_response
        ):
            result = asyncio.run(
                connector.discover_tracks(
                    artist_name="Unknown Artist",
                    service_links=None,
                    limit=5,
                )
            )
        assert result == []
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_listenbrainz_connector.py::TestDiscoverTracks -v`
Expected: FAIL — `discover_tracks` not defined

**Step 3: Implement**

In `src/resonance/connectors/listenbrainz.py`:

1. Add `TRACK_DISCOVERY` to the capabilities frozenset:
   ```python
   capabilities = frozenset({
       ConnectorCapability.AUTHENTICATION,
       ConnectorCapability.LISTENING_HISTORY,
       ConnectorCapability.TRACK_DISCOVERY,
   })
   ```

2. Add the `discover_tracks` method:
   ```python
   _MUSICBRAINZ_API = "https://musicbrainz.org/ws/2"

   async def discover_tracks(
       self,
       artist_name: str,
       service_links: dict[str, str] | None,
       limit: int = 20,
   ) -> list[base_module.DiscoveredTrack]:
       """Discover tracks for an artist via MusicBrainz recordings."""
       mbid = (service_links or {}).get("listenbrainz")

       if not mbid:
           # Search MusicBrainz by name
           search_resp = await self._request(
               "GET",
               f"{self._MUSICBRAINZ_API}/artist/",
               params={"query": artist_name, "fmt": "json", "limit": 1},
           )
           artists = search_resp.json().get("artists", [])
           if not artists:
               return []
           mbid = artists[0]["id"]

       # Fetch recordings for artist
       rec_resp = await self._request(
           "GET",
           f"{self._MUSICBRAINZ_API}/recording/",
           params={
               "artist": mbid,
               "fmt": "json",
               "limit": limit,
           },
       )
       recordings = rec_resp.json().get("recordings", [])

       return [
           base_module.DiscoveredTrack(
               external_id=rec["id"],
               title=rec["title"],
               artist_name=artist_name,
               artist_external_id=mbid,
               service=types_module.ServiceType.LISTENBRAINZ,
               duration_ms=rec.get("length"),
               popularity_score=max(0, 100 - i * 5),
           )
           for i, rec in enumerate(recordings)
       ]
   ```

**Step 4: Run tests**

Run: `uv run pytest tests/test_listenbrainz_connector.py::TestDiscoverTracks -v && uv run mypy src/`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/connectors/listenbrainz.py tests/test_listenbrainz_connector.py
git commit -m "feat: add TRACK_DISCOVERY capability to ListenBrainz connector"
```

---

### Task 10: Add generator API endpoints (profile CRUD + generate)

**Files:**
- Create: `src/resonance/api/v1/generators.py`
- Modify: `src/resonance/api/v1/__init__.py`
- Test: `tests/test_api_generators.py` (create)

**Step 1: Write tests**

```python
# tests/test_api_generators.py
"""Tests for generator profile API endpoints."""

import uuid
import unittest.mock

import fastapi
import fastapi.testclient
import pytest

import resonance.api.v1.generators as generators_module
import resonance.types as types_module


@pytest.fixture
def mock_db():
    """Yield a mock async session."""
    return unittest.mock.AsyncMock()


class TestCreateProfile:
    def test_valid_concert_prep(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {"event_id": str(uuid.uuid4())},
            "parameter_values": {"hit_depth": 75},
        }
        request = generators_module.CreateProfileRequest(**body)
        assert request.generator_type == types_module.GeneratorType.CONCERT_PREP
        assert request.parameter_values["hit_depth"] == 75

    def test_missing_required_input_raises(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        with pytest.raises(ValueError, match="event_id"):
            generators_module.validate_profile_inputs(request)


class TestUpdateProfile:
    def test_partial_update(self) -> None:
        body = {"parameter_values": {"hit_depth": 25}}
        request = generators_module.UpdateProfileRequest(**body)
        assert request.name is None
        assert request.parameter_values == {"hit_depth": 25}


class TestGenerateRequest:
    def test_freshness_target(self) -> None:
        body = {"freshness_target": 50}
        request = generators_module.GenerateRequest(**body)
        assert request.freshness_target == 50

    def test_default_no_freshness(self) -> None:
        request = generators_module.GenerateRequest()
        assert request.freshness_target is None
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api_generators.py -v`
Expected: FAIL — module not found

**Step 3: Implement the API module**

Create `src/resonance/api/v1/generators.py`:

```python
"""Generator profile API routes."""

from __future__ import annotations

import uuid
from typing import Annotated

import fastapi
import pydantic
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.dependencies as deps_module
import resonance.generators.parameters as params_module
import resonance.models.generator as generator_models
import resonance.models.playlist as playlist_models
import resonance.models.task as task_models
import resonance.types as types_module

router = fastapi.APIRouter(prefix="/generator-profiles", tags=["generators"])


class CreateProfileRequest(pydantic.BaseModel):
    name: str
    generator_type: types_module.GeneratorType
    input_references: dict[str, str]
    parameter_values: dict[str, int] = pydantic.Field(default_factory=dict)


class UpdateProfileRequest(pydantic.BaseModel):
    name: str | None = None
    parameter_values: dict[str, int] | None = None
    input_references: dict[str, str] | None = None


class GenerateRequest(pydantic.BaseModel):
    freshness_target: int | None = None
    max_tracks: int = 30


def validate_profile_inputs(request: CreateProfileRequest) -> None:
    """Validate that required inputs are present for the generator type."""
    config = params_module.GENERATOR_TYPE_CONFIG.get(request.generator_type)
    if config is None:
        return
    for required in config.required_inputs:
        if required not in request.input_references:
            msg = f"Missing required input: {required}"
            raise ValueError(msg)


@router.post("")
async def create_profile(
    body: CreateProfileRequest,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    try:
        validate_profile_inputs(body)
    except ValueError as exc:
        raise fastapi.HTTPException(status_code=422, detail=str(exc)) from exc

    filled_params = params_module.apply_defaults(body.parameter_values)

    profile = generator_models.GeneratorProfile(
        user_id=user_id,
        name=body.name,
        generator_type=body.generator_type,
        input_references=body.input_references,
        parameter_values=filled_params,
    )
    db.add(profile)
    await db.commit()
    return {"id": str(profile.id), "status": "created"}


@router.get("")
async def list_profiles(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> list[dict[str, object]]:
    result = await db.execute(
        sa.select(generator_models.GeneratorProfile)
        .where(generator_models.GeneratorProfile.user_id == user_id)
        .order_by(generator_models.GeneratorProfile.updated_at.desc())
    )
    profiles = result.scalars().all()
    return [
        {
            "id": str(p.id),
            "name": p.name,
            "generator_type": p.generator_type.value,
            "input_references": p.input_references,
            "parameter_values": p.parameter_values,
            "created_at": p.created_at.isoformat() if p.created_at else None,
            "updated_at": p.updated_at.isoformat() if p.updated_at else None,
        }
        for p in profiles
    ]


@router.get("/{profile_id}")
async def get_profile(
    profile_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    result = await db.execute(
        sa.select(generator_models.GeneratorProfile).where(
            generator_models.GeneratorProfile.id == profile_id,
            generator_models.GeneratorProfile.user_id == user_id,
        )
    )
    profile = result.scalar_one_or_none()
    if profile is None:
        raise fastapi.HTTPException(status_code=404, detail="Profile not found")

    # Fetch generation history
    gen_result = await db.execute(
        sa.select(generator_models.GenerationRecord)
        .where(generator_models.GenerationRecord.profile_id == profile_id)
        .order_by(generator_models.GenerationRecord.created_at.desc())
        .limit(20)
    )
    generations = gen_result.scalars().all()

    return {
        "id": str(profile.id),
        "name": profile.name,
        "generator_type": profile.generator_type.value,
        "input_references": profile.input_references,
        "parameter_values": profile.parameter_values,
        "auto_sync_targets": profile.auto_sync_targets,
        "created_at": profile.created_at.isoformat() if profile.created_at else None,
        "generations": [
            {
                "id": str(g.id),
                "playlist_id": str(g.playlist_id),
                "parameter_snapshot": g.parameter_snapshot,
                "freshness_target": g.freshness_target,
                "freshness_actual": g.freshness_actual,
                "generation_duration_ms": g.generation_duration_ms,
                "track_sources_summary": g.track_sources_summary,
                "created_at": g.created_at.isoformat() if g.created_at else None,
            }
            for g in generations
        ],
    }


@router.patch("/{profile_id}")
async def update_profile(
    profile_id: uuid.UUID,
    body: UpdateProfileRequest,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    result = await db.execute(
        sa.select(generator_models.GeneratorProfile).where(
            generator_models.GeneratorProfile.id == profile_id,
            generator_models.GeneratorProfile.user_id == user_id,
        )
    )
    profile = result.scalar_one_or_none()
    if profile is None:
        raise fastapi.HTTPException(status_code=404, detail="Profile not found")

    if body.name is not None:
        profile.name = body.name
    if body.parameter_values is not None:
        current = dict(profile.parameter_values)
        current.update(body.parameter_values)
        profile.parameter_values = current
    if body.input_references is not None:
        profile.input_references = body.input_references

    await db.commit()
    return {"id": str(profile.id), "status": "updated"}


@router.delete("/{profile_id}")
async def delete_profile(
    profile_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    result = await db.execute(
        sa.select(generator_models.GeneratorProfile).where(
            generator_models.GeneratorProfile.id == profile_id,
            generator_models.GeneratorProfile.user_id == user_id,
        )
    )
    profile = result.scalar_one_or_none()
    if profile is None:
        raise fastapi.HTTPException(status_code=404, detail="Profile not found")

    await db.delete(profile)
    await db.commit()
    return {"id": str(profile.id), "status": "deleted"}


@router.post("/{profile_id}/generate")
async def trigger_generation(
    profile_id: uuid.UUID,
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    body: GenerateRequest | None = None,
) -> dict[str, str]:
    result = await db.execute(
        sa.select(generator_models.GeneratorProfile).where(
            generator_models.GeneratorProfile.id == profile_id,
            generator_models.GeneratorProfile.user_id == user_id,
        )
    )
    profile = result.scalar_one_or_none()
    if profile is None:
        raise fastapi.HTTPException(status_code=404, detail="Profile not found")

    gen_body = body or GenerateRequest()

    task = task_models.Task(
        user_id=user_id,
        task_type=types_module.TaskType.PLAYLIST_GENERATION,
        status=types_module.SyncStatus.PENDING,
        params={
            "profile_id": str(profile_id),
            "freshness_target": gen_body.freshness_target,
            "max_tracks": gen_body.max_tracks,
        },
        description=f"Generate playlist: {profile.name}",
    )
    db.add(task)
    await db.commit()

    arq_redis = request.app.state.arq_redis
    await arq_redis.enqueue_job(
        "generate_playlist",
        str(task.id),
        _job_id=f"generate_playlist:{task.id}",
    )

    return {"task_id": str(task.id), "status": "started"}
```

Update `src/resonance/api/v1/__init__.py` to include the new router:

```python
import resonance.api.v1.generators as generators_module
router.include_router(generators_module.router)
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_api_generators.py -v && uv run mypy src/`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/api/v1/generators.py src/resonance/api/v1/__init__.py tests/test_api_generators.py
git commit -m "feat: add generator profile API endpoints with CRUD and generation trigger"
```

---

### Task 11: Add playlist API endpoints

**Files:**
- Create: `src/resonance/api/v1/playlists.py`
- Modify: `src/resonance/api/v1/__init__.py`
- Test: `tests/test_api_playlists.py` (create)

**Step 1: Write tests for request/response models**

**Step 2: Implement playlist list, detail, and diff endpoints**

Endpoints:
- `GET /api/v1/playlists` — list user's playlists
- `GET /api/v1/playlists/{id}` — playlist with tracks
- `GET /api/v1/playlists/{id}/diff/{other_id}` — compare two playlist versions

Follow the same patterns as `generators.py`. The diff endpoint compares track
lists and returns added/removed/common track IDs with counts.

**Step 3: Register router in `__init__.py`**

**Step 4: Run tests and type checks**

**Step 5: Commit**

```bash
git add src/resonance/api/v1/playlists.py src/resonance/api/v1/__init__.py tests/test_api_playlists.py
git commit -m "feat: add playlist API endpoints with list, detail, and diff"
```

---

### Task 12: Add worker tasks for playlist generation

**Files:**
- Modify: `src/resonance/worker.py`
- Modify: `src/resonance/types.py` (already done in Task 1)
- Test: `tests/test_worker.py` (add tests)

This adds three arq job functions: `generate_playlist` (parent),
`discover_tracks_for_artist` (child), and `score_and_build_playlist` (child).

**Step 1: Write tests for the generate_playlist orchestrator**

Test that `generate_playlist`:
- Loads the profile and resolves event → artists
- Creates TRACK_DISCOVERY child tasks per artist needing discovery
- Creates a TRACK_SCORING child task
- Handles the case where all tracks are in library (no discovery needed)

**Step 2: Write tests for discover_tracks_for_artist**

Test that:
- It calls the connector's `discover_tracks` method
- It upserts discovered tracks into the database
- It marks the task COMPLETED with result summary
- It handles rate limit exceeded → DEFERRED

**Step 3: Write tests for score_and_build_playlist**

Test that:
- It queries library tracks for the event's artists
- It calls `concert_prep.score_and_select()`
- It creates Playlist + PlaylistTrack + GenerationRecord rows
- It marks the parent task COMPLETED

**Step 4: Implement all three worker functions**

Key implementation details:

In `_TASK_DISPATCH`, add entries for the three new task types:
```python
TaskType.PLAYLIST_GENERATION: ("generate_playlist", lambda t: (str(t.id),)),
TaskType.TRACK_DISCOVERY: ("discover_tracks_for_artist", lambda t: (str(t.id),)),
TaskType.TRACK_SCORING: ("score_and_build_playlist", lambda t: (str(t.id),)),
```

In `WorkerSettings.functions`, add:
```python
arq.func(heartbeat_module.with_heartbeat(generate_playlist), timeout=3600),
arq.func(heartbeat_module.with_heartbeat(discover_tracks_for_artist), timeout=600),
arq.func(heartbeat_module.with_heartbeat(score_and_build_playlist), timeout=600),
```

**Step 5: Run tests**

Run: `uv run pytest tests/test_worker.py -v && uv run mypy src/`
Expected: PASS

**Step 6: Commit**

```bash
git add src/resonance/worker.py tests/test_worker.py
git commit -m "feat: add worker tasks for playlist generation pipeline"
```

---

### Task 13: Add CLI commands for profile management and generation

**Files:**
- Modify: `src/resonance/cli.py`
- Test: Manual testing via `uv run resonance-api profile --help` etc.

**Step 1: Add profile commands**

Add to `cli.py`:

- `_cmd_profile()` — dispatcher for `profile create|list|show|update|delete`
- `_cmd_profile_create()` — `POST /api/v1/generator-profiles`
- `_cmd_profile_list()` — `GET /api/v1/generator-profiles`
- `_cmd_profile_show()` — `GET /api/v1/generator-profiles/{id}`
- `_cmd_profile_update()` — `PATCH /api/v1/generator-profiles/{id}`
- `_cmd_profile_delete()` — `DELETE /api/v1/generator-profiles/{id}`

**Step 2: Add generate command**

- `_cmd_generate()` — `POST /api/v1/generator-profiles/{id}/generate` + poll task

**Step 3: Add playlist commands**

- `_cmd_playlists()` — `GET /api/v1/playlists`
- `_cmd_playlist()` — `GET /api/v1/playlists/{id}`
- `_cmd_playlist_diff()` — `GET /api/v1/playlists/{id}/diff/{other_id}`

**Step 4: Register in _COMMANDS dict**

```python
"profile": ("Manage generator profiles", _cmd_profile),
"generate": ("Generate a playlist", _cmd_generate),
"playlists": ("List playlists", _cmd_playlists),
"playlist": ("Show or diff a playlist", _cmd_playlist),
```

**Step 5: Update _USAGE string**

**Step 6: Run lint and type checks**

Run: `uv run ruff check . && uv run mypy src/`
Expected: PASS

**Step 7: Commit**

```bash
git add src/resonance/cli.py
git commit -m "feat: add CLI commands for profile management and playlist generation"
```

---

### Task 14: Update architecture docs and CLAUDE.md

**Files:**
- Modify: `docs/architecture.md`
- Modify: `CLAUDE.md`

**Step 1: Add Generator System section to architecture.md**

Add after the Bulk Operations section:
- Generator System overview
- Parameter Registry description
- Task Hierarchy for generation
- Data flow for concert prep

**Step 2: Update CLAUDE.md conventions**

Add entries for:
- Generator parameter registry lives in `generators/parameters.py`
- Generator types declared in `generators/parameters.py` via `GENERATOR_TYPE_CONFIG`
- Scoring logic in `generators/scoring.py`
- Generator-specific logic in `generators/<type>.py`

**Step 3: Commit**

```bash
git add docs/architecture.md CLAUDE.md
git commit -m "docs: add generator system to architecture and conventions"
```

---

## Execution Notes

- **Tasks 1-6** are foundational — types, models, migration, parameter registry. These must be done in order.
- **Tasks 7-8** (scoring + concert prep) are the generator logic — pure functions, no DB required, highly testable.
- **Task 9** (ListenBrainz discovery) can be developed in parallel with 7-8 since it's on the connector side.
- **Tasks 10-11** (API endpoints) depend on models (Tasks 3-4) being done.
- **Task 12** (worker) depends on everything above — it's the integration point.
- **Task 13** (CLI) depends on Task 10-11 (API endpoints exist).
- **Task 14** (docs) should be done last.

### Not in Scope (v1)

- UI pages (deferred — CLI is the primary testing surface for now)
- Spotify PLAYLIST_WRITE (export deferred until generation quality is validated)
- Similar artist discovery (similar_artist_ratio parameter exists but defaults to 0)
- Presets (parameter defaults serve as the default preset)
- Auto-sync targets (model column exists but not wired up)

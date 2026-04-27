"""Tests for shared enumeration types."""

import enum

import resonance.types as types_module


class TestGeneratorType:
    """Tests for GeneratorType enum."""

    def test_is_str_enum(self) -> None:
        """GeneratorType should be a StrEnum."""
        assert issubclass(types_module.GeneratorType, enum.StrEnum)

    def test_has_six_members(self) -> None:
        """GeneratorType should have exactly 6 members."""
        assert len(types_module.GeneratorType) == 6

    def test_concert_prep_value(self) -> None:
        assert types_module.GeneratorType.CONCERT_PREP == "concert_prep"

    def test_artist_deep_dive_value(self) -> None:
        assert types_module.GeneratorType.ARTIST_DEEP_DIVE == "artist_deep_dive"

    def test_rediscovery_value(self) -> None:
        assert types_module.GeneratorType.REDISCOVERY == "rediscovery"

    def test_discography_value(self) -> None:
        assert types_module.GeneratorType.DISCOGRAPHY == "discography"

    def test_playlist_refresh_value(self) -> None:
        assert types_module.GeneratorType.PLAYLIST_REFRESH == "playlist_refresh"

    def test_curated_mix_value(self) -> None:
        assert types_module.GeneratorType.CURATED_MIX == "curated_mix"


class TestParameterScaleType:
    """Tests for ParameterScaleType enum."""

    def test_is_str_enum(self) -> None:
        """ParameterScaleType should be a StrEnum."""
        assert issubclass(types_module.ParameterScaleType, enum.StrEnum)

    def test_has_two_members(self) -> None:
        """ParameterScaleType should have exactly 2 members."""
        assert len(types_module.ParameterScaleType) == 2

    def test_bipolar_value(self) -> None:
        assert types_module.ParameterScaleType.BIPOLAR == "bipolar"

    def test_unipolar_value(self) -> None:
        assert types_module.ParameterScaleType.UNIPOLAR == "unipolar"


class TestTrackSource:
    """Tests for TrackSource enum."""

    def test_is_str_enum(self) -> None:
        """TrackSource should be a StrEnum."""
        assert issubclass(types_module.TrackSource, enum.StrEnum)

    def test_has_three_members(self) -> None:
        """TrackSource should have exactly 3 members."""
        assert len(types_module.TrackSource) == 3

    def test_library_value(self) -> None:
        assert types_module.TrackSource.LIBRARY == "library"

    def test_discovery_value(self) -> None:
        assert types_module.TrackSource.DISCOVERY == "discovery"

    def test_manual_value(self) -> None:
        assert types_module.TrackSource.MANUAL == "manual"


class TestTaskTypeNewValues:
    """Tests for new TaskType enum values added for playlist generation."""

    def test_playlist_generation_value(self) -> None:
        assert types_module.TaskType.PLAYLIST_GENERATION == "playlist_generation"

    def test_track_discovery_value(self) -> None:
        assert types_module.TaskType.TRACK_DISCOVERY == "track_discovery"

    def test_track_scoring_value(self) -> None:
        assert types_module.TaskType.TRACK_SCORING == "track_scoring"

    def test_tasktype_includes_all_original_values(self) -> None:
        """Existing TaskType values should still be present."""
        assert types_module.TaskType.SYNC_JOB == "sync_job"
        assert types_module.TaskType.TIME_RANGE == "time_range"
        assert types_module.TaskType.PAGE_FETCH == "page_fetch"
        assert types_module.TaskType.BULK_JOB == "bulk_job"
        assert types_module.TaskType.CALENDAR_SYNC == "calendar_sync"

    def test_tasktype_has_eight_members(self) -> None:
        """TaskType should have 8 members total (5 original + 3 new)."""
        assert len(types_module.TaskType) == 8

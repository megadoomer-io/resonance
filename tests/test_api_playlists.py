"""Tests for playlist API endpoints."""

from __future__ import annotations

import uuid


class TestDiffLogic:
    """Tests for the pure set-operation logic used by the diff endpoint."""

    def test_diff_added_and_removed(self) -> None:
        set_a = {"track1", "track2", "track3"}
        set_b = {"track2", "track3", "track4"}
        added = set_b - set_a
        removed = set_a - set_b
        common = set_a & set_b
        assert added == {"track4"}
        assert removed == {"track1"}
        assert common == {"track2", "track3"}

    def test_diff_identical_sets(self) -> None:
        set_a = {"track1", "track2"}
        set_b = {"track1", "track2"}
        added = set_b - set_a
        removed = set_a - set_b
        common = set_a & set_b
        assert added == set()
        assert removed == set()
        assert common == {"track1", "track2"}

    def test_diff_disjoint_sets(self) -> None:
        set_a = {"track1", "track2"}
        set_b = {"track3", "track4"}
        added = set_b - set_a
        removed = set_a - set_b
        common = set_a & set_b
        assert added == {"track3", "track4"}
        assert removed == {"track1", "track2"}
        assert common == set()

    def test_diff_empty_first_set(self) -> None:
        set_a: set[str] = set()
        set_b = {"track1", "track2"}
        added = set_b - set_a
        removed = set_a - set_b
        common = set_a & set_b
        assert added == {"track1", "track2"}
        assert removed == set()
        assert common == set()

    def test_diff_empty_second_set(self) -> None:
        set_a = {"track1", "track2"}
        set_b: set[str] = set()
        added = set_b - set_a
        removed = set_a - set_b
        common = set_a & set_b
        assert added == set()
        assert removed == {"track1", "track2"}
        assert common == set()

    def test_diff_both_empty(self) -> None:
        set_a: set[str] = set()
        set_b: set[str] = set()
        added = set_b - set_a
        removed = set_a - set_b
        common = set_a & set_b
        assert added == set()
        assert removed == set()
        assert common == set()

    def test_diff_counts(self) -> None:
        set_a = {"t1", "t2", "t3", "t4"}
        set_b = {"t3", "t4", "t5", "t6", "t7"}
        added = set_b - set_a
        removed = set_a - set_b
        common = set_a & set_b
        assert len(added) == 3
        assert len(removed) == 2
        assert len(common) == 2


class TestBuildDiffResponse:
    """Tests for the build_diff_response helper function."""

    def test_builds_correct_structure(self) -> None:
        import resonance.api.v1.playlists as playlists_module

        playlist_a_id = uuid.uuid4()
        playlist_b_id = uuid.uuid4()
        tracks_a = {uuid.uuid4(), uuid.uuid4(), uuid.uuid4()}
        common_track = uuid.uuid4()
        tracks_a.add(common_track)
        tracks_b = {uuid.uuid4(), uuid.uuid4()}
        tracks_b.add(common_track)

        result = playlists_module.build_diff_response(
            playlist_a_id=playlist_a_id,
            playlist_b_id=playlist_b_id,
            track_ids_a=tracks_a,
            track_ids_b=tracks_b,
        )

        assert result["playlist_a_id"] == str(playlist_a_id)
        assert result["playlist_b_id"] == str(playlist_b_id)
        assert str(common_track) in result["common"]
        assert result["common_count"] == 1
        assert result["added_count"] == 2
        assert result["removed_count"] == 3

    def test_empty_sets(self) -> None:
        import resonance.api.v1.playlists as playlists_module

        a_id = uuid.uuid4()
        b_id = uuid.uuid4()
        result = playlists_module.build_diff_response(
            playlist_a_id=a_id,
            playlist_b_id=b_id,
            track_ids_a=set(),
            track_ids_b=set(),
        )
        assert result["added"] == []
        assert result["removed"] == []
        assert result["common"] == []
        assert result["added_count"] == 0
        assert result["removed_count"] == 0
        assert result["common_count"] == 0


class TestFormatPlaylistSummary:
    """Tests for the format_playlist_summary helper."""

    def test_formats_playlist_fields(self) -> None:
        import datetime

        import resonance.api.v1.playlists as playlists_module
        import resonance.models.playlist as playlist_models

        playlist = playlist_models.Playlist(
            name="Test Playlist",
            user_id=uuid.uuid4(),
            description="A test",
            track_count=10,
            is_pinned=True,
        )
        # Manually set timestamps since no DB
        now = datetime.datetime.now(datetime.UTC)
        playlist.created_at = now
        playlist.updated_at = now

        result = playlists_module.format_playlist_summary(playlist)

        assert result["name"] == "Test Playlist"
        assert result["description"] == "A test"
        assert result["track_count"] == 10
        assert result["is_pinned"] is True
        assert result["created_at"] == now.isoformat()
        assert "id" in result

    def test_null_description(self) -> None:
        import datetime

        import resonance.api.v1.playlists as playlists_module
        import resonance.models.playlist as playlist_models

        playlist = playlist_models.Playlist(
            name="No Desc",
            user_id=uuid.uuid4(),
        )
        now = datetime.datetime.now(datetime.UTC)
        playlist.created_at = now
        playlist.updated_at = now

        result = playlists_module.format_playlist_summary(playlist)

        assert result["description"] is None
        assert result["is_pinned"] is False
        assert result["track_count"] == 0

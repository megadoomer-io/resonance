"""Tests for playlist export API endpoint."""

from __future__ import annotations

import datetime
import uuid

import resonance.api.v1.playlists as playlists_module
import resonance.models.playlist as playlist_models
import resonance.types as types_module


class TestExportEndpointValidation:
    """Tests for export-related type validation."""

    def test_playlist_export_task_type_exists(self) -> None:
        assert types_module.TaskType.PLAYLIST_EXPORT == "playlist_export"

    def test_export_requires_spotify_service_type(self) -> None:
        assert types_module.ServiceType.SPOTIFY == "spotify"


class TestExportRequestModel:
    """Tests for the ExportRequest Pydantic model."""

    def test_export_request_defaults_to_none(self) -> None:
        req = playlists_module.ExportRequest()
        assert req.connection_ids is None

    def test_export_request_accepts_connection_ids(self) -> None:
        cid = uuid.uuid4()
        req = playlists_module.ExportRequest(connection_ids=[cid])
        assert req.connection_ids == [cid]

    def test_export_request_accepts_empty_list(self) -> None:
        req = playlists_module.ExportRequest(connection_ids=[])
        assert req.connection_ids == []

    def test_export_request_accepts_multiple_ids(self) -> None:
        ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
        req = playlists_module.ExportRequest(connection_ids=ids)
        assert req.connection_ids == ids


class TestFormatPlaylistSummaryServiceLinks:
    """Tests that format_playlist_summary includes service_links and updated_at."""

    def test_includes_service_links_none(self) -> None:
        playlist = playlist_models.Playlist(
            name="Test",
            user_id=uuid.uuid4(),
        )
        now = datetime.datetime.now(datetime.UTC)
        playlist.created_at = now
        playlist.updated_at = now

        result = playlists_module.format_playlist_summary(playlist)
        assert result["service_links"] is None

    def test_includes_service_links_with_data(self) -> None:
        connection_id = str(uuid.uuid4())
        links = {
            "spotify": {
                connection_id: {
                    "playlist_id": "sp123",
                    "exported_at": "2026-05-10T22:30:00Z",
                }
            }
        }
        playlist = playlist_models.Playlist(
            name="Test",
            user_id=uuid.uuid4(),
            service_links=links,
        )
        now = datetime.datetime.now(datetime.UTC)
        playlist.created_at = now
        playlist.updated_at = now

        result = playlists_module.format_playlist_summary(playlist)
        assert result["service_links"] == links
        assert (
            result["service_links"]["spotify"][connection_id]["playlist_id"] == "sp123"
        )

    def test_includes_updated_at(self) -> None:
        playlist = playlist_models.Playlist(
            name="Test",
            user_id=uuid.uuid4(),
        )
        now = datetime.datetime.now(datetime.UTC)
        playlist.created_at = now
        playlist.updated_at = now

        result = playlists_module.format_playlist_summary(playlist)
        assert "updated_at" in result
        assert result["updated_at"] == now.isoformat()

"""Utilities for reading artist metadata from service_links."""

from typing import Any


def get_mbid(service_links: dict[str, Any] | None) -> str | None:
    """Extract an MBID from service_links, checking canonical and legacy locations.

    Checks the canonical location (musicbrainz.id nested dict) first,
    then falls back to the legacy flat listenbrainz key.

    Args:
        service_links: The entity's service_links dict, or None.

    Returns:
        The MBID string if found, or None.
    """
    if not service_links:
        return None
    # Canonical: service_links["musicbrainz"]["id"]
    mb = service_links.get("musicbrainz")
    if isinstance(mb, dict):
        mbid = mb.get("id")
        if mbid:
            return str(mbid)
    # Legacy fallback: service_links["listenbrainz"] (flat string MBID)
    lb = service_links.get("listenbrainz")
    if isinstance(lb, str) and lb:
        return lb
    return None


def has_mbid(service_links: dict[str, Any] | None) -> bool:
    """Check whether service_links contains an MBID in any known location.

    Args:
        service_links: The entity's service_links dict, or None.

    Returns:
        True if an MBID is present.
    """
    return get_mbid(service_links) is not None

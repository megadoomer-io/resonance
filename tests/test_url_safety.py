"""Tests for the SSRF guard on user-supplied feed URLs (security review #141 / #1).

The guard must block an authenticated user from pointing a feed URL at cloud
metadata (169.254.169.254), loopback, or any private/internal address, and must
close the DNS-rebinding window by pinning the connection to a validated IP.
"""

from __future__ import annotations

import ipaddress
import socket
from unittest.mock import patch

import httpx
import pytest

import resonance.concerts.url_safety as url_safety

# ---------------------------------------------------------------------------
# is_public_address — pure classification
# ---------------------------------------------------------------------------


class TestIsPublicAddress:
    """Classification of resolved IPs into public (allowed) vs not."""

    @pytest.mark.parametrize(
        "ip",
        [
            "8.8.8.8",
            "1.1.1.1",
            "93.184.216.34",  # example.com
            "2606:4700:4700::1111",  # cloudflare v6
        ],
    )
    def test_public_addresses_allowed(self, ip: str) -> None:
        assert url_safety.is_public_address(ipaddress.ip_address(ip)) is True

    @pytest.mark.parametrize(
        "ip",
        [
            "127.0.0.1",  # loopback
            "::1",  # loopback v6
            "10.0.0.5",  # private
            "172.16.0.1",  # private
            "192.168.1.1",  # private
            "169.254.169.254",  # link-local / cloud metadata
            "169.254.0.1",  # link-local
            "0.0.0.0",  # unspecified
            "224.0.0.1",  # multicast
            "240.0.0.1",  # reserved
            "fc00::1",  # ULA (private v6)
            "fe80::1",  # link-local v6
            "::",  # unspecified v6
        ],
    )
    def test_non_public_addresses_rejected(self, ip: str) -> None:
        assert url_safety.is_public_address(ipaddress.ip_address(ip)) is False

    @pytest.mark.parametrize(
        "ip",
        [
            "::ffff:127.0.0.1",  # IPv4-mapped loopback
            "::ffff:169.254.169.254",  # IPv4-mapped cloud metadata
            "::ffff:10.0.0.1",  # IPv4-mapped private
        ],
    )
    def test_ipv4_mapped_v6_is_unwrapped_and_rejected(self, ip: str) -> None:
        """An attacker can't smuggle a private v4 address inside a v6 wrapper."""
        assert url_safety.is_public_address(ipaddress.ip_address(ip)) is False


# ---------------------------------------------------------------------------
# resolve_safe_addresses — DNS resolution + filtering
# ---------------------------------------------------------------------------


def _addrinfo(*ips: str) -> list[tuple]:
    """Build a getaddrinfo-shaped return value for the given IPs."""
    out = []
    for ip in ips:
        family = socket.AF_INET6 if ":" in ip else socket.AF_INET
        sockaddr: tuple = (ip, 443, 0, 0) if ":" in ip else (ip, 443)
        out.append((family, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", sockaddr))
    return out


class TestResolveSafeAddresses:
    def test_returns_public_ips(self) -> None:
        with patch.object(
            url_safety.socket, "getaddrinfo", return_value=_addrinfo("8.8.8.8")
        ):
            safe = url_safety.resolve_safe_addresses("dns.google", 443)
        assert "8.8.8.8" in {ip for _fam, ip in safe}

    def test_rejects_host_resolving_only_to_private(self) -> None:
        """A hostname that resolves to a private IP (DNS rebinding setup) is blocked."""
        with (
            patch.object(
                url_safety.socket, "getaddrinfo", return_value=_addrinfo("10.0.0.5")
            ),
            pytest.raises(url_safety.UnsafeFeedURLError),
        ):
            url_safety.resolve_safe_addresses("evil.example.com", 443)

    def test_filters_private_keeps_public_when_dual_homed(self) -> None:
        """Only the public IP survives; we pin to it, never the private one."""
        with patch.object(
            url_safety.socket,
            "getaddrinfo",
            return_value=_addrinfo("10.0.0.5", "93.184.216.34"),
        ):
            safe = url_safety.resolve_safe_addresses("dual.example.com", 443)
        ips = {ip for _fam, ip in safe}
        assert "93.184.216.34" in ips
        assert "10.0.0.5" not in ips

    def test_unresolvable_host_raises(self) -> None:
        with (
            patch.object(
                url_safety.socket, "getaddrinfo", side_effect=socket.gaierror("nope")
            ),
            pytest.raises(url_safety.UnsafeFeedURLError),
        ):
            url_safety.resolve_safe_addresses("nx.example.com", 443)


# ---------------------------------------------------------------------------
# fetch_feed — end-to-end guard + pinned fetch
# ---------------------------------------------------------------------------


class TestFetchFeed:
    @pytest.mark.anyio()
    @pytest.mark.parametrize(
        "url",
        [
            "ftp://example.com/feed.ics",
            "file:///etc/passwd",
            "gopher://example.com/",
            "http://169.254.169.254/latest/meta-data/",
            "http://localhost:8000/admin",
            "http://127.0.0.1/",
            "http://10.0.0.5/internal",
            "https://[::1]/",
            "not-a-url",
            "https:///no-host",
        ],
    )
    async def test_blocks_unsafe_url_before_network(self, url: str) -> None:
        """No request is ever issued for a disallowed scheme or non-public target."""
        sent: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            sent.append(request)
            return httpx.Response(200, text="should not happen")

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        # Literal IPs/localhost are classified without DNS; for the hostname cases
        # (none here) getaddrinfo would run, but every case above is scheme-bad or
        # a literal non-public IP, so resolution isn't reached.
        async with client:
            with pytest.raises(url_safety.UnsafeFeedURLError):
                await url_safety.fetch_feed(url, client=client)
        assert sent == [], "a request was issued for an unsafe URL"

    @pytest.mark.anyio()
    async def test_pins_to_validated_ip_and_preserves_host(self) -> None:
        """Connect target is the validated IP; Host + SNI keep the real hostname."""
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["host_in_url"] = request.url.host
            captured["host_header"] = request.headers.get("Host")
            captured["sni"] = request.extensions.get("sni_hostname")
            return httpx.Response(200, text="BEGIN:VCALENDAR\nEND:VCALENDAR\n")

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        with patch.object(
            url_safety.socket, "getaddrinfo", return_value=_addrinfo("93.184.216.34")
        ):
            async with client:
                body = await url_safety.fetch_feed(
                    "https://feeds.example.com/calendar.ics", client=client
                )

        assert body.startswith("BEGIN:VCALENDAR")
        assert captured["host_in_url"] == "93.184.216.34"  # pinned to the IP
        assert captured["host_header"] == "feeds.example.com"  # real vhost
        assert captured["sni"] == "feeds.example.com"  # TLS verifies real host

    @pytest.mark.anyio()
    async def test_raises_for_status(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="boom")

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        with patch.object(
            url_safety.socket, "getaddrinfo", return_value=_addrinfo("93.184.216.34")
        ):
            async with client:
                with pytest.raises(httpx.HTTPStatusError):
                    await url_safety.fetch_feed(
                        "https://feeds.example.com/calendar.ics", client=client
                    )

    @pytest.mark.anyio()
    async def test_caps_response_size(self) -> None:
        """An oversized body is rejected rather than buffered without bound."""
        big = "A" * (url_safety.MAX_FEED_BYTES + 1)

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text=big)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        with patch.object(
            url_safety.socket, "getaddrinfo", return_value=_addrinfo("93.184.216.34")
        ):
            async with client:
                with pytest.raises(url_safety.UnsafeFeedURLError):
                    await url_safety.fetch_feed(
                        "https://feeds.example.com/calendar.ics", client=client
                    )

    @pytest.mark.anyio()
    async def test_returns_body_text(self) -> None:
        ical = "BEGIN:VCALENDAR\nVERSION:2.0\nEND:VCALENDAR\n"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text=ical)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        with patch.object(
            url_safety.socket, "getaddrinfo", return_value=_addrinfo("8.8.8.8")
        ):
            async with client:
                body = await url_safety.fetch_feed(
                    "http://feeds.example.com/calendar.ics", client=client
                )
        assert body == ical

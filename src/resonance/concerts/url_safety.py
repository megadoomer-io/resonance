"""SSRF guard for user-supplied feed URLs (security review #141, finding #1).

An authenticated USER can register an iCal feed URL, which a background task then
fetches. Without validation that URL could point at cloud metadata
(``169.254.169.254``), loopback, or any cluster-internal service — a classic SSRF.

``fetch_feed`` is the single safe entry point. It:

- allows only ``http`` / ``https`` schemes,
- resolves the host and rejects any private/loopback/link-local/reserved/
  multicast/unspecified address (covering ``169.254.169.254`` and IPv4-mapped
  IPv6 smuggling),
- **pins** the connection to a validated IP while keeping the real hostname for
  the ``Host`` header and TLS SNI, so a DNS-rebind between validation and connect
  cannot redirect us to an internal address,
- sets an explicit timeout, refuses redirects, and caps the response size.
"""

from __future__ import annotations

import ipaddress
import socket

import httpx

# Security floors. Intentionally constants, not config — these are a safety
# baseline for fetching untrusted URLs, not per-deployment tunables.
MAX_FEED_BYTES = 10 * 1024 * 1024  # 10 MiB
_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
_ALLOWED_SCHEMES = frozenset({"http", "https"})
_DEFAULT_PORTS = {"http": 80, "https": 443}

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address


class UnsafeFeedURLError(ValueError):
    """A feed URL uses a disallowed scheme or targets a non-public address."""


def is_public_address(ip: IPAddress) -> bool:
    """True only for globally routable addresses safe to fetch from a server.

    Rejects loopback, private (RFC1918 / ULA), link-local (incl. the cloud
    metadata endpoint ``169.254.169.254``), multicast, reserved, and the
    unspecified address. IPv4-mapped IPv6 addresses are unwrapped and
    re-checked so a private v4 target can't be smuggled inside a v6 wrapper.
    """
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return is_public_address(ip.ipv4_mapped)
    return not (
        ip.is_loopback
        or ip.is_private
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def resolve_safe_addresses(host: str, port: int) -> list[tuple[int, str]]:
    """Resolve ``host`` and return ``(family, ip)`` for its public addresses.

    Raises ``UnsafeFeedURLError`` if the host cannot be resolved or resolves to
    no public address (so a host that points only at private space is blocked).
    """
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise UnsafeFeedURLError(f"cannot resolve host: {host}") from exc

    safe: list[tuple[int, str]] = []
    for family, _socktype, _proto, _canon, sockaddr in infos:
        ip_str = str(sockaddr[0]).split("%", 1)[0]  # drop any IPv6 scope id
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if is_public_address(ip):
            safe.append((family, ip_str))

    if not safe:
        raise UnsafeFeedURLError(f"host resolves only to non-public addresses: {host}")
    return safe


def _validated_target(url: httpx.URL) -> tuple[str, str | None]:
    """Validate scheme/host and return ``(connect_ip, sni_hostname)``.

    ``sni_hostname`` is ``None`` when the URL already targets a literal IP (no
    rebinding is possible, so no host/SNI rewrite is needed); otherwise it is the
    original hostname to preserve for the ``Host`` header and TLS verification.
    """
    scheme = url.scheme.lower()
    if scheme not in _ALLOWED_SCHEMES:
        raise UnsafeFeedURLError(f"disallowed scheme: {scheme or '(none)'}")

    host = url.host
    if not host:
        raise UnsafeFeedURLError("URL has no host")

    try:
        literal_ip = ipaddress.ip_address(host)
    except ValueError:
        literal_ip = None

    if literal_ip is not None:
        if not is_public_address(literal_ip):
            raise UnsafeFeedURLError(f"non-public address: {host}")
        return host, None

    port = url.port or _DEFAULT_PORTS[scheme]
    safe = resolve_safe_addresses(host, port)
    return safe[0][1], host


async def fetch_feed(url: str, *, client: httpx.AsyncClient | None = None) -> str:
    """Safely fetch a user-supplied feed URL and return its body as text.

    Raises ``UnsafeFeedURLError`` for a disallowed scheme or non-public target
    (before any network I/O), and ``httpx.HTTPStatusError`` on a 4xx/5xx
    response. The response body is size-capped at ``MAX_FEED_BYTES``.

    A ``client`` may be injected (tests); otherwise one is created with redirects
    disabled and an explicit timeout. Redirects are refused regardless, since a
    302 to an internal address would bypass the up-front validation.
    """
    parsed = httpx.URL(url)
    connect_ip, sni_hostname = _validated_target(parsed)

    if sni_hostname is None:
        # Literal-IP target: already pinned, no rewrite needed.
        request_url = parsed
        headers: dict[str, str] = {}
        extensions: dict[str, object] = {}
    else:
        # Hostname target: pin the connection to the validated IP, but keep the
        # real hostname for routing (Host) and TLS verification (SNI).
        request_url = parsed.copy_with(host=connect_ip)
        host_header = sni_hostname
        if parsed.port is not None:
            host_header = f"{sni_hostname}:{parsed.port}"
        headers = {"Host": host_header}
        extensions = {"sni_hostname": sni_hostname}

    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=False)
    try:
        async with client.stream(
            "GET",
            request_url,
            headers=headers,
            extensions=extensions,
            follow_redirects=False,
            timeout=_TIMEOUT,
        ) as response:
            response.raise_for_status()
            chunks: list[bytes] = []
            total = 0
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > MAX_FEED_BYTES:
                    raise UnsafeFeedURLError(
                        f"feed exceeds size cap of {MAX_FEED_BYTES} bytes"
                    )
                chunks.append(chunk)
            encoding = response.encoding or "utf-8"
        return b"".join(chunks).decode(encoding, errors="replace")
    finally:
        if owns_client:
            await client.aclose()

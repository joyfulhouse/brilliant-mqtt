"""Classify broker hosts that a Brilliant panel on the LAN cannot resolve."""

from __future__ import annotations

import ipaddress
import re

_DNS_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$", re.IGNORECASE)
_IPV6_ZONE = re.compile(r"[a-zA-Z0-9_.~-]+")


def panel_unreachable_broker_host_reason(host: str) -> str | None:
    """Return a stable reason when panels on the LAN cannot use this broker host.

    ``None`` means the host is not in the known-unreachable set. It is not proof
    that a panel can resolve or route to it.
    """
    if not isinstance(host, str):
        return None
    candidate = host.strip().lower().rstrip(".")
    if not candidate or any(ord(char) < 32 or ord(char) == 127 for char in candidate):
        return None
    if _is_loopback(candidate):
        return "loopback"
    if candidate == "local.hass.io" or candidate.endswith(".local.hass.io"):
        return "hass_internal_dns"
    single_label = "." not in candidate
    # Known Home Assistant-internal single-label names only. Dotted LAN names
    # and ordinary hyphenated labels may resolve from a panel.
    if single_label and candidate.startswith(("a0d7b954-", "core-")):
        return "addon_internal_name"
    return None


def format_broker_endpoint(host: str, port: int) -> str:
    """Return a redacted-safe ``host:port`` label, or empty when it is unsafe."""
    if (
        not isinstance(host, str)
        or type(port) is not int
        or not 1 <= port <= 65535
        or not host
        or len(host) > 253
        or any(ord(char) < 33 or ord(char) == 127 for char in host)
        or not _is_diagnostic_broker_host(host)
    ):
        return ""
    rendered_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    endpoint = f"{rendered_host}:{port}"
    if len(endpoint) > 300:
        return ""
    return endpoint


def _is_diagnostic_broker_host(host: str) -> bool:
    """True for a DNS name or IP literal, never a URL, query, or userinfo form."""
    bare = host[1:-1] if host.startswith("[") and host.endswith("]") else host
    # ipaddress accepts opaque zone text; diagnostics allow only URI-unreserved scopes.
    if "%" in bare and _IPV6_ZONE.fullmatch(bare.partition("%")[2]) is None:
        return False
    if host.startswith("[") and host.endswith("]"):
        try:
            ipaddress.ip_address(host[1:-1])
        except ValueError:
            return False
        return True
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        return True
    candidate = host[:-1] if host.endswith(".") else host
    if not candidate or len(candidate) > 253 or candidate.startswith(".") or ".." in candidate:
        return False
    return all(_DNS_LABEL.fullmatch(label) is not None for label in candidate.split("."))


def _is_loopback(host: str) -> bool:
    if host in {"localhost", "localhost.localdomain"}:
        return True
    bare = host[1:-1] if host.startswith("[") and host.endswith("]") else host
    try:
        address = ipaddress.ip_address(bare)
    except ValueError:
        return False
    return address.is_loopback

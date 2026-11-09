"""Classify broker hosts that a Brilliant panel on the LAN cannot resolve."""

from __future__ import annotations

import ipaddress
import re

_ADDON_SLUG = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)+$")
_REPO_ADDON_HOST = re.compile(r"^[0-9a-f]{8}-[a-z0-9-]+$")


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
    if candidate.startswith("a0d7b954-") or (single_label and candidate.startswith("core-")):
        return "addon_internal_name"
    if single_label and (
        _REPO_ADDON_HOST.fullmatch(candidate) is not None
        or _ADDON_SLUG.fullmatch(candidate) is not None
    ):
        return "addon_slug"
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
        or any(char in host for char in "@/\\")
    ):
        return ""
    rendered_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    endpoint = f"{rendered_host}:{port}"
    if len(endpoint) > 300:
        return ""
    return endpoint


def _is_loopback(host: str) -> bool:
    if host in {"localhost", "localhost.localdomain"}:
        return True
    bare = host[1:-1] if host.startswith("[") and host.endswith("]") else host
    try:
        address = ipaddress.ip_address(bare)
    except ValueError:
        return False
    return address.is_loopback

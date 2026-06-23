"""Ensure a ``hostname=ip`` mapping is present in ``/etc/hosts``.

The panel's ``/etc`` directory is part of the OTA-replaced filesystem — a
firmware update silently wipes any manual edits.  The voice supervisor
therefore re-applies any host mapping at every startup, just as it re-applies
the nftables rule (same idempotent discipline: return ``True`` only when a
write actually occurred).

The ``VOICE_HA_HOST`` config variable carries the mapping as
``"hostname=ip"`` (e.g. ``"homeassistant.local=192.168.1.10"``).  An empty
value means the panel can already resolve the HA host via its own DNS and no
``/etc/hosts`` entry is needed — the most common case; the function is a
no-op.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

#: Returns the current text of ``/etc/hosts``.
HostsReader = Callable[[], str]
#: Writes new text to ``/etc/hosts``.
HostsWriter = Callable[[str], None]

_HOSTS_PATH = Path("/etc/hosts")


def _default_read() -> str:
    return _HOSTS_PATH.read_text(encoding="utf-8")


def _default_write(text: str) -> None:
    _HOSTS_PATH.write_text(text, encoding="utf-8")


def ensure_host_mapping(
    spec: str,
    *,
    read: HostsReader = _default_read,
    write: HostsWriter = _default_write,
) -> bool:
    """Idempotently add a ``hostname=ip`` mapping to ``/etc/hosts``.

    Parameters
    ----------
    spec:
        A ``"hostname=ip"`` string (e.g. ``"homeassistant.local=192.168.1.10"``).
        An empty or whitespace-only string is a no-op (returns ``False``
        without reading or writing ``/etc/hosts``).
    read:
        Callable that returns the current ``/etc/hosts`` text.  Injected in
        tests; the default reads the real file.
    write:
        Callable that writes the new ``/etc/hosts`` text.  Injected in tests;
        the default writes the real file.

    Returns
    -------
    bool
        ``True`` when a new line was appended, ``False`` when the mapping was
        already present (or ``spec`` was empty).

    Raises
    ------
    ValueError
        When ``spec`` is non-empty but malformed (no ``=``, or either side of
        the ``=`` is empty after stripping).
    """
    if not spec.strip():
        return False

    if "=" not in spec:
        raise ValueError(f"VOICE_HA_HOST spec missing '=': {spec!r}")

    hostname, _, ip = spec.partition("=")
    hostname = hostname.strip()
    ip = ip.strip()

    if not hostname:
        raise ValueError(f"VOICE_HA_HOST spec has empty hostname: {spec!r}")
    if not ip:
        raise ValueError(f"VOICE_HA_HOST spec has empty ip: {spec!r}")

    current = read()

    for line in current.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        fields = stripped.split()
        if fields[0] == ip and hostname in fields[1:]:
            return False

    separator = "" if current.endswith("\n") else "\n"
    write(current + separator + f"{ip}\t{hostname}\n")
    return True

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

import unicodedata
from collections.abc import Callable
from pathlib import Path

#: Returns the current text of ``/etc/hosts``.
HostsReader = Callable[[], str]
#: Writes new text to ``/etc/hosts``.
HostsWriter = Callable[[str], None]

_HOSTS_PATH = Path("/etc/hosts")


def _is_control(ch: str) -> bool:
    """True for a control character (Unicode category ``Cc``, e.g. NUL, DEL).

    ``str.isspace()`` already covers tab/newline/space; this catches the
    remaining non-printing controls (NUL ``\\x00`` … DEL ``\\x7f``) that are not
    classed as whitespace but would still corrupt an ``/etc/hosts`` line.
    """
    return unicodedata.category(ch) == "Cc"


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
        When ``spec`` is non-empty but malformed: no ``=``; either side of the
        ``=`` empty after stripping; or either side contains internal whitespace
        or a control character (which would corrupt the ``/etc/hosts`` line).
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

    # Reject internal whitespace or control characters: either would split or
    # corrupt the appended "ip\thostname" line (e.g. inject a second field or a
    # newline), so a malformed hand-written VOICE_HA_HOST can never write a bad
    # /etc/hosts entry.
    if any(c.isspace() or _is_control(c) for c in hostname):
        raise ValueError(f"VOICE_HA_HOST hostname has whitespace/control chars: {spec!r}")
    if any(c.isspace() or _is_control(c) for c in ip):
        raise ValueError(f"VOICE_HA_HOST ip has whitespace/control chars: {spec!r}")

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

"""Shared low-level helpers for the Brilliant Virtual Control operator tooling.

These primitives were copy-pasted verbatim across the ``brilliant_vc`` modules.
They are collected here so the security-sensitive behaviour (secret wiping,
identifier redaction, directory durability) and the cross-module contract
constants (pinned firmware, dedicated runtime account) stay in lockstep.
"""

from __future__ import annotations

import os
from pathlib import Path

# Cross-module contract constants. These MUST stay identical everywhere they are
# used; a divergence would be a live deployment bug, which is why they live in a
# single place.
PINNED_FIRMWARE = "v26.06.03.1"
RUNTIME_USER = "brilliant-vc"


def wipe(buffer: bytearray) -> None:
    """Overwrite a mutable secret buffer with zero bytes in place."""
    for index in range(len(buffer)):
        buffer[index] = 0


def redact(value: str) -> str:
    """Redact an identifier or digest to its first and last four characters."""
    return f"{value[:4]}…{value[-4:]}"


def fsync_directory(path: Path) -> None:
    """Flush a directory entry to stable storage using a hardened descriptor."""
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

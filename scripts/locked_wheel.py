#!/usr/bin/env python3
"""Resolve one unambiguous universal wheel from a frozen uv lockfile."""

from __future__ import annotations

import re
import sys
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import unquote, urlparse

import tomli

_LOCKED_SHA256 = re.compile(r"sha256:([0-9a-f]{64})")


class LockedWheelError(ValueError):
    """The lockfile cannot supply one safe wheel for the requested package."""


def _read_lock(lock_path: Path) -> dict[str, Any]:
    try:
        with lock_path.open("rb") as lock_file:
            lock = tomli.load(lock_file)
    except (OSError, tomli.TOMLDecodeError) as error:
        raise LockedWheelError(f"cannot read lockfile: {error}") from error
    if not isinstance(lock, dict):
        raise LockedWheelError("lockfile root must be a table")
    return lock


def resolve_locked_wheel(lock_path: Path, package: str, version: str) -> tuple[str, str, str]:
    """Return the URL, SHA-256, and filename for one universal locked wheel."""
    packages = _read_lock(lock_path).get("package")
    if not isinstance(packages, list):
        raise LockedWheelError("lockfile has no package records")

    matches = [
        record
        for record in packages
        if isinstance(record, dict)
        and record.get("name") == package
        and record.get("version") == version
    ]
    if len(matches) != 1:
        raise LockedWheelError(
            f"expected exactly one {package}=={version} package record; found {len(matches)}"
        )

    wheels = matches[0].get("wheels")
    if not isinstance(wheels, list):
        raise LockedWheelError(f"{package}=={version} has no wheel records")

    candidates: list[tuple[str, str, str]] = []
    for wheel in wheels:
        if not isinstance(wheel, dict):
            continue
        url = wheel.get("url")
        locked_hash = wheel.get("hash")
        if not isinstance(url, str) or not isinstance(locked_hash, str):
            continue
        parsed = urlparse(url)
        filename = PurePosixPath(unquote(parsed.path)).name
        if not filename.endswith("-py3-none-any.whl"):
            continue
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or any(char.isspace() for char in url)
            or any(char.isspace() for char in filename)
        ):
            raise LockedWheelError(f"{package}=={version} has an unsafe universal wheel URL")
        hash_match = _LOCKED_SHA256.fullmatch(locked_hash)
        if hash_match is None:
            raise LockedWheelError(f"{package}=={version} has an invalid universal wheel SHA-256")
        candidates.append((url, hash_match.group(1), filename))

    if len(candidates) != 1:
        raise LockedWheelError(
            f"expected exactly one py3-none-any wheel for {package}=={version}; "
            f"found {len(candidates)}"
        )
    return candidates[0]


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: locked_wheel.py UV_LOCK PACKAGE VERSION", file=sys.stderr)
        return 2
    try:
        wheel = resolve_locked_wheel(Path(argv[0]), argv[1], argv[2])
    except LockedWheelError as error:
        print(f"locked wheel resolution failed: {error}", file=sys.stderr)
        return 1
    print("\t".join(wheel))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

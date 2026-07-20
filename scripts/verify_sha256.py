#!/usr/bin/env python3
"""Fail closed unless one downloaded artifact matches its locked SHA-256."""

from __future__ import annotations

import hashlib
import hmac
import re
import sys
from pathlib import Path

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(argv: list[str]) -> int:
    if len(argv) != 2 or _SHA256_PATTERN.fullmatch(argv[1]) is None:
        print("usage: verify_sha256.py ARTIFACT EXPECTED_SHA256", file=sys.stderr)
        return 2
    artifact = Path(argv[0])
    actual = _sha256(artifact)
    if not hmac.compare_digest(actual, argv[1]):
        print(
            f"SHA-256 mismatch for {artifact.name}: expected {argv[1]}, got {actual}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

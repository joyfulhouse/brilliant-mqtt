from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ManifestEntry:
    relative_path: str
    size: int
    sha256: str


def build_manifest(private_root: Path, repository_root: Path) -> tuple[ManifestEntry, ...]:
    private = private_root.resolve(strict=True)
    repository = repository_root.resolve(strict=True)
    if private == repository or repository in private.parents:
        raise ValueError("private firmware root must remain outside the repository")
    entries: list[ManifestEntry] = []
    for path in sorted(candidate for candidate in private.rglob("*") if candidate.is_file()):
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        entries.append(
            ManifestEntry(
                relative_path=path.relative_to(private).as_posix(),
                size=path.stat().st_size,
                sha256=digest.hexdigest(),
            )
        )
    return tuple(entries)

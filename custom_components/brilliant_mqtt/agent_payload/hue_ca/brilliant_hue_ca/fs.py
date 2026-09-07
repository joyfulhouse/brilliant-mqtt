"""Filesystem boundary. Real impl is stdlib-only; tests fake the Protocol."""

from __future__ import annotations

import os
from typing import Protocol


class FileSystem(Protocol):
    def exists(self, path: str) -> bool: ...
    def read_text(self, path: str) -> str: ...
    def append_text(self, path: str, text: str) -> None: ...
    def write_text(self, path: str, text: str) -> None: ...
    def glob(self, root: str, name: str) -> str | None: ...


class RealFileSystem:
    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def read_text(self, path: str) -> str:
        with open(path, encoding="utf-8") as f:
            return f.read()

    def append_text(self, path: str, text: str) -> None:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text)

    def write_text(self, path: str, text: str) -> None:
        # Atomic replace — used for the small pending-reload state file. Writing
        # to a sibling temp file, fsyncing, then os.replace() means a crash mid
        # write can only leave the *previous* marker (or nothing), never a torn
        # one that load_pending would misread as "nothing owed" (issue #96). The
        # parent dir is created so a first write on a fresh panel can't fail.
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def glob(self, root: str, name: str) -> str | None:
        for dirpath, _dirs, files in os.walk(root):
            if name in files:
                return os.path.join(dirpath, name)
        return None

"""Persistent reboot cooldown + cap so a fleet-wide gateway loss can't reboot-loop."""

from __future__ import annotations

import json
import logging
import math
import os
import stat
import time
from dataclasses import dataclass

_LOG = logging.getLogger(__name__)
_MAX_STATE_BYTES = 65536


@dataclass(frozen=True)
class GuardPolicy:
    cooldown: float = 3600.0
    cap: int = 3
    window: float = 21600.0


def _is_stamp(x: object) -> bool:
    if not isinstance(x, (int, float)) or isinstance(x, bool):
        return False
    try:
        return math.isfinite(x)
    except OverflowError:  # a huge JSON integer
        return False


class RebootGuard:
    """Persistent reboot cooldown + rolling cap with fail-closed bad-state handling."""

    def __init__(self, path: str, policy: GuardPolicy) -> None:
        self._path = path
        self._p = policy
        self._bad_since: float | None = None
        self._last_read_bad = False
        self._future_clamp: float | None = None
        self._future_history: tuple[float, ...] | None = None
        self._last_wall: float | None = None

    @property
    def last_read_bad(self) -> bool:
        """Whether the most recent can_reboot() read found the state bad."""
        return self._last_read_bad

    def _load(self) -> list[float] | None:
        """Return [] when missing, None when present but unreadable or invalid."""
        try:
            info = os.stat(self._path)
            if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_STATE_BYTES:
                return None
            fd = os.open(self._path, os.O_RDONLY | os.O_NONBLOCK)
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_STATE_BYTES:
                    return None
                raw = b""
                while len(raw) <= _MAX_STATE_BYTES:
                    chunk = os.read(fd, _MAX_STATE_BYTES + 1 - len(raw))
                    if not chunk:
                        break
                    raw += chunk
            finally:
                os.close(fd)
            if len(raw) > _MAX_STATE_BYTES:
                return None
            data = json.loads(raw)
        except FileNotFoundError:
            return []
        except (OSError, ValueError, RecursionError):
            return None
        if not isinstance(data, list) or not all(_is_stamp(x) for x in data):
            return None
        return [float(x) for x in data]

    def _save(self, stamps: list[float]) -> None:
        tmp = f"{self._path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(stamps, f)
        os.replace(tmp, self._path)

    def _history_allows(self, stamps: list[float], now: float) -> bool:
        if stamps and now - max(stamps) < self._p.cooldown:
            return False
        return len(stamps) < self._p.cap

    def can_reboot(self, now: float) -> bool:
        loaded = self._load()
        future = loaded is not None and any(t > now for t in loaded)
        identity = tuple(loaded) if loaded is not None else None
        if future:
            if identity != self._future_history or (
                self._last_wall is not None and now < self._last_wall
            ):
                # A new history or another clock correction is a new episode;
                # an earlier normalization must not age these stamps away.
                self._future_clamp = None
                self._future_history = identity
                self._bad_since = None
        else:
            self._future_clamp = None
            self._future_history = None
        self._last_wall = now
        bad = loaded is None or (future and self._future_clamp is None)
        self._last_read_bad = loaded is None or future
        mono = time.monotonic()
        if bad and self._bad_since is None:
            self._bad_since = mono
            _LOG.error(
                "reboot guard state %s is unreadable or invalid; blocking reboots for %.0fs",
                self._path,
                self._p.cooldown,
            )
        if self._bad_since is not None:
            # Wall-clock corrections must not shorten this safety interval.
            if mono - self._bad_since < self._p.cooldown:
                return False
            self._bad_since = None
            if future:
                # Anchor once: repeatedly clamping to each poll's `now` would
                # continually restart the normal cooldown and wedge recovery.
                self._future_clamp = now
        stamps = self._normalize(loaded or [], now)
        return self._history_allows(stamps, now)

    def _normalize(self, stamps: list[float], now: float) -> list[float]:
        clamp = self._future_clamp if tuple(stamps) == self._future_history else None
        normalized = [
            min(now, clamp if clamp is not None else now) if t > now else t for t in stamps
        ]
        return [t for t in normalized if 0 <= now - t <= self._p.window]

    def record(self, now: float) -> None:
        stamps = self._normalize(self._load() or [], now)
        stamps.append(now)
        self._save(stamps)

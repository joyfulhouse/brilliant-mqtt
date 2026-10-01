"""Persistent reboot cooldown + cap so a fleet-wide gateway loss can't reboot-loop."""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass

_LOG = logging.getLogger(__name__)


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

    def _load(self) -> list[float] | None:
        """Return [] when missing, None when present but unreadable or invalid."""
        try:
            with open(self._path, encoding="utf-8") as f:
                data = json.load(f)
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
        if loaded is not None and any(t > now for t in loaded):
            loaded = None
        if loaded is None and self._bad_since is None:
            self._bad_since = now
            _LOG.error(
                "reboot guard state %s is unreadable or invalid; blocking reboots for %.0fs",
                self._path,
                self._p.cooldown,
            )
        if self._bad_since is not None:
            if now < self._bad_since:
                self._bad_since = now
            if now - self._bad_since < self._p.cooldown:
                return False
            self._bad_since = None
        stamps = [t for t in loaded or [] if 0 <= now - t <= self._p.window]
        return self._history_allows(stamps, now)

    def can_request(self, now: float) -> bool:
        return self.can_reboot(now)

    def record(self, now: float) -> None:
        stamps = [t for t in self._load() or [] if 0 <= now - t <= self._p.window]
        stamps.append(now)
        self._save(stamps)

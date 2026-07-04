"""Persistent reboot cooldown + cap so a fleet-wide gateway loss can't reboot-loop."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class GuardPolicy:
    cooldown: float = 3600.0
    cap: int = 3
    window: float = 21600.0


class RebootGuard:
    def __init__(self, path: str, policy: GuardPolicy) -> None:
        self._path = path
        self._p = policy

    def _load(self) -> list[float]:
        try:
            with open(self._path, encoding="utf-8") as f:
                data = json.load(f)
            return [float(x) for x in data] if isinstance(data, list) else []
        except (OSError, ValueError, TypeError):
            return []

    def _save(self, stamps: list[float]) -> None:
        tmp = f"{self._path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(stamps, f)
        os.replace(tmp, self._path)

    def can_reboot(self, now: float) -> bool:
        stamps = [t for t in self._load() if now - t <= self._p.window]
        if stamps and now - max(stamps) < self._p.cooldown:
            return False
        return len(stamps) < self._p.cap

    def record(self, now: float) -> None:
        stamps = [t for t in self._load() if now - t <= self._p.window]
        stamps.append(now)
        self._save(stamps)

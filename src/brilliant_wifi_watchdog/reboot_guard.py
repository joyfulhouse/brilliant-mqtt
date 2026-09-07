"""Persistent reboot cooldown + cap so a fleet-wide gateway loss can't reboot-loop."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

_BOOT_ID_PATH = "/proc/sys/kernel/random/boot_id"


def _read_boot_id() -> str | None:
    try:
        with open(_BOOT_ID_PATH, encoding="utf-8") as f:
            boot_id = f.read().strip()
    except OSError:
        return None
    return boot_id or None


@dataclass(frozen=True)
class GuardPolicy:
    cooldown: float = 3600.0
    cap: int = 3
    window: float = 21600.0


class RebootGuard:
    def __init__(
        self,
        path: str,
        policy: GuardPolicy,
        read_boot_id: Callable[[], str | None] = _read_boot_id,
    ) -> None:
        self._path = path
        self._p = policy
        self._read_boot_id = read_boot_id

    @property
    def _request_path(self) -> str:
        return f"{self._path}.request"

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

    def _load_request(self) -> tuple[str, float] | None:
        try:
            with open(self._request_path, encoding="utf-8") as f:
                data: Any = json.load(f)
            boot_id = data["boot_id"]
            requested_at = float(data["requested_at"])
            if not isinstance(boot_id, str) or not boot_id:
                return None
            return boot_id, requested_at
        except (KeyError, OSError, ValueError, TypeError):
            return None

    def _save_request(self, boot_id: str, requested_at: float) -> None:
        tmp = f"{self._request_path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"boot_id": boot_id, "requested_at": requested_at}, f)
        os.replace(tmp, self._request_path)

    def _clear_request(self) -> None:
        try:
            os.unlink(self._request_path)
        except OSError:
            pass

    def _boot_id(self) -> str | None:
        try:
            boot_id = self._read_boot_id()
        except OSError:
            return None
        return boot_id.strip() if boot_id else None

    def _confirmed_stamps(self, now: float) -> tuple[list[float], float | None]:
        stamps = [t for t in self._load() if now - t <= self._p.window]
        request = self._load_request()
        boot_id = self._boot_id()
        if request is None:
            return stamps, None

        request_boot_id, requested_at = request
        if boot_id is not None and request_boot_id == boot_id:
            return stamps, requested_at

        # A changed boot confirms the request worked. If boot identity is
        # unavailable, counting it is the conservative fail-safe.
        if now - requested_at <= self._p.window and requested_at not in stamps:
            stamps.append(requested_at)
        self._save(stamps)
        self._clear_request()
        return stamps, None

    def _history_allows(self, stamps: list[float], now: float) -> bool:
        if stamps and now - max(stamps) < self._p.cooldown:
            return False
        return len(stamps) < self._p.cap

    def can_reboot(self, now: float) -> bool:
        stamps, _ = self._confirmed_stamps(now)
        return self._history_allows(stamps, now)

    def can_request(self, now: float) -> bool:
        stamps, requested_at = self._confirmed_stamps(now)
        if not self._history_allows(stamps, now):
            return False
        return requested_at is None or now - requested_at >= self._p.cooldown

    def record(self, now: float) -> None:
        stamps = [t for t in self._load() if now - t <= self._p.window]
        stamps.append(now)
        self._save(stamps)

    def record_request(self, now: float) -> None:
        boot_id = self._boot_id()
        if boot_id is None:
            self.record(now)
            return
        self._save_request(boot_id, now)

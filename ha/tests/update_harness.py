"""Shared harness for the agent update retry, resume and rollback tests (#219).

Fixtures wrapping these live in conftest.py (``clock``, ``spies``, ``shells``,
``durable_journal_store``, ``reconnect_sleeps``).
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import json as json_util
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import connect_retry, panel_ops
from custom_components.brilliant_mqtt import manager as manager_module
from custom_components.brilliant_mqtt.const import DOMAIN
from custom_components.brilliant_mqtt.entry_data import LegacyPanelStore
from custom_components.brilliant_mqtt.fleet_manager import legacy_fleet_config
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.provisioning_journal import Store
from custom_components.brilliant_mqtt.shell import PanelShell
from tests.canary_rehearsal import RehearsalShell
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA

BASELINE_TRANSACTION = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")


@dataclass
class Clock:
    """Deterministic monotonic clock + backoff sleep for the update retry loop."""

    now: float = 1000.0
    sleeps: list[float] = field(default_factory=list)
    gate: asyncio.Event | None = None
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    on_sleep: Callable[[float], None] | None = None

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        if self.on_sleep is not None:
            self.on_sleep(delay)
        self.entered.set()
        if self.gate is not None:
            await self.gate.wait()
        self.now += delay


def install_clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    fake = Clock()
    monkeypatch.setattr(manager_module, "_connect_retry_clock", fake.monotonic)
    monkeypatch.setattr(manager_module, "_connect_retry_sleep", fake.sleep)
    monkeypatch.setattr(connect_retry, "jitter", lambda: 1.0)
    return fake


@dataclass
class Spies:
    """Counts of release admissions and baseline captures (the first mutations)."""

    admissions: int = 0
    baselines: int = 0
    # Optional hook run inside the stubbed baseline stage (before it yields).
    on_baseline: Callable[[PanelShell], Any] | None = None


def install_spies(monkeypatch: pytest.MonkeyPatch) -> Spies:
    counts = Spies()
    real_transaction = panel_ops.release_transaction

    @asynccontextmanager
    async def counted_transaction(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        counts.admissions += 1
        async with real_transaction(*args, **kwargs) as admission:
            yield admission

    @asynccontextmanager
    async def counted_baseline(
        manager: PanelManager, shell: PanelShell, version: str
    ) -> AsyncIterator[UUID]:
        counts.baselines += 1
        if counts.on_baseline is not None:
            result = counts.on_baseline(shell)
            if asyncio.iscoroutine(result):
                await result
        yield BASELINE_TRANSACTION

    monkeypatch.setattr(panel_ops, "release_transaction", counted_transaction)
    monkeypatch.setattr(PanelManager, "_async_canary_update", counted_baseline)
    return counts


def update_manager(
    hass: HomeAssistant,
    *,
    lock: asyncio.Lock | None = None,
    options: dict[str, Any] | None = None,
) -> PanelManager:
    """A legacy-entry PanelManager whose connects go through the Shells factory."""
    entry = MockConfigEntry(
        domain=DOMAIN, unique_id="office", data=ENTRY_DATA, options=options or {}
    )
    entry.add_to_hass(hass)
    return PanelManager(
        hass,
        LegacyPanelStore(hass, entry),
        legacy_fleet_config(entry),
        lock or asyncio.Lock(),
    )


@dataclass
class Shells:
    """Each connect attempt constructs the next scripted shell (default: healthy)."""

    scripted: list[FakeShell] = field(default_factory=list)
    created: list[FakeShell] = field(default_factory=list)
    default: Callable[[], FakeShell] = FakeShell

    def __call__(self, *_args: object) -> FakeShell:
        shell = self.scripted.pop(0) if self.scripted else self.default()
        self.created.append(shell)
        return shell

    @property
    def connects(self) -> list[int]:
        return [shell.connect_count for shell in self.created]

    @property
    def mutated(self) -> bool:
        return any(
            shell.uploads or shell.dir_uploads or shell.file_uploads or shell.commands
            for shell in self.created
        )


def install_durable_store(
    hass: HomeAssistant, config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Undo the HA harness's memory-only writer so the journal persists to disk."""
    hass.config.config_dir = str(config_dir)

    async def write(store: Store, data: dict[str, object]) -> None:
        def persist() -> None:
            Path(store.path).parent.mkdir(parents=True, exist_ok=True)
            json_util.save_json(store.path, data, private=True, atomic_writes=True)

        await hass.async_add_executor_job(persist)

    async def remove(store: Store) -> None:
        await hass.async_add_executor_job(Path(store.path).unlink, True)

    monkeypatch.setattr(Store, "_async_write_data", write)
    monkeypatch.setattr(Store, "async_remove", remove)


def fresh_view(shell: RehearsalShell) -> RehearsalShell:
    """A new, unconnected session onto the same rehearsal panel filesystem."""
    clone = copy.copy(shell)
    clone.lost = False
    clone.drop_on = None
    clone.drop_after = None
    clone.connected = False
    clone.connect_count = 0
    clone.commands = []
    clone.started_processes = []
    return clone

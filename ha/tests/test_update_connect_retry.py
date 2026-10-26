"""Bounded automatic retry of the agent update's pre-mutation SSH connect (#219)."""

from __future__ import annotations

import asyncio
import errno
import logging
import socket
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID

import asyncssh
import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import manager as manager_module
from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.const import (
    CONF_HOST,
    DOMAIN,
    OPT_TRUST_HOST_KEY_CHANGES,
)
from custom_components.brilliant_mqtt.entry_data import LegacyPanelStore
from custom_components.brilliant_mqtt.fleet_manager import legacy_fleet_config
from custom_components.brilliant_mqtt.manager import (
    _CONNECT_RETRY_BASE_DELAY_S,
    _CONNECT_RETRY_DEADLINE_S,
    _CONNECT_RETRY_MAX_ATTEMPTS,
    PanelManager,
    _classify_transient_connect_failure,
    _HostKeyChanged,
)
from custom_components.brilliant_mqtt.shell import PanelShell
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA

_PANEL_ADDRESS = str(ENTRY_DATA[CONF_HOST])
_UNREACHABLE = OSError(errno.EHOSTUNREACH, "No route to host")
_CONNECT_SUMMARY = "agent update could not connect to the panel"


@dataclass
class _Clock:
    """Deterministic monotonic clock + backoff sleep for the retry loop."""

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


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr(manager_module, "_connect_retry_clock", fake.monotonic)
    monkeypatch.setattr(manager_module, "_connect_retry_sleep", fake.sleep)
    monkeypatch.setattr(manager_module, "_connect_retry_jitter", lambda: 1.0)
    return fake


@dataclass
class _Spies:
    admissions: int = 0
    baselines: int = 0


@pytest.fixture
def spies(monkeypatch: pytest.MonkeyPatch) -> _Spies:
    """Count release admissions and baseline captures (the first mutation stages)."""
    counts = _Spies()
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
        yield UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")

    monkeypatch.setattr(panel_ops, "release_transaction", counted_transaction)
    monkeypatch.setattr(PanelManager, "_async_canary_update", counted_baseline)
    return counts


def _manager(
    hass: HomeAssistant,
    *,
    lock: asyncio.Lock | None = None,
    options: dict[str, Any] | None = None,
) -> PanelManager:
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
class _Shells:
    """Each connect attempt constructs the next scripted shell (default: healthy)."""

    scripted: list[FakeShell] = field(default_factory=list)
    created: list[FakeShell] = field(default_factory=list)

    def __call__(self, *_args: object) -> FakeShell:
        shell = self.scripted.pop(0) if self.scripted else FakeShell()
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


@pytest.fixture
def shells() -> Iterator[_Shells]:
    factory = _Shells()
    with patch("custom_components.brilliant_mqtt.manager.LegacyAsyncsshShell", side_effect=factory):
        yield factory


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (OSError(errno.EHOSTUNREACH, "No route to host"), "host_unreachable"),
        (OSError(errno.ENETUNREACH, "Network is unreachable"), "network_unreachable"),
        (ConnectionRefusedError(errno.ECONNREFUSED, "refused"), "connection_refused"),
        (OSError(errno.ETIMEDOUT, "timed out"), "timeout"),
        (TimeoutError(), "timeout"),
        (socket.gaierror(socket.EAI_AGAIN, "Temporary failure"), "dns_temporary_failure"),
        (asyncssh.ConnectionLost("lost"), "connection_lost"),
        (socket.gaierror(socket.EAI_NONAME, "unknown host"), None),
        (OSError("panel SSH session could not be closed"), None),
        (OSError(errno.EACCES, "Permission denied"), None),
        (asyncssh.PermissionDenied("bad password"), None),
        (asyncssh.HostKeyNotVerifiable("changed"), None),
        (asyncssh.KeyExchangeFailed("kex"), None),
        (asyncssh.ProtocolError("protocol"), None),
        (_HostKeyChanged(), None),
        (ValueError("identity"), None),
    ],
)
def test_connect_failure_classification(error: BaseException, reason: str | None) -> None:
    """Only network-level, pre-authentication failures are transient."""
    assert _classify_transient_connect_failure(error) == reason


async def test_transient_connect_failure_then_success_updates_once(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: _Clock,
    spies: _Spies,
    shells: _Shells,
    caplog: pytest.LogCaptureFixture,
) -> None:
    shells.scripted.extend([FakeShell(connect_error=_UNREACHABLE), FakeShell()])
    manager = _manager(hass)
    progress: list[int] = []
    retry_states: list[object] = []
    clock.on_sleep = lambda _delay: retry_states.append(
        (dict(manager.update_connect_retry or {}), list(progress))
    )
    caplog.set_level(logging.WARNING, logger="custom_components.brilliant_mqtt.manager")

    await manager.async_update_agent(progress=progress.append)

    assert shells.connects == [1, 1]
    assert spies.admissions == 1
    assert spies.baselines == 1
    assert clock.sleeps == [_CONNECT_RETRY_BASE_DELAY_S]
    # While waiting, progress stays at the pre-connect stage and the retry is visible.
    assert retry_states == [
        (
            {
                "attempt": 1,
                "max_attempts": _CONNECT_RETRY_MAX_ATTEMPTS,
                "reason": "host_unreachable",
                "retry_in_seconds": _CONNECT_RETRY_BASE_DELAY_S,
            },
            [10],
        )
    ]
    assert manager.update_connect_retry is None
    assert progress[-1] == 100
    assert manager.problem is False
    retry_logs = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING and "retrying" in record.getMessage()
    ]
    assert retry_logs == [
        f"office: agent update connect attempt 1/{_CONNECT_RETRY_MAX_ATTEMPTS} failed "
        "(host_unreachable); retrying in 5.0 s"
    ]
    assert _PANEL_ADDRESS not in repr(caplog.messages)
    await manager.async_shutdown()


async def test_exhausted_connect_retries_fail_typed_without_mutation(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: _Clock,
    spies: _Spies,
    shells: _Shells,
) -> None:
    shells.scripted.extend(
        FakeShell(connect_error=_UNREACHABLE) for _ in range(_CONNECT_RETRY_MAX_ATTEMPTS + 2)
    )
    manager = _manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1] * _CONNECT_RETRY_MAX_ATTEMPTS
    # Capped exponential backoff: 5, 10, 20, 30 (cap) with jitter pinned to 1.0.
    assert clock.sleeps == [5.0, 10.0, 20.0, 30.0]
    summary = f"{_CONNECT_SUMMARY} after {_CONNECT_RETRY_MAX_ATTEMPTS} attempts"
    assert raised.value.translation_key == "update_failed"
    assert raised.value.translation_placeholders == {"error": summary}
    assert raised.value.__cause__ is None
    assert manager.problem_reason == summary
    assert _PANEL_ADDRESS not in repr(raised.value.translation_placeholders)
    assert spies.admissions == 0
    assert spies.baselines == 0
    assert not shells.mutated
    assert manager.update_connect_retry is None
    assert manager._repairing is False
    assert manager._recovery_cancel is None


@pytest.mark.parametrize(
    "error",
    [
        asyncssh.PermissionDenied("bad password"),
        asyncssh.KeyExchangeFailed("kex"),
        OSError(errno.EACCES, "Permission denied"),
    ],
)
async def test_permanent_connect_failure_is_not_retried(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: _Clock,
    spies: _Spies,
    shells: _Shells,
    error: Exception,
) -> None:
    """Never retry authentication (password-only root SSH: avoid lockout)."""
    shells.scripted.append(FakeShell(connect_error=error))
    manager = _manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1]
    assert clock.sleeps == []
    assert raised.value.translation_placeholders == {"error": _CONNECT_SUMMARY}
    assert spies.admissions == 0


async def test_host_key_mismatch_is_not_retried(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: _Clock,
    spies: _Spies,
    shells: _Shells,
) -> None:
    shells.scripted.append(FakeShell(connect_error=asyncssh.HostKeyNotVerifiable("changed")))
    manager = _manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert raised.value.translation_key == "host_key_changed"
    assert shells.connects == [1]
    assert clock.sleeps == []
    assert spies.admissions == 0


async def test_transient_failure_during_opt_in_repin_is_not_retried(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: _Clock,
    spies: _Spies,
    shells: _Shells,
) -> None:
    """The unpinned re-pin connect offers the password to a new key: never repeat it."""
    shells.scripted.extend(
        [
            FakeShell(connect_error=asyncssh.HostKeyNotVerifiable("changed")),
            FakeShell(connect_error=_UNREACHABLE, pinned=None),
        ]
    )
    manager = _manager(hass, options={OPT_TRUST_HOST_KEY_CHANGES: True})

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1, 1]
    assert clock.sleeps == []
    assert raised.value.translation_placeholders == {"error": _CONNECT_SUMMARY}


async def test_transient_then_permanent_failure_stops_and_counts_attempts(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: _Clock,
    spies: _Spies,
    shells: _Shells,
) -> None:
    shells.scripted.extend(
        [
            FakeShell(connect_error=_UNREACHABLE),
            FakeShell(connect_error=asyncssh.PermissionDenied("bad password")),
        ]
    )
    manager = _manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1, 1]
    assert len(clock.sleeps) == 1
    assert raised.value.translation_placeholders == {
        "error": f"{_CONNECT_SUMMARY} after 2 attempts"
    }


class _SlowFailingShell(FakeShell):
    """A connect that burns *cost* seconds of the fake clock before failing."""

    def __init__(self, clock: _Clock, cost: float) -> None:
        super().__init__(connect_error=_UNREACHABLE)
        self._clock = clock
        self._cost = cost

    async def connect(self) -> None:
        self._clock.now += self._cost
        await super().connect()


async def test_overall_deadline_bounds_retries(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: _Clock,
    spies: _Spies,
    shells: _Shells,
) -> None:
    """Slow failing connects exhaust the deadline before the attempt budget."""
    shells.scripted.extend(
        _SlowFailingShell(clock, 40.0) for _ in range(_CONNECT_RETRY_MAX_ATTEMPTS)
    )
    manager = _manager(hass)
    start = clock.now

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    # t=40 fail, sleep 5, t=85 fail, sleep 10, t=135 fail: past the deadline, stop.
    assert shells.connects == [1, 1, 1]
    assert clock.sleeps == [5.0, 10.0]
    assert clock.now - start <= _CONNECT_RETRY_DEADLINE_S + 40.0
    assert raised.value.translation_placeholders == {
        "error": f"{_CONNECT_SUMMARY} after 3 attempts"
    }
    assert spies.admissions == 0


async def test_retry_is_not_scheduled_past_the_deadline(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: _Clock,
    spies: _Spies,
    shells: _Shells,
) -> None:
    """A backoff that would end after the deadline is not slept at all."""
    shells.scripted.extend(
        [_SlowFailingShell(clock, 100.0), _SlowFailingShell(clock, 10.0), FakeShell()]
    )
    manager = _manager(hass)

    with pytest.raises(HomeAssistantError):
        await manager.async_update_agent()

    # t=100 fail, sleep 5 (ends 105 <= 120), t=115 fail, sleep 10 would end at 125.
    assert shells.connects == [1, 1]
    assert clock.sleeps == [5.0]


async def test_cancellation_during_backoff_stops_promptly(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: _Clock,
    spies: _Spies,
    shells: _Shells,
) -> None:
    lock = asyncio.Lock()
    clock.gate = asyncio.Event()
    shells.scripted.append(FakeShell(connect_error=_UNREACHABLE))
    manager = _manager(hass, lock=lock)

    update = asyncio.create_task(manager.async_update_agent())
    await clock.entered.wait()
    assert manager.update_connect_retry is not None
    update.cancel()
    with pytest.raises(asyncio.CancelledError):
        await update

    assert shells.connects == [1]
    assert not lock.locked()
    assert manager._repairing is False
    assert manager.update_connect_retry is None
    assert spies.admissions == 0
    assert not shells.mutated


async def test_shutdown_during_backoff_stops_without_another_attempt(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: _Clock,
    spies: _Spies,
    shells: _Shells,
) -> None:
    clock.gate = asyncio.Event()
    shells.scripted.append(FakeShell(connect_error=_UNREACHABLE))
    manager = _manager(hass)

    update = asyncio.create_task(manager.async_update_agent())
    await clock.entered.wait()
    await manager.async_shutdown()
    clock.gate.set()
    with pytest.raises(HomeAssistantError) as raised:
        await update

    assert shells.connects == [1]
    assert raised.value.translation_placeholders == {"error": _CONNECT_SUMMARY}
    assert spies.admissions == 0


async def test_backoff_releases_fleet_lock_but_admits_no_concurrent_panel_writer(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: _Clock,
    spies: _Spies,
    shells: _Shells,
) -> None:
    """Other panels may use the fleet SSH lock during backoff; this panel stays owned."""
    lock = asyncio.Lock()
    clock.gate = asyncio.Event()
    shells.scripted.extend([FakeShell(connect_error=_UNREACHABLE), FakeShell()])
    manager = _manager(hass, lock=lock)

    update = asyncio.create_task(manager.async_update_agent())
    await clock.entered.wait()

    assert not lock.locked()
    with pytest.raises(HomeAssistantError) as concurrent:
        await manager.async_update_agent()
    assert concurrent.value.translation_key == "already_in_progress"
    await manager.async_repair()  # early-returns: the update owns the panel
    assert shells.connects == [1]
    assert spies.admissions == 0

    clock.gate.set()
    await update
    assert shells.connects == [1, 1]
    assert spies.admissions == 1
    assert spies.baselines == 1
    await manager.async_shutdown()

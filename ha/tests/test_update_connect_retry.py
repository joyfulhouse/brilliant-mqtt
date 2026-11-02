"""Bounded automatic retry of the agent update's pre-mutation SSH connect (#219)."""

from __future__ import annotations

import asyncio
import errno
import logging
import socket
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncssh
import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import issue_registry as ir

from custom_components.brilliant_mqtt import connect_retry, panel_ops, update_state
from custom_components.brilliant_mqtt import manager as manager_module
from custom_components.brilliant_mqtt.const import (
    CONF_HOST,
    DATA_SSH_HOST_KEY,
    DOMAIN,
    OPT_TRUST_HOST_KEY_CHANGES,
)
from custom_components.brilliant_mqtt.manager import (
    _CONNECT_RETRY_BASE_DELAY_S,
    _CONNECT_RETRY_DEADLINE_S,
    _CONNECT_RETRY_MAX_ATTEMPTS,
    _classify_transient_connect_failure,
    _HostKeyChanged,
)
from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningJournal
from custom_components.brilliant_mqtt.shell import PanelShell
from custom_components.brilliant_mqtt.update_intent import pinned_fingerprint, update_intent_store
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA
from tests.test_provisioning_journal import _record
from tests.test_release_identity import _identity
from tests.update_harness import Clock, Shells, Spies, update_manager

_PANEL_ADDRESS = str(ENTRY_DATA[CONF_HOST])
# asyncio's real message for a failed TCP connect carries the panel address.
_UNREACHABLE = OSError(errno.EHOSTUNREACH, f"Connect call failed ('{_PANEL_ADDRESS}', 22)")
_CONNECT_SUMMARY = "agent update could not connect to the panel"
_SHUTDOWN_SUMMARY = "agent update stopped before connecting because the integration is unloading"
_POST_AUTH_FAILURES = [
    TimeoutError(),
    asyncssh.ConnectionLost("lost"),
    ConnectionResetError(errno.ECONNRESET, "Connection reset by peer"),
]


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
    clock: Clock,
    spies: Spies,
    shells: Shells,
    caplog: pytest.LogCaptureFixture,
) -> None:
    shells.scripted.extend([FakeShell(connect_error=_UNREACHABLE), FakeShell()])
    manager = update_manager(hass)
    progress: list[int] = []
    retry_states: list[tuple[dict[str, Any], list[int]]] = []
    clock.on_sleep = lambda _delay: retry_states.append(
        (dict(manager.update_retry or {}), list(progress))
    )
    caplog.set_level(logging.WARNING, logger="custom_components.brilliant_mqtt.manager")

    await manager.async_update_agent(progress=progress.append)

    assert shells.connects == [1, 1]
    assert spies.admissions == 1
    assert spies.baselines == 1
    assert clock.sleeps == [_CONNECT_RETRY_BASE_DELAY_S]
    # While waiting, progress stays at the pre-connect stage and the retry is visible.
    assert len(retry_states) == 1
    state, waiting_progress = retry_states[0]
    next_attempt = state.pop("next_attempt_at")
    assert isinstance(next_attempt, str)
    assert datetime.fromisoformat(next_attempt).tzinfo is not None
    assert update_state.safe_retry_state({**state, "next_attempt_at": next_attempt}) is not None
    assert [(state, waiting_progress)] == [
        (
            {
                "stage": "connect",
                "attempt": 1,
                "max_attempts": _CONNECT_RETRY_MAX_ATTEMPTS,
                "reason": "host_unreachable",
                "retry_in_seconds": _CONNECT_RETRY_BASE_DELAY_S,
            },
            [10],
        )
    ]
    assert manager.update_retry is None
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
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    shells.scripted.extend(
        FakeShell(connect_error=_UNREACHABLE) for _ in range(_CONNECT_RETRY_MAX_ATTEMPTS + 2)
    )
    manager = update_manager(hass)

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
    assert manager.update_retry is None
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
    clock: Clock,
    spies: Spies,
    shells: Shells,
    error: Exception,
) -> None:
    """Never retry authentication (password-only root SSH: avoid lockout)."""
    shells.scripted.append(FakeShell(connect_error=error))
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1]
    assert clock.sleeps == []
    assert raised.value.translation_placeholders == {"error": _CONNECT_SUMMARY}
    assert spies.admissions == 0


async def test_host_key_mismatch_is_not_retried(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    shells.scripted.append(FakeShell(connect_error=asyncssh.HostKeyNotVerifiable("changed")))
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert raised.value.translation_key == "host_key_changed"
    assert shells.connects == [1]
    assert clock.sleeps == []
    assert spies.admissions == 0


async def test_transient_failure_during_opt_in_repin_is_not_retried(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    """The unpinned re-pin connect offers the password to a new key: never repeat it."""
    shells.scripted.extend(
        [
            FakeShell(connect_error=asyncssh.HostKeyNotVerifiable("changed")),
            FakeShell(connect_error=_UNREACHABLE, pinned=None),
        ]
    )
    manager = update_manager(hass, options={OPT_TRUST_HOST_KEY_CHANGES: True})

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1, 1]
    assert clock.sleeps == []
    assert raised.value.translation_placeholders == {"error": _CONNECT_SUMMARY}


async def test_transient_then_permanent_failure_stops_and_counts_attempts(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    shells.scripted.extend(
        [
            FakeShell(connect_error=_UNREACHABLE),
            FakeShell(connect_error=asyncssh.PermissionDenied("bad password")),
        ]
    )
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1, 1]
    assert len(clock.sleeps) == 1
    assert raised.value.translation_placeholders == {
        "error": f"{_CONNECT_SUMMARY} after 2 attempts"
    }


class _SlowFailingShell(FakeShell):
    """A connect that burns *cost* seconds of the fake clock before failing."""

    def __init__(self, clock: Clock, cost: float) -> None:
        super().__init__(connect_error=_UNREACHABLE)
        self._clock = clock
        self._cost = cost

    async def connect(self) -> None:
        self._clock.now += self._cost
        await super().connect()


async def test_overall_deadline_bounds_retries(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    """Slow failing connects exhaust the deadline before the attempt budget."""
    shells.scripted.extend(
        _SlowFailingShell(clock, 40.0) for _ in range(_CONNECT_RETRY_MAX_ATTEMPTS)
    )
    manager = update_manager(hass)
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
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    """A backoff that would end after the deadline is not slept at all."""
    shells.scripted.extend(
        [_SlowFailingShell(clock, 100.0), _SlowFailingShell(clock, 10.0), FakeShell()]
    )
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError):
        await manager.async_update_agent()

    # t=100 fail, sleep 5 (ends 105 <= 120), t=115 fail, sleep 10 would end at 125.
    assert shells.connects == [1, 1]
    assert clock.sleeps == [5.0]


async def test_cancellation_during_backoff_stops_promptly(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    lock = asyncio.Lock()
    clock.gate = asyncio.Event()
    shells.scripted.append(FakeShell(connect_error=_UNREACHABLE))
    manager = update_manager(hass, lock=lock)

    update = asyncio.create_task(manager.async_update_agent())
    await clock.entered.wait()
    assert manager.update_retry is not None
    update.cancel()
    with pytest.raises(asyncio.CancelledError):
        await update

    assert shells.connects == [1]
    assert not lock.locked()
    assert manager._repairing is False
    assert manager.update_retry is None
    assert spies.admissions == 0
    assert not shells.mutated


async def test_shutdown_during_backoff_stops_without_another_attempt(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    clock.gate = asyncio.Event()
    shells.scripted.append(FakeShell(connect_error=_UNREACHABLE))
    manager = update_manager(hass)

    update = asyncio.create_task(manager.async_update_agent())
    await clock.entered.wait()
    await manager.async_shutdown()
    clock.gate.set()
    with pytest.raises(HomeAssistantError) as raised:
        await update

    assert shells.connects == [1]
    assert raised.value.translation_placeholders == {"error": _SHUTDOWN_SUMMARY}
    assert spies.admissions == 0
    # A reload/unload is not a panel fault: no Repair issue, no problem flag.
    assert manager.problem is False
    assert ir.async_get(hass).async_get_issue(DOMAIN, manager._issue_id) is None


async def test_shutdown_while_waiting_for_the_lock_never_connects(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    """Shutdown latched while the update queued behind another panel stops it."""
    lock = asyncio.Lock()
    await lock.acquire()
    manager = update_manager(hass, lock=lock)

    update = asyncio.create_task(manager.async_update_agent())
    await asyncio.sleep(0)
    await manager.async_shutdown()
    lock.release()
    with pytest.raises(HomeAssistantError) as raised:
        await update

    assert shells.connects == []
    assert raised.value.translation_placeholders == {"error": _SHUTDOWN_SUMMARY}
    assert manager.problem is False
    assert not lock.locked()


async def test_deadline_starts_after_the_first_lock_acquisition(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    """Queueing behind other panels' updates does not consume the retry budget."""
    lock = asyncio.Lock()
    await lock.acquire()
    shells.scripted.extend([FakeShell(connect_error=_UNREACHABLE), FakeShell()])
    manager = update_manager(hass, lock=lock)

    update = asyncio.create_task(manager.async_update_agent())
    await asyncio.sleep(0)
    clock.now += 10 * _CONNECT_RETRY_DEADLINE_S  # a long fleet rollout ahead of us
    lock.release()
    await update

    assert shells.connects == [1, 1]
    assert clock.sleeps == [_CONNECT_RETRY_BASE_DELAY_S]
    assert spies.admissions == 1
    await manager.async_shutdown()


async def test_retry_lock_wait_past_the_deadline_gives_up_without_connecting(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    """A retry that only gets the lock after the deadline does not connect."""
    lock = asyncio.Lock()
    clock.gate = asyncio.Event()
    shells.scripted.extend([FakeShell(connect_error=_UNREACHABLE), FakeShell()])
    manager = update_manager(hass, lock=lock)

    update = asyncio.create_task(manager.async_update_agent())
    await clock.entered.wait()
    await lock.acquire()  # another panel's operation takes the lock during backoff
    clock.gate.set()
    await asyncio.sleep(0)
    clock.now += _CONNECT_RETRY_DEADLINE_S  # ... and holds it past the deadline
    lock.release()
    with pytest.raises(HomeAssistantError) as raised:
        await update

    assert shells.connects == [1]
    assert raised.value.translation_placeholders == {"error": _CONNECT_SUMMARY}
    assert spies.admissions == 0


async def test_retry_lock_wait_is_bounded_by_the_deadline(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    """A lock held indefinitely by another panel cannot stretch the retry budget."""
    lock = asyncio.Lock()
    clock.gate = asyncio.Event()

    def leave_50_ms(delay: float) -> None:
        clock.now += _CONNECT_RETRY_DEADLINE_S - delay - 0.05

    clock.on_sleep = leave_50_ms
    shells.scripted.append(FakeShell(connect_error=_UNREACHABLE))
    manager = update_manager(hass, lock=lock)

    update = asyncio.create_task(manager.async_update_agent())
    await clock.entered.wait()
    await lock.acquire()  # never released while the update waits
    clock.gate.set()
    with pytest.raises(HomeAssistantError) as raised:
        await asyncio.wait_for(update, timeout=5)

    assert shells.connects == [1]
    assert raised.value.translation_placeholders == {"error": _CONNECT_SUMMARY}
    lock.release()


@pytest.mark.parametrize("error", _POST_AUTH_FAILURES)
async def test_failure_after_authentication_began_is_not_retried(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
    error: Exception,
) -> None:
    """Once asyncssh began userauth the password may have been offered: no retry."""
    shells.scripted.append(FakeShell(connect_error=error, reached_auth=True))
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1]
    assert clock.sleeps == []
    assert raised.value.translation_placeholders == {"error": _CONNECT_SUMMARY}


@pytest.mark.parametrize("error", _POST_AUTH_FAILURES)
async def test_failure_before_authentication_is_retried(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
    error: Exception,
) -> None:
    shells.scripted.extend([FakeShell(connect_error=error), FakeShell()])
    manager = update_manager(hass)

    await manager.async_update_agent()

    assert shells.connects == [1, 1]
    assert spies.admissions == 1
    await manager.async_shutdown()


async def test_backoff_releases_fleet_lock_but_admits_no_concurrent_panel_writer(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    """Other panels may use the fleet SSH lock during backoff; this panel stays owned."""
    lock = asyncio.Lock()
    clock.gate = asyncio.Event()
    shells.scripted.extend([FakeShell(connect_error=_UNREACHABLE), FakeShell()])
    manager = update_manager(hass, lock=lock)

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


def test_backoff_delay_is_capped_and_jittered(monkeypatch: pytest.MonkeyPatch) -> None:
    """The shared policy: capped exponential backoff scaled by the one jitter seam."""
    assert [connect_retry.backoff_delay(n, 1.0) for n in range(1, 6)] == [
        5.0,
        10.0,
        20.0,
        30.0,
        30.0,
    ]
    assert connect_retry.backoff_delay(2, 0.5) == 5.0
    monkeypatch.setattr(connect_retry, "jitter", lambda: 0.5)
    assert manager_module._connect_retry_delay(1) == 2.5


# --- Resume of a lost release admission and stage boundaries (#219) ---------------

_IDENTITY_READ = "BRILLIANT_RELEASE_IDENTITY_READ"
_ADMISSION_LOST = "agent update lost the panel connection during release admission"
_OTHER_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBsF8pQ8C3EWxF7bGrXWvqJXV1fbNHo6p5FC0gIjJbqv"
_PINNED = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKIykuTed7zNwJwn20eCelcKcHKJ9c/pGFfvulRWazuC"


def _dropping(marker: str = _IDENTITY_READ) -> FakeShell:
    return FakeShell(drop_on=marker)


async def test_lost_admission_resumes_once_with_one_transaction(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    shells.scripted.extend([_dropping(), FakeShell()])
    manager = update_manager(hass)
    progress: list[int] = []
    waiting: list[tuple[dict[str, Any], list[int]]] = []
    clock.on_sleep = lambda _delay: waiting.append(
        (dict(manager.update_retry or {}), list(progress))
    )

    await manager.async_update_agent(progress=progress.append)

    assert shells.connects == [1, 1]
    assert spies.admissions == 2
    assert spies.baselines == 1  # one transaction only
    assert clock.sleeps == [_CONNECT_RETRY_BASE_DELAY_S]
    state, seen = waiting[0]
    assert (state["stage"], state["attempt"], state["reason"]) == (
        "admission",
        1,
        "connection_lost",
    )
    assert seen[-1] == 10  # a resume shows no completed work while waiting
    assert progress[-1] == 100
    assert manager.update_last_error is None
    assert manager.update_progress is None
    await manager.async_shutdown()


async def test_live_admission_failure_is_not_retried(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failing_read(_shell: object) -> object:
        raise panel_ops.PanelOpError("release_identity_read_failed")

    monkeypatch.setattr(panel_ops, "_read_release_identities", failing_read)
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1]
    assert clock.sleeps == []
    assert spies.baselines == 0
    error = (raised.value.translation_placeholders or {})["error"]
    assert error.startswith("agent update failed during deployment")
    assert manager.update_last_error == {
        "stage": "admission",
        "code": "admission_failed",
        "attempts": 1,
    }


async def test_loss_after_the_transaction_began_is_never_rerun(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    shells.scripted.append(FakeShell(drop_on="brilliant-mqtt.staging"))
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1]
    assert spies.admissions == 1
    assert spies.baselines == 1
    assert clock.sleeps == []
    error = (raised.value.translation_placeholders or {})["error"]
    assert error.startswith("agent update failed during deployment")
    assert manager.update_last_error == {
        "stage": "deployment",
        "code": "deployment_failed",
        "attempts": 1,
    }


async def test_connects_and_resumes_share_one_budget(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    shells.scripted.extend(
        [
            FakeShell(connect_error=_UNREACHABLE),
            _dropping(),
            FakeShell(connect_error=_UNREACHABLE),
            _dropping(),
            _dropping(),
            FakeShell(),
        ]
    )
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1] * _CONNECT_RETRY_MAX_ATTEMPTS
    assert clock.sleeps == [5.0, 10.0, 20.0, 30.0]
    summary = f"{_ADMISSION_LOST} after {_CONNECT_RETRY_MAX_ATTEMPTS} attempts"
    assert raised.value.translation_placeholders == {"error": summary}
    assert "during deployment" not in summary
    assert spies.baselines == 0
    assert manager.update_last_error == {
        "stage": "admission",
        "code": "admission_connection_lost",
        "attempts": _CONNECT_RETRY_MAX_ATTEMPTS,
    }


async def test_connect_failure_after_a_resume_uses_the_admission_summary(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    """Once connected, a later non-permanent connect failure is an admission loss."""

    def jump(_delay: float) -> None:
        clock.now += 200

    clock.on_sleep = jump
    shells.scripted.extend([_dropping(), FakeShell()])
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    # The resume backoff ran past the deadline: the next attempt never connected.
    assert shells.connects == [1]
    assert raised.value.translation_placeholders == {"error": f"{_ADMISSION_LOST} after 1 attempt"}


async def test_permanent_reconnect_failure_keeps_the_connect_wording(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    shells.scripted.extend(
        [_dropping(), FakeShell(connect_error=asyncssh.PermissionDenied("bad password"))]
    )
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1, 1]
    assert raised.value.translation_placeholders == {
        "error": f"{_CONNECT_SUMMARY} after 2 attempts"
    }


def test_attempt_pluralisation() -> None:
    assert manager_module._after_attempts("x", 1) == "x after 1 attempt"
    assert manager_module._after_attempts("x", 2) == "x after 2 attempts"


async def test_resume_never_repins_a_changed_host_key(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    shells.scripted.extend(
        [
            _dropping(),
            FakeShell(connect_error=asyncssh.HostKeyNotVerifiable("changed")),
            FakeShell(pinned=None),
        ]
    )
    manager = update_manager(hass, options={OPT_TRUST_HOST_KEY_CHANGES: True})

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert raised.value.translation_key == "host_key_changed"
    assert len(shells.created) == 2  # no unpinned re-pin shell
    assert spies.baselines == 0


async def test_cancellation_during_resume_backoff_releases_the_lock_and_keeps_the_window(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    lock = asyncio.Lock()
    clock.gate = asyncio.Event()
    shells.scripted.append(_dropping())
    manager = update_manager(hass, lock=lock)

    update = asyncio.create_task(manager.async_update_agent())
    await clock.entered.wait()
    assert not lock.locked()
    assert manager.update_retry is not None and manager.update_retry["stage"] == "admission"
    update.cancel()
    with pytest.raises(asyncio.CancelledError):
        await update

    assert not lock.locked()
    assert manager._repairing is False
    assert manager.update_retry is None
    record = await update_intent_store(hass).async_get(manager.management_id)
    assert record is not None and record.stage == "admission"
    assert spies.baselines == 0


async def test_shutdown_during_resume_backoff_stops_and_keeps_the_window(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    lock = asyncio.Lock()
    clock.gate = asyncio.Event()
    shells.scripted.extend([_dropping(), FakeShell()])
    manager = update_manager(hass, lock=lock)

    update = asyncio.create_task(manager.async_update_agent())
    await clock.entered.wait()
    assert not lock.locked()
    await manager.async_shutdown()
    clock.gate.set()
    with pytest.raises(HomeAssistantError) as raised:
        await update

    assert raised.value.translation_placeholders == {"error": _SHUTDOWN_SUMMARY}
    assert shells.connects == [1]
    assert manager.problem is False
    assert manager.update_last_error == {
        "stage": "connect",
        "code": "update_stopped",
        "attempts": 1,
    }
    assert await update_intent_store(hass).async_get(manager.management_id) is not None


async def test_terminal_failure_clears_the_window(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    shells.scripted.append(FakeShell(connect_error=asyncssh.PermissionDenied("bad")))
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError):
        await manager.async_update_agent()

    assert await update_intent_store(hass).async_get(manager.management_id) is None


async def test_lost_admission_with_an_override_is_not_resumed(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    """The override record may already be written: the single-use approval is spent."""
    probe = FakeShell()
    probe.release_identities["bridge"] = _identity()
    await probe.connect()
    with pytest.raises(panel_ops.ReleaseIdentityBlocked) as blocked:
        async with panel_ops.release_transaction(probe, str(payload_dir), panel="office"):
            pytest.fail("unapproved downgrade")
    shell = FakeShell(drop_after="BRILLIANT_RELEASE_IDENTITY_WRITE")
    shell.release_identities["bridge"] = _identity()
    shells.scripted.extend([shell, FakeShell()])
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent(release_override=blocked.value.override)

    assert shells.connects == [1]
    assert clock.sleeps == []
    assert shell.identity_uploads and "override-" in shell.identity_uploads[0][0]
    error = (raised.value.translation_placeholders or {})["error"]
    assert "release_override_used_or_write_failed" in error
    assert "obtain a fresh override" in error
    assert spies.baselines == 0


async def test_lost_admission_with_a_journal_present_is_not_resumed(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def present(_self: ProvisioningJournal) -> object:
        return _record()

    monkeypatch.setattr(ProvisioningJournal, "async_load", present)
    shells.scripted.extend([_dropping(), FakeShell()])
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1]
    assert raised.value.translation_placeholders == {"error": f"{_ADMISSION_LOST} after 1 attempt"}


@pytest.mark.parametrize(
    ("request_changes", "store_key", "recovery"),
    [
        ({"slug": "kitchen", "host": "kitchen.example"}, _PINNED, True),
        ({"host": "kitchen.example", "public_key": _OTHER_KEY}, _PINNED, True),
        ({"slug": "kitchen", "public_key": _OTHER_KEY}, _PINNED, True),
        ({"slug": "kitchen", "host": "kitchen.example", "public_key": _OTHER_KEY}, _PINNED, False),
        ({"slug": "kitchen", "host": "kitchen.example", "public_key": _OTHER_KEY}, "junk", True),
        ({"slug": "kitchen", "host": "kitchen.example", "public_key": _OTHER_KEY}, None, True),
    ],
    ids=["identity", "slug", "host", "none", "invalid_key", "missing_key"],
)
async def test_baseline_loss_stops_with_the_journal_aware_summary(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
    monkeypatch: pytest.MonkeyPatch,
    request_changes: dict[str, str],
    store_key: str | None,
    recovery: bool,
) -> None:
    base = _record()
    changes: dict[str, str] = {"host": str(ENTRY_DATA[CONF_HOST]), **request_changes}
    if "public_key" in request_changes:
        fingerprint = pinned_fingerprint(request_changes["public_key"])
        assert fingerprint is not None
        changes["fingerprint"] = fingerprint
    request = base.panel_request
    record = replace(
        base,
        panel_request=replace(
            request,
            host=changes["host"],
            slug=changes.get("slug", request.slug),
            public_key=changes.get("public_key", request.public_key),
            fingerprint=changes.get("fingerprint", request.fingerprint),
        ),
    )

    async def journal(_self: ProvisioningJournal) -> object:
        return record

    monkeypatch.setattr(ProvisioningJournal, "async_load", journal)

    async def lose(shell: PanelShell) -> None:
        assert isinstance(shell, FakeShell)
        shell.drop_on = "BASELINE_CAPTURE"
        await shell.run("BASELINE_CAPTURE")

    spies.on_baseline = lose
    manager = update_manager(hass)
    manager.store.update_data({**manager.store.data, DATA_SSH_HOST_KEY: store_key})

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1]
    assert spies.baselines == 1
    assert clock.sleeps == []
    error = (raised.value.translation_placeholders or {})["error"]
    if recovery:
        assert "provisioning_recovery_required" in error
        assert "remove nothing by hand" in error
    else:
        assert "baseline_capture_interrupted" in error
        assert "Interrupted baseline capture" in error


async def test_resume_refuses_a_changed_release_candidate(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    spies: Spies,
    shells: Shells,
) -> None:
    def change_payload(_delay: float) -> None:
        (payload_dir / "vendor/new.py").write_text("changed = True\n")

    clock.on_sleep = change_payload
    shells.scripted.extend([_dropping(), FakeShell()])
    manager = update_manager(hass)

    with pytest.raises(HomeAssistantError) as raised:
        await manager.async_update_agent()

    assert shells.connects == [1]
    error = (raised.value.translation_placeholders or {})["error"]
    assert "release_candidate_changed" in error
    assert manager.update_last_error == {
        "stage": "admission",
        "code": "release_candidate_changed",
        "attempts": 1,
    }

"""Historical detached-ticket contracts; API gaps are recorded separately."""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import AsyncIterator

import pytest

from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.write_admission import AdmissionTicket, WriteCancelled
from tests.fakes import _GatedRpcObserver
from tests.mesh_regression_support import settlement
from tests.mesh_regression_support import turns as _settle
from tests.test_bus_adapter import _gated_adapter


@pytest.fixture(autouse=True)
async def cleanup_adapters(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[None]:
    original = _gated_adapter
    owned: list[tuple[_GatedRpcObserver, RpcBusAdapter]] = []

    def create(*, fail_with: Exception | None = None) -> tuple[_GatedRpcObserver, RpcBusAdapter]:
        pair = original(fail_with=fail_with)
        owned.append(pair)
        return pair

    monkeypatch.setattr(sys.modules[__name__], "_gated_adapter", create)
    try:
        yield
    finally:
        for observer, adapter in owned:
            observer.release.set()
            await adapter.shutdown()


async def test_late_completion_reaches_the_settle_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    """#225: the detached write's late receipt reaches its ticket's callback."""
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.01)
    observer, adapter = _gated_adapter()
    ticket = AdmissionTicket()
    with pytest.raises(asyncio.TimeoutError):
        await adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    assert not settlement(ticket).settled
    seen: list[object] = []
    settlement(ticket).set_settle_callback(seen.append)

    observer.release.set()
    await asyncio.gather(*adapter._write_tasks)
    await _settle()

    assert seen == ["'ok'"]
    await adapter.shutdown()


async def test_late_failure_reaches_the_settle_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.01)
    error = RuntimeError("late boom")
    observer, adapter = _gated_adapter(fail_with=error)
    ticket = AdmissionTicket()
    with pytest.raises(asyncio.TimeoutError):
        await adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    seen: list[object] = []
    settlement(ticket).set_settle_callback(seen.append)

    observer.release.set()
    await asyncio.gather(*adapter._write_tasks, return_exceptions=True)
    await _settle()

    assert seen == [error]
    await adapter.shutdown()


async def test_shutdown_of_detached_write_settles_write_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.01)
    _observer, adapter = _gated_adapter()
    ticket = AdmissionTicket()
    with pytest.raises(asyncio.TimeoutError):
        await adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    seen: list[object] = []
    settlement(ticket).set_settle_callback(seen.append)

    await adapter.shutdown()  # cancels the still-running detached RPC
    await _settle()

    assert len(seen) == 1 and isinstance(seen[0], WriteCancelled)


async def test_failing_settle_callback_is_contained(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.01)
    observer, adapter = _gated_adapter()
    ticket = AdmissionTicket()
    with pytest.raises(asyncio.TimeoutError):
        await adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)

    def fail(_outcome: object) -> None:
        raise RuntimeError("private-payload-sentinel")

    settlement(ticket).set_settle_callback(fail)
    with caplog.at_level(logging.WARNING, logger="brilliant_mqtt.bus"):
        observer.release.set()
        await asyncio.gather(*adapter._write_tasks)
        await _settle()

    failures = [r for r in caplog.records if r.getMessage().startswith("WRITE_SETTLE")]
    assert [r.getMessage() for r in failures] == ["WRITE_SETTLE_CALLBACK_FAILED RuntimeError"]
    assert failures[0].exc_info is None
    assert "private-payload-sentinel" not in caplog.text
    # The lane still moved on: the next write is admitted and served.
    observer.release.set()
    assert await adapter.set_variables("ble_mesh", "p", [VarSet("on", "1")]) == "'ok'"
    await adapter.shutdown()

"""Adapter admission policy against gated native calls and a deterministic clock."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.write_admission import AdmissionTicket, Superseded, WriteClass, WriteResult
from tests.fakes import FakeClock


async def _settle() -> None:
    for _ in range(12):
        await asyncio.sleep(0)


class _Observer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, str]]] = []
        self.gates: list[asyncio.Event] = []
        self.in_flight: dict[str, int] = {}
        self.max_in_flight: dict[str, int] = {}
        self.max_total = 0
        self.cancelled = 0
        self.delay_cancellation = False
        self.release_all = False
        self.receipt = "ok"

    async def request_set_variables_in_peripheral(
        self, peripheral_id: str, values: dict[str, str], *, device_id: str
    ) -> str:
        self.calls.append((device_id, peripheral_id, dict(values)))
        gate = asyncio.Event()
        self.gates.append(gate)
        if self.release_all:
            gate.set()
        self.in_flight[device_id] = self.in_flight.get(device_id, 0) + 1
        self.max_in_flight[device_id] = max(
            self.max_in_flight.get(device_id, 0), self.in_flight[device_id]
        )
        self.max_total = max(self.max_total, sum(self.in_flight.values()))
        try:
            try:
                await gate.wait()
            except asyncio.CancelledError:
                self.cancelled += 1
                if not self.delay_cancellation:
                    raise
                await gate.wait()
        finally:
            self.in_flight[device_id] -= 1
        return self.receipt

    async def shutdown(self) -> None:
        pass


class _Harness:
    def __init__(self) -> None:
        self.clock = FakeClock()
        self.observer = _Observer()
        self.adapter = RpcBusAdapter(clock=self.clock)
        self.adapter._obs = self.observer
        self.adapter._own_device_id = "own-device"
        self.callers: list[asyncio.Task[WriteResult]] = []

    def submit(
        self,
        peripheral: str,
        payload: dict[str, str],
        *,
        device: str = "ble_mesh",
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> asyncio.Task[WriteResult]:
        caller = asyncio.create_task(
            self.adapter.set_variables(
                device,
                peripheral,
                [VarSet(name, value) for name, value in payload.items()],
                write_class=write_class,
                ticket=ticket,
            )
        )
        self.callers.append(caller)
        return caller

    async def queue(
        self,
        peripheral: str,
        payload: dict[str, str],
        *,
        device: str = "ble_mesh",
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> asyncio.Task[WriteResult]:
        caller = self.submit(
            peripheral, payload, device=device, write_class=write_class, ticket=ticket
        )
        await _settle()
        return caller

    async def release(self, index: int) -> None:
        self.observer.gates[index].set()
        await _settle()

    async def drain(self) -> list[WriteResult]:
        self.observer.release_all = True
        for gate in self.observer.gates:
            gate.set()
        return await asyncio.wait_for(asyncio.gather(*self.callers), timeout=2.0)


@pytest.fixture
async def harness() -> AsyncIterator[_Harness]:
    fixture = _Harness()
    try:
        yield fixture
    finally:
        fixture.observer.release_all = True
        for gate in fixture.observer.gates:
            gate.set()
        await asyncio.wait_for(fixture.adapter.shutdown(), timeout=2.0)
        await asyncio.wait_for(
            asyncio.gather(*fixture.callers, return_exceptions=True), timeout=2.0
        )


@pytest.mark.parametrize("add_variable", [False, True], ids=["same-keys", "superset"])
async def test_waiting_ticket_accepts_superset_and_issues_only_replacement(
    harness: _Harness, add_variable: bool
) -> None:
    await harness.queue("blocker", {"on": "1"})
    ticket = AdmissionTicket()
    original = await harness.queue(
        "light", {"on": "0"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    payload = {"on": "1"}
    if add_variable:
        payload["intensity"] = "80"
    assert harness.adapter.try_supersede(ticket, [VarSet(k, v) for k, v in payload.items()])
    assert isinstance(await asyncio.wait_for(original, timeout=2.0), Superseded)
    replacement = await harness.queue(
        "light", payload, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )

    await harness.drain()

    assert await replacement == "'ok'"
    assert harness.observer.calls == [
        ("ble_mesh", "blocker", {"on": "1"}),
        ("ble_mesh", "light", payload),
    ]


async def test_non_superset_rejection_retains_both_writes_in_order(harness: _Harness) -> None:
    await harness.queue("blocker", {"on": "1"})
    ticket = AdmissionTicket()
    original = await harness.queue(
        "light",
        {"on": "1", "intensity": "20"},
        write_class=WriteClass.INTERACTIVE_LATEST,
        ticket=ticket,
    )
    assert harness.adapter.try_supersede(ticket, [VarSet("on", "1"), VarSet("intensity", "30")])
    assert isinstance(await original, Superseded)
    replacement = await harness.queue(
        "light",
        {"on": "1", "intensity": "30"},
        write_class=WriteClass.INTERACTIVE_LATEST,
        ticket=ticket,
    )
    assert not harness.adapter.try_supersede(ticket, [VarSet("on", "0")])
    await harness.queue("light", {"on": "0"}, write_class=WriteClass.INTERACTIVE_LATEST)
    assert not replacement.done()

    results = await harness.drain()
    assert results[0] == "'ok'"
    assert isinstance(results[1], Superseded)
    assert results[2:] == ["'ok'", "'ok'"]
    assert [payload for _, pid, payload in harness.observer.calls if pid == "light"] == [
        {"on": "1", "intensity": "30"},
        {"on": "0"},
    ]


@pytest.mark.parametrize("mismatch", ["device", "peripheral", "payload", "class"])
async def test_ticket_adoption_requires_exact_identity(harness: _Harness, mismatch: str) -> None:
    await harness.queue("blocker", {"on": "1"})
    ticket = AdmissionTicket()
    await harness.queue(
        "light", {"on": "0"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    with pytest.raises(ValueError, match="ticket does not match"):
        await asyncio.wait_for(
            harness.adapter.set_variables(
                "other-device" if mismatch == "device" else "ble_mesh",
                "other-light" if mismatch == "peripheral" else "light",
                [VarSet("on", "1" if mismatch == "payload" else "0")],
                write_class=(
                    WriteClass.INTERACTIVE_FIFO
                    if mismatch == "class"
                    else WriteClass.INTERACTIVE_LATEST
                ),
                ticket=ticket,
            ),
            timeout=2.0,
        )
    assert await harness.drain() == ["'ok'", "'ok'"]
    assert harness.observer.calls[-1] == ("ble_mesh", "light", {"on": "0"})
    assert len(harness.observer.calls) == 2


async def test_fifo_barrier_prevents_latest_replacement_across_non_idempotent_write(
    harness: _Harness,
) -> None:
    await harness.queue("blocker", {"on": "1"})
    ticket = AdmissionTicket()
    original = await harness.queue(
        "light", {"on": "1"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    assert harness.adapter.try_supersede(ticket, [VarSet("on", "0")])
    assert isinstance(await original, Superseded)
    await harness.queue(
        "light", {"on": "0"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    await harness.queue("light", {"execute_scene": "evening"})
    assert not harness.adapter.try_supersede(ticket, [VarSet("on", "1")])
    await harness.queue("light", {"on": "1"}, write_class=WriteClass.INTERACTIVE_LATEST)
    await harness.drain()

    assert [payload for _, pid, payload in harness.observer.calls if pid == "light"] == [
        {"on": "0"},
        {"execute_scene": "evening"},
        {"on": "1"},
    ]


@pytest.mark.parametrize("age", [0.0, 31.0], ids=["normal-priority", "aged-maintenance"])
async def test_per_target_order_overrides_priority_and_aging(harness: _Harness, age: float) -> None:
    await harness.queue("blocker", {"on": "1"})
    await harness.queue("light", {"on": "0"}, write_class=WriteClass.MAINTENANCE)
    await harness.queue("light", {"on": "1"}, write_class=WriteClass.INTERACTIVE_LATEST)
    await harness.queue("first-interactive", {"on": "1"})
    await harness.release(0)
    assert [pid for _, pid, _ in harness.observer.calls] == ["blocker", "first-interactive"]
    await harness.queue("other-light", {"on": "1"})
    harness.clock.advance(age)
    await harness.drain()

    expected = (
        ["blocker", "first-interactive", "light", "light", "other-light"]
        if age >= 30.0
        else ["blocker", "first-interactive", "other-light", "light", "light"]
    )
    assert [pid for _, pid, _ in harness.observer.calls] == expected
    assert [payload for _, pid, payload in harness.observer.calls if pid == "light"] == [
        {"on": "0"},
        {"on": "1"},
    ]


@pytest.mark.parametrize("write_class", [WriteClass.INTERACTIVE_FIFO, WriteClass.MAINTENANCE])
async def test_non_latest_classes_never_accept_supersession(
    harness: _Harness, write_class: WriteClass
) -> None:
    await harness.queue("blocker", {"on": "1"})
    latest_ticket = AdmissionTicket()
    latest = await harness.queue(
        "latest", {"on": "0"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=latest_ticket
    )
    assert harness.adapter.try_supersede(latest_ticket, [VarSet("on", "1")])
    assert isinstance(await latest, Superseded)
    await harness.queue(
        "latest", {"on": "1"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=latest_ticket
    )
    ticket = AdmissionTicket()
    original = await harness.queue("light", {"on": "0"}, write_class=write_class, ticket=ticket)
    assert not harness.adapter.try_supersede(ticket, [VarSet("on", "1")])
    assert not original.done()
    await harness.drain()
    assert await original == "'ok'"
    assert harness.observer.calls[-1] == ("ble_mesh", "light", {"on": "0"})


async def test_maintenance_progresses_at_thirty_seconds(harness: _Harness) -> None:
    await harness.queue("blocker", {"on": "1"})
    await harness.queue(
        "maintenance", {"enable_motion_score": "1"}, write_class=WriteClass.MAINTENANCE
    )
    await harness.queue("interactive-before", {"on": "1"})
    await harness.queue("interactive-after", {"on": "1"})
    harness.clock.advance(29.999)
    await harness.release(0)
    assert [pid for _, pid, _ in harness.observer.calls] == ["blocker", "interactive-before"]
    harness.clock.advance(0.001)
    await harness.drain()
    assert [pid for _, pid, _ in harness.observer.calls] == [
        "blocker",
        "interactive-before",
        "maintenance",
        "interactive-after",
    ]


async def test_maintenance_progresses_after_eight_overtakes(harness: _Harness) -> None:
    await harness.queue("blocker", {"on": "1"})
    await harness.queue(
        "maintenance", {"enable_motion_score": "1"}, write_class=WriteClass.MAINTENANCE
    )
    for index in range(9):
        await harness.queue(f"interactive-{index}", {"on": "1"})
    await harness.drain()
    assert harness.clock() == 0.0
    assert [pid for _, pid, _ in harness.observer.calls] == [
        "blocker",
        *(f"interactive-{index}" for index in range(8)),
        "maintenance",
        "interactive-8",
    ]


async def test_bus_devices_progress_independently_with_one_native_call_each(
    harness: _Harness,
) -> None:
    for device in ("ble_mesh", "own-device"):
        await harness.queue("same-peripheral", {"on": "0"}, device=device)
        ticket = AdmissionTicket()
        original = await harness.queue(
            "same-peripheral",
            {"on": "0"},
            device=device,
            write_class=WriteClass.INTERACTIVE_LATEST,
            ticket=ticket,
        )
        assert harness.adapter.try_supersede(ticket, [VarSet("on", "1")])
        assert isinstance(await original, Superseded)
        await harness.queue(
            "same-peripheral",
            {"on": "1"},
            device=device,
            write_class=WriteClass.INTERACTIVE_LATEST,
            ticket=ticket,
        )
    assert harness.observer.calls == [
        ("ble_mesh", "same-peripheral", {"on": "0"}),
        ("own-device", "same-peripheral", {"on": "0"}),
    ]
    assert harness.observer.max_total == 2
    await harness.drain()
    assert len(harness.observer.calls) == 4
    assert harness.observer.max_in_flight == {"ble_mesh": 1, "own-device": 1}


async def test_queued_cancellation_tombstones_payload_without_cancelling_issued_call(
    harness: _Harness,
) -> None:
    await harness.queue("blocker", {"on": "1"})
    ticket = AdmissionTicket()
    original = await harness.queue(
        "cancelled", {"on": "0"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    assert harness.adapter.try_supersede(ticket, [VarSet("on", "1")])
    assert isinstance(await original, Superseded)
    cancelled = await harness.queue(
        "cancelled", {"on": "1"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    assert not harness.adapter.try_supersede(ticket, [VarSet("on", "1")])
    remaining = await harness.queue("remaining", {"on": "1"})
    await harness.release(0)
    assert harness.observer.calls == [
        ("ble_mesh", "blocker", {"on": "1"}),
        ("ble_mesh", "remaining", {"on": "1"}),
    ]
    assert harness.observer.cancelled == 0
    await harness.release(1)
    assert await remaining == "'ok'"


async def test_issued_cancellation_retains_device_lock_until_native_completion(
    harness: _Harness,
) -> None:
    await harness.queue("blocker", {"on": "1"})
    ticket = AdmissionTicket()
    original = await harness.queue(
        "light", {"on": "0"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    assert harness.adapter.try_supersede(ticket, [VarSet("on", "1")])
    assert isinstance(await original, Superseded)
    issued = await harness.queue(
        "light", {"on": "1"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    await harness.release(0)
    assert not harness.adapter.try_supersede(ticket, [VarSet("on", "1")])
    issued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await issued
    remaining = await harness.queue("other-light", {"on": "1"})
    assert len(harness.observer.calls) == 2
    assert harness.observer.in_flight == {"ble_mesh": 1}
    assert harness.observer.cancelled == 0
    await harness.release(1)
    assert harness.observer.calls[-1] == ("ble_mesh", "other-light", {"on": "1"})
    await harness.release(2)
    assert await remaining == "'ok'"
    assert harness.observer.max_in_flight == {"ble_mesh": 1}


async def test_repeated_supersession_settles_each_predecessor_exactly_once(
    harness: _Harness,
) -> None:
    await harness.queue("blocker", {"on": "1"})
    ticket = AdmissionTicket()
    completions: list[asyncio.Task[WriteResult]] = []
    predecessors: list[asyncio.Task[WriteResult]] = []
    for intensity in ("10", "20", "30"):
        caller = await harness.queue(
            "light",
            {"intensity": intensity},
            write_class=WriteClass.INTERACTIVE_LATEST,
            ticket=ticket,
        )
        caller.add_done_callback(completions.append)
        if intensity != "30":
            predecessors.append(caller)
            assert harness.adapter.try_supersede(
                ticket, [VarSet("intensity", str(int(intensity) + 10))]
            )
            assert isinstance(await asyncio.wait_for(caller, timeout=2.0), Superseded)
    await harness.drain()
    await _settle()
    assert len(completions) == 3
    assert len(set(completions)) == 3
    assert all(isinstance(caller.result(), Superseded) for caller in predecessors)
    assert harness.observer.calls == [
        ("ble_mesh", "blocker", {"on": "1"}),
        ("ble_mesh", "light", {"intensity": "30"}),
    ]
    assert not harness.adapter.try_supersede(ticket, [VarSet("intensity", "40")])


async def test_superseded_caller_cancellation_cannot_cancel_replacement(harness: _Harness) -> None:
    await harness.queue("blocker", {"on": "1"})
    ticket = AdmissionTicket()
    original = await harness.queue(
        "light", {"on": "0"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    assert harness.adapter.try_supersede(ticket, [VarSet("on", "1")])
    original.cancel()
    with pytest.raises(asyncio.CancelledError):
        await original
    replacement = await harness.queue(
        "light", {"on": "1"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    await harness.release(0)
    assert harness.observer.calls == [
        ("ble_mesh", "blocker", {"on": "1"}),
        ("ble_mesh", "light", {"on": "1"}),
    ]
    await harness.release(1)
    assert await replacement == "'ok'"


async def test_cancel_after_selection_before_native_step_never_issues(harness: _Harness) -> None:
    await harness.queue("blocker", {"on": "1"})
    (native_blocker,) = harness.adapter._write_tasks
    waiting = await harness.queue("cancelled", {"on": "0"})
    remaining = await harness.queue("remaining", {"on": "1"})
    # Registered after the adapter's completion callback: the waiter is selected
    # first, then cancelled synchronously before its native task can run.
    native_blocker.add_done_callback(lambda _: waiting.cancel())
    await harness.release(0)
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert harness.observer.calls == [
        ("ble_mesh", "blocker", {"on": "1"}),
        ("ble_mesh", "remaining", {"on": "1"}),
    ]
    await harness.release(1)
    assert await remaining == "'ok'"


@pytest.mark.parametrize("adopt", [False, True], ids=["unadopted", "adopted"])
async def test_teardown_immediately_after_supersede_settles_every_caller(
    harness: _Harness, adopt: bool
) -> None:
    active = await harness.queue("blocker", {"on": "1"})
    ticket = AdmissionTicket()
    original = await harness.queue(
        "light", {"on": "0"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    other = await harness.queue("other-light", {"on": "1"})
    assert harness.adapter.try_supersede(ticket, [VarSet("on", "1")])
    replacement = (
        harness.submit(
            "light", {"on": "1"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
        )
        if adopt
        else None
    )
    await asyncio.wait_for(harness.adapter.shutdown(), timeout=2.0)
    results = await asyncio.wait_for(
        asyncio.gather(*harness.callers, return_exceptions=True), timeout=2.0
    )
    assert isinstance(original.result(), Superseded)
    assert active.cancelled()
    assert other.cancelled()
    if replacement is not None:
        assert isinstance(results[-1], (asyncio.CancelledError, RuntimeError))
    assert not harness.adapter._write_tasks
    assert harness.observer.calls == [("ble_mesh", "blocker", {"on": "1"})]
    assert not harness.adapter.try_supersede(ticket, [VarSet("on", "0")])


async def test_session_rollover_preserves_native_ownership_and_rejects_stale_ticket(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bus_mod, "_WRITE_SETTLE_TIMEOUT_S", 0.01)
    harness.observer.delay_cancellation = True
    ticket = AdmissionTicket()
    old = await harness.queue(
        "light", {"on": "0"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    old_session = harness.adapter._session
    await asyncio.wait_for(harness.adapter.shutdown(), timeout=2.0)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(old, timeout=2.0)
    assert len(harness.adapter._write_tasks) == 1
    assert harness.observer.in_flight == {"ble_mesh": 1}
    harness.adapter._begin_session()
    harness.adapter._obs = harness.observer
    harness.adapter._own_device_id = "own-device"
    harness.adapter._shutting_down = False
    harness.adapter._on_proc_reconnect(old_session)
    assert not harness.adapter.try_supersede(ticket, [VarSet("on", "1")])
    with pytest.raises(ValueError, match="admission ticket"):
        await asyncio.wait_for(
            harness.adapter.set_variables(
                "ble_mesh",
                "light",
                [VarSet("on", "0")],
                write_class=WriteClass.INTERACTIVE_LATEST,
                ticket=ticket,
            ),
            timeout=2.0,
        )
    fresh = await harness.queue("light", {"on": "1"})
    assert harness.observer.calls == [("ble_mesh", "light", {"on": "0"})]
    assert not fresh.done()
    await harness.release(0)
    assert harness.observer.calls == [
        ("ble_mesh", "light", {"on": "0"}),
        ("ble_mesh", "light", {"on": "1"}),
    ]
    await harness.release(1)
    assert await fresh == "'ok'"
    assert harness.observer.max_in_flight == {"ble_mesh": 1}
    assert harness.adapter.recent_reconnects(60.0) == 0


@pytest.mark.parametrize("receipt", ["superseded", "cancelled", "error", "ok"])
async def test_literal_status_strings_remain_successful_receipts(
    harness: _Harness, receipt: str
) -> None:
    harness.observer.receipt = receipt
    await harness.queue("blocker", {"on": "1"})
    ticket = AdmissionTicket()
    original = await harness.queue(
        "light", {"on": "0"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    assert harness.adapter.try_supersede(ticket, [VarSet("on", "1")])
    assert isinstance(await original, Superseded)
    caller = await harness.queue(
        "light", {"on": "1"}, write_class=WriteClass.INTERACTIVE_LATEST, ticket=ticket
    )
    await harness.drain()
    assert await caller == repr(receipt)
    assert not isinstance(caller.result(), Superseded)

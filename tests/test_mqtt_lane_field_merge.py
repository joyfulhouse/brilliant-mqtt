"""A command folded into a lane's pending write merges per field (#159).

The queue layers already merge compatible partial setters (#170). These cover
the active-admission fold: a newer command superseding a write that is admitted
but not yet issued to the device. Deterministic fakes only; no live devices.

Cases that discriminate the #159 fix (they fail without it): partial-after-full,
same-field-latest-wins and state-only-after-merge. Contract-required regression
guards that also pass on the pre-fix base, so a mutation run must not flag them
as dead: full-after-partial, test_pending_off_is_not_merged_with_a_partial and
test_fold_does_not_merge_across_lanes.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.mqttio import _InboundMessage, _TopicDispatcher
from tests.fakes import FakeMqtt, _settle
from tests.test_bus_adapter import _adapter_for, _SchedulingObserver
from tests.test_interactive_write_scheduling_fixes import _dispatch_handler, _light

TOPIC = "brilliant/test/slider/set"
OTHER_TOPIC = "brilliant/test/other/set"


async def _native_writes(
    commands: list[tuple[str, dict[str, object]]],
    *,
    issue_first: bool = False,
    back_to_back: bool = False,
) -> list[tuple[str, dict[str, str]]]:
    """Dispatch *commands* while the shared device is busy; return native writes.

    With *issue_first* the first command is issued to the device (in flight)
    before the rest arrive, so nothing may fold into it. With *back_to_back*
    the later commands arrive without the lane worker running in between.
    """
    observer, adapter = _adapter_for(_SchedulingObserver())
    bridge = Bridge(adapter, FakeMqtt(), "test")
    for topic, peripheral in ((TOPIC, "slider"), (OTHER_TOPIC, "other")):
        bridge._devices[peripheral] = replace(_light("255"), peripheral_id=peripheral)
        bridge._by_cmd_topic[topic] = (peripheral, None)
    dispatcher = _TopicDispatcher(lambda message: _dispatch_handler(bridge, message))
    blocker = None
    if not issue_first:
        blocker = asyncio.create_task(
            adapter.set_variables("shared", "blocker", [VarSet("on", "1")])
        )
    try:
        await _settle(10)
        for index, (topic, payload) in enumerate(commands):
            await dispatcher.dispatch(
                _InboundMessage(topic, json.dumps(payload), False, (), ()), latest_wins=True
            )
            if index == 0 or not back_to_back:
                await _settle(10)
            if issue_first and index == 0:
                assert observer.in_flight == 1
        observer.release.set()
        if blocker is not None:
            await blocker
        await dispatcher.shutdown()
        assert observer.max_in_flight == 1
        assert not adapter._write_admissions
        return [(pid, values) for _, pid, values in observer.payloads if pid != "blocker"]
    finally:
        observer.release.set()
        await dispatcher.shutdown()
        await adapter.shutdown()
        if blocker is not None:
            await asyncio.gather(blocker, return_exceptions=True)


@pytest.mark.parametrize(
    ("commands", "expected"),
    [
        pytest.param(
            [{"state": "ON", "brightness": 40}, {"brightness": 60}],
            {"on": "1", "intensity": "60"},
            id="partial-after-full",
        ),
        pytest.param(
            [{"brightness": 40}, {"state": "ON", "brightness": 60}],
            {"on": "1", "intensity": "60"},
            id="full-after-partial",
        ),
        pytest.param(
            [{"state": "ON", "brightness": 40}, {"brightness": 60}, {"brightness": 80}],
            {"on": "1", "intensity": "80"},
            id="same-field-latest-wins",
        ),
        # The only case that pins rebinding the lane's active write to the
        # merged payload; do not delete it as redundant.
        pytest.param(
            [{"state": "ON", "brightness": 40}, {"brightness": 60}, {"state": "ON"}],
            {"on": "1", "intensity": "60"},
            id="state-only-after-merge",
        ),
    ],
)
@pytest.mark.parametrize("back_to_back", [False, True], ids=["worker-between", "back-to-back"])
async def test_fold_into_pending_write_merges_fields(
    commands: list[dict[str, object]], expected: dict[str, str], back_to_back: bool
) -> None:
    writes = await _native_writes(
        [(TOPIC, payload) for payload in commands], back_to_back=back_to_back
    )
    assert writes == [("slider", expected)]


async def test_pending_off_is_not_merged_with_a_partial() -> None:
    writes = await _native_writes([(TOPIC, {"state": "OFF"}), (TOPIC, {"brightness": 60})])
    assert writes == [("slider", {"on": "0"}), ("slider", {"intensity": "60"})]


async def test_fold_does_not_merge_across_lanes() -> None:
    writes = await _native_writes(
        [
            (TOPIC, {"state": "ON", "brightness": 40}),
            (OTHER_TOPIC, {"brightness": 60}),
        ]
    )
    assert writes == [
        ("slider", {"on": "1", "intensity": "40"}),
        ("other", {"intensity": "60"}),
    ]


async def test_no_merge_into_write_already_issued_to_device() -> None:
    writes = await _native_writes(
        [
            (TOPIC, {"state": "ON", "brightness": 40}),
            (TOPIC, {"brightness": 60}),
            (TOPIC, {"brightness": 80}),
        ],
        issue_first=True,
    )
    # The in-flight write is untouched; later partials merge only with each
    # other, never with it.
    assert writes == [
        ("slider", {"on": "1", "intensity": "40"}),
        ("slider", {"intensity": "80"}),
    ]

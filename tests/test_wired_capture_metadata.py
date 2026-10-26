"""Capture facts used by the wired feedback reducer."""

from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from tests.fakes import FakeBus


def _light() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="owner",
        peripheral_id="load",
        name="Synthetic load",
        kind=DeviceKind.LIGHT,
        variables={"on": Variable("on", "0"), "intensity": Variable("intensity", "333")},
    )


async def test_full_read_records_complete_and_raw_presence() -> None:
    bus = FakeBus([_light()])
    captured = (await bus.get_all())[0]

    assert captured.capture_complete
    assert captured.capture_present == frozenset({"on", "intensity"})
    assert captured.capture_unknown == frozenset()
    assert captured.capture_provenance is not None
    assert dict(captured.capture_provenance.field_eras) == {"on": 0, "intensity": 0}


async def test_push_is_partial_and_missing_field_has_no_era() -> None:
    bus = FakeBus([_light()])
    received: list[BrilliantDevice] = []

    async def on_change(device: BrilliantDevice) -> None:
        received.append(device)

    bus.on_change(on_change)
    partial = _light()
    partial.variables.pop("on")
    await bus.emit(partial)

    assert not received[0].capture_complete
    assert received[0].capture_present == frozenset({"intensity"})
    assert received[0].capture_provenance is not None
    assert "on" not in dict(received[0].capture_provenance.field_eras)


async def test_capture_stamps_last_actual_issue_per_field() -> None:
    bus = FakeBus([_light()])
    before = (await bus.get_all())[0]
    await bus.set_variables("owner", "load", [VarSet("on", "1")])
    after = (await bus.get_all())[0]

    assert before.capture_provenance is not None
    assert after.capture_provenance is not None
    assert dict(before.capture_provenance.field_eras)["on"] == 0
    assert dict(after.capture_provenance.field_eras)["on"] == 1
    assert dict(after.capture_provenance.field_eras)["intensity"] == 0

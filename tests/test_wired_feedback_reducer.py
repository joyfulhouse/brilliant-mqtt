"""Pure transition tests for wired primary feedback."""

from dataclasses import replace
from random import Random

import pytest

from brilliant_mqtt.model import BrilliantDevice, CaptureProvenance, DeviceKind, Variable
from brilliant_mqtt.wired_feedback import (
    Begin,
    Capture,
    Expire,
    Issue,
    Outcome,
    PublishAccepted,
    Record,
    Shutdown,
    SourceGeneration,
    reduce,
    render,
    timer_deadline,
)


def _device(
    *,
    on: str | None = "0",
    intensity: str | None = "333",
    era: int = 0,
    complete: bool = True,
    source: int = 1,
) -> BrilliantDevice:
    variables = {}
    if on is not None:
        variables["on"] = Variable("on", on, timestamp_ms=1000)
    if intensity is not None:
        variables["intensity"] = Variable("intensity", intensity, timestamp_ms=1000)
    return BrilliantDevice(
        "owner",
        "load",
        "Synthetic load",
        DeviceKind.LIGHT,
        variables=variables,
        capture_provenance=CaptureProvenance(source, 1, tuple((name, era) for name in variables)),
        capture_complete=complete,
        capture_present=frozenset(variables),
    )


def _success(record: Record, *, target: str = "1", now: float = 0.0) -> Record:
    record = reduce(record, Begin((("on", target),))).record
    generation = record.generation
    record = reduce(record, Issue(generation, (("on", 1),))).record
    return reduce(record, Outcome(generation, "success", now, now)).record


def _render(record: Record) -> dict[str, object]:
    result = render(record)
    assert result is not None
    return result.fields


@pytest.mark.parametrize(
    ("capture", "expected_state", "expected_status"),
    [
        (_device(on="0", era=0, complete=False), "ON", "provisional"),
        (_device(on="0", era=1, complete=False), "OFF", "ambiguous"),
        (_device(on="1", era=1, complete=False), "ON", "observed"),
        (_device(on=None, era=0, complete=False), "ON", "provisional"),
    ],
)
def test_reducer_classifies_field_era_without_timestamp_ordering(
    capture: BrilliantDevice, expected_state: str, expected_status: str
) -> None:
    record = reduce(Record(), Capture(_device())).record
    record = _success(record)
    record = reduce(record, Capture(capture)).record
    payload = render(record)
    assert payload is not None
    assert payload.fields["state"] == expected_state
    assert payload.fields["wired_write_status"] == expected_status


def test_complete_missing_on_is_unknown_but_partial_missing_on_is_no_information() -> None:
    record = reduce(Record(), Capture(_device())).record
    partial = _device(on=None, complete=False)
    record = reduce(record, Capture(partial)).record
    assert _render(record)["state"] == "OFF"

    complete = replace(partial, capture_complete=True)
    record = reduce(record, Capture(complete)).record
    assert _render(record)["state"] is None


def test_success_window_is_inherited_and_native_debt_needs_acceptance() -> None:
    record = reduce(Record(), Capture(_device())).record
    record = _success(record)
    assert timer_deadline(record) == 20.0
    record = _success(record, now=7.0)
    assert timer_deadline(record) == 20.0
    record = reduce(record, Expire(20.0)).record
    rendered = render(record)
    assert rendered is not None and rendered.native
    assert record.native_debt
    record = reduce(record, Begin((("on", "1"),))).record
    assert record.native_debt
    assert timer_deadline(record) is None
    record = reduce(record, PublishAccepted(record.revision, native=True)).record
    assert not record.native_debt
    record = reduce(record, Issue(record.generation, (("on", 3),))).record
    record = reduce(record, Outcome(record.generation, "success", 24.0, 24.0)).record
    assert timer_deadline(record) == 44.0


def test_reconnect_generation_fences_old_capture_without_pending_request() -> None:
    record = reduce(Record(), Capture(_device(on="1"))).record
    record = reduce(record, SourceGeneration(2)).record
    assert reduce(record, Capture(_device(on="0", source=1))).record == record
    record = reduce(record, Capture(_device(on="0", source=2))).record
    assert _render(record)["state"] == "OFF"


def test_shutdown_rejects_future_commands_and_timers() -> None:
    record = reduce(Record(), Capture(_device())).record
    record = reduce(record, Shutdown()).record
    record = _success(record)
    assert record.closed
    assert timer_deadline(record) is None
    assert render(record) is None


@pytest.mark.parametrize("seed", range(20))
def test_random_field_history_matches_independent_era_oracle(seed: int) -> None:
    rng = Random(seed)
    record = _success(reduce(Record(), Capture(_device())).record)
    native_value = "0"
    native_era = 0
    projected = True
    status = "provisional"

    for sequence in range(100):
        incoming_era = rng.randrange(2)
        incoming_value = rng.choice(("0", "1", None))
        capture = _device(on=incoming_value, era=incoming_era, complete=False)
        assert capture.capture_provenance is not None
        capture.capture_provenance = replace(capture.capture_provenance, sequence=sequence)
        record = reduce(record, Capture(capture)).record

        if incoming_value is not None and incoming_era >= native_era:
            native_value = incoming_value
            native_era = incoming_era
            if incoming_era == 1:
                projected = False
                status = "observed" if incoming_value == "1" else "ambiguous"
        fields = _render(record)
        assert fields["state"] == ("ON" if projected or native_value == "1" else "OFF")
        assert fields["wired_write_status"] == status
        assert record.native is not None
        assert record.native.variables["on"].value == native_value
        assert timer_deadline(record) == 20.0


def test_raw_none_on_is_unknown_and_never_invented_off() -> None:
    record = reduce(Record(), Capture(_device(on="1"))).record
    unknown = _device(on=None, era=0, complete=False)
    unknown.capture_present = frozenset({"on", "intensity"})
    unknown.capture_unknown = frozenset({"on"})
    record = reduce(record, Capture(unknown)).record
    assert _render(record)["state"] is None


@pytest.mark.parametrize("name", ["intensity", "max_intensity_value"])
def test_raw_none_capability_is_unknown_without_rebinding(name: str) -> None:
    initial = _device(on="1")
    initial.variables["max_intensity_value"] = Variable("max_intensity_value", "2000")
    initial.capture_present = frozenset(initial.variables)
    record = reduce(Record(), Capture(initial)).record
    unknown = _device(on="1", complete=False)
    unknown.variables.pop("intensity")
    unknown.capture_present = frozenset({"on", name})
    unknown.capture_unknown = frozenset({name})
    before_generation = record.generation

    record = reduce(record, Capture(unknown)).record

    assert record.native is not None and record.native.is_dimmable
    assert record.native.max_intensity == 2000
    assert record.generation == before_generation
    assert "brightness" not in _render(record)


def test_older_write_era_cannot_erase_newer_native_during_second_command() -> None:
    record = reduce(Record(), Capture(_device())).record
    record = _success(record)
    record = reduce(record, Capture(_device(on="1", era=1, complete=False))).record
    record = reduce(record, Begin((("intensity", "500"),))).record
    record = reduce(record, Issue(record.generation, (("intensity", 1),))).record
    late = _device(on="0", era=0, complete=False)
    record = reduce(record, Capture(late)).record
    assert record.native is not None
    assert record.native.variables["on"].value == "1"


def test_full_read_revokes_dimming_and_partial_push_does_not() -> None:
    record = reduce(Record(), Capture(_device())).record
    missing = _device(intensity=None, complete=False)
    record = reduce(record, Capture(missing)).record
    assert record.native is not None and record.native.is_dimmable
    record = reduce(record, Capture(replace(missing, capture_complete=True))).record
    assert record.native is not None and not record.native.is_dimmable


def test_delayed_success_starts_its_window_when_success_arrives() -> None:
    record = reduce(Record(), Capture(_device())).record
    record = reduce(record, Begin((("on", "1"),))).record
    record = reduce(record, Issue(record.generation, (("on", 1),))).record
    assert timer_deadline(record) is None
    record = reduce(record, Outcome(record.generation, "success", 37.0, 1037.0)).record
    assert timer_deadline(record) == 57.0
    assert _render(record)["wired_write_deadline"] == 1057.0


def test_success_during_native_debt_waits_for_acceptance_without_renewing_old_window() -> None:
    record = _success(reduce(Record(), Capture(_device())).record)
    record = reduce(record, Expire(20.0)).record
    old_revision = record.revision
    debt_epoch = record.native_debt_epoch
    record = reduce(record, Begin((("on", "1"),))).record
    record = reduce(record, Issue(record.generation, (("on", 2),))).record
    record = reduce(record, Outcome(record.generation, "success", 23.0, 23.0)).record

    assert record.native_debt
    assert _render(record)["state"] == "OFF"
    assert timer_deadline(record) == 43.0
    record = reduce(
        record, PublishAccepted(old_revision, native=True, debt_epoch=debt_epoch)
    ).record
    assert not record.native_debt
    assert _render(record)["state"] == "ON"
    assert timer_deadline(record) == 43.0


def test_stale_native_acceptance_cannot_clear_newer_debt_epoch() -> None:
    record = _success(reduce(Record(), Capture(_device())).record)
    record = reduce(record, Expire(20.0)).record
    old_revision = record.revision
    old_epoch = record.native_debt_epoch
    record = reduce(record, SourceGeneration(2)).record
    assert record.native_debt_epoch > old_epoch

    record = reduce(record, PublishAccepted(old_revision, native=True, debt_epoch=old_epoch)).record
    assert record.native_debt
    record = reduce(
        record, PublishAccepted(record.revision, native=True, debt_epoch=record.native_debt_epoch)
    ).record
    assert not record.native_debt


def test_identical_matching_captures_do_not_renew_native_debt() -> None:
    record = _success(reduce(Record(), Capture(_device())).record)
    matching = _device(on="1", era=1, complete=False)
    record = reduce(record, Capture(matching)).record
    debt_epoch = record.native_debt_epoch
    assert record.native_debt

    record = reduce(record, Capture(matching)).record
    assert record.native_debt_epoch == debt_epoch
    record = reduce(
        record, PublishAccepted(record.revision, native=True, debt_epoch=debt_epoch)
    ).record
    assert not record.native_debt


@pytest.mark.parametrize("outcome", ["failed", "cancelled"])
def test_failed_or_cancelled_replacement_demands_native_acceptance(outcome: str) -> None:
    record = _success(reduce(Record(), Capture(_device())).record)
    record = reduce(record, Begin((("on", "0"),))).record
    record = reduce(record, Outcome(record.generation, outcome, 3.0, 3.0)).record
    assert record.native_debt
    assert _render(record)["state"] == "OFF"
    assert _render(record)["wired_write_status"] == "unconfirmed"
    record = reduce(record, PublishAccepted(record.revision, native=False)).record
    assert record.native_debt
    record = reduce(record, PublishAccepted(record.revision, native=True)).record
    assert not record.native_debt

"""Tests for the pure ``normalize_peripheral`` function (Milestone 7).

Only the PURE translation function is unit-tested here. The RpcBusAdapter
network paths, aiomqtt adapter, and entrypoint are validated on the on-panel /
broker pilot, NOT mocked.

Raw bus objects are duck-typed with ``types.SimpleNamespace`` — exactly the
attribute surface the real ttypes expose (``name``, ``peripheral_type``,
``variables`` mapping name → object with ``.value`` / ``.externally_settable``).

Importing ``brilliant_mqtt.bus`` in this file at all (the import below) proves
the deferred-import guarantee: the module loads on a machine with no panel libs.
"""

from __future__ import annotations

from types import SimpleNamespace

from brilliant_mqtt.bus import normalize_peripheral
from brilliant_mqtt.model import BrilliantDevice, DeviceKind

_MISSING = object()


def _var(
    value: object,
    externally_settable: object,
    timestamp: object = _MISSING,
) -> SimpleNamespace:
    """Build a duck-typed raw Variable.

    ``externally_settable`` is typed ``object`` because the real bus may hand any
    truthy/falsy value; ``normalize_peripheral`` coerces it with ``bool(...)``.
    """
    raw = SimpleNamespace(value=value, externally_settable=externally_settable)
    if timestamp is not _MISSING:
        raw.timestamp = timestamp
    return raw


def _light_peripheral() -> SimpleNamespace:
    """The PoC's ``gangbox_peripheral_0`` dimmer (subset of its 32 variables)."""
    return SimpleNamespace(
        name="gangbox_peripheral_0",
        peripheral_type=27,  # LIGHT
        variables={
            "display_name": _var("Lights", True),
            "on": _var("0", True),
            "intensity": _var("600", True),
            "max_intensity_value": _var("1000", False),
        },
    )


class TestNormalizeLight:
    def test_returns_brilliant_device(self) -> None:
        device = normalize_peripheral("dev123", "gangbox_peripheral_0", _light_peripheral())
        assert isinstance(device, BrilliantDevice)

    def test_kind_is_light(self) -> None:
        device = normalize_peripheral("dev123", "gangbox_peripheral_0", _light_peripheral())
        assert device.kind is DeviceKind.LIGHT

    def test_peripheral_type_plumbed(self) -> None:
        device = normalize_peripheral("dev123", "gangbox_peripheral_0", _light_peripheral())
        assert device.peripheral_type == 27

    def test_name_from_display_name(self) -> None:
        device = normalize_peripheral("dev123", "gangbox_peripheral_0", _light_peripheral())
        assert device.name == "Lights"

    def test_ids_threaded_through(self) -> None:
        device = normalize_peripheral("dev123", "gangbox_peripheral_0", _light_peripheral())
        assert device.device_id == "dev123"
        assert device.peripheral_id == "gangbox_peripheral_0"

    def test_variables_present(self) -> None:
        device = normalize_peripheral("dev123", "gangbox_peripheral_0", _light_peripheral())
        # display_name is a real bus variable and is preserved (no filtering by name).
        assert set(device.variables) == {"display_name", "on", "intensity", "max_intensity_value"}

    def test_settable_flags_preserved(self) -> None:
        device = normalize_peripheral("dev123", "gangbox_peripheral_0", _light_peripheral())
        assert device.variables["on"].externally_settable is True
        assert device.variables["intensity"].externally_settable is True
        assert device.variables["max_intensity_value"].externally_settable is False

    def test_values_are_strings(self) -> None:
        device = normalize_peripheral("dev123", "gangbox_peripheral_0", _light_peripheral())
        assert device.variables["intensity"].value == "600"
        assert isinstance(device.variables["intensity"].value, str)

    def test_derived_properties_work(self) -> None:
        device = normalize_peripheral("dev123", "gangbox_peripheral_0", _light_peripheral())
        # Sanity that the normalized model drives the downstream brightness math.
        assert device.is_dimmable is True
        assert device.intensity == 600
        assert device.max_intensity == 1000
        assert device.is_on is False


class TestNameFallback:
    def test_missing_display_name_falls_back_to_raw_name(self) -> None:
        raw = SimpleNamespace(
            name="gangbox_peripheral_0",
            peripheral_type=27,
            variables={"on": _var("1", True)},
        )
        device = normalize_peripheral("dev123", "gangbox_peripheral_0", raw)
        assert device.name == "gangbox_peripheral_0"

    def test_empty_display_name_falls_back_to_raw_name(self) -> None:
        raw = SimpleNamespace(
            name="gangbox_peripheral_0",
            peripheral_type=27,
            variables={"display_name": _var("", True)},
        )
        device = normalize_peripheral("dev123", "gangbox_peripheral_0", raw)
        assert device.name == "gangbox_peripheral_0"

    def test_empty_raw_name_falls_back_to_peripheral_id(self) -> None:
        raw = SimpleNamespace(
            name="",
            peripheral_type=27,
            variables={},
        )
        device = normalize_peripheral("dev123", "gangbox_peripheral_7", raw)
        assert device.name == "gangbox_peripheral_7"

    def test_none_raw_name_falls_back_to_peripheral_id(self) -> None:
        raw = SimpleNamespace(
            name=None,
            peripheral_type=27,
            variables={},
        )
        device = normalize_peripheral("dev123", "gangbox_peripheral_7", raw)
        assert device.name == "gangbox_peripheral_7"

    def test_missing_display_name_and_empty_raw_name_falls_back_to_peripheral_id(self) -> None:
        raw = SimpleNamespace(
            name="",
            peripheral_type=27,
            variables={"on": _var("1", True)},
        )
        device = normalize_peripheral("dev123", "gangbox_peripheral_3", raw)
        assert device.name == "gangbox_peripheral_3"


class TestVariableCoercion:
    def test_integer_timestamp_is_preserved(self) -> None:
        raw = SimpleNamespace(
            name="p",
            peripheral_type=27,
            variables={"on": _var("1", True, timestamp=1_721_234_567_890)},
        )
        device = normalize_peripheral("dev123", "p", raw)
        assert device.variables["on"].timestamp_ms == 1_721_234_567_890

    def test_missing_timestamp_becomes_none(self) -> None:
        raw = SimpleNamespace(
            name="p",
            peripheral_type=27,
            variables={"on": _var("1", True)},
        )
        device = normalize_peripheral("dev123", "p", raw)
        assert device.variables["on"].timestamp_ms is None

    def test_invalid_timestamp_becomes_none(self) -> None:
        raw = SimpleNamespace(
            name="p",
            peripheral_type=27,
            variables={"on": _var("1", True, timestamp="not-a-number")},
        )
        device = normalize_peripheral("dev123", "p", raw)
        assert device.variables["on"].timestamp_ms is None

    def test_none_value_is_skipped(self) -> None:
        raw = SimpleNamespace(
            name="p",
            peripheral_type=27,
            variables={
                "on": _var("1", True),
                "blob": _var(None, False),  # complex/absent value → skip entirely
            },
        )
        device = normalize_peripheral("dev123", "p", raw)
        assert "blob" not in device.variables
        assert "on" in device.variables

    def test_non_string_value_is_coerced_to_str(self) -> None:
        raw = SimpleNamespace(
            name="p",
            peripheral_type=27,
            variables={"count": _var(5, False)},  # int value → "5"
        )
        device = normalize_peripheral("dev123", "p", raw)
        assert device.variables["count"].value == "5"
        assert isinstance(device.variables["count"].value, str)

    def test_bytes_value_is_decoded_not_repred(self) -> None:
        raw = SimpleNamespace(
            name="p",
            peripheral_type=27,
            variables={"display_name": _var(b"Lights", True)},
        )
        device = normalize_peripheral("dev123", "p", raw)
        # utf-8 decode, NOT str(bytes) — which would yield "b'Lights'".
        assert device.variables["display_name"].value == "Lights"

    def test_externally_settable_coerced_to_bool(self) -> None:
        raw = SimpleNamespace(
            name="p",
            peripheral_type=27,
            variables={"on": _var("1", 1)},  # truthy non-bool → True
        )
        device = normalize_peripheral("dev123", "p", raw)
        assert device.variables["on"].externally_settable is True


class TestKindFromPeripheralType:
    def test_always_on_46_is_always_on(self) -> None:
        raw = SimpleNamespace(
            name="gangbox_peripheral_1",
            peripheral_type=46,  # ALWAYS_ON → power monitoring, no light/switch (M10)
            variables={"power": _var("52", False)},
        )
        device = normalize_peripheral("dev123", "gangbox_peripheral_1", raw)
        assert device.kind is DeviceKind.ALWAYS_ON
        assert device.peripheral_type == 46

"""Tests for the normalized device model (Milestone 3).

Written before the implementation — these must fail initially (ImportError / assertion
errors) and turn green once model.py exists.
"""

import dataclasses

import pytest

from brilliant_mqtt.model import (
    BrilliantDevice,
    DeviceKind,
    Variable,
    kind_for_peripheral_type,
)

# ---------------------------------------------------------------------------
# kind_for_peripheral_type
# ---------------------------------------------------------------------------


class TestKindForPeripheralType:
    def test_light(self) -> None:
        assert kind_for_peripheral_type(27) is DeviceKind.LIGHT

    def test_generic_on_off(self) -> None:
        assert kind_for_peripheral_type(45) is DeviceKind.SWITCH

    def test_outlet(self) -> None:
        assert kind_for_peripheral_type(40) is DeviceKind.SWITCH

    def test_motion_sensor(self) -> None:
        assert kind_for_peripheral_type(5) is DeviceKind.BINARY_SENSOR

    def test_climate_sensor(self) -> None:
        assert kind_for_peripheral_type(80) is DeviceKind.SENSOR

    def test_always_on(self) -> None:
        # ALWAYS_ON (46) is its own kind: power monitoring, no light/switch (M10).
        assert kind_for_peripheral_type(46) is DeviceKind.ALWAYS_ON

    def test_hardware(self) -> None:
        assert kind_for_peripheral_type(22) is DeviceKind.HARDWARE

    def test_ui(self) -> None:
        assert kind_for_peripheral_type(12) is DeviceKind.UI

    def test_wifi(self) -> None:
        assert kind_for_peripheral_type(29) is DeviceKind.WIFI

    def test_large_unknown(self) -> None:
        assert kind_for_peripheral_type(9999) is DeviceKind.UNKNOWN


# ---------------------------------------------------------------------------
# Variable parsing
# ---------------------------------------------------------------------------


class TestVariableParsing:
    def test_as_bool_true(self) -> None:
        assert Variable("on", "1").as_bool() is True

    def test_as_bool_false(self) -> None:
        assert Variable("on", "0").as_bool() is False

    def test_as_bool_non_one_is_false(self) -> None:
        assert Variable("on", "2").as_bool() is False

    def test_as_int_valid(self) -> None:
        assert Variable("intensity", "600").as_int() == 600

    def test_as_int_garbage(self) -> None:
        assert Variable("intensity", "garbage").as_int() is None

    def test_as_float_valid(self) -> None:
        result = Variable("lux", "43.60").as_float()
        assert result is not None
        assert abs(result - 43.6) < 1e-9

    def test_as_float_garbage(self) -> None:
        assert Variable("lux", "not-a-float").as_float() is None

    def test_externally_settable_defaults_false(self) -> None:
        v = Variable("on", "1")
        assert v.externally_settable is False

    def test_externally_settable_can_be_set(self) -> None:
        v = Variable("on", "1", externally_settable=True)
        assert v.externally_settable is True

    def test_variable_is_immutable(self) -> None:
        # setattr via a variable name: a literal `v.value = "0"` is (correctly)
        # rejected statically because Variable is frozen; this verifies the
        # equivalent runtime guard.
        v = Variable("on", "1")
        field_name = "value"
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(v, field_name, "0")


# ---------------------------------------------------------------------------
# BrilliantDevice — dimmer (LIGHT) fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def dimmer() -> BrilliantDevice:
    """Mirrors the real panel observations from the PoC."""
    return BrilliantDevice(
        device_id="device_001",
        peripheral_id="gangbox_peripheral_0",
        name="Lights",
        kind=DeviceKind.LIGHT,
        variables={
            "on": Variable("on", "0"),
            "intensity": Variable("intensity", "600"),
            "max_intensity_value": Variable("max_intensity_value", "1000"),
            "dimmable": Variable("dimmable", "1"),
            "display_name": Variable("display_name", "Lights"),
        },
    )


class TestDimmerDevice:
    def test_is_on_false(self, dimmer: BrilliantDevice) -> None:
        assert dimmer.is_on is False

    def test_is_dimmable_true(self, dimmer: BrilliantDevice) -> None:
        assert dimmer.is_dimmable is True

    def test_intensity(self, dimmer: BrilliantDevice) -> None:
        assert dimmer.intensity == 600

    def test_max_intensity(self, dimmer: BrilliantDevice) -> None:
        assert dimmer.max_intensity == 1000

    def test_motion_detected_none(self, dimmer: BrilliantDevice) -> None:
        assert dimmer.motion_detected is None

    def test_lux_none(self, dimmer: BrilliantDevice) -> None:
        assert dimmer.lux is None


# ---------------------------------------------------------------------------
# BrilliantDevice — switch fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def switch() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="device_002",
        peripheral_id="gangbox_peripheral_1",
        name="Fan",
        kind=DeviceKind.SWITCH,
        variables={
            "on": Variable("on", "1"),
        },
    )


class TestSwitchDevice:
    def test_is_on_true(self, switch: BrilliantDevice) -> None:
        assert switch.is_on is True

    def test_is_dimmable_false(self, switch: BrilliantDevice) -> None:
        assert switch.is_dimmable is False

    def test_intensity_none(self, switch: BrilliantDevice) -> None:
        assert switch.intensity is None


# ---------------------------------------------------------------------------
# max_intensity default
# ---------------------------------------------------------------------------


class TestMaxIntensityDefault:
    def test_missing_max_intensity_defaults_to_1000(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Light",
            kind=DeviceKind.LIGHT,
            variables={},
        )
        assert device.max_intensity == 1000

    def test_unparseable_max_intensity_defaults_to_1000(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Light",
            kind=DeviceKind.LIGHT,
            variables={"max_intensity_value": Variable("max_intensity_value", "bad")},
        )
        assert device.max_intensity == 1000

    def test_zero_max_intensity_defaults_to_1000(self) -> None:
        # Downstream milestones divide by max_intensity; non-positive values must fall back.
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Light",
            kind=DeviceKind.LIGHT,
            variables={"max_intensity_value": Variable("max_intensity_value", "0")},
        )
        assert device.max_intensity == 1000


# ---------------------------------------------------------------------------
# BrilliantDevice — motion/lux (BINARY_SENSOR) fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def motion_sensor() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="device_003",
        peripheral_id="motion_peripheral_0",
        name="Motion",
        kind=DeviceKind.BINARY_SENSOR,
        variables={
            "movement_detected": Variable("movement_detected", "1"),
            "lux": Variable("lux", "12.5"),
        },
    )


class TestMotionSensorDevice:
    def test_motion_detected_true(self, motion_sensor: BrilliantDevice) -> None:
        assert motion_sensor.motion_detected is True

    def test_lux(self, motion_sensor: BrilliantDevice) -> None:
        assert motion_sensor.lux is not None
        assert abs(motion_sensor.lux - 12.5) < 1e-9

    def test_is_on_false_no_on_variable(self, motion_sensor: BrilliantDevice) -> None:
        assert motion_sensor.is_on is False


# ---------------------------------------------------------------------------
# Device without motion/lux variables
# ---------------------------------------------------------------------------


class TestDeviceWithoutMotionLux:
    def test_motion_detected_none_when_absent(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Sensor",
            kind=DeviceKind.SENSOR,
            variables={},
        )
        assert device.motion_detected is None

    def test_lux_none_when_absent(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Sensor",
            kind=DeviceKind.SENSOR,
            variables={},
        )
        assert device.lux is None


# ---------------------------------------------------------------------------
# Empty-variables device
# ---------------------------------------------------------------------------


class TestEmptyVariablesDevice:
    def test_is_on_false(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Unknown",
            kind=DeviceKind.UNKNOWN,
            variables={},
        )
        assert device.is_on is False

    def test_motion_detected_none(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Unknown",
            kind=DeviceKind.UNKNOWN,
            variables={},
        )
        assert device.motion_detected is None


# ---------------------------------------------------------------------------
# M10 — peripheral_type field
# ---------------------------------------------------------------------------
#
# power / temperature / is_safe are NOT model properties: the AUX_SPECS table in
# mapping.py reads device.variables generically (tested in test_mapping.py).


class TestPeripheralTypeField:
    def test_defaults_to_zero(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Light",
            kind=DeviceKind.LIGHT,
            variables={},
        )
        assert device.peripheral_type == 0

    def test_can_be_set(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Light",
            kind=DeviceKind.LIGHT,
            peripheral_type=27,
            variables={},
        )
        assert device.peripheral_type == 27

"""Tests for HA command -> bus variable-set translation (Milestone 5).

Written before the implementation (TDD). Tests must fail initially with
ImportError and pass once commands.py is implemented.
"""

import pytest

from brilliant_mqtt.commands import VarSet, translate_aux, translate_command
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def dimmer() -> BrilliantDevice:
    """Dimmer (LIGHT) with intensity=600 on a 0..1000 scale."""
    return BrilliantDevice(
        device_id="device_001",
        peripheral_id="gangbox_peripheral_0",
        name="Lights",
        kind=DeviceKind.LIGHT,
        variables={
            "on": Variable("on", "0"),
            "intensity": Variable("intensity", "600"),
            "max_intensity_value": Variable("max_intensity_value", "1000"),
        },
    )


@pytest.fixture()
def switch() -> BrilliantDevice:
    """Non-dimmable switch (SWITCH) that is currently on."""
    return BrilliantDevice(
        device_id="device_002",
        peripheral_id="gangbox_peripheral_1",
        name="Fan",
        kind=DeviceKind.SWITCH,
        variables={
            "on": Variable("on", "1"),
        },
    )


@pytest.fixture()
def motion() -> BrilliantDevice:
    """Motion sensor (BINARY_SENSOR) — not controllable."""
    return BrilliantDevice(
        device_id="device_003",
        peripheral_id="motion_peripheral_0",
        name="Motion",
        kind=DeviceKind.BINARY_SENSOR,
        variables={
            "movement_detected": Variable("movement_detected", "0"),
        },
    )


@pytest.fixture()
def non_dimmable_light() -> BrilliantDevice:
    """Light without an intensity variable — is_dimmable is False."""
    return BrilliantDevice(
        device_id="device_004",
        peripheral_id="gangbox_peripheral_2",
        name="Ceiling",
        kind=DeviceKind.LIGHT,
        variables={
            "on": Variable("on", "0"),
        },
    )


@pytest.fixture()
def dimmer_100() -> BrilliantDevice:
    """Dimmer with a custom max_intensity_value of 100."""
    return BrilliantDevice(
        device_id="device_005",
        peripheral_id="gangbox_peripheral_3",
        name="Accent",
        kind=DeviceKind.LIGHT,
        variables={
            "on": Variable("on", "0"),
            "intensity": Variable("intensity", "50"),
            "max_intensity_value": Variable("max_intensity_value", "100"),
        },
    )


# ---------------------------------------------------------------------------
# Basic ON / OFF — dimmer
# ---------------------------------------------------------------------------


class TestOnOffDimmer:
    def test_state_on(self, dimmer: BrilliantDevice) -> None:
        result = translate_command(dimmer, {"state": "ON"})
        assert result == [VarSet("on", "1")]

    def test_state_off(self, dimmer: BrilliantDevice) -> None:
        result = translate_command(dimmer, {"state": "OFF"})
        assert result == [VarSet("on", "0")]

    def test_off_ignores_brightness(self, dimmer: BrilliantDevice) -> None:
        """Brightness in an OFF command must be ignored — OFF wins."""
        result = translate_command(dimmer, {"state": "OFF", "brightness": 200})
        assert result == [VarSet("on", "0")]


# ---------------------------------------------------------------------------
# Brightness + state — dimmer
# ---------------------------------------------------------------------------


class TestBrightnessDimmer:
    def test_on_full_brightness(self, dimmer: BrilliantDevice) -> None:
        result = translate_command(dimmer, {"state": "ON", "brightness": 255})
        assert result == [VarSet("on", "1"), VarSet("intensity", "1000")]

    def test_on_mid_brightness(self, dimmer: BrilliantDevice) -> None:
        # round(128 / 255 * 1000) == 502
        result = translate_command(dimmer, {"state": "ON", "brightness": 128})
        assert result == [VarSet("on", "1"), VarSet("intensity", "502")]

    def test_on_zero_brightness(self, dimmer: BrilliantDevice) -> None:
        result = translate_command(dimmer, {"state": "ON", "brightness": 0})
        assert result == [VarSet("on", "1"), VarSet("intensity", "0")]


# ---------------------------------------------------------------------------
# Brightness clamping
# ---------------------------------------------------------------------------


class TestBrightnessClamping:
    def test_brightness_above_255_clamped_to_1000(self, dimmer: BrilliantDevice) -> None:
        result = translate_command(dimmer, {"state": "ON", "brightness": 300})
        assert result == [VarSet("on", "1"), VarSet("intensity", "1000")]

    def test_brightness_below_0_clamped_to_0(self, dimmer: BrilliantDevice) -> None:
        result = translate_command(dimmer, {"state": "ON", "brightness": -5})
        assert result == [VarSet("on", "1"), VarSet("intensity", "0")]


# ---------------------------------------------------------------------------
# Custom max_intensity scale
# ---------------------------------------------------------------------------


class TestCustomScale:
    def test_brightness_255_with_max_100(self, dimmer_100: BrilliantDevice) -> None:
        result = translate_command(dimmer_100, {"state": "ON", "brightness": 255})
        assert result == [VarSet("on", "1"), VarSet("intensity", "100")]


# ---------------------------------------------------------------------------
# Non-dimmable light (no intensity variable)
# ---------------------------------------------------------------------------


class TestNonDimmableLight:
    def test_brightness_ignored_on_non_dimmable(self, non_dimmable_light: BrilliantDevice) -> None:
        """Brightness on a non-dimmable device must be ignored."""
        result = translate_command(non_dimmable_light, {"state": "ON", "brightness": 200})
        assert result == [VarSet("on", "1")]


# ---------------------------------------------------------------------------
# Brightness-only command (no state key)
# ---------------------------------------------------------------------------


class TestBrightnessOnly:
    def test_brightness_only_produces_intensity_varset(self, dimmer: BrilliantDevice) -> None:
        # Defensive: HA's JSON light always pairs brightness with state, but a
        # non-HA publisher could send brightness alone; we still translate it.
        result = translate_command(dimmer, {"brightness": 128})
        assert result == [VarSet("intensity", "502")]


# ---------------------------------------------------------------------------
# Switch
# ---------------------------------------------------------------------------


class TestSwitch:
    def test_switch_on(self, switch: BrilliantDevice) -> None:
        result = translate_command(switch, {"state": "ON"})
        assert result == [VarSet("on", "1")]

    def test_switch_off(self, switch: BrilliantDevice) -> None:
        result = translate_command(switch, {"state": "OFF"})
        assert result == [VarSet("on", "0")]


# ---------------------------------------------------------------------------
# Non-controllable kind
# ---------------------------------------------------------------------------


class TestNonControllable:
    def test_motion_sensor_returns_empty(self, motion: BrilliantDevice) -> None:
        result = translate_command(motion, {"state": "ON"})
        assert result == []

    def test_unknown_kind_returns_empty(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Unknown",
            kind=DeviceKind.UNKNOWN,
            variables={},
        )
        assert translate_command(device, {"state": "ON"}) == []

    def test_sensor_kind_returns_empty(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Temp",
            kind=DeviceKind.SENSOR,
            variables={},
        )
        assert translate_command(device, {"state": "ON"}) == []


# ---------------------------------------------------------------------------
# Garbage / edge-case payloads
# ---------------------------------------------------------------------------


class TestGarbagePayloads:
    def test_empty_payload(self, dimmer: BrilliantDevice) -> None:
        assert translate_command(dimmer, {}) == []

    def test_lowercase_state_ignored(self, dimmer: BrilliantDevice) -> None:
        """HA sends uppercase; lowercase must not produce an on VarSet."""
        assert translate_command(dimmer, {"state": "on"}) == []

    def test_numeric_state_ignored(self, dimmer: BrilliantDevice) -> None:
        assert translate_command(dimmer, {"state": 5}) == []

    def test_brightness_string_ignored(self, dimmer: BrilliantDevice) -> None:
        """Non-numeric brightness must be ignored; on VarSet still produced."""
        result = translate_command(dimmer, {"state": "ON", "brightness": "abc"})
        assert result == [VarSet("on", "1")]

    def test_brightness_bool_ignored(self, dimmer: BrilliantDevice) -> None:
        """bool is a subclass of int in Python but must NOT count as numeric brightness."""
        result = translate_command(dimmer, {"state": "ON", "brightness": True})
        assert result == [VarSet("on", "1")]

    def test_brightness_none_ignored(self, dimmer: BrilliantDevice) -> None:
        result = translate_command(dimmer, {"state": "ON", "brightness": None})
        assert result == [VarSet("on", "1")]


# ---------------------------------------------------------------------------
# M10 — translate_aux (per-variable aux command translation)
# ---------------------------------------------------------------------------


class TestTranslateAuxBool:
    def test_on(self) -> None:
        assert translate_aux("ON", "bool") == "1"

    def test_off(self) -> None:
        assert translate_aux("OFF", "bool") == "0"

    def test_invert_on(self) -> None:
        # invert swaps the mapping: ON → "0", OFF → "1".
        assert translate_aux("ON", "bool", invert=True) == "0"

    def test_invert_off(self) -> None:
        assert translate_aux("OFF", "bool", invert=True) == "1"

    def test_garbage_returns_none(self) -> None:
        assert translate_aux("maybe", "bool") is None

    def test_empty_returns_none(self) -> None:
        assert translate_aux("", "bool") is None

    def test_lowercase_on_is_none(self) -> None:
        assert translate_aux("on", "bool") is None


class TestTranslateAuxButton:
    def test_press_is_one(self) -> None:
        assert translate_aux("PRESS", "bool") == "1"

    def test_press_with_invert(self) -> None:
        # PRESS is special-cased to "1" regardless of invert (buttons reuse bool).
        assert translate_aux("PRESS", "bool", invert=True) == "1"


class TestTranslateAuxInt:
    def test_plain_int(self) -> None:
        assert translate_aux("5", "int") == "5"

    def test_float_string_truncated(self) -> None:
        assert translate_aux("3.7", "int") == "3"

    def test_clamp_to_max(self) -> None:
        assert translate_aux("150", "int", min_value=0, max_value=100) == "100"

    def test_clamp_to_min(self) -> None:
        assert translate_aux("-5", "int", min_value=0, max_value=100) == "0"

    def test_no_clamp_when_bounds_absent(self) -> None:
        assert translate_aux("9999", "int") == "9999"

    def test_garbage_returns_none(self) -> None:
        assert translate_aux("abc", "int") is None

    def test_empty_returns_none(self) -> None:
        assert translate_aux("", "int") is None

    def test_in_range_unchanged(self) -> None:
        assert translate_aux("7", "int", min_value=0, max_value=10) == "7"


class TestTranslateAuxNeverRaises:
    def test_unknown_kind_returns_none(self) -> None:
        assert translate_aux("ON", "float") is None

    def test_none_safe_for_all_kinds(self) -> None:
        # Defensive sweep — none of these should raise.
        assert translate_aux("garbage", "bool") is None
        assert translate_aux("garbage", "int") is None
        assert translate_aux("garbage", "float") is None

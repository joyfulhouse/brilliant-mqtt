"""Tests for brilliant_mqtt.mapping — entity descriptor generation."""

from __future__ import annotations

import pytest

from brilliant_mqtt.mapping import EntityDescriptor, entities_for, payload_fields
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable

# ---------------------------------------------------------------------------
# Helpers to build BrilliantDevice fixtures quickly
# ---------------------------------------------------------------------------


def _light_dimmer() -> BrilliantDevice:
    """Dimmer LIGHT using real PoC variable names."""
    return BrilliantDevice(
        device_id="dev-1",
        peripheral_id="gangbox_peripheral_0",
        name="Lights",
        kind=DeviceKind.LIGHT,
        variables={
            "on": Variable(name="on", value="0"),
            "intensity": Variable(name="intensity", value="600"),
            "max_intensity_value": Variable(name="max_intensity_value", value="1000"),
        },
    )


def _light_non_dimmable() -> BrilliantDevice:
    """Non-dimmable LIGHT (no intensity variable)."""
    return BrilliantDevice(
        device_id="dev-2",
        peripheral_id="gangbox_peripheral_1",
        name="Fan",
        kind=DeviceKind.LIGHT,
        variables={
            "on": Variable(name="on", value="1"),
        },
    )


def _switch() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="dev-3",
        peripheral_id="gangbox_peripheral_2",
        name="Outlet",
        kind=DeviceKind.SWITCH,
        variables={
            "on": Variable(name="on", value="0"),
        },
    )


def _binary_sensor_with_lux() -> BrilliantDevice:
    """Motion peripheral that also exposes a lux variable."""
    return BrilliantDevice(
        device_id="dev-4",
        peripheral_id="faceplate_peripheral_0",
        name="Motion",
        kind=DeviceKind.BINARY_SENSOR,
        variables={
            "movement_detected": Variable(name="movement_detected", value="0"),
            "lux": Variable(name="lux", value="12.5"),
        },
    )


def _binary_sensor_no_lux() -> BrilliantDevice:
    """Motion peripheral WITHOUT a lux variable."""
    return BrilliantDevice(
        device_id="dev-5",
        peripheral_id="faceplate_peripheral_1",
        name="Motion Back",
        kind=DeviceKind.BINARY_SENSOR,
        variables={
            "movement_detected": Variable(name="movement_detected", value="0"),
        },
    )


def _unknown() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="dev-6",
        peripheral_id="always_on_0",
        name="Always On",
        kind=DeviceKind.UNKNOWN,
        variables={},
    )


def _sensor() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="dev-7",
        peripheral_id="climate_0",
        name="Climate",
        kind=DeviceKind.SENSOR,
        variables={},
    )


# ---------------------------------------------------------------------------
# UNKNOWN → empty
# ---------------------------------------------------------------------------


def test_unknown_returns_empty() -> None:
    assert entities_for(_unknown(), "office") == []


# ---------------------------------------------------------------------------
# SENSOR → empty (no CLIMATE_SENSOR seen in PoC home graph)
# ---------------------------------------------------------------------------


def test_sensor_returns_empty() -> None:
    assert entities_for(_sensor(), "office") == []


# ---------------------------------------------------------------------------
# LIGHT — dimmable
# ---------------------------------------------------------------------------


def test_dimmer_light_one_descriptor() -> None:
    result = entities_for(_light_dimmer(), "office")
    assert len(result) == 1


def test_dimmer_light_component() -> None:
    (d,) = entities_for(_light_dimmer(), "office")
    assert d.component == "light"


def test_dimmer_light_supports_brightness() -> None:
    (d,) = entities_for(_light_dimmer(), "office")
    assert d.supports_brightness is True


def test_dimmer_light_unique_id() -> None:
    (d,) = entities_for(_light_dimmer(), "office")
    assert d.unique_id == "brilliant_office_gangbox_peripheral_0"


def test_dimmer_light_name() -> None:
    (d,) = entities_for(_light_dimmer(), "office")
    assert d.name == "Lights"


def test_dimmer_light_panel() -> None:
    (d,) = entities_for(_light_dimmer(), "office")
    assert d.panel == "office"


def test_dimmer_light_peripheral_id() -> None:
    (d,) = entities_for(_light_dimmer(), "office")
    assert d.peripheral_id == "gangbox_peripheral_0"


def test_dimmer_light_no_device_class() -> None:
    (d,) = entities_for(_light_dimmer(), "office")
    assert d.device_class is None


def test_dimmer_light_no_value_key() -> None:
    (d,) = entities_for(_light_dimmer(), "office")
    assert d.value_key is None


# ---------------------------------------------------------------------------
# LIGHT — non-dimmable
# ---------------------------------------------------------------------------


def test_non_dimmable_light_supports_brightness_false() -> None:
    (d,) = entities_for(_light_non_dimmable(), "office")
    assert d.supports_brightness is False


def test_non_dimmable_light_component() -> None:
    (d,) = entities_for(_light_non_dimmable(), "office")
    assert d.component == "light"


# ---------------------------------------------------------------------------
# SWITCH
# ---------------------------------------------------------------------------


def test_switch_one_descriptor() -> None:
    result = entities_for(_switch(), "office")
    assert len(result) == 1


def test_switch_component() -> None:
    (d,) = entities_for(_switch(), "office")
    assert d.component == "switch"


def test_switch_unique_id() -> None:
    (d,) = entities_for(_switch(), "office")
    assert d.unique_id == "brilliant_office_gangbox_peripheral_2"


def test_switch_no_brightness() -> None:
    (d,) = entities_for(_switch(), "office")
    assert d.supports_brightness is False


# ---------------------------------------------------------------------------
# BINARY_SENSOR with lux
# ---------------------------------------------------------------------------


def test_binary_sensor_with_lux_two_descriptors() -> None:
    result = entities_for(_binary_sensor_with_lux(), "office")
    assert len(result) == 2


def test_binary_sensor_with_lux_motion_component() -> None:
    motion, _ = entities_for(_binary_sensor_with_lux(), "office")
    assert motion.component == "binary_sensor"


def test_binary_sensor_with_lux_motion_device_class() -> None:
    motion, _ = entities_for(_binary_sensor_with_lux(), "office")
    assert motion.device_class == "motion"


def test_binary_sensor_with_lux_motion_value_key() -> None:
    motion, _ = entities_for(_binary_sensor_with_lux(), "office")
    assert motion.value_key == "motion"


def test_binary_sensor_with_lux_motion_unique_id() -> None:
    motion, _ = entities_for(_binary_sensor_with_lux(), "office")
    assert motion.unique_id == "brilliant_office_faceplate_peripheral_0"


def test_binary_sensor_with_lux_motion_name() -> None:
    motion, _ = entities_for(_binary_sensor_with_lux(), "office")
    assert motion.name == "Motion"


def test_binary_sensor_with_lux_illuminance_component() -> None:
    _, lux = entities_for(_binary_sensor_with_lux(), "office")
    assert lux.component == "sensor"


def test_binary_sensor_with_lux_illuminance_device_class() -> None:
    _, lux = entities_for(_binary_sensor_with_lux(), "office")
    assert lux.device_class == "illuminance"


def test_binary_sensor_with_lux_illuminance_unit() -> None:
    _, lux = entities_for(_binary_sensor_with_lux(), "office")
    assert lux.unit == "lx"


def test_binary_sensor_with_lux_illuminance_value_key() -> None:
    _, lux = entities_for(_binary_sensor_with_lux(), "office")
    assert lux.value_key == "lux"


def test_binary_sensor_with_lux_illuminance_unique_id_ends_lux() -> None:
    _, lux = entities_for(_binary_sensor_with_lux(), "office")
    assert lux.unique_id == "brilliant_office_faceplate_peripheral_0_lux"


def test_binary_sensor_with_lux_illuminance_name() -> None:
    _, lux = entities_for(_binary_sensor_with_lux(), "office")
    # HA prefixes the device name; the entity name is just "Illuminance" (M10).
    assert lux.name == "Illuminance"


# ---------------------------------------------------------------------------
# BINARY_SENSOR without lux
# ---------------------------------------------------------------------------


def test_binary_sensor_no_lux_one_descriptor() -> None:
    result = entities_for(_binary_sensor_no_lux(), "office")
    assert len(result) == 1


def test_binary_sensor_no_lux_component() -> None:
    (d,) = entities_for(_binary_sensor_no_lux(), "office")
    assert d.component == "binary_sensor"


def test_binary_sensor_no_lux_device_class() -> None:
    (d,) = entities_for(_binary_sensor_no_lux(), "office")
    assert d.device_class == "motion"


def test_binary_sensor_no_lux_value_key() -> None:
    (d,) = entities_for(_binary_sensor_no_lux(), "office")
    assert d.value_key == "motion"


# ---------------------------------------------------------------------------
# EntityDescriptor frozen / immutable
# ---------------------------------------------------------------------------


def test_entity_descriptor_is_frozen() -> None:
    (d,) = entities_for(_light_dimmer(), "office")
    # setattr via a variable keeps this type-clean (no suppressions) and avoids
    # ruff B010, which only flags constant attribute names. Frozen dataclasses
    # raise FrozenInstanceError (an AttributeError subclass) from __setattr__.
    frozen_field = "name"
    with pytest.raises(AttributeError):
        setattr(d, frozen_field, "changed")


# ===========================================================================
# M10 — extended entities (aux specs)
# ===========================================================================
#
# Fixtures below mirror REAL pilot-panel data (poc-findings §6 + live probe).


def _light_full() -> BrilliantDevice:
    """LIGHT gangbox with power/temperature/is_safe monitoring variables."""
    return BrilliantDevice(
        device_id="dev-light",
        peripheral_id="gangbox_peripheral_0",
        name="Lights",
        kind=DeviceKind.LIGHT,
        peripheral_type=27,
        variables={
            "on": Variable("on", "0"),
            "intensity": Variable("intensity", "600"),
            "max_intensity_value": Variable("max_intensity_value", "1000"),
            "power": Variable("power", "0"),
            "temperature": Variable("temperature", "43.60"),
            "is_safe": Variable("is_safe", "1"),
        },
    )


def _always_on() -> BrilliantDevice:
    """ALWAYS_ON gangbox — power monitoring only, no on/intensity."""
    return BrilliantDevice(
        device_id="dev-ao",
        peripheral_id="gangbox_peripheral_1",
        name="Backyard Lamps",
        kind=DeviceKind.ALWAYS_ON,
        peripheral_type=46,
        variables={
            "power": Variable("power", "52"),
            "temperature": Variable("temperature", "43.60"),
            "is_safe": Variable("is_safe", "1"),
        },
    )


def _hardware() -> BrilliantDevice:
    """HARDWARE peripheral — full diagnostics + controls."""
    return BrilliantDevice(
        device_id="dev-hw",
        peripheral_id="hardware_peripheral",
        name="Hardware",
        kind=DeviceKind.HARDWARE,
        peripheral_type=22,
        variables={
            "muted": Variable("muted", "0"),
            "screen_on": Variable("screen_on", "1"),
            "screen_brightness": Variable("screen_brightness", "7"),
            "output_volume": Variable("output_volume", "100"),
            "alert_volume": Variable("alert_volume", "100"),
            "cpu_temperature": Variable("cpu_temperature", "61"),
            "camera_on": Variable("camera_on", "0"),
            "privacy_toggle": Variable("privacy_toggle", "0"),
            "current_release_tag": Variable("current_release_tag", "v26.05.20.2"),
        },
    )


def _ui() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="dev-ui",
        peripheral_id="ui_peripheral",
        name="UI",
        kind=DeviceKind.UI,
        peripheral_type=12,
        variables={
            "active": Variable("active", "0"),
            "child_lock_enabled": Variable("child_lock_enabled", "0"),
            "enable_night_mode": Variable("enable_night_mode", "0"),
            "request_identify": Variable("request_identify", "0"),
        },
    )


def _wifi() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="dev-wifi",
        peripheral_id="wifi_peripheral",
        name="WiFi",
        kind=DeviceKind.WIFI,
        peripheral_type=29,
        variables={
            "association_status": Variable("association_status", "1"),
            "connectivity_ping_successful": Variable("connectivity_ping_successful", "1"),
            "ntp_synced": Variable("ntp_synced", "1"),
        },
    )


def _faceplate_full() -> BrilliantDevice:
    """MOTION_SENSOR faceplate with motion+lux AND the four aux variables."""
    return BrilliantDevice(
        device_id="dev-fp",
        peripheral_id="faceplate_peripheral",
        name="Faceplate",
        kind=DeviceKind.BINARY_SENSOR,
        peripheral_type=5,
        variables={
            "movement_detected": Variable("movement_detected", "0"),
            "lux": Variable("lux", "12.5"),
            "led_on": Variable("led_on", "0"),
            "enable_lux": Variable("enable_lux", "0"),
            "pir_motion_score": Variable("pir_motion_score", "0"),
            "hottest_internal_temperature": Variable("hottest_internal_temperature", "42.70"),
        },
    )


def _by_uid(descriptors: list[EntityDescriptor]) -> dict[str, EntityDescriptor]:
    return {d.unique_id: d for d in descriptors}


# --- LIGHT with aux ---------------------------------------------------------


def test_light_full_yields_light_plus_three_aux() -> None:
    result = entities_for(_light_full(), "office")
    assert len(result) == 4
    components = sorted(d.component for d in result)
    assert components == ["binary_sensor", "light", "sensor", "sensor"]


def test_light_full_primary_light_unchanged() -> None:
    result = entities_for(_light_full(), "office")
    light = next(d for d in result if d.component == "light")
    assert light.unique_id == "brilliant_office_gangbox_peripheral_0"
    assert light.name == "Lights"
    assert light.supports_brightness is True
    assert light.command_var is None


def test_light_full_power_descriptor() -> None:
    by_uid = _by_uid(entities_for(_light_full(), "office"))
    power = by_uid["brilliant_office_gangbox_peripheral_0_power"]
    assert power.component == "sensor"
    # Load kinds prefix the device name — bare "Power" collides across gangs.
    assert power.name == "Lights Power"
    assert power.device_class == "power"
    assert power.unit == "W"
    assert power.state_class == "measurement"
    assert power.value_key == "power"
    assert power.entity_category is None
    assert power.command_var is None


def test_light_full_temperature_descriptor() -> None:
    by_uid = _by_uid(entities_for(_light_full(), "office"))
    temp = by_uid["brilliant_office_gangbox_peripheral_0_temperature"]
    assert temp.component == "sensor"
    assert temp.name == "Lights Temperature"
    assert temp.device_class == "temperature"
    assert temp.unit == "°C"
    assert temp.entity_category == "diagnostic"


def test_light_full_fault_descriptor() -> None:
    by_uid = _by_uid(entities_for(_light_full(), "office"))
    fault = by_uid["brilliant_office_gangbox_peripheral_0_is_safe"]
    assert fault.component == "binary_sensor"
    assert fault.name == "Lights Fault"
    assert fault.device_class == "problem"
    assert fault.entity_category == "diagnostic"
    assert fault.value_key == "fault"
    assert fault.invert is True
    assert fault.command_var is None


# --- ALWAYS_ON --------------------------------------------------------------


def test_always_on_yields_exactly_three_aux_no_light() -> None:
    result = entities_for(_always_on(), "office")
    assert len(result) == 3
    assert not any(d.component == "light" for d in result)
    assert not any(d.component == "switch" for d in result)
    uids = {d.unique_id for d in result}
    assert uids == {
        "brilliant_office_gangbox_peripheral_1_power",
        "brilliant_office_gangbox_peripheral_1_temperature",
        "brilliant_office_gangbox_peripheral_1_is_safe",
    }


def test_always_on_aux_names_prefixed_with_load_name() -> None:
    """Two gangbox loads on one panel must not both mint bare "Power" names."""
    by_uid = _by_uid(entities_for(_always_on(), "office"))
    assert by_uid["brilliant_office_gangbox_peripheral_1_power"].name == "Backyard Lamps Power"
    assert by_uid["brilliant_office_gangbox_peripheral_1_is_safe"].name == "Backyard Lamps Fault"


def test_singleton_kind_aux_names_not_prefixed() -> None:
    """HARDWARE / UI / WIFI / faceplate are one-per-panel — short names stay."""
    hw = _by_uid(entities_for(_hardware(), "office"))
    assert hw["brilliant_office_hardware_peripheral_muted"].name == "Microphone Mute"
    fp = _by_uid(entities_for(_faceplate_full(), "office"))
    assert fp["brilliant_office_faceplate_peripheral_led_on"].name == "Faceplate LED"


# --- HARDWARE ---------------------------------------------------------------


def test_hardware_yields_nine() -> None:
    result = entities_for(_hardware(), "office")
    assert len(result) == 9


def test_hardware_screen_brightness_number() -> None:
    by_uid = _by_uid(entities_for(_hardware(), "office"))
    sb = by_uid["brilliant_office_hardware_peripheral_screen_brightness"]
    assert sb.component == "number"
    assert sb.command_var == "screen_brightness"
    assert sb.value_kind == "int"
    assert sb.min_value == 0
    assert sb.max_value == 10
    assert sb.step == 1
    assert sb.entity_category == "config"


def test_hardware_muted_switch() -> None:
    by_uid = _by_uid(entities_for(_hardware(), "office"))
    muted = by_uid["brilliant_office_hardware_peripheral_muted"]
    assert muted.component == "switch"
    assert muted.command_var == "muted"
    assert muted.entity_category == "config"


def test_hardware_camera_binary_sensor() -> None:
    by_uid = _by_uid(entities_for(_hardware(), "office"))
    cam = by_uid["brilliant_office_hardware_peripheral_camera_on"]
    assert cam.component == "binary_sensor"
    assert cam.device_class == "running"
    assert cam.command_var is None


def test_hardware_alert_volume_disabled_by_default() -> None:
    by_uid = _by_uid(entities_for(_hardware(), "office"))
    av = by_uid["brilliant_office_hardware_peripheral_alert_volume"]
    assert av.enabled_by_default is False


def test_hardware_without_camera_yields_no_camera_descriptor() -> None:
    device = _hardware()
    del device.variables["camera_on"]
    by_uid = _by_uid(entities_for(device, "office"))
    assert "brilliant_office_hardware_peripheral_camera_on" not in by_uid
    assert len(by_uid) == 8


# --- UI ---------------------------------------------------------------------


def test_ui_yields_four() -> None:
    result = entities_for(_ui(), "office")
    assert len(result) == 4


def test_ui_active_occupancy_primary() -> None:
    by_uid = _by_uid(entities_for(_ui(), "office"))
    active = by_uid["brilliant_office_ui_peripheral_active"]
    assert active.component == "binary_sensor"
    assert active.name == "In Use"
    assert active.device_class == "occupancy"
    assert active.entity_category is None


def test_ui_identify_button() -> None:
    by_uid = _by_uid(entities_for(_ui(), "office"))
    ident = by_uid["brilliant_office_ui_peripheral_request_identify"]
    assert ident.component == "button"
    assert ident.command_var == "request_identify"
    assert ident.entity_category == "config"


# --- WIFI -------------------------------------------------------------------


def test_wifi_yields_three() -> None:
    result = entities_for(_wifi(), "office")
    assert len(result) == 3
    for d in result:
        assert d.component == "binary_sensor"


def test_wifi_association_connectivity() -> None:
    by_uid = _by_uid(entities_for(_wifi(), "office"))
    wifi = by_uid["brilliant_office_wifi_peripheral_association_status"]
    assert wifi.device_class == "connectivity"
    assert wifi.entity_category == "diagnostic"


# --- Faceplate with aux -----------------------------------------------------


def test_faceplate_full_yields_motion_lux_plus_four_aux() -> None:
    result = entities_for(_faceplate_full(), "office")
    assert len(result) == 6
    uids = {d.unique_id for d in result}
    assert "brilliant_office_faceplate_peripheral" in uids  # motion (unchanged id)
    assert "brilliant_office_faceplate_peripheral_lux" in uids  # lux (unchanged id)
    assert "brilliant_office_faceplate_peripheral_led_on" in uids
    assert "brilliant_office_faceplate_peripheral_enable_lux" in uids
    assert "brilliant_office_faceplate_peripheral_pir_motion_score" in uids
    assert "brilliant_office_faceplate_peripheral_hottest_internal_temperature" in uids


def test_faceplate_motion_name_and_id_unchanged() -> None:
    result = entities_for(_faceplate_full(), "office")
    motion = next(d for d in result if d.unique_id == "brilliant_office_faceplate_peripheral")
    assert motion.name == "Motion"
    assert motion.device_class == "motion"
    assert motion.value_key == "motion"


def test_faceplate_led_switch_command() -> None:
    by_uid = _by_uid(entities_for(_faceplate_full(), "office"))
    led = by_uid["brilliant_office_faceplate_peripheral_led_on"]
    assert led.component == "switch"
    assert led.command_var == "led_on"
    assert led.entity_category == "config"


def test_faceplate_pir_score_disabled_by_default() -> None:
    by_uid = _by_uid(entities_for(_faceplate_full(), "office"))
    pir = by_uid["brilliant_office_faceplate_peripheral_pir_motion_score"]
    assert pir.enabled_by_default is False
    assert pir.value_kind == "int"


# --- payload_fields ---------------------------------------------------------


def test_payload_fields_light_full() -> None:
    payload = payload_fields(_light_full())
    assert payload == {
        "state": "OFF",
        "brightness": 153,
        "power": 0.0,
        "temperature": 43.6,
        "fault": False,
    }


def test_payload_fields_always_on() -> None:
    payload = payload_fields(_always_on())
    assert payload == {"power": 52.0, "temperature": 43.6, "fault": False}


def test_payload_fields_fault_true_when_unsafe() -> None:
    device = _always_on()
    device.variables["is_safe"] = Variable("is_safe", "0")
    payload = payload_fields(device)
    assert payload["fault"] is True


def test_payload_fields_hardware() -> None:
    payload = payload_fields(_hardware())
    assert payload == {
        "muted": False,
        "screen_on": True,
        "screen_brightness": 7,
        "output_volume": 100,
        "alert_volume": 100,
        "cpu_temperature": 61.0,
        "camera_on": False,
        "privacy_toggle": False,
        "current_release_tag": "v26.05.20.2",
    }


def test_payload_fields_ui() -> None:
    payload = payload_fields(_ui())
    assert payload == {
        "active": False,
        "child_lock_enabled": False,
        "enable_night_mode": False,
        "request_identify": False,
    }


def test_payload_fields_wifi() -> None:
    payload = payload_fields(_wifi())
    assert payload == {
        "association_status": True,
        "connectivity_ping_successful": True,
        "ntp_synced": True,
    }


def test_payload_fields_faceplate_includes_motion_lux_and_aux() -> None:
    payload = payload_fields(_faceplate_full())
    assert payload == {
        "motion": False,
        "lux": 12.5,
        "led_on": False,
        "enable_lux": False,
        "pir_motion_score": 0,
        "hottest_internal_temperature": 42.7,
    }


def test_payload_fields_unknown_empty() -> None:
    device = BrilliantDevice(
        device_id="d",
        peripheral_id="p",
        name="Unknown",
        kind=DeviceKind.UNKNOWN,
        variables={"power": Variable("power", "5")},
    )
    assert payload_fields(device) == {}


def test_payload_fields_skips_unparseable_aux() -> None:
    device = _always_on()
    device.variables["power"] = Variable("power", "garbage")
    payload = payload_fields(device)
    assert "power" not in payload
    assert payload == {"temperature": 43.6, "fault": False}


def test_payload_fields_light_no_aux_when_absent() -> None:
    """A bare LIGHT (no monitoring vars) renders only state+brightness."""
    payload = payload_fields(_light_dimmer())
    assert payload == {"state": "OFF", "brightness": 153}


# ===========================================================================
# M11 Step 2 — sentinel power gate (AuxSpec.skip_values)
# ===========================================================================
#
# Mesh loads report power == "-1" until calibrated ("no reading"); the gate
# must suppress the entity AND the payload key symmetrically so the descriptor
# set and the payload keys never drift apart.

MESH_PID = "018691f1749b000701c4e689967b8e62"


def _mesh_dimmer() -> BrilliantDevice:
    """Mesh dimmer (PeripheralType 27 on the virtual ble_mesh bus device).

    Live-verified shape: panel-dimmer-like variables but NO max_intensity_value
    (the model's guarded fallback of 1000 covers brightness scaling).
    """
    return BrilliantDevice(
        device_id="ble_mesh",
        peripheral_id=MESH_PID,
        name="Office Desk Lights",
        kind=DeviceKind.LIGHT,
        peripheral_type=27,
        variables={
            "on": Variable("on", "0"),
            "intensity": Variable("intensity", "600"),
            "dimmable": Variable("dimmable", "1"),
            "display_name": Variable("display_name", "Office Desk Lights"),
            "power": Variable("power", "-1"),
        },
    )


def _mesh_dimmer_with_power(power: str) -> BrilliantDevice:
    device = _mesh_dimmer()
    device.variables["power"] = Variable("power", power)
    return device


def test_power_descriptor_gated_by_sentinel() -> None:
    uids = {d.unique_id for d in entities_for(_mesh_dimmer(), "mesh")}
    assert f"brilliant_mesh_{MESH_PID}_power" not in uids


def test_power_payload_key_gated_by_sentinel() -> None:
    assert "power" not in payload_fields(_mesh_dimmer())


def test_mesh_dimmer_sentinel_yields_only_primary_light() -> None:
    result = entities_for(_mesh_dimmer(), "mesh")
    assert [d.component for d in result] == ["light"]
    assert result[0].unique_id == f"brilliant_mesh_{MESH_PID}"
    assert result[0].supports_brightness is True


def test_mesh_dimmer_payload_brightness_uses_fallback_scale() -> None:
    """No max_intensity_value on mesh dimmers — brightness scales against 1000."""
    payload = payload_fields(_mesh_dimmer())
    assert payload == {"state": "OFF", "brightness": 153}


def test_power_descriptor_present_when_real() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_power("52"), "mesh"))
    power = by_uid[f"brilliant_mesh_{MESH_PID}_power"]
    assert power.component == "sensor"
    assert power.device_class == "power"


def test_power_payload_present_when_real() -> None:
    payload = payload_fields(_mesh_dimmer_with_power("52"))
    assert payload["power"] == 52.0


def test_zero_power_is_a_real_reading_not_gated() -> None:
    """ "0" watts is a real measurement — only the "-1" sentinel is gated."""
    payload = payload_fields(_light_full())  # fixture power == "0"
    assert payload["power"] == 0.0
    uids = {d.unique_id for d in entities_for(_light_full(), "office")}
    assert "brilliant_office_gangbox_peripheral_0_power" in uids


def test_sentinel_gate_leaves_other_specs_alone() -> None:
    """temperature / is_safe keep their entities and keys when power is gated."""
    device = _always_on()
    device.variables["power"] = Variable("power", "-1")
    assert payload_fields(device) == {"temperature": 43.6, "fault": False}
    uids = {d.unique_id for d in entities_for(device, "office")}
    assert uids == {
        "brilliant_office_gangbox_peripheral_1_temperature",
        "brilliant_office_gangbox_peripheral_1_is_safe",
    }


# ---------------------------------------------------------------------------
# Firmware diagnostic sensor (HARDWARE current_release_tag, value_kind="str")
# ---------------------------------------------------------------------------


def _hardware_with_release_tag() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="dev-hw",
        peripheral_id="hardware_peripheral_0",
        name="Hardware",
        kind=DeviceKind.HARDWARE,
        variables={
            "current_release_tag": Variable(name="current_release_tag", value="v26.05.20.2"),
        },
    )


def test_hardware_firmware_sensor_descriptor() -> None:
    descriptors = entities_for(_hardware_with_release_tag(), "office")
    fw = [d for d in descriptors if d.unique_id.endswith("_current_release_tag")]
    assert len(fw) == 1
    d = fw[0]
    assert d.component == "sensor"
    assert d.name == "Firmware"
    assert d.entity_category == "diagnostic"
    assert d.value_kind == "str"
    assert d.command_var is None  # read-only: sensors never mint a command topic
    assert d.value_key == "current_release_tag"


def test_hardware_firmware_payload_renders_string() -> None:
    fields = payload_fields(_hardware_with_release_tag())
    assert fields["current_release_tag"] == "v26.05.20.2"


def test_hardware_without_release_tag_has_no_firmware_entries() -> None:
    bare = BrilliantDevice(
        device_id="dev-hw2",
        peripheral_id="hardware_peripheral_1",
        name="Hardware",
        kind=DeviceKind.HARDWARE,
        variables={},
    )
    assert not any(
        d.unique_id.endswith("_current_release_tag") for d in entities_for(bare, "office")
    )
    assert "current_release_tag" not in payload_fields(bare)


def test_hardware_blank_release_tag_has_no_firmware_entries() -> None:
    """A blank tag means "unknown" — matches _sw_version_from's gate on the same var."""
    blank = BrilliantDevice(
        device_id="dev-hw3",
        peripheral_id="hardware_peripheral_2",
        name="Hardware",
        kind=DeviceKind.HARDWARE,
        variables={
            "current_release_tag": Variable(name="current_release_tag", value=""),
        },
    )
    assert not any(
        d.unique_id.endswith("_current_release_tag") for d in entities_for(blank, "office")
    )
    assert "current_release_tag" not in payload_fields(blank)


# ===========================================================================
# Mesh-load motion subsystem (live-verified office.iot, 2026-06-13)
# ===========================================================================
#
# Every mesh load peripheral (LIGHT/SWITCH/ALWAYS_ON on the virtual ble_mesh
# device) carries five motion variables. Because aux specs gate on variable
# presence, panel loads (which lack these variables) are unaffected.


def _mesh_dimmer_with_motion() -> BrilliantDevice:
    """_mesh_dimmer() + the five live-verified motion variables."""
    device = _mesh_dimmer()
    device.variables.update(
        {
            "movement_detected": Variable("movement_detected", "1"),
            "motion_score": Variable("motion_score", "0"),
            "enable_motion_score": Variable("enable_motion_score", "0"),
            "motion_high_threshold": Variable("motion_high_threshold", "70"),
            "motion_low_threshold": Variable("motion_low_threshold", "20"),
        }
    )
    return device


def _always_on_with_motion() -> BrilliantDevice:
    """_always_on() + the five live-verified motion variables."""
    device = _always_on()
    device.variables.update(
        {
            "movement_detected": Variable("movement_detected", "0"),
            "motion_score": Variable("motion_score", "0"),
            "enable_motion_score": Variable("enable_motion_score", "0"),
            "motion_high_threshold": Variable("motion_high_threshold", "70"),
            "motion_low_threshold": Variable("motion_low_threshold", "20"),
        }
    )
    return device


# --- Motion binary_sensor descriptor ----------------------------------------


def test_mesh_motion_binary_sensor_component() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_movement_detected"]
    assert d.component == "binary_sensor"


def test_mesh_motion_binary_sensor_device_class() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_movement_detected"]
    assert d.device_class == "motion"


def test_mesh_motion_binary_sensor_value_key() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_movement_detected"]
    assert d.value_key == "motion"


def test_mesh_motion_binary_sensor_name() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_movement_detected"]
    assert d.name == "Office Desk Lights Motion"


def test_mesh_motion_binary_sensor_enabled_by_default() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_movement_detected"]
    assert d.enabled_by_default is True


def test_mesh_motion_binary_sensor_command_var_none() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_movement_detected"]
    assert d.command_var is None


# --- Motion Score sensor -----------------------------------------------------


def test_mesh_motion_score_sensor_component() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_score"]
    assert d.component == "sensor"


def test_mesh_motion_score_value_kind_int() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_score"]
    assert d.value_kind == "int"


def test_mesh_motion_score_entity_category_diagnostic() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_score"]
    assert d.entity_category == "diagnostic"


def test_mesh_motion_score_disabled_by_default() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_score"]
    assert d.enabled_by_default is False


def test_mesh_motion_score_command_var_none() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_score"]
    assert d.command_var is None


def test_mesh_motion_score_name() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_score"]
    assert d.name == "Office Desk Lights Motion Score"


# --- Enable Motion Score switch ----------------------------------------------


def test_mesh_enable_motion_score_switch_component() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_enable_motion_score"]
    assert d.component == "switch"


def test_mesh_enable_motion_score_command_var() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_enable_motion_score"]
    assert d.command_var == "enable_motion_score"


def test_mesh_enable_motion_score_entity_category_config() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_enable_motion_score"]
    assert d.entity_category == "config"


def test_mesh_enable_motion_score_disabled_by_default() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_enable_motion_score"]
    assert d.enabled_by_default is False


def test_mesh_enable_motion_score_name() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_enable_motion_score"]
    assert d.name == "Office Desk Lights Motion Score Reporting"


# --- Motion High Threshold number -------------------------------------------


def test_mesh_motion_high_threshold_component() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_high_threshold"]
    assert d.component == "number"


def test_mesh_motion_high_threshold_value_kind_int() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_high_threshold"]
    assert d.value_kind == "int"


def test_mesh_motion_high_threshold_command_var() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_high_threshold"]
    assert d.command_var == "motion_high_threshold"


def test_mesh_motion_high_threshold_min_max_step() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_high_threshold"]
    assert d.min_value == 0
    assert d.max_value == 100
    assert d.step == 1


def test_mesh_motion_high_threshold_entity_category_config() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_high_threshold"]
    assert d.entity_category == "config"


def test_mesh_motion_high_threshold_disabled_by_default() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_high_threshold"]
    assert d.enabled_by_default is False


# --- Motion Low Threshold number --------------------------------------------


def test_mesh_motion_low_threshold_component() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_low_threshold"]
    assert d.component == "number"


def test_mesh_motion_low_threshold_value_kind_int() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_low_threshold"]
    assert d.value_kind == "int"


def test_mesh_motion_low_threshold_command_var() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_low_threshold"]
    assert d.command_var == "motion_low_threshold"


def test_mesh_motion_low_threshold_min_max_step() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_low_threshold"]
    assert d.min_value == 0
    assert d.max_value == 100
    assert d.step == 1


def test_mesh_motion_low_threshold_entity_category_config() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_low_threshold"]
    assert d.entity_category == "config"


def test_mesh_motion_low_threshold_disabled_by_default() -> None:
    by_uid = _by_uid(entities_for(_mesh_dimmer_with_motion(), "mesh"))
    d = by_uid[f"brilliant_mesh_{MESH_PID}_motion_low_threshold"]
    assert d.enabled_by_default is False


# --- payload_fields with motion vars ----------------------------------------


def test_mesh_dimmer_with_motion_payload_fields() -> None:
    """Exact payload including power sentinel (gated) and all motion fields."""
    payload = payload_fields(_mesh_dimmer_with_motion())
    assert payload == {
        "state": "OFF",
        "brightness": 153,
        "motion": True,
        "motion_score": 0,
        "enable_motion_score": False,
        "motion_high_threshold": 70,
        "motion_low_threshold": 20,
    }


# --- ALWAYS_ON cross-kind coverage ------------------------------------------


def test_always_on_with_motion_has_motion_binary_sensor() -> None:
    """Motion specs apply to ALWAYS_ON loads too (not just LIGHT)."""
    by_uid = _by_uid(entities_for(_always_on_with_motion(), "office"))
    motion_uid = "brilliant_office_gangbox_peripheral_1_movement_detected"
    assert motion_uid in by_uid
    assert by_uid[motion_uid].device_class == "motion"


def test_always_on_with_motion_motion_name_prefixed() -> None:
    """ALWAYS_ON is a load kind — motion entity name is prefixed with load name."""
    by_uid = _by_uid(entities_for(_always_on_with_motion(), "office"))
    d = by_uid["brilliant_office_gangbox_peripheral_1_movement_detected"]
    assert d.name == "Backyard Lamps Motion"


# --- Regression: panel loads WITHOUT motion vars are unaffected --------------


def test_panel_light_full_still_yields_exactly_four_entities() -> None:
    """_light_full() has no motion vars — still 4 entities (light+power+temp+fault)."""
    result = entities_for(_light_full(), "office")
    assert len(result) == 4


def test_panel_light_full_no_motion_uid() -> None:
    """Panel LIGHT with no motion vars must not mint a movement_detected entity."""
    uids = {d.unique_id for d in entities_for(_light_full(), "office")}
    assert "brilliant_office_gangbox_peripheral_0_movement_detected" not in uids


def test_mesh_dimmer_sentinel_no_motion_vars_still_one_entity() -> None:
    """_mesh_dimmer() (without motion vars) still yields exactly one entity."""
    result = entities_for(_mesh_dimmer(), "mesh")
    assert [d.component for d in result] == ["light"]
    uids = {d.unique_id for d in result}
    assert f"brilliant_mesh_{MESH_PID}_movement_detected" not in uids

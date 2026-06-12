"""Tests for brilliant_mqtt.discovery — topic builders + config_payload."""

from __future__ import annotations

import json
from dataclasses import replace

from brilliant_mqtt.discovery import (
    aux_command_topic,
    availability_topic,
    command_topic,
    config_payload,
    config_topic,
    state_topic,
)
from brilliant_mqtt.mapping import EntityDescriptor

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

PANEL = "office"
PERIPH = "gangbox_peripheral_0"


def _light_dimmer_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="light",
        unique_id=f"brilliant_{PANEL}_{PERIPH}",
        name="Lights",
        panel=PANEL,
        peripheral_id=PERIPH,
        supports_brightness=True,
    )


def _light_non_dimmable_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="light",
        unique_id=f"brilliant_{PANEL}_{PERIPH}",
        name="Fan Light",
        panel=PANEL,
        peripheral_id=PERIPH,
        supports_brightness=False,
    )


def _switch_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="switch",
        unique_id=f"brilliant_{PANEL}_{PERIPH}",
        name="Outlet",
        panel=PANEL,
        peripheral_id=PERIPH,
    )


def _motion_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="binary_sensor",
        unique_id=f"brilliant_{PANEL}_faceplate_peripheral_0",
        name="Motion",
        panel=PANEL,
        peripheral_id="faceplate_peripheral_0",
        device_class="motion",
        value_key="motion",
    )


def _lux_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="sensor",
        unique_id=f"brilliant_{PANEL}_faceplate_peripheral_0_lux",
        name="Illuminance",
        panel=PANEL,
        peripheral_id="faceplate_peripheral_0",
        device_class="illuminance",
        unit="lx",
        value_key="lux",
        state_class="measurement",
    )


# ---------------------------------------------------------------------------
# Topic builders
# ---------------------------------------------------------------------------


def test_config_topic() -> None:
    assert config_topic(_light_dimmer_descriptor()) == (
        f"homeassistant/light/brilliant_{PANEL}_{PERIPH}/config"
    )


def test_config_topic_switch() -> None:
    assert config_topic(_switch_descriptor()) == (
        f"homeassistant/switch/brilliant_{PANEL}_{PERIPH}/config"
    )


def test_state_topic() -> None:
    assert state_topic(PANEL, PERIPH) == f"brilliant/{PANEL}/{PERIPH}/state"


def test_command_topic() -> None:
    assert command_topic(PANEL, PERIPH) == f"brilliant/{PANEL}/{PERIPH}/set"


def test_availability_topic() -> None:
    assert availability_topic(PANEL) == f"brilliant/{PANEL}/availability"


# ---------------------------------------------------------------------------
# config_payload — common fields
# ---------------------------------------------------------------------------


def test_payload_is_valid_json() -> None:
    payload = config_payload(_light_dimmer_descriptor())
    data = json.loads(payload)
    assert isinstance(data, dict)


def test_payload_unique_id() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["unique_id"] == f"brilliant_{PANEL}_{PERIPH}"


def test_payload_name() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["name"] == "Lights"


def test_payload_state_topic() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["state_topic"] == f"brilliant/{PANEL}/{PERIPH}/state"


def test_payload_availability_topic() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["availability"][0]["topic"] == f"brilliant/{PANEL}/availability"


def test_payload_device_identifiers() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["device"]["identifiers"] == [f"brilliant_panel_{PANEL}"]


def test_payload_device_name() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["device"]["name"] == "Brilliant Office"


def test_payload_device_manufacturer() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["device"]["manufacturer"] == "Brilliant"


def test_payload_device_model() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["device"]["model"] == "Control"


def test_payload_device_name_humanizes_multi_segment_panel() -> None:
    """Display name humanizes the slug; identifiers/topics keep the raw slug."""
    d = EntityDescriptor(
        component="light",
        unique_id="brilliant_office-bath_gangbox_peripheral_0",
        name="Lights",
        panel="office-bath",
        peripheral_id="gangbox_peripheral_0",
        supports_brightness=True,
    )
    data = json.loads(config_payload(d))
    assert data["device"]["name"] == "Brilliant Office Bath"
    assert data["device"]["identifiers"] == ["brilliant_panel_office-bath"]
    assert data["state_topic"] == "brilliant/office-bath/gangbox_peripheral_0/state"


def test_payload_sorted_keys() -> None:
    """Payload must be produced with sort_keys=True for determinism."""
    payload = config_payload(_light_dimmer_descriptor())
    data = json.loads(payload)
    top_keys = list(data.keys())
    assert top_keys == sorted(top_keys)


# ---------------------------------------------------------------------------
# config_payload — light (dimmable)
# ---------------------------------------------------------------------------


def test_light_payload_command_topic() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["command_topic"] == f"brilliant/{PANEL}/{PERIPH}/set"


def test_light_payload_schema_json() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["schema"] == "json"


def test_light_payload_brightness_true() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["brightness"] is True


# ---------------------------------------------------------------------------
# config_payload — light (non-dimmable)
# ---------------------------------------------------------------------------


def test_light_non_dimmable_brightness_false() -> None:
    data = json.loads(config_payload(_light_non_dimmable_descriptor()))
    assert data["brightness"] is False


def test_light_non_dimmable_has_schema() -> None:
    data = json.loads(config_payload(_light_non_dimmable_descriptor()))
    assert data["schema"] == "json"


# ---------------------------------------------------------------------------
# config_payload — switch
# ---------------------------------------------------------------------------


def test_switch_payload_on() -> None:
    data = json.loads(config_payload(_switch_descriptor()))
    assert data["payload_on"] == '{"state": "ON"}'


def test_switch_payload_off() -> None:
    data = json.loads(config_payload(_switch_descriptor()))
    assert data["payload_off"] == '{"state": "OFF"}'


def test_switch_value_template() -> None:
    data = json.loads(config_payload(_switch_descriptor()))
    assert data["value_template"] == "{{ value_json.state }}"


def test_switch_state_on() -> None:
    data = json.loads(config_payload(_switch_descriptor()))
    assert data["state_on"] == "ON"


def test_switch_state_off() -> None:
    data = json.loads(config_payload(_switch_descriptor()))
    assert data["state_off"] == "OFF"


def test_switch_no_schema_key() -> None:
    """switch must NOT have a 'schema' key — that's light-only."""
    data = json.loads(config_payload(_switch_descriptor()))
    assert "schema" not in data


def test_switch_has_command_topic() -> None:
    data = json.loads(config_payload(_switch_descriptor()))
    assert data["command_topic"] == f"brilliant/{PANEL}/{PERIPH}/set"


# ---------------------------------------------------------------------------
# config_payload — binary_sensor (motion)
# ---------------------------------------------------------------------------


def test_motion_device_class() -> None:
    data = json.loads(config_payload(_motion_descriptor()))
    assert data["device_class"] == "motion"


def test_motion_value_template() -> None:
    data = json.loads(config_payload(_motion_descriptor()))
    assert data["value_template"] == "{{ 'ON' if value_json.motion else 'OFF' }}"


def test_motion_no_command_topic() -> None:
    data = json.loads(config_payload(_motion_descriptor()))
    assert "command_topic" not in data


# ---------------------------------------------------------------------------
# config_payload — sensor (lux)
# ---------------------------------------------------------------------------


def test_lux_device_class() -> None:
    data = json.loads(config_payload(_lux_descriptor()))
    assert data["device_class"] == "illuminance"


def test_lux_unit_of_measurement() -> None:
    data = json.loads(config_payload(_lux_descriptor()))
    assert data["unit_of_measurement"] == "lx"


def test_lux_state_class() -> None:
    data = json.loads(config_payload(_lux_descriptor()))
    assert data["state_class"] == "measurement"


def test_lux_value_template() -> None:
    data = json.loads(config_payload(_lux_descriptor()))
    assert data["value_template"] == "{{ value_json.lux }}"


def test_lux_no_command_topic() -> None:
    data = json.loads(config_payload(_lux_descriptor()))
    assert "command_topic" not in data


# ===========================================================================
# M10 — aux entities + sw_version + cross-component fields
# ===========================================================================


def _aux_switch_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="switch",
        unique_id="brilliant_office_hardware_peripheral_muted",
        name="Microphone Mute",
        panel=PANEL,
        peripheral_id="hardware_peripheral",
        value_key="muted",
        entity_category="config",
        command_var="muted",
        value_kind="bool",
    )


def _aux_number_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="number",
        unique_id="brilliant_office_hardware_peripheral_screen_brightness",
        name="Screen Brightness",
        panel=PANEL,
        peripheral_id="hardware_peripheral",
        value_key="screen_brightness",
        entity_category="config",
        command_var="screen_brightness",
        value_kind="int",
        min_value=0,
        max_value=10,
        step=1,
    )


def _aux_button_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="button",
        unique_id="brilliant_office_ui_peripheral_request_identify",
        name="Identify",
        panel=PANEL,
        peripheral_id="ui_peripheral",
        value_key="request_identify",
        entity_category="config",
        command_var="request_identify",
        value_kind="bool",
    )


def _aux_sensor_diag_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="sensor",
        unique_id="brilliant_office_hardware_peripheral_cpu_temperature",
        name="CPU Temperature",
        panel=PANEL,
        peripheral_id="hardware_peripheral",
        device_class="temperature",
        unit="°C",
        value_key="cpu_temperature",
        entity_category="diagnostic",
        state_class="measurement",
    )


def _aux_sensor_disabled_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="sensor",
        unique_id="brilliant_office_faceplate_peripheral_pir_motion_score",
        name="PIR Score",
        panel=PANEL,
        peripheral_id="faceplate_peripheral",
        value_key="pir_motion_score",
        entity_category="diagnostic",
        state_class="measurement",
        enabled_by_default=False,
    )


def _fault_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="binary_sensor",
        unique_id="brilliant_office_gangbox_peripheral_0_is_safe",
        name="Fault",
        panel=PANEL,
        peripheral_id="gangbox_peripheral_0",
        device_class="problem",
        value_key="fault",
        entity_category="diagnostic",
        invert=True,
    )


# --- aux_command_topic ------------------------------------------------------


def test_aux_command_topic() -> None:
    assert aux_command_topic(PANEL, "hardware_peripheral", "muted") == (
        f"brilliant/{PANEL}/hardware_peripheral/set_muted"
    )


# --- aux switch -------------------------------------------------------------


def test_aux_switch_command_topic() -> None:
    data = json.loads(config_payload(_aux_switch_descriptor()))
    assert data["command_topic"] == f"brilliant/{PANEL}/hardware_peripheral/set_muted"


def test_aux_switch_plain_on_off_payloads() -> None:
    data = json.loads(config_payload(_aux_switch_descriptor()))
    assert data["payload_on"] == "ON"
    assert data["payload_off"] == "OFF"
    assert data["state_on"] == "ON"
    assert data["state_off"] == "OFF"


def test_aux_switch_value_template() -> None:
    data = json.loads(config_payload(_aux_switch_descriptor()))
    assert data["value_template"] == "{{ 'ON' if value_json.muted else 'OFF' }}"


def test_aux_switch_entity_category() -> None:
    data = json.loads(config_payload(_aux_switch_descriptor()))
    assert data["entity_category"] == "config"


# --- aux number -------------------------------------------------------------


def test_aux_number_command_topic() -> None:
    data = json.loads(config_payload(_aux_number_descriptor()))
    assert data["command_topic"] == f"brilliant/{PANEL}/hardware_peripheral/set_screen_brightness"


def test_aux_number_min_max_step_mode() -> None:
    data = json.loads(config_payload(_aux_number_descriptor()))
    assert data["min"] == 0
    assert data["max"] == 10
    assert data["step"] == 1
    assert data["mode"] == "slider"


def test_aux_number_value_template() -> None:
    data = json.loads(config_payload(_aux_number_descriptor()))
    assert data["value_template"] == "{{ value_json.screen_brightness }}"


def test_aux_number_none_min_max_step_omitted() -> None:
    """A bound-less number spec must OMIT min/max/step entirely — serializing
    them as JSON null makes HA silently reject the whole discovery config."""
    descriptor = replace(_aux_number_descriptor(), min_value=None, max_value=None, step=None)
    data = json.loads(config_payload(descriptor))
    assert "min" not in data
    assert "max" not in data
    assert "step" not in data
    assert data["mode"] == "slider"


# --- aux button -------------------------------------------------------------


def test_aux_button_command_topic() -> None:
    data = json.loads(config_payload(_aux_button_descriptor()))
    assert data["command_topic"] == f"brilliant/{PANEL}/ui_peripheral/set_request_identify"


def test_aux_button_payload_press() -> None:
    data = json.loads(config_payload(_aux_button_descriptor()))
    assert data["payload_press"] == "PRESS"


def test_aux_button_no_state_topic() -> None:
    data = json.loads(config_payload(_aux_button_descriptor()))
    assert "state_topic" not in data
    assert "value_template" not in data


# --- cross-component fields -------------------------------------------------


def test_diag_sensor_entity_category_and_state_class() -> None:
    data = json.loads(config_payload(_aux_sensor_diag_descriptor()))
    assert data["entity_category"] == "diagnostic"
    assert data["state_class"] == "measurement"
    assert data["device_class"] == "temperature"
    assert data["unit_of_measurement"] == "°C"


def test_disabled_sensor_enabled_by_default_false() -> None:
    data = json.loads(config_payload(_aux_sensor_disabled_descriptor()))
    assert data["enabled_by_default"] is False


def test_enabled_sensor_has_no_enabled_by_default_key() -> None:
    """enabled_by_default key is only emitted when False (absent ⇒ HA default true)."""
    data = json.loads(config_payload(_aux_sensor_diag_descriptor()))
    assert "enabled_by_default" not in data


def test_fault_binary_sensor_problem_class() -> None:
    data = json.loads(config_payload(_fault_descriptor()))
    assert data["device_class"] == "problem"
    assert data["value_template"] == "{{ 'ON' if value_json.fault else 'OFF' }}"
    assert data["entity_category"] == "diagnostic"


def test_primary_light_no_entity_category_key() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert "entity_category" not in data
    assert "enabled_by_default" not in data


def test_sensor_without_device_class_or_unit_omits_keys() -> None:
    """No "device_class": null / "unit_of_measurement": null noise in retained payloads."""
    data = json.loads(config_payload(_aux_sensor_disabled_descriptor()))  # PIR score
    assert "device_class" not in data
    assert "unit_of_measurement" not in data


def test_binary_sensor_without_device_class_omits_key() -> None:
    d = EntityDescriptor(
        component="binary_sensor",
        unique_id="brilliant_office_wifi_peripheral_ntp_synced",
        name="NTP Sync",
        panel=PANEL,
        peripheral_id="wifi_peripheral",
        value_key="ntp_synced",
        entity_category="diagnostic",
        enabled_by_default=False,
    )
    data = json.loads(config_payload(d))
    assert "device_class" not in data
    assert data["value_template"] == "{{ 'ON' if value_json.ntp_synced else 'OFF' }}"


# --- sw_version -------------------------------------------------------------


def test_sw_version_in_device_block_when_passed() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor(), sw_version="v26.05.20.2"))
    assert data["device"]["sw_version"] == "v26.05.20.2"


def test_sw_version_absent_when_none() -> None:
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert "sw_version" not in data["device"]


def test_sw_version_on_aux_entity() -> None:
    data = json.loads(config_payload(_aux_switch_descriptor(), sw_version="v26.05.20.2"))
    assert data["device"]["sw_version"] == "v26.05.20.2"


# --- regression: primary payloads byte-identical when sw_version is None -----


def test_primary_light_byte_identical_regression() -> None:
    """The exact pre-M10 dimmable-light discovery payload, byte-for-byte."""
    expected = (
        '{"availability": [{"topic": "brilliant/office/availability"}], '
        '"brightness": true, '
        '"command_topic": "brilliant/office/gangbox_peripheral_0/set", '
        '"device": {"identifiers": ["brilliant_panel_office"], '
        '"manufacturer": "Brilliant", "model": "Control", "name": "Brilliant Office"}, '
        '"name": "Lights", "schema": "json", '
        '"state_topic": "brilliant/office/gangbox_peripheral_0/state", '
        '"unique_id": "brilliant_office_gangbox_peripheral_0"}'
    )
    assert config_payload(_light_dimmer_descriptor()) == expected


def test_primary_switch_byte_identical_regression() -> None:
    """The exact pre-M10 switch discovery payload, byte-for-byte."""
    expected = (
        '{"availability": [{"topic": "brilliant/office/availability"}], '
        '"command_topic": "brilliant/office/gangbox_peripheral_0/set", '
        '"device": {"identifiers": ["brilliant_panel_office"], '
        '"manufacturer": "Brilliant", "model": "Control", "name": "Brilliant Office"}, '
        '"name": "Outlet", '
        '"payload_off": "{\\"state\\": \\"OFF\\"}", '
        '"payload_on": "{\\"state\\": \\"ON\\"}", '
        '"state_off": "OFF", "state_on": "ON", '
        '"state_topic": "brilliant/office/gangbox_peripheral_0/state", '
        '"unique_id": "brilliant_office_gangbox_peripheral_0", '
        '"value_template": "{{ value_json.state }}"}'
    )
    assert config_payload(_switch_descriptor()) == expected


# ===========================================================================
# M11 Step 2 — mesh HA device block ("mesh" is a RESERVED panel slug)
# ===========================================================================

MESH_PID = "018691f1749b000701c4e689967b8e62"


def _mesh_light_descriptor() -> EntityDescriptor:
    return EntityDescriptor(
        component="light",
        unique_id=f"brilliant_mesh_{MESH_PID}",
        name="Office Desk Lights",
        panel="mesh",
        peripheral_id=MESH_PID,
        supports_brightness=True,
    )


def test_mesh_device_block_name_and_model() -> None:
    """The mesh namespace is publisher-agnostic: no panel's name on its device."""
    data = json.loads(config_payload(_mesh_light_descriptor()))
    assert data["device"]["name"] == "Brilliant BLE Mesh"
    assert data["device"]["model"] == "BLE Mesh"


def test_mesh_device_block_identifiers_and_manufacturer_keep_pattern() -> None:
    data = json.loads(config_payload(_mesh_light_descriptor()))
    assert data["device"]["identifiers"] == ["brilliant_panel_mesh"]
    assert data["device"]["manufacturer"] == "Brilliant"


def test_mesh_topics_use_mesh_slug() -> None:
    data = json.loads(config_payload(_mesh_light_descriptor()))
    assert data["availability"][0]["topic"] == "brilliant/mesh/availability"
    assert data["state_topic"] == f"brilliant/mesh/{MESH_PID}/state"
    assert data["command_topic"] == f"brilliant/mesh/{MESH_PID}/set"


def test_non_mesh_device_block_unchanged() -> None:
    """Regression: real panel slugs keep the Control device block."""
    data = json.loads(config_payload(_light_dimmer_descriptor()))
    assert data["device"]["name"] == "Brilliant Office"
    assert data["device"]["model"] == "Control"

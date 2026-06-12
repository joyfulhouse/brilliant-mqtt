"""MQTT Discovery topic builders and config payload serialiser.

No panel imports, no MQTT imports: pure Python / stdlib only.
All functions are pure (no I/O side-effects).
"""

from __future__ import annotations

import json

from brilliant_mqtt.mapping import EntityDescriptor

# ---------------------------------------------------------------------------
# Topic builders
# ---------------------------------------------------------------------------


def config_topic(e: EntityDescriptor) -> str:
    """Return the HA MQTT Discovery config topic for *e*."""
    return f"homeassistant/{e.component}/{e.unique_id}/config"


def state_topic(panel: str, peripheral_id: str) -> str:
    """Return the retained JSON state topic for a peripheral."""
    return f"brilliant/{panel}/{peripheral_id}/state"


def command_topic(panel: str, peripheral_id: str) -> str:
    """Return the (primary JSON) command topic for a peripheral."""
    return f"brilliant/{panel}/{peripheral_id}/set"


def aux_command_topic(panel: str, peripheral_id: str, var: str) -> str:
    """Return the per-variable command topic for an auxiliary entity."""
    return f"brilliant/{panel}/{peripheral_id}/set_{var}"


def availability_topic(panel: str) -> str:
    """Return the LWT availability topic for an entire panel."""
    return f"brilliant/{panel}/availability"


# ---------------------------------------------------------------------------
# Discovery payload builder
# ---------------------------------------------------------------------------


def config_payload(e: EntityDescriptor, sw_version: str | None = None) -> str:
    """Return a sorted-keys JSON string for HA MQTT Discovery.

    Common fields are present for every component; per-component fields are
    added below.  The value_template for binary_sensor and sensor is built
    from ``e.value_key`` so there are no hardcoded key names here.

    *sw_version*, when provided, is added to the device block (so the HA device
    page shows the panel firmware). When None, the PRIMARY light/switch payloads
    are byte-identical to the pre-Milestone-10 form.
    """
    _state_topic = state_topic(e.panel, e.peripheral_id)
    _avail_topic = availability_topic(e.panel)
    _cmd_topic = command_topic(e.panel, e.peripheral_id)

    if e.panel == "mesh":
        # "mesh" is a RESERVED panel slug: the whole-home BLE mesh pseudo-panel.
        # Its namespace is publisher-agnostic — any panel may hold leadership
        # and publish it — so the HA device must not carry any panel's name.
        device_name = "Brilliant BLE Mesh"
        model = "BLE Mesh"
    else:
        # Display name only — identifiers/topics/unique_id keep the raw slug.
        panel_display = e.panel.replace("_", " ").replace("-", " ").title()
        device_name = f"Brilliant {panel_display}"
        model = "Control"

    device: dict[str, object] = {
        "identifiers": [f"brilliant_panel_{e.panel}"],
        "manufacturer": "Brilliant",
        "model": model,
        "name": device_name,
    }
    if sw_version is not None:
        device["sw_version"] = sw_version

    payload: dict[str, object] = {
        "availability": [{"topic": _avail_topic}],
        "device": device,
        "name": e.name,
        "state_topic": _state_topic,
        "unique_id": e.unique_id,
    }

    if e.component == "light":
        payload["brightness"] = e.supports_brightness
        payload["command_topic"] = _cmd_topic
        payload["schema"] = "json"

    elif e.component == "switch":
        if e.command_var is not None:
            # Aux switch: plain ON/OFF on a per-variable command topic.
            payload["command_topic"] = aux_command_topic(e.panel, e.peripheral_id, e.command_var)
            payload["payload_off"] = "OFF"
            payload["payload_on"] = "ON"
            payload["state_off"] = "OFF"
            payload["state_on"] = "ON"
            payload["value_template"] = f"{{{{ 'ON' if value_json.{e.value_key} else 'OFF' }}}}"
        else:
            # Primary switch: JSON command payload (unchanged from pre-M10).
            payload["command_topic"] = _cmd_topic
            payload["payload_off"] = '{"state": "OFF"}'
            payload["payload_on"] = '{"state": "ON"}'
            payload["state_off"] = "OFF"
            payload["state_on"] = "ON"
            payload["value_template"] = "{{ value_json.state }}"

    elif e.component == "binary_sensor":
        if e.device_class is not None:
            payload["device_class"] = e.device_class
        payload["value_template"] = f"{{{{ 'ON' if value_json.{e.value_key} else 'OFF' }}}}"

    elif e.component == "sensor":
        if e.device_class is not None:
            payload["device_class"] = e.device_class
        if e.unit is not None:
            payload["unit_of_measurement"] = e.unit
        payload["value_template"] = f"{{{{ value_json.{e.value_key} }}}}"

    elif e.component == "number":
        payload["command_topic"] = aux_command_topic(e.panel, e.peripheral_id, e.command_var or "")
        # Omit unset bounds entirely: serializing them as JSON null makes HA
        # silently reject the whole discovery config.
        if e.max_value is not None:
            payload["max"] = e.max_value
        if e.min_value is not None:
            payload["min"] = e.min_value
        payload["mode"] = "slider"
        if e.step is not None:
            payload["step"] = e.step
        payload["value_template"] = f"{{{{ value_json.{e.value_key} }}}}"

    elif e.component == "button":
        # HA mqtt button is command-side only: no state_topic/value_template.
        payload.pop("state_topic", None)
        payload["command_topic"] = aux_command_topic(e.panel, e.peripheral_id, e.command_var or "")
        payload["payload_press"] = "PRESS"

    # Optional cross-component fields (added after the per-component block so a
    # component never has to special-case them).
    if e.entity_category is not None:
        payload["entity_category"] = e.entity_category
    if not e.enabled_by_default:
        payload["enabled_by_default"] = False
    if e.state_class is not None:
        payload["state_class"] = e.state_class

    return json.dumps(payload, sort_keys=True)

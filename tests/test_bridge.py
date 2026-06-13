"""Tests for the Bridge orchestrator (Milestone 6).

Written before the implementation (TDD). All tests are async using
pytest-asyncio in auto mode (plain async def test_* works).

Fixtures mirror the real office panel:
  - dimmer  : LIGHT, dimmable, off at 600/1000
  - motion  : BINARY_SENSOR with lux
  - always_on: UNKNOWN kind (should produce no entities)
"""

from __future__ import annotations

import json

import pytest

from brilliant_mqtt import __version__
from brilliant_mqtt.bridge import Bridge, _state_payload
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from tests.fakes import FakeBus, FakeMqtt

PANEL = "office"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def dimmer() -> BrilliantDevice:
    """Dimmable LIGHT, currently off, intensity 600/1000."""
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
def motion() -> BrilliantDevice:
    """BINARY_SENSOR with lux."""
    return BrilliantDevice(
        device_id="device_002",
        peripheral_id="faceplate_peripheral",
        name="Motion",
        kind=DeviceKind.BINARY_SENSOR,
        variables={
            "movement_detected": Variable("movement_detected", "0"),
            "lux": Variable("lux", "12.5"),
        },
    )


@pytest.fixture()
def always_on() -> BrilliantDevice:
    """UNKNOWN kind — should never produce any MQTT output."""
    return BrilliantDevice(
        device_id="device_003",
        peripheral_id="gangbox_peripheral_1",
        name="Always On",
        kind=DeviceKind.UNKNOWN,
        variables={},
    )


@pytest.fixture()
def switch_device() -> BrilliantDevice:
    """Non-dimmable SWITCH, currently off."""
    return BrilliantDevice(
        device_id="device_004",
        peripheral_id="switch_peripheral_0",
        name="Fan",
        kind=DeviceKind.SWITCH,
        variables={
            "on": Variable("on", "0"),
        },
    )


@pytest.fixture()
def non_dimmable_light() -> BrilliantDevice:
    """LIGHT without intensity variable — not dimmable."""
    return BrilliantDevice(
        device_id="device_005",
        peripheral_id="gangbox_peripheral_2",
        name="Ceiling",
        kind=DeviceKind.LIGHT,
        variables={
            "on": Variable("on", "0"),
        },
    )


# ---------------------------------------------------------------------------
# _state_payload unit tests
# ---------------------------------------------------------------------------


class TestStatePayload:
    def test_light_off(self, dimmer: BrilliantDevice) -> None:
        payload = json.loads(_state_payload(dimmer))
        assert payload == {"state": "OFF", "brightness": 153}

    def test_light_on_full(self, dimmer: BrilliantDevice) -> None:
        dimmer.variables = {
            **dimmer.variables,
            "on": Variable("on", "1"),
            "intensity": Variable("intensity", "1000"),
        }
        payload = json.loads(_state_payload(dimmer))
        assert payload == {"state": "ON", "brightness": 255}

    def test_light_brightness_calculation(self, dimmer: BrilliantDevice) -> None:
        # round(600/1000*255) == 153
        payload = json.loads(_state_payload(dimmer))
        assert payload["brightness"] == 153

    def test_non_dimmable_light_no_brightness(self, non_dimmable_light: BrilliantDevice) -> None:
        payload = json.loads(_state_payload(non_dimmable_light))
        assert "brightness" not in payload
        assert payload["state"] == "OFF"

    def test_binary_sensor_motion_false(self, motion: BrilliantDevice) -> None:
        payload = json.loads(_state_payload(motion))
        assert payload["motion"] is False
        assert payload["lux"] == 12.5

    def test_binary_sensor_no_lux(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Motion Only",
            kind=DeviceKind.BINARY_SENSOR,
            variables={"movement_detected": Variable("movement_detected", "1")},
        )
        payload = json.loads(_state_payload(device))
        assert payload == {"motion": True}
        assert "lux" not in payload

    def test_binary_sensor_motion_none_collapses_to_false(self) -> None:
        device = BrilliantDevice(
            device_id="d",
            peripheral_id="p",
            name="Sensor",
            kind=DeviceKind.BINARY_SENSOR,
            variables={},
        )
        payload = json.loads(_state_payload(device))
        assert payload["motion"] is False

    def test_switch_off(self, switch_device: BrilliantDevice) -> None:
        payload = json.loads(_state_payload(switch_device))
        assert payload == {"state": "OFF"}

    def test_switch_on(self, switch_device: BrilliantDevice) -> None:
        switch_device.variables = {"on": Variable("on", "1")}
        payload = json.loads(_state_payload(switch_device))
        assert payload == {"state": "ON"}


# ---------------------------------------------------------------------------
# Bridge.reconcile
# ---------------------------------------------------------------------------


class TestReconcile:
    async def test_availability_published_retained(
        self, dimmer: BrilliantDevice, motion: BrilliantDevice, always_on: BrilliantDevice
    ) -> None:
        bus = FakeBus([dimmer, motion, always_on])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        avail = [p for p in mqtt.published if p[0] == f"brilliant/{PANEL}/availability"]
        assert len(avail) == 1
        assert avail[0] == (f"brilliant/{PANEL}/availability", "online", True)

    async def test_light_config_published_retained(
        self, dimmer: BrilliantDevice, always_on: BrilliantDevice
    ) -> None:
        bus = FakeBus([dimmer, always_on])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        config_topics = [
            p[0] for p in mqtt.published if p[0].startswith("homeassistant/") and p[2] is True
        ]
        # Dimmer → one config topic
        assert any("gangbox_peripheral_0" in t for t in config_topics)

    async def test_light_state_retained_correct_payload(
        self, dimmer: BrilliantDevice, always_on: BrilliantDevice
    ) -> None:
        bus = FakeBus([dimmer, always_on])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        state_topic = f"brilliant/{PANEL}/gangbox_peripheral_0/state"
        state_publishes = [p for p in mqtt.published if p[0] == state_topic]
        assert len(state_publishes) == 1
        payload = json.loads(state_publishes[0][1])
        assert payload == {"state": "OFF", "brightness": 153}
        assert state_publishes[0][2] is True  # retained

    async def test_motion_two_configs_one_state(self, motion: BrilliantDevice) -> None:
        bus = FakeBus([motion])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        # binary_sensor config + sensor (lux) config → two config topics
        config_topics = [p[0] for p in mqtt.published if p[0].startswith("homeassistant/")]
        assert len(config_topics) == 2

        # But exactly ONE state publish for faceplate_peripheral
        state_topic = f"brilliant/{PANEL}/faceplate_peripheral/state"
        state_publishes = [p for p in mqtt.published if p[0] == state_topic]
        assert len(state_publishes) == 1

        payload = json.loads(state_publishes[0][1])
        assert payload == {"lux": 12.5, "motion": False}

    async def test_unknown_device_no_topics(self, always_on: BrilliantDevice) -> None:
        bus = FakeBus([always_on])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        # Only the availability topic should be published
        assert all("gangbox_peripheral_1" not in p[0] for p in mqtt.published)

    async def test_only_light_subscribed_not_sensor(
        self, dimmer: BrilliantDevice, motion: BrilliantDevice
    ) -> None:
        bus = FakeBus([dimmer, motion])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        assert mqtt.subscriptions == [f"brilliant/{PANEL}/gangbox_peripheral_0/set"]

    async def test_reconcile_idempotent(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        # Calling twice must not raise
        await bridge.reconcile()
        await bridge.reconcile()


# ---------------------------------------------------------------------------
# Bridge._on_change
# ---------------------------------------------------------------------------


class TestOnChange:
    async def test_on_change_updates_state(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        # Clear published record to only track subsequent publishes
        mqtt.published.clear()

        updated = BrilliantDevice(
            device_id=dimmer.device_id,
            peripheral_id=dimmer.peripheral_id,
            name=dimmer.name,
            kind=dimmer.kind,
            variables={
                "on": Variable("on", "1"),
                "intensity": Variable("intensity", "1000"),
                "max_intensity_value": Variable("max_intensity_value", "1000"),
            },
        )
        await bus.emit(updated)

        state_topic = f"brilliant/{PANEL}/gangbox_peripheral_0/state"
        state_publishes = [p for p in mqtt.published if p[0] == state_topic]
        assert len(state_publishes) == 1
        payload = json.loads(state_publishes[0][1])
        assert payload == {"state": "ON", "brightness": 255}
        assert state_publishes[0][2] is True

    async def test_on_change_unknown_device_publishes_nothing(
        self, always_on: BrilliantDevice
    ) -> None:
        bus = FakeBus([])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        mqtt.published.clear()
        await bus.emit(always_on)
        assert mqtt.published == []


# ---------------------------------------------------------------------------
# Bridge._on_command
# ---------------------------------------------------------------------------


class TestOnCommand:
    async def test_command_on_with_brightness(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        cmd_topic = f"brilliant/{PANEL}/gangbox_peripheral_0/set"
        await mqtt.inject(cmd_topic, json.dumps({"state": "ON", "brightness": 255}))

        assert bus.commands == [
            ("device_001", "gangbox_peripheral_0", [VarSet("on", "1"), VarSet("intensity", "1000")])
        ]

    async def test_command_switch_off(self, switch_device: BrilliantDevice) -> None:
        bus = FakeBus([switch_device])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        cmd_topic = f"brilliant/{PANEL}/switch_peripheral_0/set"
        await mqtt.inject(cmd_topic, json.dumps({"state": "OFF"}))

        assert bus.commands == [("device_004", "switch_peripheral_0", [VarSet("on", "0")])]

    async def test_command_uses_current_snapshot(self, dimmer: BrilliantDevice) -> None:
        """After an on_change, commands still translate against the stored (updated) device."""
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        # Emit an update — dimmable stays True, but on=True now
        updated = BrilliantDevice(
            device_id=dimmer.device_id,
            peripheral_id=dimmer.peripheral_id,
            name=dimmer.name,
            kind=dimmer.kind,
            variables={
                "on": Variable("on", "1"),
                "intensity": Variable("intensity", "1000"),
                "max_intensity_value": Variable("max_intensity_value", "1000"),
            },
        )
        await bus.emit(updated)
        bus.commands.clear()

        cmd_topic = f"brilliant/{PANEL}/gangbox_peripheral_0/set"
        await mqtt.inject(cmd_topic, json.dumps({"state": "ON", "brightness": 128}))

        # Device is still dimmable — VarSet("intensity", ...) should be present
        assert len(bus.commands) == 1
        device_id, pid, sets = bus.commands[0]
        assert device_id == "device_001"
        assert pid == "gangbox_peripheral_0"
        intensity_sets = [s for s in sets if s.name == "intensity"]
        assert len(intensity_sets) == 1

    async def test_malformed_json_ignored(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        cmd_topic = f"brilliant/{PANEL}/gangbox_peripheral_0/set"
        await mqtt.inject(cmd_topic, "not json")

        assert bus.commands == []

    async def test_non_dict_json_ignored(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        cmd_topic = f"brilliant/{PANEL}/gangbox_peripheral_0/set"
        await mqtt.inject(cmd_topic, json.dumps([1, 2, 3]))

        assert bus.commands == []

    async def test_unknown_topic_ignored(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        await mqtt.inject("brilliant/office/unknown_peripheral/set", json.dumps({"state": "ON"}))

        assert bus.commands == []


# ===========================================================================
# M10 — extended entities (aux commands, sw_version, extra state topics)
# ===========================================================================
#
# Fixtures mirror REAL pilot-panel data (poc-findings §6 + live probe).


@pytest.fixture()
def hardware() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="device_hw",
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


@pytest.fixture()
def ui_device() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="device_ui",
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


@pytest.fixture()
def wifi_device() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="device_wifi",
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


@pytest.fixture()
def always_on_powered() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="device_ao",
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


class TestM10StatePayloadDelegation:
    def test_always_on_payload(self, always_on_powered: BrilliantDevice) -> None:
        payload = json.loads(_state_payload(always_on_powered))
        assert payload == {"power": 52.0, "temperature": 43.6, "fault": False}

    def test_hardware_payload(self, hardware: BrilliantDevice) -> None:
        payload = json.loads(_state_payload(hardware))
        assert payload["screen_brightness"] == 7
        assert payload["muted"] is False
        assert payload["camera_on"] is False


class TestM10ReconcileStateTopics:
    async def test_always_on_state_published(self, always_on_powered: BrilliantDevice) -> None:
        bus = FakeBus([always_on_powered])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        topic = f"brilliant/{PANEL}/gangbox_peripheral_1/state"
        states = [p for p in mqtt.published if p[0] == topic]
        assert len(states) == 1
        assert json.loads(states[0][1]) == {"power": 52.0, "temperature": 43.6, "fault": False}

    async def test_hardware_ui_wifi_state_published(
        self,
        hardware: BrilliantDevice,
        ui_device: BrilliantDevice,
        wifi_device: BrilliantDevice,
    ) -> None:
        bus = FakeBus([hardware, ui_device, wifi_device])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        for pid in ("hardware_peripheral", "ui_peripheral", "wifi_peripheral"):
            topic = f"brilliant/{PANEL}/{pid}/state"
            states = [p for p in mqtt.published if p[0] == topic]
            assert len(states) == 1, f"expected one state publish for {pid}"


class TestM10SwVersionThreading:
    async def test_sw_version_in_all_configs(
        self, dimmer: BrilliantDevice, hardware: BrilliantDevice
    ) -> None:
        bus = FakeBus([dimmer, hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        configs = [p for p in mqtt.published if p[0].startswith("homeassistant/")]
        assert configs  # sanity
        for _topic, payload, _retain in configs:
            data = json.loads(payload)
            assert data["device"]["sw_version"] == "v26.05.20.2"

    async def test_no_sw_version_without_hardware(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        configs = [p for p in mqtt.published if p[0].startswith("homeassistant/")]
        for _topic, payload, _retain in configs:
            data = json.loads(payload)
            assert "sw_version" not in data["device"]


class TestM10AuxSubscriptions:
    async def test_aux_command_topics_subscribed(self, hardware: BrilliantDevice) -> None:
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        # muted (switch), screen_brightness/output_volume/alert_volume (number)
        # are commandable; camera_on/privacy_toggle/cpu_temperature are not.
        assert f"brilliant/{PANEL}/hardware_peripheral/set_muted" in mqtt.subscriptions
        assert f"brilliant/{PANEL}/hardware_peripheral/set_screen_brightness" in mqtt.subscriptions
        assert f"brilliant/{PANEL}/hardware_peripheral/set_output_volume" in mqtt.subscriptions
        # Diagnostic-only entities are NOT subscribed.
        subs = mqtt.subscriptions
        assert f"brilliant/{PANEL}/hardware_peripheral/set_camera_on" not in subs
        assert f"brilliant/{PANEL}/hardware_peripheral/set_cpu_temperature" not in subs

    async def test_button_topic_subscribed(self, ui_device: BrilliantDevice) -> None:
        bus = FakeBus([ui_device])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        assert f"brilliant/{PANEL}/ui_peripheral/set_request_identify" in mqtt.subscriptions
        # active is a binary_sensor (read-only) — no command topic.
        assert f"brilliant/{PANEL}/ui_peripheral/set_active" not in mqtt.subscriptions

    async def test_primary_topic_still_subscribed_with_aux(self, dimmer: BrilliantDevice) -> None:
        """A LIGHT with monitoring vars still subscribes its primary JSON topic."""
        dimmer.variables = {
            **dimmer.variables,
            "power": Variable("power", "0"),
            "is_safe": Variable("is_safe", "1"),
        }
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        assert f"brilliant/{PANEL}/gangbox_peripheral_0/set" in mqtt.subscriptions
        # is_safe is a binary_sensor → no aux command topic.
        assert f"brilliant/{PANEL}/gangbox_peripheral_0/set_is_safe" not in mqtt.subscriptions


class TestM10AuxCommands:
    async def test_switch_on_injects_one(self, hardware: BrilliantDevice) -> None:
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_muted", "ON")
        assert bus.commands == [("device_hw", "hardware_peripheral", [VarSet("muted", "1")])]

    async def test_switch_off_injects_zero(self, hardware: BrilliantDevice) -> None:
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_screen_on", "OFF")
        assert bus.commands == [("device_hw", "hardware_peripheral", [VarSet("screen_on", "0")])]

    async def test_number_inject(self, hardware: BrilliantDevice) -> None:
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_screen_brightness", "5")
        assert bus.commands == [
            ("device_hw", "hardware_peripheral", [VarSet("screen_brightness", "5")])
        ]

    async def test_number_clamped(self, hardware: BrilliantDevice) -> None:
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        # screen_brightness max is 10 → 150 clamps to 10.
        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_screen_brightness", "150")
        assert bus.commands == [
            ("device_hw", "hardware_peripheral", [VarSet("screen_brightness", "10")])
        ]

    async def test_button_inject(self, ui_device: BrilliantDevice) -> None:
        bus = FakeBus([ui_device])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{PANEL}/ui_peripheral/set_request_identify", "PRESS")
        assert bus.commands == [("device_ui", "ui_peripheral", [VarSet("request_identify", "1")])]

    async def test_garbage_aux_payload_ignored(self, hardware: BrilliantDevice) -> None:
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_screen_brightness", "abc")
        assert bus.commands == []

    async def test_aux_and_primary_coexist(self, dimmer: BrilliantDevice) -> None:
        """A LIGHT's primary JSON command and a faceplate aux command both route."""
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        await mqtt.inject(
            f"brilliant/{PANEL}/gangbox_peripheral_0/set",
            json.dumps({"state": "ON", "brightness": 255}),
        )
        assert bus.commands == [
            ("device_001", "gangbox_peripheral_0", [VarSet("on", "1"), VarSet("intensity", "1000")])
        ]


class TestOptimisticEcho:
    """After a successful bus write, the bridge republishes state immediately.

    Pilot finding: the bus never pushed a notification for a successful muted=1
    write, leaving HA stale until the periodic resync.
    """

    async def test_aux_write_echoes_state(self, hardware: BrilliantDevice) -> None:
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_muted", "ON")

        topic = f"brilliant/{PANEL}/hardware_peripheral/state"
        states = [p for p in mqtt.published if p[0] == topic]
        assert len(states) == 1
        payload = json.loads(states[0][1])
        assert payload["muted"] is True  # commanded value, not the stale "0"
        assert payload["screen_brightness"] == 7  # other fields preserved
        assert states[0][2] is True  # retained

    async def test_aux_number_write_echoes_state(self, hardware: BrilliantDevice) -> None:
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_screen_brightness", "5")

        topic = f"brilliant/{PANEL}/hardware_peripheral/state"
        states = [p for p in mqtt.published if p[0] == topic]
        assert len(states) == 1
        assert json.loads(states[0][1])["screen_brightness"] == 5

    async def test_primary_write_echoes_state(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        await mqtt.inject(
            f"brilliant/{PANEL}/gangbox_peripheral_0/set",
            json.dumps({"state": "ON", "brightness": 255}),
        )

        topic = f"brilliant/{PANEL}/gangbox_peripheral_0/state"
        states = [p for p in mqtt.published if p[0] == topic]
        assert len(states) == 1
        assert json.loads(states[0][1]) == {"state": "ON", "brightness": 255}
        assert states[0][2] is True

    async def test_failed_aux_translate_no_publish(self, hardware: BrilliantDevice) -> None:
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_screen_brightness", "abc")

        assert mqtt.published == []

    async def test_failed_primary_translate_no_publish(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        await mqtt.inject(f"brilliant/{PANEL}/gangbox_peripheral_0/set", "not json")
        await mqtt.inject(f"brilliant/{PANEL}/gangbox_peripheral_0/set", json.dumps({}))

        assert mqtt.published == []

    async def test_echo_updates_stored_snapshot_not_shared_dict(
        self, hardware: BrilliantDevice
    ) -> None:
        """The echo builds a new variables dict; the original device is untouched."""
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_muted", "ON")

        # The fixture object handed to reconcile keeps its original value...
        assert hardware.variables["muted"].value == "0"
        # ...while a follow-up command sees (and re-echoes) the updated snapshot.
        mqtt.published.clear()
        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_screen_on", "OFF")
        payload = json.loads(mqtt.published[-1][1])
        assert payload["muted"] is True
        assert payload["screen_on"] is False


# ---------------------------------------------------------------------------
# Bridge.poll_once + state payload de-duplication
# ---------------------------------------------------------------------------


def _dimmer_on(dimmer: BrilliantDevice) -> BrilliantDevice:
    """A copy of the *dimmer* fixture turned fully on (payload differs from off)."""
    return BrilliantDevice(
        device_id=dimmer.device_id,
        peripheral_id=dimmer.peripheral_id,
        name=dimmer.name,
        kind=dimmer.kind,
        variables={
            "on": Variable("on", "1"),
            "intensity": Variable("intensity", "1000"),
            "max_intensity_value": Variable("max_intensity_value", "1000"),
        },
    )


class TestRealtimePolling:
    """poll_once() publishes only changed payloads; all paths share the diff cache.

    Root cause (pilot, 2026-06-12): the observer's notification stream can die
    silently, freezing both pushes AND the get_all mirror until the processor
    reconnects. The hot poll bounds staleness at the poll cadence; diffing the
    payload keeps the fast cadence silent on MQTT while nothing changes.
    """

    async def test_poll_unchanged_publishes_nothing(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        await bridge.poll_once()
        assert mqtt.published == []

    async def test_poll_changed_variable_publishes_once(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        bus.set_devices([_dimmer_on(dimmer)])
        await bridge.poll_once()

        topic = f"brilliant/{PANEL}/gangbox_peripheral_0/state"
        states = [p for p in mqtt.published if p[0] == topic]
        assert len(states) == 1
        assert json.loads(states[0][1]) == {"state": "ON", "brightness": 255}
        assert states[0][2] is True

        # The same data polled again must not republish.
        mqtt.published.clear()
        await bridge.poll_once()
        assert mqtt.published == []

    async def test_poll_updates_snapshot_for_commands(self, dimmer: BrilliantDevice) -> None:
        """Commands translate against the freshest polled snapshot."""
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        bus.set_devices([_dimmer_on(dimmer)])
        await bridge.poll_once()
        bus.commands.clear()

        await mqtt.inject(
            f"brilliant/{PANEL}/gangbox_peripheral_0/set",
            json.dumps({"state": "ON", "brightness": 128}),
        )
        assert len(bus.commands) == 1
        _, _, sets = bus.commands[0]
        assert VarSet("intensity", "502") in sets

    async def test_poll_skips_entityless_devices(self, always_on: BrilliantDevice) -> None:
        bus = FakeBus([])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        bus.set_devices([always_on])
        await bridge.poll_once()
        assert mqtt.published == []

    async def test_poll_new_peripheral_state_without_discovery(
        self, dimmer: BrilliantDevice, motion: BrilliantDevice
    ) -> None:
        """A peripheral first seen by the poll gets state only; discovery waits
        for the next reconcile (mirrors _on_change behaviour)."""
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        bus.set_devices([dimmer, motion])
        await bridge.poll_once()

        topics = [p[0] for p in mqtt.published]
        assert f"brilliant/{PANEL}/faceplate_peripheral/state" in topics
        assert not any(t.startswith("homeassistant/") for t in topics)

    async def test_push_then_poll_dedupes(self, dimmer: BrilliantDevice) -> None:
        """A push followed by a poll of the same data publishes exactly once."""
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        updated = _dimmer_on(dimmer)
        await bus.emit(updated)
        bus.set_devices([updated])
        await bridge.poll_once()

        topic = f"brilliant/{PANEL}/gangbox_peripheral_0/state"
        assert len([p for p in mqtt.published if p[0] == topic]) == 1

    async def test_identical_push_suppressed(self, dimmer: BrilliantDevice) -> None:
        """A push carrying a payload identical to the last publish is dropped."""
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        same = BrilliantDevice(
            device_id=dimmer.device_id,
            peripheral_id=dimmer.peripheral_id,
            name=dimmer.name,
            kind=dimmer.kind,
            variables=dict(dimmer.variables),
        )
        await bus.emit(same)
        assert mqtt.published == []

    async def test_repeat_echo_suppressed(self, hardware: BrilliantDevice) -> None:
        """Re-commanding the already-echoed value writes the bus but skips MQTT."""
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_muted", "ON")
        mqtt.published.clear()
        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_muted", "ON")

        assert len(bus.commands) == 2
        assert mqtt.published == []

    async def test_reconcile_republishes_unchanged(self, dimmer: BrilliantDevice) -> None:
        """reconcile stays level-triggered: it force-publishes even when unchanged."""
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        mqtt.published.clear()

        await bridge.reconcile()
        topic = f"brilliant/{PANEL}/gangbox_peripheral_0/state"
        assert len([p for p in mqtt.published if p[0] == topic]) == 1


# ===========================================================================
# M11 Step 1 — routed writes: every bus write carries the OWNING device id
# ===========================================================================
#
# Mesh peripherals live on the virtual "ble_mesh" bus device, so the bridge
# can no longer assume every write targets the panel's own CONTROL device:
# the snapshot's device_id travels with each set_variables call.


class TestM11RoutedWrites:
    async def test_primary_command_routes_with_device_id(self, dimmer: BrilliantDevice) -> None:
        bus = FakeBus([dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        cmd_topic = f"brilliant/{PANEL}/gangbox_peripheral_0/set"
        await mqtt.inject(cmd_topic, json.dumps({"state": "ON"}))

        assert bus.commands == [("device_001", "gangbox_peripheral_0", [VarSet("on", "1")])]

    async def test_aux_command_routes_with_device_id(self, hardware: BrilliantDevice) -> None:
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_muted", "ON")

        assert bus.commands == [("device_hw", "hardware_peripheral", [VarSet("muted", "1")])]

    async def test_aux_command_without_snapshot_is_dropped(self, hardware: BrilliantDevice) -> None:
        """An aux route whose snapshot is gone cannot be routed — no bus write."""
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        # Simulate the defensive case: the topic route exists but the device
        # snapshot does not, so the owning bus device is unknown.
        del bridge._devices["hardware_peripheral"]

        await mqtt.inject(f"brilliant/{PANEL}/hardware_peripheral/set_muted", "ON")

        assert bus.commands == []


# ===========================================================================
# M11 Step 2 — include-filter, withdraw(), mesh publisher end-to-end
# ===========================================================================
#
# Step 3 runs TWO Bridge instances on ONE shared bus in one process (the panel
# bridge excludes ble_mesh; the mesh bridge selects only it, panel slug
# "mesh"). The bus fan-out delivers every device to both, so each bridge must
# filter to its own scope.

MESH_PID = "018691f1749b000701c4e689967b8e62"
MESH_PANEL = "mesh"


def _is_mesh(device: BrilliantDevice) -> bool:
    return device.device_id == "ble_mesh"


def _is_panel(device: BrilliantDevice) -> bool:
    return device.device_id != "ble_mesh"


@pytest.fixture()
def mesh_dimmer() -> BrilliantDevice:
    """Mesh dimmer on the virtual ble_mesh device (live-verified shape).

    No max_intensity_value (the model falls back to 1000); power is the "-1"
    sentinel — no reading until the load is calibrated.
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


class TestM11IncludeFilter:
    async def test_reconcile_filters_to_mesh_devices(
        self, dimmer: BrilliantDevice, mesh_dimmer: BrilliantDevice
    ) -> None:
        bus = FakeBus([dimmer, mesh_dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        # The mesh dimmer IS bridged...
        assert any(MESH_PID in p[0] for p in mqtt.published)
        # ...while the panel dimmer produced no discovery/state/subscription...
        assert all("gangbox_peripheral_0" not in p[0] for p in mqtt.published)
        assert all("gangbox_peripheral_0" not in t for t in mqtt.subscriptions)
        # ...no snapshot...
        assert "gangbox_peripheral_0" not in bridge._devices
        # ...and no command routing under this bridge's namespace.
        await mqtt.inject(
            f"brilliant/{MESH_PANEL}/gangbox_peripheral_0/set", json.dumps({"state": "ON"})
        )
        assert bus.commands == []

    async def test_reconcile_filters_to_panel_devices(
        self, dimmer: BrilliantDevice, mesh_dimmer: BrilliantDevice
    ) -> None:
        """The mirror image: the panel bridge excludes the mesh device."""
        bus = FakeBus([dimmer, mesh_dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL, include=_is_panel)
        await bridge.reconcile()

        assert all(MESH_PID not in p[0] for p in mqtt.published)
        assert MESH_PID not in bridge._devices
        assert any("gangbox_peripheral_0" in p[0] for p in mqtt.published)

    async def test_poll_once_respects_filter(
        self, dimmer: BrilliantDevice, mesh_dimmer: BrilliantDevice
    ) -> None:
        bus = FakeBus([mesh_dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()
        mqtt.published.clear()

        # The shared bus now also reports the panel dimmer, turned ON so its
        # payload WOULD publish were it not filtered out.
        bus.set_devices([mesh_dimmer, _dimmer_on(dimmer)])
        await bridge.poll_once()

        assert all("gangbox_peripheral_0" not in p[0] for p in mqtt.published)
        assert "gangbox_peripheral_0" not in bridge._devices

    async def test_on_change_filtered_device_not_published_not_stored(
        self, dimmer: BrilliantDevice, mesh_dimmer: BrilliantDevice
    ) -> None:
        bus = FakeBus([mesh_dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()
        mqtt.published.clear()

        await bus.emit(_dimmer_on(dimmer))

        assert mqtt.published == []
        assert "gangbox_peripheral_0" not in bridge._devices

    async def test_sw_version_not_taken_from_filtered_hardware(
        self, hardware: BrilliantDevice, mesh_dimmer: BrilliantDevice
    ) -> None:
        """ble_mesh has no HARDWARE peripheral: the panel's firmware tag must
        not leak into the mesh device blocks through the shared get_all."""
        bus = FakeBus([hardware, mesh_dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        configs = [p for p in mqtt.published if p[0].startswith("homeassistant/")]
        assert configs
        for _topic, payload, _retain in configs:
            assert "sw_version" not in json.loads(payload)["device"]


class _FailingUnsubscribeMqtt(FakeMqtt):
    """FakeMqtt whose unsubscribe raises for one topic (broker hiccup)."""

    def __init__(self, fail_topic: str) -> None:
        super().__init__()
        self._fail_topic = fail_topic

    async def unsubscribe(self, topic: str) -> None:
        if topic == self._fail_topic:
            raise RuntimeError("unsubscribe rejected")
        await super().unsubscribe(topic)


class TestM11Withdraw:
    async def test_withdraw_unsubscribes_exactly_subscribed_topics(
        self, hardware: BrilliantDevice
    ) -> None:
        bus = FakeBus([hardware])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        subscribed = list(mqtt.subscriptions)
        assert subscribed  # sanity: hardware has commandable aux entities

        await bridge.withdraw()

        assert sorted(mqtt.unsubscriptions) == sorted(subscribed)
        assert mqtt.subscriptions == []  # net state: nothing still subscribed

    async def test_withdraw_clears_command_routing(self, mesh_dimmer: BrilliantDevice) -> None:
        bus = FakeBus([mesh_dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        await bridge.withdraw()
        await mqtt.inject(f"brilliant/{MESH_PANEL}/{MESH_PID}/set", json.dumps({"state": "ON"}))

        assert bus.commands == []

    async def test_reconcile_after_withdraw_rebuilds_fresh(
        self, mesh_dimmer: BrilliantDevice
    ) -> None:
        bus = FakeBus([mesh_dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()
        await bridge.withdraw()
        mqtt.published.clear()

        await bridge.reconcile()

        # Unchanged data is force-republished (the diff cache was forgotten).
        state = f"brilliant/{MESH_PANEL}/{MESH_PID}/state"
        assert len([p for p in mqtt.published if p[0] == state]) == 1
        assert f"brilliant/{MESH_PANEL}/{MESH_PID}/set" in mqtt.subscriptions
        # Commands route again, to the owning ble_mesh bus device.
        await mqtt.inject(f"brilliant/{MESH_PANEL}/{MESH_PID}/set", json.dumps({"state": "ON"}))
        assert bus.commands == [("ble_mesh", MESH_PID, [VarSet("on", "1")])]

    async def test_withdraw_before_any_reconcile_is_noop(self) -> None:
        bus = FakeBus([])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL)

        await bridge.withdraw()

        assert mqtt.unsubscriptions == []
        assert mqtt.published == []

    async def test_withdraw_survives_unsubscribe_failure(self, hardware: BrilliantDevice) -> None:
        """One broken unsubscribe must not leave the step-down half-done."""
        fail_topic = f"brilliant/{PANEL}/hardware_peripheral/set_muted"
        bus = FakeBus([hardware])
        mqtt = _FailingUnsubscribeMqtt(fail_topic)
        bridge = Bridge(bus, mqtt, PANEL)
        await bridge.reconcile()
        n_topics = len(mqtt.subscriptions)

        await bridge.withdraw()  # must not raise

        # Every OTHER topic was still unsubscribed...
        assert len(mqtt.unsubscriptions) == n_topics - 1
        assert fail_topic not in mqtt.unsubscriptions
        # ...and the routing cache is gone regardless: no bus write routes.
        await mqtt.inject(fail_topic, "ON")
        assert bus.commands == []


class TestM11MeshEndToEnd:
    """The full mesh publisher shape: pseudo-panel "mesh" through the EXISTING
    topic/unique_id builders, so entities are publisher-agnostic."""

    async def test_mesh_discovery_config_shape(self, mesh_dimmer: BrilliantDevice) -> None:
        bus = FakeBus([mesh_dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        assert ("brilliant/mesh/availability", "online", True) in mqtt.published

        config_topic = f"homeassistant/light/brilliant_mesh_{MESH_PID}/config"
        configs = [p for p in mqtt.published if p[0] == config_topic]
        assert len(configs) == 1
        data = json.loads(configs[0][1])
        assert data["availability"][0]["topic"] == "brilliant/mesh/availability"
        assert data["device"]["name"] == "Brilliant BLE Mesh"
        assert data["device"]["model"] == "BLE Mesh"
        assert data["state_topic"] == f"brilliant/mesh/{MESH_PID}/state"
        assert data["command_topic"] == f"brilliant/mesh/{MESH_PID}/set"
        # ble_mesh has no HARDWARE peripheral → no firmware tag on the block.
        assert "sw_version" not in data["device"]

    async def test_mesh_sentinel_power_publishes_no_power_config(
        self, mesh_dimmer: BrilliantDevice
    ) -> None:
        bus = FakeBus([mesh_dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        power_config = f"homeassistant/sensor/brilliant_mesh_{MESH_PID}_power/config"
        assert all(p[0] != power_config for p in mqtt.published)
        # The light's config is the ONLY discovery publish for this load.
        configs = [p for p in mqtt.published if p[0].startswith("homeassistant/")]
        assert len(configs) == 1

    async def test_mesh_state_payload(self, mesh_dimmer: BrilliantDevice) -> None:
        bus = FakeBus([mesh_dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        topic = f"brilliant/mesh/{MESH_PID}/state"
        states = [p for p in mqtt.published if p[0] == topic]
        assert len(states) == 1
        # brightness scales against the 1000 fallback; power is gated out.
        assert json.loads(states[0][1]) == {"state": "OFF", "brightness": 153}
        assert states[0][2] is True

    async def test_mesh_command_routes_to_ble_mesh_device(
        self, mesh_dimmer: BrilliantDevice
    ) -> None:
        bus = FakeBus([mesh_dimmer])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        await mqtt.inject(
            f"brilliant/mesh/{MESH_PID}/set",
            json.dumps({"state": "ON", "brightness": 255}),
        )

        assert bus.commands == [
            ("ble_mesh", MESH_PID, [VarSet("on", "1"), VarSet("intensity", "1000")])
        ]


# ---------------------------------------------------------------------------
# Bridge meta topic (agent_version + panel_firmware), Milestone 12
# ---------------------------------------------------------------------------


def _hardware_device(tag: str = "v26.05.20.2") -> BrilliantDevice:
    return BrilliantDevice(
        device_id="device_hw",
        peripheral_id="hardware_peripheral_0",
        name="Hardware",
        kind=DeviceKind.HARDWARE,
        variables={
            "current_release_tag": Variable("current_release_tag", tag),
        },
    )


async def test_reconcile_publishes_bridge_meta(dimmer: BrilliantDevice) -> None:
    bus = FakeBus([dimmer, _hardware_device()])
    mqtt = FakeMqtt()
    await Bridge(bus, mqtt, PANEL).reconcile()

    meta = [(t, p, r) for (t, p, r) in mqtt.published if t == f"brilliant/{PANEL}/bridge"]
    assert len(meta) == 1
    _topic, payload, retain = meta[0]
    assert retain is True
    assert json.loads(payload) == {
        "agent_version": __version__,
        "panel_firmware": "v26.05.20.2",
    }


async def test_reconcile_meta_omits_firmware_when_unknown(dimmer: BrilliantDevice) -> None:
    bus = FakeBus([dimmer])
    mqtt = FakeMqtt()
    await Bridge(bus, mqtt, PANEL).reconcile()

    payload = next(p for (t, p, _r) in mqtt.published if t == f"brilliant/{PANEL}/bridge")
    assert json.loads(payload) == {"agent_version": __version__}


async def test_mesh_bridge_publishes_no_meta(dimmer: BrilliantDevice) -> None:
    bus = FakeBus([dimmer])
    mqtt = FakeMqtt()
    await Bridge(bus, mqtt, "mesh").reconcile()

    assert not any(t == "brilliant/mesh/bridge" for (t, _p, _r) in mqtt.published)


async def test_reconcile_republishes_meta_on_firmware_change(dimmer: BrilliantDevice) -> None:
    """The contract the companion integration exists for: reconcile re-reads the
    firmware tag every pass, so an OTA between reconciles updates the meta."""
    bus = FakeBus([dimmer, _hardware_device()])
    mqtt = FakeMqtt()
    bridge = Bridge(bus, mqtt, PANEL)
    await bridge.reconcile()

    bus.set_devices([dimmer, _hardware_device(tag="v26.07.01.1")])
    await bridge.reconcile()

    meta = [p for (t, p, _r) in mqtt.published if t == f"brilliant/{PANEL}/bridge"]
    assert len(meta) == 2
    assert json.loads(meta[-1]) == {
        "agent_version": __version__,
        "panel_firmware": "v26.07.01.1",
    }


async def test_reconcile_meta_published_with_zero_devices() -> None:
    """agent_version is always present, even when the bus enumerates nothing."""
    bus = FakeBus([])
    mqtt = FakeMqtt()
    await Bridge(bus, mqtt, PANEL).reconcile()

    payload = next(p for (t, p, _r) in mqtt.published if t == f"brilliant/{PANEL}/bridge")
    assert json.loads(payload) == {"agent_version": __version__}


# ===========================================================================
# Mesh motion aux — bridge-level end-to-end command round-trips
# ===========================================================================
#
# The three writable motion entities (enable_motion_score, motion_high_threshold,
# motion_low_threshold) write to real BLE-mesh hardware via the virtual "ble_mesh"
# bus device.  The two read-only entities (movement_detected, motion_score) must
# NOT get command topics.  This block proves the full round-trip: HA publishes to
# brilliant/mesh/<pid>/set_<var>  →  bus.set_variables("ble_mesh", pid, [VarSet(...)]).


@pytest.fixture()
def mesh_load_with_motion() -> BrilliantDevice:
    """Mesh LIGHT on the virtual ble_mesh bus device with all five motion vars.

    Variables mirror the live-verified shape from panel-1.local (2026-06-13):
    on/intensity/display_name are the primary load vars; the five motion vars
    are the new BLE-mesh motion subsystem.  No power var (sentinel gate not
    relevant here; omitting keeps the fixture minimal).
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
            "display_name": Variable("display_name", "Office Desk Lights"),
            "movement_detected": Variable("movement_detected", "1"),
            "motion_score": Variable("motion_score", "0"),
            "enable_motion_score": Variable("enable_motion_score", "0"),
            "motion_high_threshold": Variable("motion_high_threshold", "70"),
            "motion_low_threshold": Variable("motion_low_threshold", "20"),
        },
    )


class TestMeshMotionAuxCommands:
    """Bridge-level end-to-end coverage for writable mesh motion aux entities."""

    async def test_writable_motion_topics_subscribed(
        self, mesh_load_with_motion: BrilliantDevice
    ) -> None:
        """After reconcile the bridge subscribes the three writable motion vars."""
        bus = FakeBus([mesh_load_with_motion])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        assert f"brilliant/{MESH_PANEL}/{MESH_PID}/set_enable_motion_score" in mqtt.subscriptions
        assert f"brilliant/{MESH_PANEL}/{MESH_PID}/set_motion_high_threshold" in mqtt.subscriptions
        assert f"brilliant/{MESH_PANEL}/{MESH_PID}/set_motion_low_threshold" in mqtt.subscriptions

    async def test_readonly_motion_topics_not_subscribed(
        self, mesh_load_with_motion: BrilliantDevice
    ) -> None:
        """Read-only motion entities (binary_sensor, sensor) must NOT be subscribed."""
        bus = FakeBus([mesh_load_with_motion])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        assert f"brilliant/{MESH_PANEL}/{MESH_PID}/set_movement_detected" not in mqtt.subscriptions
        assert f"brilliant/{MESH_PANEL}/{MESH_PID}/set_motion_score" not in mqtt.subscriptions

    async def test_switch_on_routes_to_ble_mesh(
        self, mesh_load_with_motion: BrilliantDevice
    ) -> None:
        """ON on set_enable_motion_score → VarSet("enable_motion_score", "1") routed to ble_mesh."""
        bus = FakeBus([mesh_load_with_motion])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{MESH_PANEL}/{MESH_PID}/set_enable_motion_score", "ON")

        assert bus.commands == [("ble_mesh", MESH_PID, [VarSet("enable_motion_score", "1")])]

    async def test_number_high_threshold_routes_to_ble_mesh(
        self, mesh_load_with_motion: BrilliantDevice
    ) -> None:
        """Numeric payload on set_motion_high_threshold → VarSet routed to ble_mesh."""
        bus = FakeBus([mesh_load_with_motion])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{MESH_PANEL}/{MESH_PID}/set_motion_high_threshold", "85")

        assert bus.commands == [("ble_mesh", MESH_PID, [VarSet("motion_high_threshold", "85")])]

    async def test_number_low_threshold_routes_to_ble_mesh(
        self, mesh_load_with_motion: BrilliantDevice
    ) -> None:
        """Numeric payload on set_motion_low_threshold → VarSet routed to ble_mesh."""
        bus = FakeBus([mesh_load_with_motion])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{MESH_PANEL}/{MESH_PID}/set_motion_low_threshold", "15")

        assert bus.commands == [("ble_mesh", MESH_PID, [VarSet("motion_low_threshold", "15")])]

    async def test_sequential_motion_commands_each_route_correctly(
        self, mesh_load_with_motion: BrilliantDevice
    ) -> None:
        """Three distinct motion writes all route to ble_mesh independently."""
        bus = FakeBus([mesh_load_with_motion])
        mqtt = FakeMqtt()
        bridge = Bridge(bus, mqtt, MESH_PANEL, include=_is_mesh)
        await bridge.reconcile()

        await mqtt.inject(f"brilliant/{MESH_PANEL}/{MESH_PID}/set_enable_motion_score", "ON")
        assert bus.commands[-1] == ("ble_mesh", MESH_PID, [VarSet("enable_motion_score", "1")])

        bus.commands.clear()
        await mqtt.inject(f"brilliant/{MESH_PANEL}/{MESH_PID}/set_motion_high_threshold", "85")
        assert bus.commands[-1] == ("ble_mesh", MESH_PID, [VarSet("motion_high_threshold", "85")])

        bus.commands.clear()
        await mqtt.inject(f"brilliant/{MESH_PANEL}/{MESH_PID}/set_motion_low_threshold", "15")
        assert bus.commands[-1] == ("ble_mesh", MESH_PID, [VarSet("motion_low_threshold", "15")])

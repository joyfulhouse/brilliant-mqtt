"""Brilliant MQTT advertisements become lifecycle-safe HA remote scanners."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import CALLBACK_TYPE, HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_mqtt_message
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.ble_protocol import (
    Advertisement,
    advertisement_topic,
    decode_advertisement,
)
from custom_components.brilliant_mqtt.ble_scanner import (
    BrilliantBleScannerBridge,
    BrilliantRemoteScanner,
    observer_status_topic,
)
from custom_components.brilliant_mqtt.const import (
    CONF_BLE_SCANNER_ENABLED,
    CONF_PANEL,
    CONFIG_ENTRY_VERSION,
    DOMAIN,
)
from tests.conftest import FakeBluetoothManager
from tests.test_init import ENTRY_DATA

FIXTURE_PATH = Path(__file__).parents[2] / "tests/fixtures/ble_observer_v1.json"
VALID = json.loads(FIXTURE_PATH.read_text())["valid_advertisement"]
ADAPTER_ADDRESS = "11:22:33:44:55:66"
BATTERY_UUID = "0000180f-0000-1000-8000-00805f9b34fb"
pytestmark = pytest.mark.allow_lingering_timers


def _payload(**changes: Any) -> str:
    value = dict(VALID["value"])
    value.update(changes)
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _message(
    payload: str | bytes,
    *,
    topic: str = "brilliant/ble/v1/shed/advertisement",
    retained: bool = False,
) -> ReceiveMessage:
    return ReceiveMessage(
        topic=topic,
        payload=payload,
        qos=0,
        retain=retained,
        subscribed_topic=advertisement_topic("shed"),
        timestamp=9_999.0,
    )


def _status_message(payload: str | bytes, *, retained: bool = True) -> ReceiveMessage:
    topic = observer_status_topic("shed")
    return ReceiveMessage(
        topic=topic,
        payload=payload,
        qos=0,
        retain=retained,
        subscribed_topic=topic,
        timestamp=9_999.0,
    )


class ScannerRegistrationRecorder:
    """Capture HA registration metadata and expose deterministic unregisters."""

    def __init__(self, events: list[str] | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self.unregister_count = 0
        self._events = events

    def __call__(
        self,
        hass: HomeAssistant,
        scanner: BrilliantRemoteScanner,
        *,
        connection_slots: int,
        source_domain: str,
        source_config_entry_id: str,
        source_device_id: str,
    ) -> CALLBACK_TYPE:
        if self._events is not None:
            self._events.append("register")
        self.calls.append(
            {
                "hass": hass,
                "scanner": scanner,
                "connection_slots": connection_slots,
                "source_domain": source_domain,
                "source_config_entry_id": source_config_entry_id,
                "source_device_id": source_device_id,
            }
        )

        def unregister() -> None:
            self.unregister_count += 1
            if self._events is not None:
                self._events.append("unregister")

        return unregister


@pytest.fixture
def scanner_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> ScannerRegistrationRecorder:
    recorder = ScannerRegistrationRecorder()
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.ble_scanner._register_scanner_api", recorder
    )
    return recorder


def _entry(panel: str = "shed") -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        unique_id=panel,
        data={
            **ENTRY_DATA,
            CONF_PANEL: panel,
            CONF_BLE_SCANNER_ENABLED: True,
        },
        version=CONFIG_ENTRY_VERSION,
    )


async def test_remote_scanner_feeds_complete_advertisement_at_ha_receipt_time(
    fake_bluetooth_manager: FakeBluetoothManager,
) -> None:
    scanner = BrilliantRemoteScanner(panel="shed", adapter_address=ADAPTER_ADDRESS)
    unsetup = scanner.async_setup()
    try:
        scanner.async_on_advertisement(
            decode_advertisement(
                VALID["encoded"], topic=advertisement_topic("shed"), retained=False
            ),
            advertisement_monotonic_time=456.75,
        )
    finally:
        unsetup()

    assert scanner.source == ADAPTER_ADDRESS
    assert scanner.adapter == "brilliant-shed"
    assert scanner.connectable is False
    info = scanner._previous_service_info["AA:BB:CC:DD:EE:FF"]
    assert info.source == ADAPTER_ADDRESS
    assert info.connectable is False
    assert info.address == "AA:BB:CC:DD:EE:FF"
    assert info.rssi == -61
    assert info.name == "Wallet"
    assert info.tx_power == -59
    assert info.service_uuids == [BATTERY_UUID]
    assert info.service_data == {BATTERY_UUID: bytes.fromhex("aabbcc")}
    assert info.manufacturer_data == {
        76: bytes.fromhex("021500112233445566778899aabbccddeeff00420007c5")
    }
    assert info.device.details["panel"] == "shed"
    assert info.time == 456.75


async def test_bridge_sets_up_and_registers_before_first_feed_with_zero_slots(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from homeassistant.components import mqtt

    events: list[str] = []
    registration = ScannerRegistrationRecorder(events)
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.ble_scanner._register_scanner_api", registration
    )
    original_setup = BrilliantRemoteScanner.async_setup
    original_feed = BrilliantRemoteScanner.async_on_advertisement
    original_subscribe = mqtt.async_subscribe

    async def tracked_subscribe(*args: Any, **kwargs: Any) -> CALLBACK_TYPE:
        topic = args[1]
        assert isinstance(topic, str)
        events.append(f"subscribe:{topic.rsplit('/', 1)[-1]}")
        return await original_subscribe(*args, **kwargs)

    def tracked_setup(scanner: BrilliantRemoteScanner) -> CALLBACK_TYPE:
        events.append("setup")
        unsetup = original_setup(scanner)

        def tracked_unsetup() -> None:
            events.append("unsetup")
            unsetup()

        return tracked_unsetup

    def tracked_feed(
        scanner: BrilliantRemoteScanner,
        advertisement: Advertisement,
        *,
        advertisement_monotonic_time: float,
    ) -> None:
        events.append("feed")
        original_feed(
            scanner,
            advertisement,
            advertisement_monotonic_time=advertisement_monotonic_time,
        )

    monkeypatch.setattr(BrilliantRemoteScanner, "async_setup", tracked_setup)
    monkeypatch.setattr(BrilliantRemoteScanner, "async_on_advertisement", tracked_feed)
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.ble_scanner.mqtt.async_subscribe",
        tracked_subscribe,
    )

    entry = _entry()
    bridge = BrilliantBleScannerBridge(
        hass,
        entry,
        device_id="panel-device-id",
        monotonic=lambda: 123.5,
    )
    await bridge.async_setup()
    bridge.async_handle_status(_status_message("online"))
    bridge.async_handle_advertisement(_message(VALID["encoded"]))

    assert events == [
        "subscribe:status",
        "subscribe:advertisement",
        "setup",
        "register",
        "feed",
    ]
    assert len(registration.calls) == 1
    call = registration.calls[0]
    scanner = call["scanner"]
    assert isinstance(scanner, BrilliantRemoteScanner)
    assert scanner.source == ADAPTER_ADDRESS
    assert scanner.connectable is False
    assert call["connection_slots"] == 0
    assert call["source_domain"] == DOMAIN
    assert call["source_config_entry_id"] == entry.entry_id
    assert call["source_device_id"] == "panel-device-id"
    assert scanner._previous_service_info["AA:BB:CC:DD:EE:FF"].time == 123.5
    assert bridge.diagnostics["last_packet_age_seconds"] == 0.0

    bridge.async_shutdown()
    assert events[-2:] == ["unregister", "unsetup"]


async def test_retained_topic_mismatch_and_malformed_packets_are_isolated(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    scanner_registration: ScannerRegistrationRecorder,
) -> None:
    entry = _entry()
    entry.add_to_hass(hass)
    bridge = BrilliantBleScannerBridge(hass, entry, device_id="panel-device-id")
    await bridge.async_setup()
    bridge.async_handle_status(_status_message("online"))

    bridge.async_handle_advertisement(_message(VALID["encoded"], retained=True))
    bridge.async_handle_advertisement(
        _message(VALID["encoded"], topic=advertisement_topic("office"))
    )
    bridge.async_handle_advertisement(_message("not-json"))

    assert bridge.scanner is None
    assert scanner_registration.calls == []
    assert bridge.diagnostics["packets_received"] == 3
    assert bridge.diagnostics["packets_accepted"] == 0
    assert bridge.diagnostics["packets_dropped"] == 3
    assert mqtt_mock.is_active_subscription(advertisement_topic("shed"))
    assert entry.state is not ConfigEntryState.SETUP_ERROR

    bridge.async_shutdown()


async def test_ordering_accepts_new_generations_and_rejects_replays(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    scanner_registration: ScannerRegistrationRecorder,
) -> None:
    bridge = BrilliantBleScannerBridge(
        hass,
        _entry(),
        device_id="panel-device-id",
        monotonic=lambda: 88.0,
    )
    await bridge.async_setup()
    bridge.async_handle_status(_status_message("online"))
    old_boot = "123e4567-e89b-12d3-a456-426614174000"
    old_session = "223e4567-e89b-12d3-a456-426614174000"
    new_session = "323e4567-e89b-12d3-a456-426614174000"
    new_boot = "423e4567-e89b-12d3-a456-426614174000"
    newest_session = "523e4567-e89b-12d3-a456-426614174000"

    bridge.async_handle_advertisement(_message(_payload(sequence=42)))
    bridge.async_handle_advertisement(_message(_payload(sequence=42)))
    bridge.async_handle_advertisement(_message(_payload(sequence=41)))
    bridge.async_handle_advertisement(
        _message(_payload(boot_id=old_boot, session_id=new_session, sequence=1))
    )
    bridge.async_handle_advertisement(
        _message(_payload(boot_id=old_boot, session_id=old_session, sequence=43))
    )
    bridge.async_handle_advertisement(
        _message(_payload(boot_id=new_boot, session_id=newest_session, sequence=1))
    )
    bridge.async_handle_advertisement(
        _message(_payload(boot_id=old_boot, session_id=new_session, sequence=2))
    )

    assert len(scanner_registration.calls) == 1
    assert bridge.scanner is not None
    assert bridge.scanner._previous_service_info["AA:BB:CC:DD:EE:FF"].time == 88.0
    assert bridge.last_accepted_monotonic == 88.0
    assert bridge.diagnostics["packets_received"] == 7
    assert bridge.diagnostics["packets_accepted"] == 3
    assert bridge.diagnostics["packets_dropped"] == 4
    bridge.async_shutdown()


async def test_recovery_duplicate_rolls_back_only_the_speculative_scanner(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    scanner_registration: ScannerRegistrationRecorder,
) -> None:
    bridge = BrilliantBleScannerBridge(hass, _entry(), device_id="panel-device-id")
    await bridge.async_setup()
    bridge.async_handle_status(_status_message("online"))
    bridge.async_handle_advertisement(_message(_payload(sequence=42)))
    established_scanner = bridge.scanner
    assert established_scanner is not None

    bridge.async_handle_advertisement(_message(_payload(sequence=42)))

    assert bridge.scanner is established_scanner
    assert len(scanner_registration.calls) == 1
    assert scanner_registration.unregister_count == 0

    bridge.async_handle_status(_status_message("offline", retained=False))
    bridge.async_handle_status(_status_message("online", retained=False))
    bridge.async_handle_advertisement(_message(_payload(sequence=42)))

    assert bridge.scanner is None
    assert bridge.diagnostics["registered"] is False
    assert bridge.diagnostics["scanning"] is False
    assert len(scanner_registration.calls) == 2
    assert scanner_registration.unregister_count == 2
    assert bridge._sequence.last_sequence == 42
    assert bridge.diagnostics["packets_received"] == 3
    assert bridge.diagnostics["packets_accepted"] == 1
    assert bridge.diagnostics["packets_dropped"] == 2

    bridge.async_handle_advertisement(_message(_payload(sequence=43)))

    assert bridge.scanner is not None
    assert bridge.scanner is not established_scanner
    assert len(scanner_registration.calls) == 3
    assert scanner_registration.unregister_count == 2
    assert bridge._sequence.last_sequence == 43
    assert bridge.diagnostics["packets_received"] == 4
    assert bridge.diagnostics["packets_accepted"] == 2
    assert bridge.diagnostics["packets_dropped"] == 2
    bridge.async_shutdown()


async def test_recovery_prior_session_rolls_back_speculative_scanner_and_current_retries(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    scanner_registration: ScannerRegistrationRecorder,
) -> None:
    old_session = "223e4567-e89b-12d3-a456-426614174000"
    current_session = "323e4567-e89b-12d3-a456-426614174000"
    bridge = BrilliantBleScannerBridge(hass, _entry(), device_id="panel-device-id")
    await bridge.async_setup()
    bridge.async_handle_status(_status_message("online"))
    bridge.async_handle_advertisement(_message(_payload(session_id=old_session, sequence=42)))
    bridge.async_handle_advertisement(_message(_payload(session_id=current_session, sequence=1)))

    bridge.async_handle_status(_status_message("offline", retained=False))
    bridge.async_handle_status(_status_message("online", retained=False))
    bridge.async_handle_advertisement(_message(_payload(session_id=old_session, sequence=43)))

    assert bridge.scanner is None
    assert len(scanner_registration.calls) == 2
    assert scanner_registration.unregister_count == 2
    assert bridge._sequence.current_session_id == current_session
    assert bridge._sequence.last_sequence == 1
    assert bridge.diagnostics["packets_received"] == 3
    assert bridge.diagnostics["packets_accepted"] == 2
    assert bridge.diagnostics["packets_dropped"] == 1

    bridge.async_handle_advertisement(_message(_payload(session_id=current_session, sequence=2)))

    assert bridge.scanner is not None
    assert len(scanner_registration.calls) == 3
    assert scanner_registration.unregister_count == 2
    assert bridge._sequence.current_session_id == current_session
    assert bridge._sequence.last_sequence == 2
    assert bridge.diagnostics["packets_received"] == 4
    assert bridge.diagnostics["packets_accepted"] == 3
    assert bridge.diagnostics["packets_dropped"] == 1
    bridge.async_shutdown()


async def test_adapter_source_is_stable_and_rejection_does_not_advance_sequence(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    scanner_registration: ScannerRegistrationRecorder,
) -> None:
    bridge = BrilliantBleScannerBridge(hass, _entry(), device_id="panel-device-id")
    await bridge.async_setup()
    bridge.async_handle_status(_status_message("online"))
    bridge.async_handle_advertisement(_message(_payload(sequence=42)))
    bridge.async_handle_advertisement(
        _message(_payload(sequence=43, adapter_address="22:33:44:55:66:77"))
    )
    bridge.async_handle_advertisement(_message(_payload(sequence=43)))

    assert len(scanner_registration.calls) == 1
    assert bridge.scanner is not None
    assert bridge.scanner.source == ADAPTER_ADDRESS
    assert bridge.diagnostics["packets_accepted"] == 2
    assert bridge.diagnostics["packets_dropped"] == 1
    bridge.async_shutdown()


async def test_mqtt_status_replay_and_fresh_transitions_gate_sequence_and_scanner(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    scanner_registration: ScannerRegistrationRecorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge = BrilliantBleScannerBridge(hass, _entry(), device_id="panel-device-id")
    await bridge.async_setup()

    async_fire_mqtt_message(hass, advertisement_topic("shed"), _payload(sequence=42))
    await hass.async_block_till_done()
    assert bridge.diagnostics["observer_online"] is None
    assert bridge.scanner is None
    assert bridge.diagnostics["packets_dropped"] == 1

    async_fire_mqtt_message(hass, observer_status_topic("shed"), "offline", retain=True)
    await hass.async_block_till_done()
    assert bridge.diagnostics["observer_online"] is False

    async_fire_mqtt_message(hass, observer_status_topic("shed"), "online", retain=False)
    async_fire_mqtt_message(hass, advertisement_topic("shed"), _payload(sequence=42))
    await hass.async_block_till_done()
    first_scanner = bridge.scanner
    assert first_scanner is not None

    monkeypatch.setattr(
        BrilliantRemoteScanner,
        "time_since_last_detection",
        lambda _scanner: 10_000.0,
    )
    first_scanner._async_scanner_watchdog()
    assert bridge.diagnostics["scanning"] is False
    async_fire_mqtt_message(hass, advertisement_topic("shed"), _payload(sequence=43))
    await hass.async_block_till_done()
    assert bridge.diagnostics["scanning"] is True

    async_fire_mqtt_message(hass, observer_status_topic("shed"), "offline", retain=False)
    await hass.async_block_till_done()
    assert bridge.scanner is None
    assert scanner_registration.unregister_count == 1
    assert bridge.diagnostics["observer_online"] is False
    assert bridge.diagnostics["registered"] is False

    async_fire_mqtt_message(hass, advertisement_topic("shed"), _payload(sequence=44))
    await hass.async_block_till_done()
    assert bridge.scanner is None
    assert bridge.diagnostics["packets_accepted"] == 2

    async_fire_mqtt_message(hass, observer_status_topic("shed"), "online", retain=False)
    async_fire_mqtt_message(hass, advertisement_topic("shed"), _payload(sequence=44))
    await hass.async_block_till_done()
    assert bridge.scanner is not None
    assert bridge.scanner is not first_scanner
    assert bridge.scanner.source == ADAPTER_ADDRESS
    assert len(scanner_registration.calls) == 2
    assert bridge.diagnostics["observer_online"] is True
    assert bridge.diagnostics["packets_accepted"] == 3
    assert bridge.diagnostics["packets_received"] == 5
    assert bridge.diagnostics["packets_dropped"] == 2
    bridge.async_shutdown()


@pytest.mark.parametrize("bad_status", ["unknown", b"\xff"])
async def test_mqtt_unknown_or_non_utf8_status_closes_an_online_scanner(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    scanner_registration: ScannerRegistrationRecorder,
    bad_status: str | bytes,
) -> None:
    bridge = BrilliantBleScannerBridge(hass, _entry(), device_id="panel-device-id")
    await bridge.async_setup()

    async_fire_mqtt_message(hass, observer_status_topic("shed"), "online", retain=False)
    async_fire_mqtt_message(hass, advertisement_topic("shed"), VALID["encoded"])
    await hass.async_block_till_done()
    assert bridge.scanner is not None

    async_fire_mqtt_message(
        hass,
        observer_status_topic("shed"),
        bad_status,
        retain=False,
    )
    await hass.async_block_till_done()

    assert bridge.diagnostics["observer_online"] is False
    assert bridge.scanner is None
    assert scanner_registration.unregister_count == 1
    bridge.async_shutdown()


async def test_mqtt_non_utf8_advertisement_is_counted_and_dropped(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    scanner_registration: ScannerRegistrationRecorder,
) -> None:
    bridge = BrilliantBleScannerBridge(hass, _entry(), device_id="panel-device-id")
    await bridge.async_setup()

    async_fire_mqtt_message(hass, observer_status_topic("shed"), "online", retain=False)
    async_fire_mqtt_message(hass, advertisement_topic("shed"), b"\xff")
    await hass.async_block_till_done()

    assert bridge.scanner is None
    assert scanner_registration.calls == []
    assert bridge.diagnostics["packets_received"] == 1
    assert bridge.diagnostics["packets_accepted"] == 0
    assert bridge.diagnostics["packets_dropped"] == 1
    bridge.async_shutdown()


async def test_shutdown_unsubscribes_is_idempotent_and_supports_ha_restart(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    scanner_registration: ScannerRegistrationRecorder,
) -> None:
    entry = _entry()
    first = BrilliantBleScannerBridge(hass, entry, device_id="panel-device-id")
    await first.async_setup()
    first.async_handle_status(_status_message("online"))
    first.async_handle_advertisement(_message(VALID["encoded"]))
    assert mqtt_mock.is_active_subscription(advertisement_topic("shed"))
    assert mqtt_mock.is_active_subscription(observer_status_topic("shed"))

    first.async_shutdown()
    first.async_shutdown()
    assert not mqtt_mock.is_active_subscription(advertisement_topic("shed"))
    assert not mqtt_mock.is_active_subscription(observer_status_topic("shed"))
    assert scanner_registration.unregister_count == 1

    restarted = BrilliantBleScannerBridge(hass, entry, device_id="panel-device-id")
    await restarted.async_setup()
    restarted.async_handle_status(_status_message("online"))
    restarted.async_handle_advertisement(_message(VALID["encoded"]))
    assert len(scanner_registration.calls) == 2
    assert restarted.scanner is not None
    restarted.async_shutdown()


async def test_partial_mqtt_setup_failure_removes_first_subscription(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from homeassistant.components import mqtt

    real_subscribe = mqtt.async_subscribe
    calls = 0

    async def fail_second(*args: Any, **kwargs: Any) -> CALLBACK_TYPE:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("advertisement subscription failed")
        return await real_subscribe(*args, **kwargs)

    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.ble_scanner.mqtt.async_subscribe", fail_second
    )
    bridge = BrilliantBleScannerBridge(hass, _entry(), device_id="panel-device-id")
    with pytest.raises(RuntimeError, match="advertisement subscription failed"):
        await bridge.async_setup()

    assert not mqtt_mock.is_active_subscription(advertisement_topic("shed"))
    assert not mqtt_mock.is_active_subscription(observer_status_topic("shed"))


async def test_recovery_registration_failure_preserves_sequence_and_same_packet_retries(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_bluetooth_manager: FakeBluetoothManager,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unsetup_calls = 0
    registration_attempts = 0
    successful_registration = ScannerRegistrationRecorder()
    original_setup = BrilliantRemoteScanner.async_setup

    def tracked_setup(scanner: BrilliantRemoteScanner) -> CALLBACK_TYPE:
        unsetup = original_setup(scanner)

        def tracked_unsetup() -> None:
            nonlocal unsetup_calls
            unsetup_calls += 1
            unsetup()

        return tracked_unsetup

    def fail_recovery_registration_once(*args: Any, **kwargs: Any) -> CALLBACK_TYPE:
        nonlocal registration_attempts
        registration_attempts += 1
        if registration_attempts == 2:
            raise RuntimeError("Bluetooth registration failed")
        return successful_registration(*args, **kwargs)

    monkeypatch.setattr(BrilliantRemoteScanner, "async_setup", tracked_setup)
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.ble_scanner._register_scanner_api",
        fail_recovery_registration_once,
    )
    bridge = BrilliantBleScannerBridge(hass, _entry(), device_id="panel-device-id")
    await bridge.async_setup()
    bridge.async_handle_status(_status_message("online"))
    bridge.async_handle_advertisement(_message(_payload(sequence=42)))
    assert bridge.scanner is not None

    bridge.async_handle_status(_status_message("offline", retained=False))
    bridge.async_handle_status(_status_message("online", retained=False))

    bridge.async_handle_advertisement(_message(_payload(sequence=43)))

    assert bridge.scanner is None
    assert unsetup_calls == 2
    assert bridge._sequence.last_sequence == 42
    assert bridge.diagnostics["packets_received"] == 2
    assert bridge.diagnostics["packets_accepted"] == 1
    assert bridge.diagnostics["packets_dropped"] == 1

    bridge.async_handle_advertisement(_message(_payload(sequence=43)))

    assert registration_attempts == 3
    assert bridge.scanner is not None
    assert bridge._sequence.last_sequence == 43
    assert bridge.diagnostics["packets_received"] == 3
    assert bridge.diagnostics["packets_accepted"] == 2
    assert bridge.diagnostics["packets_dropped"] == 1
    assert bridge.diagnostics["packets_received"] == (
        bridge.diagnostics["packets_accepted"] + bridge.diagnostics["packets_dropped"]
    )
    bridge.async_shutdown()

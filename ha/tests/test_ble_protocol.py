"""Cross-runtime tests for the Brilliant BLE MQTT advertisement protocol."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from custom_components.brilliant_mqtt.ble_protocol import (
    MAX_COLLECTION_ENTRIES,
    MAX_COUNTER,
    MAX_DATA_BYTES,
    MAX_LOCAL_NAME_BYTES,
    MAX_RSSI,
    MAX_TX_POWER,
    MIN_RSSI,
    MIN_TX_POWER,
    Advertisement,
    AdvertisementSequenceTracker,
    advertisement_topic,
    decode_advertisement,
)

FIXTURE_PATH = Path(__file__).parents[2] / "tests/fixtures/ble_observer_v1.json"
VECTORS = json.loads(FIXTURE_PATH.read_text())
VALID = VECTORS["valid_advertisement"]
BATTERY_UUID = "0000180f-0000-1000-8000-00805f9b34fb"


def _payload(**changes: Any) -> str:
    value = dict(VALID["value"])
    value.update(changes)
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _decode(**changes: Any) -> Advertisement:
    return decode_advertisement(
        _payload(**changes), topic=advertisement_topic(changes.get("panel", "shed")), retained=False
    )


def test_shared_fixture_decodes_with_complete_byte_fields() -> None:
    advertisement = decode_advertisement(
        VALID["encoded"], topic="brilliant/ble/v1/shed/advertisement", retained=False
    )

    assert json.loads(VALID["encoded"]) == VALID["value"]
    assert advertisement == Advertisement(
        panel="shed",
        adapter_address="11:22:33:44:55:66",
        boot_id="123e4567-e89b-12d3-a456-426614174000",
        sequence=42,
        address="AA:BB:CC:DD:EE:FF",
        address_type="public",
        rssi=-61,
        local_name="Wallet",
        tx_power=-59,
        service_uuids=(BATTERY_UUID,),
        service_data={BATTERY_UUID: bytes.fromhex("aabbcc")},
        manufacturer_data={76: bytes.fromhex("021500112233445566778899aabbccddeeff00420007c5")},
        capture_monotonic_ms=123456789,
    )


def test_decoder_normalizes_wire_values() -> None:
    advertisement = decode_advertisement(
        _payload(
            adapter_address="11-22-33-44-55-66",
            address="aa-bb-cc-dd-ee-ff",
            address_type=" RANDOM ",
            boot_id="123E4567-E89B-12D3-A456-426614174000",
            service_uuids=[BATTERY_UUID.upper(), BATTERY_UUID],
            service_data={BATTERY_UUID.upper(): "AABBCC"},
        ),
        topic=advertisement_topic("shed"),
        retained=False,
    )

    assert advertisement.adapter_address == "11:22:33:44:55:66"
    assert advertisement.address == "AA:BB:CC:DD:EE:FF"
    assert advertisement.address_type == "random"
    assert advertisement.boot_id == "123e4567-e89b-12d3-a456-426614174000"
    assert advertisement.service_uuids == (BATTERY_UUID,)
    assert advertisement.service_data == {BATTERY_UUID: bytes.fromhex("aabbcc")}


def test_topic_builder_rejects_invalid_panel_slugs() -> None:
    assert advertisement_topic("shed") == "brilliant/ble/v1/shed/advertisement"
    for invalid in ("", "Shed", "shed/loft", "mesh", "a" * 64):
        with pytest.raises(ValueError, match="panel"):
            advertisement_topic(invalid)


def test_decoder_rejects_retained_or_mismatched_topic_delivery() -> None:
    with pytest.raises(ValueError, match="retained"):
        decode_advertisement(VALID["encoded"], topic=advertisement_topic("shed"), retained=True)
    with pytest.raises(ValueError, match="topic panel"):
        decode_advertisement(VALID["encoded"], topic=advertisement_topic("office"), retained=False)
    with pytest.raises(ValueError, match="topic"):
        decode_advertisement(VALID["encoded"], topic="brilliant/ble/v1/shed/status", retained=False)


@pytest.mark.parametrize("payload", ["not-json", "[]", "{}", b"\xff"])
def test_decoder_rejects_malformed_payloads(payload: str | bytes) -> None:
    with pytest.raises(ValueError, match="payload"):
        decode_advertisement(payload, topic=advertisement_topic("shed"), retained=False)


def test_decoder_rejects_unknown_version_and_fields() -> None:
    with pytest.raises(ValueError, match="version"):
        _decode(version=2)
    with pytest.raises(ValueError, match="unexpected"):
        _decode(private_identity="must-not-leak")


def test_decoder_rejects_duplicate_json_keys_at_any_depth() -> None:
    duplicate_panel = VALID["encoded"].replace('"panel":"shed"', '"panel":"shed","panel":"office"')
    duplicate_manufacturer = VALID["encoded"].replace(
        '"76":"021500112233445566778899aabbccddeeff00420007c5"',
        '"76":"021500112233445566778899aabbccddeeff00420007c5","76":"00"',
    )

    for payload in (duplicate_panel, duplicate_manufacturer):
        with pytest.raises(ValueError, match="duplicate JSON key"):
            decode_advertisement(payload, topic=advertisement_topic("shed"), retained=False)


@pytest.mark.parametrize("field", ["adapter_address", "address"])
@pytest.mark.parametrize("value", ["", "AA:BB:CC:DD:EE", "GG:BB:CC:DD:EE:FF"])
def test_decoder_rejects_invalid_bluetooth_addresses(field: str, value: str) -> None:
    with pytest.raises(ValueError, match="address"):
        _decode(**{field: value})


@pytest.mark.parametrize("address_type", ["", "unknown", "public/random", 1])
def test_decoder_rejects_invalid_address_type(address_type: object) -> None:
    with pytest.raises(ValueError, match="address_type"):
        _decode(address_type=address_type)


@pytest.mark.parametrize("sequence", [False, 0, -1, MAX_COUNTER + 1])
def test_decoder_rejects_invalid_sequence(sequence: object) -> None:
    with pytest.raises(ValueError, match="sequence"):
        _decode(sequence=sequence)


@pytest.mark.parametrize("capture_ms", [False, -1, MAX_COUNTER + 1])
def test_decoder_rejects_invalid_capture_time(capture_ms: object) -> None:
    with pytest.raises(ValueError, match="capture_monotonic_ms"):
        _decode(capture_monotonic_ms=capture_ms)


@pytest.mark.parametrize("rssi", [False, MIN_RSSI - 1, MAX_RSSI + 1])
def test_decoder_rejects_impossible_rssi(rssi: object) -> None:
    with pytest.raises(ValueError, match="rssi"):
        _decode(rssi=rssi)


@pytest.mark.parametrize("tx_power", [False, MIN_TX_POWER - 1, MAX_TX_POWER + 1])
def test_decoder_rejects_impossible_tx_power(tx_power: object) -> None:
    with pytest.raises(ValueError, match="tx_power"):
        _decode(tx_power=tx_power)


def test_decoder_accepts_exact_numeric_boundaries() -> None:
    advertisement = _decode(
        sequence=MAX_COUNTER,
        capture_monotonic_ms=MAX_COUNTER,
        rssi=MIN_RSSI,
        tx_power=MAX_TX_POWER,
    )
    assert advertisement.sequence == MAX_COUNTER
    assert advertisement.capture_monotonic_ms == MAX_COUNTER


def test_decoder_enforces_local_name_utf8_byte_bound() -> None:
    assert len("é".encode()) == 2
    assert _decode(local_name="é" * (MAX_LOCAL_NAME_BYTES // 2)).local_name is not None
    with pytest.raises(ValueError, match="local_name"):
        _decode(local_name="é" * ((MAX_LOCAL_NAME_BYTES // 2) + 1))


def test_decoder_enforces_64_entry_collection_bounds() -> None:
    uuids = [str(UUID(int=index + 1)) for index in range(MAX_COLLECTION_ENTRIES)]
    service_data = {value: "" for value in uuids}
    manufacturer_data = {str(index): "" for index in range(MAX_COLLECTION_ENTRIES)}

    assert len(_decode(service_uuids=uuids).service_uuids) == MAX_COLLECTION_ENTRIES
    assert len(_decode(service_data=service_data).service_data) == MAX_COLLECTION_ENTRIES
    assert len(_decode(manufacturer_data=manufacturer_data).manufacturer_data) == (
        MAX_COLLECTION_ENTRIES
    )
    with pytest.raises(ValueError, match="service_uuids"):
        _decode(service_uuids=[*uuids, str(UUID(int=MAX_COLLECTION_ENTRIES + 1))])
    with pytest.raises(ValueError, match="service_data"):
        _decode(service_data={**service_data, str(UUID(int=MAX_COLLECTION_ENTRIES + 1)): ""})
    with pytest.raises(ValueError, match="manufacturer_data"):
        _decode(manufacturer_data={**manufacturer_data, str(MAX_COLLECTION_ENTRIES): ""})


@pytest.mark.parametrize("field", ["service_data", "manufacturer_data"])
def test_decoder_enforces_binary_field_bounds_and_hex(field: str) -> None:
    key = BATTERY_UUID if field == "service_data" else "76"
    assert _decode(**{field: {key: "aa" * MAX_DATA_BYTES}})
    for invalid in ("0", "gg", "aa" * (MAX_DATA_BYTES + 1)):
        with pytest.raises(ValueError, match=field):
            _decode(**{field: {key: invalid}})


@pytest.mark.parametrize("key", ["-1", "65536", "076", "company"])
def test_decoder_rejects_invalid_manufacturer_identifiers(key: str) -> None:
    with pytest.raises(ValueError, match="manufacturer_data"):
        _decode(manufacturer_data={key: "00"})


def test_sequence_tracker_accepts_strictly_increasing_packets() -> None:
    tracker = AdvertisementSequenceTracker()

    tracker.accept(_decode(sequence=41))
    tracker.accept(_decode(sequence=42))
    assert tracker.current_boot_id == "123e4567-e89b-12d3-a456-426614174000"
    assert tracker.last_sequence == 42

    with pytest.raises(ValueError, match="duplicate"):
        tracker.accept(_decode(sequence=42))
    with pytest.raises(ValueError, match="out-of-order"):
        tracker.accept(_decode(sequence=40))


def test_sequence_tracker_accepts_new_boot_and_tombstones_old_boot() -> None:
    tracker = AdvertisementSequenceTracker()
    old_boot = "123e4567-e89b-12d3-a456-426614174000"
    new_boot = "223e4567-e89b-12d3-a456-426614174000"

    tracker.accept(_decode(boot_id=old_boot, sequence=900))
    tracker.accept(_decode(boot_id=new_boot, sequence=1))
    assert tracker.current_boot_id == new_boot
    assert tracker.last_sequence == 1

    with pytest.raises(ValueError, match="prior boot"):
        tracker.accept(_decode(boot_id=old_boot, sequence=901))

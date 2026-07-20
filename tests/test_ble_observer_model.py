"""Contract tests for the pure Brilliant BLE advertisement model."""

from __future__ import annotations

import json
from collections.abc import MutableMapping
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from brilliant_ble_observer.model import (
    MAX_COLLECTION_ENTRIES,
    MAX_COUNTER,
    MAX_DATA_BYTES,
    MAX_LOCAL_NAME_BYTES,
    MAX_RSSI,
    MAX_TX_POWER,
    MIN_RSSI,
    MIN_TX_POWER,
    AdvertisementEnvelope,
    AllowlistEntry,
    matches_allowlist,
    normalize_advertisement_instance,
    parse_allowlist,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures/ble_observer_v1.json"
VECTORS = json.loads(FIXTURE_PATH.read_text())
IBEACON_BYTES = bytes.fromhex("021500112233445566778899aabbccddeeff00420007c5")
BATTERY_UUID = "0000180f-0000-1000-8000-00805f9b34fb"


def _advertisement(**overrides: Any) -> AdvertisementEnvelope:
    values: dict[str, Any] = {
        "panel": "shed",
        "adapter_address": "11:22:33:44:55:66",
        "boot_id": "123e4567-e89b-12d3-a456-426614174000",
        "session_id": "223e4567-e89b-12d3-a456-426614174000",
        "sequence": 42,
        "address": "AA:BB:CC:DD:EE:FF",
        "address_type": "public",
        "rssi": -61,
        "local_name": "Wallet",
        "tx_power": -59,
        "service_uuids": (BATTERY_UUID,),
        "service_data": {BATTERY_UUID: bytes.fromhex("aabbcc")},
        "manufacturer_data": {76: IBEACON_BYTES},
        "capture_monotonic_ms": 123456789,
    }
    values.update(overrides)
    return AdvertisementEnvelope(**values)


def test_envelope_normalizes_and_encodes_shared_golden_vector() -> None:
    advertisement = _advertisement(
        adapter_address="11:22:33:44:55:66",
        address="aa-bb-cc-dd-ee-ff",
        address_type=" PUBLIC ",
        boot_id="123E4567-E89B-12D3-A456-426614174000",
        session_id="223E4567-E89B-12D3-A456-426614174000",
        local_name=" Wallet ",
        service_uuids=(BATTERY_UUID.upper(), BATTERY_UUID),
        service_data={BATTERY_UUID.upper(): bytearray.fromhex("aabbcc")},
        manufacturer_data={76: memoryview(IBEACON_BYTES)},
    )

    assert advertisement.version == 1
    assert advertisement.address == "AA:BB:CC:DD:EE:FF"
    assert advertisement.address_type == "public"
    assert advertisement.boot_id == "123e4567-e89b-12d3-a456-426614174000"
    assert advertisement.session_id == "223e4567-e89b-12d3-a456-426614174000"
    assert advertisement.local_name == "Wallet"
    assert advertisement.service_uuids == (BATTERY_UUID,)
    assert advertisement.service_data == {BATTERY_UUID: bytes.fromhex("aabbcc")}
    assert advertisement.manufacturer_data == {76: IBEACON_BYTES}
    assert advertisement.to_payload() == VECTORS["valid_advertisement"]["value"]
    assert advertisement.to_json() == VECTORS["valid_advertisement"]["encoded"]


def test_shared_normalization_seam_applies_every_common_field() -> None:
    """Observation and envelope models share one canonical normalize/apply operation."""
    raw = SimpleNamespace(
        adapter_address="11-22-33-44-55-66",
        address="aa-bb-cc-dd-ee-ff",
        address_type=" PUBLIC ",
        rssi=-61,
        local_name=" Wallet ",
        tx_power=-59,
        service_uuids=(BATTERY_UUID.upper(), BATTERY_UUID),
        service_data={BATTERY_UUID.upper(): bytearray.fromhex("aabbcc")},
        manufacturer_data={76: memoryview(IBEACON_BYTES)},
        capture_monotonic_ms=123456789,
    )

    normalize_advertisement_instance(raw)

    assert raw.adapter_address == "11:22:33:44:55:66"
    assert raw.address == "AA:BB:CC:DD:EE:FF"
    assert raw.address_type == "public"
    assert raw.local_name == "Wallet"
    assert raw.service_uuids == (BATTERY_UUID,)
    assert raw.service_data == {BATTERY_UUID: bytes.fromhex("aabbcc")}
    assert raw.manufacturer_data == {76: IBEACON_BYTES}


def test_envelope_is_deeply_immutable() -> None:
    advertisement = _advertisement()
    dynamically_typed = cast(Any, advertisement)

    with pytest.raises(FrozenInstanceError):
        dynamically_typed.panel = "office"
    with pytest.raises(TypeError):
        cast(MutableMapping[str, bytes], advertisement.service_data)[BATTERY_UUID] = b"changed"
    with pytest.raises(TypeError):
        cast(MutableMapping[int, bytes], advertisement.manufacturer_data)[76] = b"changed"


@pytest.mark.parametrize("panel", ["", "Shed", "shed/loft", "mesh", "a" * 64])
def test_envelope_rejects_invalid_panel_slug(panel: str) -> None:
    with pytest.raises(ValueError, match="panel"):
        _advertisement(panel=panel)


@pytest.mark.parametrize("field", ["adapter_address", "address"])
@pytest.mark.parametrize("value", ["", "AA:BB:CC:DD:EE", "GG:BB:CC:DD:EE:FF"])
def test_envelope_rejects_invalid_bluetooth_addresses(field: str, value: str) -> None:
    with pytest.raises(ValueError, match="address"):
        _advertisement(**{field: value})


@pytest.mark.parametrize("field", ["boot_id", "session_id"])
@pytest.mark.parametrize("value", ["", "not-a-uuid", 1])
def test_envelope_rejects_invalid_generation_uuid(field: str, value: object) -> None:
    with pytest.raises(ValueError, match=field):
        _advertisement(**{field: value})


@pytest.mark.parametrize("sequence", [False, 0, -1, MAX_COUNTER + 1])
def test_envelope_rejects_invalid_sequence(sequence: object) -> None:
    with pytest.raises(ValueError, match="sequence"):
        _advertisement(sequence=sequence)


@pytest.mark.parametrize("capture_ms", [False, -1, MAX_COUNTER + 1])
def test_envelope_rejects_invalid_capture_time(capture_ms: object) -> None:
    with pytest.raises(ValueError, match="capture_monotonic_ms"):
        _advertisement(capture_monotonic_ms=capture_ms)


@pytest.mark.parametrize("rssi", [False, MIN_RSSI - 1, MAX_RSSI + 1])
def test_envelope_rejects_impossible_rssi(rssi: object) -> None:
    with pytest.raises(ValueError, match="rssi"):
        _advertisement(rssi=rssi)


@pytest.mark.parametrize("tx_power", [False, MIN_TX_POWER - 1, MAX_TX_POWER + 1])
def test_envelope_rejects_impossible_tx_power(tx_power: object) -> None:
    with pytest.raises(ValueError, match="tx_power"):
        _advertisement(tx_power=tx_power)


def test_envelope_accepts_exact_numeric_boundaries() -> None:
    advertisement = _advertisement(
        sequence=MAX_COUNTER,
        capture_monotonic_ms=MAX_COUNTER,
        rssi=MIN_RSSI,
        tx_power=MAX_TX_POWER,
    )
    assert advertisement.sequence == MAX_COUNTER
    assert advertisement.capture_monotonic_ms == MAX_COUNTER
    assert advertisement.rssi == MIN_RSSI
    assert advertisement.tx_power == MAX_TX_POWER


def test_envelope_rejects_oversized_local_name() -> None:
    assert len("é".encode()) == 2
    with pytest.raises(ValueError, match="local_name"):
        _advertisement(local_name="é" * ((MAX_LOCAL_NAME_BYTES // 2) + 1))


def test_envelope_rejects_more_than_64_uuid_or_data_entries() -> None:
    uuids = tuple(str(UUID(int=index + 1)) for index in range(MAX_COLLECTION_ENTRIES + 1))
    service_data = {value: b"" for value in uuids}
    manufacturer_data = {index: b"" for index in range(MAX_COLLECTION_ENTRIES + 1)}

    with pytest.raises(ValueError, match="service_uuids"):
        _advertisement(service_uuids=uuids)
    with pytest.raises(ValueError, match="service_data"):
        _advertisement(service_data=service_data)
    with pytest.raises(ValueError, match="manufacturer_data"):
        _advertisement(manufacturer_data=manufacturer_data)


@pytest.mark.parametrize("field", ["service_data", "manufacturer_data"])
def test_envelope_rejects_oversized_binary_fields(field: str) -> None:
    oversized = b"x" * (MAX_DATA_BYTES + 1)
    value: object = {BATTERY_UUID: oversized} if field == "service_data" else {76: oversized}
    with pytest.raises(ValueError, match=field):
        _advertisement(**{field: value})


def test_static_address_allowlist_matches_normalized_address() -> None:
    allowlist = (AllowlistEntry(address="aa-bb-cc-dd-ee-ff"),)

    assert matches_allowlist(address="AA:BB:CC:DD:EE:FF", manufacturer_data={}, allowlist=allowlist)
    assert not matches_allowlist(
        address="AA:BB:CC:DD:EE:00", manufacturer_data={}, allowlist=allowlist
    )


def test_ibeacon_allowlist_matches_uuid_major_and_minor() -> None:
    allowlist = (
        AllowlistEntry(
            ibeacon_uuid="00112233-4455-6677-8899-aabbccddeeff",
            ibeacon_major=66,
            ibeacon_minor=7,
        ),
    )

    assert matches_allowlist(
        address="00:00:00:00:00:01",
        manufacturer_data={0x004C: IBEACON_BYTES},
        allowlist=allowlist,
    )
    assert not matches_allowlist(
        address="00:00:00:00:00:01",
        manufacturer_data={0x004C: IBEACON_BYTES[:20]},
        allowlist=allowlist,
    )
    different_minor = IBEACON_BYTES[:21] + b"\x08" + IBEACON_BYTES[22:]
    assert not matches_allowlist(
        address="00:00:00:00:00:01",
        manufacturer_data={0x004C: different_minor},
        allowlist=allowlist,
    )


def test_shared_allowlist_fixture_parses_into_immutable_entries() -> None:
    entries = parse_allowlist(VECTORS["allowlist"])

    assert entries == (
        AllowlistEntry(address="AA:BB:CC:DD:EE:FF"),
        AllowlistEntry(
            ibeacon_uuid="00112233-4455-6677-8899-aabbccddeeff",
            ibeacon_major=66,
            ibeacon_minor=7,
        ),
    )


@pytest.mark.parametrize(
    "value",
    [
        {},
        ["AA:BB:CC:DD:EE:FF"],
        [{"address": "AA:BB:CC:DD:EE:FF", "unknown": True}],
        [{"address": "AA:BB:CC:DD:EE:FF", "ibeacon_uuid": str(UUID(int=1))}],
        [{"ibeacon_uuid": str(UUID(int=1)), "ibeacon_major": 1}],
        [{"ibeacon_uuid": str(UUID(int=1)), "ibeacon_major": -1, "ibeacon_minor": 1}],
        [{"ibeacon_uuid": str(UUID(int=1)), "ibeacon_major": 1, "ibeacon_minor": 65536}],
    ],
)
def test_allowlist_parser_rejects_malformed_identity(value: object) -> None:
    with pytest.raises(ValueError, match="allowlist"):
        parse_allowlist(value)

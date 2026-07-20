"""Pure, bounded model for the Brilliant BLE advertisement wire contract."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields
from types import MappingProxyType
from typing import Any, cast
from uuid import UUID

PROTOCOL_VERSION = 1
MAX_COLLECTION_ENTRIES = 64
MAX_DATA_BYTES = 512
MAX_LOCAL_NAME_BYTES = 248
MAX_PAYLOAD_BYTES = 65_536
MAX_COUNTER = (1 << 63) - 1
MIN_RSSI = -127
MAX_RSSI = 20
MIN_TX_POWER = -127
MAX_TX_POWER = 20

_PANEL_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")
_MAC_PATTERN = re.compile(r"(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}")
_ADDRESS_TYPES = frozenset({"public", "random"})
_APPLE_COMPANY_ID = 0x004C
_IBEACON_PREFIX = b"\x02\x15"
_IBEACON_MIN_LENGTH = 23
_STATIC_ALLOWLIST_KEYS = frozenset({"address"})
_IBEACON_ALLOWLIST_KEYS = frozenset({"ibeacon_uuid", "ibeacon_major", "ibeacon_minor"})


def normalize_panel(panel: object) -> str:
    """Return a validated physical-panel MQTT slug."""
    if not isinstance(panel, str) or _PANEL_PATTERN.fullmatch(panel) is None or panel == "mesh":
        raise ValueError("panel must be a percent-free lowercase physical-panel slug")
    return panel


def normalize_address(address: object, *, field_name: str = "address") -> str:
    """Normalize a colon- or dash-separated Bluetooth MAC address."""
    if not isinstance(address, str):
        raise ValueError(f"{field_name} must be a Bluetooth address")
    normalized = address.strip().replace("-", ":").upper()
    if _MAC_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must be a Bluetooth address")
    return normalized


def normalize_uuid(value: object, *, field_name: str) -> str:
    """Normalize a UUID to its canonical lowercase representation."""
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a UUID")
    try:
        return str(UUID(value.strip()))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(f"{field_name} must be a UUID") from error


def _bounded_integer(value: object, *, field_name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{field_name} must be an integer from {minimum} through {maximum}")
    return value


def _normalize_local_name(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("local_name must be a string or null")
    normalized = value.strip()
    if not normalized or not all(character.isprintable() for character in normalized):
        raise ValueError("local_name must be a non-empty printable string")
    if len(normalized.encode("utf-8")) > MAX_LOCAL_NAME_BYTES:
        raise ValueError(f"local_name must not exceed {MAX_LOCAL_NAME_BYTES} UTF-8 bytes")
    return normalized


def _normalize_binary(value: object, *, field_name: str) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError(f"{field_name} values must be bytes")
    normalized = bytes(value)
    if len(normalized) > MAX_DATA_BYTES:
        raise ValueError(f"{field_name} values must not exceed {MAX_DATA_BYTES} bytes")
    return normalized


def _normalize_service_uuids(values: object) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ValueError("service_uuids must be a sequence")
    if len(values) > MAX_COLLECTION_ENTRIES:
        raise ValueError(f"service_uuids must contain at most {MAX_COLLECTION_ENTRIES} entries")
    normalized = {normalize_uuid(value, field_name="service_uuids entry") for value in values}
    return tuple(sorted(normalized))


def _normalize_service_data(values: object) -> Mapping[str, bytes]:
    if not isinstance(values, Mapping):
        raise ValueError("service_data must be a mapping")
    if len(values) > MAX_COLLECTION_ENTRIES:
        raise ValueError(f"service_data must contain at most {MAX_COLLECTION_ENTRIES} entries")
    normalized: dict[str, bytes] = {}
    for raw_key, raw_value in values.items():
        key = normalize_uuid(raw_key, field_name="service_data key")
        value = _normalize_binary(raw_value, field_name="service_data")
        if key in normalized and normalized[key] != value:
            raise ValueError("service_data contains conflicting normalized UUID keys")
        normalized[key] = value
    return MappingProxyType(dict(sorted(normalized.items())))


def _normalize_manufacturer_data(values: object) -> Mapping[int, bytes]:
    if not isinstance(values, Mapping):
        raise ValueError("manufacturer_data must be a mapping")
    if len(values) > MAX_COLLECTION_ENTRIES:
        raise ValueError(f"manufacturer_data must contain at most {MAX_COLLECTION_ENTRIES} entries")
    normalized: dict[int, bytes] = {}
    for raw_key, raw_value in values.items():
        key = _bounded_integer(
            raw_key, field_name="manufacturer_data key", minimum=0, maximum=0xFFFF
        )
        normalized[key] = _normalize_binary(raw_value, field_name="manufacturer_data")
    return MappingProxyType(dict(sorted(normalized.items())))


@dataclass(frozen=True)
class NormalizedAdvertisementFields:
    """Canonical normalized fields shared by panel observation and wire models."""

    adapter_address: str
    address: str
    address_type: str
    rssi: int
    local_name: str | None
    tx_power: int | None
    service_uuids: tuple[str, ...]
    service_data: Mapping[str, bytes]
    manufacturer_data: Mapping[int, bytes]
    capture_monotonic_ms: int


def normalize_advertisement_fields(
    *,
    adapter_address: object,
    address: object,
    address_type: object,
    rssi: object,
    local_name: object,
    tx_power: object,
    service_uuids: object,
    service_data: object,
    manufacturer_data: object,
    capture_monotonic_ms: object,
) -> NormalizedAdvertisementFields:
    """Validate and normalize the common advertisement field set once."""
    if not isinstance(address_type, str):
        raise ValueError("address_type must be public or random")
    normalized_address_type = address_type.strip().lower()
    if normalized_address_type not in _ADDRESS_TYPES:
        raise ValueError("address_type must be public or random")
    normalized_tx_power = None
    if tx_power is not None:
        normalized_tx_power = _bounded_integer(
            tx_power,
            field_name="tx_power",
            minimum=MIN_TX_POWER,
            maximum=MAX_TX_POWER,
        )
    return NormalizedAdvertisementFields(
        adapter_address=normalize_address(adapter_address, field_name="adapter_address"),
        address=normalize_address(address),
        address_type=normalized_address_type,
        rssi=_bounded_integer(
            rssi,
            field_name="rssi",
            minimum=MIN_RSSI,
            maximum=MAX_RSSI,
        ),
        local_name=_normalize_local_name(local_name),
        tx_power=normalized_tx_power,
        service_uuids=_normalize_service_uuids(service_uuids),
        service_data=_normalize_service_data(service_data),
        manufacturer_data=_normalize_manufacturer_data(manufacturer_data),
        capture_monotonic_ms=_bounded_integer(
            capture_monotonic_ms,
            field_name="capture_monotonic_ms",
            minimum=0,
            maximum=MAX_COUNTER,
        ),
    )


def normalize_advertisement_instance(target: object) -> None:
    """Normalize and apply the common field set on an advertisement-like object."""
    source = cast(Any, target)
    normalized = normalize_advertisement_fields(
        adapter_address=source.adapter_address,
        address=source.address,
        address_type=source.address_type,
        rssi=source.rssi,
        local_name=source.local_name,
        tx_power=source.tx_power,
        service_uuids=source.service_uuids,
        service_data=source.service_data,
        manufacturer_data=source.manufacturer_data,
        capture_monotonic_ms=source.capture_monotonic_ms,
    )
    for normalized_field in fields(normalized):
        object.__setattr__(
            target,
            normalized_field.name,
            getattr(normalized, normalized_field.name),
        )


@dataclass(frozen=True)
class AdvertisementEnvelope:
    """One normalized advertisement emitted by one physical panel."""

    panel: str
    adapter_address: str
    boot_id: str
    session_id: str
    sequence: int
    address: str
    address_type: str
    rssi: int
    local_name: str | None
    tx_power: int | None
    service_uuids: Sequence[str]
    service_data: Mapping[str, bytes]
    manufacturer_data: Mapping[int, bytes]
    capture_monotonic_ms: int
    version: int = field(default=PROTOCOL_VERSION, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "panel", normalize_panel(self.panel))
        object.__setattr__(self, "boot_id", normalize_uuid(self.boot_id, field_name="boot_id"))
        object.__setattr__(
            self, "session_id", normalize_uuid(self.session_id, field_name="session_id")
        )
        object.__setattr__(
            self,
            "sequence",
            _bounded_integer(self.sequence, field_name="sequence", minimum=1, maximum=MAX_COUNTER),
        )
        normalize_advertisement_instance(self)
        if len(self.to_json().encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise ValueError(f"advertisement payload must not exceed {MAX_PAYLOAD_BYTES} bytes")

    def to_payload(self) -> dict[str, object]:
        """Return the JSON-safe wire value, with binary fields encoded as hex."""
        payload: dict[str, object] = {
            "adapter_address": self.adapter_address,
            "address": self.address,
            "address_type": self.address_type,
            "boot_id": self.boot_id,
            "capture_monotonic_ms": self.capture_monotonic_ms,
            "manufacturer_data": {
                str(key): value.hex() for key, value in self.manufacturer_data.items()
            },
            "panel": self.panel,
            "rssi": self.rssi,
            "sequence": self.sequence,
            "session_id": self.session_id,
            "service_data": {key: value.hex() for key, value in self.service_data.items()},
            "service_uuids": list(self.service_uuids),
            "version": self.version,
        }
        if self.local_name is not None:
            payload["local_name"] = self.local_name
        if self.tx_power is not None:
            payload["tx_power"] = self.tx_power
        return payload

    def to_json(self) -> str:
        """Encode this envelope as deterministic compact JSON."""
        return json.dumps(self.to_payload(), separators=(",", ":"), sort_keys=True)


@dataclass(frozen=True)
class AllowlistEntry:
    """A static-address or stable iBeacon identity permitted for publication."""

    address: str | None = None
    ibeacon_uuid: str | None = None
    ibeacon_major: int | None = None
    ibeacon_minor: int | None = None

    def __post_init__(self) -> None:
        has_address = self.address is not None
        ibeacon_values = (self.ibeacon_uuid, self.ibeacon_major, self.ibeacon_minor)
        has_complete_ibeacon = all(value is not None for value in ibeacon_values)
        has_any_ibeacon = any(value is not None for value in ibeacon_values)
        if has_address == has_complete_ibeacon or (has_any_ibeacon and not has_complete_ibeacon):
            raise ValueError(
                "allowlist entry must contain exactly one address or one complete iBeacon identity"
            )
        if has_address:
            object.__setattr__(
                self, "address", normalize_address(self.address, field_name="allowlist address")
            )
            return
        object.__setattr__(
            self,
            "ibeacon_uuid",
            normalize_uuid(self.ibeacon_uuid, field_name="allowlist ibeacon_uuid"),
        )
        object.__setattr__(
            self,
            "ibeacon_major",
            _bounded_integer(
                self.ibeacon_major,
                field_name="allowlist ibeacon_major",
                minimum=0,
                maximum=0xFFFF,
            ),
        )
        object.__setattr__(
            self,
            "ibeacon_minor",
            _bounded_integer(
                self.ibeacon_minor,
                field_name="allowlist ibeacon_minor",
                minimum=0,
                maximum=0xFFFF,
            ),
        )


def parse_allowlist(value: object) -> tuple[AllowlistEntry, ...]:
    """Parse the strict JSON-compatible allowlist value."""
    if not isinstance(value, list):
        raise ValueError("allowlist must be a JSON array")
    if len(value) > MAX_COLLECTION_ENTRIES:
        raise ValueError(f"allowlist must contain at most {MAX_COLLECTION_ENTRIES} entries")
    parsed: list[AllowlistEntry] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict) or not all(isinstance(key, str) for key in item):
            raise ValueError(f"allowlist entry {index} must be an object")
        keys = frozenset(cast(dict[str, object], item))
        try:
            if keys == _STATIC_ALLOWLIST_KEYS:
                entry = AllowlistEntry(address=item["address"])
            elif keys == _IBEACON_ALLOWLIST_KEYS:
                entry = AllowlistEntry(
                    ibeacon_uuid=item["ibeacon_uuid"],
                    ibeacon_major=item["ibeacon_major"],
                    ibeacon_minor=item["ibeacon_minor"],
                )
            else:
                raise ValueError("keys must select one supported identity")
        except ValueError as error:
            raise ValueError(f"allowlist entry {index}: {error}") from error
        if entry in parsed:
            raise ValueError(f"allowlist entry {index} duplicates an earlier identity")
        parsed.append(entry)
    return tuple(parsed)


def matches_allowlist(
    *,
    address: object,
    manufacturer_data: Mapping[int, bytes | bytearray | memoryview],
    allowlist: Sequence[AllowlistEntry],
) -> bool:
    """Return whether raw advertisement identity matches a configured entry."""
    try:
        normalized_address = normalize_address(address)
    except ValueError:
        return False
    ibeacon = _parse_ibeacon(manufacturer_data)
    for entry in allowlist:
        if entry.address is not None and entry.address == normalized_address:
            return True
        if (
            ibeacon is not None
            and (
                entry.ibeacon_uuid,
                entry.ibeacon_major,
                entry.ibeacon_minor,
            )
            == ibeacon
        ):
            return True
    return False


def _parse_ibeacon(
    manufacturer_data: Mapping[int, bytes | bytearray | memoryview],
) -> tuple[str, int, int] | None:
    raw = manufacturer_data.get(_APPLE_COMPANY_ID)
    if not isinstance(raw, (bytes, bytearray, memoryview)):
        return None
    value = bytes(raw)
    if len(value) < _IBEACON_MIN_LENGTH or value[:2] != _IBEACON_PREFIX:
        return None
    ibeacon_uuid = str(UUID(bytes=value[2:18]))
    major = int.from_bytes(value[18:20], byteorder="big")
    minor = int.from_bytes(value[20:22], byteorder="big")
    return ibeacon_uuid, major, minor

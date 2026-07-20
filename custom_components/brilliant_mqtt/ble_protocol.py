"""Versioned MQTT wire contract for Brilliant remote BLE advertisements."""

from __future__ import annotations

import json
import re
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import cast
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
MAX_PRIOR_BOOT_IDS = 8

_TOPIC_PREFIX = "brilliant/ble/v1"
_PANEL_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")
_MAC_PATTERN = re.compile(r"(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}")
_HEX_PATTERN = re.compile(r"(?:[0-9A-Fa-f]{2})*")
_ADDRESS_TYPES = frozenset({"public", "random"})
_REQUIRED_FIELDS = frozenset(
    {
        "version",
        "panel",
        "adapter_address",
        "boot_id",
        "sequence",
        "address",
        "address_type",
        "rssi",
        "service_uuids",
        "service_data",
        "manufacturer_data",
        "capture_monotonic_ms",
    }
)
_OPTIONAL_FIELDS = frozenset({"local_name", "tx_power"})


@dataclass(frozen=True)
class Advertisement:
    """A validated, normalized advertisement ready for Home Assistant."""

    panel: str
    adapter_address: str
    boot_id: str
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
    version: int = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != PROTOCOL_VERSION:
            raise ValueError("unsupported advertisement version")
        object.__setattr__(self, "panel", _validated_panel(self.panel))
        object.__setattr__(
            self,
            "adapter_address",
            _normalized_address(self.adapter_address, field_name="adapter_address"),
        )
        object.__setattr__(self, "boot_id", _normalized_uuid(self.boot_id, field_name="boot_id"))
        object.__setattr__(
            self,
            "sequence",
            _bounded_integer(self.sequence, field_name="sequence", minimum=1, maximum=MAX_COUNTER),
        )
        object.__setattr__(self, "address", _normalized_address(self.address, field_name="address"))
        if not isinstance(self.address_type, str):
            raise ValueError("address_type must be public or random")
        address_type = self.address_type.strip().lower()
        if address_type not in _ADDRESS_TYPES:
            raise ValueError("address_type must be public or random")
        object.__setattr__(self, "address_type", address_type)
        object.__setattr__(
            self,
            "rssi",
            _bounded_integer(self.rssi, field_name="rssi", minimum=MIN_RSSI, maximum=MAX_RSSI),
        )
        object.__setattr__(self, "local_name", _normalized_local_name(self.local_name))
        if self.tx_power is not None:
            object.__setattr__(
                self,
                "tx_power",
                _bounded_integer(
                    self.tx_power,
                    field_name="tx_power",
                    minimum=MIN_TX_POWER,
                    maximum=MAX_TX_POWER,
                ),
            )
        object.__setattr__(self, "service_uuids", _normalized_service_uuids(self.service_uuids))
        object.__setattr__(self, "service_data", _normalized_service_data(self.service_data))
        object.__setattr__(
            self, "manufacturer_data", _normalized_manufacturer_data(self.manufacturer_data)
        )
        object.__setattr__(
            self,
            "capture_monotonic_ms",
            _bounded_integer(
                self.capture_monotonic_ms,
                field_name="capture_monotonic_ms",
                minimum=0,
                maximum=MAX_COUNTER,
            ),
        )


class AdvertisementSequenceTracker:
    """Enforce strict sequence order while permitting one-way boot changes."""

    def __init__(self, *, max_prior_boot_ids: int = MAX_PRIOR_BOOT_IDS) -> None:
        if type(max_prior_boot_ids) is not int or not 1 <= max_prior_boot_ids <= 64:
            raise ValueError("max_prior_boot_ids must be an integer from 1 through 64")
        self._max_prior_boot_ids = max_prior_boot_ids
        self._prior_boot_ids: deque[str] = deque()
        self._prior_boot_set: set[str] = set()
        self.current_boot_id: str | None = None
        self.last_sequence: int | None = None

    def accept(self, advertisement: Advertisement) -> None:
        """Record one packet or reject a duplicate, stale packet, or old boot."""
        if not isinstance(advertisement, Advertisement):
            raise ValueError("advertisement must be validated before ordering")
        if self.current_boot_id is None:
            self.current_boot_id = advertisement.boot_id
            self.last_sequence = advertisement.sequence
            return
        if advertisement.boot_id == self.current_boot_id:
            if advertisement.sequence == self.last_sequence:
                raise ValueError("duplicate advertisement sequence")
            if self.last_sequence is not None and advertisement.sequence < self.last_sequence:
                raise ValueError("out-of-order advertisement sequence")
            self.last_sequence = advertisement.sequence
            return
        if advertisement.boot_id in self._prior_boot_set:
            raise ValueError("advertisement belongs to a prior boot")
        self._remember_prior_boot(self.current_boot_id)
        self.current_boot_id = advertisement.boot_id
        self.last_sequence = advertisement.sequence

    def _remember_prior_boot(self, boot_id: str) -> None:
        if len(self._prior_boot_ids) == self._max_prior_boot_ids:
            forgotten = self._prior_boot_ids.popleft()
            self._prior_boot_set.remove(forgotten)
        self._prior_boot_ids.append(boot_id)
        self._prior_boot_set.add(boot_id)


def advertisement_topic(panel: str) -> str:
    """Return the non-retained advertisement topic for one panel."""
    return f"{_TOPIC_PREFIX}/{_validated_panel(panel)}/advertisement"


def decode_advertisement(payload: str | bytes, *, topic: str, retained: bool) -> Advertisement:
    """Decode and validate one MQTT advertisement including delivery metadata."""
    if retained:
        raise ValueError("retained BLE advertisements are not allowed")
    topic_panel = _topic_panel(topic)
    value = _decode_payload(payload)
    advertisement = Advertisement(
        version=_required_integer(value, "version"),
        panel=_required_string(value, "panel"),
        adapter_address=_required_string(value, "adapter_address"),
        boot_id=_required_string(value, "boot_id"),
        sequence=_required_integer(value, "sequence"),
        address=_required_string(value, "address"),
        address_type=_required_string(value, "address_type"),
        rssi=_required_integer(value, "rssi"),
        local_name=_optional_string(value, "local_name"),
        tx_power=_optional_integer(value, "tx_power"),
        service_uuids=_wire_service_uuids(value.get("service_uuids")),
        service_data=_wire_service_data(value.get("service_data")),
        manufacturer_data=_wire_manufacturer_data(value.get("manufacturer_data")),
        capture_monotonic_ms=_required_integer(value, "capture_monotonic_ms"),
    )
    if advertisement.panel != topic_panel:
        raise ValueError("topic panel does not match advertisement panel")
    return advertisement


def _topic_panel(topic: object) -> str:
    if not isinstance(topic, str):
        raise ValueError("topic must be an advertisement topic")
    parts = topic.split("/")
    if len(parts) != 5 or parts[:3] != ["brilliant", "ble", "v1"] or parts[4] != "advertisement":
        raise ValueError("topic must be a version 1 advertisement topic")
    return _validated_panel(parts[3])


def _decode_payload(payload: str | bytes) -> dict[str, object]:
    if isinstance(payload, bytes):
        if len(payload) > MAX_PAYLOAD_BYTES:
            raise ValueError(f"payload must not exceed {MAX_PAYLOAD_BYTES} bytes")
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("payload must be UTF-8 JSON") from error
    elif isinstance(payload, str):
        try:
            payload_size = len(payload.encode("utf-8"))
        except UnicodeEncodeError as error:
            raise ValueError("payload must be UTF-8 JSON") from error
        if payload_size > MAX_PAYLOAD_BYTES:
            raise ValueError(f"payload must not exceed {MAX_PAYLOAD_BYTES} bytes")
        text = payload
    else:
        raise ValueError("payload must be UTF-8 JSON")
    try:
        decoded = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
    except (json.JSONDecodeError, TypeError) as error:
        raise ValueError("payload must be valid JSON") from error
    if not isinstance(decoded, dict) or not all(isinstance(key, str) for key in decoded):
        raise ValueError("payload must be a JSON object")
    value = cast(dict[str, object], decoded)
    fields = frozenset(value)
    missing = _REQUIRED_FIELDS - fields
    if missing:
        raise ValueError(f"payload is missing required fields: {sorted(missing)}")
    unexpected = fields - _REQUIRED_FIELDS - _OPTIONAL_FIELDS
    if unexpected:
        raise ValueError(f"payload has unexpected fields: {sorted(unexpected)}")
    return value


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _validated_panel(panel: object) -> str:
    if not isinstance(panel, str) or _PANEL_PATTERN.fullmatch(panel) is None or panel == "mesh":
        raise ValueError("panel must be a percent-free lowercase physical-panel slug")
    return panel


def _normalized_address(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a Bluetooth address")
    normalized = value.strip().replace("-", ":").upper()
    if _MAC_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must be a Bluetooth address")
    return normalized


def _normalized_uuid(value: object, *, field_name: str) -> str:
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


def _normalized_local_name(value: object) -> str | None:
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


def _normalized_service_uuids(values: object) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ValueError("service_uuids must be an array")
    if len(values) > MAX_COLLECTION_ENTRIES:
        raise ValueError(f"service_uuids must contain at most {MAX_COLLECTION_ENTRIES} entries")
    return tuple(
        sorted({_normalized_uuid(value, field_name="service_uuids entry") for value in values})
    )


def _normalized_service_data(values: object) -> Mapping[str, bytes]:
    if not isinstance(values, Mapping):
        raise ValueError("service_data must be an object")
    if len(values) > MAX_COLLECTION_ENTRIES:
        raise ValueError(f"service_data must contain at most {MAX_COLLECTION_ENTRIES} entries")
    normalized: dict[str, bytes] = {}
    for raw_key, raw_value in values.items():
        key = _normalized_uuid(raw_key, field_name="service_data key")
        if not isinstance(raw_value, bytes):
            raise ValueError("service_data values must be bytes")
        if len(raw_value) > MAX_DATA_BYTES:
            raise ValueError(f"service_data values must not exceed {MAX_DATA_BYTES} bytes")
        if key in normalized and normalized[key] != raw_value:
            raise ValueError("service_data contains conflicting normalized UUID keys")
        normalized[key] = raw_value
    return MappingProxyType(dict(sorted(normalized.items())))


def _normalized_manufacturer_data(values: object) -> Mapping[int, bytes]:
    if not isinstance(values, Mapping):
        raise ValueError("manufacturer_data must be an object")
    if len(values) > MAX_COLLECTION_ENTRIES:
        raise ValueError(f"manufacturer_data must contain at most {MAX_COLLECTION_ENTRIES} entries")
    normalized: dict[int, bytes] = {}
    for raw_key, raw_value in values.items():
        key = _bounded_integer(
            raw_key, field_name="manufacturer_data key", minimum=0, maximum=0xFFFF
        )
        if not isinstance(raw_value, bytes):
            raise ValueError("manufacturer_data values must be bytes")
        if len(raw_value) > MAX_DATA_BYTES:
            raise ValueError(f"manufacturer_data values must not exceed {MAX_DATA_BYTES} bytes")
        normalized[key] = raw_value
    return MappingProxyType(dict(sorted(normalized.items())))


def _required_string(value: Mapping[str, object], field_name: str) -> str:
    result = value.get(field_name)
    if not isinstance(result, str) or not result:
        raise ValueError(f"{field_name} must be a non-empty string")
    return result


def _optional_string(value: Mapping[str, object], field_name: str) -> str | None:
    result = value.get(field_name)
    if result is None:
        return None
    if not isinstance(result, str):
        raise ValueError(f"{field_name} must be a string or null")
    return result


def _required_integer(value: Mapping[str, object], field_name: str) -> int:
    result = value.get(field_name)
    if type(result) is not int:
        raise ValueError(f"{field_name} must be an integer")
    return result


def _optional_integer(value: Mapping[str, object], field_name: str) -> int | None:
    result = value.get(field_name)
    if result is None:
        return None
    if type(result) is not int:
        raise ValueError(f"{field_name} must be an integer or null")
    return result


def _wire_service_uuids(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("service_uuids must be an array")
    return tuple(cast(list[str], value))


def _wire_service_data(value: object) -> Mapping[str, bytes]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError("service_data must be an object")
    result: dict[str, bytes] = {}
    for key, raw_hex in cast(dict[str, object], value).items():
        result[key] = _decode_hex(raw_hex, field_name="service_data")
    return result


def _wire_manufacturer_data(value: object) -> Mapping[int, bytes]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError("manufacturer_data must be an object")
    result: dict[int, bytes] = {}
    for raw_key, raw_hex in cast(dict[str, object], value).items():
        try:
            key = int(raw_key)
        except ValueError as error:
            raise ValueError("manufacturer_data keys must be canonical decimal integers") from error
        if str(key) != raw_key or not 0 <= key <= 0xFFFF:
            raise ValueError("manufacturer_data keys must be canonical decimal integers")
        result[key] = _decode_hex(raw_hex, field_name="manufacturer_data")
    return result


def _decode_hex(value: object, *, field_name: str) -> bytes:
    if not isinstance(value, str) or _HEX_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} values must be even-length hexadecimal strings")
    if len(value) // 2 > MAX_DATA_BYTES:
        raise ValueError(f"{field_name} values must not exceed {MAX_DATA_BYTES} bytes")
    return bytes.fromhex(value)

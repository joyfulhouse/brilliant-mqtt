"""Private, read-only snapshots of Brilliant physical-slider bindings.

The snapshot contains device and peripheral identifiers and must never be
committed.  Public restoration results deliberately expose only comparisons
and a count.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import hashlib
import json
import os
import re
import secrets
import stat
import struct
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

_SCHEMA_VERSION = 1
_SLIDER_NAME = re.compile(r"^slider_config:(0|[1-9][0-9]*)$")
_GUARD_NAMES = ("disable_cap_touch_sliders", "slider_double_tap_timeout_ms")
_MAX_SLIDERS = 64
_MAX_INDEX = 255
_MAX_IDENTIFIER_BYTES = 1024
_MAX_ENCODED_BYTES = 64 * 1024
_MAX_PRIVATE_FILE_BYTES = 1024 * 1024
_SOCKET_PATH = "/var/run/brilliant/server_socket"
_CONNECT_TIMEOUT_S = 5.0
_READ_TIMEOUT_S = 5.0
_CLOSE_TIMEOUT_S = 2.0

_T_STOP = 0
_T_BOOL = 2
_T_BYTE = 3
_T_DOUBLE = 4
_T_I16 = 6
_T_I32 = 8
_T_I64 = 10
_T_STRING = 11
_T_STRUCT = 12
_T_MAP = 13
_T_SET = 14
_T_LIST = 15


class SliderBindingError(ValueError):
    """Raised when a binding or private snapshot cannot be trusted."""


@dataclass(frozen=True, slots=True)
class SliderBinding:
    """Decoded target fields from one Brilliant ``slider_config`` struct."""

    slider_index: int
    device_id: str
    peripheral_id: str
    action: int | None


@dataclass(frozen=True, slots=True)
class SliderConfigRecord:
    """Exact encoded variable value plus its independently decoded target."""

    variable_name: str
    encoded_value: str
    binding: SliderBinding


@dataclass(frozen=True, slots=True)
class SliderBindingSnapshot:
    """Sensitive baseline required to prove an exact later restoration."""

    owning_device_id: str
    selected_slider_index: int
    slider_configs: tuple[SliderConfigRecord, ...]
    guard_values: dict[str, str | None]


@dataclass(frozen=True, slots=True)
class OwnConfigState:
    """One scoped read of the physical Control's configuration variables."""

    owning_device_id: str
    variables: dict[str, str]


class OwnConfigReader(Protocol):
    """Read-only boundary used by snapshot collection."""

    async def read_own_config(self) -> OwnConfigState: ...


@dataclass(frozen=True, slots=True)
class RestorationResult:
    """Non-sensitive comparison result suitable for logs and reports."""

    owner_matches: bool
    slider_names_match: bool
    slider_values_match: bool
    guard_values_match: bool
    selected_binding_matches: bool
    slider_count: int
    restored: bool

    def to_public_dict(self) -> dict[str, object]:
        return {
            "owner_matches": self.owner_matches,
            "slider_names_match": self.slider_names_match,
            "slider_values_match": self.slider_values_match,
            "guard_values_match": self.guard_values_match,
            "selected_binding_matches": self.selected_binding_matches,
            "slider_count": self.slider_count,
            "restored": self.restored,
        }


class _Cursor:
    def __init__(self, raw: bytes) -> None:
        self.raw = raw
        self.offset = 0

    def take(self, length: int) -> bytes:
        if length < 0 or self.offset + length > len(self.raw):
            raise SliderBindingError("slider config is truncated")
        start = self.offset
        self.offset += length
        return self.raw[start : start + length]

    def unpack(self, format_string: str) -> int | float:
        size = struct.calcsize(format_string)
        return cast(int | float, struct.unpack(format_string, self.take(size))[0])


def decode_slider_config(encoded: str) -> SliderBinding:
    """Decode the allowlisted target fields of one TBinaryProtocol struct."""

    if not isinstance(encoded, str) or not encoded or len(encoded) > _MAX_ENCODED_BYTES:
        raise SliderBindingError("slider config must be bounded base64 text")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        raise SliderBindingError("slider config is not valid base64") from None
    if not raw or len(raw) > _MAX_ENCODED_BYTES:
        raise SliderBindingError("slider config has an invalid decoded size")

    cursor = _Cursor(raw)
    fields: dict[int, object] = {}
    while True:
        field_type = int(cursor.unpack(">b"))
        if field_type == _T_STOP:
            break
        field_id = int(cursor.unpack(">h"))
        if field_id in fields:
            raise SliderBindingError("slider config contains a duplicate field")
        if field_id in (1, 6):
            if field_type != _T_I32:
                raise SliderBindingError("slider config integer field has the wrong type")
            fields[field_id] = int(cursor.unpack(">i"))
        elif field_id in (2, 3):
            if field_type != _T_STRING:
                raise SliderBindingError("slider config target field has the wrong type")
            fields[field_id] = _read_identifier(cursor)
        else:
            _skip_value(cursor, field_type, depth=0)

    if cursor.offset != len(raw):
        raise SliderBindingError("slider config contains trailing data")
    if not all(field in fields for field in (1, 2, 3)):
        raise SliderBindingError("slider config lacks required target fields")

    slider_index = cast(int, fields[1])
    if not 0 <= slider_index <= _MAX_INDEX:
        raise SliderBindingError("slider config index is outside the supported range")
    action_value = fields.get(6)
    action = cast(int | None, action_value)
    return SliderBinding(
        slider_index=slider_index,
        device_id=cast(str, fields[2]),
        peripheral_id=cast(str, fields[3]),
        action=action,
    )


def _read_identifier(cursor: _Cursor) -> str:
    length = int(cursor.unpack(">i"))
    if not 0 < length <= _MAX_IDENTIFIER_BYTES:
        raise SliderBindingError("slider config identifier has an invalid length")
    try:
        value = cursor.take(length).decode("utf-8")
    except UnicodeDecodeError:
        raise SliderBindingError("slider config identifier is not UTF-8") from None
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise SliderBindingError("slider config identifier contains control characters")
    return value


def _read_count(cursor: _Cursor) -> int:
    count = int(cursor.unpack(">i"))
    if not 0 <= count <= 4096:
        raise SliderBindingError("slider config collection has an invalid size")
    return count


def _skip_value(cursor: _Cursor, field_type: int, *, depth: int) -> None:
    if depth > 16:
        raise SliderBindingError("slider config nesting is too deep")
    if field_type in (_T_BOOL, _T_BYTE):
        cursor.take(1)
    elif field_type == _T_I16:
        cursor.take(2)
    elif field_type == _T_I32:
        cursor.take(4)
    elif field_type in (_T_DOUBLE, _T_I64):
        cursor.take(8)
    elif field_type == _T_STRING:
        cursor.take(_read_count(cursor))
    elif field_type == _T_STRUCT:
        while True:
            nested_type = int(cursor.unpack(">b"))
            if nested_type == _T_STOP:
                break
            cursor.take(2)
            _skip_value(cursor, nested_type, depth=depth + 1)
    elif field_type == _T_MAP:
        key_type = int(cursor.unpack(">b"))
        value_type = int(cursor.unpack(">b"))
        for _ in range(_read_count(cursor)):
            _skip_value(cursor, key_type, depth=depth + 1)
            _skip_value(cursor, value_type, depth=depth + 1)
    elif field_type in (_T_SET, _T_LIST):
        element_type = int(cursor.unpack(">b"))
        for _ in range(_read_count(cursor)):
            _skip_value(cursor, element_type, depth=depth + 1)
    else:
        raise SliderBindingError("slider config contains an unknown Thrift type")


def build_private_snapshot(
    *,
    owning_device_id: str,
    variables: Mapping[str, str],
    selected_slider_index: int,
) -> SliderBindingSnapshot:
    """Build a sensitive baseline from one physical Control's variable map."""

    _validate_identifier(owning_device_id, "owning device ID")
    _validate_index(selected_slider_index, "selected slider index")
    records: list[SliderConfigRecord] = []
    for variable_name, encoded_value in variables.items():
        match = _SLIDER_NAME.fullmatch(variable_name)
        if match is None:
            continue
        if not isinstance(encoded_value, str):
            raise SliderBindingError("slider config value must be text")
        suffix_index = int(match.group(1))
        binding = decode_slider_config(encoded_value)
        if binding.slider_index != suffix_index:
            raise SliderBindingError("slider variable index does not match its wire index")
        records.append(
            SliderConfigRecord(
                variable_name=variable_name,
                encoded_value=encoded_value,
                binding=binding,
            )
        )
    records.sort(key=lambda record: record.binding.slider_index)
    if not records or len(records) > _MAX_SLIDERS:
        raise SliderBindingError("slider config count is outside the supported range")
    if all(record.binding.slider_index != selected_slider_index for record in records):
        raise SliderBindingError("selected slider is not present in the snapshot")

    guards: dict[str, str | None] = {}
    for name in _GUARD_NAMES:
        value = variables.get(name)
        if value is not None and not isinstance(value, str):
            raise SliderBindingError("slider guard value must be text")
        guards[name] = value
    return SliderBindingSnapshot(
        owning_device_id=owning_device_id,
        selected_slider_index=selected_slider_index,
        slider_configs=tuple(records),
        guard_values=guards,
    )


def extract_slider_variables(raw_device: object) -> dict[str, str]:
    """Extract only slider and restoration-guard values from an own-device read."""

    device = cast(Any, raw_device)
    try:
        peripherals = dict(device.peripherals)
    except (AttributeError, TypeError, ValueError):
        raise SliderBindingError("owning device peripherals are unavailable") from None
    raw_config = peripherals.get("device_config_peripheral")
    if raw_config is None:
        raise SliderBindingError("device configuration peripheral is unavailable")
    config = cast(Any, raw_config)
    try:
        peripheral_type = int(config.peripheral_type)
    except (AttributeError, TypeError, ValueError):
        raise SliderBindingError("device configuration peripheral type is invalid") from None
    if peripheral_type != 19:
        raise SliderBindingError("device configuration peripheral has the wrong type")
    try:
        raw_variables = dict(config.variables)
    except (AttributeError, TypeError, ValueError):
        raise SliderBindingError("device configuration variables are unavailable") from None

    variables: dict[str, str] = {}
    for raw_name, raw_variable in raw_variables.items():
        if not isinstance(raw_name, str):
            continue
        if raw_name not in _GUARD_NAMES and _SLIDER_NAME.fullmatch(raw_name) is None:
            continue
        variable = cast(Any, raw_variable)
        try:
            value = variable.value
        except AttributeError:
            raise SliderBindingError("device configuration variable value is unavailable") from None
        if isinstance(value, bytes | bytearray):
            try:
                normalized = bytes(value).decode("utf-8")
            except UnicodeDecodeError:
                raise SliderBindingError("device configuration value is not UTF-8") from None
        elif isinstance(value, str):
            normalized = value
        else:
            raise SliderBindingError("device configuration variable value is unreadable")
        variables[raw_name] = normalized
    return variables


async def collect_private_snapshot(
    reader: OwnConfigReader,
    *,
    selected_slider_index: int,
) -> SliderBindingSnapshot:
    """Perform one scoped own-config read and build its private baseline."""

    state = await reader.read_own_config()
    return build_private_snapshot(
        owning_device_id=state.owning_device_id,
        variables=state.variables,
        selected_slider_index=selected_slider_index,
    )


def dumps_private_snapshot(snapshot: SliderBindingSnapshot) -> str:
    """Serialize a private snapshot using the fixed version-1 schema."""

    payload: dict[str, object] = {
        "schema_version": _SCHEMA_VERSION,
        "owning_device_id": snapshot.owning_device_id,
        "selected_slider_index": snapshot.selected_slider_index,
        "slider_configs": [
            {
                "variable_name": record.variable_name,
                "encoded_value": record.encoded_value,
            }
            for record in snapshot.slider_configs
        ],
        "guard_values": snapshot.guard_values,
    }
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def loads_private_snapshot(serialized: str | bytes | bytearray) -> SliderBindingSnapshot:
    """Load and fully revalidate a private snapshot."""

    try:
        parsed: object = json.loads(serialized)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
        raise SliderBindingError("private snapshot is not valid JSON") from None
    if not isinstance(parsed, dict):
        raise SliderBindingError("private snapshot schema must be an object")
    payload = cast(dict[str, object], parsed)
    expected = {
        "schema_version",
        "owning_device_id",
        "selected_slider_index",
        "slider_configs",
        "guard_values",
    }
    if set(payload) != expected or payload["schema_version"] != _SCHEMA_VERSION:
        raise SliderBindingError("private snapshot schema does not match version 1")

    owner = payload["owning_device_id"]
    selected = payload["selected_slider_index"]
    if not isinstance(owner, str):
        raise SliderBindingError("private snapshot owner must be text")
    _validate_identifier(owner, "owning device ID")
    _validate_index(selected, "selected slider index")
    selected_index = cast(int, selected)

    raw_records = payload["slider_configs"]
    if not isinstance(raw_records, list) or not 0 < len(raw_records) <= _MAX_SLIDERS:
        raise SliderBindingError("private snapshot slider list is invalid")
    variables: dict[str, str] = {}
    for raw_record in raw_records:
        if not isinstance(raw_record, dict) or set(raw_record) != {
            "variable_name",
            "encoded_value",
        }:
            raise SliderBindingError("private snapshot slider record schema is invalid")
        name = raw_record["variable_name"]
        value = raw_record["encoded_value"]
        if not isinstance(name, str) or not isinstance(value, str):
            raise SliderBindingError("private snapshot slider record types are invalid")
        if name in variables:
            raise SliderBindingError("private snapshot contains a duplicate slider")
        variables[name] = value

    raw_guards = payload["guard_values"]
    if not isinstance(raw_guards, dict) or set(raw_guards) != set(_GUARD_NAMES):
        raise SliderBindingError("private snapshot guard schema is invalid")
    for name in _GUARD_NAMES:
        value = raw_guards[name]
        if value is not None and not isinstance(value, str):
            raise SliderBindingError("private snapshot guard value is invalid")
        if value is not None:
            variables[name] = value
    return build_private_snapshot(
        owning_device_id=owner,
        variables=variables,
        selected_slider_index=selected_index,
    )


def verify_restoration(
    baseline: SliderBindingSnapshot,
    *,
    current_owning_device_id: str,
    current_variables: Mapping[str, str],
) -> RestorationResult:
    """Compare current read-only state with the exact private baseline."""

    baseline_values = {
        record.variable_name: record.encoded_value for record in baseline.slider_configs
    }
    current_values = {
        name: value
        for name, value in current_variables.items()
        if _SLIDER_NAME.fullmatch(name) is not None
    }
    slider_names_match = set(current_values) == set(baseline_values)
    slider_values_match = slider_names_match and current_values == baseline_values
    current_guards = {name: current_variables.get(name) for name in _GUARD_NAMES}
    guard_values_match = current_guards == baseline.guard_values
    owner_matches = current_owning_device_id == baseline.owning_device_id

    selected_name = f"slider_config:{baseline.selected_slider_index}"
    selected_record = next(
        record
        for record in baseline.slider_configs
        if record.binding.slider_index == baseline.selected_slider_index
    )
    selected_binding_matches = False
    current_selected = current_values.get(selected_name)
    if isinstance(current_selected, str):
        try:
            selected_binding_matches = (
                decode_slider_config(current_selected) == selected_record.binding
            )
        except SliderBindingError:
            selected_binding_matches = False

    restored = (
        owner_matches
        and slider_names_match
        and slider_values_match
        and guard_values_match
        and selected_binding_matches
    )
    return RestorationResult(
        owner_matches=owner_matches,
        slider_names_match=slider_names_match,
        slider_values_match=slider_values_match,
        guard_values_match=guard_values_match,
        selected_binding_matches=selected_binding_matches,
        slider_count=len(current_values),
        restored=restored,
    )


def write_private_snapshot(
    path: Path,
    snapshot: SliderBindingSnapshot,
    *,
    safe_root: Path,
    required_uid: int = 0,
) -> str:
    """Exclusively write a mode-0600 snapshot below a private evidence root."""

    _validate_safe_path(path, safe_root=safe_root, required_uid=required_uid)
    try:
        existing = path.lstat()
    except FileNotFoundError:
        existing = None
    if existing is not None:
        if stat.S_ISLNK(existing.st_mode):
            raise SliderBindingError("private snapshot path must not be a symlink")
        raise SliderBindingError("private snapshot already exists")

    data = dumps_private_snapshot(snapshot).encode("utf-8")
    if len(data) > _MAX_PRIVATE_FILE_BYTES:
        raise SliderBindingError("private snapshot exceeds 1 MiB")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        raise SliderBindingError("private snapshot already exists") from None
    except OSError:
        raise SliderBindingError("could not safely create private snapshot") from None
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(data)
        written = 0
        while written < len(view):
            written += os.write(descriptor, view[written:])
        os.fsync(descriptor)
    except Exception:
        path.unlink(missing_ok=True)
        raise
    finally:
        os.close(descriptor)
    directory_fd = os.open(safe_root, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return hashlib.sha256(data).hexdigest()


def read_private_snapshot(
    path: Path,
    *,
    safe_root: Path,
    required_uid: int = 0,
) -> SliderBindingSnapshot:
    """Safely read and validate a private snapshot without following links."""

    _validate_safe_path(path, safe_root=safe_root, required_uid=required_uid)
    try:
        before = path.lstat()
    except FileNotFoundError:
        raise SliderBindingError("private snapshot does not exist") from None
    if stat.S_ISLNK(before.st_mode):
        raise SliderBindingError("private snapshot must not be a symlink")
    if not stat.S_ISREG(before.st_mode):
        raise SliderBindingError("private snapshot must be a regular file")
    if before.st_uid != required_uid or stat.S_IMODE(before.st_mode) != 0o600:
        raise SliderBindingError("private snapshot must have the required owner and mode 0600")
    if before.st_size > _MAX_PRIVATE_FILE_BYTES:
        raise SliderBindingError("private snapshot exceeds 1 MiB")

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise SliderBindingError("could not safely open private snapshot") from None
    data = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise SliderBindingError("private snapshot changed during open")
        while True:
            chunk = os.read(descriptor, min(8192, _MAX_PRIVATE_FILE_BYTES + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > _MAX_PRIVATE_FILE_BYTES:
                raise SliderBindingError("private snapshot exceeds 1 MiB")
    finally:
        os.close(descriptor)
    try:
        return loads_private_snapshot(data)
    finally:
        for index in range(len(data)):
            data[index] = 0


def _make_read_only_observer_class(base: Any) -> Any:
    async def handle_notification(self: Any, notification: Any) -> None:
        del self, notification

    return type(
        "_SliderBindingReadOnlyObserver",
        (base,),
        {"handle_notification": handle_notification},
    )


class NativeOwnConfigReader:
    """Panel-only adapter exposing scoped reads and no mutation methods."""

    def __init__(self) -> None:
        self._observer: Any = None
        self._processor: Any = None

    async def start(self) -> None:
        """Connect to the local panel bus with a unique, read-only observer."""

        if self._observer is not None or self._processor is not None:
            raise SliderBindingError("read-only observer is already started")
        import lib.protocol.message_bus_peer_service as mbps
        from lib.message_bus_api.observer_interface import RPCObserver
        from lib.protocol.processor import SinglePeerProcessor

        loop = asyncio.get_running_loop()
        observer_class = _make_read_only_observer_class(RPCObserver)
        observer = observer_class(loop)
        processor = SinglePeerProcessor(
            socket_path=_SOCKET_PATH,
            my_name=f"brilliant_vc_slider_ro-{secrets.token_hex(4)}",
            handler=mbps.PeripheralServer(observer),
            client_class=mbps.MessageBusClient,
            loop=loop,
        )
        self._observer = observer
        self._processor = processor
        await asyncio.wait_for(processor.start(), timeout=_CONNECT_TIMEOUT_S)
        deadline = loop.time() + _CONNECT_TIMEOUT_S
        while not processor.is_connected():
            if loop.time() >= deadline:
                raise TimeoutError("message bus connection timed out")
            await asyncio.sleep(0.1)
        await asyncio.wait_for(observer.start(processor, None), timeout=_CONNECT_TIMEOUT_S)

    async def read_own_config(self) -> OwnConfigState:
        """Call only ``get_owning_device_id`` and ``get_device(own_id)``."""

        if self._observer is None:
            raise SliderBindingError("read-only observer is not started")
        owning_device_id = str(self._observer.get_owning_device_id())
        _validate_identifier(owning_device_id, "owning device ID")
        raw_device = await asyncio.wait_for(
            self._observer.get_device(owning_device_id),
            timeout=_READ_TIMEOUT_S,
        )
        if raw_device is None:
            raise SliderBindingError("owning device snapshot is unavailable")
        return OwnConfigState(
            owning_device_id=owning_device_id,
            variables=extract_slider_variables(raw_device),
        )

    async def close(self) -> None:
        """Bound both shutdown stages and fail if either cannot close cleanly."""

        observer, processor = self._observer, self._processor
        self._observer = None
        self._processor = None
        failed = False
        cancellation: asyncio.CancelledError | None = None
        for target in (observer, processor):
            if target is None:
                continue
            try:
                await asyncio.wait_for(target.shutdown(), timeout=_CLOSE_TIMEOUT_S)
            except asyncio.CancelledError as exc:
                cancellation = exc
            except Exception:
                failed = True
        if cancellation is not None:
            raise cancellation
        if failed:
            raise SliderBindingError("read-only observer did not shut down cleanly")


async def _read_live_config() -> OwnConfigState:
    reader = NativeOwnConfigReader()
    primary_error: BaseException | None = None
    try:
        await reader.start()
        return await reader.read_own_config()
    except asyncio.CancelledError as exc:
        primary_error = exc
        raise
    except BaseException as exc:
        primary_error = exc
        if isinstance(exc, SliderBindingError):
            raise
        raise SliderBindingError(f"scoped live read failed ({type(exc).__name__})") from None
    finally:
        try:
            await reader.close()
        except BaseException:
            if primary_error is None:
                raise


async def _run_live_cli(args: argparse.Namespace) -> tuple[dict[str, object], bool]:
    safe_root = cast(Path, args.safe_root)
    if args.command == "capture":
        state = await _read_live_config()
        snapshot = build_private_snapshot(
            owning_device_id=state.owning_device_id,
            variables=state.variables,
            selected_slider_index=cast(int, args.selected_slider_index),
        )
        digest = write_private_snapshot(
            cast(Path, args.output),
            snapshot,
            safe_root=safe_root,
        )
        return (
            {
                "capture_written": True,
                "sha256": digest,
                "slider_count": len(snapshot.slider_configs),
            },
            True,
        )

    baseline = read_private_snapshot(cast(Path, args.baseline), safe_root=safe_root)
    current = await _read_live_config()
    result = verify_restoration(
        baseline,
        current_owning_device_id=current.owning_device_id,
        current_variables=current.variables,
    )
    return result.to_public_dict(), result.restored


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--safe-root",
        type=Path,
        default=Path("/data/brilliant-vc/evidence"),
        help="existing root-owned mode-0700 evidence directory",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture = subparsers.add_parser("capture", help="capture one private baseline")
    capture.add_argument("--selected-slider-index", type=int, required=True)
    capture.add_argument("--output", type=Path, required=True)
    verify = subparsers.add_parser("verify", help="compare live state with a baseline")
    verify.add_argument("--baseline", type=Path, required=True)
    args = parser.parse_args(argv)

    report, passed = asyncio.run(_run_live_cli(args))
    print(json.dumps(report, sort_keys=True))
    return 0 if passed else 2


def _validate_safe_path(path: Path, *, safe_root: Path, required_uid: int) -> None:
    root = safe_root.absolute()
    target = path.absolute()
    try:
        root_metadata = root.lstat()
    except FileNotFoundError:
        raise SliderBindingError("safe root does not exist") from None
    if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
        raise SliderBindingError("safe root must be a real directory")
    if root_metadata.st_uid != required_uid or stat.S_IMODE(root_metadata.st_mode) != 0o700:
        raise SliderBindingError("safe root must have the required owner and mode 0700")
    if target.parent != root:
        raise SliderBindingError("private snapshot must be directly below the safe root")
    if not target.name or target.name in {".", ".."}:
        raise SliderBindingError("private snapshot name is invalid")


def _validate_identifier(value: object, description: str) -> None:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 1024:
        raise SliderBindingError(f"{description} is invalid")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise SliderBindingError(f"{description} contains control characters")


def _validate_index(value: object, description: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _MAX_INDEX:
        raise SliderBindingError(f"{description} is invalid")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SliderBindingError as exc:
        print(f"slider binding validation blocked: {exc}", file=sys.stderr)
        sys.exit(2)

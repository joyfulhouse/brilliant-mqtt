"""No-start preflight for an isolated Brilliant Virtual Control runtime.

This module deliberately has no process creation, firmware import, socket
connection, or executable command builder.  It validates the pinned firmware
surface, provisioned private identity, and isolated filesystem topology, then
reports the still-unresolved official identity-consumer contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

_SCHEMA_VERSION = 1
_PINNED_FIRMWARE = "v26.06.03.1"
_PINNED_HASHES = {
    "bus.message_bus": "a85b7a2d0c2533db8d803a217027dbdd245bc104f221bf6955907dc0b8f6feb8",
    "lib.runner": "4ba40ac7d7695dc239590defbc6efd3d22efbf296fc1c2b40f139fb6e1fe3cb0",
    "peripherals.bootstrap.bootstrap_peripheral": (
        "313d526a3fe1ad1879137a83eaa55096d9b0fb7a08cac30e37a79ea3632d57db"
    ),
}
_MESSAGE_BUS_PARAMETERS = frozenset({"home_id", "device_id", "mb_state_dir", "is_virtual_control"})
_RUNNER_PARAMETERS = frozenset({"startable_config", "module_name_override"})
_BOOTSTRAP_FIELDS = frozenset({"target_home_id", "server_authentication_token", "wifi_variables"})
_IDENTITY_FILES = frozenset({"device_id", "pkcs12_certificate", "bootstrap", "metadata.json"})
_DEVICE_ID = re.compile(r"^[0-9a-f]{32}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_PHYSICAL_SOCKET = Path("/var/run/brilliant/server_socket")
_DEFAULT_PERSISTENT_ROOTS = (Path("/data/brilliant-vc"),)
_DEFAULT_RUNTIME_ROOTS = (Path("/run/brilliant-vc"), Path("/var/run/brilliant-vc"))
_PROTECTED_ROOTS = (
    Path("/var/device_variables"),
    Path("/var/run/brilliant"),
    Path("/var/brilliant"),
    Path("/data/switch-embedded"),
)
_PINNED_MODULE_PATHS = {
    "bus.message_bus": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/"
        "bus/message_bus.cpython-310-arm-linux-gnueabi.so"
    ),
    "lib.runner": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/"
        "lib/runner.cpython-310-arm-linux-gnueabi.so"
    ),
    "peripherals.bootstrap.bootstrap_peripheral": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/peripherals/"
        "bootstrap/bootstrap_peripheral.cpython-310-arm-linux-gnueabi.so"
    ),
}
_MAX_IDENTITY_BYTES = 1024 * 1024
_MAX_METADATA_BYTES = 64 * 1024
_MAX_MODULE_BYTES = 64 * 1024 * 1024


class LauncherPreflightError(ValueError):
    """Raised when a no-start prerequisite cannot be trusted."""


@dataclass(frozen=True, slots=True)
class LauncherPaths:
    """Every path that a future launcher would be permitted to use."""

    persistent_root: Path
    identity_dir: Path
    state_dir: Path
    certificate_dir: Path
    process_config_dir: Path
    runtime_dir: Path
    socket_path: Path


@dataclass(frozen=True, slots=True)
class NoStartPlan:
    """Redacted prerequisites report that cannot be turned into a command."""

    firmware_matches: bool
    interfaces_match: bool
    identity_inputs_valid: bool
    paths_isolated: bool
    private_modes_valid: bool
    empty_runtime_paths: bool
    identity_file_count: int
    device_id_redacted: str
    identity_contract_complete: bool
    launcher_implementation_present: bool
    start_permitted: bool
    blocked_reason: str

    def to_public_dict(self) -> dict[str, object]:
        return {
            "firmware_matches": self.firmware_matches,
            "interfaces_match": self.interfaces_match,
            "identity_inputs_valid": self.identity_inputs_valid,
            "paths_isolated": self.paths_isolated,
            "private_modes_valid": self.private_modes_valid,
            "empty_runtime_paths": self.empty_runtime_paths,
            "identity_file_count": self.identity_file_count,
            "device_id_redacted": self.device_id_redacted,
            "identity_contract_complete": self.identity_contract_complete,
            "launcher_implementation_present": self.launcher_implementation_present,
            "start_permitted": self.start_permitted,
            "blocked_reason": self.blocked_reason,
        }


def preflight_no_start(
    paths: LauncherPaths,
    firmware_snapshot: Mapping[str, object],
    *,
    actual_module_hashes: Mapping[str, object],
    required_uid: int = 0,
    allowed_persistent_roots: Sequence[Path] = _DEFAULT_PERSISTENT_ROOTS,
    allowed_runtime_roots: Sequence[Path] = _DEFAULT_RUNTIME_ROOTS,
) -> NoStartPlan:
    """Validate known prerequisites without creating a runnable launch command."""

    _validate_firmware_snapshot(firmware_snapshot)
    actual_hashes = _hash_inventory(actual_module_hashes, "actual module hash")
    if actual_hashes != _PINNED_HASHES:
        raise LauncherPreflightError("actual module hash drift blocks the launcher")
    _validate_path_topology(
        paths,
        required_uid=required_uid,
        allowed_persistent_roots=allowed_persistent_roots,
        allowed_runtime_roots=allowed_runtime_roots,
    )
    redacted_device_id = _validate_identity(paths.identity_dir, required_uid=required_uid)
    return NoStartPlan(
        firmware_matches=True,
        interfaces_match=True,
        identity_inputs_valid=True,
        paths_isolated=True,
        private_modes_valid=True,
        empty_runtime_paths=True,
        identity_file_count=len(_IDENTITY_FILES),
        device_id_redacted=redacted_device_id,
        identity_contract_complete=False,
        launcher_implementation_present=False,
        start_permitted=False,
        blocked_reason="official_identity_consumer_unresolved",
    )


def _validate_firmware_snapshot(snapshot: Mapping[str, object]) -> None:
    expected_fields = {
        "schema_version",
        "firmware_version",
        "module_sha256",
        "message_bus_parameters",
        "runner_parameters",
        "bootstrap_fields",
        "virtual_control_flag",
    }
    if set(snapshot) != expected_fields or snapshot.get("schema_version") != _SCHEMA_VERSION:
        raise LauncherPreflightError("firmware snapshot schema is invalid")
    if snapshot["firmware_version"] != _PINNED_FIRMWARE:
        raise LauncherPreflightError("firmware version does not match the pinned build")

    hashes = _hash_inventory(snapshot["module_sha256"], "firmware module hash")
    if hashes != _PINNED_HASHES:
        raise LauncherPreflightError("firmware module hash drift blocks the launcher")

    message_bus_parameters = _parameter_set(
        snapshot["message_bus_parameters"], "message-bus interface"
    )
    runner_parameters = _parameter_set(snapshot["runner_parameters"], "runner interface")
    bootstrap_fields = _parameter_set(snapshot["bootstrap_fields"], "bootstrap interface")
    if (
        not _MESSAGE_BUS_PARAMETERS <= message_bus_parameters
        or not _RUNNER_PARAMETERS <= runner_parameters
        or bootstrap_fields != _BOOTSTRAP_FIELDS
        or snapshot["virtual_control_flag"] != "start_as_virtual_control"
    ):
        raise LauncherPreflightError("firmware interface drift blocks the launcher")


def _hash_inventory(value: object, description: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != set(_PINNED_HASHES):
        raise LauncherPreflightError(f"{description} inventory is invalid")
    hashes: dict[str, str] = {}
    for name, digest in value.items():
        if (
            not isinstance(name, str)
            or not isinstance(digest, str)
            or not _SHA256.fullmatch(digest)
        ):
            raise LauncherPreflightError(f"{description} is invalid")
        hashes[name] = digest
    return hashes


def _parameter_set(value: object, description: str) -> frozenset[str]:
    if not isinstance(value, list) or not value or len(value) > 64:
        raise LauncherPreflightError(f"{description} parameter list is invalid")
    parameters: set[str] = set()
    for parameter in value:
        if not isinstance(parameter, str) or _SAFE_NAME.fullmatch(parameter) is None:
            raise LauncherPreflightError(f"{description} parameter is invalid")
        if parameter in parameters:
            raise LauncherPreflightError(f"{description} parameter is duplicated")
        parameters.add(parameter)
    return frozenset(parameters)


def _validate_path_topology(
    paths: LauncherPaths,
    *,
    required_uid: int,
    allowed_persistent_roots: Sequence[Path],
    allowed_runtime_roots: Sequence[Path],
) -> None:
    physical_socket = _PHYSICAL_SOCKET.resolve(strict=False)
    socket = paths.socket_path.resolve(strict=False)
    if socket == physical_socket:
        raise LauncherPreflightError("refusing the physical Control message-bus socket")

    persistent_root = _private_directory(
        paths.persistent_root,
        description="persistent root",
        required_uid=required_uid,
    )
    runtime_root = _private_directory(
        paths.runtime_dir,
        description="runtime root",
        required_uid=required_uid,
    )
    if persistent_root not in {root.resolve(strict=False) for root in allowed_persistent_roots}:
        raise LauncherPreflightError("persistent root is outside the allowed VC roots")
    if runtime_root not in {root.resolve(strict=False) for root in allowed_runtime_roots}:
        raise LauncherPreflightError("runtime root is outside the allowed VC roots")

    resolved_directories: list[Path] = []
    for description, directory in (
        ("identity directory", paths.identity_dir),
        ("state directory", paths.state_dir),
        ("certificate directory", paths.certificate_dir),
        ("process-config directory", paths.process_config_dir),
    ):
        resolved = _private_directory(
            directory,
            description=description,
            required_uid=required_uid,
        )
        if resolved.parent != persistent_root:
            raise LauncherPreflightError(f"{description} must be directly below persistent root")
        resolved_directories.append(resolved)
    if len(set(resolved_directories)) != len(resolved_directories):
        raise LauncherPreflightError(
            "identity, state, certificate, and config paths must be distinct"
        )

    if socket.parent != runtime_root:
        raise LauncherPreflightError("VC socket must be directly below the isolated runtime root")
    if paths.socket_path.exists() or paths.socket_path.is_symlink():
        raise LauncherPreflightError("VC socket path must not already exist")

    protected = tuple(root.resolve(strict=False) for root in _PROTECTED_ROOTS)
    for candidate in (*resolved_directories, persistent_root, runtime_root, socket):
        for protected_root in protected:
            try:
                candidate.relative_to(protected_root)
            except ValueError:
                continue
            raise LauncherPreflightError("VC path collides with a protected physical-Control path")

    for description, directory in (
        ("state directory", paths.state_dir),
        ("certificate directory", paths.certificate_dir),
        ("process-config directory", paths.process_config_dir),
        ("runtime directory", paths.runtime_dir),
    ):
        if any(directory.iterdir()):
            raise LauncherPreflightError(f"{description} must be empty before launch")


def _private_directory(path: Path, *, description: str, required_uid: int) -> Path:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise LauncherPreflightError(f"{description} does not exist") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must not be a symlink")
    if not stat.S_ISDIR(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must be a directory")
    if metadata.st_uid != required_uid or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise LauncherPreflightError(f"{description} must have the required owner and mode 0700")
    return path.resolve(strict=True)


def _validate_identity(identity_dir: Path, *, required_uid: int) -> str:
    entries = {entry.name: entry for entry in identity_dir.iterdir()}
    if set(entries) != _IDENTITY_FILES:
        raise LauncherPreflightError("identity directory must contain exactly four expected files")
    for name, path in entries.items():
        _validate_private_file(
            path,
            description=f"identity {name}",
            required_uid=required_uid,
            maximum_bytes=(
                _MAX_METADATA_BYTES
                if name in {"device_id", "metadata.json"}
                else _MAX_IDENTITY_BYTES
            ),
        )

    raw_device_id = _read_private_file(
        entries["device_id"],
        required_uid=required_uid,
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    try:
        try:
            device_id = bytes(raw_device_id).strip().decode("ascii")
        except UnicodeDecodeError:
            raise LauncherPreflightError("identity device ID is not ASCII") from None
        if _DEVICE_ID.fullmatch(device_id) is None:
            raise LauncherPreflightError("identity device ID is invalid")
    finally:
        _wipe(raw_device_id)

    raw_metadata = _read_private_file(
        entries["metadata.json"],
        required_uid=required_uid,
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    try:
        try:
            parsed: object = json.loads(raw_metadata)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise LauncherPreflightError("identity metadata is invalid JSON") from None
    finally:
        _wipe(raw_metadata)
    if not isinstance(parsed, dict) or set(parsed) != {
        "device_id_redacted",
        "target_home_match",
    }:
        raise LauncherPreflightError("identity metadata schema is invalid")
    expected_redacted = _redact(device_id)
    if parsed["device_id_redacted"] != expected_redacted or parsed["target_home_match"] is not True:
        raise LauncherPreflightError("identity metadata does not match the provisioned identity")
    return expected_redacted


def _validate_private_file(
    path: Path,
    *,
    description: str,
    required_uid: int,
    maximum_bytes: int,
) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise LauncherPreflightError(f"{description} does not exist") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must not be a symlink")
    if not stat.S_ISREG(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must be a regular file")
    if metadata.st_uid != required_uid or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise LauncherPreflightError(f"{description} must have the required owner and mode 0600")
    if metadata.st_nlink != 1:
        raise LauncherPreflightError(f"{description} must not be a hard link")
    if not 0 < metadata.st_size <= maximum_bytes:
        raise LauncherPreflightError(f"{description} has an invalid size")


def _read_private_file(path: Path, *, required_uid: int, maximum_bytes: int) -> bytearray:
    _validate_private_file(
        path,
        description=path.name,
        required_uid=required_uid,
        maximum_bytes=maximum_bytes,
    )
    before = path.lstat()
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise LauncherPreflightError("could not safely open private identity file") from None
    data = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise LauncherPreflightError("private identity file changed during open")
        while True:
            chunk = os.read(descriptor, min(8192, maximum_bytes + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > maximum_bytes:
                raise LauncherPreflightError("private identity file exceeds its size bound")
        return data
    except Exception:
        _wipe(data)
        raise
    finally:
        os.close(descriptor)


def _redact(device_id: str) -> str:
    return f"{device_id[:4]}…{device_id[-4:]}"


def _wipe(value: bytearray) -> None:
    for index in range(len(value)):
        value[index] = 0


def hash_firmware_modules(
    *,
    module_paths: Mapping[str, Path] = _PINNED_MODULE_PATHS,
    required_uid: int = 0,
) -> dict[str, str]:
    """Hash the three actual firmware modules without following links."""

    if set(module_paths) != set(_PINNED_HASHES):
        raise LauncherPreflightError("actual module path inventory is invalid")
    result: dict[str, str] = {}
    for name, path in module_paths.items():
        try:
            before = path.lstat()
        except FileNotFoundError:
            raise LauncherPreflightError("actual module file does not exist") from None
        if stat.S_ISLNK(before.st_mode):
            raise LauncherPreflightError("actual module file must not be a symlink")
        if not stat.S_ISREG(before.st_mode):
            raise LauncherPreflightError("actual module file must be regular")
        if before.st_uid != required_uid:
            raise LauncherPreflightError("actual module file has the wrong owner")
        if not 0 < before.st_size <= _MAX_MODULE_BYTES:
            raise LauncherPreflightError("actual module file has an invalid size")

        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError:
            raise LauncherPreflightError("could not safely open actual module file") from None
        digest = hashlib.sha256()
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise LauncherPreflightError("actual module file changed during open")
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
            after = os.fstat(descriptor)
            if (
                after.st_size != opened.st_size
                or after.st_mtime_ns != opened.st_mtime_ns
                or after.st_ctime_ns != opened.st_ctime_ns
            ):
                raise LauncherPreflightError("actual module file changed while hashing")
        finally:
            os.close(descriptor)
        result[name] = digest.hexdigest()
    return result


def load_firmware_snapshot(path: Path, *, required_uid: int = 0) -> dict[str, object]:
    """Load a bounded private introspection snapshot without following links."""

    raw = _read_private_file(
        path,
        required_uid=required_uid,
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    try:
        try:
            parsed: object = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise LauncherPreflightError("firmware snapshot is not valid JSON") from None
    finally:
        _wipe(raw)
    if not isinstance(parsed, dict):
        raise LauncherPreflightError("firmware snapshot must be an object")
    return parsed


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--firmware-snapshot", type=Path, required=True)
    parser.add_argument("--persistent-root", type=Path, default=Path("/data/brilliant-vc"))
    parser.add_argument("--identity-dir", type=Path, default=Path("/data/brilliant-vc/identity"))
    parser.add_argument("--state-dir", type=Path, default=Path("/data/brilliant-vc/state"))
    parser.add_argument(
        "--certificate-dir",
        type=Path,
        default=Path("/data/brilliant-vc/certificates"),
    )
    parser.add_argument(
        "--process-config-dir",
        type=Path,
        default=Path("/data/brilliant-vc/process-config"),
    )
    parser.add_argument("--runtime-dir", type=Path, default=Path("/run/brilliant-vc"))
    parser.add_argument("--socket-path", type=Path, default=Path("/run/brilliant-vc/server_socket"))
    args = parser.parse_args(argv)
    required_uid = os.geteuid()
    snapshot = load_firmware_snapshot(
        args.firmware_snapshot,
        required_uid=required_uid,
    )
    actual_module_hashes = hash_firmware_modules(required_uid=required_uid)
    plan = preflight_no_start(
        LauncherPaths(
            persistent_root=args.persistent_root,
            identity_dir=args.identity_dir,
            state_dir=args.state_dir,
            certificate_dir=args.certificate_dir,
            process_config_dir=args.process_config_dir,
            runtime_dir=args.runtime_dir,
            socket_path=args.socket_path,
        ),
        snapshot,
        actual_module_hashes=actual_module_hashes,
        required_uid=required_uid,
    )
    print(json.dumps(plan.to_public_dict(), sort_keys=True))
    return 0 if plan.start_permitted else 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except LauncherPreflightError as exc:
        print(f"VC launcher preflight blocked: {exc}", file=sys.stderr)
        sys.exit(2)

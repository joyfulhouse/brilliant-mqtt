"""Prepare one approved non-root Virtual Control run without starting it.

The stock firmware's ``run.pre_exec`` step must create the message-bus vassal
before uWSGI Emperor starts. This module validates the pinned firmware, final
runtime credential boundary, isolated service-owned paths, and the short-lived
root-issued approval marker already consumed by the service's stock mover.
Apply mode calls only ``run.pre_exec`` and hardens its generated files. It has
no approval-write, shell, socket, uWSGI, Emperor, or managed-process-start
capability. Selected firmware imports can still invoke bounded platform lookup
helpers.
"""

from __future__ import annotations

import argparse
import configparser
import grp
import importlib
import json
import os
import pwd
import re
import secrets
import stat
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from tools.brilliant_vc._common import RUNTIME_USER as _RUNTIME_USER
from tools.brilliant_vc._common import fsync_directory as _fsync_directory
from tools.brilliant_vc._common import redact as _redact
from tools.brilliant_vc._common import wipe as _wipe
from tools.brilliant_vc.launcher_preflight import (
    LauncherPaths,
    LauncherPreflightError,
    hash_firmware_modules,
    validate_actual_module_hashes,
)
from tools.brilliant_vc.runtime_handoff import (
    RuntimeHandoffError,
    runtime_credential_bundle_sha256,
    validate_pem_identity,
)
from tools.brilliant_vc.start_approval import validate_start_approval
from tools.brilliant_vc.vassal_manifest import ManifestError, build_candidate_manifest

_APPROVAL_SOURCE_PATH = Path("/run/brilliant-vc-approval/start-approval.json")
_APPROVAL_MARKER_PATH = Path("/run/brilliant-vc-approval/start-approval-consumed.json")
_DEVICE_ID = re.compile(r"^[0-9a-f]{32}$")
_SAFE_FLAG = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_MAX_METADATA_BYTES = 64 * 1024
_MAX_DEVICE_ID_BYTES = 64 * 1024
_MAX_BOOTSTRAP_BYTES = 1024 * 1024
_MAX_PEM_BYTES = 128 * 1024
_DEFAULT_PERSISTENT_ROOTS = (Path("/data/brilliant-vc"),)
_DEFAULT_RUNTIME_ROOTS = (Path("/run/brilliant-vc"), Path("/var/run/brilliant-vc"))
_DEFAULT_CREDENTIAL_PATHS = (Path("/data/brilliant-vc-credentials"),)
_DEFAULT_APPROVAL_MARKER_PATHS = (_APPROVAL_MARKER_PATH,)
_DEFAULT_UNCONSUMED_APPROVAL_PATHS = (_APPROVAL_SOURCE_PATH,)
_RUNTIME_CREDENTIAL_ENTRIES = frozenset({"device_id", "bootstrap", "certificates"})
_CERTIFICATE_ENTRIES = frozenset({"device.key", "device.cert"})
_GENERATED_DIRECTORIES = (
    "process_config_dir",
    "process_flagfile_dir",
    "startable_config_dir",
    "error_log_dir",
)
_EXPECTED_GENERATED_FILES = {
    "process_config_dir": frozenset({"message_bus.ini"}),
    "process_flagfile_dir": frozenset(
        {
            "message_bus_flagfile",
            "discovery_peripheral_flagfile",
            "config_peripherals_flagfile",
            "bootstrap_flagfile",
        }
    ),
    "startable_config_dir": frozenset({"message_bus", "config_peripherals"}),
    "error_log_dir": frozenset(),
}
_MAX_GENERATED_FILE_BYTES = 1024 * 1024
_ENABLED_PROCESS_NAMES = (
    "message_bus",
    "discovery_peripheral",
    "config_peripherals",
    "bootstrap",
)


class RuntimePrepareError(ValueError):
    """Raised before a stock runtime surface can be prepared safely."""


class FirmwarePreparer(Protocol):
    """Narrow adapter for the captured, no-start ``run.pre_exec`` call."""

    def prepare(self, argv: Sequence[str], *, runtime_user: str) -> None: ...


class RuntimeApproval(Protocol):
    """Minimum validated approval identity consumed by stock preparation."""

    @property
    def run_id(self) -> str: ...

    @property
    def sha256(self) -> str: ...

    @property
    def runtime_credential_bundle_sha256(self) -> str: ...


class RuntimeApprovalValidator(Protocol):
    """Exact keyword contract for a separately selected approval schema."""

    def __call__(
        self,
        path: Path,
        *,
        now_s: int,
        credential_uid: int,
        runtime_gid: int,
        allowed_paths: Sequence[Path],
    ) -> RuntimeApproval: ...


_DEFAULT_APPROVAL_VALIDATOR = cast(RuntimeApprovalValidator, validate_start_approval)


@dataclass(frozen=True, slots=True)
class RuntimePrepareResult:
    """Secret-free result for the VC gate ledger and systemd pre-start."""

    dry_run: bool
    firmware_matches: bool
    runtime_identity_valid: bool
    runtime_credentials_valid: bool
    approval_validated: bool
    preparation_complete: bool
    approval_consumed: bool
    initial_vassals: tuple[str, ...]
    device_id_redacted: str
    runtime_credential_bundle_sha256: str
    approval_run_id: str | None
    approval_sha256: str | None
    disabled_process_count: int
    contains_emperor_start_primitive: bool
    emperor_started: bool
    blocked_reason: str | None

    def to_public_dict(self) -> dict[str, object]:
        return {
            "dry_run": self.dry_run,
            "firmware_matches": self.firmware_matches,
            "runtime_identity_valid": self.runtime_identity_valid,
            "runtime_credentials_valid": self.runtime_credentials_valid,
            "approval_validated": self.approval_validated,
            "preparation_complete": self.preparation_complete,
            "approval_consumed": self.approval_consumed,
            "initial_vassals": list(self.initial_vassals),
            "device_id_redacted": self.device_id_redacted,
            "runtime_credential_bundle_sha256": self.runtime_credential_bundle_sha256,
            "approval_run_id": self.approval_run_id,
            "approval_sha256": self.approval_sha256,
            "disabled_process_count": self.disabled_process_count,
            "contains_emperor_start_primitive": self.contains_emperor_start_primitive,
            "emperor_started": self.emperor_started,
            "blocked_reason": self.blocked_reason,
        }


class _StockFirmwarePreparer:
    """Deferred adapter that imports firmware only after every apply guard."""

    def prepare(self, argv: Sequence[str], *, runtime_user: str) -> None:
        try:
            run_module: Any = importlib.import_module("run")
            process_configs: Any = run_module.process_configs
            all_configs = tuple(process_configs.get_all_configs())
            disabled = _disabled_process_names(argv)
            by_name: dict[str, Any] = {}
            for config in all_configs:
                name = config.process_name
                if not isinstance(name, str) or name in by_name:
                    raise RuntimePrepareError("captured process inventory drift blocks preparation")
                by_name[name] = config
            expected = disabled | set(_ENABLED_PROCESS_NAMES)
            if (
                len(disabled) != 34
                or disabled & set(_ENABLED_PROCESS_NAMES)
                or set(by_name) != expected
            ):
                raise RuntimePrepareError("captured process inventory drift blocks preparation")
            discovery_configs = tuple(by_name[name] for name in _ENABLED_PROCESS_NAMES)
            socket_parameters: Any = importlib.import_module("configs.socket_parameters")
            original_get_all_configs = process_configs.get_all_configs
            process_configs.get_all_configs = lambda: discovery_configs
            try:
                run_module.add_undefined_gflags(socket_parameters.get_uwsgi_socket_parameters())
            finally:
                process_configs.get_all_configs = original_get_all_configs
            run_module.FLAGS(list(argv))
            selected_configs = tuple(original_get_all_configs())
            selected_names = tuple(config.process_name for config in selected_configs)
            if selected_names != _ENABLED_PROCESS_NAMES:
                raise RuntimePrepareError("captured selected-process inventory drift")
            process_configs.get_all_configs = lambda: selected_configs
            try:
                run_module.pre_exec(unprivileged_user=runtime_user)
            finally:
                process_configs.get_all_configs = original_get_all_configs
        except RuntimePrepareError:
            raise
        except Exception:
            raise RuntimePrepareError("captured firmware pre_exec failed") from None


def _disabled_process_names(argv: Sequence[str]) -> set[str]:
    prefix = "--disable_peripherals="
    values = [argument.removeprefix(prefix) for argument in argv if argument.startswith(prefix)]
    if len(values) != 1:
        raise RuntimePrepareError("candidate disable set is invalid")
    names = values[0].split(",")
    if len(names) != len(set(names)) or any(_SAFE_FLAG.fullmatch(name) is None for name in names):
        raise RuntimePrepareError("candidate disable set is invalid")
    return set(names)


def prepare_runtime_no_start(
    paths: LauncherPaths,
    *,
    now_s: int,
    apply: bool,
    approval_marker: Path | None,
    runtime_user: str,
    runtime_uid: int,
    runtime_gid: int,
    actual_module_hashes: Mapping[str, object],
    firmware_preparer: FirmwarePreparer | None = None,
    approval_validator: RuntimeApprovalValidator = _DEFAULT_APPROVAL_VALIDATOR,
    credential_uid: int = 0,
    allowed_persistent_roots: Sequence[Path] = _DEFAULT_PERSISTENT_ROOTS,
    allowed_runtime_roots: Sequence[Path] = _DEFAULT_RUNTIME_ROOTS,
    allowed_runtime_credential_paths: Sequence[Path] = _DEFAULT_CREDENTIAL_PATHS,
    allowed_approval_marker_paths: Sequence[Path] = _DEFAULT_APPROVAL_MARKER_PATHS,
    unconsumed_approval_paths: Sequence[Path] = _DEFAULT_UNCONSUMED_APPROVAL_PATHS,
) -> RuntimePrepareResult:
    """Validate, and optionally prepare, the no-start stock runtime surface."""

    _validate_scalar_inputs(
        now_s=now_s,
        apply=apply,
        runtime_user=runtime_user,
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
        credential_uid=credential_uid,
    )
    try:
        validate_actual_module_hashes(actual_module_hashes)
    except LauncherPreflightError as error:
        raise RuntimePrepareError(str(error)) from None
    _validate_service_paths(
        paths,
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
        credential_uid=credential_uid,
        allowed_persistent_roots=allowed_persistent_roots,
        allowed_runtime_roots=allowed_runtime_roots,
        allowed_runtime_credential_paths=allowed_runtime_credential_paths,
    )
    device_id, credential_bundle_digest = _validate_runtime_credentials(
        paths,
        now_s=now_s,
        credential_uid=credential_uid,
        runtime_gid=runtime_gid,
    )
    argv, disabled_count = _build_firmware_argv(paths, device_id=device_id)
    redacted_device_id = _redact(device_id)
    if not apply:
        return RuntimePrepareResult(
            dry_run=True,
            firmware_matches=True,
            runtime_identity_valid=True,
            runtime_credentials_valid=True,
            approval_validated=False,
            preparation_complete=False,
            approval_consumed=False,
            initial_vassals=(),
            device_id_redacted=redacted_device_id,
            runtime_credential_bundle_sha256=credential_bundle_digest,
            approval_run_id=None,
            approval_sha256=None,
            disabled_process_count=disabled_count,
            contains_emperor_start_primitive=False,
            emperor_started=False,
            blocked_reason="fresh_start_approval_required",
        )

    if approval_marker is None:
        raise RuntimePrepareError("apply requires a consumed approval marker")
    if any(path.exists() or path.is_symlink() for path in unconsumed_approval_paths):
        raise RuntimePrepareError("unconsumed start approval still exists")
    marker_parent = _directory(
        approval_marker.parent,
        description="approval control directory",
        uid=credential_uid,
        gid=runtime_gid,
        mode=0o750,
    )
    if any(
        _paths_overlap(marker_parent, writable_root)
        for writable_root in (paths.persistent_root, paths.runtime_dir)
    ):
        raise RuntimePrepareError("approval control directory overlaps a writable root")
    try:
        approval = approval_validator(
            approval_marker,
            now_s=now_s,
            credential_uid=credential_uid,
            runtime_gid=runtime_gid,
            allowed_paths=allowed_approval_marker_paths,
        )
    except ValueError as error:
        raise RuntimePrepareError(str(error)) from None
    if not secrets.compare_digest(
        approval.runtime_credential_bundle_sha256,
        credential_bundle_digest,
    ):
        raise RuntimePrepareError("approval does not bind the runtime credential bundle")
    preparer = firmware_preparer or _StockFirmwarePreparer()
    previous_umask = os.umask(0o077)
    try:
        try:
            preparer.prepare(argv, runtime_user=runtime_user)
        except RuntimePrepareError:
            raise
        except Exception:
            raise RuntimePrepareError("captured firmware pre_exec failed") from None
    finally:
        os.umask(previous_umask)
    initial_vassals = _harden_and_validate_generated_surface(
        paths,
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
    )
    _validate_generated_contents(
        paths,
        argv=argv,
        device_id=device_id,
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
    )
    _validate_post_prepare_surface(
        paths,
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
    )
    return RuntimePrepareResult(
        dry_run=False,
        firmware_matches=True,
        runtime_identity_valid=True,
        runtime_credentials_valid=True,
        approval_validated=True,
        preparation_complete=True,
        approval_consumed=True,
        initial_vassals=initial_vassals,
        device_id_redacted=redacted_device_id,
        runtime_credential_bundle_sha256=credential_bundle_digest,
        approval_run_id=approval.run_id,
        approval_sha256=approval.sha256,
        disabled_process_count=disabled_count,
        contains_emperor_start_primitive=False,
        emperor_started=False,
        blocked_reason=None,
    )


def _validate_scalar_inputs(
    *,
    now_s: int,
    apply: bool,
    runtime_user: str,
    runtime_uid: int,
    runtime_gid: int,
    credential_uid: int,
) -> None:
    if isinstance(now_s, bool) or not isinstance(now_s, int) or now_s <= 0:
        raise RuntimePrepareError("current timestamp is invalid")
    if not isinstance(apply, bool):
        raise RuntimePrepareError("apply must be a boolean")
    if runtime_user != _RUNTIME_USER:
        raise RuntimePrepareError("runtime user must match the pinned dedicated account")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in (runtime_uid, runtime_gid, credential_uid)
    ):
        raise RuntimePrepareError("runtime or credential identity is invalid")
    if runtime_uid == 0 or runtime_gid == 0:
        raise RuntimePrepareError("runtime preparer must execute as a non-root identity")


def _validate_service_paths(
    paths: LauncherPaths,
    *,
    runtime_uid: int,
    runtime_gid: int,
    credential_uid: int,
    allowed_persistent_roots: Sequence[Path],
    allowed_runtime_roots: Sequence[Path],
    allowed_runtime_credential_paths: Sequence[Path],
) -> None:
    persistent = _directory(
        paths.persistent_root,
        description="persistent root",
        uid=runtime_uid,
        gid=runtime_gid,
        mode=0o700,
    )
    runtime = _directory(
        paths.runtime_dir,
        description="runtime directory",
        uid=runtime_uid,
        gid=runtime_gid,
        mode=0o700,
    )
    credentials = _directory(
        paths.runtime_credential_dir,
        description="runtime credential directory",
        uid=credential_uid,
        gid=runtime_gid,
        mode=0o750,
    )
    if persistent not in {path.resolve(strict=False) for path in allowed_persistent_roots}:
        raise RuntimePrepareError("persistent root is outside the allowed roots")
    if runtime not in {path.resolve(strict=False) for path in allowed_runtime_roots}:
        raise RuntimePrepareError("runtime directory is outside the allowed roots")
    if credentials not in {path.resolve(strict=False) for path in allowed_runtime_credential_paths}:
        raise RuntimePrepareError("runtime credentials are outside the allowed roots")
    if any(
        _paths_overlap(left, right)
        for left, right in (
            (persistent, runtime),
            (persistent, credentials),
            (runtime, credentials),
        )
    ):
        raise RuntimePrepareError("runtime roots must be disjoint")
    art_preload = _readonly_directory(
        paths.art_preload_dir,
        description="art preload directory",
        uid=credential_uid,
        mode=0o755,
    )
    if any(
        _paths_overlap(art_preload, service_path)
        for service_path in (persistent, runtime, credentials)
    ):
        raise RuntimePrepareError(
            "art preload directory must be outside service-owned runtime roots"
        )

    children: list[Path] = []
    for description, path in (
        ("state directory", paths.state_dir),
        ("process-config directory", paths.process_config_dir),
        ("process-flagfile directory", paths.process_flagfile_dir),
        ("startable-config directory", paths.startable_config_dir),
        ("log directory", paths.log_dir),
        ("error-log directory", paths.error_log_dir),
        ("trace directory", paths.trace_dir),
    ):
        resolved = _directory(
            path,
            description=description,
            uid=runtime_uid,
            gid=runtime_gid,
            mode=0o700,
        )
        if resolved.parent != persistent:
            raise RuntimePrepareError(f"{description} must be directly below persistent root")
        children.append(resolved)
    if len(children) != len(set(children)):
        raise RuntimePrepareError("service-owned runtime directories must be distinct")
    expected_persistent_entries = {child.name for child in children}
    if _entry_names(persistent, description="persistent root") != expected_persistent_entries:
        raise RuntimePrepareError("persistent root has an unexpected entry inventory")
    for description, directory in (
        ("state directory", paths.state_dir),
        ("process-config directory", paths.process_config_dir),
        ("process-flagfile directory", paths.process_flagfile_dir),
        ("startable-config directory", paths.startable_config_dir),
        ("log directory", paths.log_dir),
        ("error-log directory", paths.error_log_dir),
        ("trace directory", paths.trace_dir),
        ("runtime directory", paths.runtime_dir),
    ):
        try:
            nonempty = any(directory.iterdir())
        except OSError:
            raise RuntimePrepareError(f"could not inspect {description}") from None
        if nonempty:
            raise RuntimePrepareError(f"{description} must be empty before preparation")
    if paths.socket_path.resolve(strict=False).parent != runtime:
        raise RuntimePrepareError("message-bus socket must be below the runtime directory")
    if paths.stats_socket_path.resolve(strict=False).parent != runtime:
        raise RuntimePrepareError("stats socket must be below the runtime directory")
    if paths.socket_path.resolve(strict=False) == paths.stats_socket_path.resolve(strict=False):
        raise RuntimePrepareError("message-bus and stats sockets must be distinct")
    _readonly_metadata(
        paths.release_info_path,
        description="release metadata",
        uid=credential_uid,
    )
    _readonly_metadata(
        paths.tracking_branch_path,
        description="tracking metadata",
        uid=credential_uid,
    )
    try:
        build_candidate_manifest(paths)
    except ManifestError as error:
        raise RuntimePrepareError(str(error)) from None


def _validate_runtime_credentials(
    paths: LauncherPaths,
    *,
    now_s: int,
    credential_uid: int,
    runtime_gid: int,
) -> tuple[str, str]:
    root = paths.runtime_credential_dir.resolve(strict=True)
    try:
        entries = {entry.name: entry for entry in root.iterdir()}
    except OSError:
        raise RuntimePrepareError("could not inspect runtime credentials") from None
    if set(entries) != _RUNTIME_CREDENTIAL_ENTRIES:
        raise RuntimePrepareError("runtime credential directory has unexpected entries")
    certificate_dir = _directory(
        entries["certificates"],
        description="runtime certificate directory",
        uid=credential_uid,
        gid=runtime_gid,
        mode=0o750,
    )
    if certificate_dir != paths.certificate_dir.resolve(strict=False):
        raise RuntimePrepareError("runtime certificate directory is not canonical")
    if entries["bootstrap"].resolve(strict=False) != paths.bootstrap_path.resolve(strict=False):
        raise RuntimePrepareError("runtime bootstrap path is not canonical")
    try:
        certificate_entries = {entry.name: entry for entry in certificate_dir.iterdir()}
    except OSError:
        raise RuntimePrepareError("could not inspect runtime certificates") from None
    if set(certificate_entries) != _CERTIFICATE_ENTRIES:
        raise RuntimePrepareError("runtime certificate directory has unexpected entries")

    raw_device_id = _read_file(
        entries["device_id"],
        description="runtime device_id",
        uid=credential_uid,
        gid=runtime_gid,
        mode=0o640,
        maximum_bytes=_MAX_DEVICE_ID_BYTES,
    )
    try:
        try:
            device_id = bytes(raw_device_id).strip().decode("ascii")
        except UnicodeDecodeError:
            raise RuntimePrepareError("runtime device ID is not ASCII") from None
        if _DEVICE_ID.fullmatch(device_id) is None:
            raise RuntimePrepareError("runtime device ID is invalid")
        if bytes(raw_device_id) != f"{device_id}\n".encode("ascii"):
            raise RuntimePrepareError("runtime device ID encoding is not canonical")
    finally:
        _wipe(raw_device_id)

    bootstrap = bytearray()
    private_key = bytearray()
    certificate = bytearray()
    try:
        bootstrap = _read_file(
            entries["bootstrap"],
            description="runtime bootstrap",
            uid=credential_uid,
            gid=runtime_gid,
            mode=0o640,
            maximum_bytes=_MAX_BOOTSTRAP_BYTES,
        )
        private_key = _read_file(
            certificate_entries["device.key"],
            description="runtime device.key",
            uid=credential_uid,
            gid=runtime_gid,
            mode=0o640,
            maximum_bytes=_MAX_PEM_BYTES,
        )
        certificate = _read_file(
            certificate_entries["device.cert"],
            description="runtime device.cert",
            uid=credential_uid,
            gid=runtime_gid,
            mode=0o640,
            maximum_bytes=_MAX_PEM_BYTES,
        )
        try:
            validate_pem_identity(
                bytes(private_key),
                bytes(certificate),
                device_id,
                now_s,
            )
        except RuntimeHandoffError as error:
            raise RuntimePrepareError(str(error)) from None
        try:
            bundle_digest = runtime_credential_bundle_sha256(
                {
                    "device_id": f"{device_id}\n".encode("ascii"),
                    "bootstrap": bootstrap,
                    "device.key": private_key,
                    "device.cert": certificate,
                }
            )
        except RuntimeHandoffError as error:
            raise RuntimePrepareError(str(error)) from None
    finally:
        for value in (bootstrap, private_key, certificate):
            _wipe(value)
    return device_id, bundle_digest


def _build_firmware_argv(
    paths: LauncherPaths,
    *,
    device_id: str,
) -> tuple[tuple[str, ...], int]:
    try:
        manifest = build_candidate_manifest(paths).to_public_dict()
    except ManifestError as error:
        raise RuntimePrepareError(str(error)) from None
    raw_flags = manifest.get("flags")
    if not isinstance(raw_flags, dict):
        raise RuntimePrepareError("candidate manifest flags are invalid")
    flags = cast(dict[str, object], raw_flags)
    flags = dict(flags)
    flags["device_id"] = device_id
    disabled = flags.get("disable_peripherals")
    if not isinstance(disabled, list) or len(disabled) != 34:
        raise RuntimePrepareError("candidate disable set is invalid")
    argv = ["brilliant-vc-runtime-prepare"]
    for name in sorted(flags):
        if _SAFE_FLAG.fullmatch(name) is None:
            raise RuntimePrepareError("candidate flag name is invalid")
        value = flags[name]
        if value is None:
            continue
        rendered: str
        if isinstance(value, bool):
            rendered = "True" if value else "False"
        elif isinstance(value, int):
            rendered = str(value)
        elif isinstance(value, str):
            rendered = value
        elif isinstance(value, list) and all(isinstance(item, str) for item in value):
            if any("," in item for item in value):
                raise RuntimePrepareError("candidate list flag contains a delimiter")
            rendered = ",".join(cast(list[str], value))
        else:
            raise RuntimePrepareError("candidate flag value is invalid")
        if not rendered or "\x00" in rendered or "\n" in rendered or "\r" in rendered:
            raise RuntimePrepareError("candidate flag value is unsafe")
        argv.append(f"--{name}={rendered}")
    return tuple(argv), len(disabled)


def _harden_and_validate_generated_surface(
    paths: LauncherPaths,
    *,
    runtime_uid: int,
    runtime_gid: int,
) -> tuple[str, ...]:
    for attribute in _GENERATED_DIRECTORIES:
        path = cast(Path, getattr(paths, attribute))
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            raise RuntimePrepareError("pre_exec omitted a generated directory") from None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise RuntimePrepareError("generated directory must be real")
        if metadata.st_uid != runtime_uid or metadata.st_gid != runtime_gid:
            raise RuntimePrepareError("generated directory has the wrong runtime identity")
        os.chmod(path, 0o700)
        try:
            children = tuple(path.iterdir())
        except OSError:
            raise RuntimePrepareError("could not inspect generated directory") from None
        child_names = {child.name for child in children}
        if child_names != _EXPECTED_GENERATED_FILES[attribute]:
            raise RuntimePrepareError("pre_exec generated an unexpected file inventory")
        for child in children:
            _harden_generated_file(
                child,
                runtime_uid=runtime_uid,
                runtime_gid=runtime_gid,
            )
        _fsync_directory(path)
    return tuple(sorted(path.name for path in paths.process_config_dir.iterdir()))


def _validate_post_prepare_surface(
    paths: LauncherPaths,
    *,
    runtime_uid: int,
    runtime_gid: int,
) -> None:
    persistent = _directory(
        paths.persistent_root,
        description="persistent root",
        uid=runtime_uid,
        gid=runtime_gid,
        mode=0o700,
    )
    runtime = _directory(
        paths.runtime_dir,
        description="runtime directory",
        uid=runtime_uid,
        gid=runtime_gid,
        mode=0o700,
    )
    expected_persistent_entries = {
        path.name
        for path in (
            paths.state_dir,
            paths.process_config_dir,
            paths.process_flagfile_dir,
            paths.startable_config_dir,
            paths.log_dir,
            paths.error_log_dir,
            paths.trace_dir,
        )
    }
    if _entry_names(persistent, description="persistent root") != expected_persistent_entries:
        raise RuntimePrepareError("pre_exec changed the persistent root inventory")
    for description, path in (
        ("state directory", paths.state_dir),
        ("log directory", paths.log_dir),
        ("trace directory", paths.trace_dir),
    ):
        _directory(
            path,
            description=description,
            uid=runtime_uid,
            gid=runtime_gid,
            mode=0o700,
        )
        if _entry_names(path, description=description):
            raise RuntimePrepareError(f"{description} must remain empty after preparation")
    if _entry_names(runtime, description="runtime directory"):
        raise RuntimePrepareError("runtime directory must remain empty after preparation")
    _fsync_directory(persistent)
    _fsync_directory(runtime)


def _harden_generated_file(path: Path, *, runtime_uid: int, runtime_gid: int) -> None:
    try:
        before = path.lstat()
    except OSError:
        raise RuntimePrepareError("could not inspect generated file") from None
    if not stat.S_ISREG(before.st_mode):
        raise RuntimePrepareError("generated file must be regular")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise RuntimePrepareError("could not safely open generated file") from None
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise RuntimePrepareError("generated file changed during open")
        if not stat.S_ISREG(opened.st_mode):
            raise RuntimePrepareError("generated file must be regular")
        if opened.st_nlink != 1:
            raise RuntimePrepareError("generated file must not be a hard link")
        if opened.st_uid != runtime_uid or opened.st_gid != runtime_gid:
            raise RuntimePrepareError("generated file has the wrong runtime identity")
        if not 0 < opened.st_size <= _MAX_GENERATED_FILE_BYTES:
            raise RuntimePrepareError("generated file has an invalid size")
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_generated_contents(
    paths: LauncherPaths,
    *,
    argv: Sequence[str],
    device_id: str,
    runtime_uid: int,
    runtime_gid: int,
) -> None:
    base_flags = {
        "asyncio_debug": "False",
        "enable_uwsgi_heartbeat": "True",
        "error_log_sample_rate": "0.0",
        "error_log_storage_dir": str(paths.error_log_dir),
        "log_level": "INFO",
        "log_output_directory": str(paths.log_dir),
        "message_bus_server_socket_path": str(paths.socket_path),
        "process_configs_dir": str(paths.process_config_dir),
        "release_info_filepath": str(paths.release_info_path),
        "socket_timeout_seconds": "5",
        "thrift_serialization_validation_mode": "loose",
        "trace_dir": str(paths.trace_dir),
        "tracking_branch_filepath": str(paths.tracking_branch_path),
    }
    disable_value = _single_argv_value(argv, "disable_peripherals")
    expected_flagfiles = {
        "message_bus_flagfile": {
            **base_flags,
            "cert_dir": str(paths.certificate_dir),
            "device_id": device_id,
            "disable_peripherals": disable_value,
            "home_id": "0",
            "mb_state_dir": str(paths.state_dir),
            "message_bus_unprivileged_user": _RUNTIME_USER,
            "process_flagfiles_dir": str(paths.process_flagfile_dir),
            "start_as_virtual_control": "True",
            "startable_host_configs_dir": str(paths.startable_config_dir),
        },
        "discovery_peripheral_flagfile": {
            **base_flags,
            "discovery_peripheral_enable_remote_bridge_service_discovery": "True",
        },
        "config_peripherals_flagfile": dict(base_flags),
        "bootstrap_flagfile": {
            **base_flags,
            "bootstrap_max_provisioning_attempts_per_code": "1",
            "bootstrap_web_api_homes_endpoint": "/homes",
            "cert_dir": str(paths.certificate_dir),
            "saved_bootstrap_parameters_path": str(paths.bootstrap_path),
            "stub_bootstrap": "False",
        },
    }
    for name, expected in expected_flagfiles.items():
        actual = _parse_flagfile(
            paths.process_flagfile_dir / name,
            runtime_uid=runtime_uid,
            runtime_gid=runtime_gid,
        )
        if actual != expected:
            raise RuntimePrepareError("generated flagfile contract drift blocks preparation")

    expected_inis = {
        paths.process_config_dir / "message_bus.ini": {
            "uwsgi": {
                "startable_module": "bus.message_bus",
                "flagfile": str(paths.process_flagfile_dir / "message_bus_flagfile"),
                "additionalflags": "",
                "prio": "-10",
                "user_override": str(runtime_uid),
                "group_override": str(runtime_gid),
            }
        },
        paths.startable_config_dir / "message_bus": {
            "remote_bridge": {
                "module_path": "bridge.remote_bridge",
                "listen_port": "15455",
                "enable_bluetooth_provisioning": "False",
                "device_provisioning_ip_listen_port": "0",
                "enforce_strict_authentication": "True",
                "ble_mesh_debug_interface_listen_port": "0",
                "stub_ble_peripheral": "True",
                "uwsgi_stats_socket_path": str(paths.stats_socket_path),
            }
        },
        paths.startable_config_dir / "config_peripherals": {
            "art_config_peripheral": {
                "module_path": "peripherals.configs.art_config_peripheral",
                "art_preload_dir": str(paths.art_preload_dir),
            },
            "device_config_peripheral": {
                "module_path": "peripherals.configs.device_config_peripheral",
            },
            "motion_detection_config_peripheral": {
                "module_path": "peripherals.configs.motion_detection_config_peripheral",
            },
            "alarm_config_peripheral": {
                "module_path": "peripherals.configs.alarm_config_peripheral",
            },
        },
    }
    for path, expected_ini in expected_inis.items():
        if _parse_ini_file(path, runtime_uid=runtime_uid, runtime_gid=runtime_gid) != expected_ini:
            raise RuntimePrepareError("generated INI contract drift blocks preparation")


def _single_argv_value(argv: Sequence[str], name: str) -> str:
    prefix = f"--{name}="
    values = [argument.removeprefix(prefix) for argument in argv if argument.startswith(prefix)]
    if len(values) != 1 or not values[0]:
        raise RuntimePrepareError("candidate argv contract is invalid")
    return values[0]


def _parse_flagfile(path: Path, *, runtime_uid: int, runtime_gid: int) -> dict[str, str]:
    raw = _read_file(
        path,
        description="generated flagfile",
        uid=runtime_uid,
        gid=runtime_gid,
        mode=0o600,
        maximum_bytes=_MAX_GENERATED_FILE_BYTES,
    )
    try:
        try:
            text = bytes(raw).decode("utf-8")
        except UnicodeDecodeError:
            raise RuntimePrepareError("generated flagfile is not UTF-8") from None
    finally:
        _wipe(raw)
    if not text.endswith("\n") or "\r" in text:
        raise RuntimePrepareError("generated flagfile line endings are invalid")
    result: dict[str, str] = {}
    for line in text.splitlines():
        if not line.startswith("--") or "=" not in line:
            raise RuntimePrepareError("generated flagfile syntax is invalid")
        name, value = line[2:].split("=", 1)
        if _SAFE_FLAG.fullmatch(name) is None or not value or name in result:
            raise RuntimePrepareError("generated flagfile entry is invalid")
        result[name] = value
    return result


def _parse_ini_file(
    path: Path,
    *,
    runtime_uid: int,
    runtime_gid: int,
) -> dict[str, dict[str, str]]:
    raw = _read_file(
        path,
        description="generated INI",
        uid=runtime_uid,
        gid=runtime_gid,
        mode=0o600,
        maximum_bytes=_MAX_GENERATED_FILE_BYTES,
    )
    try:
        try:
            text = bytes(raw).decode("utf-8")
        except UnicodeDecodeError:
            raise RuntimePrepareError("generated INI is not UTF-8") from None
    finally:
        _wipe(raw)
    parser = configparser.ConfigParser(
        interpolation=None,
        strict=True,
        empty_lines_in_values=False,
    )
    try:
        parser.read_string(text)
    except configparser.Error:
        raise RuntimePrepareError("generated INI syntax is invalid") from None
    if parser.defaults():
        raise RuntimePrepareError("generated INI defaults are not permitted")
    return {section: dict(parser.items(section, raw=True)) for section in parser.sections()}


def _directory(
    path: Path,
    *,
    description: str,
    uid: int,
    gid: int,
    mode: int,
) -> Path:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise RuntimePrepareError(f"{description} does not exist") from None
    except OSError:
        raise RuntimePrepareError(f"could not inspect {description}") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise RuntimePrepareError(f"{description} must not be a symlink")
    if not stat.S_ISDIR(metadata.st_mode):
        raise RuntimePrepareError(f"{description} must be a directory")
    if metadata.st_uid != uid or metadata.st_gid != gid:
        raise RuntimePrepareError(f"{description} has the wrong owner or group")
    if stat.S_IMODE(metadata.st_mode) != mode:
        raise RuntimePrepareError(f"{description} must have mode {mode:04o}")
    return path.resolve(strict=True)


def _entry_names(path: Path, *, description: str) -> set[str]:
    try:
        return {entry.name for entry in path.iterdir()}
    except OSError:
        raise RuntimePrepareError(f"could not inspect {description}") from None


def _readonly_metadata(path: Path, *, description: str, uid: int) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise RuntimePrepareError(f"{description} does not exist") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise RuntimePrepareError(f"{description} must be a regular non-symlink file")
    if metadata.st_uid != uid or stat.S_IMODE(metadata.st_mode) != 0o644:
        raise RuntimePrepareError(
            f"{description} must have the required owner and runtime-readable mode 0644"
        )
    if metadata.st_nlink != 1 or not 0 < metadata.st_size <= _MAX_METADATA_BYTES:
        raise RuntimePrepareError(f"{description} has an invalid link count or size")


def _readonly_directory(
    path: Path,
    *,
    description: str,
    uid: int,
    mode: int,
) -> Path:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise RuntimePrepareError(f"{description} does not exist") from None
    except OSError:
        raise RuntimePrepareError(f"could not inspect {description}") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise RuntimePrepareError(f"{description} must not be a symlink")
    if not stat.S_ISDIR(metadata.st_mode):
        raise RuntimePrepareError(f"{description} must be a directory")
    if metadata.st_uid != uid or stat.S_IMODE(metadata.st_mode) != mode:
        raise RuntimePrepareError(f"{description} must have the required owner and mode {mode:04o}")
    return path.resolve(strict=True)


def _read_file(
    path: Path,
    *,
    description: str,
    uid: int,
    gid: int,
    mode: int,
    maximum_bytes: int,
) -> bytearray:
    try:
        before = path.lstat()
    except FileNotFoundError:
        raise RuntimePrepareError(f"{description} does not exist") from None
    except OSError:
        raise RuntimePrepareError(f"could not inspect {description}") from None
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise RuntimePrepareError(f"{description} must be a regular non-symlink file")
    if before.st_uid != uid or before.st_gid != gid:
        raise RuntimePrepareError(f"{description} has the wrong owner or group")
    if stat.S_IMODE(before.st_mode) != mode:
        raise RuntimePrepareError(f"{description} must have mode {mode:04o}")
    if before.st_nlink != 1 or not 0 < before.st_size <= maximum_bytes:
        raise RuntimePrepareError(f"{description} has an invalid link count or size")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise RuntimePrepareError(f"could not safely open {description}") from None
    value = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise RuntimePrepareError(f"{description} changed during open")
        while True:
            chunk = os.read(descriptor, min(8192, maximum_bytes + 1 - len(value)))
            if not chunk:
                break
            value.extend(chunk)
            if len(value) > maximum_bytes:
                raise RuntimePrepareError(f"{description} exceeds its size bound")
        after = os.fstat(descriptor)
        if (
            after.st_size != opened.st_size
            or after.st_mtime_ns != opened.st_mtime_ns
            or after.st_ctime_ns != opened.st_ctime_ns
        ):
            raise RuntimePrepareError(f"{description} changed while reading")
        return value
    except BaseException:
        _wipe(value)
        raise
    finally:
        os.close(descriptor)


def _paths_overlap(left: Path, right: Path) -> bool:
    try:
        left.relative_to(right)
        return True
    except ValueError:
        pass
    try:
        right.relative_to(left)
        return True
    except ValueError:
        return False


def _default_paths() -> LauncherPaths:
    private = Path("/data/brilliant-vc-private")
    persistent = Path("/data/brilliant-vc")
    credentials = Path("/data/brilliant-vc-credentials")
    runtime = Path("/run/brilliant-vc")
    return LauncherPaths(
        private_root=private,
        persistent_root=persistent,
        identity_dir=private / "identity",
        materialized_certificate_dir=private / "materialized-certificates",
        runtime_credential_dir=credentials,
        bootstrap_path=credentials / "bootstrap",
        state_dir=persistent / "state",
        certificate_dir=credentials / "certificates",
        process_config_dir=persistent / "process-config",
        process_flagfile_dir=persistent / "flagfiles",
        startable_config_dir=persistent / "startable-configs",
        log_dir=persistent / "logs",
        error_log_dir=persistent / "errors",
        trace_dir=persistent / "traces",
        runtime_dir=runtime,
        socket_path=runtime / "server_socket",
        stats_socket_path=runtime / "uwsgi_stats_socket",
        release_info_path=Path("/etc/release_info.json"),
        tracking_branch_path=Path("/var/lib/update_manager/tracking_branch"),
        art_preload_dir=Path("/etc/brilliant/content_preload/art_library"),
    )


def _runtime_account() -> tuple[int, int]:
    if os.geteuid() == 0:
        raise RuntimePrepareError("runtime preparer must not run as root")
    try:
        account = pwd.getpwuid(os.geteuid())
    except KeyError:
        raise RuntimePrepareError("runtime account does not exist") from None
    if account.pw_name != _RUNTIME_USER or account.pw_gid == 0:
        raise RuntimePrepareError("runtime preparer must run as brilliant-vc")
    if account.pw_dir != "/nonexistent" or account.pw_shell != "/usr/sbin/nologin":
        raise RuntimePrepareError("runtime account must have no login shell or home")
    if Path(account.pw_dir).exists() or Path(account.pw_dir).is_symlink():
        raise RuntimePrepareError("runtime account home must remain nonexistent")
    if os.getegid() != account.pw_gid:
        raise RuntimePrepareError("runtime account effective primary group is incorrect")
    try:
        group = grp.getgrgid(account.pw_gid)
    except KeyError:
        raise RuntimePrepareError("dedicated runtime group does not exist") from None
    if group.gr_name != _RUNTIME_USER:
        raise RuntimePrepareError("runtime account must use its same-name group")
    all_accounts = pwd.getpwall()
    uid_accounts = [entry for entry in all_accounts if entry.pw_uid == account.pw_uid]
    if (
        len(uid_accounts) != 1
        or uid_accounts[0].pw_name != _RUNTIME_USER
        or uid_accounts[0].pw_gid != account.pw_gid
        or uid_accounts[0].pw_dir != account.pw_dir
        or uid_accounts[0].pw_shell != account.pw_shell
    ):
        raise RuntimePrepareError("runtime UID must map to exactly one account")
    primary_accounts = [entry for entry in all_accounts if entry.pw_gid == account.pw_gid]
    if (
        len(primary_accounts) != 1
        or primary_accounts[0].pw_name != _RUNTIME_USER
        or primary_accounts[0].pw_uid != account.pw_uid
        or primary_accounts[0].pw_dir != account.pw_dir
        or primary_accounts[0].pw_shell != account.pw_shell
        or set(group.gr_mem) - {_RUNTIME_USER}
    ):
        raise RuntimePrepareError("runtime group must not include another account")
    primary_group_entries = [entry for entry in grp.getgrall() if entry.gr_gid == account.pw_gid]
    if (
        len(primary_group_entries) != 1
        or primary_group_entries[0].gr_name != _RUNTIME_USER
        or set(primary_group_entries[0].gr_mem) - {_RUNTIME_USER}
    ):
        raise RuntimePrepareError("runtime GID must map to exactly one dedicated group")
    if set(os.getgroups()) - {account.pw_gid}:
        raise RuntimePrepareError("runtime account must not have supplementary groups")
    return account.pw_uid, account.pw_gid


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--approval-marker",
        type=Path,
        default=_APPROVAL_MARKER_PATH,
    )
    args = parser.parse_args(argv)
    runtime_uid, runtime_gid = _runtime_account()
    try:
        hashes = hash_firmware_modules(required_uid=0)
    except LauncherPreflightError as error:
        raise RuntimePrepareError(str(error)) from None
    result = prepare_runtime_no_start(
        _default_paths(),
        now_s=int(time.time()),
        apply=bool(args.apply),
        approval_marker=cast(Path, args.approval_marker) if args.apply else None,
        runtime_user=_RUNTIME_USER,
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
        credential_uid=0,
        actual_module_hashes=hashes,
    )
    print(json.dumps(result.to_public_dict(), sort_keys=True))
    if result.dry_run:
        print("DRY RUN — no firmware imported and no process started")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RuntimePrepareError as error:
        print(f"VC runtime preparation blocked: {error}", file=sys.stderr)
        sys.exit(2)

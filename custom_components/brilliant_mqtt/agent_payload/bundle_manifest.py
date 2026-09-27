#!/usr/bin/env python3
"""Emit deterministic, content-safe SHA-256 manifests for deployment parity gates."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import tarfile
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

_BUFFER_SIZE = 1024 * 1024
_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
_GENERATED_RELEASE_FILES = frozenset({"brilliant-mqtt.env", "mqtt-ca.pem"})
CORE_TREES = {
    "bridge": ("app", "vendor"),
    "wifi_watchdog": ("wifi_watchdog/brilliant_wifi_watchdog",),
    "bus_watchdog": ("bus_watchdog/brilliant_bus_watchdog",),
}


class ManifestError(RuntimeError):
    """A stable manifest validation failure which never includes file content."""


@dataclass(frozen=True, slots=True)
class _PanelSelector:
    active_root: Path
    current_path: Path
    current_stat: os.stat_result
    current_target: str
    releases_path: Path
    releases_stat: os.stat_result


def _safe_name(name: str) -> bool:
    return (
        bool(name)
        and name.isascii()
        and all(character.isprintable() and character not in "\t\r\n" for character in name)
    )


def _validate_name(name: str) -> None:
    if not _safe_name(name):
        raise ManifestError("comparison tree contains an unsafe filename")


def _same_object(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev,
        left.st_ino,
        stat.S_IFMT(left.st_mode),
    ) == (
        right.st_dev,
        right.st_ino,
        stat.S_IFMT(right.st_mode),
    )


def _same_stable_file(left: os.stat_result, right: os.stat_result) -> bool:
    return _same_object(left, right) and (
        left.st_size,
        left.st_mtime_ns,
        left.st_ctime_ns,
        left.st_nlink,
    ) == (
        right.st_size,
        right.st_mtime_ns,
        right.st_ctime_ns,
        right.st_nlink,
    )


def _same_stable_directory(left: os.stat_result, right: os.stat_result) -> bool:
    return _same_object(left, right) and (
        left.st_mtime_ns,
        left.st_ctime_ns,
        left.st_nlink,
    ) == (
        right.st_mtime_ns,
        right.st_ctime_ns,
        right.st_nlink,
    )


def _open_directory_path(path: Path, label: str) -> tuple[int, os.stat_result]:
    try:
        before = path.lstat()
        descriptor = os.open(path, _DIRECTORY_FLAGS)
    except OSError as error:
        raise ManifestError(f"{label} is unavailable ({type(error).__name__})") from None
    try:
        opened = os.fstat(descriptor)
    except OSError as error:
        os.close(descriptor)
        raise ManifestError(f"{label} is unreadable ({type(error).__name__})") from None
    if (
        stat.S_ISLNK(before.st_mode)
        or not stat.S_ISDIR(before.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or not _same_object(before, opened)
    ):
        os.close(descriptor)
        raise ManifestError(f"{label} must be one stable, non-symlink directory")
    return descriptor, opened


def _verify_directory_path(
    path: Path,
    descriptor_stat: os.stat_result,
    label: str,
) -> None:
    try:
        current = path.lstat()
    except OSError as error:
        raise ManifestError(f"{label} changed ({type(error).__name__})") from None
    if (
        stat.S_ISLNK(current.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or not _same_stable_directory(descriptor_stat, current)
    ):
        raise ManifestError(f"{label} changed while hashing")


def _hash_file_at(parent_fd: int, name: str, logical_path: str) -> str:
    try:
        descriptor = os.open(name, _FILE_FLAGS, dir_fd=parent_fd)
    except OSError as error:
        raise ManifestError(
            f"file could not be opened safely: {logical_path} ({type(error).__name__})"
        ) from None
    digest = hashlib.sha256()
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ManifestError(f"non-regular entry rejected: {logical_path}")
        while chunk := os.read(descriptor, _BUFFER_SIZE):
            digest.update(chunk)
        after = os.fstat(descriptor)
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except ManifestError:
        raise
    except OSError as error:
        raise ManifestError(
            f"file could not be hashed: {logical_path} ({type(error).__name__})"
        ) from None
    finally:
        os.close(descriptor)
    if (
        stat.S_ISLNK(current.st_mode)
        or not stat.S_ISREG(current.st_mode)
        or not _same_stable_file(before, after)
        or not _same_stable_file(after, current)
    ):
        raise ManifestError(f"file changed while hashing: {logical_path}")
    return digest.hexdigest()


def _open_child_directory(
    parent_fd: int,
    name: str,
    logical_path: str,
    observed: os.stat_result,
) -> tuple[int, os.stat_result]:
    descriptor: int | None = None
    try:
        descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        opened = os.fstat(descriptor)
    except OSError as error:
        if descriptor is not None:
            os.close(descriptor)
        raise ManifestError(
            f"directory could not be opened safely: {logical_path} ({type(error).__name__})"
        ) from None
    if not stat.S_ISDIR(opened.st_mode) or not _same_object(observed, opened):
        os.close(descriptor)
        raise ManifestError(f"directory changed while opening: {logical_path}")
    return descriptor, opened


def _verify_child_directory(
    parent_fd: int,
    name: str,
    logical_path: str,
    opened: os.stat_result,
) -> None:
    try:
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as error:
        raise ManifestError(
            f"directory changed while hashing: {logical_path} ({type(error).__name__})"
        ) from None
    if (
        stat.S_ISLNK(current.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or not _same_stable_directory(opened, current)
    ):
        raise ManifestError(f"directory changed while hashing: {logical_path}")


def _scan_directory_fd(
    descriptor: int,
    prefix: str,
    *,
    excluded_tree: bool,
    exclude_generated: bool,
    output: dict[str, str],
) -> None:
    try:
        names = sorted(entry.name for entry in os.scandir(descriptor))
    except OSError as error:
        raise ManifestError(
            f"comparison directory is unreadable ({type(error).__name__})"
        ) from None
    for name in names:
        _validate_name(name)
        logical_path = f"{prefix}/{name}" if prefix else name
        try:
            observed = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        except OSError as error:
            raise ManifestError(
                f"comparison entry is unreadable ({type(error).__name__})"
            ) from None
        if stat.S_ISLNK(observed.st_mode):
            raise ManifestError(f"symlink rejected: {logical_path}")
        if stat.S_ISDIR(observed.st_mode):
            child_fd, child_opened = _open_child_directory(
                descriptor,
                name,
                logical_path,
                observed,
            )
            try:
                _scan_directory_fd(
                    child_fd,
                    logical_path,
                    excluded_tree=excluded_tree or name == "__pycache__",
                    exclude_generated=exclude_generated,
                    output=output,
                )
                try:
                    child_after = os.fstat(child_fd)
                except OSError as error:
                    raise ManifestError(
                        f"directory became unreadable: {logical_path} ({type(error).__name__})"
                    ) from None
            finally:
                os.close(child_fd)
            if not _same_stable_directory(child_opened, child_after):
                raise ManifestError(f"directory changed while hashing: {logical_path}")
            _verify_child_directory(
                descriptor,
                name,
                logical_path,
                child_opened,
            )
            continue
        if not stat.S_ISREG(observed.st_mode):
            raise ManifestError(f"non-regular entry rejected: {logical_path}")
        excluded = (
            excluded_tree
            or Path(name).suffix in {".pyc", ".pyo"}
            or exclude_generated
            and not prefix
            and name in _GENERATED_RELEASE_FILES
        )
        if excluded:
            continue
        if logical_path in output:
            raise ManifestError(f"duplicate logical path: {logical_path}")
        output[logical_path] = _hash_file_at(descriptor, name, logical_path)


def _scan_tree(path: Path, label: str, *, exclude_generated: bool = False) -> dict[str, str]:
    descriptor, opened = _open_directory_path(path, label)
    output: dict[str, str] = {}
    try:
        _scan_directory_fd(
            descriptor,
            "",
            excluded_tree=False,
            exclude_generated=exclude_generated,
            output=output,
        )
        try:
            after = os.fstat(descriptor)
        except OSError as error:
            raise ManifestError(f"{label} became unreadable ({type(error).__name__})") from None
    finally:
        os.close(descriptor)
    if not _same_stable_directory(opened, after):
        raise ManifestError(f"{label} changed while hashing")
    _verify_directory_path(path, opened, label)
    return output


def _hash_required_path(path: Path, logical_path: str) -> str:
    _validate_name(path.name)
    parent_fd, parent_opened = _open_directory_path(
        path.parent,
        f"parent of {logical_path}",
    )
    try:
        digest = _hash_file_at(parent_fd, path.name, logical_path)
        try:
            parent_after = os.fstat(parent_fd)
        except OSError as error:
            raise ManifestError(
                f"parent of {logical_path} became unreadable ({type(error).__name__})"
            ) from None
    finally:
        os.close(parent_fd)
    if not _same_stable_directory(parent_opened, parent_after):
        raise ManifestError(f"parent of {logical_path} changed while hashing")
    _verify_directory_path(path.parent, parent_opened, f"parent of {logical_path}")
    return digest


def _add_alias(manifest: dict[str, str], logical_path: str, digest: str) -> None:
    if logical_path in manifest:
        raise ManifestError(f"duplicate logical path: {logical_path}")
    manifest[logical_path] = digest


def _payload_release_manifest(root: Path) -> dict[str, str]:
    release_ordinal(root)
    manifest = _scan_tree(root, "payload release root")
    required = {
        "VERSION",
        "brilliant-mqtt-release.service",
        "brilliant-wifi-watchdog-release.service",
        "brilliant-bus-watchdog-release.service",
    }
    missing = required - manifest.keys()
    if missing:
        raise ManifestError("payload release is missing required files")
    _add_alias(manifest, "installed/VERSION", manifest["VERSION"])
    _add_alias(
        manifest,
        "installed/brilliant-mqtt.service",
        manifest["brilliant-mqtt-release.service"],
    )
    _add_alias(
        manifest,
        "installed/brilliant-wifi-watchdog.service",
        manifest["brilliant-wifi-watchdog-release.service"],
    )
    _add_alias(
        manifest,
        "installed/brilliant-bus-watchdog.service",
        manifest["brilliant-bus-watchdog-release.service"],
    )
    return manifest


def release_ordinal(root: Path) -> int | None:
    """Read reviewed release metadata; absence is unknown, never ordinal zero."""
    path = root / "RELEASE_ORDINAL"
    if not path.exists():
        return None
    _hash_required_path(path, "RELEASE_ORDINAL")
    value = path.read_text(encoding="ascii").strip()
    if re.fullmatch(r"[1-9][0-9]{0,15}", value) is None:
        raise ManifestError("invalid release_ordinal")
    return int(value)


def code_digest(root: Path, component: str) -> str | None:
    """Hash canonical code paths, independent of selectors and generated config.

    Each separately selectable core component has its own installed record. The
    release ordinal is authority metadata, not code: adding it cannot turn equal
    incumbent bytes into a different release or invent an incumbent ordering.
    """
    trees = CORE_TREES[component]
    if not any(os.path.lexists(root / tree) for tree in trees):
        return None
    manifest: dict[str, str] = {}
    for tree in trees:
        manifest.update(
            (f"{tree}/{path}", digest)
            for path, digest in _scan_tree(root / tree, "core code tree").items()
        )
    wire = "".join(f"{path}\t{manifest[path]}\n" for path in sorted(manifest))
    return hashlib.sha256(wire.encode()).hexdigest()


def private_record(
    root: Path, name: str, value: dict[str, object] | None = None, *, exclusive: bool = False
) -> dict[str, object] | None:
    """Read or durably publish a bounded root-private identity/audit record."""
    if re.fullmatch(r"[a-z0-9_-]+", name) is None:
        raise ManifestError("invalid identity record name")
    directory = root / ".release-identities"
    if not directory.exists() and value is None:
        return None
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory_fd, _ = _open_directory_path(directory, "identity store")
    try:
        if stat.S_IMODE(os.fstat(directory_fd).st_mode) != 0o700:
            raise ManifestError("identity store must be private")
        filename = name + ".json"
        if value is None:
            try:
                descriptor = os.open(filename, _FILE_FLAGS, dir_fd=directory_fd)
            except FileNotFoundError:
                return None
            with os.fdopen(descriptor, "rb") as source:
                metadata = os.fstat(source.fileno())
                if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
                    raise ManifestError("identity record must be private")
                data = source.read(16385)
            if len(data) > 16384:
                raise ManifestError("identity record too large")
            parsed: object = json.loads(data)
            if not isinstance(parsed, dict):
                raise ManifestError("invalid identity record")
            return dict(parsed)
        data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        if len(data) > 16384:
            raise ManifestError("identity record too large")
        descriptor, temporary = tempfile.mkstemp(prefix=".identity-", dir=directory)
        try:
            with os.fdopen(descriptor, "wb") as target:
                target.write(data)
                target.flush()
                os.fsync(target.fileno())
            if exclusive:
                os.link(temporary, directory / filename, follow_symlinks=False)
            else:
                os.replace(temporary, directory / filename)
            os.fsync(directory_fd)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return value
    finally:
        os.close(directory_fd)


def installed_identities(
    root: Path,
    unit_root: Path = Path("/etc/systemd/system"),
    env_path: Path = Path("/etc/brilliant-mqtt.env"),
) -> dict[str, dict[str, object] | None]:
    """Hash the live selection and enroll legacy installs without claiming age."""
    selector = _panel_release_selector(root) if os.path.lexists(root / "current") else None
    output: dict[str, dict[str, object] | None] = {}
    deployment_id = None
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("BRILLIANT_DEPLOYMENT_ID="):
                deployment_id = line.partition("=")[2].strip('"')
                if re.fullmatch(r"[0-9A-Za-z_-]{1,128}", deployment_id) is None:
                    raise ManifestError("invalid installed deployment identity")
    for component in CORE_TREES:
        service = (
            "brilliant-mqtt"
            if component == "bridge"
            else "brilliant-" + component.replace("_", "-")
        )
        unit = unit_root / (service + ".service")
        if not unit.exists():
            staged_directory = root / ("system" if component == "bridge" else component)
            unit = staged_directory / (service + ".service")
        selected_release = selector is not None
        if selector is not None and unit.exists():
            unit_bytes = unit.read_bytes()
            if b"/var/brilliant-mqtt/current/" not in unit_bytes:
                # A surviving current link must not hide an already selected legacy unit.
                selected_release = False
        active_root = selector.active_root if selector is not None and selected_release else root
        digest = code_digest(active_root, component)
        if digest is None:
            if private_record(root, component) is not None:
                raise ManifestError("recorded installed code is missing")
            output[component] = None
            continue
        layout = "release_link" if selected_release else "legacy_fixed"
        version_path = (
            active_root / "VERSION"
            if selected_release or component == "bridge"
            else active_root / component / "VERSION"
        )
        version = version_path.read_text().strip() if version_path.exists() else "unknown"
        if re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z._+-]{0,127}", version) is None:
            raise ManifestError("invalid installed version")
        identity: dict[str, object] = {
            "version": version,
            "release_ordinal": None,
            "digest": digest,
            "deployment_id": deployment_id,
            "layout": layout,
        }
        previous = private_record(root, component)
        if previous is not None and all(
            previous.get(key) == identity[key] for key in ("digest", "layout")
        ):
            identity["release_ordinal"] = previous.get("release_ordinal")
        if previous != identity:
            private_record(root, component, identity)
        output[component] = identity
    if selector is not None:
        _verify_panel_selector(selector)
    return output


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, _DIRECTORY_FLAGS)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _baseline_roots(root: Path) -> list[Path]:
    return [
        *(
            root / name
            for name in (
                "app",
                "vendor",
                "wifi_watchdog",
                "bus_watchdog",
                "VERSION",
                "RELEASE_ORDINAL",
                "tls",
                "system",
            )
        ),
        *(root / ".release-identities" / (name + ".json") for name in CORE_TREES),
        Path("/etc/brilliant-mqtt.env"),
        *(
            Path("/etc/systemd/system") / (name + ".service")
            for name in (
                "brilliant-mqtt",
                "brilliant-wifi-watchdog",
                "brilliant-bus-watchdog",
            )
        ),
    ]


def _baseline_ca_fallback(root: Path, release: Path | None) -> list[Path]:
    environment = Path("/etc/brilliant-mqtt.env")
    if not environment.exists():
        return []
    assignments = [
        line.partition("=")[2].strip().strip('"')
        for line in environment.read_text().splitlines()
        if line.partition("=")[0].strip() == "MQTT_TLS_CA_FILE"
    ]
    if not assignments:
        return []
    if len(assignments) != 1:
        raise ManifestError("baseline_ca_invalid")
    path = Path(assignments[0])
    if not path.is_file() or path.is_symlink():
        raise ManifestError("baseline_ca_missing")
    if path.parent == root / "tls" or release is not None and path == release / "mqtt-ca.pem":
        return []
    if (
        re.fullmatch(
            re.escape(str(root)) + r"/releases/[0-9A-Za-z._+-]+--[0-9a-f]{32}/mqtt-ca.pem",
            str(path),
        )
        is None
    ):
        raise ManifestError("baseline_ca_invalid")
    return [path]


def _baseline_inventory(
    roots: list[Path],
    maximum_bytes: int,
    maximum_entries: int,
    deadline: float,
) -> dict[str, dict[str, object]]:
    """Bound traversal before reading bytes; reject links/devices and racing files."""
    pending = list(roots)
    entries: dict[str, dict[str, object]] = {}
    size = 0
    while pending:
        path = pending.pop()
        if time.monotonic() >= deadline or len(entries) >= maximum_entries:
            raise ManifestError("baseline_capture_bound")
        if not os.path.lexists(path):
            entries[str(path)] = {"kind": "absent"}
            continue
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if stat.S_ISDIR(metadata.st_mode):
            entries[str(path)] = {"kind": "directory", "mode": mode}
            with os.scandir(path) as children:
                for child in children:
                    if child.name == ".rollback-retained":
                        continue
                    pending.append(Path(child.path))
                    if len(entries) + len(pending) > maximum_entries:
                        raise ManifestError("baseline_capture_bound")
        elif stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1:
            size += metadata.st_size
            if size > maximum_bytes:
                raise ManifestError("baseline_capture_bound")
            parent = os.open(path.parent, _DIRECTORY_FLAGS)
            try:
                digest = _hash_file_at(parent, path.name, "baseline file")
            finally:
                os.close(parent)
            entries[str(path)] = {
                "kind": "file",
                "mode": mode,
                "size": metadata.st_size,
                "digest": digest,
            }
        else:
            raise ManifestError("baseline_unsafe_tree")
    return entries


def capture_baseline(
    root: Path,
    identifier: str,
    *,
    maximum_bytes: int = 32 * 1024 * 1024,
    maximum_entries: int = 4096,
    capture_seconds: int = 120,
    free_reserve: int = 32 * 1024 * 1024,
) -> dict[str, object]:
    """Publish one bounded complete archive and/or immutable release reference."""
    if re.fullmatch(r"[0-9a-f]{32}", identifier) is None:
        raise ManifestError("baseline_invalid")
    deadline = time.monotonic() + capture_seconds
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    identities = installed_identities(root)
    for component, identity in identities.items():
        service = (
            "brilliant-mqtt"
            if component == "bridge"
            else "brilliant-" + component.replace("_", "-")
        )
        unit = service + ".service"
        staged = root / ("system" if component == "bridge" else component) / unit
        if identity is None and (staged.exists() or (Path("/etc/systemd/system") / unit).exists()):
            raise ManifestError("baseline_code_missing")
    bridge = identities["bridge"]
    release: Path | None = None
    if any(
        identity is not None and identity["layout"] == "release_link"
        for identity in identities.values()
    ):
        release = _panel_release_selector(root).active_root
    source = release if bridge is not None and bridge["layout"] == "release_link" else root
    if source is None:
        raise ManifestError("baseline_code_missing")
    if bridge is not None:
        config = source / "app/brilliant_mqtt/config.py"
        main = source / "app/brilliant_mqtt/__main__.py"
        if (
            not config.is_file()
            or not main.is_file()
            or b"BRILLIANT_DEPLOYMENT_ID" not in config.read_bytes()
            or b'"deployment_id"' not in main.read_bytes()
            or b"settings.deployment_id" not in main.read_bytes()
        ):
            raise ManifestError("baseline_correlation_unsupported")
    elif any(path.exists() for path in _baseline_roots(root)):
        raise ManifestError("baseline_code_missing")
    roots = _baseline_roots(root) + _baseline_ca_fallback(root, release)
    # Fixed trees may still be independently selected companions. Preserve those
    # once; the immutable release itself is verified and pinned, never copied.
    files = _baseline_inventory(roots, maximum_bytes, maximum_entries, deadline)
    pinned = (
        _baseline_inventory([release], maximum_bytes, maximum_entries, deadline)
        if release is not None
        else {}
    )
    combined = list(files.values()) + list(pinned.values())
    expanded = sum(size for item in combined if isinstance(size := item.get("size"), int))
    if expanded > maximum_bytes or len(combined) > maximum_entries:
        raise ManifestError("baseline_capture_bound")
    archive_budget = expanded + len(files) * 2048 + 10240
    if shutil.disk_usage(root).free < archive_budget + free_reserve:
        raise ManifestError("baseline_space_insufficient")
    parent = root / ".rollback"
    parent.mkdir(mode=0o700, exist_ok=True)
    if parent.is_symlink():
        raise ManifestError("baseline_unsafe_tree")
    parent.chmod(0o700)
    _sync_directory(root)
    temporary = parent / ("." + identifier + ".tmp")
    destination = parent / identifier
    temporary.mkdir(mode=0o700)
    try:
        archive = temporary / "baseline.tar"
        with tarfile.open(archive, "w") as output:
            for name, metadata in sorted(files.items()):
                if time.monotonic() >= deadline:
                    raise ManifestError("baseline_capture_bound")
                if metadata["kind"] != "absent":
                    output.add(name, arcname=name.lstrip("/"), recursive=False)
        archive.chmod(0o600)
        if files != _baseline_inventory(roots, maximum_bytes, maximum_entries, deadline):
            raise ManifestError("baseline_changed")
        if release is not None:
            if pinned != _baseline_inventory([release], maximum_bytes, maximum_entries, deadline):
                raise ManifestError("baseline_changed")
            marker = release / ".rollback-retained"
            if marker.exists():
                stranded = marker.read_text()
                if (
                    marker.is_symlink()
                    or re.fullmatch(r"[0-9a-f]{32}", stranded) is None
                    or os.path.lexists(parent / stranded)
                    or os.path.lexists(parent / ("." + stranded + ".finalizing"))
                ):
                    raise ManifestError("baseline_already_retained")
                # A pin without published completion never authorized mutation.
                # Reconcile only that exact abandoned capture under the panel lock.
                partial = parent / ("." + stranded + ".tmp")
                if partial.exists():
                    shutil.rmtree(partial)
                marker.unlink()
                _sync_directory(parent)
                _sync_directory(release)
            pending_marker = temporary / "release-pin"
            with pending_marker.open("x") as stream:
                os.fchmod(stream.fileno(), 0o600)
                stream.write(identifier)
                stream.flush()
                os.fsync(stream.fileno())
            # Publish a complete identifier without replacing an existing pin.
            os.link(pending_marker, marker, follow_symlinks=False)
            pending_marker.unlink()
            _sync_directory(release)
        with archive.open("rb") as stream:
            archive_digest = hashlib.sha256(stream.read()).hexdigest()
            os.fsync(stream.fileno())
        manifest = {
            "identifier": identifier,
            "files": files,
            "roots": [str(path) for path in roots],
            "release_target": str(release) if release else None,
            "pinned": pinned,
            "archive_digest": archive_digest,
            "identities": identities,
            "limits": {
                "maximum_bytes": maximum_bytes,
                "maximum_entries": maximum_entries,
                "capture_seconds": capture_seconds,
            },
        }
        encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        complete = temporary / "complete.json"
        with complete.open("wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        _sync_directory(temporary)
        os.rename(temporary, destination)
        _sync_directory(parent)
        return {
            "identifier": identifier,
            "digest": hashlib.sha256(encoded).hexdigest(),
            "identities": identities,
        }
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def baseline_action(root: Path, identifier: str, digest: str, action: str) -> None:
    """Verify before restoring; replay safely after interruption; finalize explicitly."""
    if re.fullmatch(r"[0-9a-f]{32}", identifier) is None:
        raise ManifestError("baseline_invalid")
    directory = root / ".rollback" / identifier
    retired = directory.with_name("." + identifier + ".finalizing")
    if action == "finalize" and not directory.exists():
        if retired.exists():
            shutil.rmtree(retired)
            _sync_directory(retired.parent)
        return
    encoded = (directory / "complete.json").read_bytes()
    if hashlib.sha256(encoded).hexdigest() != digest:
        raise ManifestError("baseline_digest_mismatch")
    manifest = json.loads(encoded)
    limits = manifest["limits"]
    maximum_bytes = limits["maximum_bytes"]
    maximum_entries = limits["maximum_entries"]
    deadline = time.monotonic() + limits["capture_seconds"]
    release = Path(manifest["release_target"]) if manifest["release_target"] else None
    if action == "finalize":
        if release is not None:
            (release / ".rollback-retained").unlink(missing_ok=True)
            _sync_directory(release)
        # Publish deletion intent on-panel too, so a lost complete.json during
        # recursive removal cannot make a durable FINALIZING record unreplayable.
        os.rename(directory, retired)
        _sync_directory(directory.parent)
        directory = retired
        shutil.rmtree(directory)
        _sync_directory(directory.parent)
        return
    if release is not None:
        observed = _baseline_inventory([release], maximum_bytes, maximum_entries, deadline)
        if observed != manifest["pinned"] or not (release / ".rollback-retained").is_file():
            raise ManifestError("baseline_pinned_release_changed")
    archive = directory / "baseline.tar"
    if hashlib.sha256(archive.read_bytes()).hexdigest() != manifest["archive_digest"]:
        raise ManifestError("baseline_archive_changed")
    if action == "verify":
        return
    roots = [Path(name) for name in manifest["roots"]]
    expected_roots = _baseline_roots(root)
    extras = roots[len(expected_roots) :]
    if (
        roots[: len(expected_roots)] != expected_roots
        or len(extras) > 1
        or any(
            re.fullmatch(
                re.escape(str(root)) + r"/releases/[0-9A-Za-z._+-]+--[0-9a-f]{32}/mqtt-ca.pem",
                str(path),
            )
            is None
            for path in extras
        )
    ):
        raise ManifestError("baseline_invalid")
    if action == "check_restored":
        if (
            _baseline_inventory(roots, maximum_bytes, maximum_entries, deadline)
            != manifest["files"]
        ):
            raise ManifestError("baseline_restore_mismatch")
        return
    if action != "restore":
        raise ManifestError("baseline_invalid")
    extracted = directory / "restore.tmp"
    if extracted.exists():
        shutil.rmtree(extracted)
    extracted.mkdir(mode=0o700)
    with tarfile.open(archive) as source:
        for member in source.getmembers():
            if (
                "/" + member.name not in manifest["files"]
                or not (member.isfile() or member.isdir())
                or ".." in Path(member.name).parts
                or member.name.startswith("/")
            ):
                raise ManifestError("baseline_archive_unsafe")
        source.extractall(extracted)
    for path in roots:
        metadata = manifest["files"][str(path)]
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            raise ManifestError("baseline_restore_unsafe")
        temporary = path.with_name(path.name + ".restore-" + identifier)
        if temporary.exists():
            if temporary.is_dir():
                shutil.rmtree(temporary)
            else:
                temporary.unlink()
        if metadata["kind"] != "absent":
            original = extracted / str(path).lstrip("/")
            if original.is_dir():
                shutil.copytree(original, temporary)
            else:
                shutil.copy2(original, temporary)
            for entry in [temporary] if temporary.is_file() else temporary.rglob("*"):
                if entry.is_file():
                    with entry.open("rb") as stream:
                        os.fsync(stream.fileno())
        if path.is_dir():
            shutil.rmtree(path)
        if metadata["kind"] == "absent":
            path.unlink(missing_ok=True)
        else:
            os.replace(temporary, path)
        _sync_directory(path.parent)
    shutil.rmtree(extracted)
    baseline_action(root, identifier, digest, "check_restored")


def _verify_panel_selector(selector: _PanelSelector) -> None:
    try:
        current = selector.current_path.lstat()
        target = os.readlink(selector.current_path)
    except OSError as error:
        raise ManifestError(f"panel current selector changed ({type(error).__name__})") from None
    if (
        not stat.S_ISLNK(current.st_mode)
        or not _same_stable_file(selector.current_stat, current)
        or target != selector.current_target
    ):
        raise ManifestError("panel current selector changed while hashing")
    _verify_directory_path(
        selector.releases_path,
        selector.releases_stat,
        "panel releases directory",
    )


def _panel_release_selector(root: Path) -> _PanelSelector:
    root = Path(os.path.abspath(root))
    current = root / "current"
    releases = root / "releases"
    releases_fd, releases_opened = _open_directory_path(
        releases,
        "panel releases directory",
    )
    os.close(releases_fd)
    _verify_directory_path(releases, releases_opened, "panel releases directory")
    try:
        current_stat = current.lstat()
        current_target = os.readlink(current)
    except OSError as error:
        raise ManifestError(
            f"panel current selector is unavailable ({type(error).__name__})"
        ) from None
    if not stat.S_ISLNK(current_stat.st_mode):
        raise ManifestError("panel current selector must be a symlink")
    target_path = Path(current_target)
    if not target_path.is_absolute():
        target_path = current.parent / target_path
    active_root = Path(os.path.abspath(target_path))
    if active_root.parent != releases or active_root == releases:
        raise ManifestError("panel current selector must resolve to one direct release directory")
    _validate_name(active_root.name)
    selector = _PanelSelector(
        active_root=active_root,
        current_path=current,
        current_stat=current_stat,
        current_target=current_target,
        releases_path=releases,
        releases_stat=releases_opened,
    )
    try:
        _verify_panel_selector(selector)
    except ManifestError:
        raise ManifestError("panel current selector is unavailable") from None
    return selector


def _panel_release_manifest(
    root: Path,
    unit: Path,
    wifi_unit: Path,
    bus_unit: Path,
) -> dict[str, str]:
    selector = _panel_release_selector(root)
    manifest = _scan_tree(
        selector.active_root,
        "active panel release",
        exclude_generated=True,
    )
    _add_alias(
        manifest,
        "installed/VERSION",
        _hash_required_path(root / "VERSION", "installed/VERSION"),
    )
    _add_alias(
        manifest,
        "installed/brilliant-mqtt.service",
        _hash_required_path(unit, "installed/brilliant-mqtt.service"),
    )
    _add_alias(
        manifest,
        "installed/brilliant-wifi-watchdog.service",
        _hash_required_path(
            wifi_unit,
            "installed/brilliant-wifi-watchdog.service",
        ),
    )
    _add_alias(
        manifest,
        "installed/brilliant-bus-watchdog.service",
        _hash_required_path(
            bus_unit,
            "installed/brilliant-bus-watchdog.service",
        ),
    )
    _verify_panel_selector(selector)
    return manifest


def build_manifest(
    layout: str,
    root: Path,
    unit: Path | None = None,
    wifi_unit: Path | None = None,
    bus_unit: Path | None = None,
) -> list[str]:
    if layout == "integration":
        if any(value is not None for value in (unit, wifi_unit, bus_unit)):
            raise ManifestError("unit arguments are valid only for panel-release")
        manifest = _scan_tree(root, "integration root")
    elif layout == "payload-release":
        if any(value is not None for value in (unit, wifi_unit, bus_unit)):
            raise ManifestError("unit arguments are valid only for panel-release")
        manifest = _payload_release_manifest(root)
    elif layout == "panel-release":
        if unit is None or wifi_unit is None or bus_unit is None:
            raise ManifestError(
                "--unit, --wifi-unit, and --bus-unit are required for panel-release"
            )
        manifest = _panel_release_manifest(root, unit, wifi_unit, bus_unit)
    else:
        raise ManifestError("unsupported layout")
    return [f"{path}\t{manifest[path]}" for path in sorted(manifest)]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=("Emit <logical-path><TAB><sha256> without file contents or host metadata.")
    )
    parser.add_argument(
        "layout",
        choices=("integration", "payload-release", "panel-release"),
    )
    parser.add_argument("root", type=Path)
    parser.add_argument("--unit", type=Path)
    parser.add_argument("--wifi-unit", type=Path)
    parser.add_argument("--bus-unit", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        lines = build_manifest(
            arguments.layout,
            arguments.root,
            arguments.unit,
            arguments.wifi_unit,
            arguments.bus_unit,
        )
    except ManifestError as error:
        print(f"bundle-manifest: {error}", file=sys.stderr)
        return 2
    for line in lines:
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

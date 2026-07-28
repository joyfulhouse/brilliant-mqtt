#!/usr/bin/env python3
"""Emit deterministic, content-safe SHA-256 manifests for deployment parity gates."""

from __future__ import annotations

import argparse
import hashlib
import os
import stat
import sys
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

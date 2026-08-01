"""Deterministic, secret-safe bundle manifest helper."""

from __future__ import annotations

import hashlib
import importlib.util
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import cast

import pytest

_REPOSITORY_ROOT = Path(__file__).parents[1]
_SCRIPT = _REPOSITORY_ROOT / "scripts/brilliant-panel/bundle_manifest.py"
_PAYLOAD = _REPOSITORY_ROOT / "custom_components/brilliant_mqtt/agent_payload"
_RELEASE_NAME = "0.6.0--0123456789abcdef0123456789abcdef"


def _run(
    *arguments: object,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_SCRIPT), *(str(argument) for argument in arguments)],
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )


def _load_helper() -> ModuleType:
    spec = importlib.util.spec_from_file_location("bundle_manifest_test", _SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _line(path: str, content: str) -> str:
    return f"{path}\t{hashlib.sha256(content.encode()).hexdigest()}"


def _panel_arguments(panel_root: Path) -> tuple[object, ...]:
    return (
        "panel-release",
        panel_root,
        "--unit",
        panel_root.parent / "brilliant-mqtt.service",
        "--wifi-unit",
        panel_root.parent / "brilliant-wifi-watchdog.service",
        "--bus-unit",
        panel_root.parent / "brilliant-bus-watchdog.service",
    )


def _make_real_panel_release(tmp_path: Path) -> Path:
    panel_root = tmp_path / "panel"
    release = panel_root / "releases" / _RELEASE_NAME
    shutil.copytree(_PAYLOAD, release)
    _write(release / "brilliant-mqtt.env", "MQTT_PASSWORD=not-manifested\n")
    _write(release / "mqtt-ca.pem", "not-manifested\n")
    (panel_root / "current").symlink_to(release)
    shutil.copyfile(_PAYLOAD / "VERSION", panel_root / "VERSION")
    for service in (
        "brilliant-mqtt",
        "brilliant-wifi-watchdog",
        "brilliant-bus-watchdog",
    ):
        shutil.copyfile(
            _PAYLOAD / f"{service}-release.service",
            panel_root.parent / f"{service}.service",
        )
    return panel_root


def test_integration_manifest_is_sorted_and_excludes_only_runtime_caches(
    tmp_path: Path,
) -> None:
    _write(tmp_path / "z.py", "z")
    _write(tmp_path / "nested/a.py", "a")
    _write(tmp_path / "nested/__pycache__/a.pyc", "cache")
    _write(tmp_path / "nested/ignored.pyo", "cache")

    result = _run("integration", tmp_path)

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.splitlines() == [
        _line("nested/a.py", "a"),
        _line("z.py", "z"),
    ]


def test_manifest_rejects_symlinks_without_reading_their_targets(
    tmp_path: Path,
) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside-secret"
    outside.write_text("SECRET-never-read", encoding="utf-8")
    os.symlink(outside, tmp_path / "linked-secret")

    result = _run("integration", tmp_path)

    assert result.returncode == 2
    assert "symlink rejected: linked-secret" in result.stderr
    assert "SECRET-never-read" not in result.stderr


@pytest.mark.parametrize("unsafe", ("tab\tname", "line\nname", "escape\x1bname"))
def test_manifest_rejects_ambiguous_control_character_names(
    tmp_path: Path,
    unsafe: str,
) -> None:
    _write(tmp_path / unsafe, "SECRET-never-rendered")

    result = _run("integration", tmp_path)

    assert result.returncode == 2
    assert result.stderr == ("bundle-manifest: comparison tree contains an unsafe filename\n")
    assert "SECRET-never-rendered" not in result.stderr
    assert unsafe not in result.stderr


def test_payload_and_real_panel_release_manifests_match_every_static_file(
    tmp_path: Path,
) -> None:
    panel_root = _make_real_panel_release(tmp_path)

    payload = _run("payload-release", _PAYLOAD)
    panel = _run(*_panel_arguments(panel_root))

    assert payload.returncode == 0, payload.stderr
    assert panel.returncode == 0, panel.stderr
    assert panel.stdout == payload.stdout
    paths = {line.split("\t", 1)[0] for line in payload.stdout.splitlines()}
    assert len(paths) > 75
    assert {
        "app/brilliant_mqtt/bridge.py",
        "bus_watchdog/brilliant_bus_watchdog/run.py",
        "ha_mirror/brilliant_ha_mirror/mirror.py",
        "hue_ca/brilliant_hue_ca/coordinator.py",
        "wifi_watchdog/brilliant_wifi_watchdog/run.py",
        "brilliant-mqtt-release.service",
        "brilliant-wifi-watchdog-release.service",
        "brilliant-bus-watchdog-release.service",
        "installed/VERSION",
        "installed/brilliant-mqtt.service",
        "installed/brilliant-wifi-watchdog.service",
        "installed/brilliant-bus-watchdog.service",
    } <= paths
    assert "brilliant-mqtt.env" not in paths
    assert "mqtt-ca.pem" not in paths


def test_panel_manifest_detects_an_extra_static_release_file(tmp_path: Path) -> None:
    panel_root = _make_real_panel_release(tmp_path)
    _write(panel_root / "releases" / _RELEASE_NAME / "unexpected.py", "extra")

    payload = _run("payload-release", _PAYLOAD)
    panel = _run(*_panel_arguments(panel_root))

    assert payload.returncode == 0
    assert panel.returncode == 0
    assert panel.stdout != payload.stdout
    assert "unexpected.py\t" in panel.stdout


def test_panel_manifest_rejects_current_outside_release_root(
    tmp_path: Path,
) -> None:
    panel_root = tmp_path / "panel"
    (panel_root / "releases").mkdir(parents=True)
    outside = tmp_path / "outside"
    _write(outside / "app/file.py", "app")
    os.symlink(outside, panel_root / "current")

    result = _run(*_panel_arguments(panel_root))

    assert result.returncode == 2
    assert "panel current selector must resolve to one direct release directory" in result.stderr


def test_panel_manifest_rejects_current_equal_to_releases(tmp_path: Path) -> None:
    panel_root = tmp_path / "panel"
    releases = panel_root / "releases"
    releases.mkdir(parents=True)
    os.symlink(releases, panel_root / "current")

    result = _run(*_panel_arguments(panel_root))

    assert result.returncode == 2
    assert "panel current selector must resolve to one direct release directory" in result.stderr


def test_required_payload_file_cannot_be_a_symlink(tmp_path: Path) -> None:
    target = tmp_path / "real-version"
    target.write_text("SECRET-version-target", encoding="utf-8")
    payload = tmp_path / "payload"
    payload.mkdir()
    os.symlink(target, payload / "VERSION")
    for name in (
        "brilliant-mqtt-release.service",
        "brilliant-wifi-watchdog-release.service",
        "brilliant-bus-watchdog-release.service",
    ):
        _write(payload / name, name)

    result = _run("payload-release", payload)

    assert result.returncode == 2
    assert "symlink rejected: VERSION" in result.stderr
    assert "SECRET-version-target" not in result.stderr


def test_payload_manifest_rejects_a_real_installed_alias_collision(
    tmp_path: Path,
) -> None:
    _write(tmp_path / "VERSION", "0.6.0")
    for name in (
        "brilliant-mqtt-release.service",
        "brilliant-wifi-watchdog-release.service",
        "brilliant-bus-watchdog-release.service",
    ):
        _write(tmp_path / name, name)
    _write(tmp_path / "installed/VERSION", "attacker-controlled")

    result = _run("payload-release", tmp_path)

    assert result.returncode == 2
    assert "duplicate logical path: installed/VERSION" in result.stderr


def test_panel_manifest_revalidates_current_after_scanning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel_root = tmp_path / "panel"
    first = panel_root / "releases" / "first"
    second = panel_root / "releases" / "second"
    _write(first / "file.py", "first")
    _write(second / "file.py", "second")
    current = panel_root / "current"
    current.symlink_to(first)
    _write(panel_root / "VERSION", "0.6.0")
    for service in (
        "brilliant-mqtt",
        "brilliant-wifi-watchdog",
        "brilliant-bus-watchdog",
    ):
        _write(panel_root.parent / f"{service}.service", service)
    helper = _load_helper()
    build_manifest = cast(
        Callable[..., list[str]],
        helper.__dict__["build_manifest"],
    )
    manifest_error = cast(type[Exception], helper.__dict__["ManifestError"])
    real_scan_tree = cast(
        Callable[..., dict[str, str]],
        helper.__dict__["_scan_tree"],
    )
    switched = False

    def switching_scan_tree(
        path: Path,
        label: str,
        *,
        exclude_generated: bool = False,
    ) -> dict[str, str]:
        nonlocal switched
        if label == "active panel release" and not switched:
            current.unlink()
            current.symlink_to(second)
            switched = True
        return real_scan_tree(
            path,
            label,
            exclude_generated=exclude_generated,
        )

    monkeypatch.setattr(helper, "_scan_tree", switching_scan_tree)

    with pytest.raises(manifest_error, match="panel current selector changed"):
        build_manifest(
            "panel-release",
            panel_root,
            panel_root.parent / "brilliant-mqtt.service",
            panel_root.parent / "brilliant-wifi-watchdog.service",
            panel_root.parent / "brilliant-bus-watchdog.service",
        )

    assert switched


def test_panel_manifest_reports_a_symlink_loop_without_a_traceback(
    tmp_path: Path,
) -> None:
    panel_root = tmp_path / "panel"
    (panel_root / "releases").mkdir(parents=True)
    current = panel_root / "current"
    current.symlink_to(current)

    result = _run(*_panel_arguments(panel_root))

    assert result.returncode == 2
    assert result.stderr.startswith("bundle-manifest:")
    assert "Traceback" not in result.stderr


def test_atomic_path_replacement_during_hashing_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    victim = tmp_path / "victim.py"
    replacement = tmp_path.parent / f"{tmp_path.name}-replacement.py"
    victim.write_bytes(b"a" * (2 * 1024 * 1024))
    replacement.write_bytes(b"replacement")
    helper = _load_helper()
    helper_os = cast(ModuleType, helper.__dict__["os"])
    build_manifest = cast(
        Callable[[str, Path], list[str]],
        helper.__dict__["build_manifest"],
    )
    manifest_error = cast(type[Exception], helper.__dict__["ManifestError"])
    real_read = os.read
    replaced = False

    def replacing_read(descriptor: int, size: int) -> bytes:
        nonlocal replaced
        data = real_read(descriptor, size)
        if data and not replaced:
            replaced = True
            os.replace(replacement, victim)
        return data

    monkeypatch.setattr(helper_os, "read", replacing_read)

    with pytest.raises(manifest_error, match="file changed while hashing"):
        build_manifest("integration", tmp_path)

    assert replaced


def test_same_inode_mutation_after_final_fstat_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    victim = tmp_path / "victim.py"
    victim.write_bytes(b"old content")
    helper = _load_helper()
    helper_os = cast(ModuleType, helper.__dict__["os"])
    build_manifest = cast(
        Callable[[str, Path], list[str]],
        helper.__dict__["build_manifest"],
    )
    manifest_error = cast(type[Exception], helper.__dict__["ManifestError"])
    real_stat = os.stat
    victim_stat_calls = 0
    mutated = False

    def mutating_stat(
        path: int | str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        nonlocal mutated, victim_stat_calls
        if path == "victim.py" and dir_fd is not None:
            victim_stat_calls += 1
            if victim_stat_calls == 2:
                victim.write_bytes(b"new content on the same inode")
                mutated = True
        return real_stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(helper_os, "stat", mutating_stat)

    with pytest.raises(manifest_error, match="file changed while hashing"):
        build_manifest("integration", tmp_path)

    assert mutated


def test_directory_replaced_by_symlink_during_hashing_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    child = root / "child"
    outside = tmp_path / "outside"
    moved = tmp_path / "opened-child"
    _write(child / "safe.py", "safe")
    _write(outside / "secret.py", "SECRET-never-read")
    helper = _load_helper()
    build_manifest = cast(
        Callable[[str, Path], list[str]],
        helper.__dict__["build_manifest"],
    )
    manifest_error = cast(type[Exception], helper.__dict__["ManifestError"])
    real_open_child = cast(
        Callable[[int, str, str, os.stat_result], tuple[int, os.stat_result]],
        helper.__dict__["_open_child_directory"],
    )
    raced = False

    def racing_open_child(
        parent_fd: int,
        name: str,
        logical_path: str,
        observed: os.stat_result,
    ) -> tuple[int, os.stat_result]:
        nonlocal raced
        opened = real_open_child(parent_fd, name, logical_path, observed)
        if not raced:
            child.rename(moved)
            child.symlink_to(outside)
            raced = True
        return opened

    monkeypatch.setattr(helper, "_open_child_directory", racing_open_child)

    with pytest.raises(manifest_error, match="directory changed while hashing"):
        build_manifest("integration", root)

    assert raced

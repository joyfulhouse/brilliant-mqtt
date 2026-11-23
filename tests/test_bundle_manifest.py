"""Deterministic, secret-safe bundle manifest helper."""

from __future__ import annotations

import hashlib
import importlib.util
import os
import re
import shutil
import stat
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
_SYSTEMD_PATHS = (
    "etc/systemd/system.control",
    "run/systemd/system.control",
    "run/systemd/transient",
    "run/systemd/generator.early",
    "etc/systemd/system",
    "etc/systemd/system.attached",
    "run/systemd/system",
    "run/systemd/system.attached",
    "run/systemd/generator",
    "usr/local/lib/systemd/system",
    "usr/lib/systemd/system",
    "lib/systemd/system",
    "run/systemd/generator.late",
)
_STOCK_ENVIRONMENT = (
    'BRILLIANT_PANEL="test"\nMQTT_HOST="broker.invalid"\nMQTT_PORT=1883\n'
    'MQTT_USERNAME="test"\nMQTT_PASSWORD="synthetic \\"value\\" \\\\ # $"\n'
    "MQTT_TLS_ENABLED=0\nMQTT_TLS_CA_FILE=/var/brilliant-mqtt/tls/ca.pem\n"
    "BRILLIANT_DEPLOYMENT_ID=0123456789abcdef0123456789abcdef\n"
    "RETAINED_TOPICS_FILE=/var/brilliant-mqtt/retained-topics.json\n"
    "MESH_PRIORITY=1\nSCENE_BRIDGE_ENABLED=0\nHOT_POLL_SECONDS=5\nRESYNC_SECONDS=300\n"
    "LOG_LEVEL=INFO\n"
)


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
        "app/brilliant_mqtt/wired_feedback.py",
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


# --- #164: read-only release identity comparison modes ---------------------------

_COMPONENTS = ("bridge", "wifi_watchdog", "bus_watchdog")
_CODE_FILES = {
    "app/brilliant_mqtt/bridge.py": "bridge code",
    "vendor/dependency.py": "vendored code",
    "wifi_watchdog/brilliant_wifi_watchdog/run.py": "wifi code",
    "bus_watchdog/brilliant_bus_watchdog/run.py": "bus code",
}


def _component_arguments(*components: str) -> list[str]:
    arguments: list[str] = []
    for component in components:
        arguments += ["--component", component]
    return arguments


def _make_payload(tmp_path: Path, *, version: str = "0.11.3", ordinal: str = "5") -> Path:
    payload = tmp_path / "payload"
    for name, content in _CODE_FILES.items():
        _write(payload / name, content)
    _write(payload / "VERSION", version + "\n")
    _write(payload / "RELEASE_ORDINAL", ordinal + "\n")
    return payload


def _make_legacy_panel(tmp_path: Path, payload: Path) -> tuple[Path, Path, Path]:
    """Mirror the fixed layout: code trees at the root, per-watchdog VERSION stamps."""
    panel = tmp_path / "panel"
    for tree in ("app", "vendor", "wifi_watchdog", "bus_watchdog"):
        shutil.copytree(payload / tree, panel / tree)
    version = (payload / "VERSION").read_text(encoding="ascii")
    _write(panel / "VERSION", version)
    _write(panel / "wifi_watchdog/VERSION", version)
    _write(panel / "bus_watchdog/VERSION", version)
    units = tmp_path / "image/etc/systemd/system"
    units.mkdir(parents=True)
    for service in ("brilliant-mqtt", "brilliant-wifi-watchdog", "brilliant-bus-watchdog"):
        shutil.copyfile(_PAYLOAD / (service + ".service"), units / (service + ".service"))
    env = tmp_path / "image/etc/brilliant-mqtt.env"
    _write(env, _STOCK_ENVIRONMENT)
    return panel, units, env


def _identity_run(
    panel: Path, units: Path, env: Path, *components: str
) -> subprocess.CompletedProcess[str]:
    return _run(
        "component-identities",
        panel,
        *_component_arguments(*components),
        "--unit-root",
        units,
        "--env",
        env,
    )


def _enroll(helper: ModuleType, panel: Path, units: Path, env: Path, ordinal: object) -> None:
    """The integration's default (persisting) path, then its ordinal enrolment."""
    for component, record in helper.installed_identities(panel, units, env).items():
        assert record is not None
        helper.private_record(panel, component, {**record, "release_ordinal": ordinal})


def _snapshot(root: Path) -> dict[str, tuple[int, int, int, bytes | None]]:
    observed: dict[str, tuple[int, int, int, bytes | None]] = {}
    for directory, names, files in os.walk(root):
        for name in [".", *names, *files]:
            path = Path(directory) / name
            metadata = path.lstat()
            content = path.read_bytes() if stat.S_ISREG(metadata.st_mode) else None
            observed[str(path.relative_to(root))] = (
                metadata.st_mode,
                metadata.st_mtime_ns,
                metadata.st_size,
                content,
            )
    return observed


def test_payload_identities_print_selected_components_in_order(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    helper = _load_helper()

    result = _run("payload-identities", payload, *_component_arguments("bus_watchdog", "bridge"))

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert result.stdout.splitlines() == [
        f"bus_watchdog\t0.11.3\t5\t{helper.code_digest(payload, 'bus_watchdog')}",
        f"bridge\t0.11.3\t5\t{helper.code_digest(payload, 'bridge')}",
    ]


def test_payload_identities_of_the_real_bundled_payload() -> None:
    helper = _load_helper()
    version = (_PAYLOAD / "VERSION").read_text(encoding="ascii").strip()
    ordinal = int((_PAYLOAD / "RELEASE_ORDINAL").read_text(encoding="ascii"))

    result = _run("payload-identities", _PAYLOAD, *_component_arguments(*_COMPONENTS))

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        f"{component}\t{version}\t{ordinal}\t{helper.code_digest(_PAYLOAD, component)}"
        for component in _COMPONENTS
    ]


def test_payload_identities_require_a_release_ordinal(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    (payload / "RELEASE_ORDINAL").unlink()

    result = _run("payload-identities", payload, "--component", "bridge")

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "bundle-manifest: payload release_ordinal is missing\n"


def test_payload_identities_require_each_selected_component(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    shutil.rmtree(payload / "bus_watchdog")

    result = _run("payload-identities", payload, *_component_arguments("bridge", "bus_watchdog"))

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "bundle-manifest: payload component 'bus_watchdog' is missing\n"


def test_non_ascii_release_ordinal_is_a_stable_error(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    (payload / "RELEASE_ORDINAL").write_bytes("５\n".encode())

    result = _run("payload-identities", payload, "--component", "bridge")

    assert result.returncode == 2
    assert result.stderr == "bundle-manifest: invalid release_ordinal\n"


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (
            ("payload-identities", "{root}", "--component", "bridge", "--component", "bridge"),
            "duplicate --component 'bridge'",
        ),
        (
            ("component-identities", "{root}", "--component", "nope"),
            "unknown component 'nope'",
        ),
        (("payload-identities", "{root}"), "--component is required for payload-identities"),
        (
            ("component-identities", "{root}"),
            "--component is required for component-identities",
        ),
        (
            ("integration", "{root}", "--component", "bridge"),
            "--component, --unit-root, and --env are valid only for identity layouts",
        ),
        (
            ("payload-release", "{root}", "--unit-root", "{root}"),
            "--component, --unit-root, and --env are valid only for identity layouts",
        ),
        (
            ("panel-release", "{root}", "--env", "{root}"),
            "--component, --unit-root, and --env are valid only for identity layouts",
        ),
        (
            ("payload-identities", "{root}", "--component", "bridge", "--unit-root", "{root}"),
            "--unit-root and --env are valid only for component-identities",
        ),
        (
            ("payload-identities", "{root}", "--component", "bridge", "--env", "{root}"),
            "--unit-root and --env are valid only for component-identities",
        ),
        (
            ("payload-identities", "{root}", "--component", "bridge", "--unit", "{root}"),
            "unit arguments are valid only for panel-release",
        ),
        (
            ("component-identities", "{root}", "--component", "bridge", "--wifi-unit", "{root}"),
            "unit arguments are valid only for panel-release",
        ),
        (
            ("component-identities", "{root}", "--component", "bridge", "--bus-unit", "{root}"),
            "unit arguments are valid only for panel-release",
        ),
    ],
)
def test_identity_argument_validation(
    tmp_path: Path, arguments: tuple[str, ...], message: str
) -> None:
    result = _run(*(argument.format(root=tmp_path) for argument in arguments))

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == f"bundle-manifest: {message}\n"


def test_legacy_panel_matches_payload_once_its_ordinal_is_enrolled(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    candidate = _run("payload-identities", payload, *_component_arguments(*_COMPONENTS))
    assert candidate.returncode == 0, candidate.stderr

    before = _identity_run(panel, units, env, *_COMPONENTS)

    assert before.returncode == 0, before.stderr
    assert before.stdout.splitlines() == [
        f"{component}\t0.11.3\tunknown\t{helper.code_digest(payload, component)}\tlegacy_fixed"
        for component in _COMPONENTS
    ]
    _enroll(helper, panel, units, env, 5)
    after = _identity_run(panel, units, env, *_COMPONENTS)
    assert after.returncode == 0, after.stderr
    assert [line.rsplit("\t", 1) for line in after.stdout.splitlines()] == [
        [line, "legacy_fixed"] for line in candidate.stdout.splitlines()
    ]


@pytest.mark.parametrize("ordinal", [0, -1, True, "5", 5.0, None])
def test_non_positive_or_non_int_ordinals_print_unknown(tmp_path: Path, ordinal: object) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    _enroll(_load_helper(), panel, units, env, ordinal)

    result = _identity_run(panel, units, env, "bridge")

    assert result.returncode == 0, result.stderr
    assert result.stdout.split("\t")[2] == "unknown"


def test_drifted_legacy_code_shows_a_different_digest_and_no_ordinal(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    _write(panel / "app/brilliant_mqtt/bridge.py", "hand-edited code")

    result = _identity_run(panel, units, env, "bridge", "bus_watchdog")

    assert result.returncode == 0, result.stderr
    bridge, bus = (line.split("\t") for line in result.stdout.splitlines())
    assert bridge[2] == "unknown"
    assert bridge[3] == helper.code_digest(panel, "bridge")
    assert bridge[3] != helper.code_digest(payload, "bridge")
    assert bus[2:4] == ["5", helper.code_digest(payload, "bus_watchdog")]


def test_missing_installed_component_prints_missing(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    shutil.rmtree(panel / "bus_watchdog")

    result = _identity_run(panel, units, env, "bus_watchdog", "bridge")

    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines[0] == "bus_watchdog\tmissing"
    assert lines[1].startswith("bridge\t0.11.3\tunknown\t")


def test_release_link_layout_is_detected(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    release = panel / "releases" / _RELEASE_NAME
    shutil.copytree(payload, release)
    (panel / "current").symlink_to(release)
    shutil.copyfile(payload / "VERSION", panel / "VERSION")
    for unit in units.glob("*.service"):
        shutil.copyfile(_PAYLOAD / unit.name.replace(".service", "-release.service"), unit)
    helper = _load_helper()

    result = _identity_run(panel, units, env, *_COMPONENTS)

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        f"{component}\t0.11.3\tunknown\t{helper.code_digest(payload, component)}\trelease_link"
        for component in _COMPONENTS
    ]


@pytest.mark.parametrize(
    "decoration",
    [
        "# Prior layout: /var/brilliant-mqtt/current/app\n",
        "  ; Prior layout: /var/brilliant-mqtt/current/app\n",
        "Description=Prior layout: /var/brilliant-mqtt/current/app\n",
        "Environment=UNRELATED=/var/brilliant-mqtt/current/app\n",
    ],
)
@pytest.mark.parametrize("release_unit", [False, True])
def test_unit_code_directives_select_the_actual_tree(
    tmp_path: Path, decoration: str, release_unit: bool
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    release = panel / "releases" / _RELEASE_NAME
    shutil.copytree(payload, release)
    (panel / "current").symlink_to(release)
    _write(panel / "app/brilliant_mqtt/bridge.py", "different fixed code")
    helper = _load_helper()
    fixed_digest = helper.code_digest(panel, "bridge")
    release_digest = helper.code_digest(release, "bridge")
    assert fixed_digest != release_digest
    filename = "brilliant-mqtt-release.service" if release_unit else "brilliant-mqtt.service"
    unit = (_PAYLOAD / filename).read_text()
    unit = unit.replace("[Service]\n", "[Service]\n" + decoration)
    _write(units / "brilliant-mqtt.service", unit)

    result = _identity_run(panel, units, env, "bridge")

    assert result.returncode == 0, result.stderr
    digest = release_digest if release_unit else fixed_digest
    layout = "release_link" if release_unit else "legacy_fixed"
    if decoration.lstrip().startswith(("#", ";")):
        assert result.stdout == f"bridge\t0.11.3\tunknown\t{digest}\t{layout}\n"
    else:
        _assert_unproven(result, "bridge", "unit template differs")
    admitted = helper.installed_identities(panel, units, env)["bridge"]
    assert admitted["digest"] == digest and admitted["layout"] == layout


@pytest.mark.parametrize(
    "directives",
    [
        "WorkingDirectory=/var/brilliant-mqtt/current/app\n"
        "Environment=PYTHONPATH=/var/brilliant-mqtt/app:/var/brilliant-mqtt/vendor\n",
        "Environment=PYTHONPATH=/var/brilliant-mqtt/current/app:/var/brilliant-mqtt/vendor\n",
        "ExecStart=/usr/bin/python3 -m brilliant_mqtt\n",
    ],
)
def test_unit_with_ambiguous_code_selection_fails(tmp_path: Path, directives: str) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    release = panel / "releases" / _RELEASE_NAME
    shutil.copytree(payload, release)
    (panel / "current").symlink_to(release)
    _write(panel / "app/brilliant_mqtt/bridge.py", "different fixed code")
    _write(units / "brilliant-mqtt.service", "[Service]\n" + directives)

    result = _identity_run(panel, units, env, "bridge")

    _assert_unproven(result, "bridge", "unit template differs")
    helper = _load_helper()
    with pytest.raises(helper.ManifestError, match="ambiguous installed code selection"):
        helper.installed_identities(panel, units, env)


@pytest.mark.parametrize(
    "directives",
    [
        'Environment="PYTHONPATH=/var/brilliant-mqtt/current/app:'
        '/var/brilliant-mqtt/current/vendor"\n',
        "Environment=UNRELATED=ignored \\\n"
        "# Ignore this continued-line comment: /var/brilliant-mqtt/app\n"
        ' "PYTHONPATH=/var/brilliant-mqtt/current/app:/var/brilliant-mqtt/current/vendor"\n',
        "ExecStart=/usr/bin/python3 /var/brilliant-mqtt/current/app/main.py\n",
    ],
)
def test_unit_code_selection_handles_quotes_continuations_and_execstart(
    tmp_path: Path, directives: str
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    release = panel / "releases" / _RELEASE_NAME
    shutil.copytree(payload, release)
    (panel / "current").symlink_to(release)
    _write(panel / "app/brilliant_mqtt/bridge.py", "different fixed code")
    _write(units / "brilliant-mqtt.service", "[Service]\n" + directives)

    result = _identity_run(panel, units, env, "bridge")

    _assert_unproven(result, "bridge", "unit template differs")
    helper = _load_helper()
    admitted = helper.installed_identities(panel, units, env)["bridge"]
    assert admitted["digest"] == helper.code_digest(release, "bridge")
    assert admitted["layout"] == "release_link"


def _assert_unproven(result: subprocess.CompletedProcess[str], component: str, reason: str) -> None:
    assert result.returncode == 0, result.stderr
    assert result.stdout == f"{component}\tunknown\n"
    assert result.stderr == f"bundle-manifest: {component} selection unproven ({reason})\n"


@pytest.mark.parametrize("component", _COMPONENTS)
@pytest.mark.parametrize("release_link", [False, True])
def test_embedded_qualification_templates_match_shipped_sources(
    component: str, release_link: bool
) -> None:
    helper = _load_helper()
    service = (
        "brilliant-mqtt" if component == "bridge" else "brilliant-" + component.replace("_", "-")
    )
    filename = service + ("-release" if release_link else "") + ".service"
    source = (_REPOSITORY_ROOT / "deploy" / filename).read_text()
    assert (_PAYLOAD / filename).read_text() == source
    fingerprint = hashlib.sha256(helper._normalized_unit(source).encode()).hexdigest()
    assert helper._STOCK_UNIT_DIGESTS[component][int(release_link)] == fingerprint


def test_qualification_environment_policy_matches_renderer_contract() -> None:
    source = (_REPOSITORY_ROOT / "custom_components/brilliant_mqtt/panel_ops.py").read_text()
    rendered = set(re.findall(r'^ENV_\w+ = "(\w+)"$', source, re.MULTILINE)) | {"LOG_LEVEL"}
    assert _load_helper()._STOCK_ENVIRONMENT_NAMES == rendered


@pytest.mark.parametrize("kind", ["missing", "directory", "symlink", "fifo", "non_utf8"])
def test_qualification_requires_readable_regular_environment(tmp_path: Path, kind: str) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    env.unlink()
    if kind == "directory":
        env.mkdir()
    elif kind == "symlink":
        env.symlink_to(tmp_path / "absent")
    elif kind == "fifo":
        os.mkfifo(env)
    elif kind == "non_utf8":
        env.write_bytes(b"\xff-private")

    _assert_unproven(_identity_run(panel, units, env, "bridge"), "bridge", "environment unreadable")


@pytest.mark.parametrize(
    "text",
    [
        'MQTT_HOST="unterminated\n',
        "MQTT_HOST=bare\\\n",
        'MQTT_HOST="multiline\nvalue"\n',
        "MQTT_HOST=x\nMQTT_HOST=y\n",
        'MQTT_HOST="x" PYTHONPATH=x\n',
        "not an assignment\n",
    ],
)
def test_qualification_rejects_unhandled_environment_syntax(tmp_path: Path, text: str) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    _write(env, text)
    _assert_unproven(_identity_run(panel, units, env, "bridge"), "bridge", "environment schema")


@pytest.mark.parametrize("dangling", [False, True])
def test_qualification_does_not_follow_unit_alias(tmp_path: Path, dangling: bool) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    unit = units / "brilliant-mqtt.service"
    unit.unlink()
    target = panel / "system" / unit.name
    if not dangling:
        _write(target, (_PAYLOAD / unit.name).read_text())
    unit.symlink_to(target)
    _assert_unproven(_identity_run(panel, units, env, "bridge"), "bridge", "unit alias")


@pytest.mark.parametrize(
    "source", [path for path in _SYSTEMD_PATHS if path != "etc/systemd/system"]
)
def test_qualification_rejects_additional_base_unit_sources(tmp_path: Path, source: str) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    unit = "brilliant-mqtt.service"
    _write(tmp_path / "image" / source / unit, (_PAYLOAD / unit).read_text())
    _assert_unproven(_identity_run(panel, units, env, "bridge"), "bridge", "additional unit source")


def test_qualification_image_default_env_never_reads_host_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    original = Path.read_text

    def guarded(path: Path, encoding: str | None = None, errors: str | None = None) -> str:
        assert path != Path("/etc/brilliant-mqtt.env"), "read the real host environment"
        return original(path, encoding=encoding, errors=errors)

    monkeypatch.setattr(Path, "read_text", guarded)
    assert (
        helper.main(
            ["component-identities", str(panel), "--component", "bridge", "--unit-root", str(units)]
        )
        == 0
    )
    result = capsys.readouterr()
    assert result.out.startswith("bridge\t0.11.3\t5\t")
    assert result.err == ""


@pytest.mark.parametrize("source", _SYSTEMD_PATHS)
@pytest.mark.parametrize(
    "dropin", ["brilliant-mqtt.service.d", "brilliant-.service.d", "service.d"]
)
def test_qualification_rejects_dropins_in_every_systemd_path(
    tmp_path: Path, source: str, dropin: str
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    _enroll(_load_helper(), panel, units, env, 5)
    _write(tmp_path / "image" / source / dropin / "override.conf", "[Service]\nNice=10\n")
    before = _snapshot(tmp_path)

    result = _identity_run(panel, units, env, "bridge")

    _assert_unproven(result, "bridge", "unit drop-ins")
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize(
    "assignment", ["PYTHONPATH", "PYTHONHOME", "LD_PRELOAD", "UNKNOWN_VARIABLE"]
)
def test_qualification_rejects_unlisted_environment_names(tmp_path: Path, assignment: str) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    _write(env, _STOCK_ENVIRONMENT + f'{assignment}="private-value"\n')
    before = _snapshot(tmp_path)

    result = _identity_run(panel, units, env, "bridge")

    _assert_unproven(result, "bridge", "environment schema")
    assert _snapshot(tmp_path) == before
    assert helper.installed_identities(panel, units, env)["bridge"]["release_ordinal"] == 5


@pytest.mark.parametrize("component", _COMPONENTS)
@pytest.mark.parametrize("release_link", [False, True])
def test_qualification_matches_all_six_shipped_units_and_stock_environment(
    tmp_path: Path, component: str, release_link: bool
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    if release_link:
        release = panel / "releases" / _RELEASE_NAME
        shutil.copytree(payload, release)
        (panel / "current").symlink_to(release)
        for unit in units.glob("*.service"):
            shutil.copyfile(_PAYLOAD / unit.name.replace(".service", "-release.service"), unit)
    _enroll(_load_helper(), panel, units, env, 5)

    result = _identity_run(panel, units, env, component)
    candidate = _run("payload-identities", payload, "--component", component)

    assert result.returncode == candidate.returncode == 0
    assert result.stderr == ""
    assert result.stdout.rsplit("\t", 1)[0] + "\n" == candidate.stdout
    layout = "release_link" if release_link else "legacy_fixed"
    assert result.stdout.endswith(f"\t{layout}\n")


@pytest.mark.parametrize(
    "alteration",
    ["execstart", "environmentfile", "pythonpath", "workingdirectory", "extra_directive"],
)
@pytest.mark.parametrize("component", _COMPONENTS)
@pytest.mark.parametrize("release_link", [False, True])
def test_qualification_nonstock_unit_is_unknown(
    tmp_path: Path, alteration: str, component: str, release_link: bool
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    service = (
        "brilliant-mqtt" if component == "bridge" else "brilliant-" + component.replace("_", "-")
    )
    unit = units / (service + ".service")
    if release_link:
        release = panel / "releases" / _RELEASE_NAME
        shutil.copytree(payload, release)
        (panel / "current").symlink_to(release)
        shutil.copyfile(_PAYLOAD / (service + "-release.service"), unit)
    _enroll(_load_helper(), panel, units, env, 5)
    content = unit.read_text()
    if alteration == "execstart":
        content = content.replace("ExecStart=", "ExecStart=/unexpected ")
    elif alteration == "environmentfile":
        content = content.replace("/etc/brilliant-mqtt.env", "/private/alternate.env")
    elif alteration == "pythonpath":
        content = content.replace(
            "Environment=PYTHONPATH=", "Environment=PYTHONPATH=/private/alternate:"
        )
    elif alteration == "workingdirectory":
        content = content.replace("[Service]", "[Service]\nWorkingDirectory=/private/alternate")
    else:
        content = content.replace(
            "[Service]", "[Service]\nEnvironment=LD_PRELOAD=/private/alternate"
        )
    _write(unit, content)

    result = _identity_run(panel, units, env, component)

    _assert_unproven(result, component, "unit template differs")


@pytest.mark.parametrize("override", ["runtime", "vendor", "environment_file"])
def test_qualification_reviewer_effective_selection_repros(tmp_path: Path, override: str) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    release = panel / "releases" / _RELEASE_NAME
    shutil.copytree(payload, release)
    _write(release / "app/brilliant_mqtt/bridge.py", "different active code")
    (panel / "current").symlink_to(release)
    assert helper.code_digest(panel, "bridge") != helper.code_digest(release, "bridge")
    assignment = "PYTHONPATH=/var/brilliant-mqtt/current/app:/var/brilliant-mqtt/current/vendor"
    if override == "environment_file":
        _write(env, _STOCK_ENVIRONMENT + assignment + "\n")
    else:
        directory = "run/systemd/system" if override == "runtime" else "usr/lib/systemd/system"
        _write(
            tmp_path / "image" / directory / "brilliant-mqtt.service.d/override.conf",
            "[Service]\nWorkingDirectory=/var/brilliant-mqtt/current/app\nEnvironment="
            + assignment,
        )

    result = _identity_run(panel, units, env, "bridge")

    reason = "environment schema" if override == "environment_file" else "unit drop-ins"
    _assert_unproven(result, "bridge", reason)
    assert helper.installed_identities(panel, units, env)["bridge"]["layout"] == "legacy_fixed"


def test_qualification_mixed_rows_preserve_order_and_only_warn_for_unknown(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    _enroll(_load_helper(), panel, units, env, 5)
    _write(units / "brilliant-bus-watchdog.service", "[Service]\nExecStart=/alternate\n")

    result = _identity_run(panel, units, env, "bus_watchdog", "bridge")

    assert result.returncode == 0
    lines = result.stdout.splitlines()
    assert lines[0] == "bus_watchdog\tunknown"
    assert lines[1].startswith("bridge\t0.11.3\t5\t")
    assert (
        result.stderr
        == "bundle-manifest: bus_watchdog selection unproven (unit template differs)\n"
    )


def _installed_or_staged_unit(panel: Path, units: Path, component: str, staged: bool) -> Path:
    service = (
        "brilliant-mqtt" if component == "bridge" else "brilliant-" + component.replace("_", "-")
    )
    directory = panel / ("system" if component == "bridge" else component) if staged else units
    if staged:
        (units / (service + ".service")).unlink(missing_ok=True)
    directory.mkdir(parents=True, exist_ok=True)
    return directory / (service + ".service")


@pytest.mark.parametrize("component", _COMPONENTS)
@pytest.mark.parametrize("staged", [False, True])
def test_present_release_unit_requires_selector(
    tmp_path: Path, component: str, staged: bool
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    unit = _installed_or_staged_unit(panel, units, component, staged)
    shutil.copyfile(_PAYLOAD / unit.name.replace(".service", "-release.service"), unit)
    assert not os.path.lexists(panel / "current")
    before = _snapshot(tmp_path)

    result = _identity_run(panel, units, env, component)

    reason = "unit unavailable" if staged else "release selector unavailable"
    _assert_unproven(result, component, reason)
    assert _snapshot(tmp_path) == before
    with pytest.raises(helper.ManifestError, match="installed release selector is missing"):
        helper.installed_identities(panel, units, env)


@pytest.mark.parametrize("component", _COMPONENTS)
@pytest.mark.parametrize(
    ("staged", "installed_dropin"), [(False, True), (True, False), (True, True)]
)
@pytest.mark.parametrize("current", [False, True])
def test_unit_dropins_cannot_verify_base_selection(
    tmp_path: Path, component: str, staged: bool, installed_dropin: bool, current: bool
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    if current:
        release = panel / "releases" / _RELEASE_NAME
        shutil.copytree(payload, release)
        for tree in (
            "app/brilliant_mqtt",
            "wifi_watchdog/brilliant_wifi_watchdog",
            "bus_watchdog/brilliant_bus_watchdog",
        ):
            _write(release / tree / "alternate.py", "different selected code")
        (panel / "current").symlink_to(release)
        assert helper.code_digest(panel, component) != helper.code_digest(release, component)
    unit = _installed_or_staged_unit(panel, units, component, staged)
    shutil.copyfile(_PAYLOAD / unit.name, unit)
    dropin_unit = units / unit.name if installed_dropin else unit
    tree = "app" if component == "bridge" else component
    _write(
        dropin_unit.parent / (dropin_unit.name + ".d") / "override.conf",
        f"[Service]\nWorkingDirectory=/var/brilliant-mqtt/current/{tree}\n"
        f"Environment=PYTHONPATH=/var/brilliant-mqtt/current/{tree}\n",
    )
    before = _snapshot(tmp_path)

    result = _identity_run(panel, units, env, component)

    reason = "unit drop-ins" if installed_dropin else "unit unavailable"
    _assert_unproven(result, component, reason)
    assert _snapshot(tmp_path) == before
    with pytest.raises(helper.ManifestError, match="unhandled installed unit drop-ins"):
        helper.installed_identities(panel, units, env)


@pytest.mark.parametrize("staged", [False, True])
@pytest.mark.parametrize(
    "directives",
    [
        "WorkingDirectory=/var/brilliant-mqtt/current/app\n"
        "Environment=PYTHONPATH=/var/brilliant-mqtt/app:/var/brilliant-mqtt/vendor\n",
        "Environment=PYTHONPATH=/var/brilliant-mqtt/current/app:/var/brilliant-mqtt/vendor\n",
        "Environment=PYTHONPATH=/var/brilliant-mqtt/app\n"
        "ExecStart=/usr/bin/python3 /var/brilliant-mqtt/current/app/main.py\n",
        "ExecStart=/usr/bin/python3 -m brilliant_mqtt\n",
        'Environment="PYTHONPATH=/var/brilliant-mqtt/app\n',
        "Environment=PYTHONPATH=/var/brilliant-mqtt/app \\\n",
    ],
)
def test_present_ambiguous_unit_fails_without_selector(
    tmp_path: Path, staged: bool, directives: str
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    unit = _installed_or_staged_unit(panel, units, "bridge", staged)
    _write(unit, "[Service]\n" + directives)

    result = _identity_run(panel, units, env, "bridge")

    reason = "unit unavailable" if staged else "unit template differs"
    _assert_unproven(result, "bridge", reason)
    with pytest.raises(helper.ManifestError, match="ambiguous installed code selection"):
        helper.installed_identities(panel, units, env)


@pytest.mark.parametrize(
    ("staged", "installed_dropin"), [(False, True), (True, False), (True, True)]
)
@pytest.mark.parametrize(
    "kind",
    ["empty_conf", "non_selection_conf", "symlink_conf", "directory_conf", "symlink_dir", "file"],
)
def test_unhandled_unit_dropin_entries_are_rejected(
    tmp_path: Path, staged: bool, installed_dropin: bool, kind: str
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    unit = _installed_or_staged_unit(panel, units, "bridge", staged)
    shutil.copyfile(_PAYLOAD / unit.name, unit)
    dropin_unit = units / unit.name if installed_dropin else unit
    directory = dropin_unit.parent / (dropin_unit.name + ".d")
    if kind == "file":
        _write(directory, "not a directory")
    elif kind == "symlink_dir":
        directory.symlink_to(tmp_path / "absent")
    else:
        directory.mkdir()
        entry = directory / "override.conf"
        if kind == "symlink_conf":
            entry.symlink_to(tmp_path / "absent")
        elif kind == "directory_conf":
            entry.mkdir()
        else:
            _write(entry, "[Service]\nNice=10\n" if kind == "non_selection_conf" else "")

    result = _identity_run(panel, units, env, "bridge")

    reason = "unit drop-ins" if installed_dropin else "unit unavailable"
    _assert_unproven(result, "bridge", reason)


@pytest.mark.parametrize("staged", [False, True])
@pytest.mark.parametrize(
    "directory", ["service.d", "brilliant-.service.d", "brilliant-wifi-.service.d"]
)
def test_shared_unit_dropins_are_also_unhandled(
    tmp_path: Path, staged: bool, directory: str
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    unit = _installed_or_staged_unit(panel, units, "wifi_watchdog", staged)
    shutil.copyfile(_PAYLOAD / unit.name, unit)
    _write(units / directory / "override.conf", "[Service]\nEnvironment=PYTHONPATH=/alternate\n")

    result = _identity_run(panel, units, env, "wifi_watchdog")

    _assert_unproven(result, "wifi_watchdog", "unit drop-ins")


@pytest.mark.parametrize("component", _COMPONENTS)
@pytest.mark.parametrize("staged", [False, True])
def test_present_fixed_unit_without_selector_still_matches(
    tmp_path: Path, component: str, staged: bool
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    unit = _installed_or_staged_unit(panel, units, component, staged)
    shutil.copyfile(_PAYLOAD / unit.name, unit)
    # Empty directories and files without the systemd .conf suffix have no effect.
    (unit.parent / (unit.name + ".d")).mkdir()
    _write(units / (unit.name + ".d") / "override.conf.disabled", "ignored")

    result = _identity_run(panel, units, env, component)

    assert result.returncode == 0, result.stderr
    digest = helper.code_digest(panel, component)
    if staged:
        _assert_unproven(result, component, "unit unavailable")
    else:
        assert result.stdout == f"{component}\t0.11.3\t5\t{digest}\tlegacy_fixed\n"
    assert helper.installed_identities(panel, units, env)[component]["digest"] == digest


def test_dangling_installed_unit_cannot_fall_back_to_staged_unit(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    staged = _installed_or_staged_unit(panel, units, "bridge", True)
    shutil.copyfile(_PAYLOAD / staged.name, staged)
    (units / staged.name).symlink_to(tmp_path / "absent")

    result = _identity_run(panel, units, env, "bridge")

    _assert_unproven(result, "bridge", "unit alias")
    with pytest.raises(FileNotFoundError):
        helper.installed_identities(panel, units, env)


def test_bridge_only_does_not_inspect_unselected_watchdog_dropins(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    _write(units / "brilliant-bus-watchdog.service.d/override.conf", "[Service]\nNice=10\n")

    result = _identity_run(panel, units, env, "bridge")

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("bridge\t0.11.3\t5\t")
    with pytest.raises(helper.ManifestError, match="unhandled installed unit drop-ins"):
        helper.installed_identities(panel, units, env)


def test_bridge_only_ignores_missing_enrolled_unselected_watchdog(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    shutil.rmtree(panel / "bus_watchdog")
    before = _snapshot(tmp_path)

    result = _identity_run(panel, units, env, "bridge")

    assert result.returncode == 0, result.stderr
    assert result.stdout == (
        f"bridge\t0.11.3\t5\t{helper.code_digest(panel, 'bridge')}\tlegacy_fixed\n"
    )
    assert _snapshot(tmp_path) == before


def test_missing_enrolled_selected_watchdog_still_fails(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()
    _enroll(helper, panel, units, env, 5)
    shutil.rmtree(panel / "bus_watchdog")

    result = _identity_run(panel, units, env, "bridge", "bus_watchdog")

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "bundle-manifest: recorded installed code is missing\n"
    with pytest.raises(helper.ManifestError, match="recorded installed code is missing"):
        helper.installed_identities(panel, units, env)


def test_mutating_scan_cannot_limit_its_components(tmp_path: Path) -> None:
    helper = _load_helper()
    with pytest.raises(helper.ManifestError, match="component selection requires persist=False"):
        helper.installed_identities(tmp_path, components=["bridge"])
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("component", ["wifi_watchdog", "bus_watchdog"])
def test_selected_historical_watchdog_keeps_bridge_version_fallback(
    tmp_path: Path, component: str
) -> None:
    payload = _make_payload(tmp_path, version="0.10.0")
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    (panel / component / "VERSION").unlink()

    result = _identity_run(panel, units, env, component)

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith(f"{component}\t0.10.0\tunknown\t")


def test_selected_watchdog_does_not_inherit_version_from_absent_bridge(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path, version="0.10.0")
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    (panel / "bus_watchdog/VERSION").unlink()
    shutil.rmtree(panel / "app")
    shutil.rmtree(panel / "vendor")

    result = _identity_run(panel, units, env, "bus_watchdog")

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("bus_watchdog\tunknown\tunknown\t")


def test_unit_root_and_env_are_forwarded_only_when_given(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    helper = _load_helper()
    calls: list[tuple[Path, dict[str, object]]] = []

    def recording(root: Path, **overrides: object) -> dict[str, object]:
        calls.append((root, overrides))
        return {component: None for component in _COMPONENTS}

    monkeypatch.setattr(helper, "installed_identities", recording)
    monkeypatch.setattr(helper, "_selection_unproven", lambda *_args: None)
    base = ["component-identities", str(tmp_path), "--component", "bridge"]

    assert helper.main(base) == 0
    assert helper.main([*base, "--unit-root", str(tmp_path / "units")]) == 0
    assert helper.main([*base, "--env", "e"]) == 0

    assert calls == [
        (tmp_path, {"persist": False, "components": ["bridge"]}),
        (tmp_path, {"persist": False, "components": ["bridge"], "unit_root": tmp_path / "units"}),
        (tmp_path, {"persist": False, "components": ["bridge"], "env_path": Path("e")}),
    ]
    assert capsys.readouterr().out == "bridge\tmissing\n" * 3


@pytest.mark.parametrize("defect", ["missing", "non_ascii", "symlink", "directory", "invalid"])
def test_candidate_version_must_be_one_valid_regular_file(tmp_path: Path, defect: str) -> None:
    payload = _make_payload(tmp_path)
    version = payload / "VERSION"
    version.unlink()
    if defect == "non_ascii":
        version.write_bytes("0.11.３\n".encode())
    elif defect == "symlink":
        _write(tmp_path / "elsewhere", "0.11.3\n")
        version.symlink_to(tmp_path / "elsewhere")
    elif defect == "directory":
        version.mkdir()
    elif defect == "invalid":
        version.write_text("not a version\n", encoding="ascii")

    result = _run("payload-identities", payload, "--component", "bridge")

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "bundle-manifest: invalid candidate version\n"


@pytest.mark.parametrize(
    ("defect", "error"),
    [("directory", "IsADirectoryError"), ("non_utf8", "UnicodeDecodeError")],
)
def test_unreadable_installed_version_is_a_content_free_error(
    tmp_path: Path, defect: str, error: str
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    (panel / "VERSION").unlink()
    if defect == "directory":
        (panel / "VERSION").mkdir()
    else:
        (panel / "VERSION").write_bytes(b"\xff\xfe-SECRET")

    result = _identity_run(panel, units, env, "bridge")

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == f"bundle-manifest: installed identity unreadable ({error})\n"
    assert str(panel) not in result.stderr
    assert "SECRET" not in result.stderr


@pytest.mark.parametrize("enrolled", [False, True])
def test_component_identities_never_write_to_the_panel_tree(tmp_path: Path, enrolled: bool) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    if enrolled:
        _enroll(_load_helper(), panel, units, env, 5)
        # A stale record which the persisting path would rewrite.
        _write(panel / "app/brilliant_mqtt/bridge.py", "drifted code")
    before = _snapshot(tmp_path)

    result = _identity_run(panel, units, env, *_COMPONENTS)

    assert result.returncode == 0, result.stderr
    assert _snapshot(tmp_path) == before
    assert (panel / ".release-identities").exists() is enrolled


def test_ordinal_provable_only_by_an_uncreated_record_stays_unknown(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)

    first = _identity_run(panel, units, env, "bridge")
    second = _identity_run(panel, units, env, "bridge")

    assert first.returncode == second.returncode == 0
    assert first.stdout == second.stdout
    assert first.stdout.split("\t")[2] == "unknown"
    assert not (panel / ".release-identities").exists()


@pytest.mark.parametrize(
    ("record", "error"),
    [(b"{bad", "JSONDecodeError"), (b"[" * 16000, "RecursionError")],
    ids=["malformed", "deeply_nested"],
)
def test_corrupt_identity_record_is_a_stable_content_free_error(
    tmp_path: Path, record: bytes, error: str
) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    _enroll(_load_helper(), panel, units, env, 5)
    (panel / ".release-identities/bridge.json").write_bytes(record)

    result = _identity_run(panel, units, env, "bridge")

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == f"bundle-manifest: installed identity unreadable ({error})\n"
    assert "Traceback" not in result.stderr
    assert str(panel) not in result.stderr


def test_component_identities_reject_an_unavailable_root(tmp_path: Path) -> None:
    result = _run("component-identities", tmp_path / "absent", "--component", "bridge")

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "bundle-manifest: installed root is unavailable\n"


def test_usage_errors_name_the_helper() -> None:
    result = _run("not-a-layout", "/")

    assert result.returncode == 2
    assert result.stderr.startswith("usage: bundle-manifest ")
    assert "bundle-manifest: error: argument layout: invalid choice" in result.stderr


def test_helper_runs_from_stdin_in_isolated_mode(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    expected = _identity_run(panel, units, env, "bridge")

    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-I",
            "-",
            "component-identities",
            str(panel),
            "--component",
            "bridge",
            "--unit-root",
            str(units),
            "--env",
            str(env),
        ],
        input=_SCRIPT.read_text(encoding="utf-8"),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == expected.stdout


def test_read_only_installed_identities_skip_every_write(tmp_path: Path) -> None:
    payload = _make_payload(tmp_path)
    panel, units, env = _make_legacy_panel(tmp_path, payload)
    helper = _load_helper()

    read_only = helper.installed_identities(panel, units, env, persist=False)

    assert not (panel / ".release-identities").exists()
    persisted = helper.installed_identities(panel, units, env)
    assert persisted == read_only
    record = panel / ".release-identities/bridge.json"
    assert record.stat().st_mode & 0o777 == 0o600
    assert helper.private_record(panel, "bridge") == persisted["bridge"]

"""Deterministic, secret-safe bundle manifest helper."""

from __future__ import annotations

import hashlib
import importlib.util
import os
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
    units = tmp_path / "units"
    units.mkdir()
    return panel, units, tmp_path / "absent.env"


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
    panel = tmp_path / "panel"
    release = panel / "releases" / _RELEASE_NAME
    shutil.copytree(payload, release)
    (panel / "current").symlink_to(release)
    shutil.copyfile(payload / "VERSION", panel / "VERSION")
    units = tmp_path / "units"
    units.mkdir()
    helper = _load_helper()

    result = _identity_run(panel, units, tmp_path / "absent.env", *_COMPONENTS)

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        f"{component}\t0.11.3\tunknown\t{helper.code_digest(payload, component)}\trelease_link"
        for component in _COMPONENTS
    ]


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
    base = ["component-identities", str(tmp_path), "--component", "bridge"]

    assert helper.main(base) == 0
    assert helper.main([*base, "--unit-root", str(tmp_path / "units")]) == 0
    assert helper.main([*base, "--env", "e"]) == 0

    assert calls == [
        (tmp_path, {"persist": False}),
        (tmp_path, {"persist": False, "unit_root": tmp_path / "units"}),
        (tmp_path, {"persist": False, "env_path": Path("e")}),
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


def _finalize_fixture(tmp_path: Path, pin: str | None, *, symlink: bool = False) -> Path:
    identifier = "c" * 32
    release = tmp_path / "releases" / _RELEASE_NAME
    release.mkdir(parents=True)
    if pin is not None:
        if symlink:
            target = tmp_path / "elsewhere"
            target.write_text(pin)
            (release / ".rollback-retained").symlink_to(target)
        else:
            (release / ".rollback-retained").write_text(pin)
    directory = tmp_path / ".rollback" / identifier
    directory.mkdir(parents=True)
    encoded = (
        '{"limits":{"capture_seconds":120,"maximum_bytes":1,"maximum_entries":1},'
        f'"release_target":"{release}"}}'
    ).encode()
    (directory / "complete.json").write_bytes(encoded)
    baseline_action = cast("Callable[[Path, str, str, str], None]", _load_helper().baseline_action)
    baseline_action(tmp_path, identifier, hashlib.sha256(encoded).hexdigest(), "finalize")
    assert not directory.exists()
    assert not (tmp_path / ".rollback" / f".{identifier}.finalizing").exists()
    return release / ".rollback-retained"


def test_finalize_removes_the_pin_naming_this_baseline(tmp_path: Path) -> None:
    assert not os.path.lexists(_finalize_fixture(tmp_path, "c" * 32))


def test_finalize_keeps_a_pin_naming_another_baseline(tmp_path: Path) -> None:
    pin = _finalize_fixture(tmp_path, "d" * 32)
    assert pin.read_text() == "d" * 32


def test_finalize_keeps_a_symlinked_pin(tmp_path: Path) -> None:
    pin = _finalize_fixture(tmp_path, "c" * 32, symlink=True)
    assert pin.is_symlink()


def test_finalize_without_a_pin_still_retires_the_baseline(tmp_path: Path) -> None:
    assert not os.path.lexists(_finalize_fixture(tmp_path, None))

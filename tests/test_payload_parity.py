"""Prove the committed panel payload exactly matches the agent source tree."""

from __future__ import annotations

import hashlib
import re
import subprocess
from pathlib import Path

import brilliant_mqtt

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src" / "brilliant_mqtt"
PAYLOAD_ROOT = (
    REPOSITORY_ROOT
    / "custom_components"
    / "brilliant_mqtt"
    / "agent_payload"
    / "app"
    / "brilliant_mqtt"
)
BUILD_SCRIPT = REPOSITORY_ROOT / "scripts" / "build_payload.sh"
PAYLOAD_WORKFLOWS = (
    REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml",
    REPOSITORY_ROOT / ".github" / "workflows" / "release.yml",
)
PAYLOAD_RELATIVE_PATH = Path("custom_components/brilliant_mqtt/agent_payload")
PAYLOAD_VERSION_FILE = REPOSITORY_ROOT / PAYLOAD_RELATIVE_PATH / "VERSION"
EXPECTED_PAYLOAD_VERSION = "0.9.1"
RELEASE_SERVICE_TEMPLATES = (
    "brilliant-mqtt-release.service",
    "brilliant-wifi-watchdog-release.service",
    "brilliant-bus-watchdog-release.service",
)


def _sha256_files(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
        and "__pycache__" not in path.relative_to(root).parts
        and path.suffix != ".pyc"
    }


def _workflow_guard(workflow: Path) -> str:
    marker = "      - name: Verify generated payload is committed\n        run: |\n"
    guard_source = workflow.read_text(encoding="utf-8").split(marker, maxsplit=1)[1]
    commands: list[str] = []
    for line in guard_source.splitlines():
        if not line.startswith("          "):
            break
        commands.append(line[10:])
    assert commands
    return "\n".join(commands)


def _guard_repository(root: Path) -> Path:
    payload = root / PAYLOAD_RELATIVE_PATH
    payload.mkdir(parents=True)
    tracked = payload / "tracked.txt"
    tracked.write_text("committed\n", encoding="utf-8")
    (root / ".gitignore").write_text("*.token\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "add", ".gitignore", tracked], cwd=root, check=True)
    return tracked


def _run_guard(workflow: Path, repository: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", _workflow_guard(workflow)],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
    )


def test_committed_agent_payload_matches_source_tree() -> None:
    source = _sha256_files(SOURCE_ROOT)
    payload = _sha256_files(PAYLOAD_ROOT)

    missing = sorted(source.keys() - payload.keys())
    extra = sorted(payload.keys() - source.keys())
    changed = sorted(
        path for path in source.keys() & payload.keys() if source[path] != payload[path]
    )

    assert not (missing or extra or changed), (
        "committed agent payload differs from src/brilliant_mqtt:\n"
        f"missing={missing}\n"
        f"extra={extra}\n"
        f"changed={changed}"
    )


def test_project_source_and_payload_versions_are_0_7_0() -> None:
    project_source = (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    project_section = project_source.split("[project]", maxsplit=1)[1].split("\n[", maxsplit=1)
    version_match = re.search(r'^version = "([^"]+)"$', project_section[0], re.MULTILINE)

    assert version_match is not None
    assert version_match.group(1) == EXPECTED_PAYLOAD_VERSION
    assert brilliant_mqtt.__version__ == EXPECTED_PAYLOAD_VERSION
    assert PAYLOAD_VERSION_FILE.read_text(encoding="utf-8").strip() == EXPECTED_PAYLOAD_VERSION


def test_preflight_and_retained_topic_modules_are_packaged_byte_for_byte() -> None:
    for relative_path in (
        Path("setup_protocol.py"),
        Path("preflight.py"),
        Path("retained_topics.py"),
    ):
        assert (PAYLOAD_ROOT / relative_path).read_bytes() == (
            SOURCE_ROOT / relative_path
        ).read_bytes()


def test_payload_build_pins_every_vendored_mqtt_distribution_without_deps() -> None:
    script = BUILD_SCRIPT.read_text(encoding="utf-8")

    assert 'm.version("typing-extensions")' in script
    assert '"typing-extensions==$TYPING_EXTENSIONS_VERSION"' in script
    assert "--no-deps" in script


def test_fleet_release_service_templates_are_packaged_byte_for_byte() -> None:
    payload_root = REPOSITORY_ROOT / PAYLOAD_RELATIVE_PATH
    for name in RELEASE_SERVICE_TEMPLATES:
        assert (payload_root / name).read_bytes() == (
            REPOSITORY_ROOT / "deploy" / name
        ).read_bytes()


def test_payload_build_copies_every_fleet_release_service_template() -> None:
    script = BUILD_SCRIPT.read_text(encoding="utf-8")

    for name in RELEASE_SERVICE_TEMPLATES:
        assert f'cp "$ROOT/deploy/{name}" "$DEST/{name}"' in script


def test_fleet_release_templates_use_current_link_without_changing_legacy_layout() -> None:
    payload_root = REPOSITORY_ROOT / PAYLOAD_RELATIVE_PATH
    expected_release_paths = {
        "brilliant-mqtt": (
            "WorkingDirectory=/var/brilliant-mqtt/current/app",
            (
                "Environment=PYTHONPATH=/var/brilliant-mqtt/current/app:"
                "/var/brilliant-mqtt/current/vendor"
            ),
        ),
        "brilliant-wifi-watchdog": (
            "WorkingDirectory=/var/brilliant-mqtt/current/wifi_watchdog",
            "Environment=PYTHONPATH=/var/brilliant-mqtt/current/wifi_watchdog",
        ),
        "brilliant-bus-watchdog": (
            "WorkingDirectory=/var/brilliant-mqtt/current/bus_watchdog",
            "Environment=PYTHONPATH=/var/brilliant-mqtt/current/bus_watchdog",
        ),
    }
    expected_legacy_paths = {
        "brilliant-mqtt": (
            "Environment=PYTHONPATH=/var/brilliant-mqtt/app:/var/brilliant-mqtt/vendor",
        ),
        "brilliant-wifi-watchdog": (
            "WorkingDirectory=/var/brilliant-mqtt/wifi_watchdog",
            "Environment=PYTHONPATH=/var/brilliant-mqtt/wifi_watchdog",
        ),
        "brilliant-bus-watchdog": (
            "WorkingDirectory=/var/brilliant-mqtt/bus_watchdog",
            "Environment=PYTHONPATH=/var/brilliant-mqtt/bus_watchdog",
        ),
    }

    for service, expected_paths in expected_release_paths.items():
        source = (REPOSITORY_ROOT / "deploy" / f"{service}-release.service").read_text(
            encoding="utf-8"
        )
        packaged = (payload_root / f"{service}-release.service").read_text(encoding="utf-8")
        assert source == packaged
        assert all(path in source for path in expected_paths)

    for service, legacy_paths in expected_legacy_paths.items():
        source = (REPOSITORY_ROOT / "deploy" / f"{service}.service").read_text(encoding="utf-8")
        packaged = (payload_root / f"{service}.service").read_text(encoding="utf-8")
        assert source == packaged
        assert "/var/brilliant-mqtt/current/" not in source
        assert all(path in source for path in legacy_paths)


def test_payload_workflow_guards_reject_untracked_generated_files() -> None:
    for workflow in PAYLOAD_WORKFLOWS:
        source = workflow.read_text(encoding="utf-8")

        assert "git diff --exit-code -- custom_components/brilliant_mqtt/agent_payload" in source, (
            workflow
        )
        assert (
            "git ls-files --others --exclude-standard -- "
            "custom_components/brilliant_mqtt/agent_payload" in source
        ), workflow
        assert (
            "git ls-files --others --ignored --exclude-standard -- "
            "custom_components/brilliant_mqtt/agent_payload" in source
        ), workflow


def test_payload_workflow_guards_reject_every_git_status_class(tmp_path: Path) -> None:
    for workflow in PAYLOAD_WORKFLOWS:
        clean = tmp_path / f"{workflow.stem}-clean"
        _guard_repository(clean)
        assert _run_guard(workflow, clean).returncode == 0

        modified = tmp_path / f"{workflow.stem}-modified"
        modified_tracked = _guard_repository(modified)
        modified_tracked.write_text("modified\n", encoding="utf-8")
        assert _run_guard(workflow, modified).returncode != 0

        deleted = tmp_path / f"{workflow.stem}-deleted"
        deleted_tracked = _guard_repository(deleted)
        deleted_tracked.unlink()
        assert _run_guard(workflow, deleted).returncode != 0

        ordinary = tmp_path / f"{workflow.stem}-ordinary-untracked"
        _guard_repository(ordinary)
        (ordinary / PAYLOAD_RELATIVE_PATH / "generated file.txt").write_text(
            "ordinary\n", encoding="utf-8"
        )
        assert _run_guard(workflow, ordinary).returncode != 0

        ignored = tmp_path / f"{workflow.stem}-ignored-untracked"
        _guard_repository(ignored)
        ignored_path = ignored / PAYLOAD_RELATIVE_PATH / "generated secret.token"
        ignored_path.write_text("credential\n", encoding="utf-8")
        result = _run_guard(workflow, ignored)
        assert result.returncode != 0, (
            f"{workflow} accepted ignored payload path containing whitespace: {ignored_path}"
        )

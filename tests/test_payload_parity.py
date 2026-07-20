"""Prove the committed panel payload exactly matches the agent source tree."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

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
OBSERVER_SOURCE_ROOT = REPOSITORY_ROOT / "src" / "brilliant_ble_observer"
OBSERVER_PAYLOAD_ROOT = (
    REPOSITORY_ROOT
    / "custom_components"
    / "brilliant_mqtt"
    / "agent_payload"
    / "ble_observer"
    / "brilliant_ble_observer"
)
BUILD_SCRIPT = REPOSITORY_ROOT / "scripts" / "build_payload.sh"
SHA256_VERIFIER = REPOSITORY_ROOT / "scripts" / "verify_sha256.py"
UV_LOCK = REPOSITORY_ROOT / "uv.lock"
PAYLOAD_WORKFLOWS = (
    REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml",
    REPOSITORY_ROOT / ".github" / "workflows" / "release.yml",
)
PAYLOAD_RELATIVE_PATH = Path("custom_components/brilliant_mqtt/agent_payload")
AGENT_PAYLOAD_ROOT = REPOSITORY_ROOT / PAYLOAD_RELATIVE_PATH


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


def test_payload_build_pins_every_vendored_mqtt_distribution_without_deps() -> None:
    script = BUILD_SCRIPT.read_text(encoding="utf-8")

    assert 'm.version("typing-extensions")' in script
    assert '"typing-extensions==$TYPING_EXTENSIONS_VERSION"' in script
    assert "--no-deps" in script


def test_committed_ble_observer_payload_matches_source_and_unit() -> None:
    assert _sha256_files(OBSERVER_PAYLOAD_ROOT) == _sha256_files(OBSERVER_SOURCE_ROOT)
    assert (AGENT_PAYLOAD_ROOT / "brilliant-ble-observer.service").read_bytes() == (
        REPOSITORY_ROOT / "deploy" / "brilliant-ble-observer.service"
    ).read_bytes()


def test_ble_observer_payload_has_pinned_dbus_next_without_build_or_secret_artifacts() -> None:
    script = BUILD_SCRIPT.read_text(encoding="utf-8")
    assert 'm.version("dbus-next")' in script
    assert '"dbus-next==$DBUS_NEXT_VERSION"' in script
    assert "--no-deps" in script
    locked_hash = "58948f9aff9db08316734c0be2a120f6dc502124d9642f55e90ac82ffb16a18b"
    locked_url = (
        "https://files.pythonhosted.org/packages/d2/fc/"
        "c0a3f4c4eaa5a22fbef91713474666e13d0ea2a69c84532579490a9f2cc8/"
        "dbus_next-0.2.3-py3-none-any.whl"
    )
    assert f'DBUS_NEXT_SHA256="{locked_hash}"' in script
    assert f'DBUS_NEXT_URL="{locked_url}"' in script
    assert 'verify_sha256.py"' in script
    assert '"$DBUS_NEXT_WHEEL" "$DBUS_NEXT_SHA256"' in script
    lock = UV_LOCK.read_text(encoding="utf-8")
    assert locked_url in lock
    assert f"sha256:{locked_hash}" in lock
    assert (AGENT_PAYLOAD_ROOT / "ble_observer/vendor/dbus_next/__init__.py").is_file()

    license_text = (
        AGENT_PAYLOAD_ROOT / "ble_observer/vendor-licenses/dbus-next-LICENSE"
    ).read_text(encoding="utf-8")
    assert license_text.startswith("Copyright (c) 2019 Tony Crisci\n")
    provenance = (
        AGENT_PAYLOAD_ROOT / "ble_observer/vendor-licenses/dbus-next-PROVENANCE.txt"
    ).read_text(encoding="utf-8")
    assert provenance.splitlines() == [
        "Name: dbus-next",
        "Version: 0.2.3",
        "Lock-Source: uv.lock",
        f"Wheel-URL: {locked_url}",
        f"Wheel-SHA256: {locked_hash}",
        "License: dbus-next-LICENSE",
    ]

    forbidden_parts = {"__pycache__"}
    forbidden_suffixes = {".pyc", ".pyo", ".env", ".pem", ".key", ".token"}
    for path in AGENT_PAYLOAD_ROOT.rglob("*"):
        relative = path.relative_to(AGENT_PAYLOAD_ROOT)
        assert not forbidden_parts.intersection(relative.parts), relative
        assert not any(part.endswith(".dist-info") for part in relative.parts), relative
        if path.is_file():
            assert path.suffix not in forbidden_suffixes, relative


def test_payload_sha256_verifier_fails_closed_on_mismatch(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.whl"
    artifact.write_bytes(b"locked artifact")
    expected = hashlib.sha256(artifact.read_bytes()).hexdigest()

    valid = subprocess.run(
        ["python", str(SHA256_VERIFIER), str(artifact), expected],
        check=False,
        capture_output=True,
        text=True,
    )
    assert valid.returncode == 0

    mismatch = subprocess.run(
        ["python", str(SHA256_VERIFIER), str(artifact), "0" * 64],
        check=False,
        capture_output=True,
        text=True,
    )
    assert mismatch.returncode != 0
    assert "SHA-256 mismatch" in mismatch.stderr


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

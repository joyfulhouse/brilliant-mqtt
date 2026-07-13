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
BUILD_SCRIPT = REPOSITORY_ROOT / "scripts" / "build_payload.sh"
PAYLOAD_WORKFLOWS = (
    REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml",
    REPOSITORY_ROOT / ".github" / "workflows" / "release.yml",
)
PAYLOAD_RELATIVE_PATH = Path("custom_components/brilliant_mqtt/agent_payload")


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

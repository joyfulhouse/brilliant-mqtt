"""The manual agent uninstall in docs/TROUBLESHOOTING.md matches the integration."""

from __future__ import annotations

import re
import textwrap
from pathlib import Path

import pytest

from custom_components.brilliant_mqtt import panel_ops
from tests.canary_rehearsal import RehearsalShell
from tests.fakes import FakeShell

_DOC = Path(__file__).resolve().parents[2] / "docs" / "TROUBLESHOOTING.md"
_IDENTIFIER = "0123456789abcdef0123456789abcdef"
_GUARD_TEST = "if ls /var/brilliant-mqtt/.rollback/*/complete.json >/dev/null 2>&1; then"
# Step 2 repeats step 1's guard as its only extra line, so pasting it alone is safe.
_STEP_TWO_GUARD = (
    f'{_GUARD_TEST} echo "STOP: retained rollback baseline present; do not uninstall"; exit 1; fi'
)


def _block(marker: str) -> str:
    text = _DOC.read_text()
    name = re.escape(marker)
    match = re.search(
        rf"<!-- {name}:start -->\n```bash\n(?P<body>.*?)```\n<!-- {name}:end -->",
        text,
        re.DOTALL,
    )
    assert match is not None, marker
    return match["body"]


def _guard() -> str:
    return _block("manual-agent-uninstall-guard")


def _uninstall_lines() -> list[str]:
    return [line for line in _block("manual-agent-uninstall").splitlines() if line.strip()]


async def test_documented_commands_equal_the_integration_commands_in_order() -> None:
    shell = FakeShell()
    await shell.connect()

    await panel_ops.uninstall_wifi_watchdog(shell)
    await panel_ops.uninstall_bus_watchdog(shell)
    await panel_ops.uninstall(shell)

    first, *rest = _uninstall_lines()
    assert first == _STEP_TWO_GUARD
    assert rest == shell.commands
    assert shell.commands.count("systemctl daemon-reload") == 3


def test_guard_comes_first_and_mutates_nothing() -> None:
    text = _DOC.read_text()
    assert text.index("manual-agent-uninstall-guard:start") < text.index(
        "manual-agent-uninstall:start"
    )
    guard = _guard()
    assert re.search(r"\brm\b", guard) is None
    assert "systemctl" not in guard
    assert "exit 1" in guard
    assert guard.splitlines()[0] == _GUARD_TEST


def test_blocks_never_touch_the_firmware_tree() -> None:
    assert "/data/switch-embedded" not in _guard()
    assert "/data/switch-embedded" not in "\n".join(_uninstall_lines())


def _layout(panel: Path, name: str) -> None:
    rollback = panel / ".rollback"
    release = panel / "releases" / f"0.10.2--{'a' * 32}"
    if name in {"complete_archive", "pin_and_archive"}:
        (rollback / _IDENTIFIER).mkdir(parents=True)
        (rollback / _IDENTIFIER / "baseline.tar").write_bytes(b"")
        (rollback / _IDENTIFIER / "complete.json").write_text("{}")
    if name == "incomplete_archive":
        (rollback / _IDENTIFIER).mkdir(parents=True)
        (rollback / _IDENTIFIER / "baseline.tar").write_bytes(b"")
    if name == "partial_tmp":
        (rollback / f".{_IDENTIFIER}.tmp").mkdir(parents=True)
        (rollback / f".{_IDENTIFIER}.tmp" / "complete.json").write_text("{}")
    if name == "finalizing":
        (rollback / f".{_IDENTIFIER}.finalizing").mkdir(parents=True)
        (rollback / f".{_IDENTIFIER}.finalizing" / "complete.json").write_text("{}")
    if name in {"pin_only", "pin_and_archive"}:
        release.mkdir(parents=True)
        (release / ".rollback-retained").write_text(_IDENTIFIER)


@pytest.mark.parametrize(
    ("layout", "refuses"),
    [
        ("none", False),
        ("complete_archive", True),
        ("incomplete_archive", False),
        ("partial_tmp", False),
        ("finalizing", False),
        ("pin_only", False),
        ("pin_and_archive", True),
    ],
)
async def test_guard_is_at_least_as_strict_as_the_integration(
    tmp_path: Path, layout: str, refuses: bool
) -> None:
    shell = RehearsalShell(tmp_path)
    _layout(shell.panel, layout)

    integration_refuses = await panel_ops.baseline_retained(shell)
    result = await shell.run(_guard())
    guard_refuses = result.exit_status != 0

    assert integration_refuses == refuses
    assert guard_refuses == refuses
    assert guard_refuses or not integration_refuses
    step_two = await shell.run(_STEP_TWO_GUARD)
    assert (step_two.exit_status != 0) == refuses
    if guard_refuses:
        assert result.stdout.startswith("STOP:")
        assert result.exit_status == 1
    else:
        assert result.stdout == "OK: no retained rollback baseline\n"


_CANARY_DOC = Path(__file__).resolve().parents[2] / "docs" / "canary-rollback.md"
_OTHER = "fedcba9876543210fedcba9876543210"


def _cleanup_block() -> str:
    text = _CANARY_DOC.read_text()
    match = re.search(
        r"<!-- interrupted-baseline-cleanup:start -->\n( *)```bash\n(?P<body>.*?)\1```\n"
        r" *<!-- interrupted-baseline-cleanup:end -->",
        text,
        re.DOTALL,
    )
    assert match is not None
    return textwrap.dedent(match["body"])


def test_interrupted_capture_has_a_documented_way_out() -> None:
    text = _CANARY_DOC.read_text()
    assert "\n## Interrupted baseline capture\n" in text
    assert re.search(r"^\| `baseline_capture_interrupted` \|", text, re.MULTILINE)
    section = text.split("\n## Interrupted baseline capture\n", 1)[1].split("\n## ", 1)[0]
    assert "-o NumberOfPasswordPrompts=1" in section
    assert "/data/switch-embedded" not in section


async def _run_cleanup(shell: RehearsalShell, identifier: str) -> int:
    script = _cleanup_block().replace("ID=<id>", f"ID={identifier}")
    return (await shell.run(script)).exit_status


@pytest.mark.parametrize("release", [False, True], ids=["legacy", "release_link"])
async def test_cleanup_mirrors_finalize_and_touches_only_this_baseline(
    tmp_path: Path, release: bool
) -> None:
    shell = RehearsalShell(tmp_path)
    shell.install(release=release)
    rollback = shell.panel / ".rollback"
    for name in (
        _IDENTIFIER,
        f".{_IDENTIFIER}.tmp",
        f".{_IDENTIFIER}.finalizing",
        _OTHER,
        f".{_OTHER}.tmp",
    ):
        (rollback / name).mkdir(parents=True)
        (rollback / name / "complete.json").write_text("{}")
    pin = shell.panel / "current" / ".rollback-retained"
    if release:
        pin.write_text(_IDENTIFIER)

    assert await _run_cleanup(shell, _IDENTIFIER) == 0

    assert sorted(path.name for path in rollback.iterdir()) == [f".{_OTHER}.tmp", _OTHER]
    assert not pin.exists()
    assert (shell.panel / "VERSION").exists()


async def test_cleanup_keeps_a_pin_naming_another_baseline(tmp_path: Path) -> None:
    shell = RehearsalShell(tmp_path)
    shell.install(release=True)
    (shell.panel / ".rollback" / _IDENTIFIER).mkdir(parents=True)
    pin = shell.panel / "current" / ".rollback-retained"
    pin.write_text(_OTHER)

    assert await _run_cleanup(shell, _IDENTIFIER) == 0

    assert pin.read_text() == _OTHER
    assert not (shell.panel / ".rollback" / _IDENTIFIER).exists()


@pytest.mark.parametrize("identifier", ["", "..", "*", "ABCDEF0123456789ABCDEF0123456789"])
async def test_cleanup_refuses_an_invalid_identifier(tmp_path: Path, identifier: str) -> None:
    shell = RehearsalShell(tmp_path)
    shell.install()
    (shell.panel / ".rollback" / _OTHER).mkdir(parents=True)

    assert await _run_cleanup(shell, identifier) != 0

    assert (shell.panel / ".rollback" / _OTHER).exists()

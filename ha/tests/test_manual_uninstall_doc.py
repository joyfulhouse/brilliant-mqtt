"""The manual agent uninstall in docs/TROUBLESHOOTING.md matches the integration."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from custom_components.brilliant_mqtt import panel_ops
from tests.canary_rehearsal import RehearsalShell
from tests.fakes import FakeShell

_DOC = Path(__file__).resolve().parents[2] / "docs" / "TROUBLESHOOTING.md"
_IDENTIFIER = "0123456789abcdef0123456789abcdef"


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

    assert _uninstall_lines() == shell.commands
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
    if guard_refuses:
        assert result.stdout.startswith("STOP:")
        assert result.exit_status == 1
    else:
        assert result.stdout == "OK: no retained rollback baseline\n"

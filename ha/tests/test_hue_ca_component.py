"""hue-ca component tests against FakeShell — command sequences and parsing.

Task 5 covers the panel_ops recipes below (deploy/ensure/enable/uninstall + the
inspect probe). Task 6 extends this same file with the component registry entry
and its config-flow field.
"""

from __future__ import annotations

import pytest

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.const import (
    HUE_CA_TIMER_NAME,
    PANEL_HUE_CA_CERT_FILE,
    PANEL_HUE_CA_DIR,
    PANEL_HUE_CA_SERVICE_UNIT_FILE,
    PANEL_HUE_CA_TIMER_UNIT_FILE,
    PANEL_VAR_DIR,
)
from custom_components.brilliant_mqtt.shell import RunResult
from tests.fakes import FakeShell

_FULL_HUE_CA_INSPECT = RunResult(0, "payload=1\n", "")
_ABSENT_HUE_CA_INSPECT = RunResult(0, "payload=0\n", "")


async def _connected(shell: FakeShell) -> FakeShell:
    await shell.connect()
    return shell


async def test_inspect_hue_ca_parses_payload_present() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.HUE_CA_INSPECT_COMMAND: _FULL_HUE_CA_INSPECT})
    )
    state = await panel_ops.inspect_hue_ca(shell)
    assert state == panel_ops.HueCaState(payload_present=True)


async def test_inspect_hue_ca_parses_payload_absent() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.HUE_CA_INSPECT_COMMAND: _ABSENT_HUE_CA_INSPECT})
    )
    state = await panel_ops.inspect_hue_ca(shell)
    assert state == panel_ops.HueCaState(payload_present=False)


def test_hue_ca_inspect_command_checks_run_py_entrypoint() -> None:
    """Probe checks the actual run.py entrypoint — not just the directory."""
    assert f"{PANEL_HUE_CA_DIR}/brilliant_hue_ca/run.py" in panel_ops.HUE_CA_INSPECT_COMMAND


# The expected hue-ca swap command spelled out independently of the impl's path
# constants (so the assertion is independent of any accidental const change).
_EXPECTED_HUE_CA_SWAP = " && ".join(
    [
        f"mkdir -p {PANEL_VAR_DIR}",
        f"rm -rf {PANEL_HUE_CA_DIR}.bak",
        f"{{ [ -e {PANEL_HUE_CA_DIR} ] && mv {PANEL_HUE_CA_DIR} {PANEL_HUE_CA_DIR}.bak; true; }}",
        f"mv {PANEL_HUE_CA_DIR}.staging {PANEL_HUE_CA_DIR}",
        f"rm -rf {PANEL_HUE_CA_DIR}.bak",
    ]
)


async def test_deploy_hue_ca_uploads_tree_then_swaps_then_writes_cert() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.deploy_hue_ca(shell, "/local/hue_ca", ca_pem="PEMDATA")
    assert shell.commands[0] == f"rm -rf {PANEL_HUE_CA_DIR}.staging"
    assert shell.dir_uploads == [("/local/hue_ca", f"{PANEL_HUE_CA_DIR}.staging")]
    assert _EXPECTED_HUE_CA_SWAP in shell.commands
    # CA PEM written last, to its own path, with the exact bytes.
    assert shell.uploads[-1] == (PANEL_HUE_CA_CERT_FILE, b"PEMDATA", 0o644)


async def test_deploy_hue_ca_failed_upload_records_no_destructive_swap() -> None:
    """A failed put_dir must not trigger the swap or the CA write."""
    shell = await _connected(FakeShell(put_dir_error=OSError("transfer aborted")))
    with pytest.raises(OSError, match="transfer aborted"):
        await panel_ops.deploy_hue_ca(shell, "/local/hue_ca", ca_pem="PEMDATA")
    assert shell.commands == [f"rm -rf {PANEL_HUE_CA_DIR}.staging"]
    assert shell.dir_uploads == []
    assert shell.uploads == []


async def test_deploy_hue_ca_raises_when_swap_fails() -> None:
    shell = await _connected(
        FakeShell(responses={_EXPECTED_HUE_CA_SWAP: RunResult(1, "", "mv failed\n")})
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.deploy_hue_ca(shell, "/local/hue_ca", ca_pem="PEMDATA")
    assert shell.uploads == []  # the CA is never written if the swap fails


async def test_ensure_hue_ca_units_writes_etc_and_staged_then_reloads() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.ensure_hue_ca_units(shell, "SERVICE_CONTENT", "TIMER_CONTENT")
    # mkdir runs first, daemon-reload last.
    assert shell.commands[0] == f"mkdir -p {PANEL_HUE_CA_DIR}"
    assert shell.commands[-1] == "systemctl daemon-reload"
    # Both units written to /etc, then staged OTA-proof copies under PANEL_HUE_CA_DIR.
    assert [(path, mode) for (path, _data, mode) in shell.uploads] == [
        (PANEL_HUE_CA_SERVICE_UNIT_FILE, 0o644),
        (PANEL_HUE_CA_TIMER_UNIT_FILE, 0o644),
        (f"{PANEL_HUE_CA_DIR}/brilliant-hue-ca.service", 0o644),
        (f"{PANEL_HUE_CA_DIR}/{HUE_CA_TIMER_NAME}", 0o644),
    ]
    assert [data for (_path, data, _mode) in shell.uploads] == [
        b"SERVICE_CONTENT",
        b"TIMER_CONTENT",
        b"SERVICE_CONTENT",
        b"TIMER_CONTENT",
    ]


async def test_ensure_hue_ca_units_raises_when_mkdir_fails() -> None:
    shell = await _connected(
        FakeShell(responses={f"mkdir -p {PANEL_HUE_CA_DIR}": RunResult(1, "", "denied\n")})
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.ensure_hue_ca_units(shell, "SERVICE_CONTENT", "TIMER_CONTENT")
    assert shell.uploads == []


async def test_enable_hue_ca_enables_timer_not_service() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.enable_hue_ca(shell)
    assert shell.commands == [f"systemctl enable --now {HUE_CA_TIMER_NAME}"]


async def test_uninstall_hue_ca_sequence_and_paths() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.uninstall_hue_ca(shell)
    assert shell.commands == [
        f"systemctl disable --now {HUE_CA_TIMER_NAME} 2>/dev/null || true",
        f"rm -f {PANEL_HUE_CA_SERVICE_UNIT_FILE} {PANEL_HUE_CA_TIMER_UNIT_FILE}",
        f"rm -rf {PANEL_HUE_CA_DIR} {PANEL_HUE_CA_DIR}.staging",
        f"rm -f {PANEL_HUE_CA_CERT_FILE}",
        "systemctl daemon-reload",
    ]
    # Uninstall must never rm the bridge's PANEL_VAR_DIR itself.
    for cmd in shell.commands:
        tokens = cmd.split()
        assert PANEL_VAR_DIR not in tokens, f"Command removes PANEL_VAR_DIR itself: {cmd!r}"

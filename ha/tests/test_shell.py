"""PanelShell seam: known-hosts construction (pure) + FakeShell behavior."""

from __future__ import annotations

import pytest

from custom_components.brilliant_mqtt.shell import RunResult, known_hosts_line
from tests.fakes import FakeShell


def test_known_hosts_line_pins_host_to_key() -> None:
    line = known_hosts_line("192.168.1.10", "ssh-ed25519 AAAAC3Nza...")
    assert line == "192.168.1.10 ssh-ed25519 AAAAC3Nza...\n"


async def test_fake_shell_scripts_commands_and_records_calls() -> None:
    shell = FakeShell(responses={"echo hi": RunResult(0, "hi\n", "")})
    await shell.connect()
    result = await shell.run("echo hi")
    assert result == RunResult(0, "hi\n", "")
    # Unscripted commands succeed with empty output by default.
    assert (await shell.run("true")).exit_status == 0
    await shell.put_bytes(b"data", "/tmp/x", 0o600)
    await shell.put_dir("/local", "/remote")
    await shell.close()
    assert shell.commands == ["echo hi", "true"]
    assert shell.uploads == [("/tmp/x", b"data", 0o600)]
    assert shell.dir_uploads == [("/local", "/remote")]


async def test_fake_shell_can_simulate_connect_failure() -> None:
    shell = FakeShell(connect_error=OSError("unreachable"))
    with pytest.raises(OSError):
        await shell.connect()


def test_implementations_satisfy_protocol() -> None:
    """mypy enforces this too, but pin it at runtime for non-typed runs."""
    from custom_components.brilliant_mqtt.shell import AsyncsshShell, PanelShell

    fake: PanelShell = FakeShell()
    real: PanelShell = AsyncsshShell("h", "p")
    assert fake.pinned_host_key() == "ssh-ed25519 FAKEKEY"
    assert real.pinned_host_key() is None

"""PanelShell seam: known-hosts construction, connect() security posture, FakeShell."""

from __future__ import annotations

import asyncssh
import pytest

from custom_components.brilliant_mqtt.shell import (
    AsyncsshShell,
    RunResult,
    known_hosts_line,
)
from tests.fakes import FakeShell

# A real (throwaway, public-half-only) ed25519 key so the pinned-connect test
# exercises asyncssh's genuine known_hosts parser end-to-end.
_REAL_ED25519_PUB = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKIykuTed7zNwJwn20eCelcKcHKJ9c/pGFfvulRWazuC"
)


def test_known_hosts_line_pins_host_to_key() -> None:
    line = known_hosts_line("10.100.0.10", "ssh-ed25519 AAAAC3Nza...")
    assert line == "10.100.0.10 ssh-ed25519 AAAAC3Nza...\n"


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


async def test_fake_shell_requires_connect_before_use() -> None:
    """Mirror the real contract so consumer tests can't pass with ordering bugs."""
    shell = FakeShell()
    with pytest.raises(RuntimeError, match="not connected"):
        await shell.run("true")
    with pytest.raises(RuntimeError, match="not connected"):
        await shell.put_bytes(b"x", "/tmp/x", 0o600)
    with pytest.raises(RuntimeError, match="not connected"):
        await shell.put_dir("/a", "/b")


def test_implementations_satisfy_protocol() -> None:
    """mypy enforces this too, but pin it at runtime for non-typed runs."""
    from custom_components.brilliant_mqtt.shell import AsyncsshShell, PanelShell

    fake: PanelShell = FakeShell()
    real: PanelShell = AsyncsshShell("h", "p")
    assert fake.pinned_host_key() == "ssh-ed25519 FAKEKEY"
    assert real.pinned_host_key() is None


# --- AsyncsshShell.connect() security posture (monkeypatched asyncssh.connect) ---


class _FakeServerHostKey:
    """Stands in for the asyncssh.SSHKey returned by get_server_host_key()."""

    def __init__(self, openssh: bytes) -> None:
        self._openssh = openssh

    def export_public_key(self) -> bytes:
        return self._openssh


class _FakeConnection:
    """Minimal asyncssh.SSHClientConnection stand-in for connect() tests."""

    def __init__(self, host_key: _FakeServerHostKey | None) -> None:
        self._host_key = host_key
        self.closed = False

    def get_server_host_key(self) -> _FakeServerHostKey | None:
        return self._host_key

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


def _patch_connect(monkeypatch: pytest.MonkeyPatch, conn: _FakeConnection) -> dict[str, object]:
    """Replace asyncssh.connect with an async stub that captures its kwargs."""
    captured: dict[str, object] = {}

    async def fake_connect(host: str, **kwargs: object) -> _FakeConnection:
        captured["host"] = host
        captured.update(kwargs)
        return conn

    monkeypatch.setattr(asyncssh, "connect", fake_connect)
    return captured


async def test_first_contact_single_password_attempt_and_pin_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConnection(_FakeServerHostKey(b"ssh-ed25519 CAPTUREDKEY\n"))
    captured = _patch_connect(monkeypatch, conn)
    shell = AsyncsshShell("10.100.0.10", "pw")
    await shell.connect()
    assert captured["host"] == "10.100.0.10"
    assert captured["username"] == "root"
    assert captured["known_hosts"] is None  # TOFU: nothing to verify yet
    assert captured["client_keys"] is None  # never offer keys
    assert captured["preferred_auth"] == ("password",)  # exactly one method
    assert captured["kbdint_auth"] is False  # no kbd-interactive fallback
    assert shell.pinned_host_key() == "ssh-ed25519 CAPTUREDKEY"


async def test_pinned_connect_passes_known_hosts_with_usable_pin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConnection(_FakeServerHostKey(b"unused\n"))
    captured = _patch_connect(monkeypatch, conn)
    shell = AsyncsshShell("10.100.0.10", "pw", pinned_host_key=_REAL_ED25519_PUB)
    await shell.connect()
    known_hosts = captured["known_hosts"]
    assert known_hosts is not None
    assert isinstance(known_hosts, asyncssh.SSHKnownHosts)
    # The pin must round-trip into exactly one matchable host key
    # (addr="" so the entry matches once, by hostname only).
    host_keys = asyncssh.match_known_hosts(known_hosts, "10.100.0.10", "", None)[0]
    assert len(host_keys) == 1
    assert shell.pinned_host_key() == _REAL_ED25519_PUB


async def test_first_contact_without_host_key_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConnection(host_key=None)
    _patch_connect(monkeypatch, conn)
    shell = AsyncsshShell("10.100.0.10", "pw")
    with pytest.raises(RuntimeError, match="refusing to pin"):
        await shell.connect()
    assert conn.closed  # the unpinnable connection is not leaked
    assert shell.pinned_host_key() is None


async def test_double_connect_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConnection(_FakeServerHostKey(b"ssh-ed25519 K\n"))
    _patch_connect(monkeypatch, conn)
    shell = AsyncsshShell("10.100.0.10", "pw")
    await shell.connect()
    with pytest.raises(RuntimeError, match="already connected"):
        await shell.connect()


async def test_asyncssh_shell_run_requires_connect() -> None:
    with pytest.raises(RuntimeError, match="not connected"):
        await AsyncsshShell("10.100.0.10", "pw").run("true")

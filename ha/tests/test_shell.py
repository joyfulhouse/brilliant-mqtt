"""PanelShell seam: known-hosts construction, connect() security posture, FakeShell."""

from __future__ import annotations

import asyncio
from typing import Any, get_type_hints

import asyncssh
import pytest

from custom_components.brilliant_mqtt.shell import (
    AsyncsshShell,
    HostIdentity,
    PanelIdentityError,
    RunResult,
    _LegacyAsyncsshShell,
    async_fetch_host_identity,
    async_verify_host_identity,
    known_hosts_line,
)
from tests.fakes import FakeShell

# A real (throwaway, public-half-only) ed25519 key so the pinned-connect test
# exercises asyncssh's genuine known_hosts parser end-to-end.
_REAL_ED25519_PUB = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKIykuTed7zNwJwn20eCelcKcHKJ9c/pGFfvulRWazuC"
)
_OTHER_ED25519_PUB = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIG/koBYdTnHujqIpcXlQkQqzGBoZJ6Y4rm22iGIdAu4B"
)
_REAL_ED25519_FINGERPRINT = "SHA256:JfCon51dCgE/yWGkyroh3Ne+ONLMm6QmHMQnEoPSLx0"
_OTHER_ED25519_FINGERPRINT = "SHA256:8mIRtm2GlHfcML0pUZInHQk3nT+hlkTq4k2FGR/Y0KM"
_HOST_KEY_ALGORITHMS = ("ssh-ed25519", "rsa-sha2-512", "rsa-sha2-256")
_ED25519_HOST_KEY_ALGORITHMS = ("ssh-ed25519",)
_RSA_HOST_KEY_ALGORITHMS = ("rsa-sha2-512", "rsa-sha2-256")
_RSA_FIRST_HOST_KEY_ALGORITHMS = (*_RSA_HOST_KEY_ALGORITHMS, "ssh-ed25519")


def test_known_hosts_line_pins_host_to_key() -> None:
    line = known_hosts_line("192.168.1.10", _REAL_ED25519_PUB)
    assert line == f"192.168.1.10 {_REAL_ED25519_PUB}\n"


@pytest.mark.parametrize(
    "host",
    (
        "bad host",
        "bad\nhost",
        "\tbad",
        "",
        "@revoked",
        "bad,host",
        "bad*host",
        "bad?host",
        "bad!host",
        "bad[host",
        "bad]host",
        "bad|host",
        "bad#host",
        "bad\\host",
        "bad\x85host",
        "tést.local",
    ),
)
def test_known_hosts_line_rejects_unsafe_host(host: str) -> None:
    with pytest.raises(PanelIdentityError) as raised:
        known_hosts_line(host, _REAL_ED25519_PUB)
    assert raised.value.code == "invalid_host"


@pytest.mark.parametrize("host", ("2001:db8::1", "fe80::1%en0"))
def test_known_hosts_line_accepts_matchable_ipv6_hosts(host: str) -> None:
    known_hosts = asyncssh.import_known_hosts(known_hosts_line(host, _REAL_ED25519_PUB))

    host_keys = asyncssh.match_known_hosts(known_hosts, host, "", None)[0]

    assert [key.export_public_key().decode().strip() for key in host_keys] == [_REAL_ED25519_PUB]


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
    real: PanelShell = AsyncsshShell("h", "p", _REAL_ED25519_PUB)
    assert fake.pinned_host_key() == "ssh-ed25519 FAKEKEY"
    assert real.pinned_host_key() == _REAL_ED25519_PUB


def test_fleet_shell_requires_a_pinned_host_key() -> None:
    untyped_constructor: Any = AsyncsshShell

    with pytest.raises(ValueError, match="pinned_host_key_required"):
        untyped_constructor("192.168.1.10", "pw", None)


def test_fleet_shell_pin_is_required_by_the_typed_interface() -> None:
    assert get_type_hints(AsyncsshShell.__init__)["pinned_host_key"] is str


def test_host_identity_rejects_fingerprint_unrelated_to_public_key() -> None:
    with pytest.raises(PanelIdentityError) as raised:
        HostIdentity(_REAL_ED25519_PUB, _OTHER_ED25519_FINGERPRINT)

    assert raised.value.code == "host_key_fingerprint_invalid"
    assert _REAL_ED25519_PUB not in str(raised.value)


def test_interim_panel_first_runtime_uses_only_private_legacy_adapter() -> None:
    from custom_components.brilliant_mqtt import config_flow, manager

    assert config_flow.AsyncsshShell is _LegacyAsyncsshShell
    assert manager.AsyncsshShell is _LegacyAsyncsshShell


# --- AsyncsshShell.connect() security posture (monkeypatched asyncssh.connect) ---


class _FakeServerHostKey:
    """Stands in for the asyncssh.SSHKey returned by get_server_host_key()."""

    def __init__(
        self,
        openssh: bytes,
        *,
        fingerprint: str = _REAL_ED25519_FINGERPRINT,
        algorithm: str = "ssh-ed25519",
    ) -> None:
        self._openssh = openssh
        self._fingerprint = fingerprint
        self._algorithm = algorithm

    def export_public_key(self) -> bytes:
        return self._openssh

    def get_fingerprint(self, hash_name: str = "sha256") -> str:
        assert hash_name == "sha256"
        return self._fingerprint

    def get_algorithm(self) -> str:
        return self._algorithm


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


def _patch_host_key_fetch(
    monkeypatch: pytest.MonkeyPatch,
    key: _FakeServerHostKey | None,
) -> dict[str, object]:
    """Replace the KEX-only host-key lookup and capture its complete call."""
    captured: dict[str, object] = {}

    async def fake_get_server_host_key(
        host: str,
        port: int,
        **kwargs: object,
    ) -> _FakeServerHostKey | None:
        captured["host"] = host
        captured["port"] = port
        captured.update(kwargs)
        return key

    monkeypatch.setattr(asyncssh, "get_server_host_key", fake_get_server_host_key)
    return captured


async def test_fetch_host_identity_uses_kex_only_and_canonicalizes_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = _FakeServerHostKey(
        f"{_REAL_ED25519_PUB}\n".encode(),
        fingerprint=_REAL_ED25519_FINGERPRINT,
    )
    captured = _patch_host_key_fetch(monkeypatch, key)

    identity = await async_fetch_host_identity("panel.local", port=2222)

    assert identity == HostIdentity(
        public_key=_REAL_ED25519_PUB,
        fingerprint=_REAL_ED25519_FINGERPRINT,
    )
    assert captured == {
        "host": "panel.local",
        "port": 2222,
        "server_host_key_algs": _HOST_KEY_ALGORITHMS,
        "config": None,
    }
    assert "username" not in captured
    assert "password" not in captured


@pytest.mark.parametrize(
    ("key", "code"),
    (
        (None, "host_key_missing"),
        (
            _FakeServerHostKey(f"{_REAL_ED25519_PUB}\n{_OTHER_ED25519_PUB}\n".encode()),
            "host_key_malformed",
        ),
        (
            _FakeServerHostKey(
                b"ecdsa-sha2-nistp256 AAAA",
                algorithm="ecdsa-sha2-nistp256",
            ),
            "host_key_unsupported",
        ),
        (
            _FakeServerHostKey(
                _REAL_ED25519_PUB.encode(),
                fingerprint="MD5:not-sha256",
            ),
            "host_key_fingerprint_invalid",
        ),
    ),
)
async def test_fetch_host_identity_rejects_invalid_keys_with_stable_code(
    monkeypatch: pytest.MonkeyPatch,
    key: _FakeServerHostKey | None,
    code: str,
) -> None:
    _patch_host_key_fetch(monkeypatch, key)

    with pytest.raises(PanelIdentityError) as raised:
        await async_fetch_host_identity("panel.local")

    assert raised.value.code == code
    assert str(raised.value) == code


async def test_fetch_timeout_is_a_stable_redacted_unreachable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from custom_components.brilliant_mqtt import shell as shell_module

    async def block_until_cancelled(
        host: str,
        port: int,
        **kwargs: object,
    ) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(asyncssh, "get_server_host_key", block_until_cancelled)
    monkeypatch.setattr(shell_module, "_CONNECT_TIMEOUT", 0.01)

    with pytest.raises(PanelIdentityError) as raised:
        await async_fetch_host_identity("panel.local")

    assert raised.value.code == "host_unreachable"
    assert str(raised.value) == "host_unreachable"
    assert raised.value.__context__ is None


@pytest.mark.parametrize(
    "failure",
    (
        OSError("private network detail"),
        asyncssh.ConnectionLost("private SSH detail"),
    ),
)
async def test_fetch_network_failures_are_stable_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
) -> None:
    async def fail(
        host: str,
        port: int,
        **kwargs: object,
    ) -> None:
        raise failure

    monkeypatch.setattr(asyncssh, "get_server_host_key", fail)

    with pytest.raises(PanelIdentityError) as raised:
        await async_fetch_host_identity("panel.local")

    assert raised.value.code == "host_unreachable"
    assert str(raised.value) == "host_unreachable"
    assert str(failure) not in str(raised.value)
    assert raised.value.__context__ is None


async def test_fetch_unsupported_kex_is_a_typed_key_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail(
        host: str,
        port: int,
        **kwargs: object,
    ) -> None:
        raise asyncssh.KeyExchangeFailed("private negotiation detail")

    monkeypatch.setattr(asyncssh, "get_server_host_key", fail)

    with pytest.raises(PanelIdentityError) as raised:
        await async_fetch_host_identity("panel.local")

    assert raised.value.code == "host_key_unsupported"
    assert str(raised.value) == "host_key_unsupported"
    assert raised.value.__context__ is None


async def test_fetch_preserves_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    async def cancel(
        host: str,
        port: int,
        **kwargs: object,
    ) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncssh, "get_server_host_key", cancel)

    with pytest.raises(asyncio.CancelledError):
        await async_fetch_host_identity("panel.local")


async def test_verify_address_accepts_same_key_and_types_changed_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = HostIdentity(_REAL_ED25519_PUB, _REAL_ED25519_FINGERPRINT)
    same_key = _FakeServerHostKey(
        _REAL_ED25519_PUB.encode(),
        fingerprint=expected.fingerprint,
    )
    captured = _patch_host_key_fetch(monkeypatch, same_key)
    assert await async_verify_host_identity("new-panel-address", expected) == expected
    assert captured["server_host_key_algs"] == _HOST_KEY_ALGORITHMS

    changed_key = _FakeServerHostKey(
        _OTHER_ED25519_PUB.encode(),
        fingerprint=_OTHER_ED25519_FINGERPRINT,
    )
    _patch_host_key_fetch(monkeypatch, changed_key)
    with pytest.raises(PanelIdentityError) as raised:
        await async_verify_host_identity("new-panel-address", expected)
    assert raised.value.code == "host_key_changed"
    assert expected.public_key not in str(raised.value)
    assert changed_key.export_public_key().decode() not in str(raised.value)


async def test_verify_rsa_address_prefers_the_pinned_rsa_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rsa_key = asyncssh.generate_private_key("ssh-rsa", key_size=2048)
    expected = HostIdentity(
        rsa_key.export_public_key().decode().strip(),
        rsa_key.get_fingerprint("sha256"),
    )
    captured: dict[str, object] = {}

    async def fake_get_server_host_key(
        host: str,
        port: int,
        **kwargs: object,
    ) -> asyncssh.SSHKey:
        captured.update(kwargs)
        return rsa_key

    monkeypatch.setattr(asyncssh, "get_server_host_key", fake_get_server_host_key)

    assert await async_verify_host_identity("new-panel-address", expected) == expected
    assert captured["server_host_key_algs"] == _RSA_FIRST_HOST_KEY_ALGORITHMS
    assert "ssh-rsa" not in captured["server_host_key_algs"]


async def test_verify_rsa_address_with_ed25519_replacement_is_host_key_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rsa_key = asyncssh.generate_private_key("ssh-rsa", key_size=2048)
    expected = HostIdentity(
        rsa_key.export_public_key().decode().strip(),
        rsa_key.get_fingerprint("sha256"),
    )

    async def return_ed25519_replacement(
        host: str,
        port: int,
        **kwargs: object,
    ) -> _FakeServerHostKey:
        assert kwargs["server_host_key_algs"] == _RSA_FIRST_HOST_KEY_ALGORITHMS
        return _FakeServerHostKey(
            _OTHER_ED25519_PUB.encode(),
            fingerprint=_OTHER_ED25519_FINGERPRINT,
        )

    monkeypatch.setattr(asyncssh, "get_server_host_key", return_ed25519_replacement)

    with pytest.raises(PanelIdentityError) as raised:
        await async_verify_host_identity("new-panel-address", expected)

    assert raised.value.code == "host_key_changed"
    assert str(raised.value) == "host_key_changed"


async def test_verify_missing_pinned_key_family_is_host_key_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rsa_key = asyncssh.generate_private_key("ssh-rsa", key_size=2048)
    expected = HostIdentity(
        rsa_key.export_public_key().decode().strip(),
        rsa_key.get_fingerprint("sha256"),
    )

    async def reject_rsa_family(
        host: str,
        port: int,
        **kwargs: object,
    ) -> None:
        assert kwargs["server_host_key_algs"] == _RSA_FIRST_HOST_KEY_ALGORITHMS
        raise asyncssh.KeyExchangeFailed("no RSA host key")

    monkeypatch.setattr(asyncssh, "get_server_host_key", reject_rsa_family)

    with pytest.raises(PanelIdentityError) as raised:
        await async_verify_host_identity("new-panel-address", expected)

    assert raised.value.code == "host_key_changed"
    assert str(raised.value) == "host_key_changed"
    assert raised.value.__context__ is None


async def test_fetch_accepts_rsa_key_with_sha2_only_negotiation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rsa_key = asyncssh.generate_private_key("ssh-rsa", key_size=2048)
    captured: dict[str, object] = {}

    async def fake_get_server_host_key(
        host: str,
        port: int,
        **kwargs: object,
    ) -> asyncssh.SSHKey:
        captured.update(kwargs)
        return rsa_key

    monkeypatch.setattr(asyncssh, "get_server_host_key", fake_get_server_host_key)

    identity = await async_fetch_host_identity("panel.local")

    assert identity.public_key.startswith("ssh-rsa ")
    assert identity.fingerprint == rsa_key.get_fingerprint("sha256")
    assert captured["server_host_key_algs"] == _HOST_KEY_ALGORITHMS
    assert "ssh-rsa" not in captured["server_host_key_algs"]


async def test_first_contact_single_password_attempt_and_pin_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConnection(_FakeServerHostKey(b"ssh-ed25519 CAPTUREDKEY\n"))
    captured = _patch_connect(monkeypatch, conn)
    shell = _LegacyAsyncsshShell("192.168.1.10", "pw")
    await shell.connect()
    assert captured["host"] == "192.168.1.10"
    assert captured["username"] == "root"
    assert captured["known_hosts"] is None  # TOFU: nothing to verify yet
    assert captured["client_keys"] is None  # never offer keys
    assert captured["preferred_auth"] == ("password",)  # exactly one method
    assert captured["kbdint_auth"] is False  # no kbd-interactive fallback
    assert captured["config"] is None
    assert captured["gss_kex"] is False
    assert captured["gss_auth"] is False
    assert captured["disable_trivial_auth"] is True
    assert captured["public_key_auth"] is False
    assert captured["host_based_auth"] is False
    assert shell.pinned_host_key() == "ssh-ed25519 CAPTUREDKEY"


async def test_legacy_shell_accepts_a_preexisting_ecdsa_pin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy_key = asyncssh.generate_private_key("ecdsa-sha2-nistp256")
    public_key = legacy_key.export_public_key().decode().strip()
    conn = _FakeConnection(None)
    captured = _patch_connect(monkeypatch, conn)

    await _LegacyAsyncsshShell("192.168.1.10", "pw", public_key).connect()

    known_hosts = captured["known_hosts"]
    assert isinstance(known_hosts, asyncssh.SSHKnownHosts)
    host_keys = asyncssh.match_known_hosts(known_hosts, "192.168.1.10", "", None)[0]
    assert [key.export_public_key().decode().strip() for key in host_keys] == [public_key]
    assert "server_host_key_algs" not in captured


async def test_pinned_connect_passes_known_hosts_with_usable_pin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _FakeServerHostKey(_REAL_ED25519_PUB.encode())
    conn = _FakeConnection(candidate)
    captured = _patch_connect(monkeypatch, conn)
    shell = AsyncsshShell("192.168.1.10", "pw", pinned_host_key=_REAL_ED25519_PUB)
    await shell.connect()
    known_hosts = captured["known_hosts"]
    assert known_hosts is not None
    assert isinstance(known_hosts, asyncssh.SSHKnownHosts)
    # The pin must round-trip into exactly one matchable host key
    # (addr="" so the entry matches once, by hostname only).
    host_keys = asyncssh.match_known_hosts(known_hosts, "192.168.1.10", "", None)[0]
    assert len(host_keys) == 1
    assert host_keys[0].export_public_key().decode().strip() == _REAL_ED25519_PUB
    assert captured["server_host_key_algs"] == _ED25519_HOST_KEY_ALGORITHMS
    assert captured["config"] is None
    assert captured["gss_kex"] is False
    assert captured["gss_auth"] is False
    assert captured["disable_trivial_auth"] is True
    assert captured["public_key_auth"] is False
    assert captured["host_based_auth"] is False
    assert shell.pinned_host_key() == _REAL_ED25519_PUB


async def test_pinned_rsa_connect_uses_only_sha2_for_the_exact_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rsa_key = asyncssh.generate_private_key("ssh-rsa", key_size=2048)
    public_key = rsa_key.export_public_key().decode().strip()
    captured = _patch_connect(monkeypatch, _FakeConnection(None))

    await AsyncsshShell("192.168.1.10", "pw", public_key).connect()

    known_hosts = captured["known_hosts"]
    assert isinstance(known_hosts, asyncssh.SSHKnownHosts)
    host_keys = asyncssh.match_known_hosts(known_hosts, "192.168.1.10", "", None)[0]
    assert [key.export_public_key().decode().strip() for key in host_keys] == [public_key]
    assert captured["server_host_key_algs"] == _RSA_HOST_KEY_ALGORITHMS
    assert "ssh-rsa" not in captured["server_host_key_algs"]


async def test_pinned_handshake_mismatch_never_reaches_password_authentication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    password_authentication_started = False

    async def fake_connect(host: str, **kwargs: object) -> _FakeConnection:
        nonlocal password_authentication_started
        known_hosts = kwargs["known_hosts"]
        assert isinstance(known_hosts, asyncssh.SSHKnownHosts)
        # Model asyncssh's real ordering: known-host verification rejects the
        # KEX before its password-auth code begins.
        if known_hosts:
            raise asyncssh.HostKeyNotVerifiable("host key changed")
        password_authentication_started = True
        return _FakeConnection(None)

    monkeypatch.setattr(asyncssh, "connect", fake_connect)
    shell = AsyncsshShell("192.168.1.10", "super-secret", _REAL_ED25519_PUB)

    with pytest.raises(asyncssh.HostKeyNotVerifiable):
        await shell.connect()

    assert password_authentication_started is False


async def test_fetch_completes_before_password_bearing_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    candidate = _FakeServerHostKey(_REAL_ED25519_PUB.encode())

    async def fake_get_server_host_key(
        host: str,
        port: int,
        **kwargs: object,
    ) -> _FakeServerHostKey:
        events.append("identity")
        return candidate

    async def fake_connect(host: str, **kwargs: object) -> _FakeConnection:
        assert kwargs["password"] == "pw"
        events.append("password")
        return _FakeConnection(candidate)

    monkeypatch.setattr(asyncssh, "get_server_host_key", fake_get_server_host_key)
    monkeypatch.setattr(asyncssh, "connect", fake_connect)

    identity = await async_fetch_host_identity("panel.local")
    await AsyncsshShell("panel.local", "pw", identity.public_key).connect()

    assert events == ["identity", "password"]


async def test_first_contact_without_host_key_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConnection(host_key=None)
    _patch_connect(monkeypatch, conn)
    shell = _LegacyAsyncsshShell("192.168.1.10", "pw")
    with pytest.raises(RuntimeError, match="refusing to pin"):
        await shell.connect()
    assert conn.closed  # the unpinnable connection is not leaked
    assert shell.pinned_host_key() is None


async def test_double_connect_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConnection(_FakeServerHostKey(b"ssh-ed25519 K\n"))
    _patch_connect(monkeypatch, conn)
    shell = _LegacyAsyncsshShell("192.168.1.10", "pw")
    await shell.connect()
    with pytest.raises(RuntimeError, match="already connected"):
        await shell.connect()


async def test_asyncssh_shell_run_requires_connect() -> None:
    with pytest.raises(RuntimeError, match="not connected"):
        await AsyncsshShell("192.168.1.10", "pw", _REAL_ED25519_PUB).run("true")

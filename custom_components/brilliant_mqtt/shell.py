"""SSH transport seam for panel operations.

PanelShell is the Protocol that panel_ops and the manager depend on;
AsyncsshShell is the real adapter. Tests use tests.fakes.FakeShell — no test
ever opens a real connection (the adapter is live-validated at rollout, the
same philosophy as the agent's bus/mqtt adapters).

Host-key policy is TOFU: the first successful connect captures the server host
key; later connects verify it BEFORE authenticating, so the per-panel root
password is never offered to an impostor host.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import asyncssh
from asyncssh import SFTPAttrs

_CONNECT_TIMEOUT = 15
_LOGIN_TIMEOUT = 15


@dataclass(frozen=True)
class RunResult:
    exit_status: int
    stdout: str
    stderr: str


def known_hosts_line(host: str, public_key: str) -> str:
    """One OpenSSH known_hosts line pinning *host* to *public_key*."""
    return f"{host} {public_key}\n"


class PanelShell(Protocol):
    """One SSH session to one panel. connect() -> run()/put_*() -> close()."""

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def run(self, command: str) -> RunResult: ...
    async def put_bytes(self, data: bytes, remote_path: str, mode: int) -> None: ...
    async def put_dir(self, local_dir: str, remote_dir: str) -> None: ...
    async def put_file(self, local_path: str, remote_path: str, mode: int) -> None: ...
    def pinned_host_key(self) -> str | None: ...


class AsyncsshShell:
    """asyncssh-backed PanelShell — password-only auth, single attempt, TOFU pinning."""

    def __init__(self, host: str, password: str, pinned_host_key: str | None = None) -> None:
        self._host = host
        self._password = password
        self._pinned = pinned_host_key
        self._conn: asyncssh.SSHClientConnection | None = None

    def pinned_host_key(self) -> str | None:
        return self._pinned

    async def connect(self) -> None:
        if self._conn is not None:
            # The manager constructs a fresh shell per operation; a second
            # connect() here is a bug and would leak the prior connection.
            raise RuntimeError("already connected")
        known_hosts: asyncssh.SSHKnownHosts | None = (
            asyncssh.import_known_hosts(known_hosts_line(self._host, self._pinned))
            if self._pinned is not None
            else None  # first contact: trust-on-first-use, key captured below
        )
        # asyncssh's defaults allow the password-over-keyboard-interactive
        # fallback, so one connect() could burn TWO credentialed attempts
        # against panels that lock out — preferred_auth restricts auth to
        # exactly one method; kbdint_auth=False is the explicit belt-and-braces.
        conn = await asyncssh.connect(
            self._host,
            username="root",
            password=self._password,
            known_hosts=known_hosts,
            client_keys=None,  # password only — never offer keys (panel lockout caution)
            preferred_auth=("password",),
            kbdint_auth=False,
            connect_timeout=_CONNECT_TIMEOUT,
            login_timeout=_LOGIN_TIMEOUT,
        )
        if self._pinned is None:
            key = conn.get_server_host_key()
            if key is None:
                # Fail closed: an unpinned shell must never stay usable, or
                # every future connect would offer the password unverified.
                # Close first so the connection isn't leaked.
                conn.close()
                await conn.wait_closed()
                raise RuntimeError(f"no server host key captured for {self._host}; refusing to pin")
            # export_public_key() returns e.g. b"ssh-ed25519 AAAA...\n"
            # Strip trailing whitespace to get a clean single-line token.
            self._pinned = key.export_public_key().decode().strip()
        self._conn = conn

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            await self._conn.wait_closed()
            self._conn = None

    def _require_conn(self) -> asyncssh.SSHClientConnection:
        # Intentional contract, not a debug check — must hold under -O too.
        if self._conn is None:
            raise RuntimeError("not connected — call connect() first")
        return self._conn

    async def run(self, command: str) -> RunResult:
        conn = self._require_conn()
        result = await conn.run(command, check=False)
        return RunResult(
            exit_status=result.exit_status or 0,
            stdout=str(result.stdout or ""),
            stderr=str(result.stderr or ""),
        )

    async def put_bytes(self, data: bytes, remote_path: str, mode: int) -> None:
        conn = self._require_conn()
        # asyncssh.SFTPClient is itself an async context manager (not start_sftp_client)
        async with await conn.start_sftp_client() as sftp:
            # The secret must never sit with wrong permissions: attrs sets the
            # mode on fresh creates, but asyncssh ignores attrs when the file
            # already exists ("wb" truncates, keeps old perms) — so also chmod
            # the open (truncated-empty) handle BEFORE the data lands.
            async with await sftp.open(remote_path, "wb", attrs=SFTPAttrs(permissions=mode)) as f:
                await f.chmod(mode)  # converge pre-existing files; no-op on fresh creates
                await f.write(data)

    async def put_dir(self, local_dir: str, remote_dir: str) -> None:
        conn = self._require_conn()
        async with await conn.start_sftp_client() as sftp:
            await sftp.put(local_dir, remote_dir, recurse=True)

    async def put_file(self, local_path: str, remote_path: str, mode: int) -> None:
        conn = self._require_conn()
        async with await conn.start_sftp_client() as sftp:
            await sftp.put(local_path, remote_path)
            await sftp.chmod(remote_path, mode)

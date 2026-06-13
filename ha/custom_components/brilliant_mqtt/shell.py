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
        known_hosts: asyncssh.SSHKnownHosts | None = (
            asyncssh.import_known_hosts(known_hosts_line(self._host, self._pinned))
            if self._pinned is not None
            else None  # first contact: trust-on-first-use, key captured below
        )
        self._conn = await asyncssh.connect(
            self._host,
            username="root",
            password=self._password,
            known_hosts=known_hosts,
            client_keys=None,  # password only — never offer keys (panel lockout caution)
            connect_timeout=_CONNECT_TIMEOUT,
            login_timeout=_LOGIN_TIMEOUT,
        )
        if self._pinned is None:
            key = self._conn.get_server_host_key()
            if key is not None:
                # export_public_key() returns e.g. b"ssh-ed25519 AAAA...\n"
                # Strip trailing whitespace to get a clean single-line token.
                self._pinned = key.export_public_key().decode().strip()

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            await self._conn.wait_closed()
            self._conn = None

    async def run(self, command: str) -> RunResult:
        assert self._conn is not None, "connect() first"
        result = await self._conn.run(command, check=False)
        return RunResult(
            exit_status=result.exit_status or 0,
            stdout=str(result.stdout or ""),
            stderr=str(result.stderr or ""),
        )

    async def put_bytes(self, data: bytes, remote_path: str, mode: int) -> None:
        assert self._conn is not None, "connect() first"
        # asyncssh.SFTPClient is itself an async context manager (not start_sftp_client)
        async with await self._conn.start_sftp_client() as sftp:
            async with await sftp.open(remote_path, "wb") as f:
                await f.write(data)
            await sftp.chmod(remote_path, mode)

    async def put_dir(self, local_dir: str, remote_dir: str) -> None:
        assert self._conn is not None, "connect() first"
        async with await self._conn.start_sftp_client() as sftp:
            await sftp.put(local_dir, remote_dir, recurse=True)

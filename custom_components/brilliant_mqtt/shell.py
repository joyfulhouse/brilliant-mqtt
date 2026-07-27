"""SSH transport and verify-before-password panel identity seams.

``AsyncsshShell`` is the fleet adapter: it always requires a host-key pin and
passes that pin to AsyncSSH for verification during KEX, before password
authentication. The interim panel-first runtime uses the private legacy adapter
at the bottom of this module until its entries are consolidated.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol, cast

import asyncssh
from asyncssh import SFTPAttrs

_CONNECT_TIMEOUT = 15
_LOGIN_TIMEOUT = 15
PANEL_PROCESS_OUTPUT_LIMIT = 32 * 1024
_PROCESS_READ_SIZE = 4096
_ED25519_HOST_KEY_ALGORITHMS = ("ssh-ed25519",)
_RSA_HOST_KEY_ALGORITHMS = ("rsa-sha2-512", "rsa-sha2-256")
_HOST_KEY_ALGORITHMS = (*_ED25519_HOST_KEY_ALGORITHMS, *_RSA_HOST_KEY_ALGORITHMS)
_SUPPORTED_PUBLIC_KEY_ALGORITHMS = frozenset({"ssh-ed25519", "ssh-rsa"})
_PROCESS_ERROR_CODES = frozenset(
    {
        "process_output_invalid",
        "process_output_too_large",
        "process_start_failed",
        "process_wait_failed",
    }
)
_IDENTITY_ERROR_CODES = frozenset(
    {
        "host_key_missing",
        "host_key_malformed",
        "host_key_unsupported",
        "host_key_fingerprint_invalid",
        "host_key_changed",
        "invalid_host",
        "invalid_port",
        "pinned_host_key_required",
        "host_unreachable",
    }
)


class PanelIdentityError(ValueError):
    """Redacted SSH identity failure with one stable onboarding code."""

    __slots__ = ("code",)

    def __init__(self, code: str) -> None:
        if code not in _IDENTITY_ERROR_CODES:
            raise ValueError("invalid_panel_identity_error_code")
        self.code = code
        super().__init__(code)


class PanelProcessError(RuntimeError):
    """Stable process-control failure which never retains command or output."""

    __slots__ = ("code",)

    def __init__(self, code: str) -> None:
        if code not in _PROCESS_ERROR_CODES:
            raise ValueError("invalid_panel_process_error_code")
        self.code = code
        super().__init__(code)
        self.__suppress_context__ = True


@dataclass(frozen=True, slots=True)
class HostIdentity:
    """Canonical, address-independent public identity for one panel."""

    public_key: str
    fingerprint: str

    def __post_init__(self) -> None:
        canonical = _canonical_public_key(self.public_key)
        try:
            computed = asyncssh.import_public_key(canonical).get_fingerprint("sha256")
        except Exception:
            raise PanelIdentityError("host_key_fingerprint_invalid") from None
        if not isinstance(self.fingerprint, str) or self.fingerprint != computed:
            raise PanelIdentityError("host_key_fingerprint_invalid")
        object.__setattr__(self, "public_key", canonical)


@dataclass(frozen=True)
class RunResult:
    exit_status: int
    stdout: str
    stderr: str


def _validate_host(host: str) -> str:
    """Reject host text which could alter OpenSSH known-hosts semantics."""
    if (
        not isinstance(host, str)
        or not host
        or len(host) > 253
        or any(not char.isascii() or not char.isprintable() or char.isspace() for char in host)
        or any(char in ",*?![]|#@\\" for char in host)
    ):
        raise PanelIdentityError("invalid_host")
    return host


def _validate_port(port: int) -> int:
    if type(port) is not int or not 1 <= port <= 65535:
        raise PanelIdentityError("invalid_port")
    return port


def _canonical_public_key(
    public_key: str,
    *,
    algorithm: str | None = None,
) -> str:
    """Validate one exact, comment-free OpenSSH public-key line."""
    if not isinstance(public_key, str):
        raise PanelIdentityError("host_key_malformed")
    canonical = public_key.strip()
    fields = canonical.split()
    if (
        len(fields) != 2
        or canonical != f"{fields[0]} {fields[1]}"
        or "\n" in canonical
        or "\r" in canonical
    ):
        raise PanelIdentityError("host_key_malformed")
    if fields[0] not in _SUPPORTED_PUBLIC_KEY_ALGORITHMS or (
        algorithm is not None and algorithm not in _SUPPORTED_PUBLIC_KEY_ALGORITHMS
    ):
        raise PanelIdentityError("host_key_unsupported")
    try:
        imported = asyncssh.import_public_key(canonical)
        imported_algorithm = imported.get_algorithm()
        round_trip = imported.export_public_key().decode().strip()
    except Exception:
        raise PanelIdentityError("host_key_malformed") from None
    if (
        imported_algorithm not in _SUPPORTED_PUBLIC_KEY_ALGORITHMS
        or round_trip != canonical
        or imported_algorithm != fields[0]
    ):
        raise PanelIdentityError("host_key_malformed")
    return canonical


def known_hosts_line(host: str, public_key: str) -> str:
    """One OpenSSH known_hosts line pinning *host* to *public_key*."""
    return f"{_validate_host(host)} {_canonical_public_key(public_key)}\n"


def _pinned_host_key_algorithms(public_key: str) -> tuple[str, ...]:
    """Return only SHA-2-safe algorithms backed by one canonical key pin."""
    algorithm = _canonical_public_key(public_key).partition(" ")[0]
    if algorithm == "ssh-ed25519":
        return _ED25519_HOST_KEY_ALGORITHMS
    return _RSA_HOST_KEY_ALGORITHMS


def _verification_host_key_algorithms(public_key: str) -> tuple[str, ...]:
    """Prefer the pinned family while still detecting safe-family replacement."""
    pinned_algorithms = _pinned_host_key_algorithms(public_key)
    if pinned_algorithms == _ED25519_HOST_KEY_ALGORITHMS:
        return _HOST_KEY_ALGORITHMS
    return (*_RSA_HOST_KEY_ALGORITHMS, *_ED25519_HOST_KEY_ALGORITHMS)


def _legacy_known_hosts_line(host: str, public_key: str) -> str:
    """Validate old persisted pins without imposing the new fleet allowlist."""
    if not isinstance(public_key, str):
        raise PanelIdentityError("host_key_malformed")
    canonical = public_key.strip()
    fields = canonical.split()
    if (
        len(fields) != 2
        or canonical != f"{fields[0]} {fields[1]}"
        or "\n" in canonical
        or "\r" in canonical
    ):
        raise PanelIdentityError("host_key_malformed")
    try:
        key = asyncssh.import_public_key(canonical)
        round_trip = key.export_public_key().decode().strip()
    except Exception:
        raise PanelIdentityError("host_key_malformed") from None
    if round_trip != canonical:
        raise PanelIdentityError("host_key_malformed")
    return f"{_validate_host(host)} {canonical}\n"


async def _async_fetch_host_identity(
    host: str,
    port: int,
    server_host_key_algs: tuple[str, ...],
    key_exchange_error_code: str,
) -> HostIdentity:
    """Fetch one identity while preserving unsupported-KEX classification."""
    safe_host = _validate_host(host)
    safe_port = _validate_port(port)
    key: asyncssh.SSHKey | None = None
    failure_code: str | None = None
    try:
        async with asyncio.timeout(_CONNECT_TIMEOUT):
            key = await asyncssh.get_server_host_key(
                safe_host,
                port=safe_port,
                server_host_key_algs=server_host_key_algs,
                config=None,
            )
    except asyncssh.KeyExchangeFailed:
        failure_code = key_exchange_error_code
    except (TimeoutError, OSError, asyncssh.Error):
        failure_code = "host_unreachable"
    if failure_code is not None:
        raise PanelIdentityError(failure_code)
    if key is None:
        raise PanelIdentityError("host_key_missing")
    try:
        algorithm = key.get_algorithm()
        public_key = key.export_public_key().decode().strip()
    except Exception:
        raise PanelIdentityError("host_key_malformed") from None
    canonical = _canonical_public_key(public_key, algorithm=algorithm)
    try:
        fingerprint = key.get_fingerprint("sha256")
    except Exception:
        raise PanelIdentityError("host_key_fingerprint_invalid") from None
    if (
        not isinstance(fingerprint, str)
        or not fingerprint.startswith("SHA256:")
        or not fingerprint.removeprefix("SHA256:")
        or any(char.isspace() or ord(char) < 33 for char in fingerprint)
    ):
        raise PanelIdentityError("host_key_fingerprint_invalid")
    return HostIdentity(public_key=canonical, fingerprint=fingerprint)


async def async_fetch_host_identity(host: str, port: int = 22) -> HostIdentity:
    """Fetch a panel key through a bounded unauthenticated SSH key exchange."""
    return await _async_fetch_host_identity(
        host,
        port,
        _HOST_KEY_ALGORITHMS,
        "host_key_unsupported",
    )


async def async_verify_host_identity(
    host: str,
    expected: HostIdentity,
    port: int = 22,
) -> HostIdentity:
    """Verify that a new address still exposes an existing panel's exact key."""
    if not isinstance(expected, HostIdentity):
        raise TypeError("expected must be a HostIdentity")
    candidate = await _async_fetch_host_identity(
        host,
        port,
        _verification_host_key_algorithms(expected.public_key),
        "host_key_changed",
    )
    if candidate.public_key != expected.public_key or candidate.fingerprint != expected.fingerprint:
        raise PanelIdentityError("host_key_changed")
    return candidate


class PanelShell(Protocol):
    """One SSH session to one panel. connect() -> run()/put_*() -> close()."""

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def run(self, command: str) -> RunResult: ...
    async def start(self, command: str) -> PanelProcess: ...
    async def put_bytes(self, data: bytes, remote_path: str, mode: int) -> None: ...
    async def put_dir(self, local_dir: str, remote_dir: str) -> None: ...
    async def put_file(self, local_path: str, remote_path: str, mode: int) -> None: ...
    def pinned_host_key(self) -> str | None: ...


class PanelProcess(Protocol):
    """One bounded remote child with explicit termination and settlement."""

    @property
    def running(self) -> bool: ...

    def terminate(self) -> None: ...

    async def wait(self) -> RunResult: ...


class _BinaryReader(Protocol):
    async def read(self, size: int = -1) -> bytes: ...


class _RawProcess(Protocol):
    stdout: _BinaryReader
    stderr: _BinaryReader

    @property
    def exit_status(self) -> int | None: ...

    @property
    def returncode(self) -> int | None: ...

    def terminate(self) -> None: ...

    def close(self) -> None: ...

    async def wait_closed(self) -> None: ...


class _AsyncsshPanelProcess:
    """Drain one AsyncSSH child under a fixed combined memory ceiling."""

    __slots__ = (
        "_output_size",
        "_overflowed",
        "_raw",
        "_settlement",
        "_stderr",
        "_stdout",
        "_terminated",
    )

    def __init__(self, raw: _RawProcess) -> None:
        self._raw = raw
        self._stdout: list[bytes] = []
        self._stderr: list[bytes] = []
        self._output_size = 0
        self._overflowed = False
        self._terminated = False
        self._settlement = asyncio.create_task(
            self._async_settle(),
            name="brilliant-mqtt-panel-process",
        )

    @property
    def running(self) -> bool:
        return not self._settlement.done()

    def terminate(self) -> None:
        """Request termination once and force the SSH channel toward EOF."""
        if self._terminated or self._settlement.done():
            return
        self._terminated = True
        try:
            self._raw.terminate()
        except Exception:
            pass
        try:
            self._raw.close()
        except Exception:
            pass

    async def wait(self) -> RunResult:
        """Await settlement without letting caller cancellation destroy it."""
        return await asyncio.shield(self._settlement)

    async def _async_read(self, reader: _BinaryReader, output: list[bytes]) -> None:
        while chunk := await reader.read(_PROCESS_READ_SIZE):
            if not isinstance(chunk, bytes):
                raise TypeError
            remaining = PANEL_PROCESS_OUTPUT_LIMIT - self._output_size
            if len(chunk) > remaining:
                self._overflowed = True
                self.terminate()
                continue
            self._output_size += len(chunk)
            output.append(chunk)

    async def _async_settle(self) -> RunResult:
        tasks = (
            asyncio.create_task(
                self._async_read(self._raw.stdout, self._stdout),
                name="brilliant-mqtt-panel-stdout",
            ),
            asyncio.create_task(
                self._async_read(self._raw.stderr, self._stderr),
                name="brilliant-mqtt-panel-stderr",
            ),
            asyncio.create_task(
                self._raw.wait_closed(),
                name="brilliant-mqtt-panel-wait-closed",
            ),
        )
        try:
            done, _pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_EXCEPTION,
            )
            if any(task.cancelled() or task.exception() is not None for task in done):
                self.terminate()
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            self.terminate()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        if self._overflowed:
            raise PanelProcessError("process_output_too_large")
        if any(isinstance(result, BaseException) for result in results):
            raise PanelProcessError("process_wait_failed")

        stdout: str | None = None
        stderr: str | None = None
        try:
            stdout = b"".join(self._stdout).decode("utf-8")
            stderr = b"".join(self._stderr).decode("utf-8")
        except UnicodeDecodeError:
            pass
        if stdout is None or stderr is None:
            raise PanelProcessError("process_output_invalid")

        exit_status = self._raw.exit_status
        if exit_status is None:
            exit_status = self._raw.returncode
        if exit_status is None:
            raise PanelProcessError("process_wait_failed")
        return RunResult(exit_status=exit_status, stdout=stdout, stderr=stderr)


class _ShellOperations:
    """Connected AsyncSSH operations shared by fleet and legacy adapters."""

    def __init__(self, host: str, password: str, pinned_host_key: str | None) -> None:
        self._host = host
        self._password = password
        self._pinned = pinned_host_key
        self._conn: asyncssh.SSHClientConnection | None = None
        self._children: set[_AsyncsshPanelProcess] = set()

    def pinned_host_key(self) -> str | None:
        return self._pinned

    async def close(self) -> None:
        conn = self._conn
        if conn is None:
            return
        self._conn = None
        children = tuple(self._children)
        cleanup = asyncio.create_task(
            self._async_close_connection(conn, children),
            name="brilliant-mqtt-shell-close",
        )
        cancellation: asyncio.CancelledError | None = None
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
        self._children.clear()
        if cancellation is not None:
            raise cancellation from None
        cleanup.result()

    @staticmethod
    async def _async_close_connection(
        conn: asyncssh.SSHClientConnection,
        children: tuple[_AsyncsshPanelProcess, ...],
    ) -> None:
        for child in children:
            child.terminate()
        conn.close()
        await asyncio.gather(*(child.wait() for child in children), return_exceptions=True)
        await conn.wait_closed()

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

    async def start(self, command: str) -> PanelProcess:
        """Start a bounded binary-output child without retaining its command."""
        conn = self._require_conn()
        raw: _RawProcess | None = None
        failed = False
        try:
            created = await conn.create_process(
                command,
                encoding=None,
                stdin=asyncssh.DEVNULL,
                stdout=asyncssh.PIPE,
                stderr=asyncssh.PIPE,
            )
            raw = cast(_RawProcess, created)
        except asyncio.CancelledError:
            raise
        except Exception:
            failed = True
        if failed or raw is None:
            raise PanelProcessError("process_start_failed")

        process = _AsyncsshPanelProcess(raw)
        self._children.add(process)
        return process

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


class AsyncsshShell(_ShellOperations):
    """Fleet SSH adapter requiring an exact key pin before password auth."""

    def __init__(self, host: str, password: str, pinned_host_key: str) -> None:
        if pinned_host_key is None:
            raise PanelIdentityError("pinned_host_key_required")
        safe_host = _validate_host(host)
        canonical_key = _canonical_public_key(pinned_host_key)
        super().__init__(safe_host, password, canonical_key)

    async def connect(self) -> None:
        """Connect once with deterministic key and password authentication."""
        if self._conn is not None:
            raise RuntimeError("already connected")
        if self._pinned is None:
            raise RuntimeError("fleet shell lost its required host-key pin")
        known_hosts = asyncssh.import_known_hosts(known_hosts_line(self._host, self._pinned))
        self._conn = await asyncssh.connect(
            self._host,
            username="root",
            password=self._password,
            known_hosts=known_hosts,
            server_host_key_algs=_pinned_host_key_algorithms(self._pinned),
            config=None,
            gss_kex=False,
            gss_auth=False,
            disable_trivial_auth=True,
            public_key_auth=False,
            host_based_auth=False,
            client_keys=None,
            preferred_auth=("password",),
            kbdint_auth=False,
            connect_timeout=_CONNECT_TIMEOUT,
            login_timeout=_LOGIN_TIMEOUT,
        )


class _LegacyAsyncsshShell(_ShellOperations):
    """Interim panel-first TOFU adapter; fleet code must never import this."""

    def __init__(self, host: str, password: str, pinned_host_key: str | None = None) -> None:
        super().__init__(host, password, pinned_host_key)

    async def connect(self) -> None:
        """Reproduce the version-3 TOFU behavior until plan-3 consolidation."""
        if self._conn is not None:
            raise RuntimeError("already connected")
        known_hosts: asyncssh.SSHKnownHosts | None = (
            asyncssh.import_known_hosts(_legacy_known_hosts_line(self._host, self._pinned))
            if self._pinned is not None
            else None
        )
        conn = await asyncssh.connect(
            self._host,
            username="root",
            password=self._password,
            known_hosts=known_hosts,
            config=None,
            gss_kex=False,
            gss_auth=False,
            disable_trivial_auth=True,
            public_key_auth=False,
            host_based_auth=False,
            client_keys=None,
            preferred_auth=("password",),
            kbdint_auth=False,
            connect_timeout=_CONNECT_TIMEOUT,
            login_timeout=_LOGIN_TIMEOUT,
        )
        if self._pinned is None:
            key = conn.get_server_host_key()
            if key is None:
                conn.close()
                await conn.wait_closed()
                raise RuntimeError(f"no server host key captured for {self._host}; refusing to pin")
            self._pinned = key.export_public_key().decode().strip()
        self._conn = conn

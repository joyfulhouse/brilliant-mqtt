"""Fake PanelShell for unit tests — scripted responses, recorded calls."""

from __future__ import annotations

import asyncio

from custom_components.brilliant_mqtt.shell import RunResult

_OK = RunResult(0, "", "")
_DEFAULT_INACTIVE_BLE_PROBE = "systemctl is-active brilliant-ble-observer 2>/dev/null"
_INACTIVE = RunResult(3, "inactive\n", "")


class FakeShell:
    """Satisfies the PanelShell Protocol. Unscripted commands return success."""

    def __init__(
        self,
        responses: dict[str, RunResult] | None = None,
        connect_error: Exception | None = None,
        put_dir_error: Exception | None = None,
        connect_gate: asyncio.Event | None = None,
        pinned: str | None = "ssh-ed25519 FAKEKEY",
        run_errors: dict[str, Exception] | None = None,
    ) -> None:
        self.responses = dict(responses or {})
        self.connect_error = connect_error
        self.put_dir_error = put_dir_error
        # Commands whose run() raises the mapped exception (models a mid-command
        # transport drop — e.g. the reboot disconnect, or a dead diagnostics probe).
        self.run_errors = dict(run_errors or {})
        # When set, connect() blocks on this event — lets a test wedge a repair
        # inside the ssh_lock to exercise the shutdown-mid-repair interleaving.
        self.connect_gate = connect_gate
        # Set the instant connect() is entered (before it blocks on the gate) so a
        # test can deterministically await "the repair is now inside connect()"
        # rather than busy-waiting on a flag.
        self.connect_entered = asyncio.Event()
        self._pinned = pinned
        self.connected = False
        self.connect_count = 0  # how many times connect() was entered (gate/error or not)
        self.commands: list[str] = []
        self.uploads: list[tuple[str, bytes, int]] = []
        self.dir_uploads: list[tuple[str, str]] = []
        self.file_uploads: list[tuple[str, str, int]] = []

    def pinned_host_key(self) -> str | None:
        return self._pinned

    async def connect(self) -> None:
        self.connect_count += 1
        self.connect_entered.set()
        if self.connect_gate is not None:
            await self.connect_gate.wait()
        if self.connect_error is not None:
            raise self.connect_error
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    def _require_connected(self) -> None:
        # Mirrors AsyncsshShell's contract so consumer tests can't pass with
        # a connect-ordering bug.
        if not self.connected:
            raise RuntimeError("not connected — call connect() first")

    async def run(self, command: str) -> RunResult:
        self._require_connected()
        self.commands.append(command)  # recorded even when it raises: proves it was attempted
        if command in self.run_errors:
            raise self.run_errors[command]
        if command in self.responses:
            return self.responses[command]
        if command == _DEFAULT_INACTIVE_BLE_PROBE:
            return _INACTIVE
        return _OK

    async def put_bytes(self, data: bytes, remote_path: str, mode: int) -> None:
        self._require_connected()
        self.uploads.append((remote_path, data, mode))

    async def put_dir(self, local_dir: str, remote_dir: str) -> None:
        self._require_connected()
        if self.put_dir_error is not None:
            # Models a mid-transfer SFTP failure; recorded only on success so
            # tests can assert nothing destructive ran after a failed upload.
            raise self.put_dir_error
        self.dir_uploads.append((local_dir, remote_dir))

    async def put_file(self, local_path: str, remote_path: str, mode: int) -> None:
        self._require_connected()
        self.file_uploads.append((local_path, remote_path, mode))


class SequencedResponseShell(FakeShell):
    """FakeShell whose selected commands return a finite state-transition sequence."""

    def __init__(
        self,
        response_sequences: dict[str, list[RunResult]],
        responses: dict[str, RunResult] | None = None,
    ) -> None:
        super().__init__(responses=responses)
        if any(not sequence for sequence in response_sequences.values()):
            raise ValueError("response sequences must not be empty")
        self._response_sequences = {
            command: list(sequence) for command, sequence in response_sequences.items()
        }

    async def run(self, command: str) -> RunResult:
        if sequence := self._response_sequences.get(command):
            self._require_connected()
            self.commands.append(command)
            return sequence.pop(0)
        return await super().run(command)

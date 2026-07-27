"""Fake PanelShell for unit tests — scripted responses, recorded calls."""

from __future__ import annotations

import asyncio

from custom_components.brilliant_mqtt.shell import PanelProcess, RunResult

_OK = RunResult(0, "", "")


class FakePanelProcess:
    """Controllable explicit-settlement implementation of PanelProcess."""

    def __init__(self, result: RunResult = _OK, *, settled: bool = True) -> None:
        self._result = result
        self._settled = asyncio.Event()
        if settled:
            self._settled.set()
        self._terminated = False
        self.terminate_count = 0

    @property
    def running(self) -> bool:
        return not self._settled.is_set()

    def terminate(self) -> None:
        if self._terminated or not self.running:
            return
        self._terminated = True
        self.terminate_count += 1
        self._settled.set()

    async def wait(self) -> RunResult:
        await self._settled.wait()
        return self._result

    def settle(self, result: RunResult | None = None) -> None:
        """Test-only completion hook for a naturally exiting child."""
        if result is not None:
            self._result = result
        self._settled.set()


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
        processes: dict[str, FakePanelProcess] | None = None,
    ) -> None:
        self.responses = dict(responses or {})
        self.connect_error = connect_error
        self.put_dir_error = put_dir_error
        # Commands whose run() raises the mapped exception (models a mid-command
        # transport drop — e.g. the reboot disconnect, or a dead diagnostics probe).
        self.run_errors = dict(run_errors or {})
        self.processes = dict(processes or {})
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
        self.started_processes: list[PanelProcess] = []

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
        for process in self.started_processes:
            if process.running:
                process.terminate()
        await asyncio.gather(
            *(process.wait() for process in self.started_processes),
            return_exceptions=True,
        )
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
        return self.responses.get(command, _OK)

    async def start(self, command: str) -> PanelProcess:
        self._require_connected()
        self.commands.append(command)
        process = self.processes.get(command, FakePanelProcess(self.responses.get(command, _OK)))
        self.started_processes.append(process)
        return process

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

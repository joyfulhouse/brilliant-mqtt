"""Fake PanelShell for unit tests — scripted responses, recorded calls."""

from __future__ import annotations

from custom_components.brilliant_mqtt.shell import RunResult

_OK = RunResult(0, "", "")


class FakeShell:
    """Satisfies the PanelShell Protocol. Unscripted commands return success."""

    def __init__(
        self,
        responses: dict[str, RunResult] | None = None,
        connect_error: Exception | None = None,
        pinned: str | None = "ssh-ed25519 FAKEKEY",
    ) -> None:
        self.responses = dict(responses or {})
        self.connect_error = connect_error
        self._pinned = pinned
        self.connected = False
        self.commands: list[str] = []
        self.uploads: list[tuple[str, bytes, int]] = []
        self.dir_uploads: list[tuple[str, str]] = []

    def pinned_host_key(self) -> str | None:
        return self._pinned

    async def connect(self) -> None:
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
        self.commands.append(command)
        return self.responses.get(command, _OK)

    async def put_bytes(self, data: bytes, remote_path: str, mode: int) -> None:
        self._require_connected()
        self.uploads.append((remote_path, data, mode))

    async def put_dir(self, local_dir: str, remote_dir: str) -> None:
        self._require_connected()
        self.dir_uploads.append((local_dir, remote_dir))

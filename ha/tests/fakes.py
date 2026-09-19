"""Fake PanelShell for unit tests — scripted responses, recorded calls."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import re
import tarfile

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
        put_bytes_error: Exception | None = None,
        connect_gate: asyncio.Event | None = None,
        pinned: str | None = "ssh-ed25519 FAKEKEY",
        run_errors: dict[str, Exception] | None = None,
        processes: dict[str, FakePanelProcess] | None = None,
    ) -> None:
        self.responses = dict(responses or {})
        self.connect_error = connect_error
        self.put_dir_error = put_dir_error
        # Models a mid-transfer SFTP failure on a streamed upload (tarball /
        # unit bytes); recorded only on success, like put_dir_error.
        self.put_bytes_error = put_bytes_error
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
        # Identity protocol traffic is tracked separately from recipe commands.
        self.identity_commands: list[str] = []
        self.identity_uploads: list[tuple[str, bytes, int]] = []
        self.release_identities: dict[str, dict[str, object] | None] = {
            "bridge": None,
            "wifi_watchdog": None,
            "bus_watchdog": None,
        }
        self._pending_identities: dict[str, dict[str, object]] = {}
        self._identity_audits: set[str] = set()

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
        if "BRILLIANT_RELEASE_IDENTITY_PREPARE" in command:
            self.identity_commands.append(command)
            return _OK
        if "BRILLIANT_RELEASE_IDENTITY_READ" in command:
            self.identity_commands.append(command)
            return self.responses.get(
                command, RunResult(0, json.dumps(self.release_identities), "")
            )
        if "BRILLIANT_RELEASE_IDENTITY_WRITE" in command:
            self.identity_commands.append(command)
            path, data, _mode = self.identity_uploads[-1]
            if "override-" in path:
                transaction = str(json.loads(data)["transaction"])
                if transaction in self._identity_audits:
                    return RunResult(1, "", "already consumed")
                self._identity_audits.add(transaction)
            else:
                for component in self.release_identities:
                    if f".release-{component}-" in path:
                        self.release_identities[component] = json.loads(data)
                        self._pending_identities[component] = json.loads(data)
            return _OK
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
        if self.put_bytes_error is not None:
            raise self.put_bytes_error
        if "/.release-" in remote_path:
            self.identity_uploads.append((remote_path, data, mode))
            return
        self.uploads.append((remote_path, data, mode))
        if remote_path.endswith("/VERSION"):
            component = next(
                (name for name in ("wifi_watchdog", "bus_watchdog") if f"/{name}/" in remote_path),
                "bridge",
            )
            archive_path = (
                "/var/brilliant-mqtt.staging.tar.gz"
                if component == "bridge"
                else f"/var/brilliant-mqtt/{component}.staging.tar.gz"
            )
            archives = [content for path, content, _mode in self.uploads if path == archive_path]
            if archives:
                entries: dict[str, str] = {}
                with tarfile.open(fileobj=io.BytesIO(archives[-1]), mode="r:gz") as archive:
                    for member in archive.getmembers():
                        path = member.name.removeprefix("./")
                        if (
                            not member.isfile()
                            or "__pycache__" in path
                            or re.search(r"\.py[co]$", path)
                        ):
                            continue
                        if component == "bridge" and not path.startswith(("app/", "vendor/")):
                            continue
                        source = archive.extractfile(member)
                        assert source is not None
                        logical = path if component == "bridge" else f"{component}/{path}"
                        entries[logical] = hashlib.sha256(source.read()).hexdigest()
                wire = "".join(f"{path}\t{entries[path]}\n" for path in sorted(entries))
                identity: dict[str, object] = {
                    "version": data.decode(),
                    "release_ordinal": None,
                    "digest": hashlib.sha256(wire.encode()).hexdigest(),
                    "deployment_id": None,
                    "layout": "legacy_fixed",
                }
                self._pending_identities[component] = identity
                incumbent = self.release_identities[component]
                if incumbent is None or incumbent["layout"] == "legacy_fixed":
                    self.release_identities[component] = identity
        for component, identity in self._pending_identities.items():
            service = (
                "brilliant-mqtt"
                if component == "bridge"
                else "brilliant-" + component.replace("_", "-")
            )
            if remote_path == f"/etc/systemd/system/{service}.service":
                self.release_identities[component] = identity

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

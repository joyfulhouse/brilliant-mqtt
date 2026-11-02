"""Disposable filesystem/process transport for the production panel recipes."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.shell import RunResult
from tests.fakes import FakeShell


class RehearsalProcess:
    def __init__(self, process: asyncio.subprocess.Process, panel: Path) -> None:
        self.process = process
        self.panel = panel

    @property
    def running(self) -> bool:
        return self.process.returncode is None

    def terminate(self) -> None:
        if self.running:
            os.killpg(self.process.pid, signal.SIGKILL)

    async def wait(self) -> RunResult:
        stdout, stderr = await self.process.communicate()
        assert self.process.returncode is not None
        output = stdout.decode().replace(str(self.panel), "/var/brilliant-mqtt")
        return RunResult(self.process.returncode, output, stderr.decode())


class RehearsalShell(FakeShell):
    def __init__(self, root: Path) -> None:
        super().__init__()
        self.root = root
        self.panel = root / "var/brilliant-mqtt"
        self.units = root / "etc/systemd/system"
        self.units.mkdir(parents=True)
        self.panel.mkdir(parents=True)
        self.bin = root / "bin"
        self.bin.mkdir()
        self.state = root / "services.json"
        self.state.write_text(json.dumps({"brilliant-mqtt": [True, True]}))
        control = self.bin / "systemctl"
        control.write_text(
            f"#!{sys.executable}\n"
            "import json, sys\nfrom pathlib import Path\n"
            f"path = Path({str(self.state)!r})\n"
            "states = json.loads(path.read_text())\n"
            "op, *args = sys.argv[1:]\n"
            "if op == 'daemon-reload': raise SystemExit(0)\n"
            # Queued samples are consumed one per `show`; a crash-looping unit that
            # is started sits in auto-restart (is-active: activating) until stopped.
            "queued = states.setdefault('__samples__', {})\n"
            "crashing = states.setdefault('__crash__', [])\n"
            "def reading(name):\n"
            "    state = states.setdefault(name, [False, False])\n"
            "    if queued.get(name):\n"
            "        return queued[name].pop(0).split('|')\n"
            "    if state[1] and name in crashing:\n"
            "        return ['activating', 'auto-restart', '7']\n"
            "    return ['active', 'running', '0'] if state[1] else ['inactive', 'dead', '0']\n"
            "if op == 'show':\n"
            "    names = [arg for arg in args if not arg.startswith('-') and arg not in "
            "('ActiveState', 'SubState', 'NRestarts')]\n"
            "    blocks = []\n"
            "    for unit in names:\n"
            "        active, sub, restarts = reading(unit.removesuffix('.service'))\n"
            "        blocks.append(f'ActiveState={active}\\nSubState={sub}\\n"
            "NRestarts={restarts}\\n')\n"
            "    sys.stdout.write('\\n'.join(blocks))\n"
            "    path.write_text(json.dumps(states))\n"
            "    raise SystemExit(0)\n"
            "name = args[-1].removesuffix('.service')\n"
            "state = states.setdefault(name, [False, False])\n"
            "if op in ('is-enabled', 'is-active'):\n"
            "    index = int(op == 'is-active')\n"
            "    running = state[1] and name not in crashing\n"
            "    if '--quiet' not in args:\n"
            "        print(('enabled' if state[0] else 'disabled') if index == 0 "
            "else ('active' if running else 'activating' if state[1] else 'inactive'))\n"
            "    raise SystemExit(0 if (state[0] if index == 0 else running) else 3)\n"
            "if op == 'enable': state[0] = True\n"
            "if op == 'disable': state[0] = False\n"
            # Like systemd, --now also starts or stops the unit.
            "if op in ('enable', 'disable') and '--now' in args: state[1] = op == 'enable'\n"
            "if op in ('start', 'restart'): state[1] = True\n"
            "if op == 'stop': state[1] = False\n"
            "path.write_text(json.dumps(states))\n"
        )
        control.chmod(0o700)

    def _update_state(self, change: Callable[[dict[str, Any]], None]) -> None:
        states = json.loads(self.state.read_text())
        change(states)
        self.state.write_text(json.dumps(states))

    def queue_samples(self, service: str, *samples: str) -> None:
        """Queue exact `Active|Sub|NRestarts` readings for the next `show` calls."""

        def change(states: dict[str, Any]) -> None:
            states.setdefault("__samples__", {}).setdefault(service, []).extend(samples)

        self._update_state(change)

    def crash_loop(self, service: str, *, crashing: bool = True) -> None:
        """Make a started unit sit in auto-restart (crash loop) until stopped."""

        def change(states: dict[str, Any]) -> None:
            names = set(states.setdefault("__crash__", []))
            if crashing:
                names.add(service)
            else:
                names.discard(service)
            states["__crash__"] = sorted(names)

        self._update_state(change)

    def translate(self, value: str) -> str:
        return (
            value.replace("/var/brilliant-mqtt", str(self.panel))
            .replace("/etc/systemd/system", str(self.units))
            .replace("/etc/brilliant-mqtt.env", str(self.root / "etc/brilliant-mqtt.env"))
            .replace(panel_ops._PANEL_PYTHON, sys.executable)
            .replace(panel_ops._PANEL_ATOMIC_MOVER, "/usr/bin/mv")
        )

    # Overrides FakeShell's transport bodies, so drop_on/drop_after apply here too.
    async def _do_run(self, command: str) -> RunResult:
        process = await self._do_start(command)
        try:
            return await process.wait()
        except BaseException:
            process.terminate()
            await process.wait()
            raise

    async def _do_start(self, command: str) -> RehearsalProcess:
        self.commands.append(command)
        env = dict(os.environ, PATH=f"{self.bin}:{os.environ['PATH']}")
        process = await asyncio.create_subprocess_exec(
            "sh",
            "-c",
            self.translate(command),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        return RehearsalProcess(process, self.panel)

    async def _do_put_bytes(self, data: bytes, remote_path: str, mode: int) -> None:
        path = Path(self.translate(remote_path))
        await asyncio.to_thread(path.write_bytes, data)
        await asyncio.to_thread(path.chmod, mode)

    async def _do_put_dir(self, local_dir: str, remote_dir: str) -> None:
        def copy() -> None:
            destination = Path(self.translate(remote_dir))
            shutil.copytree(local_dir, destination)
            for unit in destination.glob("*.service"):
                unit.write_text(self.translate(unit.read_text()))

        await asyncio.to_thread(copy)

    def install(self, *, release: bool = False) -> Path:
        code = self.panel
        if release:
            code = self.panel / "releases/0.10.2--aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
            code.mkdir(parents=True)
            (self.panel / "current").symlink_to(code)
        package = code / "app/brilliant_mqtt"
        package.mkdir(parents=True)
        (package / "config.py").write_text('deployment_id = env.get("BRILLIANT_DEPLOYMENT_ID")\n')
        (package / "__main__.py").write_text(
            'metadata = {"deployment_id": settings.deployment_id}\n'
        )
        (code / "vendor").mkdir()
        (code / "vendor/original.py").write_text("original = True\n")
        (code / "VERSION").write_text("0.10.2\n")
        (self.panel / "VERSION").write_text("0.10.2\n")
        env = self.root / "etc/brilliant-mqtt.env"
        env.write_text("MQTT_PASSWORD=fixture-secret\nBRILLIANT_DEPLOYMENT_ID=" + "b" * 32 + "\n")
        env.chmod(0o600)
        unit = self.units / "brilliant-mqtt.service"
        unit.write_text(
            self.translate(
                "[Service]\nExecStart=/var/brilliant-mqtt/"
                + ("current/" if release else "")
                + "app/brilliant_mqtt/__main__.py\n"
            )
        )
        unit.chmod(0o644)
        (self.panel / "tls").mkdir()
        (self.panel / "tls/mqtt-ca.pem").write_bytes(b"fixture CA\n")
        (self.panel / "system").mkdir()
        (self.panel / "system/brilliant-mqtt.env").write_bytes(env.read_bytes())
        (self.panel / "system/brilliant-mqtt.env").chmod(0o600)
        return code

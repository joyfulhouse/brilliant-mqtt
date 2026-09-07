from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import brilliant_bus_watchdog.health as health_mod
from brilliant_bus_watchdog.health import bus_failure_age, heartbeat_age
from brilliant_bus_watchdog.run import _service_active, should_reboot
from brilliant_mqtt import heartbeat
from brilliant_mqtt.heartbeat import (
    DEAD_WRITER_RETENTION_S,
    current_boot_id,
    process_generation,
    write_phase,
)

_CHILD_PHASE_WRITER = """
import sys
from brilliant_mqtt.heartbeat import write_phase

path, phase, raw_now = sys.argv[1:]
now = float(raw_now)
if phase == "bus":
    write_phase(path, "pre_bus", monotonic_clock=lambda: now)
write_phase(path, phase, monotonic_clock=lambda: now)
"""

_HELD_PHASE_WRITER = """
import sys
import time
from brilliant_mqtt.heartbeat import write_phase

path, raw_now = sys.argv[1:]
now = float(raw_now)
write_phase(path, "pre_bus", monotonic_clock=lambda: now)
write_phase(path, "bus", monotonic_clock=lambda: now)
print("ready", flush=True)
time.sleep(60.0)
"""

_FAILED_INVALIDATION_WRITER = """
import asyncio
import os
import signal
import sys
from brilliant_mqtt import __main__ as main_mod, heartbeat
from brilliant_mqtt.config import Settings

class RecoveredBus:
    def on_reconnect(self, callback):
        pass
    def on_change(self, callback, **kwargs):
        pass
    async def start(self):
        pass
    async def shutdown(self):
        pass

class BrokerFailure:
    def on_command(self, callback):
        pass
    def on_message(self, callback):
        pass
    async def connect(self):
        raise ConnectionError("broker refused")
    async def disconnect(self):
        pass

path = sys.argv[1]
heartbeat.write_phase(
    path,
    "bus",
    monotonic_clock=lambda: 100.0,
)
heartbeat.write_phase(path, "bus", monotonic_clock=lambda: 1900.0)
def denied(*args, **kwargs):
    raise PermissionError("read-only phase directory")
heartbeat._atomic_write = denied
heartbeat.os.unlink = denied
main_mod.RpcBusAdapter = lambda **kwargs: RecoveredBus()
main_mod.AioMqttAdapter = lambda settings: BrokerFailure()
settings = Settings(
    panel="office",
    mqtt_host="broker",
    mqtt_username="user",
    mqtt_password="password",
    retained_topics_file=f"{path}.owned.json",
    bus_phase_file=path,
)
try:
    asyncio.run(main_mod._run_session(settings, None, None))
except ConnectionError as error:
    assert str(error) == "broker refused"
else:
    raise AssertionError("broker failure was not reached")
os.kill(os.getpid(), signal.SIGKILL)
"""


def _write_phase_in_short_lived_process(path: Path, phase: str, now: float) -> None:
    subprocess.run(
        [sys.executable, "-c", _CHILD_PHASE_WRITER, str(path), phase, str(now)],
        check=True,
    )


def _start_phase_writer(path: Path, now: float) -> subprocess.Popen[str]:
    process = subprocess.Popen(
        [sys.executable, "-c", _HELD_PHASE_WRITER, str(path), str(now)],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "ready"
    return process


def _service_is(state: str) -> bool:
    return _service_active(
        "brilliant-mqtt",
        run=lambda argv: SimpleNamespace(stdout=state),
    )


def test_watchdog_imports_with_only_its_deployment_tree(tmp_path: Path) -> None:
    package_root = tmp_path / "watchdog-only"
    shutil.copytree(
        Path(__file__).parents[1] / "src" / "brilliant_bus_watchdog",
        package_root / "brilliant_bus_watchdog",
    )
    script = """
import pathlib
import sys

root = pathlib.Path(sys.argv[1]).resolve()
stdlib = pathlib.Path(sys.base_prefix)
sys.path = [
    str(root),
    *(entry for entry in sys.path if entry and pathlib.Path(entry).is_relative_to(stdlib)),
]
import brilliant_bus_watchdog.health as health
import brilliant_bus_watchdog.run as run
assert pathlib.Path(health.__file__).is_relative_to(root)
assert pathlib.Path(run.__file__).is_relative_to(root)
"""

    subprocess.run(
        [sys.executable, "-I", "-c", script, str(package_root)],
        check=True,
        cwd=tmp_path,
    )


def test_fresh(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    p.write_text("100.0")
    assert heartbeat_age(str(p), now=130.0, started_at=0.0) == 30.0


def test_stale(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    p.write_text("100.0")
    assert heartbeat_age(str(p), now=2000.0, started_at=0.0) == 1900.0


def test_missing_file_measures_from_start(tmp_path: Path) -> None:
    # no file: age is now - started_at, so a never-seen heartbeat only ages
    # relative to the watchdog's own start (not epoch 0)
    assert heartbeat_age(str(tmp_path / "nope"), now=500.0, started_at=200.0) == 300.0


def test_unparsable_measures_from_start(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    p.write_text("garbage")
    assert heartbeat_age(str(p), now=500.0, started_at=200.0) == 300.0


@pytest.mark.parametrize(
    ("contents", "directory"),
    [
        (None, False),
        (None, True),
        ("pre_bus", False),
        (f"pre_bus {os.getpid()}", False),
        ("bus", False),
        ("bus unavailable", False),
        ("bus 0", False),
        (f" bus {os.getpid()}\n", False),
    ],
)
def test_bus_failure_age_fails_closed(
    tmp_path: Path,
    contents: str | None,
    directory: bool,
) -> None:
    phase = tmp_path / "bus-phase"
    if directory:
        phase.mkdir()
    elif contents is not None:
        phase.write_text(contents, encoding="utf-8")

    assert bus_failure_age(str(phase), now=100.0) is None


def test_bus_failure_age_accepts_live_generation_with_active_lease(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    write_phase(str(phase), "bus", monotonic_clock=lambda: 100.0)

    assert bus_failure_age(str(phase), now=100.0) == 0.0


def test_killed_bus_writers_qualify_during_service_restart_backoff(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    samples = [100.0 + 299.0 * index for index in range(8)]

    decisions: list[bool] = []
    for now in samples:
        process = _start_phase_writer(phase, now)
        try:
            assert bus_failure_age(str(phase), now=now) == now - samples[0]
            process.kill()
            assert process.wait(timeout=5.0) == -signal.SIGKILL
            failure_age = bus_failure_age(str(phase), now=now)
            decisions.append(
                should_reboot(
                    age=2200.0,
                    stale_after=1800.0,
                    bridge_active=_service_is("activating"),
                    gateway_up=True,
                    bus_failure_age=failure_age,
                )
            )
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5.0)

    assert decisions[:-1] == [False] * 7
    assert bus_failure_age(str(phase), now=samples[-1]) == samples[-1] - samples[0]
    assert decisions[-1] is True
    assert not should_reboot(
        age=2200.0,
        stale_after=1800.0,
        bridge_active=_service_is("inactive"),
        gateway_up=True,
        bus_failure_age=bus_failure_age(str(phase), now=samples[-1]),
    )


def test_dead_pre_bus_writers_never_attribute_broker_only_startup(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    _write_phase_in_short_lived_process(phase, "bus", 100.0)
    for now in (200.0, 300.0, 400.0):
        _write_phase_in_short_lived_process(phase, "pre_bus", now)

    failure_age = bus_failure_age(str(phase), now=400.0)
    assert failure_age is None
    assert not should_reboot(
        age=1900.0,
        stale_after=1800.0,
        bridge_active=_service_is("activating"),
        gateway_up=True,
        bus_failure_age=failure_age,
    )


def test_dead_bus_record_expires_after_its_service_generation_window(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    _write_phase_in_short_lived_process(phase, "bus", 100.0)

    inside = DEAD_WRITER_RETENTION_S - 1.0
    outside = DEAD_WRITER_RETENTION_S + 1.0
    assert bus_failure_age(str(phase), now=100.0 + inside) == inside
    assert bus_failure_age(str(phase), now=100.0 + outside) is None


def test_bus_record_from_an_earlier_boot_is_rejected(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    write_phase(str(phase), "bus", monotonic_clock=lambda: 100.0)
    parts = phase.read_text(encoding="utf-8").split()
    parts[4] = "earlier-boot"
    phase.write_text(" ".join(parts), encoding="utf-8")

    assert bus_failure_age(str(phase), now=1900.0) is None


def test_bus_record_from_an_earlier_process_generation_is_rejected(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    write_phase(str(phase), "bus", monotonic_clock=lambda: 100.0)
    parts = phase.read_text(encoding="utf-8").split()
    parts[3] = str(int(parts[3]) + 1)
    phase.write_text(" ".join(parts), encoding="utf-8")

    assert bus_failure_age(str(phase), now=200.0) is None

    write_phase(str(phase), "pre_bus", monotonic_clock=lambda: 200.0)
    write_phase(str(phase), "bus", monotonic_clock=lambda: 200.0)
    failure_age = bus_failure_age(str(phase), now=200.0)
    assert failure_age == 0.0
    assert not should_reboot(
        age=100.0,
        stale_after=50.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=failure_age,
    )


def test_failed_invalidation_stays_revoked_after_writer_death(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    process = subprocess.run(
        [sys.executable, "-c", _FAILED_INVALIDATION_WRITER, str(phase)],
        check=False,
    )
    assert process.returncode == -signal.SIGKILL

    failure_age = bus_failure_age(str(phase), now=1901.0)
    assert failure_age is None
    assert not should_reboot(
        age=1801.0,
        stale_after=1800.0,
        bridge_active=_service_is("activating"),
        gateway_up=True,
        bus_failure_age=failure_age,
    )


def test_unknown_live_process_generation_does_not_count_as_writer_death(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    phase = tmp_path / "bus-phase"
    boot_id = current_boot_id()
    generation = process_generation(os.getpid())
    assert boot_id is not None
    assert generation is not None
    phase.write_text(
        f"v2 bus {os.getpid()} {generation} {boot_id} 100.0 100.0 attempt",
        encoding="utf-8",
    )
    monkeypatch.setattr(health_mod, "process_generation", lambda pid: None)

    assert bus_failure_age(str(phase), now=200.0) is None


def test_live_writer_with_unknown_generation_is_not_inherited(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    phase = tmp_path / "bus-phase"
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        boot_id = current_boot_id()
        generation = process_generation(process.pid)
        assert boot_id is not None
        assert generation is not None
        phase.write_text(
            f"v2 bus {process.pid} {generation} {boot_id} 100.0 100.0 attempt",
            encoding="utf-8",
        )
        real_process_generation = heartbeat.process_generation

        def _generation(pid: int) -> str | None:
            return None if pid == process.pid else real_process_generation(pid)

        monkeypatch.setattr(heartbeat, "process_generation", _generation)
        write_phase(str(phase), "pre_bus", monotonic_clock=lambda: 200.0)
        write_phase(str(phase), "bus", monotonic_clock=lambda: 200.0)

        assert bus_failure_age(str(phase), now=200.0) == 0.0
    finally:
        process.kill()
        process.wait(timeout=5.0)


@pytest.mark.parametrize("pid", [1, 0, -1])
def test_bus_failure_age_fails_closed_on_pid_le_one(tmp_path: Path, pid: int) -> None:
    """A marker naming pid <= 1 must read as unconfirmed. pid 1 (init) always
    exists, so a torn/truncated ``bus 1`` marker would otherwise read as
    confirmed forever and suppress reboots indefinitely (fail-open). pid 0 and
    negative pids would target a process group rather than an individual
    writer, so they are rejected too."""
    phase = tmp_path / "bus-phase"
    phase.write_text(f"bus {pid}", encoding="utf-8")

    assert bus_failure_age(str(phase), now=100.0) is None


def test_legacy_dead_writer_marker_fails_closed(tmp_path: Path) -> None:
    """A leftover ``bus <pid>`` whose pid is no live process (a dead or reverted
    writer, whose tmpfs marker survives until the next reboot) must read as
    unconfirmed — not as a live confirmed bus."""
    phase = tmp_path / "bus-phase"
    dead_pid = 2**31 - 1  # far beyond /proc/sys/kernel/pid_max: no such process
    phase.write_text(f"bus {dead_pid}", encoding="utf-8")

    assert bus_failure_age(str(phase), now=100.0) is None


@pytest.mark.parametrize("pid", [2**31, 2**64])
def test_bus_failure_age_fails_closed_on_out_of_range_pid(tmp_path: Path, pid: int) -> None:
    """A pid outside Linux's signed ``pid_t`` range cannot identify a writer."""
    phase = tmp_path / "bus-phase"
    boot_id = current_boot_id()
    assert boot_id is not None
    phase.write_text(f"v2 bus {pid} 1 {boot_id} 100.0 100.0 attempt", encoding="utf-8")

    assert bus_failure_age(str(phase), now=100.0) is None


def test_bus_failure_age_fails_closed_on_invalid_utf8(tmp_path: Path) -> None:
    """Invalid UTF-8 bytes raise UnicodeDecodeError (a UnicodeError, NOT an
    OSError). bus_failure_age must fail closed rather than let that kill the
    watchdog run.py loop."""
    phase = tmp_path / "bus-phase"
    phase.write_bytes(b"\xff\xfe bus")  # not decodable as UTF-8

    assert bus_failure_age(str(phase), now=100.0) is None

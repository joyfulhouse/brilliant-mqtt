"""Contract tests for the read-only i350/aarch64 panel diagnostic (issue #155).

The real script runs under ``/bin/sh`` against a fake ``DIAG_ROOT`` tree with a
PATH made only of fakes plus symlinks to the host tools the script needs, so
the host's own root, ``timeout`` and ``systemctl`` are never consulted.
"""

from __future__ import annotations

import os
import shlex
import shutil
import stat
import subprocess
import sys
import venv
from dataclasses import dataclass, field
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "brilliant-panel" / "panel_diagnostics.sh"
HEADER = "# brilliant-mqtt panel diagnostics (read-only) v1"

_HOST_TOOLS = (
    "awk",
    "cut",
    "dd",
    "df",
    "find",
    "head",
    "od",
    "sed",
    "sort",
    "tail",
    "tr",
    "true",
    "wc",
)
_HOST_PY_DIR = f"python{sys.version_info.major}.{sys.version_info.minor}"
_SP310 = "data/switch-embedded/env/lib/python3.10/site-packages"
_SECRET = "SECRET-DO-NOT-LEAK-7f3a"
_IMPORT = "from lib.message_bus_api.observer_interface import RPCObserver"


@dataclass
class Panel:
    """Knobs for one fake panel."""

    machine: str = "aarch64"
    python: bool = True
    python_version: str = "Python 3.10.13"
    py_dir: str = "python3.10"
    site_packages: bool = True
    import_ok: bool = True
    import_rc: int = 1
    release_info: str | None = '{"release_tag": "v26.07.15.1", "other": "x"}'
    elf: tuple[int, int] | None = (2, 183)
    bus_api_elf: tuple[int, int] | None = (2, 183)
    socket: bool = True
    mounts: str = (
        "/dev/mmcblk0p4 /var ext4 rw,relatime 0 0\n/dev/mmcblk0p3 /data ext4 ro,relatime 0 0\n"
    )
    cpuinfo: str = "processor\t: 0\nprocessor\t: 1\nprocessor\t: 2\nprocessor\t: 3\n"
    drivers: tuple[str, ...] = ("mtk-msdc", "sdhci", "leds-gpio")
    wlan0: bool = True
    timeout: str | None = "gnu"  # gnu | busybox | busybox_old | broken | None
    expire_rc: int | None = None
    extra_files: dict[str, str] = field(default_factory=dict)
    missing_tools: tuple[str, ...] = ()
    head_without_c: bool = False


def _write(path: Path, content: str, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    path.chmod(mode)


def _fake_timeout(style: str, expire_rc: int | None) -> str:
    expire = (
        f'case "$*" in *RPCObserver*) exit {expire_rc} ;; esac\n' if expire_rc is not None else ""
    )
    if style == "broken":
        return "#!/bin/sh\nexit 126\n"
    if style == "gnu":
        help_text = 'echo "Usage: timeout [OPTION] DURATION COMMAND [ARG]..."; echo more'
        parse = "shift\n"
    elif style == "busybox":
        help_text = 'echo "BusyBox v1.36.1 (2024-01-01) multi-call binary." >&2'
        parse = "shift\n"
    else:  # busybox_old: only `timeout -t SECS CMD`
        help_text = 'echo "BusyBox v1.24.1 (2017-01-01) multi-call binary." >&2'
        parse = '[ "$1" = -t ] || exit 1\nshift 2\n'
    return (
        "#!/bin/sh\n"
        f'if [ "${{1:-}}" = --help ]; then {help_text}; exit 1; fi\n'
        f"{parse}"
        f"{expire}"
        'exec "$@"\n'
    )


def _elf_header(elf_class: int, machine: int) -> bytes:
    return (
        b"\x7fELF"
        + bytes([elf_class, 1, 1])
        + bytes(9)
        + (3).to_bytes(2, "little")
        + machine.to_bytes(2, "little")
        + bytes(44)
    )


def _build(tmp_path: Path, panel: Panel) -> tuple[Path, Path, dict[str, str]]:
    root = tmp_path / "root"
    fakebin = tmp_path / "fakebin"
    logs = tmp_path / "logs"
    for directory in (root, fakebin, logs):
        directory.mkdir()

    for tool in _HOST_TOOLS:
        if tool in panel.missing_tools:
            continue
        found = shutil.which(tool, path="/usr/bin:/bin")
        assert found is not None, tool
        if tool == "head" and panel.head_without_c:
            # Old busybox: no `head -c`.
            _write(
                fakebin / "head",
                '#!/bin/sh\ncase " $* " in *" -c "*) echo "head: bad option" >&2; exit 1 ;; esac\n'
                f'exec {shlex.quote(found)} "$@"\n',
                0o755,
            )
            continue
        (fakebin / tool).symlink_to(found)

    _write(
        fakebin / "uname",
        "#!/bin/sh\n"
        f'case "$1" in -m) echo {panel.machine} ;; '
        "-r) echo 5.15.73-i350 ;; *) echo Linux ;; esac\n",
        0o755,
    )
    _write(
        fakebin / "systemctl",
        f'#!/bin/sh\necho "$*" >> {shlex.quote(str(logs / "systemctl.log"))}\n'
        'case "$1" in is-enabled) echo enabled ;; is-active) echo active ;; esac\n',
        0o755,
    )
    for tool in ("iw", "connmanctl"):
        _write(
            fakebin / tool,
            f'#!/bin/sh\necho "$*" >> {shlex.quote(str(logs / (tool + ".log")))}\n',
            0o755,
        )
    if panel.timeout is not None:
        _write(fakebin / "timeout", _fake_timeout(panel.timeout, panel.expire_rc), 0o755)

    env_dir = root / "data/switch-embedded/env"
    if panel.python:
        # A real venv, so the script's isolated (-I) import must find the bus
        # library through the panel venv's own site-packages, not PYTHONPATH.
        venv.EnvBuilder(symlinks=True, with_pip=False).create(env_dir)
        if panel.py_dir != _HOST_PY_DIR or not panel.site_packages:
            shutil.rmtree(env_dir / "lib" / _HOST_PY_DIR)
    sp = env_dir / "lib" / panel.py_dir / "site-packages"
    if panel.site_packages:
        pkg = sp / "lib" / "message_bus_api"
        _write(sp / "lib" / "__init__.py", "")
        _write(pkg / "__init__.py", "")
        body = (
            "class RPCObserver:\n    pass\n" if panel.import_ok else "raise ImportError('boom')\n"
        )
        _write(pkg / "observer_interface.py", body)
        if panel.bus_api_elf is not None:
            (pkg / "observer_native.cpython-310.so").write_bytes(_elf_header(*panel.bus_api_elf))
        if panel.elf is not None:
            header = _elf_header(*panel.elf)
            for name in ("processor.cpython-310.so", "observer_core.cpython-310.so"):
                target = sp / "lib" / "protocol" / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(header)

    if panel.python:
        wrapper = (
            "#!/bin/sh\n"
            f'if [ "${{1:-}}" = --version ]; then echo {shlex.quote(panel.python_version)}; '
            "exit 0; fi\n"
            f'case "$*" in *RPCObserver*) [ {int(panel.import_ok or panel.import_rc == 1)} = 1 ] '
            f"|| {{ echo 'Traceback (most recent call last):' >&2; "
            f"echo 'ImportError: boom' >&2; exit {panel.import_rc}; }} ;; esac\n"
            f'echo "$PYTHONDONTWRITEBYTECODE $*" >> {shlex.quote(str(logs / "python.log"))}\n'
            f'exec {shlex.quote(str(env_dir / "bin" / _HOST_PY_DIR))} "$@"\n'
        )
        (env_dir / "bin" / "python3").unlink()
        _write(env_dir / "bin" / "python3", wrapper, 0o755)

    if panel.release_info is not None:
        _write(root / "etc/release_info.json", panel.release_info)
    if panel.socket:
        _write(root / "var/run/brilliant/server_socket", "")
    _write(root / "proc/mounts", panel.mounts)
    _write(root / "proc/meminfo", "MemTotal:        1999580 kB\nMemFree:  12 kB\n")
    _write(root / "proc/cpuinfo", panel.cpuinfo)
    (root / "data").mkdir(exist_ok=True)
    for driver in panel.drivers:
        (root / "sys/bus/platform/drivers" / driver).mkdir(parents=True)
    if panel.wlan0:
        (root / "sys/class/net/wlan0").mkdir(parents=True)

    # Secrets the script must never read or echo.
    _write(root / "etc/shadow", f"root:{_SECRET}:19000::::::\n", 0o600)
    _write(root / "etc/brilliant-mqtt.env", f"MQTT_PASSWORD={_SECRET}\n", 0o600)
    for rel, content in panel.extra_files.items():
        _write(root / rel, content)

    env = {
        "PATH": str(fakebin),
        "DIAG_ROOT": str(root),
        "HOME": str(tmp_path),
        "LC_ALL": "C",
    }
    return root, logs, env


def _parse(stdout: str) -> dict[str, str]:
    lines = stdout.splitlines()
    assert lines, "no output"
    assert lines[0] == HEADER
    values: dict[str, str] = {}
    for line in lines[1:]:
        key, sep, value = line.partition("=")
        assert sep, f"not key=value: {line!r}"
        assert key and key.replace("_", "").isalnum() and key.islower(), line
        assert key not in values, f"duplicate key {key}"
        assert value.isascii() and value.isprintable(), line
        values[key] = value
    return values


def _run(
    tmp_path: Path,
    panel: Panel | None = None,
    *,
    cwd: Path | None = None,
    stdin_mode: bool = False,
    shell: tuple[str, ...] = ("/bin/sh",),
) -> tuple[dict[str, str], Path, Path]:
    root, logs, env = _build(tmp_path, panel or Panel())
    if stdin_mode:
        with SCRIPT.open("rb") as script:
            result = subprocess.run(
                [*shell, "-s"],
                stdin=script,
                cwd=cwd or tmp_path,
                env=env,
                check=False,
                capture_output=True,
                timeout=120,
            )
    else:
        result = subprocess.run(
            [*shell, str(SCRIPT)],
            stdin=subprocess.DEVNULL,
            cwd=cwd or tmp_path,
            env=env,
            check=False,
            capture_output=True,
            timeout=120,
        )
    assert result.returncode == 0, result.stderr
    values = _parse(result.stdout.decode("ascii"))
    for value in values.values():
        assert _SECRET not in value
    return values, root, logs


def test_full_i350_style_report(tmp_path: Path) -> None:
    values, _root, logs = _run(tmp_path)

    assert values == {
        "timeout_present": "1",
        "timeout_version": "Usage: timeout [OPTION] DURATION COMMAND [ARG]...",
        "timeout_mode": "plain",
        "machine": "aarch64",
        "kernel_release": "5.15.73-i350",
        "panel_python_present": "1",
        "panel_python_version": "Python 3.10.13",
        "release_tag": "v26.07.15.1",
        "site_packages_dir": "/" + _SP310,
        "bus_library_import": "ok",
        "bus_library_so_count": "3",
        "bus_library_elf_sample": "lib/message_bus_api/observer_native.cpython-310.so",
        "bus_library_elf_class": "2",
        "bus_library_elf_machine": "183",
        "bus_library_elf_arch": "aarch64",
        "bus_socket_present": "1",
        "message_bus_enabled": "enabled",
        "message_bus_active": "active",
        "var_mount_fstype": "ext4",
        "var_mount_options": "rw,relatime",
        "data_mount_fstype": "ext4",
        "data_mount_options": "ro,relatime",
        "var_free_kib": values["var_free_kib"],
        "data_free_kib": values["data_free_kib"],
        "wlan0_present": "1",
        "iw_present": "1",
        "connmanctl_present": "1",
        "mmc_drivers": "mtk-msdc,sdhci",
        "imx_sdhci_driver_present": "0",
        "mem_total_kib": "1999580",
        "cpu_count": "4",
        "hue_site_packages_present": "1",
        "hue_peripherals_ini_present": "0",
        "hue_ca_bundle_present": "0",
    }
    assert values["var_free_kib"].isdigit()
    assert values["data_free_kib"].isdigit()
    assert not (logs / "iw.log").exists()
    assert not (logs / "connmanctl.log").exists()


def test_bus_import_is_isolated_and_uses_the_panel_venv(tmp_path: Path) -> None:
    values, _root, logs = _run(tmp_path)
    assert values["bus_library_import"] == "ok"
    calls = (logs / "python.log").read_text().splitlines()
    assert f"1 -I -B -c {_IMPORT}" in calls


def test_import_failure_reports_last_error_line(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(import_ok=False))
    assert values["bus_library_import"] == "failed: exit 1: ImportError: boom"


def test_release_info_missing(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(release_info=None))
    assert values["release_tag"] == "missing"


def test_release_info_without_tag_is_unknown(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(release_info='{"build": 3}'))
    assert values["release_tag"] == "unknown"


def test_armv7_elf_is_reported_as_arm32(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(machine="armv7l", elf=(1, 40), bus_api_elf=(1, 40)))
    assert values["machine"] == "armv7l"
    assert values["bus_library_elf_class"] == "1"
    assert values["bus_library_elf_machine"] == "40"
    assert values["bus_library_elf_arch"] == "arm32"


def test_elf_sample_prefers_message_bus_api(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(elf=(2, 183), bus_api_elf=(1, 40)))
    assert values["bus_library_elf_sample"] == "lib/message_bus_api/observer_native.cpython-310.so"
    assert values["bus_library_elf_arch"] == "arm32"


def test_elf_sample_falls_back_to_lib(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(bus_api_elf=None))
    assert values["bus_library_elf_sample"] == "lib/protocol/observer_core.cpython-310.so"
    assert values["bus_library_so_count"] == "2"
    assert values["bus_library_elf_arch"] == "aarch64"


def test_non_elf_sample_is_not_elf(tmp_path: Path) -> None:
    panel = Panel(
        bus_api_elf=None,
        elf=None,
        extra_files={f"{_SP310}/lib/protocol/fake.cpython-310.so": "not an elf file\n"},
    )
    values, _root, _logs = _run(tmp_path, panel)
    assert values["bus_library_elf_arch"] == "not_elf"
    assert values["bus_library_elf_class"] == "unknown"


def test_missing_od_reports_unknown_not_not_elf(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(missing_tools=("od",)))
    assert values["bus_library_elf_sample"] == "lib/message_bus_api/observer_native.cpython-310.so"
    assert values["bus_library_elf_class"] == "unknown"
    assert values["bus_library_elf_machine"] == "unknown"
    assert values["bus_library_elf_arch"] == "unknown"


def test_missing_find_reports_unknown_count(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(missing_tools=("find",)))
    assert values["bus_library_so_count"] == "unknown"
    # The preferred sample needs no find.
    assert values["bus_library_elf_sample"] == "lib/message_bus_api/observer_native.cpython-310.so"
    assert values["bus_library_elf_arch"] == "aarch64"


def test_missing_find_without_bus_api_so_is_unknown(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(missing_tools=("find",), bus_api_elf=None))
    assert values["bus_library_so_count"] == "unknown"
    assert values["bus_library_elf_sample"] == "unknown"
    assert values["bus_library_elf_arch"] == "unknown"


_PADDED_TAG = '{"pad": "' + "x" * 5000 + '", "release_tag": "v26.07.15.1"}'
_TAG_THEN_PADDING = '{"release_tag": "v26.07.15.1", "pad": "' + "x" * 5000 + '"}'


@pytest.mark.parametrize("head_without_c", (False, True), ids=("head_c", "dd_fallback"))
@pytest.mark.parametrize(
    ("release_info", "expected"),
    ((_PADDED_TAG, "unknown"), (_TAG_THEN_PADDING, "v26.07.15.1")),
    ids=("tag_beyond_4096", "tag_within_4096"),
)
def test_release_info_is_read_bounded(
    tmp_path: Path, head_without_c: bool, release_info: str, expected: str
) -> None:
    values, _root, _logs = _run(
        tmp_path, Panel(release_info=release_info, head_without_c=head_without_c)
    )
    assert values["release_tag"] == expected


def test_no_socket(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(socket=False))
    assert values["bus_socket_present"] == "0"


def test_no_panel_python(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(python=False))
    assert values["panel_python_present"] == "0"
    assert values["panel_python_version"] == "missing"
    assert values["bus_library_import"] == "skipped: panel python missing"


def test_python311_layout(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(py_dir="python3.11"))
    assert values["site_packages_dir"] == "/data/switch-embedded/env/lib/python3.11/site-packages"
    # The panel interpreter's venv only searches its own python3.X directory.
    assert values["bus_library_import"].startswith("failed: exit 1: ModuleNotFoundError")
    assert values["bus_library_so_count"] == "3"
    # The Hue CA paths are hardcoded to python3.10 by src/brilliant_hue_ca.
    assert values["hue_site_packages_present"] == "0"


def test_missing_site_packages(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(site_packages=False, import_ok=False))
    assert values["site_packages_dir"] == "missing"
    assert values["bus_library_so_count"] == "0"
    assert values["bus_library_elf_sample"] == "none"
    assert values["bus_library_elf_class"] == "none"
    assert values["bus_library_elf_machine"] == "none"
    assert values["bus_library_elf_arch"] == "none"


def test_stacked_mounts_last_entry_wins(tmp_path: Path) -> None:
    mounts = (
        "/dev/mmcblk0p4 /var ext4 rw,relatime 0 0\n"
        "overlay /var overlay rw,lowerdir=/a 0 0\n"
        "/dev/mmcblk0p3 /data squashfs ro 0 0\n"
        "tmpfs /data tmpfs rw,nosuid 0 0\n"
        "tmpfs /var/run tmpfs rw 0 0\n"
    )
    values, _root, _logs = _run(tmp_path, Panel(mounts=mounts))
    assert values["var_mount_fstype"] == "overlay"
    assert values["var_mount_options"] == "rw,lowerdir=/a"
    assert values["data_mount_fstype"] == "tmpfs"
    assert values["data_mount_options"] == "rw,nosuid"


def test_unmounted_paths_report_none(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(mounts="proc /proc proc rw 0 0\n"))
    assert values["var_mount_fstype"] == "none"
    assert values["data_mount_options"] == "none"


def test_no_processor_lines(tmp_path: Path) -> None:
    values, _root, _logs = _run(tmp_path, Panel(cpuinfo="Hardware\t: i350\n"))
    assert values["cpu_count"] == "0"


def test_imx_driver_detected(tmp_path: Path) -> None:
    values, _root, _logs = _run(
        tmp_path, Panel(machine="armv7l", drivers=("sdhci-esdhc-imx", "imx-sdma"))
    )
    assert values["imx_sdhci_driver_present"] == "1"
    assert values["mmc_drivers"] == "sdhci-esdhc-imx"


def test_hue_ca_paths_report_presence_never_content(tmp_path: Path) -> None:
    panel = Panel(
        extra_files={
            f"{_SP310}/lib/certs/hue-bridge-ca-certs.pem": (
                f"-----BEGIN CERTIFICATE-----\n{_SECRET}\n-----END CERTIFICATE-----\n"
            ),
            "var/run/brilliant/processes/hue_bridge_peripherals.ini": f"[uwsgi]\nkey={_SECRET}\n",
        }
    )
    values, _root, _logs = _run(tmp_path, panel)
    assert values["hue_ca_bundle_present"] == "1"
    assert values["hue_peripherals_ini_present"] == "1"
    assert values["hue_site_packages_present"] == "1"
    # _run asserts the marker appears in no value.


def test_cwd_lib_cannot_shadow_bus_library(tmp_path: Path) -> None:
    hostile = tmp_path / "hostile"
    _write(hostile / "lib" / "__init__.py", "raise RuntimeError('shadowed')\n")
    _write(hostile / "lib" / "message_bus_api" / "__init__.py", "raise RuntimeError('x')\n")
    values, _root, _logs = _run(tmp_path, cwd=hostile)
    assert values["bus_library_import"] == "ok"


def test_stdin_mode_matches_file_execution(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    by_file, _root, _logs = _run(tmp_path / "a")
    by_stdin, _root, _logs = _run(tmp_path / "b", stdin_mode=True)
    for key in ("var_free_kib", "data_free_kib"):
        by_file.pop(key)
        by_stdin.pop(key)
    assert by_stdin == by_file


def _snapshot(root: Path) -> dict[str, tuple[int, int, bytes | str]]:
    snap: dict[str, tuple[int, int, bytes | str]] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        for name in [*dirnames, *filenames]:
            path = Path(dirpath) / name
            info = path.lstat()
            content: bytes | str
            if stat.S_ISREG(info.st_mode):
                content = path.read_bytes()
            elif stat.S_ISLNK(info.st_mode):
                content = os.readlink(path)
            else:
                content = b""
            snap[str(path.relative_to(root))] = (info.st_mode, info.st_mtime_ns, content)
    return snap


def test_read_only_against_fake_root(tmp_path: Path) -> None:
    root, _logs, env = _build(tmp_path, Panel())
    before = _snapshot(root)
    result = subprocess.run(
        ["/bin/sh", str(SCRIPT)],
        stdin=subprocess.DEVNULL,
        cwd=tmp_path,
        env=env,
        check=False,
        capture_output=True,
        timeout=120,
    )
    assert result.returncode == 0
    assert _snapshot(root) == before
    calls = (tmp_path / "logs" / "systemctl.log").read_text().splitlines()
    assert sorted(calls) == ["is-active message_bus.service", "is-enabled message_bus.service"]


@pytest.mark.parametrize(
    "forbidden",
    (
        "shadow",
        "hostname",
        "ifconfig",
        "ip addr",
        "ip link",
        "iw dev",
        "wpa",
        "/etc/brilliant-mqtt.env",
        "ssh_host",
    ),
)
def test_script_never_references_secret_sources(forbidden: str) -> None:
    assert forbidden not in SCRIPT.read_text().lower()


def test_script_is_executable_posix_sh() -> None:
    text = SCRIPT.read_text()
    assert text.startswith("#!/bin/sh\n")
    assert SCRIPT.stat().st_mode & stat.S_IXUSR
    assert "set -u" in text
    assert "set -e" not in text
    assert "cd / || exit 1" in text


@pytest.mark.parametrize("import_ok", (True, False), ids=("import_ok", "import_fails"))
@pytest.mark.parametrize(
    ("style", "expire_rc", "mode", "version"),
    (
        ("gnu", 124, "plain", "Usage: timeout [OPTION] DURATION COMMAND [ARG]..."),
        ("busybox", 143, "plain", "BusyBox v1.36.1 (2024-01-01) multi-call binary."),
        ("busybox", 137, "plain", "BusyBox v1.36.1 (2024-01-01) multi-call binary."),
        ("busybox_old", 143, "busybox_t", "BusyBox v1.24.1 (2017-01-01) multi-call binary."),
    ),
)
def test_bounded_timeout_flavours(
    tmp_path: Path, style: str, expire_rc: int, mode: str, version: str, import_ok: bool
) -> None:
    values, _root, _logs = _run(tmp_path, Panel(timeout=style, import_ok=import_ok))
    assert values["timeout_present"] == "1"
    assert values["timeout_mode"] == mode
    assert values["timeout_version"] == version
    expected = "ok" if import_ok else "failed: exit 1: ImportError: boom"
    assert values["bus_library_import"] == expected

    (tmp_path / "expired").mkdir()
    values, _root, _logs = _run(
        tmp_path / "expired", Panel(timeout=style, expire_rc=expire_rc, import_ok=import_ok)
    )
    assert values["bus_library_import"] == "failed: timed out"


@pytest.mark.parametrize("timeout_style", ("broken", None), ids=("broken", "absent"))
@pytest.mark.parametrize("import_ok", (True, False), ids=("import_ok", "import_fails"))
def test_unusable_timeout_runs_unbounded(
    tmp_path: Path, timeout_style: str | None, import_ok: bool
) -> None:
    values, _root, _logs = _run(
        tmp_path, Panel(timeout=timeout_style, import_ok=import_ok, import_rc=124)
    )
    assert values["timeout_mode"] == "unbounded"
    assert values["timeout_present"] == ("1" if timeout_style else "0")
    if timeout_style is None:
        assert values["timeout_version"] == "missing"
    # 124 means "timed out" only when a timeout wrapper actually bounded the import.
    expected = "ok" if import_ok else "failed: exit 124: ImportError: boom"
    assert values["bus_library_import"] == expected
    assert values["message_bus_active"] == "active"


@pytest.mark.skipif(shutil.which("busybox") is None, reason="busybox not installed")
def test_busybox_sh_matches_posix_sh(tmp_path: Path) -> None:
    busybox = shutil.which("busybox")
    assert busybox is not None
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    expected, _root, _logs = _run(tmp_path / "a")
    actual, _root, _logs = _run(tmp_path / "b", shell=(busybox, "sh"))
    for key in ("var_free_kib", "data_free_kib"):
        expected.pop(key)
        actual.pop(key)
    assert actual == expected

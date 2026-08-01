"""Bounded, read-only panel compatibility inspection."""

from __future__ import annotations

import asyncio
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from custom_components.brilliant_mqtt import panel_inspection
from custom_components.brilliant_mqtt.const import PANEL_VERSION_FILE
from custom_components.brilliant_mqtt.panel_inspection import (
    PANEL_INSPECTION_COMMAND,
    PanelCompatibilityError,
    PanelFacts,
    async_inspect_panel,
)
from custom_components.brilliant_mqtt.shell import (
    HostIdentity,
    PanelIdentityError,
    RunResult,
)
from tests.fakes import FakeShell

_PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKIykuTed7zNwJwn20eCelcKcHKJ9c/pGFfvulRWazuC"
_IDENTITY = HostIdentity(_PUBLIC_KEY, "SHA256:JfCon51dCgE/yWGkyroh3Ne+ONLMm6QmHMQnEoPSLx0")

_BASE_VALUES = {
    "hostname": "office-panel",
    "model": "Brilliant Control Development Board",
    "architecture": "armv7l",
    "firmware": "v26.06.03.1",
    "python_version": "Python 3.10.9",
    "init_system": "systemd 247",
    "available_kib": "131072",
    "available_memory_kib": "65536",
    "installed_agent_version": "0.6.0",
    "service_brilliant_mqtt": "active",
    "service_brilliant_voice": "inactive",
    "service_brilliant_wifi_watchdog": "active",
    "service_brilliant_bus_watchdog": "active",
    "service_brilliant_hue_ca_timer": "inactive",
    "service_brilliant_ha_mirror": "inactive",
    "toolchain_mv_no_clobber": "1",
    "toolchain_mv_no_target_directory": "1",
    "toolchain_stat_mode": "1",
    "toolchain_grep_fixed_exact_quiet": "1",
    "toolchain_grep_extended_count": "1",
    "toolchain_find_print_quit": "1",
    "toolchain_sha256sum": "1",
    "toolchain_python_3_10": "1",
    "toolchain_systemd_manager": "1",
    "toolchain_systemd_is_active": "1",
    "toolchain_systemd_is_enabled": "1",
}

_TOOLCHAIN_FAILURES = (
    ("toolchain_mv_no_clobber", "mv_no_clobber"),
    ("toolchain_mv_no_target_directory", "mv_no_target_directory"),
    ("toolchain_stat_mode", "stat_mode"),
    ("toolchain_grep_fixed_exact_quiet", "grep_fixed_exact_quiet"),
    ("toolchain_grep_extended_count", "grep_extended_count"),
    ("toolchain_find_print_quit", "find_print_quit"),
    ("toolchain_sha256sum", "sha256sum"),
    ("toolchain_python_3_10", "python_3_10"),
    ("toolchain_systemd_manager", "systemd_manager"),
    ("toolchain_systemd_is_active", "systemd_is_active"),
    ("toolchain_systemd_is_enabled", "systemd_is_enabled"),
)


def _output(**overrides: str) -> str:
    values = {**_BASE_VALUES, **overrides}
    return "".join(f"{key}={value}\n" for key, value in values.items())


def _run_file_probe(probe: str, source_path: str, path: Path) -> subprocess.CompletedProcess[str]:
    command = probe.replace(panel_inspection._PANEL_PYTHON, shlex.quote(sys.executable)).replace(
        source_path, str(path)
    )
    return subprocess.run(
        ["/bin/sh", "-c", command],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )


async def _connected_shell(
    result: RunResult | None = None,
    *,
    pinned: str = _PUBLIC_KEY,
) -> FakeShell:
    shell = FakeShell(
        responses={PANEL_INSPECTION_COMMAND: result or RunResult(0, _output(), "")},
        pinned=pinned,
    )
    await shell.connect()
    return shell


async def test_inspection_returns_strict_facts_from_one_read_only_command() -> None:
    shell = await _connected_shell(
        RunResult(
            0,
            _output(
                service_brilliant_voice="active",
                service_brilliant_ha_mirror="active",
            ),
            "",
        )
    )

    facts = await async_inspect_panel(shell, _IDENTITY)

    assert facts == PanelFacts(
        fingerprint=_IDENTITY.fingerprint,
        hostname="office-panel",
        model="Brilliant Control Development Board",
        architecture="armv7l",
        firmware="v26.06.03.1",
        python_version="3.10.9",
        init_system="systemd 247",
        available_bytes=128 * 1024 * 1024,
        available_memory_bytes=64 * 1024 * 1024,
        installed_agent_version="0.6.0",
        active_services=(
            "brilliant-mqtt",
            "brilliant-voice",
            "brilliant-wifi-watchdog",
            "brilliant-bus-watchdog",
        ),
        conflicting_services=("brilliant-ha-mirror",),
    )
    assert shell.commands == [PANEL_INSPECTION_COMMAND]
    assert shell.uploads == []
    assert shell.dir_uploads == []
    assert shell.file_uploads == []


def test_inspection_command_uses_fixed_read_only_compatibility_sources() -> None:
    required_fragments = (
        "hostname",
        "/sys/firmware/devicetree/base/model",
        "/etc/release_info.json",
        "release_tag",
        "/data/switch-embedded/env/bin/python3 -B -c",
        "uname -m",
        "/data/switch-embedded/env/bin/python3 --version",
        "systemctl --version",
        "df -Pk /var",
        "/proc/meminfo",
        "/var/brilliant-mqtt/VERSION",
        "systemctl is-active brilliant-mqtt",
        "systemctl is-active brilliant-voice",
        "systemctl is-active brilliant-wifi-watchdog",
        "systemctl is-active brilliant-bus-watchdog",
        "systemctl is-active brilliant-hue-ca.timer",
        "systemctl is-active brilliant-ha-mirror",
        "/usr/bin/mv.coreutils --help",
        "stat -c %a -- /dev/null",
        "grep -Fxq",
        "grep -Ec --",
        "find /dev/null -type l -print -quit",
        "/usr/bin/sha256sum -- /dev/null",
        "systemctl is-system-running",
        "systemctl is-enabled brilliant-mqtt",
    )
    assert all(fragment in PANEL_INSPECTION_COMMAND for fragment in required_fragments)
    assert "\n" not in PANEL_INSPECTION_COMMAND
    assert PANEL_INSPECTION_COMMAND.endswith("} 2>&1 | head -c 16385")
    assert ".read(4097)" in PANEL_INSPECTION_COMMAND
    assert PANEL_INSPECTION_COMMAND.count(".read(129)") == 2
    assert 'raw[:-1] if raw.endswith(b"\\x00") else raw' in PANEL_INSPECTION_COMMAND
    assert "tr -d" not in PANEL_INSPECTION_COMMAND
    assert not any(
        mutating in PANEL_INSPECTION_COMMAND
        for mutating in (
            " install ",
            " rm ",
            " mv ",
            " cp ",
            " chmod ",
            " chown ",
            "systemctl start",
            "systemctl enable",
            "systemctl restart",
            "systemctl stop",
        )
    )


@pytest.mark.parametrize(("wire_key", "capability"), _TOOLCHAIN_FAILURES)
async def test_missing_toolchain_capability_has_one_allowlisted_redacted_identity(
    wire_key: str,
    capability: str,
) -> None:
    shell = await _connected_shell(RunResult(0, _output(**{wire_key: "0"}), ""))

    with pytest.raises(PanelCompatibilityError) as raised:
        await async_inspect_panel(shell, _IDENTITY)

    assert raised.value.code == "unsupported_panel_toolchain"
    assert raised.value.capability == capability
    assert str(raised.value) == "unsupported_panel_toolchain"
    assert raised.value.__context__ is None
    assert shell.uploads == []
    assert shell.dir_uploads == []
    assert shell.file_uploads == []


@pytest.mark.parametrize("wire_value", ("", "2", "true", "01"))
async def test_malformed_toolchain_result_is_not_a_capability_diagnostic(
    wire_value: str,
) -> None:
    shell = await _connected_shell(RunResult(0, _output(toolchain_mv_no_clobber=wire_value), ""))

    with pytest.raises(PanelCompatibilityError) as raised:
        await async_inspect_panel(shell, _IDENTITY)

    assert raised.value.code == "inspection_output_invalid"
    assert raised.value.capability is None


def test_toolchain_error_rejects_non_allowlisted_capability_identity() -> None:
    with pytest.raises(ValueError, match="^invalid_panel_compatibility_error_code$"):
        PanelCompatibilityError(
            "unsupported_panel_toolchain",
            capability="SECRET-untrusted-tool-output",
        )


def test_non_toolchain_error_rejects_any_capability_identity() -> None:
    with pytest.raises(ValueError, match="^invalid_panel_compatibility_error_code$"):
        PanelCompatibilityError(
            "inspection_failed",
            capability="SECRET-untrusted-tool-output",
        )


async def test_missing_optional_agent_version_is_explicitly_unknown() -> None:
    shell = await _connected_shell(RunResult(0, _output(installed_agent_version=""), ""))

    facts = await async_inspect_panel(shell, _IDENTITY)

    assert facts.installed_agent_version is None


async def test_invalid_nonempty_agent_version_wire_value_fails_closed() -> None:
    shell = await _connected_shell(
        RunResult(0, _output(installed_agent_version="partial-version"), "")
    )

    with pytest.raises(PanelCompatibilityError) as raised:
        await async_inspect_panel(shell, _IDENTITY)

    assert raised.value.code == "inspection_output_invalid"


@pytest.mark.parametrize(
    "raw",
    (
        b"\xff",
        b"0.6",
        b"partial-version",
        b"0.6.0\npartial",
        b"x" * 129,
    ),
)
def test_agent_version_probe_reports_corrupt_bounded_files_as_unknown(
    tmp_path: Path,
    raw: bytes,
) -> None:
    version_path = tmp_path / "VERSION"
    version_path.write_bytes(raw)

    result = _run_file_probe(
        panel_inspection._AGENT_VERSION_PROBE,
        PANEL_VERSION_FILE,
        version_path,
    )

    assert result.returncode == 0
    assert result.stdout == "installed_agent_version=\n"
    assert result.stderr == ""


@pytest.mark.parametrize(
    ("terminator", "expected"),
    (
        (b"\x00", "model=Brilliant Control Development Board\n"),
        (b"", "model=Brilliant Control Development Board\n"),
        (b"\x00\x00", "model=\n"),
        (b"\n", "model=\n"),
        (b"\r\n", "model=\n"),
    ),
)
def test_model_probe_removes_at_most_one_device_tree_nul(
    tmp_path: Path,
    terminator: bytes,
    expected: str,
) -> None:
    model_path = tmp_path / "model"
    model_path.write_bytes(b"Brilliant Control Development Board" + terminator)

    result = _run_file_probe(
        panel_inspection._MODEL_PROBE,
        panel_inspection._MODEL_PATH,
        model_path,
    )

    assert result.returncode == 0
    assert result.stdout == expected
    assert result.stderr == ""


@pytest.mark.parametrize(
    "model",
    (
        "Future Brilliant Panel rev-42",
        "  Future Brilliant Panel  ",
    ),
)
async def test_safe_unfamiliar_model_round_trips_as_descriptive_text(model: str) -> None:
    shell = await _connected_shell(RunResult(0, _output(model=model), ""))

    facts = await async_inspect_panel(shell, _IDENTITY)

    assert facts.model == model


@pytest.mark.parametrize(
    "model",
    (
        "",
        "   ",
        "x" * 129,
        "bad\x7fmodel",
        "bad\x01model",
    ),
)
async def test_malformed_model_fails_as_invalid_inspection_output(model: str) -> None:
    shell = await _connected_shell(RunResult(0, _output(model=model), ""))

    with pytest.raises(PanelCompatibilityError) as raised:
        await async_inspect_panel(shell, _IDENTITY)

    assert raised.value.code == "inspection_output_invalid"


def test_firmware_probe_returns_live_valid_release_tag(tmp_path: Path) -> None:
    release_info_path = tmp_path / "release_info.json"
    release_info_path.write_bytes(b'{"release_tag":"v26.07.15.1"}')

    result = _run_file_probe(
        panel_inspection._FIRMWARE_PROBE,
        panel_inspection._RELEASE_INFO_PATH,
        release_info_path,
    )

    assert result.returncode == 0
    assert result.stdout == "firmware=v26.07.15.1\n"
    assert result.stderr == ""


def test_firmware_probe_reports_missing_file_as_unknown(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing-release-info.json"

    result = _run_file_probe(
        panel_inspection._FIRMWARE_PROBE,
        panel_inspection._RELEASE_INFO_PATH,
        missing_path,
    )

    assert result.returncode == 0
    assert result.stdout == "firmware=\n"
    assert result.stderr == ""


@pytest.mark.parametrize(
    "raw",
    (
        b"{",
        b'{"release_tag":"\xff"}',
        b'["v26.07.15.1"]',
        b"{}",
        b'{"release_tag":26}',
        b'{"release_tag":"v26.07.15.1\\u0000"}',
        b'{"release_tag":"v26.07.15.\\u00e9"}',
        b'{"release_tag":"' + (b"x" * 65) + b'"}',
        b'{"release_tag":"v26.07.15.1"}'.ljust(4097, b" "),
    ),
)
def test_firmware_probe_reports_malformed_or_oversized_file_as_unknown(
    tmp_path: Path,
    raw: bytes,
) -> None:
    release_info_path = tmp_path / "release_info.json"
    release_info_path.write_bytes(raw)

    result = _run_file_probe(
        panel_inspection._FIRMWARE_PROBE,
        panel_inspection._RELEASE_INFO_PATH,
        release_info_path,
    )

    assert result.returncode == 0
    assert result.stdout == "firmware=\n"
    assert result.stderr == ""


@pytest.mark.parametrize(
    ("override", "code"),
    (
        ({"architecture": "aarch64"}, "unsupported_architecture"),
        ({"firmware": ""}, "firmware_unknown"),
        ({"firmware": "2174d3882504"}, "firmware_unknown"),
        ({"python_version": "Python 3.9.18"}, "unsupported_python"),
        ({"python_version": "not-python"}, "unsupported_python"),
        ({"available_kib": "65535"}, "insufficient_storage"),
        ({"available_memory_kib": "32767"}, "insufficient_memory"),
    ),
)
async def test_unsupported_or_undersized_panel_has_stable_code(
    override: dict[str, str],
    code: str,
) -> None:
    shell = await _connected_shell(RunResult(0, _output(**override), ""))

    with pytest.raises(PanelCompatibilityError) as raised:
        await async_inspect_panel(shell, _IDENTITY)

    assert raised.value.code == code
    assert str(raised.value) == code


@pytest.mark.parametrize(
    "stdout",
    (
        _output() + "unknown=value\n",
        _output() + "hostname=duplicate\n",
        _output().replace("hostname=office-panel\n", ""),
        _output(available_kib="-1"),
        _output(available_kib="9" * 5000),
        _output(available_memory_kib="lots"),
        _output(service_brilliant_mqtt="surprising"),
        _output(hostname="bad\x00host"),
        _output(hostname="\ud800"),
    ),
)
async def test_malformed_output_fails_closed_without_echoing_it(stdout: str) -> None:
    shell = await _connected_shell(RunResult(0, stdout, ""))

    with pytest.raises(PanelCompatibilityError) as raised:
        await async_inspect_panel(shell, _IDENTITY)

    assert raised.value.code == "inspection_output_invalid"
    assert stdout not in str(raised.value)


@pytest.mark.parametrize(
    "result",
    (
        RunResult(1, _output(), ""),
        RunResult(0, _output(), "unexpected stderr"),
        RunResult(0, "x" * 16385, ""),
    ),
)
async def test_failed_or_unbounded_command_output_fails_closed(result: RunResult) -> None:
    shell = await _connected_shell(result)

    with pytest.raises(PanelCompatibilityError) as raised:
        await async_inspect_panel(shell, _IDENTITY)

    assert raised.value.code in {"inspection_failed", "inspection_output_invalid"}


async def test_missing_python_takes_precedence_over_dependent_firmware_probe() -> None:
    shell = await _connected_shell(
        RunResult(
            0,
            _output(
                model="",
                firmware="",
                python_version="",
                toolchain_python_3_10="0",
            ),
            "",
        )
    )

    with pytest.raises(PanelCompatibilityError) as raised:
        await async_inspect_panel(shell, _IDENTITY)

    assert raised.value.code == "unsupported_panel_toolchain"
    assert raised.value.capability == "python_3_10"


async def test_fixed_panel_python_newer_than_3_10_is_compatible() -> None:
    shell = await _connected_shell(RunResult(0, _output(python_version="Python 3.11.8"), ""))

    facts = await async_inspect_panel(shell, _IDENTITY)

    assert facts.python_version == "3.11.8"


class _BlockingShell(FakeShell):
    async def run(self, command: str) -> RunResult:
        self._require_connected()
        self.commands.append(command)
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


async def test_inspection_has_a_hard_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(panel_inspection, "_INSPECTION_TIMEOUT", 0.01)
    shell = _BlockingShell(pinned=_PUBLIC_KEY)
    await shell.connect()

    with pytest.raises(PanelCompatibilityError) as raised:
        await async_inspect_panel(shell, _IDENTITY)

    assert raised.value.code == "inspection_timeout"
    assert raised.value.__context__ is None
    assert shell.commands == [PANEL_INSPECTION_COMMAND]


async def test_inspection_transport_failure_is_stable_and_context_free() -> None:
    secret_failure = RuntimeError("secret-bearing SSH diagnostic")
    shell = FakeShell(
        run_errors={PANEL_INSPECTION_COMMAND: secret_failure},
        pinned=_PUBLIC_KEY,
    )
    await shell.connect()

    with pytest.raises(PanelCompatibilityError) as raised:
        await async_inspect_panel(shell, _IDENTITY)

    assert raised.value.code == "inspection_failed"
    assert str(raised.value) == "inspection_failed"
    assert str(secret_failure) not in str(raised.value)
    assert raised.value.__context__ is None


async def test_inspection_preserves_cancellation() -> None:
    class CancellingShell(FakeShell):
        async def run(self, command: str) -> RunResult:
            self._require_connected()
            self.commands.append(command)
            raise asyncio.CancelledError

    shell = CancellingShell(pinned=_PUBLIC_KEY)
    await shell.connect()

    with pytest.raises(asyncio.CancelledError):
        await async_inspect_panel(shell, _IDENTITY)


async def test_inspection_rejects_shell_for_a_different_identity_before_command() -> None:
    shell = await _connected_shell(pinned="ssh-ed25519 DIFFERENT")

    with pytest.raises(PanelIdentityError) as raised:
        await async_inspect_panel(shell, _IDENTITY)

    assert raised.value.code == "host_key_changed"
    assert shell.commands == []

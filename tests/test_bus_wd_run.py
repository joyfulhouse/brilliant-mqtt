from __future__ import annotations

import errno
import json
import logging
import os
from types import SimpleNamespace
from typing import Any, NoReturn

import pytest

from brilliant_bus_watchdog import bounded
from brilliant_bus_watchdog.reboot_guard import GuardPolicy, RebootGuard
from brilliant_bus_watchdog.run import (
    _service_active,
    _service_started_at,
    handle,
    load_config,
    should_reboot,
)

STATE = "/var/brilliant-mqtt/bus-watchdog.state"


@pytest.mark.parametrize(
    ("age", "bridge_active", "gateway_up", "bus_failure_age", "expected"),
    [
        (1900.0, True, True, 1900.0, True),
        (100.0, True, True, 1900.0, False),
        (9999.0, False, True, 9999.0, False),
        (9999.0, True, False, 9999.0, False),
        (9999.0, True, True, None, False),
        (9999.0, True, True, 1799.9, False),
    ],
)
def test_should_reboot_requires_every_bus_wedge_signal(
    age: float,
    bridge_active: bool,
    gateway_up: bool,
    bus_failure_age: float | None,
    expected: bool,
) -> None:
    assert (
        should_reboot(
            age=age,
            bridge_active=bridge_active,
            gateway_up=gateway_up,
            bus_failure_age=bus_failure_age,
            stale_after=1800.0,
        )
        is expected
    )


def test_handle_reboots_when_guard_allows_record_before_reboot() -> None:
    calls: list[str] = []

    class G:
        last_read_bad = False

        def can_reboot(self, now: float) -> bool:
            return True

        def record(self, now: float) -> None:
            calls.append("record")

    handle(
        should=True, guard=G(), now=1.0, state_path=STATE, reboot_fn=lambda: calls.append("reboot")
    )
    assert calls == ["record", "reboot"]


def test_handle_blocked_by_guard() -> None:
    calls: list[str] = []

    class G:
        last_read_bad = False

        def can_reboot(self, now: float) -> bool:
            return False

        def record(self, now: float) -> None:
            calls.append("record")

    handle(
        should=True, guard=G(), now=1.0, state_path=STATE, reboot_fn=lambda: calls.append("reboot")
    )
    assert calls == []


def test_handle_noop_when_should_false() -> None:
    calls: list[str] = []

    class G:
        last_read_bad = False

        def can_reboot(self, now: float) -> bool:
            calls.append("checked")
            return True

        def record(self, now: float) -> None:
            calls.append("record")

    handle(
        should=False, guard=G(), now=1.0, state_path=STATE, reboot_fn=lambda: calls.append("reboot")
    )
    assert calls == []


def test_load_config_defaults() -> None:
    c = load_config({})
    assert c.interval == 60.0 and c.stale_after == 1800.0
    assert c.heartbeat_path == "/run/brilliant-mqtt/bus-heartbeat"
    assert c.state_path == "/var/brilliant-mqtt/bus-watchdog.state"
    assert c.bridge_service == "brilliant-mqtt"


def test_load_config_overrides() -> None:
    c = load_config({"BUS_WATCHDOG_STALE_AFTER": "600", "BUS_HEARTBEAT_FILE": "/x"})
    assert c.stale_after == 600.0 and c.heartbeat_path == "/x"


@pytest.mark.parametrize(
    ("environ", "expected"),
    [({}, "/run/brilliant-mqtt/bus-phase"), ({"BUS_PHASE_FILE": "/phase"}, "/phase")],
)
def test_load_config_bus_phase_path(environ: dict[str, str], expected: str) -> None:
    assert load_config(environ).phase_path == expected


@pytest.mark.parametrize("state", ["active", "activating"])
def test_service_active_true_while_running_or_restarting(state: str) -> None:
    def run(argv: list[str]) -> SimpleNamespace:
        return SimpleNamespace(stdout=f"{state}\n")

    assert _service_active("brilliant-mqtt", run=run) is True


def test_service_active_false_when_stdout_inactive() -> None:
    def run(argv: list[str]) -> SimpleNamespace:
        return SimpleNamespace(stdout="inactive")

    assert _service_active("brilliant-mqtt", run=run) is False


def test_service_active_false_when_runner_raises_oserror() -> None:
    def runner(argv: list[str]) -> NoReturn:
        raise OSError("systemctl not found")

    assert _service_active("brilliant-mqtt", run=runner) is False


def test_service_active_default_runner_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def spy(argv: Any, *, timeout: float, capture: bool = False) -> bounded.Completed:
        calls.append({"argv": list(argv), "timeout": timeout, "capture": capture})
        return bounded.Completed(returncode=0, stdout="active\n", timed_out=False)

    monkeypatch.setattr(bounded, "run_bounded", spy)
    assert _service_active("brilliant-mqtt") is True
    assert calls[0]["argv"] == ["systemctl", "is-active", "brilliant-mqtt"]
    assert calls[0]["timeout"] > 0
    assert calls[0]["capture"] is True


def test_service_active_timeout_reads_as_inactive(monkeypatch: pytest.MonkeyPatch) -> None:
    """A hung `systemctl is-active` must not read as active — that would let the
    watchdog believe the bridge is up when it cannot actually tell."""

    def spy(argv: Any, *, timeout: float, capture: bool = False) -> bounded.Completed:
        return bounded.Completed(returncode=bounded.TIMEOUT_RC, stdout="", timed_out=True)

    monkeypatch.setattr(bounded, "run_bounded", spy)
    assert _service_active("brilliant-mqtt") is False


def test_service_start_generation_uses_systemd_monotonic_timestamp() -> None:
    calls: list[list[str]] = []

    def run(argv: list[str]) -> SimpleNamespace:
        calls.append(argv)
        return SimpleNamespace(stdout="ExecMainStartTimestampMonotonic=1901000000\n")

    assert _service_started_at("brilliant-mqtt", run=run) == 1901.0
    assert calls == [
        [
            "systemctl",
            "show",
            "--property=ExecMainStartTimestampMonotonic",
            "brilliant-mqtt",
        ]
    ]


def test_empty_service_start_generation_disables_reboot_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def run(argv: list[str]) -> SimpleNamespace:
        del argv
        return SimpleNamespace(stdout="")

    with caplog.at_level(logging.WARNING, logger="brilliant_bus_watchdog"):
        started_at = _service_started_at("brilliant-mqtt", run=run)

    assert started_at is None
    assert not should_reboot(
        age=1900.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=None,
        stale_after=1800.0,
    )
    assert any(
        "ExecMainStartTimestampMonotonic unavailable" in record.getMessage()
        and "reboot guard disabled" in record.getMessage()
        for record in caplog.records
    )


def _write_failures(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        r
        for r in caplog.records
        if r.levelno == logging.ERROR and "state write failed" in r.getMessage()
    ]


def test_bad_state_write_failure_reboots_but_rearms_after_restart(
    tmp_path: Any, caplog: pytest.LogCaptureFixture
) -> None:
    state = tmp_path / "guard"
    state.mkdir()
    policy = GuardPolicy(cooldown=10.0, cap=3, window=60.0)
    guard = RebootGuard(str(state), policy)
    assert guard.can_reboot(0.0) is False
    reboots: list[str] = []
    with caplog.at_level(logging.ERROR, logger="brilliant_bus_watchdog"):
        handle(
            should=True,
            guard=guard,
            now=10.0,
            state_path=str(state),
            reboot_fn=lambda: reboots.append("reboot"),
        )
    assert reboots == ["reboot"]
    assert "IsADirectoryError" in _write_failures(caplog)[0].getMessage()
    assert str(state) in _write_failures(caplog)[0].getMessage()
    fresh = RebootGuard(str(state), policy)
    assert fresh.can_reboot(20.0) is False
    assert fresh.can_reboot(30.0) is True


@pytest.mark.parametrize("stale", [None, [100.0]], ids=["missing", "readable"])
@pytest.mark.parametrize("err", [errno.EACCES, errno.ENOSPC], ids=["eacces", "enospc"])
def test_readable_or_missing_state_write_failure_refuses_across_boots(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    stale: list[float] | None,
    err: int,
) -> None:
    state = tmp_path / "guard"
    if stale is not None:
        state.write_text(json.dumps(stale), encoding="utf-8")

    def fail_replace(src: str, dst: str) -> None:
        if err == errno.ENOSPC:
            raise OSError(err, os.strerror(err))
        raise OSError(err, os.strerror(err), src)

    monkeypatch.setattr(os, "replace", fail_replace)
    reboots: list[str] = []
    with caplog.at_level(logging.ERROR, logger="brilliant_bus_watchdog"):
        for boot in range(4):
            now = 1000.0 + boot * 20
            guard = RebootGuard(str(state), GuardPolicy(cooldown=10.0))
            handle(
                should=True,
                guard=guard,
                now=now,
                state_path=str(state),
                reboot_fn=lambda: reboots.append("reboot"),
            )
    assert reboots == []
    failures = _write_failures(caplog)
    assert len(failures) == 4
    assert all(str(state) in record.getMessage() for record in failures)
    if err == errno.ENOSPC:
        assert all("OSError:" in record.getMessage() for record in failures)
        assert all(record.getMessage().endswith(f"OSError: {state})") for record in failures)


def test_last_read_bad_tracks_latest_authorizing_read(tmp_path: Any) -> None:
    state = tmp_path / "guard"
    state.write_text("invalid", encoding="utf-8")
    guard = RebootGuard(str(state), GuardPolicy(cooldown=10.0))
    assert guard.can_reboot(0.0) is False
    assert guard.last_read_bad is True
    state.write_text("[]", encoding="utf-8")
    assert guard.can_reboot(1.0) is False
    assert guard.last_read_bad is False


def test_future_stamp_is_a_bad_authorizing_read(tmp_path: Any) -> None:
    state = tmp_path / "guard"
    state.write_text("[999999.0]", encoding="utf-8")
    guard = RebootGuard(str(state), GuardPolicy(cooldown=10.0))
    assert guard.can_reboot(1.0) is False
    assert guard.last_read_bad is True


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
def test_real_read_only_directory_refuses_reboot(tmp_path: Any) -> None:
    directory = tmp_path / "readonly"
    directory.mkdir()
    state = directory / "guard"
    state.write_text("[]", encoding="utf-8")
    directory.chmod(0o555)
    try:
        reboots: list[str] = []
        for boot in range(3):
            now = 10000.0 + boot * 20
            guard = RebootGuard(str(state), GuardPolicy())
            assert guard.can_reboot(now) is True
            handle(
                should=True,
                guard=guard,
                now=now,
                state_path=str(state),
                reboot_fn=lambda: reboots.append("reboot"),
            )
        assert reboots == []
    finally:
        directory.chmod(0o755)


@pytest.mark.parametrize(
    ("stdout", "returncode"),
    [
        pytest.param(
            "ExecMainStartTimestampMonotonic=garbage",
            0,
            id="non-integer-value",
        ),
        pytest.param("ExecMainStartTimestampMonotonic=0", 0, id="zero-value"),
        pytest.param("ExecMainStartTimestampMonotonic=-1", 0, id="negative-value"),
        pytest.param(
            "ExecMainStartTimestampMonotonic=1901000000",
            1,
            id="systemctl-failure",
        ),
    ],
)
def test_unknown_service_start_generation_fails_closed(
    stdout: str,
    returncode: int,
) -> None:
    def run(argv: list[str]) -> SimpleNamespace:
        del argv
        return SimpleNamespace(stdout=stdout, returncode=returncode)

    started_at = _service_started_at("brilliant-mqtt", run=run)

    assert started_at is None
    assert not should_reboot(
        age=1900.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=started_at,
        stale_after=1800.0,
    )

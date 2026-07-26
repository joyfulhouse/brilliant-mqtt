"""Behavioral hardening checks for the disposable Mosquitto runner."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
RUNNER = ROOT / "scripts/run_mqtt_validation_tests.sh"
SUFFIX = "0123456789abcdef"
PASSWORD_NAME = f"brilliant-mqtt-live-{SUFFIX}-passwd"
PLAIN_NAME = f"brilliant-mqtt-live-{SUFFIX}-plain"
DENY_NAME = f"brilliant-mqtt-live-{SUFFIX}-deny"
MISMATCH_NAME = f"brilliant-mqtt-live-{SUFFIX}-mismatch"
TLS_NAME = f"brilliant-mqtt-live-{SUFFIX}-tls"
ALL_NAMES = [TLS_NAME, MISMATCH_NAME, DENY_NAME, PLAIN_NAME, PASSWORD_NAME]


@dataclass(frozen=True, slots=True)
class _RunnerHarness:
    runtime_base: Path
    state_dir: Path
    environment: dict[str, str]

    def run(
        self,
        *,
        pytest_status: int = 0,
        fail_stage: str = "",
        signal_on_create: str = "",
        signal_on_inspect: str = "",
        timeout: float = 10,
    ) -> subprocess.CompletedProcess[str]:
        environment = self.environment | {
            "FAKE_PYTEST_STATUS": str(pytest_status),
            "FAKE_DOCKER_FAIL_STAGE": fail_stage,
            "FAKE_SIGNAL_ON_CREATE": signal_on_create,
            "FAKE_SIGNAL_ON_INSPECT": signal_on_inspect,
        }
        return subprocess.run(
            [str(RUNNER)],
            cwd=ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def removed_names(self) -> list[str]:
        removed = self.state_dir / "removed"
        return removed.read_text(encoding="utf-8").splitlines() if removed.exists() else []

    def assert_isolated_cleanup(self) -> None:
        assert list(self.runtime_base.glob("brilliant-mqtt-live.*")) == []
        assert (self.state_dir / "foreign").read_text(encoding="utf-8") == "untouched\n"
        assert not (self.state_dir / "foreign-deleted").exists()


@pytest.fixture
def runner_harness(tmp_path: Path) -> _RunnerHarness:
    runtime_base = tmp_path / "runtime"
    runtime_base.mkdir()
    state_dir = tmp_path / "docker-state"
    state_dir.mkdir()
    (state_dir / "containers").mkdir()
    (state_dir / "foreign").write_text("untouched\n", encoding="utf-8")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()

    _write_executable(
        fake_bin / "openssl",
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "rand" ]]; then
    printf '%s\n' "0123456789abcdef"
fi
""",
    )
    _write_executable(
        fake_bin / "sleep",
        """#!/usr/bin/env bash
exit 0
""",
    )
    _write_executable(
        fake_bin / "uv",
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >"${FAKE_DOCKER_STATE}/pytest-args"
exit "${FAKE_PYTEST_STATUS:-0}"
""",
    )
    _write_executable(
        fake_bin / "docker",
        """#!/usr/bin/env bash
set -euo pipefail
state="${FAKE_DOCKER_STATE:?}"
command_name="${1:?}"
shift

container_file() {
    printf '%s/containers/%s' "${state}" "$1"
}

case "${command_name}" in
    create)
        exact_name=""
        cid_file=""
        writable_source=""
        original_args="$*"
        while (($#)); do
            case "$1" in
                --name)
                    exact_name="$2"
                    shift 2
                    ;;
                --cidfile)
                    cid_file="$2"
                    shift 2
                    ;;
                --mount)
                    mount_spec="$2"
                    if [[ "${mount_spec}" == *"target=/work"* ]]; then
                        writable_source="${mount_spec#*source=}"
                        writable_source="${writable_source%%,target=*}"
                    fi
                    shift 2
                    ;;
                *)
                    shift
                    ;;
            esac
        done
        [[ -n "${exact_name}" ]]
        printf '%s\n' "${original_args}" >>"${state}/create-args"
        if [[ -f "$(container_file "${exact_name}")" ]]; then
            exit 76
        fi
        if [[ "${FAKE_DOCKER_FAIL_STAGE:-}" == "create" ]]; then
            exit 70
        fi
        count=1
        if [[ -f "${state}/create-count" ]]; then
            count=$(( $(<"${state}/create-count") + 1 ))
        fi
        printf '%s\n' "${count}" >"${state}/create-count"
        cid="$(printf '%064x' "${count}")"
        printf '%s\n%s\n' "${cid}" "${writable_source}" >"$(container_file "${exact_name}")"
        if [[ -n "${cid_file}" ]]; then
            printf '%s' "${cid}" >"${cid_file}"
        fi
        if [[ "${FAKE_DOCKER_FAIL_STAGE:-}" == "create_after_record" ]]; then
            exit 75
        fi
        if [[ -n "${FAKE_SIGNAL_ON_CREATE:-}" && ! -e "${state}/signal-sent" ]]; then
            : >"${state}/signal-sent"
            candidate="${PPID}"
            target="${candidate}"
            while [[ "${candidate}" =~ ^[0-9]+$ ]] && ((candidate > 1)); do
                command_line="$(ps -o command= -p "${candidate}" || true)"
                if [[ "${command_line}" == *"run_mqtt_validation_tests.sh"* ]]; then
                    target="${candidate}"
                else
                    break
                fi
                candidate="$(ps -o ppid= -p "${candidate}" | tr -d ' ')"
            done
            kill -s "${FAKE_SIGNAL_ON_CREATE}" "${target}"
        fi
        ;;
    start)
        exact_name="${!#}"
        if [[ "${FAKE_DOCKER_FAIL_STAGE:-}" == "start" ]]; then
            exit 71
        fi
        if [[ "${exact_name}" == *"-passwd" ]]; then
            writable_source="$(sed -n '2p' "$(container_file "${exact_name}")")"
            mkdir -p -- "${writable_source}"
            printf '%s\n' "fake-password-database" >"${writable_source}/passwords"
        fi
        ;;
    inspect)
        exact_name="${!#}"
        record="$(container_file "${exact_name}")"
        [[ -f "${record}" ]]
        if [[ "$*" == *".State.ExitCode"* ]]; then
            printf '%s\n' "0"
        elif [[ "$*" == *".NetworkSettings.Ports"* ]]; then
            case "${exact_name}" in
                *-plain) printf '%s\n' "21001" ;;
                *-deny) printf '%s\n' "21002" ;;
                *-mismatch) printf '%s\n' "21003" ;;
                *-tls) printf '%s\n' "21004" ;;
                *) exit 72 ;;
            esac
        elif [[ "$*" == *".Id"* ]]; then
            if [[ -n "${FAKE_SIGNAL_ON_INSPECT:-}" && ! -e "${state}/inspect-signal-sent" ]]; then
                : >"${state}/inspect-signal-sent"
                candidate="${PPID}"
                target="${candidate}"
                while [[ "${candidate}" =~ ^[0-9]+$ ]] && ((candidate > 1)); do
                    command_line="$(ps -o command= -p "${candidate}" || true)"
                    if [[ "${command_line}" == *"run_mqtt_validation_tests.sh"* ]]; then
                        target="${candidate}"
                    else
                        break
                    fi
                    candidate="$(ps -o ppid= -p "${candidate}" | tr -d ' ')"
                done
                kill -s "${FAKE_SIGNAL_ON_INSPECT}" "${target}"
                exit 77
            fi
            sed -n '1p' "${record}"
        else
            exit 72
        fi
        ;;
    exec)
        if [[ "${FAKE_DOCKER_FAIL_STAGE:-}" == "readiness" ]]; then
            exit 72
        fi
        ;;
    logs)
        : >"${state}/logs-called"
        ;;
    rm)
        exact_name="${!#}"
        printf '%s\n' "${exact_name}" >>"${state}/removed"
        if [[ "${exact_name}" == "pre-existing-foreign" ]]; then
            : >"${state}/foreign-deleted"
            exit 73
        fi
        rm -f -- "$(container_file "${exact_name}")"
        ;;
    *)
        exit 74
        ;;
esac
""",
    )
    environment = os.environ.copy()
    environment["PATH"] = f"{fake_bin}:{environment['PATH']}"
    environment["TMPDIR"] = str(runtime_base)
    environment["FAKE_DOCKER_STATE"] = str(state_dir)
    return _RunnerHarness(runtime_base, state_dir, environment)


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def test_suffix_generation_failure_does_not_leak_runtime_directory(
    tmp_path: Path,
) -> None:
    """A failure before Docker starts must still leave no disposable directory."""
    runtime_base = tmp_path / "runtime"
    runtime_base.mkdir()
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_openssl = fake_bin / "openssl"
    fake_openssl.write_text("#!/bin/sh\nexit 91\n", encoding="utf-8")
    fake_openssl.chmod(0o755)
    environment = os.environ.copy()
    environment["PATH"] = f"{fake_bin}:{environment['PATH']}"
    environment["TMPDIR"] = str(runtime_base)

    result = subprocess.run(
        [str(RUNNER)],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 91
    assert list(runtime_base.glob("brilliant-mqtt-live.*")) == []


def test_fake_runner_success_cleans_only_owned_names(
    runner_harness: _RunnerHarness,
) -> None:
    result = runner_harness.run()

    assert result.returncode == 0
    assert runner_harness.removed_names() == ALL_NAMES
    assert "ha/tests/test_broker_validation_live.py -m mqtt_live -q" in (
        runner_harness.state_dir / "pytest-args"
    ).read_text(encoding="utf-8")
    create_args = (runner_harness.state_dir / "create-args").read_text(encoding="utf-8")
    assert create_args.count("eclipse-mosquitto:2.0.22") == 5
    assert "/password-output,target=/work" in create_args
    assert "source=" in create_args
    runner_harness.assert_isolated_cleanup()


def test_fake_runner_preserves_pytest_failure_status(
    runner_harness: _RunnerHarness,
) -> None:
    result = runner_harness.run(pytest_status=37)

    assert result.returncode == 37
    assert runner_harness.removed_names() == ALL_NAMES
    runner_harness.assert_isolated_cleanup()


@pytest.mark.parametrize(
    ("fail_stage", "expected_status", "expected_removed"),
    [
        ("create", 70, []),
        ("create_after_record", 75, [PASSWORD_NAME]),
        ("start", 71, [PASSWORD_NAME]),
        ("readiness", 1, [PLAIN_NAME, PASSWORD_NAME]),
    ],
)
def test_fake_runner_cleans_exact_owned_names_after_startup_failure(
    runner_harness: _RunnerHarness,
    fail_stage: str,
    expected_status: int,
    expected_removed: list[str],
) -> None:
    result = runner_harness.run(fail_stage=fail_stage, timeout=5)

    assert result.returncode == expected_status
    assert runner_harness.removed_names() == expected_removed
    runner_harness.assert_isolated_cleanup()


def test_fake_runner_preserves_foreign_container_at_generated_name(
    runner_harness: _RunnerHarness,
) -> None:
    foreign_record = runner_harness.state_dir / "containers" / PASSWORD_NAME
    foreign_record.write_text(f"{'f' * 64}\nforeign\n", encoding="utf-8")

    result = runner_harness.run()

    assert result.returncode == 76
    assert runner_harness.removed_names() == []
    assert foreign_record.read_text(encoding="utf-8") == f"{'f' * 64}\nforeign\n"
    runner_harness.assert_isolated_cleanup()


@pytest.mark.parametrize(
    ("signal_name", "expected_status"),
    [("INT", 130), ("TERM", 143), ("HUP", 129)],
)
def test_fake_runner_tracks_create_before_honoring_signal(
    runner_harness: _RunnerHarness,
    signal_name: str,
    expected_status: int,
) -> None:
    result = runner_harness.run(signal_on_create=signal_name)

    assert result.returncode == expected_status
    assert runner_harness.removed_names() == [PASSWORD_NAME]
    runner_harness.assert_isolated_cleanup()


@pytest.mark.parametrize(
    ("signal_name", "expected_status"),
    [("INT", 130), ("TERM", 143), ("HUP", 129)],
)
def test_fake_runner_preserves_signal_when_first_ownership_inspect_fails(
    runner_harness: _RunnerHarness,
    signal_name: str,
    expected_status: int,
) -> None:
    result = runner_harness.run(signal_on_inspect=signal_name)

    assert result.returncode == expected_status
    assert runner_harness.removed_names() == [PASSWORD_NAME]
    runner_harness.assert_isolated_cleanup()

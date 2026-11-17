"""Regression matrix for the BVD pilot runbook's fail-closed shell checks.

The exact snippets are extracted from the runbook and executed with bash
under ``set -euo pipefail`` against stubbed ``systemctl``/``pgrep`` exit
statuses, so a future edit that reintroduces "any unit active" or "pgrep
error means absent" fails here.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

RUNBOOK = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "brilliant-panel"
    / "runbooks"
    / "bvd-current-owner-light-pilot.md"
)
_LOCAL_START = "# One unit per call:"
_LOCAL_END = '  test "$?" -eq 1\nfi\n'
_DESTAGE_START = "    if pgrep -f '[t]ools.brilliant_bvd.live' >/dev/null; then\n"
_DESTAGE_END = "    fi\n"

_SYSTEMCTL_STUB = """#!/bin/sh
test "$1" = is-active || exit 64
unit=$2
case " $STUB_INACTIVE_UNITS " in
  *" $unit "*) echo inactive; exit 3 ;;
esac
if test "$unit" = brilliant-ha-mirror; then
  if test -n "$STUB_MIRROR_STATE"; then echo "$STUB_MIRROR_STATE"; fi
  exit "$STUB_MIRROR_STATUS"
fi
echo active
"""
_PGREP_STUB = '#!/bin/sh\nexit "$STUB_PGREP_STATUS"\n'

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash is required")


def _snippets(start: str, end: str) -> list[str]:
    text = RUNBOOK.read_text(encoding="utf-8")
    found: list[str] = []
    position = text.find(start)
    while position != -1:
        stop = text.index(end, position) + len(end)
        found.append(text[position:stop])
        position = text.find(start, stop)
    return found


def _local_gate() -> str:
    snippets = _snippets(_LOCAL_START, _LOCAL_END)
    assert len(snippets) == 2, "Phase 2.2 and Phase 4 must both carry the gate"
    assert snippets[0] == snippets[1], "Phase 2.2 and Phase 4 gates diverged"
    return snippets[0]


def _destage_gate() -> str:
    snippets = _snippets(_DESTAGE_START, _DESTAGE_END)
    assert len(snippets) == 1
    # The de-stage check is inside a double-quoted remote script.
    return snippets[0].replace('\\"', '"').replace("\\$", "$")


def _run(
    tmp_path: Path,
    snippet: str,
    *,
    inactive_units: str = "",
    mirror_state: str = "inactive",
    mirror_status: int = 3,
    pgrep_status: int = 1,
) -> bool:
    stubs = tmp_path / "stubs"
    stubs.mkdir(exist_ok=True)
    for name, body in (("systemctl", _SYSTEMCTL_STUB), ("pgrep", _PGREP_STUB)):
        stub = stubs / name
        stub.write_text(body, encoding="ascii")
        stub.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{stubs}{os.pathsep}{os.environ.get('PATH', '')}",
        "STUB_INACTIVE_UNITS": inactive_units,
        "STUB_MIRROR_STATE": mirror_state,
        "STUB_MIRROR_STATUS": str(mirror_status),
        "STUB_PGREP_STATUS": str(pgrep_status),
    }
    completed = subprocess.run(
        ["bash", "-c", f"set -euo pipefail\n{snippet}\necho GATE_PASSED\n"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    passed = completed.returncode == 0 and completed.stdout.splitlines()[-1:] == ["GATE_PASSED"]
    assert passed or "GATE_PASSED" not in completed.stdout
    return passed


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, True),
        ({"mirror_state": "unknown"}, True),
        ({"inactive_units": "message_bus"}, False),
        ({"inactive_units": "switch_ui_app"}, False),
        ({"inactive_units": "brilliant-mqtt"}, False),
        ({"mirror_state": "active", "mirror_status": 0}, False),
        ({"mirror_state": "failed"}, False),
        ({"mirror_state": "", "mirror_status": 1}, False),
        ({"pgrep_status": 0}, False),
        ({"pgrep_status": 2}, False),
        ({"pgrep_status": 3}, False),
    ],
    ids=[
        "all-healthy",
        "mirror-unknown",
        "message-bus-inactive",
        "switch-ui-inactive",
        "bridge-inactive",
        "mirror-active",
        "mirror-failed",
        "systemctl-error",
        "mirror-process-running",
        "pgrep-error",
        "pgrep-fatal",
    ],
)
def test_phase_service_and_mirror_gate_status_matrix(
    tmp_path: Path, overrides: dict[str, object], expected: bool
) -> None:
    inactive = overrides.get("inactive_units", "")
    state = overrides.get("mirror_state", "inactive")
    mirror_status = overrides.get("mirror_status", 3)
    pgrep_status = overrides.get("pgrep_status", 1)
    assert isinstance(inactive, str) and isinstance(state, str)
    assert isinstance(mirror_status, int) and isinstance(pgrep_status, int)

    assert (
        _run(
            tmp_path,
            _local_gate(),
            inactive_units=inactive,
            mirror_state=state,
            mirror_status=mirror_status,
            pgrep_status=pgrep_status,
        )
        is expected
    )


@pytest.mark.parametrize(
    ("pgrep_status", "expected"),
    [(1, True), (0, False), (2, False), (3, False)],
    ids=["absent", "pilot-running", "pgrep-error", "pgrep-fatal"],
)
def test_destage_pilot_process_gate_status_matrix(
    tmp_path: Path, pgrep_status: int, expected: bool
) -> None:
    assert _run(tmp_path, _destage_gate(), pgrep_status=pgrep_status) is expected

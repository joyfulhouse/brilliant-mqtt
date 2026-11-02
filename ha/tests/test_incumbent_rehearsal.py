"""Incumbent health rehearsal: an installed bridge must prove it can come back.

A bridge can stay ``active`` while it cannot reach MQTT. Before a baseline is
journaled, the installed bridge is restarted once under a fresh deployment id and
must publish health evidence carrying that id; otherwise the install or update is
refused while its baseline is still unjournaled (and so is removed), instead of
failing later with a rollback whose health proof can never succeed.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from homeassistant.core import HomeAssistant

from custom_components.brilliant_mqtt import manager as manager_module
from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.panel_health import PanelHealthError, PanelHealthEvidence
from custom_components.brilliant_mqtt.panel_provisioner import (
    PanelProvisioner,
    PanelProvisioningError,
    _RehearsalOutcome,
    panel_snapshot_from_stored,
    rehearsal_failure_code,
)
from custom_components.brilliant_mqtt.provisioning_journal import (
    PROVISIONING_ERROR_CODES,
    ProvisioningJournal,
    ProvisioningRecord,
)
from custom_components.brilliant_mqtt.release_identity import ReleaseIdentity
from custom_components.brilliant_mqtt.shell import PanelProcess, PanelShell
from custom_components.brilliant_mqtt.update_state import UPDATE_ERROR_CODES
from tests.canary_rehearsal import RehearsalShell
from tests.fakes import FakeShell
from tests.test_panel_provisioner import (
    _PUBLIC_KEY,
    _FakeObserver,
    _fleet,
    _Harness,
    _health,
    _release_link_snapshot,
    _request,
)
from tests.test_provisioning_journal import _record
from tests.update_harness import install_durable_store

REHEARSAL_CODES = (
    "incumbent_unhealthy",
    "incumbent_unhealthy_unverified",
    "incumbent_rehearsal_failed",
    "incumbent_rehearsal_interrupted",
    "incumbent_health_unobservable",
)
_RESTART = "systemctl restart --no-block brilliant-mqtt"
_LAYOUTS = pytest.mark.parametrize("release", [False, True], ids=["legacy", "release_link"])
_HEX = "0123456789abcdef"


# --- the pure outcome -> code mapping ----------------------------------------

_H = _RehearsalOutcome.HEALTHY
_P = _RehearsalOutcome.PROOF_FAILED
_R = _RehearsalOutcome.RESTART_FAILED
_U = _RehearsalOutcome.UNOBSERVABLE
_C = _RehearsalOutcome.CANCELLED


@pytest.mark.parametrize(
    ("outcome", "transport_lost", "bridge_running", "expected"),
    [
        (_H, False, True, None),
        (_H, False, False, None),
        (_H, True, True, "incumbent_rehearsal_interrupted"),
        (_H, True, False, "incumbent_rehearsal_interrupted"),
        (_P, False, True, "incumbent_unhealthy"),
        (_P, False, False, "incumbent_unhealthy_unverified"),
        (_P, True, True, "incumbent_rehearsal_interrupted"),
        (_P, True, False, "incumbent_rehearsal_interrupted"),
        (_R, False, True, "incumbent_rehearsal_failed"),
        (_R, False, False, "incumbent_unhealthy_unverified"),
        (_R, True, True, "incumbent_rehearsal_interrupted"),
        (_R, True, False, "incumbent_rehearsal_interrupted"),
        (_U, False, True, "incumbent_health_unobservable"),
        (_U, False, False, "incumbent_health_unobservable"),
        (_U, True, True, "incumbent_health_unobservable"),
        (_U, True, False, "incumbent_health_unobservable"),
        (_C, False, True, None),
        (_C, False, False, None),
        (_C, True, True, None),
        (_C, True, False, None),
    ],
)
def test_rehearsal_failure_code_table(
    outcome: _RehearsalOutcome, transport_lost: bool, bridge_running: bool, expected: str | None
) -> None:
    assert (
        rehearsal_failure_code(
            outcome, transport_lost=transport_lost, bridge_running=bridge_running
        )
        == expected
    )


def test_rehearsal_failure_code_table_is_exhaustive() -> None:
    assert set(_RehearsalOutcome) == {_H, _P, _R, _U, _C}


# --- every new code is wired everywhere it must be ----------------------------


def test_rehearsal_codes_are_allowlisted_and_translated() -> None:
    root = Path(__file__).parents[2] / "custom_components/brilliant_mqtt"
    strings = json.loads((root / "strings.json").read_text())
    english = json.loads((root / "translations/en.json").read_text())
    for code in REHEARSAL_CODES:
        assert code in PROVISIONING_ERROR_CODES
        assert code in UPDATE_ERROR_CODES
        assert code in manager_module._SAFE_PANEL_OP_DETAILS
        assert strings["config_subentries"]["panel"]["error"][code].strip()
        assert (
            english["config_subentries"]["panel"]["error"][code]
            == strings["config_subentries"]["panel"]["error"][code]
        )


@pytest.mark.parametrize(
    ("code", "guidance"),
    [
        ("incumbent_unhealthy", "uninstall then re-add"),
        ("incumbent_unhealthy", "Home Assistant's MQTT integration"),
        ("incumbent_unhealthy_unverified", "systemctl is-active brilliant-mqtt"),
        ("incumbent_rehearsal_failed", "storage/systemd"),
        ("incumbent_rehearsal_interrupted", "systemd completes it on its own"),
        ("incumbent_rehearsal_interrupted", "Interrupted baseline capture"),
        ("incumbent_health_unobservable", "never touched"),
    ],
)
def test_rehearsal_summaries_expose_only_safe_guidance(code: str, guidance: str) -> None:
    summary = manager_module._safe_failure_summary(
        "agent update failed", panel_ops.PanelOpError(f"{code}: SECRET stderr")
    )
    assert code in summary
    assert guidance in summary
    assert "SECRET" not in summary


def test_rehearsal_lost_summary_is_dedicated() -> None:
    lost = manager_module._REHEARSAL_LOST_SUMMARY
    assert "incumbent_rehearsal_interrupted" in lost
    assert "incumbent health rehearsal" in lost
    assert lost != manager_module._BASELINE_LOST_SUMMARY


# --- fake harness: ordering, refusals, fresh installs -------------------------


def _installed(harness: _Harness) -> None:
    harness.operations.snapshot = panel_snapshot_from_stored(_release_link_snapshot())


async def _update(provisioner: PanelProvisioner, shell: PanelShell) -> UUID:
    record = _record()
    async with provisioner._lock:
        async with provisioner.async_managed_update(
            shell,
            record.panel_request,
            record.fleet_profile,
            record.staged_version,
            subentry_id="fixture-owner",
        ) as transaction:
            return transaction


async def _route(route: str, harness: _Harness) -> None:
    provisioner = harness.provisioner()
    if route == "install":
        await provisioner.async_install(_request(), _fleet(), harness.progress)
    else:
        await _update(provisioner, harness.shell)


def _names(harness: _Harness) -> list[object]:
    return [event[0] for event in harness.events]


@pytest.mark.parametrize("route", ["install", "update"])
async def test_healthy_rehearsal_runs_before_the_journal_and_rebinds(route: str) -> None:
    harness = _Harness()
    _installed(harness)

    await _route(route, harness)

    rehearsal = [
        event
        for event in harness.events
        if event[0]
        in {
            "health_subscribe",
            "rehearse_env",
            "health_boundary",
            "rehearse_restart",
            "health_wait",
            "health_close",
            "rebind",
            "journal_create",
        }
    ]
    deployment_id = rehearsal[1][1]
    assert isinstance(deployment_id, str) and len(deployment_id) == 32
    assert set(deployment_id) <= set(_HEX)
    assert rehearsal[:8] == [
        ("health_subscribe",),
        ("rehearse_env", deployment_id),
        ("health_boundary", "0.6.0", deployment_id),
        ("rehearse_restart",),
        ("health_wait", "0.6.0", 45.0),
        ("health_close",),
        ("rebind", deployment_id),
        ("journal_create", "staged"),
    ]
    names = _names(harness)
    assert names.index("rebind") < names.index("journal_create")
    if route == "install":
        assert names.index("journal_create") < names.index("stage")
    assert "finalize_baseline" not in names
    assert "bridge_running" not in names


@pytest.mark.parametrize("route", ["install", "update"])
async def test_fresh_install_skips_the_rehearsal(route: str) -> None:
    harness = _Harness()

    await _route(route, harness)

    names = _names(harness)
    assert "rehearse_env" not in names
    assert "rebind" not in names
    assert names.index("journal_create") < names.index("health_factory")


type _Setup = Callable[[_Harness, pytest.MonkeyPatch], None]


def _wait(
    harness: _Harness,
    monkeypatch: pytest.MonkeyPatch,
    *,
    lose: bool = False,
    evidence: PanelHealthEvidence | None = None,
) -> None:
    async def wait(expected_version: str, bound_seconds: float = 90.0) -> PanelHealthEvidence:
        harness.events.append(("health_wait", expected_version, bound_seconds))
        if lose:
            harness.shell.lost = True
        if evidence is not None:
            return evidence
        raise PanelHealthError("panel_health_timeout")

    monkeypatch.setattr(harness.observer, "async_wait", wait)


def _subscribe_fails(harness: _Harness, _monkeypatch: pytest.MonkeyPatch) -> None:
    harness.observer.fail_subscribe = True


def _boundary_fails(harness: _Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    def mark(expected_version: str, expected_deployment_id: str) -> None:
        raise PanelHealthError("panel_health_not_subscribed")

    monkeypatch.setattr(harness.observer, "mark_activation_started", mark)


def _fail(*steps: str) -> _Setup:
    def setup(harness: _Harness, _monkeypatch: pytest.MonkeyPatch) -> None:
        harness.operations.fail_at.update(steps)

    return setup


def _proof_fails(*steps: str) -> _Setup:
    def setup(harness: _Harness, monkeypatch: pytest.MonkeyPatch) -> None:
        harness.operations.fail_at.update(steps)
        _wait(harness, monkeypatch)

    return setup


def _mismatch(harness: _Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    _wait(harness, monkeypatch, evidence=_health())  # another id and version


def _lost_during_wait(harness: _Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    _wait(harness, monkeypatch, lose=True)


def _lost_after_proof(harness: _Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    def mark(expected_version: str, expected_deployment_id: str) -> None:
        harness.events.append(("health_boundary", expected_version, expected_deployment_id))
        _wait(
            harness,
            monkeypatch,
            lose=True,
            evidence=replace(
                _health(), agent_version=expected_version, deployment_id=expected_deployment_id
            ),
        )

    monkeypatch.setattr(harness.observer, "mark_activation_started", mark)


_REFUSALS: list[tuple[str, _Setup, str, bool, set[str], set[str]]] = [
    # (id, setup, code, baseline removed, events expected, events forbidden)
    (
        "subscribe",
        _subscribe_fails,
        "incumbent_health_unobservable",
        True,
        set(),
        {"rehearse_env", "rehearse_restart", "bridge_running"},
    ),
    (
        "boundary",
        _boundary_fails,
        "incumbent_health_unobservable",
        True,
        {"rehearse_env"},
        {"rehearse_restart", "bridge_running"},
    ),
    (
        "env_write",
        _fail("rehearse_env"),
        "incumbent_rehearsal_failed",
        True,
        {"bridge_running"},
        {"rehearse_restart", "start_bridge"},
    ),
    (
        "restart_then_started",
        _fail("rehearse_restart"),
        "incumbent_rehearsal_failed",
        True,
        {"start_bridge", "bridge_running"},
        set(),
    ),
    (
        "restart_start_failed",
        _fail("rehearse_restart", "start_bridge", "bridge_stopped"),
        "incumbent_unhealthy_unverified",
        True,
        {"start_bridge", "bridge_running"},
        set(),
    ),
    (
        "proof_running",
        _proof_fails(),
        "incumbent_unhealthy",
        True,
        {"rehearse_restart", "bridge_running"},
        {"start_bridge"},
    ),
    (
        "proof_stopped",
        _proof_fails("bridge_stopped"),
        "incumbent_unhealthy_unverified",
        True,
        {"bridge_running"},
        set(),
    ),
    (
        "proof_probe_failed",
        _proof_fails("bridge_probe"),
        "incumbent_unhealthy_unverified",
        True,
        {"bridge_running"},
        set(),
    ),
    ("mismatch", _mismatch, "incumbent_unhealthy", True, {"bridge_running"}, set()),
    (
        "lost_during_wait",
        _lost_during_wait,
        "incumbent_rehearsal_interrupted",
        False,
        set(),
        {"bridge_running"},
    ),
    (
        "lost_after_proof",
        _lost_after_proof,
        "incumbent_rehearsal_interrupted",
        False,
        set(),
        {"bridge_running", "rebind"},
    ),
]


@pytest.mark.parametrize("route", ["install", "update"])
@pytest.mark.parametrize(
    ("setup", "code", "removed", "present", "absent"),
    [case[1:] for case in _REFUSALS],
    ids=[case[0] for case in _REFUSALS],
)
async def test_refused_rehearsal_is_typed_and_retires_the_baseline(
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    setup: _Setup,
    code: str,
    removed: bool,
    present: set[str],
    absent: set[str],
) -> None:
    harness = _Harness()
    _installed(harness)
    setup(harness, monkeypatch)

    with pytest.raises(PanelProvisioningError) as raised:
        await _route(route, harness)

    assert raised.value.code == code
    names = set(_names(harness))
    assert present <= names
    assert not absent & names
    assert ("finalize_baseline" in names) is removed
    assert "journal_create" not in names
    assert "stage" not in names
    assert "rebind" not in names
    assert ("shell_factory",) not in harness.events[harness.events.index(("snapshot",)) :]


@pytest.mark.parametrize("route", ["install", "update"])
async def test_rebind_refusal_retires_the_baseline(route: str) -> None:
    harness = _Harness()
    _installed(harness)
    harness.operations.fail_at.add("rebind")

    expected: type[Exception] = (
        PanelProvisioningError if route == "install" else (panel_ops.PanelOpError)
    )
    with pytest.raises(expected):
        await _route(route, harness)

    names = _names(harness)
    assert names.index("rebind") < names.index("finalize_baseline")
    assert "journal_create" not in names


async def test_refusal_whose_removal_fails_is_baseline_capture_interrupted() -> None:
    harness = _Harness()
    _installed(harness)
    harness.operations.fail_at.update({"rehearse_env", "finalize"})

    with pytest.raises(PanelProvisioningError, match="^baseline_capture_interrupted$"):
        await _update(harness.provisioner(), harness.shell)


# --- re-binding the admission to the rotated deployment id --------------------

_OLD = "b" * 32
_NEW = "a" * 32
_BRIDGE = ReleaseIdentity("0.10.2", 3, "e" * 64, _OLD, "release_link")
_BUS = ReleaseIdentity("0.10.2", 3, "f" * 64, _OLD, "release_link")


def _rotated(identity: ReleaseIdentity, deployment_id: str | None = _NEW) -> ReleaseIdentity:
    return replace(identity, deployment_id=deployment_id)


_BOTH: dict[str, ReleaseIdentity | None] = {"bridge": _BRIDGE, "bus_watchdog": _BUS}
_CODE: dict[str, ReleaseIdentity] = {"bridge": _BRIDGE, "bus_watchdog": _BUS}
_ROTATED: dict[str, ReleaseIdentity | None] = {key: _rotated(value) for key, value in _CODE.items()}
_OTHER: dict[str, ReleaseIdentity | None] = {
    key: _rotated(value, "c" * 32) for key, value in _CODE.items()
}
_UNSET: dict[str, ReleaseIdentity | None] = {
    "bridge": _rotated(_BRIDGE, None),
    "bus_watchdog": _rotated(_BUS, None),
}

_REBIND_TABLE: list[
    tuple[str, dict[str, ReleaseIdentity | None], dict[str, ReleaseIdentity | None], bool]
] = [
    ("exact", dict(_BOTH), dict(_ROTATED), True),
    *(
        (
            f"{component}_{field}",
            dict(_BOTH),
            {**_ROTATED, component: changed},
            False,
        )
        for component in ("bridge", "bus_watchdog")
        for field, changed in (
            ("version", replace(_rotated(_CODE[component]), version="0.10.3")),
            ("release_ordinal", replace(_rotated(_CODE[component]), release_ordinal=4)),
            ("digest", replace(_rotated(_CODE[component]), digest="0" * 64)),
            ("layout", replace(_rotated(_CODE[component]), layout="legacy_fixed")),
        )
    ),
    ("companion_not_rotated", dict(_BOTH), {**_ROTATED, "bus_watchdog": _BUS}, False),
    (
        "wrong_id",
        dict(_BOTH),
        dict(_OTHER),
        False,
    ),
    (
        "set_grows",
        {"bridge": _BRIDGE, "bus_watchdog": None},
        dict(_ROTATED),
        False,
    ),
    (
        "set_shrinks",
        dict(_BOTH),
        {"bridge": _rotated(_BRIDGE), "bus_watchdog": None},
        False,
    ),
    (
        "absent_stays_absent",
        {"bridge": _BRIDGE, "bus_watchdog": None},
        {"bridge": _rotated(_BRIDGE), "bus_watchdog": None},
        True,
    ),
    ("unset_to_generated", dict(_UNSET), dict(_ROTATED), True),
    (
        "unset_to_other",
        dict(_UNSET),
        dict(_OTHER),
        False,
    ),
    ("unset_to_unset", dict(_UNSET), dict(_UNSET), False),
    (
        "mixed_recorded_ids",
        {"bridge": _BRIDGE, "bus_watchdog": _rotated(_BUS, "d" * 32)},
        dict(_ROTATED),
        False,
    ),
    (
        "mixed_unset_and_set",
        {"bridge": _BRIDGE, "bus_watchdog": _rotated(_BUS, None)},
        dict(_ROTATED),
        False,
    ),
    (
        "no_bridge_companion_present",
        {"bridge": None, "bus_watchdog": _BUS},
        {"bridge": None, "bus_watchdog": _rotated(_BUS)},
        False,
    ),
]


@pytest.mark.parametrize(
    ("recorded", "observed", "accepted"),
    [case[1:] for case in _REBIND_TABLE],
    ids=[case[0] for case in _REBIND_TABLE],
)
def test_rebinding_accepts_only_an_exact_rotation(
    recorded: dict[str, ReleaseIdentity | None],
    observed: dict[str, ReleaseIdentity | None],
    accepted: bool,
) -> None:
    rebound = panel_ops.rehearsal_rebinding(recorded, observed, _NEW)
    if accepted:
        assert rebound == {key: observed[key] for key in recorded}
    else:
        assert rebound is None


def _identity_shell(observed: dict[str, ReleaseIdentity | None]) -> FakeShell:
    shell = FakeShell()
    shell.connected = True
    for component, identity in observed.items():
        shell.release_identities[component] = identity.as_dict() if identity else None
    return shell


@asynccontextmanager
async def _active(
    admission: panel_ops.ReleaseAdmission,
) -> AsyncIterator[panel_ops.ReleaseAdmission]:
    token = panel_ops._RELEASE_ADMISSION.set(admission)
    try:
        yield admission
    finally:
        panel_ops._RELEASE_ADMISSION.reset(token)


def _admission(shell: PanelShell) -> panel_ops.ReleaseAdmission:
    return panel_ops.ReleaseAdmission(
        shell, {}, dict(_BOTH), {"bridge": True, "bus_watchdog": True}, "c" * 32
    )


async def test_rebind_without_admission_is_a_noop_that_reads_nothing() -> None:
    shell = _identity_shell(dict(_ROTATED))

    await panel_ops.rebind_rehearsed_deployment(shell, _NEW)

    assert shell.identity_commands == []


async def test_rebind_updates_the_active_admission_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    shell = _identity_shell(dict(_ROTATED))
    admission = _admission(shell)
    caplog.set_level(logging.INFO, logger=panel_ops.__name__)

    async with _active(admission):
        await panel_ops.rebind_rehearsed_deployment(shell, _NEW)
        await panel_ops._revalidate_release(shell, admission)

    assert admission.incumbent == _ROTATED
    assert f"{_OLD} -> {_NEW}" in caplog.text


async def test_rebind_refusal_keeps_the_admitted_identities() -> None:
    shell = _identity_shell({**_ROTATED, "bus_watchdog": _BUS})
    admission = _admission(shell)

    async with _active(admission):
        with pytest.raises(panel_ops.PanelOpError, match="^release_identity_changed"):
            await panel_ops.rebind_rehearsed_deployment(shell, _NEW)

    assert admission.incumbent == _BOTH


@pytest.mark.parametrize("problem", ["inactive", "other_task", "other_shell"])
async def test_rebind_refuses_an_admission_it_does_not_own(problem: str) -> None:
    shell = _identity_shell(dict(_ROTATED))
    admission = _admission(shell)
    if problem == "inactive":
        admission.active = False
    elif problem == "other_shell":
        admission.shell = _identity_shell(dict(_ROTATED))
    else:
        other = asyncio.create_task(asyncio.sleep(0))
        await other
        admission.owner = other

    async with _active(admission):
        with pytest.raises(panel_ops.PanelOpError, match="^release_transaction_mismatch$"):
            await panel_ops.rebind_rehearsed_deployment(shell, _NEW)

    assert shell.identity_commands == []
    assert admission.incumbent == _BOTH


# --- the real recipes on a disposable panel, both layouts ---------------------


def _env(shell: RehearsalShell) -> str:
    return (shell.root / "etc/brilliant-mqtt.env").read_text()


def _deployment_id(shell: RehearsalShell) -> str:
    return next(
        line.partition("=")[2]
        for line in _env(shell).splitlines()
        if line.startswith("BRILLIANT_DEPLOYMENT_ID=")
    )


def _running(shell: RehearsalShell) -> bool:
    states: dict[str, Any] = json.loads(shell.state.read_text())
    return bool(states["brilliant-mqtt"][1])


def _restarts(shell: RehearsalShell) -> int:
    return sum(_RESTART in command for command in shell.commands)


def _env_temporaries(shell: RehearsalShell) -> list[Path]:
    return list((shell.root / "etc").glob("brilliant-mqtt.env.rollback-*.tmp"))


def _baseline_left(shell: RehearsalShell) -> bool:
    parent = shell.panel / ".rollback"
    archives = list(parent.iterdir()) if parent.exists() else []
    pins = list(shell.panel.glob("releases/*/.rollback-retained"))
    return bool(archives or pins)


class SimulatedBridge:
    """The restarted incumbent's MQTT session, derived from the panel's own state.

    Evidence arrives only when the bridge was restarted after the boundary, is
    running, and its env carries the expected deployment id (it reads env only
    at start). ``mqtt_ok=False`` models an incumbent that cannot reach MQTT.
    """

    def __init__(
        self,
        shell: RehearsalShell,
        *,
        mqtt_ok: bool = True,
        fail_subscribe: bool = False,
        hold: asyncio.Event | None = None,
    ) -> None:
        self.shell = shell
        self.mqtt_ok = mqtt_ok
        self.fail_subscribe = fail_subscribe
        self.hold = hold
        self.waiting = asyncio.Event()
        self.marks: list[tuple[str, str]] = []
        self.restarts_at_mark = 0
        self.closed = False

    async def async_subscribe(self) -> None:
        if self.fail_subscribe:
            raise PanelHealthError("panel_health_subscription_failed")

    def mark_activation_started(self, expected_version: str, expected_deployment_id: str) -> None:
        self.marks.append((expected_version, expected_deployment_id))
        self.restarts_at_mark = _restarts(self.shell)

    async def async_wait(
        self, expected_version: str, bound_seconds: float = 90.0
    ) -> PanelHealthEvidence:
        self.waiting.set()
        if self.hold is not None:
            await self.hold.wait()
        version, deployment_id = self.marks[-1]
        if (
            self.mqtt_ok
            and _restarts(self.shell) > self.restarts_at_mark
            and _running(self.shell)
            and _deployment_id(self.shell) == deployment_id
        ):
            return replace(_health(), agent_version=version, deployment_id=deployment_id)
        raise PanelHealthError("panel_health_timeout")

    async def async_close(self) -> None:
        self.closed = True


async def _panel(tmp_path: Path, *, release: bool) -> RehearsalShell:
    shell = RehearsalShell(tmp_path / "panel")
    shell.install(release=release)
    shell._pinned = _PUBLIC_KEY
    await shell.connect()
    return shell


def _canary(shell: RehearsalShell, bridge: SimulatedBridge) -> tuple[_Harness, PanelProvisioner]:
    harness = _Harness()
    provisioner = harness.provisioner()
    provisioner._operations = panel_ops
    observers: list[object] = []

    def factory(_slug: str) -> SimulatedBridge | _FakeObserver:
        observers.append(_slug)
        return bridge if len(observers) == 1 else _FakeObserver([], _health())

    provisioner._health_observer_factory = factory
    return harness, provisioner


def _replace_command(
    monkeypatch: pytest.MonkeyPatch, shell: RehearsalShell, needle: str, replacement: str
) -> None:
    start = shell.start

    async def patched(command: str) -> PanelProcess:
        return await start(replacement if needle in command else command)

    monkeypatch.setattr(shell, "start", patched)


@_LAYOUTS
async def test_canary_healthy_rehearsal_rotates_the_id_before_the_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: bool
) -> None:
    shell = await _panel(tmp_path, release=release)
    bridge = SimulatedBridge(shell)
    harness, provisioner = _canary(shell, bridge)
    created: list[ProvisioningRecord] = []
    env_at_create: list[str] = []
    create = harness.journal.async_create

    async def recorded_create(record: ProvisioningRecord) -> ProvisioningRecord:
        created.append(record)
        env_at_create.append(_deployment_id(shell))
        return await create(record)

    monkeypatch.setattr(harness.journal, "async_create", recorded_create)

    await _update(provisioner, shell)

    assert len(bridge.marks) == 1
    version, rehearsed = bridge.marks[0]
    assert version == "0.10.2"
    assert rehearsed != "b" * 32
    assert env_at_create == [rehearsed]
    assert _restarts(shell) == 1
    assert _running(shell)
    # The journal keeps the PRE-rehearsal env bytes: a rollback restores them.
    prior = created[0].prior_snapshot.environment_file.content
    assert prior is not None and b"BRILLIANT_DEPLOYMENT_ID=" + b"b" * 32 in prior
    assert _baseline_left(shell)


@_LAYOUTS
async def test_canary_unhealthy_incumbent_is_refused_and_cleaned(
    tmp_path: Path, release: bool
) -> None:
    shell = await _panel(tmp_path, release=release)
    bridge = SimulatedBridge(shell, mqtt_ok=False)
    harness, provisioner = _canary(shell, bridge)

    with pytest.raises(PanelProvisioningError, match="^incumbent_unhealthy$"):
        await _update(provisioner, shell)

    assert _restarts(shell) == 1
    assert _running(shell)
    assert _deployment_id(shell) == bridge.marks[0][1]
    assert not _baseline_left(shell)
    assert harness.journal.created_record is None
    assert bridge.closed


@_LAYOUTS
async def test_canary_cancelled_rehearsal_keeps_the_bridge_running(
    tmp_path: Path, release: bool
) -> None:
    shell = await _panel(tmp_path, release=release)
    bridge = SimulatedBridge(shell, hold=asyncio.Event())
    harness, provisioner = _canary(shell, bridge)
    task = asyncio.create_task(_update(provisioner, shell))
    await bridge.waiting.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert _running(shell)
    assert not _baseline_left(shell)
    assert harness.journal.created_record is None


@_LAYOUTS
async def test_canary_session_lost_after_the_restart_is_never_reconnected(
    tmp_path: Path, release: bool
) -> None:
    shell = await _panel(tmp_path, release=release)
    bridge = SimulatedBridge(shell)
    harness, provisioner = _canary(shell, bridge)
    shell.drop_after = _RESTART

    with pytest.raises(PanelProvisioningError, match="^incumbent_rehearsal_interrupted$"):
        await _update(provisioner, shell)

    assert shell.transport_lost()
    assert shell.connect_count == 1
    assert ("shell_factory",) not in harness.events
    assert _restarts(shell) == 1
    assert _running(shell), "systemd completes the queued restart on its own"
    assert _baseline_left(shell)


@_LAYOUTS
async def test_canary_env_write_failure_never_restarts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: bool
) -> None:
    shell = await _panel(tmp_path, release=release)
    bridge = SimulatedBridge(shell)
    _, provisioner = _canary(shell, bridge)
    put = shell._do_put_bytes

    async def partial_upload(data: bytes, remote_path: str, mode: int) -> None:
        await put(data, remote_path, mode)
        if remote_path.startswith("/etc/brilliant-mqtt.env.rollback-"):
            raise OSError("upload interrupted")

    monkeypatch.setattr(shell, "_do_put_bytes", partial_upload)

    with pytest.raises(PanelProvisioningError, match="^incumbent_rehearsal_failed$"):
        await _update(provisioner, shell)

    assert _restarts(shell) == 0
    assert _running(shell)
    assert _deployment_id(shell) == "b" * 32
    assert _env_temporaries(shell) == []
    assert not _baseline_left(shell)


@_LAYOUTS
async def test_canary_env_move_failure_removes_the_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: bool
) -> None:
    shell = await _panel(tmp_path, release=release)
    bridge = SimulatedBridge(shell)
    _, provisioner = _canary(shell, bridge)
    _replace_command(
        monkeypatch, shell, "--force --no-target-directory -- /etc/brilliant", "exit 1"
    )

    with pytest.raises(PanelProvisioningError, match="^incumbent_rehearsal_failed$"):
        await _update(provisioner, shell)

    assert _restarts(shell) == 0
    assert _running(shell)
    assert _deployment_id(shell) == "b" * 32
    assert _env_temporaries(shell) == []
    assert not _baseline_left(shell)


@_LAYOUTS
@pytest.mark.parametrize("start_works", [True, False])
async def test_canary_failed_restart_command_gets_a_best_effort_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: bool, start_works: bool
) -> None:
    shell = await _panel(tmp_path, release=release)
    bridge = SimulatedBridge(shell)
    _, provisioner = _canary(shell, bridge)
    start = shell.start
    starts: list[str] = []

    async def patched(command: str) -> PanelProcess:
        if command == _RESTART:
            return await start("systemctl stop brilliant-mqtt; exit 1")
        if command == "systemctl start brilliant-mqtt":
            starts.append(command)
            return await start(command if start_works else "exit 1")
        return await start(command)

    monkeypatch.setattr(shell, "start", patched)
    code = "incumbent_rehearsal_failed" if start_works else "incumbent_unhealthy_unverified"

    with pytest.raises(PanelProvisioningError, match=f"^{code}$"):
        await _update(provisioner, shell)

    assert len(starts) == 1
    assert _running(shell) is start_works
    assert not _baseline_left(shell)


@_LAYOUTS
async def test_canary_cancel_while_the_restart_is_in_flight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: bool
) -> None:
    shell = await _panel(tmp_path, release=release)
    bridge = SimulatedBridge(shell)
    _, provisioner = _canary(shell, bridge)
    in_flight = asyncio.Event()
    start = shell.start

    async def slow_restart(command: str) -> PanelProcess:
        if command != _RESTART:
            return await start(command)
        process = await start(f"sleep 30; {_RESTART}")
        in_flight.set()
        return process

    monkeypatch.setattr(shell, "start", slow_restart)
    task = asyncio.create_task(_update(provisioner, shell))
    await in_flight.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert _running(shell)


@_LAYOUTS
async def test_canary_unobservable_health_never_touches_the_bridge(
    tmp_path: Path, release: bool
) -> None:
    shell = await _panel(tmp_path, release=release)
    bridge = SimulatedBridge(shell, fail_subscribe=True)
    _, provisioner = _canary(shell, bridge)
    env_before = _env(shell)

    with pytest.raises(PanelProvisioningError, match="^incumbent_health_unobservable$"):
        await _update(provisioner, shell)

    assert _restarts(shell) == 0
    assert _env(shell) == env_before
    assert _running(shell)
    assert not _baseline_left(shell)


# --- the manager: real admission, re-bind, deploy; and the lost session ------


async def _manager_update(
    hass: HomeAssistant,
    tmp_path: Path,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    shell: RehearsalShell,
    bridge: SimulatedBridge,
) -> tuple[ProvisioningJournal, Any]:
    from custom_components.brilliant_mqtt import fleet_manager
    from custom_components.brilliant_mqtt.const import CONF_SSH_HOST_KEY
    from tests.test_manager import _fleet_panel_manager

    install_durable_store(hass, tmp_path / "ha-config", monkeypatch)
    (payload_dir / "VERSION").write_text("0.11.0")
    (payload_dir / "RELEASE_ORDINAL").write_text("2\n")
    shell._pinned = _record().panel_request.public_key
    _, manager = _fleet_panel_manager(hass, panel_overrides={CONF_SSH_HOST_KEY: shell._pinned})
    journal = ProvisioningJournal(hass)
    _, provisioner = _canary(shell, bridge)
    provisioner._lock = manager._ssh_lock
    provisioner._journal = journal
    monkeypatch.setattr(fleet_manager, "_get_recovery_provisioner", lambda _hass: provisioner)
    monkeypatch.setattr(manager, "_connect_for_repair", AsyncMock(return_value=shell))
    monkeypatch.setattr(
        manager, "_async_stage_broker_ca", AsyncMock(return_value="MQTT_TLS_ENABLED=0\n")
    )
    return journal, manager


async def test_update_after_a_healthy_rehearsal_rebinds_and_deploys(
    hass: HomeAssistant,
    tmp_path: Path,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    shell = RehearsalShell(tmp_path / "panel")
    shell.install()
    bridge = SimulatedBridge(shell)
    journal, manager = await _manager_update(
        hass, tmp_path, payload_dir, monkeypatch, shell, bridge
    )
    caplog.set_level(logging.INFO, logger=panel_ops.__name__)
    try:
        await manager.async_update_agent()
    finally:
        await manager.async_shutdown()

    rehearsed = bridge.marks[0][1]
    assert f"-> {rehearsed}" in caplog.text
    retained = await journal.async_retained_records()
    assert len(retained) == 1 and retained[0].state == "soak"
    assert _deployment_id(shell) == retained[0].record.transaction_id.hex
    prior = retained[0].record.prior_snapshot.environment_file.content
    assert prior is not None and rehearsed.encode() not in prior
    installed = (await panel_ops._read_release_identities(shell))["bridge"]
    assert installed is not None and installed.version == "0.11.0"
    assert _running(shell)


async def test_lost_session_during_the_rehearsal_records_its_own_last_error(
    hass: HomeAssistant,
    tmp_path: Path,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from homeassistant.exceptions import HomeAssistantError

    shell = RehearsalShell(tmp_path / "panel")
    shell.install()
    bridge = SimulatedBridge(shell)
    journal, manager = await _manager_update(
        hass, tmp_path, payload_dir, monkeypatch, shell, bridge
    )
    shell.drop_after = _RESTART
    try:
        with pytest.raises(HomeAssistantError) as raised:
            await manager.async_update_agent()
    finally:
        await manager.async_shutdown()

    error = (raised.value.translation_placeholders or {})["error"]
    assert error == manager_module._REHEARSAL_LOST_SUMMARY
    assert manager.update_last_error == {
        "stage": "baseline",
        "code": "incumbent_rehearsal_interrupted",
        "attempts": 1,
    }
    assert manager.update_retry is None
    assert manager._connect_for_repair.await_count == 1, "never reconnected"
    assert _running(shell)
    assert await journal.async_load() is None


@_LAYOUTS
async def test_canary_rollback_after_a_healthy_rehearsal_restores_the_prior_env(
    tmp_path: Path, release: bool
) -> None:
    shell = await _panel(tmp_path, release=release)
    original = _env(shell)
    bridge = SimulatedBridge(shell)
    harness, provisioner = _canary(shell, bridge)
    record = _record()

    with pytest.raises(RuntimeError, match="candidate failed"):
        async with provisioner._lock:
            async with provisioner.async_managed_update(
                shell,
                record.panel_request,
                record.fleet_profile,
                record.staged_version,
                subentry_id="fixture-owner",
            ):
                assert _deployment_id(shell) == bridge.marks[0][1]
                raise RuntimeError("candidate failed")

    # verify_baseline and the byte-exact restore are unaffected by the rehearsal:
    # everything but the fresh recovery correlation is the pre-rehearsal env.
    assert harness.journal.record is None
    restored = _env(shell)
    recovery = _deployment_id(shell)
    assert recovery not in {"b" * 32, bridge.marks[0][1]}
    assert restored.replace(recovery, "b" * 32) == original
    assert _running(shell)

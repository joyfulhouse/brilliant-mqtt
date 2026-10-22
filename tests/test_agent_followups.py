"""Regression contracts for bounded recovery and stable mesh discovery."""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import math
import signal
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from typing import NoReturn
from unittest.mock import patch

import aiomqtt
import pytest

from brilliant_mqtt import __main__ as runner
from brilliant_mqtt import mqttio
from brilliant_mqtt.bus import BusStartFencedError
from brilliant_mqtt.config import Settings
from brilliant_mqtt.discovery import config_payload, config_topic
from brilliant_mqtt.mapping import EntityDescriptor
from brilliant_mqtt.write_admission import WriteCancelled, command_admission
from tests.test_main import _mesh_dimmer, _SessionHarness


def _settings() -> Settings:
    return Settings(
        panel="office",
        mqtt_host="invalid",
        mqtt_username="u",
        mqtt_password="p",
        motion_reconcile_enabled=False,
    )


@pytest.mark.parametrize("limit", [0, -1, False, 1.5])
def test_fenced_start_limit_requires_positive_integer(limit: int) -> None:
    with pytest.raises(ValueError, match="BUS_FENCED_START_LIMIT"):
        replace(_settings(), bus_fenced_start_limit=limit)


@pytest.mark.parametrize("raw", ["0", "-1", "1.5", "true", ""])
def test_fenced_start_limit_env_rejects_invalid_values(raw: str) -> None:
    with pytest.raises(ValueError):
        Settings.from_env(
            {
                "BRILLIANT_PANEL": "office",
                "MQTT_HOST": "invalid",
                "MQTT_USERNAME": "u",
                "MQTT_PASSWORD": "p",
                "BUS_FENCED_START_LIMIT": raw,
            }
        )


def test_fenced_start_limit_env_default_and_override() -> None:
    env = {
        "BRILLIANT_PANEL": "office",
        "MQTT_HOST": "invalid",
        "MQTT_USERNAME": "u",
        "MQTT_PASSWORD": "p",
    }
    assert Settings.from_env(env).bus_fenced_start_limit == 3
    assert Settings.from_env(env | {"BUS_FENCED_START_LIMIT": "7"}).bus_fenced_start_limit == 7


@pytest.mark.parametrize("outcome", ["connect", "fenced", "nonfenced", "reconcile", "healthy"])
async def test_real_session_reports_precise_startup_progress(
    monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    harness = _SessionHarness(
        monkeypatch,
        bridge_reconcile_error=RuntimeError("reconcile") if outcome == "reconcile" else None,
        bridge_poll_effects={"panel": [asyncio.CancelledError()]},
    )
    progress = runner._SessionProgress(connect_failures=4, fenced_starts=2)
    if outcome == "connect":
        harness.mqtt.connect_error = RuntimeError("connect")

    async def fail_start() -> None:
        if outcome == "fenced":
            raise BusStartFencedError("pending close")
        raise RuntimeError("nonfenced start")

    if outcome in ("fenced", "nonfenced"):
        monkeypatch.setattr(harness.bus, "start", fail_start)
    error: type[BaseException] = asyncio.CancelledError if outcome == "healthy" else RuntimeError
    with pytest.raises(error) as caught:
        await runner._run_session(_settings(), None, None, progress=progress)
    assert isinstance(caught.value, runner.MqttConnectError) == (outcome == "connect")
    assert progress.connect_failures == (0 if outcome == "healthy" else 4)
    assert progress.fenced_starts == (2 if outcome in ("connect", "fenced") else 0)


def test_mesh_mapping_names_primary_and_aux_devices_consistently() -> None:
    from brilliant_mqtt.mapping import entities_for

    device = replace(_mesh_dimmer(), name="Garden", peripheral_id="load 1")
    descriptors = entities_for(device, "mesh")
    assert len(descriptors) > 1
    assert {descriptor.device_name for descriptor in descriptors} == {"Garden"}
    blocks = [json.loads(config_payload(descriptor))["device"] for descriptor in descriptors]
    assert all(block == blocks[0] for block in blocks)
    assert blocks[0]["name"] == "Garden"


@pytest.mark.parametrize("error", [runner.MqttReaderDeadError(), runner.BusReconnectStormError()])
async def test_outer_retry_after_reader_death_or_storm(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    calls = 0
    sleeps: list[float] = []

    async def session(*args: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise error
        raise asyncio.CancelledError

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(runner, "_run_session", session)
    monkeypatch.setattr(asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await runner.run(_settings())
    assert calls == 2
    assert sleeps == [5]


async def test_connect_backoff_survives_failed_reconcile_and_resets_on_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    calls = 0

    async def session(
        settings: Settings,
        panel: object,
        mesh: object,
        diagnostics: object,
        progress: runner._SessionProgress,
    ) -> None:
        nonlocal calls
        calls += 1
        if calls <= 6 or calls in (8, 10):
            raise runner.MqttConnectError("refused")
        if calls == 7:
            raise RuntimeError("connected but reconcile failed")
        if calls == 9:
            progress.reconciled()
            raise RuntimeError("healthy session later failed")
        raise asyncio.CancelledError

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(runner, "_run_session", session)
    monkeypatch.setattr(asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await runner.run(_settings())
    assert sleeps == [5, 10, 20, 40, 60, 60, 5, 60, 5, 5]


async def test_fenced_starts_escalate_and_only_actual_nonfenced_start_resets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []
    exits: list[int] = []

    class Escalated(BaseException):
        pass

    async def session(
        settings: Settings,
        panel: object,
        mesh: object,
        diagnostics: object,
        progress: runner._SessionProgress,
    ) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            progress.bus_started()
            raise RuntimeError("post-start failure")
        if calls == 4:
            progress.bus_started()
            raise RuntimeError("nonfenced bus start failure")
        if calls == 6:
            raise runner.MqttConnectError("before bus start")
        raise BusStartFencedError("pending close")

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    def exit_process(count: int) -> NoReturn:
        exits.append(count)
        raise Escalated

    monkeypatch.setattr(runner, "_run_session", session)
    monkeypatch.setattr(runner, "_exit_fenced_process", exit_process)
    monkeypatch.setattr(asyncio, "sleep", sleep)
    with pytest.raises(Escalated):
        await runner.run(_settings())
    assert calls == 8
    assert exits == [3]
    assert sleeps == [5] * 7


def test_fenced_restart_exits_despite_immortal_resource_close() -> None:
    code = """
import asyncio
import logging
from brilliant_mqtt import __main__ as runner
from brilliant_mqtt.bus import _RESOURCE_CLOSE_TASKS, RpcBusAdapter
from brilliant_mqtt.config import Settings
logging.basicConfig(level=logging.ERROR)
sleep = asyncio.sleep
async def immortal():
    while True:
        try:
            await sleep(3600)
        except asyncio.CancelledError:
            pass
async def session(*args):
    if not _RESOURCE_CLOSE_TASKS:
        task = asyncio.create_task(immortal())
        _RESOURCE_CLOSE_TASKS.add(task)
        await sleep(0)
    RpcBusAdapter()._begin_session()
async def fast_sleep(delay):
    await sleep(0)
runner._run_session = session
runner.asyncio.sleep = fast_sleep
settings = Settings(panel='office', mqtt_host='invalid', mqtt_username='u',
    mqtt_password='p', motion_reconcile_enabled=False)
asyncio.run(runner.run(settings))
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=5, check=False
    )
    assert result.returncode == 1
    assert "3 consecutive" in result.stderr


async def test_aiomqtt_timeout_is_explicit_finite_and_positive() -> None:
    with patch.object(aiomqtt, "Client", wraps=aiomqtt.Client) as constructor:
        mqttio.AioMqttAdapter(_settings())
    timeout = constructor.call_args.kwargs["timeout"]
    assert isinstance(timeout, (float, int))
    assert math.isfinite(timeout) and timeout > 0


@pytest.mark.parametrize(
    ("before", "after", "expected"),
    [
        (
            {"state": "ON", "brightness": 40, "transition": 2},
            {"brightness": 60},
            {"state": "ON", "brightness": 60},
        ),
        ({"state": "ON"}, {"brightness": 60, "transition": 2}, {"state": "ON", "brightness": 60}),
        ({"state": "OFF", "transition": 1}, {"state": "OFF", "transition": 2}, {"state": "OFF"}),
        ({"state": "OFF", "transition": 1}, {"brightness": 60}, None),
        ({"transition": 1}, {"brightness": 60}, None),
        ({"state": "ON", "effect": "party"}, {"brightness": 60}, None),
        ({"state": "ON", "unknown": 1}, {"brightness": 60}, None),
    ],
)
def test_transition_coalescing_preserves_conservative_boundaries(
    before: dict[str, object], after: dict[str, object], expected: dict[str, object] | None
) -> None:
    result = mqttio._coalesce_payload(
        "brilliant/mesh/load/set", json.dumps(before), json.dumps(after)
    )
    assert (json.loads(result) if result is not None else None) == expected


@pytest.mark.parametrize("write_cancelled", [False, True])
async def test_merged_fold_discard_warns_without_raw_payload(
    caplog: pytest.LogCaptureFixture, write_cancelled: bool
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    handled: list[str | bytes] = []

    async def handler(message: mqttio._InboundMessage) -> None:
        handled.append(message.payload)
        admission = command_admission.get()
        assert admission is not None
        admission.try_supersede = lambda payload: True
        entered.set()
        await release.wait()
        if write_cancelled:
            raise WriteCancelled

    dispatcher = mqttio._TopicDispatcher(handler)
    topic = "brilliant/mesh/secret-peripheral/set"
    try:
        await dispatcher.dispatch(
            mqttio._InboundMessage(topic, '{"state":"ON","brightness":40}', False, (), ()),
            latest_wins=True,
        )
        await entered.wait()
        await dispatcher.dispatch(
            mqttio._InboundMessage(topic, '{"brightness":60}', False, (), ()), latest_wins=True
        )
        assert json.loads(dispatcher._folded["brilliant/mesh/secret-peripheral"].payload) == {
            "state": "ON",
            "brightness": 60,
        }
        with caplog.at_level(logging.WARNING):
            if write_cancelled:
                release.set()
                await asyncio.sleep(0)
            else:
                worker = dispatcher._workers["brilliant/mesh/secret-peripheral"]
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
        assert len(handled) == 1
        assert "discarded a folded replacement" in caplog.text
        assert "secret-peripheral" not in caplog.text
        assert "brightness" not in caplog.text
    finally:
        release.set()
        await dispatcher.shutdown()


def test_mesh_device_grouping_preserves_entity_identity_and_aux_name() -> None:
    primary = EntityDescriptor(
        "light", "brilliant_mesh_load_1", "Garden", "mesh", "load 1", device_name="Garden"
    )
    aux = replace(
        primary,
        component="sensor",
        unique_id=primary.unique_id + "_write_status",
        name="Garden Write status",
        value_key="write_status",
    )
    other = replace(
        primary, peripheral_id="load 2", unique_id="brilliant_mesh_load_2", device_name="Patio"
    )
    configs = [json.loads(config_payload(entity)) for entity in (primary, aux, other)]
    assert configs[0]["device"] == configs[1]["device"]
    assert configs[0]["device"]["name"] == "Garden"
    assert configs[0]["device"]["identifiers"] == ["brilliant_mesh_load_1"]
    assert configs[0]["device"] != configs[2]["device"]
    assert configs[0]["unique_id"] == primary.unique_id
    assert config_topic(primary) == "homeassistant/light/brilliant_mesh_load_1/config"
    assert configs[0]["state_topic"] == "brilliant/mesh/load 1/state"


@pytest.mark.parametrize(
    "package,prefix", [("brilliant_bus_watchdog", "BUS"), ("brilliant_wifi_watchdog", "WIFI")]
)
def test_watchdog_sigterm_persists_shutdown_marker(
    tmp_path: Path, package: str, prefix: str
) -> None:
    log_path = tmp_path / "watchdog.log"
    code = """
import importlib, os, signal, sys
module = importlib.import_module(sys.argv[1] + '.run')
prefix, path = sys.argv[2:]
os.environ[prefix + '_WATCHDOG_LOG'] = path
os.environ[prefix + '_WATCHDOG_STATE'] = path + '.state'
# Trigger the real process signal once main has entered its normal loop.
def terminate(interval):
    os.kill(os.getpid(), signal.SIGTERM)
module.time.sleep = terminate
module.probe.default_gateway = lambda: None
module.probe.gateway_probe = lambda gateway: (None, module.probe.TcpProbe.INCONCLUSIVE)
module._service_active = lambda service: False
module._service_started_at = lambda service: None
module.main()
"""
    result = subprocess.run(
        [sys.executable, "-c", code, package, prefix, str(log_path)],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "stopped by SIGTERM at " in log_path.read_text()
    assert "UTC" in log_path.read_text()


@pytest.mark.parametrize("package", ["brilliant_bus_watchdog", "brilliant_wifi_watchdog"])
def test_watchdog_restores_sigterm_handler_after_loop_failure(
    monkeypatch: pytest.MonkeyPatch, package: str
) -> None:
    module = importlib.import_module(package + ".run")
    original = signal.getsignal(signal.SIGTERM)

    def fail_guard(*args: object) -> NoReturn:
        raise RuntimeError("failed startup")

    monkeypatch.setattr(module, "_configure_logging", lambda path: None)
    monkeypatch.setattr(module, "RebootGuard", fail_guard)
    with pytest.raises(RuntimeError, match="failed startup"):
        module.main()
    assert signal.getsignal(signal.SIGTERM) == original

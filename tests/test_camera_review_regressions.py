"""Camera feature review regressions at MQTT, cache and startup boundaries."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

import brilliant_mqtt.__main__ as main_mod
from brilliant_mqtt.ambilight import AmbilightController, AmbilightCrop, retire_ambilight_topics
from brilliant_mqtt.config import Settings
from brilliant_mqtt.retained_topics import RetainedTopicLedger
from tests import test_main as session_tests
from tests.fakes import FakeMqtt
from tests.test_camera_lifecycle import HeldCamera


@pytest.mark.parametrize("interruption", ["denial", "regrant", "generation"])
@pytest.mark.parametrize("in_flight", [False, True])
async def test_cached_color_never_outlives_its_capture_grant(
    interruption: str,
    in_flight: bool,
) -> None:
    epoch = (1, -1, 0)
    allowed = True
    entered, release = asyncio.Event(), asyncio.Event()

    class HeldMqtt(FakeMqtt):
        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if in_flight and topic.endswith("/ambilight/state") and not release.is_set():
                entered.set()
                await release.wait()
            await super().publish(topic, payload, retain=retain, qos=qos)

    mqtt, camera = HeldMqtt(), HeldCamera()
    camera.release.set()
    controller = AmbilightController(
        mqtt, "office", camera, privacy_off=lambda: allowed, privacy_epoch=lambda: epoch
    )
    work = asyncio.create_task(controller.sample_once())
    if in_flight:
        await asyncio.wait_for(entered.wait(), 1)
    else:
        assert await work is not None
        assert controller.last_sample is not None
    epoch = (2, -1, 0) if interruption == "generation" else (1, 5, 0)
    allowed = interruption != "denial"
    release.set()
    if in_flight:
        assert await work is None
    assert controller.last_sample is None
    mqtt.published.clear()
    await controller.on_mqtt_command("brilliant/office/ambilight/set_interval", "2")
    states = [json.loads(p) for t, p, _ in mqtt.published if t.endswith("/ambilight/state")]
    assert states and all(s["color"]["rgb_color"] == [0, 0, 0] for s in states)
    if allowed:
        assert await controller.sample_once() is not None
        assert controller.last_sample is not None
    await controller.shutdown()


@pytest.mark.parametrize("action,payload", [("snapshot", "PRESS"), ("auto_crop", "ON")])
async def test_camera_actions_require_live_valid_intent(action: str, payload: str) -> None:
    mqtt, camera = FakeMqtt(), HeldCamera()
    camera.release.set()
    controller = AmbilightController(
        mqtt, "office", camera, privacy_off=lambda: True, privacy_epoch=lambda: (1, -1, 0)
    )
    await controller.start()
    topic = f"brilliant/office/ambilight/set_{action}"
    await mqtt.inject(topic, payload, retained=True)
    for invalid in ("", "garbage", "press" if action == "snapshot" else "PRESS"):
        await mqtt.inject(topic, invalid)
    assert camera.calls == 0
    await mqtt.inject(topic, payload)
    assert camera.calls == 1
    await controller.shutdown()


@pytest.mark.parametrize("failure", ["subscription", "state"])
async def test_partial_start_closes_registered_handler_and_settles_capture(failure: str) -> None:
    camera = HeldCamera(ignore_cancel=True)
    command: asyncio.Task[None] | None = None

    class FailingMqtt(FakeMqtt):
        async def fail_with_capture(self) -> None:
            nonlocal command
            command = asyncio.create_task(
                self.inject("brilliant/office/ambilight/set_snapshot", "PRESS")
            )
            await asyncio.wait_for(camera.started.wait(), 1)
            raise OSError("startup interrupted after snapshot subscription")

        async def subscribe(self, topic: str) -> None:
            await super().subscribe(topic)
            if failure == "subscription" and topic.endswith("/set_auto_crop"):
                await self.fail_with_capture()

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if failure == "state" and topic.endswith("/ambilight/state"):
                await self.fail_with_capture()
            await super().publish(topic, payload, retain=retain, qos=qos)

    mqtt = FailingMqtt()
    controller = AmbilightController(
        mqtt, "office", camera, privacy_off=lambda: True, privacy_epoch=lambda: (1, -1, 0)
    )
    with pytest.raises(OSError, match="startup interrupted"):
        await controller.start()
    assert controller._closed
    assert camera.settled.is_set()
    assert not controller._camera_tasks
    assert command is not None
    await command
    await mqtt.inject("brilliant/office/ambilight/set_snapshot", "PRESS")
    assert camera.calls == 1
    assert not any(t.endswith("/snapshot") and p for t, p, _ in mqtt.published)


async def test_transient_poll_failure_recovers_after_interval() -> None:
    recovered, sleep_entered, retry = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class FlakyCamera(HeldCamera):
        async def capture_i420_frame(self, crop: AmbilightCrop) -> bytes:
            if self.calls == 0:
                self.calls += 1
                raise OSError("temporary allocation failure")
            recovered.set()
            return await super().capture_i420_frame(crop)

    delays = []

    async def sleep(delay: float) -> None:
        delays.append(delay)
        sleep_entered.set()
        await retry.wait()
        retry.clear()

    camera = FlakyCamera()
    camera.release.set()
    controller = AmbilightController(
        FakeMqtt(),
        "office",
        camera,
        default_enabled=True,
        privacy_off=lambda: True,
        privacy_epoch=lambda: (1, -1, 0),
        sleep=sleep,
    )
    await controller.start()
    await asyncio.wait_for(sleep_entered.wait(), 1)
    assert controller._poll_task is not None and not controller._poll_task.done()
    assert delays == [1.0]
    retry.set()
    await asyncio.wait_for(recovered.wait(), 1)
    assert controller.enabled
    await controller.shutdown()


@pytest.mark.parametrize("field", ["crop", "saved_crop"])
@pytest.mark.parametrize("invalid", [None, "invalid", float("inf"), []])
def test_malformed_persisted_crop_falls_back(tmp_path: Path, field: str, invalid: Any) -> None:
    path = tmp_path / "camera.json"
    path.write_text(json.dumps({field: {"top": invalid}}))
    controller = AmbilightController(FakeMqtt(), "office", HeldCamera(), state_path=path)
    assert controller.crop == AmbilightCrop().normalized()
    assert controller.saved_crop == AmbilightCrop().normalized()


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "0.1", "11"])
def test_camera_interval_config_rejects_nonfinite_and_out_of_range(value: str) -> None:
    with pytest.raises(ValueError, match="AMBILIGHT_INTERVAL_S"):
        Settings.from_env(
            {
                "BRILLIANT_PANEL": "office",
                "MQTT_HOST": "h",
                "MQTT_USERNAME": "u",
                "MQTT_PASSWORD": "p",
                "AMBILIGHT_INTERVAL_S": value,
            }
        )


async def test_installation_disable_retires_only_owned_camera_topics(tmp_path: Path) -> None:
    ledger = RetainedTopicLedger("office", tmp_path / "ledger.json")
    await ledger.async_load()
    mqtt = FakeMqtt()
    unrelated = "brilliant/office/light/state"
    await ledger.async_publish(mqtt, unrelated, "ON")
    controller = AmbilightController(mqtt, "office", HeldCamera(), owned_topics=ledger)
    await controller.start()
    camera_topics = ledger.topics - {unrelated}
    await controller.shutdown()
    mqtt.published.clear()
    await retire_ambilight_topics(mqtt, "office", ledger)
    cleared = {t for t, p, retain in mqtt.published if not p and retain}
    assert cleared == camera_topics
    assert ledger.topics == {unrelated}
    mqtt.published.clear()
    await retire_ambilight_topics(mqtt, "office", ledger)
    assert not mqtt.published


async def test_disabled_session_retires_previous_camera_installation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger_path = tmp_path / "ledger.json"
    ledger = RetainedTopicLedger("office", ledger_path)
    await ledger.async_load()
    await AmbilightController(FakeMqtt(), "office", HeldCamera(), owned_topics=ledger).start()
    previous = ledger.topics
    harness = session_tests._SessionHarness(monkeypatch)
    settings = session_tests._settings()
    object.__setattr__(settings, "retained_topics_file", str(ledger_path))
    finished = asyncio.Event()

    async def retire(mqtt: Any, panel: str, owned: RetainedTopicLedger) -> None:
        await retire_ambilight_topics(mqtt, panel, owned)
        finished.set()

    monkeypatch.setattr(main_mod, "retire_ambilight_topics", retire, raising=False)
    task = asyncio.create_task(main_mod._run_session(settings, None, None))
    try:
        await asyncio.wait_for(finished.wait(), 1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    cleared = {t for t, p, retain, _ in harness.mqtt.published if not p and retain}
    assert previous <= cleared
    assert not previous & ledger.topics


@pytest.mark.parametrize("cached", [False, True])
async def test_state_grant_rechecked_after_ledger_wait(tmp_path: Path, cached: bool) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    epoch = (1, -1, 0)

    class HeldLedger(RetainedTopicLedger):
        block = False

        async def async_publish(self, mqtt: Any, topic: str, payload: str) -> None:
            if self.block and topic.endswith("/ambilight/state"):
                entered.set()
                await release.wait()
            await super().async_publish(mqtt, topic, payload)

    ledger = HeldLedger("office", tmp_path / "ledger.json")
    await ledger.async_load()
    mqtt, camera = FakeMqtt(), HeldCamera()
    camera.release.set()
    controller = AmbilightController(
        mqtt,
        "office",
        camera,
        owned_topics=ledger,
        privacy_off=lambda: True,
        privacy_epoch=lambda: epoch,
    )
    if cached:
        assert await controller.sample_once() is not None
    ledger.block = True
    mqtt.published.clear()
    work = asyncio.create_task(
        controller.on_mqtt_command("brilliant/office/ambilight/set_interval", "2")
        if cached
        else controller.sample_once()
    )
    await asyncio.wait_for(entered.wait(), 1)
    epoch = (1, 10, 0)
    release.set()
    await work
    assert controller.last_sample is None
    assert not any(t.endswith(("/ambilight/state", "/color")) for t, _, _ in mqtt.published)
    await controller.shutdown()

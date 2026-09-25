"""Real loopback MQTT reconnect during a production filesystem rollback rehearsal."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import aiomqtt
import pytest
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.core import HomeAssistant
from paho.mqtt.client import topic_matches_sub

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.panel_health import PanelHealthObserver
from custom_components.brilliant_mqtt.panel_provisioner import stored_snapshot_from_panel
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    ProvisioningOperation,
    ProvisioningPhase,
)
from custom_components.brilliant_mqtt.shell import PanelShell
from tests.canary_rehearsal import RehearsalShell
from tests.test_canary_rollback import durable_store as durable_store
from tests.test_panel_health import (
    AVAILABILITY,
    DISCOVERY,
    METADATA,
    STATE,
    MessageCallback,
    _discovery,
    _FakeHaMqtt,
)
from tests.test_panel_provisioner import _Harness
from tests.test_provisioning_journal import _record

_BROKER = """
import asyncio, logging
from amqtt.broker import Broker
logging.disable(logging.CRITICAL)
async def main():
    broker = Broker({
        "listeners": {"default": {"type": "tcp", "bind": "127.0.0.1:0"}},
        "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}},
    })
    await broker.start()
    print(broker._servers["default"].instance.sockets[0].getsockname()[1], flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        await broker.shutdown()
asyncio.run(main())
"""


class BrokerHaMqtt(_FakeHaMqtt):
    def __init__(self, client: aiomqtt.Client) -> None:
        super().__init__()
        self.client = client
        self.retained_seen = 0

    async def async_subscribe(
        self,
        hass: HomeAssistant,
        topic: str,
        callback: MessageCallback,
        qos: int = 0,
        encoding: str | None = "utf-8",
    ) -> Callable[[], None]:
        self.callbacks[topic] = callback
        await self.client.subscribe(topic, qos=qos)
        self.status_callbacks[topic]()

        def unsubscribe() -> None:
            self.callbacks.pop(topic, None)

        return unsubscribe

    async def receive(self) -> None:
        async for message in self.client.messages:
            for topic, callback in tuple(self.callbacks.items()):
                if topic_matches_sub(topic, str(message.topic)):
                    self.retained_seen += int(message.retain)
                    callback(
                        ReceiveMessage(
                            topic=str(message.topic),
                            payload=bytes(message.payload).decode(),
                            qos=message.qos,
                            retain=message.retain,
                            subscribed_topic=topic,
                            timestamp=time.monotonic(),
                        )
                    )


# Run after the HA socket-guard hook; the marker alone depends on plugin order.
# The fixture restores socket creation and preserves HA's loopback-only connect guard.
@pytest.mark.usefixtures("socket_enabled")
async def test_post_success_rollback_with_measured_disposable_broker_reconnect(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shell = RehearsalShell(tmp_path)
    code = shell.install()
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    record = replace(
        _record(),
        operation=ProvisioningOperation.UPGRADE,
        panel_request=replace(_record().panel_request, selected_components=("bridge",)),
        prior_snapshot=stored_snapshot_from_panel(snapshot),
    )
    shell._pinned = record.panel_request.public_key
    journal = ProvisioningJournal(hass)
    await journal.async_create(record)
    for phase in (
        ProvisioningPhase.ACTIVATION_PENDING,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.VERIFYING,
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
    ):
        await journal.async_transition(record.transaction_id, phase)
    await journal.async_complete_commit(record.transaction_id, subentry_id="fixture-owner")
    (code / "vendor/original.py").write_text("candidate = True\n")
    broker = await asyncio.create_subprocess_exec(
        "uv",
        "run",
        "--no-project",
        "--python",
        "3.10",
        "--with",
        "amqtt==0.11.3",
        "python",
        "-c",
        _BROKER,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        assert broker.stdout is not None
        async with asyncio.timeout(30):
            port = int(await broker.stdout.readline())

        async def publish(client: aiomqtt.Client, identifier: str) -> None:
            for topic, payload in (
                (AVAILABILITY, "online"),
                (METADATA, json.dumps({"agent_version": "0.10.2", "deployment_id": identifier})),
                (STATE, '{"on":true}'),
                (DISCOVERY, _discovery()),
            ):
                await client.publish(topic, payload, qos=1, retain=True)

        # Disconnect an incumbent publisher leaving genuinely retained broker state.
        async with aiomqtt.Client("127.0.0.1", port, identifier="fixture-panel") as previous:
            await publish(previous, "b" * 32)
        async with aiomqtt.Client("127.0.0.1", port, identifier="fixture-observer") as client:
            seam = BrokerHaMqtt(client)
            seam.install(monkeypatch)
            consumer = asyncio.create_task(seam.receive())
            observer = PanelHealthObserver(hass, "office")
            harness = _Harness()
            provisioner = harness.provisioner()
            provisioner._journal = journal
            provisioner._operations = panel_ops
            provisioner._health_observer_factory = lambda _slug: observer
            provisioner._shell_factory = lambda _host, _password, _key: shell

            async def credential(_request: object) -> str:
                return record.panel_request.root_password

            provisioner._credential_resolver = credential
            original_restart = panel_ops.restart_restored
            reconnect_times: list[float] = []

            async def reconnect(
                active_shell: PanelShell,
                baseline: panel_ops.PanelSnapshot,
                identifier: str,
                *,
                on_service_stopped: Callable[[], None],
            ) -> None:
                await original_restart(
                    active_shell, baseline, identifier, on_service_stopped=on_service_stopped
                )
                assert observer._evidence() is None
                env = (shell.root / "etc/brilliant-mqtt.env").read_text()
                assert f"BRILLIANT_DEPLOYMENT_ID={identifier}" in env
                started = time.monotonic()
                async with aiomqtt.Client(
                    "127.0.0.1", port, identifier="fixture-panel"
                ) as restored:
                    reconnect_times.append(time.monotonic() - started)
                    await publish(restored, identifier)

            monkeypatch.setattr(panel_ops, "restart_restored", reconnect)
            try:
                started = time.monotonic()
                await provisioner.async_rollback(record.transaction_id)
                elapsed = time.monotonic() - started
                assert await journal.async_load() is None
                retained = await journal.async_retained(record.transaction_id)
                assert retained is not None and retained.state == "restored"
                assert retained.recovery_deployment_id != "b" * 32
                assert seam.retained_seen > 0
                assert (code / "vendor/original.py").read_text() == "original = True\n"
                assert elapsed < 300 and reconnect_times[0] < 90
                print(
                    f"recovery_seconds={elapsed:.6f}; reconnect_seconds={reconnect_times[0]:.6f}; "
                    "mqtt=real_loopback_amqtt; agent_publications=fixture"
                )
            finally:
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
    finally:
        broker.terminate()
        await broker.wait()

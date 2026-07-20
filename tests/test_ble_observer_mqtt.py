"""Bounded non-retained BLE MQTT publisher tests."""

from __future__ import annotations

import asyncio
import logging

import pytest

from brilliant_ble_observer.config import Settings
from brilliant_ble_observer.model import AdvertisementEnvelope, AllowlistEntry
from brilliant_ble_observer.mqtt import (
    AdvertisementPublisher,
    MqttConnectionConfig,
    PublisherStats,
    advertisement_topic,
    health_topic,
)

BATTERY_UUID = "0000180f-0000-1000-8000-00805f9b34fb"
IBEACON_BYTES = bytes.fromhex("021500112233445566778899aabbccddeeff00420007c5")


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeTransport:
    def __init__(
        self,
        *,
        connect_error: Exception | None = None,
        failed_observations: int = 0,
    ) -> None:
        self.connect_error = connect_error
        self.failed_observations = failed_observations
        self.published: list[tuple[str, str, int, bool]] = []
        self.connect_count = 0
        self.disconnect_count = 0
        self.observation_published = asyncio.Event()

    async def connect(self) -> None:
        self.connect_count += 1
        if self.connect_error is not None:
            raise self.connect_error

    async def publish(self, topic: str, payload: str, *, qos: int, retain: bool) -> None:
        self.published.append((topic, payload, qos, retain))
        if topic.endswith("/advertisement") and self.failed_observations:
            self.failed_observations -= 1
            raise ConnectionError("broker link dropped")
        if topic.endswith("/advertisement"):
            self.observation_published.set()

    async def disconnect(self) -> None:
        self.disconnect_count += 1


def _settings(*, rate: float = 10.0, allowlist: tuple[AllowlistEntry, ...] = ()) -> Settings:
    return Settings(
        panel="shed",
        mqtt_host="mqtt.iot.joyful.house",
        mqtt_username="brilliant-shed",
        mqtt_password="not-a-real-password",
        max_events_per_second=rate,
        allowlist=allowlist,
    )


def _advertisement(
    sequence: int,
    *,
    address_suffix: int = 0xFF,
    adapter_address: str = "11:22:33:44:55:66",
    manufacturer_data: bytes = IBEACON_BYTES,
) -> AdvertisementEnvelope:
    return AdvertisementEnvelope(
        panel="shed",
        adapter_address=adapter_address,
        boot_id="123e4567-e89b-12d3-a456-426614174000",
        session_id="223e4567-e89b-12d3-a456-426614174000",
        sequence=sequence,
        address=f"AA:BB:CC:DD:EE:{address_suffix:02X}",
        address_type="public",
        rssi=-61,
        local_name="Wallet",
        tx_power=-59,
        service_uuids=(BATTERY_UUID,),
        service_data={BATTERY_UUID: b"\xaa\xbb\xcc"},
        manufacturer_data={76: manufacturer_data},
        capture_monotonic_ms=sequence,
    )


async def _cancel(task: asyncio.Task[None]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_observations_are_qos_zero_nonretained_and_health_is_separate() -> None:
    transport = FakeTransport()
    configs: list[MqttConnectionConfig] = []

    def factory(config: MqttConnectionConfig) -> FakeTransport:
        configs.append(config)
        return transport

    publisher = AdvertisementPublisher(_settings(), transport_factory=factory)
    advertisement = _advertisement(1)
    assert publisher.enqueue(advertisement)

    task = asyncio.create_task(publisher.run())
    await asyncio.wait_for(transport.observation_published.wait(), timeout=1)
    await _cancel(task)

    assert configs == [
        MqttConnectionConfig(
            hostname="mqtt.iot.joyful.house",
            port=1883,
            username="brilliant-shed",
            password="not-a-real-password",
            identifier="brilliant-ble-shed",
            will_topic="brilliant/ble/v1/shed/status",
            will_payload="offline",
            will_qos=0,
            will_retain=True,
        )
    ]
    assert transport.published == [
        (health_topic("shed"), "online", 0, True),
        (advertisement_topic("shed"), advertisement.to_json(), 0, False),
        (health_topic("shed"), "offline", 0, True),
    ]
    assert transport.disconnect_count == 1
    assert publisher.stats == PublisherStats(
        queued=0,
        published=1,
        dropped_oldest=0,
        rate_limited=0,
        publish_failures=0,
        reconnects=0,
    )


async def test_bounded_queue_drops_oldest_pending_observation() -> None:
    transport = FakeTransport()
    publisher = AdvertisementPublisher(
        _settings(rate=100.0),
        transport_factory=lambda _config: transport,
        queue_size=2,
    )
    first = _advertisement(1, address_suffix=1)
    second = _advertisement(2, address_suffix=2)
    third = _advertisement(3, address_suffix=3)

    assert publisher.enqueue(first)
    assert publisher.enqueue(second)
    assert publisher.enqueue(third)
    assert publisher.stats.queued == 2
    assert publisher.stats.dropped_oldest == 1

    task = asyncio.create_task(publisher.run())
    for _attempt in range(20):
        observations = [item for item in transport.published if item[0].endswith("/advertisement")]
        if len(observations) == 2:
            break
        await asyncio.sleep(0)
    await _cancel(task)

    observations = [item for item in transport.published if item[0].endswith("/advertisement")]
    assert [item[1] for item in observations] == [second.to_json(), third.to_json()]


def test_rate_limit_is_per_device_identity_and_adapter_source() -> None:
    clock = FakeClock()
    publisher = AdvertisementPublisher(
        _settings(rate=2.0),
        transport_factory=lambda _config: FakeTransport(),
        monotonic=clock,
    )

    assert publisher.enqueue(_advertisement(1))
    assert not publisher.enqueue(_advertisement(2))
    assert publisher.enqueue(_advertisement(3, address_suffix=1))
    assert publisher.enqueue(_advertisement(4, adapter_address="22:33:44:55:66:77"))
    clock.advance(0.5)
    assert publisher.enqueue(_advertisement(5))
    assert publisher.stats.rate_limited == 1


def test_rotating_addresses_for_one_ibeacon_share_a_rate_bucket() -> None:
    clock = FakeClock()
    other_ibeacon = bytearray(IBEACON_BYTES)
    other_ibeacon[21] = 8
    publisher = AdvertisementPublisher(
        _settings(
            rate=2.0,
            allowlist=(
                AllowlistEntry(address="AA:BB:CC:DD:EE:01"),
                AllowlistEntry(
                    ibeacon_uuid="00112233-4455-6677-8899-aabbccddeeff",
                    ibeacon_major=66,
                    ibeacon_minor=7,
                ),
                AllowlistEntry(
                    ibeacon_uuid="00112233-4455-6677-8899-aabbccddeeff",
                    ibeacon_major=66,
                    ibeacon_minor=8,
                ),
            ),
        ),
        transport_factory=lambda _config: FakeTransport(),
        monotonic=clock,
    )

    assert publisher.enqueue(_advertisement(1, address_suffix=1))
    assert not publisher.enqueue(_advertisement(2, address_suffix=2))
    assert publisher.enqueue(
        _advertisement(3, address_suffix=3, manufacturer_data=bytes(other_ibeacon))
    )
    assert publisher.enqueue(
        _advertisement(
            4,
            address_suffix=4,
            adapter_address="22:33:44:55:66:77",
        )
    )


async def test_publish_failures_reconnect_with_exponential_backoff() -> None:
    transports = iter(
        (
            FakeTransport(failed_observations=1),
            FakeTransport(failed_observations=1),
            FakeTransport(),
        )
    )
    created: list[FakeTransport] = []
    sleeps: list[float] = []

    def factory(_config: MqttConnectionConfig) -> FakeTransport:
        transport = next(transports)
        created.append(transport)
        return transport

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await asyncio.sleep(0)

    publisher = AdvertisementPublisher(
        _settings(),
        transport_factory=factory,
        sleep=sleep,
        initial_backoff=1.0,
        max_backoff=8.0,
    )
    assert publisher.enqueue(_advertisement(1, address_suffix=1))
    assert publisher.enqueue(_advertisement(2, address_suffix=2))
    assert publisher.enqueue(_advertisement(3, address_suffix=3))

    task = asyncio.create_task(publisher.run())
    for _attempt in range(20):
        if len(created) == 3:
            break
        await asyncio.sleep(0)
    assert len(created) == 3
    await asyncio.wait_for(created[-1].observation_published.wait(), timeout=1)
    await _cancel(task)

    assert sleeps == [1.0, 2.0]
    assert [transport.disconnect_count for transport in created] == [1, 1, 1]
    assert publisher.stats.publish_failures == 2
    assert publisher.stats.reconnects == 2
    assert publisher.stats.published == 1


async def test_connect_failure_is_cleaned_up_before_reconnect() -> None:
    first = FakeTransport(connect_error=ConnectionError("broker unavailable"))
    second = FakeTransport()
    transports = iter((first, second))
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await asyncio.sleep(0)

    publisher = AdvertisementPublisher(
        _settings(),
        transport_factory=lambda _config: next(transports),
        sleep=sleep,
        initial_backoff=1.0,
    )
    assert publisher.enqueue(_advertisement(1))

    task = asyncio.create_task(publisher.run())
    await asyncio.wait_for(second.observation_published.wait(), timeout=1)
    await _cancel(task)

    assert sleeps == [1.0]
    assert first.disconnect_count == second.disconnect_count == 1
    assert publisher.stats.reconnects == 1
    assert publisher.stats.publish_failures == 0


async def test_logs_never_include_advertisement_payload_or_private_address(
    caplog: pytest.LogCaptureFixture,
) -> None:
    transport = FakeTransport()
    publisher = AdvertisementPublisher(_settings(), transport_factory=lambda _config: transport)
    advertisement = _advertisement(1)
    publisher.enqueue(advertisement)

    with caplog.at_level(logging.DEBUG, logger="brilliant_ble_observer.mqtt"):
        task = asyncio.create_task(publisher.run())
        await asyncio.wait_for(transport.observation_published.wait(), timeout=1)
        await _cancel(task)

    log_text = caplog.text
    assert advertisement.address not in log_text
    assert IBEACON_BYTES.hex() not in log_text
    assert advertisement.local_name is not None
    assert advertisement.local_name not in log_text


def test_connection_config_repr_redacts_password() -> None:
    config = MqttConnectionConfig.from_settings(_settings())

    assert "not-a-real-password" not in repr(config)
    assert config.password == "not-a-real-password"

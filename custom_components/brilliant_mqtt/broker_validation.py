"""End-to-end validation of Home Assistant and device MQTT broker paths."""

from __future__ import annotations

import asyncio
import inspect
import math
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from functools import partial
from time import monotonic
from typing import Any
from uuid import UUID, uuid4

from homeassistant.components import mqtt
from homeassistant.components.mqtt.const import (
    CONF_DISCOVERY_PREFIX,
    DEFAULT_PREFIX,
)
from homeassistant.components.mqtt.const import (
    DOMAIN as MQTT_DOMAIN,
)
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.config_entries import SOURCE_IGNORE, ConfigEntryState
from homeassistant.core import HomeAssistant

from .broker import BrokerProfile, DeviceMqttClient, DeviceMqttMessage
from .errors import OperationError, OperationStage
from .setup_protocol import SetupRequest, SetupResult, SetupTopics

_DeviceClientFactory = Callable[
    [BrokerProfile, str],
    AbstractAsyncContextManager[DeviceMqttClient],
]


@dataclass(frozen=True, slots=True)
class BrokerValidationResult:
    """Redacted result for one successful broker validation."""

    setup_id: UUID
    completed_stages: tuple[OperationStage, ...]
    elapsed_seconds: float
    stage_elapsed_seconds: tuple[tuple[OperationStage, float], ...]

    def redacted_dict(self) -> dict[str, object]:
        """Return the complete, fixed diagnostic representation."""
        return {
            "setup_id": str(self.setup_id),
            "completed_stages": [stage.value for stage in self.completed_stages],
            "elapsed_seconds": self.elapsed_seconds,
            "stage_elapsed_seconds": {
                stage.value: elapsed for stage, elapsed in self.stage_elapsed_seconds
            },
        }


@dataclass(slots=True)
class _ValidationState:
    topics: SetupTopics
    device_context: AbstractAsyncContextManager[DeviceMqttClient] | None = None
    device: DeviceMqttClient | None = None
    device_messages: AsyncIterator[DeviceMqttMessage] | None = None
    ha_unsubscribes: list[Callable[[], None]] = field(default_factory=list)
    device_subscriptions: list[str] = field(default_factory=list)
    retained_topics: list[str] = field(default_factory=list)


class BrokerValidator:
    """Prove both MQTT principals share the required broker behavior."""

    def __init__(
        self,
        hass: HomeAssistant,
        device_client_factory: _DeviceClientFactory,
        timeout_seconds: float = 10.0,
    ) -> None:
        if (
            not isinstance(timeout_seconds, (int, float))
            or isinstance(timeout_seconds, bool)
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("invalid_validation_timeout")
        self.hass = hass
        self._device_client_factory = device_client_factory
        self._timeout_seconds = float(timeout_seconds)

    async def async_validate(
        self,
        profile: BrokerProfile,
        setup_id: UUID | None = None,
    ) -> BrokerValidationResult:
        """Run the ordered broker validation without mutating config entries."""
        candidate_id = uuid4() if setup_id is None else setup_id
        if not isinstance(candidate_id, UUID) or candidate_id.version != 4:
            raise ValueError("invalid_setup_id") from None
        validation_id = candidate_id
        topics = SetupTopics.for_id(validation_id)
        state = _ValidationState(topics)
        panel_nonce = f"panel-{secrets.token_urlsafe(24)}"
        ha_nonce = f"ha-{secrets.token_urlsafe(24)}"
        discovery_nonce = f"discovery-{secrets.token_urlsafe(24)}"
        retained_nonce = f"retained-{secrets.token_urlsafe(24)}"
        started = monotonic()
        completed: list[OperationStage] = []
        elapsed: list[tuple[OperationStage, float]] = []
        primary: BaseException | None = None

        try:
            await self._run_stage(
                OperationStage.HA_MQTT_READY,
                "ha_mqtt_unavailable",
                self._async_ha_mqtt_ready,
                allowed_codes={
                    "ha_mqtt_unavailable",
                    "unsupported_discovery_prefix",
                },
            )
            self._record_stage(
                OperationStage.HA_MQTT_READY,
                completed,
                elapsed,
                started,
            )

            await self._run_stage(
                OperationStage.FLEET_AUTH,
                "fleet_auth_failed",
                lambda: self._async_open_device(state, profile, validation_id),
            )
            self._record_stage(
                OperationStage.FLEET_AUTH,
                completed,
                elapsed,
                started,
            )

            await self._run_stage(
                OperationStage.PANEL_TO_HA,
                "panel_to_ha_timeout",
                lambda: self._async_panel_to_ha(state, validation_id, panel_nonce),
            )
            self._record_stage(
                OperationStage.PANEL_TO_HA,
                completed,
                elapsed,
                started,
            )

            await self._run_stage(
                OperationStage.HA_TO_PANEL,
                "ha_to_panel_timeout",
                lambda: self._async_ha_to_panel(
                    state,
                    validation_id,
                    ha_nonce,
                    panel_nonce,
                ),
            )
            self._record_stage(
                OperationStage.HA_TO_PANEL,
                completed,
                elapsed,
                started,
            )

            await self._run_stage(
                OperationStage.DISCOVERY_WRITE,
                "discovery_write_denied",
                lambda: self._async_discovery_write(state, discovery_nonce),
            )
            self._record_stage(
                OperationStage.DISCOVERY_WRITE,
                completed,
                elapsed,
                started,
            )

            await self._run_stage(
                OperationStage.RETAINED_MESSAGE,
                "retained_message_invalid",
                lambda: self._async_retained_message(state, retained_nonce),
            )
            self._record_stage(
                OperationStage.RETAINED_MESSAGE,
                completed,
                elapsed,
                started,
            )
        except BaseException as error:
            primary = error

        cleanup_started = monotonic()
        cleanup_failure, cleanup_control = await self._async_cleanup(state)
        cleanup_elapsed = monotonic() - cleanup_started

        if primary is not None and not isinstance(primary, Exception):
            raise primary
        if cleanup_control is not None:
            raise cleanup_control
        if isinstance(primary, OperationError):
            if cleanup_failure:
                raise primary.with_cleanup_error(_cleanup_error()) from None
            raise primary from None
        if isinstance(primary, Exception):
            mapped_primary = OperationError.for_code(
                OperationStage.HA_MQTT_READY,
                "ha_mqtt_unavailable",
            )
            if cleanup_failure:
                mapped_primary = mapped_primary.with_cleanup_error(_cleanup_error())
            raise mapped_primary from None
        if cleanup_failure:
            raise _cleanup_error() from None

        completed.append(OperationStage.CLEANUP)
        elapsed.append((OperationStage.CLEANUP, cleanup_elapsed))
        return BrokerValidationResult(
            setup_id=validation_id,
            completed_stages=tuple(completed),
            elapsed_seconds=monotonic() - started,
            stage_elapsed_seconds=tuple(elapsed),
        )

    async def _run_stage(
        self,
        stage: OperationStage,
        default_code: str,
        operation: Callable[[], Awaitable[None]],
        *,
        allowed_codes: set[str] | None = None,
    ) -> None:
        failure: BaseException | None = None
        try:
            async with asyncio.timeout(self._timeout_seconds):
                await operation()
        except BaseException as error:
            failure = error

        if failure is None:
            return
        if not isinstance(failure, Exception):
            raise failure
        if (
            isinstance(failure, OperationError)
            and allowed_codes is not None
            and failure.stage is stage
            and failure.code in allowed_codes
        ):
            raise failure from None
        raise OperationError.for_code(stage, default_code) from None

    async def _async_ha_mqtt_ready(self) -> None:
        if not await mqtt.async_wait_for_mqtt_client(self.hass):
            raise OperationError.for_code(
                OperationStage.HA_MQTT_READY,
                "ha_mqtt_unavailable",
            )

        connected = False
        try:
            connected = mqtt.is_connected(self.hass)
        except Exception:
            pass
        if not connected:
            raise OperationError.for_code(
                OperationStage.HA_MQTT_READY,
                "ha_mqtt_unavailable",
            )

        entries = [
            entry
            for entry in self.hass.config_entries.async_entries(MQTT_DOMAIN)
            if entry.state is ConfigEntryState.LOADED
            and entry.disabled_by is None
            and entry.source != SOURCE_IGNORE
        ]
        if len(entries) != 1:
            raise OperationError.for_code(
                OperationStage.HA_MQTT_READY,
                "ha_mqtt_unavailable",
            )
        effective = dict(entries[0].data)
        effective.update(entries[0].options)
        if effective.get(CONF_DISCOVERY_PREFIX, DEFAULT_PREFIX) != DEFAULT_PREFIX:
            raise OperationError.for_code(
                OperationStage.HA_MQTT_READY,
                "unsupported_discovery_prefix",
            )

    async def _async_open_device(
        self,
        state: _ValidationState,
        profile: BrokerProfile,
        setup_id: UUID,
    ) -> None:
        context = self._device_client_factory(
            profile,
            f"brilliant-mqtt-setup-{setup_id}",
        )
        state.device_context = context
        device = await context.__aenter__()
        state.device = device
        state.device_messages = device.messages

    async def _async_panel_to_ha(
        self,
        state: _ValidationState,
        setup_id: UUID,
        panel_nonce: str,
    ) -> None:
        received = self.hass.loop.create_future()

        def on_message(message: ReceiveMessage) -> None:
            if received.done() or message.topic != state.topics.panel_to_ha:
                return
            if not isinstance(message.payload, (bytes, str)):
                return
            try:
                request = SetupRequest.from_payload(message.payload)
            except (TypeError, ValueError):
                return
            if request.setup_id == setup_id and request.nonce == panel_nonce:
                received.set_result(None)

        await self._async_subscribe_ha(state, state.topics.panel_to_ha, on_message)
        assert state.device is not None
        await state.device.publish(
            state.topics.panel_to_ha,
            SetupRequest(setup_id, panel_nonce).to_payload(),
            qos=1,
            retain=False,
        )
        await received

    async def _async_ha_to_panel(
        self,
        state: _ValidationState,
        setup_id: UUID,
        ha_nonce: str,
        panel_nonce: str,
    ) -> None:
        assert state.device is not None
        assert state.device_messages is not None
        await state.device.subscribe(state.topics.ha_to_panel, qos=1)
        state.device_subscriptions.append(state.topics.ha_to_panel)
        await mqtt.async_publish(
            self.hass,
            state.topics.ha_to_panel,
            SetupResult(setup_id, ha_nonce, panel_nonce).to_payload(),
            qos=1,
            retain=False,
        )
        async for message in state.device_messages:
            if message.topic != state.topics.ha_to_panel:
                continue
            try:
                result = SetupResult.from_payload(message.payload)
            except (TypeError, ValueError):
                continue
            if (
                result.setup_id == setup_id
                and result.nonce == ha_nonce
                and result.reply_to_nonce == panel_nonce
            ):
                return

    async def _async_discovery_write(
        self,
        state: _ValidationState,
        nonce: str,
    ) -> None:
        received = self.hass.loop.create_future()

        def on_message(message: ReceiveMessage) -> None:
            if (
                not received.done()
                and message.topic == state.topics.discovery_probe
                and _payload_matches(message.payload, nonce)
            ):
                received.set_result(None)

        await self._async_subscribe_ha(
            state,
            state.topics.discovery_probe,
            on_message,
        )
        assert state.device is not None
        state.retained_topics.append(state.topics.discovery_probe)
        await state.device.publish(
            state.topics.discovery_probe,
            nonce,
            qos=1,
            retain=True,
        )
        await received

    async def _async_retained_message(
        self,
        state: _ValidationState,
        nonce: str,
    ) -> None:
        assert state.device is not None
        state.retained_topics.append(state.topics.retained)
        await state.device.publish(
            state.topics.retained,
            nonce,
            qos=1,
            retain=True,
        )
        received = self.hass.loop.create_future()

        def on_message(message: ReceiveMessage) -> None:
            if (
                not received.done()
                and message.topic == state.topics.retained
                and message.retain is True
                and _payload_matches(message.payload, nonce)
            ):
                received.set_result(None)

        await self._async_subscribe_ha(state, state.topics.retained, on_message)
        await received

    async def _async_subscribe_ha(
        self,
        state: _ValidationState,
        topic: str,
        callback: Callable[[ReceiveMessage], Any],
    ) -> None:
        subscribed = self.hass.loop.create_future()

        def on_subscribe_done() -> None:
            if not subscribed.done():
                subscribed.set_result(None)

        remove_status = mqtt.async_on_subscribe_done(
            self.hass,
            topic,
            1,
            on_subscribe_done,
        )
        try:
            unsubscribe = await mqtt.async_subscribe(
                self.hass,
                topic,
                callback,
                qos=1,
            )
            state.ha_unsubscribes.append(unsubscribe)
            await subscribed
        finally:
            remove_status()

    async def _async_cleanup(
        self,
        state: _ValidationState,
    ) -> tuple[bool, BaseException | None]:
        """Try every action with its own bound.

        The total worst-case bound is the number of applicable actions multiplied
        by ``timeout_seconds``; one timeout never skips a later cleanup action.
        """
        failed = False
        control: BaseException | None = None

        actions: list[Callable[[], Awaitable[None]]] = []
        device = state.device
        if device is not None:
            for topic in state.retained_topics:
                actions.append(
                    partial(
                        device.publish,
                        topic,
                        b"",
                        qos=1,
                        retain=True,
                    )
                )
        for topic in state.retained_topics:
            actions.append(
                partial(
                    mqtt.async_publish,
                    self.hass,
                    topic,
                    b"",
                    qos=1,
                    retain=True,
                )
            )
        for unsubscribe in state.ha_unsubscribes:
            actions.append(partial(_async_call, unsubscribe))
        if device is not None:
            for topic in state.device_subscriptions:
                actions.append(partial(device.unsubscribe, topic))
            actions.append(device.disconnect)
        context = state.device_context
        if context is not None and device is not None:
            actions.append(partial(_async_exit, context))

        for action in actions:
            action_failure = await self._async_cleanup_action(action)
            if action_failure is None:
                continue
            if not isinstance(action_failure, Exception):
                if control is None:
                    control = action_failure
            else:
                failed = True
        return failed, control

    async def _async_cleanup_action(
        self,
        action: Callable[[], Awaitable[None]],
    ) -> BaseException | None:
        try:
            async with asyncio.timeout(self._timeout_seconds):
                await action()
        except BaseException as error:
            return error
        return None

    @staticmethod
    def _record_stage(
        stage: OperationStage,
        completed: list[OperationStage],
        elapsed: list[tuple[OperationStage, float]],
        validation_started: float,
    ) -> None:
        previous = sum(duration for _, duration in elapsed)
        completed.append(stage)
        elapsed.append((stage, max(0.0, monotonic() - validation_started - previous)))


def _payload_matches(payload: object, expected: str) -> bool:
    if isinstance(payload, str):
        return payload == expected
    if isinstance(payload, bytes):
        return payload == expected.encode()
    return False


async def _async_call(callback: Callable[[], Any]) -> None:
    result = callback()
    if inspect.isawaitable(result):
        await result


async def _async_exit(
    context: AbstractAsyncContextManager[DeviceMqttClient],
) -> None:
    await context.__aexit__(None, None, None)


def _cleanup_error() -> OperationError:
    return OperationError.for_code(OperationStage.CLEANUP, "cleanup_failed")

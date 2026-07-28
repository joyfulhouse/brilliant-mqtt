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
from typing import Any, cast
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
from .errors import OperationError, OperationStage, from_exception
from .setup_protocol import (
    MAX_PREFLIGHT_REPORT_BYTES,
    PreflightReport,
    PreflightRequest,
    PreflightStage,
    SetupRequest,
    SetupResult,
    SetupTopics,
)
from .shell import PanelProcess, RunResult

_DeviceClientFactory = Callable[
    [BrokerProfile, str],
    AbstractAsyncContextManager[DeviceMqttClient],
]
_PanelLauncher = Callable[[str], Awaitable[PanelProcess]]
_FLEET_AUTH_CODES = {
    "broker_tls_verification_failed",
    "broker_authentication_failed",
    "broker_connection_rejected",
    "broker_unavailable",
    "broker_connect_failed",
    "broker_timeout",
}


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


@dataclass(slots=True)
class _PanelValidationState:
    topics: SetupTopics
    ha_unsubscribes: list[Callable[[], None]] = field(default_factory=list)
    process: PanelProcess | None = None
    panel_request_observed: bool = False
    ha_response_published: bool = False
    discovery_observed: bool = False
    callback_failure: OperationError | None = None
    callback_failure_event: asyncio.Event = field(default_factory=asyncio.Event)
    panel_request_event: asyncio.Event = field(default_factory=asyncio.Event)
    ha_response_event: asyncio.Event = field(default_factory=asyncio.Event)
    discovery_event: asyncio.Event = field(default_factory=asyncio.Event)


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
                allowed_codes=_FLEET_AUTH_CODES,
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
                "discovery_write_timeout",
                lambda: self._async_discovery_write(state, discovery_nonce),
                allowed_codes={
                    "discovery_write_denied",
                    "operation_failed",
                },
            )
            self._record_stage(
                OperationStage.DISCOVERY_WRITE,
                completed,
                elapsed,
                started,
            )

            await self._run_stage(
                OperationStage.RETAINED_MESSAGE,
                "retained_message_timeout",
                lambda: self._async_retained_message(state, retained_nonce),
                allowed_codes={
                    "retained_message_invalid",
                    "operation_failed",
                },
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

    async def async_validate_panel(
        self,
        profile: BrokerProfile,
        launcher: _PanelLauncher,
        setup_id: UUID | None = None,
    ) -> BrokerValidationResult:
        """Coordinate one bounded panel-side preflight through Home Assistant."""
        candidate_id = uuid4() if setup_id is None else setup_id
        if not isinstance(candidate_id, UUID) or candidate_id.version != 4:
            raise ValueError("invalid_setup_id") from None
        if not isinstance(profile, BrokerProfile):
            raise TypeError("profile must be a BrokerProfile")
        if not callable(launcher):
            raise TypeError("launcher must be callable")

        validation_id = candidate_id
        state = _PanelValidationState(SetupTopics.for_id(validation_id))
        request_box = [
            PreflightRequest(
                setup_id=validation_id,
                panel_nonce=f"panel-{secrets.token_urlsafe(24)}",
                ha_nonce=f"ha-{secrets.token_urlsafe(24)}",
                timeout_seconds=self._timeout_seconds,
            )
        ]
        started = monotonic()
        completed: list[OperationStage] = []
        elapsed: list[tuple[OperationStage, float]] = []
        process_result: RunResult | None = None
        panel_report: PreflightReport | None = None
        launched_process: PanelProcess | None = None
        primary: BaseException | None = None
        settlement_failed = False
        settlement_control: BaseException | None = None
        del profile

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
                OperationStage.PANEL_TO_HA,
                "panel_to_ha_timeout",
                partial(
                    self._async_prepare_panel_request_subscription,
                    state,
                    request_box[0],
                ),
            )
            await self._run_stage(
                OperationStage.DISCOVERY_WRITE,
                "discovery_write_timeout",
                partial(
                    self._async_prepare_discovery_subscription,
                    state,
                    request_box[0],
                ),
            )

            async with asyncio.timeout(min(self._timeout_seconds * 8.0, 300.0)):
                launched_process = await launcher(request_box[0].to_json())
                if not _is_panel_process(launched_process):
                    raise TypeError("invalid_panel_process")
                state.process = launched_process
                process_result, callback_failure = await self._async_wait_panel_process(
                    state,
                    launched_process,
                )
                if callback_failure is not None:
                    primary = callback_failure
                elif launched_process.running:
                    primary = OperationError.for_code(
                        OperationStage.CLEANUP,
                        "operation_failed",
                    )
                elif process_result is not None:
                    panel_report, report_error = _parsed_panel_report(
                        process_result,
                        validation_id,
                    )
                    if report_error is not None:
                        primary = report_error
                    elif panel_report is not None and panel_report.success:
                        callback_failure = await self._async_wait_panel_success_evidence(state)
                        if callback_failure is not None:
                            primary = callback_failure
        except BaseException as error:
            primary = error

        settled_process = state.process
        if primary is not None and settled_process is not None:
            if settled_process.running:
                terminate_failed = False
                try:
                    settled_process.terminate()
                except Exception:
                    terminate_failed = True
                if terminate_failed:
                    settlement_failed = True
            settle_failed, settle_control = await self._async_settle_panel_process(settled_process)
            settlement_failed = settlement_failed or settle_failed
            settlement_control = settle_control

        mapped_primary: OperationError | None = None
        control: BaseException | None = None
        if primary is not None:
            if not isinstance(primary, Exception):
                control = primary
            elif isinstance(primary, OperationError):
                mapped_primary = primary
            elif isinstance(primary, TimeoutError):
                mapped_primary = _panel_evidence_error(state)
            else:
                mapped_primary = OperationError.for_code(
                    OperationStage.FLEET_AUTH,
                    "operation_failed",
                )
        elif panel_report is not None:
            if panel_report.success:
                for panel_stage in panel_report.completed_stages:
                    operation_stage = OperationStage(panel_stage.value)
                    completed.append(operation_stage)
                    elapsed.append(
                        (
                            operation_stage,
                            panel_report.stage_elapsed_ms[panel_stage] / 1_000,
                        )
                    )
            else:
                mapped_primary = _panel_report_error(panel_report)
        else:
            mapped_primary = OperationError.for_code(
                OperationStage.CLEANUP,
                "operation_failed",
            )

        cleanup_failed, cleanup_control = await self._async_cleanup_panel(state)
        request_box.clear()
        process_result = None
        panel_report = None
        launched_process = None
        primary = None
        state.process = None
        state.ha_unsubscribes.clear()
        del settled_process
        del launcher

        if control is not None:
            raise control
        if settlement_control is not None:
            raise settlement_control
        if cleanup_control is not None:
            raise cleanup_control
        if mapped_primary is not None:
            if settlement_failed or cleanup_failed:
                mapped_primary = mapped_primary.with_cleanup_error(_cleanup_error())
            raise mapped_primary from None
        if settlement_failed or cleanup_failed:
            raise _cleanup_error() from None

        return BrokerValidationResult(
            setup_id=validation_id,
            completed_stages=tuple(completed),
            elapsed_seconds=monotonic() - started,
            stage_elapsed_seconds=tuple(elapsed),
        )

    async def _async_prepare_panel_request_subscription(
        self,
        state: _PanelValidationState,
        request: PreflightRequest,
    ) -> None:
        async def on_message(message: ReceiveMessage) -> None:
            if (
                state.panel_request_observed
                or message.topic != state.topics.panel_to_ha
                or message.qos != 1
                or message.retain is not False
                or message.subscribed_topic != state.topics.panel_to_ha
                or not isinstance(message.payload, (bytes, str))
            ):
                return
            try:
                received = SetupRequest.from_payload(message.payload)
            except (TypeError, ValueError):
                return
            if received.setup_id != request.setup_id or received.nonce != request.panel_nonce:
                return

            state.panel_request_observed = True
            state.panel_request_event.set()
            publish_failed = False
            try:
                await mqtt.async_publish(
                    self.hass,
                    state.topics.ha_to_panel,
                    SetupResult(
                        request.setup_id,
                        request.ha_nonce,
                        request.panel_nonce,
                    ).to_payload(),
                    qos=1,
                    retain=False,
                )
            except Exception:
                publish_failed = True
            if publish_failed:
                state.callback_failure = OperationError.for_code(
                    OperationStage.HA_TO_PANEL,
                    "operation_failed",
                )
                state.callback_failure_event.set()
                return
            state.ha_response_published = True
            state.ha_response_event.set()

        await self._async_subscribe_ha(
            state,
            state.topics.panel_to_ha,
            on_message,
        )

    async def _async_prepare_discovery_subscription(
        self,
        state: _PanelValidationState,
        request: PreflightRequest,
    ) -> None:
        def on_message(message: ReceiveMessage) -> None:
            if (
                not state.discovery_observed
                and message.topic == state.topics.discovery_probe
                and message.qos == 1
                and message.retain is False
                and message.subscribed_topic == state.topics.discovery_probe
                and _payload_matches(message.payload, request.panel_nonce)
            ):
                state.discovery_observed = True
                state.discovery_event.set()

        await self._async_subscribe_ha(
            state,
            state.topics.discovery_probe,
            on_message,
        )

    async def _async_wait_panel_success_evidence(
        self,
        state: _PanelValidationState,
    ) -> OperationError | None:
        evidence_wait = asyncio.gather(
            state.panel_request_event.wait(),
            state.ha_response_event.wait(),
            state.discovery_event.wait(),
        )
        callback_wait = asyncio.create_task(
            state.callback_failure_event.wait(),
            name="brilliant-mqtt-panel-preflight-evidence-callback",
        )
        try:
            done, _ = await asyncio.wait(
                (evidence_wait, callback_wait),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if callback_wait in done and state.callback_failure is not None:
                evidence_wait.cancel()
                await asyncio.gather(evidence_wait, return_exceptions=True)
                return state.callback_failure
            callback_wait.cancel()
            await asyncio.gather(callback_wait, return_exceptions=True)
            await evidence_wait
            return None
        except BaseException:
            for task in (evidence_wait, callback_wait):
                if not task.done():
                    task.cancel()
            await asyncio.gather(evidence_wait, callback_wait, return_exceptions=True)
            raise

    async def _async_wait_panel_process(
        self,
        state: _PanelValidationState,
        process: PanelProcess,
    ) -> tuple[RunResult | None, OperationError | None]:
        process_wait = asyncio.create_task(
            process.wait(),
            name="brilliant-mqtt-panel-preflight-wait",
        )
        callback_wait = asyncio.create_task(
            state.callback_failure_event.wait(),
            name="brilliant-mqtt-panel-preflight-callback",
        )
        try:
            done, _ = await asyncio.wait(
                (process_wait, callback_wait),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if callback_wait in done and state.callback_failure is not None:
                process_wait.cancel()
                await asyncio.gather(process_wait, return_exceptions=True)
                return None, state.callback_failure
            callback_wait.cancel()
            await asyncio.gather(callback_wait, return_exceptions=True)
            return process_wait.result(), None
        except BaseException:
            for task in (process_wait, callback_wait):
                if not task.done():
                    task.cancel()
            await asyncio.gather(process_wait, callback_wait, return_exceptions=True)
            raise

    async def _async_settle_panel_process(
        self,
        process: PanelProcess,
    ) -> tuple[bool, BaseException | None]:
        settlement = asyncio.create_task(
            process.wait(),
            name="brilliant-mqtt-panel-preflight-settlement",
        )
        deadline = asyncio.get_running_loop().call_later(
            self._timeout_seconds,
            settlement.cancel,
        )
        owner = asyncio.current_task()
        cancellation_count = owner.cancelling() if owner is not None else 0
        control: BaseException | None = None
        try:
            while not settlement.done():
                try:
                    await asyncio.shield(settlement)
                except asyncio.CancelledError as error:
                    if (
                        owner is not None
                        and owner.cancelling() > cancellation_count
                        and control is None
                    ):
                        control = error
                    if settlement.cancelled():
                        break
                except Exception:
                    break
        finally:
            deadline.cancel()
        failed = False
        if settlement.cancelled():
            failed = True
        elif settlement.done():
            try:
                settlement.result()
            except BaseException as error:
                if not isinstance(error, Exception):
                    if control is None:
                        control = error
                else:
                    failed = True
        return failed, control

    async def _async_cleanup_panel(
        self,
        state: _PanelValidationState,
    ) -> tuple[bool, BaseException | None]:
        failed = False
        control: BaseException | None = None
        actions: list[Callable[[], Awaitable[None]]] = [
            *(partial(_async_call, unsubscribe) for unsubscribe in state.ha_unsubscribes),
            partial(
                mqtt.async_publish,
                self.hass,
                state.topics.discovery_probe,
                b"",
                qos=1,
                retain=True,
            ),
            partial(
                mqtt.async_publish,
                self.hass,
                state.topics.retained,
                b"",
                qos=1,
                retain=True,
            ),
        ]
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
        if allowed_codes is not None:
            classified = from_exception(stage, failure)
            if classified.stage is stage and classified.code in allowed_codes:
                raise classified from None
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
        state.device_subscriptions.append(state.topics.ha_to_panel)
        await state.device.subscribe(state.topics.ha_to_panel, qos=1)
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
                and _payload_matches(message.payload, nonce)
            ):
                received.set_result(message.retain is True)

        await self._async_subscribe_ha(state, state.topics.retained, on_message)
        if await received is not True:
            raise OperationError.for_code(
                OperationStage.RETAINED_MESSAGE,
                "retained_message_invalid",
            )

    async def _async_subscribe_ha(
        self,
        state: _ValidationState | _PanelValidationState,
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
        if state.device_messages is not None:
            # Close the suspended consumer first, while its underlying client
            # and context are still valid and before any slower cleanup work.
            actions.append(partial(_async_close_iterator, state.device_messages))
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


def _is_panel_process(value: object) -> bool:
    candidate = cast(Any, value)
    try:
        running = candidate.running
        terminate = candidate.terminate
        wait = candidate.wait
    except Exception:
        return False
    return isinstance(running, bool) and callable(terminate) and callable(wait)


def _parsed_panel_report(
    result: RunResult,
    setup_id: UUID,
) -> tuple[PreflightReport | None, OperationError | None]:
    invalid = (
        not isinstance(result, RunResult)
        or type(result.exit_status) is not int
        or not isinstance(result.stdout, str)
        or not isinstance(result.stderr, str)
    )
    stdout = result.stdout if isinstance(result, RunResult) else ""
    if not invalid:
        encoded_size: int | None = None
        try:
            encoded_size = len(stdout.encode("utf-8"))
        except UnicodeError:
            pass
        invalid = (
            encoded_size is None
            or encoded_size > MAX_PREFLIGHT_REPORT_BYTES + 1
            or not stdout.endswith("\n")
            or "\n" in stdout[:-1]
            or "\r" in stdout
        )

    report: PreflightReport | None = None
    if not invalid:
        try:
            report = PreflightReport.from_json(stdout[:-1])
        except ValueError:
            pass
    stdout = ""
    if report is None or report.setup_id != setup_id:
        return None, OperationError.for_code(
            OperationStage.CLEANUP,
            "operation_failed",
        )
    if report.success:
        if result.exit_status != 0:
            return None, OperationError.for_code(
                OperationStage.CLEANUP,
                "operation_failed",
            )
        return report, None
    if result.exit_status == 0:
        return None, OperationError.for_code(
            OperationStage.CLEANUP,
            "operation_failed",
        )
    return None, _panel_report_error(report)


def _panel_report_error(report: PreflightReport) -> OperationError:
    assert report.failed_stage is not None
    assert report.error_code is not None
    stage = OperationStage(report.failed_stage.value)
    code = "operation_failed"
    if report.failed_stage is PreflightStage.CLEANUP:
        code = "cleanup_failed"
    elif report.error_code == "mqtt_timeout":
        code = {
            PreflightStage.FLEET_AUTH: "broker_timeout",
            PreflightStage.PANEL_TO_HA: "panel_to_ha_timeout",
            PreflightStage.HA_TO_PANEL: "ha_to_panel_timeout",
            PreflightStage.DISCOVERY_WRITE: "discovery_write_timeout",
            PreflightStage.RETAINED_MESSAGE: "retained_message_timeout",
        }.get(report.failed_stage, "operation_failed")
    elif (
        report.failed_stage is PreflightStage.DISCOVERY_WRITE
        and report.error_code == "mqtt_authorization"
    ):
        code = "discovery_write_denied"
    elif (
        report.failed_stage is PreflightStage.RETAINED_MESSAGE
        and report.error_code == "retained_flag_missing"
    ):
        code = "retained_message_invalid"
    elif (
        report.failed_stage is PreflightStage.FLEET_AUTH
        and report.error_code == "mqtt_authorization"
    ):
        code = "fleet_auth_failed"
    elif report.failed_stage is PreflightStage.FLEET_AUTH and report.error_code == "mqtt_connect":
        code = "broker_connect_failed"
    elif (
        report.failed_stage is PreflightStage.FLEET_AUTH and report.error_code == "settings_invalid"
    ):
        code = "panel_settings_invalid"

    error = OperationError.for_code(stage, code)
    if (
        report.failed_stage is not PreflightStage.CLEANUP
        and PreflightStage.CLEANUP not in report.completed_stages
    ):
        error = error.with_cleanup_error(_cleanup_error())
    return error


def _panel_evidence_error(state: _PanelValidationState) -> OperationError:
    if not state.panel_request_observed:
        return OperationError.for_code(
            OperationStage.PANEL_TO_HA,
            "panel_to_ha_timeout",
        )
    if not state.ha_response_published:
        return OperationError.for_code(
            OperationStage.HA_TO_PANEL,
            "operation_failed",
        )
    if not state.discovery_observed:
        return OperationError.for_code(
            OperationStage.DISCOVERY_WRITE,
            "discovery_write_timeout",
        )
    return OperationError.for_code(
        OperationStage.RETAINED_MESSAGE,
        "retained_message_timeout",
    )


async def _async_call(callback: Callable[[], Any]) -> None:
    result = callback()
    if inspect.isawaitable(result):
        await result


async def _async_close_iterator(
    iterator: AsyncIterator[DeviceMqttMessage],
) -> None:
    close = getattr(iterator, "aclose", None)
    if not callable(close):
        return
    result = close()
    if inspect.isawaitable(result):
        await result


async def _async_exit(
    context: AbstractAsyncContextManager[DeviceMqttClient],
) -> None:
    await context.__aexit__(None, None, None)


def _cleanup_error() -> OperationError:
    return OperationError.for_code(OperationStage.CLEANUP, "cleanup_failed")

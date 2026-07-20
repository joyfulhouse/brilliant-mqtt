"""Home Assistant remote-scanner bridge for Brilliant BLE advertisements."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from habluetooth import BaseHaRemoteScanner
from homeassistant.components import mqtt
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback

from .ble_protocol import (
    Advertisement,
    AdvertisementSequenceTracker,
    advertisement_topic,
    decode_advertisement,
)
from .const import CONF_PANEL, DOMAIN

_LOGGER = logging.getLogger(__name__)

type Monotonic = Callable[[], float]
type ScannerDiagnosticValue = bool | int | float | None


def _message_payload(message: ReceiveMessage) -> str | bytes:
    """Normalize HA MQTT's mutable byte payload to the decoder's immutable input."""
    payload = message.payload
    return bytes(payload) if isinstance(payload, bytearray) else payload


def observer_status_topic(panel: str) -> str:
    """Return the retained observer-health topic for one physical panel."""
    return f"{advertisement_topic(panel).removesuffix('/advertisement')}/status"


@callback
def _register_scanner_api(
    hass: HomeAssistant,
    scanner: BrilliantRemoteScanner,
    *,
    connection_slots: int,
    source_domain: str,
    source_config_entry_id: str,
    source_device_id: str,
) -> CALLBACK_TYPE:
    """Late-bind HA Bluetooth so a disabled bridge imports no integration API."""
    from homeassistant.components.bluetooth import async_register_scanner

    return async_register_scanner(
        hass,
        scanner,
        connection_slots=connection_slots,
        source_domain=source_domain,
        source_config_entry_id=source_config_entry_id,
        source_device_id=source_device_id,
    )


class BrilliantRemoteScanner(BaseHaRemoteScanner):
    """One non-connectable HA scanner backed by a Brilliant panel adapter."""

    def __init__(self, *, panel: str, adapter_address: str) -> None:
        self.panel = panel
        super().__init__(
            source=adapter_address,
            adapter=f"brilliant-{panel}",
            connector=None,
            connectable=False,
        )

    @callback
    def async_on_advertisement(
        self,
        advertisement: Advertisement,
        *,
        advertisement_monotonic_time: float,
    ) -> None:
        """Feed one already-validated packet at its Home Assistant receipt time."""
        if advertisement.panel != self.panel:
            raise ValueError("advertisement panel does not match scanner panel")
        if advertisement.adapter_address != self.source:
            raise ValueError("advertisement adapter does not match scanner source")
        self._async_on_advertisement(
            address=advertisement.address,
            rssi=advertisement.rssi,
            local_name=advertisement.local_name,
            service_uuids=list(advertisement.service_uuids),
            service_data=dict(advertisement.service_data),
            manufacturer_data=dict(advertisement.manufacturer_data),
            tx_power=advertisement.tx_power,
            details={"panel": self.panel},
            advertisement_monotonic_time=advertisement_monotonic_time,
        )


def disabled_scanner_diagnostics() -> dict[str, ScannerDiagnosticValue]:
    """Return the stable aggregate shape for a disabled scanner bridge."""
    return {
        "enabled": False,
        "registered": False,
        "observer_online": None,
        "scanning": False,
        "packets_received": 0,
        "packets_accepted": 0,
        "packets_dropped": 0,
        "last_packet_age_seconds": None,
    }


class BrilliantBleScannerBridge:
    """Validate/order MQTT packets and own one panel scanner's complete lifecycle."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        *,
        device_id: str,
        monotonic: Monotonic = time.monotonic,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.panel = str(entry.data[CONF_PANEL])
        self.device_id = device_id
        self._monotonic = monotonic
        self._sequence = AdvertisementSequenceTracker()
        self._adapter_address: str | None = None
        self._scanner: BrilliantRemoteScanner | None = None
        self._scanner_unregister: CALLBACK_TYPE | None = None
        self._scanner_unsetup: CALLBACK_TYPE | None = None
        self._mqtt_unsubscribers: list[CALLBACK_TYPE] = []
        self._observer_online: bool | None = None
        self._packets_received = 0
        self._packets_accepted = 0
        self._packets_dropped = 0
        self._last_accepted_monotonic: float | None = None
        self._closed = False

    @property
    def scanner(self) -> BrilliantRemoteScanner | None:
        """Return the active scanner, if observer health permits one."""
        return self._scanner

    @property
    def last_accepted_monotonic(self) -> float | None:
        """Return the HA receipt time of the latest accepted advertisement."""
        return self._last_accepted_monotonic

    @property
    def diagnostics(self) -> dict[str, ScannerDiagnosticValue]:
        """Return address-free aggregate health and packet counters."""
        last_age = (
            None
            if self._last_accepted_monotonic is None
            else max(0.0, self._monotonic() - self._last_accepted_monotonic)
        )
        return {
            "enabled": True,
            "registered": self._scanner is not None,
            "observer_online": self._observer_online,
            "scanning": self._scanner.scanning if self._scanner is not None else False,
            "packets_received": self._packets_received,
            "packets_accepted": self._packets_accepted,
            "packets_dropped": self._packets_dropped,
            "last_packet_age_seconds": last_age,
        }

    async def async_setup(self) -> None:
        """Subscribe to BLE topics for an explicitly enabled config entry."""
        if self._mqtt_unsubscribers or self._closed:
            raise RuntimeError("BLE scanner bridge is already set up or closed")
        try:
            self._mqtt_unsubscribers.append(
                await mqtt.async_subscribe(
                    self.hass,
                    observer_status_topic(self.panel),
                    self.async_handle_status,
                    qos=0,
                )
            )
            self._mqtt_unsubscribers.append(
                await mqtt.async_subscribe(
                    self.hass,
                    advertisement_topic(self.panel),
                    self.async_handle_advertisement,
                    qos=0,
                )
            )
        except BaseException:
            self.async_shutdown()
            raise

    @callback
    def async_handle_advertisement(self, message: ReceiveMessage) -> None:
        """Isolate a bad/replayed packet without tearing down the config entry."""
        if self._closed:
            return
        self._packets_received += 1
        # Retained offline status is a trust boundary: quarantine advertisements
        # without feeding HA or mutating sequence state until retained online reopens it.
        if self._observer_online is False:
            self._drop_packet()
            return
        try:
            advertisement = decode_advertisement(
                _message_payload(message),
                topic=message.topic,
                retained=message.retain,
            )
            if (
                self._adapter_address is not None
                and advertisement.adapter_address != self._adapter_address
            ):
                raise ValueError("advertisement adapter does not match stable scanner source")
        except ValueError:
            self._drop_packet()
            return

        # Establish HA lifecycle before mutating ordering state on the first packet,
        # allowing a registration failure to be retried with the same sequence.
        if self._adapter_address is None:
            self._ensure_scanner(advertisement.adapter_address)

        try:
            self._sequence.accept(advertisement)
        except ValueError:
            self._drop_packet()
            return

        # Offline tears down only the scanner; source and accepted ordering state persist.
        if self._scanner is None:
            self._ensure_scanner(advertisement.adapter_address)
        scanner = self._scanner
        if scanner is None:  # pragma: no cover - _ensure_scanner sets or raises
            raise RuntimeError("BLE scanner registration did not produce a scanner")
        received_monotonic = self._monotonic()
        scanner.async_on_advertisement(
            advertisement,
            advertisement_monotonic_time=received_monotonic,
        )
        self._packets_accepted += 1
        self._last_accepted_monotonic = received_monotonic

    @callback
    def async_handle_status(self, message: ReceiveMessage) -> None:
        """Apply retained health; offline quarantines feeds and sequence mutation."""
        if self._closed or not message.retain or message.topic != observer_status_topic(self.panel):
            return
        payload = _message_payload(message)
        if isinstance(payload, bytes):
            try:
                status = payload.decode("utf-8")
            except UnicodeDecodeError:
                return
        else:
            status = payload
        if status == "online":
            self._observer_online = True
        elif status == "offline":
            self._observer_online = False
            self._teardown_scanner()

    @callback
    def async_shutdown(self) -> None:
        """Unsubscribe and unregister once, draining every synchronous callback."""
        if self._closed:
            return
        self._closed = True
        self._teardown_scanner()
        unsubscribers = self._mqtt_unsubscribers
        self._mqtt_unsubscribers = []
        for unsubscribe in reversed(unsubscribers):
            try:
                unsubscribe()
            except Exception as error:
                _LOGGER.warning(
                    "%s: BLE MQTT unsubscribe failed (%s)",
                    self.panel,
                    type(error).__name__,
                )

    def _ensure_scanner(self, adapter_address: str) -> None:
        if self._scanner is not None:
            return
        scanner = BrilliantRemoteScanner(panel=self.panel, adapter_address=adapter_address)
        unsetup = scanner.async_setup()
        try:
            unregister = _register_scanner_api(
                self.hass,
                scanner,
                connection_slots=0,
                source_domain=DOMAIN,
                source_config_entry_id=self.entry.entry_id,
                source_device_id=self.device_id,
            )
        except BaseException:
            try:
                unsetup()
            except Exception as error:
                _LOGGER.warning(
                    "%s: BLE scanner cleanup after registration failure failed (%s)",
                    self.panel,
                    type(error).__name__,
                )
            raise
        self._adapter_address = adapter_address
        self._scanner = scanner
        self._scanner_unregister = unregister
        self._scanner_unsetup = unsetup

    def _teardown_scanner(self) -> None:
        unregister = self._scanner_unregister
        unsetup = self._scanner_unsetup
        self._scanner = None
        self._scanner_unregister = None
        self._scanner_unsetup = None
        for operation, label in ((unregister, "unregister"), (unsetup, "unsetup")):
            if operation is None:
                continue
            try:
                operation()
            except Exception as error:
                _LOGGER.warning(
                    "%s: BLE scanner %s failed (%s)",
                    self.panel,
                    label,
                    type(error).__name__,
                )

    def _drop_packet(self) -> None:
        self._packets_dropped += 1
        _LOGGER.debug(
            "%s: dropped invalid or stale BLE advertisement (dropped=%d)",
            self.panel,
            self._packets_dropped,
        )

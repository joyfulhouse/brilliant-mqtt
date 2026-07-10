from __future__ import annotations

import asyncio
from dataclasses import dataclass

import ifaddr
from zeroconf import ServiceStateChange
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

from brilliant_protocol_lab.redaction import safe_id

INIT_SERVICE = "_init-brilliant._tcp.local."
HOME_SERVICE = "_brilliant._tcp.local."


@dataclass(frozen=True)
class ServiceObservation:
    service_type: str
    instance: str
    addresses: tuple[str, ...]
    port: int
    properties: dict[str, str | int]


def normalize_service(
    service_type: str,
    instance: str,
    addresses: tuple[str, ...],
    port: int,
    properties: dict[bytes, bytes | None],
) -> ServiceObservation:
    safe: dict[str, str | int] = {}
    device_id = properties.get(b"device_id")
    home_id = properties.get(b"home_id")
    provisioning_port = properties.get(b"provisioning_port")
    if device_id:
        safe["device_id"] = safe_id(device_id.decode("ascii"))
    if home_id:
        safe["home_id"] = safe_id(home_id.decode("ascii"))
    if provisioning_port:
        safe["provisioning_port"] = int(provisioning_port)
    return ServiceObservation(service_type, instance, addresses, port, safe)


def _interface_ipv4(interface_name: str) -> str:
    for adapter in ifaddr.get_adapters():
        if interface_name not in (adapter.name, adapter.nice_name):
            continue
        for address in adapter.ips:
            if isinstance(address.ip, str):
                return address.ip
    raise ValueError(f"interface {interface_name!r} has no IPv4 address")


async def browse_read_only(interface_name: str, timeout_s: float) -> tuple[ServiceObservation, ...]:
    if timeout_s <= 0:
        raise ValueError("timeout_s must be greater than zero")
    zeroconf = AsyncZeroconf(interfaces=[_interface_ipv4(interface_name)])
    observations: dict[tuple[str, str], ServiceObservation] = {}
    pending: set[asyncio.Task[None]] = set()

    async def resolve(service_type: str, name: str) -> None:
        info = AsyncServiceInfo(service_type, name)
        found = await info.async_request(zeroconf.zeroconf, int(timeout_s * 1000))
        if not found or info.port is None:
            return
        observations[(service_type, name)] = normalize_service(
            service_type=service_type,
            instance=name,
            addresses=tuple(info.parsed_scoped_addresses()),
            port=info.port,
            properties=dict(info.properties),
        )

    def on_change(
        _zeroconf: object,
        service_type: str,
        name: str,
        state: ServiceStateChange,
    ) -> None:
        if state not in (ServiceStateChange.Added, ServiceStateChange.Updated):
            return
        task = asyncio.create_task(resolve(service_type, name))
        pending.add(task)
        task.add_done_callback(pending.discard)

    browsers = [
        AsyncServiceBrowser(zeroconf.zeroconf, service_type, handlers=[on_change])
        for service_type in (INIT_SERVICE, HOME_SERVICE)
    ]
    try:
        await asyncio.sleep(timeout_s)
        if pending:
            await asyncio.gather(*tuple(pending))
        return tuple(
            sorted(observations.values(), key=lambda item: (item.service_type, item.instance))
        )
    finally:
        for browser in browsers:
            await browser.async_cancel()
        await zeroconf.async_close()

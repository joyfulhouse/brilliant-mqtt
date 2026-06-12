# Brilliant Message Bus — API Reference

> Reverse-engineered by introspection on a pilot panel (firmware v26.05.20.2),
> 2026-06-12. The modules are **compiled Cython `.so`** (no source). Treat method
> signatures and field names below as **confirmed-by-introspection** unless
> marked *(infer — confirm in PoC)*. Milestone 1 of the plan re-confirms all of
> this on a live panel and fills the gaps.

All paths are relative to the panel venv site-packages:
`/data/switch-embedded/env/lib/python3.10/site-packages/`

## Connection

- Transport: Apache **Thrift RPC** over the unix socket
  `/var/run/brilliant/server_socket` (owner `brilliant`, mode `srwxrwxrwx`).
- You do **not** open the socket yourself — use the on-box client (`RPCObserver`),
  which encapsulates the transport/framing/protocol (unknown framing; do not
  hand-roll a Thrift client unless the PoC proves it necessary).

## `lib.message_bus_api.observer_interface`

### `RPCObserver(loop)`

Concrete async message-bus client. Construct with an `asyncio` event loop.
Has **built-in auto-reconnect** (`_handle_reconnect`) — this is the reliability
win over `homekit_controller`.

Confirmed methods (from `dir()`):

```
start()                                   # connect + begin serving
shutdown()                                # disconnect cleanly
get_all()                                 # -> all peripherals/devices/state
get_device(...)                           # -> one device                  (infer args)
get_peripheral(...)                       # -> one peripheral               (infer args)
subscribe(...)                            # register interest in device(s)  (infer args)
unsubscribe(...)
handle_notification(...)                  # OVERRIDE: called on state change (push)
report_notification(...)
request_set_variables_in_peripheral(...)  # THE COMMAND CALL — drive a load (infer args)
handle_home_id_updated(...)
get_home_id()  /  unsafe_get_home_id()
get_owning_device_id()                    # this observer's identity on the bus
get_loop()
```

`ObserverInterface` (the ABC `RPCObserver` implements) exposes the same logical
surface: `get_all`, `get_device`, `get_peripheral`, `subscribe`, `unsubscribe`,
`handle_notification`, `report_notification`, `request_set_variables_in_peripheral`,
`handle_home_id_updated`.

### Usage shape (to validate in PoC)

```python
import asyncio
from lib.message_bus_api.observer_interface import RPCObserver

class BridgeObserver(RPCObserver):
    def handle_notification(self, notification):   # signature TBD in PoC
        # parse via lib.message_bus_api.notification_utils, publish to MQTT
        ...

async def main():
    loop = asyncio.get_running_loop()
    obs = BridgeObserver(loop)
    obs.start()
    state = obs.get_all()           # snapshot all devices/variables
    obs.subscribe(...)              # then receive handle_notification callbacks
    # to command a load:
    obs.request_set_variables_in_peripheral(...)
```

> **PoC must determine:** whether `start()`/`get_all()` are sync or coroutine;
> the exact arg shapes for `subscribe`, `get_device`,
> `request_set_variables_in_peripheral`; and how `register_virtual_device` /
> owning-device-id registration works (whether the observer needs to register an
> identity before commanding). **The single best reference is the on-box source
> of `peripherals.homekit.homekit_peripheral`** — read it on-panel; it does
> exactly subscribe + command and shows the real call shapes.

## `lib.message_bus_api` helper modules

- **`notification_utils`** — turn incoming bus notifications into state deltas:
  `apply_modified_variables`, `apply_notification_modifications`,
  `merge_modified_peripherals`, `get_updated_variable`.
- **`peripheral_utils`** — `get_modified_peripheral(s)`, `get_modified_variables`,
  `get_variable_modification_timestamp`, `modified_variable_has_change`.
- **`subscription_utils`** — `matches_subscription`,
  `get_matching_devices_for_subscription_request`.
- **`device_utils`** — `guess_device_type_for_id`, `is_valid_uuid`,
  `is_mobile_device_id`, and constants `VIRTUAL_DEVICES`,
  `KNOWN_VIRTUAL_DEVICE_IDS`, `CLOUD_DEVICES`, etc.

## Data model — `thrift_types.message_bus.ttypes`

Struct classes present (fields *(infer — confirm in PoC)*):

```
Device          # a controllable thing (light/switch/load/sensor). Has an id,
                # a DeviceType, DeviceAttributes, and a set of Variables.
DeviceType      # enum of device kinds
DeviceAttributes
Devices / SavedDevices
Variable        # a single named value on a device (e.g. on/off, brightness)
ModifiedVariable
Peripheral / ModifiedPeripheral / PeripheralStatus / PeripheralType
SubscriptionRequest / SubscriptionNotification
VirtualDeviceRegistration / PeripheralRegistration / MessageBusRegistration
SetVariableResponse / SetUpdatedVariablesResponse
Event / SentinelValue / InitializationTarget
```

> The exact field names of `Device` and `Variable` (e.g. `device_id`, `name`,
> `value`, `device_type`, `variables`) are the **highest-value output of the
> PoC** — they define the HA entity mapping. Capture them with
> `print(thrift_types.message_bus.ttypes.Device.thrift_spec)` and a real
> `get_all()` dump (redact home_id / tokens before saving any dump into the repo).

## Service method list (raw, from the generated client)

`MessageBus` client (`send_*`/`recv_*`/`process_*` wrappers omitted): `get_all`,
`get_device`, `get_peripheral`, `get_attributes`, `subscribe`,
`register_peripheral`, `register_virtual_device`, `set_variables_request`,
`set_updated_variables`, `set_home_id`, `update_peripheral_status`,
`delete_peripheral`, `handle_notification`, `handle_home_id_updated`.

## Reference peripherals to read on-panel (read-only)

- `peripherals/homekit/homekit_peripheral` — **the canonical bus-client + device
  mapping reference.** Check whether it's `.py` (readable) or `.so`.
- `peripherals/lib/peripheral_service` — the peripheral base/framework.
- `lib/clients/{object_store_test_client,web_api,web_api_test_client,cloud_remote_bridge_test_client}`
  — possible simpler client examples (note `web_api` hints at an internal HTTP
  abstraction worth a quick look, though the bus is the clean path).

## Introspection recipe (read-only, safe)

```bash
PY=/data/switch-embedded/env/bin/python3
$PY - <<'PY'
import inspect, importlib
from lib.message_bus_api import observer_interface as oi
print(inspect.signature(oi.RPCObserver.subscribe))
print(inspect.signature(oi.RPCObserver.request_set_variables_in_peripheral))
print(inspect.signature(oi.RPCObserver.get_device))
import thrift_types.message_bus.ttypes as t
print('Device.thrift_spec:', t.Device.thrift_spec)
print('Variable.thrift_spec:', t.Variable.thrift_spec)
PY
```

(Cython may hide some signatures as `(*args, **kwargs)`; fall back to reading the
homekit peripheral source for real call shapes.)

# Milestone 1 PoC Findings — live pilot-panel probe

**Date:** 2026-06-12 · **Panel:** the pilot panel · **Firmware:**
v26.05.20.2 (confirmed live via `hardware_peripheral.current_release_tag`) ·
**Method:** read-only SSH probes + one live read-only bus session
(`get_all()`; **no set/command calls were made**). Redactions: `home_id`,
tokens, and complex blob values are omitted; device ids are truncated.

This document closes the open questions in the design spec §10 and is the
authoritative reference for Milestones 3–7. Where it contradicts the
representative snippets in the plan, **this document wins.**

## 1. Headline answers (spec §10)

| Open question | Answer |
|---|---|
| `start()`/`get_all()` sync or async? | **Every `RPCObserver` method is a coroutine** (start, get_all, get_device, subscribe, request_set_variables_in_peripheral, shutdown) |
| `start()` args? | `start(message_bus_processor, virtual_device_id=None)` — see §2 recipe |
| Need `register_virtual_device` before commanding? | Not for reading; **untested for writes** (deferred to the M7 pilot — read-only PoC). `virtual_device_id=None` connects fine |
| Per-panel ownership vs whole-home graph? | **`get_all()` returns the whole home graph** (36 devices). Filter to `id == get_owning_device_id()` — see §5 |
| `Device`/`Variable` field names + ranges? | Real schema in §4/§6; brightness is `intensity` 0..`max_intensity_value` (1000) |
| MQTT lib on panel / vendoring? | **No mqtt/paho/aiomqtt anywhere on the panel — vendor both.** `/var` is rw with 4.9 G free |
| Does `/etc/systemd/system` survive OTA? | **Inconclusive** (all units dated at the current deployment time). Keep the planned mitigation: Ansible role re-installs + re-enables the unit post-OTA |

## 2. Connection recipe (the `bus.py` adapter contract)

Confirmed working end-to-end on the live panel:

```python
import asyncio
import thrift_types.message_bus.PeripheralService as PS  # only for typing ref
import lib.protocol.message_bus_peer_service as mbps
from lib.protocol.processor import SinglePeerProcessor
from lib.message_bus_api.observer_interface import RPCObserver

loop = asyncio.get_running_loop()
obs = RPCObserver(loop)                      # subclass; override handle_notification
proc = SinglePeerProcessor(
    socket_path="/var/run/brilliant/server_socket",
    my_name="brilliant_mqtt",                # unique bus client name
    handler=mbps.PeripheralServer(obs),      # inbound dispatch -> our observer
    client_class=mbps.MessageBusClient,      # outbound call wrapper
    loop=loop,
)
await proc.start()                           # opens socket + hello handshake
# poll proc.is_connected() (connected in <1 s in practice)
await obs.start(proc, None)                  # registers observer w/ the bus
own_id = obs.get_owning_device_id()          # sync, returns this panel's id
devices = await obs.get_all()                # -> Devices{devices: list, home_id}
...
await obs.shutdown()                         # also proc.shutdown() on exit
```

Notes:
- Ordering matters: `proc.start()` **before** `obs.start(proc)` — the observer
  immediately uses the processor's client (first failure mode we hit was
  `'NoneType' object has no attribute 'get_attributes'` when not yet connected).
- `obs.start()` calls `proc.add_reconnect_callback(...)` — auto-reconnect comes
  from `SinglePeerProcessor`; on reconnect the bridge should re-run its
  reconcile.
- `get_owning_device_id()` / `get_home_id()` are sync getters (cached).
- Constructor contract (from `ProcessorBase`):
  `(my_name, handler, client_class, my_domain=None, my_aliases=None, …, loop=None)`;
  `SinglePeerProcessor` adds `socket_path=` / `peer_address=`.

## 3. Real `RPCObserver` signatures (introspected)

```
__init__(self, loop)
start(self, message_bus_processor, virtual_device_id=None)          async
shutdown(self)                                                       async
get_all(self)                                                        async
get_device(self, device_id)                                          async
get_peripheral(self, device_id, peripheral_id)                       async
subscribe(self, subscription_request, callback_func=None,
          forward_to_message_bus=True)                               async
unsubscribe(self, callback_func, subscription_request=None)
request_set_variables_in_peripheral(self, peripheral_id,
          variable_dict, device_id=None, last_set_timestamps=None)   async
handle_notification(self, notification)        # override point (push)
handle_home_id_updated(...)                    # part of PeripheralService.Iface
get_owning_device_id(self) / get_home_id(self) # sync
```

The inbound service we serve (`PeripheralService.Iface`):
`handle_notification`, `handle_home_id_updated`, `set_variables_request`.

## 4. Real data model (thrift_spec, confirmed)

**A bus `Device` is a participant, not a load.** Loads are `Peripheral`s on a
panel's own CONTROL device.

```
Devices    { devices: list<Device>, home_id: str }
Device     { id: str, peripherals: map<str, Peripheral>, timestamp: i64,
             device_type: DeviceType, version: str }
Peripheral { name: str, variables: map<str, Variable>,
             peripheral_type: PeripheralType, dynamic_variable_prefix: str,
             status: PeripheralStatus, timestamp: i64,
             deleted_variables: list<ModifiedVariable>, version: str }
Variable   { name: str, value: str, timestamp: i64, externally_settable: bool }
ModifiedPeripheral { peripheral_id, deleted, modified_variables:
             list<ModifiedVariable>, status, peripheral_type, …,
             peripheral_type_changed, peripheral_status_changed }
SubscriptionRequest      { device_id: str, peripheral_type: PeripheralType,
                           peripheral_id_glob: str }
SubscriptionNotification { updated_device: Device, timestamp: i64,
                           modified_peripherals: list<ModifiedPeripheral>,
                           deleted: bool }
```

**All `Variable.value`s are strings.** Scalars are plain (`"0"`, `"1"`,
`"600"`, `"43.60"`, `"Lights"`); complex values are base64-encoded thrift
blobs (ignore those). `externally_settable` marks writable variables.

Enums (relevant subset):
- `DeviceType`: 0 UNKNOWN, **1 CONTROL (a physical panel)**, 2 MOBILE_APP,
  3 VIRTUAL, 4 CLOUD, 5 THIRDPARTY_VIRTUAL, 6 VIRTUAL_CONTROL
- `PeripheralType` (~105 values; load-relevant): **27 LIGHT**, **46 ALWAYS_ON**,
  **5 MOTION_SENSOR**, 45 GENERIC_ON_OFF, 40 OUTLET, 53 SHADE, 22 HARDWARE,
  80 CLIMATE_SENSOR, 42 GANGBOX_CONFIGURATION, 12 UI, 66 HOMEKIT
- `PeripheralStatus`: 0 OFFLINE, **1 ONLINE**, 2 DISCONNECTED,
  3 MALFUNCTIONING, 4 DEGRADED → drives per-entity availability

## 5. Device scoping — ANSWERED (spec §4)

`get_all()` on the pilot panel returned **36 devices = the whole home**:

- **15 × CONTROL** — the physical panels, ids are 32-hex strings
  (e.g. `017ff607…`). Each carries its own gangbox load peripherals.
- VIRTUAL: `ble_mesh` (Brilliant smart switches/dimmers: 11 LIGHT + 20
  SWITCH_CONFIGURATION + …), `configuration_virtual_device`,
  `brilliant_virtual_device`
- THIRDPARTY_VIRTUAL: `smartthings` (13 LIGHT…), `tplink`, `hue_bridge`,
  `lifx`, `ring_virtual_device` (11 CAMERA…), `schlage` (6 LOCK), `ecobee`,
  `nest`, `sonos`, `somfy`, `wemo`, `hunter_douglas`, `bluesound`
- CLOUD: `cloud` · MOBILE_APP: 4 phones

**Decision rule (now confirmed): each bridge instance publishes ONLY the
device whose `id == obs.get_owning_device_id()`** — verified to return the
panel's own CONTROL device id. Everything else (VIRTUAL / THIRDPARTY_VIRTUAL /
CLOUD / MOBILE_APP and the other 14 CONTROL panels) is excluded. Third-party
ecosystems (hue, tplink, ring, schlage…) already have native HA integrations.
`device_utils.VIRTUAL_DEVICES / CLOUD_DEVICES / KNOWN_VIRTUAL_DEVICE_IDS`
exist as belt-and-braces filters.

> Note: `ble_mesh` carries Brilliant's own *plug-in/remote* switches and
> dimmers for the whole home. Out of scope for the per-panel bridge (no single
> owner panel); revisit later as a possible elected-publisher extension.

## 6. The panel's own load peripherals (pilot panel detail)

`gangbox_config_peripheral.expected_total_gang_count = 2` → two physical gangs.

### `[LIGHT] gangbox_peripheral_0` — dimmer ("Lights"), 32 variables. Key ones:

| Variable | Observed | Settable | Meaning |
|---|---|---|---|
| `on` | `0` | **yes** | power state, `"0"`/`"1"` |
| `intensity` | `600` | **yes** | brightness, int string |
| `max_intensity_value` | `1000` | no | **brightness scale denominator** |
| `minimum_dim_level` / `maximum_dim_level` | `100` / `1000` | yes | calibration bounds |
| `dimmable` | `1` | yes | dimmer vs relay behavior |
| `display_name` | `Lights` | yes | **the human entity name** |
| `power` | `0` | no | live wattage (sensor) |
| `temperature` | `43.60` | no | internal °C (sensor) |
| `is_safe` | `1` | no | fault flag |

→ HA `light` with brightness: HA 0–255 ↔ `round(intensity / max_intensity_value * 255)`.

### `[ALWAYS_ON] gangbox_peripheral_1` — "Backyard Lamps", 18 variables

**No `on`, no `intensity`** (always-powered circuit). Has live `power` (`52` W),
`temperature`. → not a switchable entity; optionally expose `power` as a
sensor; do not publish a light/switch.

### `[MOTION_SENSOR] faceplate_peripheral` — 12 variables

`movement_detected` (`0`/`1`, → `binary_sensor.motion`), `lux` (→ illuminance
sensor; gated by `enable_lux`), `pir_motion_score`, internal temperatures.

### `[HARDWARE] hardware_peripheral` — diagnostics

`cpu_temperature` (`61`), `current_release_tag` (`v26.05.20.2`),
`screen_brightness`, `muted`, … → optional diagnostic sensors later.

Other peripherals on the panel (UI, VOICE, ART, HOMEKIT, EXECUTION, WIFI,
BLE, OBJECT_STORE, configs…) are infrastructure — **not entities**.

## 7. Command call (signature captured; live test deferred to M7)

```python
await obs.request_set_variables_in_peripheral(
    peripheral_id,              # e.g. "gangbox_peripheral_0"
    variable_dict,              # {"on": "1", "intensity": "750"} — string values
    device_id=own_device_id,    # the owning panel device id
)
```

- The homekit peripheral (same bus client) drives loads through this surface;
  its source is compiled (`.so`) so the exact `variable_dict` value formatting
  is asserted from the string-typed `Variable.value` schema — **verify with
  one live `on` toggle as the first step of the M7 pilot, observing the
  physical load.**
- `set_variables_request` also exists on the inbound service (others asking
  *us* to set variables) — irrelevant unless we register a virtual device.

## 8. Notifications (PILOT-CONFIRMED 2026-06-12)

> **Pilot findings:** (1) the lib's inbound dispatcher
> (`thrift_inspect.handle_method`) **awaits** handler methods — a synchronous
> `handle_notification` override produces `TypeError: object NoneType can't be
> used in 'await' expression` on every push; the override MUST be `async def`.
> (2) The same-loop invocation assumption holds — `create_task` from inside the
> handler works. (3) The §7 command call with plain string values
> (`{"on": "1"}`) works exactly as written and returns a
> `SetVariableResponse`; the physical load responds immediately.

- `subscribe(SubscriptionRequest(device_id=own_id), callback_func=None,
  forward_to_message_bus=True)` registers interest; pushes arrive at
  `handle_notification(notification)` (override) carrying
  `SubscriptionNotification{updated_device, modified_peripherals, deleted}`.
  The lib invokes the override **by keyword** — the parameter MUST be named
  `notification` or every push raises
  `TypeError: ... unexpected keyword argument 'notification'`.
- `lib.message_bus_api.notification_utils`
  (`apply_modified_variables`, `apply_notification_modifications`,
  `merge_modified_peripherals`, `get_updated_variable`) translate deltas.
- The periodic `get_all()` reconcile (design §5) covers any missed push.
- On a healthy session the bus pushes `on`, `power`, and `temperature`
  changes within ~0.5–2 s of the physical change (verified 2026-06-12 with a
  second spy client + 1 s `get_device` poll side by side). `movement_detected`
  is a real, live variable house-wide (observed `1` on another panel during
  the same scan); `lux` reads 0 everywhere because `enable_lux=0` on every
  panel. `get_device(own_id)` costs ~30–80 ms — cheap enough to poll every
  couple of seconds.

### 8b. THE STREAM CAN DIE SILENTLY — and `get_all` is a mirror (2026-06-12)

Observed live on the pilot, with a healthy spy client connected in parallel:

- The deployed bridge's notification stream **stopped delivering pushes with
  no error, while the process kept running** (commands and bus writes still
  worked). The spy client on the same socket received every push instantly.
- During the outage the bridge's `get_all()` returned data **~20 s stale**
  (it reported `on=1/power=418` ~3 s after the spy read `on=0/power=0`):
  `RPCObserver.get_all()/get_device()` are served from the observer's
  **notification-fed in-process mirror**, not by querying the bus — a dead
  stream freezes reads too, so the periodic resync republished stale state.
- The stream later **self-healed via the processor's auto-reconnect**
  (`SinglePeerProcessor.add_reconnect_callback`); everything in the gap was
  simply lost.

Consequences for the bridge (implemented post-pilot):
`add_reconnect_callback` hook → re-subscribe + full reconcile after gaps; a
fast scoped `get_device` poll (`HOT_POLL_SECONDS`) publishing payload diffs to
bound staleness; a stale-stream watchdog (`BUS_STALE_SECONDS`) that rebuilds
the whole session when no push arrives at all.

> Caveat: the earlier "the bus never pushes `muted`" finding was observed
> through the deployed bridge and is **confounded by this failure mode** — a
> dead stream is indistinguishable from a never-pushed variable. The
> optimistic echo + hot poll cover that class either way.

## 9. Runtime / vendoring / OTA facts

- venv `sys.path` = stdlib + `/data/switch-embedded/env/lib/python3.10/site-packages`
  only → our `/var` code needs `PYTHONPATH=/var/brilliant-mqtt/app:/var/brilliant-mqtt/vendor`
  (as designed).
- **No MQTT library on the panel** (no aiomqtt/paho/anything `mqtt` in
  site-packages) → vendor `aiomqtt` + `paho-mqtt` py3.10 wheels.
- `/var` is rw, 4.9 G free (mmcblk0p4, 17 % used).
- `/etc/systemd/system/` exists and is writable root fs; all current units
  carry the deployment timestamp, so OTA survival of a locally-added unit is
  **unconfirmed** — keep the role's re-install-after-OTA step.
- `peripherals.homekit.homekit_peripheral` and the whole peripheral framework
  are compiled Cython `.so` (no readable source anywhere in site-packages);
  everything above came from runtime introspection + one live session.

## 10. Reconciliation for Milestones 3–6 (normalized model)

The plan's representative snippets used `BrilliantDevice(power, brightness)`.
Real mapping for the implementation:

| Plan placeholder | Reality |
|---|---|
| one "device" = one load | one **load = (device_id, peripheral_id)**; only peripherals of the own CONTROL device |
| `power` variable (bool) | `on` variable, string `"0"`/`"1"` |
| `brightness` 0–100 + min/max on Variable | `intensity` string int, scale `0..max_intensity_value` (read from sibling variable, observed 1000) |
| `DeviceKind` guessed from device | HA component from `PeripheralType`: LIGHT→`light`; GENERIC_ON_OFF/OUTLET→`switch`; MOTION_SENSOR→`binary_sensor` (+`lux` sensor); ALWAYS_ON→no control entity (optional power sensor); others→skip |
| device name | `display_name` variable (fallback: peripheral `name`) |
| availability | panel LWT + `PeripheralStatus` (≠ ONLINE → entity unavailable) |
| change events | `SubscriptionNotification.modified_peripherals[].modified_variables[]` |
| `unique_id` | `brilliant_<panel>_<peripheral_id>` (peripheral ids like `gangbox_peripheral_0` are stable per panel) |

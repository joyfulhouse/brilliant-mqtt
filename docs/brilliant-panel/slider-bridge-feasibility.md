# Home Assistant entity to physical-slider feasibility

## Current decision

Bridging a Home Assistant light to a Brilliant physical cap-touch slider is
**structurally supported by firmware but not yet live-validated**.

The `v26.06.03.1` UI selects slider targets by the target peripheral's
slider-gesture capability and `PeripheralType`, not by the `DeviceType` of the
device hosting that peripheral. A correctly owned `LIGHT` peripheral on a
Virtual Control should therefore pass the same type gate as a Brilliant- or
partner-hosted light. This does not prove that an improvised peripheral is
discoverable, selectable, routable, persistent, or removable. Those properties
depend on a real Virtual Control identity and peripheral owner.

No live disposable Virtual-Control-owned HA-backed light exists in the Office
home at the time of this finding, so Virtual-Control picker admission and
physical operation remain unconfirmed. Raw bus injection and physical-Control
hosting are not substitutes: the former rendered transient state without an
owner to accept commands, and the latter interfered with the real Control's
ownership responsibilities.

### 2026-07-13 legacy-tile observation

Five earlier physical-Control-hosted mirror records rendered in their assigned
Backyard and Balcony rooms, but the UI marked them offline. A scoped read-only
bus snapshot identified them as `LIGHT` (27) peripherals owned by Office's
ordinary physical `CONTROL` (DeviceType 1), not a Virtual Control. The
`brilliant-ha-mirror` unit, environment, payload, and process were absent while
`brilliant-mqtt` remained healthy. This is direct live evidence that the native
tile renderer accepts the ordinary light schema and room-assignment metadata,
and that persisted records outlive their host. The offline badge is consistent
with an absent owner/host rather than missing tile metadata.

The operator subsequently opened the Office physical-slider target picker
without selecting or saving a binding and saw the three offline Backyard
lights. This live-confirms that the picker admits these physical-Control-owned
`LIGHT` records despite their offline state. It validates the target type and
selector metadata more strongly than room rendering alone.

This observation still does **not** advance the Virtual Control ownership,
online routing, binding, or physical-operation gates. The records have no live
owner to accept slider writes and are attached to the wrong device type for the
proposed architecture. No target was selected and no binding was changed.

## Required data path

```text
Home Assistant light
  <-> retained MQTT command/state route
  <-> one bounded Virtual-Control process
  <-> Virtual-Control-owned LIGHT peripheral
  <-> Brilliant home graph and native target selector
  <-> CapTouchSliderConfig(device_id, peripheral_id)
  <-> explicitly selected physical Control slider
```

Every arrow must work in both directions. Slider gestures must produce exactly
one HA command, and HA state must update the hosted peripheral so the panel does
not snap back to stale state. The hosted light must have a stable peripheral ID,
valid room assignment, the Virtual Control's own configuration linkage, and an
active owner for its full lifetime.

## Decompiled UI evidence

The following symbols are Ghidra names in the sanitized, ignored analysis
project for `switch-ui`; addresses are relative to that firmware ELF and are
not stable API names.

| Evidence | Finding | Integration consequence |
|---|---|---|
| Qt meta-object strings | `SwitchSliderSettingsScreen` exports `homePeripheralSelector`, `supportsSliderConfiguredPeripheral`, and `sliderCapabilitiesText`. | Native slider assignment has a deliberate eligibility model; writing a config directly would bypass behavior that must be validated. |
| `FUN_00382a20` | The screen constructor calls `FUN_003f3cc8(..., 3)` and retains the returned type collection. | Capability index 3 supplies the screen's permitted peripheral types. |
| `FUN_001e7bbc` | `HomePeripheralSelector` copies the supplied type collection into its filter state and composes source/proxy models for the picker. Offline filtering, if any, can occur inside those subordinate models and is not disproved by the screen-level getter. | Picker admission still requires a live UI observation; the type-only getter is not the whole selector pipeline. |
| `FUN_0039f664` | The generic gesture renderer calls the same `FUN_003f3cc8(..., 3)` collection while formatting the UI label `Slider Gesture`; a type outside the collection becomes `Invalid selection`. | The collection is the firmware's slider-gesture peripheral-type filter, not an account/device-host allowlist invented by this integration. |
| `FUN_00385878` | The `supportsSliderConfiguredPeripheral` getter resolves the configured `(device_id, peripheral_id)`. If resolution returns no target it returns true; a resolved target returns true only when its type identifier is in the retained capability collection. | Eligibility is evaluated on the resolved target peripheral. The function does not reject a target because its host is DeviceType 6 (`VIRTUAL_CONTROL`). |
| `FUN_00383900` | `sliderCapabilitiesText` explicitly handles `MUSIC` (3), `LIGHT` (27), `OUTLET` (40), `GENERIC_ON_OFF` (45), and `SHADE` (53). Light copy includes tap/flick toggle and slide-up/down brightness behavior. | A dimmable HA light should use `PeripheralType.LIGHT` and the standard light variables; a generic invented type will not work. |
| `FUN_001fcb08` | The generic peripheral-action path logs `Peripheral data is null or offline, ignoring` and returns before dispatch when its resolved data is absent or offline. | A persisted offline tile cannot prove command routing or operate as a usable slider bridge, even if the picker happens to list it. |
| Shipped test templates and Thrift types | Slider bindings serialize `CapTouchSliderConfig` with a slider index plus target `device_id` and `peripheral_id`. | The physical slider points at the Virtual Control and hosted light; it does not point at an HA entity ID or MQTT topic directly. |

The strongest source-level conclusion is narrow: **DeviceType 6 is not itself a
slider-eligibility blocker once its hosted `LIGHT` resolves in the home graph.**
The direct configured-target getter is type-only, but the picker is composed
from additional proxy models and ordinary action dispatch rejects offline data.
Selector discovery, room filtering, online/ownership state, command routing,
restart persistence, and cleanup therefore remain separate live gates.

## Hosted-light contract

The single-light pilot uses the ordinary light schema already consumed by the
UI and slider path:

| Variable | Type | Writable | Pilot purpose |
|---|---:|---:|---|
| `on` | integer/boolean semantic | yes | Tap/flick state and HA on/off |
| `intensity` | integer, 0–1000 | yes | Physical and on-screen dimming |
| `dimmable` | integer/boolean semantic | no | Advertise slider brightness support |
| `max_intensity_value` | integer | no | Declare the 1000-point scale |
| `minimum_dim_level` | integer | yes | Preserve a valid lower bound |
| `maximum_dim_level` | integer | yes | Preserve a valid upper bound |
| `display_name` | string | yes | Native label; independent of stable ID |
| `room_assignment` | Thrift `RoomAssignment` | yes | Native room placement and discovery context |
| `mode_transition_settings` | serialized string | yes | Match the normal light surface |
| `configuration_peripheral_id` | string | no | Link only to the disposable Virtual Control's configuration peripheral |

HA brightness 0–255 must be converted to Brilliant intensity 0–1000 with
round-half-up behavior in both directions. A state echo or retry must not become
a second HA command.

The bounded implementation now lives in
`tools/brilliant_vc/single_light_pilot.py`. Off-panel tests prove the exact
schema, scaling boundaries, stable ID, canonical socket isolation, MQTT
envelopes, HA-restart sequence epochs, unavailable-state fencing, interleaved
duplicate suppression, reconnect/resubscribe behavior, one-registration
lifecycle with a cross-process runtime lease, bounded cancellation cleanup,
and idempotent cleanup. Its apply-mode lease is acquired before live bus
preflight and held through cleanup, preventing concurrent pilots from racing or
deleting one another's registration. Its live preflight also re-reads the
isolated VC bus and refuses to register unless all of the following are
simultaneously true:

- the message-bus owning identity is the provisioned 32-hex VC ID, not Office;
- the own Device record reports DeviceType 6;
- the selected room exists in the decoded scoped `home_configuration.rooms`;
- the complete room and VC peripheral sets match the root-only topology
  snapshot, with exactly one VC-owned configuration-type peripheral; and
- the configuration link is not
  `brilliant_virtual_device_configuration`.

This implementation evidence does not advance the live status. No provisioned
VC identity, configuration peripheral, or native selector entry has yet been
observed. In particular, the firmware may provision no suitable VC-owned
configuration peripheral; that result is an explicit blocked outcome, not a
reason to borrow a shared or physical configuration.

The existing HA control-plane consumer requires `null` for `turn_on` and
`turn_off`, and an integer 0–255 for `set_brightness`. The pilot emits that
exact contract, carries the latest observed HA sequence, never retains a
command, and applies HA feedback through the framework's internal updater so it
cannot echo into another command. A broker reconnect keeps the single native
host but fences commands until retained HA authority is replayed; transient HA
unavailability similarly shows safe `off` and remains fenced until recovery.

## Provisioning boundary

The firmware ships
`WebAPIProvisioningClient.get_virtual_control_self_bootstrap(property_id,
token)`. Static strings confirm that it posts to
`/provisioning/virtual-control-self-bootstrap` and returns a device ID, PKCS#12
certificate, and serialized bootstrap parameters. It does **not** mint the
required scoped token. The bundled test client can create synthetic tokens for
test infrastructure; that is not a supported production credential path.

The prior account JWT was rejected with HTTP 401 and its claims did not allow
the self-bootstrap endpoint. The feasibility run therefore remains gated on a
token produced by an official Brilliant app workflow. Do not guess a GraphQL
mutation, reuse the test-client secret, or replay an unrelated account token.

## What will confirm feasibility

The answer becomes `confirmed` only when one disposable, officially
provisioned Virtual Control passes all of these checks:

1. Its single owned `LIGHT` renders on Office and a second panel in the intended
   room.
2. The light appears in the native physical-slider picker without a hand-written
   `slider_config`.
3. After a separate approval naming the physical slider, the operator binds it
   through the native UI and performs the approved tap/dim checks; the agent
   does not trigger a load or scene.
4. Each physical gesture reaches HA once, and HA on/off/brightness state returns
   to both panels without oscillation.
5. HA, MQTT, Virtual Control, Office, and second-panel restarts preserve or
   reconcile the route.
6. WAN-up and WAN-denied tests classify the real cloud dependency.
7. The exact original physical-slider binding is restored in the native UI.
8. The light and Virtual Control are removed through supported paths, with two
   later snapshots showing no tile, owner, or stale slider reference.

Until then, the accurate integration status is `source-eligible, live-blocked`.

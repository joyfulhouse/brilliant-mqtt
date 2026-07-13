# Home Assistant control plane and Brilliant scene bridge

This is the authoritative maintainer and operator guide for the safe Home
Assistant integration. It describes the source and off-panel tests at commit
`747b207`. Hardware support remains conditional on the Office acceptance gate
in the [scene-bridge pilot runbook](runbooks/scene-bridge-pilot.md).

The approved architecture is in the
[control-plane and Virtual Control design](../superpowers/specs/2026-07-12-ha-control-plane-and-virtual-control-design.md).
The corresponding [implementation plan](../superpowers/plans/2026-07-12-ha-control-plane-scene-bridge.md)
records the validation and retirement gates.

## Status and evidence

| Surface | Status | Evidence and limit |
|---|---|---|
| HA-owned entity selection, area resolution, manifest/state publishing, and constrained HA service execution | Implemented; off-panel tested | Unit/integration tests cover registries, mappings, MQTT, lifecycle, idempotency, and failure fencing. No production panel transport consumes this generic entity manifest yet. |
| Panel scene/mode catalogs, execution events, and confirmed commands | Implemented; off-panel tested | The agent reuses its existing bus/MQTT clients. Firmware codecs and lifecycle paths are tested with acquired-format fixtures and fakes. |
| HA scene select, run button, services, events, configured scene actions, and diagnostics | Implemented; off-panel tested | HA tests cover strict input, action dispatch, confirmation, timeouts, restarts, and bounded state. |
| Office deployment and physical acceptance | Pending hardware acceptance | Do not call the feature hardware-supported until every criterion in the [Office runbook](runbooks/scene-bridge-pilot.md) passes. |
| Native HA device/room tiles on Brilliant | Blocked research | Physical-Control hosting is rejected. Virtual Control remains blocked behind the [feasibility gates](../superpowers/plans/2026-07-12-virtual-control-feasibility-gates.md). |
| Creating or editing Brilliant scenes/modes | Not implemented | The bridge catalogs and executes existing IDs only. It never creates arbitrary configuration blobs. |

Evidence labels and safe validation levels are defined in the generic
[panel validation runbook](validation-runbook.md). Firmware structure and
behavior are documented in [software architecture](software-architecture.md),
[UI information architecture](ui-information-architecture.md), and
[peripheral/control surfaces](peripheral-surfaces.md).

## The native-tile expectation

> **Enabling this baseline does not create `HA_PILOT_ROOM_D`, or any other HA
> room/device tile, in Backyard or anywhere else in Brilliant's native UI.**

The supported HA surfaces are:

- one scene select and one run-selected button per loaded panel;
- the `brilliant_mqtt.run_scene` and `brilliant_mqtt.set_mode` services;
- `brilliant_mqtt_scene` and `brilliant_mqtt_mode` events;
- configured HA actions for known Brilliant scene executions; and
- redacted integration diagnostics.

Existing Brilliant scenes remain visible in Brilliant's own UI because they are
native configuration. Labels, HA areas, Brilliant-room overrides, and display
metadata prepare an HA-owned entity manifest; they do not render panel tiles
while the native peripheral transport is blocked.

The earlier pilots proved that room metadata was necessary but insufficient.
The UI excludes empty room assignments from ordinary room models, so a valid
typed `RoomAssignment` is required for a native peripheral. But physical-Control
hosting still co-managed the real Control, added peers, and threatened physical
load responsiveness. Raw injected records could render only when enough graph
metadata propagated, but without an owned manager their UI commands routed
elsewhere and state reverted. Metadata cannot repair ownership or routing. See
the [deprecated mirror guide](../ha-mirror.md) for the retirement decision.

## Ownership and topology

```text
Home Assistant
  entity/device/area/label registries
  state, services, configured actions, diagnostics
                    |
                    | local MQTT: brilliant/ha-control/v1/...
                    v
one existing brilliant-mqtt process per panel
  existing MQTT session + existing message-bus observer/session
  scene/mode catalog codec + execution adapter
                    |
                    v
configuration_virtual_device + the Control's execution_peripheral
```

Home Assistant owns all entity selection, names, devices, areas, service calls,
and configured actions. MQTT is the local versioned transport. Each panel's one
existing `brilliant-mqtt` process constructs `SceneBridge` with the exact bus and
MQTT objects already used by the forward bridge; enabling it does not create a
second adapter session.

The scene bridge does not instantiate `PeripheralHost`, host a peripheral, add a
manager/lease, bid for a virtual device, or overwrite ownership. It only reads
scoped native configuration, observes the existing execution peripheral, and
writes one reviewed execution variable. The panel stores MQTT connection
material as before, but no HA WebSocket URL or HA access credential is required
by this bridge.

## Configuration

The seven control settings are fleet-global values copied to each panel config
entry. At runtime, the enabled entry with the lexicographically smallest panel
slug supplies the singleton settings. New panels inherit the existing global
values instead of creating a second control plane.

| UI/config key | Default | Validation and ownership |
|---|---|---|
| HA control enabled (`ha_control_enabled`) | `false` | HA-owned global. It starts HA subscriptions/publication and renders `SCENE_BRIDGE_ENABLED=1` during the normal panel reconfigure/redeploy path. |
| HA control label (`ha_control_label`) | `brilliant` | Non-empty, trimmed string, at most 256 characters. HA looks up this label and selects labeled entity-registry entries. |
| Room overrides (`room_overrides`) | `{}` | JSON object, at most 200 entries. Each trimmed HA-area key and Brilliant-room value is non-empty and at most 256 characters. Matching is case-insensitive after trimming. |
| Domains (`ha_control_domains`) | `light`, `switch` | Unique list drawn from `light`, `switch`, `lock`, and `cover`. This controls generic manifest eligibility; it does not create a native tile. |
| Maximum entities (`max_mirrored_entities`) | `50` | Integer from 1 through 200. Applied after deterministic entity-ID sorting. |
| Default scene panel (`scene_panel`) | current panel on first setup | Must be a loaded panel slug. A service call may override it with an attached panel. |
| Scene actions (`scene_actions`) | `{}` | JSON object with at most 1,024 entries. It maps existing panel/scene IDs to constrained HA service calls; shape below. |

The JSON forms are bounded to 64 KiB, 2,048 JSON nodes, depth 12, and 4,096
characters per general string. Invalid JSON is rejected rather than partially
applied. Room, area, label, domain, and maximum fields influence only the
HA-owned generic manifest today. They are groundwork for a future gated native
transport, not evidence that native HA tiles are available.

### Scene-action JSON

Every key is `<panel-slug>:<scene-id>`. Every action has exactly `domain`,
`service`, `target`, and `data`. `target` may contain only `entity_id`,
`device_id`, and/or `area_id`; `data` is bounded safe JSON.

```json
{
  "office:all_off": {
    "domain": "input_boolean",
    "service": "turn_on",
    "target": {
      "entity_id": "input_boolean.brilliant_scene_pilot_marker"
    },
    "data": {}
  }
}
```

Configured actions currently apply to scene events, not mode events. The HA
event is fired first; the configured action is then dispatched non-blocking. A
dispatch exception is logged in sanitized form and does not retract the event.

## Entity selection and mapping precedence

The generic manifest is deterministic:

1. Look up the configured HA label and select labeled entity-registry entries.
2. Sort selected entries by `entity_id`.
3. Keep only supported, enabled domains.
4. Resolve the HA area from the entity-registry area first, then the associated
   device-registry area. An entity area always wins.
5. Apply an explicit case-insensitive, trimmed HA-area to Brilliant-room
   override. Without an override, the HA area name is copied into the manifest.
6. Truncate to the configured maximum.

Friendly name precedence is the live state's `friendly_name`, registry name,
registry original name, then entity ID. Device class precedence is the live
state attribute, registry device class, then original device class.

The published command vocabulary is capability-derived:

| Domain | Commands |
|---|---|
| `light` | `turn_on`, `turn_off`; `set_brightness` only when HA advertises a valid brightness-capable color mode |
| `switch` | `turn_on`, `turn_off` |
| `lock` | `lock`, `unlock` |
| `cover` | `open`, `close`, `set_position`, and/or `set_tilt`, only when the corresponding HA feature bit exists |

Command values are `null` for actions without an argument, integer 0–255 for
`set_brightness`, and integer 0–100 for cover position or tilt. State payloads
allowlist brightness, current position, current tilt position, supported feature
mask, and device class; arbitrary HA attributes are not copied to MQTT.

## MQTT version 1 contract

All JSON is canonical compact JSON with sorted keys. `schema_version` and
`mapping_version` are both `1`. Stable entity IDs are deterministic UUIDv5
values; panel slugs are lowercase, percent-free slugs.

### Generic HA entity control plane

| Topic | Retained | Exact payload fields |
|---|---:|---|
| `brilliant/ha-control/v1/manifest` | Yes | `schema_version`, `mapping_version`, `revision`, `generated_at_ms`, `entities`, `unsupported_domains`. Each entity: `stable_id`, `entity_id`, `domain`, `device_class`, `friendly_name`, `ha_area`, `brilliant_room`, `commands`, `capabilities`. |
| `brilliant/ha-control/v1/state/<stable_id>` | Yes | `schema_version`, `mapping_version`, `stable_id`, `entity_id`, `sequence`, `generated_at_ms`, `available`, `state`, `attributes`. |
| `brilliant/ha-control/v1/command/<stable_id>` | No | `schema_version`, `mapping_version`, `command_id`, `stable_id`, `kind`, `value`, `observed_sequence`, `issued_at_ms`. |
| `brilliant/ha-control/v1/result/<command_id>` | No | `schema_version`, `mapping_version`, `command_id`, `stable_id`, `accepted`, `resulting_sequence`, `timestamp_ms`, `error`, `elapsed_ms`. `error` is always present and is `null` on acceptance. |

The HA integration publishes manifest/state and implements the constrained
entity command executor. The current panel scene bridge does **not** subscribe to
or consume the generic manifest/state/command topics. Do not interpret their
presence as a working native entity transport.

### Per-panel scenes and modes

Replace `<panel>` with a validated panel slug and `<command_id>` with a UUID.

| Topic | Retained | Exact payload fields |
|---|---:|---|
| `brilliant/ha-control/v1/scene/catalog/<panel>` | Yes | `schema_version`, `mapping_version`, `panel`, `generated_at_ms`, `scenes`; each scene has `scene_id`, `display_name`, `icon`. |
| `brilliant/ha-control/v1/mode/catalog/<panel>` | Yes | `schema_version`, `mapping_version`, `panel`, `generated_at_ms`, `modes`; each mode has `mode_id`, `display_name`. |
| `brilliant/ha-control/v1/scene/event/<panel>` | No | `schema_version`, `mapping_version`, `panel`, `scene_id`, `executed_at_ms`, `deduplication_key`. |
| `brilliant/ha-control/v1/mode/event/<panel>` | No | `schema_version`, `mapping_version`, `panel`, `mode_id`, `executed_at_ms`, `deduplication_key`. |
| `brilliant/ha-control/v1/scene/command/<panel>` | No | `schema_version`, `mapping_version`, `command_id`, `panel`, `scene_id`, `issued_at_ms`. |
| `brilliant/ha-control/v1/mode/command/<panel>` | No | `schema_version`, `mapping_version`, `command_id`, `panel`, `mode_id`, `issued_at_ms`. |
| `brilliant/ha-control/v1/scene/result/<command_id>` | No | `schema_version`, `mapping_version`, `command_id`, `panel`, `scene_id`, `accepted`, `timestamp_ms`; rejected results add `error`. |
| `brilliant/ha-control/v1/mode/result/<command_id>` | No | `schema_version`, `mapping_version`, `command_id`, `panel`, `mode_id`, `accepted`, `timestamp_ms`; rejected results add `error`. |
| `brilliant/ha-control/v1/status/scene/<panel>` | Yes | `schema_version`, `mapping_version`, `transport` (`scene`), `panel`, `available`, `reason`, `timestamp_ms`. |
| `brilliant/ha-control/v1/status/mode/<panel>` | Yes | The same status fields with `transport` set to `mode`. |

Catalog/status retained messages are valid broker replay. Event, command, and
result messages must not be retained. HA requires exact keys for inbound
scene/mode catalogs, status, events, and results. The shared command decoder
validates the required version, IDs, timestamp, topic context, and retain flag,
but currently does not reject every additional JSON key; consumers must not use
extra fields as an extension mechanism.

## Scene and mode semantics

The acquired firmware stores scene and mode definitions under
`configuration_virtual_device`. The bridge performs scoped reads of only
`scene_configuration` and `mode_configuration`, decodes their binary Thrift
definitions, and publishes IDs/display names (plus scene icon). An absent native
configuration peripheral produces a valid empty catalog; malformed data marks
the corresponding transport unhealthy.

The bridge observes the Control's existing `execution_peripheral` through its
shared bus session. Scene executions are dynamic variables named
`execution_state:scene_execution_handler:scene:<scene_id>` whose binary payload
contains an execution timestamp. A non-empty `manual_mode_id` update uses the
bus variable timestamp as the mode execution time.

HA-to-panel commands are accepted only for an ID in the latest catalog and only
while the execution transport is available. The bridge writes exactly one
variable on the existing execution peripheral:

- scene: `last_executed_scene_id=<existing-scene-id>`;
- mode: `manual_mode_id=<existing-mode-id>`.

The write response is not success. A scene command succeeds only after a newer
matching dynamic scene execution record. A mode command succeeds only after a
newer matching `manual_mode_id` observation. The bridge cannot create, edit,
upload, or execute arbitrary scene blobs.

## Home Assistant surfaces

### Events

`brilliant_mqtt_scene` data:

```yaml
panel: office
scene_id: all_off
executed_at_ms: 1700000000300
deduplication_key: office:all_off:1700000000300
```

`brilliant_mqtt_mode` has the same shape with `mode_id` in place of
`scene_id`. These are domain-specific HA events, distinct from the older
general `brilliant_mqtt_event` manager event.

### Services

```yaml
action: brilliant_mqtt.run_scene
data:
  panel: office       # optional; omit to use configured scene_panel
  scene_id: all_off   # must exist in this panel's accepted catalog
```

```yaml
action: brilliant_mqtt.set_mode
data:
  panel: office       # optional
  mode_id: away       # must exist in this panel's accepted catalog
```

Both service schemas reject extra fields. Both publish a non-retained command
and wait up to 16 seconds for a matching result. They raise a Home Assistant
error for reconfiguration, no attached/default panel, offline transport,
unknown/unavailable ID, a full pending queue, publish failure, panel rejection,
or confirmation timeout.

### Entities

Each loaded panel has:

- a scene select populated from that panel's accepted catalog; and
- a run-selected-scene button.

Changing the select updates only HA-local selection. It publishes no command.
The button is available only when the scene transport/catalog are available and
a selection exists. Pressing it calls `brilliant_mqtt.run_scene` with blocking
confirmation; it does not optimistically declare success.

## Safety and reliability

- Commands are non-retained, expire after 15 seconds, and are rejected when
  issued more than five seconds in the future. Scene/mode input timestamps also
  reject values more than five seconds ahead of HA's clock.
- Topic panel/stable IDs must match payload IDs. UUIDs and panel slugs are
  validated before routing.
- The current catalog is the scene/mode allowlist. Unknown IDs produce no bus
  write and a rejected result when they reach the agent.
- Command IDs are idempotency keys. Entity results are cached in HA for up to
  ten minutes (bounded at 1,024). Scene/mode fingerprints, pending intents,
  terminal results, delivery outboxes, and execution watermarks are durable in
  `/data/brilliant-mqtt/scene-watermarks.json`.
- Startup seeds old execution history without emitting it. Reconnect processing
  compares durable timestamps/hashes, and HA keeps a bounded 1,024-key event
  deduplication cache. This suppresses ordinary retained/reconnect replay. It is
  not a claim of mathematical exactly-once delivery across every possible
  broker/persistence crash boundary.
- HA limits catalogs to 256 items, configured actions to 1,024, pending service
  commands to 128, and event deduplication to 1,024. Panel state limits scene
  watermarks to 4,096 and mode watermarks/events/results/pending records to
  1,024 each, with a 4 MiB file cap.
- State is written atomically with private permissions on a single process-wide
  serializer. Invalid or oversized state makes transport unavailable with
  `state_untrusted`; capacity exhaustion uses `state_capacity`. No command is
  executed until trustworthy state has been persisted.
- A bus write failure yields `write_failed`; a panel-side 15-second deadline
  yields `timeout`; unavailable execution yields `execution_unavailable`;
  malformed catalog/execution data makes status unavailable with
  `malformed_data`.
- HA waits 16 seconds so the panel's 15-second terminal result can arrive. A
  successful service response confirms a matching observed execution, not merely
  MQTT publication or a successful bus set call.

## Diagnostics and troubleshooting

Download diagnostics from the panel's HA config entry. Sensitive entry fields
are redacted; room overrides and configured action bodies are omitted entirely.
The `ha_control` section contains:

- enabled state, label, domains, and maximum entity limit;
- room-override and scene-action counts, plus selected label entity count;
- manifest revision and manifest entity count;
- selected scene panel, accepted scene catalog revision, last scene-event
  timestamp, and raw scene transport status (`online`, `offline`, or unknown);
- `native_tiles: {status: blocked, validated: false}`.

The current diagnostic intentionally does not disclose room mappings, action
targets/data, catalog contents, MQTT credentials, panel root material, or old HA
credentials. Mode catalog/event details are available on the versioned retained
MQTT topics but are not yet duplicated in this diagnostic payload.

Use these checks without printing credentials:

```bash
# Repository/source checks
git status --short
scripts/build_payload.sh
git diff --exit-code -- custom_components/brilliant_mqtt/agent_payload

# On the selected panel; authenticate through SSHPASS and do not print env values
sshpass -e ssh root@<panel-ip> \
  'systemctl is-active brilliant-mqtt; systemctl is-active brilliant-ha-mirror || true'

sshpass -e ssh root@<panel-ip> \
  "sed -n 's/=.*//p' /etc/brilliant-mqtt.env | sort"
```

Use HA's MQTT integration “Listen to a topic” tool for the exact retained
catalog/status topic. Do not paste broker credentials into a command or capture
all `brilliant/#` traffic.

| Symptom | Distinguish it by | Operator response |
|---|---|---|
| Scene select empty/unavailable | Empty or absent retained catalog; catalog revision; raw status | Confirm the panel has native scenes and `SCENE_BRIDGE_ENABLED` is present as a key. Reconfigure/redeploy normally; do not create a test peripheral. |
| Mode unavailable | Empty mode catalog can be legitimate | Record “no configured modes”; do not invent a mode. Keep `set_mode` at off-panel coverage until a real mode exists. |
| Offline transport | Retained status `available:false` and `reason` | `execution_unavailable` means no execution peripheral; `state_untrusted`/`state_capacity` concerns durable state; `malformed_data` concerns native records. Stop the pilot on bus instability. |
| Unknown ID | HA service says the scene/mode is unavailable, or agent result is `unknown_scene`/`unknown_mode` | Refresh catalog and use its exact current ID. Never send a display name or arbitrary blob. |
| Confirmation timeout | HA waits 16 seconds; panel result may say `timeout` after 15 seconds | Check for a matching execution record and panel status. Do not retry repeatedly; verify physical state and logs first. |
| Malformed MQTT | HA logs one ignored invalid scene-control message; no event/action | Compare exact fields, schema/mapping version, retain flag, timestamp, topic/payload panel, and canonical deduplication key. |
| No Backyard tile | Expected safe-baseline behavior | Use HA scene surfaces. Native tiles remain blocked; room metadata alone does not enable them. |

The complete hardware procedure, timing record, restart matrix, and rollback are
in the [Office scene-bridge pilot](runbooks/scene-bridge-pilot.md).

## Deployment, migration, rollback, and cleanup

Deploy through the integration's normal reconfigure/redeploy workflow using the
committed, parity-checked payload. Do not copy `src/` directly. The unsafe
`brilliant-ha-mirror` component is deprecated, hidden from install selection,
and its installer fails closed. Existing entries migrate to config version 3;
the manager attempts a verified uninstall and raises a redacted Repair if it
cannot prove absence. See [HA mirror retirement and cleanup](../ha-mirror.md).

Disabling HA control and applying/redeploying the panel configuration removes
the HA scene subscriptions and prevents construction of the panel SceneBridge.
It does not delete a Brilliant device or peripheral because the safe bridge did
not create one. Leave `brilliant-ha-mirror` inactive during every rollback.

Persistent peripherals from old experiments are a separate concern. The
`cleanup_legacy_mirror` CLI is dry-run-first and deletes only records that match
strict, case-sensitive legacy ID and display-name allowlists. Never make cleanup
apply an automatic part of scene-bridge deployment or rollback.

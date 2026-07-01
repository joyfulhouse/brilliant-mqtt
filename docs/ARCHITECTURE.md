# Architecture

How brilliant-mqtt is structured and why.

## Overview

Each Brilliant Control panel runs a complete internal control plane: an Apache
Thrift message bus over a local unix socket that every on-panel peripheral —
including Brilliant's own HomeKit bridge — uses to read device state, subscribe
to changes, and drive loads. There is no off-box API, so the bridge is an
**on-panel agent**: it speaks the bus locally and mirrors devices to the home's
central MQTT broker as Home Assistant MQTT-Discovery entities, with
bidirectional control.

```
   ┌─────────────────── one Brilliant panel (× N) ─────────────────────┐
   │  uwsgi emperor ── message_bus ──(unix socket                      │
   │                      ▲  │        /var/run/brilliant/server_socket)│
   │       subscribe/get_all  │ handle_notification (state push)       │
   │  request_set_variables   ▼                                        │
   │               ┌───────────────────────────┐                       │
   │               │   brilliant-mqtt bridge   │  in /var, run by the  │
   │               │   RPCObserver + mapper    │  panel's python3.10   │
   │               └─────────────┬─────────────┘                       │
   └─────────────────────────────│ MQTT (tcp 1883, user `brilliant`)───┘
                                 ▼
                       central MQTT broker
                                 ▲
                                 │ MQTT Discovery (homeassistant/.../config)
                                 │ + state/command topics (brilliant/...)
                                 ▼
                        Home Assistant (native MQTT entities)
```

## Components

| Module | Responsibility | Tested |
|---|---|---|
| `bus.py` | Real `RPCObserver` adapter — connect, `get_all`, subscribe, route notifications, issue commands. **The only module that may import `lib.message_bus_api`.** | on-panel |
| `mqttio.py` | Real aiomqtt adapter — publish, subscribe, LWT, reconnect. | on-panel |
| `protocols.py` | `BusClient` / `MqttClient` Protocols — the typing seam the orchestrator depends on. | — |
| `model.py` | Normalized `BrilliantDevice` / `Variable` dataclasses + change events. | unit |
| `mapping.py` | Device → HA entity descriptor (component, capabilities). | unit |
| `discovery.py` | HA MQTT-Discovery payloads + topic builders. | unit |
| `commands.py` | Inbound MQTT command → bus variable-set translation. | unit |
| `desired_state.py` | Operator desired-state store for the motion vars (`RECONCILED_VARS`) — in-memory + durable JSON under `/var`; feeds the bridge's drift re-assertion. | unit |
| `bridge.py` | Orchestrator: reconcile, change → state publish, command → bus, desired-state enforcement. | unit (fakes) |
| `mesh_leader.py` | Fleet-wide mesh leader election over MQTT (retained priority claim + heartbeat); gates the mesh bridge's publishes and writes. | unit |
| `config.py` | Env-driven settings. | unit |
| `__main__.py` | Entry point: wire real adapters, run the loop. | thin |

## Data Flow

- **Read path:** on (re)connect the bridge reconciles — retained discovery +
  state for every in-scope device; afterwards, bus push notifications
  (`handle_notification`) update the state topics within ~1 s. A fast scoped
  poll (default every 2 s) publishes any payload diffs the pushes missed, and
  a periodic full re-sync (default every 5 min) repairs discovery
  (level-triggered).
- **Command path:** HA publishes JSON to `brilliant/<panel>/<device>/set`; the
  bridge translates it (e.g. HA brightness 0–255 → device range) and calls
  `request_set_variables_in_peripheral` on the bus, then optimistically echoes
  the commanded state. The bus notification/poll confirms it.
- **Availability:** `brilliant/<panel>/availability` with an MQTT LWT —
  `offline` the moment the agent dies; systemd restarts it; reconnect
  re-reconciles.
- **Bridge meta:** retained `brilliant/<panel>/bridge` JSON
  (`{"agent_version", "panel_firmware"}`), republished on every reconcile. The
  machine contract for the companion HA integration (OTA detection + agent-update
  entity); the firmware tag is also exposed as a per-panel diagnostic sensor.
  Never published for the reserved `mesh` pseudo-panel.
- **Self-healing:** the panel lib's notification stream can die *silently*
  while the process lives, freezing both pushes and the observer's
  `get_all()` mirror (live pilot finding). Three layers compensate: the
  processor's reconnect callback triggers re-subscribe + full reconcile; the
  hot poll bounds staleness at its cadence; and a stale-stream watchdog
  rebuilds the whole session when no push arrives for `BUS_STALE_SECONDS`.
- **BLE mesh (elected publisher):** the bus's virtual `ble_mesh` device
  (Brilliant plug-in switches/mesh dimmers, whole-home) is published under the
  reserved `mesh` pseudo-panel by exactly one panel, elected via a retained
  MQTT claim + heartbeat (`MESH_PRIORITY`, lower wins; stale after 3×
  heartbeat → failover; higher priority preempts on return). The namespace
  never references the publishing panel, so HA sees one stable set of
  entities regardless of which panel serves them. In-process this is a second
  `Bridge` instance (panel slug `mesh`) gated by an include predicate that
  checks leadership, fed by the same bus adapter subscribed to both devices.

## Key Design Decisions

- **On-panel agent, not an off-box client.** The bus is unix-socket-only; no
  TCP control API exists. The agent runs where the socket is.
- **Use the on-box `RPCObserver`, never hand-roll Thrift.** The panel ships an
  auto-reconnecting async client — the same one Brilliant's HomeKit peripheral
  uses. The bridge stays within the API surface the vendor's own code exercises.
- **Python 3.10, locked.** The panel interpreter is 3.10.9 and is the runtime;
  `requires-python = ">=3.10,<3.11"`. This deliberately overrides the org-wide
  Python 3.13+ default.
- **Protocol seams for off-panel TDD.** All mapping/discovery/command/bridge
  logic is pure and unit-tested anywhere against `FakeBus`/`FakeMqtt`; only the
  two thin adapters touch the real bus and broker.
- **Retained discovery + retained state.** HA restarts recover instantly — the
  core reliability win over the HomeKit Controller path (`setup_retry`,
  entities lost across restarts).
- **Per-panel device scoping.** Each panel publishes only the device whose id
  matches `get_owning_device_id()` — its own loads and sensors — so a house
  full of panels doesn't publish the whole home graph N times over (settled by
  the Milestone-1 PoC).
- **Never trust the observer's mirror alone.** `get_all()`/`get_device()` are
  served from a notification-fed in-process cache that freezes when the push
  stream silently dies (live pilot finding). The reconnect hook, the fast
  diff-publishing poll, and the stale-stream watchdog exist specifically to
  bound how stale that mirror can get.
- **Survive firmware OTA.** App + unit live in `/var` (persistent); the
  interpreter path is stable; firmware is gated via the operator's OSTree
  mirror and the bus API is re-validated after each bump.
- **Declarative variable-entity table.** Beyond the loads, per-variable
  entities (power, panel controls, presence/privacy, diagnostics) are driven
  by one `AUX_SPECS` table in `mapping.py` — the single source of truth for
  both discovery payloads and the shared state JSON, so the template ↔
  payload contract cannot drift.
- **Resource-capped.** systemd `MemoryMax`/`CPUQuota`/`Nice` ensure a bridge
  bug cannot degrade the panel's touchscreen UI.
- **HomeKit stays paired** as a fallback control path during and after
  migration.

---

The full design spec, research evidence, and the executable implementation
plan are operator-internal working artifacts and are not part of the public
tree; the live-verified facts they produced are captured in
[`reference/poc-findings.md`](reference/poc-findings.md).

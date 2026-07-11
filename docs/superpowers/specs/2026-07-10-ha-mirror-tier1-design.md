# HA Mirror (Tier 1) — Design

- **Date:** 2026-07-10
- **Status:** Approved (design); pending implementation plan
- **Component:** `brilliant_ha_mirror` (new sibling package)
- **Related:** `docs/superpowers/specs/2026-07-09-ha-virtual-brilliant-control-design.md`
  (the reverse-bridge design this realizes), and the live control-routing proof
  recorded in the operator memory `brilliant-mqtt-synthetic-control-gate`.

## Goal

Reflect selected Home Assistant entities as **first-class, controllable native
peripherals** in the Brilliant in-wall panel UI: a panel control (slider/toggle)
drives an HA service call, and HA state changes drive the panel peripheral's
variables. Runtime control is local via the panel message bus.

This is operator software running on the operator's own panels using each
panel's own Brilliant identity (root, on-box Python 3.10 framework, device mTLS
cert). It is **not** a redistributable add-on.

## Proven foundation (from the 2026-07-10 spike — treat as given)

- Control routing works: host a peripheral on the panel's **own bus device**
  (`PeripheralConfig(peripheral_id, MirrorClass)` with `virtual_device_id=None`),
  launched via `python -m lib.startables.run_startable
  --message_bus_server_socket_path=/var/run/brilliant/server_socket <module>`.
  A variable declared `externally_settable=True` with a `push_func` receives
  panel commands. Verified live for a LIGHT (`CONTROL_ROUTED push on=1`).
- The firmware ships this exact pattern as 12+ third-party integration vassals
  (hue, lifx, smartthings, august, ring, schlage, ecobee, nest, honeywell,
  sonos, wemo, tplink, hunter_douglas), hosting external devices as native
  panel-controllable peripherals. This is the firmware's own architecture.
- Verified build facts:
  - `name` / `peripheral_type` are **properties**; `_my_variables` is a
    **method** the base calls (a `@property` there raises
    `TypeError: 'dict' object is not callable`).
  - `VariableSpec` cannot take `bool` — represent thrift BOOL as `int` (0/1).
  - The bus peripheral registry is keyed by the peripheral's **name**, not the
    config `peripheral_id`.
  - Set values are **strings** (`{"on": "1"}`), matching agent `bus.py`
    `Variable(name, str(value), settable)`. A raw int raises
    `AttributeError: 'int' has no attribute 'thrift_spec'`.
  - Own-device peripherals **persist in object_store and survive reboot**; they
    do NOT self-clean on host exit. Clean deletion that persists across reboot:
    re-host, then invoke the delete impl unbound on the base host instance —
    `ConditionalPeripheralHost.__dict__["delete_peripheral"](host, "<name>")`
    (arg is the peripheral's **name**).

## Non-goals (this tier)

- Cameras and doorbells (CAMERA 59 / DOORBELL 2). They use
  `streaming_configuration` + `remote_sessions` from the `remote_media`
  subsystem over WebRTC/TURN — a separate, heavier spike.
- Thermostat (4), security system (89), scene, valve, media_player — deferred to
  a later tier; the mapping table is structured to extend to them.
- Fan speed control (no dedicated fan interface; on/off only would fall back to
  GENERIC_ON_OFF, deferred).

## Decisions

1. **HA connection: WebSocket API.** The mirror connects to HA's WS API with a
   long-lived token: subscribe to `state_changed`, `get_states`, `call_service`,
   and enumerate entities/areas/labels. Full access to all entities + services,
   no HA-side MQTT config. The token is a secret loaded from the environment
   (never committed).
2. **Host coordination: dynamic leader election.** Reuse the proven
   `brilliant_mqtt.mesh_leader` priority-election pattern so exactly one panel
   is the active mirror host, with automatic failover.
3. **Entity selection: HA labels.** Operators apply a label (e.g. `brilliant`)
   to entities they want mirrored — HA-native, precise, scalable. Each entity's
   HA area drives its Brilliant room (auto-match by name; config override for
   mismatches).
4. **Process structure: separate package + systemd unit.** New
   `brilliant_ha_mirror` with its own unit and resource caps (matches
   `brilliant_voice` / `brilliant_bus_watchdog` / `brilliant_wifi_watchdog`
   precedent). Isolates the new hosting + WebSocket logic from the proven
   panel→MQTT bridge. It runs on every panel but is active only on the elected
   leader; it opens its own bus peer session (unique client name) and WS
   connection.

## Architecture & components (`src/brilliant_ha_mirror/`)

| Module | Responsibility |
|---|---|
| `__main__.py` | Wire real adapters; supervised loop (mirrors `brilliant_mqtt` supervisor). |
| `config.py` | `Settings`: HA WS URL + token (env), mirror label, room overrides, leader priority, resource knobs. |
| `ha_client.py` | `HaClient` Protocol + real WS adapter: auth, `subscribe_events(state_changed)`, `get_states`, `call_service`, list entities/areas/labels. **Only module importing the WS library.** |
| `hosting.py` | `PeripheralHostClient` Protocol + real adapter wrapping the framework `PeripheralHost`: `register`, `update_variables`, `delete_peripheral` (borrowed `ConditionalPeripheralHost` path). **Only module importing the framework host / `run_startable`.** |
| `mapping.py` | Pure Tier-1 translation table: HA domain → Brilliant type + bidirectional variable translation. No I/O; fully unit-tested. |
| `mirror.py` | Orchestrator: reconcile labeled HA entities ↔ hosted peripherals; state→vars; panel-command→service-call; add/remove. |
| `leader.py` | Reuse/adapt `mesh_leader` election so only one panel is active. |

All real adapters are wired in `__main__`; everything else is behind Protocols
and unit-tested off-panel with fakes, mirroring the repo non-negotiable
(`message_bus_api` isolated to `bus.py`; here the WS lib is isolated to
`ha_client.py` and the framework host to `hosting.py`).

## Data flow

- **Startup (leader only):** elect → connect HA WS + bus host → fetch labeled
  entities → for each, create a typed mirror peripheral, room-assigned
  (HA area → Brilliant room), register on the leader's own bus device, seed
  variables from current HA state.
- **HA → panel:** WS `state_changed` → `mapping` → push updated variables to the
  peripheral → panel UI updates.
- **Panel → HA:** panel control → peripheral `push_func` → `mapping` → HA
  `call_service` → HA's resulting `state_changed` echoes back and confirms the
  variable (authoritative). Optional optimistic local echo to avoid UI lag,
  reconciled by the authoritative event.
- **Reconcile:** entity labeled / unlabeled / removed → register new /
  `delete_peripheral` gone. No phantoms (cleanup lesson baked in).

## Tier-1 mapping table

| HA domain | Brilliant type (#) | State → variable | Command variable → HA service |
|---|---|---|---|
| light | LIGHT (27) | on/brightness → `on`/`intensity` (+ `dimmable`) | `on`/`intensity` → `light.turn_on` / `light.turn_off` |
| switch | GENERIC_ON_OFF (45) | state → `on` | `on` → `switch.turn_on` / `switch.turn_off` |
| lock | LOCK (1) | state → `locked` | `locked` → `lock.lock` / `lock.unlock` |
| cover (position) | SHADE (53) | position → `position` (+ `tilt_position`) | `position` → `cover.set_cover_position` |
| cover (garage) | GARAGE_DOOR (74) | state → `event` | `event` → `cover.open_cover` / `cover.close_cover` |

Exact required interface variables per type (from
`thrift_types/peripheral_interfaces/*/ttypes.py`) are captured in V3 below;
the LIGHT set (`on`, `dimmable`, `intensity`, + framework-added
`mode_transition_settings`, `room_assignment`) is verified live.

## Config delivery

The companion HA integration (`custom_components/brilliant_mqtt`, which already
SSH-installs and configures the agent) writes the mirror config (HA URL,
long-lived token, label, room overrides, leader priority), installs and enables
the systemd unit, and exposes a switch / Repair to turn the reverse mirror on.
Secrets come from the environment / operator secret store at deploy time.

## Failure handling & lifecycle

- WS reconnect with backoff; re-seed variable state on reconnect.
- Bus session supervision reusing the stale-stream / reconnect-storm concepts
  from `brilliant_mqtt`.
- systemd resource caps (`MemoryMax` / `CPUQuota` / `Nice`) so a mirror bug
  cannot degrade the panel UI.
- **Leader handoff:** on leader loss, the departing host `delete_peripheral`s
  its hosted mirrors so the new leader re-registers cleanly — no duplicates or
  phantoms.
- **Shutdown:** `delete_peripheral` all hosted mirrors (avoid persistent
  phantoms; own-device peripherals survive reboot otherwise).

## Open verification items (front-load as spikes in the plan)

Load-bearing unknowns — the plan proves these before building on them:

- **V1 (critical, first):** Does a peripheral hosted on the leader's own device
  render **home-wide on all panels** (room-assigned), or only on the hosting
  panel? If only local, the leader-election model must be reconsidered
  (candidate fallback: host on a device all panels subscribe to). Verify on two
  panels.
- **V2:** `room_assignment` variable format and how to resolve a Brilliant room
  id; mechanism for HA-area → room mapping.
- **V3:** Replicate the live LIGHT control-routing proof for lock, cover, garage,
  switch — each interface's exact required vars, `externally_settable` set, and
  string encodings.
- **V4:** Clean leader handoff (delete on old, register on new) with no phantom.

## Testing

- **Off-panel unit tests** (must run on any machine): `mapping` per-type round
  trips (state→vars and command→service for each Tier-1 domain), `mirror`
  reconciliation (add/remove/relabel) with fakes for `HaClient` and
  `PeripheralHostClient`, leader election.
- **On-panel integration checks:** V1–V4, on the designated pilot first, with
  the delete-cleanup path exercised each time (no phantoms left behind).
- Both gates stay green: the agent gate (`ruff`/`ruff format`/`mypy --strict`/
  `pytest`) and, if the integration changes, the integration gate.

## Build sequence (informs the plan)

1. V1 spike (home-wide visibility) — gate the whole approach.
2. `mapping.py` + tests (pure, no panel).
3. `hosting.py` Protocol + fake + real adapter; V3 per-type live proofs.
4. `ha_client.py` Protocol + fake + real WS adapter.
5. `mirror.py` orchestrator + reconciliation tests.
6. `leader.py` (adapt `mesh_leader`) + V4 handoff check.
7. `config.py`, `__main__.py`, systemd unit, integration config delivery.

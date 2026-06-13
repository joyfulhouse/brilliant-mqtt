# Configuration

Full configuration reference for brilliant-mqtt.

> **Status:** implemented and verified live on a pilot panel — this contract
> is enforced by the test suite (`config.py`).

## Runtime configuration (environment)

The agent is configured entirely from the environment, loaded by systemd from
`/etc/brilliant-mqtt.env` on the panel (written by hand or rendered by your
configuration management; keep credentials out of git).

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `BRILLIANT_PANEL` | yes | — | Stable panel slug (e.g. `office`); namespaces all topics and entity ids |
| `MQTT_HOST` | yes | — | Central broker host/IP |
| `MQTT_PORT` | no | `1883` | Broker port |
| `MQTT_USERNAME` | yes | — | Broker user (the dedicated `brilliant` user) |
| `MQTT_PASSWORD` | yes | — | Broker password (from your secret store — never git) |
| `RESYNC_SECONDS` | no | `300` | Period of the full discovery + state re-sync |
| `HOT_POLL_SECONDS` | no | `2.0` | Cadence of the scoped state poll that bounds staleness even when bus pushes silently die; only changed payloads are published. `0` disables |
| `BUS_STALE_SECONDS` | no | `900` | Rebuild the bus session when no push arrived for this long (half-dead stream watchdog). `0` disables |
| `RECONNECT_STORM_THRESHOLD` | no | `20` | Rebuild the bus session when it reconnects at least this many times within the window — breaks a reconnect storm the stale watchdog can't see (each reconnect resets its push clock). `0` disables |
| `RECONNECT_STORM_WINDOW_SECONDS` | no | `60` | Sliding window for `RECONNECT_STORM_THRESHOLD` |
| `MESH_PRIORITY` | no | `0` | Participate in BLE-mesh leader election with this priority (lower number wins; ties broken by panel name). `0` = never publish the mesh |
| `MESH_HEARTBEAT_SECONDS` | no | `10` | Mesh leadership heartbeat; a claim is stale (failover triggers) after 3× this |
| `LOG_LEVEL` | no | `INFO` | Python log level (`DEBUG` for troubleshooting) |

> `BRILLIANT_PANEL=mesh` is **reserved** — the BLE-mesh pseudo-panel namespace.

Example `/etc/brilliant-mqtt.env`:

```
BRILLIANT_PANEL=office
MQTT_HOST=192.0.2.10
MQTT_PORT=1883
MQTT_USERNAME=brilliant
MQTT_PASSWORD=<your broker password>
LOG_LEVEL=INFO
```

## MQTT topic scheme

| Purpose | Topic | Notes |
|---|---|---|
| Discovery | `homeassistant/<component>/brilliant_<panel>_<peripheral>[_<var>]/config` | retained |
| State | `brilliant/<panel>/<peripheral>/state` | retained JSON, shared by all of a peripheral's entities |
| Load command | `brilliant/<panel>/<peripheral>/set` | JSON (HA light `schema: json`; switch payloads) |
| Aux command | `brilliant/<panel>/<peripheral>/set_<variable>` | plain payloads: `ON`/`OFF` (switches), numeric (numbers), `PRESS` (buttons) |
| Availability | `brilliant/<panel>/availability` | `online`/`offline`, LWT, retained |
| Mesh namespace | `brilliant/mesh/...` (same shapes as above) | published by the **elected leader panel only** — publisher-agnostic, so failover causes zero HA churn |
| Mesh leadership | `brilliant/mesh/leader` | retained claim `{"panel", "priority"}` + heartbeat |

## BLE mesh loads (elected publisher)

Brilliant's plug-in switches and mesh dimmers live on the bus's virtual
`ble_mesh` device, visible identically from every panel. Exactly **one** panel
publishes them — under the reserved `mesh` pseudo-panel and one
"Brilliant BLE Mesh" HA device — elected by `MESH_PRIORITY`:

- The leader heartbeats a retained claim every `MESH_HEARTBEAT_SECONDS`;
  standbys are completely silent on the mesh namespace.
- If the leader goes quiet for 3× the heartbeat, the best-priority standby
  takes over (republishes discovery/state, takes the command subscriptions).
  When a higher-priority panel returns, it preempts and control moves back.
- Known gaps: commands sent during a takeover window (≤ ~30 s) are lost, and
  the claim has no LWT (a whole-fleet outage leaves it stale until a bridge
  returns).
- Mesh loads whose `power` reports the `-1` sentinel (uncalibrated) get no
  power sensor; the entity appears automatically once real wattage shows up.
- Each mesh load also exposes a **`Motion` binary_sensor** (device_class:
  motion, enabled by default). Additional disabled-by-default entities:
  `Motion Score` (diagnostic sensor), `Motion Score Reporting` (config
  switch), and `Motion High Threshold` / `Motion Low Threshold` (config
  numbers, 0–100 assumed range). Panel loads are unaffected — these entities
  only appear when the backing bus variables are present.

## Entities per panel

Beyond the loads (light/switch with brightness), each panel exposes:

- **Power & electrical**: per-gang `Power` (W) — including always-on circuits —
  plus `Temperature` and a `Fault` problem sensor (diagnostic).
- **Panel controls** (config category): `Microphone Mute`, `Screen`,
  `Screen Brightness` (0–10), `Volume` / `Alert Volume` (0–100),
  `Child Lock`, `Night Mode`, `Faceplate LED`, `Illuminance Sensor` enable,
  and an `Identify` button.
- **Presence & privacy**: `In Use` (someone at the panel, occupancy),
  `Motion` + `Illuminance` (faceplate), `Camera` active and `Privacy Mode`
  (read-only, diagnostic).
- **Diagnostics**: `CPU Temperature`, `Wi-Fi` / `Internet` connectivity,
  `NTP Sync`, `PIR Score`, `Internal Temperature` (some disabled by default);
  panel firmware version on the HA device page (`sw_version`).

Each panel's entities are grouped under one HA device
(`brilliant_panel_<panel>`), with availability tied to the panel's LWT topic.

## Broker user and ACL

The bridge uses a dedicated `brilliant` broker user with a scoped ACL:

- `brilliant/#` — read/write (state, commands, availability)
- `homeassistant/#` — write (discovery configs)

Provision the user on your broker — full instructions (including the
no-standalone-broker path via Home Assistant's Mosquitto add-on) are in
[INSTALL.md](../INSTALL.md#set-up-the-mqtt-broker). **Mosquitto ACL denials
are silent** — if state or commands vanish without errors, check the ACL
first (see [TROUBLESHOOTING.md](TROUBLESHOOTING.md)).

## Home Assistant

No HA-side configuration is required: entities appear via MQTT Discovery as
native MQTT entities (light/switch/sensor per device capability), and recover
automatically across HA restarts from retained discovery + state.

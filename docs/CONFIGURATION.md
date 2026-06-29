# Configuration

Full configuration reference for brilliant-mqtt.

> **Status:** implemented and verified live on a pilot panel — this contract
> is enforced by the test suite (`config.py`).

## Runtime configuration (environment)

The agent is configured entirely from the environment, loaded by systemd from
`/etc/brilliant-mqtt.env` on the panel (written by hand or rendered by your
configuration management; keep credentials out of git).

### Core variables

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `BRILLIANT_PANEL` | yes | — | Stable panel slug (e.g. `office`); namespaces all MQTT topics and entity IDs. `mesh` is reserved — do not use. |
| `MQTT_HOST` | yes | — | Central broker hostname or IP (e.g. `192.0.2.10`) |
| `MQTT_PORT` | no | `1883` | Broker TCP port |
| `MQTT_USERNAME` | yes | — | Broker user (the dedicated `brilliant` user) |
| `MQTT_PASSWORD` | yes | — | Broker password — store in your secret store, never in git |
| `LOG_LEVEL` | no | `INFO` | Python log level: `DEBUG` turns on verbose tracing; `WARNING` quiets normal traffic |

### Polling and watchdog timers

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `RESYNC_SECONDS` | no | `300` (5 min) | Period of the full discovery + state re-sync |
| `HOT_POLL_SECONDS` | no | `2.0` seconds | Cadence of the scoped state poll that bounds staleness when bus push notifications die silently. Only changed payloads are published. `0` disables the hot poll. |
| `BUS_STALE_SECONDS` | no | `900` (15 min) | Rebuild the bus session when no push has arrived for this many seconds (half-dead stream watchdog). `0` disables. |

### Reconnect-storm breaker

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `RECONNECT_STORM_THRESHOLD` | no | `20` reconnects | Rebuild the bus session when the bus reconnects at least this many times within the window. Breaks a storm that the stale watchdog cannot detect (each reconnect resets its push clock). `0` disables. |
| `RECONNECT_STORM_WINDOW_SECONDS` | no | `60` seconds | Sliding window for `RECONNECT_STORM_THRESHOLD` |

### BLE mesh

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `MESH_PRIORITY` | no | `0` (never publish) | Participate in BLE-mesh leader election with this priority. Lower number wins; ties broken by panel name. `0` means this panel never publishes the mesh namespace. Set to `1` or higher on every panel that should be a standby. |
| `MESH_HEARTBEAT_SECONDS` | no | `10` seconds | How often the elected leader heartbeats its retained claim. A claim older than 3× this triggers a standby takeover. |

Example `/etc/brilliant-mqtt.env`:

```
BRILLIANT_PANEL=office
MQTT_HOST=192.0.2.10
MQTT_PORT=1883
MQTT_USERNAME=brilliant
MQTT_PASSWORD=<your broker password>
LOG_LEVEL=INFO
```

---

## Voice configuration

> These vars are for **manual or advanced setups only.** When you enable voice
> via the HACS integration the HA integration writes `/etc/brilliant-voice.env`
> for you — you do not need to touch these directly.

The voice satellite is opt-in, per panel. Enable it in the HA integration during
onboarding (or later via the **Voice satellite** switch on the panel's device
page). The satellite advertises via zeroconf so HA auto-discovers it as an
ESPHome `assist_satellite` entity.

| Variable | Default | Meaning |
|---|---|---|
| `BRILLIANT_PANEL` | _(required)_ | Same panel slug as the bridge — shared with the main agent |
| `VOICE_NAME` | `Brilliant <panel>` | Display name for the satellite in HA (e.g. `Brilliant Office`) |
| `VOICE_API_PORT` | `6053` | ESPHome native API port that HA connects to |
| `VOICE_WAKE_WORD` | `okay_nabu` | Active wake word: `okay_nabu`, `hey_jarvis`, or `hey_mycroft` |
| `VOICE_HA_HOST` | _(empty)_ | `hostname=ip` mapping (e.g. `homeassistant.example=10.0.0.5`) — only needed when the panel cannot resolve your HA URL's hostname (e.g. IoT VLAN). Leave blank to rely on the panel's own DNS. |
| `VOICE_ENABLE_AEC` | `off` | `on` to enable acoustic echo cancellation (for barge-in). Ships off — not needed for normal use because the mic is muted during TTS. |
| `VOICE_MIC_DEVICE` | `default` | ALSA capture device. `default` uses the panel's own tuned DSP chain (recommended; far-field wake detection verified at 0.996 confidence). |
| `VOICE_SND_DEVICE` | `plug:dmix_48000` | ALSA playback device. `plug:dmix_48000` mixes with the panel's other audio. |
| `VOICE_AEC_MIC_DEVICE` | `plug:dsnoop_48000` | ALSA capture device for the AEC daemon (raw 2-mic tap, pre-DSP). Only used when `VOICE_ENABLE_AEC=on`. |
| `VOICE_AEC_DELAY_MS` | `0` ms | Loudspeaker-to-mic delay hint for the AEC algorithm |
| `VOICE_AEC_TYPE` | `1` | AEC algorithm: `0`=DSP_WIDGETS, `1`=SPEEX, `2`=WEBRTC |
| `LOG_LEVEL` | `INFO` | Shared with the bridge. Set `DEBUG` for voice tracing. |

See [docs/voice.md](voice.md) for the full end-to-end voice setup guide.

---

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

---

## BLE mesh loads (elected publisher)

Brilliant's plug-in switches and mesh dimmers live on the bus's virtual
`ble_mesh` device, visible identically from every panel. Exactly **one** panel
publishes them — under the reserved `mesh` pseudo-panel / "Brilliant BLE Mesh"
HA device — elected by `MESH_PRIORITY`.

### How leader election works

- The leader heartbeats a retained claim every `MESH_HEARTBEAT_SECONDS`;
  standbys are completely silent on the mesh namespace.
- If the leader goes quiet for 3× the heartbeat, the best-priority standby
  takes over: it republishes discovery/state and takes the command subscriptions.
- When a higher-priority panel returns, it preempts the current leader and
  control moves back automatically.

### Known gaps

- Commands sent during a takeover window (≤ ~30 s) are lost.
- The leadership claim has no LWT — a whole-fleet outage leaves the claim stale
  until a panel returns.

### Power sensors and motion

- Mesh loads whose `power` reports `-1` (uncalibrated) get no power sensor; the
  entity appears automatically once real wattage shows up.
- Each mesh load also exposes a **`Motion` binary_sensor** (device_class:
  motion, enabled by default). Additional disabled-by-default entities:
  `Motion Score` (diagnostic sensor), `Motion Score Reporting` (config
  switch), and `Motion High Threshold` / `Motion Low Threshold` (config
  numbers, 0–100 assumed range). Panel loads are unaffected — these entities
  only appear when the backing bus variables are present.

> For mesh motion to reflect real presence you must enable the device's
> **Motion Score Reporting** switch first. See
> [Troubleshooting → Mesh-load motion](TROUBLESHOOTING.md#mesh-load-motion-always-reads-off-or-read-a-stuck-on)
> for the full tuning steps.

---

## Entities per panel

Each panel exposes roughly 20–30 entities across these categories:

- **Loads** (per gang): `light` (dimmer) or `switch`, with brightness where
  the hardware supports it.
- **Power & electrical**: per-gang `Power` (W) — including always-on circuits —
  plus `Temperature` and a `Fault` problem sensor (diagnostic).
- **Panel controls** (config category): `Microphone Mute`, `Screen`,
  `Screen Brightness` (0–10), `Volume` / `Alert Volume` (0–100),
  `Child Lock`, `Night Mode`, `Faceplate LED`, `Illuminance Sensor` enable,
  and an `Identify` button.
- **Presence & privacy**: `In Use` (someone at the panel, occupancy),
  `Motion` + `Illuminance` (faceplate), `Camera` active and `Privacy Mode`
  (read-only, diagnostic).
- **Voice** (when enabled): `Voice satellite` switch and `Wake word` select.
- **Diagnostics**: `CPU Temperature`, `Wi-Fi` / `Internet` connectivity,
  `NTP Sync`, `PIR Score`, `Internal Temperature` (some disabled by default);
  panel firmware version on the HA device page (`sw_version`).

All entities are grouped under one HA device (`brilliant_panel_<panel>`), with
availability tied to the panel's LWT topic.

---

## Broker user and ACL

The bridge uses a dedicated `brilliant` broker user scoped to its own topics:

```
# /etc/mosquitto/acl
user brilliant
topic readwrite brilliant/#
topic write homeassistant/#
```

- `brilliant/#` — read/write for state, commands, and availability
- `homeassistant/#` — write for MQTT Discovery configs

**Mosquitto ACL denials are silent.** A publish to a denied topic is dropped
with no error to the client. If entities never appear or commands are silently
swallowed, check the ACL first — it is the most common cause.

For provisioning steps (password file, `mosquitto.conf` references, and the
HA Mosquitto add-on path) see [INSTALL.md](../INSTALL.md#step-2--set-up-the-mqtt-broker).

---

## Home Assistant

No HA-side configuration is required: entities appear via MQTT Discovery as
native MQTT entities (light/switch/sensor per device capability), and recover
automatically across HA restarts from retained discovery + state.

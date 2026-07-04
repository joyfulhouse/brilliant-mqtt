# Troubleshooting

Common problems with brilliant-mqtt and how to resolve them.

> **Status:** validated against a live pilot panel (2026-06-12).

## Enabling Debug Logging

Enable verbose output first — it is the primary diagnostic tool for almost every issue below.

Set `LOG_LEVEL=DEBUG` in `/etc/brilliant-mqtt.env`, then:

```bash
systemctl restart brilliant-mqtt
journalctl -u brilliant-mqtt -f
```

Reset to `LOG_LEVEL=INFO` when done.

---

## Install

### Entities never appear in Home Assistant

1. **Check the ACL first** — Mosquitto drops denied publishes silently, with no
   error to the client. Verify the `brilliant` user has `homeassistant/#` write
   and `brilliant/#` read+write. See
   [Configuration → Broker user and ACL](CONFIGURATION.md#broker-user-and-acl)
   for the required ACL lines.
2. Confirm broker credentials in `/etc/brilliant-mqtt.env` and that the panel
   VLAN can reach the broker on port 1883.
3. Check the agent is running: `systemctl status brilliant-mqtt` on the panel.

---

## Connectivity

### Entities show as unavailable / panel `offline`

- The availability topic is an LWT — `offline` means the agent died or lost the
  broker. systemd restarts it (`Restart=always`); check
  `journalctl -u brilliant-mqtt` for the crash cause.
- If the agent is up but entities stay unavailable, the broker may have dropped
  the retained `online` publish — restart the unit to re-reconcile.

### Commands from HA don't drive the load

1. Check the startup log line `reconcile: N devices -> M entities, K command
   topics subscribed` (`journalctl -u brilliant-mqtt`). If it is missing, the
   initial reconcile did not complete; the periodic resync (default 5 min)
   self-heals.
2. Verify the `brilliant` ACL allows **read** on `brilliant/#` — see
   [Configuration → Broker user and ACL](CONFIGURATION.md#broker-user-and-acl)
   for the required rules.
3. Run with `LOG_LEVEL=DEBUG` and watch the full trace: mqtt receipt →
   translated command → `SetVariableResponse` from the bus.
4. If the light turns on/off right after a command, suspect the panel's own
   motion logic or an HA automation (e.g. Adaptive Lighting) re-asserting
   state — not the bridge.

---

## Data quality

### Sensors lag or freeze (power/motion/occupancy stale for minutes)

**Why:** The panel bus's notification stream can die silently while the process
keeps running — pushes stop and bus reads serve a frozen in-process mirror
(live pilot finding, 2026-06-12).

**Built-in fixes (all on by default):**
- Hot poll (`HOT_POLL_SECONDS`, default 2 s) bounds staleness — only changed
  payloads are published.
- Reconnect hook re-reconciles after bus gaps (look for
  `bus processor reconnected` warnings in the journal).
- Stale watchdog (`BUS_STALE_SECONDS`, default 900 s) rebuilds the session
  when no push arrives.

**If sensors still lag:**
1. Confirm the deployed version has the hot poll:
   `journalctl -u brilliant-mqtt | grep version` — `HOT_POLL_SECONDS=0`
   disables it.
2. Run `LOG_LEVEL=DEBUG` and watch for `state publish` lines — a healthy bridge
   emits one within a poll interval of any bus change.
3. A motion entity that never triggers may simply be a PIR with no activity
   (verify `movement_detected` on the bus). Illuminance reads `0` while
   `enable_lux=0` on the panel (the default).

### Mesh-load motion always reads `off` (or read a stuck `on`)

**Why:** A BLE-mesh load's **Motion** binary_sensor only reflects live presence
while the device's motion-scoring subsystem is enabled. With scoring **off**
(`enable_motion_score=0`, the factory default), `movement_detected` is a frozen
latch — so the bridge publishes `motion=off` rather than the stale value
(live-verified 2026-06-14: with scoring off, mesh sensors latched a permanent
`occupied` with nobody home).

**Why (published sensor):** The firmware `movement_detected` latch never
fires, so the bridge derives the Motion binary_sensor from the `motion_score`
stream instead: it turns **on** the moment `motion_score` >= **Motion High
Threshold**, and turns **off** after `MOTION_DERIVED_HOLD_S` seconds (default
60 s) with no qualifying spike. **Motion Low Threshold** does not affect this
sensor — it only writes to the device NVM (kept for firmware/HomeKit parity).
Both thresholds and `motion_score` are 8-bit, range 0–255.

**Fix:** Enable real mesh presence in two steps:

1. In HA, enable the device's **Motion Score Reporting** switch
   (`enable_motion_score` — a `config`, disabled-by-default entity; enable it
   in the entity settings first to make it visible).
2. Enable the **Motion High Threshold** number entity (also disabled by
   default, 0–255 range) and tune it: sample the idle `motion_score` noise
   floor with scoring on, then set High above that floor so the sensor stays
   off when the space is empty. Raise `MOTION_DERIVED_HOLD_S` (env,
   default 60 s) if the sensor clears too eagerly between spikes.

The panel **faceplate** occupancy sensor is a separate subsystem — these steps
do not affect it.

---

## Advanced

### Panel load spikes / availability flaps `offline` (reconnect storm)

**Symptom:** `brilliant/<panel>/availability` flips `offline`/`online`
repeatedly; the journal floods with `Lost connection to peer` / `Backing off
after failed connection` lines (many per second); panel load climbs well above
~1.0 baseline — yet `systemctl is-active` still shows `active`.

**Why:** The panel bus server gets briefly saturated, drops the bridge's peer,
the lib auto-reconnects aggressively, and each reconnect's re-reconcile feeds
the load back — a self-reinforcing loop. The stale watchdog does not catch it:
every reconnect resets its push clock so the session never looks stale.

**Built-in fix:** The reconnect-storm breaker rebuilds the session (after a
supervisor backoff that gives the bus server a breather) once it reconnects
`RECONNECT_STORM_THRESHOLD` times within `RECONNECT_STORM_WINDOW_SECONDS`
(defaults: 20 reconnects / 60 s window).

**Confirm recovery:** Watch `brilliant/<panel>/availability` on the broker
return to `online` and the journal's reconnect rate drop.

**If it storms repeatedly:** The underlying cause is the panel's
switch-embedded stack being overloaded — compare `top`/`uptime` against a
healthy panel. The breaker only stops the bridge from amplifying the load; the
root issue is an operator/panel concern.

### Panel rebooted on its own (bus-health watchdog)

**Symptom:** A panel rebooted with nobody touching it, and its entities briefly
went unavailable.

**Why:** If you enabled the per-panel **Bus watchdog** switch, an independent
daemon reboots the panel when the Brilliant message bus wedges — the bridge can
no longer read the bus and only a reboot clears it — after the bus-liveness
heartbeat has been stale for `BUS_WATCHDOG_STALE_AFTER` (default 30 min), and
only while the bridge unit is still active and the gateway pings (a plain
network outage is left to the Wi-Fi watchdog, so the two never fight).

**Confirm it was the watchdog:** on the panel,

```bash
tail /var/brilliant-mqtt/bus-watchdog.log
```

a `— rebooting` line names the trigger. Reboots are rate-limited
(`BUS_WATCHDOG_REBOOT_COOLDOWN` / `_CAP` / `_WINDOW`) so it can't reboot-loop.

**If it reboots repeatedly:** the bus keeps wedging — capture
`bus-watchdog.log` plus `journalctl -u brilliant-mqtt` and open an issue. Full
settings in [CONFIGURATION.md → Bus-health watchdog](CONFIGURATION.md#bus-health-watchdog).

### Bridge broken after a panel firmware update

Firmware OTA replaces `/data/switch-embedded` — including the closed-source bus
libraries the bridge imports. If their API changed, the bridge can fail at
startup.

Re-run the read-only PoC smoke checks and re-validate before letting the rest
of the fleet update — see [reference/deployment.md](reference/deployment.md).

### Panel UI feels sluggish

The bridge unit is resource-capped (`MemoryMax`, `CPUQuota`, `Nice`) precisely
to prevent it from degrading the touchscreen. If the caps were edited, restore
them:

```bash
systemctl show brilliant-mqtt | grep -E 'Memory|CPU'
```

---

## Voice

> For the full voice setup guide, see [voice.md](voice.md).

### Voice satellite not discovered by HA

- The satellite advertises over zeroconf (`_esphomelib._tcp`). Confirm the
  panel and HA are on the same network segment (or that mDNS is forwarded
  between VLANs).
- Port **6053** (`VOICE_API_PORT`) must be reachable from HA to the panel.
  The integration opens this port in the panel firewall (`nftables`)
  automatically — if you deployed manually, verify it.
- Check that the satellite is running: `systemctl status brilliant-voice` on the
  panel.

### Wake word not detected

- Try a different wake word: `okay_nabu`, `hey_jarvis`, or `hey_mycroft` are
  bundled. Change it via the **Wake word** select entity on the panel's HA
  device page (the change restarts the satellite automatically).
- Confirm the satellite shows **Connected** in HA (*Settings → Voice assistants
  → [your pipeline] → Satellite*). If it shows Disconnected, see
  [satellite not discovered](#voice-satellite-not-discovered-by-ha) above.

### No TTS audio / "panel can't reach HA"

The panel must be able to reach your HA host to download TTS audio. If the
panel is on an IoT VLAN with restricted DNS, set `VOICE_HA_HOST` in
`/etc/brilliant-voice.env`:

```
VOICE_HA_HOST=homeassistant.local=10.0.0.5
```

Format: `hostname=ip`. This adds an `/etc/hosts` entry on the panel so the
satellite can reach HA's TTS endpoint. The HA integration writes this for you
when you set **Home Assistant host override** during onboarding.

### `voice_missing` repair issue in HA

This issue is raised when voice is enabled for a panel but the voice payload is
not running. Press **Repair** in HA — the integration re-downloads the release
asset from GitHub and re-deploys it over SSH. The GitHub release asset must
exist for the installed version of brilliant-mqtt; if you are running from
source or a dev build, deploy the payload manually per
[deploy/voice/README.md](../deploy/voice/README.md).

---

## Cleanup

### Removing a panel's entities from HA

Discovery topics are retained — they must be explicitly cleared. Publish an
**empty retained payload** to each
`homeassistant/<component>/<unique_id>/config` topic, then stop the unit.
HA removes the entities on the next restart or after the discovery timeout.

---

## Getting Help

Open an issue at <https://github.com/joyfulhouse/brilliant-mqtt/issues> with
logs and reproduction steps.

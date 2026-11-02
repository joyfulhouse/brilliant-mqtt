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

### "This panel architecture is not supported"

Setup stopped with `unsupported_architecture`. brilliant-mqtt is qualified only
on armv7l (i.MX6) Brilliant Controls. Newer i350 panels report `aarch64` and are
**not supported yet**. This is deliberate. Do not patch the check out: nobody
has verified that the bridge, its watchdogs or the voice satellite work on that
hardware.

If you have an i350 and want to help qualify it, run the read-only diagnostic
from a checkout of this repo and attach the output to
[issue #155](https://github.com/joyfulhouse/brilliant-mqtt/issues/155) after
reviewing it:

```bash
ssh -o NumberOfPasswordPrompts=1 root@<panel-ip> 'sh -s' \
  < scripts/brilliant-panel/panel_diagnostics.sh > i350-diagnostics.txt
```

See [brilliant-panel/i350-support.md](brilliant-panel/i350-support.md) for what
the diagnostic collects, what is architecture-bound, and the qualification gates.

---

### Add or update fails with `incumbent_unhealthy` (or another `incumbent_*` code)

**Cause:** the panel already has the agent installed and its bridge is
`active`, but when the integration restarted it once under a fresh deployment
ID (the [incumbent health rehearsal](canary-rollback.md#incumbent-health-rehearsal))
it did not publish MQTT health evidence with that ID within the health timeout
(90 s by default). A rollback to such a bridge could never be proven, so the add
or update stops before staging anything: no agent code, unit or other
configuration was changed (only the deployment ID line of
`/etc/brilliant-mqtt.env`), and the just-captured baseline is removed.

**Costs:** every add or update on a panel that already has the agent restarts
its bridge once (entities are briefly unavailable) and waits up to the health
timeout.

**Likely causes:** changed or missing broker credentials in
`/etc/brilliant-mqtt.env`, or a broker the panel cannot reach
(`journalctl -u brilliant-mqtt -n 50`). The health wait cannot tell this apart
from Home Assistant's own MQTT connection dropping mid-wait, so also check Home
Assistant's MQTT integration.

**Ways out:** fix the old bridge's MQTT connection and retry, or uninstall the
agent and re-add the panel (same steps as for `service_crash_looping` below).
The other codes: `incumbent_unhealthy_unverified` — check
`systemctl is-active brilliant-mqtt` and start it; `incumbent_rehearsal_failed`
— the bridge was not restarted and is running, check panel storage and systemd,
then retry; `incumbent_rehearsal_interrupted` — the SSH session dropped and
nothing reconnects: if the restart was already issued, systemd completes it on
its own, so check `systemctl is-active brilliant-mqtt` and start it if it's
down, then follow
[Interrupted baseline capture](canary-rollback.md#interrupted-baseline-capture);
`incumbent_health_unobservable` — Home Assistant could not subscribe to the
panel's health topics; the bridge was not restarted (only its deployment-ID
line may have changed); check the MQTT
integration.

### Add or update fails with `service_crash_looping`

**Cause:** the panel already has the agent installed (for example from an
earlier Home Assistant entry), and its bridge, Wi-Fi watchdog or bus watchdog
kept restarting during the pre-change service check. The fleet units run with
`Restart=always`, so a unit that exits at startup shows `activating
(auto-restart)` between attempts. The integration samples each core service for
about 13.5 seconds before any change; one restart in that window is enough. A
crash-looping bridge can never pass the fresh-MQTT health check that a rollback
waits for, so no safe rollback baseline can be taken. The add or update stops
there: no agent code, unit or configuration was changed, and no baseline is
kept. A crash loop that starts while the baseline is being captured is caught by
the second check, and that just-captured baseline is removed again.

**Costs:** the panel keeps running the old, broken bridge until you fix it. If
the release needed an explicit `release_override`, that override was consumed
and a retry needs a fresh approval. An automatic pre-ordinal admission was also
recorded before the check.

**Likely causes:** missing or changed broker credentials in
`/etc/brilliant-mqtt.env`, or a broker the panel cannot reach. Check with
`journalctl -u brilliant-mqtt -n 50` (or `-u brilliant-wifi-watchdog` /
`-u brilliant-bus-watchdog`) on the panel.

**Ways out:**

1. **Fix the old bridge so it stays up**, for example by restoring its broker
   credentials or broker reachability. When `systemctl status brilliant-mqtt`
   shows `active (running)` and stays that way, retry. If it was a one-off
   restart and the service now stays up, simply retry.
2. **Uninstall the agent, then re-add the panel.**
   - If the panel still has a Home Assistant entry, turn off both the **Wi-Fi
     watchdog** and **Bus watchdog** switches first, then run
     `brilliant_mqtt.uninstall`. `brilliant_mqtt.uninstall` removes only the
     bridge. A watchdog unit left under `/etc/systemd/system` keeps the layout
     `legacy_fixed`, and with the bridge's code gone that watchdog crash-loops
     too, so the next add still fails with `service_crash_looping`. This is the
     same warning as in
     [ha-integration.md → Recovering a stopped pre-ordinal bridge](ha-integration.md#recovering-a-stopped-pre-ordinal-bridge).
   - If the panel has no Home Assistant entry (the usual case for a failed
     re-add), use the
     [manual agent uninstall](#manual-agent-uninstall-panel-without-a-home-assistant-entry).

## Connectivity

### Entities show as unavailable / panel `offline`

- The availability topic is an LWT — `offline` means the agent died or lost the
  broker. systemd restarts it (`Restart=always`); check
  `journalctl -u brilliant-mqtt` for the crash cause.
- If the agent is up but entities stay unavailable, the broker may have dropped
  the retained `online` publish — restart the unit to re-reconcile.

### One panel times out connecting to the broker while others connect

**Symptom.** The panel journal (`journalctl -u brilliant-mqtt`) keeps repeating
`MQTT connection failed; will reconnect after Ns`. Other panels connect normally.
During an attempt the TCP connection is `SYN_SENT` on the panel and `SYN_RECV`
on the broker. The broker received the SYN, but its SYN-ACK never reaches the
panel.

**This is not the agent.** It is a network fault. The agent already limits each
connect attempt to 10 s (aiomqtt `timeout=10.0` in `src/brilliant_mqtt/mqttio.py`).
It then retries with a backoff of 5, 10, 20 and 40 s, capped at 60 s
(`run()` in `src/brilliant_mqtt/__main__.py`). Restarting or redeploying the
agent won't help, because the next attempt takes the same network path.

**Known cause ([#173](https://github.com/joyfulhouse/brilliant-mqtt/issues/173)).**
One access point stopped delivering broadcast and multicast frames to its
wireless clients, while unicast kept working. Hosts on the panels' VLAN, such as
the broker host and Home Assistant, learn a panel's MAC address with a broadcast
ARP request. Those requests never reached the panels, so the hosts' neighbour
entries stayed `INCOMPLETE` or `FAILED` and the SYN-ACK never left the host. A
soft restart of the affected AP restored delivery. Two details can mislead you:

- A capture inside the broker's network namespace can still show SYN-ACKs being
  sent. That does not prove they left the host's physical interface: without a
  resolved neighbour entry they are never transmitted.
- Panels that happened to send their own traffic to the host looked healthy. The
  panel's ARP request teaches the host its MAC, so the host never needs the
  broadcast path. A "healthy" panel on the same AP can be affected too.

**Read-only checks on the broker host.** Replace the placeholders with your
values:

```sh
ip neigh show <panel-ip>              # INCOMPLETE / FAILED = MAC never learned
ip route get <panel-ip>               # direct on the VLAN interface, or via a gateway?
ss -tn state syn-recv '( sport = :1883 or sport = :8883 )'
timeout 15 tcpdump -ni <vlan-iface> -c 200 'arp and host <panel-ip>'
```

Run the same checks against a healthy panel on the same VLAN and the same AP. If
the host sends ARP requests for the failing panel and gets no replies, while the
healthy panel answers, suspect group-addressed frame delivery on the AP rather
than the panel or the broker.

**Rule out the other causes:**

- **Firewall / conntrack:** check that no rule or full conntrack table drops the
  broker's reply for that panel only.
- **Asymmetric route:** `ip route get <panel-ip>` should send the reply back on
  the same path the SYN arrived on.
- **MTU:** an MTU problem usually breaks the connection after the handshake (for
  example during the TLS or MQTT CONNECT exchange), not during the handshake.

**Do not** flush neighbour tables, add permanent neighbour entries, or restart
panels while you diagnose. Each can make the symptom disappear temporarily and
hide the fault.

For the full qualification procedure see
[broadcast-arp-vs-mqtt-diagnostics.md](brilliant-panel/runbooks/broadcast-arp-vs-mqtt-diagnostics.md).

### Update fails with "could not connect to the panel"

An Update-entity install (or `redeploy`) retries a transient SSH connect failure
before it changes anything on the panel: host or network unreachable, connection
refused or reset, connect timeout, a temporary DNS failure, or an SSH connection
lost during connect. A failure is retried only if the connection never reached
SSH authentication, so the password was never sent on that attempt. A timeout or
dropped connection after authentication began is not retried.

The update makes up to 5 attempts with capped, jittered exponential backoff (at
most 5, 10, 20 and 30 seconds). The 2-minute retry budget starts when the first
attempt gets the integration's SSH slot, so waiting behind other panels' updates
does not use it up. No retry starts after the budget ends, and a retry that cannot
get the SSH slot in time gives up. The retries therefore end within about 2
minutes plus one connect timeout of the first attempt. While it waits, the
progress bar stays at 10% and each retry is logged at warning level, for example
`office: agent update connect attempt 1/5 failed (host_unreachable); retrying in 4.2 s`.
Connect-failure logs name the errno (for example `OSError 113 EHOSTUNREACH`),
not the panel address.

Other panels can use SSH during the backoff. A second update or repair of the
same panel is refused until this one finishes. Other operations on the same panel
(uninstall, reboot, component changes, refreshing staged copies) are not blocked
during the backoff. If Home Assistant unloads or reloads the integration while an
update is waiting, the update stops without connecting and does not raise a
panel Repair issue; after a restart it is surfaced as described in
[Update interrupted by a Home Assistant restart](#update-interrupted-by-a-home-assistant-restart).

Authentication failures, host-key changes, key-exchange or protocol errors, and
release-admission refusals are not retried. Panels use password-only root SSH,
so a retried bad password could lock the panel out. When every attempt fails,
the update reports `agent update could not connect to the panel after N
attempts`. Check that the panel is powered and on the network, then retry. If
MQTT shows the panel online but SSH from Home Assistant is unreachable, suspect a
stale neighbor (ARP) entry on the Home Assistant host's route to the panel.
Traffic from the panel toward Home Assistant usually clears it.

If MQTT shows the panel online, also check Home Assistant's MQTT integration:
online MQTT says nothing about SSH reachability from Home Assistant.

#### Retry state

While an update waits to retry, its state is logged at warning level and shown
in diagnostics (`panels.<id>.update.retry`) and on the Update entity's `update`
attribute:

| Field | Meaning |
|---|---|
| `stage` | `connect` (connecting) or `admission` (the session was lost during release admission) |
| `attempt` / `max_attempts` | attempts used so far of the update's single budget (5) |
| `reason` | classified fault, for example `host_unreachable`, `timeout`, `connection_lost` |
| `retry_in_seconds` | backoff before the next attempt |
| `next_attempt_at` | wall-clock time (UTC, ISO 8601) of the next attempt |

After a failed update, `update.last_error` holds its typed final error
(`stage`, `code`, `attempts`), for example `admission` /
`admission_connection_lost` / `5`. A queued attempt never shows as completed
work: a resumed admission resets the progress bar to 10%.

#### Lost connection during an update

Connects and resumed admissions share one budget: 5 attempts within 2 minutes.

- **During release admission**: the update reconnects and admits again, without
  re-pinning a host key and only for exactly the same release. It is resumed
  only when no release override was used and no provisioning journal exists. A
  changed bundled release stops it with `release_candidate_changed`. When the
  budget runs out the update reports `agent update lost the panel connection
  during release admission after N attempts`.
- **With a release override**: the update is not resumed and reports
  `release_override_used_or_write_failed`. The single-use override may already be
  consumed on the panel. Obtain a fresh override from `redeploy`; do not reuse
  the old one.
- **During baseline capture**: nothing is resumed. If a provisioning journal or
  retained baseline concerns the panel, the update reports
  `provisioning_recovery_required`: recovery settles the journal, so remove
  nothing by hand. Otherwise it reports `baseline_capture_interrupted`: a
  completed capture may remain on the panel; follow
  [Interrupted baseline capture](canary-rollback.md#interrupted-baseline-capture).
  Any other failure between baseline capture and the journal (verification, a
  journal write, a cancellation) removes the unjournaled baseline automatically
  and reports the original error; `baseline_capture_interrupted` then appears
  only if that removal failed, timed out, or could not prove the baseline is
  unowned.
  The recovery wording also appears when a journal or retained record matched
  this panel only by host (for example a DHCP address reused by another panel).
  If diagnostics show no journal and no retained record that actually names this
  panel (by `panel`), treat the capture as unjournaled and use Interrupted
  baseline capture.
- **After the transaction began** (deployment, activation, health check): the
  journal's verified rollback runs over a fresh SSH session pinned to the
  journaled host key. Activation is never repeated. If the panel cannot be
  reached, the restore intent stays in the journal for automatic recovery, the
  journal records a typed `last_error`, and a Repair names `connection_lost`.
  See [canary-rollback.md](canary-rollback.md#if-recovery-fails).

Residual window: a command already sent on the lost session is not fenced. If
it lands on the panel after the rollback verified the restore, nothing detects
it. Before updating that panel again, confirm its installed agent version (the
Update entity or diagnostics `meta.agent_version`).

### Update interrupted by a Home Assistant restart

If Home Assistant stops while an update is connecting or in release admission
(before any rollback baseline is captured), the update is recorded. After the
restart a fixable Repair issue, **Agent update was interrupted**, names the
panel, target version, stage and attempts. Setup never connects to the panel
for it, and it is never resumed automatically. If a provisioning journal or
retained baseline concerns the panel, the journal wins and no issue is shown.

Open the issue and choose:

- **Retry**: starts a fresh update with a fresh retry budget. Its progress shows
  on the panel's Update entity. Retry is refused if the bundled release changed
  (dismiss and update again), if a provisioning journal now concerns the panel
  (the record is dropped; recovery owns the panel), or while another update or
  repair of the panel runs (the issue stays; retry later).
- **Dismiss**: forgets the interrupted update.

The issue's `expired` flag (diagnostics `update.interrupted.expired`) means the
interrupted update's retry deadline has passed; on the entity it refreshes only
when the entity state is otherwise written. Home Assistant's storage helper does
not report disk write errors, so a failed write of this record cannot be
detected.

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

## diyHue path

For panels driving HA lights through a local diyHue bridge — see
[the diyHue bridge guide](brilliant-panel/diyhue-bridge.md).

### A diyHue light shows on the panel but is offline / won't respond

The light populated but sits `reachable=False` (panel `status=0`) and the panel
drops its commands. Cause: the HA entity is **not tagged for diyHue**, so diyHue
never syncs its state.

1. Add `diyhue: include` to the entity under `homeassistant: → customize:` in HA
   `configuration.yaml`, run `ha core check`, then call the
   `homeassistant.reload_core_config` service. Confirm the attribute on
   `/api/states/<entity>`.
2. Restart the diyHue pod (forces a full state sync; it does **not** duplicate
   already-served lights).
3. Restart the **current Hue leader's** coordinator (`touch
   /var/run/brilliant/processes/hue_bridge_peripherals.ini`). The light should
   flip to `status=1` and become controllable.

### diyHue bridge is grayed out / "0 configured devices" / stuck "searching"

Usually the aftermath of **removing and re-adding the Hue integration on the
panel** — the removal empties the bridge's stored credential, leaving it
paired-but-empty, and the *Add Philips Hue* flow then hangs on
"press the button". The UI re-pair will not recover it; the credential must be
re-injected on the bus and the coordinator restarted. Full procedure:
[diyHue bridge → Recover a stuck / grayed-out bridge](brilliant-panel/diyhue-bridge.md#operational-runbook).

### diyHue lights appear but aren't in any room

Freshly imported diyHue lights have no room assignment, so the panel files them
under no room. Assign each light's `room_assignment` per
[diyHue bridge → Assign lights to rooms](brilliant-panel/diyhue-bridge.md#operational-runbook).

### diyHue lights all went offline after a firmware OTA or a panel restart

Two things happen on OTA/restart, and both are expected to self-heal:

- **The CA was wiped** (`/data` is replaced). The **Hue CA recovery** component
  re-appends it and restarts the coordinator within ~2 min of boot — if it is
  enabled on the panel. If lights stay dark, confirm the component is enabled on
  **every** panel (see below) — see
  [Configuration → Hue CA recovery](CONFIGURATION.md#hue-ca-recovery).
- **The Hue integration leader moved** to another panel. That's normal; the new
  leader reconnects because the CA is on all panels. If it doesn't, that panel is
  probably missing the CA — the whole reason Hue CA recovery must be enabled
  **fleet-wide**, not on a subset.

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
network outage is left to the Wi-Fi watchdog, so the two never fight). Since
issue #87 it also requires `bus_confirmed`: the last session must have actually
reached the local-bus phase with a still-live writer (recorded in
`BUS_PHASE_FILE`), so a broker/DNS/TLS/auth or ledger startup failure — which
never reaches the bus — no longer counts as a wedge and cannot reboot a healthy
panel.

**After a partial upgrade or rollback:** the bridge (phase writer) and this
watchdog (phase reader) must move together. If you revert only one side, delete
the phase file so a stale `bus` marker can't be misread as confirmed:

```bash
rm -f /run/brilliant-mqtt/bus-phase
```

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

### Manual agent uninstall (panel without a Home Assistant entry)

Use this only when the panel has no Home Assistant entry, so the watchdog
switches and `brilliant_mqtt.uninstall` are unavailable. It runs the same
commands the integration runs when you turn off the Wi-Fi and Bus watchdog
switches and then call `brilliant_mqtt.uninstall`, in that order. It never
touches the firmware tree.

**Prerequisite:** confirm that Home Assistant shows no pending provisioning
recovery, rollback, Repair issue, or interrupted-onboarding journal for this
panel. Settle any of those first, as described in
[canary-rollback.md → Interrupted onboarding without an HA panel owner](canary-rollback.md#interrupted-onboarding-without-an-ha-panel-owner).
The manual uninstall deletes `/var/brilliant-mqtt`, which would destroy the
rollback baseline such a recovery still needs.

Connect to the panel (the password is only offered once):

```bash
ssh -o NumberOfPasswordPrompts=1 root@<panel-address>
```

**Step 1: check for a retained rollback baseline.** Paste and run this block
**on its own** and read its output before going further. If it prints `STOP`,
its `exit` ends the SSH session: do not uninstall, and settle the baseline from
Home Assistant first (see [canary-rollback.md](canary-rollback.md)). This guard
is at least as strict as the integration's own check, which refuses to
uninstall with `baseline_retained_finalize_first`.

<!-- manual-agent-uninstall-guard:start -->
```bash
if ls /var/brilliant-mqtt/.rollback/*/complete.json >/dev/null 2>&1; then
  echo "STOP: retained rollback baseline present; do not uninstall"
  exit 1
fi
echo "OK: no retained rollback baseline"
```
<!-- manual-agent-uninstall-guard:end -->

**Step 2: remove the watchdogs, then the bridge.** Run this only after step 1
printed `OK`. Its first line repeats the step 1 check, so it stops (and ends
the session) instead of deleting a retained baseline:

<!-- manual-agent-uninstall:start -->
```bash
if ls /var/brilliant-mqtt/.rollback/*/complete.json >/dev/null 2>&1; then echo "STOP: retained rollback baseline present; do not uninstall"; exit 1; fi
systemctl disable --now brilliant-wifi-watchdog 2>/dev/null || true
rm -f /etc/systemd/system/brilliant-wifi-watchdog.service
rm -rf /var/brilliant-mqtt/wifi_watchdog /var/brilliant-mqtt/wifi_watchdog.staging
rm -f /var/brilliant-mqtt/wifi-watchdog.log /var/brilliant-mqtt/wifi-watchdog.state
systemctl daemon-reload
systemctl disable --now brilliant-bus-watchdog 2>/dev/null || true
rm -f /etc/systemd/system/brilliant-bus-watchdog.service
rm -rf /var/brilliant-mqtt/bus_watchdog /var/brilliant-mqtt/bus_watchdog.staging
rm -f /var/brilliant-mqtt/bus-watchdog.log /var/brilliant-mqtt/bus-watchdog.state
systemctl daemon-reload
systemctl disable --now brilliant-mqtt 2>/dev/null || true
rm -f /etc/systemd/system/brilliant-mqtt.service /etc/brilliant-mqtt.env
rm -rf /var/brilliant-mqtt /var/brilliant-mqtt.staging /var/brilliant-mqtt.staging.tar.gz
systemctl daemon-reload
```
<!-- manual-agent-uninstall:end -->

The panel now has no agent layout. Add it again with **Add Brilliant panel**.

---

## Getting Help

Open an issue at <https://github.com/joyfulhouse/brilliant-mqtt/issues> with
logs and reproduction steps.

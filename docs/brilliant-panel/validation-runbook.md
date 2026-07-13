# Validation runbook

Use the designated pilot for the first validation of every new firmware build or control surface. These are production in-wall panels: begin read-only, change one variable at a time, restore the exact baseline, and stop if the bus/UI/watchdogs show instability.

## Evidence levels

| Level | Allowed action | Suitable claims |
|---|---|---|
| A: static | Corpus, Thrift schemas, strings, decompiler, code/tests | “Firmware defines/contains…” |
| B: read-only live | SSH metadata, bus snapshot/subscription, MQTT/HA state read | “Instance exists/reports…” |
| C: benign write/restore | One scalar variable, observed result, immediate baseline restore | “Command path works…” |
| D: end-to-end/failover | Physical/UI/HA action, state convergence, restart/handoff | “Feature is supported…” |

Never label a Schema- or UI-only finding as live support.

## 1. Preflight

1. Confirm the target is the designated pilot.
2. Confirm physical access is possible if the UI or load needs recovery.
3. Record active and rollback OSTree releases.
4. Record free memory, load, temperature, and disk.
5. Confirm native UI, message bus, bridge, and relevant watchdogs are active.
6. Ensure HomeKit remains paired as fallback.
7. Ensure no firmware update, mesh DFU, factory reset, or electrical calibration is in progress.
8. Use the SOPS-backed per-panel SSH password only in `SSHPASS`; never paste it into a command, log, or document.

Example read-only preflight:

```bash
sshpass -e ssh \
  -o PreferredAuthentications=password \
  -o PubkeyAuthentication=no \
  -o NumberOfPasswordPrompts=1 \
  root@<pilot-ip> '
    ostree admin status
    free -m
    uptime
    df -h /var
    systemctl is-active message_bus switch_ui_app brilliant-mqtt
  '
```

## 2. Firmware/corpus gate

For every new release:

```bash
scripts/brilliant-panel/acquire.sh --dry-run <release> <pilot-ip>
git check-ignore -v artifacts/brilliant-panel/<release>/raw/pilot-corpus.tar.zst
```

After acquisition:

```bash
zstd -t artifacts/brilliant-panel/<release>/raw/pilot-corpus.tar.zst
sha256sum artifacts/brilliant-panel/<release>/raw/pilot-corpus.tar.zst
```

Compare these before running writes:

- message-bus Thrift service methods;
- `PeripheralType`, `DeviceType`, and `PeripheralStatus` enums;
- required fields for mapped interfaces;
- signatures of `RPCObserver` and peripheral-host classes;
- mapped first-party Cython module hashes;
- UI build ID and relevant variable names;
- systemd service launch paths.

A removed type, required field, method, or socket path is a stop condition until the adapter is reviewed.

## 3. Read-only bus validation

Use a unique client name and the firmware's own `RPCObserver` recipe. Validate:

- processor connects within five seconds;
- owning device ID is present in `get_all()`;
- expected own-device peripheral types exist;
- `ble_mesh` exists on at least one healthy panel;
- every mapped variable has the expected settable flag;
- a subscription delivers a notification after a benign physical change;
- reconnect callback re-subscribes and reconciles;
- the observer mirror and a second spy agree after the same change.

Sanitize snapshots before tracking anything. At minimum redact home ID, device IDs, tokens, keys, secrets, SDP, SSIDs, emails, account data, and complex blob values.

## 4. MQTT and HA telemetry validation

Without changing a device:

1. Read the bus value for one wired load, one faceplate sensor, the hardware peripheral, and one mesh load.
2. Read the corresponding retained MQTT state and availability topics.
3. Read the HA entity states.
4. Confirm value, unit, scaling, availability, and device assignment agree.
5. Restart only the HA MQTT consumer and confirm retained state restores entities.
6. Restart the bridge on the pilot and confirm LWT transitions offline → online and state reconciles.

For the frozen-stream failure class, do not trust `get_all()` as an independent oracle. Use a second observer or direct notification timing.

## 5. Scalar write/restore protocol

Use only variables already identified as externally settable and suitable for the experiment.

For each variable:

1. Read and record exact baseline value and timestamp.
2. Choose a reversible alternate value within native bounds.
3. Subscribe before writing.
4. Issue one bus write.
5. Observe set response, bus notification, UI/physical effect, MQTT echo, and HA state.
6. Restore the exact baseline immediately.
7. Re-read from a healthy observer and confirm the baseline.
8. Check UI/message-bus/bridge logs and resource usage.

Abort on permission error, malformed response, UI restart, bus disconnect storm, unexpected second variable change, physical fault, high temperature, or inability to restore.

### Safe current-build checklist

| Surface | Alternate | Required observation | Restore |
|---|---|---|---|
| Screen brightness | Adjacent integer | UI brightness and HA number converge | Original integer |
| Screen on | Toggle once | Display and state converge | Original bool |
| Alert/output volume | Small bounded delta | State/UI only; avoid loud playback | Original integer |
| Child lock | Toggle with physical access | Native UI behavior and HA switch | Original bool |
| Night mode | Toggle | UI state and notification | Original bool |
| Wake screen on motion | Toggle | Native Motion settings reflect | Original bool |
| Motion sleep timeout | Nearby native-valid value | UI number/state reflect | Original integer |
| Screensaver/widget | Toggle one at a time | Lock screen setting reflects | Original bool |
| Touch sliders | Disable briefly | Physical slider ignored, touchscreen still usable | Original bool immediately |
| Intercom receive preference | Toggle without placing call | UI setting reflects | Original bool |
| Mesh status LED brightness | Small delta | One selected accessory only | Original integer |

Do not test auto-update by causing an update. Validate only read/setting reflection, then restore. Do not enable vendor remote assistance unless the operator explicitly intends to open the tunnel.

### Prohibited generic probes

- wildcard writes;
- `trigger_mesh_dfu`;
- bootstrap/pivot variables;
- reset/reboot variables during feature validation;
- break-circuit/break-dimming/current-sense controls;
- HomeKit reset/token regeneration;
- rootfs/integrity challenges;
- factory/unsupported-configuration flows;
- random serialized blob mutation.

## 6. Load control

Load tests require the operator to identify the physical circuit and confirm it is safe to toggle.

### On/off

1. Choose a non-critical pilot load.
2. Record bus `on`, watts, MQTT state, and HA state.
3. Toggle through HA.
4. Confirm physical response, bus push, watts, MQTT, and HA converge.
5. Return to baseline.

### Dimming

Test 0%, one mid-point, and prior level while respecting `minimum_dim_level`, `maximum_dim_level`, and `max_intensity_value`. Confirm HA 0–255 scaling and avoid calibration variables.

### Always-on

Confirm no switch/light entity is created and power remains observable. Never attempt to synthesize an `on` variable.

## 7. Motion

### Faceplate

Validate the enabled detection mode, then create controlled motion at known distance. Record PIR score, movement state, screen state, MQTT, and HA timing. Restore enable flags and thresholds.

### Mesh

Validate `enable_motion_score`, raw score, configured thresholds, derived motion, clear delay, desired-state reconcile, and leader failover. Do not rely on the firmware's stale `movement_detected` latch as the only oracle.

## 8. Deprecated physical-Control HA mirror

Do not run the former hosted-peripheral validation procedure. Physical-Control
HA hosting is rejected: it co-managed a real Control, added bus peers, threatened
physical responsiveness, and did not reliably admit or propagate native tiles.
Room metadata did not repair ownership or command routing. Keep
`brilliant-ha-mirror` inactive and follow the
[retirement/cleanup guide](../ha-mirror.md).

The supported hardware gate is the non-hosting
[Office scene-bridge pilot](runbooks/scene-bridge-pilot.md). Native HA tiles are
a separate Virtual Control research track and remain blocked until every
feasibility gate passes. Historical Tier-1 observations may support failure
analysis, but they do not authorize another physical-host pilot.

## 9. Scene validation

Only after the operator approves a benign scene:

1. read the scene catalog and identify an existing no-risk scene;
2. record all target loads and their baselines;
3. subscribe to execution and target peripherals;
4. write the candidate scene trigger once;
5. verify execution state and every target action;
6. restore target baselines if the scene changed them;
7. confirm no mesh DFU or state-config handler was invoked.

One successful write is not enough to expose arbitrary scene blobs. The public integration should accept a stable scene ID only after catalog lookup.

## 10. Media isolation validation

Camera/intercom testing requires separate consent because it can capture audio/video. Record privacy-toggle state, camera LED/state, active sessions, network endpoints, and storage behavior. Do not retain personal media in tracked artifacts.

Test in this order:

1. speaker-only local playback;
2. chime/announcement and ducking;
3. panel-to-panel audio;
4. panel-to-panel video;
5. HA live view;
6. remote access/network isolation.

Stop if camera/mic remains active after teardown, media is written unexpectedly, or CPU/memory threatens the UI watchdog.

## 11. Evidence record

Every completed live validation should record:

```text
date/time and timezone:
firmware release + OSTree commit:
panel role (pilot/secondary; no public hostname or ID):
surface and exact variable(s):
baseline:
action:
set response:
bus notification latency:
MQTT/HA convergence latency:
physical/UI observation:
restored value:
post-checks:
result: PASS / FAIL / INCONCLUSIVE
artifact paths (ignored):
```

Update the support matrix only after the restored baseline and post-checks pass.

## 12. Repository verification

Before committing documentation or scripts:

```bash
uv run pytest tests/test_panel_acquire_script.py -q
bash -n scripts/brilliant-panel/acquire.sh
git check-ignore -v artifacts/brilliant-panel/v26.06.03.1/raw/pilot-corpus.tar.zst
git status --short --ignored
rg -n -i 'password|token|secret|authorization|home_id|ssid' docs/brilliant-panel scripts/brilliant-panel
```

Review every match. Variable names and warnings may legitimately contain those words; secret values may not.

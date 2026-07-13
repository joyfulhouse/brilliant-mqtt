# Office scene-bridge hardware acceptance

This runbook validates only the safe scene/mode bridge on the Office panel. It
does not validate native HA device tiles, provision a Virtual Control, host a
peripheral, or authorize legacy mirror removal until the final gate passes.

Read the [HA control/scene integration guide](../home-assistant-integration.md),
the generic [panel validation runbook](../validation-runbook.md), and Task 12 of
the [implementation plan](../../superpowers/plans/2026-07-12-ha-control-plane-scene-bridge.md)
before starting.

## Scope, roles, and hard stops

Target: **Office only**. Do not redeploy another panel during this pilot.

Required people and conditions:

- one operator with physical access to Office and knowledge of the affected
  circuit(s);
- one HA operator able to reconfigure the Brilliant MQTT entry, inspect events,
  run services, and restart HA/MQTT in the approved local way;
- a benign, existing Brilliant scene whose exact target loads and restore
  baselines are known;
- no firmware update, mesh DFU, electrical calibration, or other panel pilot in
  progress;
- working native UI and physical controls before deployment; and
- an agreed recovery path for every affected load.

Stop immediately and begin rollback if any of these occurs:

- physical touch/slider/light control becomes sluggish, inconsistent, or lost;
- `message_bus` or `switch_ui_app` restarts;
- bus socket peers/connections do not return to baseline after the scoped
  observer exits;
- a new peer-add timeout/rejection, reconnect storm, or Brilliant cloud-peer
  disconnect appears;
- an unexpected peripheral, manager, or hosted record appears;
- scene execution affects an unlisted load or cannot be restored;
- the old mirror becomes active or direct HA connection fields appear on-panel;
- credentials or personal data reach terminal capture or the repository; or
- evidence is contradictory, missing, or cannot be conservatively redacted.

Any hard stop is `FAIL` or `INCONCLUSIVE`, never an inferred pass. Leave
`brilliant-ha-mirror` stopped. Do not restart Tier 1.

## Artifact and credential rules

Create one ignored directory from the repository root:

```bash
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="artifacts/brilliant-panel/pilots/scene-bridge-${RUN_TS}"
mkdir -p "$RUN_DIR"
git check-ignore -q "$RUN_DIR"
```

If the ignore check fails, stop before collecting evidence. Store only redacted
summaries and protocol payloads known to contain no private data. Do not store a
decrypted SOPS file, shell history, broad `/var` capture, environment dump, raw
journal, MQTT credential, account/home identifier, network name, device ID,
certificate, bootstrap material, HA credential, or panel password, even under
this ignored directory.

Use the repository's approved SOPS/sealed-secret workflow to load the panel
password directly into the temporary `SSHPASS` environment. Do not put the
decryption command in a recorded terminal, assign the value on a command line,
pass it as an argument, echo it, enable shell tracing, or redirect it. Commands
below assume `SSHPASS` is already exported.

```bash
# Check presence without revealing a value.
test -n "${SSHPASS+x}"

# Remove it immediately after the last SSH operation.
unset SSHPASS
```

Before saving any evidence, check it for:

- credentials, authorization headers, cookies, certificates, and private keys;
- HA URLs or access material;
- public/private addresses, network names, account/home/device identifiers;
- emails, personal names, scene target details not needed for the result;
- serialized variable blobs, media/session data, and full command lines.

Replace sensitive values with stable role labels such as `<office-panel>` or
`<pilot-load-1>`. Keep timestamps, counts, event types, scene/mode IDs approved
for this pilot, status reasons, and measured durations.

## Evidence record

Create `record.md` inside the ignored run directory with one row per criterion.
Every row must include:

```text
criterion:
start UTC / end UTC / elapsed ms:
precondition and baseline:
action (sanitized):
MQTT observation and count:
HA event/action observation and count:
panel/bus/physical observation:
post-state and restored baseline:
artifact filename + SHA-256 (if any):
result: PASS / FAIL / INCONCLUSIVE
operator initials and reason:
```

Use monotonic elapsed time for latency when the tool supplies it; record UTC
wall time for correlation. Never “correct” a timestamp after the test. Hash
evidence after redaction:

```bash
sha256sum "$RUN_DIR"/*
```

## Phase 0: repository and payload gate

Run from the exact commit intended for Office. All commands must exit zero.

```bash
git status --short --branch
git rev-parse HEAD

scripts/build_payload.sh
git diff --exit-code -- custom_components/brilliant_mqtt/agent_payload

uv run ruff check
uv run ruff format --check
uv run mypy --strict src tests
uv run pytest

uv run --project ha ruff check --config ha/pyproject.toml \
  custom_components/brilliant_mqtt ha/tests
uv run --project ha ruff format --check --config ha/pyproject.toml \
  custom_components/brilliant_mqtt ha/tests
uv run --project ha mypy --strict --config-file ha/pyproject.toml \
  custom_components/brilliant_mqtt ha/tests
uv run --project ha pytest -c ha/pyproject.toml ha/tests

git diff --exit-code
```

Record the commit, payload `VERSION`, test counts, and clean-diff result. Do not
deploy an uncommitted rebuild or copy `src/` directly to the panel.

## Phase 1: read-only baseline

Run every baseline before changing configuration.

### 1.1 Firmware, OSTree, resources, and services

Use password authentication from `SSHPASS`; do not add it to the command.

```bash
sshpass -e ssh \
  -o PreferredAuthentications=password \
  -o PubkeyAuthentication=no \
  -o NumberOfPasswordPrompts=1 \
  root@<office-panel-ip> '
    ostree admin status
    free -m
    uptime
    df -h /var /data
    systemctl is-active message_bus
    systemctl is-active switch_ui_app
    systemctl is-active brilliant-mqtt
    systemctl is-active brilliant-ha-mirror || true
    ps -C brilliant-mqtt -o pid=,pcpu=,rss=,etime=,comm=
  '
```

Record the firmware release/OSTree commit, active and rollback deployments,
load average, free memory, bridge CPU/RSS, disk space, and service states.
Expected baseline: `message_bus`, `switch_ui_app`, and `brilliant-mqtt` active;
`brilliant-ha-mirror` inactive or absent.

### 1.2 Panel environment key names only

The first command prints names, never values. The second rejects old direct HA
keys without revealing whether any value existed. The third proves the old
mirror environment is absent.

```bash
sshpass -e ssh root@<office-panel-ip> \
  "sed -n 's/=.*//p' /etc/brilliant-mqtt.env | sort"

sshpass -e ssh root@<office-panel-ip> '
  if sed -n "s/=.*//p" /etc/brilliant-mqtt.env |
     grep -Eq "^(HA_TOKEN|HA_WS_URL)$"; then
    echo "forbidden direct-HA key present"
    exit 1
  fi
  test ! -e /etc/brilliant-ha-mirror.env
'
```

Record key names and pass/fail only. Expected before enable:
`SCENE_BRIDGE_ENABLED` exists and is `0` when inspected through the integration's
adopt/reconfigure state; never print the file to learn the value. The normal
reconfigure flow is the authoritative value check.

### 1.3 Retained MQTT and HA baseline

Use Home Assistant's MQTT integration “Listen to a topic” tool, not a broker
password in a shell command. Capture these exact topics separately:

```text
brilliant/ha-control/v1/scene/catalog/office
brilliant/ha-control/v1/mode/catalog/office
brilliant/ha-control/v1/status/scene/office
brilliant/ha-control/v1/status/mode/office
```

Before deployment these may be absent. Record absence as baseline, not failure.
Also record the Office config-entry availability, current diagnostics, current
scene-select availability, and whether the benign scene/mode exists in native
Brilliant UI.

### 1.4 Scoped bus observer and peer/host baseline

Use the read-only observer recipe from
[generic runbook section 3](../validation-runbook.md#3-read-only-bus-validation).
It must:

1. use a unique, time-bounded client name;
2. connect only to `/var/run/brilliant/server_socket`;
3. read the Office owning device, its `execution_peripheral`, and the two scoped
   configuration peripherals only;
4. never construct `PeripheralHost`, call `register`, acquire/bid a lease, write a
   variable, or use a whole-home unbounded host read; and
5. shut down and prove its one temporary socket peer disappeared.

Count server-socket connections with the same command immediately before the
observer connects, while it is connected, and after it closes:

```bash
sshpass -e ssh root@<office-panel-ip> '
  ss -xanp | awk "/\\/var\\/run\\/brilliant\\/server_socket/ {n++}
                  END {print \"message_bus_socket_rows=\" n+0}"
'
```

This is a repeatable socket-row proxy, not a firmware API claim. Record the
exact counting method and all three values. The observer should add its known
temporary row(s) only; the post-close value must equal the pre-observer value.
Record separately the own-device peripheral count, whether any allowlisted old
mirror/pilot IDs are present, and that no extra host/manager is constructed.
Use identical baseline and post-deployment procedures; method changes make the
criterion `INCONCLUSIVE`.

### 1.5 Logs and physical load baseline

Inspect recent `brilliant-mqtt`, `message_bus`, UI, and approved Brilliant
cloud-peer logs on-screen. Do not redirect raw journal output. Save only counts
of peer rejection/timeout, reconnect, process restart, and cloud disconnect
markers for the fixed baseline window. Example count-only queries:

```bash
sshpass -e ssh root@<office-panel-ip> \
  'journalctl --since "<baseline-window-start-UTC>" -u brilliant-mqtt -u message_bus |
   grep -Eic "peer.*(timeout|reject)|reconnect storm" || true'

sshpass -e ssh root@<office-panel-ip> \
  'journalctl --since "<baseline-window-start-UTC>" -u switch_ui_app |
   grep -Eic "restart|fatal|abort" || true'
```

Use the already-approved local cloud-peer status/log surface for this firmware
and save only state plus disconnect count. If no reviewed read-only surface is
available, mark the cloud-peer criterion `INCONCLUSIVE`; absence of a matching
log line is not proof of connection.

For each target load, record exact logical/physical baseline and ten ordinary
touch/slider interaction latencies by operator observation. Do not change
calibration or electrical variables.

## Phase 2: Office-only deployment

1. In Home Assistant, open the Office Brilliant MQTT config entry and choose
   **Reconfigure**.
2. Set HA control enabled to on, select `office` as the default scene panel, and
   enter only the reviewed benign scene-action JSON.
3. Keep native tiles blocked. There is no native-tile enable control and the
   diagnostic must remain `blocked` / `validated:false`.
4. Save through the normal integration flow. Do not use `scp`, a foreground
   `PeripheralHost`, or the old mirror component.
5. If the committed payload is not already installed, call the integration's
   **Redeploy agent** action/service targeted only to the Office config entry.
   Do not trigger fleet redeploy/repair during this pilot.
6. Confirm the manager's retirement Repair is clear only after verified mirror
   uninstall. Never start the old service to test it.

The settings are HA-global, but the hardware rollout remains Office-only:
redeploy/reconfigure only Office. Other panels must not receive a payload restart
during this gate.

Repeat Phases 1.1–1.5 after deployment. Expected:

- `brilliant-mqtt` active and `brilliant-ha-mirror` inactive/absent;
- no direct HA key or old mirror environment on-panel;
- valid retained Office scene/mode catalogs and status;
- peer/socket count returns to the exact baseline after the observer exits;
- no additional hosted peripheral/manager;
- no new peer, cloud, reconnect, UI, resource, or physical-load regression.

## Phase 3: acceptance matrix

Perform the tests in order. Stop at the first hard stop.

| # | Test and exact observation | PASS requirement |
|---:|---|---|
| 1 | Services and credentials | `brilliant-mqtt` active; `brilliant-ha-mirror` inactive/absent; UI/message bus active; no old environment/direct HA key. |
| 2 | Bus peer/host invariance | Identical before/after scoped-observer method; post-close socket count equals baseline; no new manager/hosted peripheral. |
| 3 | Physical Brilliant scene → MQTT → HA | One approved physical scene execution produces exactly one non-retained scene MQTT event, one `brilliant_mqtt_scene` HA event, and one configured HA action dispatch in the observation window. IDs/timestamps/dedup key agree. |
| 4 | Brilliant mode (conditional) | If a real mode exists, one physical change produces exactly one MQTT mode event and one `brilliant_mqtt_mode` event. Otherwise record the empty catalog and explicit `INCONCLUSIVE — no configured modes`; do not invent one. |
| 5 | Reconnect replay suppression | After a controlled MQTT/agent reconnect with no new physical execution, zero old scene/mode MQTT events and zero HA scene/mode events/actions are emitted. |
| 6 | Confirmed HA `run_scene` | The service returns only after a matching scene event/result; command/result IDs, panel, scene ID, and timing correlate; physical targets match and are restored. |
| 7 | Confirmed HA `set_mode` (conditional) | Hardware-test only when a real configured mode exists. Otherwise keep off-panel test evidence and the explicit no-modes diagnostic. |
| 8 | Restart recovery | HA, MQTT, Office agent, and Office panel are restarted one at a time. Each recovers catalogs/status/control; no old event replays; one new benign execution still crosses end-to-end after each recovery. |
| 9 | Physical responsiveness and stability | Ten consecutive ordinary light interactions remain subjectively immediate; no peer-add timeout, cloud drop, reconnect storm, UI restart, or material resource regression. |
| 10 | Disable rollback | Disabling HA control and applying the Office config removes scene subscriptions/command availability and creates/deletes no Brilliant device or peripheral. The unsafe mirror stays stopped. |

### 3.1 Physical scene event

Before touching the scene:

- reset the benign HA pilot marker/action target to its baseline;
- listen to `brilliant/ha-control/v1/scene/event/office` in the HA MQTT tool;
- listen to HA event `brilliant_mqtt_scene`;
- open the configured action's service/automation trace counter;
- record observation-window start UTC; and
- record all scene target load baselines.

Trigger the native scene once from Office. Do not double tap or retry. Wait at
least 20 seconds, then close the window. Record raw counts, the sanitized MQTT
payload, HA event data, action trace, physical effects, and restore result.
Multiple events/actions, a retained event, mismatched IDs/timestamps, or an
unknown target is failure.

### 3.2 Mode event or explicit no-modes result

Inspect the retained Office mode catalog. If it contains a real safe mode,
repeat the one-action observation using:

```text
brilliant/ha-control/v1/mode/event/office
brilliant_mqtt_mode
```

If `modes` is empty, do not create or guess a mode. Record the retained empty
catalog, status, HA diagnostic limitation, and `INCONCLUSIVE — no configured
modes`. This does not invalidate scene acceptance but it does prevent claiming
live mode support for this home.

### 3.3 Replay after reconnect

With both event listeners open, create no new scene/mode execution. Restart only
the Office `brilliant-mqtt` service through the integration's normal agent
restart/redeploy path, or perform the approved MQTT reconnect. Observe for 30
seconds after catalogs/status return. Expected event/action count: zero.

An old retained catalog/status publication is expected and is not an execution
event. Any old scene/mode event/action replay is failure.

### 3.4 Confirmed HA commands

Call the service from HA Developer Tools and record request start/return times:

```yaml
action: brilliant_mqtt.run_scene
data:
  panel: office
  scene_id: <approved-existing-scene-id>
```

Listen simultaneously to the scene command, event, and result topics. The
command must be non-retained. The service must remain pending until the matching
execution event/result, then return before the 16-second HA limit. Confirm the
physical effect and restore every target baseline.

If a real safe mode exists, repeat once with:

```yaml
action: brilliant_mqtt.set_mode
data:
  panel: office
  mode_id: <approved-existing-mode-id>
```

Do not test an unknown ID merely to generate an error on production hardware.

### 3.5 Restart recovery

Restart one component at a time, wait for full recovery, and run the read-only
post-check before continuing:

1. Home Assistant;
2. the local MQTT broker;
3. Office `brilliant-mqtt`;
4. Office panel reboot, with the physical operator present.

For each restart record outage duration, retained catalog/status recovery,
service availability, event replay count before new input, one new benign
end-to-end scene result, peer/socket baseline, process resource state, and cloud
peer observation. Never overlap restarts; overlapping failures are
`INCONCLUSIVE`.

### 3.6 Physical controls and log regression

Repeat ten consecutive ordinary interactions against the same non-critical
light used for baseline. Record subjective immediate/not-immediate plus any
available non-sensitive timing. Repeat fixed-window count-only log checks and
the peer/host procedure. Compare like-for-like windows and methods.

## Phase 4: disable rollback

Rollback disables only safe scene control:

1. Reconfigure HA control enabled to off.
2. Apply/redeploy only the Office agent through the integration.
3. Confirm scene/mode subscriptions and service command availability are gone;
   retained catalogs/status may remain as broker history and must not be mistaken
   for a live subscription.
4. Confirm `brilliant-mqtt` forward bridging remains active.
5. Confirm `brilliant-ha-mirror` remains inactive/absent and no direct HA key is
   present.
6. Repeat the scoped own-device/peer snapshot and prove that no Brilliant
   peripheral was deleted or created by disable.
7. Restore every affected load to its recorded baseline.
8. Repeat physical control and regression checks.

Do not remove the whole forward agent as scene rollback. Do not enable the old
mirror. If rollback cannot restore physical/load/bus baselines, stop and mark
the pilot failed.

After rollback validation, the operator may re-enable the accepted safe scene
bridge through the same normal Office-only flow. Record that as a separate
action and recheck service/peer state.

## Phase 5: legacy cleanup decision

Only after scene acceptance is complete, run a cleanup dry run on Office:

```bash
python -m brilliant_mqtt.cleanup_legacy_mirror
```

Save only its fixed redacted JSON. If there are no candidates, record that and
stop. If candidates exist, review every ID/name/type against the strict dual
allowlist in the [retirement guide](../../ha-mirror.md). Apply is never automatic
and is not part of enable/rollback.

A fresh operator decision is required immediately before deletion. If approved:

```bash
python -m brilliant_mqtt.cleanup_legacy_mirror \
  --apply \
  --snapshot /data/brilliant-mqtt/cleanup/legacy-mirror-<timestamp>.json
```

Require exit zero, `success:true`, empty `remaining_ids`, fresh second-snapshot
proof, clean native-client shutdown, and a private atomic report. On any failure
stop; do not rerun or recreate a host.

## Final disposition and legacy-removal gate

Summarize each matrix row as `PASS`, `FAIL`, or `INCONCLUSIVE`. A conditional
mode row may remain explicitly “no configured modes” without blocking the scene
baseline, but no live mode claim may be made.

Task 12 may delete the legacy mirror source, tests, systemd unit, and payload
only after all applicable hardware criteria pass and the evidence proves:

- physical scene → exactly one MQTT event and HA event/action;
- HA `run_scene` → matching observed execution confirmation;
- unchanged peer/host baseline and no peripheral creation;
- restart/reconnect recovery without old event replay;
- unchanged physical responsiveness and no bus/cloud/reconnect regression;
- successful disable rollback with no peripheral deletion; and
- old mirror inactive/absent with no direct HA connection material on-panel.

Any `FAIL`, unexplained `INCONCLUSIVE`, credential-handling incident, or failed
rollback blocks legacy removal. It never authorizes restarting Tier 1.

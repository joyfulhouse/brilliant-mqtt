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
this ignored directory. Cleanup JSON is especially sensitive: review it only
on-screen and unrecorded, then discard it. Never save or copy the dry-run or
apply JSON, owning device ID, candidate IDs, or candidate names into the run
directory. Persist only sanitized candidate counts/outcome, operator decision,
exit status, and the private apply report's SHA-256 when applicable.

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

# Syntax-check the embedded observer without executing or importing panel libs.
PILOT_DOC=docs/brilliant-panel/runbooks/scene-bridge-pilot.md
awk '/<!-- scoped-observer-shell-start -->/{inside=1; next}
     /<!-- scoped-observer-shell-end -->/{inside=0}
     inside' "$PILOT_DOC" | sed '1d;$d' | bash -n
awk '/<!-- scoped-observer-shell-start -->/{inside=1; next}
     /<!-- scoped-observer-shell-end -->/{inside=0}
     inside' "$PILOT_DOC" | sed '1d;$d' |
  awk '/<<.*PY/{python=1; next} /^PY$/{python=0} python' |
  python3 -c 'import sys; compile(sys.stdin.read(), "scoped-observer", "exec")'

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

### 1.2 Panel environment key names and exact disabled proof

The first command prints names, never values. The second command rejects old
direct HA keys without revealing whether any value existed, and also proves the
old mirror environment is absent.

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

Prove the scene bridge is disabled without printing the environment file or the
matched value. The agent defaults the flag to off when the key is absent, so
either absence or exactly one `SCENE_BRIDGE_ENABLED=0` line is a valid disabled
state:

```bash
sshpass -e ssh root@<office-panel-ip> '
  count=$(grep -c "^SCENE_BRIDGE_ENABLED=" /etc/brilliant-mqtt.env || true)
  if [ "$count" -eq 0 ]; then
    echo "scene_bridge_disabled=true"
  elif [ "$count" -eq 1 ] &&
       grep -qx "SCENE_BRIDGE_ENABLED=0" /etc/brilliant-mqtt.env; then
    echo "scene_bridge_disabled=true"
  else
    echo "scene_bridge_disabled=false"
    exit 1
  fi
'
```

Record key names and the safe boolean result only. Run this exact disabled proof
before the payload redeploy and again after it. Do not infer the state from the
payload `VERSION`, the config-entry UI, or a service restart.

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

Run this exact on-panel observer. It uses the raw `RPCObserver` /
`SinglePeerProcessor` path, a random client suffix, scoped reads only, bounded
operation and shutdown waits, and a shell-level TERM/KILL fallback. It emits
only counts and booleans. It does **not** print the owning device ID, peripheral
IDs, variables, values, or names.

<!-- scoped-observer-shell-start -->
```bash
sshpass -e ssh root@<office-panel-ip> '
  timeout -k 5s 45s \
    env PYTHONPATH=/data/switch-embedded \
    /data/switch-embedded/env/bin/python3 - <<'"'"'PY'"'"'
import asyncio
import secrets

try:
    import lib.protocol.message_bus_peer_service as mbps
    from lib.message_bus_api.observer_interface import RPCObserver
    from lib.protocol.processor import SinglePeerProcessor
except BaseException:
    print("observer_ok=false")
    raise SystemExit(1)

SOCKET_PATH = "/var/run/brilliant/server_socket"


class ReadOnlyObserver(RPCObserver):
    async def handle_notification(self, notification):
        del notification


def socket_rows():
    try:
        with open("/proc/net/unix", encoding="ascii") as stream:
            return sum(SOCKET_PATH in row for row in stream)
    except OSError:
        return -1


async def bounded_shutdown(target):
    if target is None:
        return True
    try:
        await asyncio.wait_for(target.shutdown(), timeout=2.0)
    except BaseException:
        return False
    return True


async def probe():
    loop = asyncio.get_running_loop()
    observer = ReadOnlyObserver(loop)
    processor = SinglePeerProcessor(
        socket_path=SOCKET_PATH,
        my_name=f"brilliant_scene_pilot_ro-{secrets.token_hex(4)}",
        handler=mbps.PeripheralServer(observer),
        client_class=mbps.MessageBusClient,
        loop=loop,
    )
    before = socket_rows()
    while_connected = -1
    after = -1
    own_device_read = False
    own_peripheral_count = 0
    execution_present = False
    scene_present = False
    mode_present = False
    reads_ok = False
    observer_closed = False
    processor_closed = False

    try:
        await asyncio.wait_for(processor.start(), timeout=5.0)
        deadline = loop.time() + 5.0
        while not processor.is_connected():
            if loop.time() >= deadline:
                raise TimeoutError("message bus connection timed out")
            await asyncio.sleep(0.1)
        await asyncio.wait_for(observer.start(processor, None), timeout=5.0)
        while_connected = socket_rows()

        own_id = observer.get_owning_device_id()
        own_device = await asyncio.wait_for(observer.get_device(own_id), timeout=5.0)
        own_peripherals = getattr(own_device, "peripherals", None)
        if own_peripherals is None:
            raise RuntimeError("owning device snapshot unavailable")
        own_peripherals = dict(own_peripherals)
        own_device_read = True
        own_peripheral_count = len(own_peripherals)
        execution_present = "execution_peripheral" in own_peripherals

        scene = await asyncio.wait_for(
            observer.get_peripheral(
                "configuration_virtual_device", "scene_configuration"
            ),
            timeout=5.0,
        )
        mode = await asyncio.wait_for(
            observer.get_peripheral(
                "configuration_virtual_device", "mode_configuration"
            ),
            timeout=5.0,
        )
        scene_present = scene is not None
        mode_present = mode is not None
        reads_ok = True
    except BaseException:
        reads_ok = False
    finally:
        observer_closed = await bounded_shutdown(observer)
        processor_closed = await bounded_shutdown(processor)
        for _ in range(20):
            after = socket_rows()
            if after == before:
                break
            await asyncio.sleep(0.1)

    socket_restored = before >= 0 and after == before
    observer_ok = (
        reads_ok
        and while_connected > before >= 0
        and socket_restored
        and observer_closed
        and processor_closed
    )
    print(f"socket_rows_before={before}")
    print(f"socket_rows_while={while_connected}")
    print(f"socket_rows_after={after}")
    print(f"socket_rows_restored={str(socket_restored).lower()}")
    print(f"own_device_read={str(own_device_read).lower()}")
    print(f"own_peripheral_count={own_peripheral_count}")
    print(f"execution_peripheral_present={str(execution_present).lower()}")
    print(f"scene_configuration_present={str(scene_present).lower()}")
    print(f"mode_configuration_present={str(mode_present).lower()}")
    print(f"observer_shutdown={str(observer_closed).lower()}")
    print(f"processor_shutdown={str(processor_closed).lower()}")
    print(f"observer_ok={str(observer_ok).lower()}")
    return observer_ok


try:
    result = asyncio.run(asyncio.wait_for(probe(), timeout=35.0))
except BaseException:
    print("observer_ok=false")
    raise SystemExit(1)
raise SystemExit(0 if result else 1)
PY
'
```
<!-- scoped-observer-shell-end -->

This observer calls only `get_owning_device_id()`, `get_device(own_id)`, and the
two allowlisted `get_peripheral()` reads. It must never be modified to call
`get_all()`, construct `PeripheralHost`, call `register`, acquire or bid a
lease, invoke a write/delete method, or subscribe to the whole home graph.

The `/proc/net/unix` count is a repeatable socket-row proxy, not a firmware API
claim. Require `observer_ok=true`, the while-connected count greater than the
pre-count, and the post-count exactly equal to the pre-count. Record the method
and emitted counts/booleans only. Use this identical procedure at baseline and
after deployment; changing the observer or counting method makes the criterion
`INCONCLUSIVE`.

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

Never enable a possibly stale payload. Install and prove the exact committed
integration/payload while scene control remains disabled, then enable it:

1. Through the approved HA integration deployment path, install the Brilliant
   MQTT custom integration built from the exact Phase 0 commit. Restart or
   reload HA as required by that path and record the installed commit. A
   matching payload `VERSION` is necessary but is not source-parity proof.
2. In Home Assistant, open the Office Brilliant MQTT config entry, choose
   **Reconfigure**, and explicitly leave HA control disabled. Save through the
   normal integration flow. Run the exact disabled proof from Phase 1.2 again;
   require `scene_bridge_disabled=true` before continuing.
3. Call the integration's **Redeploy agent** action/service targeted only to the
   Office config entry. This step is mandatory even if the displayed version
   matches. Do not use `scp` and do not trigger fleet redeploy/repair.
4. From the exact clean Phase 0 checkout, compare the committed payload files
   against the installed Office files. All three hashes must match:

   ```bash
   for name in scene_bridge.py scene_state.py ha_control_protocol.py; do
     local_path="custom_components/brilliant_mqtt/agent_payload/app/brilliant_mqtt/$name"
     panel_path="/var/brilliant-mqtt/app/brilliant_mqtt/$name"
     local_sha=$(sha256sum "$local_path" | awk '{print $1}')
     panel_sha=$(
       sshpass -e ssh root@<office-panel-ip> "sha256sum '$panel_path'" |
         awk '{print $1}'
     )
     if [ -z "$local_sha" ] || [ "$local_sha" != "$panel_sha" ]; then
       echo "$name sha256_match=false"
       exit 1
     fi
     echo "$name sha256_match=true sha256=$local_sha"
   done
   ```

5. Run the exact disabled proof from Phase 1.2 after redeploy. Stop unless it
   emits `scene_bridge_disabled=true`; do not proceed on UI state or version
   evidence alone.
6. Only after steps 1–5 pass, reconfigure the Office entry: turn HA control on,
   select `office` as the default scene panel, and enter only the reviewed
   benign scene-action JSON. Keep native tiles blocked; the diagnostic must
   remain `blocked` / `validated:false`. Save through the normal integration
   flow.
7. Prove enablement without printing the environment file or matched value:

   ```bash
   sshpass -e ssh root@<office-panel-ip> '
     count=$(grep -c "^SCENE_BRIDGE_ENABLED=" /etc/brilliant-mqtt.env || true)
     if [ "$count" -eq 1 ] &&
        grep -qx "SCENE_BRIDGE_ENABLED=1" /etc/brilliant-mqtt.env; then
       echo "scene_bridge_enabled=true"
     else
       echo "scene_bridge_enabled=false"
       exit 1
     fi
   '
   ```

8. Confirm the manager's retirement Repair is clear only after verified mirror
   uninstall. Never start the old service to test it.

The settings are HA-global, but the hardware rollout remains Office-only:
redeploy/reconfigure only Office. Other panels must not receive a payload restart
during this gate.

After enablement, repeat Phase 1.1, the key-name/direct-HA/old-mirror checks in
Phase 1.2, and Phases 1.3–1.5. Do **not** rerun the disabled proof while enabled;
the Phase 2 `scene_bridge_enabled=true` proof is authoritative. Expected:

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
3. Run the exact disabled proof from Phase 1.2 and require
   `scene_bridge_disabled=true`.
4. Confirm scene/mode subscriptions and service command availability are gone;
   retained catalogs/status may remain as broker history and must not be mistaken
   for a live subscription.
5. Confirm `brilliant-mqtt` forward bridging remains active.
6. Confirm `brilliant-ha-mirror` remains inactive/absent and no direct HA key is
   present.
7. Repeat the scoped own-device/peer snapshot and prove that no Brilliant
   peripheral was deleted or created by disable.
8. Restore every affected load to its recorded baseline.
9. Repeat physical control and regression checks.

Do not remove the whole forward agent as scene rollback. Do not enable the old
mirror. If rollback cannot restore physical/load/bus baselines, stop and mark
the pilot failed.

After rollback validation, the operator may re-enable the accepted safe scene
bridge through the same normal Office-only flow. Record that as a separate
action and recheck service/peer state.

## Phase 5: legacy cleanup decision

Only after scene acceptance is complete, run a cleanup dry run on Office:

```bash
PYTHONPATH=/var/brilliant-mqtt/app:/var/brilliant-mqtt/vendor \
  /data/switch-embedded/env/bin/python3 \
  -m brilliant_mqtt.cleanup_legacy_mirror
```

Review its JSON only on-screen in an unrecorded terminal. Never redirect, save,
copy, or paste that JSON; it includes the owning ID and candidate identity.
Persist only the sanitized candidate count, outcome, exit status, and operator
decision. If there are no candidates, record that and stop. If candidates
exist, review every ID/name/type against the strict dual allowlist in the
[retirement guide](../../ha-mirror.md). Apply is never automatic and is not part
of enable/rollback.

A fresh operator decision is required immediately before deletion. If approved:

```bash
test "$(id -u)" -eq 0
install -d -m 0700 /data/brilliant-mqtt/cleanup
PYTHONPATH=/var/brilliant-mqtt/app:/var/brilliant-mqtt/vendor \
  /data/switch-embedded/env/bin/python3 \
  -m brilliant_mqtt.cleanup_legacy_mirror \
  --apply \
  --snapshot /data/brilliant-mqtt/cleanup/legacy-mirror-<timestamp>.json
test "$(stat -c %a /data/brilliant-mqtt/cleanup/legacy-mirror-<timestamp>.json)" = 600
sha256sum /data/brilliant-mqtt/cleanup/legacy-mirror-<timestamp>.json
```

Require exit zero, `success:true`, empty `remaining_ids`, fresh second-snapshot
proof, clean native-client shutdown, and the mode-0600 private atomic report.
Review command JSON only on-screen and unrecorded. Record only the report
SHA-256, sanitized counts/outcome, exit status, and operator decision; never
copy the report or command JSON into artifacts. On any failure stop; do not
rerun or recreate a host.

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

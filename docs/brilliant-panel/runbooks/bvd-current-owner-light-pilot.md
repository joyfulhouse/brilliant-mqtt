# BVD current-owner single-light pilot: live test and rollback

This runbook is for the Office panel at `10.100.0.10`, device
`017ff60733f100038e04fa0fbab29096`, and only the HA entity
`light.backyard_light_group`. It is pinned to Brilliant firmware
`v26.06.03.1` and OSTree commit
`2174d3882504c03bf9c7b3f78f0cad4a5ae039f7a32f0bb9c5eca02dc0370b0b`,
with embedded-stack revision
`2332ced103d755d48d2302b592f95f8e7b6c66f5`. These identifiers are the
acquired baseline in the [panel evidence index](../README.md) and
[acquisition record](../acquisition.md#integrity-record). A different
`/data/switch-embedded/.version`, active OSTree commit, OTA, or unaccounted
firmware hash is a hard NO-GO pending a new static and read-only review.

The code is implemented, but live apply is **conditionally NO-GO**. Do not run
`--apply` until all of these operational gates are satisfied:

1. The exact `--cleanup-only` bundle is staged and import-checked on every panel
   that could become BVD owner, and the orchestrator can identify and reach the
   current owner after an Office failure. BVD ownership can drift, while a
   registered peripheral persists after its creating process exits
   ([Control path investigation](../../claude/research/2026-07-06-mirror-poc/FINDINGS.md#control-path-investigation-render-works-raw-injection-control-does-not)).
2. The operator names a reversible stock BVD/device-group canary and approves
   one check before binding. The BVD `request_dispatcher` participates in
   home-wide group intensity, so ONLINE records alone are not a functional
   safety proof
   ([True slider mirroring](../../claude/research/2026-07-06-mirror-poc/FINDINGS.md#true-slider-mirroring--only-via-brilliant_virtual_devices-lease-hazardous)).
3. A reviewed executable observer and broker-side listener can continuously
   evaluate the process, cloud-peer, and measurable MQTT latency aborts below,
   with named operators for physical-load and UI behavior.
4. Every candidate panel matches the pinned firmware evidence; on Office the
   retired `brilliant-ha-mirror` unit/process is inactive or absent before and
   after the pilot.

If the firmware/evidence gate fails, do not run the staged bundle at all. If
any other gate is unavailable, run at most the read-only dry run. Do not treat
`--cross-owner-cleanup-staged` as proof; it records the orchestrator's prior
work and intentionally cannot discover SSH reachability itself. Likewise,
`--stock-canary-approved` records an operator decision; it does not run or
observe the stock canary, and `--external-observer-approved` records that the
required non-writing observer is armed; it does not supply that observer.

## Fixed scope

- BVD device: `brilliant_virtual_device` (DeviceType 3).
- Configuration: `brilliant_virtual_device_configuration`.
- Stable HA ID: `026ea406-dd9b-5dfc-8851-65e6f5dfee14`.
- Native peripheral ID:
  `ha_bvd_026ea406dd9b5dfc885165e6f5dfee14`.
- MQTT state/command routes:
  `brilliant/ha-control/v1/state/026ea406-dd9b-5dfc-8851-65e6f5dfee14`
  and
  `brilliant/ha-control/v1/command/026ea406-dd9b-5dfc-8851-65e6f5dfee14`.
- One type-27 LIGHT, one physical Office slider, 60–120 seconds from
  `host.start()`, then deletion and two absence reads 30 seconds apart.
- No LeaseManager call, BVD owner write, HA token, slider bus write, Control
  device host, named-vdev bootstrap, or Virtual Control provisioning.
- Lease release is always reported `not_applicable`: this pilot acquires no BVD
  lease and writes no owner variable.

The hard-coded identities, read-only owner boundary, controller, and lifecycle
are in
[`single_light_pilot.py`](../../../tools/brilliant_bvd/single_light_pilot.py).
The physical-bus, host, MQTT, cleanup-only, and CLI adapters are in
[`live.py`](../../../tools/brilliant_bvd/live.py).

## Phase 0 — build and review gate

From the exact workstation checkout intended for staging, require a committed,
reviewed tree. Run the root gate with a writable uv cache if the default cache
is sandboxed. `REVIEWED_COMMIT` must be the full commit approved by the code
reviewer, not a branch name:

```bash
set -euo pipefail
: "${REVIEWED_COMMIT:?export the full reviewed commit}"
command -v git uv sha256sum sshpass jq >/dev/null
test "$(git rev-parse HEAD^{commit})" = "$REVIEWED_COMMIT"
test -z "$(git status --porcelain)"
git status --short --branch
git show -s --format='%H %cI' "$REVIEWED_COMMIT"

UV_CACHE_DIR=/private/tmp/brilliant-mqtt-uv-cache uv run ruff check
UV_CACHE_DIR=/private/tmp/brilliant-mqtt-uv-cache uv run ruff format --check
UV_CACHE_DIR=/private/tmp/brilliant-mqtt-uv-cache uv run mypy --strict src tests tools
UV_CACHE_DIR=/private/tmp/brilliant-mqtt-uv-cache uv run pytest

git diff --check
git status --short
```

Stop if the reviewed commit differs from the checkout, any gate fails, or an
unreviewed file would be staged. Record the commit and test count.

## Phase 1 — stage without starting

Run this phase on the workstation. Export an exhaustive, review-approved list
of every panel that can win the BVD election; the list must include Office.
Each release is created from committed Git bytes, not the mutable worktree, and
the command refuses to reuse a release directory. A failed partial stage is
quarantined for investigation; never fill it in or overwrite it. Do not replace
the installed Brilliant MQTT service or restart any panel service. Releases,
recovery code, and private evidence live under root-only `/var`, which survives
OSTree deployment changes; only the lock lives under volatile `/run`
([persistence map](../var-persistence.md#why-var-matters)).

```bash
set -euo pipefail
: "${REVIEWED_COMMIT:?export the full reviewed commit}"
: "${BVD_CANDIDATE_PANEL_IPS:?export the exhaustive space-separated panel IP list}"
: "${SSHPASS:?export the approved temporary SSH password for sshpass -e}"
test "$(git rev-parse "$REVIEWED_COMMIT^{commit}")" = "$REVIEWED_COMMIT"
test -z "$(git status --porcelain)"
read -r -a PANEL_IPS <<<"$BVD_CANDIDATE_PANEL_IPS"
test "${#PANEL_IPS[@]}" -gt 0
printf '%s\n' "${PANEL_IPS[@]}" | grep -Fx '10.100.0.10' >/dev/null

BUNDLE=$(mktemp /private/tmp/brilliant-bvd-pilot.XXXXXX)
trap 'rm -f "$BUNDLE"' EXIT
git archive --format=tar "$REVIEWED_COMMIT" -- \
  src/brilliant_mqtt \
  custom_components/brilliant_mqtt/agent_payload/vendor \
  tools/__init__.py \
  tools/brilliant_bvd \
  tools/brilliant_vc/__init__.py \
  tools/brilliant_vc/slider_binding.py >"$BUNDLE"
export BUNDLE_SHA
BUNDLE_SHA=$(sha256sum "$BUNDLE" | awk '{print $1}')
test -n "$BUNDLE_SHA"

for PANEL in "${PANEL_IPS[@]}"; do
  RELEASE="/var/brilliant-bvd-pilot/releases/$REVIEWED_COMMIT"
  sshpass -e ssh root@"$PANEL" "
    set -euo pipefail
    umask 077
    grep -Fx '2332ced103d755d48d2302b592f95f8e7b6c66f5' \
      /data/switch-embedded/.version >/dev/null
    ostree admin status --json | \
      /data/switch-embedded/env/bin/python3 -c \
      'import json,sys; d=json.load(sys.stdin)[\"deployments\"]; b=[x for x in d if x.get(\"booted\")]; s=[x for x in d if x.get(\"staged\")]; ok=len(b)==1 and not s and b[0].get(\"checksum\")==\"2174d3882504c03bf9c7b3f78f0cad4a5ae039f7a32f0bb9c5eca02dc0370b0b\"; raise SystemExit(0 if ok else \"unexpected active or staged OSTree deployment\")'
    test ! -e '$RELEASE'
    install -d -o 0 -g 0 -m 0700 /var/brilliant-bvd-pilot
    install -d -o 0 -g 0 -m 0700 \
      /var/brilliant-bvd-pilot/releases \
      /var/brilliant-bvd-pilot/runs \
      /run/brilliant-bvd-pilot
    install -d -o 0 -g 0 -m 0700 '$RELEASE' '$RELEASE/app'
    cat >'$RELEASE/source.tar'
    printf '%s  %s\n' '$BUNDLE_SHA' '$RELEASE/source.tar' | sha256sum -c -
    tar -xf '$RELEASE/source.tar' -C '$RELEASE/app'
    printf '%s\n' '$REVIEWED_COMMIT' >'$RELEASE/COMMIT'
    chmod -R a-w '$RELEASE'
    cd '$RELEASE/app'
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH='$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded' \
      /data/switch-embedded/env/bin/python3 -m tools.brilliant_bvd.live --help \
      >/dev/null
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH='$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded' \
      /data/switch-embedded/env/bin/python3 -c \
      'import aiomqtt,brilliant_mqtt.bus as b,pathlib,paho.mqtt.client as p,tools.brilliant_bvd.live as l; r=pathlib.Path(\"$RELEASE/app\").resolve(); modules=(aiomqtt,b,p,l); ok=all(getattr(m,\"__file__\",None) and pathlib.Path(m.__file__).resolve().is_relative_to(r) for m in modules); raise SystemExit(0 if ok else \"module escaped reviewed release\")'
    test \"\$(stat -c %u:%a /var/brilliant-bvd-pilot)\" = 0:700
    test \"\$(stat -c %u:%a /run/brilliant-bvd-pilot)\" = 0:700
  " <"$BUNDLE"
done
```

The successful archive checksum check proves that every panel extracted the
same committed bytes. Recheck the now read-only release and its import without
writing into it:

```bash
set -euo pipefail
: "${REVIEWED_COMMIT:?export the full reviewed commit}"
: "${BUNDLE_SHA:?export the recorded Git archive SHA-256}"
: "${BVD_CANDIDATE_PANEL_IPS:?export the exhaustive space-separated panel IP list}"
: "${SSHPASS:?export the approved temporary SSH password for sshpass -e}"
read -r -a PANEL_IPS <<<"$BVD_CANDIDATE_PANEL_IPS"
for PANEL in "${PANEL_IPS[@]}"; do
  RELEASE="/var/brilliant-bvd-pilot/releases/$REVIEWED_COMMIT"
  sshpass -e ssh root@"$PANEL" "
    set -euo pipefail
    test \"\$(cat '$RELEASE/COMMIT')\" = '$REVIEWED_COMMIT'
    sha256sum '$RELEASE/source.tar' | \
      grep -Fx '$BUNDLE_SHA  $RELEASE/source.tar' >/dev/null
    test -z \"\$(find '$RELEASE' -perm /0222 -print -quit)\"
    cd '$RELEASE/app'
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH='$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded' \
      /data/switch-embedded/env/bin/python3 -m tools.brilliant_bvd.live --help \
      >/dev/null
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH='$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded' \
      /data/switch-embedded/env/bin/python3 -c \
      'import aiomqtt,brilliant_mqtt.bus as b,pathlib,paho.mqtt.client as p,tools.brilliant_bvd.live as l; r=pathlib.Path(\"$RELEASE/app\").resolve(); modules=(aiomqtt,b,p,l); ok=all(getattr(m,\"__file__\",None) and pathlib.Path(m.__file__).resolve().is_relative_to(r) for m in modules); raise SystemExit(0 if ok else \"module escaped reviewed release\")'
  "
done
```

Retain `REVIEWED_COMMIT`, `BUNDLE_SHA`, and the exact candidate list in the
private session record. Before apply, create one private run directory on
Office and put the MQTT password there through the approved sealed-secret
workflow. The password must be a regular root-owned mode-0400 or mode-0600 file.
Never put an HA token on a panel; the pilot needs only the existing MQTT control
plane.

```bash
set -euo pipefail
export COMMIT='<full-reviewed-commit>'
export RUN_ID='<single-approved-UTC-run-id>'
[[ "$COMMIT" =~ ^[0-9a-f]{40}$ ]]
[[ "$RUN_ID" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
export RELEASE="/var/brilliant-bvd-pilot/releases/$COMMIT"
export RUN="/var/brilliant-bvd-pilot/runs/$RUN_ID"
umask 077
test ! -e "$RUN"
install -d -m 0700 "$RUN" "$RUN/evidence"
test "$(stat -c %u:%a "$RUN")" = '0:700'
```

The already import-checked entry point and exact recovery command below must be
available in the orchestrator before apply; do not execute cleanup merely as a
check. Repeat the exports with the same literal commit in any new terminal. The
command is valid only on the panel whose own device ID currently equals the BVD
owner:

```bash
set -euo pipefail
export COMMIT='<full-reviewed-commit>'
[[ "$COMMIT" =~ ^[0-9a-f]{40}$ ]]
export RELEASE="/var/brilliant-bvd-pilot/releases/$COMMIT"
if ! test -e /run/brilliant-bvd-pilot; then
  install -d -o 0 -g 0 -m 0700 /run/brilliant-bvd-pilot
fi
test "$(stat -c %u:%a /run/brilliant-bvd-pilot)" = '0:700'
cd "$RELEASE/app"
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH="$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded" \
  /data/switch-embedded/env/bin/python3 -m tools.brilliant_bvd.live \
  --cleanup-only
```

It deletes only the fixed pilot ID with a wall-clock `deletion_time_ms`, closes
that peer, then proves absence through two fresh peers 30 seconds apart. It
refuses to run on a panel that is not the current BVD owner.

### 1.1 Exact read-only fleet owner discovery

Run this from the workstation before admission and repeat it after any owner
drift or failed cleanup. `--owner-status` performs no write and emits one JSON
object. Every approved candidate must answer; all must report the same
configuration owner; exactly one reachable candidate must identify itself as
that owner. Keep these files private because they contain native device IDs.

```bash
set -euo pipefail
: "${REVIEWED_COMMIT:?export the full reviewed commit}"
: "${BVD_CANDIDATE_PANEL_IPS:?export the exhaustive space-separated panel IP list}"
: "${SSHPASS:?export the approved temporary SSH password for sshpass -e}"
read -r -a PANEL_IPS <<<"$BVD_CANDIDATE_PANEL_IPS"
OWNER_EVIDENCE=$(mktemp -d /private/tmp/brilliant-bvd-owner.XXXXXX)
chmod 0700 "$OWNER_EVIDENCE"
OWNER_COUNT=0
CURRENT_OWNER_IP=''
CONFIGURATION_OWNER=''

for PANEL in "${PANEL_IPS[@]}"; do
  RELEASE="/var/brilliant-bvd-pilot/releases/$REVIEWED_COMMIT"
  STATUS=$(sshpass -e ssh root@"$PANEL" "
    set -euo pipefail
    cd '$RELEASE/app'
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH='$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded' \
      /data/switch-embedded/env/bin/python3 -m tools.brilliant_bvd.live \
      --owner-status
  ")
  printf '%s\n' "$STATUS" >"$OWNER_EVIDENCE/$PANEL.json"
  test "$(jq -r '.event' "$OWNER_EVIDENCE/$PANEL.json")" = OWNER_STATUS
  jq -e '.panel_device_id | type == "string" and length > 0' \
    "$OWNER_EVIDENCE/$PANEL.json" >/dev/null
  jq -e '.configuration_owner | type == "string" and length > 0' \
    "$OWNER_EVIDENCE/$PANEL.json" >/dev/null
  jq -e '.current_owner == true or .current_owner == false' \
    "$OWNER_EVIDENCE/$PANEL.json" >/dev/null

  THIS_OWNER=$(jq -r '.configuration_owner' "$OWNER_EVIDENCE/$PANEL.json")
  if test -z "$CONFIGURATION_OWNER"; then
    CONFIGURATION_OWNER=$THIS_OWNER
  else
    test "$THIS_OWNER" = "$CONFIGURATION_OWNER"
  fi
  if jq -e '.current_owner == true' "$OWNER_EVIDENCE/$PANEL.json" >/dev/null; then
    test "$(jq -r '.panel_device_id' "$OWNER_EVIDENCE/$PANEL.json")" = \
      "$CONFIGURATION_OWNER"
    OWNER_COUNT=$((OWNER_COUNT + 1))
    CURRENT_OWNER_IP=$PANEL
  fi
done

test "$OWNER_COUNT" -eq 1
test -n "$CURRENT_OWNER_IP"
export CURRENT_OWNER_IP
printf 'current_owner_reachable=true\ncurrent_owner_ip=%s\n' "$CURRENT_OWNER_IP"
```

This is a point-in-time observation, not a lock. Run it immediately before a
cross-owner cleanup, then let `--cleanup-only` re-read and enforce ownership.
For apply, require `CURRENT_OWNER_IP=10.100.0.10`; never write the owner to make
that condition true.

## Phase 2 — private baselines and hard NO-GO checks

Run the panel commands in a root shell on Office. In every new Office terminal,
repeat these exports with the same literal values; do not assume variables from
another SSH session. The run directory must already exist from Phase 1.

```bash
set -euo pipefail
export COMMIT='<full-reviewed-commit>'
export RUN_ID='<single-approved-UTC-run-id>'
[[ "$COMMIT" =~ ^[0-9a-f]{40}$ ]]
[[ "$RUN_ID" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
export RELEASE="/var/brilliant-bvd-pilot/releases/$COMMIT"
export RUN="/var/brilliant-bvd-pilot/runs/$RUN_ID"
test "$(cat "$RELEASE/COMMIT")" = "$COMMIT"
test "$(stat -c %u:%a "$RUN")" = '0:700'
cd "$RELEASE/app"
```

### 2.1 Slider binding

Choose one Office slider and record its numeric index. Capture all slider
bindings and guard values with the existing read-only tool:

```bash
set -euo pipefail
: "${RELEASE:?repeat the Phase 2 exports in this terminal}"
: "${RUN:?repeat the Phase 2 exports in this terminal}"
export SLIDER_INDEX='<approved-numeric-index>'
[[ "$SLIDER_INDEX" =~ ^(0|[1-9][0-9]*)$ ]]
cd "$RELEASE/app"
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH="$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded" \
  /data/switch-embedded/env/bin/python3 -m tools.brilliant_vc.slider_binding \
  --safe-root "$RUN/evidence" \
  capture \
  --selected-slider-index "$SLIDER_INDEX" \
  --output "$RUN/evidence/office-slider-before.json"
```

Record only the emitted SHA-256 in public notes. The evidence file is private
because it contains native device/peripheral IDs. Binding and restoration are
operator actions in the native UI; never write `slider_config:*` on the bus.
Before proceeding, prove privately that no slider already points at the exact
pilot target. Emit only the boolean result:

```bash
set -euo pipefail
: "${RELEASE:?repeat the Phase 2 exports in this terminal}"
: "${RUN:?repeat the Phase 2 exports in this terminal}"
cd "$RELEASE/app"
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH="$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded" \
  /data/switch-embedded/env/bin/python3 - \
  "$RUN/evidence" \
  "$RUN/evidence/office-slider-before.json" <<'PY'
import json
import sys
from pathlib import Path

from tools.brilliant_vc.slider_binding import read_private_snapshot

safe_root, before_path = map(Path, sys.argv[1:])
before = read_private_snapshot(before_path, safe_root=safe_root)
pilot_absent = all(
    not (
        record.binding.device_id == "brilliant_virtual_device"
        and record.binding.peripheral_id == "ha_bvd_026ea406dd9b5dfc885165e6f5dfee14"
    )
    for record in before.slider_configs
)
print(json.dumps({"pilot_absent_from_all_sliders": pilot_absent}, sort_keys=True))
if not pilot_absent:
    raise SystemExit(1)
PY
```

### 2.2 Panel, bus, BVD stock host, and cloud peer

Before opening a pilot peer, record these allowlisted facts on Office. This is
also the explicit preflight retirement gate: the deprecated physical-Control
host must be inactive or absent as required by the
[HA mirror retirement guide](../../ha-mirror.md#inspect-service-retirement).

```bash
set -euo pipefail
date -u +%Y-%m-%dT%H:%M:%SZ
grep -Fx '2332ced103d755d48d2302b592f95f8e7b6c66f5' \
  /data/switch-embedded/.version >/dev/null
ostree admin status --json | \
  /data/switch-embedded/env/bin/python3 -c \
  'import json,sys; d=json.load(sys.stdin)["deployments"]; b=[x for x in d if x.get("booted")]; s=[x for x in d if x.get("staged")]; ok=len(b)==1 and not s and b[0].get("checksum")=="2174d3882504c03bf9c7b3f78f0cad4a5ae039f7a32f0bb9c5eca02dc0370b0b"; raise SystemExit(0 if ok else "unexpected active or staged OSTree deployment")'
systemctl is-active message_bus switch_ui_app brilliant-mqtt
MIRROR_STATE=$(systemctl is-active brilliant-ha-mirror 2>/dev/null || true)
case "$MIRROR_STATE" in
  inactive|unknown|'') ;;
  *) exit 1 ;;
esac
! pgrep -f '[b]rilliant[_-]ha[_-]mirror' >/dev/null
systemctl show message_bus -p MainPID -p ActiveEnterTimestampMonotonic
STOCK_PID=$(pgrep -f '[b]rilliant_virtual_device_peripherals')
test "$(printf '%s\n' "$STOCK_PID" | wc -l)" -eq 1
ps -o pid=,lstart=,args= -p "$STOCK_PID"
cat /proc/loadavg
free -m
grep -c /var/run/brilliant/server_socket /proc/net/unix
```

Record the exact `message_bus` PID/start identity, the one stock
`brilliant_virtual_device_peripherals` PID/start identity, socket-row proxy,
load, and memory. Run ten ordinary physical load interactions and require no
subjective lag.

Live apply additionally requires a separately reviewed, executable observer;
this repository does not supply one for the external process, cloud-peer,
physical-load, or end-to-end latency gates. Its exact command and digest must
be approved before the session, dry-run without writes, and recorded privately.
It must continuously timestamp and evaluate the baseline `message_bus` and
stock-vassal identities, the firmware-specific cloud-peer CONNECTED state and
disconnect counter, peer timeout/rejection and UI restart evidence, the pilot's
CPU/RSS, and MQTT command/result/state timing. It must not open another bus
peer, restart a service, or send SIGKILL; its only permitted mutation is one
`SIGTERM` to the PID in `$RUN/pilot.pid` on a hard threshold. An operator must
separately watch real-load responsiveness, the stock canary, slider routing,
and snap-back, which cannot be inferred from process logs.

If that approved executable observer, its cloud-peer surface, a broker-side
timestamped listener, or the named human observers are unavailable, live apply
is NO-GO. Ad hoc `watch`, subjective log absence, and the pilot's internal
topology loop are not substitutes. Record a journal cursor, but do not claim
that absence of a disconnect line proves connection.

Do not run `tools.brilliant_vc.monitor` unchanged: it escalates to SIGKILL after
ten seconds, shorter than this pilot's deliberate absence-proof interval
([monitor.py](../../../tools/brilliant_vc/monitor.py#L286)).

### 2.3 HA authority

In HA's MQTT listener, inspect these retained topics without copying broker
credentials to the panel shell:

```text
brilliant/ha-control/v1/manifest
brilliant/ha-control/v1/state/026ea406-dd9b-5dfc-8851-65e6f5dfee14
```

Require the manifest entry to contain exactly the fixed stable/entity IDs,
`domain=light`, and `turn_on`, `turn_off`, `set_brightness`. Require the state
publication to be retained and available with a nonnegative sequence and
brightness 0–255. Apply refuses to register until both retained authorities
arrive. Any later manifest mismatch/removal, state unavailability, MQTT stream
loss, or sequence/epoch conflict fences commands and aborts into cleanup. The
HA executor rejects commands absent from its committed manifest
([ha_control.py](../../../custom_components/brilliant_mqtt/ha_control.py#L531)).

### 2.4 Read-only BVD admission

Supply an opaque room ID already decoded from the scoped Brilliant room catalog;
do not guess a display name. On Office, run exactly once:

```bash
set -euo pipefail
: "${RELEASE:?repeat the Phase 2 exports in this terminal}"
export ROOM_ASSIGNMENT_ID='<opaque-Brilliant-room-id>'
test -n "$ROOM_ASSIGNMENT_ID"
cd "$RELEASE/app"
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH="$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded" \
  /data/switch-embedded/env/bin/python3 -m tools.brilliant_bvd.live \
  --room-assignment-id "$ROOM_ASSIGNMENT_ID" \
  --display-name 'HA Backyard Light Group Pilot' \
  --active-runtime-s 120
```

Expected output is `DRY_RUN_OK`. It proves, at that instant, exact Office bus
identity, natural Office BVD ownership no older than 30 seconds, DeviceType 3,
one current stock-vassal identity, five exact process configs, six exact ONLINE
stock peripherals, Office relay, room existence, and pilot absence. Compare the
process identity by immediately repeating the Phase 2 process check; the dry-run
JSON intentionally does not expose it. These are new-peer scoped observations,
not a lease promise about the next step. A
refusal because Office is not owner is an expected NO-GO; never bid, refresh,
clear, or restore the owner to make the test schedulable.

Finally, name the approved stock BVD/device-group canary, its reversible
baseline, and the operator who will exercise it after READY. Exercise and
restore it once now, before apply, and record the approved observer's result;
`--stock-canary-approved` attests that this happened. If no harmless functional
canary exists or the baseline check is not clean, stop.

## Phase 3 — bounded apply and operator gestures

Open four terminals: pilot, the approved health observer, the timestamped MQTT
listener, and an operator abort shell. Repeat the Phase 2 exports in every
Office shell. Arm the approved external observer and broker listener first and
prove that both are recording. The health observer must not auto-SIGKILL the
pilot. Its only automated action may be one `SIGTERM`; allow the full cleanup
reserve of 180 seconds to finish. If any observer is only proposed rather than
executable and approved, stop here.

Start the pilot on Office. The hard active timer starts immediately before the
native host starts, not at READY:

```bash
set -euo pipefail
: "${RELEASE:?repeat the Phase 2 exports in this terminal}"
: "${RUN:?repeat the Phase 2 exports in this terminal}"
: "${ROOM_ASSIGNMENT_ID:?export the approved opaque room ID}"
: "${MQTT_HOST:?export the approved broker LAN host}"
: "${MQTT_USERNAME:?export the pilot MQTT user}"
umask 077
test "$(stat -c %u:%a "$RUN/mqtt-password")" = '0:600' || \
  test "$(stat -c %u:%a "$RUN/mqtt-password")" = '0:400'
cd "$RELEASE/app"
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH="$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded" \
  nohup \
  /data/switch-embedded/env/bin/python3 -m tools.brilliant_bvd.live \
  --apply \
  --cross-owner-cleanup-staged \
  --stock-canary-approved \
  --external-observer-approved \
  --room-assignment-id "$ROOM_ASSIGNMENT_ID" \
  --display-name 'HA Backyard Light Group Pilot' \
  --active-runtime-s 120 \
  --mqtt-host "$MQTT_HOST" \
  --mqtt-port 1883 \
  --mqtt-username "$MQTT_USERNAME" \
  --mqtt-password-file "$RUN/mqtt-password" \
  >"$RUN/evidence/pilot.jsonl" \
  2>"$RUN/evidence/pilot.stderr" \
  </dev/null &
PILOT_PID=$!
printf '%s\n' "$PILOT_PID" >"$RUN/pilot.pid"
test "$(cat "$RUN/pilot.pid")" = "$PILOT_PID"
kill -0 "$PILOT_PID"
```

The `nohup` inheritance protects the short interval before Python installs its
own `SIGHUP` handler; after installation, `SIGHUP`, `SIGINT`, and `SIGTERM` all
request the same bounded cleanup. In the pilot terminal, tail the record and
stop the tail with Ctrl-C only after one `READY` event. The explicit status
guard keeps interactive `errexit` from exiting the shell and hanging up the
background pilot:

```bash
set -euo pipefail
: "${RUN:?repeat the Phase 2 exports in this terminal}"
tail -n +1 -F "$RUN/evidence/pilot.jsonl" || true
grep -F '"event": "READY"' "$RUN/evidence/pilot.jsonl" >/dev/null
kill -0 "$(cat "$RUN/pilot.pid")"
rm -f -- "$RUN/mqtt-password"
test ! -e "$RUN/mqtt-password"
```

The MQTT client has already loaded its dedicated credential before `READY`;
unlinking the file does not alter that bounded session. If the process exits
before `READY`, wait for cleanup to finish and then unlink the file instead.
After normal or emergency teardown, revoke or rotate the dedicated broker user
off-panel. Recovery and cleanup-only never require MQTT credentials.

Do nothing to the UI until `READY`. It is emitted only after initial retained
manifest and available-state authority, a fresh second owner/topology preflight
immediately before mutation, native registration initialized from that
authoritative state, a second internal state replay, and a post-notification
ACTIVE probe from the subscribed mirror showing the six stock peripherals plus
exactly one pilot with the unchanged stock-host identity. The apply flags are
assertions only; READY does not prove
the external canary, cloud peer, or physical loads.

Immediately after READY:

1. The operator exercises the named stock BVD/device-group canary once and
   restores it. Any lag, wrong target, or request timeout means `SIGTERM` now.
2. The operator opens the Office slider picker, selects only
   `HA Backyard Light Group Pilot`, and saves. Confirm the decoded selected
   binding is BVD plus the fixed pilot ID and no other slider/guard value
   changed.
3. Move the slider to one nontrivial settled brightness. Observe a non-retained
   `set_brightness` command with the current state sequence, a matching
   non-retained accepted result, a strictly advancing retained HA state, HA
   brightness convergence, and the native slider settling without snap-back.
4. Perform one off action, wait for the confirmed state, then one on action.
   Require `turn_off`/`turn_on`, accepted results, advancing state, HA behavior,
   and native convergence. The controller allows one command in flight per HA
   sequence and coalesces a moving-slider burst to its latest value.
5. Between every gesture, operate one ordinary real load. Stop on any lag.

Before continuing from step 2 to step 3, take a second private snapshot and
prove locally that only the selected slider changed, both guard values and all
other slider bytes stayed identical, and the selected target decodes to the
fixed BVD/pilot pair. The final JSON contains booleans only; do not copy either
private snapshot off-panel.

```bash
set -euo pipefail
: "${RELEASE:?repeat the Phase 2 exports in this terminal}"
: "${RUN:?repeat the Phase 2 exports in this terminal}"
: "${SLIDER_INDEX:?export the same approved slider index}"
cd "$RELEASE/app"
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH="$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded" \
  /data/switch-embedded/env/bin/python3 -m tools.brilliant_vc.slider_binding \
  --safe-root "$RUN/evidence" \
  capture \
  --selected-slider-index "$SLIDER_INDEX" \
  --output "$RUN/evidence/office-slider-bound.json"

PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH="$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded" \
  /data/switch-embedded/env/bin/python3 - \
  "$RUN/evidence" \
  "$RUN/evidence/office-slider-before.json" \
  "$RUN/evidence/office-slider-bound.json" <<'PY'
import json
import sys
from pathlib import Path

from tools.brilliant_vc.slider_binding import read_private_snapshot

safe_root, before_path, bound_path = map(Path, sys.argv[1:])
before = read_private_snapshot(before_path, safe_root=safe_root)
bound = read_private_snapshot(bound_path, safe_root=safe_root)
selected_name = f"slider_config:{before.selected_slider_index}"
before_values = {record.variable_name: record.encoded_value for record in before.slider_configs}
bound_values = {record.variable_name: record.encoded_value for record in bound.slider_configs}
selected = next(record for record in bound.slider_configs if record.variable_name == selected_name)
checks = {
    "owner_unchanged": before.owning_device_id == bound.owning_device_id,
    "guards_unchanged": before.guard_values == bound.guard_values,
    "slider_set_unchanged": set(before_values) == set(bound_values),
    "selected_slider_changed": before_values[selected_name] != bound_values[selected_name],
    "unselected_sliders_unchanged": all(
        before_values[name] == bound_values[name]
        for name in before_values
        if name != selected_name
    ),
    "selected_device_is_bvd": selected.binding.device_id == "brilliant_virtual_device",
    "selected_peripheral_is_pilot": (
        selected.binding.peripheral_id == "ha_bvd_026ea406dd9b5dfc885165e6f5dfee14"
    ),
}
if not all(checks.values()):
    raise SystemExit(1)
print(json.dumps(checks, sort_keys=True))
PY
```

For each settled gesture, the approved timestamped observer must measure a
command-to-result interval of at most 1 second and command-to-confirmed-HA-state
interval of at most 1.5 seconds. Claim slider-to-command within 500 ms or
state-to-native convergence within 2 seconds only if the approved observer has
an instrumented timestamp for the physical gesture/native value; a person
watching a screen cannot establish those numeric bounds. The operator must
still require prompt visible convergence and no snap-back. A result is
diagnostic, not authority. Firmware may provisionally expose the pushed value;
confirmed HA state must promptly overwrite it and remain visibly settled. A
rejection or the pilot's 15-second confirmation timeout restores its cached HA
values before bounded abort and deletion.

### Hard-abort triggers

Send exactly one `SIGTERM` to the recorded PID immediately on any of these:

- real-load or stock-canary lag/wrong behavior;
- slider snap-back, wrong routing, rejected/stale command, missing convergence,
  or duplicate/unrelated command;
- `message_bus` PID/start change, reconnect, peer add timeout/rejection, or
  subscribed BVD notification stream 30 seconds stale, or socket-row count
  outside the observer's pre-approved envelope;
- stock BVD-vassal PID/start change, owner/relay change, or a stock peripheral
  missing/offline;
- approved cloud-peer state leaves CONNECTED or its disconnect counter rises;
- MQTT disconnect, manifest route removal/change, unavailable HA state, or
  command confirmation timeout;
- the approved external observer measures pilot RSS above 100 MiB or CPU above
  15% for five consecutive samples; or
- any inability to observe the safety probes.

```bash
set -euo pipefail
: "${RUN:?repeat the Phase 2 exports in this terminal}"
PILOT_PID=$(cat "$RUN/pilot.pid")
kill -0 "$PILOT_PID"
kill -TERM "$PILOT_PID"
```

Never restart `message_bus`, start the retired HA mirror, write the BVD owner,
or send SIGKILL as first response.

## Phase 4 — normal teardown

Normal teardown differs from an emergency abort. While the pilot LIGHT still
exists, the operator first restores the original slider target through the
native UI. Then prove byte-for-byte restoration:

```bash
set -euo pipefail
: "${RELEASE:?repeat the Phase 2 exports in this terminal}"
: "${RUN:?repeat the Phase 2 exports in this terminal}"
cd "$RELEASE/app"
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH="$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded" \
  /data/switch-embedded/env/bin/python3 -m tools.brilliant_vc.slider_binding \
  --safe-root "$RUN/evidence" \
  verify \
  --baseline "$RUN/evidence/office-slider-before.json"
```

Require `restored=true` and ordinary physical behavior. Only then stop the
pilot and wait for its cleanup result:

```bash
set -euo pipefail
: "${RUN:?repeat the Phase 2 exports in the original pilot terminal}"
PILOT_PID=$(cat "$RUN/pilot.pid")
kill -0 "$PILOT_PID"
kill -TERM "$PILOT_PID"
wait "$PILOT_PID"
grep -F '"event": "STOPPED_CLEAN"' "$RUN/evidence/pilot.jsonl" >/dev/null
```

Require exit zero, `STOPPED_CLEAN`, and no cleanup error. Internally the order
is fence, timestamped delete, host shutdown, persistent guard shutdown, fresh
absence read/close, 30 seconds, fresh absence read/close, then a newly opened
subscribed-mirror POST peer validating the exact six-stock topology and the
original owner/relay and stock-host continuity. `STOPPED_CLEAN` is emitted only after
that POST peer closes. No lease release is attempted.

Repeat the fleet `--owner-status` workflow. Success requires Office still be
the natural owner. On Office, rerun the exact Phase 2 dry-run command from a new
peer; this is the separate POST topology observation, and it must validate exactly the
six stock peripherals with no pilot. If Office is no longer owner, if owner
status is inconsistent, or if the dry run refuses, treat the session as a
failed bounded experiment even if exact-target cleanup proved absence. The
current CLI intentionally has no owner-agnostic shortcut around that POST gate.

```bash
set -euo pipefail
: "${RELEASE:?repeat the Phase 2 exports in this terminal}"
: "${ROOM_ASSIGNMENT_ID:?export the same approved opaque room ID}"
cd "$RELEASE/app"
POST=$(PYTHONDONTWRITEBYTECODE=1 \
  PYTHONPATH="$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded" \
  /data/switch-embedded/env/bin/python3 -m tools.brilliant_bvd.live \
  --room-assignment-id "$ROOM_ASSIGNMENT_ID" \
  --display-name 'HA Backyard Light Group Pilot' \
  --active-runtime-s 120)
printf '%s\n' "$POST"
printf '%s\n' "$POST" | grep -F '"event": "DRY_RUN_OK"' >/dev/null
```

Repeat the Phase 2 PID/start, socket-row, approved cloud-peer, journal,
stock-canary, physical-load, HA-authority, and slider-binding checks. Re-run the
explicit firmware and mirror-retirement gate:

```bash
set -euo pipefail
grep -Fx '2332ced103d755d48d2302b592f95f8e7b6c66f5' \
  /data/switch-embedded/.version >/dev/null
ostree admin status --json | \
  /data/switch-embedded/env/bin/python3 -c \
  'import json,sys; d=json.load(sys.stdin)["deployments"]; b=[x for x in d if x.get("booted")]; s=[x for x in d if x.get("staged")]; ok=len(b)==1 and not s and b[0].get("checksum")=="2174d3882504c03bf9c7b3f78f0cad4a5ae039f7a32f0bb9c5eca02dc0370b0b"; raise SystemExit(0 if ok else "unexpected active or staged OSTree deployment")'
systemctl is-active message_bus switch_ui_app brilliant-mqtt
MIRROR_STATE=$(systemctl is-active brilliant-ha-mirror 2>/dev/null || true)
case "$MIRROR_STATE" in
  inactive|unknown|'') ;;
  *) exit 1 ;;
esac
! pgrep -f '[b]rilliant[_-]ha[_-]mirror' >/dev/null
```

The retired mirror remaining inactive/absent is a hard postflight gate, not a
cleanup action ([retirement guide](../../ha-mirror.md#inspect-service-retirement)).
Require:

- exact pilot absent twice and no slider reference;
- the Office BVD owner value (timestamp may advance naturally), six stock
  peripherals, five process configs, and stock-vassal PID/start identity;
- unchanged `message_bus` PID/start and baseline socket-row proxy;
- no new peer timeout/rejection, reconnect, cloud disconnect, or UI restart;
- HA remains authoritative and the retired mirror remains inactive; and
- real loads and the stock canary behave as at baseline.

### 4.1 De-stage only after pristine proof

After recording the sanitized acceptance facts and only after every Phase 4
gate passes, remove the exact run, release, and unlocked pilot lock from all
candidate panels. This unlinks private snapshots; it does not claim secure
erasure of flash. The MQTT password was already removed after startup or
termination. Do not de-stage after a failed run because the same immutable
release is the rollback tool.

Run from the workstation:

```bash
set -euo pipefail
: "${REVIEWED_COMMIT:?export the same full reviewed commit}"
: "${RUN_ID:?export the same UTC run ID}"
: "${BVD_CANDIDATE_PANEL_IPS:?export the exhaustive space-separated panel IP list}"
: "${SSHPASS:?export the approved temporary SSH password for sshpass -e}"
[[ "$REVIEWED_COMMIT" =~ ^[0-9a-f]{40}$ ]]
[[ "$RUN_ID" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
read -r -a PANEL_IPS <<<"$BVD_CANDIDATE_PANEL_IPS"

for PANEL in "${PANEL_IPS[@]}"; do
  RELEASE="/var/brilliant-bvd-pilot/releases/$REVIEWED_COMMIT"
  RUN="/var/brilliant-bvd-pilot/runs/$RUN_ID"
  sshpass -e ssh root@"$PANEL" "
    set -euo pipefail
    ! pgrep -f '[t]ools.brilliant_bvd.live' >/dev/null
    test \"\$(cat '$RELEASE/COMMIT')\" = '$REVIEWED_COMMIT'
    if test -e /run/brilliant-bvd-pilot/single-light.lock; then
      flock -n /run/brilliant-bvd-pilot/single-light.lock true
    fi
    if test '$PANEL' = '10.100.0.10'; then
      test \"\$(stat -c %u:%a '$RUN')\" = '0:700'
      rm -rf -- '$RUN'
      test ! -e '$RUN'
    fi
    chmod -R u+w '$RELEASE'
    rm -rf -- '$RELEASE'
    test ! -e '$RELEASE'
    rm -f -- /run/brilliant-bvd-pilot/single-light.lock
    rmdir /run/brilliant-bvd-pilot 2>/dev/null || true
    rmdir /var/brilliant-bvd-pilot/runs 2>/dev/null || true
    rmdir /var/brilliant-bvd-pilot/releases 2>/dev/null || true
    rmdir /var/brilliant-bvd-pilot 2>/dev/null || true
  "
done
unset SSHPASS
```

Require the exact release and Office run paths absent on every intended panel;
empty common parents may remain only when another reviewed release/run uses
them. This filesystem de-stage follows—and never substitutes for—the bus POST,
slider restoration, service, cloud-peer, and physical checks.

## Phase 5 — emergency rollback and owner drift

On a hard abort, signal first; do not spend time restoring the UI while a
possibly contending host is alive. Give in-process cleanup its 180-second
operational reserve. The worst successful sequential timeout budget is about
160 seconds: up to 60 for fencing/lifecycle/two probes including the fixed
30-second interval, then up to 50 to open+subscribe the POST peer, 30 for its
three reads, and 20 to close its two components. The remaining 20 seconds is
scheduling margin, not a code-enforced aggregate deadline. If it reports clean,
restore the slider in the UI and run the byte verifier.

If cleanup fails, the process dies, ownership changes, or no clean result
arrives after the reserve:

If the recorded PID is still running after 180 seconds, do not start a second
deleter concurrently. The orchestrator—not the automatic observer—may use the
following last-resort process stop only after verifying the PID, module, apply
mode, and immutable-release `PYTHONPATH`. If any check fails, do not signal the
process; escalate with live apply disabled.

```bash
set -euo pipefail
: "${RELEASE:?repeat the Phase 2 exports in this terminal}"
: "${RUN:?repeat the Phase 2 exports in this terminal}"
PILOT_PID=$(cat "$RUN/pilot.pid")
kill -0 "$PILOT_PID"
PILOT_CMD=$(tr '\0' ' ' <"/proc/$PILOT_PID/cmdline")
[[ "$PILOT_CMD" == *'/data/switch-embedded/env/bin/python3'* ]]
[[ "$PILOT_CMD" == *'tools.brilliant_bvd.live'* ]]
[[ "$PILOT_CMD" == *'--apply'* ]]
tr '\0' '\n' <"/proc/$PILOT_PID/environ" | \
  grep -Fx "PYTHONPATH=$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded" \
  >/dev/null
kill -KILL "$PILOT_PID"
for _ in {1..40}; do
  test ! -e "/proc/$PILOT_PID/status" && break
  PILOT_STATE=$(awk '$1 == "State:" {print $2}' "/proc/$PILOT_PID/status")
  test "$PILOT_STATE" = Z && break
  sleep 0.25
done
if test -e "/proc/$PILOT_PID/status"; then
  test "$(awk '$1 == "State:" {print $2}' "/proc/$PILOT_PID/status")" = Z
fi
```

SIGKILL is never the first abort response and is never automatic. It makes the
exact peripheral potentially persistent, so immediately proceed through fresh
fleet owner discovery and exact-target cleanup; do not call the panel pristine.

1. Do not rerun `--apply` and do not restart `message_bus` merely to clear the
   pilot.
2. Use a read-only fleet owner check to identify the current BVD owner.
3. SSH only to that already-staged owner and run the exact Phase 1
   `--cleanup-only` command. If it says the panel is not owner, re-read owner;
   never guess or write it.
4. Require `CLEANUP_PROVEN`, which includes two fresh absence reads 30 seconds
   apart.
5. Restore the Office slider through the native UI and byte-verify the original
   snapshot.
6. Repeat every postflight stock, bus, cloud-peer, HA, and physical check.

After repeating Phase 1.1 in a workstation shell, the exact cross-owner cleanup
invocation is:

```bash
set -euo pipefail
: "${REVIEWED_COMMIT:?export the same full reviewed commit}"
: "${CURRENT_OWNER_IP:?rerun the complete Phase 1.1 owner workflow}"
: "${SSHPASS:?export the approved temporary SSH password for sshpass -e}"
RELEASE="/var/brilliant-bvd-pilot/releases/$REVIEWED_COMMIT"
RESULT=$(sshpass -e ssh root@"$CURRENT_OWNER_IP" "
  set -euo pipefail
  if ! test -e /run/brilliant-bvd-pilot; then
    install -d -o 0 -g 0 -m 0700 /run/brilliant-bvd-pilot
  fi
  test \"\$(stat -c %u:%a /run/brilliant-bvd-pilot)\" = '0:700'
  cd '$RELEASE/app'
  PYTHONDONTWRITEBYTECODE=1 \
  PYTHONPATH='$RELEASE/app:$RELEASE/app/src:$RELEASE/app/custom_components/brilliant_mqtt/agent_payload/vendor:/data/switch-embedded' \
    /data/switch-embedded/env/bin/python3 -m tools.brilliant_bvd.live \
    --cleanup-only
")
printf '%s\n' "$RESULT"
printf '%s\n' "$RESULT" | grep -F '"event": "CLEANUP_PROVEN"' >/dev/null
```

If this command refuses because ownership moved, discard `CURRENT_OWNER_IP`,
rerun the entire fleet workflow, and retry cleanup only on the newly proven
owner. Never loop the delete against a guessed panel.

If no staged/reachable current owner exists, or cleanup-only cannot prove
absence, the pilot has failed its bounded/reversible requirement. Leave live
apply disabled, preserve private evidence, and escalate; do not claim the panel
is pristine.

## Acceptance record

Record only sanitized facts: reviewed commit and archive SHA-256; pinned
firmware/OSTree/embedded-stack matches; candidate count and exactly-one-owner
boolean; approved observer command digest; mirror inactive/absent pre/post;
start/end UTC; chosen slider index; room reference hash (not raw ID); dry-run
result; natural owner boolean; stock/PID continuity booleans; canary result;
READY; command kinds/counts; accepted/rejected counts; state sequence deltas;
only instrumented latency bounds; snap-back boolean; abort reason; restoration
SHA match; two absence booleans; postflight booleans; and
`lease_release=not_applicable`.

Cloud acceptance of a forced BVD bid remains `not_tested`; this procedure never
makes such a bid. The two principal exercised live unknowns are whether this
generic second framework host can coexist with the stock BVD host/canary and
whether a real physical slider routes to its `push_func` and remains settled
after confirmed HA state instead of reverting.

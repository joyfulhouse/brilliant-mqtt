# Native Home Assistant light to physical-slider E2E runbook

This runbook is the evidence contract for proving that one officially
provisioned Brilliant Virtual Control can expose one Home Assistant light to a
physical Control slider. It does not authorize provisioning, binding, device
operation, or cleanup.

Current status is **not ready for a live run**. The Office UI result involved
offline legacy lights owned by the physical Control, no Virtual Control exists,
and the operator has not authorized slider gestures. See the
[validation status](../native-ha-slider-validation-status.md) before using this
procedure.

## Safety boundaries

- Never bind one of the five offline legacy `HA Backyard/Balcony Lamp` records.
- The agent must not trigger a light, scene, physical load, or slider. At the
  gesture gate, only the operator may act and only after fresh permission is
  recorded.
- Do not write `slider_config:*` directly. Binding and restoration occur in the
  native UI.
- `tools.brilliant_vc.slider_binding` exposes no bus write method. It reads only
  `get_owning_device_id()`, `get_device(own_id)`, and the allowlisted variables
  on the physical Control's type-19 `device_config_peripheral`.
- `tools.brilliant_vc.e2e_acceptance` is offline. It opens one private JSON file
  and never connects to a panel, MQTT, or Home Assistant.
- Raw slider blobs contain device/peripheral IDs. E2E transcripts contain stable
  and command UUIDs. Both are private evidence, not documentation.
- Stop on a peer timeout, physical-control lag, cloud disconnect, unavailable
  HA state, duplicate command, sequence regression, stale snap-back, uncertain
  cleanup, or any mismatch with the named disposable light/VC.

## Private artifact layout

Use an operator-local run directory below the already ignored tree:

```text
artifacts/brilliant-panel/pilots/virtual-control/<run-id>/
  evidence/                 # mode 0700
    office-slider-before.json   # mode 0600; raw IDs/blobs
    gesture-trials.json         # mode 0600; raw event UUIDs
  public/                   # sanitized reports only
```

The repository ignores all of `artifacts/`, the narrower VC pilot tree,
`**/vc-captures/`, packet captures, and `var-collections/`. Verify before every
run:

```text
git check-ignore -v artifacts/brilliant-panel/pilots/virtual-control/<run-id>/evidence/x
git status --short --ignored
```

Do not use `docs/`, `/tmp`, a group-readable directory, or a synced cloud folder
for private evidence. A private evidence root must be a real directory owned by
the invoking UID with exact mode `0700`; each evidence file must be a real,
direct child with exact mode `0600`.

## Gate A — online target prerequisites

Do not select a slider until every item passes:

1. VC0 and VC1 pass with official-app and bus evidence.
2. One fresh, disposable DeviceType-6 identity is provisioned after the scoped
   approval gate.
3. The identity materializer passes first in dry-run mode and then creates only
   the exact private `device.key`/`device.cert` pair.
4. The dedicated non-root account exists, the root-private identity has passed
   the credential handoff, and schema-4 no-start preflight confirms the 15
   pinned launcher/configuration files, message-bus-first lifecycle, exact
   ownership/path surface, and direct-runner rejection. It reports
   `runtime_credentials_present=true` and has advanced to
   `nonroot_emperor_launcher_not_implemented` before the separately reviewed
   launcher is installed.
5. The bounded VC runtime and monitor pass with no physical-panel regression.
6. Exactly one VC-owned type-27 `LIGHT` exists, its
   `configuration_peripheral_id` resolves to the VC's own type-19
   `device_config_peripheral`, and retained HA state makes it online. The stock
   type-16, type-20, and type-48 config records may coexist.
7. Office and a second panel render the same online light in the intended room.
8. The native Office slider picker offers that VC-owned online light without a
   hand-written binding.

The current legacy picker result satisfies none of items 2–8.

## Gate B — capture the original Office binding

Name one physical Office slider and map it to its numeric
`slider_config:<index>` before capture. Record its ordinary wired/default
behavior separately; the file verifier proves configuration restoration, not
physical behavior.

On Office, stage the reviewed repository tool in the ignored pilot workspace.
Create the private evidence root with mode `0700`, then run the capture command
with no `--apply` option (none exists):

```text
python -m tools.brilliant_vc.slider_binding \
  --safe-root /data/brilliant-vc/evidence \
  capture \
  --selected-slider-index <index> \
  --output /data/brilliant-vc/evidence/office-slider-before.json
```

The capture:

- creates the output exclusively and refuses overwrite;
- writes exact mode `0600` and fsyncs it;
- snapshots every `slider_config:*` value, not only the selected slider;
- snapshots `disable_cap_touch_sliders` and
  `slider_double_tap_timeout_ms`, including absence;
- base64-decodes and bounds-checks the TBinaryProtocol structure;
- requires the variable suffix to equal wire field 1;
- retains exact raw bytes for later byte-for-byte comparison; and
- prints only the SHA-256, slider count, and `capture_written=true`.

An invalid base64 value, duplicate/missing target field, unknown Thrift type,
trailing bytes, owner/mode error, symlink, missing selected index, or partial
read blocks the capture. Copy the private file into the ignored evidence root
without printing it. Record only its SHA-256 in a sanitized ledger.

## Gate C — native-UI binding

Stop and obtain fresh approval naming:

- Office;
- the exact physical slider/index;
- the disposable VC and light;
- the original snapshot SHA-256;
- the operator as the person saving the native-UI change; and
- mandatory restoration and removal.

The operator, not the agent, selects the online VC light and saves the binding
through the native UI. Immediately repeat the scoped read and verify that:

- only the selected `slider_config:<index>` changed;
- its decoded `(device_id, peripheral_id)` is the disposable VC/light pair;
- every other slider config and both guard variables are byte-identical; and
- the original physical load remains responsive.

The baseline verifier intentionally does not offer a write or restore command.
If any unrelated variable changes, restore in the UI and stop.

## Gate D — collect a passive trial transcript

No trial is allowed under the current instruction prohibiting triggers. After
separate permission, the operator performs the minimum agreed gesture set while
collectors only observe. At least one on/off tap and one nontrivial brightness
gesture are needed to cover both mappings.

Use one collector clock for every `observed_at_ms`. Copy MQTT payload timestamps
without rewriting them. Subscribe only to the exact disposable stable-ID
command/state routes and the result UUIDs discovered from those commands. Use
scoped native reads/notifications for only the disposable VC/light on Office
and the named second panel. Do not subscribe to or serialize the whole home
graph.

The repository currently supplies the strict analyzer and schema, not a live
multi-panel collector. The passive collector adapter remains a live-topology
task: it must be reviewed against the provisioned VC socket and both panels
before use. Hand-edited or retrospectively inferred events are not acceptance
evidence.

`gesture-trials.json` has this fixed top-level schema:

```json
{
  "schema_version": 1,
  "stable_id": "d353e38a-793e-5b6f-813b-17a1c38aba96",
  "panels": ["office", "peer"],
  "limits_ms": {
    "command": 500,
    "result": 1000,
    "state": 1500,
    "panel": 2000
  },
  "trials": []
}
```

Exactly two distinct, non-identifying panel labels are required. Limits must be
positive and no greater than 60 seconds. One to sixteen non-overlapping trial
windows are allowed. A trial has these exact fields:

```json
{
  "gesture_at_ms": 1700000000000,
  "ended_at_ms": 1700000005000,
  "baseline_sequence": 7,
  "expected_kind": "set_brightness",
  "expected_value": 128,
  "commands": [
    {
      "observed_at_ms": 1700000000100,
      "issued_at_ms": 1700000000050,
      "command_id": "11111111-1111-4111-8111-111111111111",
      "stable_id": "d353e38a-793e-5b6f-813b-17a1c38aba96",
      "kind": "set_brightness",
      "value": 128,
      "observed_sequence": 7
    }
  ],
  "results": [
    {
      "observed_at_ms": 1700000000250,
      "timestamp_ms": 1700000000240,
      "elapsed_ms": 30,
      "command_id": "11111111-1111-4111-8111-111111111111",
      "stable_id": "d353e38a-793e-5b6f-813b-17a1c38aba96",
      "accepted": true,
      "resulting_sequence": 8,
      "error": null
    }
  ],
  "states": [
    {
      "observed_at_ms": 1700000000300,
      "generated_at_ms": 1700000000280,
      "stable_id": "d353e38a-793e-5b6f-813b-17a1c38aba96",
      "sequence": 8,
      "available": true,
      "state": "on",
      "brightness": 128
    }
  ],
  "panel_states": [
    {
      "observed_at_ms": 1700000000400,
      "panel": "office",
      "source_sequence": 8,
      "on": 1,
      "intensity": 502
    },
    {
      "observed_at_ms": 1700000000500,
      "panel": "peer",
      "source_sequence": 8,
      "on": 1,
      "intensity": 502
    }
  ]
}
```

For `turn_on` and `turn_off`, `expected_value` and command `value` are `null`.
For `set_brightness`, the value is `0..255`; the expected native intensity is
the production round-half-up mapping (`128 -> 502`). `source_sequence` is a
collector correlation annotation tying the native observation to the latest
authoritative HA state; it is not claimed to be a Brilliant native variable.

## Gate E — offline acceptance analysis

On the repository workstation, ensure the evidence directory/file modes are
`0700`/`0600`, then run:

```text
python -m tools.brilliant_vc.e2e_acceptance \
  --safe-root artifacts/brilliant-panel/pilots/virtual-control/<run-id>/evidence \
  --evidence artifacts/brilliant-panel/pilots/virtual-control/<run-id>/evidence/gesture-trials.json
```

The analyzer rejects extra schema fields, malformed/noncanonical UUIDs,
unexpected panels, oversized collections/files, symlinks, mode/owner errors,
and events outside their trial window. A pass requires all of the following:

1. Exactly one command per operator gesture and unique command UUIDs.
2. Coverage includes at least one on/off tap and one brightness gesture.
3. Exact stable ID, mapping kind/value, baseline sequence, and issuance order.
4. Exactly one correlated accepted result with `error=null`.
5. A strictly advancing result sequence and no later HA sequence regression.
6. An available authoritative HA state matching the requested on/brightness.
7. Matching native on/intensity observations from both named panels.
8. No extra command after feedback, no conflicting duplicate state, and no
   HA/native snap-back within the trial window.
9. Command, result, state, and per-panel latency within the declared limits,
   followed by at least one full second of passive observation after the last
   recorded event.

Output contains only counts, booleans, generic failed-gate names, and the final
`passed` value. It contains no stable ID, command ID, entity ID, device ID,
peripheral ID, topic, or payload.

## Gate F — restart and locality matrix

Only after the basic transcript passes, repeat isolated trials across:

- MQTT reconnect;
- Home Assistant restart and sequence epoch reset;
- VC process restart with retained-state fencing;
- Office restart;
- second-panel restart; and
- WAN denied while LAN/MQTT/HA/panel traffic remains available.

Each case needs a new non-overlapping trial transcript. A reconnect without
fresh authoritative state must remain fenced. Any replayed command, duplicate,
sequence regression, stale snap-back, missing peer update, or cloud-only
failure is a failed case. Classify locality as `local`, `cloud-dependent`, or
`not viable`; do not infer local operation from a successful WAN-up run.

## Gate G — restoration and removal

The operator restores the original target through the native UI. The agent may
then perform the read-only comparison:

```text
python -m tools.brilliant_vc.slider_binding \
  --safe-root /data/brilliant-vc/evidence \
  verify \
  --baseline /data/brilliant-vc/evidence/office-slider-before.json
```

`restored=true` requires the same owning Control, exact slider variable-name
set, exact raw values, exact guard values, and decoded selected binding. Also
repeat the ordinary physical-behavior check; configuration equality alone is
not enough.

Only after restoration passes may the hosted light and disposable VC be
removed through their supported flows. Two later scoped snapshots must show no
light, owner, or slider reference. Cleanup of the five separate legacy offline
lights is an independent, separately approved operation.

## Remaining implementation/live work

1. Finish official-app VC0 and obtain the official VC1 token.
2. Provision one disposable VC after fresh approval and confirm official
   removal before starting it.
3. Validate and materialize the actual official PKCS#12 locally.
4. Implement the dedicated non-root runtime-principal handoff for only the
   validated PEM pair and saved bootstrap blob. Schema-3 preflight currently
   blocks here after materialization.
5. After separate live-start approval, prove target-home bootstrap under the
   stock message-bus-first Emperor lifecycle, including local derived bus
   addressing, stubbed BLE isolation, alternate remote-bridge/discovery paths,
   type-19 Device Configuration registration, and clean stop/removal.
6. Run the isolated runtime/monitor and host the one online light.
7. Review and implement the passive multi-panel transcript collector against
   that actual topology.
8. Capture the original slider baseline and obtain binding approval.
9. Obtain separate gesture permission; the current instruction forbids it.
10. Run basic, restart, WAN, restoration, and supported-removal gates in order.

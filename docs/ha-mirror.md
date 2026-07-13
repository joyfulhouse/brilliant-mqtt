# HA mirror retirement and legacy peripheral cleanup

> **Deprecated and unsafe:** do not install, enable, or restart the physical-
> Control HA mirror. `brilliant-ha-mirror` must remain inactive. This page is a
> retirement and cleanup guide, not installation advice.

The supported replacement is the
[HA-owned control plane and scene/mode bridge](brilliant-panel/home-assistant-integration.md).
It reuses the existing panel agent sessions and does not host peripherals. The
separate native-tile idea remains blocked behind the
[Virtual Control feasibility gates](superpowers/plans/2026-07-12-virtual-control-feasibility-gates.md).

## Why physical-Control hosting was rejected

The old Tier-1 process reflected labeled HA entities into a real panel's own
device through `PeripheralHost`. Live validation disproved that architecture:

- it co-managed a physical Control that already had a native manager;
- the deployed design created one bus peer/host per entity, increasing local
  bus load and contributing to peer timeouts;
- real light controls became unresponsive under the experiment;
- native UI admission and home-wide propagation were unreliable;
- the process placed a direct HA WebSocket credential on the panel; and
- correct display metadata and room assignment did not repair the ownership,
  manager-routing, or propagation model.

Room metadata still matters to the native UI: unassigned devices are excluded
from normal room models. It was not the missing architectural ingredient. A
raw record without an owned manager can render and still route commands to the
wrong manager; a physical-Control host can own commands and still interfere
with the panel's real loads. Neither is suitable for community support.

No native HA tile, including `HA_PILOT_ROOM_D`, is supported by the replacement
baseline. Existing Brilliant scenes remain in the native UI; HA receives scene
and mode events and can request confirmed execution through MQTT.

## Config-entry version 3 migration

Migration is deliberately fail-closed:

1. The bridge component remains selected; the deprecated mirror component is
   forced off.
2. The legacy mirror label is copied to the HA-owned control label (or defaults
   to `brilliant`). Room mapping belongs to the new HA-side room-overrides
   object; it is never resolved by a panel-side HA client.
3. New control defaults are added: disabled, `light`/`switch`, maximum 50,
   current panel as scene panel, and empty room/action objects.
4. The mirror component is hidden from new install and reconfigure selection.
   Its install function rejects calls; reconciliation can only uninstall it.
5. The manager creates a redacted Repair, inspects all six legacy states, runs
   uninstall when anything remains, and performs a fresh strict inspection.
6. The Repair clears only after absence is proven and the panel shell closes
   cleanly. Old URL/direct-HA credential/leader fields are removed only when
   safe control is enabled and retirement is verified. The copied legacy label
   and unrelated settings remain.

If the panel is offline, inspection is ambiguous, uninstall fails, the second
proof still finds state, shell cleanup times out, or the operation is cancelled,
the Repair remains and old sensitive fields are retained. That is an explicit
unverified state, not a successful migration. Do not respond by restarting the
old service.

## Inspect service retirement

Use the integration Repair/reconfigure flow first. A healthy retired panel has
an active `brilliant-mqtt` service and an inactive or absent
`brilliant-ha-mirror` service. Authenticate using the approved temporary
`SSHPASS` environment only; never place a credential in an argument or capture.

```bash
sshpass -e ssh root@<panel-ip> \
  'systemctl is-active brilliant-mqtt; systemctl is-active brilliant-ha-mirror || true'
```

Do not print `/etc/brilliant-ha-mirror.env`. The config-entry manager owns
removal of its unit, environment file, payload directory, and staged copy.

## Legacy peripheral cleanup

Service retirement removes the unsafe runtime but cannot assume that records
persisted by earlier experiments disappeared. The cleanup CLI inventories only
the Control's own device and is safe to run in dry-run mode. It neither hosts a
peripheral nor scans the whole home graph.

### Candidate rule

A peripheral is eligible only when **both** independent, case-sensitive tests
pass:

- its peripheral ID starts with one of `ha_`, `ha-pilot-`, or `zzz_mirror_`;
- its display name starts with one of `HA `, `HA_PILOT_`, or `ZZZ Mirror `.

The prefixes need not be paired by position, but both dimensions must match.
Near matches, different casing, normal Brilliant loads, and special native
peripherals are excluded. If an eligible peripheral ID occurs more than once,
every occurrence of that ID is excluded as ambiguous. Do not rename a record to
make it pass.

### 1. Dry run

Run this first on the panel with the exact deployed agent interpreter and module
paths:

```bash
PYTHONPATH=/var/brilliant-mqtt/app:/var/brilliant-mqtt/vendor \
  /data/switch-embedded/env/bin/python3 \
  -m brilliant_mqtt.cleanup_legacy_mirror
```

Dry run performs one scoped snapshot, no delete, no sleep, and no file write. It
prints one canonical redacted JSON object with exactly:

- `timestamp_ms` and `owning_device_id`;
- `candidates` containing only `id`, `name`, and integer `type`;
- `deleted_ids`, `remaining_ids`, and `success`.

It never copies variables, values, serialized blobs, paths, exception text, or
credentials into the report. In dry run, candidates appear in
`remaining_ids`; `success:true` means inventory completed, not that candidates
were removed.

Review the JSON only on-screen in an unrecorded terminal. Do not redirect it,
copy it into the ignored pilot artifacts, or record `owning_device_id`, candidate
IDs, or candidate names. Persist only a manually sanitized candidate count,
success/failure outcome, and operator decision. If every candidate is not
unmistakably from an old HA mirror/pilot, stop. Keep the mirror inactive and
investigate manually.

### 2. Apply only after a fresh operator decision

Apply is root-only and requires an absolute report path beneath
`/data/brilliant-mqtt/cleanup/`. Immediately before an approved apply, prove the
effective user and create/normalize the private parent directory:

```bash
test "$(id -u)" -eq 0
install -d -m 0700 /data/brilliant-mqtt/cleanup

PYTHONPATH=/var/brilliant-mqtt/app:/var/brilliant-mqtt/vendor \
  /data/switch-embedded/env/bin/python3 \
  -m brilliant_mqtt.cleanup_legacy_mirror \
  --apply \
  --snapshot /data/brilliant-mqtt/cleanup/legacy-mirror-<timestamp>.json
```

Relative paths, traversal, missing parents, symlinks, directories, paths
outside the safe tree, and unwritable parents are rejected before the native
client is constructed. Create the intended private parent deliberately; do not
weaken permissions or redirect the report elsewhere.

Before the first delete, apply atomically writes a mode-0600 failure-state
report. It then calls the firmware's native `delete_peripheral` method serially
with a fresh millisecond timestamp and a one-second pause between candidates
(never after the last). A delete error stops subsequent deletes.

Whether deletion succeeds or fails, the CLI takes a fresh second scoped
snapshot. Success requires every original candidate to be absent from the same
owning device, successful native-client shutdown, and a final atomic redacted
report write. Verification/read/report/shutdown failure produces a nonzero exit
and conservative `success:false` output. Re-running with no candidates is
idempotent and performs no delete.

The actual report contains identifiers and remains on-panel, mode 0600. Verify
its mode and compute its digest without displaying the content:

```bash
test "$(stat -c %a /data/brilliant-mqtt/cleanup/legacy-mirror-<timestamp>.json)" = 600
sha256sum /data/brilliant-mqtt/cleanup/legacy-mirror-<timestamp>.json
```

Only the report SHA-256, sanitized counts/outcome, exit status, and operator
decision may be copied into the ignored pilot record. Never save the cleanup
JSON itself in repository artifacts.

### Abort and rollback expectations

Deletion of a stale peripheral has no automatic inverse. The CLI cannot and
must not re-host a record to “roll back.” On any nonzero result:

1. stop; do not rerun apply in a loop;
2. retain the private report and the terminal redacted JSON;
3. leave `brilliant-ha-mirror` inactive;
4. verify physical loads and the native UI from the panel;
5. resolve remaining/ambiguous IDs from a new read-only snapshot; and
6. do not remove legacy source/runtime packaging until the Office scene bridge
   hardware gate passes.

Cleanup is intentionally separate from scene-control enable, disable, deploy,
and rollback. The [Office pilot runbook](brilliant-panel/runbooks/scene-bridge-pilot.md)
permits dry run after acceptance and requires a new operator decision before any
apply.

## Historical references

The superseded Tier-1 design and research notes remain evidence of the failure,
not implementation guidance:

- [Tier-1 design](superpowers/specs/2026-07-10-ha-mirror-tier1-design.md)
- [visibility research](superpowers/research/2026-07-10-ha-mirror-v1-visibility.md)
- [per-type research](superpowers/research/2026-07-10-ha-mirror-v3-per-type.md)
- [replacement design](superpowers/specs/2026-07-12-ha-control-plane-and-virtual-control-design.md)

Do not use the old manual deploy, leader election, direct HA connection, or
physical-host instructions from superseded documents.

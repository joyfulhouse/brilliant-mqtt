# Single-panel canary release readiness

This runbook decides whether the single-panel **software** canary for the
responsiveness work merged in
[#156](https://github.com/joyfulhouse/brilliant-mqtt/pull/156),
[#158](https://github.com/joyfulhouse/brilliant-mqtt/pull/158), and
[#163](https://github.com/joyfulhouse/brilliant-mqtt/pull/163) may start. It
extends the existing [deployment parity and soak procedure](deployment.md#roll-out-order)
and [validation evidence levels](../brilliant-panel/validation-runbook.md#evidence-levels);
it does not replace either one.

This is not a fleet release or permission to operate arbitrary loads. The
qualification is limited to software health and deterministic idempotent,
single-field write behavior. An MQTT or bus acknowledgement does not prove
physical actuation, and this canary makes no physical slider-latency claim.

## Candidate identity and provenance

The candidate identity is the tuple:

- commit `e4ae3176b605f5f7ac8e971823ec47ba506af1b8`; and
- SHA-256 digest
  `20763b35a4ed6f2d45a74f9c48b11ae913a9b2b73e5866060216955f660b6cb6`
  of the sorted `payload-release` path/hash manifest.

The payload `VERSION` is `0.10.2`, but that label is necessary and not
sufficient: several byte-distinct revisions can share it. Reproduce and record
the identity from a qualified repository checkout:

```bash
git checkout --detach e4ae3176b605f5f7ac8e971823ec47ba506af1b8
scripts/build_payload.sh
git diff --exit-code -- custom_components/brilliant_mqtt/agent_payload

candidate_manifest=artifacts/canary/candidate-payload.sha256
mkdir -p "$(dirname "$candidate_manifest")"
uv run python scripts/brilliant-panel/bundle_manifest.py payload-release \
  custom_components/brilliant_mqtt/agent_payload > "$candidate_manifest"
sha256sum "$candidate_manifest"
# Record and compare with: <recorded-64-hex-manifest-digest>
```

The build script creates the committed mirror from source and writes its
version ([`build_payload.sh` lines 9-13 and 69-70](../../scripts/build_payload.sh#L9)).
CI rebuilds it and requires a clean diff plus no untracked or ignored mirror
files ([`ci.yml` lines 16-22](../../.github/workflows/ci.yml#L16)); release CI
does the same and puts that mirror in the shipped HACS zip
([`release.yml` lines 28-36](../../.github/workflows/release.yml#L28)). The
manifest helper hashes required release files, adds normalized installed-file
aliases, and emits sorted path/hash rows
([`bundle_manifest.py` lines 329-356](../../scripts/brilliant-panel/bundle_manifest.py#L329),
[`bundle_manifest.py` line 484](../../scripts/brilliant-panel/bundle_manifest.py#L484)).
Thus checkout -> rebuild -> clean mirror diff -> manifest digest proves
commit-to-bytes independently of the zip checksum and `VERSION` label.

No new provenance file is needed. The existing manifest distinguishes bytes
that share `0.10.2`. Embedding the enclosing Git SHA in the committed generated
mirror would be circular: changing the embedded SHA changes the commit, and the
mirror could not both name its enclosing commit and remain clean under the CI
diff gate.

## Supported upgrade and exact parity

Use only the journaled panel provisioner for this canary. It detects an absent,
legacy fixed, or release-link layout
([`panel_ops.py` lines 376-405](../../custom_components/brilliant_mqtt/panel_ops.py#L376)),
captures the prior snapshot before staging
([`panel_provisioner.py` lines 991-1067](../../custom_components/brilliant_mqtt/panel_provisioner.py#L991)),
stages an immutable `releases/<version>--<UUID>` tree
([`panel_ops.py` lines 862-887](../../custom_components/brilliant_mqtt/panel_ops.py#L862)),
then stops the owned services and atomically switches `current` before
restarting them
([`panel_ops.py` lines 1074-1117](../../custom_components/brilliant_mqtt/panel_ops.py#L1074),
[`panel_ops.py` lines 1159-1214](../../custom_components/brilliant_mqtt/panel_ops.py#L1159)).
Do not use the legacy manual installer or invent a parallel migration path.

The post-install `panel-release` gate deliberately requires `current` to select
one direct child of `releases`; a missing directory/link fails with exit 2.
There is no legacy fallback
([`bundle_manifest.py` lines 378-416](../../scripts/brilliant-panel/bundle_manifest.py#L378),
[`bundle_manifest.py` lines 502-514](../../scripts/brilliant-panel/bundle_manifest.py#L502)).
Do not weaken or bypass that failure.

Before declaring the installed software an active canary, run the complete
[exact-bundle parity gate](deployment.md#office-exact-bundle-parity-gate). Exact
parity means an empty byte-for-byte diff between:

1. the `payload-release` manifest rebuilt from the pinned committed mirror;
2. the `payload-release` manifest from the integration actually loaded by Home
   Assistant; and
3. the `panel-release` manifest of the release the panel actually loaded.

In particular, require the final panel comparison to be empty:

```bash
diff -u \
  artifacts/canary/candidate-payload.sha256 \
  artifacts/canary/panel-active-payload.sha256
```

Use the host-key-pinned collection commands in the deployment reference rather
than copying them here. Record the installed manifest digest and confirm it is
the candidate digest above. `VERSION == 0.10.2` alone does not pass this gate.

### Manual-update downgrade blocker

During candidate installation and the entire soak, **do not trigger a manual
agent update** (the Update entity's install action or the equivalent service).
The manual `async_update_agent` path unconditionally calls `deploy_payload`
([`manager.py` lines 1365-1384](../../custom_components/brilliant_mqtt/manager.py#L1365),
[`manager.py` lines 1430-1447](../../custom_components/brilliant_mqtt/manager.py#L1430));
that function swaps bytes and writes only `VERSION`, with no manifest guard or
journaled rollback
([`panel_ops.py` lines 2146-2161](../../custom_components/brilliant_mqtt/panel_ops.py#L2146)).
An older HACS bundle also labelled `0.10.2` can therefore overwrite the
candidate. This action is manual, not scheduled. Automatic repair only lays
down the currently bundled payload when the payload is absent
([`manager.py` lines 1292-1303](../../custom_components/brilliant_mqtt/manager.py#L1292));
with the loaded HA bundle pinned by the parity gate, that is the same candidate.
It does not make a later bundle change or manual update safe. Keep the loaded HA
bundle pinned and record an operator change freeze as the temporary mitigation.
Durable hardening is tracked in
[#165](https://github.com/joyfulhouse/brilliant-mqtt/issues/165).

## Complete rollback gate

The journaled provisioner is the rollback authority. Its durable
`StoredPanelSnapshot` covers layout, active release target, environment and
version file content/mode, and bridge, Wi-Fi-watchdog, and bus-watchdog unit
content/mode/enabled/active state
([`provisioning_journal.py` lines 476-569](../../custom_components/brilliant_mqtt/provisioning_journal.py#L476)).
Rollback stops services, restores those files and modes, restores the selector,
reloads systemd, restores each recorded service state, and re-snapshots for
equality
([`panel_ops.py` lines 1326-1421](../../custom_components/brilliant_mqtt/panel_ops.py#L1326)).
The provisioner then removes the staged candidate and calls
`async_complete_rollback(verified=True)`
([`panel_provisioner.py` lines 1218-1258](../../custom_components/brilliant_mqtt/panel_provisioner.py#L1218));
the journal clears only after that asserted verification
([`provisioning_journal.py` lines 952-969](../../custom_components/brilliant_mqtt/provisioning_journal.py#L952)).

Two known gaps are hard pre-activation gates under
[#166](https://github.com/joyfulhouse/brilliant-mqtt/issues/166):

- preflight must prove that a missing prior immutable tree can be restored
  before any mutation; and
- rollback must not be marked verified until a **fresh prior-version MQTT
  health probe** succeeds. Current exact-restore tests use fakes and mocked
  snapshots rather than an exercised panel restore
  ([`test_panel_ops.py` lines 3543-3561](../../ha/tests/test_panel_ops.py#L3543),
  [`test_panel_provisioner.py` lines 707-720](../../ha/tests/test_panel_provisioner.py#L707)).

A successful provisioning commit clears the journal
([`provisioning_journal.py` lines 915-938](../../custom_components/brilliant_mqtt/provisioning_journal.py#L915)).
Before activation, the operator must prove that the rollback snapshot and
complete prior release remain retained by the approved recovery mechanism and
that rollback stays callable for the **entire** soak. Do not interpret a prior
release directory alone as a complete snapshot of files, modes, and service
states.

Rollback is forward-only for state outside that snapshot: candidate-published
retained MQTT topics can remain, Home Assistant entity/registry state can
remain, and the panel's OSTree firmware is not rolled back. Inspect and
reconcile those separately; never claim this application rollback reversed
them.

### Required restore rehearsal

Before the candidate is allowed onto the designated panel, exercise the same
journaled path against a safe representative installation and retain sanitized
evidence that all of these pass:

1. restore from a deliberately absent prior tree using the approved retained
   snapshot, with exact manifest, file-mode, selector, and service-state proof;
2. interrupt at a documented post-mutation crash cut point, restart recovery,
   and prove it converges to the complete prior state without a partial layout;
3. measure a fresh prior-version MQTT offline -> online transition and health
   publication within 90 seconds after rollback; record elapsed time and prior
   byte identity, not raw topics, logs, or private host data;
4. prove the candidate staging/release is cleaned, the journal reaches its
   verified terminal behavior, and no persistent rollback repair remains; and
5. repeat the recovery invocation or equivalent read-only audit to demonstrate
   idempotence, then prove the retained snapshot and prior release will remain
   available throughout a one-day-or-longer soak.

Passing unit tests alone does not satisfy this rehearsal.

## Qualification boundaries and diagnostics

Qualify idempotent, single-field write behavior with the existing deterministic
fake tests
([`test_mqttio_transport_backlog.py` lines 143-167](../../tests/test_mqttio_transport_backlog.py#L143),
[`test_bus_adapter.py` lines 625-655](../../tests/test_bus_adapter.py#L625)). A
live scalar write is outside this software-health claim unless separately
authorized; if authorized, constrain it with the existing validation runbook's
[scalar write/restore protocol](../brilliant-panel/validation-runbook.md#5-scalar-writerestore-protocol)
and still make no physical-actuation or latency claim. Do not send mixed-field
write traffic during qualification. The lane queue replaces a pending
same-topic message as a whole payload
([`mqttio.py` lines 84-111](../../src/brilliant_mqtt/mqttio.py#L84)).
[#159](https://github.com/joyfulhouse/brilliant-mqtt/issues/159) documents the
pre-existing latest-wins field-loss defect and blocks every multi-field
correctness claim; a multi-field canary must wait for #159. The unreaped lane
worker risk in [#160](https://github.com/joyfulhouse/brilliant-mqtt/issues/160)
also remains a monitored residual.

Bounded response diagnostics from
[#152](https://github.com/joyfulhouse/brilliant-mqtt/issues/152) and
[#157](https://github.com/joyfulhouse/brilliant-mqtt/pull/157) are **excluded**
from the candidate at `e4ae3176`; they are separate-branch work, and the
combined-branch conflict remains tracked in
[#162](https://github.com/joyfulhouse/brilliant-mqtt/issues/162). Consequently,
the missing coalescing evidence described by
[#161](https://github.com/joyfulhouse/brilliant-mqtt/issues/161) reduces soak
observability. Do not infer an absence of folding/coalescing anomalies from an
absence of diagnostics.

## Readiness and blocker matrix

`MET` means evidence already exists for this exact candidate or the scope has
been explicitly constrained. `BLOCKED` means the canary must not activate until
the cited issue and operator evidence satisfy the pass criteria.

| Gate | How verified | Pass criteria | Current status |
| --- | --- | --- | --- |
| Candidate identity | Rebuild, clean mirror diff, hash sorted manifest ([CI parity](../../.github/workflows/ci.yml#L16)) | Commit and manifest digest equal the values above | **MET** |
| Legacy -> immutable upgrade | Journaled snapshot/stage/atomic-select path only ([provisioner](../../custom_components/brilliant_mqtt/panel_provisioner.py#L991)) | Exercised migration retains a complete recoverable prior state | **BLOCKED - #166 rehearsal** |
| `panel-release` exact-release gate | Run the deployment reference's panel manifest command ([selector rules](../../scripts/brilliant-panel/bundle_manifest.py#L378)) | Exit 0 with `current -> releases/<direct-child>`; no legacy fallback | **BLOCKED - #166 must clear before migration/activation** |
| Exact HA/panel parity | Empty candidate/loaded-HA/active-panel manifest diffs ([deployment gate](deployment.md#office-exact-bundle-parity-gate)) | Every normalized path and SHA-256 matches; installed manifest digest recorded | **BLOCKED - #166 prevents the install evidence** |
| Exercised complete rollback | Missing-tree and crash rehearsals plus measured fresh prior MQTT health ([current mocked test](../../ha/tests/test_panel_ops.py#L3543)) | Exact restore and fresh MQTT health within 90 seconds | **BLOCKED - #166** |
| Rollback available throughout soak | Audit retained snapshot/prior tree after journal clear ([commit clear](../../custom_components/brilliant_mqtt/provisioning_journal.py#L915)) | Recovery remains callable through the final soak observation | **BLOCKED - #166 / retention evidence absent** |
| Same-version update footgun | Operator change freeze; audit that no manual update is invoked ([unguarded deploy](../../custom_components/brilliant_mqtt/panel_ops.py#L2146)) | Written do-not-update control covers install and full soak | **BLOCKED - #165 until control is attested** |
| Multi-field correctness excluded | Review traffic plan and #159; use only single-field writes ([queue replacement](../../src/brilliant_mqtt/mqttio.py#L99)) | No mixed-field traffic or claim; any future claim waits for #159 | **MET - multi-field remains blocked by #159** |
| Diagnostics excluded | Pin commit; record #152/#157/#162 exclusion and #161 limitation | Evidence makes no diagnostics-based or physical-actuation claim | **MET** |

## Deployment evidence template

Keep public evidence generic and sanitized. Store path/hash-only manifests and
diffs as directed by the [deployment reference](deployment.md#office-exact-bundle-parity-gate).
Do not publish credentials, hostnames, addresses, panel identifiers, private
topology, journal contents, raw topics, or raw logs.

```text
CANARY DECISION
date/time + timezone:
operator/reviewer roles:
result: GO / NO-GO / ABORTED

CANDIDATE
commit: e4ae3176b605f5f7ac8e971823ec47ba506af1b8
VERSION label: 0.10.2 (necessary, not sufficient)
candidate payload-release manifest digest:
loaded-HA payload-release manifest digest:
active-panel panel-release manifest digest:
all exact manifest diffs empty: yes/no

BASELINE (read-only)
installed layout + byte identity:
fresh pre-deploy MQTT software-health evidence + timestamp:
process restart/reconnect counters:
resource observations (RSS/CPU/load/free memory):
configured MemoryMax / CPUQuota / Nice:
rollback snapshot + prior release retained (contents kept private): yes/no

DEPLOYMENT
journaled provisioner transaction reference (identifier kept private):
legacy-to-immutable migration result:
panel-release exact gate result:
candidate manifest digest installed:
services stopped before atomic in-place swap: yes/no
fresh post-deploy MQTT software-health evidence + elapsed time:
MemoryMax / CPUQuota / Nice observed after deploy:
unexpected restarts/reconnects or resource-cap events:

ROLLBACK READINESS
missing-prior-tree rehearsal result + artifact reference:
crash-recovery rehearsal result + artifact reference:
measured fresh prior-version MQTT reconnect/health time:
rollback retained and callable through soak: yes/no
manual agent-update freeze attested: yes/no

SOAK (one panel, >= 1 day)
start/end + elapsed duration:
software-health samples and resource results:
identity/digest rechecked at end:
unexpected restarts/reconnects:
single-field qualification performed (if authorized):
mixed-field traffic excluded: yes/no
diagnostics exclusion/#161 limitation acknowledged: yes/no
abort criteria encountered:
result: PASS / FAIL / INCONCLUSIVE

CLAIM BOUNDARY
software health demonstrated:
physical actuation measured: no / separately authorized evidence reference
physical slider latency claimed: no
forward-only retained MQTT / HA registry / OSTree reconciliation:
```

The release unit specifies `Nice=10`, `MemoryMax=96M`, and `CPUQuota=20%`
([`brilliant-mqtt-release.service` lines 19-24](../../deploy/brilliant-mqtt-release.service#L19));
record what systemd actually reports rather than copying those desired values
into the evidence.

## Soak and abort

The deployment is an in-place atomic swap, not an A/B run: all owned services
stop before the selector changes, then the selected services restart
([`panel_ops.py` lines 1178-1214](../../custom_components/brilliant_mqtt/panel_ops.py#L1178)).
Keep HomeKit paired as an independent operator fallback, as required by the
[validation preflight](../brilliant-panel/validation-runbook.md#1-preflight).
After every readiness row passes, soak exactly one panel for at least one day
using the existing [roll-out order](deployment.md#roll-out-order).

Abort the canary immediately on any of these conditions:

- candidate, loaded-HA, or panel manifest identity drifts;
- an unexpected process restart, MQTT reconnect, or bus reconnect occurs;
- no fresh MQTT software-health evidence arrives for 90 seconds;
- an observed resource cap differs from the reviewed unit, a cap is breached,
  or resource pressure threatens the native UI; or
- rollback retention/callability, the manual-update freeze, or the single-field
  traffic boundary can no longer be proven.

On abort, stop qualification traffic and invoke only the rehearsed journaled
rollback. Measure fresh prior-version MQTT health; do not treat HomeKit fallback,
a process restart, a bus acknowledgement, or a restored `VERSION` label as
proof that rollback completed or that a physical load actuated.

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

The deployment gate writes these three payload manifests as
`repository-payload.sha256`, `home-assistant-payload.sha256`, and
`office-active-payload.sha256`. Require both payload legs to be empty using
those exact emitted filenames:

```bash
parity_evidence=artifacts/brilliant-panel/pilots/office-bundle-parity
diff -u \
  "$parity_evidence/repository-payload.sha256" \
  "$parity_evidence/home-assistant-payload.sha256"
diff -u \
  "$parity_evidence/repository-payload.sha256" \
  "$parity_evidence/office-active-payload.sha256"
```

Use the host-key-pinned collection commands in the deployment reference rather
than copying them here; those commands create every referenced file and also
compare the complete integration manifests. Record the installed payload
manifest digest and confirm it is the candidate digest above.
`VERSION == 0.10.2` alone does not pass this gate.

### Manual-update downgrade blocker

During candidate installation and the entire soak, **do not trigger a manual
agent update** (the Update entity's install action or the equivalent service).
The manual `async_update_agent` path unconditionally calls `deploy_payload`
([`manager.py` lines 1365-1384](../../custom_components/brilliant_mqtt/manager.py#L1365),
[`manager.py` lines 1430-1447](../../custom_components/brilliant_mqtt/manager.py#L1430));
that function swaps the fixed legacy `/var/brilliant-mqtt/app` and `vendor`
trees and writes the shared root `VERSION`, with no manifest guard or journaled
rollback
([`panel_ops.py` lines 2146-2161](../../custom_components/brilliant_mqtt/panel_ops.py#L2146),
[`panel_ops.py` lines 2194-2210](../../custom_components/brilliant_mqtt/panel_ops.py#L2194)).
After immutable migration it does **not** overwrite the active candidate below
`current/`; instead, an older same-version HACS bundle destroys the leftover
legacy app/vendor bytes that are the de-facto prior-code source and makes the
root `VERSION` diverge from the active release. Before migration, those fixed
paths are still active and can be overwritten directly. This action is manual,
not scheduled. Automatic repair probes the same fixed payload path
([`panel_ops.py` lines 1535-1559](../../custom_components/brilliant_mqtt/panel_ops.py#L1535))
and deploys only when that path is absent
([`manager.py` lines 1292-1303](../../custom_components/brilliant_mqtt/manager.py#L1292));
it does not make a later bundle change or manual update safe. Keep the loaded HA
bundle pinned and record an operator change freeze as the temporary mitigation.
Durable hardening is tracked in
[#165](https://github.com/joyfulhouse/brilliant-mqtt/issues/165).

## Complete rollback gate

The journaled provisioner is the rollback authority. Its durable
`StoredPanelSnapshot` covers layout, active release target, environment and
version file content/mode, and bridge, Wi-Fi-watchdog, and bus-watchdog unit
content/mode/enabled/active state. It also stores `selected_components`, a
validated derivative of which of those three unit files exist, rather than an
independently restored resource
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

- for the expected first legacy-to-immutable migration, the snapshot contains
  no app/vendor bytes and has no active release target. Its rollback only
  removes `current`; it relies on the fixed legacy app/vendor tree remaining
  untouched
  ([`panel_ops.py` lines 1260-1277](../../custom_components/brilliant_mqtt/panel_ops.py#L1260)).
  Preflight must therefore explicitly retain and prove restoration of those
  prior code bytes before any mutation; and
- rollback must not be marked verified until a **fresh prior-version MQTT
  health probe** succeeds. Current exact-restore tests use fakes and mocked
  snapshots rather than an exercised panel restore, while the provisioner
  passes `verified=True` immediately after restore and cleanup without that
  probe
  ([`test_panel_ops.py` lines 3543-3561](../../ha/tests/test_panel_ops.py#L3543),
  [`panel_provisioner.py` lines 1243-1257](../../custom_components/brilliant_mqtt/panel_provisioner.py#L1243)).

A successful provisioning commit clears the journal
([`provisioning_journal.py` lines 915-938](../../custom_components/brilliant_mqtt/provisioning_journal.py#L915)).
The journal API has no export, import, or re-arm operation
([`provisioning_journal.py` lines 861-969](../../custom_components/brilliant_mqtt/provisioning_journal.py#L861));
after commit, the journaled rollback path therefore has no snapshot to execute.
#166 must define and prove a **named** retention-and-restore mechanism that
preserves the complete snapshot plus the prior legacy app/vendor bytes and can
reinstate them into a supported executable rollback path. No such mechanism is
currently defined, and an external Home Assistant backup is only an operator
precaution, not this canary gate
([`deployment.md` lines 241-252](deployment.md#L241)). Until that mechanism
exists, rollback is unavailable during the post-commit soak.

Effects outside the core snapshot are forward-only; this list is
non-exhaustive. Candidate-published retained MQTT topics and the on-panel
owned-topics ledger can remain
([`const.py` lines 178-190](../../custom_components/brilliant_mqtt/const.py#L178)),
as can Home Assistant entity **and device** registry state
([`__init__.py` lines 249-269](../../custom_components/brilliant_mqtt/__init__.py#L249)),
the hue-ca, voice, and retired HA-mirror subsystems
([`const.py` lines 195-229](../../custom_components/brilliant_mqtt/const.py#L195)),
and the retained mesh-leader claim
([`mesh_leader.py` lines 203-208](../../src/brilliant_mqtt/mesh_leader.py#L203)).
The panel's OSTree firmware is not rolled back either. Inspect and reconcile
these separately; never claim this application rollback reversed them.

### Required restore rehearsal

Before the candidate is allowed onto the designated panel, exercise the same
journaled path against a safe representative installation and retain sanitized
evidence that all of these pass:

1. restore a first-migration legacy app/vendor tree after deliberately removing
   the prior code bytes, using the named retained snapshot-and-bytes mechanism,
   with exact manifest, file-mode, selector, and service-state proof;
2. interrupt at a documented post-mutation crash cut point, restart recovery,
   and prove it converges to the complete prior state without a partial layout;
3. measure a fresh prior-version MQTT offline -> online transition and health
   publication within 90 seconds after rollback; record elapsed time and prior
   byte identity, not raw topics, logs, or private host data;
4. prove the candidate staging/release is cleaned, the journal reaches its
   verified terminal behavior, and no persistent rollback repair remains; and
5. repeat the recovery invocation or equivalent read-only audit to demonstrate
   idempotence, then prove the named mechanism can reinstate the snapshot and
   prior bytes throughout a one-day-or-longer post-commit soak.

Passing unit tests alone does not satisfy this rehearsal.

## Qualification boundaries and diagnostics

Existing deterministic fake tests establish narrower software behavior: only
the newest single-field intensity value issues after a blocked bus gate, and
supersession retains its original order ahead of another target
([`test_bus_adapter.py` lines 682-712](../../tests/test_bus_adapter.py#L682),
[`test_write_admission.py` lines 162-201](../../tests/test_write_admission.py#L162)).
They do not by themselves qualify end-to-end idempotent writes. A live scalar
write is outside this software-health claim unless separately authorized; if
authorized, constrain it with the existing validation runbook's
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
| Legacy -> immutable upgrade | Journaled snapshot/stage/atomic-select path only ([provisioner](../../custom_components/brilliant_mqtt/panel_provisioner.py#L991)) | Exercised migration explicitly retains and restores the journal snapshot plus legacy app/vendor bytes | **BLOCKED - #166 rehearsal** |
| `panel-release` exact-release gate | Run the deployment reference's panel manifest command ([selector rules](../../scripts/brilliant-panel/bundle_manifest.py#L378)) | Exit 0 with `current -> releases/<direct-child>`; no legacy fallback | **BLOCKED - #166 must clear before migration/activation** |
| Exact HA/panel parity | Empty candidate/loaded-HA/active-panel manifest diffs ([deployment gate](deployment.md#office-exact-bundle-parity-gate)) | Every normalized path and SHA-256 matches; installed manifest digest recorded | **BLOCKED - #166 prevents the install evidence** |
| Exercised complete rollback | Missing-prior-legacy-bytes and crash rehearsals plus measured fresh prior MQTT health ([current mocked test](../../ha/tests/test_panel_ops.py#L3543)) | Exact snapshot and legacy-byte restore plus fresh MQTT health within 90 seconds | **BLOCKED - #166** |
| Rollback available throughout soak | Exercise the named snapshot-and-bytes retention mechanism after journal clear ([commit clear](../../custom_components/brilliant_mqtt/provisioning_journal.py#L915)) | Named mechanism is defined, exercised, and can reinstate an executable rollback through the final post-commit soak observation | **BLOCKED - #166 / mechanism undefined** |
| Same-version update footgun | Operator change freeze; audit that no manual update is invoked ([unguarded deploy](../../custom_components/brilliant_mqtt/panel_ops.py#L2146)) | Written do-not-update control protects legacy prior bytes and root `VERSION` through install and soak | **BLOCKED - #165 until control is attested** |
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
named rollback retention-and-restore mechanism:
rollback snapshot + prior legacy app/vendor bytes retained (contents kept private): yes/no

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
missing-prior-legacy-bytes rehearsal result + artifact reference:
crash-recovery rehearsal result + artifact reference:
measured fresh prior-version MQTT reconnect/health time:
post-commit snapshot/prior-byte reinstatement exercised: yes/no
rollback mechanism callable through soak: yes/no
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
forward-only MQTT topics/ledger, HA registries, companion state, and OSTree reconciliation:
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

On a pre-commit abort, stop qualification traffic and invoke only the rehearsed
journaled rollback. On a post-commit soak abort, that journal has been cleared:
use only the named, rehearsed #166 retention-and-restore mechanism. Until that
mechanism exists, no executable rollback is available; stop traffic, preserve
evidence, and escalate without improvising a partial redeploy. After any
rollback, measure fresh prior-version MQTT health; do not treat HomeKit
fallback, a process restart, a bus acknowledgement, or a restored `VERSION`
label as proof that rollback completed or that a physical load actuated.

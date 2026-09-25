# Single-panel canary release readiness

Use this runbook to qualify the final responsiveness release on one designated
panel before fleet rollout. It complements the [deployment procedure](deployment.md),
[validation evidence levels](../brilliant-panel/validation-runbook.md#evidence-levels),
and [retained rollback procedure](../canary-rollback.md).

A software-health soak, a reported Home Assistant state change, and a physically
observed light response are different measurements. Record each separately. A
bus acknowledgement or a retained MQTT `online` message does not establish
physical actuation or native panel-slider responsiveness.

## Candidate scope and identity

Do not deploy the old `e4ae3176` documentation candidate or select a checkout by
its branch name or `0.10.2` label. Several byte-distinct candidates used that
version. Record the exact final commit, version, release ordinal, integration
manifest and payload manifest in the deployment evidence for each attempt.

The release must include these independently reviewed changes:

| Change | Tracking | Qualification consequence |
| --- | --- | --- |
| Pending mesh feedback and interactive scheduling | #156, #158, #163 | Distinguish pending intent from observed state; preserve one native writer per target |
| Bounded response diagnostics | #157 / #152 | Measure queue wait, native RPC duration, timeouts and session rebuilds; diagnostics are included in current main |
| Identity admission and retained rollback | #168 / #165 / #166 | Require guarded updates and a complete named predecessor baseline throughout soak |
| Partial command preservation | #170 / #159 | Include partial and overlapping fields in the final correctness tests |
| Dead lane recovery and fold evidence | #160, #161 | Verify bounded worker recovery, independent lane progress and observable failed folds |

PR links and passing checks are not proof that a candidate contains those
changes. Inspect the final commit's ancestry and source, regenerate its payload,
and verify both runtime gates on the actual combined tree. Resolve outstanding
reviews before declaring the candidate ready. #162's earlier branch conflict is
historical; it is not a reason to exclude diagnostics from the final release.

Use an isolated clean checkout. Never deploy an operator's dirty development
checkout. From the selected committed candidate:

```bash
set -e
umask 0022
git rev-parse HEAD
scripts/build_payload.sh
git diff --exit-code -- custom_components/brilliant_mqtt/agent_payload
test -z "$(git ls-files --others --exclude-standard -- custom_components/brilliant_mqtt/agent_payload)"
test -z "$(git ls-files --others --ignored --exclude-standard -- custom_components/brilliant_mqtt/agent_payload)"

candidate_manifest=artifacts/canary/candidate-payload.sha256
mkdir -p "$(dirname "$candidate_manifest")"
uv run python scripts/brilliant-panel/bundle_manifest.py payload-release \
  custom_components/brilliant_mqtt/agent_payload > "$candidate_manifest"
sha256sum "$candidate_manifest"
```

Record the output instead of copying a digest from this document. The
[payload builder](../../scripts/build_payload.sh), [manifest helper](../../scripts/brilliant-panel/bundle_manifest.py),
[CI](../../.github/workflows/ci.yml), and [release workflow](../../.github/workflows/release.yml)
define commit-to-bundle parity. A new release needs a distinct semantic version
and the reviewed next release ordinal; neither replaces digest verification.

## Guarded deployment and installed parity

Use the integration's supported, journaled update or onboarding/migration path.
The shared [release identity policy](../../custom_components/brilliant_mqtt/release_identity.py)
admits equal code without changing selection, or different code with a strictly
newer known ordinal. Unknown, equal-ordinal or older differing code requires an
explicitly reviewed, bound, single-operation override. Never configure an
override for automatic repair or bypass an identity refusal with a manual copy.

The [provisioner](../../custom_components/brilliant_mqtt/panel_provisioner.py)
and [panel operations](../../custom_components/brilliant_mqtt/panel_ops.py)
capture the predecessor before mutation. Keep the loaded HA integration pinned
to the reviewed candidate throughout qualification. Avoid concurrent updates,
credential changes or unrelated component changes during the attempt.

Record the actual layout selected by each running service, not merely the
existence of a `current` symlink. Supported targeted updates may preserve a
legacy fixed layout. A surviving release selector does not prove that the unit
executes that release.

For release-link layouts, run the complete
[exact-bundle parity gate](deployment.md#office-exact-bundle-parity-gate): compare
the committed payload, the integration actually loaded by HA, and the panel's
active `panel-release` manifests. Require empty diffs. The `panel-release`
command requires `current` to select one direct child of `releases`; its failure
on a legacy layout must not be suppressed or relabeled as a pass.

For a retained legacy layout, record that the release-link gate is inapplicable
and verify the complete selected component code digests through the supported
identity-admission/verification path. Compare each selected bridge/watchdog
component against the candidate, and separately verify its installed unit,
configuration, version, resource limits and restarted process. The helper's
`installed_identities` hashes actual service-selected trees; identity enrollment
may write its private record. Do not describe enrollment as a read-only probe.
A match for only `bus.py`, `mqttio.py`, or `VERSION` is useful evidence but does
not substitute for complete component parity.

### Collecting legacy component evidence

Run the following from the qualified candidate checkout. The SSH aliases must
already have verified host-key pins, as in the deployment reference. Set
`selected_components` to the reviewed selection; do not omit a failing component
to obtain a passing comparison. The example qualifies all three core services.

First run the repository and loaded-HA **integration and payload** manifest
commands and their two comparisons from the deployment reference. Those legs
apply to both layouts. Omit only its release-link-specific panel command and
panel comparison for an explicitly retained legacy layout. Then collect the
service-selected component identities with the same trusted helper used by
`panel_ops._read_release_identities`:

```bash
set -euo pipefail
legacy_evidence=artifacts/canary/legacy-parity
mkdir -p "$legacy_evidence"
uv run python - "$legacy_evidence" <<'PY'
import json
import runpy
import sys
from pathlib import Path

out = Path(sys.argv[1])
helper = Path("scripts/brilliant-panel/bundle_manifest.py")
api = runpy.run_path(str(helper), run_name="canary_identity")
payload = Path("custom_components/brilliant_mqtt/agent_payload")
selected_components = ("bridge", "wifi_watchdog", "bus_watchdog")
candidate_ordinal = api["release_ordinal"](payload)
assert type(candidate_ordinal) is int and candidate_ordinal > 0, "candidate ordinal required"
expected = {
    component: {
        "digest": api["code_digest"](payload, component),
        "version": (payload / "VERSION").read_text().strip(),
        "release_ordinal": candidate_ordinal,
    }
    for component in selected_components
}
assert all(item["digest"] is not None for item in expected.values())
(out / "expected.json").write_text(json.dumps(expected, sort_keys=True))
# Match the existing integration's helper-loading convention. This source comes
# from the qualified local checkout, never from an unverified panel helper.
source = helper.read_text().rsplit('if __name__ == "__main__":', 1)[0]
source += '\nidentities = installed_identities(Path("/var/brilliant-mqtt"))\n'
source += f'selected = {selected_components!r}\n'
source += '''print(json.dumps({
    name: ({key: identities[name][key]
            for key in ("digest", "version", "release_ordinal", "layout")}
           if identities[name] is not None else None)
    for name in selected
}, sort_keys=True))
'''
(out / "collect-identities.py").write_text(source)
PY

# This can enroll/update private identity records; it does not update code.
ssh office-qualified '/data/switch-embedded/env/bin/python3 -' \
  < "$legacy_evidence/collect-identities.py" \
  > "$legacy_evidence/installed.json"

uv run python - "$legacy_evidence" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
expected = json.loads((root / "expected.json").read_text())
installed = json.loads((root / "installed.json").read_text())
assert installed.keys() == expected.keys(), "component selection differs"
for component, wanted in expected.items():
    actual = installed[component]
    assert actual is not None, f"missing component: {component}"
    assert actual["layout"] in ("legacy_fixed", "release_link")
    assert all(actual[key] == value for key, value in wanted.items()), component
print("Selected component identity, version and ordinal match")
PY
```

Keep the layout report as evidence, and repeat collection and comparison at the
end of soak from the same qualified checkout. Never treat a null/unknown ordinal
as equal to the candidate ordinal. If the compare fails, stop qualification and
investigate the guarded deployment instead of editing the identity record.
These component digests cover code, not the complete installed unit or mutable
configuration: retain the provisioner's verified unit/configuration evidence and
check the actual systemd process and resource limits separately. The collector
prints no environment contents, credentials, deployment ID or private hostname.

## Retained rollback gate

#168 provides a named baseline retained after successful provisioning clears the
execution journal. This mechanism replaces the earlier requirement to invent an
export/re-arm path. Follow the [rollback runbook](../canary-rollback.md) for exact
service targeting, recovery deadlines, restart recovery and finalization.

Before mutation, require a complete durable predecessor baseline. Depending on
layout it uses a verified pinned release, a bounded private archive, or both.
It covers prior code, configuration/CA, units, service state and selectors.
An incomplete capture, missing predecessor code, unsupported correlation or
insufficient storage must refuse the update.

Record the baseline name and `armed` then `soak` state in private evidence. The
baseline has no automatic expiry. An existing retained baseline blocks another
manual update: finish its documented qualification or recovery and explicitly
finalize it before creating a new attempt. Never delete its record or archive
to get past admission.

Exercise recovery with the production recipes in the disposable filesystem and
loopback-broker tests, including interrupted mutation and restart recovery. Then
record the agreed live canary recovery evidence separately. Unit tests, fixture
publishers and simulated systemd state cannot prove hardware recovery.

A successful named rollback requires exact predecessor restoration, followed by
fresh MQTT evidence correlated to a new recovery deployment ID. Allow up to
300 seconds overall, including a 90-second MQTT verification window. The live
deployment-correlation environment field intentionally changes; this exception
prevents stale messages from certifying recovery. Verify the retained state is
`restored`, inspect actual service/code identity, and record recovery time.

Keep the baseline callable through the entire soak. Finalize only after the
qualification decision and recovery/signoff; finalization purges retained
credential-bearing material and is not a harmless status reset. #169 tracks the
ownerless-restored finalization edge case; an affected attempt needs explicit
operator reauthorization, not deletion or long-term credential retention.

## Responsiveness qualification

Use only designated, authorized test loads, record their starting state, and
restore that state after testing. Follow the
[write/restore protocol](../brilliant-panel/validation-runbook.md#5-scalar-writerestore-protocol).
Set the acceptable response-time and reliability thresholds before testing;
report measured results even when they miss those thresholds.

Collect a bounded, timestamped trace for:

1. Sequential ON/OFF and brightness commands, including brightness zero.
2. A rapid slider-like burst with the final requested value recorded. Verify the
   final observed state, intermediate contradictions and time from the last
   request to that state. Do not count API acceptance as completion.
3. Partial and overlapping state/brightness payloads after #159 is included.
   Check OFF ordering and unsupported/non-idempotent barriers using deterministic
   tests; do not issue unsupported commands to a live load as a probe.
4. Another independent target making progress while one target is busy. Use
   synthetic tests for lane death and cancellation; do not kill production
   workers to reproduce #160.
5. Physical/native-slider response when direct observation is available. Without
   that evidence, mark physical latency unmeasured rather than inferred from HA.

Compare like-for-like before/after traces where available. Distinguish panel-local
loads from mesh loads and identify whether the state came from bridge feedback
or a separate physical observation. Record polling resolution, requested values,
final-state accuracy, queue wait, native RPC time, timeout/cancellation counters,
and session-rebuild deltas from the same running process. Counter resets or
stale retained snapshots invalidate simple subtraction.

#157 exposes bounded timing and rebuild diagnostics. #161 improves evidence when
folding fails or pending folds are discarded. Neither proves physical actuation.
Zero systemd restarts does not mean zero MQTT sessions or native-bus failures.
A mean native RPC duration across maintenance and interactive writes is not an
isolated slider-latency measurement.

## Soak, abort and fleet gate

Qualify exactly one panel for at least one day on the **final candidate bytes**.
An earlier build's elapsed soak cannot qualify changed code. Observe process and
session stability, actual MemoryMax/CPUQuota/Nice, memory and CPU pressure,
command outcomes, and identity/rollback retention at the end.

Stop qualification traffic and classify the attempt as failed or inconclusive
if identity drifts, fresh health is absent for 90 seconds, a resource limit is
breached, native UI behavior deteriorates, commands lose intent, final state is
wrong, response thresholds are missed, unexpected restarts/reconnects occur, or
rollback is no longer provable. Investigate the event and use the rehearsed
rollback procedure when recovery is needed. Do not silently waive failures
because retained availability remains online. A new qualification window starts
only after the cause/disposition is recorded and the candidate remains valid.

The first six rows below are prerequisites for publication and starting rollout.
The final two rows apply during rollout and at completion; they cannot be
preconditions for starting the first batch:

| Gate | Required evidence |
| --- | --- |
| Final code and review | Exact combined commit, independent review, both runtime gates, hosted CI |
| Distinct release identity | Consistent agent/integration version, ordinal, regenerated payload and hashes |
| Installed parity | Loaded HA and actual selected panel components match the candidate |
| Recovery | Complete named retained baseline, rehearsal evidence, agreed live recovery verification |
| Responsiveness | Authorized sequential/burst/partial-command results meet predefined thresholds; physical claims independently supported |
| Final-candidate soak | At least one day, with failures investigated and resolved or explicitly blocking |
| Batch advancement | Small batches; per-panel bridge/watchdog identity, restarted process and fresh health verified |
| Completion | Inaccessible or failed panels reported as incomplete; retained baselines finalized only after signoff |

The fleet is not current merely because an update service accepted a request.
Record each panel's actual code/version and health, including selected watchdogs.
Keep release publication, successful canary qualification and completed fleet
rollout as separate recorded outcomes.

## Evidence template

Keep public evidence generic. Store credentials, private topology, panel and
transaction identifiers, raw topics/logs and retained journals only in the
operator's private evidence location. Publish sanitized timing summaries and
path/hash manifests where appropriate.

```text
Decision: GO / NO-GO / ABORTED / INCONCLUSIVE
Timestamp and operator/reviewer:
Candidate commit / version / ordinal:
Repository and loaded-HA manifest digests:
Actual per-component panel layout / digests / versions:
Installed unit and resource-limit verification:
Named baseline state and private evidence reference:
Disposable recovery / live recovery evidence and measured duration:
Starting load state / restored load state:
Predefined response and reliability thresholds:
Sequential / burst / partial-command observations:
API acceptance / HA state / independent physical observation times:
Queue-wait / RPC / timeout / session-rebuild deltas and process identity:
Physical slider latency: measured evidence reference / unmeasured
Final-candidate soak start / end / duration:
Failures, disposition and renewed qualification window:
Fleet per-panel completion record / outstanding panels:
Release publication / finalization decision:
```

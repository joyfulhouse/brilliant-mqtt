# Release qualification after rollout

This runbook records the live qualification still owed for a release that is
already published and installed, and the procedure for completing it. It
complements the [deployment procedure](deployment.md),
[validation evidence levels](../brilliant-panel/validation-runbook.md#evidence-levels)
and [retained rollback procedure](../canary-rollback.md).

A software-health soak, a reported Home Assistant state change and a physically
observed light response are different measurements. Record each separately. A
bus acknowledgement or a retained MQTT `online` message does not establish
physical actuation or native panel-slider responsiveness.

## Current release: 0.11.3

[`v0.11.3`](https://github.com/joyfulhouse/brilliant-mqtt/releases/tag/v0.11.3)
(release ordinal 5, voice payload 0.1.1) is the latest stable release and is
installed on every panel. Its contents are in the [changelog](../../CHANGELOG.md).
Earlier versions of this runbook targeted 0.11.2 and listed gates to satisfy
before publication. Publication and rollout have already happened, so those
gates now record what was met rather than control a decision:

| Gate | Status for 0.11.3 |
| --- | --- |
| Final code and review | Met: reviewed combined commit, both runtime gates and hosted CI |
| Distinct release identity | Met: version 0.11.3 with ordinal 5, admitted normally over ordinal 4 |
| Installed parity | Met: the published HACS zip and the loaded integration matched the reviewed manifests; pilot component digests matched |
| Final-candidate soak | Met: more than one day on one pilot with matching digests, unchanged service processes and no automatic restarts |
| Rollback rehearsal | **Outstanding**: retained baselines exist, but no live rollback has been exercised |
| Responsiveness | **Outstanding**: no sequential, burst or partial-command measurements |
| Fleet completion | Met: every panel reports 0.11.3 and fresh health; one update needed retries after connection failures (#219) |
| Baseline finalization | **Outstanding**: every panel still retains its predecessor baseline |

The fleet was updated before the rollback and responsiveness rows were complete.
The remaining work therefore qualifies an installed release. If it fails, the
response is a targeted rollback of the affected panels and a fix in a new
release, not a decision about whether to deploy.

## Verify installed identity

Before and after each qualification window, confirm that the panel under test
still runs the release. For the Home Assistant integration, run the
[exact-bundle parity gate](deployment.md#office-exact-bundle-parity-gate)
against the loaded integration. On a release-link panel layout, also run its
`panel-release` leg. That command requires `current` to select one direct child
of `releases`; its failure on a legacy layout must not be suppressed or relabeled
as a pass.

Panels updated through the supported single-panel update keep their legacy fixed
layout. For those, record that the release-link leg does not apply and compare
the complete selected component code digests instead. Separately check each
component's installed unit, version, resource limits and running process. A
match for only `bus.py`, `mqttio.py` or `VERSION` does not substitute for
complete component parity.

### Collecting component identities

Run the following from the qualified candidate checkout. The SSH aliases must
already have verified host-key pins, as in the deployment reference. Set
`selected_components` to the panel's actual core selection (Wi-Fi watchdog only
where it is installed); do not omit a failing component to obtain a passing
comparison. The example checks all three core services.

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
end of each qualification window from the same qualified checkout. Never treat a null/unknown ordinal
as equal to the candidate ordinal. If the compare fails, stop qualification and
investigate the guarded deployment instead of editing the identity record.
These component digests cover code, not the complete installed unit or mutable
configuration: retain the provisioner's verified unit/configuration evidence and
check the actual systemd process and resource limits separately. The collector
prints no environment contents, credentials, deployment ID or private hostname.

## Rollback rehearsal

Rehearse on one designated panel, using the [rollback runbook](../canary-rollback.md)
for exact service targeting, deadlines, restart recovery and finalization. The
rehearsal temporarily returns that panel to its predecessor release.

1. Confirm the panel's current identity as above, and record its retained
   baseline name and `soak` state from diagnostics in private evidence.
2. **Turn off automatic repair on that panel** for the whole rehearsal. Repair
   admits a newer release ordinal and does not check for a retained baseline,
   so it can upgrade the rolled-back panel again without capturing a new
   baseline. For the same reason, do not press the panel's **Repair** button,
   run the `brilliant_mqtt.repair` action against that panel or the fleet entity
   (which selects every panel), or turn on a watchdog component there until
   step 6.
3. Run the single-target `brilliant_mqtt.canary_rollback` action with the
   recorded name. Allow up to 300 seconds overall, including the 90-second
   MQTT verification window. Record the measured duration.
4. Confirm diagnostics show `restored` with a new `recovery_deployment_id`.
   Then confirm the panel's component identities match its predecessor
   (version, ordinal and digests) and that its services are running with fresh
   health. A retained `online` message does not count.
5. Any retained baseline for a panel blocks its next update, including a
   `restored` one. To return the panel to the current release, explicitly run
   `brilliant_mqtt.canary_finalize` for the restored baseline. Then update the
   panel again through the supported single-panel update, which captures a new
   predecessor baseline.
6. Verify the panel's identity matches the current release again, then restore
   its automatic-repair setting.

The disposable filesystem and loopback-broker tests in `ha/tests` exercise the
same recipes, including interrupted mutation and restart recovery. They do not
replace this live rehearsal.

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
3. Partial and overlapping state/brightness payloads. Check OFF ordering and unsupported/non-idempotent barriers using deterministic
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

## Finalizing baselines and handling failures

Keep every panel's retained baseline until the rehearsal and responsiveness
results are recorded and signed off. Finalization purges retained
credential-bearing material and removes that panel's recovery path; it is not a
status reset. Finalize each panel explicitly by its baseline name. #169 tracks the
ownerless-restored finalization edge case, which needs explicit operator
reauthorization rather than deletion.

Classify an attempt as failed or inconclusive if identity drifts, fresh health is
absent for 90 seconds, a resource limit is breached, native UI behavior
deteriorates, commands lose intent, final state is wrong, response thresholds are
missed, unexpected restarts or reconnects occur, or rollback is no longer
provable. Record the cause, roll back affected panels where needed, and start a
new window only after the disposition is recorded. Do not waive a failure because
retained availability remains online.

## Next release

Complete the rollback and responsiveness work on one panel before updating
others. Give the release a distinct semantic version and the next release
ordinal. HACS installs this integration from the release zip, so it cannot load
an unreleased commit. To canary a build before it becomes the latest release,
publish it as a non-latest prerelease at the reviewed commit.

## Evidence template

Keep public evidence generic. Store credentials, private topology, panel and
transaction identifiers, raw topics and logs, and retained journals only in the
operator's private evidence location. Publish sanitized timing summaries and
path/hash manifests where appropriate.

```text
Decision: PASS / FAIL / ABORTED / INCONCLUSIVE
Timestamp and operator/reviewer:
Release tag / commit / version / ordinal (current: v0.11.3 / 5):
Loaded-HA integration and payload manifest digests:
Panel layout / per-component digests / versions, before and after:
Installed unit and resource-limit verification:
Retained baseline state and private evidence reference:
Automatic repair held during rehearsal: yes / no
Rollback duration / restored identity / fresh-health evidence:
Return to current release: finalize and update times, new baseline state:
Starting load state / restored load state:
Predefined response and reliability thresholds:
Sequential / burst / partial-command observations:
API acceptance / HA state / independent physical observation times:
Queue-wait / RPC / timeout / session-rebuild deltas and process identity:
Physical slider latency: measured evidence reference / unmeasured
Failures, disposition and renewed qualification window:
Baseline finalization decision and per-panel record:
```

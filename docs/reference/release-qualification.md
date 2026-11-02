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

## Current release: 0.11.3 agent, 0.11.4 integration

[`v0.11.3`](https://github.com/joyfulhouse/brilliant-mqtt/releases/tag/v0.11.3)
(release ordinal 5, voice payload 0.1.1) is the agent release installed on
every panel. Its contents are in the [changelog](../../CHANGELOG.md).
[`v0.11.4`](https://github.com/joyfulhouse/brilliant-mqtt/releases/tag/v0.11.4)
is a Home Assistant integration release only: it bundles the same 0.11.3 agent
payload with release ordinal 5, so it neither changes nor updates panels.
Earlier versions of this runbook targeted 0.11.2 and listed gates to satisfy
before publication. Publication and rollout have already happened, so those
gates now record what was met rather than control a decision:

| Gate | Status for 0.11.3 |
| --- | --- |
| Final code and review | Met: reviewed combined commit, both runtime gates and hosted CI |
| Distinct release identity | Met: version 0.11.3 with ordinal 5, admitted normally over ordinal 4 |
| Installed parity | Met: the published HACS zip and the loaded integration matched the reviewed manifests; pilot component digests matched |
| Final-candidate soak | Met: more than one day on one pilot with matching digests, unchanged service processes and no automatic restarts |
| Rollback rehearsal | Met: live on the designated pilot on 2026-10-26, from the legacy fixed layout. `canary_rollback` took 105 s (limit 300 s) and restored the predecessor's component digests exactly; the panel then returned to 0.11.3 through finalize and the supported update |
| Responsiveness | **Outstanding**: the 2026-10-26 run on a mesh load is recorded as **FAIL / confounded**, because native peer forwarding was failing (`NoConnectionError`) during it. Repeat it on the mesh load and on a like-for-like panel-local load once the peers are restored |
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
as a pass. Panels on the [legacy fixed layout](#legacy-fixed-layout) use the
[component identity comparison](#comparing-component-identities) instead.

### Legacy fixed layout

A panel on the legacy fixed layout keeps its code in `/var/brilliant-mqtt/app`,
`vendor`, `wifi_watchdog` and `bus_watchdog`, with no `current` link. Manual
installs and panels updated through the supported single-panel update are in
this state. Two integration paths can upgrade such a panel, and they are
qualified differently.

**(a) Update entity and `brilliant_mqtt.redeploy`: qualified.** The supported
single-panel update and the redeploy action keep the fixed layout. Before they
change code they capture a journaled predecessor baseline under
`/var/brilliant-mqtt/.rollback/<id>/`, which `brilliant_mqtt.canary_rollback`
restores (see the [rollback runbook](../canary-rollback.md)). This is the path
the 2026-10-26 rollback rehearsal exercised.

**(b) Onboarding conversion: unqualified.** Adding a panel through onboarding
runs `PanelProvisioner.async_install`, the journaled conversion from the fixed
layout to `current`/`releases`. The flow reaches it from `_async_provision`
(`custom_components/brilliant_mqtt/flow/onboarding.py:604`). It runs only if
all of the following hold:

- The panel is not already configured. Onboarding looks up the panel's SSH
  identity with `_duplicate_panel` (`flow/onboarding.py:260`) and aborts with
  `already_configured` (`flow/onboarding.py:319`). The provisioner repeats the
  check as `duplicate_panel` (`panel_provisioner.py:1129`).
- The installed code differs from the bundle. Otherwise release admission is a
  no-op and the install stops with `release_unchanged`
  (`panel_provisioner.py:1168-1169`), which the flow shows as an abort
  (`flow/onboarding.py:462-463`).
- Every selected component changes. Otherwise it stops with
  `release_selection_conflict` (`panel_provisioner.py:1170-1171`).

When it runs, it snapshots the panel and journals the operation as `UPGRADE`
for any snapshot that is not `ABSENT` (`panel_provisioner.py:1219-1223`),
stages the release with `stage_release` (`panel_provisioner.py:1266`), and
activation repoints `current` at the staged release (`activate_staged`,
`panel_provisioner.py:1338`; the link swap is `panel_ops.py:1706-1709`). Rolling
back a converted legacy panel restores the saved fixed-layout files and removes
`current` (`panel_ops.py:1906`). Off-panel coverage is partial: the tests in
`ha/tests` exercise parts of this sequence with fakes, not a complete
legacy-to-release conversion and rollback on a real tree, and it has not been
rehearsed on a panel. Use path (a) for qualified upgrades of legacy panels.

Never hand-create a `current` link to make a legacy panel pass the
`panel-release` leg. A link the integration did not stage is not a release,
and it changes which code the services run.

### Which checkout to use

Run the integration and payload parity legs from a checkout of the
[`v0.11.4`](https://github.com/joyfulhouse/brilliant-mqtt/releases/tag/v0.11.4)
tag, because they must match the loaded integration byte for byte. The
`payload-identities` and `component-identities` modes of
`scripts/brilliant-panel/bundle_manifest.py` are newer than that tag. Run them
from a checkout that contains them, and point `payload-identities` at the
v0.11.4 payload. This is valid because an identity covers only the core code
trees, `VERSION` and `RELEASE_ORDINAL`, none of which a newer helper changes.

### Comparing component identities

Set `components` to the panel's actual core selection (Wi-Fi watchdog only
where it is installed); do not omit a failing component to obtain a passing
comparison. The example checks all three core services. The SSH alias must
already have a verified host-key pin, as in the deployment reference.

```bash
set -euo pipefail
legacy_evidence=artifacts/canary/legacy-parity
manifest_helper=scripts/brilliant-panel/bundle_manifest.py
# The v0.11.4 payload, for example from `git worktree add ../brilliant-mqtt-v0.11.4 v0.11.4`.
release_payload=../brilliant-mqtt-v0.11.4/custom_components/brilliant_mqtt/agent_payload
components="--component bridge --component wifi_watchdog --component bus_watchdog"
mkdir -p "$legacy_evidence"

uv run python "$manifest_helper" payload-identities "$release_payload" $components \
  > "$legacy_evidence/expected.tsv"

ssh -o NumberOfPasswordPrompts=1 office-qualified \
  "/data/switch-embedded/env/bin/python3 - component-identities /var/brilliant-mqtt $components" \
  < "$manifest_helper" \
  > "$legacy_evidence/installed.tsv"

cut -f1-4 "$legacy_evidence/installed.tsv" > "$legacy_evidence/installed-identity.tsv"
diff -u "$legacy_evidence/expected.tsv" "$legacy_evidence/installed-identity.tsv" \
  > "$legacy_evidence/identity.diff" || {
    echo "STOP: installed component identity differs" >&2
    exit 1
  }
```

`payload-identities` prints the component, version, ordinal and digest for each
selected component, separated by tabs. It fails if the payload has no
`RELEASE_ORDINAL` or lacks a selected component. `component-identities` prints
the same four fields followed by the layout (`legacy_fixed` or
`release_link`), or the component followed by `missing`, in the order of the
`--component` arguments. `cut -f1-4` drops the layout so the two files compare
directly; keep `installed.tsv` as the layout evidence.

### Read-only collection and manual installs

`component-identities` is strictly read-only. It consults the panel's existing
private identity records but never creates or updates them and never creates a
directory, so it is safe to run over SSH on a production panel. It prints an
ordinal only when an existing record proves it for exactly the installed code;
otherwise it prints `unknown`, which never equals a candidate ordinal. A legacy
or manual install whose ordinal the integration never enrolled therefore shows
`unknown` even when its code digests match. That is expected, and it is not a
pass: record it, and let the supported update or redeploy enroll the identity.
Do not edit identity records by hand.

Repeat collection and comparison at the end of each qualification window. If
the comparison fails, stop qualification and investigate the guarded
deployment instead of editing the identity record. These component digests
cover code, not the complete installed unit or mutable configuration: retain
the provisioner's verified unit and configuration evidence, and check each
component's installed unit, version, resource limits and running process
separately. A match for only `bus.py`, `mqttio.py` or `VERSION` does not
substitute for complete component parity. Both modes print no file contents,
environment values, credentials, deployment ID or paths, and report an
unreadable installed file only by its error type.

## Rollback rehearsal

Rehearse on one designated panel, using the [rollback runbook](../canary-rollback.md)
for exact service targeting, deadlines, restart recovery and finalization. The
rehearsal temporarily returns that panel to its predecessor release.

1. Confirm the panel's current identity as above, and record its retained
   baseline name and `soak` state from diagnostics in private evidence.
2. Expect repair to leave the rolled-back code alone. While the panel has a
   retained baseline in any state, automatic repair, the **Repair** button, the
   `brilliant_mqtt.repair` action (including against the fleet entity, which
   selects every panel), post-OTA refresh and watchdog enablement never install
   or replace its bridge or watchdog code. They may still restore the installed
   release's units, configuration and services. Where code would be needed they
   refuse with `baseline_retained_finalize_first`. Repair never upgrades installed
   code without a baseline either. Automatic repair can still restart services
   during the rehearsal, so turning it off on that panel until step 6 keeps
   timing and health evidence attributable to the rollback. Record whether you
   did.
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
   its automatic-repair setting if you changed it.

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

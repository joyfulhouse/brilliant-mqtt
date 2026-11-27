# Canary rollback: operator procedure

This procedure applies to the retained canary baseline created by fleet
provisioning or a manual agent update/redeploy. Use Home Assistant's existing
Brilliant MQTT integration. No shell commands, panel credentials, or direct MQTT
publishes are needed for these operations.

## Before the canary

1. Record the panel's HA entity/device and the reviewed candidate identity. A
   version string alone does not distinguish builds. Resolve an identity refusal
   by selecting a reviewed newer ordinal or explicitly reviewing the exact
   single-operation `release_override` shown by `redeploy`. Never put an override
   into an automation or repair configuration.
   Onboarding/migration uses the same admission policy before capture or staging.
   If refused, review the displayed binding and explicitly paste it into the
   optional release-override field to authorize that one transaction. An identical
   payload ends with no staging or layout change. A migration with a mixture of
   unchanged and differing components is refused so unchanged selectors survive;
   use the targeted component management path instead.
2. Run the targeted update. Before any CA, config, or code mutation, the integration
   captures a complete baseline and durably records `armed`. Capture fails closed
   if code is missing, deployment correlation is unsupported, the prior bridge
   cannot run, storage is insufficient, or a bound is exceeded. An existing
   retained baseline must be finalized before another manual update is admitted.
3. In Settings > Devices & services > Brilliant MQTT, download diagnostics. In
   `provisioning.retained`, record the exact `name` (transaction UUID), `panel`, and
   `state`. Give these three values and the targeted entity to the second operator.
   Do not distribute the integration's private storage files or on-panel archive.
4. A completed update retains `soak` even though the execution journal is cleared.
   Leave it retained for the full canary observation period. There is no automatic
   expiry or background reaper. Legacy HA mirror retirement and Hue CA changes are
   deferred while the core baseline is retained. Repair (automatic, button, or
   `brilliant_mqtt.repair`), post-OTA refresh and component enablement never change
   the panel's bridge or watchdog code while any baseline for it is retained, in
   any state including `restored`. They still restore the installed release's
   units, configuration and services, and refuse with
   `baseline_retained_finalize_first` when code would be needed.

## Roll back now

1. Confirm the affected panel and baseline UUID from diagnostics. Keep its current
   HA configuration and host-key pin available; recovery resolves the root
   credential from that configuration. Do not delete/recreate the integration,
   change broker credentials, or run a concurrent update during recovery.
2. In Developer tools > Actions, select **Brilliant MQTT: Roll back retained
   canary**, target exactly one entity on that panel, and supply the recorded name:

   ```yaml
   action: brilliant_mqtt.canary_rollback
   target:
     entity_id: binary_sensor.brilliant_office_bridge_health
   data:
     name: "12345678-1234-4abc-8def-1234567890ab"
   ```

   Replace both example values with the handoff values. Untargeted and multi-panel
   calls are refused. The integration verifies the named record's panel ownership.
3. Allow up to **300 seconds overall**, including a maximum **90-second MQTT
   verification window**. A stale retained `online` message is insufficient. The
   integration verifies exact restored code, CA, configuration bytes/modes, units,
   service state, and layout, then restarts with a newly generated recovery
   deployment ID. Only fresh availability, matching metadata, state, and discovery
   from that attempt authorize success.
4. Download diagnostics again. Success is `state: restored` with a new
   `recovery_deployment_id`. The execution journal is cleared only after this
   evidence is durably saved. Confirm normal panel behavior during the agreed soak;
   MQTT verification does not prove physical slider latency or electrical behavior.
5. If HA restarts or the action is interrupted, allow the existing recovery runner
   to reconcile durable intent. A restore operation is explicitly journaled as
   rollback; it is never recovered as an unactivated candidate cleanup. Retrying a
   failed rollback uses another fresh deployment ID.

Restoration is byte-exact **except for the deployment-correlation field in the
live environment file**. That field must change to reject delayed messages from
the prior deployment. The captured deployment ID remains provenance in the
baseline; it is never reused as recovery evidence. A first-install undo restores
verified absence and does not claim a predecessor MQTT session.

## If recovery fails

Keep the baseline, journal, and HA panel configuration. Do not delete private
files to silence a repair issue, and do not finalize a failed recovery. Repairs
surface stable failure codes; remote stderr and raw stored dictionaries are not
operator output. Diagnostics identify the active journal's `name`, `panel`,
`operation`, `phase`, and allowlisted `last_error` when present, independently of
whether that panel has an HA owner. `last_error: null` means no error was saved;
it does not mean recovery succeeded. Releases after 0.11.3 save every recovery
failure before durable restore intent with stage `recovery` and one of the codes
below, so a stuck journal carries a reason after its next attempt. `unknown`
means a token was deliberately withheld. Older failures cannot be reconstructed
from phase alone. Preserve the journal and baseline
while obtaining redacted diagnostics and the HA repair/log code.

| Code/state | Operator action |
| --- | --- |
| `baseline_capture_failed`, `baseline_verify_failed` | Candidate mutation is refused. Preserve the incumbent; investigate missing/drifting code, permissions, free space, or capture bounds before retrying. |
| `baseline_capture_interrupted` | A capture re-check failed after publication, and removing its unused baseline also failed. Preserve artifacts and follow Interrupted baseline capture. |
| `transaction_in_progress` | Another operation holds the shared panel lock. Allow it to finish and retry. |
| `provisioning_recovery_required` | Another panel's interrupted provisioning journal must be recovered first. Diagnostics name it (`provisioning.name`, `panel`, `phase`, `last_error`). Resolve that journal's own code below; for an onboarding that never created an HA panel, see [Interrupted onboarding without an HA panel owner](#interrupted-onboarding-without-an-ha-panel-owner). Operations stay serialized; nothing is skipped. |
| `recovery_connection_failed` | Recovery could not reach the panel at the journal's recorded address. Bring the panel online. If its address or root password changed and it has no HA panel entry, settle it with Add Brilliant panel (below); otherwise repair the owning panel's address/credentials. |
| `panel_authentication_failed` (stage `recovery`) | The recorded root password was rejected. Same remedy as above, with the current password. |
| `panel_identity_mismatch` (stage `recovery`) | The host no longer proves the journal's pinned SSH identity. Do not trust a new key automatically; confirm the physical panel and address before any further action. |
| `recovery_artifact_invalid` | The journal's recorded candidate or snapshot cannot be reconstructed. Preserve everything and send redacted diagnostics to the maintainer. |
| `baseline_retained_finalize_first` | A rollback baseline remains for this panel. Keep it through the agreed soak. After sign-off, explicitly run Finalize retained canary with its transaction name before another update, or roll back; rejection leaves the baseline intact. Repair, post-OTA refresh and component enablement report the same code instead of installing or replacing bridge or watchdog code while the baseline is retained. |
| `release_update_required` | Repair, post-OTA refresh and component enablement keep installed code and only install code that is missing. Change it with the canaried Update entity or `brilliant_mqtt.redeploy`. |
| `rollback_credentials_unavailable` | Restore the exact HA panel owner/configuration and its pinned host identity; then retry. Do not enable automatic host-key trust. |
| `rollback_health_failed` | Bytes may already be restored. Check HA's broker connection and panel connectivity. Retained MQTT history cannot satisfy this check. Retry after resolving connectivity. |
| `rollback_deadline_exceeded` | Treat recovery as unverified. The pending journal remains; remote restore processes are terminated and settled before reuse. Resolve connectivity/storage delays before retrying. |
| `rollback_failed` / `rollback_failed` state | Preserve artifacts and the actionable repair code for the maintainer. A missing or changed pinned release is not silently replaced with bundled code. |

## Finalize only after sign-off

Finalization intentionally and permanently removes this recovery guarantee.
After the second operator confirms the canary result (or completed rollback),
invoke **Brilliant MQTT: Finalize retained canary**, with the same single target
and name:

```yaml
action: brilliant_mqtt.canary_finalize
target:
  entity_id: binary_sensor.brilliant_office_bridge_health
data:
  name: "12345678-1234-4abc-8def-1234567890ab"
```

`finalizing` is saved before deleting artifacts. If HA restarts, the existing
recovery runner finishes deletion. Download diagnostics and confirm the name no
longer appears. Successful finalization synchronously purges the named HA retained
record and the on-panel archive and `complete.json`; no deferred garbage collection
is required. Finalize promptly at the agreed soak end to bound the extra credential
storage window. Until explicitly finalized, the baseline has no automatic expiry.
An in-flight execution journal prevents finalization. Uninstall
also refuses to destroy a retained baseline; finalize intentionally first.

## Changing a panel's network address with a retained baseline

The current integration does not move a retained baseline when **Change network
address** updates a panel subentry. The baseline keeps the address recorded at
capture. Finalize and rollback require that address, the panel slug, and the
pinned SSH identity to match the current panel configuration. This limitation is
tracked in [issue #259](https://github.com/joyfulhouse/brilliant-mqtt/issues/259).
The steps below are a manual workaround for the same physical panel with its
existing HA panel subentry and unchanged pinned identity.

Before either procedure, download fresh diagnostics. Require
`provisioning.count: 0`, no `provisioning.storage_error`, and a readable named
retained row in `state: soak` or a previously verified `state: restored`.
`count: 0` alone does not rule out pending recovery: `restore_requested` and
`finalizing` can exist without an execution journal and resume during
reconciliation. For those states, `rollback_failed`, any other state, unreadable
diagnostics, or an active journal, preserve the records and follow
[If recovery fails](#if-recovery-fails) before changing addresses or starting
another action. A verified `restored` row needs finalization after sign-off,
rather than another rollback. Do not switch a pending finalization to rollback.

### Before a planned address or DHCP change

1. While the panel is still reachable at its recorded address, download
   diagnostics and identify its baseline in `provisioning.retained` by `panel`
   and `name`. Wait until the agreed canary observation and sign-off are complete.
2. Settle that baseline using the existing actions above, with exactly one panel
   target and the recorded transaction name: use
   `brilliant_mqtt.canary_finalize` to accept the current installation, or
   `brilliant_mqtt.canary_rollback` to restore the prior installation. A successful
   rollback leaves a `restored` baseline; after reviewing the restored panel and
   signing off, finalize it as well. Do not finalize a failed recovery.
   Rolling back a first install with no prior agent removes the new agent and
   verifies its absence; it does not restore a running agent or produce a new
   `recovery_deployment_id`. Finalize instead if accepting that new installation.
3. Download diagnostics again and confirm the transaction name no longer appears
   in `provisioning.retained` and no execution journal remains
   (`provisioning.count: 0`). Only then change the router/DHCP assignment. Once
   the same panel is reachable at the new address, open its subentry under
   Settings > Devices & services > Brilliant MQTT and choose **Change network
   address**. Enter the new address; the flow verifies the existing pinned
   identity before saving it. This flow updates HA's address setting; it does not
   assign an address on the panel or router.

### If the address already changed

After a successful **Change network address**, an older retained baseline can
still prevent both canary actions. The action error is exactly:

```text
canary_operation_failed: see docs/canary-rollback.md
```

A code-changing agent update that reaches the retained-baseline guard is refused
with `baseline_retained_finalize_first`. Redeploying an identical bundle can
return successfully before that guard; it does not settle the baseline. No update
attempt is needed to recognize the address-change problem. The generic canary
error can also have other causes; it does not establish an address mismatch on
its own.

Diagnostics show the current subentry address in
`panels.<panel-id>.configuration.address`. The `provisioning.retained` list shows
`name`, `panel`, `state`, and `recovery_deployment_id`, but **does not expose the
retained baseline's host**. After this address change, the current address can
therefore be the new one while the baseline internally still refers to the old
one. Use your prior network records to identify the exact previous address;
do not inspect or edit private storage to find it.

1. Preserve the integration and panel subentry. Check the diagnostic count,
   storage status, and named retained state as described above before restoring
   network reachability or changing the subentry. Pending intent can resume as
   soon as the recorded host and matching HA owner become usable again.
2. If the baseline recorded an IP address, use the router or DHCP reservation to
   give the same physical panel that previous IP again. First confirm it is not
   assigned to another device. If the baseline recorded a hostname, make that
   exact hostname resolve from HA to the same panel at its current IP; reclaiming
   the old numeric IP is unnecessary. If the subentry already uses that hostname
   and it still resolves to the panel, a DHCP change alone does not create this
   host-string mismatch. If the recorded host cannot be made reachable at this
   panel, ask for help on #259.
3. In the existing panel subentry, choose **Change network address** and enter
   the exact previous address or hostname used at baseline capture. Even if HA
   already has that address saved, the panel must be reachable there. If the
   pinned identity check fails, stop and ask for help; do not use **Replace
   physical panel** to bypass it.
4. Run finalize or rollback using the single target and transaction name as
   described above. If rolling back, wait for verified restoration, review the
   result, and finalize only after sign-off. Confirm the retained name has gone
   and `provisioning.count` is `0` before proceeding.
5. If you temporarily restored an old IP, move the panel to the new address in
   the router/DHCP configuration. Once the intended address or hostname reaches
   the panel, use **Change network address** again to save it in the subentry.
   No DHCP move is needed if the original hostname already reaches the panel
   at the intended IP.

Do not manually edit HA `.storage` files, rewrite the retained host, remove
baseline artifacts, or delete/recreate the integration or panel subentry while a
baseline is retained. If the recorded host is unreachable or unknown, the identity
check fails, an action fails, or the retained record remains after finalization,
preserve the records and request help on
[#259](https://github.com/joyfulhouse/brilliant-mqtt/issues/259). Include the
integration version, redacted diagnostics, exact error text, and the attempted
step. Review diagnostics before sharing; do not attach private storage files.
This procedure does not resolve #259's underlying address-change limitation.

## Storage, limits, and evidence

- Release-link predecessors are verified and pinned in their existing immutable
  release directory with a retention marker. Cleanup refuses to delete a pinned
  release. Independently selected fixed trees and mutable config are archived.
- Legacy predecessors use one persistent `/var/brilliant-mqtt/.rollback/<id>/`
  archive. Completion is atomically published and fsynced before mutation. Initial
  tunable capture bounds are 32 MiB expanded, 4096 entries, 120 seconds, and a
  32 MiB free-space reserve. Partial captures never authorize mutation.
- HA stores passive retained metadata separately from the execution journal using
  its existing private atomic Store. The retained record omits `root_password`.
  The temporary execution journal still needs that credential for crash recovery.
  Interrupted onboarding compensation resumes from that matching active journal,
  even before the HA panel owner has been created. Its distinct onboarding rollback
  operation preserves this authority across a restart. Post-soak named rollback
  still requires the current matching HA owner; retained storage gains no root password.
  Byte-exact environment recovery also duplicates the **live MQTT broker credential**
  in the retained HA `.storage` record and the on-panel archive during soak. The
  on-panel `complete.json` is private capture metadata (hashes, paths and identity),
  not raw environment bytes; it is purged alongside the archive. Thus the widened
  at-rest footprint includes `root_password` in the temporary execution journal
  and the live MQTT broker credential in both that journal and the retained baseline.
  Secret-bearing files and `complete.json` have mode `0600`; on-panel capture
  directories have mode `0700`. These copies are necessary for crash-safe recovery.
  Private permissions and redacting representations are not encryption: theft of
  HA storage or the panel archive requires rotation of exposed broker credentials
  (and the root password if the execution journal/configuration was stolen), then
  revalidation of the panel owner, host pin, and broker connection.
- The operator service targeting, complete restore, interrupted restore/finalize,
  stale MQTT rejection, and reconnect failure are exercised in
  `ha/tests/test_canary_rollback.py`. `ha/tests/test_canary_broker.py` executes the
  production filesystem/process recipes with a disposable loopback `amqtt` broker
  and a real MQTT disconnect/reconnect. The publisher and systemd state are test
  fixtures; this is not a hardware, production-broker, or physical-actuation claim.

Maintainer rehearsal (from the repository root):

```sh
umask 0022
PYTHONDONTWRITEBYTECODE=1 uv run --project ha pytest -s -c ha/pyproject.toml \
  ha/tests/test_canary_rollback.py ha/tests/test_canary_broker.py
```

The broker test uses `uv` to run pinned `amqtt==0.11.3` in a disposable Python 3.10
environment, listens only on loopback at an OS-assigned port, and shuts down the
process after the test. Sanitized measured recovery/reconnect durations are printed
by the test. Those measurements are rehearsal evidence, not a production SLA.

## Restored baseline without an HA panel owner

An interrupted Add Brilliant panel operation can successfully restore its prior
state before any panel subentry was created. Its verified `restored` baseline
still protects that result. To intentionally remove that guarantee, start **Add
Brilliant panel** and enter the panel's current address and root password.
Home Assistant matches the retained address or SSH identity and compares both
stored identity fields before offering the fresh password. A moved address works
when it presents the same retained identity. A conflicting identity is refused.

After authentication, an explicit confirmation names the transaction, host and
fingerprint and explains the permanent loss. Cleanup proceeds only while no
configuration entry owns the pin or transaction and no execution journal exists.
It accepts verified restored onboarding baselines, including a verified
first-install undo to absence. It also accepts one narrower case: an `armed`
onboarding baseline whose phase is still `staged`, whose recovery deployment
id is empty, and which has no execution journal and no panel subentry. That
is the residue of a journal save that failed before candidate staging.
Confirming it re-reads the panel, requires the live owned files to match the
capture, requires `check_restored` to succeed, and requires this transaction's
release directory to be absent. Only then does it delete the capture and the
Home Assistant baseline. It still refuses armed baselines that have left
`staged`, soaking, failed, or unverified recovery states. Successful cleanup
continues normal panel setup; it does not automatically install a release.

Fresh login credentials remain in the active flow only and are never written to
retained metadata. If cleanup is interrupted after durable intent, its
`ownerless_restored` finalization marker preserves that provenance. Background
recovery does not reuse stored credentials to finish this operation. Start Add
Brilliant panel again, authenticate freshly and confirm again. Unmarked older
`finalizing` records are not accepted through this path because their original
state cannot be proven; preserve those records for investigation.

## Interrupted baseline capture

`baseline_capture_interrupted` means the post-capture snapshot failed or changed
and the bounded attempt to remove that capture's unused baseline also failed.
No candidate mutation was authorized. The baseline may exist without an HA
retained record, so the named `canary_finalize` action cannot necessarily settle
it. Uninstall can refuse with `baseline_retained_finalize_first`.
If the operation is cancelled and cleanup also fails, cancellation propagates
and a sanitized `baseline_capture_interrupted` warning reports the possible
stranded baseline. Preserve any existing journal and baseline in this case too.

Restore connectivity, preserve the installation, and collect redacted HA
diagnostics and read-only service status for maintainer assistance. Settle any
existing recovery journal through its supported HA recovery flow. Do not delete
baseline directories, release pins, or HA storage, and do not uninstall while
recovery remains unresolved. A retry is not guaranteed to remove an orphaned
capture; automated orphan cleanup is outside this change.

## Interrupted onboarding without an HA panel owner

If Add Brilliant panel is interrupted after activation but before Home Assistant
created the panel entry, the execution journal stays at an onboarding phase such
as `pending_config_commit` with an `armed` baseline. Automatic recovery rolls it
back using the address and root password recorded at the time. When that
recorded access no longer works (for example the panel's DHCP address or root
password changed), recovery fails with a typed code (`recovery_connection_failed`,
`panel_authentication_failed`) on every restart, and the journal blocks other
panels' updates and canary actions with `provisioning_recovery_required`.

Settle it with fresh authority, not by deleting storage:

1. Download diagnostics. Note `provisioning.name`, `panel`, `phase` and
   `last_error`, and confirm the retained baseline for that name is `armed`,
   `restore_requested` or `rollback_failed` (or `restored` while the journal is
   still `rollback_pending`; see below).
2. Start **Add Brilliant panel** and enter the panel's *current* address and
   root password. Home Assistant first fetches the host key and requires it to
   match both the journal's pinned key and fingerprint before the password is
   used. A different identity is refused with `panel_identity_mismatch`.
3. The **Roll back interrupted panel onboarding** step names the transaction,
   panel, host, fingerprint, phase and last error. Confirm it. Home Assistant
   restores the panel's recorded prior state from the journal (for a first
   install, the agent is removed and the panel returns to verified absence). The
   candidate identity comes only from the journal; the currently bundled release
   is not used, so upgrading the integration in between is safe.
4. On success the journal is cleared, the baseline is `restored`, and the flow
   continues with the existing explicit **Finalize restored onboarding baseline**
   confirmation (above), then normal panel setup.

This path is offered only while no HA configuration entry owns the pin or
transaction, no flow owns the transaction, and the journal is an install or
upgrade (or its own compensation) whose baseline is not soaking or finalizing,
and not `restored` before rollback began. Committed, soaking, managed-update and owner-requested rollback
journals are never eligible. Nothing is finalized, skipped or erased unless
restoration is verified. A failure leaves the journal and baseline in place with
a typed `last_error`; fix the cause and repeat.

The fresh password replaces the recorded one only inside the private execution
journal, so a Home Assistant restart during the rollback can resume it; it is
never written to retained baseline metadata and is cleared with the journal. If
Home Assistant restarts mid-rollback, the background runner retries with the
journal's recorded address. If that address is still unreachable, repeat steps
2-3. The baseline then reads `restore_requested` or `rollback_failed`, which this
path accepts.

If Home Assistant stopped after the restore was verified but before the journal
was cleared, the baseline reads `restored` (with its recovery deployment ID,
or a first-install undo to absence) while the journal is still
`rollback_pending`. The same Add Brilliant panel steps settle it. The panel is
not restored again. Home Assistant authenticates freshly against the pinned
identity, prunes the transaction-owned candidate and clears the journal, then
offers the explicit finalization.

Failures caused by the address or password entered in the flow are shown in the
flow only. They do not replace the `last_error` saved by background recovery. A
shell that cannot be built for an invalid or reassigned address reports
`recovery_connection_failed`; only key or pin evidence reports
`panel_identity_mismatch`.

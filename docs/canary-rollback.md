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
   deferred while the core baseline is retained.

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
operator output.

| Code/state | Operator action |
| --- | --- |
| `baseline_capture_failed`, `baseline_verify_failed` | Candidate mutation is refused. Preserve the incumbent; investigate missing/drifting code, permissions, free space, or capture bounds before retrying. |
| `transaction_in_progress` | Check diagnostics for an in-flight operation or retained baseline. Finish recovery, or intentionally finalize a successful canary before the next update. |
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
longer appears. An in-flight execution journal prevents finalization. Uninstall
also refuses to destroy a retained baseline; finalize intentionally first.

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

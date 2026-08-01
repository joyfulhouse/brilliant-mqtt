# Fleet Migration and Day-Two Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Consolidate compatible legacy panel entries without changing MQTT/entity identity, guide incompatible fleets without data loss, and add safe retained cleanup, panel removal, broker changes, credential rotation, recovery, and release qualification.

**Architecture:** MigrationPlanner is a pure no-write comparison engine. A domain-scoped MigrationCoordinator persists one complete plan, quiesces legacy runtimes, converts a deterministic anchor into the fleet, creates panel subentries, retargets integration-owned registry references, verifies the new runtime, and only then removes siblings. BrokerChangeCoordinator uses a separate per-panel journal: preflight every panel, switch sequentially, halt on failure, and expose explicit per-panel reverts. RetainedCleanup validates an agent ownership manifest or performs a bounded strict legacy scan; it never infers ownership from topic text alone and never clears mesh.

**Tech Stack:** Home Assistant 2026.6 config entries/subentries, device/entity registries, HA Store, Repair flows, Python 3.14 integration and Python 3.10 ownership seed helper, HA MQTT APIs, asyncssh, pytest golden fixtures/failure injection, disposable Mosquitto, uv, ruff, mypy strict.

## Global Constraints

- Complete [MQTT foundations](2026-07-21-mqtt-foundations.md) and [fleet onboarding](2026-07-21-fleet-onboarding.md) first.
- Migration is automatic only when every approved eligibility check passes. One conflict makes the entire automatic plan ineligible.
- MigrationPlanner performs no I/O and no mutation. Per-entry async_migrate_entry never enumerates or edits siblings.
- MigrationCoordinator is the only cross-entry writer and runs under one domain lock after Brilliant entries are enumerated and HA MQTT is connected.
- Automatic migration never contacts, reinstalls, restarts, or rewrites a panel. Existing panel environment files and MQTT topics remain unchanged.
- The deterministic anchor is the lexicographically smallest candidate entry_id. Each migrated panel stores management_id equal to its legacy entry_id.
- Entity IDs, entity unique IDs, MQTT discovery unique IDs, device identifiers, panel slugs, and mesh priorities remain byte-for-byte unchanged.
- No sibling entry is removed until the fleet runtime and every registry target have been verified. Past that commit point, recovery finishes removal rather than attempting an unsafe partial reverse.
- Conflicting legacy entries remain loaded in compatibility mode. Repair reports field names and entry titles but never secret values.
- A broker change requires Home Assistant's MQTT integration to already be connected to the proposed broker. Preflight all panels before switching the first.
- Switch panels sequentially. On first failure, stop. Never promise or attempt automatic fleet-wide rollback; expose explicit, journaled per-panel revert actions.
- Config-only panel removal does not stop the agent or clear retained topics. Managed uninstall stops the service first and clears only proven panel-owned topics.
- Reject invalid/oversized ownership manifests, fall back to the bounded strict scan, and surface a repair issue. Never clear brilliant/mesh or discovery whose parsed device identifier is not the exact panel.
- Journal secrets are redacted from every diagnostic/log/issue and deleted after committed success or verified reversal.
- The migration feature flag remains off until dry-run fixtures, interruption recovery, registry preservation, broker reversion, retained cleanup, and hardware gates pass.

---

## File map

- Create custom_components/brilliant_mqtt/migration.py: pure LegacySnapshot, conflict, canonicalization, and plan generation.
- Create ha/tests/fixtures/legacy_fleet_v3.json and ha/tests/test_migration.py: redacted eligibility/conflict matrix.
- Create custom_components/brilliant_mqtt/migration_journal.py and ha/tests/test_migration_journal.py: durable migration state and pre-change snapshots.
- Create custom_components/brilliant_mqtt/migration_coordinator.py and ha/tests/test_migration_coordinator.py: recoverable cross-entry commit/reverse.
- Modify custom_components/brilliant_mqtt/__init__.py and ha/tests/test_init.py: schedule domain migration after MQTT readiness; keep per-entry hook local.
- Create custom_components/brilliant_mqtt/broker_change.py and ha/tests/test_broker_change.py: preflight-all/sequential-switch journal.
- Modify custom_components/brilliant_mqtt/config_flow.py and ha/tests/test_config_flow.py: guided broker/credential change and explicit revert UI.
- Create custom_components/brilliant_mqtt/legacy_repair.py and ha/tests/test_legacy_repair.py: conflict selection, acknowledgement, and convergence.
- Create custom_components/brilliant_mqtt/repairs.py and modify ha/tests/test_repairs.py: repair-flow routing and stable issues.
- Modify src/brilliant_mqtt/retained_topics.py and tests/test_retained_topics.py: validated offline seed import.
- Create src/brilliant_mqtt/ownership_seed.py and tests/test_ownership_seed.py: panel CLI to seed local ledger without opening the Brilliant bus.
- Modify scripts/build_payload.sh and custom_components/brilliant_mqtt/agent_payload/: ship ownership seed helper.
- Create custom_components/brilliant_mqtt/retained_cleanup.py and ha/tests/test_retained_cleanup.py: manifest/fallback discovery validation and clears.
- Create custom_components/brilliant_mqtt/panel_removal.py and ha/tests/test_panel_removal.py: config-only versus managed uninstall.
- Modify custom_components/brilliant_mqtt/panel_ops.py, custom_components/brilliant_mqtt/manager.py, and their tests: stop-before-seed/cleanup operations.
- Modify custom_components/brilliant_mqtt/diagnostics.py, strings.json, translations/en.json, and tests: migration/operation state and remediation.
- Modify docs/ha-integration.md, INSTALL.md, docs/CONFIGURATION.md, and docs/reference/deployment.md: migration, conflicts, removal, broker changes, and recovery.
- Create scripts/run_fleet_migration_tests.sh and ha/tests/test_fleet_operations_live.py: disposable multi-broker failure/reversion tests.

---

### Task 1: Build the pure migration planner and fixture matrix

**Files:**
- Create: custom_components/brilliant_mqtt/migration.py
- Create: ha/tests/fixtures/legacy_fleet_v3.json
- Create: ha/tests/test_migration.py

**Interfaces:**
- LegacySnapshot.from_entry(entry: ConfigEntry) -> LegacySnapshot.
- MigrationPlanner.plan(snapshots: Sequence[LegacySnapshot]) -> MigrationPlan | MigrationConflictReport.
- MigrationPlan contains anchor_entry_id, ordered candidates, normalized fleet data, panel conversions, registry expectations, and plan_digest.
- MigrationConflict contains field, entry_ids, code, and redacted summaries.

- [ ] **Step 1: Commit a synthetic legacy fixture**

The fixture contains three entries named Kitchen, Office, and Bedroom using documentation-only hosts and secret values test-fleet-password/test-root-password. Include expected exact broker profile, seven globals, panel slugs, component selections, mesh priorities 1/2/3, canonical SSH public keys, expected SHA256 fingerprints, management IDs, and scene_panel conversion.

Add variants in the test module for:

- broker host, port, username, password, TLS mode, and CA conflict independently;
- each of the seven global fields independently;
- missing/invalid host key;
- duplicate fingerprint;
- duplicate slug;
- trust_host_key_changes true;
- scene_panel missing or referencing an unknown slug;
- a registry-preservation precheck failure;
- no entries and one entry;
- input-order permutations.

- [ ] **Step 2: Write failing pure-planner tests**

Assert eligible output chooses min(entry_id), preserves every panel value, stores one fleet password/global set, converts scene_panel to the expected future subentry key, and has the same digest for every input order. Assert conflict output is deterministically sorted by field/code/entry_id and does not contain any password, username value, CA body, root password, or host public key.

Assert plan() does not access hass, MQTT, filesystem, ConfigEntries mutation methods, or async APIs.

- [ ] **Step 3: Run and verify the missing-module failure**

Run: uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_migration.py -q

Expected: FAIL during collection for missing migration.

- [ ] **Step 4: Implement canonical comparison**

Normalize broker data through BrokerProfile. Normalize CA line endings to LF and one trailing newline after ssl.create_default_context(cadata=normalized_ca) accepts it. Compare passwords with hmac.compare_digest but never include them in conflict summaries.

Canonicalize:

- room_overrides and scene_actions as recursively sorted JSON mappings;
- ha_control_domains as a sorted tuple of unique domain strings;
- scalar globals with their existing typed validators;
- host keys through asyncssh.import_public_key/export_public_key, then get_fingerprint("sha256");
- components as a sorted mapping while forcing bridge true and retired ha_mirror false.

The planner receives precomputed RegistryExpectation values because it remains pure. Build plan_digest with sha256 over canonical non-secret structure only: replace MQTT/root passwords with presence booleans, use SSH fingerprints rather than public-key text, and include entry IDs, normalized non-secret profile/global/panel fields, and registry expectations. The coordinator separately compares current secrets to the protected journal snapshot with hmac.compare_digest immediately before mutation, so the public digest cannot become an offline password oracle.

- [ ] **Step 5: Run planner tests**

Run: uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_migration.py -q

Expected: PASS for eligible, every conflict, redaction, and permutation cases.

- [ ] **Step 6: Commit planner**

~~~bash
git add custom_components/brilliant_mqtt/migration.py ha/tests/fixtures/legacy_fleet_v3.json ha/tests/test_migration.py
git commit -m "feat: plan lossless legacy fleet migration"
~~~

---

### Task 2: Implement the recoverable domain migration coordinator

**Files:**
- Create: custom_components/brilliant_mqtt/migration_journal.py
- Create: custom_components/brilliant_mqtt/migration_coordinator.py
- Create: ha/tests/test_migration_journal.py
- Create: ha/tests/test_migration_coordinator.py
- Modify: custom_components/brilliant_mqtt/__init__.py
- Modify: ha/tests/test_init.py

**Interfaces:**
- MigrationJournal uses Store(hass, 1, "brilliant_mqtt.migration").
- MigrationPhase values: planned, quiesced, anchor_converted, registries_retargeted, fleet_verified, siblings_removing, complete, reversing.
- MigrationCoordinator.async_run() -> MigrationOutcome.
- MigrationCoordinator.async_recover(record) -> MigrationOutcome.

- [ ] **Step 1: Write failing phase and snapshot tests**

Journal fields are schema_version, transaction_id, plan_digest, phase, anchor_entry_id, ordered candidate IDs, legacy-entry snapshots, planned subentry IDs/data, registry before/after mappings, removed sibling IDs, and last_error. Snapshots include title, unique_id, version, disabled state, data, options, entity registry associations, and device registry associations. They remain private Store data and diagnostics expose only IDs/count/phase.

Allow only:

~~~text
planned -> quiesced -> anchor_converted -> registries_retargeted
registries_retargeted -> fleet_verified -> siblings_removing -> complete
quiesced|anchor_converted|registries_retargeted -> reversing -> complete
~~~

After fleet_verified, recovery may only finish sibling removal. Before fleet_verified, any commit failure reverses to all original entries/registry associations.

- [ ] **Step 2: Write failure-injection coordinator tests**

Inject a Home Assistant stop/error immediately before and after every journal save and every external mutation. On restart, assert exactly one fleet or the complete original legacy set, never duplicate managers or a mixed unjournaled state.

Prove:

- broker validation and prefix validation happen before planned is persisted;
- planning conflict writes one repair issue and performs zero config/registry mutations;
- every candidate is unloaded before anchor conversion;
- planned ConfigSubentry objects retain their journaled subentry_id on recovery;
- physical MQTT entities (config entry domain mqtt) are untouched;
- integration management entity entity_id/unique_id/disabled state is unchanged while config_entry_id becomes anchor and config_subentry_id becomes planned panel ID;
- device associations are retargeted without dropping their MQTT config-entry association;
- fleet setup produces every expected manager before fleet_verified;
- sibling removal starts only after verification;
- a second async_run is idempotent.

- [ ] **Step 3: Run and verify missing-module failures**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_migration_journal.py ha/tests/test_migration_coordinator.py -q
~~~

Expected: FAIL during collection for missing coordinator/journal.

- [ ] **Step 4: Implement deterministic conversion**

Under the domain migration lock:

1. enumerate every registered legacy-shaped Brilliant entry, whether still version 3 or already marked entry_kind=legacy_pending_consolidation;
2. collect RegistryExpectation values;
3. run the pure planner;
4. run BrokerValidator against the one normalized profile;
5. persist the complete planned record;
6. unload every candidate and persist quiesced;
7. update the anchor to entry_kind=fleet, title Brilliant MQTT, unique_id brilliant_mqtt_fleet, version 4, fleet data/options;
8. create ConfigSubentry objects using journaled IDs, identity unique IDs, legacy titles, and management_id=legacy entry_id;
9. persist anchor_converted;
10. retarget integration-owned entity/device registry associations using the Home Assistant 2026.6 WAQI migration pattern, preserving disabled flags;
11. persist registries_retargeted;
12. set up anchor and verify exact panel/registry sets;
13. persist fleet_verified;
14. remove siblings in recorded order, persisting each removed ID;
15. persist complete and delete the journal.

Convert legacy scene_panel slug to the planned subentry_id. Do not call PanelProvisioner, SSH, panel_ops, or an agent update.

- [ ] **Step 5: Implement reverse before the commit point**

For failures through registries_retargeted: unload the partial fleet, restore registry associations from journal, remove planned subentries, restore anchor title/unique/version/data/options, reload all original entries, verify their manager and registry sets, then mark/delete the journal. If reverse fails, keep phase reversing and create migration_recovery_failed with both errors.

- [ ] **Step 6: Schedule only at the domain boundary**

In async_setup, register services first, then schedule one coordinator task. Wait until config entries have been enumerated and mqtt.async_wait_for_mqtt_client returns true. If MQTT remains unavailable, leave legacy entries in compatibility mode and one retryable issue; do not block Home Assistant startup.

Keep async_migrate_entry limited to its Task 1 self-normalization marker. Add an AST/source test rejecting async_entries, async_remove, async_unload, and sibling update calls inside async_migrate_entry.

- [ ] **Step 7: Run migration and existing-runtime tests**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_migration_journal.py ha/tests/test_migration_coordinator.py ha/tests/test_migration.py ha/tests/test_init.py ha/tests/test_fleet_manager.py ha/tests/test_entities.py -q
~~~

Expected: PASS, including every restart boundary and two idempotent reruns.

- [ ] **Step 8: Commit coordinator**

~~~bash
git add custom_components/brilliant_mqtt/migration_journal.py custom_components/brilliant_mqtt/migration_coordinator.py custom_components/brilliant_mqtt/__init__.py ha/tests/test_migration_journal.py ha/tests/test_migration_coordinator.py ha/tests/test_init.py
git commit -m "feat: consolidate legacy entries into a fleet"
~~~

---

### Task 3: Add journaled broker and credential changes

**Files:**
- Create: custom_components/brilliant_mqtt/broker_change.py
- Create: ha/tests/test_broker_change.py
- Modify: custom_components/brilliant_mqtt/config_flow.py
- Modify: ha/tests/test_config_flow.py

**Interfaces:**
- BrokerChangeJournal uses Store(hass, 1, "brilliant_mqtt.broker_change").
- PanelBrokerState values: old, preflighted, switching, new, failed, revert_pending, reverted.
- BrokerChangeCoordinator.async_change(target: BrokerProfile, progress) -> BrokerChangeResult.
- async_revert(panel_ids: Sequence[str], progress) -> BrokerChangeResult.

- [ ] **Step 1: Write failing state-machine and restart tests**

The journal stores transaction ID, redacted target metadata plus protected target profile, exact old fleet profile, ordered panel IDs, per-panel old env/snapshot, current state, errors, and timestamps.

Assert:

- Home Assistant MQTT must be connected and BrokerValidator must pass target before any panel SSH;
- every panel completes staged panel preflight against target before the first active env changes;
- panels switch in stable subentry_id order;
- after each successful fresh-health verification, that panel becomes new and journal persists;
- first failure becomes failed and no later panel is touched;
- fleet entry profile changes only after all panels are new;
- cancellation behaves as failure/halt, not an automatic rollback;
- restart resumes the one switching/verification operation without duplicating it;
- explicit revert touches only selected new panels and persists each result.

- [ ] **Step 2: Run and verify missing-module failure**

Run: uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_broker_change.py -q

Expected: FAIL during collection for missing broker_change.

- [ ] **Step 3: Implement preflight-all then sequential switch**

Reuse PanelProvisioner staging/snapshot and BrokerValidator.async_validate_panel. Preflight may temporarily stage target env/CA but cannot activate. After all states are preflighted, subscribe fresh health, activate target config on one panel, verify through HA's current MQTT path, persist new, and continue.

On all-new success, update the fleet entry once through the secret-preserving BrokerProfile serializer and clear the journal after reload verification. On failure keep the old fleet profile in config data because the fleet is split; diagnostics and repairs read the journal as authority.

- [ ] **Step 4: Implement explicit per-panel reversion**

For a selected new panel, run panel-side preflight against its old profile, mark revert_pending, restore its exact old snapshot/env/CA, and verify the old agent locally with a temporary observer client. Mark reverted only after expected old-profile availability/meta. Because Home Assistant remains on the proposed broker, show restore_ha_mqtt_to_old_broker before finalizing a full reversion. Never claim the HA path is restored until BrokerValidator passes old profile after the user reconfigures HA MQTT.

- [ ] **Step 5: Expose a guided progress UI**

Fleet Broker and credentials options calls BrokerChangeCoordinator whenever any normalized connection field changes and panels exist. Show preflight counts, current panel title, halted failure, Retry failed panel, Revert switched panels, and Keep partial state for manual repair. Replacing only the password follows the same journal; it is not a fast unverified path.

- [ ] **Step 6: Run focused broker-change tests**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_broker_change.py ha/tests/test_config_flow.py ha/tests/test_panel_provisioner.py ha/tests/test_broker_validation.py -q
~~~

Expected: PASS for success, all failure positions, cancellation, restart, and explicit reversion.

- [ ] **Step 7: Commit broker operations**

~~~bash
git add custom_components/brilliant_mqtt/broker_change.py custom_components/brilliant_mqtt/config_flow.py ha/tests/test_broker_change.py ha/tests/test_config_flow.py
git commit -m "feat: change fleet brokers with a journal"
~~~

---

### Task 4: Guide conflicting legacy fleets without discarding values

**Files:**
- Create: custom_components/brilliant_mqtt/legacy_repair.py
- Create: ha/tests/test_legacy_repair.py
- Create: custom_components/brilliant_mqtt/repairs.py
- Modify: ha/tests/test_repairs.py
- Modify: custom_components/brilliant_mqtt/config_flow.py
- Modify: ha/tests/test_config_flow.py

**Interfaces:**
- LegacyRepairJournal uses Store(hass, 1, "brilliant_mqtt.legacy_repair").
- LegacyConflictRepairFlow chooses a broker source entry and a globals source entry, validates, converges panels, then invokes MigrationCoordinator.
- Repair issue ID is legacy_consolidation_conflict.

- [ ] **Step 1: Write failing conflict-report/repair tests**

Assert one issue lists conflict field names and affected entry titles only. It provides a Fix action and docs slug. The flow:

1. shows redacted conflict summary;
2. chooses one existing entry as canonical broker source;
3. chooses one existing entry as canonical global source;
4. requires explicit acknowledgement when any trust_host_key_changes is true;
5. validates the chosen broker against current HA MQTT;
6. uses BrokerChangeCoordinator for panels not already matching;
7. saves originals before changing any legacy global/repin fields;
8. reruns MigrationCoordinator;
9. clears the issue only after one verified fleet exists.

Inject cancellation/failure after every step and assert every competing value is still in its original entry or the protected repair journal, never only in logs.

- [ ] **Step 2: Run and verify missing repair failures**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_legacy_repair.py ha/tests/test_repairs.py -q
~~~

Expected: FAIL because conflicts currently have no guided convergence flow.

- [ ] **Step 3: Implement selection and convergence**

Do not allow free-form broker/global JSON. Choices are current entry titles/IDs; the subsequent broker screen permits deliberate edits through typed schemas. Validate any edited profile before accepting it.

After all broker changes succeed, persist exact original data/options in the repair journal, write the selected canonical globals and fail-closed repin option to each legacy entry, and rerun the pure planner. If still ineligible, restore those entry values and keep the issue. If eligible, invoke MigrationCoordinator with the exact new plan digest. Migration success deletes the repair journal.

- [ ] **Step 4: Run repair/migration integration tests**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_legacy_repair.py ha/tests/test_repairs.py ha/tests/test_migration.py ha/tests/test_migration_coordinator.py ha/tests/test_broker_change.py -q
~~~

Expected: PASS for broker/global/key-policy conflicts and interrupted repair.

- [ ] **Step 5: Commit conflict repair**

~~~bash
git add custom_components/brilliant_mqtt/legacy_repair.py custom_components/brilliant_mqtt/repairs.py custom_components/brilliant_mqtt/config_flow.py ha/tests/test_legacy_repair.py ha/tests/test_repairs.py ha/tests/test_config_flow.py
git commit -m "feat: guide conflicting fleet consolidation"
~~~

---

### Task 5: Seed ownership safely and implement retained cleanup/removal

**Files:**
- Modify: src/brilliant_mqtt/retained_topics.py
- Modify: src/brilliant_mqtt/scene_bridge.py
- Create: src/brilliant_mqtt/ownership_seed.py
- Modify: tests/test_retained_topics.py
- Modify: tests/test_scene_bridge.py
- Create: tests/test_ownership_seed.py
- Modify: scripts/build_payload.sh
- Modify: custom_components/brilliant_mqtt/agent_payload/
- Create: custom_components/brilliant_mqtt/retained_cleanup.py
- Create: custom_components/brilliant_mqtt/panel_removal.py
- Create: ha/tests/test_retained_cleanup.py
- Create: ha/tests/test_panel_removal.py
- Modify: custom_components/brilliant_mqtt/panel_ops.py
- Modify: custom_components/brilliant_mqtt/manager.py
- Modify: ha/tests/test_panel_ops.py
- Modify: ha/tests/test_manager.py
- Modify: custom_components/brilliant_mqtt/config_flow.py
- Modify: ha/tests/test_config_flow.py

**Interfaces:**
- RetainedTopicLedger.async_seed(topics: Collection[str]) -> None uses the same validation/atomic persistence as runtime.
- python -m brilliant_mqtt.ownership_seed accepts required --topics-json canonical JSON plus --publish-manifest and seeds while the service is stopped.
- RetainedCleanup.async_discover(panel) -> OwnershipEvidence.
- RetainedCleanup.async_clear(evidence) -> CleanupResult.
- PanelRemoval.async_remove(panel_id, mode) where mode is config_only or uninstall_and_cleanup.

- [ ] **Step 1: Write failing seed and defensive manifest tests**

Agent tests prove seed rejects a mismatched slug, brilliant/mesh, mesh discovery, unknown topic shape, duplicate, 4,097 topics, and canonical JSON above 256 KiB. Exact panel-scoped retained scene/mode catalog and scene/mode transport-status topics are accepted; non-retained event, command, and result topics are rejected. It writes atomically and publishes the ownership manifest at QoS 1 before success. The CLI imports no bus module and emits one redacted JSON result.

HA tests parse a retained ownership payload only when:

- schema_version is exactly 1;
- keys are exactly schema_version, panel_slug, topics;
- slug matches;
- count/bytes are within bounds;
- every brilliant topic matches a known retained shape, including only the
  exact panel-scoped retained scene/mode catalog and transport-status shapes;
- every discovery topic's currently retained config payload parses as JSON and contains device.identifiers including f"brilliant_panel_{slug}".

Reject legacy/single-string device identifiers, a different panel, absent retained config, invalid JSON, and any mesh identifier. Invalid manifest must return strict_fallback_required, never partial deletion authority.

- [ ] **Step 2: Write failing bounded fallback/removal tests**

Fallback subscribes only to homeassistant/+/+/config, f"brilliant/{slug}/#",
and the four exact panel-scoped scene/mode catalog and transport-status topics.
It collects retained messages for at most 2 seconds, 4,096 topics, and 256 KiB,
and applies the same payload/device validation. It recognizes only
availability, bridge, ownership, f"{peripheral}/state" below the exact slug,
and those four exact HA-control topics. Unknown messages are preserved and
reported.

Removal tests prove:

- config_only removes the subentry but performs no SSH, service stop, MQTT clear, or file deletion and creates panel_agent_still_running guidance;
- uninstall_and_cleanup verifies the SSH key, stops/disables the agent first, discovers ownership, clears concrete topics at QoS 1, clears ownership last, removes owned service/files, then removes the subentry;
- failure to stop prevents cleanup;
- failure to prove ownership preserves the message and subentry;
- cleanup failure creates one issue with exact remaining topics;
- native subentry deletion is treated as config_only by FleetManager's diff listener.

- [ ] **Step 3: Run and verify missing-module failures**

Run:

~~~bash
uv run pytest tests/test_retained_topics.py tests/test_ownership_seed.py -q
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_retained_cleanup.py ha/tests/test_panel_removal.py -q
~~~

Expected: FAIL for missing seed/cleanup/removal APIs.

- [ ] **Step 4: Implement first-upgrade seeding**

Every legacy-entry migration seeds ownership before the fleet entry may treat
the manifest as complete. This is required even when a prerelease/canary has
already run a ledger-aware agent and the broker currently has a valid manifest:
that manifest proves only topics republished since the upgrade, not that legacy
orphans were scanned. For an existing panel:

1. collect the strict bounded legacy scan through HA MQTT and union it with any
   defensively validated current manifest;
2. stop the old agent and verify inactive;
3. stage/activate the 0.6+ payload without starting;
4. invoke ownership_seed with the proven concrete topics and local ledger path;
5. start the agent and verify fresh health;
6. verify broker manifest equals local seed result;
7. persist a migration-owned `ownership_seeded` fact so a merely valid runtime
   manifest can never be mistaken for completed legacy seeding.

If scan or seed fails, restore/start the previous agent and create ownership_seed_failed. A new install begins with an empty ledger and needs no scan.

- [ ] **Step 5: Implement cleanup ordering**

Before enabling managed removal, route SceneBridge's retained scene/mode
catalog and transport-status publications through the same panel ledger.
Retained-ledger failures from catalog reads or background status tasks must
reach session supervision rather than being reduced to a malformed-catalog or
status-publication log; add failure-injection coverage for both paths.

For valid manifest evidence, publish empty retained payloads for every listed topic except ownership, sorted for deterministic logs. After each acknowledgement remove it from the evidence result. Clear ownership last. For fallback, clear only proven topics and report every preserved unknown. Never use a wildcard in a publish call.

- [ ] **Step 6: Add explicit removal screens**

Panel reconfigure menu adds Remove from Home Assistant only and Uninstall agent and remove. The destructive path shows the panel name/address/fingerprint, states retained topics will be cleared, and requires confirmation. It calls PanelRemoval; direct native subentry removal remains config-only.

- [ ] **Step 7: Regenerate payload and run focused tests**

Run:

~~~bash
scripts/build_payload.sh
uv run pytest tests/test_retained_topics.py tests/test_scene_bridge.py tests/test_ownership_seed.py tests/test_payload_parity.py -q
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_retained_cleanup.py ha/tests/test_panel_removal.py ha/tests/test_panel_ops.py ha/tests/test_manager.py ha/tests/test_config_flow.py -q
~~~

Expected: PASS, including invalid-manifest preservation and mesh exclusion.

- [ ] **Step 8: Commit ownership/removal operations**

~~~bash
git add src/brilliant_mqtt/retained_topics.py src/brilliant_mqtt/scene_bridge.py src/brilliant_mqtt/ownership_seed.py tests/test_retained_topics.py tests/test_scene_bridge.py tests/test_ownership_seed.py scripts/build_payload.sh custom_components/brilliant_mqtt/agent_payload custom_components/brilliant_mqtt/retained_cleanup.py custom_components/brilliant_mqtt/panel_removal.py custom_components/brilliant_mqtt/panel_ops.py custom_components/brilliant_mqtt/manager.py custom_components/brilliant_mqtt/config_flow.py ha/tests/test_retained_cleanup.py ha/tests/test_panel_removal.py ha/tests/test_panel_ops.py ha/tests/test_manager.py ha/tests/test_config_flow.py
git commit -m "feat: clean up proven panel MQTT ownership"
~~~

---

### Task 6: Finish diagnostics, repairs, and operator documentation

**Files:**
- Modify: custom_components/brilliant_mqtt/diagnostics.py
- Modify: ha/tests/test_diagnostics.py
- Modify: custom_components/brilliant_mqtt/strings.json
- Modify: custom_components/brilliant_mqtt/translations/en.json
- Modify: custom_components/brilliant_mqtt/repairs.py
- Modify: ha/tests/test_repairs.py
- Modify: docs/ha-integration.md
- Modify: INSTALL.md
- Modify: docs/CONFIGURATION.md
- Modify: docs/reference/deployment.md

- [ ] **Step 1: Write failing state/redaction coverage**

Diagnostics must show:

- migration phase/digest prefix/candidate count/last redacted code;
- broker change target host/port/TLS, per-panel states, and revert availability;
- ownership schema/topic count/source/last cleanup result;
- active repair IDs.

It must not show journal snapshots, passwords, usernames, CA bodies, environment bytes, nonces, SSH public keys, or removed retained payload bodies. Add one recursive secret scan over every fixture value.

Repairs must exist for migration conflict/recovery, split broker fleet, HA broker restore required, ownership invalid/seed failure, cleanup incomplete, config-only agent still running, and rollback failure. Repeated identical faults update the same issue ID.

- [ ] **Step 2: Run and verify incomplete output**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_diagnostics.py ha/tests/test_repairs.py -q
~~~

Expected: FAIL until all operation states and issues are represented/redacted.

- [ ] **Step 3: Write task-oriented operations docs**

Add exact procedures:

1. read automatic migration dry-run/conflicts;
2. choose canonical values without exposing secrets;
3. change broker or rotate fleet credentials;
4. recover a halted partial switch;
5. explicitly revert switched panels and restore HA MQTT;
6. remove config only;
7. uninstall and clear proven retained topics;
8. resolve every stable error code.

State that migration never rewrites panels, external brokers remain first-class, automatic cross-panel rollback is not promised, and manual retained deletion must use the listed concrete topics rather than wildcard guesses.

- [ ] **Step 4: Validate strings and documentation**

Run:

~~~bash
python -m json.tool custom_components/brilliant_mqtt/strings.json >/dev/null
python -m json.tool custom_components/brilliant_mqtt/translations/en.json >/dev/null
rg -n "migration|canonical|broker change|explicit revert|config only|retained|external broker" docs/ha-integration.md INSTALL.md docs/CONFIGURATION.md docs/reference/deployment.md
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_diagnostics.py ha/tests/test_repairs.py -q
~~~

Expected: JSON valid, concepts found, tests PASS.

- [ ] **Step 5: Commit operational UX**

~~~bash
git add custom_components/brilliant_mqtt/diagnostics.py custom_components/brilliant_mqtt/strings.json custom_components/brilliant_mqtt/translations/en.json custom_components/brilliant_mqtt/repairs.py ha/tests/test_diagnostics.py ha/tests/test_repairs.py docs/ha-integration.md INSTALL.md docs/CONFIGURATION.md docs/reference/deployment.md
git commit -m "docs: add fleet migration and recovery guidance"
~~~

---

### Task 7: Add disposable multi-broker and migration failure tests

**Files:**
- Create: scripts/run_fleet_migration_tests.sh
- Create: ha/tests/test_fleet_operations_live.py
- Modify: ha/pyproject.toml

- [ ] **Step 1: Add an opt-in live operations test**

Mark mqtt_live and skip unless both BRILLIANT_MQTT_TEST_BROKER_OLD_URL and BRILLIANT_MQTT_TEST_BROKER_NEW_URL exist. Use synthetic panel clients, not hardware, to exercise:

- HA and three panels on old broker;
- panel preflight of new broker;
- sequential switch of two panels;
- injected third-panel activation failure;
- halt with first two new/third failed;
- explicit first/second panel reversion;
- HA restore to old broker;
- old profile validation and journal completion;
- retained manifest cleanup containing valid panel topics, hostile other-panel topics, and mesh topics.

Assert only valid panel topics clear and both brokers end with no setup topics.

- [ ] **Step 2: Implement the disposable runner**

Reuse the hardened Mosquitto harness from plan 1 but start two pinned eclipse-mosquitto:2.0.22 instances with distinct localhost ports and volumes. Use mktemp -d, runtime-generated passwords/certificates, bounded readiness, and trap cleanup. Do not reuse a production hostname, credential, CA, or Docker volume.

- [ ] **Step 3: Run disposable operations**

Run: scripts/run_fleet_migration_tests.sh

Expected: PASS for split/halt/revert and hostile retained cleanup. Docker absence is an unmet gate, not a reason to weaken the test.

- [ ] **Step 4: Run exhaustive mocked restart tests**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_migration_coordinator.py ha/tests/test_broker_change.py ha/tests/test_legacy_repair.py ha/tests/test_retained_cleanup.py ha/tests/test_panel_removal.py -q
~~~

Expected: PASS for every journal phase and injected mutation boundary.

- [ ] **Step 5: Commit live operations tests**

~~~bash
git add scripts/run_fleet_migration_tests.sh ha/tests/test_fleet_operations_live.py ha/pyproject.toml
git commit -m "test: exercise fleet migration and broker recovery"
~~~

---

### Task 8: Qualify migration rollout and remove the feature gate

- [ ] **Step 1: Run both complete project gates**

Run:

~~~bash
uv run ruff check --fix
uv run ruff format
uv run mypy --strict src tests
uv run pytest
uv run --project ha ruff check --fix --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests
uv run --project ha ruff format --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests
uv run --project ha mypy --strict --config-file ha/pyproject.toml custom_components/brilliant_mqtt ha/tests
uv run --project ha pytest -c ha/pyproject.toml ha/tests
scripts/run_mqtt_validation_tests.sh
scripts/run_fleet_migration_tests.sh
git diff --check
~~~

Expected: every command exits 0.

- [ ] **Step 2: Run migration dry-run against sanitized real-shape data**

Export a secrets-redacted structural snapshot of the existing multi-panel config entries and registries under gitignored artifacts/. Run MigrationPlanner twice with shuffled entry order. Verify identical eligibility/digest, expected anchor, exact slug/priority/component/global conversion, unique host-key fingerprints, and zero writes/panel connections.

- [ ] **Step 3: Run compatible multi-panel migration canary**

On a disposable HA clone:

- capture config entries, device registry, entity registry, MQTT discovery entities, topics, and panel files;
- trigger coordinator;
- verify one fleet plus the expected subentries;
- compare entity IDs/unique IDs/device identifiers/topics byte-for-byte;
- verify panel files/service restart counters unchanged;
- restart HA after each journal phase using the test fault controls and prove finish/reverse;
- remove the fault controls and repeat cleanly.

- [ ] **Step 4: Run conflicting migration and guided repair canary**

Create broker, global, missing-key, and auto-repin conflicts one at a time. Verify no automatic mutation, redacted issue text, canonical selection, panel-by-panel broker convergence, acknowledgement, migration, and cancellation recovery without lost values.

- [ ] **Step 5: Run retained removal and broker reversion canaries**

On the pilot:

- upgrade through the first-ledger seed path and compare manifest/local ledger;
- create one stale panel-owned discovery topic plus hostile other-panel/mesh topics;
- managed-remove the panel and verify only proven panel topics clear;
- reinstall, preflight a second broker, inject a later-panel switch failure, explicitly revert switched panels, restore HA MQTT, and verify the original profile end to end.

- [ ] **Step 6: Repeat the 30-minute hardware resource/safety soak**

Verify no extra resident process/client/bus peer; RSS increase no more than 5 MiB; CPU increase no more than 2 percentage points; 96 MiB/20% limits; no message_bus increase; no reconnect storm, ghost peer, physical-control delay, retained leak, failed rollback, or failed explicit revert.

- [ ] **Step 7: Enable migration and record sanitized evidence**

Only after Steps 1-6 pass, remove the internal migration rollout guard, rerun the full HA gate, and document dates/results in docs/reference/deployment.md. Keep raw snapshots/logs under gitignored artifacts/.

- [ ] **Step 8: Commit release qualification**

~~~bash
git add custom_components/brilliant_mqtt/migration_coordinator.py ha/tests/test_migration_coordinator.py docs/reference/deployment.md
git commit -m "feat: enable qualified fleet migration"
~~~

---

## Plan 3 completion criteria

- Compatible legacy entries consolidate automatically without panel contact or identity changes.
- Conflicts preserve all entries/values and offer a redacted, guided convergence flow.
- Every migration phase survives restart and produces either the complete original set or one verified fleet.
- Management registry associations move to subentries without changing entity IDs/unique IDs; MQTT entities/topics remain untouched.
- Broker/credential changes preflight all panels, switch sequentially, halt on failure, and expose verified explicit reverts.
- Ownership is seeded on first ledger-aware upgrade, bounded defensively, and excludes mesh.
- Config-only removal clears nothing; managed removal stops first and clears only proven panel ownership.
- Full mock, disposable-broker, interrupted migration, conflict repair, retained cleanup, multi-panel canary, and hardware resource gates pass before migration is enabled.

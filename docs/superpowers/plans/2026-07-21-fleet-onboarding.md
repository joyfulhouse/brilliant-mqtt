# Fleet Config Entry and Panel Onboarding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Replace repeated per-panel setup with one validated Brilliant MQTT fleet entry and one Home Assistant config subentry per panel, including verify-before-password SSH identity, staged panel preflight, atomic activation, and focused day-two configuration.

**Architecture:** A fleet entry owns the normalized broker profile, one MQTT credential, seven installation-global settings, and a FleetManager. Panel subentries own immutable SSH-key identity, address/credential, stable MQTT slug, components, overrides, and mesh priority. PanelManager consumes a storage adapter rather than a raw ConfigEntry so legacy entries remain loadable while fleet subentries use the same runtime. Config flows contain only UI state; BrokerValidator, PanelProvisioner, ProvisioningJournal, and FleetManager own network and mutation work.

**Tech Stack:** Home Assistant 2026.6 config entries/subentries, Python 3.14, asyncssh 2.23.1, HA MQTT APIs, the MQTT foundation services from plan 1, voluptuous/selectors, Store-backed journals, pytest-homeassistant-custom-component, uv, ruff, mypy strict.

## Global Constraints

- Complete [MQTT foundations](2026-07-21-mqtt-foundations.md) first. Do not begin this plan with failing foundation gates.
- This is plan 2 of 3. It introduces the fleet runtime and onboarding but retains legacy runtime compatibility; plan 3 performs cross-entry consolidation and retained cleanup.
- Exactly one fleet config entry is allowed per Home Assistant installation. A fleet matches Home Assistant's one active MQTT broker path.
- Initial setup validates the broker and provisions the first panel in one continuous flow. No fleet entry is created if either validation or first-panel provisioning fails.
- Subsequent Add panel flows ask initially for panel address and root password only. They never ask for MQTT settings, raw JSON, mesh priority, or unrelated optional features.
- Broker kind changes copy/defaults only. Both kinds normalize into the same BrokerProfile and pass the same validator.
- A panel root password is never sent until an unauthenticated key exchange has produced a candidate key and the authenticated connection pins that exact key.
- New fleet panels never auto-repin. A key change requires an explicit rebind flow. Legacy auto-repin behavior remains isolated in its compatibility adapter until plan 3 resolves it.
- The first eligible panel receives mesh priority 1; later panels receive the next unused positive integer. Slugs and priorities are never implicitly renumbered.
- New installs enable bridge, Wi-Fi watchdog, and bus watchdog. Voice, diyHue, HA control, certificate recovery, and retired HA mirror remain off until focused configuration.
- Subscribe for health before activation and reject retained/replayed messages. Success requires fresh availability, metadata with the staged version, initial state, and normal discovery.
- Journal every activation before mutating the active service. First-install failure leaves no active partial unit; upgrade failure restores the exact prior release/env/unit/component state.
- Never log or place MQTT passwords, root passwords, CA PEM bodies, or environment contents in flow placeholders, issues, diagnostics, or exceptions.

---

## File map

- Modify custom_components/brilliant_mqtt/const.py: fleet/subentry keys, schema version 4, defaults, and stable IDs.
- Create custom_components/brilliant_mqtt/entry_data.py: FleetConfig, PanelConfig, PanelConfigStore, and legacy/fleet adapters.
- Create ha/tests/test_entry_data.py: ownership, normalization, immutable slug, and update tests.
- Modify custom_components/brilliant_mqtt/shell.py and ha/tests/test_shell.py: pre-auth key retrieval, canonical identity, and pinned-only fleet connections.
- Create custom_components/brilliant_mqtt/panel_inspection.py and ha/tests/test_panel_inspection.py: bounded read-only compatibility facts.
- Create custom_components/brilliant_mqtt/provisioning_journal.py and ha/tests/test_provisioning_journal.py: durable sensitive transaction record.
- Create custom_components/brilliant_mqtt/panel_health.py and ha/tests/test_panel_health.py: fresh post-activation MQTT evidence.
- Create custom_components/brilliant_mqtt/panel_provisioner.py and ha/tests/test_panel_provisioner.py: staged preflight, activation, rollback, and recovery.
- Modify custom_components/brilliant_mqtt/broker_validation.py and ha/tests/test_broker_validation.py: coordinate a real panel preflight process through HA MQTT.
- Modify custom_components/brilliant_mqtt/panel_ops.py, custom_components/brilliant_mqtt/components.py, ha/tests/test_panel_ops.py, and ha/tests/test_components.py: non-activating stage/snapshot/activate/rollback operations.
- Create custom_components/brilliant_mqtt/fleet_manager.py and ha/tests/test_fleet_manager.py: fleet lifecycle and panel lookup.
- Modify custom_components/brilliant_mqtt/manager.py and ha/tests/test_manager.py: consume PanelConfigStore and remove raw-entry ownership.
- Modify custom_components/brilliant_mqtt/__init__.py, all platform modules, custom_components/brilliant_mqtt/entity.py, and entity/service tests: one fleet runtime with subentry-associated entities.
- Rewrite custom_components/brilliant_mqtt/config_flow.py and ha/tests/test_config_flow.py: recommended/existing broker paths, progress validation, first panel, and panel subentry flow.
- Create custom_components/brilliant_mqtt/flow_schemas.py and ha/tests/test_flow_schemas.py: focused normal/advanced forms and secret-preserving updates.
- Modify custom_components/brilliant_mqtt/strings.json and custom_components/brilliant_mqtt/translations/en.json: all new screens/errors/remediation links.
- Modify custom_components/brilliant_mqtt/diagnostics.py and ha/tests/test_diagnostics.py: fleet/panel redacted diagnostics.
- Modify custom_components/brilliant_mqtt/manifest.json and custom_components/brilliant_mqtt/quality_scale.yaml: integration_type hub and documented quality behavior.
- Modify docs/ha-integration.md and INSTALL.md: fleet-first setup and Add panel task flow.

---

### Task 1: Define fleet and panel storage ownership

**Files:**
- Modify: custom_components/brilliant_mqtt/const.py
- Create: custom_components/brilliant_mqtt/entry_data.py
- Create: ha/tests/test_entry_data.py
- Modify: ha/tests/test_const.py

**Interfaces:**
- CONFIG_ENTRY_VERSION = 4 and ENTRY_KIND_FLEET = "fleet".
- SUBENTRY_TYPE_PANEL = "panel".
- FleetConfig.from_entry(entry: ConfigEntry) -> FleetConfig.
- PanelConfig.from_subentry(subentry: ConfigSubentry) -> PanelConfig.
- PanelConfigStore Protocol exposes panel_id, management_id, data, options, update_data(), update_options(), and async_create_background_task().
- FleetPanelStore and LegacyPanelStore implement that protocol.

- [ ] **Step 1: Write failing exact-ownership tests**

Build one fleet entry and two ConfigSubentry objects. Assert FleetConfig accepts only these fleet-owned keys:

~~~text
entry_kind, broker_kind, mqtt_host, mqtt_port, mqtt_username, mqtt_password,
mqtt_tls_enabled, mqtt_tls_ca, next_mesh_priority, ha_control_enabled,
ha_control_label, room_overrides, ha_control_domains, max_mirrored_entities,
scene_panel, scene_actions, schema_version
~~~

Assert PanelConfig accepts only:

~~~text
identity_fingerprint, ssh_host_key, host, ssh_username, root_password, name,
panel, management_id, components, feature_overrides, mesh_priority,
provisioning_transaction_id
~~~

Assert broker/HA globals in a subentry and SSH/panel fields in fleet data raise EntryDataError. Assert slug and management_id cannot change through FleetPanelStore.update_data. Assert a legacy store can expose the current version-3 shape unchanged.

- [ ] **Step 2: Run and verify the missing-module failure**

Run: uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_entry_data.py ha/tests/test_const.py -q

Expected: FAIL during collection for missing entry_data.

- [ ] **Step 3: Implement immutable typed views and adapters**

Use frozen slot dataclasses. BrokerProfile remains the nested normalized connection type. PanelConfig.components is a MappingProxyType[str, bool], and feature_overrides is a MappingProxyType[str, JSON-compatible scalar/list/mapping]. provisioning_transaction_id is temporary internal state: it is required on a newly provisioned subentry and removed only after async_setup_entry or the live subentry update listener matches it to the journal and verifies the runtime. Reject panel slug mesh and anything not matching ^[a-z0-9][a-z0-9_-]{0,63}$.

FleetPanelStore updates a subentry through:

~~~python
self.hass.config_entries.async_update_subentry(
    self.entry,
    self.subentry,
    data=new_data,
    title=str(new_data[CONF_NAME]),
)
~~~

It schedules background work on the parent fleet entry. LegacyPanelStore delegates updates/background tasks to the ConfigEntry. Both expose a stable management_id: new panels store identity_fingerprint; legacy panels use their existing entry_id so entity migration can preserve unique IDs later.

- [ ] **Step 4: Add a version-4 compatibility boundary**

Set BrilliantMqttConfigFlow.VERSION and CONFIG_ENTRY_VERSION to 4. Modify async_migrate_entry only enough to normalize the current entry itself, add entry_kind="legacy_pending_consolidation" for version-3 panel entries, and preserve their complete data/options/runtime. It must not enumerate, update, unload, or remove sibling entries. Cross-entry work remains plan 3.

- [ ] **Step 5: Run focused tests**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_entry_data.py ha/tests/test_const.py ha/tests/test_init.py -q
~~~

Expected: PASS, including a test that migrating one entry produces zero updates to its sibling.

- [ ] **Step 6: Commit storage ownership**

~~~bash
git add custom_components/brilliant_mqtt/const.py custom_components/brilliant_mqtt/entry_data.py custom_components/brilliant_mqtt/__init__.py custom_components/brilliant_mqtt/config_flow.py ha/tests/test_entry_data.py ha/tests/test_const.py ha/tests/test_init.py
git commit -m "refactor: define fleet and panel config ownership"
~~~

---

### Task 2: Fetch and pin SSH identity before password authentication

**Files:**
- Modify: custom_components/brilliant_mqtt/shell.py
- Modify: ha/tests/test_shell.py
- Create: custom_components/brilliant_mqtt/panel_inspection.py
- Create: ha/tests/test_panel_inspection.py

**Interfaces:**
- HostIdentity(public_key: str, fingerprint: str).
- async_fetch_host_identity(host: str, port: int = 22) -> HostIdentity.
- AsyncsshShell requires pinned_host_key: str for fleet use.
- async_inspect_panel(shell: PanelShell, identity: HostIdentity) -> PanelFacts.

- [ ] **Step 1: Write failing verify-before-auth tests**

Patch asyncssh.get_server_host_key and asyncssh.connect separately. Assert:

1. key retrieval receives host/port but no username or password;
2. canonical public_key is key.export_public_key().decode().strip();
3. fingerprint is key.get_fingerprint("sha256") and starts SHA256:;
4. authenticated connect imports a known_hosts line containing the exact candidate key;
5. mismatch raises HostKeyNotVerifiable and asyncssh.connect never reaches password authentication;
6. AsyncsshShell(host, password, None) raises ValueError for fleet construction;
7. changing address with the same key succeeds; a different key returns a typed host_key_changed result.

- [ ] **Step 2: Run and verify failing constructor/key-fetch tests**

Run: uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_shell.py -q

Expected: FAIL because async_fetch_host_identity is absent and unpinned construction is still allowed.

- [ ] **Step 3: Implement unauthenticated identity retrieval**

Call:

~~~python
key = await asyncssh.get_server_host_key(
    host,
    port=port,
    server_host_key_algs=("ssh-ed25519", "rsa-sha2-512", "rsa-sha2-256"),
)
~~~

Reject None, multi-line exports, and unsupported key types with typed PanelIdentityError codes. Keep known_hosts_line address-dependent only for connection verification; use get_fingerprint("sha256") as the address-independent identity.

Retain a private LegacyAsyncsshShell adapter for legacy compatibility only. It may reproduce the old TOFU behavior behind LegacyPanelStore, but no new flow or fleet manager may import it.

- [ ] **Step 4: Add one bounded read-only inspection command**

PanelFacts contains fingerprint, hostname, model, architecture, firmware, python_version, init_system, available_bytes, available_memory_bytes, installed_agent_version, active_services, and conflicting_services.

Run one semicolon-delimited command using hostname, uname -m, python3 --version, systemctl --version, df -Pk /var, awk against /proc/meminfo, the existing VERSION path, and systemctl is-active for owned/retired services. Parse key=value lines strictly. Reject unsupported architectures, Python outside 3.10.x, unavailable /var below 64 MiB, or available memory below 32 MiB with stable compatibility codes. Inspection performs no writes.

- [ ] **Step 5: Run identity and inspection tests**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_shell.py ha/tests/test_panel_inspection.py -q
~~~

Expected: PASS, including an ordered mock proving fetch_host_identity completes before AsyncsshShell.connect receives the password.

- [ ] **Step 6: Commit SSH identity**

~~~bash
git add custom_components/brilliant_mqtt/shell.py custom_components/brilliant_mqtt/panel_inspection.py ha/tests/test_shell.py ha/tests/test_panel_inspection.py
git commit -m "feat: pin panel identity before SSH authentication"
~~~

---

### Task 3: Add durable staged provisioning and fresh health verification

**Files:**
- Create: custom_components/brilliant_mqtt/provisioning_journal.py
- Create: custom_components/brilliant_mqtt/panel_health.py
- Create: custom_components/brilliant_mqtt/panel_provisioner.py
- Create: ha/tests/test_provisioning_journal.py
- Create: ha/tests/test_panel_health.py
- Create: ha/tests/test_panel_provisioner.py
- Modify: custom_components/brilliant_mqtt/broker_validation.py
- Modify: ha/tests/test_broker_validation.py
- Modify: custom_components/brilliant_mqtt/panel_ops.py
- Modify: custom_components/brilliant_mqtt/components.py
- Modify: ha/tests/test_panel_ops.py
- Modify: ha/tests/test_components.py

**Interfaces:**
- ProvisioningJournal uses Store(hass, 1, "brilliant_mqtt.provisioning").
- PanelProvisioner.async_install(request, fleet, progress) -> ProvisionedPanel.
- PanelProvisioner.async_recover() -> None.
- PanelHealthObserver.async_wait(expected_version, timeout=90.0) -> PanelHealthEvidence.
- BrokerValidator.async_validate_panel(profile, launcher) -> BrokerValidationResult.

- [ ] **Step 1: Write failing journal transition tests**

Allow only:

~~~text
staged -> activation_pending -> activated -> verifying -> pending_config_commit -> committed
staged -> rollback_pending -> rolled_back
activation_pending|activated|verifying -> rollback_pending -> rolled_back
~~~

The journal record includes transaction_id, operation, phase, setup_id, panel identity/request, fleet profile, staged version, prior PanelSnapshot, started_at, and last_error. Save before returning from every transition. Delete only after committed subentry creation or verified rolled_back state. Assert diagnostics returns count/phase only and never record data.

- [ ] **Step 2: Write failing panel-operation and health tests**

Add tests proving:

- stage_release uploads versioned payload, unit, env, and CA without replacing /etc files or starting a unit;
- snapshot captures exact active release link, env bytes, unit bytes, enabled/active states, and component selection;
- activate_staged uses same-filesystem rename/link replacement, daemon-reload, enable/start;
- rollback restores the exact snapshot and removes a first-install unit;
- health subscriptions exist before activate_staged;
- retained callbacks cannot satisfy any health gate;
- non-retained online, expected bridge agent_version, one state topic, and one discovery config for device identifier f"brilliant_panel_{slug}" are all required;
- cancellation closes subscriptions and drives rollback.

- [ ] **Step 3: Run focused tests and verify missing service failures**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_provisioning_journal.py ha/tests/test_panel_health.py ha/tests/test_panel_provisioner.py ha/tests/test_panel_ops.py -q
~~~

Expected: FAIL because the journal, observer, provisioner, and staged operations do not exist.

- [ ] **Step 4: Implement the HA/panel preflight coordinator**

Before launching the SSH command, subscribe HA to SetupTopics.panel_to_ha and discovery_probe. Launch:

~~~text
set -a; . /var/brilliant-mqtt/system/brilliant-mqtt.env; set +a; PYTHONPATH=/var/brilliant-mqtt.staging/app:/var/brilliant-mqtt.staging/vendor /data/switch-embedded/env/bin/python3 -m brilliant_mqtt.preflight --request-json '{"schema_version":1,"setup_id":"12345678-1234-4abc-8def-1234567890ab","panel_nonce":"panel-nonce","ha_nonce":"ha-nonce","timeout_seconds":10.0}'
~~~

using safe single-quote escaping from a dedicated shell_arg helper. panel_ops builds this command from fixed paths and one escaped request argument; it never accepts a free-form command. The staged env points at an immutable versioned CA file, and PYTHONPATH points only at the staged app/vendor while the service remains unchanged. When HA receives the exact SetupRequest on panel_to_ha, publish SetupResult on ha_to_panel at QoS 1. Require the panel process report to prove fleet_auth, both directions, discovery_write, retained_message, and cleanup. Require HA to observe panel_to_ha and discovery_write. A timeout remains the approved same-broker-or-ACL ambiguous error. Always unsubscribe and terminate a still-running preflight process before returning.

`timeout_seconds` is a per-stage result deadline, not a hard process return
bound: executor-backed MQTT connect/close work may need to settle after that
deadline so it cannot race cleanup. The HA coordinator must therefore impose a
separate outer SSH-process deadline, terminate a still-running preflight
process, await SSH process settlement, and only then return an onboarding
error.

- [ ] **Step 5: Implement staged installation and recovery**

PanelProvisioner order is exact:

1. fetch and pin identity;
2. authenticate and inspect;
3. reject duplicate fingerprint before writes;
4. snapshot current owned files/unit state;
5. stage payload/env/public CA;
6. run coordinated panel preflight;
7. write journal phase activation_pending;
8. subscribe PanelHealthObserver;
9. activate atomically and mark activated;
10. await fresh health and mark verifying;
11. return ProvisionedPanel for config-entry/subentry commit;
12. caller marks pending_config_commit and includes the transaction ID in the proposed panel subentry;
13. async_setup_entry or FleetManager's live subentry listener marks committed and clears both the journal and temporary subentry field only after Home Assistant storage and runtime verification succeed.

On first-install failure, disable/remove the owned unit/env and verify inactive. On upgrade failure, restore snapshot and verify the old version/active state. If rollback fails, preserve journal at rollback_pending and create one repair issue containing original and rollback codes.

async_recover runs before accepting another provisioning request. For activated/verifying it first subscribes for fresh health and either advances to pending_config_commit or rolls back. For pending_config_commit it searches all fleet subentries for the exact transaction ID: a match completes runtime verification and commit; no match after config-flow completion or abort is known rolls the panel back. It never starts a different transaction while a journal exists.

- [ ] **Step 6: Run complete provisioning tests**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_broker_validation.py ha/tests/test_provisioning_journal.py ha/tests/test_panel_health.py ha/tests/test_panel_provisioner.py ha/tests/test_panel_ops.py ha/tests/test_components.py -q
~~~

Expected: PASS for first install, upgrade, every failure boundary, cancellation, restart at every journal phase, and secret redaction.

- [ ] **Step 7: Commit provisioning**

~~~bash
git add custom_components/brilliant_mqtt/provisioning_journal.py custom_components/brilliant_mqtt/panel_health.py custom_components/brilliant_mqtt/panel_provisioner.py custom_components/brilliant_mqtt/broker_validation.py custom_components/brilliant_mqtt/panel_ops.py custom_components/brilliant_mqtt/components.py ha/tests/test_provisioning_journal.py ha/tests/test_panel_health.py ha/tests/test_panel_provisioner.py ha/tests/test_broker_validation.py ha/tests/test_panel_ops.py ha/tests/test_components.py
git commit -m "feat: provision panels with staged rollback"
~~~

---

### Task 4: Refactor runtime ownership into FleetManager

**Files:**
- Create: custom_components/brilliant_mqtt/fleet_manager.py
- Create: ha/tests/test_fleet_manager.py
- Modify: custom_components/brilliant_mqtt/manager.py
- Modify: custom_components/brilliant_mqtt/__init__.py
- Modify: custom_components/brilliant_mqtt/entity.py
- Modify: custom_components/brilliant_mqtt/binary_sensor.py
- Modify: custom_components/brilliant_mqtt/button.py
- Modify: custom_components/brilliant_mqtt/select.py
- Modify: custom_components/brilliant_mqtt/switch.py
- Modify: custom_components/brilliant_mqtt/update.py
- Modify: ha/tests/test_manager.py
- Modify: ha/tests/test_entities.py
- Modify: ha/tests/test_services.py

**Interfaces:**
- BrilliantMqttConfigEntry = ConfigEntry[FleetManager].
- FleetManager.panels: Mapping[str, PanelManager], keyed by subentry_id or legacy entry_id.
- FleetManager.async_setup(), async_shutdown(), async_panel_added(), async_panel_updated(), async_panel_removed().
- PanelManager(hass, store: PanelConfigStore, fleet: FleetConfig, ssh_lock).

- [ ] **Step 1: Write failing mixed-runtime tests**

Assert a version-4 fleet with two subentries creates two managers, while one legacy entry creates one compatibility manager. Assert duplicate slug/fingerprint fails setup. Assert one panel failure marks only that manager degraded and does not unload its healthy sibling. Assert fleet shutdown drains every manager even if one raises.

For each platform, assert entities for both panels are added with config_subentry_id matching their subentry. Unique IDs prepend management_id to each existing suffix: bridge_health, repair_bridge, reboot_panel, run_selected_scene, voice_enabled, wifi_watchdog_enabled, bus_watchdog_enabled, hue_ca_enabled, ha_mirror_enabled, voice_wake_word, scene, and agent_update. Legacy unique IDs prepend legacy_entry_id to those same suffixes. Assert entity device_info still attaches to ("mqtt", f"brilliant_panel_{slug}").

- [ ] **Step 2: Run and verify current single-manager failures**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_fleet_manager.py ha/tests/test_manager.py ha/tests/test_entities.py ha/tests/test_services.py -q
~~~

Expected: FAIL because runtime_data is one PanelManager and platforms add one entity set.

- [ ] **Step 3: Remove ConfigEntry access from PanelManager**

Replace entry.data/options/update calls with PanelConfigStore.data/options/update_data/update_options. Replace entry.entry_id in signals/issues/events/unique IDs with store.management_id. Delete the fleet-panel use of _shell_unpinned; fleet stores always fail closed. Keep legacy opt-in repin in LegacyPanelStore only, including its existing audit event, until plan 3.

PanelManager receives the seven globals through FleetConfig and reads panel overrides through the store. It must never copy a changed fleet global back into panel data.

- [ ] **Step 4: Implement FleetManager lifecycle**

Construct all stores before starting any manager, validate uniqueness, then start managers independently. Keep one SSH lock for the fleet. Register a config-entry update listener that diffs subentry IDs and calls add/update/remove under a lifecycle lock. Aggregate broker outage into one fleet issue; preserve panel-level grace timers for isolated outages.

For legacy entries construct a synthetic FleetConfig from their existing broker/global values and one LegacyPanelStore. This path is compatibility-only and cannot add a subentry.

- [ ] **Step 5: Update platforms and service targeting**

Each platform loops entry.runtime_data.panels.values(), builds entities from PanelManager, groups them by manager.store.subentry_id, and calls:

~~~python
async_add_entities(entities, config_subentry_id=subentry_id)
~~~

Omit config_subentry_id only for legacy stores. Resolve service targets through entity/device registries so targeting one subentry entity operates on one manager, targeting the fleet operates on all panels, and no target continues to mean all loaded Brilliant panels. Aggregate failures without skipping later panels.

- [ ] **Step 6: Run runtime regression suite**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_fleet_manager.py ha/tests/test_manager.py ha/tests/test_entities.py ha/tests/test_services.py ha/tests/test_init.py ha/tests/test_switch.py ha/tests/test_update.py -q
~~~

Expected: PASS for fleet and legacy fixtures.

- [ ] **Step 7: Commit fleet runtime**

~~~bash
git add custom_components/brilliant_mqtt/fleet_manager.py custom_components/brilliant_mqtt/manager.py custom_components/brilliant_mqtt/__init__.py custom_components/brilliant_mqtt/entity.py custom_components/brilliant_mqtt/binary_sensor.py custom_components/brilliant_mqtt/button.py custom_components/brilliant_mqtt/select.py custom_components/brilliant_mqtt/switch.py custom_components/brilliant_mqtt/update.py ha/tests/test_fleet_manager.py ha/tests/test_manager.py ha/tests/test_entities.py ha/tests/test_services.py ha/tests/test_init.py ha/tests/test_switch.py ha/tests/test_update.py
git commit -m "refactor: manage panels through one fleet runtime"
~~~

---

### Task 5: Implement the fleet-first initial flow and Add panel subentry flow

**Files:**
- Create: custom_components/brilliant_mqtt/flow_schemas.py
- Create: ha/tests/test_flow_schemas.py
- Rewrite: custom_components/brilliant_mqtt/config_flow.py
- Rewrite: ha/tests/test_config_flow.py

**Interfaces:**
- BrilliantMqttConfigFlow.async_get_supported_subentry_types returns {"panel": PanelSubentryFlow}.
- Initial steps: user -> broker -> broker_advanced when selected -> broker_validation -> panel_connect -> panel_confirm -> panel_provision -> create.
- PanelSubentryFlow steps: user -> confirm -> provision -> create.
- Fleet entry unique_id is brilliant_mqtt_fleet; panel subentry unique_id is SSH SHA256 fingerprint.

- [ ] **Step 1: Write failing UI contract tests**

Test exact paths:

1. user menu shows Home Assistant Mosquitto (Recommended) and Existing MQTT broker equally;
2. official path prefills hass.config.api.local_ip when present and port 1883, but both remain editable;
3. existing path exposes host/port/username/password in the normal form;
4. advanced exposes TLS and custom public CA for either path;
5. missing/disconnected HA MQTT or a non-homeassistant discovery prefix preserves non-secret inputs, creates no entry, and links the matching docs slug;
6. broker success advances to panel_connect with no broker fields shown again;
7. panel_connect asks host and root_password, with root fixed unless Advanced SSH is selected;
8. confirm shows fingerprint/facts and only name is editable;
9. provision uses async_show_progress and cannot double-submit;
10. success creates one fleet entry plus one ConfigSubentryData in the same result;
11. existing fleet aborts already_configured;
12. any legacy entry aborts legacy_migration_required rather than creating a competing fleet;
13. subsequent PanelSubentryFlow receives broker/globals from parent, allocates priority, and creates only panel data;
14. duplicate fingerprint aborts already_configured and identifies the existing subentry;
15. failure persists no root/MQTT password in flow result, issue, or logs.

- [ ] **Step 2: Run and verify old-flow failures**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_flow_schemas.py ha/tests/test_config_flow.py -q
~~~

Expected: FAIL because the current flow is panel-first and repeats broker/global/component fields.

- [ ] **Step 3: Build focused schemas**

Use TextSelector(type=PASSWORD) for secrets and section(advanced_schema, {"collapsed": True}) for Advanced. Reject control characters before trimming. Define SECRET_UNCHANGED = "**BRILLIANT_MQTT_SECRET_UNCHANGED**" and use it only as the password selector's suggested value during reconfigure. Submission of that exact sentinel retains the stored value, blank means validation error, and a new value replaces it only after successful validation. Unit tests must prove the sentinel can never be persisted as the password or rendered outside a masked password field.

Slug allocation lowercases the confirmed name, replaces non-alphanumerics with one hyphen, rejects mesh, truncates at 64 characters, and appends -2, -3 in the first available slot. It is written once. Mesh allocation returns the smallest positive integer not already used and never changes existing priorities.

- [ ] **Step 4: Implement the initial flow as thin orchestration**

Store in-progress secrets only on the flow instance. Call BrokerValidator in a config-flow progress task. Fetch identity before constructing an authenticated shell. Call async_inspect_panel for confirm. After confirmation call PanelProvisioner in a second progress task.

On success return:

~~~python
self.async_create_entry(
    title="Brilliant MQTT",
    data=fleet_data,
    subentries=[
        ConfigSubentryData(
            data=panel_data,
            subentry_type=SUBENTRY_TYPE_PANEL,
            title=panel_name,
            unique_id=identity.fingerprint,
        )
    ],
)
~~~

Set unique ID brilliant_mqtt_fleet before validation and abort any existing fleet. Put provisioning_transaction_id in panel_data and leave the journal at pending_config_commit when returning the result. async_setup_entry matches the stored subentry to the journal, verifies its manager, removes the temporary field, and only then clears the journal. If Home Assistant stops between flow completion and storage, startup recovery either finds that exact subentry or performs the recorded rollback.

- [ ] **Step 5: Implement PanelSubentryFlow with the same services**

Return {"panel": PanelSubentryFlow} only for an entry_kind=fleet parent. Use _get_entry() and async_create_entry(title=panel_name, data=panel_data, unique_id=identity.fingerprint). Do not copy broker profile, fleet password, CA, or globals into panel_data. Schedule parent reload after successful creation only if its update listener cannot add the new runtime manager live.

- [ ] **Step 6: Run flow and provisioning tests**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_flow_schemas.py ha/tests/test_config_flow.py ha/tests/test_panel_provisioner.py ha/tests/test_entry_data.py -q
~~~

Expected: PASS across both broker kinds and retry/cancellation branches.

- [ ] **Step 7: Commit fleet onboarding UI**

~~~bash
git add custom_components/brilliant_mqtt/flow_schemas.py custom_components/brilliant_mqtt/config_flow.py ha/tests/test_flow_schemas.py ha/tests/test_config_flow.py
git commit -m "feat: add fleet-first panel onboarding"
~~~

---

### Task 6: Move globals and day-two changes to focused fleet/panel flows

**Files:**
- Modify: custom_components/brilliant_mqtt/config_flow.py
- Modify: custom_components/brilliant_mqtt/flow_schemas.py
- Modify: custom_components/brilliant_mqtt/fleet_manager.py
- Modify: custom_components/brilliant_mqtt/manager.py
- Modify: ha/tests/test_config_flow.py
- Modify: ha/tests/test_fleet_manager.py
- Modify: ha/tests/test_manager.py

- [ ] **Step 1: Write failing ownership and reconfiguration tests**

Fleet options menu must expose:

- Broker and credentials;
- Home Assistant control;
- Scenes;
- Fleet defaults;
- Mesh priorities under Advanced;
- Add panel through the subentry action, not a duplicate options form.

Panel reconfigure must expose:

- Rename;
- Change address;
- Repair SSH credentials;
- Components;
- Panel overrides;
- Explicit rebind replaced/reflashed panel.

Assert the seven globals update once on fleet data and never in a subentry. Assert scene_panel accepts only a current subentry_id. Assert a renamed panel keeps slug, fingerprint, management_id, MQTT topics, and entity unique IDs. Assert a changed address must present the existing key before any password is sent.

- [ ] **Step 2: Run focused tests and verify old options failures**

Run: uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_config_flow.py ha/tests/test_fleet_manager.py -q

Expected: FAIL because options remain per-panel and include auto-repin.

- [ ] **Step 3: Implement fleet options and safe credential rotation**

Broker/credential change in this plan may update an empty fleet or validate an identical profile. If a fleet has panels and the normalized profile changes, abort with broker_change_requires_guided_flow; plan 3 implements the journaled multi-panel change. Password sentinel retains the stored secret unless a new value passes BrokerValidator.

Update globals atomically on the fleet entry and notify FleetManager. Validate room_overrides, ha_control_domains, scene_actions, and max count with existing typed validators, now in flow_schemas.py. Replace panel-slug scene owner with the chosen panel subentry_id.

- [ ] **Step 4: Implement safe panel reconfigure/rebind**

Rename changes title/name only. Address or password change fetches candidate identity first and accepts it only if fingerprint equals stored identity. Rebind is a separate confirm screen displaying old/new fingerprints; deliberate confirmation preserves slug/management ID but stores the new canonical key/fingerprint and records an audit event. It never runs from a normal credential repair.

Remove OPT_TRUST_HOST_KEY_CHANGES from fleet options. Legacy adapters still surface a repair issue explaining that their opt-in cannot migrate automatically.

- [ ] **Step 5: Run day-two tests**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_config_flow.py ha/tests/test_fleet_manager.py ha/tests/test_manager.py ha/tests/test_scene_control.py ha/tests/test_ha_control.py -q
~~~

Expected: PASS with one fleet-global configuration source.

- [ ] **Step 6: Commit focused day-two flows**

~~~bash
git add custom_components/brilliant_mqtt/config_flow.py custom_components/brilliant_mqtt/flow_schemas.py custom_components/brilliant_mqtt/fleet_manager.py custom_components/brilliant_mqtt/manager.py ha/tests/test_config_flow.py ha/tests/test_fleet_manager.py ha/tests/test_manager.py
git commit -m "feat: add focused fleet and panel settings"
~~~

---

### Task 7: Finish translations, diagnostics, metadata, and user documentation

**Files:**
- Modify: custom_components/brilliant_mqtt/strings.json
- Modify: custom_components/brilliant_mqtt/translations/en.json
- Modify: custom_components/brilliant_mqtt/diagnostics.py
- Modify: ha/tests/test_diagnostics.py
- Modify: custom_components/brilliant_mqtt/manifest.json
- Modify: custom_components/brilliant_mqtt/quality_scale.yaml
- Modify: docs/ha-integration.md
- Modify: INSTALL.md

- [ ] **Step 1: Write failing translation and redaction tests**

Add a test that recursively compares strings.json and en.json keys. Require titles/descriptions/errors for every new step, both broker choices, every stable foundation/provisioning code, duplicate panel, explicit rebind, and legacy migration required. Assert each error includes one recommended action and documentation slug.

Fleet diagnostics allow broker kind/host/port/TLS boolean, HA MQTT state, validation stage/time, panel counts/health, and schema version. Panel diagnostics allow fingerprint/model/address/version/components/priority/leadership/health timestamps/journal phase. Assert serialized diagnostics contain none of either password, username value, CA PEM, environment body, or raw setup nonce.

- [ ] **Step 2: Run and verify incomplete metadata failures**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_diagnostics.py ha/tests/test_config_flow.py ha/tests/test_brand.py -q
~~~

Expected: FAIL until strings/diagnostics describe the fleet model.

- [ ] **Step 3: Update metadata and documentation**

Set manifest integration_type to hub. Explain the official Mosquitto prerequisite as recommended, never required. Document the Existing broker path at the same hierarchy level, panel-reachable host requirement, first continuous setup, later Add panel, automatic mesh allocation, focused optional-feature setup, strict key pin/rebind, and why a broker change with panels is deferred to the guided operation.

- [ ] **Step 4: Run translation, HACS, and docs checks**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_diagnostics.py ha/tests/test_config_flow.py ha/tests/test_brand.py -q
python -m json.tool custom_components/brilliant_mqtt/strings.json >/dev/null
python -m json.tool custom_components/brilliant_mqtt/translations/en.json >/dev/null
rg -n "Recommended|Existing MQTT broker|Add panel|rebind|mesh priority" INSTALL.md docs/ha-integration.md
~~~

Expected: PASS and every concept is found.

- [ ] **Step 5: Commit user-facing fleet setup**

~~~bash
git add custom_components/brilliant_mqtt/strings.json custom_components/brilliant_mqtt/translations/en.json custom_components/brilliant_mqtt/diagnostics.py custom_components/brilliant_mqtt/manifest.json custom_components/brilliant_mqtt/quality_scale.yaml ha/tests/test_diagnostics.py docs/ha-integration.md INSTALL.md
git commit -m "docs: describe fleet-first MQTT onboarding"
~~~

---

### Task 8: Run full gates and one-panel onboarding canaries

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
git diff --check
~~~

Expected: every command exits 0.

- [ ] **Step 2: Run the disposable broker suite**

Run: scripts/run_mqtt_validation_tests.sh

Expected: PASS for plaintext/TLS and the expected typed ACL/auth/mismatch failures.

- [ ] **Step 3: Exercise one official-Mosquitto canary**

On a disposable HA test instance and designated pilot panel:

- select Home Assistant Mosquitto;
- enter a dedicated Brilliant user and panel-reachable broker host;
- observe all broker stages;
- enter only panel address/password;
- verify the displayed fingerprint/facts;
- complete staged preflight/activation;
- verify one fleet entry, one panel subentry, fresh normal MQTT entities, and management entities associated with that subentry;
- rename the panel and verify topics/unique IDs do not change;
- uninstall/reinstall the integration entry only after preserving raw evidence under gitignored artifacts/.

- [ ] **Step 4: Exercise one external-broker canary**

Repeat against a broker not managed by Home Assistant, once plaintext or trusted-public-CA TLS and once custom-CA TLS. Verify no core_mosquitto lookup/install is attempted, both ACL principals are required, panel path validation succeeds, and the resulting stored schema is identical except broker kind/profile values.

- [ ] **Step 5: Re-run resource/safety gates**

Verify one resident agent/MQTT connection/bus peer, RSS and CPU limits from plan 1, temporary preflight exit before agent start, no duplicate client-ID disconnect, rollback after a deliberately failed staged unit, and normal physical controls through broker/HA restarts.

- [ ] **Step 6: Commit sanitized canary evidence**

~~~bash
git add docs/ha-integration.md docs/reference/deployment.md
git commit -m "test: qualify fleet onboarding canaries"
~~~

---

## Plan 2 completion criteria

- A new user chooses recommended official Mosquitto or an existing broker and passes identical behavioral validation.
- Initial setup provisions the first panel without repeating broker or advanced component settings.
- Later panels are Home Assistant config subentries and initially ask only for address/password.
- SSH identity is collected before authentication, remains stable across address changes, and fails closed outside explicit rebind.
- Provisioning is staged, panel-path validated, journaled, atomically activated, fresh-health verified, and recoverable.
- One FleetManager owns all panel runtimes; management entities associate with their panel subentries.
- Fleet-global settings exist once; panel slugs and priorities remain stable.
- Legacy entries still load unchanged in compatibility mode, ready for plan 3 consolidation.
- Full mock, broker, HA, official-Mosquitto, external-broker, rollback, and hardware resource gates pass.

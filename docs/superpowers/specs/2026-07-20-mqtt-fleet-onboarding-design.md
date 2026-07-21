# MQTT Fleet Onboarding Simplification — Design

- **Date:** 2026-07-20
- **Status:** Approved direction; pending specification review
- **Scope:** Replace the one-entry-per-panel setup wizard with a fleet-first
  Home Assistant integration, validate MQTT end to end during onboarding, and
  preserve the existing panel agent and MQTT entity contract.
- **Home Assistant baseline:** 2026.6.0 or newer, matching `hacs.json`.
- **Related documentation:**
  - [Current Home Assistant integration](../../ha-integration.md)
  - [Current configuration reference](../../CONFIGURATION.md)
  - [Current architecture](../../ARCHITECTURE.md)
  - [Home Assistant MQTT documentation](https://www.home-assistant.io/integrations/mqtt/)
  - [Official Mosquitto Broker app documentation](https://github.com/home-assistant/addons/blob/master/mosquitto/DOCS.md)
  - [Home Assistant config entries and subentries](https://developers.home-assistant.io/docs/config_entries_index/)

## Executive decision

Keep MQTT as the panel transport. It is already lightweight, preserves retained
state and discovery outside the constrained wall panels, and avoids introducing
a second panel daemon or a custom Home Assistant network protocol. The setup
complexity belongs in the Home Assistant integration, not in fields repeated
for every panel.

The integration becomes a hub with one fleet config entry and one config
subentry per panel. A fleet owns one MQTT connection profile and a shared
dedicated Brilliant MQTT credential. A panel subentry owns only panel identity,
SSH provisioning, and panel-specific feature overrides.

On first setup, users choose one of two fully supported paths:

1. **Home Assistant Mosquitto** — the recommended shortcut using the official
   Mosquitto Broker app/add-on (`core_mosquitto`).
2. **Existing MQTT broker** — any compatible local, remote, or hosted broker
   reachable by Home Assistant and the panels.

The integration never installs the official app, edits broker configuration,
creates broker accounts, or forces a broker choice. It documents the official
app as a prerequisite only when that path is selected.

Fleet creation succeeds only after a temporary device client and Home
Assistant's MQTT connection prove authentication, bidirectional messaging,
same-broker routing, and retained-message behavior. Panel creation succeeds
only after the same MQTT path works from the panel and the installed service
publishes its normal availability and discovery data.

## Why MQTT remains the transport

The dominant panel costs are the resident Python runtime, Brilliant
`RPCObserver`/message-bus session, normalization, and safety polling. MQTT adds
one small client library and one persistent TCP connection while moving
retention, replay, discovery, fan-out, and availability ownership to the broker.

A direct WebSocket, HTTP, or ESPHome-style API would not remove the Python or
message-bus costs and would recreate broker capabilities in the integration and
agent. A custom framed TCP protocol could be marginally smaller, but it would
need new authentication, replay, availability, backpressure, and state-sync
semantics. A pure-Rust panel agent could eventually reduce runtime use, but it
would first have to reimplement the private bidirectional Thrift observer
contract and its reconnect behavior. That is a separate, high-risk research
track rather than an onboarding prerequisite.

This design therefore changes Home Assistant ownership and setup UX while
leaving the steady-state panel transport and Brilliant bus boundary intact.

## Current problem

The existing config flow creates one top-level config entry per panel. Every
new panel repeats fleet-wide MQTT details and then presents component toggles,
mesh priority, voice settings, certificates, and Home Assistant control-plane
settings. Some controls accept raw JSON. Reconfiguration repeats much of the
same surface and asks for secrets again.

The flow also performs installation inside form submission without first
proving the complete panel → broker → Home Assistant path. A user can finish a
large form yet still discover a DNS, ACL, credential, TLS, or broker mismatch
only after deployment.

The result has several failure modes:

- shared broker credentials drift between panel entries;
- a broker change requires editing panels independently;
- copied fleet settings can disagree silently;
- defaults require knowledge of internal components and mesh election;
- error messages identify an operation rather than the failed network stage;
- users need repository knowledge, YAML/JSON knowledge, or an LLM to recover.

## Goals

1. Make MQTT configuration a once-per-fleet concern.
2. Make the official Mosquitto app the easiest documented path without making
   it mandatory.
3. Support existing local, remote, and hosted brokers as a first-class path.
4. Prove MQTT authentication, ACLs, routing, retention, and panel reachability
   before reporting setup success.
5. Reduce add-panel input to panel address and root password, followed by an
   editable discovered name.
6. Move optional capabilities into focused post-install flows.
7. Replace generic failures with actionable, stage-specific remediation and
   direct documentation links.
8. Preserve MQTT topics, discovery identities, entity IDs, automations, panel
   runtime behavior, and current resource use.
9. Migrate existing entries without requiring a panel reinstall or forcing
   panels that use different brokers into one fleet.

## Non-goals

- Replacing MQTT with a direct panel API.
- Rewriting the Brilliant message-bus client or panel agent in Rust.
- Automatically installing, starting, or configuring `core_mosquitto`.
- Reading or reusing Home Assistant's hidden internal MQTT credentials.
- Automatically creating Home Assistant users or external broker accounts.
- Managing arbitrary broker ACL files.
- Redesigning the MQTT discovery/state/command topic contract.
- Redesigning voice, diyHue, scene control, or HA control-plane behavior.
- Introducing an additional resident process or MQTT connection on a panel.
- Replacing stored panel root passwords with managed SSH keys in this release.

## User-facing success criteria

A new user who knows the broker endpoint, one dedicated MQTT credential, and a
panel's SSH credentials can complete setup without YAML, raw JSON, shell access,
or repository-specific knowledge.

The default experience has these properties:

- Broker selection visibly offers both the recommended official app and an
  existing broker.
- The official path explains its prerequisites and asks only for the endpoint
  reachable from panels plus the dedicated fleet username and password.
- The existing-broker path asks for host, port, username, and password; TLS and
  certificate controls appear only when Advanced settings are opened.
- Add panel asks initially for address and root password. SSH inspection
  discovers identity and suggests the name on a confirmation step.
- Core bridge and watchdog behavior use safe defaults.
- Mesh priority is assigned automatically.
- Voice, diyHue, HA controls, certificates, and other specialist features do
  not appear during core onboarding.
- Setup cannot finish until real MQTT messages traverse both directions.

## Alternatives considered

| Approach | Advantages | Problems | Decision |
|---|---|---|---|
| Fleet config entry with panel subentries | Shared settings have one owner; add-panel flow is small; broker rotation and health are fleet operations; matches HA's shared-resource model | Requires config-entry migration and subentry-aware management entities | **Selected** |
| Keep one entry per panel and store broker data in hidden integration storage | Smaller initial migration | Creates invisible cross-entry coupling, unclear ownership, and fragile deletion/reload behavior | Rejected |
| Separate top-level broker and panel entries | Explicit dependency graph | Produces multiple integration cards, reference lifecycle problems, and much of the current UX complexity | Rejected |

Multiple fleet entries remain valid. One fleet always maps to one exact broker
connection profile, but a Home Assistant installation may intentionally manage
different Brilliant fleets on different brokers.

## Architecture and ownership

Change the integration type from `device` to `hub`. The top-level config entry
is the fleet hub; panels are Home Assistant config subentries.

```text
Home Assistant MQTT integration ───────────────┐
                                               │ same broker
Brilliant MQTT fleet entry                    │
  broker profile + fleet credential            │
  BrokerValidator                              │
  FleetManager                                 │
  ├── panel subentry: Kitchen ─ SSH ─ agent ───┤
  ├── panel subentry: Office  ─ SSH ─ agent ───┤
  └── panel subentry: Bedroom ─ SSH ─ agent ───┘
                                               │
                                     MQTT Discovery entities
```

### Fleet config entry

The fleet entry persists:

- broker kind: `official_mosquitto` or `existing_broker`;
- broker hostname or IP address as reachable from panels;
- broker port;
- dedicated fleet MQTT username and password;
- TLS enabled/disabled;
- certificate validation mode and optional public CA material/reference;
- MQTT discovery prefix, defaulting to `homeassistant` and prefilled from the
  Home Assistant MQTT integration when available;
- fleet feature defaults and mesh-priority allocation state;
- schema and migration version.

The broker kind changes guidance and defaults only. Runtime validation and
panel rendering use one normalized connection-profile type for both paths.

The fleet password is stored exactly once. It is never copied into every panel
subentry, although the provisioner renders it into each panel's protected
environment file when deploying. Reconfigure forms use Home Assistant's secret
sentinel behavior so an unchanged password is never re-entered or replaced by
display text.

### Panel config subentry

Each panel subentry persists:

- stable identity discovered from the panel;
- current hostname or IP address;
- friendly name and stable MQTT panel slug;
- SSH username and root password;
- installed component selection;
- explicit per-panel feature overrides;
- assigned mesh priority.

The MQTT panel slug is allocated once. Renaming a panel changes its display
name but never its slug, topics, device identifiers, or entity unique IDs.

Runtime-only health, installed versions, last validation stage, and timestamps
belong to a panel controller/diagnostics store rather than being rewritten into
the config entry on every update.

The root password is accepted during onboarding and persisted in the long-lived
subentry only when panel creation succeeds. Immediately before activation, a
durable provisioning journal may hold the credential temporarily so a Home
Assistant restart can finish or roll back the transaction. The journal is
deleted after promotion to the subentry or verified rollback. Routine options
flows do not ask for the password. A repair flow asks for a replacement only
after SSH authentication fails or the user explicitly chooses **Repair SSH
credentials**. Diagnostics and logs always redact it.

### Component boundaries

`config_flow.py` becomes a thin UI/state-machine layer over focused services:

- **`BrokerValidator`** owns temporary MQTT connections, nonce exchange,
  retention checks, cleanup, timeouts, and typed broker failures. It has no SSH
  or config-entry mutation responsibilities.
- **`PanelProvisioner`** owns SSH inspection, duplicate identity checks, staged
  file transfer, panel-side MQTT preflight, service activation, rollback, and
  post-start health checks. It also owns the durable provisioning journal used
  to recover an interrupted activation. It receives a normalized fleet profile
  and panel request; it does not read Home Assistant forms directly.
- **`FleetManager`** owns runtime panel controllers, shared MQTT subscriptions,
  fleet health aggregation, management entities, broker reconfiguration, and
  global configuration propagation.
- **`MigrationPlanner`** is an idempotent helper that groups legacy entries,
  builds and validates a migration plan, and commits it in recoverable phases.
  It is not a long-lived runtime service.

Physical controls continue to be created by MQTT Discovery. Status, update,
restart, diagnostics, and configuration entities supplied by this custom
integration are associated with the relevant panel subentry.

## Broker prerequisites and selection

### Home Assistant Mosquitto path

The UI marks this path **Recommended** and links to a concise prerequisite page:

1. Install and start the official Mosquitto Broker app/add-on.
2. Configure Home Assistant's MQTT integration and enable discovery.
3. Create a dedicated, non-owner Home Assistant user for the Brilliant fleet,
   or deliberately create an equivalent local Mosquitto login.
4. Supply a hostname or IP that panels can resolve and reach. Internal add-on
   hostnames are not assumed to be reachable from the IoT network.

The official app authenticates Home Assistant users and does not permit
anonymous access. The integration does not use the generated secret credential
that Home Assistant keeps for its own MQTT connection.

The form prefills the likely Home Assistant host and port `1883`, but both are
editable because VLANs, DNS, port mappings, and reverse tunnels vary. Advanced
TLS controls remain available for customized official-app deployments.

### Existing MQTT broker path

This path is visible beside the recommended path, not hidden behind Advanced.
It requires:

- Home Assistant's MQTT integration already connected to the same broker;
- support for MQTT 5 as required by current Home Assistant and MQTT 3.1.1 as
  used by the current panel client;
- an endpoint reachable from both Home Assistant and each panel;
- retained messages and wildcard subscriptions;
- a dedicated fleet credential;
- permissions to read and write `brilliant/#`;
- permission to write `<discovery_prefix>/#` for MQTT Discovery.

Host, port, username, and password are normal fields. TLS enablement,
certificate validation, custom CA input, and other certificate controls are
under Advanced settings. Error text and documentation remain broker-neutral.

No fleet entry is created while Home Assistant's MQTT integration is absent or
disconnected. The flow preserves non-secret values, explains how to configure
MQTT, and allows retry without restarting the integration setup.

## Broker validation contract

Validation must prove behavior, not merely open a TCP socket. It uses a random
setup ID and topics below `brilliant/setup/<setup_id>/`. The setup ID is not
retained or reused.

| Stage | Operation | What it proves |
|---|---|---|
| `ha_mqtt_ready` | Confirm Home Assistant's MQTT integration is loaded and connected | HA has an active broker path |
| `fleet_auth` | Open a temporary device client with the supplied Brilliant credential | Endpoint, protocol, TLS, and authentication work |
| `panel_to_ha` | Subscribe through Home Assistant, then publish a random nonce from the temporary device client | Device publish ACL, HA subscribe path, and same-broker routing work |
| `ha_to_panel` | Subscribe through the temporary client, then publish a different nonce through Home Assistant | HA publish path and device subscribe ACL work |
| `retained_message` | Publish a retained nonce, subscribe after publication, require the retained flag and exact payload | Broker retention required by discovery/state works |
| `cleanup` | Publish empty retained payloads and unsubscribe/disconnect both sides | No setup artifacts remain |

Use bounded waits and unique MQTT client IDs. QoS 1 is used for probes where
supported by the existing HA MQTT API. A timeout identifies the exact stage.
Cleanup executes in a `finally` path after success, rejection, cancellation, or
timeout.

This exchange detects the failure class where both clients connect but messages
do not cross. A different broker is a common cause, but a broker that silently
drops ACL-denied messages is observationally similar. When CONNACK, SUBACK, or
other broker responses identify an ACL failure, the UI reports it precisely.
Otherwise it reports **Messages did not cross between Home Assistant and the
Brilliant client; verify that they use the same broker and that the Brilliant
ACL permits this direction**. The design does not claim false certainty from a
timeout alone.

## Add-panel onboarding

### Step 1: Connect

Ask only for:

- panel hostname or IP address;
- root password.

Use root as the default SSH username and keep it out of the standard form unless
an advanced deployment needs to override it.

### Step 2: Inspect and confirm

SSH inspection collects the minimum safe facts needed to continue:

- stable device identity;
- hostname/model and suggested friendly name;
- supported architecture, firmware, Python, and service-manager versions;
- available disk and memory;
- current Brilliant MQTT installation/version;
- conflicting retired services;
- duplicate-panel status in every configured fleet.

The second screen shows the discovered facts and an editable friendly name. A
duplicate stable identity offers reconfiguration of the existing panel rather
than creating a second subentry.

### Step 3: Stage and test from the panel

After confirmation, show Home Assistant config-flow progress rather than
holding a form submission open.

The provisioner uploads a staged release and normalized environment file but
does not enable or replace the active service. A temporary preflight command
from the staged package uses the same bundled aiomqtt/Paho stack, broker
profile, TLS assets, and credentials as the agent. It does not open a Brilliant
message-bus peer.

The panel preflight participates in a nonce exchange with `BrokerValidator` and
proves:

- panel-side DNS resolution;
- TCP routing from the panel VLAN;
- TLS validation using the panel's clock and trust material;
- authentication and publish/subscribe ACLs;
- retained-message behavior through the same broker used by Home Assistant.

The temporary process exits and disconnects before activation.

### Step 4: Activate and verify

Subscribe to the panel's health topics and record a durable provisioning
transaction before activation. Then atomically install the staged
release/config and start the service. Onboarding waits for:

- a fresh, non-replayed panel availability publication of `online`;
- a fresh bridge metadata publication with the expected agent version;
- a fresh initial state publication;
- fresh normal discovery publications.

Pre-existing retained values do not satisfy these checks. Only then promote the
transaction into the panel subentry, clear the journal, and report success. The
core bridge plus the current Wi-Fi and independent bus watchdog defaults are
enabled. Optional voice, diyHue, HA control, and certificate-recovery components
remain off until their focused post-install flow is completed.

### Automatic mesh priority

`MESH_PRIORITY=0` continues to mean “does not participate,” and lower positive
values continue to win. For a new fleet, the first panel receives priority `1`;
each subsequent eligible panel receives the next unused positive integer.

Priorities are stable and are never renumbered merely because a panel is added
or removed. This avoids unnecessary leader preemption. Existing installations
retain their configured priorities during migration. A focused Advanced fleet
screen may disable participation or reorder priorities, but add-panel
onboarding never asks for a number.

## Day-two configuration

### Fleet menu

The fleet options menu contains:

- **Broker and credentials**
- **Fleet defaults**
- **Mesh failover order**
- **Agent rollout**
- **Add panel**
- **Fleet diagnostics**

A broker endpoint, TLS, discovery-prefix, or credential change is staged. The
new profile is validated through Home Assistant and from every registered
panel before any running service is changed. The default policy is strict: an
offline or failing panel blocks the fleet change and is listed with its failed
stage. The user may repair, remove, or move that panel before retrying; the
integration does not silently create a mixed-broker fleet.

After all preflights pass, panels switch in a rolling transaction. The previous
profile and release remain available until all panels publish health on the new
broker. Any failure rolls already-switched panels back to the prior profile.

### Panel menu

Each panel subentry exposes focused actions:

- **Rename or change address**
- **Repair SSH credentials**
- **Manage optional features**
- **Reinstall or update agent**
- **Restart services**
- **Collect diagnostics**
- **Remove panel**

Enabling an optional feature opens only that feature's required inputs. Home
Assistant area, device, entity, and label selectors replace raw JSON wherever
the source data is in Home Assistant registries. Expert-only values remain in a
clearly labeled Advanced step and are documented individually.

Removing a panel first runs an explicit uninstall-and-cleanup transaction. It
captures the panel's current discovery publications before stopping the
service, then clears the panel-owned retained discovery, state, bridge metadata,
and availability topics. Fleet-owned `brilliant/mesh` topics are never removed
with one panel. The subentry is deleted only after verified cleanup. If the
panel is unreachable, the UI explicitly distinguishes **Remove Home Assistant
configuration only** from a verified panel uninstall, warns that retained
topics may remain, and links to the safe cleanup procedure.

## Error model and remediation

Every broker/provisioning operation returns a typed result containing:

- stage;
- stable error code;
- retryable/non-retryable classification;
- short user-facing summary;
- redacted technical detail;
- documentation slug;
- cleanup or rollback result.

Canonical error families include:

| Family | Representative failures | Primary remediation |
|---|---|---|
| Home Assistant MQTT | Integration missing, entry not loaded, broker disconnected | Configure/reload HA MQTT and retry |
| Network | DNS failure, timeout, connection refused, route unavailable | Correct endpoint, DNS, firewall, or VLAN route |
| Authentication/ACL | Bad username/password, publish denied, subscribe denied, discovery write denied | Correct fleet credentials or broker ACL |
| Broker behavior | Messages do not cross due to broker mismatch or a silent ACL drop; retained payload missing/changed; protocol incompatibility | Verify the shared broker and both ACL directions, then enable retention |
| TLS | Unknown CA, hostname mismatch, expired/not-yet-valid certificate, panel clock invalid | Correct time, hostname, validation mode, or CA |
| Panel SSH | Host unreachable, root authentication rejected, duplicate identity | Correct address/credential or reconfigure existing panel |
| Panel compatibility | Unsupported firmware/architecture, low disk/memory, conflicting service | Follow compatibility or cleanup guidance |
| Activation | Unit failed, availability timeout, initial state/discovery timeout | Show a redacted service-log excerpt, roll back, and offer retry |
| Migration | Conflicting/invalid legacy data, phased commit interrupted | Leave legacy entry active or resume idempotent migration |

The UI presents one recommended action, a retry action, and a direct link to the
matching troubleshooting section. It never displays or logs MQTT passwords,
root passwords, private keys, tokens, or unredacted environment files.

## Transaction and rollback rules

- Broker-validation failure creates no fleet entry.
- Panel preflight failure never enables or replaces the service.
- A durable provisioning journal is written before activation and recovered on
  Home Assistant startup. Recovery either completes health verification and
  creates the subentry or rolls the panel back before deleting the journal.
- First-install activation failure leaves no active partial service; staged
  files may remain only as a versioned, inactive retry artifact.
- Upgrade activation failure restores the previous release, environment file,
  unit state, and component selection.
- Broker-change failure keeps or restores the prior connection profile on every
  panel.
- Probe subscriptions, temporary clients, files, and retained topics are
  cleaned after all outcomes.
- Cleanup or rollback failure is never hidden behind the original error. It
  creates a repair issue containing both failures and the safe manual action.

Runtime outages do not block Home Assistant startup. The fleet loads degraded
and reconnects with bounded exponential backoff. One broker outage produces one
fleet repair issue rather than one alert per panel. A single panel produces a
panel repair issue only after a grace period. Repeated identical errors update
the existing issue instead of generating notification storms.

## Migration from per-panel entries

Migration is automatic, idempotent, and does not contact or reinstall a panel.

### Grouping

Legacy entries are grouped by an exact normalized broker profile:

- host and port;
- username and password;
- TLS and certificate settings;
- discovery prefix.

Each group becomes one fleet. Entries using different profiles become separate
fleets. This preserves working deployments instead of guessing that similarly
named brokers or usernames are equivalent.

Within a group, legacy panel-specific component and feature values become
subentry overrides. Existing mesh priorities are retained exactly. A later
options flow can adopt fleet defaults deliberately.

### Recoverable phases

1. Read all legacy entries and build a complete migration plan without writes.
2. Validate unique panel identities, config-entry references, entity/device
   registry targets, and normalized broker groups.
3. Select one deterministic legacy entry per group as the fleet anchor so its
   config entry ID can be preserved.
4. Create panel subentries and retarget this integration's management
   entities/devices while preserving unique IDs and entity IDs.
5. Load the new fleet and verify every expected panel controller exists.
6. Mark non-anchor legacy entries superseded and remove them only after the
   fleet passes verification.

A persisted phase marker makes restart recovery deterministic. Before the
commit phase, any error leaves all legacy entries untouched. During commit, a
restart resumes or rolls back from the marker rather than creating duplicate
managers.

Physical MQTT Discovery entities belong to the Home Assistant MQTT integration,
so their topics, discovery unique IDs, device identifiers, and entity IDs do not
change. Panel environment files also remain unchanged until a later explicit
reconfigure or update.

If a migration cannot prove registry or entry consistency, it leaves the
legacy configuration operational and creates one repair issue with redacted
diagnostics. It never discards conflicting values.

## Security and credential handling

- Use one dedicated fleet MQTT credential rather than Home Assistant owner or
  internal broker credentials.
- Store the fleet MQTT password once in the fleet entry.
- Keep the existing panel SSH-secret storage model for this release, but never
  repeat root credentials in fleet data.
- Protect the temporary provisioning journal like Home Assistant config-entry
  storage, redact it from diagnostics, and delete it after promotion or
  verified rollback.
- Use unique, unguessable setup topics and client IDs.
- Remove retained probe payloads after every outcome.
- Redact secrets from logs, config-flow errors, repair issues, diagnostics, and
  support bundles.
- Treat custom CA certificates as public trust material but never expose client
  private keys if future broker configurations add mutual TLS.
- Do not weaken certificate validation automatically. A TLS error explains the
  correct CA/hostname/time remedy and leaves any insecure override in Advanced.

For custom ACLs, documentation grants the Brilliant user only the required
topic families: read/write `brilliant/#` and write access to the configured
discovery prefix. Home Assistant's broker user retains its own permissions.

## Diagnostics and documentation

Fleet diagnostics include, with secrets redacted:

- broker kind, host, port, TLS mode, and discovery prefix;
- Home Assistant MQTT entry state;
- last successful validation timestamp and stage;
- aggregate panel health and migration version;
- active repair issues.

Panel diagnostics include:

- identity/model, address, installed version, and enabled components;
- assigned mesh priority and current leadership observation;
- SSH reachability result;
- panel-side broker preflight result;
- availability and last state/discovery timestamps;
- last deployment transaction and rollback result;
- bounded/redacted service journal excerpts.

User documentation is reorganized around tasks:

1. **Choose MQTT:** official Mosquitto or an existing broker.
2. **Prepare credentials and ACLs.**
3. **Create a Brilliant fleet.**
4. **Add a panel.**
5. **Enable optional features.**
6. **Change brokers or rotate credentials.**
7. **Resolve error `<code>`.**

Every stable error code has an anchor containing cause, checks, corrective
action, and a safe retry procedure. Mosquitto-specific guidance appears only in
the official-app path; external-broker guidance remains protocol-neutral.

## Testing strategy

### Unit tests

- Fleet and panel schemas, defaults, menus, progress states, and Advanced-field
  visibility.
- Secret sentinel preservation and redaction.
- Normalization and equality of broker profiles.
- Validation state-machine timeouts, cancellation, cleanup, and error mapping.
- Panel inspection, duplicate identity handling, priority allocation, staged
  activation, and rollback.
- Fleet health aggregation and repair-issue deduplication.
- Migration grouping, phase recovery, overrides, and registry retargeting.

### Broker integration tests

Run real disposable broker instances in CI for:

- plain authenticated MQTT;
- TLS with trusted and untrusted CAs;
- incorrect hostname and expired/not-yet-valid certificates;
- bad credentials;
- publish-only and subscribe-only ACL failures;
- discovery-prefix denial;
- retained messages disabled or altered;
- Home Assistant and device clients connected to different brokers;
- broker disconnect during every validation stage;
- probe cleanup after success, timeout, cancellation, and process failure.

Tests must use Home Assistant's MQTT APIs on one side and the same client stack
as the panel preflight on the other, so a mock-only path cannot mask protocol or
retention defects.

### Provisioning and migration tests

Use a controlled SSH test server/fake panel filesystem to verify:

- supported and unsupported inspection results;
- first install and idempotent reinstall;
- interruption before activation;
- Home Assistant interruption immediately after activation and provisioning-
  journal recovery;
- service failure after activation;
- exact restoration of a prior release/config;
- offline panels during fleet changes;
- legacy single-broker, multi-broker, and differing-override fixtures;
- restart during every migration phase;
- unchanged MQTT entity IDs and integration management entity IDs.

### Hardware qualification

Run canaries against both the official Mosquitto app and an existing remote
broker. Exercise:

- panel, Home Assistant, and broker restarts;
- DNS loss, route loss, and firewall rejection;
- segmented IoT VLAN access;
- TLS CA, hostname, and clock failures;
- credential rotation and broker moves;
- an offline panel during a proposed fleet change;
- rollback after service activation failure;
- retained discovery/state recovery after outages.

## Performance and safety gates

This release keeps one resident `brilliant-mqtt` process, one MQTT connection,
and one Brilliant message-bus observer per panel. The panel-side preflight is
temporary, never opens the Brilliant bus, and must exit before the agent starts.

Against the same release and panel workload, a hardware soak must show:

- no additional resident process, MQTT connection, or bus peer;
- steady-state agent RSS no more than 5 MiB above baseline;
- average agent CPU no more than 2 percentage points above baseline during an
  idle 30-minute comparison;
- continued compliance with the existing 96 MiB memory and 20% CPU service
  limits;
- no material increase in `message_bus` CPU;
- no duplicate client-ID disconnects, reconnect storms, or ghost bus peers;
- normal physical-control responsiveness throughout broker and HA restarts.

Any duplicate bus peer, reconnect storm, loss of physical responsiveness,
retained-topic leak, or failed rollback is a release blocker regardless of
average CPU/RSS.

## Rollout

1. Land the isolated data types, validators, and migration dry-run diagnostics
   without changing existing entries.
2. Exercise dry-run migration against captured/redacted config fixtures.
3. Enable the new flow on a one-panel official-Mosquitto canary.
4. Validate one panel against an existing remote broker.
5. Migrate a mixed multi-panel fleet and run the resource/failure soak.
6. Publish the new task-oriented prerequisite and error-code documentation.
7. Release the config-entry migration only after dry-run output, rollback, and
   entity-registry preservation pass.

The implementation plan may divide these into separately reviewable milestones,
but the user-visible migration is not shipped until the full validation,
rollback, documentation, and preservation gates are met.

## Final acceptance checklist

- MQTT remains the production transport.
- Official Mosquitto is recommended, documented, and never forced.
- Existing brokers are first-class and pass the same validation contract.
- One fleet credential is stored once and deployed to all panels in that fleet.
- New panel onboarding has no broker fields, raw JSON, mesh priority, or
  unrelated optional-feature settings.
- Broker validation proves both message directions and retained behavior.
- Panel validation proves the actual panel network path before activation.
- Success requires normal agent availability and discovery.
- Errors identify the failed stage and provide a specific recovery link.
- Failed installs, upgrades, and broker changes are inactive or rolled back.
- Existing entries migrate without changing MQTT topics or entity IDs.
- Different legacy brokers become different fleets rather than being merged.
- The panel runs no additional resident process and passes resource gates.

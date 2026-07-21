# MQTT Fleet Onboarding Simplification — Design

- **Date:** 2026-07-20
- **Last revised:** 2026-07-21, after medium-effort Fable review and local
  verification
- **Status:** Approved 2026-07-21; implementation plans ready
- **Scope:** Replace the one-entry-per-panel setup wizard with a fleet-first
  Home Assistant integration, validate MQTT end to end during onboarding, and
  preserve the existing MQTT topic/entity contract. Narrow, additive panel-
  agent changes are authorized for server-authenticated TLS and retained-topic
  ownership; this is not an agent rewrite.
- **Home Assistant baseline:** 2026.6.0 or newer, matching `hacs.json`.
- **Related documentation:**
  - [Plan 1 — MQTT validation and panel-agent foundations](../plans/2026-07-21-mqtt-foundations.md)
  - [Plan 2 — fleet config entry and panel onboarding](../plans/2026-07-21-fleet-onboarding.md)
  - [Plan 3 — fleet migration and day-two operations](../plans/2026-07-21-fleet-migration-operations.md)
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

The integration becomes a hub with exactly one fleet config entry per Home
Assistant installation and one config subentry per panel. Home Assistant's MQTT
integration exposes one active broker path, so the Brilliant fleet must use
that same broker. The fleet owns one normalized panel connection profile and a
shared dedicated Brilliant MQTT credential. A panel subentry owns only panel
identity, SSH provisioning, and panel-specific feature overrides.

On first setup, users choose one of two fully supported paths:

1. **Home Assistant Mosquitto** — the recommended shortcut using the official
   Mosquitto Broker app/add-on (`core_mosquitto`).
2. **Existing MQTT broker** — any compatible local, remote, or hosted broker
   reachable by Home Assistant and the panels.

The integration never installs the official app, edits broker configuration,
creates broker accounts, or forces a broker choice. It documents the official
app as a prerequisite only when that path is selected.

MQTT Discovery uses the existing fixed `homeassistant` prefix in this release.
If Home Assistant is configured with a different discovery prefix, onboarding
stops with a documented compatibility error rather than creating entities that
the current panel agent cannot discover correctly.

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
The only panel-agent additions are TLS client configuration, a shared client
factory used by runtime and preflight, and a retained-topic ownership ledger.

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

1. Make MQTT configuration a once-per-Home-Assistant-installation concern.
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
8. Preserve existing MQTT topic names, discovery identities, entity IDs,
   automations, panel runtime behavior, and current resource use. An additive
   management topic may be introduced for cleanup ownership.
9. Migrate compatible existing entries without requiring a panel reinstall.
   Preserve incompatible or conflicting legacy entries in compatibility mode
   until an explicit consolidation repair succeeds.
10. Enforce one Brilliant fleet and broker path, matching Home Assistant's
    active MQTT integration, rather than implying unsupported multi-broker
    operation.

## Non-goals

- Replacing MQTT with a direct panel API.
- Rewriting the Brilliant message-bus client or panel agent in Rust.
- Automatically installing, starting, or configuring `core_mosquitto`.
- Reading or reusing Home Assistant's hidden internal MQTT credentials.
- Automatically creating Home Assistant users or external broker accounts.
- Managing arbitrary broker ACL files.
- Redesigning existing MQTT discovery/state/command topic identifiers or
  payloads.
- Supporting multiple simultaneous Brilliant brokers in one Home Assistant
  installation.
- MQTT over WebSockets, mutual TLS/client certificates, or automatically
  weakening server certificate validation in this release.
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

Exactly one Brilliant fleet entry is allowed. Starting setup when that entry
already exists aborts with a link to **Add panel** on the existing fleet. This
matches Home Assistant's single active MQTT client and avoids presenting
multi-broker behavior that the MQTT integration cannot supply. An existing,
remote, or hosted broker remains fully supported; it simply becomes the one
broker shared by Home Assistant and every managed Brilliant panel.

## Architecture and ownership

Change the integration type from `device` to `hub`. The top-level config entry
is the fleet hub; panels are Home Assistant config subentries.

```text
Home Assistant MQTT integration (one client) ─┐
                                               │ same broker
Single Brilliant MQTT fleet entry             │
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
- optional public CA material/reference for strict server certificate
  validation;
- fleet feature defaults and mesh-priority allocation state;
- the installation-global Home Assistant control and scene configuration;
- schema and migration version.

The MQTT Discovery prefix is not configurable or persisted as connection data
in v1. It remains the current hard-coded `homeassistant` contract. The flow
reads Home Assistant's effective MQTT discovery prefix and returns
`unsupported_discovery_prefix` with a direct remediation link if it differs.

The seven current installation-global values move from copied per-panel data
to this single fleet entry: `ha_control_enabled`, `ha_control_label`,
`room_overrides`, `ha_control_domains`, `max_mirrored_entities`, `scene_panel`,
and `scene_actions`. `scene_panel` must reference a panel subentry belonging to
this fleet. No lexicographic panel-slug election or implicit settings owner
remains.

The broker kind changes guidance and defaults only. Runtime validation and
panel rendering use one normalized connection-profile type for both paths.

The fleet password is stored exactly once. It is never copied into every panel
subentry, although the provisioner renders it into each panel's protected
environment file when deploying. Reconfigure forms use Home Assistant's secret
sentinel behavior so an unchanged password is never re-entered or replaced by
display text.

### Panel config subentry

Each panel subentry persists:

- stable identity derived from the pinned SSH host public key;
- the canonical pinned SSH host public key and its SHA-256 fingerprint;
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
- **`MigrationPlanner`** is a pure, idempotent helper that inspects all legacy
  entries and produces a no-write consolidation plan or a conflict report.
- **`MigrationCoordinator`** is a domain-scoped, single-lock service that runs
  only after all Brilliant entries have been enumerated. It commits a safe
  plan in recoverable phases; per-entry migration hooks never mutate sibling
  entries.

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

- Home Assistant's sole active MQTT integration already connected to the same
  broker;
- support for MQTT 5 as required by current Home Assistant and MQTT 3.1.1 as
  used by the current panel client;
- an endpoint reachable from both Home Assistant and each panel;
- retained messages and wildcard subscriptions;
- a dedicated fleet credential;
- permissions to read and write `brilliant/#`;
- permission to write `homeassistant/#` for MQTT Discovery;
- an effective Home Assistant discovery prefix of `homeassistant`.

Host, port, username, and password are normal fields. TLS enablement, strict
certificate validation, and custom CA input are under Advanced settings. Error
text and documentation remain broker-neutral.

External-broker documentation must describe both broker principals, because a
correct panel ACL alone cannot complete the end-to-end probes:

| Principal | Minimum Brilliant-related access |
|---|---|
| Dedicated Brilliant fleet user used by every panel | Read/write `brilliant/#`; write `homeassistant/#` |
| Home Assistant MQTT user | Read/write `brilliant/#`; read `homeassistant/#`; retain its normal MQTT permissions, including write access to its configured birth/will/status topics (default `homeassistant/status`) |

The official Mosquitto app normally supplies Home Assistant's internal access;
the dedicated Brilliant user is still required. On an external broker, the
operator configures both principals. Validation names the client and direction
when the broker returns an explicit authorization failure; an otherwise silent
timeout remains an ambiguous routing-or-ACL error.

### TLS compatibility contract

TLS for an existing or customized official broker is a supported v1 path, not
documentation-only configuration. The panel agent gains these additive
settings:

- `MQTT_TLS_ENABLED`;
- `MQTT_TLS_CA_FILE`, optional when the broker chains to the panel's system
  trust store.

A single MQTT client factory constructs the strict server-authenticated TLS
context for both the resident agent and the temporary panel preflight. It
enables hostname verification and certificate-chain validation and never falls
back to plaintext or an insecure context after failure. The broker hostname in
the fleet profile is the verified TLS server name; IP endpoints therefore need
a certificate valid for that IP or must be replaced with a matching DNS name.
Mutual TLS, client key distribution, and MQTT over WebSockets are deferred.

No fleet entry is created while Home Assistant's MQTT integration is absent or
disconnected. The flow preserves non-secret values, explains how to configure
MQTT, and allows retry without restarting the integration setup.

No fleet entry is created when Home Assistant's effective discovery prefix is
not `homeassistant`. The `unsupported_discovery_prefix` error explains the
current agent compatibility constraint and links to the exact Home Assistant
MQTT option that must be restored before retrying.

## Broker validation contract

Validation must prove behavior, not merely open a TCP socket. It uses a random
setup ID, data probes below `brilliant/setup/<setup_id>/`, and one discovery-ACL
probe at `homeassistant/brilliant_mqtt_setup/<setup_id>/probe`. The latter does
not end in `/config`, so Home Assistant cannot interpret it as entity
discovery. The setup ID is not retained or reused.

| Stage | Operation | What it proves |
|---|---|---|
| `ha_mqtt_ready` | Confirm Home Assistant's MQTT integration is loaded and connected | HA has an active broker path |
| `fleet_auth` | Open a temporary device client with the supplied Brilliant credential | Endpoint, protocol, TLS, and authentication work |
| `panel_to_ha` | Subscribe through Home Assistant, then publish a random nonce from the temporary device client | Device publish ACL, HA subscribe path, and same-broker routing work |
| `ha_to_panel` | Subscribe through the temporary client, then publish a different nonce through Home Assistant | HA publish path and device subscribe ACL work |
| `discovery_write` | Subscribe through Home Assistant, then publish a nonce from the temporary device client to the non-entity discovery-ACL probe topic | Device discovery-prefix write ACL and HA discovery-prefix subscription work without creating a test entity |
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

Before sending the password, perform an unauthenticated SSH key exchange and
collect the server host public key. The candidate key and its SHA-256
fingerprint become the in-progress flow's trust-on-first-use pin. The
authenticated inspection must present that exact key before the password is
sent; the first connection never disables host-key checking merely to make
password authentication convenient.

The stable fingerprint is the standard SHA-256 digest of the decoded SSH public
key blob, not a hash of address-dependent `known_hosts` text. Reformatting the
same key therefore cannot change panel identity.

### Step 2: Inspect and confirm

SSH inspection collects the minimum safe facts needed to continue:

- the candidate SSH host-key fingerprint, used as the stable panel identity;
- hostname/model and suggested friendly name;
- supported architecture, firmware, Python, and service-manager versions;
- available disk and memory;
- current Brilliant MQTT installation/version;
- conflicting retired services;
- duplicate-panel status in the single configured fleet.

The second screen shows the fingerprint, discovered facts, and an editable
friendly name. Confirming the screen persists the canonical host public key in
the successful subentry. A duplicate fingerprint offers reconfiguration of the
existing panel rather than creating a second subentry.

Every later SSH connection verifies the pin before transmitting credentials.
Credential repair does not replace the pin. Changing a panel address first
checks that the new endpoint presents the same key. A different key fails
closed and opens an explicit **Rebind replaced/reflashed panel** flow that
shows both fingerprints and preserves the existing MQTT slug and entity
identity only after deliberate confirmation. Host-key changes are never
silently accepted.

The current opt-in `trust_host_key_changes` auto-repin behavior is not carried
into the fleet model. A legacy panel with that option enabled remains in
compatibility mode until the repair flow explains the change and the user
explicitly accepts fail-closed rebind behavior.

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
- write access from the panel to the non-entity `homeassistant` discovery-ACL
  probe topic;
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

A broker endpoint, TLS, or credential change is staged. For an endpoint change,
the user first points Home Assistant's MQTT integration at the proposed broker;
the old broker and old panel credentials must remain available until the fleet
move is confirmed. The new profile is then validated through Home Assistant
and from every registered panel before any running service is changed. An
offline or failing panel blocks the change and is listed with its failed stage.
The user may repair or remove that panel before retrying; the integration never
silently creates a mixed-broker fleet.

After all preflights pass, panels switch sequentially using a durable per-panel
journal. The screen continuously identifies panels still on the old profile,
confirmed on the new profile, and failed. On failure, the transaction halts;
no remaining panel is changed. V1 offers an explicit one-click revert for each
already-switched panel, followed by instructions to restore Home Assistant's
old MQTT connection when the endpoint changed. It does not promise automatic
cross-panel rollback while Home Assistant can observe only one broker. Fully
automatic fleet rollback is deferred until the provisioning journal has proven
safe on hardware. Old broker/profile material is removed only after the user
confirms that every panel is healthy on the new path.

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
stops the service, consumes its retained-topic ownership manifest, then clears
the panel-owned discovery, state, bridge metadata, and availability topics.
Fleet-owned `brilliant/mesh` topics are never removed with one panel. Normal
removal deletes the subentry only after verified cleanup. If the panel is
unreachable, **Remove Home Assistant configuration only** is an explicit escape
hatch: it cannot claim that the service was stopped and does not clear broker
topics that a still-running panel could immediately republish. It warns about
the orphaned service/topics and records the cleanup procedure. A separate
advanced **Clean broker topics after externally stopping the agent** action may
use a valid manifest, but only after an operator attestation; it still does not
claim a verified panel uninstall.

### Retained-topic ownership and cleanup

The additive retained topic `brilliant/<panel_slug>/ownership` carries a
versioned JSON manifest:

```json
{
  "schema_version": 1,
  "panel_slug": "kitchen",
  "topics": [
    "brilliant/kitchen/availability",
    "brilliant/kitchen/bridge",
    "brilliant/kitchen/<peripheral>/state",
    "homeassistant/<component>/<unique_id>/config"
  ]
}
```

The actual array contains complete concrete topic names, not patterns. It lists
only retained topics owned by that panel. The agent keeps the same ledger in a
durable file below `/var/brilliant-mqtt/`. Before publishing a new retained
topic, it durably adds the topic and receives acknowledgement for the updated
manifest; it does not publish the new retained value if that step fails. After
successfully clearing a retired topic, it removes that topic and republishes the
manifest. During uninstall, cleanup clears every listed topic and clears the
ownership manifest last. This ordering favors harmless over-inclusion after a
crash rather than an orphaned retained topic.

Manifest parsing is defensive: v1 accepts at most 4,096 unique topics and
256 KiB of JSON, rejects unknown fields/types or a mismatched slug, and never
uses a manifest as authority to delete an arbitrary topic. Each
`brilliant/<panel_slug>/...` entry must match a known retained panel-topic
shape. Each discovery entry must still contain a retained payload whose parsed
device identifier matches `brilliant_panel_<panel_slug>` before it is cleared.
An invalid or over-limit manifest falls back to the strict legacy scan and
surfaces a repair issue.

The ledger and manifest never claim `brilliant/mesh` or mesh discovery topics,
which are fleet-owned. On the first ledger-aware upgrade, the integration seeds
the ledger with a bounded legacy scan before treating it as authoritative. For
an older agent with no valid manifest, the fallback subscribes briefly to
retained `homeassistant/+/+/config` messages and the known
`brilliant/<panel_slug>/#` namespace. It clears only discovery payloads whose
parsed Brilliant device identifier exactly matches the target panel slug and
known panel state/meta/availability topics. It never clears mesh or an
unrecognized payload. If ownership cannot be proven, removal preserves the
message and reports the exact manual-cleanup guidance instead of guessing.

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
| Home Assistant MQTT | Integration missing, entry not loaded, broker disconnected, non-default discovery prefix | Configure/reload HA MQTT, restore the `homeassistant` prefix, and retry |
| Network | DNS failure, timeout, connection refused, route unavailable | Correct endpoint, DNS, firewall, or VLAN route |
| Authentication/ACL | Bad username/password, publish denied, subscribe denied, discovery write denied | Correct fleet credentials or broker ACL |
| Broker behavior | Messages do not cross due to broker mismatch or a silent ACL drop; retained payload missing/changed; protocol incompatibility | Verify the shared broker and both ACL directions, then enable retention |
| TLS | Unknown CA, hostname mismatch, expired/not-yet-valid certificate, panel clock invalid | Correct panel time, endpoint hostname, broker certificate, or CA |
| Panel SSH | Host unreachable, host-key mismatch, root authentication rejected, duplicate identity | Correct the address/credential, reconfigure the existing panel, or deliberately rebind a replaced panel |
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
- Broker-change failure halts before touching another panel, preserves the
  per-panel old/new state in its journal, and offers explicit reversion of each
  already-switched panel. Automatic cross-panel rollback is not a v1 guarantee.
- Probe subscriptions, temporary clients, files, and retained setup topics are
  cleaned after all outcomes. Panel removal clears only topics proven by the
  ownership manifest or strict legacy fallback.
- Cleanup or rollback failure is never hidden behind the original error. It
  creates a repair issue containing both failures and the safe manual action.

Runtime outages do not block Home Assistant startup. The fleet loads degraded
and reconnects with bounded exponential backoff. One broker outage produces one
fleet repair issue rather than one alert per panel. A single panel produces a
panel repair issue only after a grace period. Repeated identical errors update
the existing issue instead of generating notification storms.

## Migration from per-panel entries

Migration is automatic only when consolidation is provably lossless. It is
idempotent and does not contact or reinstall a panel on the automatic path.

### Automatic-consolidation eligibility

The planner reads every legacy entry before changing any of them. Exactly one
fleet can be produced automatically only when all of these conditions hold:

- every entry has the same exact normalized broker host, port, username,
  password, TLS mode, and CA material;
- the profile passes the broker validation contract against Home Assistant's
  active MQTT connection;
- Home Assistant's effective discovery prefix is `homeassistant`;
- the seven installation-global Home Assistant control/scene values are
  canonically identical across entries;
- every entry has a canonical pinned SSH host public key, and its SHA-256
  fingerprint is unique;
- every panel already uses the default fail-closed SSH host-key policy; an
  enabled legacy `trust_host_key_changes` option requires user acknowledgement;
- all config-entry, device-registry, and entity-registry references can be
  retargeted without changing unique IDs or entity IDs.

Canonical panel identity is derived from the existing pinned SSH host-key data,
including legacy `DATA_SSH_HOST_KEY`; migration does not invent identity from a
mutable address or panel slug. A missing/invalid key, duplicate fingerprint,
different normalized broker profile, or conflicting global value makes the
whole automatic plan ineligible. Similar DNS names, broker aliases, or
different credentials are never assumed equivalent because proving that would
require panel mutation.

Within an eligible plan, panel-specific component and feature values become
subentry overrides. Existing mesh priorities and immutable MQTT panel slugs are
retained exactly. The shared global values move once to the fleet entry, and
`scene_panel` is converted to a same-fleet subentry reference.

### Coordinator and recoverable phases

Home Assistant invokes `async_migrate_entry` one entry at a time, so that hook
must never inspect-and-edit sibling entries. It may normalize that entry's own
schema, advance a compatibility version, and mark it
`legacy_pending_consolidation`; it returns success without disabling the
legacy runtime.

A domain-scoped `MigrationCoordinator` runs after all Brilliant entries have
been enumerated and Home Assistant MQTT is ready. Under one integration-wide
lock it:

1. reads all legacy entries and builds a complete no-write plan;
2. either records a conflict repair issue or persists the eligible plan and
   phase marker;
3. unloads/quiesces all candidate legacy managers;
4. converts the deterministic anchor entry into the fleet and creates panel
   subentries, including the single copy of global settings;
5. retargets this integration's management entities/devices while preserving
   their unique IDs and entity IDs;
6. loads the fleet and verifies that every expected panel controller and
   registry target exists;
7. marks sibling legacy entries superseded and removes them only after
   verification.

The persisted marker includes the plan digest, anchor, candidate IDs, current
phase, and enough pre-change data to resume or reverse an interrupted commit.
Before quiescing, any error leaves every legacy entry untouched. After
quiescing, startup recovery must finish or reverse that exact plan before a new
one can begin, preventing duplicate managers and partial sibling deletion.

Physical MQTT Discovery entities belong to the Home Assistant MQTT integration,
so their topics, discovery unique IDs, device identifiers, and entity IDs do not
change. Panel environment files also remain unchanged until a later explicit
reconfigure or update.

If automatic consolidation is ineligible, all legacy entries and their values
remain in compatibility mode and one repair issue explains each conflicting
field without exposing secrets. The repair flow asks the user to choose one
canonical broker profile and one global control/scene configuration, validates
that choice against Home Assistant, then reconfigures and verifies panels one
at a time using the normal journaled broker-change path. It also requires an
explicit acknowledgement before disabling any legacy automatic host-key
repinning. Only after every panel matches does the coordinator rerun
consolidation. Cancellation or failure never discards a competing value or
falsely reports that multiple brokers are supported.

## Security and credential handling

- Use one dedicated fleet MQTT credential rather than Home Assistant owner or
  internal broker credentials.
- Store the fleet MQTT password once in the fleet entry.
- Keep the existing panel SSH-secret storage model for this release, but never
  repeat root credentials in fleet data.
- Fetch the SSH host public key before password authentication, pin the exact
  key for the authenticated connection, persist it on success, and fail closed
  on every later mismatch.
- Protect the temporary provisioning journal like Home Assistant config-entry
  storage, redact it from diagnostics, and delete it after promotion or
  verified rollback.
- Use unique, unguessable setup topics and client IDs.
- Remove retained probe payloads after every outcome.
- Redact secrets from logs, config-flow errors, repair issues, diagnostics, and
  support bundles.
- Treat custom CA certificates as public trust material. V1 does not accept or
  distribute MQTT client private keys.
- Never weaken certificate validation automatically or expose an insecure TLS
  toggle. A TLS error explains the correct CA, hostname, or panel-time remedy.

For custom ACLs, documentation grants the Brilliant fleet user only the
required topic families: read/write `brilliant/#` and write
`homeassistant/#`. It separately documents Home Assistant's required access
from the broker-principal table above; validation exercises both principals.

## Diagnostics and documentation

Fleet diagnostics include, with secrets redacted:

- broker kind, host, port, TLS mode, and the fixed discovery-prefix
  compatibility result;
- Home Assistant MQTT entry state;
- last successful validation timestamp and stage;
- aggregate panel health and migration version;
- active repair issues.

Panel diagnostics include:

- SSH host-key fingerprint, model, address, installed version, and enabled
  components;
- assigned mesh priority and current leadership observation;
- SSH reachability result;
- panel-side broker preflight result;
- availability and last state/discovery timestamps;
- last deployment transaction and rollback result;
- retained-topic ownership schema/count and last verified cleanup result;
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
- Singleton fleet enforcement and second-setup redirection to **Add panel**.
- Fixed `homeassistant` discovery-prefix compatibility checks and rejection of
  a non-default Home Assistant prefix.
- Secret sentinel preservation and redaction.
- Normalization and equality of broker profiles.
- Validation state-machine timeouts, cancellation, cleanup, and error mapping.
- TLS settings parsing and one strict client factory shared by runtime and
  preflight.
- SSH trust-on-first-use pinning, fingerprint-derived identity, duplicate
  identity handling, mismatch failure, and explicit rebind.
- Priority allocation, staged activation, and rollback.
- Fleet health aggregation and repair-issue deduplication.
- Single ownership of all seven global control/scene keys and same-fleet scene
  references.
- Retained-topic ledger ordering, manifest validation, mesh exclusion, and
  strict legacy cleanup filtering.
- Migration eligibility/conflict reporting, coordinator locking, phase
  recovery, overrides, and registry retargeting.

### Broker integration tests

Run real disposable broker instances in CI for:

- plain authenticated MQTT;
- server-authenticated TLS with system/custom trusted and untrusted CAs using
  the actual panel client factory;
- incorrect hostname and expired/not-yet-valid certificates;
- bad credentials;
- panel-principal and Home-Assistant-principal publish/subscribe ACL failures in
  both message directions;
- `homeassistant/#` discovery-write denial;
- non-default Home Assistant discovery-prefix rejection before provisioning;
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
- unauthenticated host-key collection followed by pinned password
  authentication;
- address changes with the same key, mismatches that fail before password
  transmission, and deliberate rebind after replacement/reflash;
- first install and idempotent reinstall;
- plaintext and strict-TLS agent/preflight configuration parity;
- interruption before activation;
- Home Assistant interruption immediately after activation and provisioning-
  journal recovery;
- service failure after activation;
- exact restoration of a prior release/config;
- offline panels during fleet changes;
- sequential broker change, halt-on-failure, explicit per-panel reversion, and
  old/new journal recovery;
- legacy exact-profile consolidation, conflicting broker/global compatibility
  mode, legacy auto-repin acknowledgement, and repair-driven consolidation
  fixtures;
- restart during every migration phase;
- unchanged MQTT entity IDs and integration management entity IDs;
- ownership-manifest uninstall, ledger-aware upgrade seeding, legacy fallback,
  and refusal to clear ambiguous or mesh topics.

### Hardware qualification

Run canaries against both the official Mosquitto app and an existing remote
broker. Exercise:

- panel, Home Assistant, and broker restarts;
- DNS loss, route loss, and firewall rejection;
- segmented IoT VLAN access;
- TLS CA, hostname, and clock failures;
- credential rotation and broker moves;
- an offline panel during a proposed fleet change;
- rollback after service activation failure and explicit broker-profile
  reversion after a partial rolling change;
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
retained-topic leak, failed service rollback, or failed explicit broker-profile
reversion is a release blocker regardless of average CPU/RSS.

## Rollout

1. Land the isolated data types, validators, and migration dry-run diagnostics
   without changing existing entries.
2. Exercise dry-run migration against captured/redacted config fixtures.
3. Enable the new flow on a one-panel official-Mosquitto canary.
4. Validate one panel against an existing remote broker.
5. Migrate a compatible multi-panel installation, exercise the conflicting-
   legacy repair path, and run the resource/failure soak.
6. Publish the new task-oriented prerequisite and error-code documentation.
7. Release the config-entry migration only after dry-run output, interrupted-
   phase recovery, explicit reversion, and entity-registry preservation pass.

The implementation plan may divide these into separately reviewable milestones,
but the user-visible migration is not shipped until the full validation,
rollback, documentation, and preservation gates are met.

## Final acceptance checklist

- MQTT remains the production transport.
- Official Mosquitto is recommended, documented, and never forced.
- Existing brokers are first-class and pass the same validation contract.
- Exactly one fleet matches Home Assistant's one active MQTT broker path.
- One fleet credential is stored once and deployed to every managed panel.
- TLS uses strict server authentication in both agent runtime and preflight.
- MQTT Discovery remains on the existing `homeassistant` prefix, and an
  incompatible Home Assistant prefix fails before provisioning.
- New panel onboarding has no broker fields, raw JSON, mesh priority, or
  unrelated optional-feature settings.
- Broker validation proves both message directions and retained behavior.
- Panel validation proves the actual panel network path before activation.
- Success requires normal agent availability and discovery.
- Errors identify the failed stage and provide a specific recovery link.
- Failed installs and upgrades are inactive or rolled back. Partial broker
  changes halt, preserve per-panel state, and expose verified explicit reverts.
- Compatible existing entries migrate without changing MQTT topics or entity
  IDs; conflicts remain intact until the guided repair succeeds.
- SSH host keys are pinned before password authentication and changes fail
  closed outside an explicit rebind.
- Panel-owned retained topics have a durable manifest; cleanup never guesses or
  removes fleet-owned mesh topics.
- The panel runs no additional resident process and passes resource gates.

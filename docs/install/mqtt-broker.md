# MQTT broker prerequisite

brilliant-mqtt keeps MQTT as its lightweight panel transport. Before adding a
panel, Home Assistant and every panel must be able to reach the **same** broker.
Choose either path below:

1. [Home Assistant Mosquitto](#home-assistant-mosquitto-recommended) is the
   recommended shortcut for Home Assistant OS installations.
2. [An existing broker](#existing-broker) is an equally supported path for any
   compatible local, remote, or hosted service.

The integration does not install or start a broker, create users, edit ACLs, or
change broker configuration. Fleet onboarding validates the selected broker
before creating the Brilliant MQTT fleet, then validates the same behavior from
each staged panel before activation. Failures report a stable stage/code and
leave broker configuration untouched.

Primary references:

- [Home Assistant MQTT integration](https://www.home-assistant.io/integrations/mqtt/)
- [Official Mosquitto Broker app/add-on documentation](https://github.com/home-assistant/addons/blob/master/mosquitto/DOCS.md)

## Home Assistant Mosquitto (Recommended)

The official app/add-on is identified internally as `core_mosquitto`. It is the
default recommendation, but it is not a runtime requirement and brilliant-mqtt
never installs or manages it.

1. Install and start **Mosquitto broker** in Home Assistant. Follow the
   [official app instructions](https://github.com/home-assistant/addons/blob/master/mosquitto/DOCS.md).
2. Add or accept the discovered **MQTT** integration under **Settings →
   Devices & services**, and leave MQTT Discovery enabled.
3. Create a dedicated, non-owner Home Assistant user such as `brilliant` under
   **Settings → People → Users**. A deliberately configured local Mosquitto
   login is also valid. Do not grant this device credential Owner or
   Administrator privileges.
4. Give brilliant-mqtt that dedicated username and password. Do **not** try to
   extract or reuse the generated secret credential Home Assistant keeps for
   its own MQTT connection.
5. Enter a LAN hostname or IP address and TCP port that the panels can resolve
   and reach. Use the Home Assistant host's reachable address or an intentional
   DNS name/port mapping—not the app's internal hostname.

Home Assistant Container/Core installations do not provide apps/add-ons; use
the [existing broker](#existing-broker) path instead.

## Existing broker

An existing Mosquitto-compatible broker may be on the LAN, another network, or
a hosted service. It is not a second-class or advanced-only option. Before
starting fleet onboarding:

1. Configure Home Assistant's MQTT integration to use that broker.
2. Confirm the endpoint is reachable from Home Assistant and from the panels'
   network. A successful connection from Home Assistant alone is insufficient.
3. Confirm the broker supports MQTT 5 for Home Assistant, MQTT 3.1.1 for the
   panel client, retained messages, and wildcard subscriptions.
4. Create a dedicated Brilliant fleet username/password and configure both
   principals in the ACL table below.

<a id="mqtt-broker-profile"></a>
<a id="invalid_broker_profile"></a>
## Broker profile troubleshooting

Fleet onboarding reports `invalid_broker_profile` before it opens a
connection. Both broker choices use the same typed profile and limits.

- **Cause:** A required broker value is missing, the port or TLS flag has the
  wrong type or range, or custom CA content is empty or invalid.
- **Check:** Recheck the hostname, TCP port, username/password, TLS selection,
  and complete public CA certificate. Do not paste a private key.
- **Fix:** Correct the profile and use a custom CA only with TLS enabled.
- **Retry:** Correct the form and retry onboarding. No fleet or panel change
  has occurred at this stage.

<a id="mqtt-broker-connect"></a>
## Broker connection troubleshooting

Connection, timeout, unavailable, and broker-rejected errors share this
remediation target.

- **Cause:** DNS, routing, firewall, listener, or broker availability prevents
  the client from completing a connection, or the broker rejects the selected
  protocol/settings.
- **Check:** Resolve the entered hostname from the relevant network, test the
  exact TCP port, inspect broker logs, and confirm MQTT 3.1.1 and MQTT 5
  support.
- **Fix:** Correct DNS/routing/firewall/listener settings or restore the broker.
- **Retry:** Reconnect Home Assistant, confirm the endpoint from the panels'
  network, and retry the failed broker-validation or panel-preflight stage.

<a id="mqtt-broker-authentication"></a>
## Authentication

- **Cause:** The broker rejects the dedicated Brilliant credential. Initial
  validation reports `fleet_auth_failed` or
  `broker_authentication_failed`; staged panel preflight reports the matching
  secret-safe failure.
- **Check:** Confirm the user is enabled, verify the entered username/password,
  and inspect the broker authentication log without exposing credentials.
- **Fix:** Create, correct, or rotate the dedicated non-owner Brilliant
  credential; never substitute Home Assistant's hidden generated credential.
- **Retry:** Re-enter the corrected Brilliant password and retry the failed
  validation stage.

Username/password authentication is required. Anonymous access is unsupported.
Use one dedicated, non-owner Brilliant fleet principal on every panel; do not
reuse Home Assistant's MQTT principal or its hidden generated credential.

An empty fleet may accept a validated credential change. Once panels exist, a
changed broker endpoint, TLS profile, or credential is deliberately deferred
to a guided multi-panel operation; the integration refuses the edit rather
than creating a mixed-broker fleet. Even on an empty fleet, an active Add panel
flow, provisioning journal, recovery, or unreadable journal blocks a broker
change so recoverable panel state cannot be orphaned. Finish or recover that
operation before retrying. Never put broker passwords in git or documentation.

<a id="broker_change_blocked_by_panel_onboarding"></a>
### `broker_change_blocked_by_panel_onboarding`

- **Cause:** A panel onboarding/recovery flow or its durable provisioning
  journal is active, or Home Assistant could not prove that journal is empty.
- **Check:** Finish or cancel the visible **Add Brilliant panel** flow, then
  inspect any Brilliant MQTT fleet repair issue and Home Assistant storage
  health.
- **Fix:** Let the existing transaction commit or complete its verified
  rollback. Repair unreadable Home Assistant storage before any broker edit.
- **Retry:** Reopen the fleet's MQTT broker settings after no panel operation or
  recovery issue remains. The blocked attempt changed no broker setting.

<a id="mqtt-broker-acl"></a>
## Broker ACL

- **Cause:** One principal can connect but a required Brilliant, discovery, or
  cleanup topic direction is denied or silently dropped.
- **Check:** Compare both principals with the table below and inspect broker
  authorization logs while manually testing each publish/subscribe direction.
- **Fix:** Apply the minimum Brilliant-related grants below while preserving
  Home Assistant's existing birth, will, status, and reserved-principal access.
- **Retry:** Reconnect both principals and retry. Fleet validation reruns both
  message directions, Discovery, retained-message, and cleanup probes.

For an external broker, configure both principals:

| Principal | Minimum Brilliant-related access |
|---|---|
| Dedicated Brilliant fleet principal used by the panels | Read/write `brilliant/#`; write `homeassistant/#` |
| Home Assistant MQTT principal | Read/write `brilliant/#`; read `homeassistant/#`; write `homeassistant/brilliant_mqtt_setup/+/probe` for validation cleanup (or retain broader existing write access); keep its normal birth/will/status permissions, including write access to its configured status topic (default `homeassistant/status`) |

Equivalent Mosquitto rules for the Brilliant principal are:

```text
user brilliant
topic readwrite brilliant/#
topic write homeassistant/#
```

Do not replace Home Assistant's existing permissions with only the rows above.
The official Mosquitto app normally owns the internal Home Assistant
principal. If you enable a custom ACL there, preserve the app's documented
permissions for its reserved internal principals. The narrow setup-probe write
rule lets Home Assistant independently clear the packaged validator's
temporary discovery probe; it does not grant general discovery publishing.

Mosquitto can silently drop an unauthorized publish. A timeout may therefore
mean an ACL or routing error even when login succeeded.

The staged panel validator treats `timeout_seconds` as a per-stage result
deadline. If the operating system has already started a connection worker that
cannot be cancelled, the validator waits for that worker and MQTT cleanup to
settle before it exits. The fleet coordinator also owns an outer SSH-process
deadline and terminates a still-running validator before onboarding returns;
this avoids racing cleanup against a late connection.

<a id="mqtt-discovery-prefix"></a>
## MQTT Discovery prefix

- **Cause:** Home Assistant listens below a discovery prefix other than the
  agent's fixed `homeassistant` prefix. Fleet validation returns
  `unsupported_discovery_prefix`.
- **Check:** Inspect Home Assistant's effective MQTT Discovery setting and
  confirm discovery is enabled with prefix `homeassistant`.
- **Fix:** Restore the exact `homeassistant` prefix and reload MQTT if Home
  Assistant requests it.
- **Retry:** Verify existing MQTT entities rediscover after the reload, then
  return to Brilliant MQTT onboarding and retry.

This release publishes discovery only below the fixed `homeassistant` prefix.
In Home Assistant's MQTT integration, keep discovery enabled and its Discovery
Prefix set to `homeassistant`. Fleet onboarding stops before creating a fleet
when the effective prefix differs; panel preflight rolls back the staged
candidate if it changes later.

<a id="mqtt-retained-messages"></a>
## Retained-message requirement

- **Cause:** The broker, bridge, proxy, or hosted-service policy drops, rewrites,
  or replays retained messages without the retained flag.
- **Check:** Publish a harmless retained test value, subscribe after publishing,
  and confirm the exact payload arrives with retained status.
- **Fix:** Enable unmodified retained-message storage/delivery and remove any
  rule that rewrites or prematurely clears the relevant topics.
- **Retry:** Correct the broker/proxy policy and retry. The validator uses a
  unique probe and independently attempts cleanup on every run.

The broker must preserve the MQTT retained flag and retained payloads. The
bridge uses retained discovery, state, and availability so Home Assistant
recovers immediately after a restart; fleet validation also uses retained
setup probes. Proxies or hosted-broker rules must not rewrite or discard
retained messages.

<a id="mqtt-broker-tls"></a>
## Transport security

Fleet onboarding and manual deployment support all three TCP profiles below;
each still requires a username and password:

| Profile | Panel environment | Use when |
|---|---|---|
| Plaintext TCP | `MQTT_TLS_ENABLED=0`; no CA file | A trusted, isolated LAN where unencrypted MQTT is an accepted risk |
| TLS with public/system CA | `MQTT_TLS_ENABLED=1`; no CA file | The broker certificate chains to a CA in the panel's system trust store |
| TLS with custom CA | `MQTT_TLS_ENABLED=1`; `MQTT_TLS_CA_FILE` points to the integration-staged or manually staged public CA | A private CA signs the broker certificate |

TLS is strict: the panel verifies both the certificate chain and the broker
hostname. Use a DNS name present in the certificate, or a certificate valid for
the entered IP address, and keep panel time correct. A TLS failure never falls
back to plaintext or disables verification. The custom CA is public trust
material, never a private key. Fleet onboarding stores it once with the broker
profile and stages an immutable, content-addressed copy on each panel. Repair
and update regenerate the panel environment from that stored fleet profile, so
they preserve TLS rather than silently downgrading it.

Anonymous MQTT, insecure/ignore-certificate TLS, mutual TLS client
certificates, and MQTT over WebSockets are unsupported by the Brilliant panel
transport in this release.

- **Cause:** Broker validation or staged panel preflight cannot verify the
  broker certificate chain/hostname, or the panel cannot use the selected
  trust store. The stable code is `broker_tls_verification_failed`.
- **Check:** Confirm the entered hostname/IP is present in the certificate,
  inspect the full chain and validity period, verify panel time, and confirm the
  selected public/custom CA is correct.
- **Fix:** Correct the endpoint name, server certificate chain, panel time, or
  CA. Never disable verification.
- **Retry:** Correct the broker profile and retry. Initial validation creates no
  fleet; staged panel failure leaves the candidate inactive and rolls it back.

<a id="mqtt-panel-configuration"></a>
## Staged panel configuration

- **Cause:** The generated panel environment is missing a required field, has
  an invalid value, or does not match the transaction-owned TLS assets.
- **Check:** Recheck the broker host, port, username, password, TLS selection,
  and custom CA. The panel error intentionally does not echo those values.
- **Fix:** Correct the fleet broker settings or CA, then let onboarding render
  and stage a new transaction; do not hand-edit the staged environment.
- **Retry:** Select **Retry** in fleet onboarding. The invalid candidate stays
  inactive and is cleaned before another transaction proceeds.

<a id="mqtt-validation"></a>
## Fleet-onboarding validation

- **Cause:** The validator cannot complete Home Assistant readiness, either
  message direction, Discovery write, retained delivery, or cleanup.
- **Check:** Use the reported stage/code and the specific stable error section
  below. Logs and UI placeholders intentionally exclude credentials, CA
  bodies, nonces, and raw exception text.
- **Fix:** Correct the broker endpoint, credential, ACL direction, discovery
  prefix, retained behavior, or cleanup access identified by that check.
- **Retry:** Correct the prerequisite and retry. Initial broker failure creates
  no fleet. Panel-preflight failure cleans or rolls back the staged candidate
  and leaves the durable fleet and active panel state unchanged.

Fleet onboarding calls `BrokerValidator` before creating the durable empty
fleet and checks behavior rather than only opening a socket. It verifies that:

- Home Assistant's MQTT client is loaded, connected, and using the
  `homeassistant` discovery prefix;
- the dedicated Brilliant credential authenticates;
- temporary nonce messages travel panel-client → Home Assistant and Home
  Assistant → panel-client through the same broker;
- the Brilliant principal can write the discovery prefix;
- retained messages arrive unchanged with the retained flag; and
- all temporary subscriptions and retained probes are cleaned up.

Panel provisioning runs the matching stages from its staged, non-active agent
before activation. The validator never creates or modifies a broker account,
ACL, app/add-on, or broker setting.

These software gates do not claim hardware qualification across every panel
firmware, broker, proxy, and network combination. Pilot and soak one panel
before expanding the fleet.

## Validation errors

Each error below has a stable anchor so onboarding can link directly to its
cause, check, fix, and retry instructions.

<a id="mqtt-broker-validation-failed"></a>
<a id="broker_validation_failed"></a>
### `broker_validation_failed`

- **Cause:** The validator reached an unexpected internal failure that could
  not be reduced to a more specific broker, MQTT, ACL, or TLS code.
- **Check:** Confirm Home Assistant's MQTT integration is loaded and connected,
  inspect the Brilliant MQTT and broker logs around the validation attempt, and
  verify the entered endpoint and credential. Keep credentials out of logs and
  support bundles.
- **Fix:** Restore the reported MQTT or broker dependency. If those checks are
  healthy and the code repeats, collect Home Assistant's secret-redacted
  diagnostics and report the integration failure.
- **Retry:** Retry fleet validation after the dependency is healthy. The
  failed attempt created no fleet and changed no broker configuration.

<a id="ha_mqtt_unavailable"></a>
### `ha_mqtt_unavailable`

- **Cause:** Home Assistant's MQTT integration is missing, not loaded, or
  disconnected.
- **Check:** Open **Settings → Devices & services → MQTT** and inspect its
  connection status and broker endpoint.
- **Fix:** Install/reload MQTT, start the broker, and make Home Assistant use
  the same broker intended for the panels.
- **Retry:** Return to Brilliant MQTT onboarding and retry. Initial validation
  has not created a fleet; panel preflight has not activated its candidate.

<a id="unsupported_discovery_prefix"></a>
### `unsupported_discovery_prefix`

- **Cause:** Home Assistant's effective discovery prefix is not
  `homeassistant`.
- **Check:** Open the MQTT integration's discovery options.
- **Fix:** Enable discovery and restore the prefix to `homeassistant`.
- **Retry:** Reload MQTT if requested, then retry Brilliant MQTT onboarding.

<a id="fleet_auth_failed"></a>
### `fleet_auth_failed`

- **Cause:** The broker rejected the dedicated Brilliant fleet credential.
- **Check:** Confirm the fleet username is enabled, verify its password without
  logging it, and inspect the broker authentication log.
- **Fix:** Correct the dedicated non-owner fleet credential without
  substituting Home Assistant's hidden credential.
- **Retry:** Re-enter the corrected password and retry fleet validation.

<a id="panel_to_ha_timeout"></a>
### `panel_to_ha_timeout`

- **Cause:** The temporary Brilliant client published a nonce, but Home
  Assistant did not receive it.
- **Check:** Confirm both clients target the same broker and that Home
  Assistant can read `brilliant/#`.
- **Fix:** Correct the broker endpoint, routing, or Home Assistant principal's
  read ACL.
- **Retry:** Reconnect Home Assistant, then retry the failed validation stage.

<a id="ha_to_panel_timeout"></a>
### `ha_to_panel_timeout`

- **Cause:** Home Assistant published a nonce, but the temporary Brilliant
  client did not receive it.
- **Check:** Confirm Home Assistant can write `brilliant/#` and the Brilliant
  principal can read it.
- **Fix:** Correct those ACL directions and any broker/proxy routing rule.
- **Retry:** Reconnect both principals, then retry the failed validation stage.

<a id="discovery_write_denied"></a>
<a id="discovery_write_timeout"></a>
### `discovery_write_denied` / `discovery_write_timeout`

- **Cause:** The broker denied, silently dropped, or failed to route the
  Brilliant principal's Discovery probe; MQTT 3.1.1 brokers do not always
  report publish denial to the client.
- **Check:** Inspect the ACL matching that principal and
  `homeassistant/#`, plus broker-side authorization logs.
- **Fix:** Grant the Brilliant principal write access to
  `homeassistant/#`; keep the fixed discovery prefix.
- **Retry:** Reload the broker ACL if required, reconnect, and retry the failed
  validation stage.

<a id="retained_message_invalid"></a>
<a id="retained_message_timeout"></a>
### `retained_message_invalid` / `retained_message_timeout`

- **Cause:** A retained probe was missing, changed, or delivered without its
  retained flag.
- **Check:** Verify retained-message support and look for a proxy, bridge,
  rule, or hosted-service policy that rewrites or clears retained traffic.
- **Fix:** Enable unmodified retained messages and ensure both clients use the
  same broker path.
- **Retry:** Clear any test rule/cache involved, then retry the failed
  validation stage.

<a id="mqtt-validation-cleanup"></a>
<a id="cleanup_failed"></a>
### `cleanup_failed`

- **Cause:** Validation finished or failed, but one or more temporary retained
  probes, subscriptions, or client sessions could not be cleaned up.
- **Check:** Inspect connectivity and write ACLs for both temporary setup
  topic directions.
- **Fix:** Restore the broker connection and permissions. Setup IDs are unique,
  but retained probes should still be cleared.
- **Retry:** Restore connectivity and retry. Cleanup is attempted independently
  on every run and a new setup ID prevents reuse of stale evidence.

Back to the [install overview](../../INSTALL.md).

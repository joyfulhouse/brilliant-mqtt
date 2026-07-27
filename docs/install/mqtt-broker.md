# MQTT broker prerequisite

brilliant-mqtt keeps MQTT as its lightweight panel transport. Before adding a
panel, Home Assistant and every panel must be able to reach the **same** broker.
Choose either path below:

1. [Home Assistant Mosquitto](#home-assistant-mosquitto-recommended) is the
   recommended shortcut for Home Assistant OS installations.
2. [An existing broker](#existing-broker) is an equally supported path for any
   compatible local, remote, or hosted service.

The integration does not install or start a broker, create users, edit ACLs, or
change broker configuration. The current one-panel setup flow also does **not**
call the packaged `BrokerValidator`: it can install the agent and create an
entry without proving the MQTT path. The forthcoming Plan 2 fleet-onboarding
flow will use that validator to check the settings you provide and return a
specific error when the end-to-end MQTT path does not work.

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
using the current one-panel setup or the forthcoming fleet-onboarding flow:

1. Configure Home Assistant's MQTT integration to use that broker.
2. Confirm the endpoint is reachable from Home Assistant and from the panels'
   network. A successful connection from Home Assistant alone is insufficient.
3. Confirm the broker supports MQTT 5 for Home Assistant, MQTT 3.1.1 for the
   panel client, retained messages, and wildcard subscriptions.
4. Create a dedicated Brilliant fleet username/password and configure both
   principals in the ACL table below.

<a id="mqtt-broker-profile"></a>
## Broker profile troubleshooting

The forthcoming fleet validator reports an invalid broker profile before it
opens a connection. The current one-panel UI performs only basic host, port,
username, and password form validation.

- **Cause:** A required broker value is missing, the port or TLS flag has the
  wrong type or range, or custom CA content is empty or invalid.
- **Check:** Recheck the hostname, TCP port, username/password, TLS selection,
  and complete public CA certificate. Do not paste a private key.
- **Fix:** Correct the profile and use a custom CA only with TLS enabled.
- **Retry:** Verify the corrected values manually today; retry the validator
  when using the forthcoming fleet-onboarding flow.

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
- **Retry:** Reconnect both Home Assistant and the panel path, then retry the
  manual check or forthcoming fleet validation.

<a id="mqtt-broker-authentication"></a>
## Authentication

- **Cause:** The broker rejects the dedicated Brilliant credential. The current
  one-panel flow may still create an entry because it does not test MQTT
  authentication; Plan 2 reports the stable authentication failure directly.
- **Check:** Confirm the user is enabled, verify the entered username/password,
  and inspect the broker authentication log without exposing credentials.
- **Fix:** Create, correct, or rotate the dedicated non-owner Brilliant
  credential; never substitute Home Assistant's hidden generated credential.
- **Retry:** Verify the credential manually with the current flow. In Plan 2,
  re-enter the corrected password and select **Retry**.

Username/password authentication is required. Anonymous access is unsupported.
Use one dedicated, non-owner Brilliant fleet principal on every panel; do not
reuse Home Assistant's MQTT principal or its hidden generated credential.

If credentials are rotated, update the broker and complete the integration's
guided reconfiguration. Never put broker passwords in git or documentation.

<a id="mqtt-broker-acl"></a>
## Broker ACL

- **Cause:** One principal can connect but a required Brilliant, discovery, or
  cleanup topic direction is denied or silently dropped.
- **Check:** Compare both principals with the table below and inspect broker
  authorization logs while manually testing each publish/subscribe direction.
- **Fix:** Apply the minimum Brilliant-related grants below while preserving
  Home Assistant's existing birth, will, status, and reserved-principal access.
- **Retry:** Reconnect both clients and verify both directions manually today.
  Plan 2 will rerun discovery, retained-message, and cleanup probes on
  **Retry**.

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
temporary discovery probe when the forthcoming fleet flow uses it; it does not
grant general discovery publishing.

Mosquitto can silently drop an unauthorized publish. A timeout may therefore
mean an ACL or routing error even when login succeeded.

The packaged panel validator treats `timeout_seconds` as a per-stage result
deadline. If the operating system has already started a connection worker that
cannot be cancelled, the validator waits for that worker and MQTT cleanup to
settle before it exits. The forthcoming fleet coordinator therefore also owns
an outer SSH-process deadline and terminates a still-running validator before
onboarding returns; this avoids racing cleanup against a late connection.

<a id="mqtt-discovery-prefix"></a>
## MQTT Discovery prefix

- **Cause:** Home Assistant listens below a discovery prefix other than the
  agent's fixed `homeassistant` prefix. The current flow does not detect this;
  Plan 2 returns `unsupported_discovery_prefix`.
- **Check:** Inspect Home Assistant's effective MQTT Discovery setting and
  confirm discovery is enabled with prefix `homeassistant`.
- **Fix:** Restore the exact `homeassistant` prefix and reload MQTT if Home
  Assistant requests it.
- **Retry:** Verify current entities rediscover after the reload. In Plan 2,
  return to onboarding and select **Retry**.

This release publishes discovery only below the fixed `homeassistant` prefix.
In Home Assistant's MQTT integration, keep discovery enabled and its Discovery
Prefix set to `homeassistant`. The forthcoming fleet flow will stop with
`unsupported_discovery_prefix` when that differs; the current one-panel flow
does not check the effective prefix.

<a id="mqtt-retained-messages"></a>
## Retained-message requirement

- **Cause:** The broker, bridge, proxy, or hosted-service policy drops, rewrites,
  or replays retained messages without the retained flag.
- **Check:** Publish a harmless retained test value, subscribe after publishing,
  and confirm the exact payload arrives with retained status.
- **Fix:** Enable unmodified retained-message storage/delivery and remove any
  rule that rewrites or prematurely clears the relevant topics.
- **Retry:** Reconnect the current agent and confirm retained state manually. In
  Plan 2, clear any manual probe and select **Retry**.

The broker must preserve the MQTT retained flag and retained payloads. The
bridge uses retained discovery, state, and availability so Home Assistant
recovers immediately after a restart; the forthcoming fleet validator also
uses retained setup probes. Proxies or hosted-broker rules must not rewrite or
discard retained messages.

<a id="mqtt-broker-tls"></a>
## Transport security

The panel agent supports all three TCP profiles below for manual deployment;
each still requires a username and password:

| Profile | Panel environment | Use when |
|---|---|---|
| Plaintext TCP | `MQTT_TLS_ENABLED=0`; no CA file | A trusted, isolated LAN where unencrypted MQTT is an accepted risk |
| TLS with public/system CA | `MQTT_TLS_ENABLED=1`; no CA file | The broker certificate chains to a CA in the panel's system trust store |
| TLS with custom CA | `MQTT_TLS_ENABLED=1`; `MQTT_TLS_CA_FILE` points to a manually staged public CA today or a future integration-staged CA | A private CA signs the broker certificate |

TLS is strict: the panel verifies both the certificate chain and the broker
hostname. Use a DNS name present in the certificate, or a certificate valid for
the entered IP address, and keep panel time correct. A TLS failure never falls
back to plaintext or disables verification. The custom CA is public trust
material. The packaged integration contains a content-addressed CA staging seam
for the forthcoming fleet flow, but the current one-panel UI does not expose
TLS or a custom-CA field.

> **Current integration limitation:** adoption does not retain manual
> `MQTT_TLS_ENABLED`/`MQTT_TLS_CA_FILE` values, and current reconfigure,
> repair, and update paths still render a plaintext destination. Before any
> write, they inspect both the live and OTA-staged panel environments and abort
> with `mqtt_tls_downgrade_refused` if either enables TLS or has an ambiguous
> TLS value; no panel file is changed. Keep managing the TLS environment
> manually until Plan 2 fleet onboarding wires the packaged staging seam
> through adoption and lifecycle operations.

Anonymous MQTT, insecure/ignore-certificate TLS, mutual TLS client
certificates, and MQTT over WebSockets are unsupported by the Brilliant panel
transport in this release.

- **Cause:** The future validator cannot verify the broker certificate chain or
  hostname, or the panel cannot use the selected trust store.
- **Check:** Confirm the entered hostname/IP is present in the certificate,
  inspect the full chain and validity period, verify panel time, and confirm the
  selected public/custom CA is correct.
- **Fix:** Correct the endpoint name, server certificate chain, panel time, or
  CA. Never disable verification.
- **Retry:** Verify TLS manually today; retry the validator when using the
  forthcoming fleet-onboarding flow.

<a id="mqtt-validation"></a>
## Forthcoming fleet-onboarding validation

- **Cause:** The packaged validator cannot complete Home Assistant readiness,
  either message direction, discovery write, retained delivery, or cleanup.
  The current one-panel flow does not invoke this validator.
- **Check:** Perform those checks manually today and use the specific stable
  error section below when Plan 2 reports a code.
- **Fix:** Correct the broker endpoint, credential, ACL direction, discovery
  prefix, retained behavior, or cleanup access identified by that check.
- **Retry:** Re-test manually with the current flow. In Plan 2, correct the
  prerequisite and select **Retry**; validation never modifies broker state.

The packaged `BrokerValidator` is not called by the current one-panel setup
flow. Plan 2 fleet onboarding will call it before creating a fleet entry and
will check behavior rather than only opening a socket. It will verify that:

- Home Assistant's MQTT client is loaded, connected, and using the
  `homeassistant` discovery prefix;
- the dedicated Brilliant credential authenticates;
- temporary nonce messages travel panel-client → Home Assistant and Home
  Assistant → panel-client through the same broker;
- the Brilliant principal can write the discovery prefix;
- retained messages arrive unchanged with the retained flag; and
- all temporary subscriptions and retained probes are cleaned up.

The validator never creates or modifies a broker account, ACL, app/add-on, or
broker setting. Until the fleet flow is wired, perform these checks manually;
the stable error contract below documents the future **Retry** path.

## Validation errors

Each error below has a stable anchor so the forthcoming fleet flow can link
directly to the matching cause, check, fix, and retry instructions. The checks
and fixes are also useful for diagnosing the current one-panel flow manually.

<a id="ha_mqtt_unavailable"></a>
### `ha_mqtt_unavailable`

- **Cause:** Home Assistant's MQTT integration is missing, not loaded, or
  disconnected.
- **Check:** Open **Settings → Devices & services → MQTT** and inspect its
  connection status and broker endpoint.
- **Fix:** Install/reload MQTT, start the broker, and make Home Assistant use
  the same broker intended for the panels.
- **Retry:** In the forthcoming fleet flow, return to Brilliant MQTT
  onboarding and select **Retry**.

<a id="unsupported_discovery_prefix"></a>
### `unsupported_discovery_prefix`

- **Cause:** Home Assistant's effective discovery prefix is not
  `homeassistant`.
- **Check:** Open the MQTT integration's discovery options.
- **Fix:** Enable discovery and restore the prefix to `homeassistant`.
- **Retry:** Reload MQTT if requested, then retry forthcoming fleet onboarding.

<a id="fleet_auth_failed"></a>
### `fleet_auth_failed`

- **Cause:** Opening the temporary Brilliant client failed in a way that did
  not match the stable TLS, authentication, rejection, availability,
  connection, or timeout classifications.
- **Check:** Inspect the adjacent stable broker guidance and the broker log;
  diagnostics intentionally omit raw exception text and secrets.
- **Fix:** Correct the endpoint, transport, or broker policy identified there
  without substituting Home Assistant's hidden credential.
- **Retry:** Retry forthcoming fleet validation after correcting the
  prerequisite. Known failures surface their more specific stable code.

<a id="panel_to_ha_timeout"></a>
### `panel_to_ha_timeout`

- **Cause:** The temporary Brilliant client published a nonce, but Home
  Assistant did not receive it.
- **Check:** Confirm both clients target the same broker and that Home
  Assistant can read `brilliant/#`.
- **Fix:** Correct the broker endpoint, routing, or Home Assistant principal's
  read ACL.
- **Retry:** Retry forthcoming fleet validation after Home Assistant has
  reconnected.

<a id="ha_to_panel_timeout"></a>
### `ha_to_panel_timeout`

- **Cause:** Home Assistant published a nonce, but the temporary Brilliant
  client did not receive it.
- **Check:** Confirm Home Assistant can write `brilliant/#` and the Brilliant
  principal can read it.
- **Fix:** Correct those ACL directions and any broker/proxy routing rule.
- **Retry:** Retry forthcoming fleet validation after both clients reconnect.

<a id="discovery_write_denied"></a>
### `discovery_write_denied`

- **Cause:** The broker denied or silently dropped the Brilliant principal's
  discovery probe; MQTT 3.1.1 brokers do not always report publish denial to
  the client.
- **Check:** Inspect the ACL matching that principal and
  `homeassistant/#`, plus broker-side authorization logs.
- **Fix:** Grant the Brilliant principal write access to
  `homeassistant/#`; keep the fixed discovery prefix.
- **Retry:** Reload the broker ACL if required, reconnect, and retry forthcoming
  fleet validation.

<a id="retained_message_invalid"></a>
### `retained_message_invalid`

- **Cause:** A retained probe was missing, changed, or delivered without its
  retained flag.
- **Check:** Verify retained-message support and look for a proxy, bridge,
  rule, or hosted-service policy that rewrites or clears retained traffic.
- **Fix:** Enable unmodified retained messages and ensure both clients use the
  same broker path.
- **Retry:** Clear any test rule/cache involved, then retry forthcoming fleet
  validation.

<a id="mqtt-validation-cleanup"></a>
<a id="cleanup_failed"></a>
### `cleanup_failed`

- **Cause:** Validation finished or failed, but one or more temporary retained
  probes, subscriptions, or client sessions could not be cleaned up.
- **Check:** Inspect connectivity and write ACLs for both temporary setup
  topic directions.
- **Fix:** Restore the broker connection and permissions. Setup IDs are unique,
  but retained probes should still be cleared.
- **Retry:** Retry forthcoming fleet validation; cleanup is attempted
  independently on every run.

Back to the [install overview](../../INSTALL.md).

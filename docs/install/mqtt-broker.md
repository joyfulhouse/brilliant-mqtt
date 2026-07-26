# MQTT broker prerequisite

brilliant-mqtt keeps MQTT as its lightweight panel transport. Before adding a
panel, Home Assistant and every panel must be able to reach the **same** broker.
Choose either path below:

1. [Home Assistant Mosquitto](#home-assistant-mosquitto-recommended) is the
   recommended shortcut for Home Assistant OS installations.
2. [An existing broker](#existing-broker) is an equally supported path for any
   compatible local, remote, or hosted service.

The integration does not install or start a broker, create users, edit ACLs, or
change broker configuration. Onboarding validates the settings you provide and
gives a specific error if the end-to-end MQTT path does not work.

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
onboarding:

1. Configure Home Assistant's MQTT integration to use that broker.
2. Confirm the endpoint is reachable from Home Assistant and from the panels'
   network. A successful connection from Home Assistant alone is insufficient.
3. Confirm the broker supports MQTT 5 for Home Assistant, MQTT 3.1.1 for the
   panel client, retained messages, and wildcard subscriptions.
4. Create a dedicated Brilliant fleet username/password and configure both
   principals in the ACL table below.

<a id="mqtt-broker-authentication"></a>
## Authentication

Username/password authentication is required. Anonymous access is unsupported.
Use one dedicated, non-owner Brilliant fleet principal on every panel; do not
reuse Home Assistant's MQTT principal or its hidden generated credential.

If credentials are rotated, update the broker and complete the integration's
guided reconfiguration. Never put broker passwords in git or documentation.

<a id="mqtt-broker-acl"></a>
## Broker ACL

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
rule lets Home Assistant independently clear the validator's temporary
discovery probe if the Brilliant client disconnects before cleanup; it does not
grant general discovery publishing.

Mosquitto can silently drop an unauthorized publish. A timeout may therefore
mean an ACL or routing error even when login succeeded.

<a id="mqtt-discovery-prefix"></a>
## MQTT Discovery prefix

This release publishes discovery only below the fixed `homeassistant` prefix.
In Home Assistant's MQTT integration, keep discovery enabled and its Discovery
Prefix set to `homeassistant`. A different prefix stops onboarding with
`unsupported_discovery_prefix`; it is not copied into the panel configuration.

<a id="mqtt-retained-messages"></a>
## Retained-message requirement

The broker must preserve the MQTT retained flag and retained payloads. The
bridge uses retained discovery, state, availability, and setup probes so
Home Assistant recovers immediately after a restart. Proxies or hosted-broker
rules must not rewrite or discard retained messages.

## Transport security

All three supported TCP profiles still require a username and password:

| Profile | Panel environment | Use when |
|---|---|---|
| Plaintext TCP | `MQTT_TLS_ENABLED=0`; no CA file | A trusted, isolated LAN where unencrypted MQTT is an accepted risk |
| TLS with public/system CA | `MQTT_TLS_ENABLED=1`; no CA file | The broker certificate chains to a CA in the panel's system trust store |
| TLS with custom CA | `MQTT_TLS_ENABLED=1`; `MQTT_TLS_CA_FILE` points to the integration-staged public CA | A private CA signs the broker certificate |

TLS is strict: the panel verifies both the certificate chain and the broker
hostname. Use a DNS name present in the certificate, or a certificate valid for
the entered IP address, and keep panel time correct. A TLS failure never falls
back to plaintext or disables verification. The custom CA is public trust
material; the integration stages its exact bytes in a content-addressed file
under `/var/brilliant-mqtt/tls/` and stores only that path in the protected
environment file.

Anonymous MQTT, insecure/ignore-certificate TLS, mutual TLS client
certificates, and MQTT over WebSockets are unsupported by the Brilliant panel
transport in this release.

<a id="mqtt-validation"></a>
## Onboarding validation

Onboarding checks behavior rather than only opening a socket. It verifies that:

- Home Assistant's MQTT client is loaded, connected, and using the
  `homeassistant` discovery prefix;
- the dedicated Brilliant credential authenticates;
- temporary nonce messages travel panel-client → Home Assistant and Home
  Assistant → panel-client through the same broker;
- the Brilliant principal can write the discovery prefix;
- retained messages arrive unchanged with the retained flag; and
- all temporary subscriptions and retained probes are cleaned up.

Validation never creates or modifies a broker account, ACL, app/add-on, or
broker setting. Fix the reported prerequisite and use **Retry**.

## Validation errors

Each error below has a stable anchor so the integration can link directly to
the matching cause, check, fix, and retry instructions.

<a id="ha_mqtt_unavailable"></a>
### `ha_mqtt_unavailable`

- **Cause:** Home Assistant's MQTT integration is missing, not loaded, or
  disconnected.
- **Check:** Open **Settings → Devices & services → MQTT** and inspect its
  connection status and broker endpoint.
- **Fix:** Install/reload MQTT, start the broker, and make Home Assistant use
  the same broker intended for the panels.
- **Retry:** Return to Brilliant MQTT onboarding and select **Retry**.

<a id="unsupported_discovery_prefix"></a>
### `unsupported_discovery_prefix`

- **Cause:** Home Assistant's effective discovery prefix is not
  `homeassistant`.
- **Check:** Open the MQTT integration's discovery options.
- **Fix:** Enable discovery and restore the prefix to `homeassistant`.
- **Retry:** Reload MQTT if requested, then retry onboarding.

<a id="fleet_auth_failed"></a>
### `fleet_auth_failed`

- **Cause:** The broker rejected the dedicated Brilliant username/password.
- **Check:** Confirm the user exists, is enabled, and the entered password is
  current; inspect the broker authentication log.
- **Fix:** Correct or rotate the dedicated credential without substituting
  Home Assistant's hidden credential.
- **Retry:** Enter the corrected password and retry validation.

<a id="panel_to_ha_timeout"></a>
### `panel_to_ha_timeout`

- **Cause:** The temporary Brilliant client published a nonce, but Home
  Assistant did not receive it.
- **Check:** Confirm both clients target the same broker and that Home
  Assistant can read `brilliant/#`.
- **Fix:** Correct the broker endpoint, routing, or Home Assistant principal's
  read ACL.
- **Retry:** Retry after Home Assistant has reconnected.

<a id="ha_to_panel_timeout"></a>
### `ha_to_panel_timeout`

- **Cause:** Home Assistant published a nonce, but the temporary Brilliant
  client did not receive it.
- **Check:** Confirm Home Assistant can write `brilliant/#` and the Brilliant
  principal can read it.
- **Fix:** Correct those ACL directions and any broker/proxy routing rule.
- **Retry:** Retry validation after both clients reconnect.

<a id="discovery_write_denied"></a>
### `discovery_write_denied`

- **Cause:** The broker denied or silently dropped the Brilliant principal's
  discovery probe; MQTT 3.1.1 brokers do not always report publish denial to
  the client.
- **Check:** Inspect the ACL matching that principal and
  `homeassistant/#`, plus broker-side authorization logs.
- **Fix:** Grant the Brilliant principal write access to
  `homeassistant/#`; keep the fixed discovery prefix.
- **Retry:** Reload the broker ACL if required, reconnect, and retry.

<a id="retained_message_invalid"></a>
### `retained_message_invalid`

- **Cause:** A retained probe was missing, changed, or delivered without its
  retained flag.
- **Check:** Verify retained-message support and look for a proxy, bridge,
  rule, or hosted-service policy that rewrites or clears retained traffic.
- **Fix:** Enable unmodified retained messages and ensure both clients use the
  same broker path.
- **Retry:** Clear any test rule/cache involved, then retry validation.

<a id="mqtt-validation-cleanup"></a>
<a id="cleanup_failed"></a>
### `cleanup_failed`

- **Cause:** Validation finished or failed, but one or more temporary retained
  probes, subscriptions, or client sessions could not be cleaned up.
- **Check:** Inspect connectivity and write ACLs for both temporary setup
  topic directions.
- **Fix:** Restore the broker connection and permissions. Setup IDs are unique,
  but retained probes should still be cleared.
- **Retry:** Retry validation; cleanup is attempted independently on every run.

Back to the [install overview](../../INSTALL.md).

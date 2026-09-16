# Broadcast, ARP, and established MQTT qualification

Refs #147. Use this runbook to qualify missing inbound broadcast/multicast or
missing fresh ARP replies while a panel's established MQTT session still works.
This is an operator procedure, not a record of a recovered network. Documentation
delivery or merge proves neither infrastructure recovery nor runtime qualification.
No production probes or interventions were performed for this workstream.

Follow the [validation runbook](../validation-runbook.md) preflight and hard
stops. For response attribution beyond this branch, use
[Wi-Fi response qualification](wifi-response-qualification.md). All durations,
packet limits, and repetition counts below are chosen procedure budgets, not
measured panel performance or universal acceptance thresholds. Runtime constants
are separately cited. Record actual elapsed time and incomplete coverage.

## Scope and hard stops

- Name one suspect panel and a **working comparison device on the same Layer-2
  segment** in private inventory. Prefer a healthy panel with comparable AP,
  association, client-isolation, VLAN, and multicast policy. A routed observer or
  a result from one device alone cannot establish selective delivery failure.
- Use existing approved operator access and installed diagnostic tools. Do not
  install packages on a panel, restart it, flush neighbor tables, change MQTT
  sessions, or change network settings to manufacture a baseline.
- Confirm physical recovery access, working native controls, and no concurrent
  firmware update, mesh DFU, calibration, or other experiment. Stop collection
  immediately on UI lag, bus/UI restart, unexpected load behavior, lost control,
  or evidence collection affecting responsiveness.
- Keep packet captures, raw logs, and inventory private, with restricted access.
  Publish only role labels, timestamps, counts, durations, and verdicts. Never
  publish addresses, network names, hardware identities, topics containing panel
  identifiers, bridge credentials, or discovery payloads.
- Record `FAIL` for an observed fault and `INCONCLUSIVE` for missing prerequisites,
  ambiguous evidence, or an interrupted observation. Neither permits recovery
  changes under this diagnostic procedure.

**Before any RF-affecting action, account for the existing Wi-Fi watchdog.** Its
recovery ladder can reconnect Wi-Fi, restart `connman`/`wpa_supplicant`, or
**REBOOT the panel** ([docs/CONFIGURATION.md:132](../../CONFIGURATION.md#L132)).
An RF experiment can trip that ladder, invalidate the measurement, and power-cycle
a production in-wall panel. First read its enabled/running state, effective
thresholds, pending recovery and reboot-guard state; record the operator's plan
for that risk and physical recovery. If these cannot be established, stop before
intervention. Do not enable, disable, restart, or invoke the watchdog to collect
evidence; any change to its operation needs separate approval and restoration.

## Keep the evidence paths separate

| Path | What the evidence establishes | What it cannot establish |
|---|---|---|
| Already-established unicast MQTT connection | Retained `online` follows a successful local-bus read over an established MQTT connection ([src/brilliant_mqtt/bridge.py:271](../../../src/brilliant_mqtt/bridge.py#L271)); the retained QoS-0 LWT is `offline` ([src/brilliant_mqtt/mqttio.py:457](../../../src/brilliant_mqtt/mqttio.py#L457)). These are connection/birth semantics, not a fresh probe. | A retained value alone cannot date the last successful exchange. With contemporaneous broker keepalive/session evidence, usability is established as of that last exchange only. It proves nothing about inbound broadcast/multicast or fresh neighbor resolution. |
| Neighbor resolution (ARP) | A fresh ordinary ARP request and attributable reply test address-to-link-layer resolution on this segment, in this direction, during this window. | An existing TCP session can keep using cached neighbor information. Its health cannot substitute for a fresh ARP exchange. |
| Discovery and broadcast/multicast receipt | An expected frame observed at a receiver establishes delivery of that frame. Discovery application processing requires separate evidence. | HA MQTT discovery is retained MQTT over the broker TCP session, not a broadcast test ([src/brilliant_mqtt/bridge.py:252](../../../src/brilliant_mqtt/bridge.py#L252)). The scoped bridge/integration review found no mDNS, SSDP, or ARP implementation. Native Hue discovery uses mDNS, but the documented credential-injection path bypasses discovery ([docs/brilliant-panel/diyhue-bridge.md:46](../diyhue-bridge.md#L46)). |

The panel agent's staged dependency declares a 60 s default as `keepalive: int = 60`
([custom_components/brilliant_mqtt/agent_payload/vendor/aiomqtt/client.py:226](../../../custom_components/brilliant_mqtt/agent_payload/vendor/aiomqtt/client.py#L226)).
The adapter supplies no override
([src/brilliant_mqtt/mqttio.py:461](../../../src/brilliant_mqtt/mqttio.py#L461)).
This is an **inherited vendored-library default**, not a configured or tuned
deployment setting; confirm the effective value against the running session's
existing telemetry. An unavailable session value remains unverified.

**Healthy MQTT does not imply healthy multicast.** Failed ICMP to a broker VIP
does not prove failed MQTT: a VIP commonly does not answer ICMP while its TCP
service works. Do not use an ICMP result as the MQTT gate, or successful
credential-injected Hue control as proof of discovery delivery.

## Phase 1: eliminate agent-side explanations first

This is a **read-only first gate over shipped surfaces**, before any new procedure.
Use the existing HA-side `wifi_link` and `wifi_power_save` probes
([custom_components/brilliant_mqtt/panel_ops.py:1914](../../../custom_components/brilliant_mqtt/panel_ops.py#L1914),
[custom_components/brilliant_mqtt/panel_ops.py:1915](../../../custom_components/brilliant_mqtt/panel_ops.py#L1915))
through independently approved read-only access. Their parsed summaries expose
link state/signal and `power_save`
([custom_components/brilliant_mqtt/panel_ops.py:2030](../../../custom_components/brilliant_mqtt/panel_ops.py#L2030),
[custom_components/brilliant_mqtt/panel_ops.py:2046](../../../custom_components/brilliant_mqtt/panel_ops.py#L2046)).
Require a timestamped runtime read from the current boot: a stale saved summary
does not establish current power-save state. **Do not invoke HA's Reboot action
to obtain diagnostics**: that action captures them and then reboots
([custom_components/brilliant_mqtt/manager.py:1536](../../../custom_components/brilliant_mqtt/manager.py#L1536)).
If no independent read-only route is available, record `INCONCLUSIVE` and stop.

`power_save=on` is an **agent-side FAIL** with a documented cause: the service
unit identifies power-save dropping inbound packets as a cause of MQTT keepalive
flaps ([deploy/brilliant-mqtt.service:13](../../../deploy/brilliant-mqtt.service#L13)).
Its disable command is strictly best-effort, including the `-` prefix
([deploy/brilliant-mqtt.service:17](../../../deploy/brilliant-mqtt.service#L17));
runtime state **must be read, never assumed** from an active service or unit text.
Route remediation separately; do not alter power-save during this baseline.

Read existing watchdog/agent telemetry for a 20-minute lookback ending at the
incident, then observe for 5 minutes without issuing test commands. Use the
watchdog's existing log and persistent reboot-guard record
([docs/CONFIGURATION.md:153](../../CONFIGURATION.md#L153)), not new ICMP or TCP
probes. Its broker TCP-open result is already logged as
`gateway=%s up=%s broker=%s`
([src/brilliant_wifi_watchdog/run.py:131](../../../src/brilliant_wifi_watchdog/run.py#L131));
it is informational and does not drive recovery
([docs/CONFIGURATION.md:156](../../CONFIGURATION.md#L156)). `INCONCLUSIVE` probe
results are not proof that the broker is down
([src/brilliant_wifi_watchdog/probe.py:213](../../../src/brilliant_wifi_watchdog/probe.py#L213)).
Record any recovery actions; an action during collection invalidates causal
comparison. Do not start an absent watchdog merely to obtain its log.

Record deployed version, effective settings, available log levels, gaps, restarts,
and the symptom window before continuing with the existing agent checks below.
Read only the named non-secret settings through the approved operator interface;
do not dump an environment file. Defaults are not proof of deployed values.

| Signal | Exact setting or log format and source | Interpretation |
|---|---|---|
| Hot diff-poll | `HOT_POLL_SECONDS` / `hot_poll_seconds`, default 2 s ([src/brilliant_mqtt/config.py:55](../../../src/brilliant_mqtt/config.py#L55), [src/brilliant_mqtt/config.py:189](../../../src/brilliant_mqtt/config.py#L189)). `hot poll bus read timed out; backing off for %.0fs before retrying once` ([src/brilliant_mqtt/__main__.py:390](../../../src/brilliant_mqtt/__main__.py#L390)). | Check enabled state and read-timeout evidence first. A polling interval concerns observation, not command or physical actuation latency. A successful mirror read is not independent proof of a healthy notification stream ([src/brilliant_mqtt/__main__.py:340](../../../src/brilliant_mqtt/__main__.py#L340)). |
| Stale-stream rebuild | `BUS_STALE_SECONDS` / `bus_stale_seconds`, default 900 s ([src/brilliant_mqtt/config.py:58](../../../src/brilliant_mqtt/config.py#L58), [src/brilliant_mqtt/config.py:190](../../../src/brilliant_mqtt/config.py#L190)). Exception format: `no bus push for {age:.0f}s (threshold {settings.bus_stale_seconds:.0f}s)` ([src/brilliant_mqtt/__main__.py:347](../../../src/brilliant_mqtt/__main__.py#L347)). | A stale stream can freeze both pushes and mirror reads; examine the rebuild timeline before diagnosing LAN delivery. A quiet window without expected pushes is not proof of failure or recovery. |
| Bus reconnect storm | `RECONNECT_STORM_THRESHOLD` / `reconnect_storm_threshold`, default 20; `RECONNECT_STORM_WINDOW_SECONDS` / `reconnect_storm_window_seconds`, default 60 s ([src/brilliant_mqtt/config.py:193](../../../src/brilliant_mqtt/config.py#L193)). Exception format: `bus reconnected >={settings.reconnect_storm_threshold} times in {settings.reconnect_storm_window_seconds:.0f}s — rebuilding session` ([src/brilliant_mqtt/__main__.py:356](../../../src/brilliant_mqtt/__main__.py#L356)). | Inspect the internal `recent_reconnects` window and timestamped bus reconnect events, not MQTT reconnect counts. A storm resets the push clock and can hide behind apparent freshness ([src/brilliant_mqtt/__main__.py:115](../../../src/brilliant_mqtt/__main__.py#L115)). |
| Native peer-RPC write queue | `set_variables(%s) acquired device lock after %.3fs queue wait` and `set_variables(%s) receipt: %s (rpc %.3fs, queue wait %.3fs)` ([src/brilliant_mqtt/bus.py:1047](../../../src/brilliant_mqtt/bus.py#L1047), [src/brilliant_mqtt/bus.py:1084](../../../src/brilliant_mqtt/bus.py#L1084)). | Writes serialize **per owning bus device**; reads remain independent. Queue wait and RPC latency are separate. Queueing is not RF latency. These normal timing logs are DEBUG; unavailable logs make timing unmeasured, not zero. |

The quoted strings are source format strings; placeholders acquire runtime
values. Match those values against the effective settings. The enclosing session
failure log is `bridge session failed; will reconnect after backoff`
([src/brilliant_mqtt/__main__.py:502](../../../src/brilliant_mqtt/__main__.py#L502));
its exception identifies the cause, so that line alone does not identify a network
fault. Count `bus processor reconnected; re-subscribing and re-reconciling`
separately ([src/brilliant_mqtt/bus.py:874](../../../src/brilliant_mqtt/bus.py#L874)).

The issue #72 write contract matters when interpreting an apparent timeout:
the 5 s caller deadline begins **after** the device lock is acquired and detaches
the write; the RPC keeps running with its lock held and its late outcome is
handled and logged. It is not a lost write. The 15 s hard cap latches a rebuild,
which the supervisor consumes on its next tick
([src/brilliant_mqtt/bus.py:44](../../../src/brilliant_mqtt/bus.py#L44),
[src/brilliant_mqtt/bus.py:979](../../../src/brilliant_mqtt/bus.py#L979),
[src/brilliant_mqtt/__main__.py:323](../../../src/brilliant_mqtt/__main__.py#L323)).
Do not retry a load command just because its caller detached.

The fixed **80 s mesh confirmation wait is expected policy, not a fault**. It
outlasts the measured false-ack/revert tail, runs in a background task off the
command lane, does not delay command dispatch, and is superseded by a newer
command ([src/brilliant_mqtt/bridge.py:49](../../../src/brilliant_mqtt/bridge.py#L49),
[src/brilliant_mqtt/bridge.py:699](../../../src/brilliant_mqtt/bridge.py#L699),
[src/brilliant_mqtt/bridge.py:758](../../../src/brilliant_mqtt/bridge.py#L758)).
Keep physical actuation, optimistic ordinary MQTT reflection, and fresh-observation
mesh confirmation distinct; an RPC receipt proves transport acceptance only
([src/brilliant_mqtt/bridge.py:687](../../../src/brilliant_mqtt/bridge.py#L687),
[src/brilliant_mqtt/bridge.py:888](../../../src/brilliant_mqtt/bridge.py#L888)).

v0.10.2 / #146 hardened **HA-side watchdog deployment** with a single archive,
one retry after 15 s, and preservation of restart debt. It is not resident RF
detection and its successful deployment does not qualify Wi-Fi health
([CHANGELOG.md:10](../../../CHANGELOG.md#L10),
[custom_components/brilliant_mqtt/manager.py:262](../../../custom_components/brilliant_mqtt/manager.py#L262),
[custom_components/brilliant_mqtt/manager.py:1028](../../../custom_components/brilliant_mqtt/manager.py#L1028)).

| Verdict | Agent gate |
|---|---|
| PASS | Runtime power-save is off, watchdog state/risk is accounted for, the symptom window is covered, and neither a stream fault nor queue/RPC timing explains the symptom. This only permits the network investigation. |
| FAIL | Runtime `power_save=on`, or a correlated stale-stream, reconnect storm, read fault, or measured queue/RPC delay supplies an agent/native-path explanation. Stop network attribution and route that evidence to its owner. |
| INCONCLUSIVE | Runtime power-save, watchdog state, logs, timing, effective settings, or incident coverage are missing, or a watchdog action interrupted collection. Preserve the gap; do not claim the agent is eliminated. |

## Phase 2: qualify the established session

During a 5-minute window, use existing broker/session telemetry and normal traffic
to timestamp successful exchanges for the suspect and comparison device. Record
session continuity, most recent keepalive or application exchange, and reconnect
events. Do not reconnect the panel or send a load command to prove connectivity.
Continue this passive session observation through the following bounded captures.

| Verdict | Established-session gate |
|---|---|
| PASS | Both devices have contemporaneous successful exchanges in their established sessions, including during the subsequent delivery/ARP test. |
| FAIL | The suspect's established session demonstrably stops exchanging traffic or reconnects during the test; the working-session premise is false for that window. Investigate unicast/session health separately. |
| INCONCLUSIVE | Only retained `online`, an ICMP result, an uncorrelated TCP connection, or incomplete broker evidence is available. Do not call the session currently healthy. |

## Phase 3: bounded inbound broadcast/multicast capture

Exhaust Phase 1's read-only STA/link, `power_save`, and watchdog/agent evidence and pass Phases 1-2 before this more invasive on-panel capture.

1. Predeclare the broadcast class and multicast group/protocol to observe, the
   expected sender, and why **both** receivers should receive it. Verify relevant
   group membership, AP policy, and direction. Seeing outbound discovery requests
   says nothing about inbound delivery. If the devices should receive different
   groups or policies differ, stop with `INCONCLUSIVE` for selective delivery.
2. Capture concurrently at the suspect's receiving interface and the working
   comparison receiver, with a sender/AP trace to establish emission if available.
   A switch uplink capture alone cannot prove receipt at a wireless client. Use
   existing ordinary discovery traffic; do not create a discovery flood or pair a
   new device. If no eligible frame occurs, the result is `INCONCLUSIVE`.
3. In each already-authorized operator session, use the template below only if
   the installed tools support these options. Substitute the interface and a new
   private output path from private inventory. Never overwrite earlier evidence.
   Set `CAPTURE_FILTER` to the predeclared class's protocol/group and expected
   sender where appropriate; verify it matches eligible traffic at the control.
   Use a separate deciding capture per class. A broad all-broadcast/multicast
   filter is suitable only for an optional orientation pass, never the deciding capture.

```sh
: "${CAPTURE_IFACE:?set the approved receiver interface privately}"
: "${PRIVATE_CAPTURE:?set a new private capture path}"
: "${CAPTURE_FILTER:?set the narrow predeclared-class filter privately}"
umask 077
timeout --signal=INT --kill-after=5s 60s \
  tcpdump -i "$CAPTURE_IFACE" -Q in -nn -e -s 128 -c 2000 \
  -w "$PRIVATE_CAPTURE" "$CAPTURE_FILTER"
```

This is a 60 s capture budget with a 5 s shutdown allowance, a 2,000-packet cap,
and a 128-byte snapshot limit. All are collection limits, not measured traffic
rates. Reaching the frame cap without observing the predeclared class is
**INCONCLUSIVE, never PASS or FAIL**. For a new capture, narrow the filter further,
remove `-c 2000` to use the time bound alone, or record and justify a different cap;
retain the time and snapshot limits. Do not infer absence from a truncated window.

An `ip multicast` filter covers AP multicast-to-unicast conversion only while the
IPv4 destination remains in `224/4`; `ip6 multicast` likewise requires an IPv6
multicast destination. An `ether multicast` term does **not** match frames whose
L2 destination has been rewritten to unicast.
Check capture start/end, interface/direction support, packet-drop statistics,
and whether the count cap ended collection early. Timeout termination is an
expected bound, not evidence of a network failure. Unsupported tools, forced
termination with uncertain capture integrity, or drops make absent-frame evidence
`INCONCLUSIVE`. Do not enable monitor mode on a serving panel or install tools.

Match eligible frames privately by sender, protocol, timing, and packet identity
where available. Truncated or encrypted data may prevent correlation; do not infer
application receipt from an unreadable payload. Report **separate verdicts for
broadcast and each observed multicast class**, not a universal multicast verdict.

| Verdict | Inbound-delivery gate, per class |
|---|---|
| PASS | Correlated expected frames reach both receivers during valid, overlapping captures. This proves delivery only for the observed class/window; it does not prove discovery application processing or ARP response. |
| FAIL | Eligible frames reach the working comparison receiver but are absent at the suspect across the complete valid window, with equivalent delivery eligibility and emission established. Missing inbound delivery is evidenced; the faulty infrastructure component is not yet identified. |
| INCONCLUSIVE | No eligible traffic, no working control, different membership/policy, unknown capture direction, only an upstream trace, drops, premature cap, or ambiguous frame matching. A silent capture alone proves nothing. |

## Phase 4: ordinary fresh ARP requests

Use an approved diagnostic station on the **same segment**, and ordinary iputils
`arping` already installed there. Verify that the target and comparison addresses
are on-link and that their expected response identities are known privately.
Do not use duplicate-address detection, gratuitous/unsolicited modes, spoofed
sources, cache flushes, static neighbor entries, or MQTT disconnections.

Take a new bounded 60 s capture at both endpoints using the Phase 3 template,
but with `-Q inout` and filter `'arp'`, so request arrival and reply emission
can be distinguished. Keep the same packet/snapshot/shutdown limits. Start both
captures before the following control/suspect/control sequence on the station:

```sh
: "${PROBE_IFACE:?set the approved same-segment station interface privately}"
: "${CONTROL_IP:?set the working comparison address privately}"
: "${TARGET_IP:?set the suspect address privately}"
timeout --signal=INT --kill-after=2s 8s \
  arping -b -c 3 -I "$PROBE_IFACE" "$CONTROL_IP"
timeout --signal=INT --kill-after=2s 8s \
  arping -b -c 3 -I "$PROBE_IFACE" "$TARGET_IP"
timeout --signal=INT --kill-after=2s 8s \
  arping -b -c 3 -I "$PROBE_IFACE" "$CONTROL_IP"
```

These chosen limits request at most three ordinary broadcast queries per probe,
with an outer 8 s bound plus 2 s shutdown allowance per probe.
`-b` keeps requests broadcast instead of switching to unicast after a reply.
This emits fresh requests without relying on the station's neighbor cache; it
does not reset the panel's cache. Record request/reply timestamps and counts,
not just the tool exit code. Verify tool flavor/options before execution.

| Verdict | Fresh-ARP gate |
|---|---|
| PASS | The working control answers before and after the suspect probe; fresh suspect requests receive replies attributable to the suspect, with endpoint and station evidence consistent. This qualifies resolution only for this segment/window. |
| FAIL | Both control probes succeed but fresh suspect requests have no attributable reply during complete captures. Classify further: request absent at suspect = inbound ARP delivery failure; request received but no reply emitted = panel did not answer; reply emitted but absent at station = return-path failure. |
| INCONCLUSIVE | Control fails, endpoint evidence is unavailable/invalid, routing or response identity is uncertain, duplicate/proxy replies are possible, or the established-session premise cannot be checked concurrently. A station-only timeout cannot distinguish a missing request from a missing reply. |

**Proxy ARP mitigates ARP resolution only. It does not restore broadcast or
multicast delivery.** An infrastructure reply can make address resolution work
while the real delivery fault persists. If proxy ARP is active, record resolution
via the proxy separately and mark the panel's own answer `INCONCLUSIVE` unless
endpoint evidence independently establishes it. Do not enable/disable proxy ARP
under this procedure or present proxy success as broadcast recovery.

## Phase 5: separately qualify infrastructure recovery

A targeted AP, segment, or other infrastructure recovery is a **separately
qualified operator action executed outside this repository**. First identify
the affected component from evidence and obtain the operator's approved change
and rollback procedure. This runbook does not prescribe an AP restart or fleet
reconfiguration. Keep agent/native-path explanations and incomplete gates visible.
Recheck the watchdog risk gate before an RF-affecting recovery; its automatic
reconnect/restart/reboot can otherwise change the experiment without the operator.

1. Preserve the before-state, configuration baseline, per-path verdicts, working
   control, and incident timestamps. Check client compatibility and management/
   physical recovery access before changing the selected infrastructure component.
2. Perform one approved change at a time. Record its exact scope and timestamp
   privately; do not combine recovery with RF tuning or an agent update.
3. Repeat the same bounded agent, session, inbound-delivery, and ARP observations
   after the change, using the same controls, frame classes, and eligibility.
4. Use a declared 24-hour recurrence watch with existing passive telemetry and
   repeat the bounded delivery/ARP windows at its start and end. This is a chosen
   observation budget, not a measured recurrence period. Record whether the
   original trigger actually recurred; a longer suspected cycle remains unmeasured
   until an approved window covers it. Stop active testing on any hard stop.

| Verdict | Recovery qualification |
|---|---|
| PASS | The previously failing path passes after the isolated action, compatibility checks pass, controls remain healthy, and the covered recurrence window contains the original trigger without recurrence. Claim recovery only within that scope/window. |
| FAIL | The fault persists/recurs, another client regresses, or restoration is needed. Record and execute the approved rollback. |
| INCONCLUSIVE | Before/after/control evidence is incomplete, changes were combined, the trigger was not exercised, or recurrence coverage is incomplete. Immediate MQTT `online` is not a recovery verdict. |

## Rollback and sibling-stream boundaries

Stop captures/probes at their bounds, verify that collection processes exit, and
confirm native controls and existing sessions remain usable. The diagnostic
phases make no configuration change. If a separately approved intervention was
performed, restore its exact prior configuration using the external procedure,
then repeat the bounded checks; uncertain restoration is a hard stop requiring
the named operator's recovery path. Keep failed evidence private for comparison.

Issues #149/#150 own queued write policy; #151 owns requested/pending versus
confirmed state; #152 owns bounded metrics. This runbook does not change those
contracts. Existing surfaces are logs, fixed fleet counts, bounded diagnostic
categories, and internal counters, not a general metrics endpoint; there is no
cardinality budget to assume
([custom_components/brilliant_mqtt/diagnostics.py:257](../../../custom_components/brilliant_mqtt/diagnostics.py#L257)).
If a requested counter is not exposed, record it as unmeasured. A narrow #152 hook
would expose the existing bus reconnect-window count or separately recorded
queue-wait/RPC timing for bounded observation; this document defines no endpoint,
labels, cardinality scheme, or new sampling implementation.

## Evidence record

```text
operator / approval / private inventory reference:
code and firmware versions / effective non-secret settings:
runtime power_save / read timestamp and boot / watchdog state, guard and actions:
suspect role / working comparison role / segment and policy equivalence:
incident UTC / observation start UTC / end UTC / actual duration:
agent signals / queue wait / RPC duration / missing timing:
established MQTT last successful exchange / session continuity / verdict:
broadcast class / expected sender / eligibility / received counts / verdict:
multicast class / expected sender / eligibility / received counts / verdict:
capture direction / count cap reached / capture drops / integrity:
ARP control-before / suspect / control-after requests and replies:
ARP request arrival / reply emission / station receipt / proxy ambiguity:
ARP verdict and classified failure direction:
separate recovery approval / baseline / one action / compatibility:
after-state / rollback and restoration evidence:
recurrence window / original trigger coverage / recurrence verdict:
overall result: PASS / FAIL / INCONCLUSIVE
claim scope / unmeasured paths / private evidence references:
```

An overall `PASS` requires every claimed path to pass; carry each path's verdict
alongside it. Missing evidence stays `INCONCLUSIVE`, even when MQTT remains online.

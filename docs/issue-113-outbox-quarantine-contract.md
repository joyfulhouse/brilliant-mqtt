# #113: scene outbox poison-frame quarantine (implemented contract)

Status: **implemented** in the agent (`scene_bridge.py`, `outbox_quarantine.py`,
`outbox_admin.py`, `mqttio.py`). This document is the contract the code
follows. Where the code and this document disagree, that is a bug.

## Problem

Since #92 / PR #107 the durable scene/mode outbox publishes at QoS 1 and marks a
record delivered only after the broker's PUBACK. If the broker closes the
connection every time one specific frame is sent, that record is never
acknowledged. Before this change the delivery loop retried it forever, so one
record blocked every later event and command result (a head-of-line wedge).

## Reproduction analysis

### Deployed defaults cannot produce the wedge

- The deployed broker is Mosquitto 2.1.2. Its configuration sets neither
  `max_packet_size` nor `message_size_limit`, so the only size limit is the
  MQTT protocol maximum of about 256 MB.
- The agent persists every outbox record before sending it, and the state file
  rejects anything over 4 MiB. No outbox frame can therefore approach the
  protocol limit. Normal frames are about 310 to 330 bytes.
- Topics come from the validated topic builders and payloads are ASCII JSON, so
  there is no malformed frame for the broker to reject either.

### What a broker does with a frame it does not want (MQTT 3.1.1, QoS 1)

| Broker setting | Broker behaviour | Effect on the agent |
| --- | --- | --- |
| operator `max_packet_size` smaller than one frame | closes the connection when that frame arrives | **wedge**: the frame is never acknowledged |
| `message_size_limit` smaller than one frame | sends PUBACK, then drops the message | silent loss: the record is marked delivered |
| ACL denies the topic | sends PUBACK, then drops the message | silent loss: the record is marked delivered |
| `max_qos 0` | closes the connection on every QoS-1 publish | the whole outbox and every other QoS-1 publish fail, not one frame |

A wedge needs a broker that closes the connection on one specific frame, which
in practice means an operator `max_packet_size`. Before this change that caused:

- a session teardown about every 5 seconds. Every restart republished
  discovery and state, and the LWT `offline` made Home Assistant mark the panel
  unavailable each time;
- the poison event blocking every later event and result;
- no way to count the failures, because in-memory counters reset with each
  session. Detection counters must therefore be persisted.

### Loopback-only Docker Mosquitto recipe

Use this only on a development machine, bound to loopback. Never point it at a
production broker or panel. Each case needs its own `mosquitto.conf` in an
empty directory:

```bash
mkdir -p /tmp/mosq-113 && cd /tmp/mosq-113
# write mosquitto.conf (and acl for case C) from the case below, then:
docker run --rm -p 127.0.0.1:18830:1883 \
  -v "$PWD:/mosquitto/config:ro" eclipse-mosquitto:2
```

In a second shell, subscribe to everything and keep it running:

```bash
mosquitto_sub -h 127.0.0.1 -p 18830 -V mqttv311 -v -t '#'
```

Then publish one QoS-1 frame of about the agent's real size, with debug output
to see the PUBACK or the disconnect:

```bash
PAYLOAD="$(head -c 320 /dev/zero | tr '\0' x)"
mosquitto_pub -h 127.0.0.1 -p 18830 -V mqttv311 -q 1 -d \
  -t brilliant/ha-control/v1/scene/event/office -m "$PAYLOAD"
```

Every `mosquitto.conf` starts with:

```text
listener 1883
allow_anonymous true
```

| Case | Extra configuration | Expected result |
| --- | --- | --- |
| A. `max_packet_size` | `max_packet_size 200` | no PUBACK; the broker closes the connection. This is the wedge |
| B. `message_size_limit` | `message_size_limit 100` | PUBACK received; the subscriber never sees the message |
| C. ACL deny | `acl_file /mosquitto/config/acl`, with `acl` containing `topic read #` | PUBACK received; the subscriber never sees the message |
| D. `max_qos 0` | `max_qos 0` | every QoS-1 publish closes the connection, whatever its size |

The expected results restate the Mosquitto 2.x behaviour from the issue
investigation. This change was verified with the repository's fake broker
(`tests/test_scene_outbox_quarantine.py`), not by running this recipe.

## The contract as implemented

### 1. Detection: observed broker behaviour only

There are no pre-checks and no invented limits. The agent never inspects a
frame's size, topic or encoding before sending it.

A failure is attributed to the head record only when all of these hold:

- **The session was healthy.** An earlier QoS-1 publish in the same MQTT
  session was acknowledged by the broker.
- **The head record was sent.** The delivery loop marks it in flight right
  before handing it to the broker.
- **The connection dropped within 2 s of sending it.** The MQTT adapter fires a
  connection-lost signal as soon as its reader sees the drop, and the outbox
  compares that time with the send time.

The healthy-session signal uses existing per-session QoS-1 traffic; no probe
was added. Every session publishes the retained-ownership manifest at QoS 1
while the panel bridge reconciles, and that happens before the scene bridge
starts. Outbox records already delivered in the same session also count. The
adapter counts every broker-acknowledged QoS-1 publish
(`AioMqttAdapter.acknowledged_qos1_publishes()`), and the scene bridge reads
that count when it sends the head record.

If the session never had an acknowledged QoS-1 publish, nothing is counted. The
outbox records a `blocked` marker instead, and status reports `outbox_blocked`
until that record is delivered.

A drop more than 2 s after sending, or a drop with nothing in flight, is
ordinary link loss and records nothing.

The record is quarantined after **3 attributable failures in 3 different
sessions spanning at least 60 seconds** of wall-clock time. Each session counts
once. Evidence is bound to the payload's SHA-256, so a changed payload starts a
new count. The counters are persisted, and a successful delivery of that record
clears its evidence.

### 2. Durability

- Quarantine evidence lives in a small sidecar next to the outbox state file,
  `<state file>.quarantine.json`, and not in new state-file fields. After a
  canary rollback, an older agent ignores the sidecar and retries the record
  instead of rejecting its state file.
- The sidecar is written atomically with the same protocol as the state file:
  temporary file, fsync, rename, 0600 file in a 0700 directory. It goes through
  the same single-threaded state executor, so sidecar and state writes are
  totally ordered.
- A quarantined record stays in the state file unchanged. It is never pruned or
  expired, because only delivered records are ever pruned. Only an audited
  `discard` removes it.
- The quarantine is persisted before the record is skipped. If the sidecar
  write fails, the quarantine does not take effect and the record is retried.
- A corrupt or unreadable sidecar logs a warning and loads as "not
  quarantined", so the record is retried. The file is left in place for
  inspection.

### 3. Ordering (O1)

- While a record is quarantined the outbox halts. Nothing is sent past it:
  neither later events nor later results.
- No result is ever released ahead of its event.
- The quarantined record is not offered to the broker, so the disconnect churn
  stops and Home Assistant keeps the panel available.
- State, discovery and commands keep working. Outbox records created meanwhile
  are stored and held.

### 4. Capacity

- Quarantined and held records count against the existing outbox limits and
  are never evicted.
- Under O1 at most one record is quarantined at a time.
- When the outbox fills while halted, the existing full-capacity behaviour
  applies unchanged: new events are refused and new commands are not written.

### 5. Status and diagnostics

- The scene and mode transport status topics report `available: false` with
  reason `outbox_quarantine` or `outbox_blocked`. The payload format and keys
  are unchanged. These reasons take precedence over `state_capacity`, because a
  full outbox is a consequence of the halt; an untrusted state file still wins.
- The companion integration needed no code change. It validates the exact
  status keys and accepts any non-empty reason string for an unavailable
  transport. A test pins this.
- Details go to the bridge meta `diag` object as `diag.outbox`, present only
  while there is evidence: `state` (`quarantined`, `suspect` or `blocked`),
  `key`, `kind` (`event` or `result`), `payload_bytes`, `payload_sha256`,
  `failure_count` and `quarantined_at_ms`. Payload contents are never included.
  The meta is republished on each reconcile and resync, so the details can lag
  the status topic by up to one resync interval.

### 6. Operator recovery

The on-panel CLI is `python3 -m brilliant_mqtt.outbox_admin`. Usage is in
[TROUBLESHOOTING → Scene outbox quarantined](TROUBLESHOOTING.md#scene-outbox-quarantined-outbox_quarantine--outbox_blocked).

- It reads the same environment and paths as the bridge (`Settings.from_env`).
- It refuses to run while the agent is running. It uses the agent's existing
  bus-phase marker (pid, process start tick and boot id). A marker from an
  earlier boot, a dead pid, or a reused pid means the agent is stopped. An
  unrecognised marker or a disabled `BUS_PHASE_FILE` is refused.
- `list` prints key, kind, size, sha256, status and failure count for every
  undelivered record.
- `release <key> <sha256>` clears the evidence so the record is retried.
- `discard <key> <sha256>` removes a quarantined record. Discarding an event
  also discards the command results that depend on it.
- The hash must match the current payload.
- Every action is appended to `<state file>.outbox-audit.log`, rotated once to
  `.1` at 256 KiB. `release` and `discard` are refused if their audit entry
  cannot be written first.

## Rejected approaches

An earlier attempt was rejected. This implementation avoids each of its
problems:

- **A fixed 128 KiB size pre-check** quarantined frames the real broker
  accepts. Detection now uses only observed disconnects.
- **Quarantined records starved capacity invisibly.** They now count against
  the existing limits and are reported through status and `diag.outbox`.
- **A dependent result was blocked forever with no status.** The halt now
  reports `outbox_quarantine`, and `discard` removes the event together with
  its dependent results.
- **There was no recovery path.** The audited `release` and `discard` CLI now
  provides one.
- **Later events silently overtook the quarantined one.** O1 now halts the
  whole outbox instead.

Topic and encoding pre-checks were also left out: topics come from validated
builders and payloads are ASCII JSON, so such checks would be dead code.

## Separate observation: silent loss cannot be detected under MQTT 3.1.1

Under MQTT 3.1.1, a QoS-1 publish denied by an ACL or rejected by
`message_size_limit` still receives a PUBACK, and Mosquitto then drops it. The
agent cannot tell this apart from a delivered message, so it marks the record
delivered. That loss is silent and outside this quarantine. Detecting it needs
MQTT 5 PUBACK reason codes or an end-to-end acknowledgement from Home
Assistant. Keep the broker ACL as documented in
[CONFIGURATION.md](CONFIGURATION.md#broker-user-and-acl) and leave
`message_size_limit` unset.

# Response diagnostics

The agent adds a versioned `diag` object to the existing retained
`brilliant/{panel}/bridge` JSON on its usual reconcile cadence. It creates no
new MQTT topics, publishes, discovery configurations, or Home Assistant entities.
The existing `agent_version`, `deployment_id` (when configured), and
`panel_firmware` (when available) fields keep their meanings.

**`write_ok` is a transport-level bus acknowledgment, NOT proof of physical
actuation.** The bus can acknowledge a write that the physical load does not
apply. This is exactly why the separate, observation-based 80-second mesh
confirmation policy exists (#46). That confirmation runs off the command lane,
does not delay dispatch, and is not included in native RPC response time.

## Scope and lifecycle

These observations describe this agent process. The reserved `mesh` pseudo-panel
has no meta topic; a leader's mesh write activity legitimately contributes to
that leader panel's own `diag`. There are no device, peripheral, variable, or
topic labels, and no receipts, payloads, identifiers, or exception strings.
The fixed schema and two 64-sample buffers keep storage independent of home size;
the diagnostic object is budgeted below 4 KiB.

Counters and sums accumulate for the process lifetime. They survive MQTT and
bus session rebuilds, including stale-stream, stuck-write, and reconnect-storm
recovery. Publishing a snapshot never resets them. Nothing is persisted, and
there is no disk I/O on the metric path. Process exit loses the observations;
process restart begins again at zero. Diagnostics do not determine availability
or change the existing retained `online` / `offline` behavior.

## Schema v1

| Key inside `diag` | Operator definition |
| --- | --- |
| `v` | Schema version, currently `1`. |
| `uptime_s` | Monotonic seconds since the process recorder was constructed. |
| `write_total` | Settled native write tasks, equal to the sum of the seven outcome counters below. |
| `write_ok` | Bus RPC returns while the write remains attached to its caller. |
| `write_error` | Attached ordinary exceptions, excluding the two exact timeout classes below. |
| `write_timeout_bus` | Settlements raising the exact builtin `TimeoutError` class. |
| `write_timeout_async` | Settlements where the RPC raises the exact `asyncio.TimeoutError` class. |
| `write_cancelled` | Cancelled write tasks, including cancellation before the task's first step. |
| `write_detached_late_ok` | RPC returns after the caller deadline detached the write. |
| `write_detached_late_error` | Ordinary exceptions after detachment, excluding the two exact timeout classes. |
| `write_hard_cap_total` | Trips of the existing 15-second unresolved-write latch; separate from outcomes. |
| `superseded_before_dispatch` | Pending command payloads replaced by newer payloads for the same topic in either MQTT queue. |
| `bus_reconnect_total` | Admitted bus-processor reconnect callbacks, excluding initial connection and fenced callbacks. |
| `session_rebuild` | Fixed set of supervisor rebuild counters, defined in the next table. |
| `queue_wait_s_sum` | Cumulative measured seconds from enqueue to acquiring the per-device write lock. |
| `queue_wait_s_count` | Number of measured queue waits; only writes that acquired their lock contribute. |
| `rpc_s_sum` | Cumulative measured seconds from RPC start to settlement, including timeouts, errors, and cancellations. |
| `rpc_s_count` | Number of measured RPC settlements; writes cancelled before starting the RPC contribute no sample. |
| `queue_wait_s_recent_max` | Largest of the last 64 measured queue waits, or `null` before the first sample. |
| `rpc_s_recent_max` | Largest of the last 64 measured RPC durations, or `null` before the first sample. |

| Key inside `diag.session_rebuild` | Operator definition |
| --- | --- |
| `mqtt_reader_dead` | Rebuilds caused by `MqttReaderDeadError`: the MQTT receive loop stopped. |
| `bus_stale` | Rebuilds caused by `BusStaleError`: the bus push stream became stale. |
| `bus_write_stuck` | Rebuilds caused by `BusWriteStuckError`: an unresolved write tripped the hard cap. |
| `bus_reconnect_storm` | Rebuilds caused by `BusReconnectStormError`: the existing reconnect-rate breaker tripped. |
| `mqtt_transport_overload` | Rebuilds caused by `MqttTransportOverloadError`: the bounded transport queue overloaded. |
| `other` | Rebuilds caused by any other exception, including retained-ledger failures. |

On the panel's Python 3.10, builtin `TimeoutError` and `asyncio.TimeoutError` are
distinct, unrelated classes. Timeout subclasses count as ordinary errors.
The caller's own deadline does not count an outcome: the still-running write
counts once when it later settles. Caller cancellation alone does not detach
or cancel that write. A hard-cap trip is also not another outcome.

## Reading trends

Diff successive retained snapshots from the same process to obtain interval
counts and sums. Divide a counter delta by the `uptime_s` delta to obtain a rate.
After a process restart, start a new comparison instead of mixing lifetimes.
The recent maxima describe the last 64 measured samples, not a time window or
the worst event since startup.

For a timing mean, use that family's **sum / count**, never **sum / write_total**.
For an interval mean, divide the change in its sum by the change in its count.
When the count or count delta is zero, there is no measured mean.

Timeouts deliberately contribute to `rpc_s`, so a rising RPC mean may mean writes
are timing out rather than merely slowing. A write cancelled before its first
step or while still awaiting the lock has no RPC-start stamp and contributes
neither timing sample. Consequently, `queue_wait_s_count` can be lower than
`write_total`: no complete enqueue-to-lock interval was measured. `rpc_s_count`
normally equals `write_total`, except for writes cancelled before RPC dispatch.
An absent measurement is omitted, never replaced with a fabricated zero.

- Rising queue-wait sums or recent maxima indicate contention before dispatch;
  RPC durations describe time after lock acquisition.
- Rising timeout, error, or detached-outcome counters warrant comparing bus
  response timing with independently observed device state.
- Increasing `session_rebuild` counters mean the supervisor keeps rebuilding
  the session; use the reason to investigate the broker or bus.
- Increasing `bus_reconnect_total` reports processor reconnect activity, which
  may happen without a supervisor rebuild.
- Increasing `superseded_before_dispatch` means newer queued intent replaced
  older queued intent. It does not describe physical delivery or actuation.

These are observations, not health verdicts or new policy thresholds.

## Supersession limitation and evolution

**Mesh generation supersession is excluded and belongs to #151.** The counter
covers only pending replacements in the transport and lane queues. Those queues
own disjoint pending sets: a command removed by replacement never advances to
the next queue, so one logical command can be superseded at most once. Mesh
generation invalidation and pending/confirmed state need their own accounting.

Sibling work in #149/#150 and #151 can adopt the shared recorder's narrow verbs:
`note_superseded`, `note_write_settled`, `note_bus_reconnect`,
`note_session_rebuild`, and `note_hard_cap`. Snapshots are synchronous copies;
all mutations stay on the event loop.

Within `diag.v = 1`, schema evolution is additive only. Renaming a key is
forbidden: add a new key and document deprecation of the old one. Breaking
semantic changes require a new `v`. A future integration can expose these keys
as entities without changing the agent's publishing behavior.

Because the agent and its consumers are released separately, consumers MUST
ignore unknown keys inside `diag` without failing or warning. A missing `diag`
means diagnostics are unavailable or unsupported, never measured zeros.
Retained metadata survives an agent rollback until replaced. Each full meta
republish replaces the entire retained payload rather than merging fields;
the ledger-degraded publish, for example, omits `diag`. Consumers MUST tolerate
both missing diagnostics and stale diagnostics left by a previous agent version,
and MUST NOT treat a retained replay as fresh evidence.

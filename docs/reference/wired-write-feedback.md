# Wired write feedback

Wired light and switch commands get immediate MQTT feedback, but a successful
message-bus RPC means only that the transport accepted the request. It does not
prove physical actuation, and the observer's notification-fed mirror can remain
frozen. Native variable timestamps are optional, are not guaranteed monotonic
or unique, and redundant writes need not re-stamp a value. The bridge therefore
does not order wired observations by their numeric timestamps.

The bridge keeps two forms of state for a wired primary load:

- Native per-variable observations retain their value and native timestamp.
- A successful command owns a timestamp-less provisional projection for only
  the fields it requested. A newer request generation supersedes the older one.

The adapter records a local source generation and capture sequence when it
actually copies a push/read result. The same sequence is sampled when a native
write is issued, after admission and the per-device lock. A capture positively
known to precede that boundary remains native evidence, but it cannot override
the active projection for the commanded field. Unrelated fields from the same
capture still apply. Source generations are incomparable; reconnect or session
replacement retires the old projection and comparison state.

Capture order is not physical chronology. In particular, these histories are
indistinguishable after the fact: an OFF snapshot captured before an effective
ON write and delivered late, or an accepted but ineffective ON write followed
by the mirror's genuine unchanged OFF. Equal value/timestamp pairs are only
baseline-identical, while different timestamps can also describe pre-command
state. When local capture provenance does not prove that a contradiction
predates issue, the bridge publishes the native value immediately and exposes
the ambiguity instead of suppressing a possible genuine OFF.

## State payload

While wired feedback is active, the normal state payload has three additional
keys, following the mesh feedback convention:

- `wired_write_status`: `provisional`, `ambiguous`, `observed`, or
  `unconfirmed`. `observed` means only that a matching native record was
  received; it never means physically confirmed.
- `wired_requested`: the native variable targets while the request remains
  active, otherwise `{}`.
- `wired_write_deadline`: the wall-clock deadline while active, otherwise
  `null`.

`provisional` means no post-issue native record has displaced the requested
projection. `ambiguous` means an unorderable native contradiction is being
published. `observed` labels a matching native record. `unconfirmed` labels the
latest native state after the projection expires without resolution.

The fixed, non-renewing provisional interval is 20 seconds
(`WIRED_PROVISIONAL_SECONDS`). This matches the approximately 20-second frozen
mirror observed in the pilot and bounds how long requested values can be shown.
Its timer runs outside the MQTT command lane. Expiry publishes the latest native
observation as `unconfirmed`; it does not retry, reassert, or read back the
command. Mesh primary, mesh auxiliary, writer serialization, and admission
semantics are unchanged.

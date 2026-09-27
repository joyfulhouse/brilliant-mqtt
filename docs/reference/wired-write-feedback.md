# Wired write feedback

Wired light and switch commands get immediate MQTT feedback, but a successful
message-bus RPC means only that the transport accepted the request. It does not
prove physical actuation, and the observer's notification-fed mirror can remain
frozen. Native variable timestamps are optional, are not guaranteed monotonic
or unique, and redundant writes need not re-stamp a value. The bridge therefore
does not order wired observations by their numeric timestamps.

The bridge keeps one feedback record per wired primary load, with two forms of
state:

- Native per-variable observations retain their value and native timestamp.
- A successful command owns a timestamp-less provisional projection for only
  the fields it requested. A newer request generation supersedes the older one.

The adapter stamps each captured field with its last issued write sequence.
Actual writes advance that field's sequence after admission, under the
per-device lock. A delayed capture with an older sequence is positively known
to predate an intervening write to that field; it cannot erase later native
evidence or restore a displaced projection. Other fields in the same capture
still apply. An equal sequence is publishable, even if its value and timestamp
match the pre-write baseline. These sequences establish only this local causal
boundary; they are not an observation high-water mark.

Push notifications are partial: an omitted field provides no information.
Full `get_all` and `get_peripheral` reads are complete: an omitted field becomes
unknown, including `on` (published as `state: null`) or dimming metadata.
An explicit raw `None` also means unknown. Reconnect advances the local source
generation before any await. Reads pin that generation and their field sequences
before awaiting the observer, so a delayed result retains its capture boundary;
captures from an older generation cannot mutate the current record. Rebinding
retires request feedback after the per-field fence has excluded known pre-write
values, so a translation change cannot restore one of those values.

The reducer in `wired_feedback.py` makes all wired feedback decisions. The
bridge owns one timer and one serialized, latest-wins publisher per load. A
native publication obligation survives a failed or overtaken publish and is
cleared only after MQTT accepts native state. Reconnect retirement happens
before the replacement read; an absent load cannot discard publication debt.
Shutdown cancels and joins feedback work and prevents in-flight reads from
restarting it.

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
The window begins when the first successful RPC completes after the last
accepted native publication; delayed success starts the window at that success
time. Further successes cannot extend it. Its timer runs outside the MQTT
command lane. Expiry publishes the latest native observation as `unconfirmed`;
it does not retry, reassert, or read back the command. A replacement waiting
for admission leaves the existing deadline in force.
Mesh primary, mesh auxiliary, writer serialization, and admission semantics
are unchanged.

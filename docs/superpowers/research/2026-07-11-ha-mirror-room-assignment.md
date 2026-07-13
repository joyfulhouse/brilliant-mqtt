# HA Mirror V2 — Room Assignment: firmware facts (live-verified)

- **Date:** 2026-07-11 (pilot panel, read-only introspection)
- **Resolves:** the two open sub-questions from
  `2026-07-10-ha-mirror-v1-visibility.md` (room enumeration; struct variables).

## 1. Enumerating Brilliant rooms (id + name)

The room catalog lives on the **virtual home device** as the
`home_configuration` peripheral's `rooms` variable. The value is a
**base64-encoded thrift-binary `Rooms` struct**
(`thrift_types.configuration.ttypes.Rooms` = `{rooms: map<string, Room>}`,
`Room = {id: string, name: string}`), decodable with the firmware's own helper:

```python
from lib.serialization import deserialize, serialize   # firmware, on-panel only
from thrift_types.configuration.ttypes import Rooms, RoomAssignment
rooms = deserialize(Rooms, rooms_variable_value)        # id -> Room{id,name}
```

Live catalog (2026-07-11) includes `Backyard`, `Balcony`, `Office`, etc. Room
ids are opaque strings — three formats coexist in this home (`"1"`, `"2"`,
`"<32-hex>:<ms>"`, `"<20-hex>:<ms>"`). **Treat the id as fully opaque.**

## 2. `room_assignment` — encoding CORRECTION

`room_assignment` exists on **every** peripheral (the base `Peripheral` class
provides it — our mirrored lamps already expose it, empty:
`'DwABCwAAAAAA'` = `RoomAssignment(room_ids=[])`).

The value is a base64 thrift-binary `RoomAssignment{room_ids: list<string>}`,
and — **correction to the 2026-07-10 note** — each `room_ids` entry is the
catalog `Room.id` **verbatim**. The `:timestamp` suffix seen in decoded values
is *part of the room id itself*, not an assignment timestamp to append.
Verified: office panel's gangbox `room_assignment.room_ids ==
["b6f97347b34010df5d52:1683406303305"]` which is exactly the catalog id of the
"Office" room; a Kitchen-assigned load carries `["2"]` — the literal Kitchen id.

Round-trip verified: `serialize(deserialize(RoomAssignment, v)) == v`
(byte-identical) using `lib.serialization`.

## 3. Setting it on a mirrored peripheral (CONFIRMED on pilot, 2026-07-12)

The working mechanism is the existing
`Peripheral.__dict__["_set_value_internal"](notify=True)` reflection path,
passing the **`RoomAssignment` struct OBJECT** (`RoomAssignment(room_ids=[...])`).
Passing the base64-serialized string raises
`TypeError: Expected type RoomAssignment but got str` — the framework validates
the in-process value against the variable's declared thrift STRUCT type. The
base64 string form is only the wire/snapshot representation seen via observer
`get_all()` (so READS still `deserialize(...)` from the string).

Also confirmed the hard way: the firmware `PeripheralHost` (hosting side) has
**no** registry-read API — reading `home_configuration.rooms` requires a
dedicated read-only `RPCObserver` connection (the `brilliant_mqtt/bus.py`
recipe). Both facts were live-verified end-to-end: all five mirrored lamps
assigned to their Backyard/Balcony rooms on the pilot.

## 4. Where to read the catalog from

`get_all()` (which the mirror's host process can already reach via its bus
connection) returns the virtual home device with `home_configuration` and its
`rooms` variable — no new API needed. Note `get_all()` device containers vary
(`.items()` map vs immutable list) — iterate defensively.

## Safety

All introspection was read-only (`get_all` + local thrift decode); no sets, no
writes, no reboots.

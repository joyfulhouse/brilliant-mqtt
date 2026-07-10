# HA Mirror V1 — Home-Wide Visibility Spike

- **Date:** 2026-07-10
- **Plan task:** Task 1 (gate) of `docs/superpowers/plans/2026-07-10-ha-mirror-tier1.md`
- **Verdict: GO** — a peripheral hosted on one panel's own bus device renders in
  other panels' home models. The leader-election coordination model is valid.

## Question

Does a peripheral hosted on the elected leader panel's own bus device
(`virtual_device_id=None`) appear on **other** panels in the home, or only on
the hosting panel? The entire Tier-1 design (one leader hosts all HA mirrors)
depends on home-wide visibility.

## Method (read-only except the bounded test host)

1. On **panel A**, hosted a minimal test LIGHT (peripheral_type 27; vars
   `on`/`dimmable`/`intensity`) on panel A's own device via the proven recipe
   (`run_startable`, bus socket `/var/run/brilliant/server_socket`),
   time-bounded with `timeout`.
2. From **panel B** (a physically different panel, its own bus), ran a
   read-only observer that subscribed to panel A's device id and read panel A's
   peripheral set as panel B sees it.
3. Deleted the test peripheral (borrowed
   `ConditionalPeripheralHost.delete_peripheral`) and re-observed from both
   panels.

## Result

- Panel B's observer reported **panel A's device visible** and the test
  peripheral **present** in panel B's home model (panel A's peripheral count as
  seen from panel B rose by exactly one over its steady-state baseline, and the
  test peripheral's name appeared in the list).
- After deletion, panel B's observer reported the peripheral **absent** and the
  count back to baseline — the delete propagated home-wide.
- Both panels remained healthy throughout (load steady, well under the caution
  threshold); no residue left on either panel.

## Implications for the build

- **GO:** proceed with the leader-election model unchanged. One elected panel
  hosting the HA mirrors on its own device makes them visible across the home.
- **V2 follow-up (room assignment):** `room_assignment` on real peripherals is a
  base64-encoded thrift structure (a list of room-id entries), **not** a plain
  string. The mirror must construct the proper `room_assignment` thrift value to
  place a mirrored entity in a specific room — resolve this format before the
  orchestrator task (plan Task 6) rather than assuming a string room id. Visibility
  itself does not depend on room assignment.

## Safety notes

All on-panel work ran on the designated pilot pair, time-bounded, with the test
peripheral deleted and verified gone on both panels before finishing. No device
ids, hostnames, or credentials are recorded here (kept value-free per repo
policy).

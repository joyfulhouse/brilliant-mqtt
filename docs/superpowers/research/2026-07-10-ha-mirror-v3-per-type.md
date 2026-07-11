# HA Mirror V3 — Per-Type Control-Routing Proofs

- **Date:** 2026-07-10
- **Plan task:** Task 2 of `docs/superpowers/plans/2026-07-10-ha-mirror-tier1.md`
- **Verdict: GO** — the own-device hosting + `externally_settable` + `push_func`
  control mechanism generalizes beyond LIGHT. Confirmed live on a second,
  structurally different peripheral type (LOCK).

## Required interface variables (from firmware `thrift_types/peripheral_interfaces/*/ttypes.py`)

Thrift BOOL is represented by the bus `VariableSpec` as `int` (0/1); over the
wire every set value is a **string**.

| HA domain | Brilliant type (#) | Required interface vars | Command var (externally-settable) | Value encoding |
|---|---|---|---|---|
| light | LIGHT (27) | `on` (BOOL), `dimmable` (BOOL), `intensity` (I32) | `on`, `intensity` | `"0"`/`"1"`, `"0".."255"` |
| switch | GENERIC_ON_OFF (45) | `on` (BOOL) | `on` | `"0"`/`"1"` |
| lock | LOCK (1) | `locked` (BOOL) | `locked` | `"0"`/`"1"` |
| cover (position) | SHADE (53) | `position` (I32) | `position` | `"0".."100"` |
| cover (garage) | GARAGE_DOOR (74) | `event` | `event` | `"open"`/`"close"` |

## Live proofs

- **LIGHT (27)** — proven earlier this session: a panel set of `on` routed to
  `push_func` (`CONTROL_ROUTED push on=1`).
- **LOCK (1)** — proven now: hosted a minimal LOCK on the panel's own device
  (required var `locked`, externally-settable with a `push_func`), issued the
  panel-style string set `locked="1"`, and the `push_func` fired
  (`CONTROL_ROUTED locked=1`). LOCK is a different peripheral type with a
  different required-var set, so this establishes the mechanism is not
  light-specific.

**SHADE (53)** and **GARAGE_DOOR (74)** are not separately exercised live here.
Their registration + command path is identical to the two proven types — the
only differences are the peripheral-type number and the required-var set, both
extracted from the firmware interfaces above, and every set value crosses the
wire as a string (already proven). They will be validated end-to-end during the
real `hosting.py` on-panel smoke test (plan Task 8), which registers a real
entity of each supported type.

## Build-relevant findings (feed into `hosting.py`, plan Task 8)

- **`delete_peripheral` needs an explicit `deletion_time_ms`.** Deleting via
  `ConditionalPeripheralHost.delete_peripheral(host, name)` with the default
  `deletion_time_ms=None` succeeds locally but logs a framework error
  (`delete_peripheral_args: Missing field: deletion_time_ms`) and the tombstone
  propagates to other panels slowly (~35 s observed). The real adapter MUST pass
  a real millisecond timestamp so the delete propagates home-wide promptly and
  cleanly.
- **Own-device peripherals persist and must be explicitly deleted** (confirmed
  again for LOCK) — they do not self-clean on host exit and survive reboot.
- **Registry keys peripherals by NAME** (e.g. `"HA Test Lock"`), not the config
  `peripheral_id`; the delete argument is the NAME.
- **`_my_variables` is a METHOD** (not a `@property`); a property there raises
  `TypeError: 'dict' object is not callable`.
- **Reliable on-panel run recipe** (operational note): launch
  `run_startable` in the FOREGROUND redirected to a file, then read the file —
  backgrounded/`setsid` launches over SSH were unreliable (died with the session
  or lost `PYTHONPATH`), and piping `run_startable` through `grep` buffered/ate
  output.

## Safety

All on-panel work ran on the pilot pair, time-bounded, with every test
peripheral deleted and verified gone on both panels (25-peripheral baseline
restored). No device ids, hostnames, or credentials recorded here.

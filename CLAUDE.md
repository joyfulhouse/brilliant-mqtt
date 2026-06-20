# CLAUDE.md — brilliant-mqtt

Guidance for Claude Code (and other agents) working in this repo.

## What this is

An on-panel agent that bridges each **Brilliant Control** in-wall panel's internal
**message bus** (Apache Thrift over a unix socket) to the home **central MQTT
broker** as Home Assistant **MQTT-Discovery** entities — replacing the unreliable
HomeKit Controller path with a robust local read+control mechanism.

**Status (2026-06-12):** Milestones 1–8.1, 10, and 11 complete — live PoC, full
implementation, the dedicated broker user/ACL, and **the pilot: the bridge is
live under systemd on the pilot panel** with verified
discovery/telemetry/commands/LWT, the realtime hardening (hot diff-poll +
reconnect hook + stale-stream watchdog; see poc-findings §8b — the bus
notification stream can die silently, and `get_all` reads a frozen mirror when
it does), and **M11: BLE-mesh loads under the reserved `mesh` pseudo-panel via
priority leader election** (`mesh_leader.py`; MESH_PRIORITY env; plan M11 has
the protocol + live-verified facts). **Remaining:** fleet roll-out automation
(M8 Step 2 — must set per-panel MESH_PRIORITY), roll-out to the remaining
panels, HA automation migration (M9). Operator-private deployment context
(pilot hostname, broker/infra repos, fleet inventory) lives in
`CREDENTIALS.local.md`.

## Read these first (in order)

1. `docs/reference/poc-findings.md` — **the live-verified bus facts**
   (connection recipe, real schema, scoping decision). Where older docs
   disagree, this wins.
2. `docs/reference/message-bus-api.md` — the on-box `RPCObserver` / ttypes API
   (introspected background).
3. `docs/ARCHITECTURE.md` — how the bridge is structured and why.
4. `docs/reference/deployment.md` — how it runs on a panel + OTA/MQTT details.
5. `CREDENTIALS.local.md` — **gitignored**; panel SSH password + hardware + MQTT
   + HA details you need to do the work.
6. `docs/claude/` — **operator-local, untracked in the public repo**
   (gitignored): the dated research report, design spec, and the executable
   implementation plan with per-milestone status. If you are working in the
   operator's clone these exist on disk; in a fresh public clone they don't —
   the public docs above carry everything needed to use or extend the bridge.

## How to execute the remaining plan

Milestones 1–7 (code), the pilot, and M10–11 are done. Next: fleet roll-out
automation, then roll out to the remaining panels and migrate the HA
automations (M8–M9) — the operator-private repos and hosts involved are listed
in `CREDENTIALS.local.md`. For any new code tasks use
`superpowers:subagent-driven-development`; skills are in `.claude/skills/`.

## Non-negotiables

- **Python 3.10 only (agent, repo root).** The panel interpreter is Python
  3.10.9 and that is the runtime. `requires-python = ">=3.10,<3.11"`. This
  deliberately overrides the global "Python 3.13+" default — do not bump; do not
  use 3.11+ syntax. The **integration is separate** (companion HA integration,
  Python 3.14, own tooling/tests in `ha/`). For HACS tree-compliance it now lives
  at the repo ROOT (`custom_components/brilliant_mqtt/`); the py3.14 `pyproject` +
  tests stay in `ha/`. Run its gate from the repo root with the `ha/` env+config:
  `uv run --project ha ruff check --fix --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha ruff format --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha mypy --strict --config-file ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha pytest -c ha/pyproject.toml ha/tests`.
- **uv for everything.** `uv sync`, `uv run pytest`, `uv run ruff check --fix`,
  `uv run ruff format`, `uv run mypy --strict src tests`. **Never** `pip`/`pip3`.
- **TDD.** Failing test → minimal impl → green → commit. Small, frequent commits.
- **Never disable linters** (`# noqa`, `# type: ignore`). Fix the root cause.
- **Never import `lib.message_bus_api` outside `src/brilliant_mqtt/bus.py`.**
  Everything else is unit-tested off-panel behind `BusClient`/`MqttClient`
  Protocols with fakes — the suite must run on any machine.
- **No secrets in git.** `CREDENTIALS.local.md` and `*.local.md` are gitignored;
  MQTT creds come from the environment / the operator's secret store at deploy
  time. If you ever see a secret staged, STOP and unstage.

## Panel safety (these are production in-wall touchscreens)

- SSH is **root, password-only** — use `sshpass` with `SSHPASS` env (the password
  has shell-hostile characters). Details in `CREDENTIALS.local.md`.
- **Treat SSH as read-only/diagnostic** except deliberate deploys, on **the
  designated pilot panel first** (named in `CREDENTIALS.local.md`). A bricked
  panel needs a physical visit.
- Use `NumberOfPasswordPrompts=1` so a bad password fails fast (avoid lockout).
- The bridge agent + its systemd unit live in **`/var`** (persistent across OTA);
  `/data/switch-embedded` is replaced on every firmware update.
- Resource-cap the agent (systemd `MemoryMax`/`CPUQuota`/`Nice`) so a bug can't
  degrade the panel UI. It only uses the same bus APIs the HomeKit peripheral does.

## Pre-commit gate

`uv run ruff check --fix && uv run ruff format && uv run mypy --strict src tests && uv run pytest`
— all green before any commit. **If you changed the integration
(`custom_components/brilliant_mqtt/`) or its tests (`ha/tests/`), also run the
integration gate** (`uv run --project ha ruff check --fix --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha ruff format --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha mypy --strict --config-file ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha pytest -c ha/pyproject.toml ha/tests`)
— BOTH must be green.

## Repo / deploy split

- This repo = the agent + its docs/plan + a `deploy/` reference. Broker setup
  and install steps for the public are in `INSTALL.md`.
- Fleet automation and broker provisioning live in the operator's **private
  infrastructure repos** — names, paths, and conventions are listed in
  `CREDENTIALS.local.md` (gitignored), together with the operator
  cross-references (panel inventory, firmware mirror, related docs).

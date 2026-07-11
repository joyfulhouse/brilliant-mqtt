# Documentation

Documentation for brilliant-mqtt.

## Install

Start at the **[install overview](../INSTALL.md)**, which walks the
prerequisites checklist and deploy choice, and links these focused guides:

- [install/root-ssh.md](install/root-ssh.md) — enabling Brilliant's official
  root SSH on a panel, with caveats and a verify step.
- [install/mqtt-broker.md](install/mqtt-broker.md) — standalone Mosquitto or
  the Home Assistant Mosquitto add-on, plus the dedicated user and ACL.

## User guides

- [ha-integration.md](ha-integration.md) — the HACS companion integration:
  onboarding flow, management entities, services, events, options, and repair.
- [voice.md](voice.md) — voice satellite: enabling wake word + mic + speaker
  on a panel, wake word choices, Assist pipeline assignment, and AEC.
- [CONFIGURATION.md](CONFIGURATION.md) — env vars, MQTT topics, broker user/ACL
  (the canonical ACL snippet lives here), mesh leader election.
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) — problem → fix entries and debug
  logging.

## Reference

- [ARCHITECTURE.md](ARCHITECTURE.md) — how the bridge is structured and why.
- [brilliant-panel/README.md](brilliant-panel/README.md) — reverse-engineered
  panel software, UI/UX information architecture, cloud boundaries, complete
  peripheral type catalog, HA support matrix, and validation runbook.
- [DEVELOPMENT.md](DEVELOPMENT.md) — development environment, quality gates,
  testing rules.
- [reference/message-bus-api.md](reference/message-bus-api.md) — the
  introspected on-box `RPCObserver` / ttypes API the bridge codes against.
- [reference/deployment.md](reference/deployment.md) — on-panel runtime, OTA
  survival, credentials, roll-out and rollback.
- [reference/poc-findings.md](reference/poc-findings.md) — Milestone-1
  live-panel findings: connection recipe, real schema, device-scoping decision.

---

> Internal design notes, session logs, and process artifacts (the dated
> research report, design spec, and implementation plan) are operator-local
> and untracked in the public repository. Agents working in this repo start
> at [../CLAUDE.md](../CLAUDE.md).

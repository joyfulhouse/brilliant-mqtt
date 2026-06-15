# Documentation

Documentation for brilliant-mqtt.

| Document | Description |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | How the bridge is structured and why |
| [CONFIGURATION.md](CONFIGURATION.md) | Runtime configuration, MQTT topics, broker user/ACL |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Common problems and fixes |
| [DEVELOPMENT.md](DEVELOPMENT.md) | Development environment, quality gates, testing rules |

## Installing

Start at the [install overview](../INSTALL.md), which walks the three steps and
links these focused guides:

- [install/root-ssh.md](install/root-ssh.md) — enabling Brilliant's official
  root SSH on a panel, with the caveats to read first.
- [install/mqtt-broker.md](install/mqtt-broker.md) — standalone Mosquitto or the
  Home Assistant Mosquitto add-on, plus the dedicated user and ACL.
- [ha-integration.md](ha-integration.md) — the HACS companion integration that
  deploys/updates/repairs panels from the HA UI.

## Reference

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

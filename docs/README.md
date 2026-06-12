# Documentation

Documentation for brilliant-mqtt.

| Document | Description |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | How the bridge is structured and why |
| [CONFIGURATION.md](CONFIGURATION.md) | Runtime configuration, MQTT topics, broker user/ACL |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Common problems and fixes |
| [DEVELOPMENT.md](DEVELOPMENT.md) | Development environment, quality gates, testing rules |

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

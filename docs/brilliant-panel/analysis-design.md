# Brilliant Panel Reverse-Engineering Design

**Date:** 2026-07-11

**Target build:** `v26.06.03.1` (`2174d3882504c03bf9c7b3f78f0cad4a5ae039f7a32f0bb9c5eca02dc0370b0b`)
**Purpose:** Build an evidence-backed model of Brilliant Control software, UI information architecture, and local control surfaces that can guide the Home Assistant integration.

## Artifact boundary

The raw corpus lives under the gitignored path `artifacts/brilliant-panel/<release>/`. It includes the pilot panel's `/data/switch-ui`, `/data/switch-embedded`, and `/var` trees, plus narrowly selected system launch files and executables. Raw files, decompiler projects, runtime state, logs, databases, credentials, identifiers, and user content must never be committed.

Tracked outputs are limited to:

- reproducible acquisition and analysis scripts that accept credentials through the environment;
- cryptographic manifests and sanitized structural inventories;
- original documentation and small derived excerpts needed to explain behavior;
- explicit confidence labels distinguishing static evidence, runtime introspection, and live control validation.

## Acquisition model

Use the designated pilot for the corpus after comparing signed OSTree commits across the fleet. Capture files read-only over SSH, preserve paths and metadata in tar archives, hash each archive, and extract working copies locally. A fleet table records release/commit coverage without recording panel credentials or unique device identifiers.

The `/var` capture is authorized for local analysis. It is treated as sensitive even though it is ignored: analysis may use configuration, logs, databases, caches, and runtime history, but tracked outputs must redact secrets, account data, home/device identifiers, network credentials, and personal content.

## Analysis tracks

1. **Platform and launch topology:** OSTree, systemd, uWSGI emperor/vassals, message-bus transport, persistence boundaries, watchdogs, and update behavior.
2. **Control plane:** Cython modules, generated Thrift types, peripherals, variables, subscriptions, commands, scenes, third-party bridges, and cloud/local boundaries.
3. **UI application:** ELF metadata, Qt resources, embedded QML/strings, route/screen concepts, interaction patterns, media paths, settings hierarchy, and UI-to-message-bus mappings.
4. **Home Assistant mapping:** compare discovered capabilities with bridge and companion-integration code, classify implemented/unimplemented behavior, and define safe live-validation probes.
5. **Operational independence:** identify which functions continue locally, which require Brilliant cloud services, and what HA can replace without emulating unrelated third-party ecosystems.

## Evidence and safety rules

- Never mutate the Brilliant application stack during acquisition or static analysis.
- Live writes are opt-in validation steps and use the pilot first; this phase prefers existing verified observations and read-only probes.
- Every material finding cites an artifact path, symbol/string, runtime observation, or existing live probe.
- Inference is labeled as inference. Absence of a string or symbol is not proof that a feature does not exist.
- Proprietary implementation details are summarized rather than reproduced wholesale in tracked documentation.

## Deliverables

Documentation under `docs/brilliant-panel/` will include a corpus manifest, software architecture, control-plane catalog, UI/UX IA map, cloud-dependency analysis, Home Assistant capability matrix, validation runbook, and prioritized integration recommendations. A top-level index will state scope, confidence, and known coverage gaps.

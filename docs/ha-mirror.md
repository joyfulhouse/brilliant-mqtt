# HA Mirror — Home Assistant entities as native panel devices

The **HA mirror** is the reverse bridge: it reflects selected Home Assistant
entities into the Brilliant in-wall panel UI as **native, controllable
devices**. A panel toggle/slider drives a Home Assistant service call; a Home
Assistant state change updates the panel tile. Control is local via the panel
message bus.

It runs on every panel but is **active on only one** — a priority election over
a retained MQTT topic picks a single leader that hosts the mirrored devices for
the whole home (they render on every panel).

## What can be mirrored (Tier 1)

| Home Assistant domain | Appears on panel as | Control |
|---|---|---|
| `light` | Light (on/off + brightness) | ✅ |
| `switch` | On/off device | ✅ |
| `lock` | Lock | ✅ |
| `cover` (position) | Shade | ✅ |
| `cover` (garage) | Garage door | ✅ |

Cameras, doorbells, thermostats, and media players are **not** in this tier.

## Choose what to mirror

Apply a **label** (default `brilliant`) to each Home Assistant entity you want
on the panels — in Home Assistant: *Settings → entity → Labels*. Only labeled,
supported entities are mirrored. Change the label name with `MIRROR_LABEL`.

## Enable it (recommended: via the integration)

The Brilliant MQTT integration installs and configures the mirror per panel — no
manual SSH needed. In each panel's config entry (add or **Reconfigure**), tick
**HA mirror** and fill in:

| Field | Value |
|---|---|
| HA WebSocket URL | e.g. `ws://homeassistant.local:8123/api/websocket` |
| Long-lived token | a Home Assistant long-lived access token |
| Leader priority | election rank, **lower wins**, `0` = never lead — give each panel a distinct value |
| Mirror label | entity label to mirror (default `brilliant`) |

The integration ships the agent, writes `/etc/brilliant-ha-mirror.env` (0600),
installs the systemd unit, and enables it — reusing the panel's existing broker
credentials for the leader election. A per-panel **HA mirror** switch turns it on
and off; a Repair re-installs the payload if it goes missing. The token is stored
in the config entry like the broker password and is never logged.

> The mirror requires the (always-installed) MQTT bridge on the same panel, which
> the integration guarantees.

The manual deploy below is the fallback for panels not managed by the integration.

## Configuration (`/etc/brilliant-ha-mirror.env`, mode 0600)

| Variable | Required | Purpose |
|---|---|---|
| `PANEL` | yes | This panel's slug (e.g. `office`). |
| `HA_WS_URL` | yes | Home Assistant WebSocket URL, e.g. `ws://homeassistant.local:8123/api/websocket`. |
| `HA_TOKEN` | yes | A Home Assistant long-lived access token. |
| `MQTT_HOST` / `MQTT_USERNAME` / `MQTT_PASSWORD` | yes | Broker creds (used for the leader election). |
| `MQTT_PORT` | no | Default `1883`. |
| `MIRROR_LABEL` | no | Entity label to mirror. Default `brilliant`. |
| `ROOM_OVERRIDES` | no | JSON object mapping HA area names to opaque Brilliant room IDs. Overrides automatic name matching. |
| `LEADER_PRIORITY` | no | Election rank; **lower number wins**, `0` = never lead. Give each panel a distinct value. |
| `LEADER_HEARTBEAT_SECONDS` | no | Election heartbeat. Default `10`. |
| `LOG_LEVEL` | no | Default `INFO`. |

> **Set a distinct `LEADER_PRIORITY` per panel.** The lowest number that is
> online leads and hosts the mirrors; if it drops, the next takes over.

## Manual deploy (pilot one panel first)

> **Prerequisite:** the panel must already run the main bridge — the mirror
> reuses `brilliant_mqtt` (installed at `/var/brilliant-mqtt/app`) and its
> vendored `aiomqtt` (`/var/brilliant-mqtt/vendor`) for the leader election.
> `aiohttp` is part of the panel's own Python; nothing else needs vendoring.

1. **Copy the app**: `scp -r src/brilliant_ha_mirror root@<panel-ip>:/var/brilliant-ha-mirror/app/`
2. **Write** `/etc/brilliant-ha-mirror.env` (table above), mode 0600.
3. **Smoke-run in the foreground** (watch logs):
   ```bash
   PYTHONPATH=/var/brilliant-ha-mirror/app:/var/brilliant-mqtt/app:/var/brilliant-mqtt/vendor:/data/switch-embedded \
     /data/switch-embedded/env/bin/python3 -m brilliant_ha_mirror
   ```
4. Confirm the labeled entities appear on the panels and control works both ways.
5. Install the unit and enable it:
   ```bash
   cp deploy/brilliant-ha-mirror.service /etc/systemd/system/
   systemctl enable --now brilliant-ha-mirror
   ```

Repeat per panel with a distinct `LEADER_PRIORITY`. The unit lives under `/var`
(survives OTA); after a firmware update, re-install it.

## Room assignment (V2)

Mirrored entities are placed in native Brilliant rooms automatically. The mirror
uses the entity registry's area, falling back to the entity's device area, and
matches that Home Assistant area name to a Brilliant room name with a
case-insensitive exact comparison. Brilliant room IDs are opaque and are used
verbatim.

`ROOM_OVERRIDES` takes precedence over automatic matching. Its value is a JSON
object whose keys are HA area names and whose values are Brilliant room IDs; for
example, `{"Back Yard":"opaque-brilliant-room-id"}`. It is optional—automatic
matching works with no new configuration.

If an area has no matching room and no override, the peripheral remains
unassigned (`room_ids` is empty) and the mirror logs that outcome once rather
than on every reconciliation. On later reconciliations it re-asserts the native
`room_assignment` whenever the entity's HA area or the Brilliant rooms catalog
changes.

## How it works (for maintainers)

- Each mirrored entity is hosted as a peripheral on the leader panel's **own**
  message-bus device (`virtual_device_id=None`), which the firmware propagates
  home-wide — the same mechanism the panel's built-in hue/lifx/smartthings
  integrations use.
- A panel command reaches the peripheral's `push_func`, which calls the Home
  Assistant service. Home Assistant state is reflected back with the framework's
  internal value-update (no command feedback loop).
- On leader loss or shutdown the hosted peripherals are deleted (with an
  explicit deletion timestamp) so no stale devices linger.

**Design + verification:** `docs/superpowers/specs/2026-07-10-ha-mirror-tier1-design.md`
and the `docs/superpowers/research/2026-07-10-ha-mirror-*` notes (operator-local).

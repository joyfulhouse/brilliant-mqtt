"""Constants and the MQTT/on-panel contract for the Brilliant MQTT panel manager."""

from __future__ import annotations

from homeassistant.const import Platform

DOMAIN = "brilliant_mqtt"
PLATFORMS: list[Platform] = [Platform.BINARY_SENSOR, Platform.BUTTON, Platform.UPDATE]

# Config entry data keys (one entry per panel; each stores ITS OWN root password).
CONF_HOST = "host"
CONF_ROOT_PASSWORD = "root_password"
CONF_PANEL = "panel"
CONF_MESH_PRIORITY = "mesh_priority"
# Broker credentials written into the panel's env file (PANEL_ENV_FILE) over SSH at
# deploy/repair — the integration itself talks to MQTT only via HA's mqtt integration
# (see manifest dependencies).
CONF_MQTT_HOST = "mqtt_host"
CONF_MQTT_PORT = "mqtt_port"
CONF_MQTT_USERNAME = "mqtt_username"
CONF_MQTT_PASSWORD = "mqtt_password"

# Internally managed config-entry state (never shown in a config-flow form).
DATA_SSH_HOST_KEY = "ssh_host_key"  # TOFU-pinned on first successful connect
DATA_LAST_FIRMWARE = "last_firmware"  # persisted so panel_updated survives HA restarts

# Options keys + defaults.
OPT_AUTO_REPAIR = "auto_repair"
OPT_OFFLINE_GRACE_MINUTES = "offline_grace_minutes"
OPT_REPAIR_COOLDOWN_MINUTES = "repair_cooldown_minutes"
OPT_TRUST_HOST_KEY_CHANGES = "trust_host_key_changes"
DEFAULT_AUTO_REPAIR = True
DEFAULT_OFFLINE_GRACE_MINUTES = 10
DEFAULT_REPAIR_COOLDOWN_MINUTES = 60
# Opt-in, default OFF: let repair/update auto-re-pin a rotated SSH host key on the
# already-adopted same-host panel (offers the root password to the new-key host).
DEFAULT_TRUST_HOST_KEY_CHANGES = False

# The reserved whole-home pseudo-panel — never manageable (no host behind it).
MESH_PANEL = "mesh"

# MQTT contract with the on-panel agent (docs/ARCHITECTURE.md "Data Flow").
AVAILABILITY_ONLINE = "online"
AVAILABILITY_OFFLINE = "offline"


def availability_topic(panel: str) -> str:
    """LWT availability topic published by the on-panel agent."""
    return f"brilliant/{panel}/availability"


def meta_topic(panel: str) -> str:
    """Retained bridge meta topic ({"agent_version", "panel_firmware"})."""
    return f"brilliant/{panel}/bridge"


# On-panel paths owned by the integration (mirror docs/reference/deployment.md).
PANEL_VAR_DIR = "/var/brilliant-mqtt"
PANEL_APP_DIR = f"{PANEL_VAR_DIR}/app"
PANEL_VENDOR_DIR = f"{PANEL_VAR_DIR}/vendor"
PANEL_STAGED_DIR = f"{PANEL_VAR_DIR}/system"
PANEL_VERSION_FILE = f"{PANEL_VAR_DIR}/VERSION"
PANEL_ENV_FILE = "/etc/brilliant-mqtt.env"
PANEL_UNIT_FILE = "/etc/systemd/system/brilliant-mqtt.service"
SERVICE_NAME = "brilliant-mqtt"

EVENT_TYPE = "brilliant_mqtt_event"
SIGNAL_PANEL_STATE = f"{DOMAIN}_panel_state"  # dispatcher: f"{SIGNAL_PANEL_STATE}_{entry_id}"

# `brilliant_mqtt_event` subtypes (the event's data["type"]) — the public automation
# contract documented in docs/ha-integration.md.
EVENT_PANEL_UPDATED = "panel_updated"
EVENT_REPAIR_STARTED = "repair_started"
EVENT_REPAIR_SUCCEEDED = "repair_succeeded"
EVENT_REPAIR_FAILED = "repair_failed"
EVENT_NEEDS_ATTENTION = "needs_attention"
EVENT_AGENT_UPDATED = "agent_updated"
# Fired when a rotated SSH host key was auto-trusted during repair/update (opt-in
# OPT_TRUST_HOST_KEY_CHANGES). Extra data: new_host_key. Auditable security event.
EVENT_HOST_KEY_REPINNED = "host_key_repinned"

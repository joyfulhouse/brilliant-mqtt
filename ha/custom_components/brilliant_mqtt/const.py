"""Constants and the MQTT/on-panel contract for the Brilliant MQTT panel manager."""

from __future__ import annotations

DOMAIN = "brilliant_mqtt"
PLATFORMS = ["binary_sensor", "button", "update"]

# Config entry data keys (one entry per panel; each stores ITS OWN root password).
CONF_HOST = "host"
CONF_ROOT_PASSWORD = "root_password"
CONF_PANEL = "panel"
CONF_MESH_PRIORITY = "mesh_priority"
CONF_MQTT_HOST = "mqtt_host"
CONF_MQTT_PORT = "mqtt_port"
CONF_MQTT_USERNAME = "mqtt_username"
CONF_MQTT_PASSWORD = "mqtt_password"
CONF_SSH_HOST_KEY = "ssh_host_key"  # TOFU-pinned on first successful connect
CONF_LAST_FIRMWARE = "last_firmware"  # persisted so panel_updated survives HA restarts

# Options keys + defaults.
OPT_AUTO_REPAIR = "auto_repair"
OPT_OFFLINE_GRACE_MINUTES = "offline_grace_minutes"
OPT_REPAIR_COOLDOWN_MINUTES = "repair_cooldown_minutes"
DEFAULT_AUTO_REPAIR = True
DEFAULT_OFFLINE_GRACE_MINUTES = 10
DEFAULT_REPAIR_COOLDOWN_MINUTES = 60

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

"""Constants and the MQTT/on-panel contract for the Brilliant MQTT panel manager."""

from __future__ import annotations

from homeassistant.const import Platform

DOMAIN = "brilliant_mqtt"
CONFIG_ENTRY_VERSION = 3
PLATFORMS: list[Platform] = [
    Platform.BINARY_SENSOR,
    Platform.BUTTON,
    Platform.SELECT,
    Platform.SWITCH,
    Platform.UPDATE,
]

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
# Voice satellite feature keys.
CONF_VOICE_ENABLED = "voice_enabled"
CONF_VOICE_WAKE_WORD = "voice_wake_word"
CONF_VOICE_HA_HOST = "voice_ha_host"
# HA mirror feature keys.
CONF_HA_MIRROR_WS_URL = "ha_mirror_ws_url"
CONF_HA_MIRROR_TOKEN = "ha_mirror_token"
CONF_HA_MIRROR_LEADER_PRIORITY = "ha_mirror_leader_priority"
CONF_HA_MIRROR_LABEL = "ha_mirror_label"
DEFAULT_HA_MIRROR_LABEL = "brilliant"
DEFAULT_HA_MIRROR_LEADER_PRIORITY = 0
# diyHue CA-recovery hook feature keys.
CONF_HUE_CA_CERT = "hue_ca_cert"

# Home Assistant remote-scanner bridge. This kill switch is deliberately
# independent from the on-panel observer and remains off when absent.
CONF_BLE_SCANNER_ENABLED = "ble_scanner_enabled"
DEFAULT_BLE_SCANNER_ENABLED = False

# Home Assistant-owned MQTT control plane. These global values are copied to each
# panel entry by the configuration vertical slice; the singleton elects the enabled
# entry with the lexicographically smallest panel slug as its settings owner.
CONF_HA_CONTROL_ENABLED = "ha_control_enabled"
CONF_HA_CONTROL_LABEL = "ha_control_label"
CONF_ROOM_OVERRIDES = "room_overrides"
CONF_HA_CONTROL_DOMAINS = "ha_control_domains"
CONF_MAX_MIRRORED_ENTITIES = "max_mirrored_entities"
# Scene-control configuration is surfaced by Task 9's config flow. Task 8 consumes
# the stored keys already so the singleton can select a default panel and actions.
CONF_SCENE_PANEL = "scene_panel"
CONF_SCENE_ACTIONS = "scene_actions"
DEFAULT_HA_CONTROL_ENABLED = False
DEFAULT_HA_CONTROL_LABEL = "brilliant"
DEFAULT_HA_CONTROL_DOMAINS = ("light", "switch")
DEFAULT_MAX_MIRRORED_ENTITIES = 50
HA_CONTROL_DOMAINS = ("light", "switch", "lock", "cover")

# Per-panel component selection (see docs/ha-integration.md — components/switches).
CONF_COMPONENTS = "components"  # entry data: {component_id: bool}
COMPONENT_BRIDGE = "bridge"
COMPONENT_VOICE = "voice"
COMPONENT_WIFI_WATCHDOG = "wifi_watchdog"
COMPONENT_BUS_WATCHDOG = "bus_watchdog"
COMPONENT_HA_MIRROR = "ha_mirror"
COMPONENT_HUE_CA = "hue_ca"

# Internally managed config-entry state (never shown in a config-flow form).
DATA_SSH_HOST_KEY = "ssh_host_key"  # TOFU-pinned on first successful connect
DATA_LAST_FIRMWARE = "last_firmware"  # persisted so panel_updated survives HA restarts
DATA_CONTROL_PLANE = "ha_control_plane"
DATA_HA_MIRROR_RETIRE_VERIFIED = "ha_mirror_retire_verified"

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

# Panel reboot + pre-reboot diagnostics. The panels wedge two ways (an uptime-decay
# wedge that only a reboot clears, and Wi-Fi power-save packet starvation), and the
# panel's journald is VOLATILE (/run tmpfs — only the current boot survives), so a
# diagnostics bundle is pulled over SSH BEFORE the reboot that would erase the
# evidence. Bundles land under <config>/brilliant_mqtt/diagnostics/<panel>/, newest
# DIAGNOSTICS_RETENTION kept.
DEFAULT_REBOOT_JOURNAL_LINES = 400
MIN_REBOOT_JOURNAL_LINES = 100
MAX_REBOOT_JOURNAL_LINES = 2000
DIAGNOSTICS_SUBDIR = "diagnostics"
DIAGNOSTICS_RETENTION = 14

# MQTT contract with the on-panel agent (docs/ARCHITECTURE.md "Data Flow").
AVAILABILITY_ONLINE = "online"
AVAILABILITY_OFFLINE = "offline"


def panel_device_name(slug: str) -> str:
    """Display name for a panel slug — "office-bath" → "Brilliant Office Bath".

    MUST stay byte-identical to the MQTT-discovery device name the agent publishes
    (and to the management entities' device name) so both land on ONE device page.
    Hoisted here so the config flow, the manager's voice env, and the base entity all
    agree on exactly one transform.
    """
    display = slug.replace("_", " ").replace("-", " ").title()
    return f"Brilliant {display}"


def availability_topic(panel: str) -> str:
    """LWT availability topic published by the on-panel agent."""
    return f"brilliant/{panel}/availability"


def meta_topic(panel: str) -> str:
    """Retained bridge meta topic ({"agent_version", "panel_firmware"})."""
    return f"brilliant/{panel}/bridge"


GITHUB_REPO_SLUG = "joyfulhouse/brilliant-mqtt"


def voice_asset_url(integration_version: str) -> str:
    """GitHub release-asset URL for the voice payload matching this integration release.

    Every release uploads brilliant-voice-payload-<VOICE_PAYLOAD_VERSION>.tar.gz to
    its own tag, so the asset for the version the user installed always resolves.
    """
    return (
        f"https://github.com/{GITHUB_REPO_SLUG}/releases/download/"
        f"v{integration_version}/brilliant-voice-payload-{VOICE_PAYLOAD_VERSION}.tar.gz"
    )


# Voice payload version + selectable wake words.
# Must equal src/brilliant_voice/__init__.py __version__ (a release-workflow guard
# enforces the match). The integration downloads the matching release asset.
VOICE_PAYLOAD_VERSION = "0.1.0"
# Bundled microWakeWord models the wake-word select offers.
VOICE_WAKE_WORDS = ("okay_nabu", "hey_jarvis", "hey_mycroft")
DEFAULT_VOICE_WAKE_WORD = "okay_nabu"

# On-panel paths owned by the integration (mirror docs/reference/deployment.md).
PANEL_VAR_DIR = "/var/brilliant-mqtt"
PANEL_APP_DIR = f"{PANEL_VAR_DIR}/app"
PANEL_VENDOR_DIR = f"{PANEL_VAR_DIR}/vendor"
PANEL_STAGED_DIR = f"{PANEL_VAR_DIR}/system"
PANEL_VERSION_FILE = f"{PANEL_VAR_DIR}/VERSION"
PANEL_ENV_FILE = "/etc/brilliant-mqtt.env"
PANEL_UNIT_FILE = "/etc/systemd/system/brilliant-mqtt.service"
SERVICE_NAME = "brilliant-mqtt"

# On-panel voice paths.
PANEL_VOICE_VAR_DIR = "/var/brilliant-voice"
PANEL_VOICE_STAGED_DIR = f"{PANEL_VOICE_VAR_DIR}/system"
PANEL_VOICE_VERSION_FILE = f"{PANEL_VOICE_VAR_DIR}/VOICE_VERSION"
PANEL_VOICE_ENV_FILE = "/etc/brilliant-voice.env"
PANEL_VOICE_UNIT_FILE = "/etc/systemd/system/brilliant-voice.service"
VOICE_SERVICE_NAME = "brilliant-voice"

# On-panel HA mirror paths.
PANEL_HA_MIRROR_VAR_DIR = "/var/brilliant-ha-mirror"
PANEL_HA_MIRROR_APP_DIR = f"{PANEL_HA_MIRROR_VAR_DIR}/app"
PANEL_HA_MIRROR_STAGED_DIR = f"{PANEL_HA_MIRROR_VAR_DIR}/system"
PANEL_HA_MIRROR_ENV_FILE = "/etc/brilliant-ha-mirror.env"
PANEL_HA_MIRROR_UNIT_FILE = "/etc/systemd/system/brilliant-ha-mirror.service"
HA_MIRROR_SERVICE_NAME = "brilliant-ha-mirror"

# On-panel Wi-Fi watchdog paths (stdlib script tree under the OTA-proof /var bridge dir).
PANEL_WIFI_WATCHDOG_DIR = f"{PANEL_VAR_DIR}/wifi_watchdog"
PANEL_WIFI_WATCHDOG_UNIT_FILE = "/etc/systemd/system/brilliant-wifi-watchdog.service"
WIFI_WATCHDOG_SERVICE_NAME = "brilliant-wifi-watchdog"

# On-panel bus-health watchdog paths (stdlib script tree under the OTA-proof /var bridge dir).
PANEL_BUS_WATCHDOG_DIR = f"{PANEL_VAR_DIR}/bus_watchdog"
PANEL_BUS_WATCHDOG_UNIT_FILE = "/etc/systemd/system/brilliant-bus-watchdog.service"
BUS_WATCHDOG_SERVICE_NAME = "brilliant-bus-watchdog"

# On-panel diyHue CA-recovery hook paths. Code tree lives under the OTA-proof /var
# bridge dir like the other watchdogs; the operator's injected CA PEM lives under its
# OWN top-level dir (outside PANEL_HUE_CA_DIR) so a hook uninstall/reinstall never
# touches it. Two units (service + timer) because the hook is a periodic oneshot.
PANEL_HUE_CA_DIR = f"{PANEL_VAR_DIR}/hue_ca"
PANEL_HUE_CA_CERT_FILE = "/var/brilliant-hue-ca/injected-ca.pem"
PANEL_HUE_CA_SERVICE_UNIT_FILE = "/etc/systemd/system/brilliant-hue-ca.service"
PANEL_HUE_CA_TIMER_UNIT_FILE = "/etc/systemd/system/brilliant-hue-ca.timer"
HUE_CA_TIMER_NAME = "brilliant-hue-ca.timer"

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

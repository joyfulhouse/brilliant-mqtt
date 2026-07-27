from pathlib import Path
from typing import Any

import pytest
from homeassistant.core import HomeAssistant

from custom_components.brilliant_mqtt import components as comp
from custom_components.brilliant_mqtt import const, panel_ops
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HA_MIRROR,
    COMPONENT_HUE_CA,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
)
from tests.fakes import FakeShell

_VALID_MQTT_CA_PEM = """-----BEGIN CERTIFICATE-----
MIIBfTCCASOgAwIBAgIUdPxf3XpyWhlomBsnOw4v6PnRbEwwCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJY2EtYS10ZXN0MB4XDTI2MDcxODIzMDQyMFoXDTM2MDcxNTIz
MDQyMFowFDESMBAGA1UEAwwJY2EtYS10ZXN0MFkwEwYHKoZIzj0CAQYIKoZIzj0D
AQcDQgAELbHkjdm57Utb7nuP+u68qOg+5DtLm3J3BkkLthx4TSYFkD02O8STczCH
/eykkJrKVd90Zn4NlnnwPHh1TqXBKaNTMFEwHQYDVR0OBBYEFHI1jyb/yVM80rJa
pCrwjLltX/JzMB8GA1UdIwQYMBaAFHI1jyb/yVM80rJapCrwjLltX/JzMA8GA1Ud
EwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSAAwRQIhALuIYO82yKVgMuFSWB70ALJE
UZ0KQhgbgLS5gw+Rh6xeAiBu0CzhNXZ6QO4blinurR+/lGd5m1qRG/RuKanWrWOo
Jw==
-----END CERTIFICATE-----
"""


def test_component_id_constants() -> None:
    assert const.CONF_COMPONENTS == "components"
    assert const.COMPONENT_BRIDGE == "bridge"
    assert const.COMPONENT_VOICE == "voice"
    assert const.COMPONENT_HA_MIRROR == "ha_mirror"
    assert const.COMPONENT_WIFI_WATCHDOG == "wifi_watchdog"
    assert const.COMPONENT_BUS_WATCHDOG == "bus_watchdog"


def test_mqtt_tls_and_panel_state_constants_are_stable() -> None:
    assert const.CONF_MQTT_TLS_ENABLED == "mqtt_tls_enabled"
    assert const.CONF_MQTT_TLS_CA == "mqtt_tls_ca"
    assert const.PANEL_MQTT_TLS_DIR == "/var/brilliant-mqtt/tls"
    assert const.PANEL_RETAINED_TOPICS_FILE == "/var/brilliant-mqtt/state/owned-topics.json"


def test_registry_has_bridge_and_voice() -> None:
    assert comp.REGISTRY[COMPONENT_BRIDGE].locked is True
    assert comp.REGISTRY[COMPONENT_VOICE].locked is False
    assert comp.REGISTRY[COMPONENT_VOICE].default_enabled is False


def test_default_components_bridge_on_voice_off() -> None:
    d = comp.default_components()
    assert d[COMPONENT_BRIDGE] is True
    assert d[COMPONENT_VOICE] is False


def test_selected_ids_includes_bridge_always() -> None:
    assert comp.selected_ids({}) == [COMPONENT_BRIDGE]
    sel = comp.selected_ids({CONF_COMPONENTS: {COMPONENT_VOICE: True}})
    assert COMPONENT_BRIDGE in sel and COMPONENT_VOICE in sel


def test_wifi_watchdog_registry_row_default_enabled() -> None:
    row = comp.REGISTRY[COMPONENT_WIFI_WATCHDOG]
    assert row.default_enabled is True
    assert row.locked is False


def test_default_components_wifi_watchdog_on() -> None:
    d = comp.default_components()
    assert d[COMPONENT_WIFI_WATCHDOG] is True


def test_bus_watchdog_registry_row_default_enabled() -> None:
    row = comp.REGISTRY[COMPONENT_BUS_WATCHDOG]
    assert row.default_enabled is True
    assert row.locked is False


def test_default_components_bus_watchdog_on() -> None:
    d = comp.default_components()
    assert d[COMPONENT_BUS_WATCHDOG] is True


def test_ha_mirror_registry_row_is_deprecated_but_keeps_removal_recipe() -> None:
    row = comp.REGISTRY[COMPONENT_HA_MIRROR]
    assert row.id == COMPONENT_HA_MIRROR
    assert row.label == "HA mirror"
    assert row.default_enabled is False
    assert row.locked is False
    assert row.deprecated is True
    assert row.remove is panel_ops.uninstall_ha_mirror


def test_default_components_excludes_deprecated_ha_mirror() -> None:
    assert COMPONENT_HA_MIRROR not in comp.default_components()


def test_optional_order_hides_deprecated_ha_mirror() -> None:
    opts = comp.optional()
    ids = [c.id for c in opts]
    assert ids == [
        COMPONENT_VOICE,
        COMPONENT_WIFI_WATCHDOG,
        COMPONENT_BUS_WATCHDOG,
        COMPONENT_HUE_CA,
    ]


async def test_deprecated_ha_mirror_install_always_fails_closed(
    hass: HomeAssistant,
) -> None:
    shell = FakeShell()
    await shell.connect()
    with pytest.raises(panel_ops.PanelOpError, match="deprecated"):
        await comp.REGISTRY[COMPONENT_HA_MIRROR].install(hass, shell, {})
    assert not shell.commands
    assert not shell.uploads
    assert not shell.dir_uploads


def test_selected_ids_never_selects_deprecated_ha_mirror() -> None:
    selected = comp.selected_ids(
        {CONF_COMPONENTS: {COMPONENT_BRIDGE: True, COMPONENT_HA_MIRROR: True}}
    )
    assert selected == [COMPONENT_BRIDGE]


def _bridge_data(*, tls_enabled: bool, ca_pem: str | None = None) -> dict[str, Any]:
    data: dict[str, Any] = {
        const.CONF_PANEL: "office",
        const.CONF_MESH_PRIORITY: 1,
        const.CONF_MQTT_HOST: "broker.example",
        const.CONF_MQTT_PORT: 8883 if tls_enabled else 1883,
        const.CONF_MQTT_USERNAME: "fleet",
        const.CONF_MQTT_PASSWORD: "secret",
        "mqtt_tls_enabled": tls_enabled,
    }
    if ca_pem is not None:
        data["mqtt_tls_ca"] = ca_pem
    return data


async def test_bridge_install_stages_custom_ca_before_payload_and_uses_returned_path(
    hass: HomeAssistant,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del payload_dir
    events: list[str] = []
    staged_bytes: list[bytes] = []
    rendered_env: list[str] = []
    ca_path = "/var/brilliant-mqtt/tls/mqtt-ca-0fa6a631898df0f5.pem"

    async def stage_mqtt_ca(shell: Any, ca_bytes: bytes) -> str:
        events.append("stage_ca")
        staged_bytes.append(ca_bytes)
        return ca_path

    async def deploy_payload(shell: Any, local_payload_dir: str, version: str) -> None:
        events.append("deploy")

    async def ensure_configs(shell: Any, unit: str, env: str) -> None:
        events.append("config")
        rendered_env.append(env)

    async def enable_now(shell: Any) -> None:
        events.append("enable")

    monkeypatch.setattr(panel_ops, "stage_mqtt_ca", stage_mqtt_ca, raising=False)
    monkeypatch.setattr(panel_ops, "deploy_payload", deploy_payload)
    monkeypatch.setattr(panel_ops, "ensure_configs", ensure_configs)
    monkeypatch.setattr(panel_ops, "enable_now", enable_now)

    shell = FakeShell()
    await shell.connect()
    await comp._bridge_install(
        hass,
        shell,
        _bridge_data(tls_enabled=True, ca_pem=_VALID_MQTT_CA_PEM),
    )

    assert events == ["stage_ca", "deploy", "config", "enable"]
    assert staged_bytes == [_VALID_MQTT_CA_PEM.encode()]
    assert len(rendered_env) == 1
    parsed = panel_ops.parse_env(rendered_env[0])
    assert parsed["MQTT_TLS_ENABLED"] == "1"
    assert parsed["MQTT_TLS_CA_FILE"] == ca_path


@pytest.mark.parametrize("tls_enabled", [False, True])
async def test_bridge_install_without_custom_ca_does_not_stage_one(
    hass: HomeAssistant,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    tls_enabled: bool,
) -> None:
    del payload_dir
    rendered_env: list[str] = []

    async def unexpected_stage(shell: Any, ca_bytes: bytes) -> str:
        raise AssertionError("custom CA must not be staged")

    async def deploy_payload(shell: Any, local_payload_dir: str, version: str) -> None:
        return None

    async def ensure_configs(shell: Any, unit: str, env: str) -> None:
        rendered_env.append(env)

    async def enable_now(shell: Any) -> None:
        return None

    monkeypatch.setattr(panel_ops, "stage_mqtt_ca", unexpected_stage, raising=False)
    monkeypatch.setattr(panel_ops, "deploy_payload", deploy_payload)
    monkeypatch.setattr(panel_ops, "ensure_configs", ensure_configs)
    monkeypatch.setattr(panel_ops, "enable_now", enable_now)

    shell = FakeShell()
    await shell.connect()
    await comp._bridge_install(
        hass,
        shell,
        _bridge_data(tls_enabled=tls_enabled),
    )

    assert len(rendered_env) == 1
    parsed = panel_ops.parse_env(rendered_env[0])
    assert parsed["MQTT_TLS_ENABLED"] == ("1" if tls_enabled else "0")
    assert "MQTT_TLS_CA_FILE" not in parsed


async def test_bridge_install_ca_stage_failure_blocks_panel_deployment(
    hass: HomeAssistant,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del payload_dir
    downstream_calls: list[str] = []

    async def stage_mqtt_ca(shell: Any, ca_bytes: bytes) -> str:
        raise panel_ops.PanelOpError("ca upload failed")

    async def deploy_payload(shell: Any, local_payload_dir: str, version: str) -> None:
        downstream_calls.append("deploy")

    async def ensure_configs(shell: Any, unit: str, env: str) -> None:
        downstream_calls.append("config")

    async def enable_now(shell: Any) -> None:
        downstream_calls.append("enable")

    monkeypatch.setattr(panel_ops, "stage_mqtt_ca", stage_mqtt_ca, raising=False)
    monkeypatch.setattr(panel_ops, "deploy_payload", deploy_payload)
    monkeypatch.setattr(panel_ops, "ensure_configs", ensure_configs)
    monkeypatch.setattr(panel_ops, "enable_now", enable_now)

    shell = FakeShell()
    await shell.connect()
    with pytest.raises(panel_ops.PanelOpError, match="ca upload failed"):
        await comp._bridge_install(
            hass,
            shell,
            _bridge_data(tls_enabled=True, ca_pem=_VALID_MQTT_CA_PEM),
        )

    assert downstream_calls == []


async def test_bridge_install_tls_refusal_precedes_payload_deployment(
    hass: HomeAssistant,
    payload_dir: Path,
) -> None:
    """A fail-closed TLS guard must not leave a partially updated payload behind."""
    del payload_dir
    from custom_components.brilliant_mqtt.shell import RunResult

    shell = FakeShell(
        responses={
            panel_ops.MQTT_TLS_GUARD_COMMAND: RunResult(
                0,
                "MQTT_TLS_ENABLED=1\n",
                "",
            )
        }
    )
    await shell.connect()

    with pytest.raises(panel_ops.PanelOpError, match="mqtt_tls_downgrade_refused"):
        await comp._bridge_install(hass, shell, _bridge_data(tls_enabled=False))

    assert shell.commands == [panel_ops.MQTT_TLS_GUARD_COMMAND]
    assert shell.dir_uploads == []
    assert shell.uploads == []


@pytest.mark.parametrize(
    ("tls_enabled", "ca_value"),
    [
        ("true", None),
        (1, None),
        (None, None),
        (True, ""),
        (True, "not a certificate"),
        (
            True,
            "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----\n",
        ),
        (True, 42),
        (False, _VALID_MQTT_CA_PEM),
    ],
)
async def test_bridge_install_rejects_invalid_tls_before_any_panel_mutation(
    hass: HomeAssistant,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    tls_enabled: object,
    ca_value: object,
) -> None:
    del payload_dir
    calls: list[str] = []

    async def stage_mqtt_ca(shell: Any, ca_bytes: bytes) -> str:
        calls.append("stage")
        return "/var/brilliant-mqtt/tls/mqtt-ca-0fa6a631898df0f5.pem"

    async def deploy_payload(shell: Any, local_payload_dir: str, version: str) -> None:
        calls.append("deploy")

    async def ensure_configs(shell: Any, unit: str, env: str) -> None:
        calls.append("config")

    async def enable_now(shell: Any) -> None:
        calls.append("enable")

    monkeypatch.setattr(panel_ops, "stage_mqtt_ca", stage_mqtt_ca)
    monkeypatch.setattr(panel_ops, "deploy_payload", deploy_payload)
    monkeypatch.setattr(panel_ops, "ensure_configs", ensure_configs)
    monkeypatch.setattr(panel_ops, "enable_now", enable_now)

    data = _bridge_data(tls_enabled=False)
    data[const.CONF_MQTT_TLS_ENABLED] = tls_enabled
    if ca_value is not None or tls_enabled is True:
        data[const.CONF_MQTT_TLS_CA] = ca_value

    shell = FakeShell()
    await shell.connect()
    with pytest.raises(ValueError, match="invalid_mqtt_tls"):
        await comp._bridge_install(hass, shell, data)

    assert calls == []
    assert shell.commands == []
    assert shell.uploads == []
    assert shell.dir_uploads == []

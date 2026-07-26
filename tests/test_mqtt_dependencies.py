"""The panel and HA runtimes declare one exact MQTT client stack."""

from __future__ import annotations

import json
from importlib.metadata import version
from pathlib import Path
from typing import Any, cast

import tomli

ROOT = Path(__file__).parents[1]
MQTT_REQUIREMENTS = ["aiomqtt==2.5.1", "paho-mqtt==2.1.0"]


def _mqtt_requirements(requirements: list[str]) -> list[str]:
    return [
        requirement
        for requirement in requirements
        if requirement.startswith(("aiomqtt", "paho-mqtt"))
    ]


def test_root_installs_exact_mqtt_versions() -> None:
    assert version("aiomqtt") == "2.5.1"
    assert version("paho-mqtt") == "2.1.0"


def test_every_runtime_declares_only_exact_mqtt_requirements() -> None:
    root_project: dict[str, Any] = tomli.loads((ROOT / "pyproject.toml").read_text())
    ha_project: dict[str, Any] = tomli.loads((ROOT / "ha/pyproject.toml").read_text())
    manifest = cast(
        dict[str, Any],
        json.loads((ROOT / "custom_components/brilliant_mqtt/manifest.json").read_text()),
    )

    root_dependencies = cast(list[str], root_project["project"]["dependencies"])
    ha_dependencies = cast(list[str], ha_project["dependency-groups"]["dev"])
    manifest_requirements = cast(list[str], manifest["requirements"])

    assert _mqtt_requirements(root_dependencies) == MQTT_REQUIREMENTS
    assert _mqtt_requirements(manifest_requirements) == MQTT_REQUIREMENTS
    assert _mqtt_requirements(ha_dependencies) == MQTT_REQUIREMENTS

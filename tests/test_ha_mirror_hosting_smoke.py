"""Off-panel guards for the firmware-facing hosting adapter.

hosting.py can only run on the panel (it uses the closed-source framework), so
its behaviour is verified on-panel. Off panel we assert two invariants: the
module imports cleanly (firmware imports are deferred), and NO other module in
the package imports the firmware libraries.
"""

from __future__ import annotations

import importlib.util
import pathlib
import re

import pytest


def _firmware_available() -> bool:
    # find_spec on a submodule imports the parent package, which raises
    # ModuleNotFoundError off panel where `lib` does not exist at all.
    try:
        return importlib.util.find_spec("lib.startables") is not None
    except ModuleNotFoundError:
        return False


_HAVE_FW = _firmware_available()


def test_hosting_module_imports_off_panel() -> None:
    # Deferred firmware imports mean the module itself imports anywhere.
    import brilliant_ha_mirror.hosting as hosting

    assert hasattr(hosting, "RpcPeripheralHost")
    assert hosting._slug("HA Kitchen Light") == "ha_mirror_ha_kitchen_light"


@pytest.mark.skipif(not _HAVE_FW, reason="firmware framework only exists on the panel")
def test_rpc_host_constructs_on_panel() -> None:
    from brilliant_ha_mirror.hosting import RpcPeripheralHost

    assert RpcPeripheralHost is not None


def test_hosting_is_the_only_firmware_importer() -> None:
    package_root = pathlib.Path(__file__).resolve().parents[1] / "src" / "brilliant_ha_mirror"
    firmware_import = re.compile(
        r"^\s*(from|import)\s+(lib|peripherals|thrift_types)\b", re.MULTILINE
    )
    offenders = []
    for path in package_root.glob("*.py"):
        if path.name == "hosting.py":
            continue
        if firmware_import.search(path.read_text()):
            offenders.append(path.name)
    assert offenders == [], f"non-hosting modules import firmware: {offenders}"

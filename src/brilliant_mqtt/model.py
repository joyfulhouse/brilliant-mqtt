"""Normalized device model for the Brilliant Control MQTT bridge.

Maps one bus Peripheral into a BrilliantDevice. No Thrift/panel imports —
this module is pure Python and runs on any machine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class DeviceKind(str, Enum):
    LIGHT = "light"
    SWITCH = "switch"
    BINARY_SENSOR = "binary_sensor"
    SENSOR = "sensor"
    ALWAYS_ON = "always_on"
    HARDWARE = "hardware"
    UI = "ui"
    WIFI = "wifi"
    UNKNOWN = "unknown"


# Map of bus PeripheralType integer values → DeviceKind.
# Integer constants are inlined here so this module never imports panel libs.
_PERIPHERAL_TYPE_MAP: dict[int, DeviceKind] = {
    27: DeviceKind.LIGHT,  # LIGHT
    45: DeviceKind.SWITCH,  # GENERIC_ON_OFF
    40: DeviceKind.SWITCH,  # OUTLET
    5: DeviceKind.BINARY_SENSOR,  # MOTION_SENSOR
    80: DeviceKind.SENSOR,  # CLIMATE_SENSOR
    46: DeviceKind.ALWAYS_ON,  # ALWAYS_ON (power monitoring only — no light/switch)
    22: DeviceKind.HARDWARE,  # HARDWARE (panel diagnostics + controls)
    12: DeviceKind.UI,  # UI (presence / child-lock / identify)
    29: DeviceKind.WIFI,  # WIFI (connectivity diagnostics)
}


def kind_for_peripheral_type(peripheral_type: int) -> DeviceKind:
    """Return the DeviceKind for a bus PeripheralType integer value.

    Any type not in the known map returns DeviceKind.UNKNOWN.
    """
    return _PERIPHERAL_TYPE_MAP.get(peripheral_type, DeviceKind.UNKNOWN)


@dataclass(frozen=True)
class Variable:
    """A single variable from a bus Peripheral, always stored as a string value."""

    name: str
    value: str
    externally_settable: bool = False

    def as_bool(self) -> bool:
        """True iff value is exactly "1"."""
        return self.value == "1"

    def as_int(self) -> int | None:
        """Parse value as int; returns None on any parse failure."""
        try:
            return int(self.value)
        except (ValueError, TypeError):
            return None

    def as_float(self) -> float | None:
        """Parse value as float; returns None on any parse failure."""
        try:
            return float(self.value)
        except (ValueError, TypeError):
            return None


@dataclass
class BrilliantDevice:
    """Normalized representation of one load/sensor peripheral on a Brilliant panel."""

    device_id: str
    peripheral_id: str
    name: str
    kind: DeviceKind
    peripheral_type: int = 0
    variables: dict[str, Variable] = field(default_factory=dict)

    # Deliberate asymmetry: is_on collapses an absent "on" variable to False (a load
    # without "on" is just off/uncontrollable), while motion_detected preserves None
    # (an absent sensor reading is unknown, not "no motion").
    @property
    def is_on(self) -> bool:
        """True iff the "on" variable is present and equals "1"."""
        on_var = self.variables.get("on")
        if on_var is None:
            return False
        return on_var.as_bool()

    @property
    def is_dimmable(self) -> bool:
        """True iff an "intensity" variable is present."""
        return "intensity" in self.variables

    @property
    def intensity(self) -> int | None:
        """Parsed "intensity" variable; None when absent or unparseable."""
        var = self.variables.get("intensity")
        if var is None:
            return None
        return var.as_int()

    @property
    def max_intensity(self) -> int:
        """Parsed "max_intensity_value"; defaults to 1000 when absent, unparseable, or <= 0.

        Downstream milestones divide by this value, so non-positive values fall back too.
        """
        var = self.variables.get("max_intensity_value")
        if var is None:
            return 1000
        parsed = var.as_int()
        return parsed if parsed is not None and parsed > 0 else 1000

    @property
    def motion_detected(self) -> bool | None:
        """ "movement_detected" variable as bool; None when the variable is absent."""
        var = self.variables.get("movement_detected")
        if var is None:
            return None
        return var.as_bool()

    @property
    def lux(self) -> float | None:
        """Parsed "lux" variable; None when absent or unparseable."""
        var = self.variables.get("lux")
        if var is None:
            return None
        return var.as_float()

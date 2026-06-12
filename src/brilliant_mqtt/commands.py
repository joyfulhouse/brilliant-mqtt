"""Translation of inbound Home Assistant MQTT commands into bus variable sets.

Pure module: no I/O, no MQTT, no Thrift imports.
Called by the bridge loop (Milestone 7) which issues the actual bus write.
"""

from __future__ import annotations

from dataclasses import dataclass

from brilliant_mqtt.model import BrilliantDevice, DeviceKind

# Kinds that accept write commands from HA.
_CONTROLLABLE_KINDS = {DeviceKind.LIGHT, DeviceKind.SWITCH}


@dataclass(frozen=True)
class VarSet:
    """A single bus variable write: name and its string-encoded value."""

    name: str
    value: str


def translate_command(device: BrilliantDevice, payload: dict[str, object]) -> list[VarSet]:
    """Translate an HA MQTT JSON payload into a list of bus variable writes.

    Rules:
    - Only LIGHT and SWITCH are controllable; any other kind returns [].
    - "state": "OFF"  → [VarSet("on", "0")] only (brightness ignored).
    - "state": "ON"   → VarSet("on", "1") prepended; then brightness if valid.
    - Missing / non-string / non-uppercase state → no "on" VarSet.
    - "brightness" is accepted only when is_dimmable and state != "OFF".
      Must be numeric (int or float, bool excluded). Clamped 0..255, then
      scaled to 0..device.max_intensity.
    - Brightness without a valid "ON" state still produces an intensity VarSet
      (defensive — HA itself always pairs brightness with state).
    - Other HA keys (e.g. "transition") are ignored; the bus has no equivalent.
    - Never raises on malformed payloads.
    """
    if device.kind not in _CONTROLLABLE_KINDS:
        return []

    result: list[VarSet] = []

    state = payload.get("state")

    if state == "OFF":
        return [VarSet("on", "0")]

    if state == "ON":
        result.append(VarSet("on", "1"))

    # Brightness: only when dimmable and state was not "OFF" (early return above).
    if device.is_dimmable:
        brightness = payload.get("brightness")
        # bool is a subclass of int in Python — explicitly exclude it.
        if isinstance(brightness, (int, float)) and not isinstance(brightness, bool):
            clamped = max(0.0, min(255.0, float(brightness)))
            intensity = round(clamped / 255.0 * device.max_intensity)
            result.append(VarSet("intensity", str(intensity)))

    return result


def translate_aux(
    payload: str,
    value_kind: str,
    invert: bool = False,
    min_value: float | None = None,
    max_value: float | None = None,
) -> str | None:
    """Translate an aux-entity command *payload* into a bus variable string value.

    Used for the per-variable command topics of switch / number / button
    entities (Milestone 10). Returns None when the payload is not understood;
    never raises.

    - ``value_kind == "bool"``: "ON" → "1", "OFF" → "0" (``invert`` swaps the
      pair). "PRESS" → "1" (buttons reuse the bool kind). Anything else → None.
    - ``value_kind == "int"``: parse an int (accepting float strings via
      ``int(float(...))``), clamp to ``min_value``/``max_value`` when given,
      return ``str(int)``; unparseable → None.
    - Any other ``value_kind`` → None.
    """
    if value_kind == "bool":
        if payload == "PRESS":
            return "1"
        if payload == "ON":
            return "0" if invert else "1"
        if payload == "OFF":
            return "1" if invert else "0"
        return None

    if value_kind == "int":
        try:
            value = int(float(payload))
        except (ValueError, TypeError):
            return None
        if min_value is not None:
            value = max(value, int(min_value))
        if max_value is not None:
            value = min(value, int(max_value))
        return str(value)

    return None

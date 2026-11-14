"""Virtual HMI / Detached Slider Controller for Brilliant panels and BLE mesh switches.

Provides full-resolution capacitive slider virtualization, multi-tap gesture
classification, and runtime mode switching (``virtual`` vs. ``passthrough``) for:

1. **Panel Faceplate Sliders** (``faceplate_peripheral`` -> ``slider_config:<idx>``)
2. **BLE Mesh Switches / Dimmers** (``ble_mesh`` -> ``switch_config:<mac>`` -> ``slider_config``)

Architecture
------------
- **Virtual Dimmer Hosting (0-255 + 5-Step + Swipe Gestures):**
  For each configured slider in ``VIRTUAL`` mode, the daemon hosts a local
  virtual peripheral (``vs_dimmer_<slider_id>``, ``peripheral_type=27``) on the
  panel's Thrift bus and binds the physical slider's ``CapTouchSliderConfig``
  Thrift struct to it. Before overwriting any ``CapTouchSliderConfig``, the daemon
  reads and persists the original Thrift payload so switching to
  ``PASSTHROUGH`` (or removing a slider from ``VIRTUAL_SLIDERS_JSON``) restores
  the exact original hardware binding.
- **Multi-Tap Classification (Single / Double / Triple / Quadruple / Quintuple):**
  Supports both:
  * **Hardware multi-tap scenes (0x0404 BLE / faceplate scene bindings):**
    Lossless listener on ``execution_peripheral`` -> ``last_executed_scene_id``
    seeded at startup so pre-existing scene timestamps never fire phantom taps.
  * **Virtual peripheral ``on`` toggle counting:**
    When a slider is bound solely to the virtual peripheral without separate
    scene bindings, consecutive ``on`` toggles within ``tap_window_s`` are
    classified into ``single_tap``, ``double_tap``, ``triple_tap``,
    ``quadruple_tap``, or ``quintuple_tap``.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import struct
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.desired_state import atomic_write_json
from brilliant_mqtt.discovery import availability_topic, device_block, state_topic
from brilliant_mqtt.model import BrilliantDevice
from brilliant_mqtt.protocols import (
    BusClient,
    MqttClient,
    VirtualPeripheralHostProtocol,
    VirtualPeripheralSpec,
)
from brilliant_mqtt.retained_topics import RetainedTopicLedger
from brilliant_mqtt.write_admission import Superseded

logger = logging.getLogger(__name__)

VIRTUAL_DIMMER_PERIPHERAL_TYPE = 27  # PeripheralType.LIGHT

SliderEventType = Literal[
    "single_tap",
    "double_tap",
    "triple_tap",
    "quadruple_tap",
    "quintuple_tap",
    "swipe_up",
    "swipe_down",
    "step_1",
    "step_2",
    "step_3",
    "step_4",
    "step_5",
    "brightness_change",
]

ALL_SLIDER_EVENT_TYPES: tuple[SliderEventType, ...] = (
    "single_tap",
    "double_tap",
    "triple_tap",
    "quadruple_tap",
    "quintuple_tap",
    "swipe_up",
    "swipe_down",
    "step_1",
    "step_2",
    "step_3",
    "step_4",
    "step_5",
    "brightness_change",
)

_TAP_COUNT_NAMES: dict[int, SliderEventType] = {
    1: "single_tap",
    2: "double_tap",
    3: "triple_tap",
    4: "quadruple_tap",
    5: "quintuple_tap",
}

_STEP_EVENT_NAMES: dict[int, SliderEventType] = {
    1: "step_1",
    2: "step_2",
    3: "step_3",
    4: "step_4",
    5: "step_5",
}


class SliderMode(str, Enum):
    """Operating mode of a physical capacitive slider."""

    VIRTUAL = "virtual"
    PASSTHROUGH = "passthrough"


# ---------------------------------------------------------------------------
# Brightness / raw intensity scaling helpers (single source of truth)
# ---------------------------------------------------------------------------


def raw_to_brightness(raw: int, min_raw: int = 100, max_raw: int = 1000) -> int:
    """Convert raw Brilliant intensity (``min_raw..max_raw``) to HA ``1..255``."""
    span = max(1, max_raw - min_raw)
    clamped = max(min_raw, min(max_raw, raw))
    scaled = round(((clamped - min_raw) / span) * 254) + 1
    return max(1, min(255, scaled))


def brightness_to_raw(brightness: int, min_raw: int = 100, max_raw: int = 1000) -> int:
    """Convert HA brightness (``1..255``) to raw Brilliant intensity."""
    span = max(1, max_raw - min_raw)
    clamped = max(1, min(255, brightness))
    raw = round(((clamped - 1) / 254) * span) + min_raw
    return max(min_raw, min(max_raw, raw))


# ---------------------------------------------------------------------------
# Pure Thrift TBinaryProtocol Encoder / Decoder for CapTouchSliderConfig
# ---------------------------------------------------------------------------

_T_STOP = 0
_T_BOOL = 2
_T_BYTE = 3
_T_DOUBLE = 4
_T_I16 = 6
_T_I32 = 8
_T_I64 = 10
_T_STRING = 11
_T_STRUCT = 12
_T_MAP = 13
_T_SET = 14
_T_LIST = 15

_MAX_SLIDER_INDEX = 255
_MAX_IDENTIFIER_BYTES = 1024
_MAX_ENCODED_BYTES = 64 * 1024


def encode_slider_config(
    device_id: str,
    peripheral_id: str,
    *,
    slider_index: int = 0,
    double_tap_scene_id: str = "",
) -> str:
    """Build a base64-encoded Thrift ``TBinaryProtocol`` ``CapTouchSliderConfig`` string.

    Wire layout verified against the panel's ``CapTouchSliderConfig`` Thrift struct
    and ``tools/brilliant_vc/slider_binding.py``:
    - field 1 (``i32``): ``slider_index`` (0..255)
    - field 2 (``string``): primary target ``device_id``
    - field 3 (``string``): primary target ``peripheral_id``
    - field 5 (``string``, optional): ``double_tap_scene_id``
    - field 6 (``i32``): ``action`` = 0 (``LIGHTS``)
    - field 7 (``struct`` -> field 1 ``list<struct>`` of length 1): ``devices``
    """
    if not device_id and not peripheral_id:
        return ""
    if not device_id or not peripheral_id:
        raise ValueError("device_id and peripheral_id must both be non-empty")
    if not 0 <= slider_index <= _MAX_SLIDER_INDEX:
        raise ValueError(f"slider_index must be 0..{_MAX_SLIDER_INDEX}, got {slider_index}")

    dev_bytes = device_id.encode("utf-8")
    per_bytes = peripheral_id.encode("utf-8")
    buf = bytearray()
    # Field 1 (i32): slider_index
    buf += b"\x08\x00\x01" + slider_index.to_bytes(4, "big", signed=True)
    # Field 2 (string): primary target device_id
    buf += b"\x0b\x00\x02" + len(dev_bytes).to_bytes(4, "big") + dev_bytes
    # Field 3 (string): primary target peripheral_id
    buf += b"\x0b\x00\x03" + len(per_bytes).to_bytes(4, "big") + per_bytes
    # Field 5 (string, optional): double_tap_scene_id
    if double_tap_scene_id:
        dt_bytes = double_tap_scene_id.encode("utf-8")
        buf += b"\x0b\x00\x05" + len(dt_bytes).to_bytes(4, "big") + dt_bytes
    # Field 6 (i32): action = 0 (LIGHTS)
    buf += b"\x08\x00\x06\x00\x00\x00\x00"
    # Field 7 (struct -> field 1 list<struct> of length 1): devices = [(device_id, peripheral_id)]
    buf += b"\x0c\x00\x07\x0f\x00\x01\x0c\x00\x00\x00\x01"
    buf += b"\x0b\x00\x01" + len(dev_bytes).to_bytes(4, "big") + dev_bytes
    buf += b"\x0b\x00\x02" + len(per_bytes).to_bytes(4, "big") + per_bytes
    # Stop bytes for inner struct, outer struct, and root CapTouchSliderConfig
    buf += b"\x00\x00\x00"
    return base64.b64encode(bytes(buf)).decode("ascii")


def decode_slider_config_target(payload_b64: str) -> tuple[str, str] | None:
    """Decode ``(device_id, peripheral_id)`` from a base64 Thrift ``CapTouchSliderConfig``.

    Returns ``None`` if the payload is empty, malformed, or lacks required target fields.
    """
    if not isinstance(payload_b64, str) or not payload_b64 or len(payload_b64) > _MAX_ENCODED_BYTES:
        return None
    try:
        raw = base64.b64decode(payload_b64, validate=True)
    except (binascii.Error, ValueError):
        return None
    if not raw or len(raw) > _MAX_ENCODED_BYTES:
        return None

    offset = 0

    def _take(n: int) -> bytes:
        nonlocal offset
        if n < 0 or offset + n > len(raw):
            raise ValueError("truncated slider_config")
        chunk = raw[offset : offset + n]
        offset += n
        return chunk

    def _read_i32() -> int:
        return int(struct.unpack(">i", _take(4))[0])

    def _read_identifier() -> str:
        length = _read_i32()
        if not 0 < length <= _MAX_IDENTIFIER_BYTES:
            raise ValueError("invalid identifier length")
        val = _take(length).decode("utf-8")
        if any(ord(ch) < 32 or ord(ch) == 127 for ch in val):
            raise ValueError("control character in identifier")
        return val

    def _read_count() -> int:
        count = _read_i32()
        if not 0 <= count <= 4096:
            raise ValueError("invalid collection count")
        return count

    def _skip(ftype: int, depth: int = 0) -> None:
        if depth > 16:
            raise ValueError("nesting too deep")
        if ftype in (_T_BOOL, _T_BYTE):
            _take(1)
        elif ftype == _T_I16:
            _take(2)
        elif ftype == _T_I32:
            _take(4)
        elif ftype in (_T_DOUBLE, _T_I64):
            _take(8)
        elif ftype == _T_STRING:
            _take(_read_count())
        elif ftype == _T_STRUCT:
            while True:
                nested_type = int(struct.unpack(">b", _take(1))[0])
                if nested_type == _T_STOP:
                    break
                _take(2)
                _skip(nested_type, depth + 1)
        elif ftype == _T_MAP:
            ktype = int(struct.unpack(">b", _take(1))[0])
            vtype = int(struct.unpack(">b", _take(1))[0])
            for _ in range(_read_count()):
                _skip(ktype, depth + 1)
                _skip(vtype, depth + 1)
        elif ftype in (_T_SET, _T_LIST):
            etype = int(struct.unpack(">b", _take(1))[0])
            for _ in range(_read_count()):
                _skip(etype, depth + 1)
        else:
            raise ValueError(f"unknown Thrift field type {ftype}")

    try:
        seen_fields: set[int] = set()
        slider_index: int | None = None
        device_id: str | None = None
        peripheral_id: str | None = None

        while True:
            field_type = int(struct.unpack(">b", _take(1))[0])
            if field_type == _T_STOP:
                break
            field_id = int(struct.unpack(">h", _take(2))[0])
            if field_id in seen_fields:
                return None
            seen_fields.add(field_id)
            if field_id == 1:
                if field_type != _T_I32:
                    return None
                slider_index = _read_i32()
            elif field_id == 2:
                if field_type != _T_STRING:
                    return None
                device_id = _read_identifier()
            elif field_id == 3:
                if field_type != _T_STRING:
                    return None
                peripheral_id = _read_identifier()
            elif field_id == 6:
                if field_type != _T_I32:
                    return None
                _read_i32()
            else:
                _skip(field_type, 0)

        if offset != len(raw):
            return None
        if slider_index is None or device_id is None or peripheral_id is None:
            return None
        if not 0 <= slider_index <= _MAX_SLIDER_INDEX:
            return None
        return (device_id, peripheral_id)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Configuration & Runtime State Models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VirtualSliderSpec:
    """Configuration for a single physical capacitive slider."""

    slider_id: str
    name: str
    config_device_id: str
    config_peripheral_id: str
    config_var_name: str
    slider_index: int = 0
    load_device_id: str = ""
    load_peripheral_id: str = ""
    min_raw: int = 100
    max_raw: int = 1000
    num_steps: int = 5
    step_delta_threshold: int = 40
    swipe_delta_threshold: int = 150
    tap_window_s: float = 0.45
    settle_window_s: float = 0.85
    default_mode: SliderMode = SliderMode.VIRTUAL
    scene_single_tap: str = ""
    scene_double_tap: str = ""

    @property
    def virtual_peripheral_id(self) -> str:
        """The peripheral_id of the hosted virtual dimmer for this slider."""
        return f"vs_dimmer_{self.slider_id}"

    def virtual_slider_config_b64(self, host_device_id: str) -> str:
        """Return base64 ``CapTouchSliderConfig`` pointing at the virtual peripheral."""
        return encode_slider_config(
            host_device_id,
            self.virtual_peripheral_id,
            slider_index=self.slider_index,
            double_tap_scene_id=self.scene_double_tap,
        )

    def passthrough_slider_config_b64(self, default_device_id: str = "") -> str:
        """Return base64 ``CapTouchSliderConfig`` pointing at the fallback load."""
        if not self.load_peripheral_id:
            return ""
        dev_id = self.load_device_id or default_device_id
        return encode_slider_config(
            dev_id,
            self.load_peripheral_id,
            slider_index=self.slider_index,
            double_tap_scene_id=self.scene_double_tap,
        )

    def raw_to_brightness(self, raw: int) -> int:
        """Convert raw Brilliant intensity to HA brightness (1-255)."""
        return raw_to_brightness(raw, self.min_raw, self.max_raw)

    def brightness_to_raw(self, brightness: int) -> int:
        """Convert HA brightness (1-255) to raw Brilliant intensity."""
        return brightness_to_raw(brightness, self.min_raw, self.max_raw)

    @classmethod
    def for_ble_switch(
        cls,
        slider_id: str,
        name: str,
        switch_id: str,
        *,
        load_peripheral_id: str = "",
        default_mode: SliderMode = SliderMode.VIRTUAL,
        scene_single_tap: str = "",
        scene_double_tap: str = "",
        min_raw: int = 100,
        max_raw: int = 1000,
        num_steps: int = 5,
        step_delta_threshold: int = 40,
        swipe_delta_threshold: int = 150,
        tap_window_s: float = 0.45,
        settle_window_s: float = 0.85,
    ) -> VirtualSliderSpec:
        """Build a :class:`VirtualSliderSpec` for a BLE mesh switch (``switch_config:<id>``)."""
        cfg_periph = (
            switch_id if switch_id.startswith("switch_config:") else f"switch_config:{switch_id}"
        )
        return cls(
            slider_id=slider_id,
            name=name,
            config_device_id="ble_mesh",
            config_peripheral_id=cfg_periph,
            config_var_name="slider_config",
            load_device_id="ble_mesh" if load_peripheral_id else "",
            load_peripheral_id=load_peripheral_id,
            min_raw=min_raw,
            max_raw=max_raw,
            num_steps=num_steps,
            step_delta_threshold=step_delta_threshold,
            swipe_delta_threshold=swipe_delta_threshold,
            tap_window_s=tap_window_s,
            settle_window_s=settle_window_s,
            default_mode=default_mode,
            scene_single_tap=scene_single_tap,
            scene_double_tap=scene_double_tap,
        )

    @classmethod
    def for_panel_slider(
        cls,
        slider_id: str,
        name: str,
        panel_device_id: str,
        slider_index: int,
        *,
        load_device_id: str = "",
        load_peripheral_id: str = "",
        default_mode: SliderMode = SliderMode.VIRTUAL,
        scene_single_tap: str = "",
        scene_double_tap: str = "",
        min_raw: int = 100,
        max_raw: int = 1000,
        num_steps: int = 5,
        step_delta_threshold: int = 40,
        swipe_delta_threshold: int = 150,
        tap_window_s: float = 0.45,
        settle_window_s: float = 0.85,
    ) -> VirtualSliderSpec:
        """Build a :class:`VirtualSliderSpec` for a wired panel faceplate slider."""
        if slider_index < 0 or slider_index > 3:
            raise ValueError(f"slider_index must be 0..3, got {slider_index}")
        return cls(
            slider_id=slider_id,
            name=name,
            config_device_id=panel_device_id,
            config_peripheral_id="faceplate_peripheral",
            config_var_name=f"slider_config:{slider_index}",
            slider_index=slider_index,
            load_device_id=load_device_id or (panel_device_id if load_peripheral_id else ""),
            load_peripheral_id=load_peripheral_id,
            min_raw=min_raw,
            max_raw=max_raw,
            num_steps=num_steps,
            step_delta_threshold=step_delta_threshold,
            swipe_delta_threshold=swipe_delta_threshold,
            tap_window_s=tap_window_s,
            settle_window_s=settle_window_s,
            default_mode=default_mode,
            scene_single_tap=scene_single_tap,
            scene_double_tap=scene_double_tap,
        )


@dataclass
class VirtualSliderState:
    """Mutable runtime state for a single virtual slider."""

    mode: SliderMode = SliderMode.VIRTUAL
    brightness: int = 255
    raw_intensity: int = 1000
    step: int = 5
    is_on: bool = True
    last_event: SliderEventType | None = None
    event_seq: int = 0
    original_slider_config: str | None = None


@dataclass(frozen=True)
class OrphanedSliderBinding:
    """Saved original slider binding for a slider removed from configuration."""

    config_device_id: str
    config_peripheral_id: str
    config_var_name: str
    original_slider_config: str


# ---------------------------------------------------------------------------
# Per-Slider Gesture & Multi-Tap Classifier
# ---------------------------------------------------------------------------


class _SliderClassifier:
    """Classifies raw peripheral updates and scene executions into HA state + events."""

    def __init__(
        self,
        spec: VirtualSliderSpec,
        state: VirtualSliderState,
        on_classified: Callable[[SliderEventType | None], Awaitable[None]],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._spec = spec
        self._state = state
        self._on_classified = on_classified
        self._clock = clock

        self._tap_count = 0
        self._tap_timer: asyncio.Task[None] | None = None
        self._intensity_timer: asyncio.Task[None] | None = None
        self._last_Tap_ts: float = 0.0
        self._last_intensity_ts: float = 0.0
        self._baseline_raw: int = state.raw_intensity

    def cancel(self) -> None:
        """Cancel any pending tap-window or intensity settle timer."""
        if self._tap_timer is not None and not self._tap_timer.done():
            self._tap_timer.cancel()
        self._tap_timer = None
        if self._intensity_timer is not None and not self._intensity_timer.done():
            self._intensity_timer.cancel()
        self._intensity_timer = None

    async def handle_virtual_var_set(self, var_name: str, value: Any) -> None:
        """Handle an external set on the hosted virtual peripheral (``on`` or ``intensity``)."""
        now = self._clock()
        if var_name == "intensity":
            try:
                raw_val = int(value)
            except (TypeError, ValueError):
                return
            await self._process_intensity(raw_val, now)
        elif var_name == "on":
            try:
                on_val = int(value)
            except (TypeError, ValueError):
                return
            self._state.is_on = bool(on_val)
            if not self._spec.scene_single_tap and not self._spec.scene_double_tap:
                self._record_tap(now)

    async def handle_scene_executed(self, scene_id: str) -> None:
        """Handle a ``last_executed_scene_id`` bus notification matching this slider."""
        now = self._clock()
        if scene_id == self._spec.scene_single_tap and self._spec.scene_single_tap:
            self._record_tap(now, count_increment=1)
        elif scene_id == self._spec.scene_double_tap and self._spec.scene_double_tap:
            self._record_tap(now, count_increment=2)

    def _record_tap(self, now: float, count_increment: int = 1) -> None:
        if now - self._last_Tap_ts > self._spec.tap_window_s * 1.5:
            self._tap_count = 0
        self._tap_count = min(5, self._tap_count + count_increment)
        self._last_Tap_ts = now

        if self._tap_timer is not None and not self._tap_timer.done():
            self._tap_timer.cancel()
        self._tap_timer = asyncio.create_task(self._finalize_tap())

    async def _finalize_tap(self) -> None:
        try:
            await asyncio.sleep(self._spec.tap_window_s)
        except asyncio.CancelledError:
            return
        count = self._tap_count
        self._tap_count = 0
        if count <= 0:
            return
        ev = _TAP_COUNT_NAMES.get(min(5, count), "single_tap")
        self._state.last_event = ev
        self._state.event_seq += 1
        await self._on_classified(ev)

    async def _process_intensity(self, raw_val: int, now: float) -> None:
        reset_gap = max(0.35, self._spec.settle_window_s * 1.5)
        if (
            self._intensity_timer is None or self._intensity_timer.done()
        ) and now - self._last_intensity_ts > reset_gap:
            self._baseline_raw = self._state.raw_intensity
        self._last_intensity_ts = now

        self._state.raw_intensity = raw_val
        brightness = self._spec.raw_to_brightness(raw_val)
        self._state.brightness = brightness

        step = max(
            1,
            min(
                self._spec.num_steps,
                round((brightness / 255.0) * self._spec.num_steps),
            ),
        )
        self._state.step = step

        if self._spec.settle_window_s <= 0:
            await self._emit_locked_intensity()
            return

        if self._intensity_timer is not None and not self._intensity_timer.done():
            self._intensity_timer.cancel()
        self._intensity_timer = asyncio.create_task(self._finalize_intensity())

    async def _finalize_intensity(self) -> None:
        try:
            await asyncio.sleep(self._spec.settle_window_s)
        except asyncio.CancelledError:
            return
        await self._emit_locked_intensity()

    async def _emit_locked_intensity(self) -> None:
        raw_val = self._state.raw_intensity
        step = self._state.step
        delta = raw_val - self._baseline_raw

        event: SliderEventType = "brightness_change"
        if abs(delta) >= self._spec.swipe_delta_threshold:
            event = "swipe_up" if delta > 0 else "swipe_down"
            self._baseline_raw = raw_val
        elif abs(delta) >= self._spec.step_delta_threshold:
            event = _STEP_EVENT_NAMES.get(step, "brightness_change")
            self._baseline_raw = raw_val

        self._state.last_event = event
        self._state.event_seq += 1
        await self._on_classified(event)


# ---------------------------------------------------------------------------
# Virtual Slider Controller (Orchestrator)
# ---------------------------------------------------------------------------

VirtualHostFactory = Callable[
    [VirtualPeripheralSpec, Callable[[str, Any], Awaitable[None]]],
    VirtualPeripheralHostProtocol,
]


class VirtualSliderController:
    """Manages virtual peripherals, slider bindings, gesture classification, and MQTT entities."""

    def __init__(
        self,
        bus: BusClient,
        mqtt: MqttClient,
        panel: str,
        own_device_id: str,
        specs: list[VirtualSliderSpec],
        host_factory: VirtualHostFactory,
        state_path: Path | None = None,
        clock: Callable[[], float] = time.monotonic,
        *,
        owned_topics: RetainedTopicLedger | None = None,
        want_device: Callable[[str], bool] | None = None,
        is_mesh_leader: Callable[[], bool] = lambda: True,
    ) -> None:
        self._bus = bus
        self._mqtt = mqtt
        self._panel = panel
        self._own_device_id = own_device_id
        self._specs: dict[str, VirtualSliderSpec] = {s.slider_id: s for s in specs}
        self._host_factory = host_factory
        self._state_path = state_path
        self._clock = clock
        self._owned_topics = owned_topics
        self._want_device = want_device
        self._is_mesh_leader = is_mesh_leader

        self._states: dict[str, VirtualSliderState] = {}
        self._orphaned_bindings: dict[str, OrphanedSliderBinding] = {}
        self._hosts: dict[str, VirtualPeripheralHostProtocol] = {}
        self._classifiers: dict[str, _SliderClassifier] = {}
        self._last_exec_scene_ts: int | None = None
        self._callbacks_registered = False
        self._last_persisted_snapshot: dict[str, Any] | None = None

        for s in specs:
            self._states[s.slider_id] = VirtualSliderState(
                mode=s.default_mode,
                brightness=255,
                raw_intensity=s.max_raw,
                step=s.num_steps,
                is_on=True,
            )
        self._load_state()

    def state_for(self, slider_id: str) -> VirtualSliderState:
        """Return current state for *slider_id*."""
        return self._states[slider_id]

    def _register_callbacks(self) -> None:
        if self._callbacks_registered:
            return
        self._bus.on_change(
            self.on_bus_device_change,
            coalesce_pushes=False,
            want_device=self._want_device,
        )
        self._mqtt.on_command(self.on_mqtt_command)
        self._callbacks_registered = True

    # ------------------------------------------------------------------
    # Persistence (atomic + changed-only)
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        if self._state_path is None or not self._state_path.exists():
            return
        try:
            data = json.loads(self._state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to load virtual slider state from %s: %s", self._state_path, exc)
            return
        if not isinstance(data, dict):
            return
        self._last_persisted_snapshot = data
        for sid, entry in data.items():
            if not isinstance(entry, dict):
                continue
            orig_cfg = entry.get("original_slider_config")
            orig_str = str(orig_cfg) if isinstance(orig_cfg, str) and orig_cfg else None
            if sid not in self._states:
                cfg_dev = str(entry.get("config_device_id", ""))
                cfg_periph = str(entry.get("config_peripheral_id", ""))
                cfg_var = str(entry.get("config_var_name", ""))
                if orig_str and cfg_dev and cfg_periph and cfg_var:
                    self._orphaned_bindings[str(sid)] = OrphanedSliderBinding(
                        config_device_id=cfg_dev,
                        config_peripheral_id=cfg_periph,
                        config_var_name=cfg_var,
                        original_slider_config=orig_str,
                    )
                continue
            st = self._states[sid]
            spec = self._specs[sid]
            mode_str = entry.get("mode")
            if mode_str in (SliderMode.VIRTUAL.value, SliderMode.PASSTHROUGH.value):
                st.mode = SliderMode(mode_str)
            if isinstance(entry.get("brightness"), int):
                st.brightness = max(1, min(255, int(entry["brightness"])))
                st.raw_intensity = spec.brightness_to_raw(st.brightness)
            if isinstance(entry.get("step"), int):
                st.step = max(1, min(spec.num_steps, int(entry["step"])))
            if isinstance(entry.get("is_on"), bool):
                st.is_on = bool(entry["is_on"])
            if orig_str is not None:
                st.original_slider_config = orig_str

    def _build_persisted_snapshot(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        for sid, st in self._states.items():
            spec = self._specs[sid]
            data[sid] = {
                "mode": st.mode.value,
                "brightness": st.brightness,
                "step": st.step,
                "is_on": st.is_on,
                "original_slider_config": st.original_slider_config,
                "config_device_id": spec.config_device_id,
                "config_peripheral_id": spec.config_peripheral_id,
                "config_var_name": spec.config_var_name,
            }
        for sid, orphan in self._orphaned_bindings.items():
            data[sid] = {
                "original_slider_config": orphan.original_slider_config,
                "config_device_id": orphan.config_device_id,
                "config_peripheral_id": orphan.config_peripheral_id,
                "config_var_name": orphan.config_var_name,
            }
        return data

    def _save_state_if_changed(self) -> None:
        if self._state_path is None:
            return
        snapshot = self._build_persisted_snapshot()
        if snapshot == self._last_persisted_snapshot:
            return
        try:
            atomic_write_json(self._state_path, snapshot)
            self._last_persisted_snapshot = snapshot
        except Exception as exc:
            logger.warning("Failed to save virtual slider state to %s: %s", self._state_path, exc)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _seed_execution_timestamp(self) -> None:
        """Seed ``_last_exec_scene_ts`` from the bus so startup state does not replay."""
        for dev_id in (self._own_device_id, "configuration_virtual_device"):
            if not dev_id:
                continue
            try:
                exec_dev = await self._bus.get_peripheral(dev_id, "execution_peripheral")
            except Exception:
                continue
            if exec_dev is not None and "last_executed_scene_id" in exec_dev.variables:
                self._last_exec_scene_ts = exec_dev.variables["last_executed_scene_id"].timestamp_ms
                return

    async def _restore_orphaned_bindings(self) -> None:
        """Restore original ``CapTouchSliderConfig`` for sliders removed from config."""
        for sid, orphan in list(self._orphaned_bindings.items()):
            if orphan.config_device_id == "ble_mesh" and not self._is_mesh_leader():
                continue
            try:
                result = await self._bus.set_variables(
                    orphan.config_device_id,
                    orphan.config_peripheral_id,
                    [VarSet(orphan.config_var_name, orphan.original_slider_config)],
                )
            except Exception:
                logger.exception("Failed to restore orphaned slider binding for %s", sid)
                continue
            if not isinstance(result, Superseded):
                self._orphaned_bindings.pop(sid, None)
                logger.info(
                    "Restored original slider_config for removed virtual slider %s on %s/%s",
                    sid,
                    orphan.config_device_id,
                    orphan.config_peripheral_id,
                )
        self._save_state_if_changed()

    async def start(self) -> None:
        """Host virtual peripherals, seed scene watermark, bind sliders, and publish discovery."""
        self._register_callbacks()
        await self._seed_execution_timestamp()
        await self._restore_orphaned_bindings()

        for sid, spec in self._specs.items():
            st = self._states[sid]

            async def _on_classified(event: SliderEventType | None, _sid: str = sid) -> None:
                self._save_state_if_changed()
                await self._publish_state(_sid, event=event)

            classifier = _SliderClassifier(
                spec=spec,
                state=st,
                on_classified=_on_classified,
                clock=self._clock,
            )
            self._classifiers[sid] = classifier

            vspec = VirtualPeripheralSpec(
                peripheral_id=spec.virtual_peripheral_id,
                display_name=spec.name,
                peripheral_type=VIRTUAL_DIMMER_PERIPHERAL_TYPE,
                initial_variables={
                    "on": 1 if st.is_on else 0,
                    "intensity": st.raw_intensity,
                    "min_intensity_value": spec.min_raw,
                    "max_intensity_value": spec.max_raw,
                },
                settable_variables=frozenset({"on", "intensity"}),
            )

            async def _on_ext_set(
                var_name: str, val: Any, _clf: _SliderClassifier = classifier
            ) -> None:
                await _clf.handle_virtual_var_set(var_name, val)

            host = self._host_factory(vspec, _on_ext_set)
            await host.start()
            self._hosts[sid] = host

            await self._apply_slider_binding(spec, st.mode)
            self._save_state_if_changed()

            await self._publish_discovery(spec)
            await self._subscribe_commands(spec)
            await self._publish_state(sid, event=None)

    async def on_mesh_leadership_acquired(self) -> None:
        """Apply deferred ``ble_mesh`` slider bindings once mesh leadership is won."""
        await self._restore_orphaned_bindings()
        for sid, spec in self._specs.items():
            if spec.config_device_id != "ble_mesh":
                continue
            st = self._states[sid]
            await self._apply_slider_binding(spec, st.mode)
        self._save_state_if_changed()

    async def shutdown(self) -> None:
        """Cancel timers and shut down hosted virtual peripherals."""
        for clf in self._classifiers.values():
            clf.cancel()
        self._classifiers.clear()
        for host in self._hosts.values():
            try:
                await host.shutdown()
            except Exception:
                logger.exception("Error shutting down virtual peripheral host")
        self._hosts.clear()

    # ------------------------------------------------------------------
    # Bus & MQTT Routing
    # ------------------------------------------------------------------

    async def _apply_slider_binding(self, spec: VirtualSliderSpec, mode: SliderMode) -> bool:
        """Write ``CapTouchSliderConfig`` to the target peripheral after backing up original."""
        if spec.config_device_id == "ble_mesh" and not self._is_mesh_leader():
            logger.debug(
                "Deferring ble_mesh slider_config binding for %s until mesh leader",
                spec.slider_id,
            )
            return False

        st = self._states[spec.slider_id]
        current_b64: str | None = None
        try:
            current_dev = await self._bus.get_peripheral(
                spec.config_device_id, spec.config_peripheral_id
            )
        except Exception:
            current_dev = None

        if current_dev is not None and spec.config_var_name in current_dev.variables:
            current_b64 = current_dev.variables[spec.config_var_name].value

        decoded: tuple[str, str] | None = None
        if current_b64:
            decoded = decode_slider_config_target(current_b64)
            if decoded is not None:
                curr_dev_id, curr_periph_id = decoded
                if (curr_dev_id, curr_periph_id) != (
                    self._own_device_id,
                    spec.virtual_peripheral_id,
                ) and curr_periph_id != "":
                    st.original_slider_config = current_b64
                    self._save_state_if_changed()

        if mode is SliderMode.VIRTUAL:
            if not st.original_slider_config:
                if decoded is not None and spec.load_peripheral_id:
                    st.original_slider_config = spec.passthrough_slider_config_b64(
                        self._own_device_id
                    )
                    self._save_state_if_changed()
                else:
                    logger.warning(
                        "Refusing to overwrite %s on %s/%s for slider %s in virtual mode: "
                        "no original_slider_config backup and bus value could not be decoded",
                        spec.config_var_name,
                        spec.config_device_id,
                        spec.config_peripheral_id,
                        spec.slider_id,
                    )
                    return False
            target_b64 = spec.virtual_slider_config_b64(self._own_device_id)
        else:
            if st.original_slider_config:
                target_b64 = st.original_slider_config
            elif spec.load_peripheral_id:
                target_b64 = spec.passthrough_slider_config_b64(self._own_device_id)
            else:
                logger.warning(
                    "Cannot switch virtual slider %s to passthrough: no original_slider_config "
                    "captured and no load_peripheral_id configured",
                    spec.slider_id,
                )
                return False

        if current_b64 is not None and current_b64 == target_b64:
            return True

        try:
            result = await self._bus.set_variables(
                spec.config_device_id,
                spec.config_peripheral_id,
                [VarSet(spec.config_var_name, target_b64)],
            )
        except Exception:
            logger.exception(
                "Failed to write %s on %s/%s for slider %s",
                spec.config_var_name,
                spec.config_device_id,
                spec.config_peripheral_id,
                spec.slider_id,
            )
            return False

        if isinstance(result, Superseded):
            logger.warning(
                "slider_config write superseded on %s/%s for slider %s",
                spec.config_device_id,
                spec.config_peripheral_id,
                spec.slider_id,
            )
            return False

        try:
            verify_dev = await self._bus.get_peripheral(
                spec.config_device_id, spec.config_peripheral_id
            )
        except Exception:
            verify_dev = None

        if verify_dev is not None and spec.config_var_name in verify_dev.variables:
            observed_b64 = verify_dev.variables[spec.config_var_name].value
            if observed_b64 != target_b64:
                logger.warning(
                    "slider_config read-back verification mismatch on %s/%s for slider %s",
                    spec.config_device_id,
                    spec.config_peripheral_id,
                    spec.slider_id,
                )
                return False

        logger.info(
            "Bound slider %s (%s/%s %s) -> mode=%s",
            spec.slider_id,
            spec.config_device_id,
            spec.config_peripheral_id,
            spec.config_var_name,
            mode.value,
        )
        return True

    async def on_bus_device_change(self, device: BrilliantDevice) -> None:
        """Inspect bus notifications for ``execution_peripheral`` scene triggers."""
        if device.peripheral_id != "execution_peripheral":
            return
        var = device.variables.get("last_executed_scene_id")
        if var is None or not var.value:
            return
        if var.timestamp_ms is None or var.timestamp_ms == self._last_exec_scene_ts:
            return
        self._last_exec_scene_ts = var.timestamp_ms

        scene_id = var.value
        for sid, spec in self._specs.items():
            if scene_id in (spec.scene_single_tap, spec.scene_double_tap) and scene_id:
                clf = self._classifiers.get(sid)
                if clf is not None:
                    await clf.handle_scene_executed(scene_id)

    async def on_mqtt_command(self, topic: str, payload: str) -> None:
        """Handle inbound MQTT commands for virtual slider entities."""
        prefix = f"brilliant/{self._panel}/virtual_slider/"
        if not topic.startswith(prefix):
            return
        rest = topic[len(prefix) :]
        parts = rest.split("/")
        if len(parts) != 2:
            return
        slider_id, cmd = parts[0], parts[1]
        if slider_id not in self._specs:
            return

        if cmd == "set_mode":
            await self._handle_mode_command(slider_id, payload.strip().lower())
        elif cmd == "set_brightness":
            await self._handle_brightness_command(slider_id, payload.strip())
        elif cmd == "set_light":
            await self._handle_light_command(slider_id, payload.strip())

    async def _handle_mode_command(self, slider_id: str, mode_str: str) -> None:
        if mode_str not in (SliderMode.VIRTUAL.value, SliderMode.PASSTHROUGH.value):
            logger.warning("Ignoring invalid slider mode %r for %s", mode_str, slider_id)
            return
        new_mode = SliderMode(mode_str)
        st = self._states[slider_id]
        spec = self._specs[slider_id]
        if not await self._apply_slider_binding(spec, new_mode):
            return
        st.mode = new_mode
        self._save_state_if_changed()
        await self._publish_state(slider_id, event=None)

    async def _handle_brightness_command(self, slider_id: str, payload: str) -> None:
        try:
            val = round(float(payload))
        except ValueError:
            return
        await self._set_virtual_brightness(slider_id, max(1, min(255, val)), is_on=True)

    async def _handle_light_command(self, slider_id: str, payload: str) -> None:
        try:
            data = json.loads(payload)
        except ValueError:
            return
        if not isinstance(data, dict):
            return
        st = self._states[slider_id]
        is_on = st.is_on
        if "state" in data:
            is_on = str(data["state"]).upper() == "ON"
        brightness = st.brightness
        if "brightness" in data:
            try:
                brightness = max(1, min(255, round(float(data["brightness"]))))
            except (TypeError, ValueError):
                pass
        await self._set_virtual_brightness(slider_id, brightness, is_on=is_on)

    async def _set_virtual_brightness(
        self, slider_id: str, brightness: int, *, is_on: bool
    ) -> None:
        spec = self._specs[slider_id]
        st = self._states[slider_id]
        st.is_on = is_on
        st.brightness = brightness
        st.raw_intensity = spec.brightness_to_raw(brightness)
        st.step = max(
            1,
            min(spec.num_steps, round((brightness / 255.0) * spec.num_steps)),
        )
        host = self._hosts.get(slider_id)
        if host is not None:
            await host.set_variable("on", 1 if is_on else 0)
            await host.set_variable("intensity", st.raw_intensity)
        self._save_state_if_changed()
        await self._publish_state(slider_id, event=None)

    # ------------------------------------------------------------------
    # MQTT Discovery & State Publishing
    # ------------------------------------------------------------------

    @staticmethod
    def _peripheral_slug(slider_id: str) -> str:
        return f"virtual_slider_{slider_id}"

    def _state_topic(self, slider_id: str) -> str:
        return state_topic(self._panel, self._peripheral_slug(slider_id))

    def _event_topic(self, slider_id: str) -> str:
        return f"brilliant/{self._panel}/virtual_slider/{slider_id}/event"

    def _mode_cmd_topic(self, slider_id: str) -> str:
        return f"brilliant/{self._panel}/virtual_slider/{slider_id}/set_mode"

    def _brightness_cmd_topic(self, slider_id: str) -> str:
        return f"brilliant/{self._panel}/virtual_slider/{slider_id}/set_brightness"

    def _light_cmd_topic(self, slider_id: str) -> str:
        return f"brilliant/{self._panel}/virtual_slider/{slider_id}/set_light"

    async def _publish_retained(self, topic: str, payload: str) -> None:
        if self._owned_topics is not None:
            await self._owned_topics.async_publish(self._mqtt, topic, payload)
        else:
            await self._mqtt.publish(topic, payload, retain=True)

    async def _subscribe_commands(self, spec: VirtualSliderSpec) -> None:
        sid = spec.slider_id
        await self._mqtt.subscribe(self._mode_cmd_topic(sid))
        await self._mqtt.subscribe(self._brightness_cmd_topic(sid))
        await self._mqtt.subscribe(self._light_cmd_topic(sid))

    async def _publish_discovery(self, spec: VirtualSliderSpec) -> None:
        sid = spec.slider_id
        avail = availability_topic(self._panel)
        st_topic = self._state_topic(sid)
        ev_topic = self._event_topic(sid)
        dev_block = device_block(self._panel)
        uid_base = f"brilliant_{self._panel}_vs_{sid}"

        # 1. Select entity: Mode (virtual vs passthrough)
        select_uid = f"{uid_base}_mode"
        select_cfg = {
            "availability": [{"topic": avail}],
            "command_topic": self._mode_cmd_topic(sid),
            "device": dev_block,
            "entity_category": "config",
            "icon": "mdi:tune-vertical",
            "name": f"{spec.name} Slider Mode",
            "options": [SliderMode.VIRTUAL.value, SliderMode.PASSTHROUGH.value],
            "state_topic": st_topic,
            "unique_id": select_uid,
            "value_template": "{{ value_json.mode }}",
        }
        await self._publish_retained(
            f"homeassistant/select/{select_uid}/config",
            json.dumps(select_cfg, sort_keys=True),
        )

        # 2. Number entity: Virtual Brightness (1-255)
        number_uid = f"{uid_base}_brightness"
        number_cfg = {
            "availability": [{"topic": avail}],
            "command_topic": self._brightness_cmd_topic(sid),
            "device": dev_block,
            "icon": "mdi:brightness-6",
            "max": 255,
            "min": 1,
            "mode": "slider",
            "name": f"{spec.name} Virtual Slider Brightness",
            "state_topic": st_topic,
            "step": 1,
            "unique_id": number_uid,
            "value_template": "{{ value_json.brightness }}",
        }
        await self._publish_retained(
            f"homeassistant/number/{number_uid}/config",
            json.dumps(number_cfg, sort_keys=True),
        )

        # 3. Light entity: Virtual Dimmer Light (JSON schema)
        light_uid = f"{uid_base}_light"
        light_cfg = {
            "availability": [{"topic": avail}],
            "brightness": True,
            "command_topic": self._light_cmd_topic(sid),
            "device": dev_block,
            "icon": "mdi:wall-sconce-flat",
            "name": f"{spec.name} Virtual Light",
            "schema": "json",
            "state_topic": st_topic,
            "unique_id": light_uid,
        }
        await self._publish_retained(
            f"homeassistant/light/{light_uid}/config",
            json.dumps(light_cfg, sort_keys=True),
        )

        # 4. Sensor entity: Step (1..num_steps)
        step_uid = f"{uid_base}_step"
        step_cfg = {
            "availability": [{"topic": avail}],
            "device": dev_block,
            "icon": "mdi:stairs",
            "name": f"{spec.name} Slider Step",
            "state_topic": st_topic,
            "unique_id": step_uid,
            "value_template": "{{ value_json.step }}",
        }
        await self._publish_retained(
            f"homeassistant/sensor/{step_uid}/config",
            json.dumps(step_cfg, sort_keys=True),
        )

        # 5. Event entity: Multi-Tap & Gesture Events
        event_uid = f"{uid_base}_event"
        event_cfg = {
            "availability": [{"topic": avail}],
            "device": dev_block,
            "event_types": list(ALL_SLIDER_EVENT_TYPES),
            "icon": "mdi:gesture-tap-button",
            "name": f"{spec.name} Action",
            "state_topic": ev_topic,
            "unique_id": event_uid,
        }
        await self._publish_retained(
            f"homeassistant/event/{event_uid}/config",
            json.dumps(event_cfg, sort_keys=True),
        )

    async def _publish_state(self, slider_id: str, *, event: SliderEventType | None = None) -> None:
        st = self._states[slider_id]
        state_payload = {
            "state": "ON" if st.is_on else "OFF",
            "brightness": st.brightness,
            "raw_intensity": st.raw_intensity,
            "step": st.step,
            "mode": st.mode.value,
            "last_event": st.last_event,
            "event_seq": st.event_seq,
        }
        await self._publish_retained(
            self._state_topic(slider_id),
            json.dumps(state_payload, sort_keys=True),
        )
        if event is not None:
            event_payload = {
                "event_type": event,
                "brightness": st.brightness,
                "raw_intensity": st.raw_intensity,
                "step": st.step,
                "seq": st.event_seq,
            }
            await self._mqtt.publish(
                self._event_topic(slider_id),
                json.dumps(event_payload, sort_keys=True),
                retain=False,
            )


# ---------------------------------------------------------------------------
# JSON Spec Parser for VIRTUAL_SLIDERS_JSON env var
# ---------------------------------------------------------------------------

_INT_OPT_KEYS = (
    "min_raw",
    "max_raw",
    "num_steps",
    "step_delta_threshold",
    "swipe_delta_threshold",
)


def parse_virtual_slider_specs(raw_json: str, own_device_id: str = "") -> list[VirtualSliderSpec]:
    """Parse ``VIRTUAL_SLIDERS_JSON`` into a list of :class:`VirtualSliderSpec` objects."""
    if not raw_json or not raw_json.strip():
        return []
    data = json.loads(raw_json)
    if not isinstance(data, list):
        raise ValueError("VIRTUAL_SLIDERS_JSON must be a JSON list of slider objects")
    specs: list[VirtualSliderSpec] = []
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("Each entry in VIRTUAL_SLIDERS_JSON must be an object")
        slider_id = str(item["slider_id"]).strip()
        name = str(item.get("name", slider_id)).strip()
        if not slider_id:
            raise ValueError("slider_id must not be empty")
        default_mode = SliderMode(str(item.get("default_mode", "virtual")).lower())
        scene_single = str(item.get("scene_single_tap", ""))
        scene_double = str(item.get("scene_double_tap", ""))
        load_periph = str(item.get("load_peripheral_id", ""))
        opt_kwargs: dict[str, Any] = {k: int(item[k]) for k in _INT_OPT_KEYS if k in item}
        if "tap_window_s" in item:
            opt_kwargs["tap_window_s"] = float(item["tap_window_s"])
        if "settle_window_s" in item:
            opt_kwargs["settle_window_s"] = float(item["settle_window_s"])

        if "switch_id" in item:
            specs.append(
                VirtualSliderSpec.for_ble_switch(
                    slider_id=slider_id,
                    name=name,
                    switch_id=str(item["switch_id"]),
                    load_peripheral_id=load_periph,
                    default_mode=default_mode,
                    scene_single_tap=scene_single,
                    scene_double_tap=scene_double,
                    **opt_kwargs,
                )
            )
        elif "slider_index" in item:
            panel_dev = str(item.get("panel_device_id") or own_device_id)
            specs.append(
                VirtualSliderSpec.for_panel_slider(
                    slider_id=slider_id,
                    name=name,
                    panel_device_id=panel_dev,
                    slider_index=int(item["slider_index"]),
                    load_device_id=str(item.get("load_device_id", "")),
                    load_peripheral_id=load_periph,
                    default_mode=default_mode,
                    scene_single_tap=scene_single,
                    scene_double_tap=scene_double,
                    **opt_kwargs,
                )
            )
        else:
            specs.append(
                VirtualSliderSpec(
                    slider_id=slider_id,
                    name=name,
                    config_device_id=str(item["config_device_id"]),
                    config_peripheral_id=str(item["config_peripheral_id"]),
                    config_var_name=str(item.get("config_var_name", "slider_config")),
                    load_device_id=str(item.get("load_device_id", "")),
                    load_peripheral_id=load_periph,
                    default_mode=default_mode,
                    scene_single_tap=scene_single,
                    scene_double_tap=scene_double,
                    **opt_kwargs,
                )
            )
    return specs

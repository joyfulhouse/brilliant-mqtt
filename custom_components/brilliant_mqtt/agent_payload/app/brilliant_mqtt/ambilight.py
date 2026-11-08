"""Local Panel Camera Snapshot & Hardware-Cropped TV Ambilight Engine.

Uses the Brilliant panel's NXP i.MX6 camera pipeline (``imxv4l2src`` ->
``bayer2yuv`` sensor-window crop -> ``I420`` / ``jpegenc``) to provide:

1. **On-Demand Cropped JPEG Camera Snapshots** over MQTT (``retain=False``) so
   users can visually align the bounding box around their TV screen in Home
   Assistant.
2. **Real-Time Dominant RGB / Brightness Extraction** from raw ``I420`` (YUV420p)
   cropped frames in pure Python stdlib without external imaging dependencies.
3. **Hardware Privacy Switch Enforcement:** Checks the physical faceplate privacy
   shutter GPIO (``/sys/class/gpio/gpio5/value``) before every capture and
   refuses capture whenever the privacy shutter is closed (``"0"``).
"""

from __future__ import annotations

import asyncio
import colorsys
import contextlib
import json
import logging
import os
import struct
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol

from brilliant_mqtt.desired_state import atomic_write_json
from brilliant_mqtt.discovery import availability_topic, device_block, state_topic
from brilliant_mqtt.protocols import MqttClient
from brilliant_mqtt.retained_topics import RetainedTopicLedger

logger = logging.getLogger(__name__)

SENSOR_WIDTH = 1280
SENSOR_HEIGHT = 720
MIN_CROP_SPAN = 32
DEFAULT_PRIVACY_GPIO_PATH = "/sys/class/gpio/gpio5/value"


def is_privacy_shutter_closed(
    gpio_path: str | Path = DEFAULT_PRIVACY_GPIO_PATH,
) -> bool:
    """Return ``True`` if the panel's hardware privacy switch is closed (``0``).

    On the Brilliant Control faceplate, ``/sys/class/gpio/gpio5/value`` reads
    ``"0"`` when the physical privacy slider covers/disables the camera and
    ``"1"`` when the shutter is open. If the GPIO file is absent (e.g. off-panel
    unit tests), returns ``False``.
    """
    path = Path(gpio_path)
    try:
        if not path.exists():
            return False
        return path.read_text(encoding="utf-8").strip() == "0"
    except OSError:
        return False


@dataclass(frozen=True)
class AmbilightCrop:
    """Hardware crop margins (in pixels) for the 1280x720 OV5640 sensor mode."""

    top: int = 180
    bottom: int = 180
    left: int = 240
    right: int = 240

    @property
    def width(self) -> int:
        """Cropped image width in pixels."""
        return max(MIN_CROP_SPAN, SENSOR_WIDTH - self.left - self.right)

    @property
    def height(self) -> int:
        """Cropped image height in pixels."""
        return max(MIN_CROP_SPAN, SENSOR_HEIGHT - self.top - self.bottom)

    def normalized(self) -> AmbilightCrop:
        """Return a copy with margins clamped to valid 8-pixel aligned sensor bounds."""
        top = max(0, min(SENSOR_HEIGHT - MIN_CROP_SPAN, (self.top // 8) * 8))
        max_bottom = max(0, SENSOR_HEIGHT - MIN_CROP_SPAN - top)
        bottom = max(0, min(max_bottom, (self.bottom // 8) * 8))

        left = max(0, min(SENSOR_WIDTH - MIN_CROP_SPAN, (self.left // 8) * 8))
        max_right = max(0, SENSOR_WIDTH - MIN_CROP_SPAN - left)
        right = max(0, min(max_right, (self.right // 8) * 8))

        return AmbilightCrop(top=top, bottom=bottom, left=left, right=right)


@dataclass(frozen=True)
class AmbilightSample:
    """Extracted dominant RGB color and brightness from a cropped TV frame."""

    r: int
    g: int
    b: int
    brightness: int
    hex_color: str


# ---------------------------------------------------------------------------
# Pure Python I420 (YUV420p) Dominant Color Extractor
# ---------------------------------------------------------------------------


def _yuv_to_rgb(y: float, u: float, v: float) -> tuple[int, int, int]:
    """Convert BT.601 YUV (0..255) to clamped sRGB (0..255)."""
    c = y - 16.0
    d = u - 128.0
    e = v - 128.0
    r = round(max(0.0, min(255.0, 1.164383 * c + 1.596027 * e)))
    g = round(max(0.0, min(255.0, 1.164383 * c - 0.391762 * d - 0.812968 * e)))
    b = round(max(0.0, min(255.0, 1.164383 * c + 2.017232 * d)))
    return (r, g, b)


def extract_ambilight_sample_from_i420(
    frame: bytes,
    width: int,
    height: int,
    *,
    saturation_boost: float = 1.35,
) -> AmbilightSample:
    """Compute dominant vibrant RGB and brightness from a raw ``I420`` frame."""
    y_size = width * height
    uv_w = width // 2
    uv_h = height // 2
    uv_size = uv_w * uv_h
    expected = y_size + 2 * uv_size
    if len(frame) < expected or width <= 0 or height <= 0:
        raise ValueError(
            f"I420 buffer too small: got {len(frame)} bytes, "
            f"expected >= {expected} for {width}x{height}"
        )

    y_plane = frame[:y_size]
    u_plane = frame[y_size : y_size + uv_size]
    v_plane = frame[y_size + uv_size : expected]

    step_x = max(1, uv_w // 32)
    step_y = max(1, uv_h // 32)

    total_luma = 0.0
    sample_count = 0

    weighted_r = 0.0
    weighted_g = 0.0
    weighted_b = 0.0
    weight_sum = 0.0

    plain_r = 0.0
    plain_g = 0.0
    plain_b = 0.0

    for uy in range(0, uv_h, step_y):
        y_row = (uy * 2) * width
        uv_row = uy * uv_w
        for ux in range(0, uv_w, step_x):
            yv = float(y_plane[y_row + ux * 2])
            uv = float(u_plane[uv_row + ux])
            vv = float(v_plane[uv_row + ux])

            total_luma += yv
            sample_count += 1

            r, g, b = _yuv_to_rgb(yv, uv, vv)
            plain_r += r
            plain_g += g
            plain_b += b

            if 22.0 <= yv <= 245.0:
                chroma = ((uv - 128.0) ** 2 + (vv - 128.0) ** 2) ** 0.5
                w = 1.0 + (chroma / 32.0) ** 2
                weighted_r += r * w
                weighted_g += g * w
                weighted_b += b * w
                weight_sum += w

    if sample_count == 0:
        return AmbilightSample(r=0, g=0, b=0, brightness=1, hex_color="#000000")

    avg_luma = total_luma / sample_count
    if avg_luma < 18.0:
        return AmbilightSample(r=0, g=0, b=0, brightness=1, hex_color="#000000")

    if weight_sum > 0.0:
        rf = weighted_r / weight_sum
        gf = weighted_g / weight_sum
        bf = weighted_b / weight_sum
    else:
        rf = plain_r / sample_count
        gf = plain_g / sample_count
        bf = plain_b / sample_count

    h, s, v = colorsys.rgb_to_hsv(rf / 255.0, gf / 255.0, bf / 255.0)
    s = min(1.0, s * saturation_boost)
    v = max(0.15, min(1.0, v))
    ro, go, bo = colorsys.hsv_to_rgb(h, s, v)

    r_int = max(0, min(255, round(ro * 255.0)))
    g_int = max(0, min(255, round(go * 255.0)))
    b_int = max(0, min(255, round(bo * 255.0)))

    brightness = max(1, min(255, round(((avg_luma - 16.0) / 219.0) * 254.0) + 1))
    hex_color = f"#{r_int:02x}{g_int:02x}{b_int:02x}"
    return AmbilightSample(
        r=r_int,
        g=g_int,
        b=b_int,
        brightness=brightness,
        hex_color=hex_color,
    )


# ---------------------------------------------------------------------------
# GStreamer Pipeline Builders & Capture Adapter Protocol
# ---------------------------------------------------------------------------


def _build_gstreamer_args(
    crop: AmbilightCrop,
    output_path: str,
    *,
    jpeg: bool,
    device: str = "/dev/video0",
    quality: int = 85,
) -> list[str]:
    """Build ``gst-launch-1.0`` CLI args for cropped I420 or JPEG capture."""
    c = crop.normalized()
    args = [
        "gst-launch-1.0",
        "-q",
        "imxv4l2src",
        f"device={device}",
        "num-buffers=1",
        "!",
        "bayer2yuv",
        f"crop-top={c.top}",
        f"crop-bottom={c.bottom}",
        f"crop-left={c.left}",
        f"crop-right={c.right}",
        "!",
    ]
    if jpeg:
        args.extend(["jpegenc", f"quality={quality}", "!"])
    else:
        args.extend(["video/x-raw,format=I420", "!"])
    args.extend(["filesink", f"location={output_path}"])
    return args


def build_gstreamer_i420_args(
    crop: AmbilightCrop,
    output_path: str,
    device: str = "/dev/video0",
) -> list[str]:
    """Build ``gst-launch-1.0`` CLI args for a single cropped ``I420`` frame."""
    return _build_gstreamer_args(crop, output_path, jpeg=False, device=device)


def build_gstreamer_jpeg_args(
    crop: AmbilightCrop,
    output_path: str,
    device: str = "/dev/video0",
    quality: int = 85,
) -> list[str]:
    """Build ``gst-launch-1.0`` CLI args for a single cropped JPEG snapshot."""
    return _build_gstreamer_args(crop, output_path, jpeg=True, device=device, quality=quality)


class CameraCaptureProtocol(Protocol):
    """Protocol for capturing cropped I420 frames and JPEG snapshots."""

    async def capture_i420_frame(self, crop: AmbilightCrop) -> bytes | None:
        """Capture a single cropped ``I420`` frame, or ``None`` on error/shutter closed."""
        ...

    async def capture_jpeg_snapshot(self, crop: AmbilightCrop) -> bytes | None:
        """Capture a single cropped JPEG image, or ``None`` on error/shutter closed."""
        ...


def _device_exists_on_panel(device: str) -> bool:
    if os.path.exists("/proc") and device.startswith("/"):
        return Path(device).exists()
    return True


def _read_capture_output(out_path: Path) -> bytes | None:
    if not out_path.exists():
        return None
    data = out_path.read_bytes()
    return data if data else None


def _cleanup_capture_output(out_path: Path) -> None:
    with contextlib.suppress(OSError):
        if out_path.exists():
            out_path.unlink()


class GStreamerCameraAdapter(CameraCaptureProtocol):
    """Concrete :class:`CameraCaptureProtocol` using ``gst-launch-1.0`` on the panel."""

    def __init__(
        self,
        device: str = "/dev/video0",
        timeout_s: float = 4.0,
        privacy_gpio_path: str | Path = DEFAULT_PRIVACY_GPIO_PATH,
    ) -> None:
        self._device = device
        self._timeout_s = timeout_s
        self._privacy_gpio_path = Path(privacy_gpio_path)
        self._lock = asyncio.Lock()

    async def _run_gst_capture(self, args: list[str], out_path: Path) -> bytes | None:
        if is_privacy_shutter_closed(self._privacy_gpio_path):
            logger.info(
                "Camera capture skipped: hardware privacy switch is closed (%s=0)",
                self._privacy_gpio_path,
            )
            _cleanup_capture_output(out_path)
            return None
        if not _device_exists_on_panel(self._device):
            logger.warning("Camera device %s does not exist on this panel", self._device)
            _cleanup_capture_output(out_path)
            return None

        async with self._lock:
            try:
                proc = await asyncio.create_subprocess_exec(
                    *args,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    _, stderr = await asyncio.wait_for(proc.communicate(), timeout=self._timeout_s)
                except (TimeoutError, asyncio.TimeoutError):
                    logger.warning("gst-launch-1.0 timed out after %.1fs", self._timeout_s)
                    return None
                finally:
                    if proc.returncode is None:
                        with contextlib.suppress(ProcessLookupError):
                            proc.kill()
                        with contextlib.suppress(Exception):
                            await proc.communicate()

                if proc.returncode != 0:
                    err_txt = (stderr or b"").decode("utf-8", errors="replace").strip()
                    logger.warning("gst-launch-1.0 exited with %s: %s", proc.returncode, err_txt)
                    return None
                return _read_capture_output(out_path)
            except Exception:
                logger.exception("Error executing gst-launch-1.0 camera capture")
                return None
            finally:
                _cleanup_capture_output(out_path)

    async def _capture_file(self, crop: AmbilightCrop, *, jpeg: bool) -> bytes | None:
        suffix = ".jpg" if jpeg else ".yuv"
        fd, tmp_name = tempfile.mkstemp(prefix="ambilight_", suffix=suffix)
        os.close(fd)
        out_path = Path(tmp_name)
        args = _build_gstreamer_args(crop, str(out_path), jpeg=jpeg, device=self._device)
        return await self._run_gst_capture(args, out_path)

    async def capture_i420_frame(self, crop: AmbilightCrop) -> bytes | None:
        return await self._capture_file(crop, jpeg=False)

    async def capture_jpeg_snapshot(self, crop: AmbilightCrop) -> bytes | None:
        return await self._capture_file(crop, jpeg=True)


# ---------------------------------------------------------------------------
# Ambilight & Panel Camera Controller
# ---------------------------------------------------------------------------


class AmbilightController:
    """Manages panel camera snapshots, crop settings, and TV Ambilight polling."""

    def __init__(
        self,
        mqtt: MqttClient,
        panel: str,
        camera: CameraCaptureProtocol,
        state_path: Path | None = None,
        *,
        default_enabled: bool = False,
        default_interval_s: float = 1.0,
        default_crop: AmbilightCrop | None = None,
        owned_topics: RetainedTopicLedger | None = None,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self._mqtt = mqtt
        self._panel = panel
        self._camera = camera
        self._state_path = state_path
        self._owned_topics = owned_topics
        self._sleep = sleep

        self._enabled = default_enabled
        self._interval_s = max(0.25, min(10.0, default_interval_s))
        self._crop = (default_crop or AmbilightCrop()).normalized()
        self._last_sample: AmbilightSample | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._last_persisted_snapshot: dict[str, Any] | None = None

        self._load_state()

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def crop(self) -> AmbilightCrop:
        return self._crop

    @property
    def interval_s(self) -> float:
        return self._interval_s

    @property
    def last_sample(self) -> AmbilightSample | None:
        return self._last_sample

    # ------------------------------------------------------------------
    # Persistence (atomic + changed-only)
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        if self._state_path is None or not self._state_path.exists():
            return
        try:
            data = json.loads(self._state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to load ambilight state from %s: %s", self._state_path, exc)
            return
        if not isinstance(data, dict):
            return
        self._last_persisted_snapshot = data
        if isinstance(data.get("enabled"), bool):
            self._enabled = bool(data["enabled"])
        if isinstance(data.get("interval_s"), (int, float)):
            self._interval_s = max(0.25, min(10.0, float(data["interval_s"])))
        crop_data = data.get("crop")
        if isinstance(crop_data, dict):
            self._crop = AmbilightCrop(
                top=int(crop_data.get("top", self._crop.top)),
                bottom=int(crop_data.get("bottom", self._crop.bottom)),
                left=int(crop_data.get("left", self._crop.left)),
                right=int(crop_data.get("right", self._crop.right)),
            ).normalized()

    def _build_persisted_snapshot(self) -> dict[str, Any]:
        return {
            "enabled": self._enabled,
            "interval_s": self._interval_s,
            "crop": {
                "top": self._crop.top,
                "bottom": self._crop.bottom,
                "left": self._crop.left,
                "right": self._crop.right,
            },
        }

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
            logger.warning("Failed to save ambilight state to %s: %s", self._state_path, exc)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Publish MQTT discovery, subscribe command topics, and start polling if enabled."""
        self._mqtt.on_command(self.on_mqtt_command)
        await self._publish_discovery()
        await self._subscribe_commands()
        await self._publish_state()
        self._sync_poll_task()

    async def shutdown(self) -> None:
        """Stop background polling and clear any retained snapshot on the broker."""
        if self._poll_task is not None and not self._poll_task.done():
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
        self._poll_task = None
        with contextlib.suppress(Exception):
            await self._clear_snapshot_topic()

    def _sync_poll_task(self) -> None:
        if self._enabled:
            if self._poll_task is None or self._poll_task.done():
                self._poll_task = asyncio.create_task(self._poll_loop())
        else:
            if self._poll_task is not None and not self._poll_task.done():
                self._poll_task.cancel()
            self._poll_task = None

    async def _poll_loop(self) -> None:
        while self._enabled:
            await self.sample_once()
            try:
                await self._sleep(self._interval_s)
            except asyncio.CancelledError:
                return

    async def sample_once(self) -> AmbilightSample | None:
        """Capture one cropped I420 frame, compute dominant color, and publish state."""
        c = self._crop.normalized()
        frame = await self._camera.capture_i420_frame(c)
        if frame is None:
            return None
        try:
            sample = extract_ambilight_sample_from_i420(frame, c.width, c.height)
        except ValueError as exc:
            logger.warning("Ambilight frame extraction failed: %s", exc)
            return None
        self._last_sample = sample
        await self._publish_state()
        await self._mqtt.publish(
            self._color_topic(),
            json.dumps(
                {
                    "r": sample.r,
                    "g": sample.g,
                    "b": sample.b,
                    "rgb_color": [sample.r, sample.g, sample.b],
                    "brightness": sample.brightness,
                    "hex_color": sample.hex_color,
                    "state": "ON" if sample.brightness > 1 else "OFF",
                },
                sort_keys=True,
            ),
            retain=False,
        )
        return sample

    async def trigger_snapshot(self) -> bool:
        """Capture a cropped JPEG snapshot and publish it to MQTT with ``retain=False``."""
        c = self._crop.normalized()
        jpeg = await self._camera.capture_jpeg_snapshot(c)
        if jpeg is None:
            return False
        payload_str = jpeg.decode("latin1")
        await self._mqtt.publish(self._snapshot_topic(), payload_str, retain=False)
        return True

    async def _clear_snapshot_topic(self) -> None:
        """Clear any retained snapshot from the broker with an empty retained payload."""
        await self._mqtt.publish(self._snapshot_topic(), "", retain=True)

    # ------------------------------------------------------------------
    # MQTT Command Handling
    # ------------------------------------------------------------------

    async def on_mqtt_command(self, topic: str, payload: str) -> None:
        prefix = f"brilliant/{self._panel}/ambilight/set_"
        if not topic.startswith(prefix):
            return
        cmd = topic[len(prefix) :]
        val = payload.strip()

        if cmd == "enabled":
            was_enabled = self._enabled
            self._enabled = val.upper() in ("ON", "1", "TRUE")
            self._save_state_if_changed()
            self._sync_poll_task()
            if was_enabled and not self._enabled:
                await self._clear_snapshot_topic()
            await self._publish_state()
        elif cmd == "snapshot":
            await self.trigger_snapshot()
        elif cmd == "interval":
            try:
                self._interval_s = max(0.25, min(10.0, float(val)))
            except ValueError:
                return
            self._save_state_if_changed()
            await self._publish_state()
        elif cmd in ("crop_top", "crop_bottom", "crop_left", "crop_right"):
            try:
                px = round(float(val))
            except ValueError:
                return
            field_name = cmd[5:]
            self._crop = replace(self._crop, **{field_name: px}).normalized()
            self._save_state_if_changed()
            await self._publish_state()

    # ------------------------------------------------------------------
    # Topics & Discovery
    # ------------------------------------------------------------------

    def _state_topic(self) -> str:
        return state_topic(self._panel, "ambilight")

    def _color_topic(self) -> str:
        return f"brilliant/{self._panel}/ambilight/color"

    def _snapshot_topic(self) -> str:
        return f"brilliant/{self._panel}/ambilight/snapshot"

    def _cmd_topic(self, suffix: str) -> str:
        return f"brilliant/{self._panel}/ambilight/set_{suffix}"

    async def _publish_retained(self, topic: str, payload: str) -> None:
        if self._owned_topics is not None:
            await self._owned_topics.async_publish(self._mqtt, topic, payload)
        else:
            await self._mqtt.publish(topic, payload, retain=True)

    async def _subscribe_commands(self) -> None:
        for suffix in (
            "enabled",
            "snapshot",
            "interval",
            "crop_top",
            "crop_bottom",
            "crop_left",
            "crop_right",
        ):
            await self._mqtt.subscribe(self._cmd_topic(suffix))

    async def _publish_discovery(self) -> None:
        avail = availability_topic(self._panel)
        st_topic = self._state_topic()
        dev_block = device_block(self._panel)
        uid_base = f"brilliant_{self._panel}_ambilight"

        # 1. Switch: Enable TV Ambilight Mode
        sw_uid = f"{uid_base}_enabled"
        sw_cfg = {
            "availability": [{"topic": avail}],
            "command_topic": self._cmd_topic("enabled"),
            "device": dev_block,
            "icon": "mdi:television-ambient-light",
            "name": "TV Ambilight Mode",
            "payload_off": "OFF",
            "payload_on": "ON",
            "state_off": "OFF",
            "state_on": "ON",
            "state_topic": st_topic,
            "unique_id": sw_uid,
            "value_template": "{{ 'ON' if value_json.enabled else 'OFF' }}",
        }
        await self._publish_retained(
            f"homeassistant/switch/{sw_uid}/config",
            json.dumps(sw_cfg, sort_keys=True),
        )

        # 2. Button: Capture Cropped Snapshot
        btn_uid = f"{uid_base}_capture"
        btn_cfg = {
            "availability": [{"topic": avail}],
            "command_topic": self._cmd_topic("snapshot"),
            "device": dev_block,
            "icon": "mdi:camera-iris",
            "name": "Capture TV Snapshot",
            "payload_press": "PRESS",
            "unique_id": btn_uid,
        }
        await self._publish_retained(
            f"homeassistant/button/{btn_uid}/config",
            json.dumps(btn_cfg, sort_keys=True),
        )

        # 3. MQTT Camera: Cropped TV Snapshot Preview
        cam_uid = f"{uid_base}_camera"
        cam_cfg = {
            "availability": [{"topic": avail}],
            "device": dev_block,
            "icon": "mdi:crop-free",
            "name": "TV Ambilight Crop Preview",
            "topic": self._snapshot_topic(),
            "unique_id": cam_uid,
        }
        await self._publish_retained(
            f"homeassistant/camera/{cam_uid}/config",
            json.dumps(cam_cfg, sort_keys=True),
        )

        # 4. Sensor: Dominant Hex Color + RGB attributes
        col_uid = f"{uid_base}_color"
        col_cfg = {
            "availability": [{"topic": avail}],
            "device": dev_block,
            "icon": "mdi:palette",
            "json_attributes_template": "{{ value_json.color | tojson }}",
            "json_attributes_topic": st_topic,
            "name": "TV Ambilight Dominant Color",
            "state_topic": st_topic,
            "unique_id": col_uid,
            "value_template": "{{ value_json.color.hex_color }}",
        }
        await self._publish_retained(
            f"homeassistant/sensor/{col_uid}/config",
            json.dumps(col_cfg, sort_keys=True),
        )

        # 5. Number entities: Crop Top/Bottom/Left/Right & Poll Interval
        crop_specs = [
            ("crop_top", "Crop Top", 0, 640, 8, "{{ value_json.crop.top }}"),
            ("crop_bottom", "Crop Bottom", 0, 640, 8, "{{ value_json.crop.bottom }}"),
            ("crop_left", "Crop Left", 0, 1200, 8, "{{ value_json.crop.left }}"),
            ("crop_right", "Crop Right", 0, 1200, 8, "{{ value_json.crop.right }}"),
        ]
        for suffix, label, mn, mx, step, tpl in crop_specs:
            n_uid = f"{uid_base}_{suffix}"
            n_cfg = {
                "availability": [{"topic": avail}],
                "command_topic": self._cmd_topic(suffix),
                "device": dev_block,
                "entity_category": "config",
                "icon": "mdi:crop",
                "max": mx,
                "min": mn,
                "mode": "box",
                "name": f"TV Ambilight {label}",
                "state_topic": st_topic,
                "step": step,
                "unique_id": n_uid,
                "unit_of_measurement": "px",
                "value_template": tpl,
            }
            await self._publish_retained(
                f"homeassistant/number/{n_uid}/config",
                json.dumps(n_cfg, sort_keys=True),
            )

        int_uid = f"{uid_base}_interval"
        int_cfg = {
            "availability": [{"topic": avail}],
            "command_topic": self._cmd_topic("interval"),
            "device": dev_block,
            "entity_category": "config",
            "icon": "mdi:timer-outline",
            "max": 10.0,
            "min": 0.25,
            "mode": "box",
            "name": "TV Ambilight Poll Interval",
            "state_topic": st_topic,
            "step": 0.25,
            "unique_id": int_uid,
            "unit_of_measurement": "s",
            "value_template": "{{ value_json.interval_s }}",
        }
        await self._publish_retained(
            f"homeassistant/number/{int_uid}/config",
            json.dumps(int_cfg, sort_keys=True),
        )

    async def _publish_state(self) -> None:
        c = self._crop.normalized()
        s = self._last_sample or AmbilightSample(r=0, g=0, b=0, brightness=1, hex_color="#000000")
        payload = {
            "enabled": self._enabled,
            "interval_s": self._interval_s,
            "crop": {
                "top": c.top,
                "bottom": c.bottom,
                "left": c.left,
                "right": c.right,
                "width": c.width,
                "height": c.height,
            },
            "color": {
                "r": s.r,
                "g": s.g,
                "b": s.b,
                "rgb_color": [s.r, s.g, s.b],
                "brightness": s.brightness,
                "hex_color": s.hex_color,
            },
        }
        await self._publish_retained(
            self._state_topic(),
            json.dumps(payload, sort_keys=True),
        )


# ---------------------------------------------------------------------------
# Multi-Zone Perceptual RPMLight Streamer Engine (52-Zone Perimeter + Sky)
# Standalone upstream package: https://github.com/rpmccormick/rpmlight
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ZoneFrameResult:
    """Computed 52-zone RGB output, transition time, scene-cut flag, and overhead sky."""

    zones_rgb: list[tuple[int, int, int]]
    nl_transition_ds: int
    is_scene_cut: bool
    up_rgb: list[int]
    up_bri: int
    down_rgb: list[int]
    down_bri: int


def build_52_panel_coords() -> list[tuple[int, int, int]]:
    """Build the 52-zone clockwise perimeter coordinate ring (starting bottom-center)."""
    coords: list[tuple[int, int, int]] = []
    pid = 0
    for x in range(400, -1, -50):
        coords.append((pid, x, 0))
        pid += 1
    for y in range(50, 451, 50):
        coords.append((pid, 0, y))
        pid += 1
    for x in range(50, 851, 50):
        coords.append((pid, x, 450))
        pid += 1
    for y in range(400, -1, -50):
        coords.append((pid, 850, y))
        pid += 1
    for x in range(800, 449, -50):
        coords.append((pid, x, 0))
        pid += 1
    return coords


def _precompute_box_indices(
    width: int, x0: int, y0: int, x1: int, y1: int, step: int = 4
) -> tuple[list[int], list[int]]:
    uv_stride = width // 2
    y_indices: list[int] = []
    uv_indices: list[int] = []
    for py in range(y0, y1, step):
        y_row = py * width
        uv_row = (py >> 1) * uv_stride
        for px in range(x0, x1, step):
            y_indices.append(y_row + px)
            uv_indices.append(uv_row + (px >> 1))
    return y_indices, uv_indices


def build_depth_glow_boxes(
    width: int = 320,
    height: int = 184,
) -> list[tuple[int, list[int], list[int], list[int], list[int]]]:
    """Precompute a tight Outer Edge Strip + a 54x44 Deep Inner Strip per zone."""
    ex_min, ex_max, ey_min, ey_max, ew, eh = 3, 315, 2, 170, 16, 14
    espan_x = (ex_max - ex_min) - ew
    espan_y = (ey_max - ey_min) - eh

    ix_min, ix_max, iy_min, iy_max, iw, ih = 18, 302, 18, 154, 54, 44
    ispan_x = (ix_max - ix_min) - iw
    ispan_y = (iy_max - iy_min) - ih

    precomputed: list[tuple[int, list[int], list[int], list[int], list[int]]] = []
    for pid, nx, ny in build_52_panel_coords():
        u = nx / 850.0
        v = 1.0 - (ny / 450.0)

        ex0 = round(ex_min + u * espan_x)
        ey0 = round(ey_min + v * espan_y)
        cur_eh = eh
        if ex0 >= 265 and ey0 >= 142:
            ey0 = 138
        elif ey0 >= 152:
            ey0 = 162
            cur_eh = 8
        ey_idx, euv_idx = _precompute_box_indices(
            width, ex0, ey0, min(width, ex0 + ew), min(height, ey0 + cur_eh), step=3
        )

        ix0 = round(ix_min + u * ispan_x)
        iy0 = round(iy_min + v * ispan_y)
        cur_ih = ih
        if ey0 >= 152:
            iy0 = 88
            cur_ih = 46
        iy_idx, iuv_idx = _precompute_box_indices(
            width, ix0, iy0, min(width, ix0 + iw), min(height, iy0 + cur_ih), step=8
        )

        precomputed.append((pid, ey_idx, euv_idx, iy_idx, iuv_idx))
    return precomputed


def sample_box_fast(
    y_plane: bytes | memoryview,
    u_plane: bytes | memoryview,
    v_plane: bytes | memoryview,
    y_indices: list[int],
    uv_indices: list[int],
    chroma_magnet: bool = True,
    ignore_bright_matte: bool = False,
) -> tuple[int, int, int]:
    """Extract perceptual RGB from an I420 sampling box with Bayer fringe suppression."""
    y_acc = 0
    u_acc = 0
    v_acc = 0
    w_acc = 0
    luma_sum = 0
    count = len(y_indices)

    for idx in range(count):
        yv = y_plane[y_indices[idx]]
        uv_i = uv_indices[idx]
        u = u_plane[uv_i] - 128
        v = v_plane[uv_i] - 128

        if yv >= 85 and -8 <= u <= 24 and -24 <= v <= 4 and (u * u + v * v) < 750:
            u = int(u * 0.25)
            v = int(v * 0.25)
        elif 10 <= yv < 34 and u >= 2 and v <= -2:
            u = int(u * 1.55)
            v = int(v * 1.35)
        uv_sq = u * u + v * v

        vr = (v * 380) >> 8
        ub = (u * 320) >> 8
        chroma_lift = max(0, vr, ub)
        peak_ch = yv + chroma_lift
        luma_sum += peak_ch

        luma_f = peak_ch - 8
        if luma_f <= 0:
            w = 1
        else:
            luma_f = min(72, luma_f)
            if not chroma_magnet:
                w = 2 + (luma_f >> 2)
            else:
                if yv > 130 and uv_sq < 650:
                    md = 64 - (((yv - 130) * (650 - uv_sq)) >> 8)
                    min_md = 4 if ignore_bright_matte else 16
                    matte_damp = max(min_md, md)
                else:
                    matte_damp = 64
                uv_eff = min(550, uv_sq)
                chroma_f = max(0, uv_eff - 110)
                w = 4 + ((((luma_f >> 2) + ((chroma_f * luma_f) >> 9)) * matte_damp) >> 6)

        y_acc += yv * w
        u_acc += u * w
        v_acc += v * w
        w_acc += w

    if count == 0 or w_acc <= 0:
        return (0, 0, 0)

    mean_luma = luma_sum / count
    ym = y_acc // w_acc
    um = u_acc // w_acc
    vm = v_acc // w_acc
    uvm_sq = um * um + vm * vm

    if ym >= 75:
        taper_limit = 220.0 if (um > 0 and vm <= 4) else 160.0
    else:
        taper_limit = 14.0
    if uvm_sq < taper_limit:
        c_taper = (uvm_sq / taper_limit) ** 1.2
        um = round(um * c_taper)
        vm = round(vm * c_taper)

    win_peak = ym + max(0, (vm * 380) >> 8, (um * 320) >> 8)
    if mean_luma < 20.0 and uvm_sq < 25:
        win_peak = min(win_peak, int(mean_luma * 1.75))

    pw = min(0.65, max(0.0, (mean_luma - 9.0) / 18.0) * 0.65)
    eff_luma = (1.0 - pw) * mean_luma + pw * float(win_peak)
    if eff_luma < 7.8:
        return (0, 0, 0)

    if eff_luma < 14.0:
        chroma_scale = max(0.35, (eff_luma - 7.8) / 6.2)
        um = round(um * chroma_scale)
        vm = round(vm * chroma_scale)

    c = max(3, ym - 6)
    r = max(0, min(255, (298 * c + 409 * vm + 128) >> 8))
    g = max(0, min(255, (298 * c - 100 * um - 208 * vm + 128) >> 8))
    b = max(0, min(255, (298 * c + 516 * um + 128) >> 8))

    if r >= 125 and g >= 38 and b >= 26 and r > g * 1.25 and b <= g * 1.35:
        b = min(255, max(b, round(g * 1.18)))

    h, s, val = colorsys.rgb_to_hsv(r / 255.0, g / 255.0, b / 255.0)
    if s > 0.14:
        if h < 0.08 or h > 0.88:
            sat_boost = 1.05 if s < 0.72 else 1.20
        else:
            sat_boost = 1.32
        s = min(1.0, 0.14 + (s - 0.14) * sat_boost)
        if ym < 48:
            max_s = 0.58 + 0.42 * min(1.0, max(0.0, (ym - 10.0) / 38.0))
            s = min(s, max_s)
    else:
        s = s * 0.25

    luma_drive = max(0.0, (eff_luma - 9.5) / 120.0)
    if eff_luma < 28.0:
        v_out = ((max(0.0, eff_luma - 9.5) / 18.5) ** 1.10) * 0.30
    else:
        v_drive = max(luma_drive, max(0.0, (val - 0.05) / 0.72))
        v_out = min(1.0, 0.30 + 0.70 * (max(0.0, (v_drive - 0.15) / 0.85) ** 0.72))
    if s > 0.22 and 0.50 <= h <= 0.72:
        v_out *= 0.54

    rf2, gf2, bf2 = colorsys.hsv_to_rgb(h, s, min(1.0, v_out))
    r_gain = 242.0 - 12.0 * min(1.0, s / 0.20)
    g_gain = 240.0 + 5.0 * min(1.0, s / 0.20)
    b_gain = 236.0 + 19.0 * min(1.0, s / 0.20)
    return (
        max(0, min(255, round(rf2 * r_gain))),
        max(0, min(255, round(gf2 * g_gain))),
        max(0, min(255, round(bf2 * b_gain))),
    )


def encode_nanoleaf_v2_packet(
    zones_rgb: list[tuple[int, int, int]], transition_ds: int = 1
) -> bytes:
    """Encode a 52-zone Nanoleaf External Control v2 UDP packet (port 60222)."""
    parts = [struct.pack(">H", len(zones_rgb))]
    for i, (r, g, b) in enumerate(zones_rgb):
        parts.append(struct.pack(">HBBBBH", i, r, g, b, 0, transition_ds))
    return b"".join(parts)


"""Local Panel Camera Snapshot & Hardware-Cropped TV Ambilight Engine.

Uses the Brilliant panel's NXP i.MX6 camera pipeline (``imxv4l2src`` ->
``bayer2yuv`` sensor-window crop -> ``I420`` / ``jpegenc``) to provide:

1. **On-Demand Cropped JPEG Camera Snapshots** over MQTT (``retain=False``) so
   users can visually align the bounding box around their TV screen in Home
   Assistant.
2. **Real-Time Dominant RGB / Brightness Extraction** from raw ``I420`` (YUV420p)
   cropped frames in pure Python stdlib without external imaging dependencies.
3. **Fail-Closed Privacy Gate:** Checks the live-verified bus ``privacy_toggle``
   variable before every capture and refuses capture unless privacy mode is
   positively reported off (``"0"``).
"""

from __future__ import annotations

import asyncio
import colorsys
import contextlib
import json
import logging
import os
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


def is_privacy_shutter_closed(
    privacy_toggle: str | None = None,
    *,
    privacy_off: Callable[[], bool] | None = None,
) -> bool:
    """Return ``True`` unless the panel's bus ``privacy_toggle`` positively reads ``"0"``.

    Fails closed: when ``privacy_toggle`` is ``None``, empty, ``"unknown"``, or
    ``"1"`` (privacy mode active / shutter closed), returns ``True`` so camera
    capture is blocked.
    """
    if privacy_off is not None:
        try:
            return not bool(privacy_off())
        except Exception:
            return True
    if privacy_toggle is None:
        return True
    return privacy_toggle.strip() != "0"


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

    def with_sub_bbox_norm(
        self,
        bbox_norm: tuple[float, float, float, float],
        *,
        safety_inset_norm: float = 0.015,
    ) -> AmbilightCrop:
        """Return a tightened 8px-aligned crop from a normalized ``(x0, y0, x1, y1)`` sub-box."""
        base = self.normalized()
        x0, y0, x1, y1 = bbox_norm
        x0_c = max(0.0, min(0.85, float(x0) + safety_inset_norm))
        y0_c = max(0.0, min(0.85, float(y0) + safety_inset_norm))
        x1_c = max(x0_c + 0.10, min(1.0, float(x1) - safety_inset_norm))
        y1_c = max(y0_c + 0.10, min(1.0, float(y1) - safety_inset_norm))

        new_left = base.left + round(x0_c * base.width)
        new_right = base.right + round((1.0 - x1_c) * base.width)
        new_top = base.top + round(y0_c * base.height)
        new_bottom = base.bottom + round((1.0 - y1_c) * base.height)
        return AmbilightCrop(
            top=new_top,
            bottom=new_bottom,
            left=new_left,
            right=new_right,
        ).normalized()


def estimate_i420_active_bbox_norm(
    frame: bytes,
    width: int = 320,
    height: int = 176,
    *,
    luma_threshold: int = 26,
) -> tuple[float, float, float, float]:
    """Estimate active normalized ``(x0, y0, x1, y1)`` TV area from an ``I420`` frame."""
    y_size = width * height
    if len(frame) < y_size or width <= 0 or height <= 0:
        return (0.0, 0.0, 1.0, 1.0)
    y_plane = frame[:y_size]

    active_rows: list[int] = []
    for r in range(height):
        row = y_plane[r * width : (r + 1) * width : 4]
        if row:
            s_row = sorted(row)
            p92 = s_row[min(len(s_row) - 1, int(len(s_row) * 0.92))]
            if p92 >= luma_threshold:
                active_rows.append(r)

    active_cols: list[int] = []
    for c in range(0, width, 2):
        col = [y_plane[r * width + c] for r in range(0, height, 4)]
        if col:
            s_col = sorted(col)
            p92 = s_col[min(len(s_col) - 1, int(len(s_col) * 0.92))]
            if p92 >= luma_threshold:
                active_cols.append(c)

    if not active_rows or not active_cols:
        return (0.0, 0.0, 1.0, 1.0)

    x0 = max(0.0, min(0.45, active_cols[0] / float(width)))
    x1 = min(1.0, max(0.55, (active_cols[-1] + 2) / float(width)))
    y0 = max(0.0, min(0.45, active_rows[0] / float(height)))
    y1 = min(1.0, max(0.55, (active_rows[-1] + 1) / float(height)))
    return (round(x0, 4), round(y0, 4), round(x1, 4), round(y1, 4))


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
        *,
        privacy_off: Callable[[], bool] | None = None,
    ) -> None:
        self._device = device
        self._timeout_s = timeout_s
        self._privacy_off = privacy_off
        self._lock = asyncio.Lock()

    async def _run_gst_capture(self, args: list[str], out_path: Path) -> bytes | None:
        if is_privacy_shutter_closed(privacy_off=self._privacy_off):
            logger.info("Camera capture skipped: bus privacy_toggle is not positively off")
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
        default_auto_crop_enabled: bool = False,
        owned_topics: RetainedTopicLedger | None = None,
        privacy_off: Callable[[], bool] | None = None,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self._mqtt = mqtt
        self._panel = panel
        self._camera = camera
        self._state_path = state_path
        self._owned_topics = owned_topics
        self._privacy_off = privacy_off
        self._sleep = sleep

        self._enabled = default_enabled
        self._interval_s = max(0.25, min(10.0, default_interval_s))
        self._crop = (default_crop or AmbilightCrop()).normalized()
        self._saved_crop = self._crop
        self._auto_crop_enabled = default_auto_crop_enabled
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
    def saved_crop(self) -> AmbilightCrop:
        """Preserved manual/hardcoded crop margins used when reverting Auto-Crop."""
        return self._saved_crop

    @property
    def auto_crop_enabled(self) -> bool:
        return self._auto_crop_enabled

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
        if isinstance(data.get("auto_crop_enabled"), bool):
            self._auto_crop_enabled = bool(data["auto_crop_enabled"])
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
        saved_data = data.get("saved_crop")
        if isinstance(saved_data, dict):
            self._saved_crop = AmbilightCrop(
                top=int(saved_data.get("top", self._saved_crop.top)),
                bottom=int(saved_data.get("bottom", self._saved_crop.bottom)),
                left=int(saved_data.get("left", self._saved_crop.left)),
                right=int(saved_data.get("right", self._saved_crop.right)),
            ).normalized()

    def _build_persisted_snapshot(self) -> dict[str, Any]:
        return {
            "enabled": self._enabled,
            "auto_crop_enabled": self._auto_crop_enabled,
            "interval_s": self._interval_s,
            "crop": {
                "top": self._crop.top,
                "bottom": self._crop.bottom,
                "left": self._crop.left,
                "right": self._crop.right,
            },
            "saved_crop": {
                "top": self._saved_crop.top,
                "bottom": self._saved_crop.bottom,
                "left": self._saved_crop.left,
                "right": self._saved_crop.right,
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

    def _is_blocked_by_privacy(self) -> bool:
        if self._privacy_off is None:
            return False
        return is_privacy_shutter_closed(privacy_off=self._privacy_off)

    async def sample_once(self) -> AmbilightSample | None:
        """Capture one cropped I420 frame, compute dominant color, and publish state."""
        if self._is_blocked_by_privacy():
            return None
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
        if self._is_blocked_by_privacy():
            return False
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
    # Optional Auto-Crop & Saved Hardcoded Crop Revert
    # ------------------------------------------------------------------

    def revert_crop(self) -> AmbilightCrop:
        """Disable Auto-Crop and restore the saved manual/hardcoded crop margins."""
        self._auto_crop_enabled = False
        self._crop = self._saved_crop.normalized()
        self._save_state_if_changed()
        return self._crop

    def apply_auto_crop_bbox(
        self,
        bbox_norm: tuple[float, float, float, float],
        *,
        safety_inset_norm: float = 0.015,
    ) -> AmbilightCrop:
        """Apply a normalized ``(x0, y0, x1, y1)`` sub-bbox relative to ``saved_crop``."""
        if not self._auto_crop_enabled:
            return self._crop
        self._crop = self._saved_crop.with_sub_bbox_norm(
            bbox_norm, safety_inset_norm=safety_inset_norm
        )
        self._save_state_if_changed()
        return self._crop

    async def auto_calibrate_crop(self) -> AmbilightCrop | None:
        """Capture an ``I420`` frame from ``saved_crop`` and auto-fit active TV bounds."""
        if self._is_blocked_by_privacy():
            return None
        base = self._saved_crop.normalized()
        frame = await self._camera.capture_i420_frame(base)
        if frame is None:
            return None
        bbox = estimate_i420_active_bbox_norm(frame, base.width, base.height)
        self._auto_crop_enabled = True
        return self.apply_auto_crop_bbox(bbox)

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
        elif cmd == "auto_crop":
            if val.upper() in ("OFF", "0", "FALSE"):
                self.revert_crop()
            else:
                await self.auto_calibrate_crop()
            await self._publish_state()
        elif cmd == "revert_crop":
            self.revert_crop()
            await self._publish_state()
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
            self._saved_crop = self._crop
            self._auto_crop_enabled = False
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
            "auto_crop",
            "revert_crop",
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
        saved = self._saved_crop.normalized()
        s = self._last_sample or AmbilightSample(r=0, g=0, b=0, brightness=1, hex_color="#000000")
        payload = {
            "enabled": self._enabled,
            "auto_crop_enabled": self._auto_crop_enabled,
            "interval_s": self._interval_s,
            "crop": {
                "top": c.top,
                "bottom": c.bottom,
                "left": c.left,
                "right": c.right,
                "width": c.width,
                "height": c.height,
            },
            "saved_crop": {
                "top": saved.top,
                "bottom": saved.bottom,
                "left": saved.left,
                "right": saved.right,
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

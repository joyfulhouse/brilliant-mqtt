"""Unit tests for the Local Panel Camera & Hardware-Cropped TV Ambilight Engine."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from brilliant_mqtt.ambilight import (
    AmbilightController,
    AmbilightCrop,
    CameraCaptureProtocol,
    GStreamerCameraAdapter,
    build_52_panel_coords,
    build_depth_glow_boxes,
    build_gstreamer_i420_args,
    build_gstreamer_jpeg_args,
    encode_nanoleaf_v2_packet,
    extract_ambilight_sample_from_i420,
    is_privacy_shutter_closed,
    sample_box_fast,
)
from brilliant_mqtt.retained_topics import RetainedTopicLedger
from tests.fakes import FakeMqtt


def _make_solid_i420_frame(width: int, height: int, y: int, u: int, v: int) -> bytes:
    """Create a synthetic planar I420 frame with uniform Y, U, V values."""
    y_plane = bytes([y]) * (width * height)
    uv_size = (width // 2) * (height // 2)
    u_plane = bytes([u]) * uv_size
    v_plane = bytes([v]) * uv_size
    return y_plane + u_plane + v_plane


class FakeCameraAdapter(CameraCaptureProtocol):
    def __init__(self, y: int = 120, u: int = 90, v: int = 210) -> None:
        self.y = y
        self.u = u
        self.v = v
        self.jpeg_bytes = b"\xff\xd8\xff\xe0FAKE_JPEG_DATA\xff\xd9"
        self.last_crop: AmbilightCrop | None = None

    async def capture_i420_frame(self, crop: AmbilightCrop) -> bytes | None:
        self.last_crop = crop
        c = crop.normalized()
        return _make_solid_i420_frame(c.width, c.height, self.y, self.u, self.v)

    async def capture_jpeg_snapshot(self, crop: AmbilightCrop) -> bytes | None:
        self.last_crop = crop
        return self.jpeg_bytes


class TestAmbilightCropAndGstArgs:
    def test_crop_normalization_aligns_to_8px_and_clamps(self) -> None:
        crop = AmbilightCrop(top=183, bottom=179, left=245, right=241).normalized()
        assert crop.top == 176
        assert crop.bottom == 176
        assert crop.left == 240
        assert crop.right == 240
        assert crop.width == 1280 - 480
        assert crop.height == 720 - 352

    def test_gstreamer_args_builders(self) -> None:
        crop = AmbilightCrop(top=240, bottom=200, left=320, right=320)
        i420_args = build_gstreamer_i420_args(crop, "/tmp/frame.yuv")
        assert "imxv4l2src" in i420_args
        assert "crop-top=240" in i420_args
        assert "video/x-raw,format=I420" in i420_args

        jpeg_args = build_gstreamer_jpeg_args(crop, "/tmp/snap.jpg", quality=90)
        assert "jpegenc" in jpeg_args
        assert "quality=90" in jpeg_args


class TestI420ColorExtractor:
    def test_extract_vibrant_red_frame(self) -> None:
        frame = _make_solid_i420_frame(64, 64, y=82, u=90, v=240)
        sample = extract_ambilight_sample_from_i420(frame, 64, 64)
        assert sample.r > 200
        assert sample.g < 60
        assert sample.b < 60
        assert sample.hex_color.startswith("#")

    def test_extract_dark_black_frame_returns_low_brightness(self) -> None:
        frame = _make_solid_i420_frame(64, 64, y=16, u=128, v=128)
        sample = extract_ambilight_sample_from_i420(frame, 64, 64)
        assert sample.r == 0
        assert sample.g == 0
        assert sample.b == 0
        assert sample.brightness == 1
        assert sample.hex_color == "#000000"

    def test_buffer_too_small_raises(self) -> None:
        with pytest.raises(ValueError, match="I420 buffer too small"):
            extract_ambilight_sample_from_i420(b"short", 64, 64)


class TestPrivacyShutterAndSubprocessCancellation:
    async def test_privacy_shutter_closed_refuses_capture(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gpio = tmp_path / "gpio5_value"
        gpio.write_text("0\n", encoding="utf-8")
        assert is_privacy_shutter_closed(gpio) is True

        spawn_called = False

        async def _fail_if_spawned(*args: Any, **kwargs: Any) -> Any:
            nonlocal spawn_called
            spawn_called = True
            raise AssertionError("subprocess must not spawn when privacy shutter is closed")

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fail_if_spawned)
        adapter = GStreamerCameraAdapter(privacy_gpio_path=gpio)
        assert await adapter.capture_jpeg_snapshot(AmbilightCrop()) is None
        assert await adapter.capture_i420_frame(AmbilightCrop()) is None
        assert spawn_called is False

        # Open privacy shutter ("1")
        gpio.write_text("1\n", encoding="utf-8")
        assert is_privacy_shutter_closed(gpio) is False

    async def test_cancelled_capture_kills_and_reaps_gst_subprocess(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        killed = False
        reaped = False
        started = asyncio.Event()
        hang = asyncio.Event()

        class _HangingProc:
            def __init__(self) -> None:
                self.returncode: int | None = None

            def kill(self) -> None:
                nonlocal killed
                killed = True
                self.returncode = -9
                hang.set()

            async def communicate(self) -> tuple[bytes, bytes]:
                nonlocal reaped
                started.set()
                await hang.wait()
                reaped = True
                return (b"", b"")

        async def _fake_exec(*args: Any, **kwargs: Any) -> _HangingProc:
            return _HangingProc()

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)
        adapter = GStreamerCameraAdapter(
            device="test_device",
            timeout_s=10.0,
            privacy_gpio_path=tmp_path / "absent_gpio",
        )
        task = asyncio.create_task(adapter.capture_i420_frame(AmbilightCrop()))
        await asyncio.wait_for(started.wait(), timeout=1.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert killed is True
        assert reaped is True


class TestAmbilightController:
    async def test_discovery_ledger_snapshot_non_retained_and_tombstone_on_disable(
        self, tmp_path: Path
    ) -> None:
        mqtt = FakeMqtt()
        cam = FakeCameraAdapter(y=110, u=80, v=220)
        state_file = tmp_path / "ambilight.json"
        ledger = RetainedTopicLedger("living_room", tmp_path / "owned-topics.json")
        await ledger.async_load()

        ctrl = AmbilightController(
            mqtt=mqtt,
            panel="living_room",
            camera=cam,
            state_path=state_file,
            default_enabled=False,
            owned_topics=ledger,
        )
        await ctrl.start()

        # Verify 4-part state topic is claimed in RetainedTopicLedger and device model is "Control"
        assert "brilliant/living_room/ambilight/state" in ledger.topics
        sw_pub = next(
            p
            for p in mqtt.published
            if p[0] == "homeassistant/switch/brilliant_living_room_ambilight_enabled/config"
        )
        sw_cfg = json.loads(sw_pub[1])
        assert sw_cfg["device"]["model"] == "Control"
        assert sw_cfg["state_topic"] == "brilliant/living_room/ambilight/state"

        # Trigger snapshot -> published with retain=False!
        mqtt.published.clear()
        await ctrl.on_mqtt_command("brilliant/living_room/ambilight/set_snapshot", "PRESS")
        snaps = [p for p in mqtt.published if p[0] == "brilliant/living_room/ambilight/snapshot"]
        assert len(snaps) == 1
        assert snaps[0][2] is False  # retain=False!

        # Update crop via dataclasses.replace path
        await ctrl.on_mqtt_command("brilliant/living_room/ambilight/set_crop_top", "240")
        await ctrl.on_mqtt_command("brilliant/living_room/ambilight/set_crop_left", "320")
        assert ctrl.crop.top == 240
        assert ctrl.crop.left == 320

        # Sample once -> color published with retain=False!
        mqtt.published.clear()
        sample = await ctrl.sample_once()
        assert sample is not None
        color_pubs = [p for p in mqtt.published if p[0] == "brilliant/living_room/ambilight/color"]
        assert len(color_pubs) == 1
        assert color_pubs[0][2] is False  # retain=False!

        # Enable then disable -> clears snapshot topic with retained empty tombstone
        await ctrl.on_mqtt_command("brilliant/living_room/ambilight/set_enabled", "ON")
        mqtt.published.clear()
        await ctrl.on_mqtt_command("brilliant/living_room/ambilight/set_enabled", "OFF")
        tombstones = [
            p for p in mqtt.published if p == ("brilliant/living_room/ambilight/snapshot", "", True)
        ]
        assert len(tombstones) == 1

        # Shutdown also clears snapshot topic with retained empty tombstone
        mqtt.published.clear()
        await ctrl.shutdown()
        assert ("brilliant/living_room/ambilight/snapshot", "", True) in mqtt.published


class TestMultiZoneRPMLightHelpers:
    def test_52_panel_coords_and_nanoleaf_v2_packet(self) -> None:
        coords = build_52_panel_coords()
        assert len(coords) == 52
        boxes = build_depth_glow_boxes(320, 184)
        assert len(boxes) == 52

        frame = _make_solid_i420_frame(320, 184, y=22, u=120, v=165)
        y_size = 320 * 184
        uv_size = 160 * 92
        mv = memoryview(frame)
        _pid, ey_idx, euv_idx, _iy_idx, _iuv_idx = boxes[0]
        r, g, b = sample_box_fast(
            mv[:y_size],
            mv[y_size : y_size + uv_size],
            mv[y_size + uv_size : y_size + 2 * uv_size],
            ey_idx,
            euv_idx,
        )
        assert r > g > 0
        assert b > 0

        pkt = encode_nanoleaf_v2_packet([(r, g, b)] * 52, transition_ds=1)
        assert len(pkt) == 2 + 52 * 8


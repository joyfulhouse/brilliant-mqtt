"""Native On-Panel Atmosphere Visual Curator, Google Cast v2 Sender & Virtual Switch.

Hosts a native Brilliant touchscreen light/slider peripheral (``PeripheralType.LIGHT = 27``)
that turns any Brilliant panel + Castable TV (with optional coupled AmbiLED) into a
privacy-gated conversational ambient photography display:

1. **One-Button ON Experience:**
   - Wakes and streams high-resolution photography directly to a Google Cast / Android TV
     device over local TLS ``:8009`` (``CC1AD845`` Default Media Receiver, zero external
     dependencies) and publishes the active photo URL/metadata over MQTT.
   - Optionally turns on coupled AmbiLED (local bus peripheral and/or ``AmbilightController``)
     at ``85%`` automatically when configured/present, while working 100% standalone when
     only a Brilliant panel and Castable TV are present.
2. **Brightness Slider Controls Next-Photo Time Delay (``5s`` .. ``300s`` / ``5m``):**
   - Maps ``1%..100%`` along a smooth perceptual curve (``1% = 300s``, ``25% = 120s``,
     ``50% = 45s`` default, ``75% = 15s``, ``100% = 5s``).
3. **Fail-Closed Camera-Cover Microphone Privacy Gate:**
   - Captures room audio via non-exclusive ALSA ``arecord -D default`` **only** when the
     panel's physical camera shutter is open (``privacy_toggle == "0"``) and its
     ``PrivacyEpoch`` remains unbroken. Closing the shutter immediately aborts capture,
     zeroes buffered PCM bytes, and clears cached conversational state.
4. **Conversational Curation + Optional Personal Photo Shared Album Link:**
   - Ranks rolling conversation topics for the next scheduled time slot while switching
     immediately on explicit cue phrases or Mode 1 Smart Q&A answers.
   - Supports zero-OAuth Google Photos / iCloud shared album links (with optional cached
     Gemini Vision auto-tagging), single-key standalone Gemini multimodal audio+curation,
     optional Pexels HD stock photos, and key-free Wikipedia / Wikimedia Commons fallback.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import ipaddress
import json
import logging
import math
import re
import socket
import ssl
import struct
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar

from brilliant_mqtt.ambilight import (
    PrivacyEpoch as PrivacyEpoch,
)
from brilliant_mqtt.ambilight import (
    _GuardedStatePublisher,
    _privacy_capture_blocked,
    _read_privacy_epoch,
    is_privacy_shutter_closed,
)
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

_T = TypeVar("_T")

logger = logging.getLogger(__name__)

VIRTUAL_LIGHT_PERIPHERAL_TYPE = 27
MIN_PHOTO_DELAY_S = 5.0
MAX_PHOTO_DELAY_S = 300.0
DEFAULT_SLIDER_PCT = 50
DEFAULT_AMBILED_BRIGHTNESS_PCT = 85

_CAST_DEFAULT_APP_ID = "CC1AD845"
_NS_CONNECTION = "urn:x-cast:com.google.cast.tp.connection"
_NS_HEARTBEAT = "urn:x-cast:com.google.cast.tp.heartbeat"
_NS_RECEIVER = "urn:x-cast:com.google.cast.receiver"
_NS_MEDIA = "urn:x-cast:com.google.cast.media"

_ALLOWED_ALBUM_HOSTS = frozenset(
    {
        "photos.app.goo.gl",
        "photos.google.com",
        "www.icloud.com",
    }
)
_ICLOUD_SHARED_HOST_RE = re.compile(r"^p\d+-sharedstreams\.icloud\.com$", re.IGNORECASE)
_GPHOTOS_TUPLE_RE = re.compile(
    r'\["(https://lh3\.googleusercontent\.com/(?:pw/)?[A-Za-z0-9_-]{20,})",\s*(\d+),\s*(\d+)'
)
_GPHOTOS_URL_RE = re.compile(r"(https://lh3\.googleusercontent\.com/pw/[A-Za-z0-9_-]{20,})")

_IMMEDIATE_CUE_RE = re.compile(
    r"\b(?:next\s+(?:photo|picture|image|slide)|change\s+(?:the\s+)?(?:photo|picture|image)|"
    r"skip\s+(?:this\s+)?(?:photo|picture)|different\s+(?:photo|picture)|"
    r"show\s+(?:me|us)\s+(?:a\s+|an\s+|some\s+)?([a-z0-9][a-z0-9 '\-]{2,40}))\b",
    re.IGNORECASE,
)

_DEFAULT_AMBIENT_TOPICS: tuple[str, ...] = (
    "mountain waterfall autumn landscape",
    "northern lights fjord night sky",
    "coastal sunset ocean cliffs",
    "misty redwood forest morning light",
    "alpine lake reflection snow peaks",
    "japanese zen garden cherry blossoms",
)

_STOP_WORDS = frozenset(
    {
        "about",
        "actually",
        "after",
        "again",
        "almost",
        "already",
        "also",
        "always",
        "among",
        "another",
        "anybody",
        "anyone",
        "anything",
        "anyway",
        "around",
        "because",
        "before",
        "being",
        "believe",
        "between",
        "both",
        "bring",
        "called",
        "cannot",
        "change",
        "close",
        "comes",
        "coming",
        "could",
        "course",
        "doing",
        "during",
        "early",
        "either",
        "enough",
        "every",
        "everyone",
        "everything",
        "first",
        "found",
        "friend",
        "friends",
        "getting",
        "going",
        "great",
        "guess",
        "happen",
        "having",
        "heard",
        "hello",
        "house",
        "however",
        "image",
        "images",
        "inside",
        "instead",
        "just",
        "know",
        "later",
        "least",
        "leave",
        "light",
        "lights",
        "like",
        "little",
        "living",
        "looking",
        "looks",
        "make",
        "makes",
        "making",
        "maybe",
        "might",
        "minute",
        "minutes",
        "month",
        "morning",
        "mostly",
        "much",
        "never",
        "next",
        "night",
        "nothing",
        "nowhere",
        "often",
        "other",
        "others",
        "outside",
        "panel",
        "people",
        "person",
        "photo",
        "photos",
        "picture",
        "pictures",
        "place",
        "places",
        "please",
        "pretty",
        "probably",
        "question",
        "quite",
        "really",
        "right",
        "screen",
        "second",
        "seeing",
        "seems",
        "should",
        "since",
        "small",
        "somebody",
        "someone",
        "something",
        "sometimes",
        "still",
        "stuff",
        "talking",
        "thank",
        "thanks",
        "their",
        "there",
        "these",
        "thing",
        "things",
        "think",
        "thinking",
        "those",
        "though",
        "thought",
        "three",
        "through",
        "today",
        "together",
        "tomorrow",
        "tonight",
        "under",
        "until",
        "using",
        "usually",
        "very",
        "want",
        "wanted",
        "watch",
        "watching",
        "where",
        "which",
        "while",
        "world",
        "would",
        "years",
        "yesterday",
    }
)

_BANNED_KEYWORDS = frozenset(
    {
        "pause",
        "paused",
        "test",
        "testing",
        "screen",
        "display",
        "website",
        "button",
        "microphone",
        "pill",
        "pills",
        "medicine",
        "medication",
        "hospital",
        "surgery",
        "disease",
        "symptom",
        "symptoms",
        "nothing",
        "anything",
        "something",
        "everything",
    }
)


# ---------------------------------------------------------------------------
# Topic Retirement Helper
# ---------------------------------------------------------------------------


async def retire_atmosphere_topics(
    mqtt: MqttClient, panel: str, owned_topics: RetainedTopicLedger
) -> None:
    """Retire only Atmosphere's recorded topics when installation disables it."""
    uid = f"brilliant_{panel}_atmosphere"
    topics = {
        f"homeassistant/{component}/{uid}_{suffix}/config"
        for component, suffix in (
            ("switch", "enabled"),
            ("number", "delay_s"),
            ("button", "next_photo"),
            ("sensor", "topic"),
        )
    }
    topics.add(state_topic(panel, "atmosphere"))
    for topic in sorted(topics & owned_topics.topics):
        await owned_topics.async_clear(mqtt, topic)


# ---------------------------------------------------------------------------
# Pure Slider <-> Time-Delay Curve & Thrift RoomAssignment Decoder
# ---------------------------------------------------------------------------

_SLIDER_CURVE_POINTS: tuple[tuple[float, float], ...] = (
    (1.0, MAX_PHOTO_DELAY_S),
    (25.0, 120.0),
    (50.0, 45.0),
    (75.0, 15.0),
    (100.0, MIN_PHOTO_DELAY_S),
)


def slider_pct_to_delay_s(pct: float) -> float:
    """Map brightness slider percentage (``1..100``) to photo delay (``300s..5s``)."""
    if not math.isfinite(pct):
        return 45.0
    clamped = max(1.0, min(100.0, float(pct)))
    for idx in range(len(_SLIDER_CURVE_POINTS) - 1):
        p0, d0 = _SLIDER_CURVE_POINTS[idx]
        p1, d1 = _SLIDER_CURVE_POINTS[idx + 1]
        if p0 <= clamped <= p1:
            t = (clamped - p0) / (p1 - p0)
            return round(d0 + t * (d1 - d0), 1)
    return MIN_PHOTO_DELAY_S


def delay_s_to_slider_pct(delay_s: float) -> int:
    """Map photo delay in seconds (``5..300``) back to slider percentage (``1..100``)."""
    if not math.isfinite(delay_s):
        return DEFAULT_SLIDER_PCT
    clamped = max(MIN_PHOTO_DELAY_S, min(MAX_PHOTO_DELAY_S, float(delay_s)))
    for idx in range(len(_SLIDER_CURVE_POINTS) - 1):
        p0, d0 = _SLIDER_CURVE_POINTS[idx]
        p1, d1 = _SLIDER_CURVE_POINTS[idx + 1]
        if d1 <= clamped <= d0:
            t = (d0 - clamped) / (d0 - d1)
            return max(1, min(100, round(p0 + t * (p1 - p0))))
    return DEFAULT_SLIDER_PCT


def decode_room_assignment_ids(raw_b64: str) -> tuple[str, ...]:
    """Decode a Base64-encoded Brilliant Thrift ``RoomAssignment`` binary payload.

    Live-verified binary wire format on panel firmware:
    ``0x0f (LIST) 0x00 0x01 (field 1) 0x0b (STRING) <count:i32> (<len:i32> <utf8>)* 0x00``
    """
    text = raw_b64.strip()
    if not text:
        return ()
    try:
        buf = base64.b64decode(text, validate=True)
    except Exception:
        return ()
    if len(buf) < 9 or buf[0] != 0x0F:
        return ()
    field_id = struct.unpack_from(">h", buf, 1)[0]
    elem_type = buf[3]
    if field_id != 1 or elem_type != 0x0B:
        return ()
    count = struct.unpack_from(">i", buf, 4)[0]
    if count <= 0 or count > 32:
        return ()
    pos = 8
    rooms: list[str] = []
    for _ in range(count):
        if pos + 4 > len(buf):
            return ()
        slen = struct.unpack_from(">i", buf, pos)[0]
        pos += 4
        if slen < 0 or slen > 256 or pos + slen > len(buf):
            return ()
        room_id = buf[pos : pos + slen].decode("utf-8", errors="strict").strip()
        pos += slen
        if room_id:
            rooms.append(room_id)
    return tuple(rooms)


def discover_panel_room_ids(devices: Iterable[BrilliantDevice]) -> tuple[str, ...]:
    """Extract the local panel's primary ``room_ids`` from its config peripherals."""
    preferred = ("device_config_peripheral", "art_config_peripheral")
    by_id = {d.peripheral_id: d for d in devices if d.device_id != "ble_mesh"}
    for pid in preferred:
        dev = by_id.get(pid)
        if dev is not None and "room_assignment" in dev.variables:
            rooms = decode_room_assignment_ids(dev.variables["room_assignment"].value)
            if rooms:
                return rooms
    for dev in by_id.values():
        if "room_assignment" in dev.variables:
            rooms = decode_room_assignment_ids(dev.variables["room_assignment"].value)
            if rooms:
                return rooms
    return ()


def discover_ambiled_target(
    devices: Iterable[BrilliantDevice], configured_id: str
) -> tuple[str, str] | None:
    """Resolve ``(device_id, peripheral_id)`` for an optional coupled AmbiLED light."""
    spec = configured_id.strip()
    if not spec or spec.lower() in ("none", "off", "0", "false"):
        return None
    panel_devs = [d for d in devices if d.device_id != "ble_mesh"]
    if spec.lower() != "auto":
        for dev in panel_devs:
            if dev.peripheral_id == spec:
                return (dev.device_id, dev.peripheral_id)
        return None
    candidates = ("tv_ambiled", "TV AmbiLED")
    for dev in panel_devs:
        if dev.peripheral_id in candidates or dev.name in candidates:
            return (dev.device_id, dev.peripheral_id)
    return None


# ---------------------------------------------------------------------------
# Data Models & Adapter Protocols
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CuratedPhoto:
    """Single displayable high-resolution image and caption metadata."""

    url: str
    topic: str
    source: str = "stock"
    is_answer: bool = False
    answer_label: str | None = None

    @property
    def display_title(self) -> str:
        if self.is_answer and self.answer_label:
            return f"Answer: {self.answer_label}"
        if self.source == "personal":
            return f"Personal • {self.topic.title()}"
        return self.topic.title()

    @property
    def subtitle(self) -> str:
        if self.source == "personal":
            return "Atmosphere • Personal Photo"
        if self.is_answer:
            return "Atmosphere • Smart Q&A"
        return "Atmosphere"


@dataclass(frozen=True)
class CuratedTopicBatch:
    """Ranked conversational topic and its resolved photo set."""

    topic: str
    photos: tuple[CuratedPhoto, ...]
    score: float = 7.0
    topic_root: str = ""
    is_answer: bool = False
    answer_label: str | None = None
    immediate: bool = False


class PhotoDisplaySink(Protocol):
    """Protocol for casting or displaying photos on a TV."""

    async def show_photo(self, photo: CuratedPhoto) -> bool:
        """Display *photo* on the target TV (waking it if needed)."""
        ...

    async def turn_off(self) -> None:
        """Stop the active receiver session cleanly."""
        ...


class MicrophoneCaptureAdapter(Protocol):
    """Protocol for privacy-gated utterance capture from the panel microphone."""

    async def capture_utterance_wav(self, epoch: PrivacyEpoch | None) -> bytes | None:
        """Capture one spoken utterance as 16 kHz mono WAV bytes, or ``None``."""
        ...


class CuratorAdapter(Protocol):
    """Protocol for conversational STT + visual photo curation."""

    @property
    def personal_photo_count(self) -> int:
        """Number of loaded personal album photos."""
        ...

    async def set_personal_album_url(self, url: str) -> int:
        """Configure or clear the shared personal photo album URL."""
        ...

    async def curate_audio(
        self,
        wav_bytes: bytes,
        context: str,
        recent_keywords: Sequence[str],
    ) -> tuple[str, CuratedTopicBatch | None]:
        """Transcribe *wav_bytes* and curate a visual batch."""
        ...

    async def curate_text(
        self,
        transcript: str,
        context: str,
        recent_keywords: Sequence[str],
        *,
        direct_topic: bool = False,
    ) -> CuratedTopicBatch | None:
        """Curate a visual batch directly from text."""
        ...

    async def next_ambient_batch(self, recent_keywords: Sequence[str]) -> CuratedTopicBatch | None:
        """Return the next ambient fallback batch (personal album first, else scenic)."""
        ...


# ---------------------------------------------------------------------------
# Pure-Stdlib Google Cast v2 Protobuf Framing & TLS Client
# ---------------------------------------------------------------------------


def _encode_varint(value: int) -> bytes:
    out = bytearray()
    v = value & 0xFFFFFFFFFFFFFFFF
    while v > 0x7F:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v & 0x7F)
    return bytes(out)


def _decode_varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while pos < len(buf):
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")
    raise ValueError("truncated varint")


def encode_cast_message(
    source_id: str,
    destination_id: str,
    namespace: str,
    payload_utf8: str,
) -> bytes:
    """Encode a length-prefixed Google ``CastMessage`` protobuf packet."""
    src_b = source_id.encode("utf-8")
    dst_b = destination_id.encode("utf-8")
    ns_b = namespace.encode("utf-8")
    pay_b = payload_utf8.encode("utf-8")
    body = b"".join(
        (
            b"\x08\x00",  # field 1: protocol_version = CASTV2_1_0 (0)
            b"\x12" + _encode_varint(len(src_b)) + src_b,  # field 2: source_id
            b"\x1a" + _encode_varint(len(dst_b)) + dst_b,  # field 3: destination_id
            b"\x22" + _encode_varint(len(ns_b)) + ns_b,  # field 4: namespace
            b"\x28\x00",  # field 5: payload_type = STRING (0)
            b"\x32" + _encode_varint(len(pay_b)) + pay_b,  # field 6: payload_utf8
        )
    )
    return struct.pack(">I", len(body)) + body


def decode_cast_message_body(body: bytes) -> tuple[str, str, str, str]:
    """Decode ``(source_id, destination_id, namespace, payload_utf8)`` from protobuf bytes."""
    pos = 0
    source_id = ""
    destination_id = ""
    namespace = ""
    payload_utf8 = ""
    while pos < len(body):
        tag, pos = _decode_varint(body, pos)
        field_no = tag >> 3
        wire_type = tag & 0x07
        if wire_type == 0:
            _, pos = _decode_varint(body, pos)
        elif wire_type == 2:
            length, pos = _decode_varint(body, pos)
            if pos + length > len(body):
                raise ValueError("truncated length-delimited field")
            raw = body[pos : pos + length]
            pos += length
            if field_no == 2:
                source_id = raw.decode("utf-8", errors="replace")
            elif field_no == 3:
                destination_id = raw.decode("utf-8", errors="replace")
            elif field_no == 4:
                namespace = raw.decode("utf-8", errors="replace")
            elif field_no == 6:
                payload_utf8 = raw.decode("utf-8", errors="replace")
        else:
            raise ValueError(f"unsupported protobuf wire type {wire_type}")
    return source_id, destination_id, namespace, payload_utf8


def _recv_exact(sock: ssl.SSLSocket, count: int) -> bytes:
    buf = bytearray()
    while len(buf) < count:
        chunk = sock.recv(count - len(buf))
        if not chunk:
            raise EOFError("Cast TLS connection closed")
        buf.extend(chunk)
    return bytes(buf)


class CastV2PhotoClient:
    """Zero-dependency Google Cast v2 Default Media Receiver photo sender."""

    def __init__(
        self,
        host: str,
        port: int = 8009,
        *,
        connect_timeout_s: float = 4.0,
        io_timeout_s: float = 6.0,
    ) -> None:
        self._host = host.strip()
        self._port = port
        self._connect_timeout_s = connect_timeout_s
        self._io_timeout_s = io_timeout_s
        self._io_lock = asyncio.Lock()
        self._sock: ssl.SSLSocket | None = None
        self._transport_id: str | None = None
        self._session_id: str | None = None
        self._req_id = 1

    def _next_req_id(self) -> int:
        rid = self._req_id
        self._req_id += 1
        return rid

    async def show_photo(self, photo: CuratedPhoto) -> bool:
        if not self._host or not photo.url.startswith("https://"):
            return False
        async with self._io_lock:
            return await asyncio.to_thread(self._show_photo_sync, photo)

    async def turn_off(self) -> None:
        if not self._host:
            return
        async with self._io_lock:
            await asyncio.to_thread(self._turn_off_sync)

    def _close_socket_sync(self) -> None:
        sock = self._sock
        self._sock = None
        self._transport_id = None
        self._session_id = None
        if sock is not None:
            with contextlib.suppress(Exception):
                sock.close()

    def _ensure_session_sync(self) -> tuple[ssl.SSLSocket, str]:
        if self._sock is not None and self._transport_id is not None:
            return self._sock, self._transport_id

        self._close_socket_sync()
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        raw = socket.create_connection((self._host, self._port), timeout=self._connect_timeout_s)
        raw.settimeout(self._io_timeout_s)
        tls = ctx.wrap_socket(raw, server_hostname=self._host)
        self._sock = tls

        tls.sendall(
            encode_cast_message(
                "sender-0",
                "receiver-0",
                _NS_CONNECTION,
                json.dumps({"type": "CONNECT", "origin": {}}),
            )
        )
        launch_req = self._next_req_id()
        tls.sendall(
            encode_cast_message(
                "sender-0",
                "receiver-0",
                _NS_RECEIVER,
                json.dumps(
                    {
                        "type": "LAUNCH",
                        "appId": _CAST_DEFAULT_APP_ID,
                        "requestId": launch_req,
                    }
                ),
            )
        )

        deadline = time.monotonic() + self._io_timeout_s
        transport_id: str | None = None
        session_id: str | None = None
        while time.monotonic() < deadline:
            hdr = _recv_exact(tls, 4)
            msg_len = struct.unpack(">I", hdr)[0]
            if msg_len > 262_144:
                raise ValueError("oversized Cast packet")
            body = _recv_exact(tls, msg_len)
            _src, _dst, ns, payload = decode_cast_message_body(body)
            if ns == _NS_HEARTBEAT and '"PING"' in payload:
                tls.sendall(
                    encode_cast_message("sender-0", "receiver-0", _NS_HEARTBEAT, '{"type":"PONG"}')
                )
                continue
            if ns == _NS_RECEIVER and payload:
                data = json.loads(payload)
                if data.get("type") == "RECEIVER_STATUS":
                    apps = data.get("status", {}).get("applications", [])
                    for app in apps:
                        if app.get("appId") == _CAST_DEFAULT_APP_ID:
                            transport_id = str(app.get("transportId") or "")
                            session_id = str(app.get("sessionId") or "")
                            break
                    if transport_id:
                        break

        if not transport_id:
            self._close_socket_sync()
            raise TimeoutError("Cast Default Media Receiver did not report transportId")

        tls.sendall(
            encode_cast_message(
                "sender-0",
                transport_id,
                _NS_CONNECTION,
                json.dumps({"type": "CONNECT", "origin": {}}),
            )
        )
        self._transport_id = transport_id
        self._session_id = session_id
        return tls, transport_id

    def _show_photo_sync(self, photo: CuratedPhoto) -> bool:
        for attempt in range(2):
            try:
                tls, transport_id = self._ensure_session_sync()
                load_req = self._next_req_id()
                content_type = "image/png" if photo.url.lower().endswith(".png") else "image/jpeg"
                load_payload = {
                    "type": "LOAD",
                    "requestId": load_req,
                    "autoplay": True,
                    "media": {
                        "contentId": photo.url,
                        "contentType": content_type,
                        "streamType": "NONE",
                        "metadata": {
                            "metadataType": 4,
                            "title": photo.display_title,
                            "subtitle": photo.subtitle,
                        },
                    },
                }
                tls.sendall(
                    encode_cast_message(
                        "sender-0",
                        transport_id,
                        _NS_MEDIA,
                        json.dumps(load_payload),
                    )
                )
                return True
            except Exception as exc:
                self._close_socket_sync()
                if attempt == 1:
                    logger.warning("Cast photo display failed on %s: %s", self._host, exc)
        return False

    def _turn_off_sync(self) -> None:
        sock = self._sock
        session_id = self._session_id
        if sock is not None:
            with contextlib.suppress(Exception):
                stop_msg: dict[str, Any] = {
                    "type": "STOP",
                    "requestId": self._next_req_id(),
                }
                if session_id:
                    stop_msg["sessionId"] = session_id
                sock.sendall(
                    encode_cast_message(
                        "sender-0",
                        "receiver-0",
                        _NS_RECEIVER,
                        json.dumps(stop_msg),
                    )
                )
        self._close_socket_sync()


# ---------------------------------------------------------------------------
# SSRF-Safe Personal Shared Photo Album Loader & Semantic Matcher
# ---------------------------------------------------------------------------


def is_allowed_personal_album_url(url: str) -> bool:
    """Return ``True`` only for HTTPS Google Photos or iCloud public shared album links."""
    parsed = urllib.parse.urlparse(url.strip())
    if parsed.scheme != "https" or not parsed.hostname:
        return False
    host = parsed.hostname.lower()
    with contextlib.suppress(ValueError):
        ip = ipaddress.ip_address(host)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            return False
    return host in _ALLOWED_ALBUM_HOSTS or bool(_ICLOUD_SHARED_HOST_RE.match(host))


class _SafeAlbumRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject redirects to any non-allowlisted host."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        if not is_allowed_personal_album_url(newurl):
            raise urllib.error.URLError(f"Blocked redirect to disallowed album host: {newurl}")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def parse_google_photos_album_html(html: str, *, max_photos: int = 100) -> list[str]:
    """Extract deduplicated high-resolution ``=w1920-h1080`` URLs from shared album HTML."""
    landscape: list[str] = []
    other: list[str] = []
    seen: set[str] = set()

    for match in _GPHOTOS_TUPLE_RE.finditer(html):
        base_url = match.group(1)
        if base_url in seen:
            continue
        seen.add(base_url)
        width = int(match.group(2))
        height = int(match.group(3))
        full_url = f"{base_url}=w1920-h1080"
        if width >= int(height * 0.8):
            landscape.append(full_url)
        else:
            other.append(full_url)
        if len(landscape) + len(other) >= max_photos:
            break

    if not seen:
        for match in _GPHOTOS_URL_RE.finditer(html):
            base_url = match.group(1)
            if base_url not in seen:
                seen.add(base_url)
                landscape.append(f"{base_url}=w1920-h1080")
                if len(landscape) >= max_photos:
                    break

    return (landscape + other)[:max_photos]


@dataclass
class IndexedPersonalPhoto:
    """Cached personal album photo with optional AI visual tags."""

    photo_id: str
    url: str
    thumb_url: str
    caption: str = ""
    tags: tuple[str, ...] = ()


class PersonalAlbumStore:
    """Loads public shared photo albums and matches photos to spoken topics."""

    def __init__(self, cache_path: Path | None = None) -> None:
        self._cache_path = cache_path
        self._photos: list[IndexedPersonalPhoto] = []
        self._cursor = 0
        self._tag_cache: dict[str, dict[str, Any]] = {}
        self._load_tag_cache()

    @property
    def count(self) -> int:
        return len(self._photos)

    def _load_tag_cache(self) -> None:
        if self._cache_path is None or not self._cache_path.exists():
            return
        with contextlib.suppress(Exception):
            raw = json.loads(self._cache_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                self._tag_cache = {str(k): v for k, v in raw.items() if isinstance(v, dict)}

    def _save_tag_cache(self) -> None:
        if self._cache_path is None:
            return
        with contextlib.suppress(Exception):
            atomic_write_json(self._cache_path, self._tag_cache)

    def load_from_urls(self, urls: Sequence[str]) -> int:
        items: list[IndexedPersonalPhoto] = []
        for raw_url in urls:
            u = raw_url.strip()
            if not u.startswith("https://"):
                continue
            base = u.split("=", 1)[0]
            pid = hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]
            thumb = f"{base}=w320-h240" if "googleusercontent.com" in base else u
            cached = self._tag_cache.get(pid, {})
            caption = str(cached.get("caption") or "")
            raw_tags = cached.get("tags")
            tags = (
                tuple(str(t).lower().strip() for t in raw_tags if str(t).strip())
                if isinstance(raw_tags, list)
                else ()
            )
            items.append(
                IndexedPersonalPhoto(
                    photo_id=pid,
                    url=u,
                    thumb_url=thumb,
                    caption=caption,
                    tags=tags,
                )
            )
        self._photos = items
        self._cursor = 0
        return len(self._photos)

    async def load_shared_album(
        self,
        album_url: str,
        *,
        gemini_api_key: str = "",
        gemini_model: str = "gemini-2.5-flash",
    ) -> int:
        clean_url = album_url.strip()
        if not clean_url:
            self._photos = []
            return 0
        if not is_allowed_personal_album_url(clean_url):
            raise ValueError(
                "Personal album URL must be an https:// Google Photos or iCloud shared link"
            )
        urls = await asyncio.to_thread(self._fetch_album_urls_sync, clean_url)
        count = self.load_from_urls(urls)
        if count > 0 and gemini_api_key:
            await asyncio.to_thread(
                self._index_untagged_with_gemini_sync, gemini_api_key, gemini_model
            )
        return count

    def _fetch_album_urls_sync(self, album_url: str) -> list[str]:
        parsed = urllib.parse.urlparse(album_url)
        if parsed.hostname and "icloud.com" in parsed.hostname.lower():
            return self._fetch_icloud_shared_urls_sync(album_url)

        opener = urllib.request.build_opener(_SafeAlbumRedirectHandler())
        req = urllib.request.Request(
            album_url,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux armv7l) Atmosphere/1.0",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        with opener.open(req, timeout=8.0) as resp:
            html = resp.read(2_500_000).decode("utf-8", errors="replace")
        return parse_google_photos_album_html(html)

    def _fetch_icloud_shared_urls_sync(self, album_url: str) -> list[str]:
        fragment = urllib.parse.urlparse(album_url).fragment.strip()
        token = fragment.split(";")[0].strip()
        if not re.fullmatch(r"[A-Za-z0-9]{10,32}", token):
            return []
        opener = urllib.request.build_opener(_SafeAlbumRedirectHandler())
        base_host = "p01-sharedstreams.icloud.com"
        for _ in range(2):
            stream_url = f"https://{base_host}/{token}/sharedstreams/webstream"
            req = urllib.request.Request(
                stream_url,
                data=b'{"streamCtag":null}',
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            try:
                with opener.open(req, timeout=8.0) as resp:
                    stream_data = json.loads(resp.read(1_500_000).decode("utf-8"))
                break
            except urllib.error.HTTPError as herr:
                if herr.code == 330:
                    body = json.loads(herr.read(65536).decode("utf-8"))
                    redirected = str(body.get("X-Apple-MMe-Host") or "")
                    if _ICLOUD_SHARED_HOST_RE.match(redirected):
                        base_host = redirected
                        continue
                return []
            except Exception:
                return []
        else:
            return []

        photos = stream_data.get("photos") or []
        guids = [str(p.get("photoGuid")) for p in photos[:60] if p.get("photoGuid")]
        if not guids:
            return []
        assets_url = f"https://{base_host}/{token}/sharedstreams/webasseturls"
        areq = urllib.request.Request(
            assets_url,
            data=json.dumps({"photoGuids": guids}).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with opener.open(areq, timeout=8.0) as aresp:
                asset_data = json.loads(aresp.read(1_500_000).decode("utf-8"))
        except Exception:
            return []
        items = asset_data.get("items") or {}
        locations = asset_data.get("locations") or {}
        resolved: list[str] = []
        for item in items.values():
            if not isinstance(item, dict):
                continue
            loc_key = str(item.get("url_location") or "")
            url_path = str(item.get("url_path") or "")
            loc = locations.get(loc_key) or {}
            hosts = loc.get("hosts") or []
            scheme = str(loc.get("scheme") or "https")
            if scheme == "https" and hosts and url_path:
                resolved.append(f"https://{hosts[0]}{url_path}")
        return resolved[:60]

    def _index_untagged_with_gemini_sync(self, api_key: str, model: str) -> None:
        untagged = [p for p in self._photos if not p.tags and not p.caption][:24]
        if not untagged:
            return
        for start in range(0, len(untagged), 6):
            batch = untagged[start : start + 6]
            parts: list[dict[str, Any]] = [
                {
                    "text": (
                        "For each numbered photo (0..N-1), return concise visual tags and a short "
                        "caption as JSON: "
                        '{"items":[{"idx":0,"caption":"...","tags":["dog","beach"]}]}'
                    )
                }
            ]
            loaded_indices: list[int] = []
            for idx, item in enumerate(batch):
                try:
                    req = urllib.request.Request(
                        item.thumb_url,
                        headers={"User-Agent": "Atmosphere/1.0"},
                    )
                    with urllib.request.urlopen(req, timeout=5.0) as resp:
                        img_bytes = resp.read(250_000)
                    parts.append(
                        {
                            "inline_data": {
                                "mime_type": "image/jpeg",
                                "data": base64.b64encode(img_bytes).decode("ascii"),
                            }
                        }
                    )
                    loaded_indices.append(idx)
                except Exception:
                    continue
            if not loaded_indices:
                continue
            try:
                gem_url = (
                    "https://generativelanguage.googleapis.com/v1beta/models/"
                    f"{urllib.parse.quote(model)}:generateContent?key={urllib.parse.quote(api_key)}"
                )
                greq = urllib.request.Request(
                    gem_url,
                    data=json.dumps(
                        {
                            "contents": [{"role": "user", "parts": parts}],
                            "generationConfig": {
                                "temperature": 0.1,
                                "responseMimeType": "application/json",
                            },
                        }
                    ).encode("utf-8"),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(greq, timeout=12.0) as gresp:
                    gdata = json.loads(gresp.read(250_000).decode("utf-8"))
                txt = (
                    gdata.get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "{}")
                )
                parsed_items = json.loads(txt).get("items") or []
                for entry in parsed_items:
                    pos = int(entry.get("idx", -1))
                    if 0 <= pos < len(loaded_indices):
                        photo = batch[loaded_indices[pos]]
                        cap = str(entry.get("caption") or "").strip().lower()
                        tlist = tuple(
                            str(t).strip().lower()
                            for t in (entry.get("tags") or [])
                            if str(t).strip()
                        )
                        photo.caption = cap
                        photo.tags = tlist
                        self._tag_cache[photo.photo_id] = {
                            "caption": cap,
                            "tags": list(tlist),
                        }
            except Exception as exc:
                logger.debug("Personal album Gemini indexing skipped batch: %s", exc)
        self._save_tag_cache()

    def match_topic(self, keyword: str, topic_root: str = "") -> list[CuratedPhoto]:
        """Return personal photos whose tags/caption match *keyword* or *topic_root*."""
        if not self._photos:
            return []
        terms = {
            w
            for w in re.split(r"[^a-z0-9]+", f"{keyword} {topic_root}".lower())
            if len(w) >= 3 and w not in _STOP_WORDS
        }
        if not terms:
            return []
        scored: list[tuple[int, IndexedPersonalPhoto]] = []
        for photo in self._photos:
            hay_words = set(re.split(r"[^a-z0-9]+", photo.caption.lower()))
            tag_words: set[str] = set()
            for tag in photo.tags:
                tag_words.update(re.split(r"[^a-z0-9]+", tag.lower()))
            overlap = len(terms & tag_words) * 3 + len(terms & hay_words) * 2
            if topic_root and topic_root.lower() in (tag_words | hay_words):
                overlap += 3
            if overlap >= 2:
                scored.append((overlap, photo))
        scored.sort(key=lambda item: item[0], reverse=True)
        return [CuratedPhoto(url=p.url, topic=keyword, source="personal") for _, p in scored[:5]]

    def next_ambient_photos(self, batch_size: int = 3) -> list[CuratedPhoto]:
        """Return the next slice of personal photos for idle ambient rotation."""
        if not self._photos:
            return []
        out: list[CuratedPhoto] = []
        total = len(self._photos)
        for _ in range(min(batch_size, total)):
            photo = self._photos[self._cursor % total]
            self._cursor = (self._cursor + 1) % total
            label = photo.caption.title() if photo.caption else "Memories"
            out.append(CuratedPhoto(url=photo.url, topic=label, source="personal"))
        return out


# ---------------------------------------------------------------------------
# Fail-Closed ALSA Microphone Utterance Capture
# ---------------------------------------------------------------------------


def build_wav_bytes(pcm_s16le: bytes, sample_rate: int = 16000, channels: int = 1) -> bytes:
    """Wrap raw S16_LE PCM bytes in a standard 44-byte RIFF/WAVE header."""
    byte_rate = sample_rate * channels * 2
    block_align = channels * 2
    data_len = len(pcm_s16le)
    header = struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",
        36 + data_len,
        b"WAVE",
        b"fmt ",
        16,
        1,
        channels,
        sample_rate,
        byte_rate,
        block_align,
        16,
        b"data",
        data_len,
    )
    return header + pcm_s16le


def compute_pcm_s16le_rms(chunk: bytes) -> float:
    """Compute RMS amplitude of 16-bit signed little-endian PCM bytes."""
    count = len(chunk) // 2
    if count <= 0:
        return 0.0
    samples = struct.unpack(f"<{count}h", chunk[: count * 2])
    acc = sum(s * s for s in samples)
    return math.sqrt(acc / count)


class AlsaMicrophoneAdapter:
    """Non-exclusive ALSA ``arecord`` utterance listener gated by ``PrivacyEpoch``."""

    def __init__(
        self,
        *,
        device: str = "default",
        sample_rate: int = 16000,
        speech_rms_threshold: float = 320.0,
        max_listen_window_s: float = 6.5,
        max_utterance_s: float = 6.0,
        silence_hold_s: float = 0.9,
        privacy_off: Callable[[], bool] | None = None,
        privacy_epoch: Callable[[], PrivacyEpoch] | None = None,
    ) -> None:
        self._device = device
        self._sample_rate = sample_rate
        self._speech_rms = speech_rms_threshold
        self._max_listen_window_s = max_listen_window_s
        self._max_utterance_s = max_utterance_s
        self._silence_hold_s = silence_hold_s
        self._privacy_off = privacy_off
        self._privacy_epoch = privacy_epoch
        self._lock = asyncio.Lock()

    def _blocked(self, epoch: PrivacyEpoch | None) -> bool:
        return _privacy_capture_blocked(self._privacy_off, self._privacy_epoch, epoch)

    async def capture_utterance_wav(self, epoch: PrivacyEpoch | None) -> bytes | None:
        if self._blocked(epoch):
            return None
        async with self._lock:
            if self._blocked(epoch):
                return None
            cmd = [
                "arecord",
                "-q",
                "-D",
                self._device,
                "-f",
                "S16_LE",
                "-r",
                str(self._sample_rate),
                "-c",
                "1",
                "-t",
                "raw",
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            pcm_buf = bytearray()
            pre_roll: deque[bytes] = deque(maxlen=3)
            chunk_bytes = (self._sample_rate // 10) * 2  # 100 ms = 3200 B
            speaking = False
            speech_chunks = 0
            silent_chunks = 0
            max_window_chunks = max(5, round(self._max_listen_window_s * 10))
            max_speech_chunks = max(5, round(self._max_utterance_s * 10))
            silence_limit_chunks = max(2, round(self._silence_hold_s * 10))

            try:
                assert proc.stdout is not None
                for _ in range(max_window_chunks):
                    if self._blocked(epoch):
                        pcm_buf[:] = b"\x00" * len(pcm_buf)
                        pcm_buf.clear()
                        pre_roll.clear()
                        return None
                    chunk = await asyncio.wait_for(proc.stdout.read(chunk_bytes), timeout=1.5)
                    if not chunk:
                        break
                    if self._blocked(epoch):
                        pcm_buf[:] = b"\x00" * len(pcm_buf)
                        pcm_buf.clear()
                        pre_roll.clear()
                        return None
                    rms = compute_pcm_s16le_rms(chunk)
                    if not speaking:
                        pre_roll.append(chunk)
                        if rms >= self._speech_rms:
                            speaking = True
                            for pre in pre_roll:
                                pcm_buf.extend(pre)
                            pre_roll.clear()
                            speech_chunks = 1
                            silent_chunks = 0
                    else:
                        pcm_buf.extend(chunk)
                        speech_chunks += 1
                        if rms < self._speech_rms * 0.72:
                            silent_chunks += 1
                            if silent_chunks >= silence_limit_chunks:
                                break
                        else:
                            silent_chunks = 0
                        if speech_chunks >= max_speech_chunks:
                            break
            except (TimeoutError, asyncio.TimeoutError):
                pass
            finally:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await proc.wait()

            if not speaking or speech_chunks < 3 or self._blocked(epoch):
                pcm_buf[:] = b"\x00" * len(pcm_buf)
                pcm_buf.clear()
                return None
            return build_wav_bytes(bytes(pcm_buf), sample_rate=self._sample_rate)


# ---------------------------------------------------------------------------
# Standalone + Hybrid Atmosphere Visual Curator
# ---------------------------------------------------------------------------


def extract_immediate_cue(transcript: str) -> tuple[bool, str | None]:
    """Return ``(is_immediate_cue, explicit_requested_topic)`` for spoken commands."""
    match = _IMMEDIATE_CUE_RE.search(transcript)
    if not match:
        return False, None
    explicit_subject = (match.group(1) or "").strip()
    return True, explicit_subject or None


def extract_fallback_keywords(transcript: str) -> list[str]:
    """Deterministic zero-API-key noun/keyword extractor."""
    words = [
        w
        for w in re.split(r"[^a-z0-9]+", transcript.lower())
        if len(w) >= 4 and w not in _STOP_WORDS and w not in _BANNED_KEYWORDS and not w.isdigit()
    ]
    seen: set[str] = set()
    out: list[str] = []
    for word in words:
        if word not in seen:
            seen.add(word)
            out.append(word)
    return out[:3]


class HttpAtmosphereCurator:
    """Self-contained curator supporting Gemini Audio+Text, Pexels, Wikimedia & Personal Albums."""

    def __init__(
        self,
        *,
        gemini_api_key: str = "",
        gemini_model: str = "gemini-2.5-flash",
        pexels_api_key: str = "",
        personal_album_url: str = "",
        stt_url: str = "",
        curate_url: str = "",
        album_cache_path: Path | None = None,
    ) -> None:
        self._gemini_api_key = gemini_api_key.strip()
        self._gemini_model = gemini_model.strip() or "gemini-2.5-flash"
        self._pexels_api_key = pexels_api_key.strip()
        self._initial_album_url = personal_album_url.strip()
        self._stt_url = stt_url.strip()
        self._curate_url = curate_url.strip()
        self._album_store = PersonalAlbumStore(cache_path=album_cache_path)
        self._ambient_idx = 0
        self._album_loaded = False

    @property
    def personal_photo_count(self) -> int:
        return self._album_store.count

    async def ensure_initial_album_loaded(self) -> None:
        if self._album_loaded:
            return
        self._album_loaded = True
        if self._initial_album_url:
            with contextlib.suppress(Exception):
                await self.set_personal_album_url(self._initial_album_url)

    async def set_personal_album_url(self, url: str) -> int:
        self._album_loaded = True
        self._initial_album_url = url.strip()
        return await self._album_store.load_shared_album(
            self._initial_album_url,
            gemini_api_key=self._gemini_api_key,
            gemini_model=self._gemini_model,
        )

    async def curate_audio(
        self,
        wav_bytes: bytes,
        context: str,
        recent_keywords: Sequence[str],
    ) -> tuple[str, CuratedTopicBatch | None]:
        await self.ensure_initial_album_loaded()
        if self._stt_url:
            transcript = await asyncio.to_thread(self._post_stt_sync, wav_bytes)
            if not transcript:
                return "", None
            batch = await self.curate_text(transcript, context, recent_keywords)
            return transcript, batch

        if not self._gemini_api_key:
            return "", None

        parsed = await asyncio.to_thread(
            self._call_gemini_multimodal_sync, wav_bytes, context, recent_keywords
        )
        transcript = str(parsed.get("transcript") or "").strip()
        if not transcript:
            return "", None
        immediate, explicit_topic = extract_immediate_cue(transcript)
        if explicit_topic:
            batch = await self.curate_text(
                explicit_topic, context, recent_keywords, direct_topic=True
            )
            if batch is not None:
                return transcript, CuratedTopicBatch(
                    topic=batch.topic,
                    photos=batch.photos,
                    score=10.0,
                    topic_root=batch.topic_root,
                    is_answer=batch.is_answer,
                    answer_label=batch.answer_label,
                    immediate=True,
                )
        raw_cands = parsed.get("candidates")
        if isinstance(raw_cands, list) and raw_cands:
            batch = await self._resolve_candidates(
                raw_cands, recent_keywords, force_immediate=immediate
            )
            return transcript, batch
        batch = await self.curate_text(transcript, context, recent_keywords)
        return transcript, batch

    async def curate_text(
        self,
        transcript: str,
        context: str,
        recent_keywords: Sequence[str],
        *,
        direct_topic: bool = False,
    ) -> CuratedTopicBatch | None:
        await self.ensure_initial_album_loaded()
        clean = transcript.strip()
        if len(clean) < 2:
            return None

        immediate, explicit_topic = extract_immediate_cue(clean)
        if explicit_topic and not direct_topic:
            direct_topic = True
            clean = explicit_topic
            immediate = True

        if direct_topic:
            photos = await self._fetch_photos_for_topic(clean, topic_root=clean)
            if not photos:
                return None
            return CuratedTopicBatch(
                topic=clean,
                photos=tuple(photos),
                score=10.0,
                topic_root=clean.lower(),
                immediate=True,
            )

        if self._curate_url:
            ext = await asyncio.to_thread(
                self._call_external_curate_sync, clean, context, recent_keywords
            )
            if ext is not None:
                if immediate and not ext.immediate:
                    return CuratedTopicBatch(
                        topic=ext.topic,
                        photos=ext.photos,
                        score=ext.score,
                        topic_root=ext.topic_root,
                        is_answer=ext.is_answer,
                        answer_label=ext.answer_label,
                        immediate=True,
                    )
                return ext

        candidates: list[dict[str, Any]] = []
        if self._gemini_api_key:
            with contextlib.suppress(Exception):
                parsed = await asyncio.to_thread(
                    self._call_gemini_text_sync, clean, context, recent_keywords
                )
                raw = parsed.get("candidates")
                if isinstance(raw, list):
                    candidates = [c for c in raw if isinstance(c, dict)]

        if not candidates:
            candidates = [
                {
                    "keyword": kw,
                    "topic_root": kw,
                    "score": 7,
                    "is_answer": False,
                }
                for kw in extract_fallback_keywords(clean)
            ]

        return await self._resolve_candidates(
            candidates, recent_keywords, force_immediate=immediate
        )

    async def next_ambient_batch(self, recent_keywords: Sequence[str]) -> CuratedTopicBatch | None:
        await self.ensure_initial_album_loaded()
        personal = self._album_store.next_ambient_photos(batch_size=3)
        if personal:
            return CuratedTopicBatch(
                topic=personal[0].topic,
                photos=tuple(personal),
                score=5.0,
            )
        for _ in range(len(_DEFAULT_AMBIENT_TOPICS)):
            theme = _DEFAULT_AMBIENT_TOPICS[self._ambient_idx % len(_DEFAULT_AMBIENT_TOPICS)]
            self._ambient_idx += 1
            if theme.lower() in {r.lower() for r in recent_keywords}:
                continue
            photos = await self._fetch_photos_for_topic(theme, topic_root=theme)
            if photos:
                return CuratedTopicBatch(topic=theme, photos=tuple(photos), score=5.0)
        return None

    async def _resolve_candidates(
        self,
        raw_candidates: Sequence[dict[str, Any]],
        recent_keywords: Sequence[str],
        *,
        force_immediate: bool = False,
    ) -> CuratedTopicBatch | None:
        recent_set = {k.lower().strip() for k in recent_keywords if k.strip()}
        valid: list[dict[str, Any]] = []
        for cand in raw_candidates:
            kw = str(cand.get("keyword") or "").strip().lower()
            if len(kw) < 2 or kw in _BANNED_KEYWORDS:
                continue
            root = str(cand.get("topic_root") or kw).strip().lower()
            if kw in recent_set or root in recent_set:
                continue
            valid.append(cand)
        valid.sort(
            key=lambda c: (
                1 if c.get("is_answer") else 0,
                float(c.get("score") or 5.0),
            ),
            reverse=True,
        )
        for cand in valid:
            kw = str(cand.get("keyword") or "").strip()
            root = str(cand.get("topic_root") or kw).strip().lower()
            wiki_query = str(cand.get("wiki_query") or "").strip() or None
            is_answer = bool(cand.get("is_answer"))
            answer_label = str(cand.get("answer_label") or "").strip() or None
            score = float(cand.get("score") or 7.0)
            photos = await self._fetch_photos_for_topic(
                kw,
                topic_root=root,
                wiki_query=wiki_query,
                is_answer=is_answer,
                answer_label=answer_label,
            )
            if photos:
                return CuratedTopicBatch(
                    topic=kw,
                    photos=tuple(photos),
                    score=score,
                    topic_root=root,
                    is_answer=is_answer,
                    answer_label=answer_label,
                    immediate=force_immediate or is_answer,
                )
        return None

    async def _fetch_photos_for_topic(
        self,
        keyword: str,
        *,
        topic_root: str = "",
        wiki_query: str | None = None,
        is_answer: bool = False,
        answer_label: str | None = None,
    ) -> list[CuratedPhoto]:
        if not is_answer and not wiki_query:
            personal_matches = self._album_store.match_topic(keyword, topic_root)
            if personal_matches:
                return personal_matches

        urls: list[str] = []
        source = "stock"
        if is_answer or wiki_query:
            urls = await asyncio.to_thread(self._fetch_wikipedia_urls_sync, wiki_query or keyword)
            source = "wikipedia"
        if len(urls) < 4 and self._pexels_api_key:
            pexels_urls = await asyncio.to_thread(self._fetch_pexels_urls_sync, keyword)
            for u in pexels_urls:
                if u not in urls:
                    urls.append(u)
        if not urls:
            urls = await asyncio.to_thread(self._fetch_wikimedia_commons_urls_sync, keyword)
            source = "wikimedia"
        if not urls:
            urls = await asyncio.to_thread(self._fetch_wikipedia_urls_sync, keyword)
            source = "wikipedia"
        return [
            CuratedPhoto(
                url=u,
                topic=keyword,
                source=source,
                is_answer=is_answer,
                answer_label=answer_label,
            )
            for u in urls[:5]
        ]

    def _post_stt_sync(self, wav_bytes: bytes) -> str:
        req = urllib.request.Request(
            self._stt_url,
            data=wav_bytes,
            method="POST",
            headers={"Content-Type": "audio/wav"},
        )
        with urllib.request.urlopen(req, timeout=8.0) as resp:
            data = json.loads(resp.read(131_072).decode("utf-8"))
        return str(data.get("text") or data.get("transcript") or "").strip()

    def _call_external_curate_sync(
        self,
        transcript: str,
        context: str,
        recent_keywords: Sequence[str],
    ) -> CuratedTopicBatch | None:
        personal_matches = self._album_store.match_topic(transcript, "")
        if personal_matches:
            return CuratedTopicBatch(
                topic=personal_matches[0].topic,
                photos=tuple(personal_matches),
                score=9.0,
            )
        payload: dict[str, Any] = {
            "transcript": transcript,
            "text": transcript,
            "context": context,
            "sensitivity": "eager",
            "recent_keywords": list(recent_keywords[-20:]),
            "recentKeywords": list(recent_keywords[-20:]),
        }
        if self._initial_album_url:
            payload["personal_album_url"] = self._initial_album_url
        req = urllib.request.Request(
            self._curate_url,
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=8.0) as resp:
            data = json.loads(resp.read(262_144).decode("utf-8"))
        if not data.get("matched"):
            return None
        query = str(data.get("query") or "").strip()
        topic_root = str(data.get("topic_root") or query).strip().lower()
        is_answer = bool(data.get("is_answer"))
        answer_label = str(data.get("answer_label") or "").strip() or None
        if not is_answer:
            p_hits = self._album_store.match_topic(query, topic_root)
            if p_hits:
                return CuratedTopicBatch(
                    topic=query,
                    photos=tuple(p_hits),
                    score=float(data.get("score") or 9.0),
                    topic_root=topic_root,
                )
        src = str(data.get("source") or "stock")
        imgs = [
            CuratedPhoto(
                url=str(u),
                topic=query,
                source=src,
                is_answer=is_answer,
                answer_label=answer_label,
            )
            for u in (data.get("images") or [])
            if str(u).startswith("https://")
        ]
        if not imgs:
            return None
        return CuratedTopicBatch(
            topic=query,
            photos=tuple(imgs),
            score=float(data.get("score") or 8.0),
            topic_root=topic_root,
            is_answer=is_answer,
            answer_label=answer_label,
            immediate=is_answer,
        )

    def _build_curation_prompt(self, recent_keywords: Sequence[str]) -> str:
        recent_str = ", ".join(recent_keywords[-20:])
        avoid = f"Avoid already-shown topics: [{recent_str}]. " if recent_str else ""
        return (
            "You are the visual curation engine for Atmosphere, an ambient TV photo display. "
            f"{avoid}"
            "Mode 1: If speakers ask a specific visual/factual question or debate, resolve the "
            "actual answer and set is_answer=true, answer_label='...', wiki_query='...', score=10. "
            "Mode 2: Otherwise extract up to 3 concrete visual nouns, places, animals, or subjects "
            "from the spoken conversation. Return JSON only: "
            '{"transcript":"...","candidates":[{"keyword":"2-4 word visual search",'
            '"topic_root":"noun","wiki_query":null,"is_answer":false,"answer_label":null,'
            '"score":8}]}'
        )

    def _call_gemini_multimodal_sync(
        self,
        wav_bytes: bytes,
        context: str,
        recent_keywords: Sequence[str],
    ) -> dict[str, Any]:
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{urllib.parse.quote(self._gemini_model)}:generateContent"
            f"?key={urllib.parse.quote(self._gemini_api_key)}"
        )
        body = {
            "systemInstruction": {
                "parts": [{"text": self._build_curation_prompt(recent_keywords)}]
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": (
                                f"Rolling conversation context: {context}\n"
                                "Transcribe this audio clip (empty if silence/noise) "
                                "and extract visual candidates."
                            )
                        },
                        {
                            "inline_data": {
                                "mime_type": "audio/wav",
                                "data": base64.b64encode(wav_bytes).decode("ascii"),
                            }
                        },
                    ],
                }
            ],
            "generationConfig": {
                "temperature": 0.2,
                "responseMimeType": "application/json",
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            data = json.loads(resp.read(262_144).decode("utf-8"))
        text = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "{}")
        )
        res = json.loads(text)
        return res if isinstance(res, dict) else {}

    def _call_gemini_text_sync(
        self,
        transcript: str,
        context: str,
        recent_keywords: Sequence[str],
    ) -> dict[str, Any]:
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{urllib.parse.quote(self._gemini_model)}:generateContent"
            f"?key={urllib.parse.quote(self._gemini_api_key)}"
        )
        body = {
            "systemInstruction": {
                "parts": [{"text": self._build_curation_prompt(recent_keywords)}]
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": (f"Context: {context}\nLatest transcript: {transcript}")}],
                }
            ],
            "generationConfig": {
                "temperature": 0.2,
                "responseMimeType": "application/json",
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=8.0) as resp:
            data = json.loads(resp.read(262_144).decode("utf-8"))
        text = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "{}")
        )
        res = json.loads(text)
        return res if isinstance(res, dict) else {}

    def _fetch_pexels_urls_sync(self, query: str) -> list[str]:
        if not self._pexels_api_key:
            return []
        url = (
            "https://api.pexels.com/v1/search?"
            f"query={urllib.parse.quote(query)}&per_page=6&orientation=landscape"
        )
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": self._pexels_api_key,
                "User-Agent": "BrilliantAtmosphere/1.0",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=6.0) as resp:
                data = json.loads(resp.read(262_144).decode("utf-8"))
        except Exception:
            return []
        out: list[str] = []
        for photo in data.get("photos") or []:
            src = photo.get("src") or {}
            img = str(src.get("large2x") or src.get("landscape") or src.get("large") or "")
            if img.startswith("https://"):
                out.append(img)
        return out[:5]

    def _fetch_wikimedia_commons_urls_sync(self, query: str) -> list[str]:
        params = urllib.parse.urlencode(
            {
                "action": "query",
                "generator": "search",
                "gsrnamespace": "6",
                "gsrsearch": f"{query} filetype:bitmap",
                "gsrlimit": "6",
                "prop": "imageinfo",
                "iiprop": "url|dimensions",
                "iiurlwidth": "1920",
                "format": "json",
            }
        )
        url = f"https://commons.wikimedia.org/w/api.php?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "BrilliantAtmosphere/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=6.0) as resp:
                data = json.loads(resp.read(262_144).decode("utf-8"))
        except Exception:
            return []
        pages = (data.get("query") or {}).get("pages") or {}
        urls: list[str] = []
        for page in pages.values():
            infos = page.get("imageinfo") or []
            if not infos:
                continue
            info = infos[0]
            w = int(info.get("width") or 0)
            h = int(info.get("height") or 0)
            if w < 800 or h <= 0 or w < int(h * 1.1):
                continue
            thumb = str(info.get("thumburl") or info.get("url") or "")
            if thumb.startswith("https://") and thumb.lower().endswith((".jpg", ".jpeg", ".png")):
                urls.append(thumb)
        return urls[:5]

    def _fetch_wikipedia_urls_sync(self, query: str) -> list[str]:
        search_url = (
            "https://en.wikipedia.org/w/api.php?action=query&list=search"
            f"&srsearch={urllib.parse.quote(query)}&utf8=&format=json&srlimit=2"
        )
        req = urllib.request.Request(search_url, headers={"User-Agent": "BrilliantAtmosphere/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=6.0) as resp:
                sdata = json.loads(resp.read(131_072).decode("utf-8"))
        except Exception:
            return []
        hits = (sdata.get("query") or {}).get("search") or []
        urls: list[str] = []
        for hit in hits[:2]:
            title = str(hit.get("title") or "").strip()
            if not title:
                continue
            sum_url = (
                f"https://en.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(title)}"
            )
            sreq = urllib.request.Request(
                sum_url, headers={"User-Agent": "BrilliantAtmosphere/1.0"}
            )
            try:
                with urllib.request.urlopen(sreq, timeout=5.0) as sresp:
                    pdata = json.loads(sresp.read(131_072).decode("utf-8"))
            except Exception:
                continue
            img = str(
                (pdata.get("originalimage") or {}).get("source")
                or (pdata.get("thumbnail") or {}).get("source")
                or ""
            )
            if img.startswith("https://") and img.lower().endswith((".jpg", ".jpeg", ".png")):
                urls.append(img)
        return urls


# ---------------------------------------------------------------------------
# Atmosphere Controller
# ---------------------------------------------------------------------------


class AtmosphereController:
    """Orchestrates the native Brilliant Atmosphere light/slider, Cast TV, Mic & AmbiLED."""

    def __init__(
        self,
        mqtt: MqttClient,
        panel: str,
        *,
        display: PhotoDisplaySink,
        mic: MicrophoneCaptureAdapter,
        curator: CuratorAdapter,
        bus: BusClient | None = None,
        host_factory: Callable[
            [VirtualPeripheralSpec, Callable[[str, Any], Awaitable[None]]],
            VirtualPeripheralHostProtocol,
        ]
        | None = None,
        peripheral_id: str = "tv_atmosphere",
        display_name: str = "Atmosphere",
        room_ids: tuple[str, ...] = (),
        ambiled_peripheral_id: str = "auto",
        ambiled_brightness_pct: int = DEFAULT_AMBILED_BRIGHTNESS_PCT,
        on_ambiled_coupled: Callable[[bool], Awaitable[None]] | None = None,
        state_path: Path | None = None,
        owned_topics: RetainedTopicLedger | None = None,
        privacy_off: Callable[[], bool] | None = None,
        privacy_epoch: Callable[[], PrivacyEpoch] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._mqtt = mqtt
        self._panel = panel
        self._display = display
        self._mic = mic
        self._curator = curator
        self._bus = bus
        self._host_factory = host_factory
        self._peripheral_id = peripheral_id
        self._display_name = display_name
        self._room_ids = room_ids
        self._ambiled_peripheral_id = ambiled_peripheral_id
        self._ambiled_brightness_pct = max(0, min(100, int(ambiled_brightness_pct)))
        self._on_ambiled_coupled = on_ambiled_coupled
        self._state_path = state_path
        self._owned_topics = owned_topics
        self._privacy_off = privacy_off
        self._privacy_epoch = privacy_epoch
        self._sleep = sleep

        self._enabled = False
        self._slider_pct = DEFAULT_SLIDER_PCT
        self._delay_s = slider_pct_to_delay_s(DEFAULT_SLIDER_PCT)
        self._closed = False
        self._published_topics: set[str] = set()
        self._state_publish_lock = asyncio.Lock()
        self._virtual_host: VirtualPeripheralHostProtocol | None = None
        self._coupled_ambiled_active = False

        self._current_photo: CuratedPhoto | None = None
        self._current_batch: list[CuratedPhoto] = []
        self._current_batch_idx = 0
        self._next_slot_batch: CuratedTopicBatch | None = None
        self._next_slot_epoch: PrivacyEpoch | None = None
        self._last_conversational_photo: CuratedPhoto | None = None
        self._last_conversational_epoch: PrivacyEpoch | None = None
        self._rolling_context: deque[str] = deque(maxlen=4)
        self._recent_keywords: deque[str] = deque(maxlen=25)

        self._slot_wake = asyncio.Event()
        self._slot_task: asyncio.Task[None] | None = None
        self._listen_task: asyncio.Task[None] | None = None
        self._work_tasks: set[asyncio.Task[Any]] = set()
        self._last_persisted: dict[str, Any] | None = None
        self._load_state()

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def slider_pct(self) -> int:
        return self._slider_pct

    @property
    def delay_s(self) -> float:
        return self._delay_s

    @property
    def current_photo(self) -> CuratedPhoto | None:
        if (
            self._current_photo is not None
            and self._current_photo == self._last_conversational_photo
            and self._is_blocked_by_privacy(self._last_conversational_epoch)
        ):
            self._current_photo = None
            self._last_conversational_photo = None
            self._last_conversational_epoch = None
            self._rolling_context.clear()
        return self._current_photo

    def _is_blocked_by_privacy(self, epoch: PrivacyEpoch | None) -> bool:
        return self._closed or _privacy_capture_blocked(
            self._privacy_off, self._privacy_epoch, epoch
        )

    def _is_mic_allowed_now(self) -> bool:
        return not self._closed and not is_privacy_shutter_closed(privacy_off=self._privacy_off)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        if self._state_path is None or not self._state_path.exists():
            return
        try:
            data = json.loads(self._state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to load atmosphere state from %s: %s", self._state_path, exc)
            return
        if not isinstance(data, dict):
            return
        self._last_persisted = data
        raw_pct = data.get("slider_pct")
        if isinstance(raw_pct, (int, float)) and not isinstance(raw_pct, bool):
            if math.isfinite(raw_pct):
                self._slider_pct = max(1, min(100, round(float(raw_pct))))
                self._delay_s = slider_pct_to_delay_s(self._slider_pct)

    def _save_state_if_changed(self) -> None:
        if self._state_path is None:
            return
        snap = {"slider_pct": self._slider_pct, "delay_s": self._delay_s}
        if snap == self._last_persisted:
            return
        try:
            atomic_write_json(self._state_path, snap)
            self._last_persisted = snap
        except Exception as exc:
            logger.warning("Failed to persist atmosphere state to %s: %s", self._state_path, exc)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._closed:
            return
        self._mqtt.on_message(self.on_mqtt_message)
        try:
            await self._start_virtual_peripheral()
            await self._publish_discovery()
            await self._subscribe_commands()
            await self._publish_state()
        except BaseException:
            try:
                await self.shutdown()
                if self._owned_topics is not None:
                    await retire_atmosphere_topics(self._mqtt, self._panel, self._owned_topics)
                else:
                    for topic in sorted(self._published_topics):
                        await self._mqtt.publish(topic, "", retain=True)
            except Exception:
                logger.exception("Atmosphere startup rollback failed; session must rebuild")
            raise

    async def shutdown(self) -> None:
        self._closed = True
        was_enabled = self._enabled
        self._enabled = False
        tasks = set(self._work_tasks)
        for bg in (self._slot_task, self._listen_task):
            if bg is not None:
                tasks.add(bg)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._slot_task = None
        self._listen_task = None
        if was_enabled:
            with contextlib.suppress(Exception):
                await self._display.turn_off()
            with contextlib.suppress(Exception):
                await self._couple_ambiled(False)
        if self._virtual_host is not None:
            vhost = self._virtual_host
            self._virtual_host = None
            with contextlib.suppress(Exception):
                await vhost.shutdown()

    async def _start_virtual_peripheral(self) -> None:
        if self._host_factory is None:
            return
        rooms = self._room_ids
        if not rooms and self._bus is not None:
            with contextlib.suppress(Exception):
                rooms = discover_panel_room_ids(await self._bus.get_all())
        spec = VirtualPeripheralSpec(
            peripheral_id=self._peripheral_id,
            display_name=self._display_name,
            peripheral_type=VIRTUAL_LIGHT_PERIPHERAL_TYPE,
            initial_variables={
                "on": 0,
                "intensity": self._slider_pct * 10,
                "dimmable": 1,
                "max_intensity_value": 1000,
                "minimum_dim_level": 10,
                "maximum_dim_level": 1000,
                "display_name": self._display_name,
                "mode_transition_settings": "{}",
            },
            settable_variables=frozenset(
                {
                    "on",
                    "intensity",
                    "minimum_dim_level",
                    "maximum_dim_level",
                    "display_name",
                    "mode_transition_settings",
                }
            ),
            room_ids=rooms,
        )
        vhost = self._host_factory(spec, self._on_virtual_peripheral_push)
        await vhost.start()
        self._virtual_host = vhost

    async def _sync_virtual_ui(self) -> None:
        vhost = self._virtual_host
        if vhost is None:
            return
        with contextlib.suppress(Exception):
            await vhost.set_variable("on", 1 if self._enabled else 0)
            await vhost.set_variable("intensity", self._slider_pct * 10)

    async def _on_virtual_peripheral_push(self, var_name: str, value: Any) -> None:
        if self._closed:
            return
        if self._virtual_host is not None and var_name not in ("on", "intensity"):
            with contextlib.suppress(Exception):
                await self._virtual_host.set_variable(var_name, value)
            return
        if var_name == "on":
            turn_on = str(value).strip().lower() in ("1", "true", "on")
            await self.set_enabled(turn_on, reset_default_slider=turn_on and not self._enabled)
        elif var_name == "intensity":
            try:
                raw_int = int(float(str(value).strip()))
            except (TypeError, ValueError):
                return
            if raw_int <= 0:
                await self.set_enabled(False)
            else:
                pct = max(1, min(100, round(raw_int / 10.0)))
                await self.set_slider_pct(pct, enable_if_off=True)

    # ------------------------------------------------------------------
    # State Transitions & Optional AmbiLED Coupling
    # ------------------------------------------------------------------

    async def set_enabled(self, enabled: bool, *, reset_default_slider: bool = False) -> None:
        if self._closed:
            return
        was_enabled = self._enabled
        self._enabled = bool(enabled)
        if self._enabled and not was_enabled:
            if reset_default_slider:
                self._slider_pct = DEFAULT_SLIDER_PCT
                self._delay_s = slider_pct_to_delay_s(DEFAULT_SLIDER_PCT)
                self._save_state_if_changed()
            await self._sync_virtual_ui()
            await self._couple_ambiled(True)
            await self.advance_photo()
            self._sync_background_tasks()
        elif not self._enabled and was_enabled:
            self._sync_background_tasks()
            self._current_photo = None
            self._current_batch.clear()
            self._next_slot_batch = None
            self._next_slot_epoch = None
            self._rolling_context.clear()
            await self._sync_virtual_ui()
            await self._display.turn_off()
            await self._couple_ambiled(False)
        await self._publish_state()

    async def set_slider_pct(self, pct: int, *, enable_if_off: bool = False) -> None:
        if self._closed or not math.isfinite(pct):
            return
        self._slider_pct = max(1, min(100, int(pct)))
        self._delay_s = slider_pct_to_delay_s(self._slider_pct)
        self._save_state_if_changed()
        if enable_if_off and not self._enabled:
            await self.set_enabled(True, reset_default_slider=False)
            return
        await self._sync_virtual_ui()
        self._slot_wake.set()
        await self._publish_state()

    async def set_delay_s(self, delay_s: float) -> None:
        if self._closed or not math.isfinite(delay_s):
            return
        clamped = max(MIN_PHOTO_DELAY_S, min(MAX_PHOTO_DELAY_S, float(delay_s)))
        self._delay_s = round(clamped, 1)
        self._slider_pct = delay_s_to_slider_pct(self._delay_s)
        self._save_state_if_changed()
        await self._sync_virtual_ui()
        self._slot_wake.set()
        await self._publish_state()

    async def _couple_ambiled(self, turn_on: bool) -> None:
        if self._on_ambiled_coupled is not None:
            with contextlib.suppress(Exception):
                await self._on_ambiled_coupled(turn_on)
        if self._bus is None or self._ambiled_brightness_pct <= 0:
            return
        if not turn_on and not self._coupled_ambiled_active:
            return
        try:
            devices = await self._bus.get_all()
            target = discover_ambiled_target(devices, self._ambiled_peripheral_id)
            if target is None:
                return
            dev_id, periph_id = target
            if turn_on:
                raw_int = str(self._ambiled_brightness_pct * 10)
                await self._bus.set_variables(
                    dev_id,
                    periph_id,
                    [VarSet("on", "1"), VarSet("intensity", raw_int)],
                )
                self._coupled_ambiled_active = True
            else:
                self._coupled_ambiled_active = False
                await self._bus.set_variables(
                    dev_id,
                    periph_id,
                    [VarSet("on", "0")],
                )
        except Exception as exc:
            logger.warning("Optional AmbiLED coupling skipped: %s", exc)

    # ------------------------------------------------------------------
    # Conversational Listening & Next-Slot Scheduling
    # ------------------------------------------------------------------

    def _sync_background_tasks(self) -> None:
        if self._enabled and not self._closed:
            if self._slot_task is None or self._slot_task.done():
                self._slot_task = asyncio.create_task(self._slot_loop())
            if self._listen_task is None or self._listen_task.done():
                self._listen_task = asyncio.create_task(self._listen_loop())
        else:
            for task in (self._slot_task, self._listen_task):
                if task is not None and not task.done():
                    task.cancel()
            self._slot_task = None
            self._listen_task = None

    async def _run_tracked(
        self, operation: Callable[[], Coroutine[Any, Any, _T]], stopped_result: _T
    ) -> _T:
        if self._closed:
            return stopped_result
        task = asyncio.create_task(operation())
        self._work_tasks.add(task)
        try:
            return await task
        except asyncio.CancelledError:
            if self._closed:
                return stopped_result
            raise
        finally:
            self._work_tasks.discard(task)

    async def listen_once(self) -> CuratedTopicBatch | None:
        """Capture and evaluate one privacy-gated utterance."""
        return await self._run_tracked(self._listen_once, None)

    async def _listen_once(self) -> CuratedTopicBatch | None:
        epoch = _read_privacy_epoch(self._privacy_epoch)
        if self._is_blocked_by_privacy(epoch):
            self._next_slot_batch = None
            self._next_slot_epoch = None
            self._rolling_context.clear()
            return None
        wav_bytes = await self._mic.capture_utterance_wav(epoch)
        if wav_bytes is None or self._is_blocked_by_privacy(epoch):
            return None
        context_str = " ".join(self._rolling_context)
        transcript, batch = await self._curator.curate_audio(
            wav_bytes, context_str, list(self._recent_keywords)
        )
        if self._is_blocked_by_privacy(epoch):
            self._rolling_context.clear()
            return None
        if transcript:
            self._rolling_context.append(transcript)
        if batch is None or not batch.photos:
            return None
        await self._accept_curated_batch(batch, epoch)
        return batch

    async def ingest_transcript(
        self, transcript: str, *, direct_topic: bool = False
    ) -> CuratedTopicBatch | None:
        """Curate a spoken/typed topic directly (used by MQTT ``set_topic`` and tests)."""
        return await self._run_tracked(
            lambda: self._ingest_transcript(transcript, direct_topic=direct_topic), None
        )

    async def _ingest_transcript(
        self, transcript: str, *, direct_topic: bool = False
    ) -> CuratedTopicBatch | None:
        epoch = _read_privacy_epoch(self._privacy_epoch)
        if not direct_topic and self._is_blocked_by_privacy(epoch):
            return None
        batch = await self._curator.curate_text(
            transcript,
            " ".join(self._rolling_context),
            list(self._recent_keywords),
            direct_topic=direct_topic,
        )
        if batch is None or not batch.photos:
            return None
        if not direct_topic and self._is_blocked_by_privacy(epoch):
            return None
        await self._accept_curated_batch(batch, epoch if not direct_topic else None)
        return batch

    async def _accept_curated_batch(
        self, batch: CuratedTopicBatch, epoch: PrivacyEpoch | None
    ) -> None:
        if batch.immediate:
            await self._activate_batch(batch, epoch)
            self._slot_wake.set()
            return
        if self._next_slot_batch is None or batch.score >= self._next_slot_batch.score:
            self._next_slot_batch = batch
            self._next_slot_epoch = epoch

    async def _activate_batch(self, batch: CuratedTopicBatch, epoch: PrivacyEpoch | None) -> bool:
        if epoch is not None and self._is_blocked_by_privacy(epoch):
            return False
        self._current_batch = list(batch.photos)
        self._current_batch_idx = 0
        photo = self._current_batch[0]
        self._recent_keywords.append(batch.topic.lower())
        if batch.topic_root:
            self._recent_keywords.append(batch.topic_root.lower())
        await self._display.show_photo(photo)
        self._current_photo = photo
        if epoch is not None:
            self._last_conversational_photo = photo
            self._last_conversational_epoch = epoch
        await self._publish_state(epoch)
        return True

    async def advance_photo(self) -> CuratedPhoto | None:
        """Advance to the queued conversational topic or next photo in rotation."""
        return await self._run_tracked(self._advance_photo, None)

    async def _advance_photo(self) -> CuratedPhoto | None:
        if self._next_slot_batch is not None:
            queued = self._next_slot_batch
            q_epoch = self._next_slot_epoch
            self._next_slot_batch = None
            self._next_slot_epoch = None
            if q_epoch is None or not self._is_blocked_by_privacy(q_epoch):
                if await self._activate_batch(queued, q_epoch):
                    return self._current_photo

        if self._current_batch and self._current_batch_idx + 1 < min(3, len(self._current_batch)):
            self._current_batch_idx += 1
            photo = self._current_batch[self._current_batch_idx]
            await self._display.show_photo(photo)
            self._current_photo = photo
            await self._publish_state()
            return photo

        ambient = await self._curator.next_ambient_batch(list(self._recent_keywords))
        if ambient is not None and ambient.photos:
            await self._activate_batch(ambient, None)
            return self._current_photo
        return self._current_photo

    async def _slot_loop(self) -> None:
        while self._enabled and not self._closed:
            self._slot_wake.clear()
            try:
                await asyncio.wait_for(self._slot_wake.wait(), timeout=self._delay_s)
                continue
            except (TimeoutError, asyncio.TimeoutError):
                pass
            except asyncio.CancelledError:
                return
            if not self._enabled or self._closed:
                return
            try:
                await self.advance_photo()
            except Exception:
                logger.exception("Atmosphere photo advance failed; retrying next slot")

    async def _listen_loop(self) -> None:
        while self._enabled and not self._closed:
            if not self._is_mic_allowed_now():
                self._next_slot_batch = None
                self._next_slot_epoch = None
                self._rolling_context.clear()
                try:
                    await self._sleep(1.0)
                except asyncio.CancelledError:
                    return
                continue
            try:
                await self.listen_once()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Atmosphere microphone cycle failed; backing off 2s")
                try:
                    await self._sleep(2.0)
                except asyncio.CancelledError:
                    return

    # ------------------------------------------------------------------
    # MQTT Commands & Discovery
    # ------------------------------------------------------------------

    def _state_topic(self) -> str:
        return state_topic(self._panel, "atmosphere")

    def _cmd_topic(self, suffix: str) -> str:
        return f"brilliant/{self._panel}/atmosphere/set_{suffix}"

    async def on_mqtt_message(self, topic: str, payload: str, retained: bool) -> None:
        if retained:
            return
        await self.on_mqtt_command(topic, payload)

    async def on_mqtt_command(self, topic: str, payload: str) -> None:
        if self._closed:
            return
        prefix = f"brilliant/{self._panel}/atmosphere/set_"
        if not topic.startswith(prefix):
            return
        cmd = topic[len(prefix) :]
        val = payload.strip()

        if cmd == "enabled":
            upper = val.upper()
            if upper in ("ON", "1", "TRUE"):
                await self.set_enabled(True, reset_default_slider=not self._enabled)
            elif upper in ("OFF", "0", "FALSE"):
                await self.set_enabled(False)
        elif cmd == "delay_s":
            try:
                parsed = float(val)
            except ValueError:
                return
            if not math.isfinite(parsed):
                return
            await self.set_delay_s(parsed)
        elif cmd == "slider_pct":
            try:
                pct_f = float(val)
            except ValueError:
                return
            if not math.isfinite(pct_f):
                return
            await self.set_slider_pct(round(pct_f))
        elif cmd == "next_photo":
            if val == "PRESS" and self._enabled:
                await self.advance_photo()
        elif cmd == "topic":
            if val:
                if not self._enabled:
                    await self.set_enabled(True, reset_default_slider=True)
                await self.ingest_transcript(val, direct_topic=True)
        elif cmd == "personal_album_url":
            with contextlib.suppress(ValueError):
                await self._curator.set_personal_album_url(val)
                await self._publish_state()

    async def _publish_retained(self, topic: str, payload: str) -> None:
        self._published_topics.add(topic)
        if self._owned_topics is not None:
            await self._owned_topics.async_publish(self._mqtt, topic, payload)
        else:
            await self._mqtt.publish(topic, payload, retain=True)

    async def _subscribe_commands(self) -> None:
        for suffix in (
            "enabled",
            "delay_s",
            "slider_pct",
            "next_photo",
            "topic",
            "personal_album_url",
        ):
            await self._mqtt.subscribe(self._cmd_topic(suffix))

    async def _publish_discovery(self) -> None:
        avail = availability_topic(self._panel)
        st_topic = self._state_topic()
        dev_block = device_block(self._panel)
        uid_base = f"brilliant_{self._panel}_atmosphere"

        sw_uid = f"{uid_base}_enabled"
        await self._publish_retained(
            f"homeassistant/switch/{sw_uid}/config",
            json.dumps(
                {
                    "availability": [{"topic": avail}],
                    "command_topic": self._cmd_topic("enabled"),
                    "device": dev_block,
                    "icon": "mdi:image-Auto-adjust".lower(),
                    "name": "Atmosphere Mode",
                    "payload_off": "OFF",
                    "payload_on": "ON",
                    "state_off": "OFF",
                    "state_on": "ON",
                    "state_topic": st_topic,
                    "unique_id": sw_uid,
                    "value_template": "{{ 'ON' if value_json.enabled else 'OFF' }}",
                },
                sort_keys=True,
            ),
        )

        delay_uid = f"{uid_base}_delay_s"
        await self._publish_retained(
            f"homeassistant/number/{delay_uid}/config",
            json.dumps(
                {
                    "availability": [{"topic": avail}],
                    "command_topic": self._cmd_topic("delay_s"),
                    "device": dev_block,
                    "entity_category": "config",
                    "icon": "mdi:timer-sand",
                    "max": MAX_PHOTO_DELAY_S,
                    "min": MIN_PHOTO_DELAY_S,
                    "mode": "slider",
                    "name": "Atmosphere Photo Delay",
                    "state_topic": st_topic,
                    "step": 5.0,
                    "unique_id": delay_uid,
                    "unit_of_measurement": "s",
                    "value_template": "{{ value_json.delay_s }}",
                },
                sort_keys=True,
            ),
        )

        btn_uid = f"{uid_base}_next_photo"
        await self._publish_retained(
            f"homeassistant/button/{btn_uid}/config",
            json.dumps(
                {
                    "availability": [{"topic": avail}],
                    "command_topic": self._cmd_topic("next_photo"),
                    "device": dev_block,
                    "icon": "mdi:skip-next",
                    "name": "Atmosphere Next Photo",
                    "payload_press": "PRESS",
                    "unique_id": btn_uid,
                },
                sort_keys=True,
            ),
        )

        topic_uid = f"{uid_base}_topic"
        await self._publish_retained(
            f"homeassistant/sensor/{topic_uid}/config",
            json.dumps(
                {
                    "availability": [{"topic": avail}],
                    "device": dev_block,
                    "icon": "mdi:image-multiple",
                    "json_attributes_template": "{{ value_json.photo | tojson }}",
                    "json_attributes_topic": st_topic,
                    "name": "Atmosphere Current Topic",
                    "state_topic": st_topic,
                    "unique_id": topic_uid,
                    "value_template": "{{ value_json.photo.title }}",
                },
                sort_keys=True,
            ),
        )

    async def _publish_state(self, epoch: PrivacyEpoch | None = None) -> None:
        async with self._state_publish_lock:
            if self._closed:
                return
            topic = self._state_topic()
            self._published_topics.add(topic)
            publisher = _GuardedStatePublisher(self._mqtt, topic, lambda: self._render_state(epoch))
            if self._owned_topics is not None:
                await self._owned_topics.async_publish(publisher, topic, "")
            else:
                await publisher.publish(topic, "", retain=True)

    def _render_state(self, epoch: PrivacyEpoch | None = None) -> str | None:
        if self._closed:
            return None
        photo = self.current_photo
        if photo is not None and epoch is not None and self._is_blocked_by_privacy(epoch):
            photo = None
        privacy_open = self._is_mic_allowed_now()
        payload = {
            "enabled": self._enabled,
            "slider_pct": self._slider_pct,
            "delay_s": self._delay_s,
            "privacy_open": privacy_open,
            "mic_listening": bool(self._enabled and privacy_open),
            "personal_photo_count": self._curator.personal_photo_count,
            "photo": {
                "title": photo.display_title if photo is not None else "Standby",
                "topic": photo.topic if photo is not None else "",
                "url": photo.url if photo is not None else "",
                "source": photo.source if photo is not None else "",
                "is_answer": photo.is_answer if photo is not None else False,
            },
        }
        return json.dumps(payload, sort_keys=True)

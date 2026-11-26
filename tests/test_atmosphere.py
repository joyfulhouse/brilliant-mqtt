"""Unit and regression tests for on-panel Atmosphere visual curation, Cast & privacy gate."""

from __future__ import annotations

import asyncio
import json
import struct
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from brilliant_mqtt.atmosphere import (
    AlsaMicrophoneAdapter,
    AtmosphereController,
    CastV2PhotoClient,
    CuratedPhoto,
    CuratedTopicBatch,
    PersonalAlbumStore,
    PrivacyEpoch,
    build_wav_bytes,
    decode_cast_message_body,
    decode_room_assignment_ids,
    delay_s_to_slider_pct,
    discover_ambiled_target,
    discover_panel_room_ids,
    encode_cast_message,
    extract_fallback_keywords,
    extract_immediate_cue,
    is_allowed_personal_album_url,
    parse_google_photos_album_html,
    retire_atmosphere_topics,
    slider_pct_to_delay_s,
)
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.protocols import VirtualPeripheralSpec
from brilliant_mqtt.retained_topics import RetainedTopicLedger
from tests.fakes import FakeBus, FakeMqtt


class FakeDisplaySink:
    def __init__(self) -> None:
        self.shown: list[CuratedPhoto] = []
        self.off_calls = 0

    async def show_photo(self, photo: CuratedPhoto) -> bool:
        self.shown.append(photo)
        return True

    async def turn_off(self) -> None:
        self.off_calls += 1


class FakeMic:
    def __init__(self, wav: bytes | None = None) -> None:
        self.wav = wav or build_wav_bytes(b"\x10\x05" * 1600)
        self.calls = 0
        self.on_capture: Callable[[], None] | None = None

    async def capture_utterance_wav(self, epoch: PrivacyEpoch | None) -> bytes | None:
        self.calls += 1
        if self.on_capture is not None:
            self.on_capture()
        return self.wav


class FakeCurator:
    def __init__(self) -> None:
        self.personal_photo_count = 0
        self.audio_calls = 0
        self.text_calls = 0
        self.next_audio_result: tuple[str, CuratedTopicBatch | None] = (
            "hiking by the mountain waterfall",
            CuratedTopicBatch(
                topic="mountain waterfall",
                photos=(
                    CuratedPhoto(
                        url="https://images.example.test/waterfall1.jpg",
                        topic="mountain waterfall",
                    ),
                    CuratedPhoto(
                        url="https://images.example.test/waterfall2.jpg",
                        topic="mountain waterfall",
                    ),
                ),
                score=8.5,
                topic_root="waterfall",
            ),
        )
        self.ambient_counter = 0

    async def set_personal_album_url(self, url: str) -> int:
        if not url:
            self.personal_photo_count = 0
            return 0
        if not is_allowed_personal_album_url(url):
            raise ValueError("disallowed")
        self.personal_photo_count = 3
        return 3

    async def curate_audio(
        self,
        wav_bytes: bytes,
        context: str,
        recent_keywords: Sequence[str],
    ) -> tuple[str, CuratedTopicBatch | None]:
        self.audio_calls += 1
        return self.next_audio_result

    async def curate_text(
        self,
        transcript: str,
        context: str,
        recent_keywords: Sequence[str],
        *,
        direct_topic: bool = False,
    ) -> CuratedTopicBatch | None:
        self.text_calls += 1
        immediate, subject = extract_immediate_cue(transcript)
        topic = subject or transcript.strip()
        return CuratedTopicBatch(
            topic=topic,
            photos=(
                CuratedPhoto(
                    url=f"https://images.example.test/{topic.replace(' ', '_')}.jpg",
                    topic=topic,
                ),
            ),
            score=9.5,
            topic_root=topic.split()[0].lower(),
            immediate=direct_topic or immediate,
        )

    async def next_ambient_batch(self, recent_keywords: Sequence[str]) -> CuratedTopicBatch | None:
        self.ambient_counter += 1
        idx = self.ambient_counter
        return CuratedTopicBatch(
            topic=f"ambient landscape {idx}",
            photos=(
                CuratedPhoto(
                    url=f"https://images.example.test/ambient_{idx}.jpg",
                    topic=f"ambient landscape {idx}",
                ),
            ),
            score=5.0,
        )


class FakeVirtualHost:
    def __init__(
        self,
        spec: VirtualPeripheralSpec,
        on_ext: Callable[[str, Any], Awaitable[None]],
    ) -> None:
        self.spec = spec
        self.on_ext = on_ext
        self.started = False
        self.stopped = False
        self.vars: dict[str, Any] = dict(spec.initial_variables)

    async def start(self) -> None:
        self.started = True

    async def set_variable(self, name: str, value: Any) -> None:
        self.vars[name] = value

    async def shutdown(self) -> None:
        self.stopped = True


# ---------------------------------------------------------------------------
# Pure Curve, Thrift RoomAssignment, Cast Framing & Album Parsing Tests
# ---------------------------------------------------------------------------


def test_slider_delay_curve_endpoints_and_roundtrip() -> None:
    assert slider_pct_to_delay_s(1) == 300.0
    assert slider_pct_to_delay_s(25) == 120.0
    assert slider_pct_to_delay_s(50) == 45.0
    assert slider_pct_to_delay_s(75) == 15.0
    assert slider_pct_to_delay_s(100) == 5.0
    assert slider_pct_to_delay_s(float("nan")) == 45.0

    prev = 999.0
    for pct in range(1, 101):
        d = slider_pct_to_delay_s(pct)
        assert 5.0 <= d <= 300.0
        assert d <= prev
        prev = d

    assert delay_s_to_slider_pct(300.0) == 1
    assert delay_s_to_slider_pct(45.0) == 50
    assert delay_s_to_slider_pct(5.0) == 100
    assert delay_s_to_slider_pct(float("inf")) == 50


def test_decode_room_assignment_and_panel_discovery() -> None:
    raw = "DwABCwAAAAEAAAAiYWFmMGZlNDhhODgyNjgzNWE3NTA6MTcwNzcxNjE2ODM3OQA="
    assert decode_room_assignment_ids(raw) == ("aaf0fe48a8826835a750:1707716168379",)
    assert decode_room_assignment_ids("") == ()
    assert decode_room_assignment_ids("not-base64!") == ()

    devices = [
        BrilliantDevice(
            device_id="panel-dev",
            peripheral_id="device_config_peripheral",
            name="device_config_peripheral",
            kind=DeviceKind.UNKNOWN,
            peripheral_type=19,
            variables={"room_assignment": Variable("room_assignment", raw)},
        ),
        BrilliantDevice(
            device_id="panel-dev",
            peripheral_id="tv_ambiled",
            name="TV AmbiLED",
            kind=DeviceKind.LIGHT,
            peripheral_type=27,
            variables={"on": Variable("on", "0"), "intensity": Variable("intensity", "250")},
        ),
    ]
    assert discover_panel_room_ids(devices) == ("aaf0fe48a8826835a750:1707716168379",)
    assert discover_ambiled_target(devices, "auto") == ("panel-dev", "tv_ambiled")
    assert discover_ambiled_target(devices, "none") is None


def test_cast_protobuf_roundtrip_and_client_handshake() -> None:
    pkt = encode_cast_message(
        "sender-0",
        "receiver-0",
        "urn:x-cast:com.google.cast.receiver",
        '{"type":"LAUNCH","appId":"CC1AD845","requestId":1}',
    )
    msg_len = struct.unpack(">I", pkt[:4])[0]
    assert msg_len == len(pkt) - 4
    src, dst, ns, payload = decode_cast_message_body(pkt[4:])
    assert (src, dst, ns) == ("sender-0", "receiver-0", "urn:x-cast:com.google.cast.receiver")
    assert json.loads(payload)["appId"] == "CC1AD845"

    sent_frames: list[tuple[str, str, str, dict[str, Any]]] = []
    inbound_frames = [
        encode_cast_message(
            "receiver-0",
            "sender-0",
            "urn:x-cast:com.google.cast.tp.heartbeat",
            '{"type":"PING"}',
        ),
        encode_cast_message(
            "receiver-0",
            "sender-0",
            "urn:x-cast:com.google.cast.receiver",
            json.dumps(
                {
                    "type": "RECEIVER_STATUS",
                    "status": {
                        "applications": [
                            {
                                "appId": "CC1AD845",
                                "transportId": "web-9",
                                "sessionId": "sess-42",
                            }
                        ]
                    },
                }
            ),
        ),
    ]
    rx_stream = bytearray(b"".join(inbound_frames))

    class FakeTlsSocket:
        def sendall(self, data: bytes) -> None:
            pos = 0
            while pos < len(data):
                flen = struct.unpack(">I", data[pos : pos + 4])[0]
                s, d, n, p = decode_cast_message_body(data[pos + 4 : pos + 4 + flen])
                sent_frames.append((s, d, n, json.loads(p)))
                pos += 4 + flen

        def recv(self, n: int) -> bytes:
            chunk = bytes(rx_stream[:n])
            del rx_stream[:n]
            return chunk

        def close(self) -> None:
            pass

    fake_tls = FakeTlsSocket()

    async def _run_cast() -> None:
        client = CastV2PhotoClient("192.0.2.55", 8009)
        with (
            patch("socket.create_connection") as mock_conn,
            patch.object(
                ssl_ctx := __import__("ssl").SSLContext,
                "wrap_socket",
                return_value=fake_tls,
            ),
        ):
            _ = ssl_ctx
            mock_conn.return_value.settimeout = lambda _t: None
            ok = await client.show_photo(
                CuratedPhoto(
                    url="https://images.example.test/mountain.jpg",
                    topic="mountain lake",
                )
            )
            assert ok is True
            await client.turn_off()

    asyncio.run(_run_cast())
    types_sent = [f[3]["type"] for f in sent_frames]
    assert types_sent == ["CONNECT", "LAUNCH", "PONG", "CONNECT", "LOAD", "STOP"]
    load_msg = sent_frames[4][3]
    assert load_msg["media"]["contentId"] == "https://images.example.test/mountain.jpg"
    assert load_msg["media"]["metadata"]["title"] == "Mountain Lake"


def test_personal_album_ssrf_guard_html_parser_and_topic_matching(tmp_path: Path) -> None:
    assert is_allowed_personal_album_url("https://photos.app.goo.gl/AbCdEf12345") is True
    assert is_allowed_personal_album_url("https://photos.google.com/share/AF1Qip?key=xyz") is True
    assert is_allowed_personal_album_url("https://www.icloud.com/sharedalbum/#B0z5qXGF1") is True
    assert is_allowed_personal_album_url("https://p12-sharedstreams.icloud.com/x") is True

    assert is_allowed_personal_album_url("http://photos.app.goo.gl/AbCdEf12345") is False
    assert is_allowed_personal_album_url("https://127.0.0.1/share") is False
    assert is_allowed_personal_album_url("https://192.168.1.10/share") is False
    assert is_allowed_personal_album_url("https://attacker.example.test/share") is False

    sample_html = (
        'AF_initDataCallback({data:[["id1",'
        '["https://lh3.googleusercontent.com/pw/AP1GczPortraitPhoto00001",900,1600]],'
        '["id2",["https://lh3.googleusercontent.com/pw/AP1GczLandscapeBeach002",1920,1080]]'
        "]});"
    )
    urls = parse_google_photos_album_html(sample_html)
    assert urls == [
        "https://lh3.googleusercontent.com/pw/AP1GczLandscapeBeach002=w1920-h1080",
        "https://lh3.googleusercontent.com/pw/AP1GczPortraitPhoto00001=w1920-h1080",
    ]

    store = PersonalAlbumStore(cache_path=tmp_path / "tags.json")
    store.load_from_urls(urls)
    store._photos[0].caption = "golden retriever running on sandy ocean beach"
    store._photos[0].tags = ("golden retriever", "dog", "beach", "ocean")
    hits = store.match_topic("golden retriever beach", "dog")
    assert len(hits) == 1
    assert hits[0].source == "personal"
    assert hits[0].url == urls[0]
    assert store.match_topic("eiffel tower paris", "tower") == []


def test_cue_and_fallback_keyword_extractors() -> None:
    assert extract_immediate_cue("hey show me northern lights") == (True, "northern lights")
    assert extract_immediate_cue("let's go to the next photo please") == (True, None)
    assert extract_immediate_cue("we went hiking in colorado") == (False, None)
    assert extract_fallback_keywords("We saw dolphins jumping near the sailboat!") == [
        "dolphins",
        "jumping",
        "near",
    ]


# ---------------------------------------------------------------------------
# Controller, Virtual Light, AmbiLED Coupling & Privacy Regressions
# ---------------------------------------------------------------------------


async def test_virtual_button_turns_on_tv_couples_ambiled_85_and_maps_slider() -> None:
    raw_room = "DwABCwAAAAEAAAAiYWFmMGZlNDhhODgyNjgzNWE3NTA6MTcwNzcxNjE2ODM3OQA="
    bus = FakeBus(
        [
            BrilliantDevice(
                device_id="panel-1",
                peripheral_id="device_config_peripheral",
                name="device_config_peripheral",
                kind=DeviceKind.UNKNOWN,
                peripheral_type=19,
                variables={"room_assignment": Variable("room_assignment", raw_room)},
            ),
            BrilliantDevice(
                device_id="panel-1",
                peripheral_id="tv_ambiled",
                name="TV AmbiLED",
                kind=DeviceKind.LIGHT,
                peripheral_type=27,
                variables={"on": Variable("on", "0"), "intensity": Variable("intensity", "250")},
            ),
        ]
    )
    mqtt = FakeMqtt()
    display = FakeDisplaySink()
    mic = FakeMic()
    curator = FakeCurator()
    hosts: list[FakeVirtualHost] = []

    def make_host(
        spec: VirtualPeripheralSpec, on_ext: Callable[[str, Any], Awaitable[None]]
    ) -> FakeVirtualHost:
        vh = FakeVirtualHost(spec, on_ext)
        hosts.append(vh)
        return vh

    ctrl = AtmosphereController(
        mqtt,
        "living_room",
        display=display,
        mic=mic,
        curator=curator,
        bus=bus,
        host_factory=make_host,
        privacy_off=lambda: True,
        privacy_epoch=lambda: (1, -1, 0),
    )
    await ctrl.start()
    assert len(hosts) == 1
    assert hosts[0].spec.room_ids == ("aaf0fe48a8826835a750:1707716168379",)

    # Tap virtual button ON -> casts initial photo, sets AmbiLED 85% (850), slider 50% (45s)
    await hosts[0].on_ext("on", 1)
    assert ctrl.enabled is True
    assert ctrl.slider_pct == 50
    assert ctrl.delay_s == 45.0
    assert len(display.shown) == 1
    assert any(
        pid == "tv_ambiled"
        and {(v.name, v.value) for v in sets} == {("on", "1"), ("intensity", "850")}
        for _did, pid, sets in bus.commands
    )

    # Drag brightness slider to 75% (intensity=750) -> next-photo delay becomes 15.0s
    await hosts[0].on_ext("intensity", 750)
    assert ctrl.slider_pct == 75
    assert ctrl.delay_s == 15.0

    # Tap virtual button OFF -> stops Cast & turns coupled AmbiLED OFF
    await hosts[0].on_ext("on", 0)
    assert ctrl.enabled is False
    assert display.off_calls == 1
    assert any(
        pid == "tv_ambiled" and [(v.name, v.value) for v in sets] == [("on", "0")]
        for _did, pid, sets in bus.commands
    )
    await ctrl.shutdown()


async def test_standalone_panel_without_ambiled_works_cleanly() -> None:
    bus = FakeBus([])
    mqtt = FakeMqtt()
    display = FakeDisplaySink()
    ctrl = AtmosphereController(
        mqtt,
        "office",
        display=display,
        mic=FakeMic(),
        curator=FakeCurator(),
        bus=bus,
        privacy_off=lambda: True,
        privacy_epoch=lambda: (1, -1, 0),
    )
    await ctrl.start()
    await ctrl.set_enabled(True, reset_default_slider=True)
    assert ctrl.enabled is True
    assert len(display.shown) == 1
    assert bus.commands == []
    await ctrl.set_enabled(False)
    assert display.off_calls == 1
    await ctrl.shutdown()


async def test_conversation_queues_for_next_slot_and_immediate_cue_switches_now() -> None:
    mqtt = FakeMqtt()
    display = FakeDisplaySink()
    mic = FakeMic()
    curator = FakeCurator()
    ctrl = AtmosphereController(
        mqtt,
        "office",
        display=display,
        mic=mic,
        curator=curator,
        privacy_off=lambda: True,
        privacy_epoch=lambda: (1, -1, 0),
    )
    await ctrl.start()
    await ctrl.set_enabled(True, reset_default_slider=True)
    assert display.shown[-1].topic == "ambient landscape 1"

    # Normal Mode 2 conversation queues for the NEXT time slot without jarring immediate switch
    batch = await ctrl.listen_once()
    assert batch is not None
    assert display.shown[-1].topic == "ambient landscape 1"

    # Next time slot fires -> selects the conversational waterfall photo!
    await ctrl.advance_photo()
    assert display.shown[-1].topic == "mountain waterfall"

    # Mode 1 Q&A or immediate cue switches immediately without waiting for slot timer
    curator.next_audio_result = (
        "what animal is the forest giraffe",
        CuratedTopicBatch(
            topic="okapi",
            photos=(
                CuratedPhoto(
                    url="https://images.example.test/okapi.jpg",
                    topic="okapi",
                    source="wikipedia",
                    is_answer=True,
                    answer_label="Okapi (Forest Giraffe)",
                ),
            ),
            score=10.0,
            is_answer=True,
            answer_label="Okapi (Forest Giraffe)",
            immediate=True,
        ),
    )
    await ctrl.listen_once()
    assert display.shown[-1].topic == "okapi"
    assert display.shown[-1].display_title == "Answer: Okapi (Forest Giraffe)"
    await ctrl.shutdown()


@pytest.mark.parametrize("revoke_during_capture", [False, True])
async def test_cam_cover_closed_blocks_mic_and_wipes_conversational_state(
    revoke_during_capture: bool,
) -> None:
    allowed = revoke_during_capture
    epoch: PrivacyEpoch = (1, -1, 0)
    mqtt = FakeMqtt()
    display = FakeDisplaySink()
    mic = FakeMic()
    curator = FakeCurator()

    def flip_shutter() -> None:
        nonlocal allowed, epoch
        if revoke_during_capture:
            allowed = False
            epoch = (1, 9, 0)

    mic.on_capture = flip_shutter
    ctrl = AtmosphereController(
        mqtt,
        "office",
        display=display,
        mic=mic,
        curator=curator,
        privacy_off=lambda: allowed,
        privacy_epoch=lambda: epoch,
    )
    await ctrl.start()
    result = await ctrl.listen_once()
    assert result is None
    assert curator.audio_calls == 0
    if not revoke_during_capture:
        assert mic.calls == 0
    await ctrl.shutdown()


async def test_alsa_mic_adapter_kills_arecord_and_zeroes_pcm_when_shutter_closes() -> None:
    allowed = True
    epoch: PrivacyEpoch = (1, -1, 0)
    killed = False

    class FakeStdout:
        def __init__(self) -> None:
            self.reads = 0

        async def read(self, n: int) -> bytes:
            nonlocal allowed, epoch
            self.reads += 1
            if self.reads >= 2:
                allowed = False
                epoch = (1, 4, 0)
            return struct.pack("<1600h", *([2000] * 1600))

    class FakeProc:
        def __init__(self) -> None:
            self.stdout = FakeStdout()

        def kill(self) -> None:
            nonlocal killed
            killed = True

        async def wait(self) -> int:
            return -9

    adapter = AlsaMicrophoneAdapter(
        privacy_off=lambda: allowed,
        privacy_epoch=lambda: epoch,
    )
    with patch("asyncio.create_subprocess_exec", return_value=FakeProc()):
        wav = await adapter.capture_utterance_wav((1, -1, 0))
    assert wav is None
    assert killed is True


async def test_conversational_photo_never_outlives_privacy_grant_across_ledger_wait(
    tmp_path: Path,
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    epoch: PrivacyEpoch = (1, -1, 0)

    class HeldLedger(RetainedTopicLedger):
        block = False

        async def async_publish(self, mqtt: Any, topic: str, payload: str) -> None:
            if self.block and topic.endswith("/atmosphere/state"):
                entered.set()
                await release.wait()
            await super().async_publish(mqtt, topic, payload)

    ledger = HeldLedger("office", tmp_path / "ledger.json")
    await ledger.async_load()
    mqtt = FakeMqtt()
    display = FakeDisplaySink()
    mic = FakeMic()
    curator = FakeCurator()
    curator.next_audio_result = (
        "show me golden retriever",
        CuratedTopicBatch(
            topic="golden retriever",
            photos=(
                CuratedPhoto(
                    url="https://images.example.test/dog.jpg",
                    topic="golden retriever",
                ),
            ),
            score=10.0,
            immediate=True,
        ),
    )
    ctrl = AtmosphereController(
        mqtt,
        "office",
        display=display,
        mic=mic,
        curator=curator,
        owned_topics=ledger,
        privacy_off=lambda: True,
        privacy_epoch=lambda: epoch,
    )
    await ctrl.start()
    ledger.block = True
    mqtt.published.clear()
    work = asyncio.create_task(ctrl.listen_once())
    await asyncio.wait_for(entered.wait(), 1.0)
    epoch = (1, 12, 0)
    release.set()
    await work
    assert ctrl.current_photo is None
    states = [json.loads(p) for t, p, _ in mqtt.published if t.endswith("/atmosphere/state")]
    assert states and states[-1]["photo"]["url"] == ""
    await ctrl.shutdown()


async def test_startup_rollback_and_topic_retirement_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger_path = tmp_path / "ledger.json"
    ledger = RetainedTopicLedger("office", ledger_path)
    await ledger.async_load()
    unrelated = "brilliant/office/light/state"

    class FailingMqtt(FakeMqtt):
        async def subscribe(self, topic: str) -> None:
            raise OSError("subscribe boom")

    fmqtt = FailingMqtt()
    await ledger.async_publish(fmqtt, unrelated, "ON")
    bad_ctrl = AtmosphereController(
        fmqtt,
        "office",
        display=FakeDisplaySink(),
        mic=FakeMic(),
        curator=FakeCurator(),
        owned_topics=ledger,
    )
    with pytest.raises(OSError, match="subscribe boom"):
        await bad_ctrl.start()
    assert ledger.topics == {unrelated}

    # Full start + disable retirement
    ok_mqtt = FakeMqtt()
    ok_ctrl = AtmosphereController(
        ok_mqtt,
        "office",
        display=FakeDisplaySink(),
        mic=FakeMic(),
        curator=FakeCurator(),
        owned_topics=ledger,
    )
    await ok_ctrl.start()
    atm_topics = ledger.topics - {unrelated}
    assert len(atm_topics) == 5
    await ok_ctrl.shutdown()

    ok_mqtt.published.clear()
    await retire_atmosphere_topics(ok_mqtt, "office", ledger)
    cleared = {t for t, p, retain in ok_mqtt.published if not p and retain}
    assert cleared == atm_topics
    assert ledger.topics == {unrelated}


async def test_session_rebuilds_after_atmosphere_start_failure_and_retires_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import brilliant_mqtt.__main__ as main_mod
    from tests import test_main as session_tests

    harness = session_tests._SessionHarness(monkeypatch)
    monkeypatch.setattr(harness.bridge_type, "is_privacy_off", lambda self: True, raising=False)
    monkeypatch.setattr(
        harness.bridge_type, "camera_privacy_epoch", lambda self: (1, -1, 0), raising=False
    )
    error = OSError("atmosphere startup failed")

    class FailedAtmosphere:
        def __init__(self, **kwargs: Any) -> None:
            pass

        async def start(self) -> None:
            raise error

        async def shutdown(self) -> None:
            harness.events.append("atmosphere_shutdown")

    monkeypatch.setattr(main_mod, "AtmosphereController", FailedAtmosphere)
    settings = session_tests._settings()
    object.__setattr__(settings, "atmosphere_enabled", True)
    object.__setattr__(settings, "retained_topics_file", str(tmp_path / "ledger.json"))
    with pytest.raises(OSError) as raised:
        await asyncio.wait_for(main_mod._run_session(settings, None, None), 1)
    assert raised.value is error
    assert harness.events[-3:] == ["atmosphere_shutdown", "bus_shutdown", "mqtt_disconnect"]


async def test_retained_mqtt_commands_ignored_and_nonfinite_delay_rejected() -> None:
    mqtt = FakeMqtt()
    display = FakeDisplaySink()
    ctrl = AtmosphereController(
        mqtt,
        "office",
        display=display,
        mic=FakeMic(),
        curator=FakeCurator(),
    )
    await ctrl.start()
    mqtt.published.clear()

    await mqtt.inject("brilliant/office/atmosphere/set_enabled", "ON", retained=True)
    assert ctrl.enabled is False

    for bad in ("nan", "inf", "-inf", "abc"):
        await mqtt.inject("brilliant/office/atmosphere/set_delay_s", bad)
    assert ctrl.delay_s == 45.0
    assert not mqtt.published
    await ctrl.shutdown()

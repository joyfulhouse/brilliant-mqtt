from __future__ import annotations

import json
from pathlib import Path

import pytest

from brilliant_mqtt.ha_control_protocol import (
    COMMAND_TTL_MS,
    EntityCommand,
    ModeCommand,
    SceneCommand,
    command_topic,
    decode_command,
    decode_mode_command,
    decode_scene_command,
    encode_json,
    manifest_topic,
    mode_catalog_topic,
    mode_command_topic,
    mode_event_topic,
    mode_result_topic,
    result_topic,
    scene_catalog_topic,
    scene_command_topic,
    scene_event_topic,
    scene_result_topic,
    stable_id,
    state_topic,
    transport_status_topic,
    validate_entity_command_context,
    validate_mode_command_context,
    validate_scene_command_context,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures/ha_control_v1_vectors.json"
VECTORS = json.loads(FIXTURE_PATH.read_text())


def test_stable_ids_match_golden_vectors() -> None:
    for entity_id, expected in VECTORS["stable_ids"].items():
        assert stable_id(entity_id) == expected


def test_topics_match_golden_vectors() -> None:
    stable = VECTORS["stable_ids"]["light.office_lamp"]
    command_id = "11111111-1111-4111-8111-111111111111"
    expected = VECTORS["topics"]
    assert {
        "manifest": manifest_topic(),
        "state": state_topic(stable),
        "command": command_topic(stable),
        "result": result_topic(command_id),
        "scene_catalog": scene_catalog_topic("office"),
        "scene_event": scene_event_topic("office"),
        "scene_command": scene_command_topic("office"),
        "scene_result": scene_result_topic(command_id),
        "scene_status": transport_status_topic("scene", "office"),
        "mode_catalog": mode_catalog_topic("office"),
        "mode_event": mode_event_topic("office"),
        "mode_command": mode_command_topic("office"),
        "mode_result": mode_result_topic(command_id),
    } == expected


@pytest.mark.parametrize("invalid", ["Office", "office/bath", "office%2Fbath", "", "a" * 64])
def test_topic_helpers_reject_invalid_panel_slugs(invalid: str) -> None:
    with pytest.raises(ValueError):
        scene_catalog_topic(invalid)


@pytest.mark.parametrize("invalid", ["not-a-uuid", "", "office%2Flamp"])
def test_topic_helpers_reject_invalid_uuids(invalid: str) -> None:
    with pytest.raises(ValueError):
        command_topic(invalid)
    with pytest.raises(ValueError):
        result_topic(invalid)


def test_canonical_json_matches_all_golden_payloads() -> None:
    for vector in VECTORS["payloads"].values():
        assert encode_json(vector["value"]) == vector["encoded"]


def test_decode_entity_command() -> None:
    vector = VECTORS["payloads"]["entity_command"]
    command = decode_command(vector["encoded"], now_ms=VECTORS["now_ms"])
    assert command == EntityCommand(
        command_id="11111111-1111-4111-8111-111111111111",
        stable_id="d353e38a-793e-5b6f-813b-17a1c38aba96",
        kind="turn_on",
        value={"brightness": 128},
        observed_sequence=7,
        issued_at_ms=1700000000000,
    )


def test_decode_scene_and_mode_commands() -> None:
    scene = VECTORS["payloads"]["scene_command"]
    mode = VECTORS["payloads"]["mode_command"]
    assert decode_scene_command(scene["encoded"], now_ms=VECTORS["now_ms"]) == SceneCommand(
        command_id="22222222-2222-4222-8222-222222222222",
        panel="office",
        scene_id="movie-time",
        issued_at_ms=1700000000000,
    )
    assert decode_mode_command(mode["encoded"], now_ms=VECTORS["now_ms"]) == ModeCommand(
        command_id="33333333-3333-4333-8333-333333333333",
        panel="office",
        mode_id="away",
        issued_at_ms=1700000000000,
    )


@pytest.mark.parametrize(
    ("decoder", "vector_name"),
    [
        (decode_command, "entity_command"),
        (decode_scene_command, "scene_command"),
        (decode_mode_command, "mode_command"),
    ],
)
@pytest.mark.parametrize("version_field", ["schema_version", "mapping_version"])
def test_decoders_reject_unknown_versions(
    decoder: object, vector_name: str, version_field: str
) -> None:
    value = dict(VECTORS["payloads"][vector_name]["value"])
    value[version_field] = 2
    with pytest.raises(ValueError):
        decoder(encode_json(value), now_ms=VECTORS["now_ms"])  # type: ignore[operator]


@pytest.mark.parametrize(
    ("decoder", "vector_name", "id_field"),
    [
        (decode_command, "entity_command", "command_id"),
        (decode_command, "entity_command", "stable_id"),
        (decode_scene_command, "scene_command", "scene_id"),
        (decode_mode_command, "mode_command", "mode_id"),
    ],
)
def test_decoders_reject_missing_ids(decoder: object, vector_name: str, id_field: str) -> None:
    value = dict(VECTORS["payloads"][vector_name]["value"])
    del value[id_field]
    with pytest.raises(ValueError):
        decoder(encode_json(value), now_ms=VECTORS["now_ms"])  # type: ignore[operator]


@pytest.mark.parametrize("payload", ["not-json", "[]", "{}"])
def test_decoders_reject_malformed_commands(payload: str) -> None:
    with pytest.raises(ValueError):
        decode_command(payload, now_ms=VECTORS["now_ms"])


@pytest.mark.parametrize("offset_ms", [-COMMAND_TTL_MS - 1, 5_001])
def test_decoders_reject_expired_or_future_commands(offset_ms: int) -> None:
    value = dict(VECTORS["payloads"]["entity_command"]["value"])
    value["issued_at_ms"] = VECTORS["now_ms"] + offset_ms
    with pytest.raises(ValueError):
        decode_command(encode_json(value), now_ms=VECTORS["now_ms"])


def test_command_context_rejects_retained_and_mismatched_topics() -> None:
    entity = decode_command(
        VECTORS["payloads"]["entity_command"]["encoded"], now_ms=VECTORS["now_ms"]
    )
    scene = decode_scene_command(
        VECTORS["payloads"]["scene_command"]["encoded"], now_ms=VECTORS["now_ms"]
    )
    mode = decode_mode_command(
        VECTORS["payloads"]["mode_command"]["encoded"], now_ms=VECTORS["now_ms"]
    )
    with pytest.raises(ValueError):
        validate_entity_command_context(entity, topic_stable_id=entity.stable_id, retained=True)
    with pytest.raises(ValueError):
        validate_entity_command_context(
            entity, topic_stable_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", retained=False
        )
    with pytest.raises(ValueError):
        validate_scene_command_context(scene, topic_panel=scene.panel, retained=True)
    with pytest.raises(ValueError):
        validate_scene_command_context(scene, topic_panel="kitchen", retained=False)
    with pytest.raises(ValueError):
        validate_mode_command_context(mode, topic_panel=mode.panel, retained=True)
    with pytest.raises(ValueError):
        validate_mode_command_context(mode, topic_panel="kitchen", retained=False)


def test_command_context_accepts_fresh_nonretained_matching_topic() -> None:
    entity = decode_command(
        VECTORS["payloads"]["entity_command"]["encoded"], now_ms=VECTORS["now_ms"]
    )
    validate_entity_command_context(entity, topic_stable_id=entity.stable_id, retained=False)

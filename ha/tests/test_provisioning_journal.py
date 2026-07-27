"""Durable provisioning journal state-machine and redaction tests."""

from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from custom_components.brilliant_mqtt.broker import BrokerKind
from custom_components.brilliant_mqtt.provisioning_journal import (
    JOURNAL_STORAGE_KEY,
    JOURNAL_STORAGE_VERSION,
    ProvisioningJournal,
    ProvisioningJournalError,
    ProvisioningOperation,
    ProvisioningPhase,
    ProvisioningRecord,
    StoredFileSnapshot,
    StoredFleetProfile,
    StoredJournalError,
    StoredPanelLayout,
    StoredPanelRequest,
    StoredPanelSnapshot,
    StoredServiceSnapshot,
)
from homeassistant.core import HomeAssistant

_PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKIykuTed7zNwJwn20eCelcKcHKJ9c/pGFfvulRWazuC"
_FINGERPRINT = "SHA256:JfCon51dCgE/yWGkyroh3Ne+ONLMm6QmHMQnEoPSLx0"
_TRANSACTION_ID = UUID("12345678-1234-4abc-8def-1234567890ab")
_SETUP_ID = UUID("87654321-4321-4abc-8def-abcdef012345")
_ROOT_PASSWORD = "root-password-must-survive-restart"
_ENVIRONMENT_BYTES = b"ROOT_PASSWORD=env-bytes-must-survive-restart\n"
_VERSION_BYTES = b"0.5.7\n"
_BRIDGE_UNIT_BYTES = b"[Service]\nEnvironmentFile=/etc/brilliant-mqtt.env\n"
_WIFI_UNIT_BYTES = b"[Service]\nExecStart=wifi-watchdog\n"
_BUS_UNIT_BYTES = b"[Service]\nExecStart=bus-watchdog\n"
_ACTIVE_RELEASE_TARGET = "/var/brilliant-mqtt/releases/0.5.7--aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


class _MemoryStore:
    """Store-compatible in-memory persistence boundary."""

    def __init__(self) -> None:
        self.data: dict[str, Any] | None = None
        self.events: list[tuple[str, str | None]] = []
        self.load_error: Exception | None = None
        self.remove_error: Exception | None = None
        self.save_error: Exception | None = None

    async def async_load(self) -> dict[str, Any] | None:
        if self.load_error is not None:
            raise self.load_error
        return copy.deepcopy(self.data)

    async def async_save(self, data: dict[str, Any]) -> None:
        if self.save_error is not None:
            raise self.save_error
        copied = copy.deepcopy(data)
        record = copied.get("record")
        phase = record.get("phase") if isinstance(record, dict) else None
        self.events.append(("save", phase if isinstance(phase, str) else None))
        self.data = copied

    async def async_remove(self) -> None:
        self.events.append(("remove", None))
        if self.remove_error is not None:
            error = self.remove_error
            self.remove_error = None
            raise error
        self.data = None


class _BlockingStore(_MemoryStore):
    """A save which proves the journal cannot return before persistence."""

    def __init__(self) -> None:
        super().__init__()
        self.save_started = asyncio.Event()
        self.allow_save = asyncio.Event()
        self.block_save = False

    async def async_save(self, data: dict[str, Any]) -> None:
        if self.block_save:
            self.save_started.set()
            await self.allow_save.wait()
        await super().async_save(data)


def _file(content: bytes, mode: int) -> StoredFileSnapshot:
    return StoredFileSnapshot(content=content, mode=mode)


def _service(
    content: bytes,
    *,
    enabled: bool = True,
    active: bool = True,
) -> StoredServiceSnapshot:
    return StoredServiceSnapshot(
        unit_file=_file(content, 0o644),
        enabled=enabled,
        active=active,
    )


def _upgrade_snapshot() -> StoredPanelSnapshot:
    return StoredPanelSnapshot(
        layout=StoredPanelLayout.RELEASE_LINK,
        active_release_target=_ACTIVE_RELEASE_TARGET,
        environment_file=_file(_ENVIRONMENT_BYTES, 0o600),
        version_file=_file(_VERSION_BYTES, 0o644),
        bridge_service=_service(_BRIDGE_UNIT_BYTES),
        wifi_watchdog_service=_service(_WIFI_UNIT_BYTES),
        bus_watchdog_service=_service(_BUS_UNIT_BYTES),
        selected_components=("bridge", "bus_watchdog", "wifi_watchdog"),
    )


def _absent_snapshot() -> StoredPanelSnapshot:
    absent_file = StoredFileSnapshot(content=None, mode=None)
    absent_service = StoredServiceSnapshot(
        unit_file=absent_file,
        enabled=False,
        active=False,
    )
    return StoredPanelSnapshot(
        layout=StoredPanelLayout.ABSENT,
        active_release_target=None,
        environment_file=absent_file,
        version_file=absent_file,
        bridge_service=absent_service,
        wifi_watchdog_service=absent_service,
        bus_watchdog_service=absent_service,
        selected_components=(),
    )


def _record(*, phase: ProvisioningPhase = ProvisioningPhase.STAGED) -> ProvisioningRecord:
    return ProvisioningRecord(
        transaction_id=_TRANSACTION_ID,
        operation=ProvisioningOperation.INSTALL,
        phase=phase,
        setup_id=_SETUP_ID,
        panel_request=StoredPanelRequest(
            host="office.iot.example",
            ssh_username="root",
            root_password=_ROOT_PASSWORD,
            public_key=_PUBLIC_KEY,
            fingerprint=_FINGERPRINT,
            slug="office",
            selected_components=("bridge", "bus_watchdog", "wifi_watchdog"),
        ),
        fleet_profile=StoredFleetProfile(
            kind=BrokerKind.EXISTING_BROKER,
            host="mqtt.iot.example",
            port=8883,
            tls_enabled=True,
        ),
        staged_version="0.6.0",
        prior_snapshot=_upgrade_snapshot(),
        started_at=datetime(2026, 7, 27, 18, 30, tzinfo=UTC),
        last_error=None,
    )


@pytest.fixture
def memory_store(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> _MemoryStore:
    store = _MemoryStore()
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def make_store(*args: object, **kwargs: object) -> _MemoryStore:
        calls.append((args, kwargs))
        return store

    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.provisioning_journal.Store",
        make_store,
    )
    ProvisioningJournal(hass)
    assert calls == [
        (
            (hass, JOURNAL_STORAGE_VERSION, JOURNAL_STORAGE_KEY),
            {"atomic_writes": True, "private": True},
        )
    ]
    return store


def _journal(hass: HomeAssistant, memory_store: _MemoryStore) -> ProvisioningJournal:
    """Construct a journal after the fixture has installed its Store factory."""
    del memory_store
    return ProvisioningJournal(hass)


async def _advance_to(
    journal: ProvisioningJournal,
    phase: ProvisioningPhase,
) -> ProvisioningRecord:
    record = await journal.async_create(_record())
    for next_phase in (
        ProvisioningPhase.ACTIVATION_PENDING,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.VERIFYING,
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
    ):
        if record.phase is phase:
            return record
        record = await journal.async_transition(_TRANSACTION_ID, next_phase)
    if record.phase is phase:
        return record
    raise AssertionError(f"unreachable phase fixture: {phase.value}")


async def test_create_is_durable_before_it_returns(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _BlockingStore()
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.provisioning_journal.Store",
        lambda *_args, **_kwargs: store,
    )
    journal = ProvisioningJournal(hass)
    store.block_save = True

    create_task = asyncio.create_task(journal.async_create(_record()))
    await store.save_started.wait()

    assert not create_task.done()
    assert await journal.async_diagnostics() == {"count": 0, "phase": None}

    store.allow_save.set()
    assert await create_task == _record()
    assert store.events == [("save", "staged")]
    assert await journal.async_diagnostics() == {"count": 1, "phase": "staged"}


async def test_restart_round_trip_preserves_recovery_data_but_redacts_surfaces(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
) -> None:
    journal = _journal(hass, memory_store)
    original = _record()
    await journal.async_create(original)

    assert memory_store.data is not None
    raw_record = memory_store.data["record"]
    assert isinstance(raw_record, dict)
    raw_profile = raw_record["fleet_profile"]
    assert raw_profile == {
        "kind": "existing_broker",
        "host": "mqtt.iot.example",
        "port": 8883,
        "tls_enabled": True,
    }
    assert not {
        "mqtt_username",
        "mqtt_password",
        "tls_ca",
        "ca_pem",
    }.intersection(raw_profile)

    serialized = json.dumps(memory_store.data)
    for recovery_value in (
        _ROOT_PASSWORD,
        "Uk9PVF9QQVNTV09SRD1lbnYtYnl0ZXMtbXVzdC1zdXJ2aXZlLXJlc3RhcnQK",
    ):
        assert recovery_value in serialized
    for duplicate_credential_key in (
        "mqtt_username",
        "mqtt_password",
        "tls_ca",
        "ca_pem",
    ):
        assert duplicate_credential_key not in serialized

    restarted = _journal(hass, memory_store)
    loaded = await restarted.async_load()

    assert loaded == original
    assert loaded is not original
    assert await restarted.async_diagnostics() == {"count": 1, "phase": "staged"}
    assert set((await restarted.async_diagnostics()).keys()) == {"count", "phase"}

    redacted_surfaces = (
        repr(original),
        repr(original.panel_request),
        repr(original.fleet_profile),
        repr(original.prior_snapshot),
        repr(original.prior_snapshot.environment_file),
        repr(original.prior_snapshot.bridge_service),
        repr(await restarted.async_diagnostics()),
    )
    for surface in redacted_surfaces:
        for secret in (
            _ROOT_PASSWORD,
            "env-bytes-must-survive-restart",
            "office.iot.example",
            _PUBLIC_KEY,
            "EnvironmentFile=/etc/brilliant-mqtt.env",
        ):
            assert secret not in surface


async def test_forward_path_saves_each_phase_then_removes_only_after_subentry_creation(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
) -> None:
    journal = _journal(hass, memory_store)
    await _advance_to(journal, ProvisioningPhase.PENDING_CONFIG_COMMIT)

    completed = await journal.async_complete_commit(
        _TRANSACTION_ID,
        subentry_id="01J6H8J0BK5GJPPF3XEGQT71QX",
    )

    assert completed.phase is ProvisioningPhase.COMMITTED
    assert memory_store.events == [
        ("save", "staged"),
        ("save", "activation_pending"),
        ("save", "activated"),
        ("save", "verifying"),
        ("save", "pending_config_commit"),
        ("save", "committed"),
        ("remove", None),
    ]
    assert memory_store.data is None
    assert await journal.async_load() is None


@pytest.mark.parametrize(
    ("source", "path"),
    (
        (ProvisioningPhase.STAGED, ()),
        (
            ProvisioningPhase.ACTIVATION_PENDING,
            (ProvisioningPhase.ACTIVATION_PENDING,),
        ),
        (
            ProvisioningPhase.ACTIVATED,
            (
                ProvisioningPhase.ACTIVATION_PENDING,
                ProvisioningPhase.ACTIVATED,
            ),
        ),
        (
            ProvisioningPhase.VERIFYING,
            (
                ProvisioningPhase.ACTIVATION_PENDING,
                ProvisioningPhase.ACTIVATED,
                ProvisioningPhase.VERIFYING,
            ),
        ),
    ),
)
async def test_each_approved_failure_path_persists_then_removes_only_after_verified_rollback(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
    source: ProvisioningPhase,
    path: tuple[ProvisioningPhase, ...],
) -> None:
    journal = _journal(hass, memory_store)
    await journal.async_create(_record())
    for phase in path:
        await journal.async_transition(_TRANSACTION_ID, phase)

    failure = StoredJournalError(stage="activation", code="service_start_failed")
    pending = await journal.async_transition(
        _TRANSACTION_ID,
        ProvisioningPhase.ROLLBACK_PENDING,
        last_error=failure,
    )

    assert pending.phase is ProvisioningPhase.ROLLBACK_PENDING
    assert pending.last_error == failure
    assert memory_store.data is not None
    assert memory_store.events[-1] == ("save", "rollback_pending")

    completed = await journal.async_complete_rollback(
        _TRANSACTION_ID,
        verified=True,
    )

    assert completed.phase is ProvisioningPhase.ROLLED_BACK
    assert memory_store.events[-2:] == [
        ("save", "rolled_back"),
        ("remove", None),
    ]
    assert memory_store.data is None
    assert source in {
        ProvisioningPhase.STAGED,
        ProvisioningPhase.ACTIVATION_PENDING,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.VERIFYING,
    }


@pytest.mark.parametrize(
    ("source", "invalid_targets"),
    (
        (
            ProvisioningPhase.STAGED,
            (
                ProvisioningPhase.ACTIVATED,
                ProvisioningPhase.VERIFYING,
                ProvisioningPhase.PENDING_CONFIG_COMMIT,
                ProvisioningPhase.COMMITTED,
                ProvisioningPhase.ROLLED_BACK,
            ),
        ),
        (
            ProvisioningPhase.ACTIVATION_PENDING,
            (
                ProvisioningPhase.STAGED,
                ProvisioningPhase.VERIFYING,
                ProvisioningPhase.PENDING_CONFIG_COMMIT,
                ProvisioningPhase.COMMITTED,
                ProvisioningPhase.ROLLED_BACK,
            ),
        ),
        (
            ProvisioningPhase.ACTIVATED,
            (
                ProvisioningPhase.STAGED,
                ProvisioningPhase.ACTIVATION_PENDING,
                ProvisioningPhase.PENDING_CONFIG_COMMIT,
                ProvisioningPhase.COMMITTED,
                ProvisioningPhase.ROLLED_BACK,
            ),
        ),
        (
            ProvisioningPhase.VERIFYING,
            (
                ProvisioningPhase.STAGED,
                ProvisioningPhase.ACTIVATION_PENDING,
                ProvisioningPhase.ACTIVATED,
                ProvisioningPhase.COMMITTED,
                ProvisioningPhase.ROLLED_BACK,
            ),
        ),
        (
            ProvisioningPhase.PENDING_CONFIG_COMMIT,
            (
                ProvisioningPhase.STAGED,
                ProvisioningPhase.ACTIVATION_PENDING,
                ProvisioningPhase.ACTIVATED,
                ProvisioningPhase.VERIFYING,
                ProvisioningPhase.COMMITTED,
                ProvisioningPhase.ROLLED_BACK,
            ),
        ),
    ),
)
async def test_unapproved_phase_edges_fail_closed_without_a_write(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
    source: ProvisioningPhase,
    invalid_targets: tuple[ProvisioningPhase, ...],
) -> None:
    journal = _journal(hass, memory_store)
    await _advance_to(journal, source)
    event_count = len(memory_store.events)

    for target in invalid_targets:
        with pytest.raises(ProvisioningJournalError) as raised:
            await journal.async_transition(_TRANSACTION_ID, target)
        assert raised.value.code == "invalid_journal_transition"
        assert str(raised.value) == "invalid_journal_transition"
        assert len(memory_store.events) == event_count
        loaded = await journal.async_load()
        assert loaded is not None
        assert loaded.phase is source


async def test_terminal_deletion_requires_explicit_external_proof(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
) -> None:
    journal = _journal(hass, memory_store)
    await _advance_to(journal, ProvisioningPhase.PENDING_CONFIG_COMMIT)
    event_count = len(memory_store.events)

    with pytest.raises(ProvisioningJournalError) as commit_error:
        await journal.async_complete_commit(_TRANSACTION_ID, subentry_id="")
    assert commit_error.value.code == "subentry_creation_not_proven"
    assert len(memory_store.events) == event_count
    assert memory_store.data is not None

    memory_store.data = None
    memory_store.events.clear()
    journal = _journal(hass, memory_store)
    await journal.async_create(_record())
    await journal.async_transition(
        _TRANSACTION_ID,
        ProvisioningPhase.ROLLBACK_PENDING,
    )
    event_count = len(memory_store.events)

    with pytest.raises(ProvisioningJournalError) as rollback_error:
        await journal.async_complete_rollback(_TRANSACTION_ID, verified=False)
    assert rollback_error.value.code == "rollback_verification_required"
    assert len(memory_store.events) == event_count
    assert memory_store.data is not None


async def test_pending_config_commit_can_enter_recovery_rollback(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
) -> None:
    journal = _journal(hass, memory_store)
    await _advance_to(journal, ProvisioningPhase.PENDING_CONFIG_COMMIT)

    pending = await journal.async_transition(
        _TRANSACTION_ID,
        ProvisioningPhase.ROLLBACK_PENDING,
        last_error=StoredJournalError(
            stage="config_commit",
            code="subentry_not_found",
        ),
    )
    completed = await journal.async_complete_rollback(
        _TRANSACTION_ID,
        verified=True,
    )

    assert pending.phase is ProvisioningPhase.ROLLBACK_PENDING
    assert completed.phase is ProvisioningPhase.ROLLED_BACK
    assert memory_store.events[-3:] == [
        ("save", "rollback_pending"),
        ("save", "rolled_back"),
        ("remove", None),
    ]


async def test_concurrent_creates_are_serialized_to_one_durable_transaction(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _BlockingStore()
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.provisioning_journal.Store",
        lambda *_args, **_kwargs: store,
    )
    journal = ProvisioningJournal(hass)
    store.block_save = True

    first = asyncio.create_task(journal.async_create(_record()))
    await store.save_started.wait()
    second = asyncio.create_task(journal.async_create(_record()))
    await asyncio.sleep(0)

    assert not first.done()
    assert not second.done()

    store.allow_save.set()
    assert await first == _record()
    with pytest.raises(ProvisioningJournalError) as raised:
        await second

    assert raised.value.code == "journal_transaction_in_progress"
    assert store.events == [("save", "staged")]


async def test_separate_journal_instances_share_one_hass_transaction_lock(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _BlockingStore()
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.provisioning_journal.Store",
        lambda *_args, **_kwargs: store,
    )
    first_journal = ProvisioningJournal(hass)
    second_journal = ProvisioningJournal(hass)
    store.block_save = True

    first = asyncio.create_task(first_journal.async_create(_record()))
    await store.save_started.wait()
    second = asyncio.create_task(second_journal.async_create(_record()))
    await asyncio.sleep(0)

    assert not first.done()
    assert not second.done()

    store.allow_save.set()
    assert await first == _record()
    with pytest.raises(ProvisioningJournalError) as raised:
        await second

    assert raised.value.code == "journal_transaction_in_progress"
    assert store.events == [("save", "staged")]


async def test_terminal_cleanup_is_restart_safe_when_remove_fails(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
) -> None:
    journal = _journal(hass, memory_store)
    await _advance_to(journal, ProvisioningPhase.PENDING_CONFIG_COMMIT)
    memory_store.remove_error = OSError("disk detail must not be retained")

    with pytest.raises(ProvisioningJournalError) as raised:
        await journal.async_complete_commit(
            _TRANSACTION_ID,
            subentry_id="01J6H8J0BK5GJPPF3XEGQT71QX",
        )

    assert raised.value.code == "journal_remove_failed"
    assert "disk detail" not in str(raised.value)
    assert raised.value.__context__ is None
    assert raised.value.__cause__ is None
    assert memory_store.events[-2:] == [
        ("save", "committed"),
        ("remove", None),
    ]
    assert memory_store.data is not None
    assert memory_store.data["record"]["phase"] == "committed"

    restarted = _journal(hass, memory_store)
    completed = await restarted.async_complete_commit(
        _TRANSACTION_ID,
        subentry_id="01J6H8J0BK5GJPPF3XEGQT71QX",
    )

    assert completed.phase is ProvisioningPhase.COMMITTED
    assert memory_store.events[-1] == ("remove", None)
    assert memory_store.data is None


@pytest.mark.parametrize("boundary", ("load", "save"))
async def test_store_failures_do_not_retain_secret_exception_objects(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
    boundary: str,
) -> None:
    journal = _journal(hass, memory_store)
    secret_error = OSError(f"{boundary}-{_ROOT_PASSWORD}")
    if boundary == "load":
        memory_store.load_error = secret_error
        operation = journal.async_load()
        expected_code = "journal_load_failed"
    else:
        memory_store.save_error = secret_error
        operation = journal.async_create(_record())
        expected_code = "journal_save_failed"

    with pytest.raises(ProvisioningJournalError) as raised:
        await operation

    assert raised.value.code == expected_code
    assert raised.value.__context__ is None
    assert raised.value.__cause__ is None
    assert _ROOT_PASSWORD not in str(raised.value)
    assert _ROOT_PASSWORD not in repr(raised.value)


async def test_unexpected_parser_failure_does_not_retain_secret_exception_object(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = _journal(hass, memory_store)
    await journal.async_create(_record())

    def fail_parser(cls: type[ProvisioningRecord], raw: object) -> ProvisioningRecord:
        del cls, raw
        raise RuntimeError(f"parser-{_ROOT_PASSWORD}")

    monkeypatch.setattr(ProvisioningRecord, "_from_storage", classmethod(fail_parser))
    restarted = _journal(hass, memory_store)

    with pytest.raises(ProvisioningJournalError) as raised:
        await restarted.async_load()

    assert raised.value.code == "invalid_provisioning_journal"
    assert raised.value.__context__ is None
    assert raised.value.__cause__ is None
    assert _ROOT_PASSWORD not in str(raised.value)
    assert _ROOT_PASSWORD not in repr(raised.value)


async def test_second_transaction_and_transaction_mismatch_fail_without_writes(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
) -> None:
    journal = _journal(hass, memory_store)
    await journal.async_create(_record())
    event_count = len(memory_store.events)

    with pytest.raises(ProvisioningJournalError) as occupied:
        await journal.async_create(_record())
    assert occupied.value.code == "journal_transaction_in_progress"

    with pytest.raises(ProvisioningJournalError) as mismatch:
        await journal.async_transition(
            UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            ProvisioningPhase.ACTIVATION_PENDING,
        )
    assert mismatch.value.code == "journal_transaction_mismatch"
    assert len(memory_store.events) == event_count


async def test_last_error_update_is_redacted_and_durable(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
) -> None:
    journal = _journal(hass, memory_store)
    await journal.async_create(_record())
    failure = StoredJournalError(stage="panel_health", code="availability_timeout")

    updated = await journal.async_record_error(_TRANSACTION_ID, failure)
    restarted = _journal(hass, memory_store)

    assert updated.last_error == failure
    loaded = await restarted.async_load()
    assert loaded is not None
    assert loaded.last_error == failure
    assert repr(updated.last_error) == (
        "StoredJournalError(stage='panel_health', code='availability_timeout')"
    )
    assert _ROOT_PASSWORD not in repr(updated)


@pytest.mark.parametrize(
    "mutation",
    (
        {"unexpected": True},
        {"transaction_id": "not-a-uuid"},
        {"phase": "unknown"},
        {"started_at": "not-a-timestamp"},
        {"environment_content": "not-base64!!"},
    ),
)
async def test_corrupt_storage_fails_with_one_redacted_error(
    hass: HomeAssistant,
    memory_store: _MemoryStore,
    mutation: Mapping[str, object],
) -> None:
    journal = _journal(hass, memory_store)
    await journal.async_create(_record())
    assert memory_store.data is not None
    raw_record = memory_store.data["record"]
    assert isinstance(raw_record, dict)
    if "unexpected" in mutation:
        raw_record["unexpected"] = _ROOT_PASSWORD
    elif "environment_content" in mutation:
        snapshot = raw_record["prior_snapshot"]
        assert isinstance(snapshot, dict)
        environment_file = snapshot["environment_file"]
        assert isinstance(environment_file, dict)
        environment_file["content"] = mutation["environment_content"]
    else:
        raw_record.update(mutation)

    restarted = _journal(hass, memory_store)
    with pytest.raises(ProvisioningJournalError) as raised:
        await restarted.async_load()

    assert raised.value.code == "invalid_provisioning_journal"
    assert str(raised.value) == "invalid_provisioning_journal"
    assert _ROOT_PASSWORD not in str(raised.value)
    assert _ROOT_PASSWORD not in repr(raised.value)
    assert memory_store.data is not None
    assert memory_store.events[-1] == ("save", "staged")


def test_invalid_typed_record_fails_with_a_stable_redacted_error() -> None:
    with pytest.raises(ProvisioningJournalError) as raised:
        replace(
            _record(),
            transaction_id=UUID("00000000-0000-1000-8000-000000000000"),
        )

    assert raised.value.code == "invalid_provisioning_journal"
    assert str(raised.value) == "invalid_provisioning_journal"


def test_first_install_snapshot_is_an_explicit_empty_state() -> None:
    snapshot = _absent_snapshot()

    record = replace(_record(), prior_snapshot=snapshot)

    assert snapshot.layout is StoredPanelLayout.ABSENT
    assert snapshot.active_release_target is None
    assert snapshot.environment_file.content is None
    assert snapshot.version_file.content is None
    assert snapshot.bridge_service.unit_file.content is None
    assert snapshot.wifi_watchdog_service.unit_file.content is None
    assert snapshot.bus_watchdog_service.unit_file.content is None
    assert snapshot.selected_components == ()
    assert record.prior_snapshot == snapshot


def test_panel_request_requires_the_root_account_used_by_fleet_ssh() -> None:
    with pytest.raises(ProvisioningJournalError) as raised:
        replace(_record().panel_request, ssh_username="admin")

    assert raised.value.code == "invalid_provisioning_journal"


def test_fleet_profile_wrong_runtime_types_fail_with_the_redacted_error() -> None:
    with pytest.raises(ProvisioningJournalError) as raised:
        StoredFleetProfile(
            kind=BrokerKind.EXISTING_BROKER,
            host=42,  # type: ignore[arg-type]
            port=1883,
            tls_enabled=False,
        )

    assert raised.value.code == "invalid_provisioning_journal"
    assert str(raised.value) == "invalid_provisioning_journal"


@pytest.mark.parametrize(
    ("content", "mode"),
    (
        (None, 0o600),
        (b"present", None),
        (b"present", -1),
        (b"present", 0o1000),
        (b"present", True),
    ),
)
def test_file_snapshot_requires_consistent_bytes_and_safe_exact_mode(
    content: bytes | None,
    mode: int | None,
) -> None:
    with pytest.raises(ProvisioningJournalError) as raised:
        StoredFileSnapshot(content=content, mode=mode)

    assert raised.value.code == "invalid_provisioning_journal"


@pytest.mark.parametrize(("enabled", "active"), ((True, False), (False, True)))
def test_service_snapshot_rejects_state_without_an_owned_unit(
    enabled: bool,
    active: bool,
) -> None:
    absent_file = StoredFileSnapshot(content=None, mode=None)

    with pytest.raises(ProvisioningJournalError) as raised:
        StoredServiceSnapshot(
            unit_file=absent_file,
            enabled=enabled,
            active=active,
        )

    assert raised.value.code == "invalid_provisioning_journal"


def test_absent_layout_rejects_any_partial_owned_state() -> None:
    with pytest.raises(ProvisioningJournalError) as raised:
        replace(
            _absent_snapshot(),
            environment_file=_file(b"partial=1\n", 0o600),
        )

    assert raised.value.code == "invalid_provisioning_journal"


def test_legacy_layout_has_no_release_target_but_preserves_exact_owned_state() -> None:
    legacy = replace(
        _upgrade_snapshot(),
        layout=StoredPanelLayout.LEGACY_FIXED,
        active_release_target=None,
    )

    assert legacy.active_release_target is None
    assert legacy.environment_file == _file(_ENVIRONMENT_BYTES, 0o600)
    assert legacy.version_file == _file(_VERSION_BYTES, 0o644)
    assert legacy.bridge_service == _service(_BRIDGE_UNIT_BYTES)
    assert legacy.wifi_watchdog_service == _service(_WIFI_UNIT_BYTES)
    assert legacy.bus_watchdog_service == _service(_BUS_UNIT_BYTES)


def test_legacy_layout_losslessly_preserves_an_absent_version_marker() -> None:
    absent_file = StoredFileSnapshot(content=None, mode=None)
    legacy = replace(
        _upgrade_snapshot(),
        layout=StoredPanelLayout.LEGACY_FIXED,
        active_release_target=None,
        version_file=absent_file,
    )

    assert legacy.version_file == absent_file


def test_release_link_layout_requires_a_release_target() -> None:
    with pytest.raises(ProvisioningJournalError) as raised:
        replace(_upgrade_snapshot(), active_release_target=None)

    assert raised.value.code == "invalid_provisioning_journal"


@pytest.mark.parametrize(
    "target",
    (
        "/var/brilliant-mqtt/releases/0.5.7",
        "/var/brilliant-mqtt/releases/../evil--aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "/var/brilliant-mqtt/releases/0.5.7--AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "/var/brilliant-mqtt/releases/0.5.7--aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "/var/brilliant-mqtt/releases/0.5.7;reboot--aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "/tmp/0.5.7--aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ),
)
def test_release_target_is_constrained_to_the_owned_exact_path_grammar(
    target: str,
) -> None:
    with pytest.raises(ProvisioningJournalError) as raised:
        replace(_upgrade_snapshot(), active_release_target=target)

    assert raised.value.code == "invalid_provisioning_journal"

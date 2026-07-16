"""Off-panel adapter tests for the BVD pilot's deferred live boundary."""

from __future__ import annotations

import asyncio
import inspect
import json
import signal
import time
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from brilliant_mqtt.ha_control_protocol import manifest_topic, stable_id, state_topic
from tools.brilliant_bvd import live as bvd_live
from tools.brilliant_bvd.live import (
    BufferedStateSink,
    BusBindings,
    FrameworkBindings,
    LiveDependencies,
    LiveVirtualLightHost,
    NativeBvdBus,
    NativeScopedProbe,
    _parser,
    _read_private_password,
    _shutdown_components,
    _stock_host_identity,
    delete_exact_pilot,
    read_owner_status,
    run_live_pilot,
)
from tools.brilliant_bvd.single_light_pilot import (
    BVD_CONFIGURATION_PERIPHERAL_ID,
    BVD_DEVICE_ID,
    EXPECTED_BVD_PERIPHERAL_TYPES,
    EXPECTED_PROCESS_CONFIGS,
    OFFICE_DEVICE_ID,
    TARGET_ENTITY_ID,
    BvdTopology,
    CleanupReport,
    PeripheralFact,
    PilotConfig,
    PilotController,
    PilotGuardError,
    build_light_variables,
    peripheral_id_for_entity,
    validate_preflight,
)

NOW_MS = 1_800_000_000_000


def test_live_runner_converts_terminal_hangup_to_bounded_cleanup() -> None:
    assert bvd_live._STOP_SIGNALS == (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


def _variable(value: str, *, timestamp: int = NOW_MS - 1_000) -> object:
    return SimpleNamespace(value=value, timestamp=timestamp)


def _raw_bvd() -> object:
    peripherals: dict[str, object] = {}
    for peripheral_id, peripheral_type in EXPECTED_BVD_PERIPHERAL_TYPES.items():
        variables = (
            {"relay_device": _variable(OFFICE_DEVICE_ID)}
            if peripheral_id == "remote_bridge"
            else {"configuration_peripheral_id": _variable(BVD_CONFIGURATION_PERIPHERAL_ID)}
        )
        peripherals[peripheral_id] = SimpleNamespace(
            peripheral_type=peripheral_type,
            status=1,
            variables=variables,
        )
    return SimpleNamespace(
        id=BVD_DEVICE_ID,
        device_type=3,
        peripherals=peripherals,
    )


class _BusHarness:
    def __init__(
        self,
        *,
        owners: list[str] | None = None,
        pilot_present: bool = True,
        fail_device_read: bool = False,
        reconnect_on_subscribe: bool = False,
    ) -> None:
        self.events: list[str] = []
        self.deletions: list[tuple[str, str, int]] = []
        self.configuration_reads = 0
        self.owners = [OFFICE_DEVICE_ID] if owners is None else owners
        self.pilot_present = pilot_present
        self.fail_device_read = fail_device_read
        self.reconnect_on_subscribe = reconnect_on_subscribe
        self.reconnect_fired = False
        self.reconnect_callback: Callable[..., object] | None = None
        self.observers: list[object] = []
        harness = self

        class Observer:
            def __init__(self, loop: object) -> None:
                del loop
                harness.observers.append(self)

            async def start(self, processor: object, virtual_device_id: object) -> None:
                del processor, virtual_device_id
                harness.events.append("observer-start")

            def get_owning_device_id(self) -> str:
                return OFFICE_DEVICE_ID

            async def get_peripheral(self, device_id: str, peripheral_id: str) -> object:
                if (
                    device_id == "configuration_virtual_device"
                    and peripheral_id == BVD_CONFIGURATION_PERIPHERAL_ID
                ):
                    owner_index = min(harness.configuration_reads, len(harness.owners) - 1)
                    owner = harness.owners[owner_index]
                    harness.configuration_reads += 1
                    variables = {"owner": _variable(owner)}
                    variables.update(
                        {
                            f"process_config:{name}": _variable("encoded")
                            for name in EXPECTED_PROCESS_CONFIGS
                        }
                    )
                    return SimpleNamespace(variables=variables)
                if (
                    device_id == BVD_DEVICE_ID
                    and peripheral_id == peripheral_id_for_entity(TARGET_ENTITY_ID)
                    and harness.pilot_present
                ):
                    return SimpleNamespace(variables={})
                return None

            async def get_device(self, device_id: str) -> object:
                assert device_id == BVD_DEVICE_ID
                if harness.fail_device_read:
                    raise RuntimeError("BVD topology is corrupt")
                return _raw_bvd()

            async def subscribe(self, request: object) -> None:
                harness.events.append(f"subscribe:{cast(Any, request).device_id}")
                if harness.reconnect_on_subscribe and not harness.reconnect_fired:
                    harness.reconnect_fired = True
                    assert harness.reconnect_callback is not None
                    harness.reconnect_callback()

            async def shutdown(self) -> None:
                harness.events.append("observer-shutdown")

        class Client:
            async def delete_peripheral(
                self, device_id: str, peripheral_id: str, deletion_time_ms: int
            ) -> None:
                harness.deletions.append((device_id, peripheral_id, deletion_time_ms))

        class Processor:
            def __init__(self, **kwargs: object) -> None:
                harness.events.append(f"processor-name:{kwargs['my_name']}")
                self.client = Client()
                self._reconnect: object = None

            async def start(self) -> None:
                harness.events.append("processor-start")

            def is_connected(self) -> bool:
                return True

            def add_reconnect_callback(self, callback: object) -> None:
                self._reconnect = callback
                harness.reconnect_callback = cast(Callable[..., object], callback)

            async def shutdown(self) -> None:
                harness.events.append("processor-shutdown")

        self.bindings = BusBindings(
            observer_class=Observer,
            processor_class=Processor,
            peripheral_server=lambda observer: observer,
            client_class=Client,
            subscription_request=lambda *, device_id: SimpleNamespace(device_id=device_id),
        )


async def test_native_bus_starts_processor_before_observer_and_reads_only_bvd() -> None:
    harness = _BusHarness()
    bus = NativeBvdBus(
        bindings_factory=lambda: harness.bindings,
        stock_identity=lambda: "123:456789",
    )

    await bus.start()
    topology = await bus.snapshot()
    validate_preflight(
        PilotConfig("backyard-room:1", "Backyard Pilot", 120),
        topology,
        now_ms=NOW_MS,
    )
    await bus.shutdown()

    assert harness.events.index("processor-start") < harness.events.index("observer-start")
    peer_names = [event for event in harness.events if event.startswith("processor-name:")]
    assert len(peer_names) == 1 and peer_names[0].startswith("processor-name:brilliant_bvd_guard-")
    assert topology.process_config_peripheral_ids == EXPECTED_PROCESS_CONFIGS
    assert harness.configuration_reads == 2
    assert f"subscribe:{BVD_DEVICE_ID}" in harness.events
    assert "subscribe:configuration_virtual_device" in harness.events


async def test_native_bus_rejects_owner_change_during_double_read() -> None:
    harness = _BusHarness(owners=[OFFICE_DEVICE_ID, "another-panel"])
    bus = NativeBvdBus(
        bindings_factory=lambda: harness.bindings,
        stock_identity=lambda: "123:456789",
    )
    await bus.start()
    with pytest.raises(PilotGuardError, match="owner changed during"):
        await bus.snapshot()
    await bus.shutdown()


async def test_native_bus_rejects_reconnect_during_initial_subscriptions() -> None:
    harness = _BusHarness(reconnect_on_subscribe=True)
    bus = NativeBvdBus(
        bindings_factory=lambda: harness.bindings,
        stock_identity=lambda: "123:456789",
    )

    with pytest.raises(PilotGuardError, match="reconnected during initial"):
        await bus.start()

    assert "observer-shutdown" in harness.events
    assert "processor-shutdown" in harness.events


async def test_native_bus_notification_clock_is_driven_by_subscribed_pushes() -> None:
    now = 10.0
    harness = _BusHarness()
    bus = NativeBvdBus(
        bindings_factory=lambda: harness.bindings,
        stock_identity=lambda: "123:456789",
        clock=lambda: now,
    )
    await bus.start()
    marker = bus.notification_marker()
    observer = cast(Any, harness.observers[0])
    await observer.handle_notification(SimpleNamespace())

    await bus.wait_for_notification_after(marker, 0.01)
    assert bus.seconds_since_last_notification() == 0.0
    await bus.shutdown()


async def test_native_bus_reconnect_invalidates_sync_gate_before_async_callback() -> None:
    harness = _BusHarness()
    bus = NativeBvdBus(
        bindings_factory=lambda: harness.bindings,
        stock_identity=lambda: "123:456789",
    )
    callback_seen = asyncio.Event()

    async def mark_reconnect() -> None:
        callback_seen.set()

    bus.on_reconnect(mark_reconnect)
    await bus.start()
    bus._on_reconnect()

    assert bus.seconds_since_last_notification() is None
    await asyncio.wait_for(callback_seen.wait(), timeout=0.1)
    await bus.shutdown()


async def test_cleanup_only_deletes_only_fixed_bvd_peripheral() -> None:
    harness = _BusHarness()

    await delete_exact_pilot(bindings_factory=lambda: harness.bindings)

    assert len(harness.deletions) == 1
    device_id, peripheral_id, deletion_time_ms = harness.deletions[0]
    assert device_id == BVD_DEVICE_ID
    assert peripheral_id == peripheral_id_for_entity(TARGET_ENTITY_ID)
    assert deletion_time_ms > 0


async def test_owner_status_does_not_depend_on_bvd_topology() -> None:
    harness = _BusHarness(fail_device_read=True)

    panel_device_id, configuration_owner = await read_owner_status(
        bindings_factory=lambda: harness.bindings
    )

    assert panel_device_id == OFFICE_DEVICE_ID
    assert configuration_owner == OFFICE_DEVICE_ID
    assert harness.configuration_reads == 2


async def test_cleanup_only_is_idempotent_when_the_exact_pilot_is_absent() -> None:
    harness = _BusHarness(pilot_present=False)

    await delete_exact_pilot(bindings_factory=lambda: harness.bindings)

    assert harness.deletions == []


class _RoomAssignment:
    def __init__(self, *, room_ids: list[str]) -> None:
        self.room_ids = room_ids


class _FrameworkHarness:
    def __init__(self) -> None:
        self.config: object = None
        self.host_kwargs: dict[str, object] = {}
        self.variable_specs: dict[str, object] = {}
        self.internal_updates: list[tuple[str, object, bool]] = []
        self.deletions: list[tuple[str, int]] = []
        harness = self

        class Peripheral:
            def __init__(self) -> None:
                variables_method = cast(Any, self)._my_variables
                harness.variable_specs = variables_method()

            def _set_value_internal(self, name: str, value: object, *, notify: bool) -> None:
                harness.internal_updates.append((name, value, notify))

        class VariableSpec:
            def __init__(
                self,
                value_type: type,
                externally_settable: bool,
                *,
                default_value: object,
                push_func: object,
            ) -> None:
                self.value_type = value_type
                self.externally_settable = externally_settable
                self.default_value = default_value
                self.push_func = push_func

        class PeripheralConfig:
            def __init__(
                self, peripheral_id: str, peripheral_class: type, **kwargs: object
            ) -> None:
                self.peripheral_id = peripheral_id
                self.peripheral_class = peripheral_class
                self.virtual_device_id = kwargs["virtual_device_id"]
                harness.config = self

        class HostedSpec:
            def __init__(self, name: str, config: object, args: object) -> None:
                self.name = name
                self.config = config
                self.args = args

        class Host:
            def __init__(self, **kwargs: object) -> None:
                harness.host_kwargs = kwargs

            async def start(self) -> None:
                startables = cast(list[object], harness.host_kwargs["startables_to_host"])
                config = cast(Any, cast(Any, startables[0]).config)
                config.peripheral_class()

            async def shutdown(self) -> None:
                return None

        def delete_impl(host: object, peripheral_id: str, deletion_time_ms: int) -> None:
            del host
            harness.deletions.append((peripheral_id, deletion_time_ms))

        self.bindings = FrameworkBindings(
            hosted_startable_spec=HostedSpec,
            peripheral_base=Peripheral,
            variable_spec=VariableSpec,
            peripheral_config=PeripheralConfig,
            peripheral_host=Host,
            delete_impl=delete_impl,
        )


async def test_live_host_targets_bvd_and_only_commands_on_or_intensity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _FrameworkHarness()
    host = LiveVirtualLightHost(
        loop=asyncio.get_running_loop(),
        bindings_factory=lambda: harness.bindings,
    )
    config = PilotConfig("backyard-room:1", "Backyard Pilot", 120)
    variables = build_light_variables(config, room_assignment_type=_RoomAssignment)

    async def on_command(variable: str, value: object) -> bool:
        del variable, value
        return True

    # Avoid importing gflags off-panel; this method only points a runtime flag.
    monkeypatch.setattr(host, "_point_socket_at_panel", lambda: None)
    peripheral_id = peripheral_id_for_entity(TARGET_ENTITY_ID)
    await host.start(
        peripheral_id=peripheral_id,
        virtual_device_id=BVD_DEVICE_ID,
        variables=variables,
        on_command=on_command,
    )

    native_config = cast(Any, harness.config)
    assert native_config.peripheral_id == peripheral_id
    assert native_config.virtual_device_id == BVD_DEVICE_ID
    specs = cast(Mapping[str, Any], harness.variable_specs)
    assert specs["on"].push_func is not None
    assert specs["intensity"].push_func is not None
    assert specs["display_name"].push_func is None
    startable_id = cast(str, harness.host_kwargs["startable_id"])
    assert startable_id.startswith("brilliant_bvd_light-")
    assert startable_id != peripheral_id
    await host.update_variables({"on": 1, "intensity": 500})
    assert harness.internal_updates == [("on", 1, True), ("intensity", 500, True)]
    await host.delete(peripheral_id, NOW_MS)
    assert harness.deletions == [(peripheral_id, NOW_MS)]
    await host.shutdown()


def test_live_module_has_no_direct_message_bus_api_import() -> None:
    source = inspect.getsource(NativeBvdBus)
    module_source = Path(inspect.getfile(NativeBvdBus)).read_text(encoding="utf-8")
    assert "request_set_variables_in_peripheral" not in source
    assert "lib.message_bus_api" not in module_source


def test_stock_host_identity_requires_exactly_one_matching_process(tmp_path: Path) -> None:
    for pid, command, ticks in (
        (
            "101",
            b"/usr/bin/uwsgi\0--ini\0"
            b"/var/run/brilliant/startable_configs/brilliant_virtual_device_peripherals\0",
            "700",
        ),
        (
            "102",
            b"pgrep\0-af\0brilliant_virtual_device_peripherals\0",
            "800",
        ),
    ):
        process = tmp_path / pid
        process.mkdir()
        (process / "cmdline").write_bytes(command)
        suffix = "S " + " ".join(["0"] * 18 + [ticks])
        (process / "stat").write_text(f"{pid} (process name) {suffix}\n", encoding="ascii")

    assert _stock_host_identity(tmp_path) == "101:700"


async def test_shutdown_components_are_sequential_and_continue_after_error() -> None:
    events: list[str] = []

    class Component:
        def __init__(self, name: str, *, fail: bool = False) -> None:
            self.name = name
            self.fail = fail

        async def shutdown(self) -> None:
            events.append(self.name)
            if self.fail:
                raise RuntimeError(f"{self.name} failed")

    with pytest.raises(PilotGuardError, match="native bus peer shutdown failed"):
        await _shutdown_components(Component("observer", fail=True), Component("processor"))

    assert events == ["observer", "processor"]


async def test_probe_shutdown_retains_handles_until_a_real_retry() -> None:
    events: list[str] = []

    class Component:
        def __init__(self, name: str) -> None:
            self.name = name
            self.calls = 0

        async def shutdown(self) -> None:
            self.calls += 1
            events.append(f"{self.name}:{self.calls}")
            if self.name == "observer" and self.calls == 1:
                raise RuntimeError("transient")

    observer = Component("observer")
    processor = Component("processor")
    probe = NativeScopedProbe(_BusHarness().bindings)
    probe._observer = observer
    probe._processor = processor

    with pytest.raises(PilotGuardError, match="native bus peer shutdown failed"):
        await probe.shutdown()
    await probe.shutdown()

    assert events == ["observer:1", "processor:1", "observer:2", "processor:2"]


async def test_native_bus_shutdown_retains_handles_until_a_real_retry() -> None:
    events: list[str] = []

    class Component:
        def __init__(self, name: str) -> None:
            self.name = name
            self.calls = 0

        async def shutdown(self) -> None:
            self.calls += 1
            events.append(f"{self.name}:{self.calls}")
            if self.name == "observer" and self.calls == 1:
                raise RuntimeError("transient")

    observer = Component("observer")
    processor = Component("processor")
    bus = NativeBvdBus(bindings_factory=lambda: _BusHarness().bindings)
    bus._observer = observer
    bus._processor = processor
    bus._owning_device_id = OFFICE_DEVICE_ID

    with pytest.raises(PilotGuardError, match="native bus peer shutdown failed"):
        await bus.shutdown()
    await bus.shutdown()

    assert events == ["observer:1", "processor:1", "observer:2", "processor:2"]


async def test_live_host_shutdown_retains_handle_until_retry() -> None:
    class Host:
        def __init__(self) -> None:
            self.calls = 0

        async def shutdown(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient")

    native = Host()
    host = LiveVirtualLightHost(loop=asyncio.get_running_loop())
    host._host = native

    with pytest.raises(RuntimeError, match="transient"):
        await host.shutdown()
    await host.shutdown()

    assert native.calls == 2


async def test_live_host_bounds_an_awaitable_internal_state_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PeripheralBase:
        async def _set_value_internal(self, name: str, value: object, *, notify: bool) -> None:
            del self, name, value, notify
            await asyncio.Event().wait()

    host = LiveVirtualLightHost(loop=asyncio.get_running_loop())
    host._bindings = FrameworkBindings(
        hosted_startable_spec=object(),
        peripheral_base=PeripheralBase,
        variable_spec=object(),
        peripheral_config=object(),
        peripheral_host=object(),
        delete_impl=lambda *_args, **_kwargs: None,
    )
    host._instance = PeripheralBase()
    monkeypatch.setattr(bvd_live, "_HOST_UPDATE_TIMEOUT_S", 0.01)

    with pytest.raises(PilotGuardError, match="state reflection.*bounded timeout"):
        await host.update_variables({"on": 1})


async def test_buffered_sink_serializes_attach_with_newer_state() -> None:
    sink = BufferedStateSink()
    await sink.update_variables({"on": 1})
    entered = asyncio.Event()
    release = asyncio.Event()
    updates: list[dict[str, object]] = []

    class Target:
        async def update_variables(self, values: Mapping[str, object]) -> None:
            entered.set()
            await release.wait()
            updates.append(dict(values))

    attach = asyncio.create_task(sink.attach(Target()))
    await entered.wait()
    newer = asyncio.create_task(sink.update_variables({"intensity": 750}))
    await asyncio.sleep(0)
    assert not newer.done()
    release.set()
    await asyncio.gather(attach, newer)

    assert updates == [{"on": 1}, {"intensity": 750}]
    assert sink.snapshot() == {"on": 1, "intensity": 750}


def test_private_password_reader_accepts_only_nonempty_private_regular_file(
    tmp_path: Path,
) -> None:
    password = tmp_path / "mqtt-password"
    password.write_text("secret\n", encoding="utf-8")
    password.chmod(0o600)
    assert _read_private_password(password) == "secret"

    password.chmod(0o644)
    with pytest.raises(PilotGuardError, match="private"):
        _read_private_password(password)
    password.chmod(0o600)
    password.write_text("\n", encoding="utf-8")
    with pytest.raises(PilotGuardError, match="empty"):
        _read_private_password(password)

    link = tmp_path / "link"
    link.symlink_to(password)
    with pytest.raises(PilotGuardError, match="regular"):
        _read_private_password(link)


def test_cli_exposes_read_only_owner_status_and_all_apply_assertions() -> None:
    owner = _parser().parse_args(["--owner-status"])
    assert owner.owner_status is True
    assert owner.room_assignment_id is None

    apply = _parser().parse_args(
        [
            "--apply",
            "--cross-owner-cleanup-staged",
            "--stock-canary-approved",
            "--external-observer-approved",
        ]
    )
    assert apply.apply is True
    assert apply.cross_owner_cleanup_staged is True
    assert apply.stock_canary_approved is True
    assert apply.external_observer_approved is True
    assert "lock_path" not in vars(apply)


def _live_manifest() -> str:
    entity_stable_id = stable_id(TARGET_ENTITY_ID)
    return json.dumps(
        {
            "schema_version": 1,
            "mapping_version": 1,
            "revision": 1,
            "generated_at_ms": NOW_MS,
            "entities": [
                {
                    "stable_id": entity_stable_id,
                    "entity_id": TARGET_ENTITY_ID,
                    "domain": "light",
                    "device_class": None,
                    "friendly_name": "Backyard",
                    "ha_area": "Backyard",
                    "brilliant_room": "Backyard",
                    "commands": ["turn_on", "turn_off", "set_brightness"],
                    "capabilities": {"brightness": True},
                }
            ],
            "unsupported_domains": [],
        }
    )


def _live_state(*, sequence: int = 1, available: bool = True) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "mapping_version": 1,
            "stable_id": stable_id(TARGET_ENTITY_ID),
            "entity_id": TARGET_ENTITY_ID,
            "sequence": sequence,
            "generated_at_ms": NOW_MS + sequence,
            "available": available,
            "state": "on" if available else "unavailable",
            "attributes": {"brightness": 128} if available else {},
        }
    )


class _LiveState:
    def __init__(self) -> None:
        self.owner_timestamp_ms = time.time_ns() // 1_000_000
        self.active = False
        self.notification_serial = 0
        self.deleted = 0
        self.host_shutdowns = 0
        self.guard_shutdowns = 0
        self.postflight_bad = False
        self.room_present = True
        self.remove_room_on_second_read = False
        self.notification_age_checks = 0
        self.active_snapshot = asyncio.Event()

    def topology(self, *, postflight: bool = False) -> BvdTopology:
        facts = [
            PeripheralFact(
                peripheral_id,
                peripheral_type,
                1,
                (
                    {"relay_device": OFFICE_DEVICE_ID}
                    if peripheral_id == "remote_bridge"
                    else {"configuration_peripheral_id": BVD_CONFIGURATION_PERIPHERAL_ID}
                ),
            )
            for peripheral_id, peripheral_type in EXPECTED_BVD_PERIPHERAL_TYPES.items()
        ]
        if self.active:
            facts.append(
                PeripheralFact(
                    peripheral_id_for_entity(TARGET_ENTITY_ID),
                    27,
                    1,
                    {"configuration_peripheral_id": BVD_CONFIGURATION_PERIPHERAL_ID},
                )
            )
        return BvdTopology(
            owning_device_id=OFFICE_DEVICE_ID,
            configuration_owner=OFFICE_DEVICE_ID,
            owner_timestamp_ms=self.owner_timestamp_ms,
            bvd_device_type=3,
            stock_host_running=True,
            stock_host_identity=(
                "changed:999" if postflight and self.postflight_bad else "123:456"
            ),
            process_config_peripheral_ids=EXPECTED_PROCESS_CONFIGS,
            peripherals=tuple(facts),
        )


class _FakeLiveBus:
    def __init__(
        self,
        state: _LiveState,
        *,
        postflight: bool,
        snapshot_hook: Callable[[int], Awaitable[None]] | None,
    ) -> None:
        self.state = state
        self.postflight = postflight
        self.snapshot_hook = snapshot_hook
        self.snapshot_calls = 0
        self.room_reads = 0
        self.reconnect_callback: Callable[[], Awaitable[None]] | None = None

    async def start(self) -> None:
        return None

    async def snapshot(self) -> BvdTopology:
        self.snapshot_calls += 1
        if self.snapshot_hook is not None and not self.postflight:
            await self.snapshot_hook(self.snapshot_calls)
        if not self.postflight and self.snapshot_calls >= 3:
            self.state.active_snapshot.set()
        return self.state.topology(postflight=self.postflight)

    async def room_ids(self) -> frozenset[str]:
        self.room_reads += 1
        if self.state.remove_room_on_second_read and self.room_reads == 2:
            self.state.room_present = False
        return frozenset({"room:1"}) if self.state.room_present else frozenset()

    async def peripheral_exists(self, device_id: str, peripheral_id: str) -> bool:
        return (
            device_id == BVD_DEVICE_ID
            and peripheral_id == peripheral_id_for_entity(TARGET_ENTITY_ID)
            and self.state.active
        )

    def on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        self.reconnect_callback = callback

    def notification_marker(self) -> int:
        return self.state.notification_serial

    async def wait_for_notification_after(self, marker: int, timeout_s: float) -> None:
        del timeout_s
        if self.state.notification_serial <= marker:
            raise PilotGuardError("missing fake notification")

    def seconds_since_last_notification(self) -> float | None:
        self.state.notification_age_checks += 1
        return 0.0

    async def shutdown(self) -> None:
        self.state.guard_shutdowns += 1


class _FakeLiveHost:
    def __init__(
        self,
        state: _LiveState,
        *,
        after_start: Callable[[_FakeLiveHost], Awaitable[None]] | None = None,
    ) -> None:
        self.state = state
        self.after_start = after_start
        self.started = asyncio.Event()
        self.on_command: Callable[[str, object], Awaitable[bool]] | None = None
        self.peripheral_id: str | None = None
        self.updates: list[dict[str, object]] = []

    async def start(
        self,
        *,
        peripheral_id: str,
        virtual_device_id: str,
        variables: Mapping[str, object],
        on_command: Callable[[str, object], Awaitable[bool]],
    ) -> None:
        del variables
        assert virtual_device_id == BVD_DEVICE_ID
        self.peripheral_id = peripheral_id
        self.on_command = on_command
        self.state.active = True
        self.state.notification_serial += 1
        self.started.set()
        if self.after_start is not None:
            await self.after_start(self)

    async def update_variables(self, values: Mapping[str, object]) -> None:
        self.updates.append(dict(values))

    async def delete(self, peripheral_id: str, deletion_time_ms: int) -> None:
        assert peripheral_id == self.peripheral_id
        assert deletion_time_ms > 0
        self.state.active = False
        self.state.deleted += 1

    async def shutdown(self) -> None:
        self.state.host_shutdowns += 1


class _FakeProbe:
    def __init__(self, state: _LiveState) -> None:
        self.state = state

    async def contains(self, device_id: str, peripheral_id: str) -> bool:
        del device_id, peripheral_id
        return self.state.active

    async def shutdown(self) -> None:
        return None


class _FakeMessages:
    def __init__(self, queue: asyncio.Queue[object]) -> None:
        self.queue = queue

    def __aiter__(self) -> _FakeMessages:
        return self

    async def __anext__(self) -> object:
        return await self.queue.get()


class _FakeMqttClient:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[object] = asyncio.Queue()
        self.messages = _FakeMessages(self.queue)
        self.publications: list[tuple[str, str, bool]] = []
        self.on_exit: Callable[[], Awaitable[None]] | None = None
        self.push(manifest_topic(), _live_manifest(), retained=True)
        self.push(
            state_topic(stable_id(TARGET_ENTITY_ID)),
            _live_state(),
            retained=True,
        )

    async def __aenter__(self) -> _FakeMqttClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        del args
        if self.on_exit is not None:
            await self.on_exit()

    async def subscribe(self, topic: str, *, timeout: float) -> None:
        del topic, timeout

    async def publish(self, topic: str, *, payload: str, retain: bool) -> None:
        self.publications.append((topic, payload, retain))

    def push(self, topic: str, payload: str, *, retained: bool) -> None:
        self.queue.put_nowait(SimpleNamespace(topic=topic, payload=payload, retain=retained))

    def clear_initial_authority(self) -> None:
        while not self.queue.empty():
            self.queue.get_nowait()


class _LiveHarness:
    def __init__(
        self,
        *,
        snapshot_hook: Callable[[int], Awaitable[None]] | None = None,
        after_start: Callable[[_FakeLiveHost], Awaitable[None]] | None = None,
    ) -> None:
        self.state = _LiveState()
        self.mqtt = _FakeMqttClient()
        self.host = _FakeLiveHost(self.state, after_start=after_start)
        self.bus_count = 0
        self.buses: list[_FakeLiveBus] = []
        self.snapshot_hook = snapshot_hook

    def bus_factory(self) -> _FakeLiveBus:
        self.bus_count += 1
        bus = _FakeLiveBus(
            self.state,
            postflight=self.bus_count > 1,
            snapshot_hook=self.snapshot_hook,
        )
        self.buses.append(bus)
        return bus

    def dependencies(self) -> LiveDependencies:
        async def no_sleep(delay: float) -> None:
            del delay

        return LiveDependencies(
            bus_factory=self.bus_factory,
            host_factory=lambda _loop: self.host,
            probe_factory=lambda: asyncio.sleep(0, result=_FakeProbe(self.state)),
            mqtt_client_factory=lambda **_kwargs: self.mqtt,
            room_assignment_type=_RoomAssignment,
            lifecycle_sleep=no_sleep,
            absence_interval_s=0.0,
            lifecycle_operation_timeout_s=0.1,
        )


def _short_live_config(runtime_s: float = 0.05) -> PilotConfig:
    config = PilotConfig("room:1", "Backyard Pilot", 60)
    object.__setattr__(config, "active_runtime_s", runtime_s)
    return config


async def _run_harness(
    harness: _LiveHarness,
    *,
    config: PilotConfig | None = None,
    stop: asyncio.Event | None = None,
    ready: asyncio.Event | None = None,
) -> CleanupReport:
    return await run_live_pilot(
        config=_short_live_config() if config is None else config,
        mqtt_host="mqtt.local",
        mqtt_port=1883,
        mqtt_username="pilot",
        mqtt_password="secret",
        stop=stop,
        ready=ready,
        dependencies=harness.dependencies(),
    )


async def test_live_orchestration_deadline_deletes_and_validates_postflight() -> None:
    harness = _LiveHarness()

    report = await _run_harness(harness)

    assert report == CleanupReport(False, True, True)
    assert harness.state.deleted == 1
    assert harness.state.active is False
    assert harness.state.host_shutdowns == 1
    assert harness.bus_count == 2


async def test_final_short_budget_uses_bounded_supervisor_ticks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _LiveHarness()
    monkeypatch.setattr(bvd_live, "_MONITOR_INTERVAL_S", 0.0)
    monkeypatch.setattr(bvd_live, "_SUPERVISOR_TICK_S", 0.005)

    report = await asyncio.wait_for(
        _run_harness(harness, config=_short_live_config(0.02)),
        timeout=0.5,
    )

    assert report == CleanupReport(False, True, True)
    assert harness.state.notification_age_checks < 30


async def test_live_orchestration_aborts_and_deletes_on_authority_loss() -> None:
    harness: _LiveHarness

    async def lose_authority(_host: _FakeLiveHost) -> None:
        harness.mqtt.push(
            state_topic(stable_id(TARGET_ENTITY_ID)),
            _live_state(sequence=2, available=False),
            retained=False,
        )
        await asyncio.sleep(0)

    harness = _LiveHarness(after_start=lose_authority)

    with pytest.raises(PilotGuardError, match="aborted after bounded cleanup"):
        await _run_harness(harness)

    assert harness.state.deleted == 1
    assert harness.state.active is False


async def test_stop_does_not_mask_an_already_failed_mqtt_reader() -> None:
    harness = _LiveHarness()
    stop = asyncio.Event()
    ready = asyncio.Event()
    task = asyncio.create_task(
        _run_harness(
            harness,
            config=_short_live_config(60.0),
            stop=stop,
            ready=ready,
        )
    )
    await ready.wait()
    harness.mqtt.push(
        state_topic(stable_id(TARGET_ENTITY_ID)),
        "not-json",
        retained=False,
    )
    await asyncio.sleep(0)
    stop.set()

    with pytest.raises(PilotGuardError, match="aborted after bounded cleanup"):
        await asyncio.wait_for(task, timeout=1.0)

    assert harness.state.deleted == 1
    assert harness.state.active is False


async def test_back_to_back_authority_loss_and_recovery_still_aborts() -> None:
    harness = _LiveHarness()
    ready = asyncio.Event()
    task = asyncio.create_task(_run_harness(harness, config=_short_live_config(60.0), ready=ready))
    await ready.wait()
    harness.mqtt.push(
        state_topic(stable_id(TARGET_ENTITY_ID)),
        _live_state(sequence=2, available=False),
        retained=False,
    )
    harness.mqtt.push(
        state_topic(stable_id(TARGET_ENTITY_ID)),
        _live_state(sequence=3, available=True),
        retained=False,
    )

    with pytest.raises(PilotGuardError, match="aborted after bounded cleanup"):
        await asyncio.wait_for(task, timeout=1.0)

    assert harness.state.deleted == 1
    assert harness.state.active is False


async def test_live_orchestration_surfaces_firmware_callback_failure() -> None:
    async def fail_callback(host: _FakeLiveHost) -> None:
        assert host.on_command is not None
        with pytest.raises(PilotGuardError, match="unsupported native command variable"):
            await host.on_command("unsupported", 1)

    harness = _LiveHarness(after_start=fail_callback)

    with pytest.raises(PilotGuardError, match="aborted after bounded cleanup"):
        await _run_harness(harness)

    assert harness.state.deleted == 1


async def test_confirmation_timeout_restores_cached_ha_state_before_abort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def push_without_confirmation(host: _FakeLiveHost) -> None:
        assert host.on_command is not None
        assert await host.on_command("on", 0) is True

    harness = _LiveHarness(after_start=push_without_confirmation)
    restored_values: list[dict[str, object]] = []
    original_reapply = PilotController.reapply_authoritative_state

    async def track_reapply(controller: PilotController) -> None:
        await original_reapply(controller)
        restored_values.append(dict(harness.host.updates[-1]))

    monkeypatch.setattr(PilotController, "reapply_authoritative_state", track_reapply)
    monkeypatch.setattr(bvd_live, "_COMMAND_CONFIRMATION_TIMEOUT_MS", 1)
    monkeypatch.setattr(bvd_live, "_SUPERVISOR_TICK_S", 0.001)

    with pytest.raises(PilotGuardError, match="aborted after bounded cleanup"):
        await _run_harness(harness)

    assert restored_values == [{"on": 1, "intensity": 502}]
    assert harness.host.updates[-1] == {"on": 0}
    assert harness.state.deleted == 1


async def test_live_orchestration_cancellation_is_reshown_after_cleanup() -> None:
    harness = _LiveHarness()
    ready = asyncio.Event()
    task = asyncio.create_task(_run_harness(harness, config=_short_live_config(60.0), ready=ready))
    await ready.wait()
    await asyncio.sleep(0)

    assert task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert harness.state.deleted == 1
    assert harness.state.active is False


async def test_cancellation_survives_a_timed_out_authority_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HangingFenceHost(_FakeLiveHost):
        async def update_variables(self, values: Mapping[str, object]) -> None:
            if dict(values) == {"on": 0}:
                await asyncio.Event().wait()
            await super().update_variables(values)

    harness = _LiveHarness()
    harness.host = HangingFenceHost(harness.state)
    monkeypatch.setattr(bvd_live, "_HOST_UPDATE_TIMEOUT_S", 0.01)
    ready = asyncio.Event()
    task = asyncio.create_task(_run_harness(harness, config=_short_live_config(60.0), ready=ready))
    await ready.wait()

    assert task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.2)

    assert harness.state.deleted == 1
    assert harness.state.active is False


async def test_active_stop_is_clean_and_fences_before_mqtt_exit() -> None:
    harness = _LiveHarness()
    stop = asyncio.Event()
    exit_was_fenced = False

    async def push_during_exit() -> None:
        nonlocal exit_was_fenced
        assert harness.host.on_command is not None
        with pytest.raises(PilotGuardError, match="authoritative HA state"):
            await harness.host.on_command("on", 0)
        exit_was_fenced = True

    harness.mqtt.on_exit = push_during_exit
    ready = asyncio.Event()
    task = asyncio.create_task(
        _run_harness(
            harness,
            config=_short_live_config(60.0),
            stop=stop,
            ready=ready,
        )
    )
    await ready.wait()
    stop.set()

    report = await asyncio.wait_for(task, timeout=1.0)

    assert report == CleanupReport(False, True, True)
    assert exit_was_fenced is True
    assert harness.mqtt.publications == []
    assert harness.state.deleted == 1


async def test_final_preflight_rechecks_stop_before_persistent_registration() -> None:
    stop = asyncio.Event()

    async def stop_during_second_snapshot(call: int) -> None:
        if call == 2:
            stop.set()

    harness = _LiveHarness(snapshot_hook=stop_during_second_snapshot)

    with pytest.raises(PilotGuardError, match="aborted after bounded cleanup"):
        await _run_harness(harness, stop=stop)

    assert harness.host.peripheral_id is None
    assert harness.state.deleted == 0


async def test_stop_during_missing_initial_authority_does_not_wait_for_timeout() -> None:
    harness = _LiveHarness()
    harness.mqtt.clear_initial_authority()
    stop = asyncio.Event()
    stop.set()

    started = time.monotonic()
    with pytest.raises(PilotGuardError, match="aborted after bounded cleanup"):
        await asyncio.wait_for(_run_harness(harness, stop=stop), timeout=0.2)

    assert time.monotonic() - started < 0.2
    assert harness.host.peripheral_id is None


async def test_final_preflight_rechecks_authority_before_persistent_registration() -> None:
    harness: _LiveHarness

    async def lose_authority_during_second_snapshot(call: int) -> None:
        if call == 2:
            harness.mqtt.push(
                state_topic(stable_id(TARGET_ENTITY_ID)),
                _live_state(sequence=2, available=False),
                retained=False,
            )
            await asyncio.sleep(0)

    harness = _LiveHarness(snapshot_hook=lose_authority_during_second_snapshot)

    with pytest.raises(PilotGuardError, match="aborted after bounded cleanup"):
        await _run_harness(harness)

    assert harness.host.peripheral_id is None
    assert harness.state.deleted == 0


async def test_final_preflight_rechecks_reconnect_before_persistent_registration() -> None:
    harness: _LiveHarness

    async def reconnect_during_second_snapshot(call: int) -> None:
        if call == 2:
            callback = harness.buses[0].reconnect_callback
            assert callback is not None
            await callback()

    harness = _LiveHarness(snapshot_hook=reconnect_during_second_snapshot)

    with pytest.raises(PilotGuardError, match="aborted after bounded cleanup"):
        await _run_harness(harness)

    assert harness.host.peripheral_id is None
    assert harness.state.deleted == 0


async def test_final_preflight_rechecks_room_before_persistent_registration() -> None:
    harness = _LiveHarness()
    harness.state.remove_room_on_second_read = True

    with pytest.raises(PilotGuardError, match="aborted after bounded cleanup"):
        await _run_harness(harness)

    assert harness.host.peripheral_id is None
    assert harness.state.deleted == 0


async def test_postflight_topology_failure_prevents_clean_result() -> None:
    harness = _LiveHarness()
    harness.state.postflight_bad = True

    with pytest.raises(PilotGuardError, match="stock BVD host identity changed"):
        await _run_harness(harness)

    assert harness.state.deleted == 1


async def test_hanging_initial_state_reflection_is_bounded_before_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HangingHost(_FakeLiveHost):
        async def update_variables(self, values: Mapping[str, object]) -> None:
            del values
            await asyncio.Event().wait()

    harness = _LiveHarness()
    harness.host = HangingHost(harness.state)
    monkeypatch.setattr(bvd_live, "_HOST_UPDATE_TIMEOUT_S", 0.01)

    with pytest.raises(PilotGuardError, match="aborted after bounded cleanup"):
        await asyncio.wait_for(_run_harness(harness), timeout=0.2)

    assert harness.state.deleted == 1
    assert harness.state.active is False

"""Bounded timer/effect and publication lane for each synchronous mesh owner."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from brilliant_mqtt.discovery import state_topic
from brilliant_mqtt.mesh_registry import MeshRegistry
from brilliant_mqtt.mesh_settlement import MeshSettlement
from brilliant_mqtt.mesh_settlement_types import (
    ArmExpiry,
    CancelExpiry,
    Effect,
    Expire,
    PublishAccepted,
    PublishFailed,
    PublishStarted,
    RequestPublication,
    RevokeEffects,
    WakePublication,
    WindowToken,
)
from brilliant_mqtt.protocols import MqttClient

logger = logging.getLogger(__name__)
_SHUTDOWN_WORKER_SETTLE_S = 1.0


@dataclass
class _Lane:
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    idle: asyncio.Event = field(default_factory=asyncio.Event)
    worker: asyncio.Task[None] | None = None
    child: asyncio.Task[None] | None = None
    timer: asyncio.Task[None] | None = None
    window: WindowToken | None = None
    revoking: bool = False
    force: bool = False
    last_fields: dict[str, object] | None = None


class MeshDriver:
    def __init__(
        self,
        registry: MeshRegistry,
        mqtt: MqttClient,
        *,
        panel: str,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.registry, self.mqtt, self.panel = registry, mqtt, panel
        self.sleep = sleep
        self._lanes: dict[MeshSettlement, _Lane] = {}
        self.cleanup: Callable[[MeshSettlement], Awaitable[None]] | None = None
        registry.effects = self.apply

    @property
    def peripheral_ids(self) -> frozenset[str]:
        return frozenset(owner.metadata.peripheral_id for owner in self._lanes)

    @property
    def tasks(self) -> tuple[asyncio.Task[None], ...]:
        return tuple(
            task
            for lane in self._lanes.values()
            for task in (lane.worker, lane.child, lane.timer)
            if task is not None and not task.done()
        )

    @property
    def timers(self) -> dict[str, asyncio.Task[None]]:
        return {
            owner.metadata.peripheral_id: lane.timer
            for owner, lane in self._lanes.items()
            if lane.timer is not None and not lane.timer.done()
        }

    @property
    def children(self) -> dict[asyncio.Task[None], str]:
        return {
            lane.child: owner.metadata.peripheral_id
            for owner, lane in self._lanes.items()
            if lane.child is not None and not lane.child.done()
        }

    def published_fields(self, owner: MeshSettlement) -> Mapping[str, object] | None:
        """Read-only actual diff-cache fields, for publication diagnostics."""
        lane = self._lanes.get(owner)
        if lane is None or lane.last_fields is None:
            return None
        return MappingProxyType(lane.last_fields)

    def _lane(self, owner: MeshSettlement) -> _Lane:
        lane = self._lanes.get(owner)
        if lane is None:
            lane = _Lane()
            self._lanes[owner] = lane
        return lane

    def wake(self, owner: MeshSettlement, *, force: bool = False) -> None:
        if force:
            if not self.registry.live(owner):
                return
            envelope = self.registry.reserve(owner, "reconcile")
            if envelope is not None and self.registry.live(owner):
                self.registry.dispatch(owner, RequestPublication(envelope))
            return
        if not self.registry.live(owner, publication=True):
            return
        lane = self._lane(owner)
        lane.idle.clear()
        lane.wake.set()
        if lane.worker is None:
            lane.worker = asyncio.create_task(
                self._run(owner, lane), name=f"mesh-publisher-{owner.metadata.peripheral_id}"
            )
            lane.worker.add_done_callback(lambda task: self._worker_done(owner, lane, task))

    def _worker_done(self, owner: MeshSettlement, lane: _Lane, task: asyncio.Task[None]) -> None:
        lane.idle.set()
        self._cancel_timer(lane)
        self._retrieve(task)
        if (lane.child is None or lane.child.done()) and self._lanes.get(owner) is lane:
            self._lanes.pop(owner)

    def _child_done(self, owner: MeshSettlement, lane: _Lane, task: asyncio.Task[None]) -> None:
        self._retrieve(task)
        if (lane.worker is None or lane.worker.done()) and self._lanes.get(owner) is lane:
            lane.idle.set()
            self._lanes.pop(owner)

    def _retrieve(self, task: asyncio.Task[None]) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as error:
            self.registry.bus.fail_mesh(error)

    def apply(self, owner: MeshSettlement, effects: tuple[Effect, ...]) -> None:
        if not owner.active and owner not in self._lanes:
            return
        lane = self._lane(owner)
        for effect in effects:
            if isinstance(effect, RevokeEffects):
                if lane.child is not None and not lane.child.done():
                    lane.child.cancel()
                    lane.last_fields = None
                    if lane.worker is not None and not lane.revoking:
                        lane.revoking = True
                        lane.worker.cancel()
                elif not owner.active and lane.worker is not None:
                    lane.worker.cancel()
            elif isinstance(effect, CancelExpiry):
                if lane.window == effect.window:
                    self._cancel_timer(lane)
            elif isinstance(effect, ArmExpiry):
                self._cancel_timer(lane)
                if self.registry.live(owner):
                    lane.window = effect.window
                    lane.timer = asyncio.create_task(
                        self._expire(owner, lane, effect),
                        name=f"mesh-expiry-{owner.metadata.peripheral_id}",
                    )
            elif isinstance(effect, WakePublication):
                lane.force |= effect.force
                self.wake(owner)

    @staticmethod
    def _cancel_timer(lane: _Lane) -> None:
        timer, lane.timer = lane.timer, None
        lane.window = None
        if timer is not None and timer is not asyncio.current_task():
            timer.cancel()

    async def _expire(self, owner: MeshSettlement, lane: _Lane, effect: ArmExpiry) -> None:
        try:
            await self.sleep(max(0, effect.deadline - self.registry.clock()))
            envelope = self.registry.reserve(owner, "expiry")
            if envelope is not None and self.registry.live(owner) and lane.window == effect.window:
                self.registry.dispatch(
                    owner, Expire(envelope, effect.window, self.registry.clock())
                )
        finally:
            if lane.timer is asyncio.current_task():
                lane.timer, lane.window = None, None

    async def flush(self, owner: MeshSettlement, *, force: bool = False) -> None:
        if not owner.active:
            # Reconnect revokes publication, but exact retired routing cleanup
            # remains eligible on this existing external repair trigger.
            lane = self._lanes.get(owner)
            if lane is not None:
                if lane.worker is not None and not lane.worker.done():
                    _, pending = await asyncio.wait(
                        {lane.worker}, timeout=_SHUTDOWN_WORKER_SETTLE_S
                    )
                    if pending:
                        self.registry.bus.fail_mesh(
                            RuntimeError("obsolete mesh worker did not settle")
                        )
                        return
                if not await self._join_child(lane):
                    return
            if (
                self.registry.enabled
                and self.registry.bus.mesh_admission_open
                and self.registry.retired.get(owner.metadata.peripheral_id) is owner
            ):
                await self._cleanup(owner)
            return
        self.wake(owner, force=force)
        lane = self._lanes.get(owner)
        if lane is not None:
            await lane.idle.wait()

    async def _join_child(self, lane: _Lane) -> bool:
        child = lane.child
        if child is None:
            return True
        if not child.done():
            done, _ = await asyncio.wait({child}, timeout=_SHUTDOWN_WORKER_SETTLE_S)
            if not done:
                self.registry.bus.fail_mesh(RuntimeError("mesh publisher ignored cancellation"))
                return False
        try:
            child.result()
        except asyncio.CancelledError:
            pass
        except Exception as error:
            self.registry.bus.fail_mesh(error)
        if lane.child is child:
            lane.child = None
        return True

    async def _cleanup(self, owner: MeshSettlement) -> bool:
        if self.cleanup is None:
            return True
        try:
            await self.cleanup(owner)
        except Exception as error:
            logger.warning("MESH_CLEANUP_FAILED %s", type(error).__name__)
            return False
        return True

    async def _run(self, owner: MeshSettlement, lane: _Lane) -> None:
        try:
            while self.registry.live(owner, publication=True):
                try:
                    await lane.wake.wait()
                    lane.wake.clear()
                    if not await self._join_child(lane):
                        return
                    if (
                        owner.retired
                        and owner.view.terminal is None
                        and owner.view.ordinary_debt is None
                        and self.cleanup is not None
                    ):
                        if not await self._cleanup(owner):
                            lane.idle.set()
                            continue
                    if not self.registry.live(owner, publication=True):
                        return
                    fields: dict[str, object] = json.loads(owner.encode(self.registry.clock()))
                    if (
                        owner.view.terminal is None
                        and not lane.force
                        and fields == lane.last_fields
                    ):
                        lane.idle.set()
                        continue
                    lane.force = False
                    lane.child = asyncio.create_task(
                        self._attempt(owner, lane),
                        name=f"mesh-publish-{owner.metadata.peripheral_id}",
                    )
                    lane.child.add_done_callback(lambda task: self._child_done(owner, lane, task))
                    # Awaiting the Task directly would make cancellation wait forever
                    # if a broker child swallowed CancelledError.
                    await asyncio.wait({lane.child})
                    if not await self._join_child(lane):
                        return
                    if (
                        owner.retired
                        and owner.view.terminal is None
                        and owner.view.ordinary_debt is None
                        and self.cleanup is not None
                    ):
                        if not await self._cleanup(owner):
                            lane.idle.set()
                            continue
                except asyncio.CancelledError:
                    if not lane.revoking and lane.child is not None:
                        lane.child.cancel()
                    if not await self._join_child(lane):
                        return
                    if not lane.revoking:
                        return
                    lane.revoking = False
                    if not self.registry.live(owner, publication=True):
                        return
                if not lane.wake.is_set():
                    lane.idle.set()
        finally:
            lane.idle.set()
            self._cancel_timer(lane)
            if lane.child is None or lane.child.done():
                if self._lanes.get(owner) is lane:
                    self._lanes.pop(owner)

    async def _attempt(self, owner: MeshSettlement, lane: _Lane) -> None:
        if not await self._join_obsolete_children(owner):
            return
        envelope = self.registry.reserve(owner, "publish")
        if envelope is None or not self.registry.live(owner, publication=True):
            return
        token = owner.publication_token(self.mqtt)
        if token is None:
            return
        payload = owner.encode(self.registry.clock(), token=token)
        self.registry.dispatch(owner, PublishStarted(envelope, token))
        if (
            not self.registry.live(owner, publication=True)
            or token.client_session is not self.mqtt
            or owner.view.in_flight is not token
        ):
            return
        if token.terminal is not None:
            logger.warning(
                "mesh write %s for %s (receipt: %s)",
                token.terminal.reason,
                owner.metadata.peripheral_id,
                owner.view.receipt,
            )
        try:
            await self.mqtt.publish(
                state_topic(self.panel, owner.metadata.peripheral_id), payload, retain=True
            )
        except BaseException as error:
            lane.last_fields = None
            result = self.registry.reserve(owner, "publish_failed")
            if result is not None:
                self.registry.dispatch(owner, PublishFailed(result, token))
            if isinstance(error, asyncio.CancelledError):
                raise
            logger.warning("MESH_PUBLISH_FAILED %s", type(error).__name__)
            return
        result = self.registry.reserve(owner, "publish_accepted")
        if result is not None:
            self.registry.dispatch(owner, PublishAccepted(result, token))
            lane.last_fields = json.loads(payload)

    async def _join_obsolete_children(self, owner: MeshSettlement) -> bool:
        while self.registry.live(owner, publication=True):
            children = {
                other_lane.child
                for other, other_lane in self._lanes.items()
                if other is not owner
                and not self.registry.live(other, publication=True)
                and other_lane.child is not None
                and not other_lane.child.done()
            }
            if not children:
                return True
            done, pending = await asyncio.wait(children, timeout=_SHUTDOWN_WORKER_SETTLE_S)
            for child in done:
                self._retrieve(child)
            if pending:
                self.registry.bus.fail_mesh(RuntimeError("obsolete mesh publisher did not settle"))
                return False
        return False

    async def shutdown(self) -> None:
        """Bound joins after registry.close() synchronously shuts admission/ingress."""
        lanes = tuple(self._lanes.values())
        workers = set()
        for lane in lanes:
            self._cancel_timer(lane)
            if lane.child is not None:
                lane.child.cancel()
            if lane.worker is not None:
                lane.revoking = True
                lane.worker.cancel()
                workers.add(lane.worker)
        if workers:
            _, pending = await asyncio.wait(workers, timeout=_SHUTDOWN_WORKER_SETTLE_S)
            if pending:
                self.registry.bus.fail_mesh(RuntimeError("mesh publisher worker did not settle"))
        for owner, lane in tuple(self._lanes.items()):
            if lane.child is not None and not lane.child.done():
                self.registry.bus.fail_mesh(RuntimeError("mesh publisher child did not settle"))
            elif lane.worker is None or lane.worker.done():
                self._lanes.pop(owner)

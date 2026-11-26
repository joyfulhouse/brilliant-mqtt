"""Latest desired retained config and membership, serialized per MQTT topic."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from brilliant_mqtt.mesh_settlement import MeshSettlement
from brilliant_mqtt.protocols import MqttClient


@dataclass(frozen=True)
class DesiredTopic:
    owner: object
    valid: Callable[[], bool] = field(compare=False)
    payload: str | None = None
    membership: bool | None = None
    forget: bool = False


@dataclass
class TopicRepair:
    desired: DesiredTopic
    peripheral_id: str | None = None
    payload: str | None = None
    membership: bool | None = False
    applied: DesiredTopic | None = None
    worker: asyncio.Task[None] | None = None
    child: asyncio.Task[None] | None = None
    revoking: bool = False


class MeshRouting:
    """Records derive only from bounded registry owners, plus shared availability."""

    def __init__(
        self,
        mqtt: MqttClient,
        subscribed: set[str],
        fail: Callable[[Exception], None],
        publish: Callable[[str, str], Awaitable[None]],
    ) -> None:
        self.mqtt, self.subscribed = mqtt, subscribed
        self.fail = fail
        self.publish = publish
        self.records: dict[str, TopicRepair] = {}
        self._topics_by_peripheral: dict[str, set[str]] = {}
        self._withdrawn: set[str] = set()

    @property
    def peripheral_ids(self) -> frozenset[str]:
        return frozenset(self._topics_by_peripheral)

    def topics(self, peripheral_id: str) -> tuple[str, ...]:
        """Bounded work lookup; entries exist only while their records exist."""
        return tuple(self._topics_by_peripheral.get(peripheral_id, ()))

    @property
    def withdrawn_peripheral_ids(self) -> frozenset[str]:
        return frozenset(
            pid
            for topic in self._withdrawn
            if (pid := self.records[topic].peripheral_id) is not None
        )

    def repaired(
        self,
        topic: str,
        owner: object,
        *,
        payload: str | None = None,
        membership: bool | None = None,
    ) -> bool:
        record = self.records.get(topic)
        return (
            record is not None
            and record.desired.owner is owner
            and record.desired.payload == payload
            and record.desired.membership is membership
            and not record.desired.forget
            and record.applied is record.desired
            and (payload is None or record.payload == payload)
            and (membership is None or record.membership is membership)
        )

    def request(
        self, topic: str, desired: DesiredTopic, *, force: bool = False, revoke: bool = False
    ) -> asyncio.Task[None]:
        record = self.records.get(topic)
        if record is None:
            record = TopicRepair(
                desired,
                desired.owner.metadata.peripheral_id
                if isinstance(desired.owner, MeshSettlement)
                else None,
            )
            self.records[topic] = record
            if record.peripheral_id is not None:
                self._topics_by_peripheral.setdefault(record.peripheral_id, set()).add(topic)
        else:
            previous = record.desired
            if record.desired != desired or force:
                record.desired = desired
            if (
                revoke and previous != desired or previous.owner is not desired.owner
            ) and record.child is not None:
                record.child.cancel()
                if record.worker is not None and not record.revoking:
                    record.revoking = True
                    record.worker.cancel()
        if desired.owner is self and desired.forget:
            self._withdrawn.add(topic)
        else:
            self._withdrawn.discard(topic)
        if (
            not force
            and record.applied is record.desired
            and record.worker is not None
            and record.worker.done()
        ):
            return record.worker
        if record.worker is None or record.worker.done():
            record.worker = asyncio.create_task(self._run(topic, record), name="mesh-topic-repair")
            record.worker.add_done_callback(self._retrieve)
        return record.worker

    @staticmethod
    def _retrieve(task: asyncio.Task[None]) -> None:
        if not task.cancelled():
            task.exception()

    async def _run(self, topic: str, record: TopicRepair) -> None:
        while self.records.get(topic) is record:
            try:
                await self._repair(topic, record)
                return
            except asyncio.CancelledError:
                if not record.revoking:
                    raise
                child = record.child
                if child is not None:
                    _, pending = await asyncio.wait({child}, timeout=1.0)
                    if pending:
                        error = RuntimeError("obsolete mesh topic operation did not settle")
                        self.fail(error)
                        raise error from None
                    self._retrieve(child)
                record.child = None
                record.revoking = False
                record.applied = None
                record.payload = None

    async def _operation(self, record: TopicRepair, operation: Awaitable[None]) -> None:
        child = asyncio.ensure_future(operation)
        child.add_done_callback(self._retrieve)
        record.child = child
        try:
            await asyncio.wait({child})
            child.result()
        finally:
            if child.done() and record.child is child:
                record.child = None

    async def _publish(self, topic: str, record: TopicRepair, desired: DesiredTopic) -> None:
        assert desired.payload is not None
        if record.desired is not desired or not desired.valid():
            return
        await self.publish(topic, desired.payload)
        record.payload, record.applied = desired.payload, desired

    async def _membership(self, topic: str, record: TopicRepair, membership: bool) -> None:
        record.membership = None
        if membership:
            await self.mqtt.subscribe(topic)
            self.subscribed.add(topic)
        else:
            await self.mqtt.unsubscribe(topic)
            self.subscribed.discard(topic)
        record.membership = membership

    async def _repair(self, topic: str, record: TopicRepair) -> None:
        while self.records.get(topic) is record:
            desired = record.desired
            if not desired.valid():
                return
            if desired.payload is not None and record.applied is not desired:
                await self._operation(record, self._publish(topic, record, desired))
                # The broker accepted old bytes; remember them before repairing
                # the latest desired value, even if their owner was replaced.
                if record.desired is not desired:
                    continue
                if record.applied is not desired:
                    return
            if desired.membership is not None and desired.membership != record.membership:
                await self._operation(record, self._membership(topic, record, desired.membership))
                if record.desired is not desired:
                    continue
            if record.desired is desired:
                record.applied = desired
                if desired.forget:
                    self.records.pop(topic, None)
                    self._withdrawn.discard(topic)
                    if record.peripheral_id is not None:
                        topics = self._topics_by_peripheral[record.peripheral_id]
                        topics.discard(topic)
                        if not topics:
                            self._topics_by_peripheral.pop(record.peripheral_id)
                return

    async def retry_withdrawn(self) -> None:
        """Retry only outstanding withdrawal work after current desires are repaired."""
        await self.join(
            tuple(
                self.request(topic, self.records[topic].desired)
                for topic in tuple(self._withdrawn)
                if topic in self.records
            )
        )

    def withdraw(self) -> tuple[asyncio.Task[None], ...]:
        """Revoke shared publications immediately; only membership cleanup remains."""
        return tuple(
            self.request(
                topic,
                DesiredTopic(
                    self,
                    lambda: True,
                    membership=False if record.desired.membership is not None else None,
                    forget=True,
                ),
                revoke=True,
            )
            for topic, record in tuple(self.records.items())
        )

    def revoke(self, owner: object) -> None:
        for record in self.records.values():
            if record.desired.owner is owner and record.child is not None:
                record.child.cancel()
                if record.worker is not None and not record.revoking:
                    record.revoking = True
                    record.worker.cancel()

    async def join(self, tasks: tuple[asyncio.Task[None], ...]) -> None:
        if not tasks:
            return
        done, pending = await asyncio.wait(tasks, timeout=1.0)
        for task in done:
            self._retrieve(task)
        if pending:
            self.fail(RuntimeError("mesh topic workers did not settle"))

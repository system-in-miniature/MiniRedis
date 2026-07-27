from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from miniredis.core.commit import CommitBatch, SnapshotImage

if TYPE_CHECKING:
    from miniredis.runtime import MiniRedis


class ReplicaSinkState(StrEnum):
    DETACHED = "detached"
    BOOTSTRAPPING = "bootstrapping"
    STREAMING = "streaming"
    NEEDS_RESYNC = "needs_resync"
    FAILED = "failed"
    PROMOTING = "promoting"
    PROMOTED = "promoted"
    SOURCE_LOST = "source_lost"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class ReplicaAttachment:
    generation: int
    image: SnapshotImage


@dataclass(frozen=True, slots=True)
class ReplicaStatus:
    generation: int | None
    state: ReplicaSinkState
    baseline_seq: int
    applied_seq: int
    primary_seq: int
    lag: int
    queued: int


class ReplicaSink:
    def __init__(
        self,
        replica: MiniRedis,
        *,
        queue_limit: int,
        install_gate: asyncio.Event | None = None,
    ) -> None:
        if queue_limit <= 0:
            raise ValueError("replica queue limit must be positive")
        self._replica = replica
        self._queue_limit = queue_limit
        self._install_gate = install_gate
        self._primary: MiniRedis | None = None
        self._generation: int | None = None
        self._baseline_seq = 0
        self._applied_seq = 0
        self._primary_seq = 0
        self._state = ReplicaSinkState.DETACHED
        self._queue: deque[CommitBatch] = deque()
        self._queue_ready = asyncio.Event()
        self._apply_allowed = asyncio.Event()
        self._apply_allowed.set()
        self._attachment_captured = asyncio.Event()
        self._status_changed = asyncio.Event()
        self._attach_task: asyncio.Task[ReplicaStatus] | None = None
        self._task: asyncio.Task[None] | None = None

    def _signal_status_change(self) -> None:
        self._status_changed.set()

    @property
    def attachment_captured(self) -> asyncio.Event:
        return self._attachment_captured

    @property
    def status(self) -> ReplicaStatus:
        return ReplicaStatus(
            generation=self._generation,
            state=self._state,
            baseline_seq=self._baseline_seq,
            applied_seq=self._applied_seq,
            primary_seq=self._primary_seq,
            lag=max(0, self._primary_seq - self._applied_seq),
            queued=len(self._queue),
        )

    def pause(self) -> None:
        self._apply_allowed.clear()

    def resume(self) -> None:
        self._apply_allowed.set()

    def register_attachment(
        self,
        attachment: ReplicaAttachment,
    ) -> None:
        if self._state is not ReplicaSinkState.BOOTSTRAPPING:
            raise RuntimeError("sink is not bootstrapping")
        self._generation = attachment.generation
        self._baseline_seq = attachment.image.checkpoint_seq
        self._applied_seq = attachment.image.checkpoint_seq
        self._primary_seq = attachment.image.checkpoint_seq
        self._attachment_captured.set()
        self._signal_status_change()

    async def attach(self, primary: MiniRedis) -> ReplicaStatus:
        if self._state is not ReplicaSinkState.DETACHED:
            raise RuntimeError("replica sink is already attached")
        current = asyncio.current_task()
        assert current is not None
        self._attach_task = current
        self._primary = primary
        self._state = ReplicaSinkState.BOOTSTRAPPING
        self._signal_status_change()
        try:
            attachment = await primary.executor.attach_replica(self)
            if self._install_gate is not None:
                await self._install_gate.wait()
            if self._state is not ReplicaSinkState.BOOTSTRAPPING:
                return self.status
            installed = (
                await self._replica.executor.install_replica_snapshot(
                    self,
                    attachment.generation,
                    attachment.image,
                )
            )
            if not installed:
                return self.status
            if self._state is not ReplicaSinkState.BOOTSTRAPPING:
                return self.status
            self._state = ReplicaSinkState.STREAMING
            self._signal_status_change()
            self._task = asyncio.create_task(
                self._run_apply(),
                name=f"miniredis-replica-{attachment.generation}",
            )
            return self.status
        finally:
            self._attach_task = None

    def install_allowed(self, generation: int) -> bool:
        return (
            self._state is ReplicaSinkState.BOOTSTRAPPING
            and self._generation == generation
        )

    def offer(self, batch: CommitBatch) -> bool:
        if self._state not in {
            ReplicaSinkState.BOOTSTRAPPING,
            ReplicaSinkState.STREAMING,
        }:
            return False
        self._primary_seq = batch.seq
        if len(self._queue) >= self._queue_limit:
            self._queue.clear()
            self._state = ReplicaSinkState.NEEDS_RESYNC
            self._queue_ready.set()
            self._signal_status_change()
            return False
        self._queue.append(batch)
        self._queue_ready.set()
        self._signal_status_change()
        return True

    async def _run_apply(self) -> None:
        try:
            while self._state is ReplicaSinkState.STREAMING:
                await self._queue_ready.wait()
                await self._apply_allowed.wait()
                if not self._queue:
                    self._queue_ready.clear()
                    continue
                batch = self._queue.popleft()
                if not self._queue:
                    self._queue_ready.clear()
                assert self._generation is not None
                applied = await self._replica.executor.apply_replica_batch(
                    self._generation,
                    batch,
                )
                if not applied:
                    self._queue.clear()
                    self._state = ReplicaSinkState.NEEDS_RESYNC
                    self._signal_status_change()
                    if self._primary is not None:
                        await self._primary.executor.detach_replica(
                            self._generation
                        )
                    return
                self._applied_seq = batch.seq
                self._signal_status_change()
        except asyncio.CancelledError:
            raise
        except BaseException:
            self._state = ReplicaSinkState.FAILED
            self._queue.clear()
            self._signal_status_change()
            if self._primary is not None and self._generation is not None:
                await self._primary.executor.detach_replica(
                    self._generation
                )
        finally:
            self._signal_status_change()

    async def wait_until_applied(self, seq: int) -> None:
        terminal = {
            ReplicaSinkState.NEEDS_RESYNC,
            ReplicaSinkState.FAILED,
            ReplicaSinkState.SOURCE_LOST,
            ReplicaSinkState.STOPPED,
        }
        while True:
            if self._applied_seq >= seq:
                return
            if self._state in terminal:
                raise RuntimeError(
                    f"replica stopped at seq {self._applied_seq}"
                )
            self._status_changed.clear()
            if self._applied_seq >= seq:
                return
            if self._state in terminal:
                raise RuntimeError(
                    f"replica stopped at seq {self._applied_seq}"
                )
            await self._status_changed.wait()

    async def wait_until_stopped(self) -> None:
        task = self._task
        if task is not None:
            await asyncio.shield(task)

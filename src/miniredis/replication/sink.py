from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from miniredis.core.commit import CommitBatch
from miniredis.replication.backlog import (
    FullSyncAttachment,
    ReplicaAttachment,
    ReplicationCursor,
)

if TYPE_CHECKING:
    from miniredis.core.executor import PromotionResult
    from miniredis.runtime import MiniRedis


class ReplicaSinkState(StrEnum):
    DETACHED = "detached"
    BOOTSTRAPPING = "bootstrapping"
    CATCHING_UP = "catching_up"
    STREAMING = "streaming"
    NEEDS_RESYNC = "needs_resync"
    FAILED = "failed"
    PROMOTING = "promoting"
    PROMOTED = "promoted"
    SOURCE_LOST = "source_lost"
    STOPPED = "stopped"


class ReplicaSyncMode(StrEnum):
    FULL = "full"
    PARTIAL = "partial"


@dataclass(frozen=True, slots=True)
class ReplicaStatus:
    generation: int | None
    state: ReplicaSinkState
    baseline_seq: int
    applied_seq: int
    primary_seq: int
    lag: int
    queued: int
    replication_id: str | None
    sync_mode: ReplicaSyncMode | None
    cursor: ReplicationCursor | None


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
        self._replication_id: str | None = None
        self._sync_mode: ReplicaSyncMode | None = None
        self._state = ReplicaSinkState.DETACHED
        self._catch_up: deque[CommitBatch] = deque()
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
            replication_id=self._replication_id,
            sync_mode=self._sync_mode,
            cursor=self.cursor,
        )

    @property
    def cursor(self) -> ReplicationCursor | None:
        if self._replication_id is None:
            return None
        return ReplicationCursor(
            self._replication_id,
            self._applied_seq,
        )

    @property
    def owned_task_count(self) -> int:
        return sum(
            task is not None and not task.done()
            for task in (self._attach_task, self._task)
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
        self._replication_id = attachment.replication_id
        if isinstance(attachment, FullSyncAttachment):
            self._sync_mode = ReplicaSyncMode.FULL
            self._baseline_seq = attachment.image.checkpoint_seq
            self._applied_seq = attachment.image.checkpoint_seq
            self._primary_seq = attachment.image.checkpoint_seq
            self._catch_up.clear()
        else:
            self._sync_mode = ReplicaSyncMode.PARTIAL
            self._baseline_seq = attachment.cursor.applied_seq
            self._applied_seq = attachment.cursor.applied_seq
            self._primary_seq = attachment.boundary_seq
            self._catch_up = deque(attachment.batches)
        self._attachment_captured.set()
        self._signal_status_change()

    async def attach(self, primary: MiniRedis) -> ReplicaStatus:
        if self._state not in {
            ReplicaSinkState.DETACHED,
            ReplicaSinkState.NEEDS_RESYNC,
            ReplicaSinkState.SOURCE_LOST,
        }:
            raise RuntimeError("replica sink is already attached")
        previous_task = self._task
        if previous_task is not None and not previous_task.done():
            previous_task.cancel()
            try:
                await previous_task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._queue.clear()
        self._catch_up.clear()
        self._queue_ready.clear()
        self._attachment_captured.clear()
        current = asyncio.current_task()
        assert current is not None
        self._attach_task = current
        self._primary = primary
        self._state = ReplicaSinkState.BOOTSTRAPPING
        self._signal_status_change()
        try:
            attachment = await primary.executor.attach_replica(
                self,
                self.cursor,
            )
            if self._install_gate is not None:
                await self._install_gate.wait()
            if self._state is not ReplicaSinkState.BOOTSTRAPPING:
                return self.status
            if isinstance(attachment, FullSyncAttachment):
                installed = (
                    await self._replica.executor.install_replica_snapshot(
                        self,
                        attachment.generation,
                        attachment.replication_id,
                        attachment.image,
                    )
                )
            else:
                installed = (
                    await self._replica.executor.prepare_replica_resume(
                        attachment.generation,
                        attachment.replication_id,
                        attachment.cursor.applied_seq,
                    )
                )
            if not installed:
                self._state = ReplicaSinkState.NEEDS_RESYNC
                self._signal_status_change()
                return self.status
            if self._state is not ReplicaSinkState.BOOTSTRAPPING:
                return self.status
            self._state = (
                ReplicaSinkState.CATCHING_UP
                if self._catch_up
                else ReplicaSinkState.STREAMING
            )
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
            ReplicaSinkState.CATCHING_UP,
            ReplicaSinkState.STREAMING,
        }:
            return False
        self._primary_seq = batch.seq
        if len(self._queue) >= self._queue_limit:
            self._queue.clear()
            self._catch_up.clear()
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
            while self._state in {
                ReplicaSinkState.CATCHING_UP,
                ReplicaSinkState.STREAMING,
            }:
                if self._state is ReplicaSinkState.CATCHING_UP:
                    await self._apply_allowed.wait()
                    if not self._catch_up:
                        self._state = ReplicaSinkState.STREAMING
                        self._signal_status_change()
                        continue
                    batch = self._catch_up.popleft()
                else:
                    await self._queue_ready.wait()
                    await self._apply_allowed.wait()
                    if not self._queue:
                        self._queue_ready.clear()
                        continue
                    batch = self._queue.popleft()
                    if not self._queue:
                        self._queue_ready.clear()
                if batch.seq != self._applied_seq + 1:
                    await self._mark_needs_resync()
                    return
                assert self._generation is not None
                applied = await self._replica.executor.apply_replica_batch(
                    self._generation,
                    batch,
                )
                if not applied:
                    await self._mark_needs_resync()
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
                await self._primary.executor.detach_replica(self._generation)
        finally:
            self._signal_status_change()

    async def _mark_needs_resync(self) -> None:
        self._queue.clear()
        self._catch_up.clear()
        self._state = ReplicaSinkState.NEEDS_RESYNC
        self._signal_status_change()
        if self._primary is not None and self._generation is not None:
            await self._primary.executor.detach_replica(
                self._generation
            )

    async def disconnect(self) -> ReplicaStatus:
        generation = self._generation
        primary = self._primary
        if primary is not None and generation is not None:
            await primary.executor.detach_replica(generation)
            primary._release_replica_sink(self)
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._queue.clear()
        self._catch_up.clear()
        self._queue_ready.clear()
        self._attachment_captured.clear()
        self._generation = None
        self._primary = None
        self._state = ReplicaSinkState.DETACHED
        self._signal_status_change()
        return self.status

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
                raise RuntimeError(f"replica stopped at seq {self._applied_seq}")
            self._status_changed.clear()
            if self._applied_seq >= seq:
                return
            if self._state in terminal:
                raise RuntimeError(f"replica stopped at seq {self._applied_seq}")
            await self._status_changed.wait()

    async def wait_until_stopped(self) -> None:
        task = self._task
        if task is not None:
            await asyncio.shield(task)

    async def promote(self, *, source_alive: bool) -> PromotionResult:
        if self._generation is None:
            raise RuntimeError("replica sink has no generation")
        if self._state not in {
            ReplicaSinkState.BOOTSTRAPPING,
            ReplicaSinkState.STREAMING,
            ReplicaSinkState.NEEDS_RESYNC,
            ReplicaSinkState.SOURCE_LOST,
        }:
            raise RuntimeError(f"cannot promote sink in state {self._state}")
        self._state = ReplicaSinkState.PROMOTING
        self._signal_status_change()

        if source_alive:
            if self._primary is None:
                raise RuntimeError("live source is unavailable")
            primary = self._primary
            await primary.executor.detach_replica(self._generation)
            primary._release_replica_sink(self)

        task = self._task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._queue.clear()
        self._catch_up.clear()
        result = await self._replica.executor.promote_replica(self._generation)
        if not result.writable:
            self._state = ReplicaSinkState.FAILED
            self._signal_status_change()
            raise RuntimeError("replica generation is no longer promotable")
        self._applied_seq = result.applied_seq
        self._primary_seq = max(self._primary_seq, result.applied_seq)
        self._state = ReplicaSinkState.PROMOTED
        self._primary = None
        self._signal_status_change()
        return result

    async def source_crashed(self) -> None:
        self._state = ReplicaSinkState.SOURCE_LOST
        self._signal_status_change()
        attach_task = self._attach_task
        current = asyncio.current_task()
        if (
            attach_task is not None
            and attach_task is not current
            and not attach_task.done()
        ):
            attach_task.cancel()
            try:
                await attach_task
            except asyncio.CancelledError:
                pass
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._queue.clear()
        self._catch_up.clear()
        self._primary = None
        self._signal_status_change()

    async def drain_and_stop(self, grace_ms: int) -> None:
        attach_task = self._attach_task
        current = asyncio.current_task()
        if (
            attach_task is not None
            and attach_task is not current
            and not attach_task.done()
        ):
            attach_task.cancel()
            try:
                await attach_task
            except asyncio.CancelledError:
                pass

        task = self._task
        if task is not None and not task.done() and self._queue:
            self._apply_allowed.set()
            try:
                async with asyncio.timeout(grace_ms / 1000):
                    while self._queue and not task.done():
                        self._status_changed.clear()
                        if not self._queue or task.done():
                            break
                        await self._status_changed.wait()
            except TimeoutError:
                pass

        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._queue.clear()
        self._catch_up.clear()
        self._state = ReplicaSinkState.STOPPED
        self._primary = None
        self._signal_status_change()

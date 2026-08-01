# Stage 27 · Replication backlog

### Goal

Retain a bounded contiguous window of recent commits and use source identity plus replica cursor to decide atomically whether an attachment can resume from deltas or needs a full snapshot.

??? note "Deliverable files"
    - `src/miniredis/config.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/replication/backlog.py`
    - `src/miniredis/replication/sink.py`
    - `src/miniredis/runtime.py`
    - `tests/helpers/runtime.py`
    - `tests/replication/test_partial_resync.py`
    - `tests/unit/replication/test_backlog.py`

### The problem at this point

Every reconnect currently transfers a full snapshot even when the replica missed only a few batches. Resume is safe only if the cursor belongs to the same primary generation and every sequence after its applied position remains available through the exact attachment boundary. A cursor beyond the primary, before retained history, or from another source must fall back to full sync.

### Test contract

#### See the failure first

Using sequence alone can replay history from a restarted or promoted source with unrelated state. Returning a partial suffix with a gap presents divergence as synchronization. Reading backlog and current sequence in different executor turns can miss a concurrent commit between selection and registration. An empty suffix at the current sequence is valid partial sync, while an empty backlog that cannot cover an older cursor is not.

??? note "File diff: tests/helpers/runtime.py"
    ```diff
    diff --git a/tests/helpers/runtime.py b/tests/helpers/runtime.py
    index 25fb82f5e7ff5f6100067d93d1c577c215b2f26a..21cb364aa8823b3cb145cc4fd1e8ee1c02802500 100644
    --- a/tests/helpers/runtime.py
    +++ b/tests/helpers/runtime.py
    @@ -69,6 +69,7 @@ async def open_test_runtime(
         aof_ops: AofFileOps | None = None,
         aof_sleep: Callable[[float], Awaitable[None]] | None = None,
         aof_rewrite_gate: bool = False,
    +    replication_id_factory: Callable[[], str] | None = None,
         lifecycle_trace: bool = False,
     ) -> TestMiniRedis:
         loop = asyncio.get_running_loop()
    @@ -91,6 +92,7 @@ async def open_test_runtime(
                 aof_ops=rewrite_gate if rewrite_gate is not None else aof_ops,
                 aof_sleep=aof_sleep,
                 lifecycle_trace=[] if lifecycle_trace else None,
    +            replication_id_factory=replication_id_factory,
             ),
         )
         if snapshot_gate is not None:
    ```

Injects only the replication-ID factory through the real runtime so tests can state source-generation boundaries without replacing attachment logic.

??? note "File diff: tests/replication/test_partial_resync.py"
    ```diff
    diff --git a/tests/replication/test_partial_resync.py b/tests/replication/test_partial_resync.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..bea308e1178575344627cc68c46ca8dff40a901f
    --- /dev/null
    +++ b/tests/replication/test_partial_resync.py
    @@ -0,0 +1,115 @@
    +from dataclasses import dataclass
    +
    +import pytest
    +
    +from miniredis import CommandRequest
    +from miniredis.config import MiniRedisConfig
    +from miniredis.core.reply import Ok
    +from miniredis.replication.backlog import (
    +    FullSyncAttachment,
    +    PartialSyncAttachment,
    +    ReplicationCursor,
    +)
    +from tests.helpers.runtime import open_test_runtime
    +
    +
    +@dataclass
    +class AttachmentProbe:
    +    attachment: FullSyncAttachment | PartialSyncAttachment | None = None
    +
    +    def register_attachment(
    +        self,
    +        attachment: FullSyncAttachment | PartialSyncAttachment,
    +    ) -> None:
    +        self.attachment = attachment
    +
    +    def offer(self, _batch) -> bool:
    +        return True
    +
    +
    +async def seeded_primary(tmp_path, *, backlog_batches=4):
    +    primary = await open_test_runtime(
    +        config=MiniRedisConfig(
    +            replication_backlog_batches=backlog_batches,
    +        ),
    +        replication_id_factory=lambda: "primary-A",
    +    )
    +    client = primary.direct_client()
    +    for seq in range(1, 4):
    +        assert await client.execute(
    +            CommandRequest(
    +                b"SET",
    +                (f"k{seq}".encode(), str(seq).encode()),
    +            )
    +        ) == Ok()
    +    return primary
    +
    +
    +@pytest.mark.asyncio
    +async def test_first_attachment_is_full_sync(tmp_path):
    +    primary = await seeded_primary(tmp_path)
    +    probe = AttachmentProbe()
    +
    +    attachment = await primary.executor.attach_replica(probe, None)
    +
    +    assert isinstance(attachment, FullSyncAttachment)
    +    assert attachment.replication_id == "primary-A"
    +    assert attachment.image.checkpoint_seq == 3
    +    await primary.executor.detach_replica(attachment.generation)
    +    await primary.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_matching_cursor_uses_captured_backlog_range(tmp_path):
    +    primary = await seeded_primary(tmp_path)
    +    probe = AttachmentProbe()
    +    cursor = ReplicationCursor("primary-A", 1)
    +
    +    attachment = await primary.executor.attach_replica(probe, cursor)
    +
    +    assert isinstance(attachment, PartialSyncAttachment)
    +    assert attachment.cursor == cursor
    +    assert tuple(batch.seq for batch in attachment.batches) == (2, 3)
    +    assert attachment.boundary_seq == 3
    +    await primary.executor.detach_replica(attachment.generation)
    +    await primary.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_matching_current_cursor_uses_empty_partial_sync(tmp_path):
    +    primary = await seeded_primary(tmp_path)
    +    probe = AttachmentProbe()
    +    cursor = ReplicationCursor("primary-A", 3)
    +
    +    attachment = await primary.executor.attach_replica(probe, cursor)
    +
    +    assert isinstance(attachment, PartialSyncAttachment)
    +    assert attachment.batches == ()
    +    assert attachment.boundary_seq == 3
    +    await primary.executor.detach_replica(attachment.generation)
    +    await primary.close()
    +
    +
    +@pytest.mark.asyncio
    +@pytest.mark.parametrize(
    +    "cursor",
    +    [
    +        ReplicationCursor("other-primary", 1),
    +        ReplicationCursor("primary-A", 4),
    +        ReplicationCursor("primary-A", 0),
    +    ],
    +)
    +async def test_diverged_or_uncovered_cursor_falls_back_to_full_sync(
    +    tmp_path,
    +    cursor,
    +):
    +    primary = await seeded_primary(tmp_path, backlog_batches=2)
    +    probe = AttachmentProbe()
    +
    +    attachment = await primary.executor.attach_replica(probe, cursor)
    +
    +    assert isinstance(attachment, FullSyncAttachment)
    +    assert attachment.replication_id == "primary-A"
    +    assert attachment.image.checkpoint_seq == 3
    +    await primary.executor.detach_replica(attachment.generation)
    +    await primary.close()
    ```

Uses a deterministic source identity and attachment probe to lock first full sync, covered partial suffix, current-cursor empty partial sync, and full fallback for diverged/future/rotated cursors.

??? note "File diff: tests/unit/replication/test_backlog.py"
    ```diff
    diff --git a/tests/unit/replication/test_backlog.py b/tests/unit/replication/test_backlog.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..c7d1ad2ea8b3b545d7dabe0aa35ef4a5925be417
    --- /dev/null
    +++ b/tests/unit/replication/test_backlog.py
    @@ -0,0 +1,60 @@
    +import pytest
    +
    +from miniredis.replication.backlog import ReplicationBacklog
    +from tests.unit.persistence.test_framing import batch
    +
    +
    +def test_backlog_drops_oldest_batches_and_reports_bounds():
    +    backlog = ReplicationBacklog(capacity_batches=2)
    +    backlog.append(batch(1))
    +    backlog.append(batch(2))
    +    backlog.append(batch(3))
    +
    +    assert backlog.oldest_seq == 2
    +    assert backlog.newest_seq == 3
    +    assert backlog.batch_count == 2
    +    assert backlog.missing_after(1, current_seq=3) == (
    +        batch(2),
    +        batch(3),
    +    )
    +
    +
    +def test_backlog_distinguishes_current_empty_range_from_gap():
    +    backlog = ReplicationBacklog(capacity_batches=2)
    +    backlog.append(batch(4))
    +    backlog.append(batch(5))
    +
    +    assert backlog.missing_after(5, current_seq=5) == ()
    +    assert backlog.missing_after(2, current_seq=5) is None
    +    assert backlog.missing_after(6, current_seq=5) is None
    +
    +
    +def test_backlog_rejects_non_contiguous_append():
    +    backlog = ReplicationBacklog(capacity_batches=2)
    +    backlog.append(batch(1))
    +
    +    with pytest.raises(
    +        ValueError,
    +        match="replication backlog must be contiguous",
    +    ):
    +        backlog.append(batch(3))
    +
    +
    +def test_clear_removes_bounds_and_coverage():
    +    backlog = ReplicationBacklog(capacity_batches=2)
    +    backlog.append(batch(1))
    +    backlog.clear()
    +
    +    assert backlog.oldest_seq is None
    +    assert backlog.newest_seq is None
    +    assert backlog.batch_count == 0
    +    assert backlog.missing_after(0, current_seq=1) is None
    +
    +
    +@pytest.mark.parametrize("capacity", [0, -1])
    +def test_backlog_capacity_must_be_positive(capacity):
    +    with pytest.raises(
    +        ValueError,
    +        match="replication backlog capacity must be positive",
    +    ):
    +        ReplicationBacklog(capacity_batches=capacity)
    ```

Locks bounded oldest-first rotation, exposed sequence bounds, exact missing suffixes, current empty range, uncovered/future cursor rejection, contiguous append, clear, and positive capacity.

### Basic concepts

A replication cursor is `(replication_id, applied_seq)`. The replication ID names one source-history generation; sequence names a position inside it. The backlog is a bounded contiguous deque of committed batches. `missing_after` returns a complete suffix, an empty suffix when already current, or `None` when coverage cannot be proven. A full attachment carries a snapshot; a partial attachment carries the frozen missing suffix and boundary.

### Why this mechanism is necessary

Partial synchronization reduces transfer and installation cost after short disconnections, but optimization must never weaken history identity. A source-generation token prevents sequence-number reuse across restart or promotion, and explicit coverage prevents a bounded buffer from pretending it contains history that has rotated away.

### Runtime mental model

Every committed batch enters the backlog before it is offered live to sinks. On attach, the executor freezes current sequence, checks the cursor ID, asks backlog for the exact suffix through that boundary, chooses full or partial attachment, registers it with the sink, then releases the turn. Commits afterward are offered as live queued batches, so the frozen catch-up suffix and live stream meet without a gap.

### Mechanism blocks

#### Bounded replication history

Represent source identity, replica cursor, full/partial attachments, and a contiguous ring of recent commit batches with explicit coverage queries.

??? note "File diff: src/miniredis/replication/backlog.py"
    ```diff
    diff --git a/src/miniredis/replication/backlog.py b/src/miniredis/replication/backlog.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..868a725825d2b0d0471753ad735d3c9d33abd620
    --- /dev/null
    +++ b/src/miniredis/replication/backlog.py
    @@ -0,0 +1,87 @@
    +from __future__ import annotations
    +
    +from collections import deque
    +from dataclasses import dataclass
    +
    +from miniredis.core.commit import CommitBatch
    +from miniredis.core.commit import SnapshotImage
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ReplicationCursor:
    +    replication_id: str
    +    applied_seq: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class FullSyncAttachment:
    +    generation: int
    +    replication_id: str
    +    image: SnapshotImage
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class PartialSyncAttachment:
    +    generation: int
    +    replication_id: str
    +    cursor: ReplicationCursor
    +    boundary_seq: int
    +    batches: tuple[CommitBatch, ...]
    +
    +
    +ReplicaAttachment = FullSyncAttachment | PartialSyncAttachment
    +
    +
    +class ReplicationBacklog:
    +    def __init__(self, capacity_batches: int) -> None:
    +        if capacity_batches <= 0:
    +            raise ValueError(
    +                "replication backlog capacity must be positive"
    +            )
    +        self._capacity = capacity_batches
    +        self._batches: deque[CommitBatch] = deque()
    +
    +    @property
    +    def oldest_seq(self) -> int | None:
    +        return self._batches[0].seq if self._batches else None
    +
    +    @property
    +    def newest_seq(self) -> int | None:
    +        return self._batches[-1].seq if self._batches else None
    +
    +    @property
    +    def batch_count(self) -> int:
    +        return len(self._batches)
    +
    +    def append(self, batch: CommitBatch) -> None:
    +        if (
    +            self._batches
    +            and batch.seq != self._batches[-1].seq + 1
    +        ):
    +            raise ValueError("replication backlog must be contiguous")
    +        self._batches.append(batch)
    +        while len(self._batches) > self._capacity:
    +            self._batches.popleft()
    +
    +    def missing_after(
    +        self,
    +        applied_seq: int,
    +        *,
    +        current_seq: int,
    +    ) -> tuple[CommitBatch, ...] | None:
    +        if applied_seq > current_seq:
    +            return None
    +        if applied_seq == current_seq:
    +            return ()
    +        expected = applied_seq + 1
    +        selected = tuple(
    +            batch for batch in self._batches if batch.seq >= expected
    +        )
    +        if not selected or selected[0].seq != expected:
    +            return None
    +        if selected[-1].seq != current_seq:
    +            return None
    +        return selected
    +
    +    def clear(self) -> None:
    +        self._batches.clear()
    ```

Defines cursor/attachment domain values and a bounded contiguous deque with a three-way coverage result: suffix, current empty suffix, or unavailable.

```python
if not selected or selected[0].seq != expected:
    return None
```

The first selected batch must be exactly the next missing sequence; later retained history cannot repair an earlier gap.

#### Atomic attachment selection

At one executor boundary, compare cursor identity and backlog coverage, freeze either a snapshot or missing batch suffix, register the sink, and count the sync mode.

??? note "File diff: src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index 5c956da86e72f58179323ca204ed9aae458599ca..c8ade4b1ae6adac06444fc53bea1fd23c195d309 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -2,6 +2,7 @@ from __future__ import annotations

     import asyncio
     import itertools
    +import uuid
     from bisect import bisect_right
     from collections.abc import Callable
     from dataclasses import dataclass, replace
    @@ -69,7 +70,13 @@ from miniredis.persistence.aof import (
         AofRewriteFailed,
         AofRewriteOutcome,
     )
    -from miniredis.replication.sink import ReplicaAttachment
    +from miniredis.replication.backlog import (
    +    FullSyncAttachment,
    +    PartialSyncAttachment,
    +    ReplicaAttachment,
    +    ReplicationBacklog,
    +    ReplicationCursor,
    +)

     if TYPE_CHECKING:
         from miniredis.replication.sink import ReplicaSink
    @@ -144,6 +151,7 @@ class BeginAofRewrite:
     @dataclass(slots=True)
     class AttachReplica:
         sink: ReplicaSink
    +    cursor: ReplicationCursor | None
         future: asyncio.Future[ReplicaAttachment]


    @@ -241,6 +249,10 @@ type ExecutorMessage = (


     class CommandExecutor:
    +    @staticmethod
    +    def _default_replication_id() -> str:
    +        return uuid.uuid4().hex
    +
         def __init__(
             self,
             *,
    @@ -266,6 +278,8 @@ class CommandExecutor:
                 ]
                 | None
             ) = None,
    +        replication_backlog_batches: int = 1024,
    +        replication_id_factory: Callable[[], str] | None = None,
         ) -> None:
             self.database = database
             self.planner = planner
    @@ -326,6 +340,17 @@ class CommandExecutor:
             self._started = False
             self._allow_failure_injection = allow_failure_injection
             self._begin_aof_rewrite = begin_aof_rewrite
    +        self._replication_id_factory = (
    +            replication_id_factory
    +            if replication_id_factory is not None
    +            else self._default_replication_id
    +        )
    +        self.replication_id = self._replication_id_factory()
    +        self.replication_backlog = ReplicationBacklog(
    +            replication_backlog_batches
    +        )
    +        self.full_sync_count = 0
    +        self.partial_sync_count = 0

         def install_database_before_start(self, database: Database) -> None:
             if self._started:
    @@ -570,8 +595,35 @@ class CommandExecutor:
             elif isinstance(message, AttachReplica):
                 generation = self._next_replica_generation
                 self._next_replica_generation += 1
    -            image = self.database.snapshot_image(self.clock.now_ms())
    -            attachment = ReplicaAttachment(generation, image)
    +            boundary = self.database.commit_seq
    +            cursor = message.cursor
    +            missing = (
    +                None
    +                if (
    +                    cursor is None
    +                    or cursor.replication_id != self.replication_id
    +                )
    +                else self.replication_backlog.missing_after(
    +                    cursor.applied_seq,
    +                    current_seq=boundary,
    +                )
    +            )
    +            if missing is None:
    +                attachment: ReplicaAttachment = FullSyncAttachment(
    +                    generation,
    +                    self.replication_id,
    +                    self.database.snapshot_image(self.clock.now_ms()),
    +                )
    +                self.full_sync_count += 1
    +            else:
    +                attachment = PartialSyncAttachment(
    +                    generation,
    +                    self.replication_id,
    +                    cursor,
    +                    boundary,
    +                    missing,
    +                )
    +                self.partial_sync_count += 1
                 message.sink.register_attachment(attachment)
                 self._replica_sinks[generation] = message.sink
                 message.future.set_result(attachment)
    @@ -1110,6 +1162,7 @@ class CommandExecutor:
                 for operation in batch.operations
             )
             self._applied_batches.append(batch)
    +        self.replication_backlog.append(batch)
             self._offer_replica_batch(batch)
             return batch

    @@ -1161,11 +1214,15 @@ class CommandExecutor:
                 return AofRewriteFailed("executor control admission is closed")
             return await asyncio.shield(future)

    -    async def attach_replica(self, sink: ReplicaSink) -> ReplicaAttachment:
    +    async def attach_replica(
    +        self,
    +        sink: ReplicaSink,
    +        cursor: ReplicationCursor | None = None,
    +    ) -> ReplicaAttachment:
             future: asyncio.Future[ReplicaAttachment] = (
                 asyncio.get_running_loop().create_future()
             )
    -        if not self.post_control(AttachReplica(sink, future)):
    +        if not self.post_control(AttachReplica(sink, cursor, future)):
                 raise RuntimeError("executor control admission is closed")
             return await asyncio.shield(future)

    ```

Owns primary generation identity, appends each commit to backlog, and performs full/partial selection plus sink registration in one serialized turn.

??? note "File diff: src/miniredis/replication/sink.py"
    ```diff
    diff --git a/src/miniredis/replication/sink.py b/src/miniredis/replication/sink.py
    index 68d29ac442aed2f97f169024bc16d7a42361c971..52beb456710365921404722cdb101baa2134638e 100644
    --- a/src/miniredis/replication/sink.py
    +++ b/src/miniredis/replication/sink.py
    @@ -6,7 +6,11 @@ from dataclasses import dataclass
     from enum import StrEnum
     from typing import TYPE_CHECKING

    -from miniredis.core.commit import CommitBatch, SnapshotImage
    +from miniredis.core.commit import CommitBatch
    +from miniredis.replication.backlog import (
    +    FullSyncAttachment,
    +    ReplicaAttachment,
    +)

     if TYPE_CHECKING:
         from miniredis.core.executor import PromotionResult
    @@ -25,12 +29,6 @@ class ReplicaSinkState(StrEnum):
         STOPPED = "stopped"


    -@dataclass(frozen=True, slots=True)
    -class ReplicaAttachment:
    -    generation: int
    -    image: SnapshotImage
    -
    -
     @dataclass(frozen=True, slots=True)
     class ReplicaStatus:
         generation: int | None
    @@ -108,6 +106,8 @@ class ReplicaSink:
         ) -> None:
             if self._state is not ReplicaSinkState.BOOTSTRAPPING:
                 raise RuntimeError("sink is not bootstrapping")
    +        if not isinstance(attachment, FullSyncAttachment):
    +            raise RuntimeError("partial sync is not supported by this sink")
             self._generation = attachment.generation
             self._baseline_seq = attachment.image.checkpoint_seq
             self._applied_seq = attachment.image.checkpoint_seq
    ```

Consumes the shared attachment union but still rejects partial installation in this preparatory stage; Stage 28 will teach the sink to apply it.

#### Backlog configuration and identity

Validate retention capacity, generate one source identity per runtime generation, wire deterministic test identities, and expose backlog/sync counters.

??? note "File diff: src/miniredis/config.py"
    ```diff
    diff --git a/src/miniredis/config.py b/src/miniredis/config.py
    index fb4b46be7c3414ad76df3a7515610230387c9b39..ce49f2da3fc8d2a9f55284f5bdab265fceeff6f7 100644
    --- a/src/miniredis/config.py
    +++ b/src/miniredis/config.py
    @@ -26,6 +26,7 @@ class MiniRedisConfig:
         aof_rewrite_delta_limit_bytes: int = 8 * 1024 * 1024
         snapshot_path: Path | None = None
         replica_queue_limit: int = 64
    +    replication_backlog_batches: int = 1024
         replica_drain_grace_ms: int = 1000
         max_session_frames: int = 128

    @@ -61,6 +62,10 @@ class MiniRedisConfig:
                 )
             if self.replica_queue_limit <= 0:
                 raise ValueError("replica_queue_limit must be positive")
    +        if self.replication_backlog_batches <= 0:
    +            raise ValueError(
    +                "replication_backlog_batches must be positive"
    +            )
             if self.replica_drain_grace_ms < 0:
                 raise ValueError("replica_drain_grace_ms cannot be negative")
             if self.max_session_frames <= 0:
    ```

Adds a positive retained-batch capacity, making resume coverage and memory use explicit.

??? note "File diff: src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 0414fb79e2083db83c34d5844d7b906e347ade38..8a6f5d01aa5ed003c564e5ebaa2e7be7b11b19e2 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -108,6 +108,7 @@ class _RuntimeTestHooks:
         aof_ops: AofFileOps | None = None
         aof_sleep: Callable[[float], Awaitable[None]] | None = None
         lifecycle_trace: list[str] | None = None
    +    replication_id_factory: Callable[[], str] | None = None


     def _direct_transport_close(_reason: str) -> None:
    @@ -168,6 +169,12 @@ class MiniRedis:
                     else self._test_hooks.replica_apply_release
                 ),
                 allow_failure_injection=self._test_hooks is not None,
    +            replication_backlog_batches=config.replication_backlog_batches,
    +            replication_id_factory=(
    +                None
    +                if self._test_hooks is None
    +                else self._test_hooks.replication_id_factory
    +            ),
             )
             self.executor.mailbox.close_user_admission()
             self._snapshot_manager = (
    ```

Wires backlog capacity and per-generation ID creation into the executor and exposes source/backlog/sync observability.

### Verification evidence

Run both focused modules from `tests.txt`, cumulatively build Stages 1–27, and require owned-tree parity with `e65b568`.

### Durable takeaways

- Resume requires source identity and sequence coverage.
- Bounded history can return suffix, current-empty, or unavailable.
- Attachment selection and sink registration need one executor turn.
- This stage builds the decision contract before sink resume execution.

### Explain it in your own words

Why is cursor `(primary-A, 3)` unsafe against a restarted source also at sequence 3, and why is `missing_after(current_seq)` an empty tuple rather than unavailable?

### Textbook

The backlog is a bounded retained log, while the replication ID acts as an epoch. Together they form an epoch-offset cursor similar to log positions used in replicated databases.

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/8cd6d5e...e65b568)

After finishing, run `python -m journey.tools.build_journey check 27` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/27-replication-backlog/stage.patch)

# Stage 27 · 复制积压日志

### 目标

保留有界连续 Recent Commit Window，并使用 Source Identity + Replica Cursor 原子决定 Attachment 能从 Delta Resume，还是必须 Full Snapshot。

??? note "交付文件"
    - `src/miniredis/config.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/replication/backlog.py`
    - `src/miniredis/replication/sink.py`
    - `src/miniredis/runtime.py`
    - `tests/helpers/runtime.py`
    - `tests/replication/test_partial_resync.py`
    - `tests/unit/replication/test_backlog.py`

### 当前遇到的问题

目前每次 Reconnect 都传 Full Snapshot，即使 Replica 只漏了几个 Batch。只有 Cursor 属于同一 Primary Generation，且 Applied Position 后到精确 Attachment Boundary 的每个 Sequence 都仍可用时，Resume 才安全。Cursor 超过 Primary、早于 Retained History 或来自其他 Source 都必须回退 Full Sync。

### 测试契约

#### 先看会坏在哪里

只用 Sequence 会从 Restart/Promoted Source 重放无关 History。返回含 Gap 的 Partial Suffix 会把 Divergence 呈现成同步。在不同 Executor Turn 读取 Backlog 与 Current Sequence，会在 Selection/Registration 间漏 Concurrent Commit。Current Sequence 的 Empty Suffix 是合法 Partial Sync；不能覆盖旧 Cursor 的 Empty Backlog 则不是。

??? note "文件差异：tests/helpers/runtime.py"
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

只通过真实 Runtime 注入 Replication-ID Factory，让测试能表达 Source-generation Boundary，而不替换 Attachment Logic。

??? note "文件差异：tests/replication/test_partial_resync.py"
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

用确定性 Source Identity 与 Attachment Probe 锁定首次 Full Sync、Covered Partial Suffix、Current-cursor Empty Partial Sync，以及 Diverged/Future/Rotated Cursor 的 Full Fallback。

??? note "文件差异：tests/unit/replication/test_backlog.py"
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

锁定有界 Oldest-first Rotation、Sequence Bound、精确 Missing Suffix、Current Empty Range、Uncovered/Future Cursor Rejection、Contiguous Append、Clear 与 Positive Capacity。

### 基本概念

Replication Cursor 是 `(replication_id, applied_seq)`。Replication ID 命名一次 Source-history Generation；Sequence 命名其中位置。Backlog 是 Bounded Contiguous CommitBatch Deque。`missing_after` 返回完整 Suffix、已经 Current 时的 Empty Suffix，或无法证明 Coverage 时的 `None`。Full Attachment 携带 Snapshot；Partial Attachment 携带 Frozen Missing Suffix 与 Boundary。

### 为什么需要这个机制

Partial Synchronization 降低短暂断连后的传输与安装成本，但 Optimization 不能削弱 History Identity。Source-generation Token 防止 Restart/Promotion 后 Sequence Reuse，Explicit Coverage 防止 Bounded Buffer 假装仍含已经 Rotate Away 的 History。

### 运行时心智模型

每个 Committed Batch 在 Live Offer 给 Sink 前先进入 Backlog。Attach 时 Executor 冻结 Current Sequence，检查 Cursor ID，向 Backlog 请求到该 Boundary 的精确 Suffix，选择 Full/Partial Attachment，注册 Sink，再释放 Turn。之后 Commit 作为 Live Queued Batch Offer，因此 Frozen Catch-up Suffix 与 Live Stream 无 Gap 相接。

### 机制板块

#### 有界 Replication History

表示 Source Identity、Replica Cursor、Full/Partial Attachment，以及带显式 Coverage Query 的连续 Recent Commit Batch Ring。

??? note "文件差异：src/miniredis/replication/backlog.py"
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

定义 Cursor/Attachment Domain Value 与 Bounded Contiguous Deque，其 Coverage Result 有三种：Suffix、Current Empty Suffix、Unavailable。

```python
if not selected or selected[0].seq != expected:
    return None
```

第一个 Selected Batch 必须恰好是 Next Missing Sequence；更晚 Retained History 无法修复早期 Gap。

#### 原子 Attachment Selection

在一个 Executor Boundary 中比较 Cursor Identity 与 Backlog Coverage，冻结 Snapshot 或 Missing Batch Suffix，注册 Sink 并统计 Sync Mode。

??? note "文件差异：src/miniredis/core/executor.py"
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

持有 Primary Generation Identity，把每个 Commit Append 到 Backlog，并在一个 Serialized Turn 中完成 Full/Partial Selection 与 Sink Registration。

??? note "文件差异：src/miniredis/replication/sink.py"
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

消费共享 Attachment Union，但在本准备阶段仍拒绝 Partial Installation；Stage 28 会让 Sink 真正应用它。

#### Backlog 配置与 Identity

校验 Retention Capacity，为每个 Runtime Generation 生成一个 Source Identity，接线确定性 Test Identity，并暴露 Backlog/Sync Counter。

??? note "文件差异：src/miniredis/config.py"
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

加入正数 Retained-batch Capacity，使 Resume Coverage 与 Memory Use 显式化。

??? note "文件差异：src/miniredis/runtime.py"
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

把 Backlog Capacity 与 Per-generation ID Creation 接入 Executor，并暴露 Source/Backlog/Sync Observability。

### 验证证据

运行 `tests.txt` 中两个聚焦模块，累计构建 Stage 1–27，并要求 Owned-tree 与 `e65b568` 一致。

### 需要真正记住的内容

- Resume 需要 Source Identity 与 Sequence Coverage。
- Bounded History 可返回 Suffix、Current-empty 或 Unavailable。
- Attachment Selection 与 Sink Registration 需要一个 Executor Turn。
- 本阶段先建立 Decision Contract，下一阶段再执行 Sink Resume。

### 用自己的话讲清楚

为什么 Cursor `(primary-A, 3)` 对同样位于 Sequence 3 的 Restarted Source 不安全？为什么 `missing_after(current_seq)` 返回 Empty Tuple 而不是 Unavailable？

### 教材

Backlog 是 Bounded Retained Log，Replication ID 则充当 Epoch。两者形成类似复制数据库中 Log Position 的 Epoch-offset Cursor。

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/8cd6d5e...e65b568)

完成后可运行 `python -m journey.tools.build_journey check 27` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/27-replication-backlog/stage.patch)

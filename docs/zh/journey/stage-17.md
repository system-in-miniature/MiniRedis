# Stage 17 · 异步复制

### 目标

在 Snapshot 安装期间不遗漏并发 Commit 地接入 Replica，同时让副本延迟和失败留在 Primary 写入路径之外。

??? note "交付文件"
    - `src/miniredis/commands/model.py`
    - `src/miniredis/config.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/replication/sink.py`
    - `src/miniredis/runtime.py`
    - `tests/helpers/runtime.py`
    - `tests/replication/test_sink_attach.py`
    - `tests/replication/test_sink_failure_isolation.py`
    - `tests/replication/test_sink_lag.py`
    - `tests/replication/test_sink_overflow.py`
    - `tests/unit/commands/test_command_traits.py`

### 当前遇到的问题

Snapshot 安装完成时已经可能过时。Primary 必须建立一个有序边界：序号 N 及以前的状态在 Image 中，N 之后的每个 Commit 都进入 Stream。这个 Stream 既不能无限增长，也不能成为写入确认的同步依赖。

### 测试契约

#### 先看会坏在哪里

安装暂停期间提交的写入可能掉进 Snapshot 与 Stream 之间；暂停的 Follower 可能无限占用内存或阻塞 Primary；Replica Apply 异常可能杀死健康的主库流量；靠命令名短名单实现只读还会漏过新加入的写命令。

??? note "文件差异：tests/helpers/runtime.py"
    ```diff
    diff --git a/tests/helpers/runtime.py b/tests/helpers/runtime.py
    index ce21d46eb6dc3c067db30674e9c00a74513a2c70..73d7f4cb16db30bca888f59c6dee1daa75990fcd 100644
    --- a/tests/helpers/runtime.py
    +++ b/tests/helpers/runtime.py
    @@ -39,6 +39,7 @@ async def open_test_runtime(
         aof_appender=None,
         config=None,
         snapshot_write_gate: bool = False,
    +    replica_apply_failure: BaseException | None = None,
     ) -> TestMiniRedis:
         loop = asyncio.get_running_loop()
         snapshot_gate = GateSnapshotFileOps(loop) if snapshot_write_gate else None
    @@ -49,6 +50,7 @@ async def open_test_runtime(
             test_hooks=_RuntimeTestHooks(
                 aof_appender=aof_appender,
                 snapshot_ops=snapshot_gate,
    +            replica_apply_failure=replica_apply_failure,
             ),
         )
         if snapshot_gate is not None:
    ```

**锁定什么**

Helper 只提供 Gate 与一次性失败注入，仍然经过真实复制机制。

**如何构造反例**

通过 Runtime 持有的 Test Hook 把失败传入真实 Executor。

**关键测试语句**

```python
replica_apply_failure=replica_apply_failure,
```

**失败意味着什么**

测试证明的是 Fake Link，而不是学习者走读的 Runtime 路径。

??? note "文件差异：tests/replication/test_sink_attach.py"
    ```diff
    diff --git a/tests/replication/test_sink_attach.py b/tests/replication/test_sink_attach.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..8d585882b79399e5686b5be3bca84cce2b2280e0
    --- /dev/null
    +++ b/tests/replication/test_sink_attach.py
    @@ -0,0 +1,66 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest
    +from miniredis.core.reply import Bytes, Ok
    +from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
    +from tests.helpers.runtime import open_test_runtime
    +
    +
    +@pytest.mark.asyncio
    +async def test_attach_registers_incremental_stream_with_snapshot_capture():
    +    primary = await open_test_runtime()
    +    replica = await open_test_runtime()
    +    client = primary.direct_client()
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"before", b"1"))
    +    ) == Ok()
    +
    +    install_gate = asyncio.Event()
    +    sink = ReplicaSink(
    +        replica,
    +        queue_limit=4,
    +        install_gate=install_gate,
    +    )
    +    attaching = asyncio.create_task(primary.attach_replica(sink))
    +    await sink.attachment_captured.wait()
    +
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"during", b"2"))
    +    ) == Ok()
    +    assert sink.status.baseline_seq == 1
    +    assert sink.status.queued == 1
    +
    +    install_gate.set()
    +    await attaching
    +    await sink.wait_until_applied(2)
    +
    +    replica_client = replica.direct_client()
    +    assert await replica_client.execute(
    +        CommandRequest(b"GET", (b"before",))
    +    ) == Bytes(b"1")
    +    assert await replica_client.execute(
    +        CommandRequest(b"GET", (b"during",))
    +    ) == Bytes(b"2")
    +    assert sink.status.state is ReplicaSinkState.STREAMING
    +    assert sink.status.applied_seq == 2
    +    await primary.close()
    +    await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_attached_replica_rejects_user_writes():
    +    primary = await open_test_runtime()
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=4)
    +    await primary.attach_replica(sink)
    +
    +    reply = await replica.direct_client().execute(
    +        CommandRequest(b"SET", (b"k", b"v"))
    +    )
    +
    +    assert reply.code == "READONLY"
    +    assert replica.debug_commit_seq == sink.status.applied_seq
    +    await primary.close()
    +    await replica.close()
    ```

**锁定什么**

锁定 Snapshot 到 Stream 的交接以及 Replica 只读边界。

**如何构造反例**

在捕获 Attachment 后暂停安装，提交另一次写入，再验证 Replica 同时得到 Baseline 与排队状态。

**关键测试语句**

```python
assert sink.status.baseline_seq == 1
assert sink.status.queued == 1
```

**失败意味着什么**

Capture 与 Stream 注册不是同一个 Primary 有序动作，或 Follower Runtime 仍接受数据集写入。

??? note "文件差异：tests/replication/test_sink_failure_isolation.py"
    ```diff
    diff --git a/tests/replication/test_sink_failure_isolation.py b/tests/replication/test_sink_failure_isolation.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..46c30763015431dd48eea639540c9b00b5e164ae
    --- /dev/null
    +++ b/tests/replication/test_sink_failure_isolation.py
    @@ -0,0 +1,28 @@
    +import pytest
    +
    +from miniredis import CommandRequest
    +from miniredis.core.reply import Ok
    +from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
    +from tests.helpers.runtime import open_test_runtime
    +
    +
    +@pytest.mark.asyncio
    +async def test_replica_apply_exception_detaches_only_that_sink():
    +    primary = await open_test_runtime()
    +    replica = await open_test_runtime(
    +        replica_apply_failure=RuntimeError("replica failed"),
    +    )
    +    sink = ReplicaSink(replica, queue_limit=2)
    +    await primary.attach_replica(sink)
    +
    +    assert await primary.direct_client().execute(
    +        CommandRequest(b"SET", (b"k", b"v"))
    +    ) == Ok()
    +    await sink.wait_until_stopped()
    +
    +    assert sink.status.state is ReplicaSinkState.FAILED
    +    assert primary.debug_commit_seq == 1
    +    assert primary.state.name == "RUNNING"
    +    assert primary.debug_stats().replica_links == 0
    +    await primary.close()
    +    await replica.close()
    ```

**锁定什么**

锁定单条 Replica Link 与 Primary Executor 之间的失败隔离。

**如何构造反例**

Primary 接受 Commit 后，向 Replica Apply 注入异常。

**关键测试语句**

```python
assert sink.status.state is ReplicaSinkState.FAILED
assert primary.state.name == "RUNNING"
```

**失败意味着什么**

Replica 工作仍然拥有或污染 Primary 请求结果。

??? note "文件差异：tests/replication/test_sink_lag.py"
    ```diff
    diff --git a/tests/replication/test_sink_lag.py b/tests/replication/test_sink_lag.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..143690b69530e078f9eac7e818b03b45afbe9f38
    --- /dev/null
    +++ b/tests/replication/test_sink_lag.py
    @@ -0,0 +1,27 @@
    +import pytest
    +
    +from miniredis import CommandRequest
    +from miniredis.replication.sink import ReplicaSink
    +from tests.helpers.runtime import open_test_runtime
    +
    +
    +@pytest.mark.asyncio
    +async def test_pause_gate_exposes_exact_sequence_lag():
    +    primary = await open_test_runtime()
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=4)
    +    await primary.attach_replica(sink)
    +    sink.pause()
    +
    +    client = primary.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"a", b"1")))
    +    await client.execute(CommandRequest(b"SET", (b"b", b"2")))
    +
    +    assert sink.status.primary_seq == 2
    +    assert sink.status.applied_seq == 0
    +    assert sink.status.lag == 2
    +    sink.resume()
    +    await sink.wait_until_applied(2)
    +    assert sink.status.lag == 0
    +    await primary.close()
    +    await replica.close()
    ```

**锁定什么**

锁定 Lag 是精确序号差，而不是时间或队列长度估计。

**如何构造反例**

暂停 Apply，提交两个 Batch，观察 Lag，再恢复并等待序号 2。

**关键测试语句**

```python
assert sink.status.lag == 2
await sink.wait_until_applied(2)
```

**失败意味着什么**

报告的复制位置无法与已提交 History 对齐。

??? note "文件差异：tests/replication/test_sink_overflow.py"
    ```diff
    diff --git a/tests/replication/test_sink_overflow.py b/tests/replication/test_sink_overflow.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..7348b563b30c1d6039ade2eb529f7690db0d2d14
    --- /dev/null
    +++ b/tests/replication/test_sink_overflow.py
    @@ -0,0 +1,96 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest
    +from miniredis.core.reply import Bytes, Ok
    +from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
    +from tests.helpers.runtime import open_test_runtime
    +
    +
    +@pytest.mark.asyncio
    +async def test_full_sink_detaches_as_needs_resync_without_blocking_primary():
    +    primary = await open_test_runtime()
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=1)
    +    await primary.attach_replica(sink)
    +    sink.pause()
    +    client = primary.direct_client()
    +
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"a", b"1"))
    +    ) == Ok()
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"b", b"2"))
    +    ) == Ok()
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"c", b"3"))
    +    ) == Ok()
    +
    +    assert sink.status.state is ReplicaSinkState.NEEDS_RESYNC
    +    assert sink.status.queued == 0
    +    assert primary.debug_commit_seq == 3
    +    assert primary.debug_stats().replica_links == 0
    +    await primary.close()
    +    await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_overflow_wakes_applied_sequence_waiters_without_a_sleep():
    +    primary = await open_test_runtime()
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=1)
    +    await primary.attach_replica(sink)
    +    sink.pause()
    +    client = primary.direct_client()
    +    waiting = asyncio.create_task(sink.wait_until_applied(2))
    +
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"a", b"1"))
    +    ) == Ok()
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"b", b"2"))
    +    ) == Ok()
    +
    +    with pytest.raises(RuntimeError, match="replica stopped at seq 0"):
    +        await asyncio.wait_for(waiting, timeout=1)
    +    assert sink.status.state is ReplicaSinkState.NEEDS_RESYNC
    +    await primary.close()
    +    await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_bootstrap_overflow_never_installs_the_stale_snapshot():
    +    primary = await open_test_runtime()
    +    replica = await open_test_runtime()
    +    replica_client = replica.direct_client()
    +    assert await replica_client.execute(
    +        CommandRequest(b"SET", (b"local", b"keep"))
    +    ) == Ok()
    +    install_gate = asyncio.Event()
    +    sink = ReplicaSink(
    +        replica,
    +        queue_limit=1,
    +        install_gate=install_gate,
    +    )
    +    attaching = asyncio.create_task(primary.attach_replica(sink))
    +    await sink.attachment_captured.wait()
    +
    +    client = primary.direct_client()
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"a", b"1"))
    +    ) == Ok()
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"b", b"2"))
    +    ) == Ok()
    +    assert sink.status.state is ReplicaSinkState.NEEDS_RESYNC
    +    install_gate.set()
    +
    +    status = await attaching
    +    assert status.state is ReplicaSinkState.NEEDS_RESYNC
    +    assert await replica_client.execute(
    +        CommandRequest(b"GET", (b"local",))
    +    ) == Bytes(b"keep")
    +    assert primary.debug_stats().replica_links == 0
    +    await primary.close()
    +    await replica.close()
    ```

**锁定什么**

锁定有界 Buffer、Primary 非阻塞推进、Waiter 唤醒，以及 Overflow 后不安装陈旧 Bootstrap Image。

**如何构造反例**

暂停容量为 1 的 Sink，产生超过容量的 Commit，包括 Bootstrap 期间的 Commit。

**关键测试语句**

```python
assert sink.status.state is ReplicaSinkState.NEEDS_RESYNC
assert primary.debug_commit_seq == 3
```

**失败意味着什么**

背压越界进入 Primary，或不完整 History 被伪装成已同步。

??? note "文件差异：tests/unit/commands/test_command_traits.py"
    ```diff
    diff --git a/tests/unit/commands/test_command_traits.py b/tests/unit/commands/test_command_traits.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..26c6ae2110a3a499d517bfd4c0f0448ef96c5ac9
    --- /dev/null
    +++ b/tests/unit/commands/test_command_traits.py
    @@ -0,0 +1,28 @@
    +from typing import get_args
    +
    +from miniredis.commands import model
    +
    +
    +def test_every_frozen_command_type_has_exactly_one_dataset_trait():
    +    command_types = frozenset(get_args(model.Command))
    +    assert (
    +        model._DATASET_MUTATING_TYPES
    +        | model._NON_DATASET_MUTATING_TYPES
    +    ) == command_types
    +    assert (
    +        model._DATASET_MUTATING_TYPES
    +        & model._NON_DATASET_MUTATING_TYPES
    +    ) == frozenset()
    +
    +
    +def test_blpop_is_mutating_and_pubsub_is_explicitly_non_dataset():
    +    assert model.is_dataset_mutating(model.BlPop((b"q",), 0)) is True
    +    assert (
    +        model.is_dataset_mutating(model.Subscribe((b"c",))) is False
    +    )
    +    assert (
    +        model.is_dataset_mutating(model.Unsubscribe((b"c",))) is False
    +    )
    +    assert (
    +        model.is_dataset_mutating(model.Publish(b"c", b"p")) is False
    +    )
    ```

**锁定什么**

锁定每个 Command 都被完备且互斥地分成数据集写入或非写入。

**如何构造反例**

把两组 Trait 与完整 `Command` Union 比较，并检查 BLPOP 与 Pub/Sub 等边界案例。

**关键测试语句**

```python
assert (_DATASET_MUTATING_TYPES | _NON_DATASET_MUTATING_TYPES) == command_types
```

**失败意味着什么**

新命令可以绕过只读策略，或在没有语义决策时被错误拒绝。

### 基本概念

Replica Attachment 由 Generation 与 Snapshot Image 组成。Generation 标识一次 Source 关系，Image 提供 Baseline Sequence，后续 `CommitBatch` 推进该序号。Lag 等于 `primary_seq - applied_seq`。`NEEDS_RESYNC` 是明确的历史不连续状态，不是普通重试提示。

### 为什么需要这个机制

异步复制让 Primary 延迟独立于 Follower 速度，但这种独立必须有有界失败方式。原子 Attachment 关闭 Snapshot/Stream 缝隙；类型化 Mutation Trait 执行只读策略；明确终态避免把不完整 History 呈现为最新状态。

### 运行时心智模型

Primary Executor 在一个 Turn 中捕获 `(generation, image)` 并注册 Sink。后续 Commit 被 Offer 到有界 Queue。Replica 安装 Image，把该 Generation 标为 Active 并进入只读，再经自己的 Executor 应用排队 Batch。Overflow 或 Apply 失败会终止连续性并 Detach，而 Primary Commit 继续推进。

### 机制板块

#### 类型化只读边界

按是否修改数据集状态对每种命令做完备分类，并在 Runtime 跟随 Primary 时只拒绝真正的数据写入。

??? note "文件差异：src/miniredis/commands/model.py"
    ```diff
    diff --git a/src/miniredis/commands/model.py b/src/miniredis/commands/model.py
    index e0d48a4d6d518f5c8f357d869bedfd6a9425367d..a35ef7c39779d6045e3909f2f07df78b544b4b96 100644
    --- a/src/miniredis/commands/model.py
    +++ b/src/miniredis/commands/model.py
    @@ -245,3 +245,57 @@ Command: TypeAlias = (
         | TimeToLive
         | Persist
     )
    +
    +
    +_DATASET_MUTATING_TYPES = frozenset(
    +    {
    +        SetString,
    +        Delete,
    +        Increment,
    +        HashSet,
    +        HashDelete,
    +        HashIncrement,
    +        ListPush,
    +        ListPop,
    +        BlPop,
    +        SetAdd,
    +        SetRemove,
    +        ZAdd,
    +        ZRemove,
    +        Expire,
    +        Persist,
    +    }
    +)
    +
    +_NON_DATASET_MUTATING_TYPES = frozenset(
    +    {
    +        Ping,
    +        Echo,
    +        GetString,
    +        Exists,
    +        TypeOf,
    +        HashGet,
    +        HashGetAll,
    +        ListRange,
    +        SetIsMember,
    +        SetMembers,
    +        SetIntersection,
    +        ZScore,
    +        ZRank,
    +        ZRange,
    +        ZRangeByScore,
    +        TimeToLive,
    +        Subscribe,
    +        Unsubscribe,
    +        Publish,
    +    }
    +)
    +
    +
    +def is_dataset_mutating(command: Command) -> bool:
    +    command_type = type(command)
    +    if command_type in _DATASET_MUTATING_TYPES:
    +        return True
    +    if command_type in _NON_DATASET_MUTATING_TYPES:
    +        return False
    +    raise AssertionError(f"unclassified command type: {command_type.__name__}")
    ```

**是什么，为什么出现**

命令模型增加完备的数据集写入 Trait。

**运行时角色**

Replica Executor 查询命令语义，而不是复制命令名黑名单。

**关键代码**

```python
if command_type in _DATASET_MUTATING_TYPES:
    return True
```

**关键语句理解**

未知命令类型会响亮失败，迫使未来每种命令显式决定只读策略。

#### 原子接入交接

在同一 Executor 顺序中捕获 Snapshot 序号与状态、注册 Sink，并把此后的每个 Commit 排入队列。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index 0126d8027bf47cfb54cda842f29eaef2de67d0d0..17421ea1ed3c88f056bebb8ac1573db2211f8b10 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -16,6 +16,7 @@ from miniredis.commands.model import (
         Publish,
         Subscribe,
         Unsubscribe,
    +    is_dataset_mutating,
     )
     from miniredis.core.blocking import (
         WaiterId,
    @@ -58,6 +59,7 @@ from miniredis.persistence.aof import (
         AofAppendOk,
         AofAppendOutcome,
     )
    +from miniredis.replication.sink import ReplicaAttachment

     if TYPE_CHECKING:
         from miniredis.replication.sink import ReplicaSink
    @@ -105,6 +107,33 @@ class SnapshotBarrier:
         future: asyncio.Future[SnapshotImage]


    +@dataclass(slots=True)
    +class AttachReplica:
    +    sink: ReplicaSink
    +    future: asyncio.Future[ReplicaAttachment]
    +
    +
    +@dataclass(slots=True)
    +class DetachReplica:
    +    generation: int
    +    future: asyncio.Future[bool] | None
    +
    +
    +@dataclass(slots=True)
    +class InstallReplicaSnapshot:
    +    sink: ReplicaSink
    +    generation: int
    +    image: SnapshotImage
    +    future: asyncio.Future[bool]
    +
    +
    +@dataclass(slots=True)
    +class ApplyReplicaBatch:
    +    generation: int
    +    batch: CommitBatch
    +    future: asyncio.Future[bool]
    +
    +
     @dataclass(frozen=True, slots=True)
     class ExecutionPlan:
         reply: Reply | None
    @@ -170,6 +199,7 @@ class CommandExecutor:
             on_debug_change: Callable[[], None],
             on_terminal_failure: Callable[[BaseException], None] | None = None,
             on_fatal: Callable[[str], None] | None = None,
    +        replica_apply_failure: BaseException | None = None,
         ) -> None:
             self.database = database
             self.planner = planner
    @@ -203,6 +233,10 @@ class CommandExecutor:
             self._accepted_changed = asyncio.Event()
             self._applied_batches: list[CommitBatch] = []
             self._replica_sinks: dict[int, ReplicaSink] = {}
    +        self._next_replica_generation = 1
    +        self._active_source_generation: int | None = None
    +        self._replica_read_only = False
    +        self._replica_apply_failure = replica_apply_failure
             self._handling_message = False
             self._failure: BaseException | None = None
             self._terminal_cleanup_complete = False
    @@ -350,6 +384,43 @@ class CommandExecutor:
                 image = self.database.snapshot_image(self.clock.now_ms())
                 if not message.future.done():
                     message.future.set_result(image)
    +        elif isinstance(message, AttachReplica):
    +            generation = self._next_replica_generation
    +            self._next_replica_generation += 1
    +            image = self.database.snapshot_image(self.clock.now_ms())
    +            attachment = ReplicaAttachment(generation, image)
    +            message.sink.register_attachment(attachment)
    +            self._replica_sinks[generation] = message.sink
    +            message.future.set_result(attachment)
    +        elif isinstance(message, DetachReplica):
    +            removed = self._replica_sinks.pop(message.generation, None) is not None
    +            if message.future is not None and not message.future.done():
    +                message.future.set_result(removed)
    +        elif isinstance(message, InstallReplicaSnapshot):
    +            if not message.sink.install_allowed(message.generation):
    +                message.future.set_result(False)
    +                return
    +            self.database.install_snapshot(
    +                message.image,
    +                now_ms=self.clock.now_ms(),
    +            )
    +            self._active_source_generation = message.generation
    +            self._replica_read_only = True
    +            message.future.set_result(True)
    +        elif isinstance(message, ApplyReplicaBatch):
    +            if message.generation != self._active_source_generation:
    +                message.future.set_result(False)
    +                return
    +            try:
    +                if self._replica_apply_failure is not None:
    +                    error = self._replica_apply_failure
    +                    self._replica_apply_failure = None
    +                    raise error
    +                self.database.apply_batch(message.batch, track_access=False)
    +            except BaseException as exc:
    +                message.future.set_exception(exc)
    +            else:
    +                message.future.set_result(True)
             else:
                 raise AssertionError(f"unknown executor message: {message!r}")

    @@ -442,6 +513,12 @@ class CommandExecutor:

         async def _execute(self, request: ExecuteRequest) -> None:
             command = request.command
    +        if self._replica_read_only and is_dataset_mutating(command):
    +            self._finish_reply(
    +                request.token,
    +                Failure("READONLY", "replica is read only"),
    +            )
    +            return
             if self.pubsub.count(request.session_id) > 0 and not isinstance(
                 command, (Ping, Subscribe, Unsubscribe)
             ):
    @@ -641,6 +718,45 @@ class CommandExecutor:
                 raise RuntimeError("executor control admission is closed")
             return await asyncio.shield(future)

    +    async def attach_replica(self, sink: ReplicaSink) -> ReplicaAttachment:
    +        future: asyncio.Future[ReplicaAttachment] = (
    +            asyncio.get_running_loop().create_future()
    +        )
    +        if not self.post_control(AttachReplica(sink, future)):
    +            raise RuntimeError("executor control admission is closed")
    +        return await asyncio.shield(future)
    +
    +    async def detach_replica(self, generation: int) -> bool:
    +        future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
    +        if not self.post_control(DetachReplica(generation, future)):
    +            return False
    +        return await asyncio.shield(future)
    +
    +    async def install_replica_snapshot(
    +        self,
    +        sink: ReplicaSink,
    +        generation: int,
    +        image: SnapshotImage,
    +    ) -> bool:
    +        future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
    +        if not self.post_control(
    +            InstallReplicaSnapshot(sink, generation, image, future)
    +        ):
    +            return False
    +        return await asyncio.shield(future)
    +
    +    async def apply_replica_batch(
    +        self,
    +        generation: int,
    +        batch: CommitBatch,
    +    ) -> bool:
    +        future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
    +        if not self.post_control(
    +            ApplyReplicaBatch(generation, batch, future)
    +        ):
    +            return False
    +        return await asyncio.shield(future)
    +
         async def _active_expire_once(self, now_ms: int) -> int:
             keys = sorted(
                 key
    @@ -754,6 +870,10 @@ class CommandExecutor:
         def endpoint_count(self) -> int:
             return len(self._endpoints)

    +    @property
    +    def replica_link_count(self) -> int:
    +        return len(self._replica_sinks)
    +
         @property
         def worker_task(self) -> asyncio.Task[None] | None:
             return self._worker_task
    ```

**是什么，为什么出现**

Single Writer 增加有序 Attach、Detach、Install 与 Apply Control Message。

**运行时角色**

在另一个 Commit 插入前捕获 Image 并注册 Sink，并在 Replica 端校验 Source Generation。

**关键代码**

```python
image = self.database.snapshot_image(self.clock.now_ms())
attachment = ReplicaAttachment(generation, image)
message.sink.register_attachment(attachment)
self._replica_sinks[generation] = message.sink
message.future.set_result(attachment)
```

**关键语句理解**

Executor Turn 就是交接边界：N 及以前在 Image 中，N 以后交给已注册 Sink。

#### 有界异步 Replica Sink

安装 Baseline，异步应用连续 Batch，暴露精确 Lag，并在 Follower 过慢或失败时将其分离而不阻塞 Primary。

??? note "文件差异：src/miniredis/replication/sink.py"
    ```diff
    diff --git a/src/miniredis/replication/sink.py b/src/miniredis/replication/sink.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..48a1ac101fd4d303da97d89969fc09c755c5ed09
    --- /dev/null
    +++ b/src/miniredis/replication/sink.py
    @@ -0,0 +1,236 @@
    +from __future__ import annotations
    +
    +import asyncio
    +from collections import deque
    +from dataclasses import dataclass
    +from enum import StrEnum
    +from typing import TYPE_CHECKING
    +
    +from miniredis.core.commit import CommitBatch, SnapshotImage
    +
    +if TYPE_CHECKING:
    +    from miniredis.runtime import MiniRedis
    +
    +
    +class ReplicaSinkState(StrEnum):
    +    DETACHED = "detached"
    +    BOOTSTRAPPING = "bootstrapping"
    +    STREAMING = "streaming"
    +    NEEDS_RESYNC = "needs_resync"
    +    FAILED = "failed"
    +    PROMOTING = "promoting"
    +    PROMOTED = "promoted"
    +    SOURCE_LOST = "source_lost"
    +    STOPPED = "stopped"
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ReplicaAttachment:
    +    generation: int
    +    image: SnapshotImage
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ReplicaStatus:
    +    generation: int | None
    +    state: ReplicaSinkState
    +    baseline_seq: int
    +    applied_seq: int
    +    primary_seq: int
    +    lag: int
    +    queued: int
    +
    +
    +class ReplicaSink:
    +    def __init__(
    +        self,
    +        replica: MiniRedis,
    +        *,
    +        queue_limit: int,
    +        install_gate: asyncio.Event | None = None,
    +    ) -> None:
    +        if queue_limit <= 0:
    +            raise ValueError("replica queue limit must be positive")
    +        self._replica = replica
    +        self._queue_limit = queue_limit
    +        self._install_gate = install_gate
    +        self._primary: MiniRedis | None = None
    +        self._generation: int | None = None
    +        self._baseline_seq = 0
    +        self._applied_seq = 0
    +        self._primary_seq = 0
    +        self._state = ReplicaSinkState.DETACHED
    +        self._queue: deque[CommitBatch] = deque()
    +        self._queue_ready = asyncio.Event()
    +        self._apply_allowed = asyncio.Event()
    +        self._apply_allowed.set()
    +        self._attachment_captured = asyncio.Event()
    +        self._status_changed = asyncio.Event()
    +        self._attach_task: asyncio.Task[ReplicaStatus] | None = None
    +        self._task: asyncio.Task[None] | None = None
    +
    +    def _signal_status_change(self) -> None:
    +        self._status_changed.set()
    +
    +    @property
    +    def attachment_captured(self) -> asyncio.Event:
    +        return self._attachment_captured
    +
    +    @property
    +    def status(self) -> ReplicaStatus:
    +        return ReplicaStatus(
    +            generation=self._generation,
    +            state=self._state,
    +            baseline_seq=self._baseline_seq,
    +            applied_seq=self._applied_seq,
    +            primary_seq=self._primary_seq,
    +            lag=max(0, self._primary_seq - self._applied_seq),
    +            queued=len(self._queue),
    +        )
    +
    +    def pause(self) -> None:
    +        self._apply_allowed.clear()
    +
    +    def resume(self) -> None:
    +        self._apply_allowed.set()
    +
    +    def register_attachment(
    +        self,
    +        attachment: ReplicaAttachment,
    +    ) -> None:
    +        if self._state is not ReplicaSinkState.BOOTSTRAPPING:
    +            raise RuntimeError("sink is not bootstrapping")
    +        self._generation = attachment.generation
    +        self._baseline_seq = attachment.image.checkpoint_seq
    +        self._applied_seq = attachment.image.checkpoint_seq
    +        self._primary_seq = attachment.image.checkpoint_seq
    +        self._attachment_captured.set()
    +        self._signal_status_change()
    +
    +    async def attach(self, primary: MiniRedis) -> ReplicaStatus:
    +        if self._state is not ReplicaSinkState.DETACHED:
    +            raise RuntimeError("replica sink is already attached")
    +        current = asyncio.current_task()
    +        assert current is not None
    +        self._attach_task = current
    +        self._primary = primary
    +        self._state = ReplicaSinkState.BOOTSTRAPPING
    +        self._signal_status_change()
    +        try:
    +            attachment = await primary.executor.attach_replica(self)
    +            if self._install_gate is not None:
    +                await self._install_gate.wait()
    +            if self._state is not ReplicaSinkState.BOOTSTRAPPING:
    +                return self.status
    +            installed = (
    +                await self._replica.executor.install_replica_snapshot(
    +                    self,
    +                    attachment.generation,
    +                    attachment.image,
    +                )
    +            )
    +            if not installed:
    +                return self.status
    +            if self._state is not ReplicaSinkState.BOOTSTRAPPING:
    +                return self.status
    +            self._state = ReplicaSinkState.STREAMING
    +            self._signal_status_change()
    +            self._task = asyncio.create_task(
    +                self._run_apply(),
    +                name=f"miniredis-replica-{attachment.generation}",
    +            )
    +            return self.status
    +        finally:
    +            self._attach_task = None
    +
    +    def install_allowed(self, generation: int) -> bool:
    +        return (
    +            self._state is ReplicaSinkState.BOOTSTRAPPING
    +            and self._generation == generation
    +        )
    +
    +    def offer(self, batch: CommitBatch) -> bool:
    +        if self._state not in {
    +            ReplicaSinkState.BOOTSTRAPPING,
    +            ReplicaSinkState.STREAMING,
    +        }:
    +            return False
    +        self._primary_seq = batch.seq
    +        if len(self._queue) >= self._queue_limit:
    +            self._queue.clear()
    +            self._state = ReplicaSinkState.NEEDS_RESYNC
    +            self._queue_ready.set()
    +            self._signal_status_change()
    +            return False
    +        self._queue.append(batch)
    +        self._queue_ready.set()
    +        self._signal_status_change()
    +        return True
    +
    +    async def _run_apply(self) -> None:
    +        try:
    +            while self._state is ReplicaSinkState.STREAMING:
    +                await self._queue_ready.wait()
    +                await self._apply_allowed.wait()
    +                if not self._queue:
    +                    self._queue_ready.clear()
    +                    continue
    +                batch = self._queue.popleft()
    +                if not self._queue:
    +                    self._queue_ready.clear()
    +                assert self._generation is not None
    +                applied = await self._replica.executor.apply_replica_batch(
    +                    self._generation,
    +                    batch,
    +                )
    +                if not applied:
    +                    self._queue.clear()
    +                    self._state = ReplicaSinkState.NEEDS_RESYNC
    +                    self._signal_status_change()
    +                    if self._primary is not None:
    +                        await self._primary.executor.detach_replica(
    +                            self._generation
    +                        )
    +                    return
    +                self._applied_seq = batch.seq
    +                self._signal_status_change()
    +        except asyncio.CancelledError:
    +            raise
    +        except BaseException:
    +            self._state = ReplicaSinkState.FAILED
    +            self._queue.clear()
    +            self._signal_status_change()
    +            if self._primary is not None and self._generation is not None:
    +                await self._primary.executor.detach_replica(
    +                    self._generation
    +                )
    +        finally:
    +            self._signal_status_change()
    +
    +    async def wait_until_applied(self, seq: int) -> None:
    +        terminal = {
    +            ReplicaSinkState.NEEDS_RESYNC,
    +            ReplicaSinkState.FAILED,
    +            ReplicaSinkState.SOURCE_LOST,
    +            ReplicaSinkState.STOPPED,
    +        }
    +        while True:
    +            if self._applied_seq >= seq:
    +                return
    +            if self._state in terminal:
    +                raise RuntimeError(
    +                    f"replica stopped at seq {self._applied_seq}"
    +                )
    +            self._status_changed.clear()
    +            if self._applied_seq >= seq:
    +                return
    +            if self._state in terminal:
    +                raise RuntimeError(
    +                    f"replica stopped at seq {self._applied_seq}"
    +                )
    +            await self._status_changed.wait()
    +
    +    async def wait_until_stopped(self) -> None:
    +        task = self._task
    +        if task is not None:
    +            await asyncio.shield(task)
    ```

**是什么，为什么出现**

Sink 持有 Attachment 状态、有界排队 History、Apply 进度与 Link 终态。

**运行时角色**

安装 Baseline，异步排空 Batch，报告 Lag，唤醒 Waiter，并隔离 Overflow 或失败。

**关键代码**

```python
if len(self._queue) >= self._queue_limit:
    self._queue.clear()
    self._state = ReplicaSinkState.NEEDS_RESYNC
```

**关键语句理解**

一旦缺少一个 Batch，保留更晚 Batch 也无法恢复连续 History；诚实状态只能是需要重新同步。

#### 复制所有权与边界

校验 Queue 与 Drain 边界，由 Runtime 持有接入任务与 Sink，并报告存活 Link 数量。

??? note "文件差异：src/miniredis/config.py"
    ```diff
    diff --git a/src/miniredis/config.py b/src/miniredis/config.py
    index aac0e9f7ec7ad70e7234688fb8e877e46b571a48..999209a5a6ad80cb8c5f48ee2e984be0b8ac061e 100644
    --- a/src/miniredis/config.py
    +++ b/src/miniredis/config.py
    @@ -23,6 +23,8 @@ class MiniRedisConfig:
         aof_repair_truncated_tail: bool = True
         aof_fsync_interval_seconds: float = 1.0
         snapshot_path: Path | None = None
    +    replica_queue_limit: int = 64
    +    replica_drain_grace_ms: int = 1000

         def __post_init__(self) -> None:
             if self.max_pending_commands <= 0:
    @@ -41,3 +43,7 @@ class MiniRedisConfig:
                 raise ValueError("active_expire_interval_ms must be positive")
             if self.aof_fsync_interval_seconds <= 0:
                 raise ValueError("aof_fsync_interval_seconds must be positive")
    +        if self.replica_queue_limit <= 0:
    +            raise ValueError("replica_queue_limit must be positive")
    +        if self.replica_drain_grace_ms < 0:
    +            raise ValueError("replica_drain_grace_ms cannot be negative")
    ```

**是什么，为什么出现**

配置显式给出 Queue 容量与关闭时的 Drain Grace。

**运行时角色**

在 Link 启动前拒绝不可能的边界值。

**关键代码**

```python
if self.replica_queue_limit <= 0:
    raise ValueError("replica_queue_limit must be positive")
```

**关键语句理解**

有界内存与有界关闭等待属于复制契约，不只是性能调参。

??? note "文件差异：src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 78b33aea879a2825b1c9b8c93a7c711161386226..8e6d41b18a22e563c92475eb3a5ebe1f4a059ba7 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -46,6 +46,7 @@ from miniredis.persistence.snapshot import (
         SnapshotManager,
         SnapshotOutcome,
     )
    +from miniredis.replication.sink import ReplicaSink, ReplicaStatus


     class RuntimeState(str, Enum):
    @@ -65,12 +66,14 @@ class RuntimeStats:
         sessions: int
         timer_handles: int
         owned_tasks: int
    +    replica_links: int


     @dataclass(slots=True)
     class _RuntimeTestHooks:
         aof_appender: CommitBarrier | None = None
         snapshot_ops: SnapshotFileOps | None = None
    +    replica_apply_failure: BaseException | None = None


     def _direct_transport_close(_reason: str) -> None:
    @@ -114,6 +117,11 @@ class MiniRedis:
                 on_debug_change=self._debug_notify,
                 on_terminal_failure=self._on_executor_terminal_failure,
                 on_fatal=self._transition_failed,
    +            replica_apply_failure=(
    +                None
    +                if self._test_hooks is None
    +                else self._test_hooks.replica_apply_failure
    +            ),
             )
             self._snapshot_manager = (
                 SnapshotManager(
    @@ -137,6 +145,7 @@ class MiniRedis:
             self._owned_tasks: set[asyncio.Task[object]] = set()
             self._failure_reason: str | None = None
             self._shutdown_complete = False
    +        self._owned_replica_sinks: set[ReplicaSink] = set()

         @classmethod
         def open(
    @@ -378,6 +387,34 @@ class MiniRedis:
             self._owned_tasks.add(task)
             task.add_done_callback(self._owned_tasks.discard)

    +    async def attach_replica(self, sink: ReplicaSink) -> ReplicaStatus:
    +        if self.state is not RuntimeState.RUNNING:
    +            raise RuntimeError("primary is not running")
    +        self._owned_replica_sinks.add(sink)
    +        task = asyncio.create_task(
    +            sink.attach(self),
    +            name="miniredis:replica-attach",
    +        )
    +        task.add_done_callback(
    +            lambda completed: self._replica_attach_done(sink, completed)
    +        )
    +        try:
    +            return await asyncio.shield(task)
    +        except BaseException:
    +            if task.done() and (
    +                task.cancelled() or task.exception() is not None
    +            ):
    +                self._owned_replica_sinks.discard(sink)
    +            raise
    +
    +    def _replica_attach_done(
    +        self,
    +        sink: ReplicaSink,
    +        task: asyncio.Task[ReplicaStatus],
    +    ) -> None:
    +        if task.cancelled() or task.exception() is not None:
    +            self._owned_replica_sinks.discard(sink)
    +
         async def __aenter__(self) -> Self:
             await self.start()
             return self
    @@ -455,6 +492,7 @@ class MiniRedis:
                     for task in self._owned_tasks
                     if task is not asyncio.current_task()
                 ),
    +            replica_links=self.executor.replica_link_count,
             )

         def _debug_notify(self) -> None:
    ```

**是什么，为什么出现**

Runtime 持有 Replica Attachment Task 与存活 Sink。

**运行时角色**

只在 Running 时接纳 Attachment，以 Shield 保护共享工作，并暴露 Link 数用于生命周期检查。

**关键代码**

```python
self._owned_replica_sinks.add(sink)
task = asyncio.create_task(
    sink.attach(self),
    name="miniredis:replica-attach",
)
```

**关键语句理解**

Attachment 一旦开始，Link 的所有者就是 Runtime，而不是发起调用的 Caller。

### 验证证据

运行 `tests.txt` 中五个聚焦测试模块，再累计构建 Stage 1–17，并与提交 `e18be82` 比较源码。

### 需要真正记住的内容

- Snapshot 与 Stream 注册需要一个有序边界。
- 异步复制需要有界的 History 断裂语义。
- Lag 是序号关系，不是经过时间。
- 只读策略属于命令语义。

### 用自己的话讲清楚

为什么 Queue Overflow 后必须进入 `NEEDS_RESYNC`，而不是保留最新 Batch？为什么这不会让 Primary 写入失败？

### 教材

这是一个紧凑的 Primary–backup Replication：State Transfer 建立 Checkpoint，有序 Log 携带后续 Transition，有界异步 Channel 则以已确认主库写入的故障耐久性，换取 Primary 可用性与延迟隔离。

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/b267f92...e18be82)

完成后可运行 `python -m journey.tools.build_journey check 17` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/17-asynchronous-replication/stage.patch)

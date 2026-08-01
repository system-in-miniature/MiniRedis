# Stage 17 · Asynchronous replication / 异步复制

<!-- journey: chapter=8 tests_added=5 -->

## English

### Goal

Attach a replica without losing the commits concurrent with snapshot installation, while keeping replica delay and failure outside the primary write path.

### Deliverable files

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

### The problem at this point

A snapshot alone is already stale when installation finishes. The primary must establish one ordered boundary: state through sequence N is in the image, and every commit after N belongs to the stream. That stream cannot be an unbounded queue or a synchronous dependency of acknowledged writes.

### Failure preview

A write committed while installation is paused can disappear between snapshot and stream. A paused follower can consume unlimited memory or stall the primary. A replica apply exception can kill healthy primary traffic. Finally, a read-only check based on a short command-name list can accidentally allow a new mutating command.

### Test contract

<!-- journey-file: tests/replication/test_sink_attach.py -->
#### `tests/replication/test_sink_attach.py`

##### What this test locks

It locks the snapshot-to-stream handoff and the replica read-only boundary.

##### How it constructs the counterexample

It pauses snapshot installation after attachment capture, commits another write, then verifies both baseline and queued state on the replica.

##### Key test statement

```python
assert sink.status.baseline_seq == 1
assert sink.status.queued == 1
```

##### What a failure means

Capture and stream registration were not one ordered primary action, or a following runtime still accepts dataset writes.

<!-- journey-file: tests/replication/test_sink_failure_isolation.py -->
#### `tests/replication/test_sink_failure_isolation.py`

##### What this test locks

It locks failure isolation between one replica link and the primary executor.

##### How it constructs the counterexample

It injects an exception into replica application after the primary accepts a commit.

##### Key test statement

```python
assert sink.status.state is ReplicaSinkState.FAILED
assert primary.state.name == "RUNNING"
```

##### What a failure means

Replica work still owns or contaminates the primary request outcome.

<!-- journey-file: tests/replication/test_sink_lag.py -->
#### `tests/replication/test_sink_lag.py`

##### What this test locks

It locks lag as an exact sequence difference rather than a timer or queue-size estimate.

##### How it constructs the counterexample

It pauses apply, commits two batches, inspects lag, then resumes and waits for sequence two.

##### Key test statement

```python
assert sink.status.lag == 2
await sink.wait_until_applied(2)
```

##### What a failure means

The reported replication position cannot be correlated with committed history.

<!-- journey-file: tests/replication/test_sink_overflow.py -->
#### `tests/replication/test_sink_overflow.py`

##### What this test locks

It locks bounded buffering, non-blocking primary progress, waiter wake-up, and rejection of a stale bootstrap image after overflow.

##### How it constructs the counterexample

It pauses a one-item sink and produces more commits than it can retain, including during bootstrap.

##### Key test statement

```python
assert sink.status.state is ReplicaSinkState.NEEDS_RESYNC
assert primary.debug_commit_seq == 3
```

##### What a failure means

Backpressure crossed into the primary or incomplete history was allowed to look synchronized.

<!-- journey-file: tests/unit/commands/test_command_traits.py -->
#### `tests/unit/commands/test_command_traits.py`

##### What this test locks

It locks an exhaustive, disjoint classification of every command as dataset-mutating or non-mutating.

##### How it constructs the counterexample

It compares both trait sets with the complete `Command` union and checks subtle cases such as blocking pop and Pub/Sub.

##### Key test statement

```python
assert (_DATASET_MUTATING_TYPES | _NON_DATASET_MUTATING_TYPES) == command_types
```

##### What a failure means

A newly added command can bypass read-only policy or be rejected without an explicit semantic decision.

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

##### What this test locks

The helper exposes production-shaped gates and one-shot apply failure injection without replacing the replication machinery.

##### How it constructs the counterexample

It passes the failure through runtime-owned test hooks into the real executor.

##### Key test statement

```python
replica_apply_failure=replica_apply_failure,
```

##### What a failure means

The tests would prove a fake link rather than the runtime path learners inspect.

### Basic concepts

A replica attachment is a generation plus a snapshot image. The generation identifies one source relationship; the image supplies a baseline sequence; later `CommitBatch` values advance that sequence. Lag is `primary_seq - applied_seq`. `NEEDS_RESYNC` is an explicit loss-of-continuity state, not a retry hint.

### Why this mechanism is necessary

Asynchronous replication keeps primary latency independent of follower speed, but that independence needs a bounded failure mode. Atomic attachment closes the snapshot/stream gap; typed mutation traits enforce read-only behavior; explicit terminal states stop incomplete history from being presented as current.

### Runtime mental model

The primary executor captures `(generation, image)` and registers the sink in one turn. Later commits are offered to its bounded queue. The replica installs the image, marks that generation active and read-only, then applies queued batches through its own executor. Overflow or apply failure clears continuity and detaches the link while primary commits continue.

### Mechanism blocks

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

##### What it is and why it appears

The command model gains one exhaustive dataset-mutation trait.

##### Runtime role

The replica executor consults semantic command type rather than duplicating a name blacklist.

##### Key code

```python
if command_type in _DATASET_MUTATING_TYPES:
    return True
```

##### Statement understanding

Unknown command types fail loudly, forcing every future command to make an explicit read-only-policy choice.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The single writer gains ordered attach, detach, install, and apply control messages.

##### Runtime role

It captures the image and registers the sink before another commit can interleave, then validates source generation on the replica.

##### Key code

```python
image = self.database.snapshot_image(self.clock.now_ms())
message.sink.register_attachment(ReplicaAttachment(generation, image))
self._replica_sinks[generation] = message.sink
```

##### Statement understanding

The executor turn is the handoff boundary: through N is in the image; after N is offered to the registered sink.

<!-- journey-file: src/miniredis/replication/sink.py -->
#### `src/miniredis/replication/sink.py`

##### What it is and why it appears

The sink owns attachment state, bounded queued history, apply progress, and link termination.

##### Runtime role

It installs the baseline, drains batches asynchronously, reports lag, wakes waiters, and isolates overflow or failure.

##### Key code

```python
if len(self._queue) >= self._queue_limit:
    self._queue.clear()
    self._state = ReplicaSinkState.NEEDS_RESYNC
```

##### Statement understanding

Once one batch is missing, retaining later batches cannot restore a contiguous history; the honest state is resynchronization required.

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### What it is and why it appears

Configuration makes queue capacity and shutdown drain grace explicit.

##### Runtime role

It rejects impossible limits before a link starts.

##### Key code

```python
if self.replica_queue_limit <= 0:
    raise ValueError("replica_queue_limit must be positive")
```

##### Statement understanding

Bounded memory and bounded shutdown waiting are part of the replication contract, not incidental tuning.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

The runtime owns replica attachment tasks and live sinks.

##### Runtime role

It admits attachment only while running, shields shared work from caller cancellation, and exposes link counts for lifecycle checks.

##### Key code

```python
self._owned_replica_sinks.add(sink)
return await asyncio.shield(task)
```

##### Statement understanding

The runtime, not the initiating caller, owns the link once attachment begins.

### Verification evidence

Run the five focused test modules from `tests.txt`, then build Stages 1–17 cumulatively and compare the result with commit `e18be82`.

### Durable takeaways

- Snapshot and stream registration need one ordered boundary.
- Async replication needs bounded loss-of-continuity semantics.
- Lag is a sequence relationship, not elapsed time.
- Read-only policy belongs to command semantics.

### Explain it in your own words

Why does queue overflow move the sink to `NEEDS_RESYNC` instead of keeping the newest batches, and why does this not fail the primary write?

### Textbook

This stage is a compact primary–backup replication design: state transfer establishes a checkpoint, an ordered log carries subsequent transitions, and a bounded asynchronous channel trades acknowledged-primary durability for primary availability and latency isolation.

## 中文

### 目标

在 Snapshot 安装期间不遗漏并发 Commit 地接入 Replica，同时让副本延迟和失败留在 Primary 写入路径之外。

### 交付文件

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

### 先看会坏在哪里

安装暂停期间提交的写入可能掉进 Snapshot 与 Stream 之间；暂停的 Follower 可能无限占用内存或阻塞 Primary；Replica Apply 异常可能杀死健康的主库流量；靠命令名短名单实现只读还会漏过新加入的写命令。

### 测试契约

<!-- journey-file: tests/replication/test_sink_attach.py -->
#### `tests/replication/test_sink_attach.py`

##### 锁定什么

锁定 Snapshot 到 Stream 的交接以及 Replica 只读边界。

##### 如何构造反例

在捕获 Attachment 后暂停安装，提交另一次写入，再验证 Replica 同时得到 Baseline 与排队状态。

##### 关键测试语句

```python
assert sink.status.baseline_seq == 1
assert sink.status.queued == 1
```

##### 失败意味着什么

Capture 与 Stream 注册不是同一个 Primary 有序动作，或 Follower Runtime 仍接受数据集写入。

<!-- journey-file: tests/replication/test_sink_failure_isolation.py -->
#### `tests/replication/test_sink_failure_isolation.py`

##### 锁定什么

锁定单条 Replica Link 与 Primary Executor 之间的失败隔离。

##### 如何构造反例

Primary 接受 Commit 后，向 Replica Apply 注入异常。

##### 关键测试语句

```python
assert sink.status.state is ReplicaSinkState.FAILED
assert primary.state.name == "RUNNING"
```

##### 失败意味着什么

Replica 工作仍然拥有或污染 Primary 请求结果。

<!-- journey-file: tests/replication/test_sink_lag.py -->
#### `tests/replication/test_sink_lag.py`

##### 锁定什么

锁定 Lag 是精确序号差，而不是时间或队列长度估计。

##### 如何构造反例

暂停 Apply，提交两个 Batch，观察 Lag，再恢复并等待序号 2。

##### 关键测试语句

```python
assert sink.status.lag == 2
await sink.wait_until_applied(2)
```

##### 失败意味着什么

报告的复制位置无法与已提交 History 对齐。

<!-- journey-file: tests/replication/test_sink_overflow.py -->
#### `tests/replication/test_sink_overflow.py`

##### 锁定什么

锁定有界 Buffer、Primary 非阻塞推进、Waiter 唤醒，以及 Overflow 后不安装陈旧 Bootstrap Image。

##### 如何构造反例

暂停容量为 1 的 Sink，产生超过容量的 Commit，包括 Bootstrap 期间的 Commit。

##### 关键测试语句

```python
assert sink.status.state is ReplicaSinkState.NEEDS_RESYNC
assert primary.debug_commit_seq == 3
```

##### 失败意味着什么

背压越界进入 Primary，或不完整 History 被伪装成已同步。

<!-- journey-file: tests/unit/commands/test_command_traits.py -->
#### `tests/unit/commands/test_command_traits.py`

##### 锁定什么

锁定每个 Command 都被完备且互斥地分成数据集写入或非写入。

##### 如何构造反例

把两组 Trait 与完整 `Command` Union 比较，并检查 BLPOP 与 Pub/Sub 等边界案例。

##### 关键测试语句

```python
assert (_DATASET_MUTATING_TYPES | _NON_DATASET_MUTATING_TYPES) == command_types
```

##### 失败意味着什么

新命令可以绕过只读策略，或在没有语义决策时被错误拒绝。

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

##### 锁定什么

Helper 只提供 Gate 与一次性失败注入，仍然经过真实复制机制。

##### 如何构造反例

通过 Runtime 持有的 Test Hook 把失败传入真实 Executor。

##### 关键测试语句

```python
replica_apply_failure=replica_apply_failure,
```

##### 失败意味着什么

测试证明的是 Fake Link，而不是学习者走读的 Runtime 路径。

### 基本概念

Replica Attachment 由 Generation 与 Snapshot Image 组成。Generation 标识一次 Source 关系，Image 提供 Baseline Sequence，后续 `CommitBatch` 推进该序号。Lag 等于 `primary_seq - applied_seq`。`NEEDS_RESYNC` 是明确的历史不连续状态，不是普通重试提示。

### 为什么需要这个机制

异步复制让 Primary 延迟独立于 Follower 速度，但这种独立必须有有界失败方式。原子 Attachment 关闭 Snapshot/Stream 缝隙；类型化 Mutation Trait 执行只读策略；明确终态避免把不完整 History 呈现为最新状态。

### 运行时心智模型

Primary Executor 在一个 Turn 中捕获 `(generation, image)` 并注册 Sink。后续 Commit 被 Offer 到有界 Queue。Replica 安装 Image，把该 Generation 标为 Active 并进入只读，再经自己的 Executor 应用排队 Batch。Overflow 或 Apply 失败会终止连续性并 Detach，而 Primary Commit 继续推进。

### 机制板块

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

##### 是什么，为什么出现

命令模型增加完备的数据集写入 Trait。

##### 运行时角色

Replica Executor 查询命令语义，而不是复制命令名黑名单。

##### 关键代码

```python
if command_type in _DATASET_MUTATING_TYPES:
    return True
```

##### 关键语句理解

未知命令类型会响亮失败，迫使未来每种命令显式决定只读策略。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么出现

Single Writer 增加有序 Attach、Detach、Install 与 Apply Control Message。

##### 运行时角色

在另一个 Commit 插入前捕获 Image 并注册 Sink，并在 Replica 端校验 Source Generation。

##### 关键代码

```python
image = self.database.snapshot_image(self.clock.now_ms())
message.sink.register_attachment(ReplicaAttachment(generation, image))
self._replica_sinks[generation] = message.sink
```

##### 关键语句理解

Executor Turn 就是交接边界：N 及以前在 Image 中，N 以后交给已注册 Sink。

<!-- journey-file: src/miniredis/replication/sink.py -->
#### `src/miniredis/replication/sink.py`

##### 是什么，为什么出现

Sink 持有 Attachment 状态、有界排队 History、Apply 进度与 Link 终态。

##### 运行时角色

安装 Baseline，异步排空 Batch，报告 Lag，唤醒 Waiter，并隔离 Overflow 或失败。

##### 关键代码

```python
if len(self._queue) >= self._queue_limit:
    self._queue.clear()
    self._state = ReplicaSinkState.NEEDS_RESYNC
```

##### 关键语句理解

一旦缺少一个 Batch，保留更晚 Batch 也无法恢复连续 History；诚实状态只能是需要重新同步。

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### 是什么，为什么出现

配置显式给出 Queue 容量与关闭时的 Drain Grace。

##### 运行时角色

在 Link 启动前拒绝不可能的边界值。

##### 关键代码

```python
if self.replica_queue_limit <= 0:
    raise ValueError("replica_queue_limit must be positive")
```

##### 关键语句理解

有界内存与有界关闭等待属于复制契约，不只是性能调参。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么出现

Runtime 持有 Replica Attachment Task 与存活 Sink。

##### 运行时角色

只在 Running 时接纳 Attachment，以 Shield 保护共享工作，并暴露 Link 数用于生命周期检查。

##### 关键代码

```python
self._owned_replica_sinks.add(sink)
return await asyncio.shield(task)
```

##### 关键语句理解

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

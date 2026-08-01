# Stage 18 · Promotion and supervised lifecycle / 晋升与受监督生命周期

<!-- journey: chapter=8 tests_added=7 -->

## English

### Goal

Promote a replica without letting old-source work overwrite new-primary writes, and make startup, failure, crash, and graceful shutdown settle every owned resource in an explainable order.

### Deliverable files

- `src/miniredis/core/executor.py`
- `src/miniredis/core/mailbox.py`
- `src/miniredis/persistence/aof.py`
- `src/miniredis/replication/sink.py`
- `src/miniredis/runtime.py`
- `tests/helpers/runtime.py`
- `tests/reliability/test_ambiguous_outcome.py`
- `tests/reliability/test_lost_acked_write.py`
- `tests/reliability/test_phase3_invariants.py`
- `tests/reliability/test_reliability_shutdown.py`
- `tests/reliability/test_restart.py`
- `tests/reliability/test_worker_failure.py`
- `tests/replication/test_promotion.py`

### The problem at this point

A replica can receive an apply message just before promotion while the caller is already preparing local writes. Without an executor barrier and generation retirement, that old-source batch can arrive late and overwrite new-primary state. Lifecycle ownership is the same ordering problem at a larger scale: admission, recovery, durability workers, snapshots, replica tasks, and the executor cannot be stopped independently.

### Failure preview

Promotion can return before an already-accepted apply, or a stale generation can mutate state afterward. A lagging replica promoted after source loss can also lose a write that the old primary acknowledged; this stage must expose that limit rather than imply synchronous durability. During failure, pending waiters can hang, startup can admit clients before recovery, or close can tear down AOF and executor before owned work settles.

### Test contract

<!-- journey-file: tests/replication/test_promotion.py -->
#### `tests/replication/test_promotion.py`

##### What this test locks

It locks promotion behind already-admitted apply controls, retires old generations, and keeps link generations monotonic.

##### How it constructs the counterexample

It gates an accepted apply, starts promotion, releases the apply, performs a post-promotion write, then submits a late old-generation batch.

##### Key test statement

```python
assert await accepted is True
assert applied is False
```

##### What a failure means

Promotion is not an ordered executor barrier or stale-source work can cross the role change.

<!-- journey-file: tests/reliability/test_lost_acked_write.py -->
#### `tests/reliability/test_lost_acked_write.py`

##### What this test locks

It locks the explicit durability limitation of asynchronous replication.

##### How it constructs the counterexample

It pauses the follower, acknowledges one primary write, crashes the source, and promotes the still-lagging replica.

##### Key test statement

```python
assert promotion.applied_seq == 0
assert await replica.direct_client().execute(
    CommandRequest(b"GET", (b"x",))
) == Bytes(None)
```

##### What a failure means

The implementation or documentation is hiding where acknowledged data can be lost.

<!-- journey-file: tests/reliability/test_ambiguous_outcome.py -->
#### `tests/reliability/test_ambiguous_outcome.py`

##### What this test locks

It locks conservative live-state behavior when append may have reached disk but completion reports failure.

##### How it constructs the counterexample

An appender writes a valid record and then returns an uncertain failure; live state rejects it while later recovery can observe the bytes.

##### Key test statement

```python
assert runtime.debug_commit_seq == 0
assert recovered.commit_seq == 1
```

##### What a failure means

An ambiguous durability result was falsely turned into a successful live reply or silently erased from recovery evidence.

<!-- journey-file: tests/reliability/test_phase3_invariants.py -->
#### `tests/reliability/test_phase3_invariants.py`

##### What this test locks

It locks one sequence-N logical state across live primary, recovered runtime, and caught-up replica, including expiry and eviction reasons in durable history.

##### How it constructs the counterexample

It mixes data types around a snapshot, catches up a replica, restarts the primary, and compares sequence and logical items.

##### Key test statement

```python
assert replica.debug_logical_items() == expected
assert recovered.debug_logical_items() == expected
```

##### What a failure means

Durability and replication no longer encode the same state transition history.

<!-- journey-file: tests/reliability/test_reliability_shutdown.py -->
#### `tests/reliability/test_reliability_shutdown.py`

##### What this test locks

It locks ordered cleanup, bounded replica drain, shielded shared close, and distinct stopped/source-lost outcomes.

##### How it constructs the counterexample

It blocks snapshot publication or replica bootstrap, overlaps close callers, and compares graceful close with simulated crash.

##### Key test statement

```python
assert trace.index("aof-closed") < trace.index("replicas-stopped")
assert trace.index("replicas-stopped") < trace.index("executor-stopped")
```

##### What a failure means

Resource owners are being torn down in an order that can strand work or allow use-after-close behavior.

<!-- journey-file: tests/reliability/test_restart.py -->
#### `tests/reliability/test_restart.py`

##### What this test locks

It locks recovery before user admission and failed-start cleanup without workers or sessions leaking.

##### How it constructs the counterexample

It restarts from snapshot plus later AOF, then separately starts from corrupt snapshot bytes.

##### Key test statement

```python
assert runtime.state is RuntimeState.FAILED
assert stats.accepting_users is False
```

##### What a failure means

Clients can observe unrecovered state or startup failure leaves live ownership behind.

<!-- journey-file: tests/reliability/test_worker_failure.py -->
#### `tests/reliability/test_worker_failure.py`

##### What this test locks

It locks terminal settlement of accepted requests, waiters, futures, and owned tasks when AOF fsync or executor work dies.

##### How it constructs the counterexample

It injects background fsync failure and unexpected executor death while a blocking request is pending.

##### Key test statement

```python
assert stats.accepted_requests == 0
assert stats.pending_futures == 0
```

##### What a failure means

A worker can die without transferring or terminating the ownership it held.

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

##### What this test locks

The helper provides narrow gates, AOF operations, sleep, and lifecycle trace hooks through the real runtime.

##### How it constructs the counterexample

It allocates paired apply events and passes explicit hooks into `MiniRedis._for_test`.

##### Key test statement

```python
replica_apply_entered=apply_entered,
replica_apply_release=apply_release,
```

##### What a failure means

Race tests cannot place a deterministic boundary around real executor ordering.

### Basic concepts

Promotion is a role transition guarded by the replica source generation. A barrier means all earlier accepted executor messages finish before the promotion message. Graceful close drains within configured bounds; crash close preserves only work already in flight. Terminal settlement means every accepted request, waiter, worker, file, snapshot job, and link ends in a visible outcome.

### Why this mechanism is necessary

Changing `read_only = False` is not promotion: it lacks an order relative to old-source traffic. Likewise, setting runtime state to closed is not shutdown: resource owners may still be active. One serialized barrier handles the role boundary, and one supervised lifecycle makes recovery and cleanup ordering auditable.

### Runtime mental model

Promotion first detaches a live source, stops the sink worker, and posts `PromoteReplica` behind accepted apply messages. The executor verifies the active generation, clears it, and reopens writes. Startup keeps user admission closed through recovery and worker initialization. Shutdown stops new users, settles requests, finishes snapshot and AOF ownership, drains or loses replica links according to close mode, then releases the executor barrier.

### Mechanism blocks

<!-- journey-file: src/miniredis/replication/sink.py -->
#### `src/miniredis/replication/sink.py`

##### What it is and why it appears

The sink gains promotion, source-loss, bounded drain, and stopped-state transitions.

##### Runtime role

It detaches from a live source, cancels streaming ownership, clears queued old history, and asks the replica executor to cross the role barrier.

##### Key code

```python
result = await self._replica.executor.promote_replica(self._generation)
self._state = ReplicaSinkState.PROMOTED
```

##### Statement understanding

The sink reports promoted only after the executor has retired that source generation and made the local database writable.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The executor gains the promotion message, a shutdown barrier, and deterministic failure settlement hooks.

##### Runtime role

Mailbox order places promotion after earlier applies; generation equality authorizes the role change; the held shutdown barrier leaves the worker available until outer resources close.

##### Key code

```python
if message.generation != self._active_source_generation:
    message.future.set_result(PromotionResult(self.database.commit_seq, False))
```

##### Statement understanding

Generation is a fencing token: once retired, delayed work from that source cannot mutate the promoted primary.

<!-- journey-file: src/miniredis/core/mailbox.py -->
#### `src/miniredis/core/mailbox.py`

##### What it is and why it appears

The mailbox can reopen user admission while control admission remains valid.

##### Runtime role

Startup and replica roles can keep user requests closed without destroying the control plane needed for recovery or promotion.

##### Key code

```python
if not self._control_open:
    raise RuntimeError("cannot reopen user admission after control close")
```

##### Statement understanding

User admission is reversible during lifecycle transitions; closing the control plane is terminal.

<!-- journey-file: src/miniredis/persistence/aof.py -->
#### `src/miniredis/persistence/aof.py`

##### What it is and why it appears

The AOF writer gains crash-specific closure semantics.

##### Runtime role

It stops accepting appends, joins its worker, shields an already-running sync, cancels an idle timer, and only then closes the descriptor.

##### Key code

```python
if self._sync_inflight:
    await asyncio.shield(self._sync_task)
```

##### Statement understanding

Crash simulation does not invent disk completion, but it must not close a descriptor underneath physical I/O already executing.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

The runtime becomes the supervisor for recovery, durability workers, snapshots, replica links, and executor shutdown.

##### Runtime role

It recovers before opening admission, turns worker failure into runtime failure, and closes owners in a recorded dependency order.

##### Key code

```python
await self._snapshot_manager.close()
await self._aof_writer.close()
await sink.drain_and_stop(self.config.replica_drain_grace_ms)
await self.executor.stop_after_shutdown_barrier()
```

##### Statement understanding

Shutdown order follows ownership dependencies: producers and durable jobs settle before the state executor they still call disappears.

### Verification evidence

Run the seven focused test modules from `tests.txt`, then cumulatively build Stages 1–18 and compare the complete owned source tree with commit `0fbaeee`.

### Durable takeaways

- Promotion needs an ordered barrier and a fencing generation.
- Asynchronous replication deliberately permits acknowledged-write loss while lagging.
- Recovery must finish before user admission opens.
- Shutdown correctness is terminal settlement, not merely task cancellation.

### Explain it in your own words

Why can an old generation not overwrite a post-promotion write, yet an acknowledged primary write can still be absent after promoting a lagging replica?

### Textbook

Generation checking is a fencing-token pattern; ordered promotion resembles a view change at one process boundary. The lost-write example exposes the consistency level of asynchronous primary–backup replication, while supervised shutdown applies structured-concurrency ownership to a storage runtime.

## 中文

### 目标

在不允许旧 Source 工作覆盖新 Primary 写入的前提下晋升 Replica，并让启动、失败、Crash 与 Graceful Shutdown 按可解释顺序收敛每个 Owned Resource。

### 交付文件

- `src/miniredis/core/executor.py`
- `src/miniredis/core/mailbox.py`
- `src/miniredis/persistence/aof.py`
- `src/miniredis/replication/sink.py`
- `src/miniredis/runtime.py`
- `tests/helpers/runtime.py`
- `tests/reliability/test_ambiguous_outcome.py`
- `tests/reliability/test_lost_acked_write.py`
- `tests/reliability/test_phase3_invariants.py`
- `tests/reliability/test_reliability_shutdown.py`
- `tests/reliability/test_restart.py`
- `tests/reliability/test_worker_failure.py`
- `tests/replication/test_promotion.py`

### 当前遇到的问题

Replica 可能在 Promotion 前一刻收到 Apply Message，而 Caller 已准备进行本地写入。没有 Executor Barrier 与 Generation 作废，旧 Source Batch 就可能迟到并覆盖新 Primary 状态。生命周期所有权也是更大尺度的排序问题：Admission、Recovery、Durability Worker、Snapshot、Replica Task 与 Executor 不能彼此独立地停止。

### 先看会坏在哪里

Promotion 可能先于已接纳 Apply 返回，或旧 Generation 在晋升后继续修改状态。Source 丢失后晋升落后的 Replica 也可能丢失旧 Primary 已确认的写入；本阶段必须暴露这个边界，而不是暗示同步耐久性。失败期间，Waiter 可能永久挂起、启动可能在恢复前接纳 Client、关闭也可能在 Owned Work 收敛前拆掉 AOF 与 Executor。

### 测试契约

<!-- journey-file: tests/replication/test_promotion.py -->
#### `tests/replication/test_promotion.py`

##### 锁定什么

锁定 Promotion 排在已接纳 Apply 之后、旧 Generation 作废且 Link Generation 单调递增。

##### 如何构造反例

用 Gate 卡住已接纳 Apply，开始 Promotion，释放 Apply，执行晋升后写入，再提交旧 Generation Batch。

##### 关键测试语句

```python
assert await accepted is True
assert applied is False
```

##### 失败意味着什么

Promotion 不是有序 Executor Barrier，或陈旧 Source 工作跨过了角色切换。

<!-- journey-file: tests/reliability/test_lost_acked_write.py -->
#### `tests/reliability/test_lost_acked_write.py`

##### 锁定什么

锁定异步复制明确存在的耐久性边界。

##### 如何构造反例

暂停 Follower，确认一个 Primary 写入，Crash Source，再晋升仍落后的 Replica。

##### 关键测试语句

```python
assert promotion.applied_seq == 0
assert await replica.direct_client().execute(
    CommandRequest(b"GET", (b"x",))
) == Bytes(None)
```

##### 失败意味着什么

实现或文档隐藏了已确认数据可能丢失的位置。

<!-- journey-file: tests/reliability/test_ambiguous_outcome.py -->
#### `tests/reliability/test_ambiguous_outcome.py`

##### 锁定什么

锁定 Append 可能落盘但 Completion 报错时，Live State 采取保守行为。

##### 如何构造反例

Appender 写入合法 Record 后返回不确定失败；Live State 拒绝它，而恢复仍能观察到字节。

##### 关键测试语句

```python
assert runtime.debug_commit_seq == 0
assert recovered.commit_seq == 1
```

##### 失败意味着什么

模糊耐久结果被错误变成成功 Reply，或其恢复证据被静默抹除。

<!-- journey-file: tests/reliability/test_phase3_invariants.py -->
#### `tests/reliability/test_phase3_invariants.py`

##### 锁定什么

锁定序号 N 在 Live Primary、Recovered Runtime 与已追平 Replica 上对应同一逻辑状态，并让 Expiry/Eviction 原因进入同一 Durable History。

##### 如何构造反例

在 Snapshot 前后混合多种数据类型，追平 Replica，重启 Primary，再比较 Sequence 与 Logical Items。

##### 关键测试语句

```python
assert replica.debug_logical_items() == expected
assert recovered.debug_logical_items() == expected
```

##### 失败意味着什么

Durability 与 Replication 不再编码同一条状态转移 History。

<!-- journey-file: tests/reliability/test_reliability_shutdown.py -->
#### `tests/reliability/test_reliability_shutdown.py`

##### 锁定什么

锁定有序清理、有界 Replica Drain、Shielded Shared Close，以及 STOPPED/SOURCE_LOST 的区别。

##### 如何构造反例

阻塞 Snapshot 发布或 Replica Bootstrap，重叠多个 Close Caller，并比较 Graceful Close 与模拟 Crash。

##### 关键测试语句

```python
assert trace.index("aof-closed") < trace.index("replicas-stopped")
assert trace.index("replicas-stopped") < trace.index("executor-stopped")
```

##### 失败意味着什么

Resource Owner 按会遗留工作或产生 Use-after-close 的顺序被拆除。

<!-- journey-file: tests/reliability/test_restart.py -->
#### `tests/reliability/test_restart.py`

##### 锁定什么

锁定 User Admission 之前完成恢复，并在启动失败后不泄漏 Worker 或 Session。

##### 如何构造反例

从 Snapshot 加后续 AOF 重启，再单独从损坏 Snapshot 字节启动。

##### 关键测试语句

```python
assert runtime.state is RuntimeState.FAILED
assert stats.accepting_users is False
```

##### 失败意味着什么

Client 能观察未恢复状态，或启动失败遗留存活所有权。

<!-- journey-file: tests/reliability/test_worker_failure.py -->
#### `tests/reliability/test_worker_failure.py`

##### 锁定什么

锁定 AOF Fsync 或 Executor Worker 死亡时 Accepted Request、Waiter、Future 与 Owned Task 的终态收敛。

##### 如何构造反例

在 Blocking Request 待处理时注入后台 Fsync 失败和意外 Executor 死亡。

##### 关键测试语句

```python
assert stats.accepted_requests == 0
assert stats.pending_futures == 0
```

##### 失败意味着什么

Worker 可以死亡却不转移或终结自己持有的所有权。

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

##### 锁定什么

Helper 通过真实 Runtime 提供窄范围 Gate、AOF Ops、Sleep 与 Lifecycle Trace Hook。

##### 如何构造反例

分配成对 Apply Event，并把显式 Hook 传入 `MiniRedis._for_test`。

##### 关键测试语句

```python
replica_apply_entered=apply_entered,
replica_apply_release=apply_release,
```

##### 失败意味着什么

竞态测试无法在真实 Executor 顺序上放置确定性边界。

### 基本概念

Promotion 是由 Replica Source Generation 防护的角色转换。Barrier 表示更早接纳的 Executor Message 都在 Promotion Message 前完成。Graceful Close 在配置边界内 Drain；Crash Close 只保留已经 In-flight 的工作。终态收敛表示每个 Accepted Request、Waiter、Worker、File、Snapshot Job 与 Link 都得到可观察结局。

### 为什么需要这个机制

把 `read_only = False` 不是 Promotion，因为它与旧 Source 流量没有顺序关系。同样，把 Runtime State 设为 Closed 也不是 Shutdown，因为 Resource Owner 可能仍然活着。一个序列化 Barrier 处理角色边界，一个受监督生命周期让恢复与清理顺序可以审计。

### 运行时心智模型

Promotion 先 Detach 存活 Source，停止 Sink Worker，再把 `PromoteReplica` 排在已接纳 Apply 之后。Executor 校验 Active Generation、清除它并重开写入。Startup 在恢复与 Worker 初始化期间保持 User Admission 关闭。Shutdown 停止新用户，收敛请求，完成 Snapshot 与 AOF 所有权，按关闭模式 Drain 或丢失 Replica Link，最后释放 Executor Barrier。

### 机制板块

<!-- journey-file: src/miniredis/replication/sink.py -->
#### `src/miniredis/replication/sink.py`

##### 是什么，为什么出现

Sink 增加 Promotion、Source Loss、有界 Drain 与 Stopped 状态转换。

##### 运行时角色

从存活 Source Detach，取消 Streaming 所有权，清除旧排队 History，再请求 Replica Executor 跨越角色屏障。

##### 关键代码

```python
result = await self._replica.executor.promote_replica(self._generation)
self._state = ReplicaSinkState.PROMOTED
```

##### 关键语句理解

只有 Executor 作废 Source Generation 并让本地 Database 可写后，Sink 才报告 Promoted。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么出现

Executor 增加 Promotion Message、Shutdown Barrier 与确定性失败收敛 Hook。

##### 运行时角色

Mailbox 顺序把 Promotion 放在更早 Apply 后面；Generation 相等才授权角色切换；Held Shutdown Barrier 让外层资源关闭前 Worker 仍可服务 Control。

##### 关键代码

```python
if message.generation != self._active_source_generation:
    message.future.set_result(PromotionResult(self.database.commit_seq, False))
```

##### 关键语句理解

Generation 是 Fencing Token：一旦作废，该 Source 的延迟工作就不能修改已晋升 Primary。

<!-- journey-file: src/miniredis/core/mailbox.py -->
#### `src/miniredis/core/mailbox.py`

##### 是什么，为什么出现

Mailbox 可以在 Control Admission 仍有效时重新开放 User Admission。

##### 运行时角色

Startup 与 Replica 角色能关闭用户请求，同时保留 Recovery 或 Promotion 所需的控制面。

##### 关键代码

```python
if not self._control_open:
    raise RuntimeError("cannot reopen user admission after control close")
```

##### 关键语句理解

生命周期转换期间 User Admission 可逆；关闭 Control Plane 才是终态。

<!-- journey-file: src/miniredis/persistence/aof.py -->
#### `src/miniredis/persistence/aof.py`

##### 是什么，为什么出现

AOF Writer 增加 Crash 专用关闭语义。

##### 运行时角色

停止接纳 Append，Join Worker，Shield 已运行 Sync，取消空闲 Timer，最后才关闭文件描述符。

##### 关键代码

```python
if self._sync_inflight:
    await asyncio.shield(self._sync_task)
```

##### 关键语句理解

Crash Simulation 不虚构磁盘完成，但不能在已经执行的物理 I/O 下方关闭 Descriptor。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么出现

Runtime 成为 Recovery、Durability Worker、Snapshot、Replica Link 与 Executor Shutdown 的 Supervisor。

##### 运行时角色

开放 Admission 前恢复，把 Worker Failure 转成 Runtime Failure，并按记录的依赖顺序关闭 Owner。

##### 关键代码

```python
await self._snapshot_manager.close()
await self._aof_writer.close()
await sink.drain_and_stop(self.config.replica_drain_grace_ms)
await self.executor.stop_after_shutdown_barrier()
```

##### 关键语句理解

Shutdown 顺序遵循所有权依赖：Producer 与 Durable Job 必须在它们仍会调用的 State Executor 消失前收敛。

### 验证证据

运行 `tests.txt` 中七个聚焦测试模块，再累计构建 Stage 1–18，并把完整 Owned Source Tree 与提交 `0fbaeee` 比较。

### 需要真正记住的内容

- Promotion 需要有序 Barrier 与 Fencing Generation。
- 异步复制明确允许落后时丢失已确认写入。
- User Admission 必须在 Recovery 后才开放。
- Shutdown 正确性是终态收敛，不只是取消 Task。

### 用自己的话讲清楚

为什么旧 Generation 不能覆盖晋升后写入，但 Primary 已确认写入仍可能在晋升落后 Replica 后消失？

### 教材

Generation Check 是 Fencing-token Pattern；有序 Promotion 类似单进程边界上的 View Change。丢失写入的反例暴露异步 Primary–backup 的一致性级别，而受监督 Shutdown 则把 Structured-concurrency Ownership 应用于存储 Runtime。

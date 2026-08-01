# Stage 15 · AOF commit barrier / AOF 提交屏障

<!-- journey: chapter=6 tests_added=11 -->

## English

### Goal

Make configured AOF acknowledgement the gate before memory apply, reply, waiter wakeup, or later replication.

### Deliverable files

- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/persistence/aof.py`
- `src/miniredis/runtime.py`
- `tests/helpers/runtime.py`
- `tests/reliability/test_commit_barrier.py`
- `tests/unit/persistence/test_aof_writer.py`

### The problem at this point

Frames can be encoded, but the runtime still needs one durability linearization point. If memory or reply advances before append acknowledgement, a crash or disk failure exposes success that recovery cannot reconstruct. Background writer failures must also settle current and future append waiters instead of hanging them.

### Failure preview

While an append is gated, database sequence and value must remain old, the reply must remain pending, and later state events must not overtake it. A failed append must apply nothing, return a durability error, transition the runtime to failed, and reject later commands.

### Test contract

<!-- journey-file: tests/reliability/test_commit_barrier.py -->
#### `tests/reliability/test_commit_barrier.py`

##### What this test locks

It locks append-before-apply/reply, no later state event during the barrier, fatal append failure, exact sequence acknowledgement, and no AOF calls for errors or no-ops.

##### How it constructs the counterexample

A gated fake appender records the batch and withholds acknowledgement while tests inspect memory, reply completion, queued commands, and failure state.

##### Key test statement

```python
assert runtime.database.commit_seq == 0
assert b"k" not in runtime.database.entries
assert not pending.done()
```

##### What a failure means

Visibility moved before durability, executor ordering crossed the barrier, or failed/no-op work consumed durable sequence.

<!-- journey-file: tests/unit/persistence/test_aof_writer.py -->
#### `tests/unit/persistence/test_aof_writer.py`

##### What this test locks

It locks ALWAYS/EVERYSEC/NO acknowledgement points, header durability, complete writes, owned periodic fsync, current/future failure settlement, and one failure notification.

##### How it constructs the counterexample

It injects manual sleep and file operations that fail write or fsync, including a concurrent record write and background fsync failure.

##### Key test statement

```python
assert outcome == AofAppendOk(1)
```

##### What a failure means

Policy acknowledged at the wrong durability point, an owned loop escaped supervision, or an append Future remained unresolved after failure.

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

##### What this test locks

The helper injects a barrier through a private runtime hook while preserving the normal executor and lifecycle path.

##### How it constructs the counterexample

It subclasses only for typed test access and starts the same runtime with `_RuntimeTestHooks(aof_appender=...)`.

##### Key test statement

```python
test_hooks=_RuntimeTestHooks(aof_appender=aof_appender)
```

##### What a failure means

Durability tests are no longer proving the production ownership path and may be exercising a separate fake runtime.

### Basic concepts

The commit barrier is the transition between proposed and visible state. `ALWAYS` acknowledges after record fsync, `EVERYSEC` after complete write while an owned loop fsyncs, and `NO` after write without record fsync. All policies still require complete append and exact sequence acknowledgement.

### Why this mechanism is necessary

Reply, memory, blocked-waiter wakeup, and replication must describe only history accepted by the configured durability contract. Waiting inside the single executor preserves global order. Fatal disk loss fails closed because continuing would create memory history that cannot be recovered.

### Runtime mental model

The executor turns a prepared commit into the next batch and awaits `commit_barrier.append`. The writer serializes encoded records and returns `AofAppendOk(seq)` at the policy point. Only the exact acknowledgement permits database apply and downstream effects. Any failure completes barriers, notifies supervision once, and transitions runtime shutdown through the existing failure path.

### Mechanism blocks

<!-- journey-file: src/miniredis/persistence/aof.py -->
#### `src/miniredis/persistence/aof.py`

##### What it is and why it appears

The AOF module gains an owned asynchronous writer, durability policies, injectable file operations, and typed append outcomes.

##### Runtime role

It initializes a durable header, serializes records through one queue, performs policy fsync, and settles every barrier on success, close, or failure.

##### Key code

```python
return await asyncio.shield(barrier)
```

##### Statement understanding

Canceling an append caller cannot cancel the writer-owned completion for bytes already admitted to its queue.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The executor centralizes every state-changing path behind `_commit_prepared`.

##### Runtime role

It allocates sequence, waits for exact AOF acknowledgement, applies memory, records the batch, then offers later consumers.

##### Key code

```python
outcome = await self.commit_barrier.append(batch)
if isinstance(outcome, AofAppendFailed):
    raise DurabilityFailure(outcome.message)
```

##### Statement understanding

No database apply occurs in the failure branch; durability error remains before the visibility linearization point.

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### What it is and why it appears

Configuration states AOF path, policy, tail repair, and fsync interval.

##### Runtime role

It selects whether persistence is enabled and which acknowledgement contract applies.

##### Key code

```python
if self.aof_fsync_interval_seconds <= 0:
    raise ValueError("aof_fsync_interval_seconds must be positive")
```

##### Statement understanding

The periodic policy needs a positive owned cadence and rejects an impossible loop at construction.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

Runtime constructs, starts, supervises, and closes the configured writer while retaining a private injection seam for contracts.

##### Runtime role

It selects the actual barrier before executor construction and maps writer/executor fatal errors into one failed lifecycle.

##### Key code

```python
actual_barrier = (
    test_hooks.aof_appender
    if test_hooks is not None and test_hooks.aof_appender is not None
    else commit_barrier
)
```

##### Statement understanding

Production has one barrier path; tests replace only its endpoint, not executor ordering or runtime supervision.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/15-aof-commit-barrier/tests.txt)`. It proves policy-specific writer behavior and end-to-end visibility/failure ordering through the real executor.

### Durable takeaways

Append before apply; acknowledge exact sequence; keep the executor stopped at the barrier; do not append errors/no-ops; supervise writer and fsync tasks; settle every Future; fail closed on durability loss.

### Explain it in your own words

The AOF is not a log written after success. Its acknowledgement is what permits success to become visible. Until the barrier passes, memory, replies, waiter wakeups, and downstream replicas remain behind the same ordered commit.

### Textbook

[Chapter 6](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/06-aof.md)

## 中文

### 目标

把已配置 AOF Ack 变成内存 Apply、Reply、Waiter Wakeup 或后续 Replication 前的 Gate。

### 交付文件

- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/persistence/aof.py`
- `src/miniredis/runtime.py`
- `tests/helpers/runtime.py`
- `tests/reliability/test_commit_barrier.py`
- `tests/unit/persistence/test_aof_writer.py`

### 当前遇到的问题

Frame 已可编码，但 Runtime 还需一个 Durability Linearization Point。如果 Memory 或 Reply 在 Append Ack 前前进，Crash 或 Disk Failure 会暴露 Recovery 无法重建的 Success。Background Writer Failure 也必须收束当前与未来 Append Waiter，而不是挂起。

### 先看会坏在哪里

Append 被 Gate 时，Database Sequence 与 Value 必须保持旧值，Reply 必须 Pending，后续 State Event 不得超车。Append Failure 必须什么都不 Apply，返回 Durability Error，把 Runtime 转为 Failed，并拒绝后续命令。

### 测试契约

<!-- journey-file: tests/reliability/test_commit_barrier.py -->
#### `tests/reliability/test_commit_barrier.py`

##### 测试锁定什么

它锁定 Append-before-apply/reply、Barrier 期间无后续 State Event、Fatal Append Failure、精确 Sequence Ack、以及 Error/No-op 不调 AOF。

##### 如何构造反例

Gated Fake Appender 记录 Batch 并扣住 Ack，测试此时检查 Memory、Reply Completion、Queued Command 与 Failure State。

##### 关键测试语句

```python
assert runtime.database.commit_seq == 0
assert b"k" not in runtime.database.entries
assert not pending.done()
```

##### 失败意味着什么

Visibility 移到 Durability 前，Executor Ordering 跨过 Barrier，或 Failed/No-op Work 消耗 Durable Sequence。

<!-- journey-file: tests/unit/persistence/test_aof_writer.py -->
#### `tests/unit/persistence/test_aof_writer.py`

##### 测试锁定什么

它锁定 ALWAYS/EVERYSEC/NO Ack Point、Header Durability、Complete Write、Owned Periodic Fsync、Current/Future Failure Settlement 与单次 Failure Notification。

##### 如何构造反例

它注入 Manual Sleep 与会使 Write/Fsync 失败的 File Operation，包括 Concurrent Record Write + Background Fsync Failure。

##### 关键测试语句

```python
assert outcome == AofAppendOk(1)
```

##### 失败意味着什么

Policy 在错误 Durability Point Ack，Owned Loop 逃离 Supervision，或 Append Future 在 Failure 后未解决。

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

##### 测试锁定什么

Helper 通过 Private Runtime Hook 注入 Barrier，但保留正常 Executor 与 Lifecycle Path。

##### 如何构造反例

它只为 Typed Test Access 子类化，并使用 `_RuntimeTestHooks(aof_appender=...)` 启动同一 Runtime。

##### 关键测试语句

```python
test_hooks=_RuntimeTestHooks(aof_appender=aof_appender)
```

##### 失败意味着什么

Durability Test 不再证明 Production Ownership Path，可能只在运行另一套 Fake Runtime。

### 基本概念

Commit Barrier 是 Proposed State 到 Visible State 的迁移。`ALWAYS` 在 Record Fsync 后 Ack，`EVERYSEC` 在 Complete Write 后 Ack，Owned Loop 后续 Fsync，`NO` 在 Write 后 Ack 且不做 Record Fsync。全部 Policy 仍要求 Complete Append 与 Exact Sequence Ack。

### 为什么需要这个机制

Reply、Memory、Blocked-waiter Wakeup 与 Replication 必须只描述已被配置 Durability Contract 接受的 History。在单 Executor 内等待保留 Global Order。Fatal Disk Loss 必须 Fail Closed，因为继续会创建无法恢复的 Memory History。

### 运行时心智模型

Executor 把 Prepared Commit 变成 Next Batch 并 Await `commit_barrier.append`。Writer 串行 Encoded Record，在 Policy Point 返回 `AofAppendOk(seq)`。只有精确 Ack 允许 Database Apply 与下游 Effect。任何 Failure 都收束 Barrier、只通知 Supervision 一次，并通过已有 Failure Path 迁移 Runtime Shutdown。

### 机制板块

<!-- journey-file: src/miniredis/persistence/aof.py -->
#### `src/miniredis/persistence/aof.py`

##### 是什么，为什么现在需要

AOF 模块增加 Owned Async Writer、Durability Policy、可注入 File Operation 与 Typed Append Outcome。

##### 在运行时做什么

它初始化 Durable Header，经单 Queue 串行 Record，执行 Policy Fsync，并在 Success、Close 或 Failure 时收束每个 Barrier。

##### 关键代码

```python
return await asyncio.shield(barrier)
```

##### 关键语句理解

取消 Append Caller 不能取消 Writer-owned Completion，因为 Bytes 已准入 Queue。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么现在需要

Executor 把所有 State-changing Path 集中到 `_commit_prepared`。

##### 在运行时做什么

它分配 Sequence，等 Exact AOF Ack，Apply Memory，记录 Batch，再 Offer 后续 Consumer。

##### 关键代码

```python
outcome = await self.commit_barrier.append(batch)
if isinstance(outcome, AofAppendFailed):
    raise DurabilityFailure(outcome.message)
```

##### 关键语句理解

Failure Branch 不执行 Database Apply；Durability Error 留在 Visibility Linearization Point 前。

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### 是什么，为什么现在需要

Config 声明 AOF Path、Policy、Tail Repair 与 Fsync Interval。

##### 在运行时做什么

它选择是否启用 Persistence 以及使用哪种 Ack Contract。

##### 关键代码

```python
if self.aof_fsync_interval_seconds <= 0:
    raise ValueError("aof_fsync_interval_seconds must be positive")
```

##### 关键语句理解

Periodic Policy 需要正 Owned Cadence，在构造时拒绝不可能 Loop。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么现在需要

Runtime 构造、启动、监督并关闭 Configured Writer，同时保留 Private Injection Seam 供 Contract 使用。

##### 在运行时做什么

它在 Executor 构造前选定 Actual Barrier，再把 Writer/Executor Fatal Error 映射到一个 Failed Lifecycle。

##### 关键代码

```python
actual_barrier = (
    test_hooks.aof_appender
    if test_hooks is not None and test_hooks.aof_appender is not None
    else commit_barrier
)
```

##### 关键语句理解

Production 只有一条 Barrier Path；Test 只替换其 Endpoint，不替换 Executor Ordering 或 Runtime Supervision。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/15-aof-commit-barrier/tests.txt)`。它证明 Policy-specific Writer 行为与经真实 Executor 的 End-to-end Visibility/Failure Ordering。

### 需要真正记住的内容

Append 先于 Apply；Ack Exact Sequence；让 Executor 停在 Barrier；不 Append Error/No-op；监督 Writer/Fsync Task；收束每个 Future；Durability Loss 时 Fail Closed。

### 用自己的话讲清楚

AOF 不是 Success 之后再写的 Log，它的 Ack 才允许 Success 可见。Barrier 通过前，Memory、Reply、Waiter Wakeup 与下游 Replica 都留在同一 Ordered Commit 后。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/06-aof.md)

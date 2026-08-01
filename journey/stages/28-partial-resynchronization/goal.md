# Stage 28 · Partial resynchronization / 部分重新同步

<!-- journey: chapter=8 tests_added=4 -->

## English

### Goal

Reconnect a detached or overflowed replica by applying an available missing backlog suffix before live batches, while fencing stale source generations and falling back to full replacement when continuity cannot be proven.

### Deliverable files

- `src/miniredis/core/executor.py`
- `src/miniredis/replication/sink.py`
- `src/miniredis/runtime.py`
- `tests/reliability/test_restart.py`
- `tests/replication/test_partial_resync.py`
- `tests/replication/test_promotion.py`
- `tests/replication/test_sink_overflow.py`

### The problem at this point

Stage 27 can select a partial attachment, but the sink still cannot install it. Resume must verify the replica still holds exactly the cursor state, apply frozen catch-up batches before any concurrently offered live batch, preserve a reconnectable cursor on disconnect/overflow, and reset source identity/backlog when a replica becomes a new primary.

### Failure preview

Applying live batch N+2 before catch-up N+1 creates divergence. Resuming onto a replica whose local sequence or source identity changed corrupts state. Reusing the old primary ID after promotion allows descendants to request unrelated history. Treating queue overflow as permanently dead wastes a still-covered backlog; forcing partial sync after rotation leaves stale keys that only full snapshot replacement removes.

### Test contract

<!-- journey-file: tests/replication/test_partial_resync.py -->
#### `tests/replication/test_partial_resync.py`

Locks current-cursor empty resume, short disconnect delta-only catch-up, catch-up-before-concurrent-live ordering, exact oldest-boundary coverage, and full replacement after backlog gaps.

<!-- journey-file: tests/replication/test_sink_overflow.py -->
#### `tests/replication/test_sink_overflow.py`

Locks overflow recovery through partial backlog when covered and full fallback after backlog rotation.

<!-- journey-file: tests/replication/test_promotion.py -->
#### `tests/replication/test_promotion.py`

Locks promotion as a new replication identity with empty backlog, rejects cursors from the old source, then starts a new backlog at the next local commit.

<!-- journey-file: tests/reliability/test_restart.py -->
#### `tests/reliability/test_restart.py`

Locks runtime restart as a new primary identity that forces full sync even when recovered commit sequence equals the replica cursor.

### Basic concepts

Partial resume has two ordered inputs: a frozen catch-up deque through attachment boundary B and a live queue containing commits after B. `CATCHING_UP` drains the former before `STREAMING` drains the latter. The replica executor stores active source ID and generation; resume is allowed only when read-only state, source ID, and current sequence match the cursor. Promotion creates a new epoch and clears inherited backlog.

### Why this mechanism is necessary

The backlog optimization is useful only when the sink can turn retained history into a verified state transition. Explicit catch-up state closes the race with concurrent commits, while executor-side source/sequence validation prevents applying a correct suffix to the wrong local base. Identity rotation on restart/promotion fences separate histories that reuse sequence numbers.

### Runtime mental model

Disconnect stops the apply task and clears volatile queues but retains `(replication_id, applied_seq)`. Reattach sends that cursor. Full attachment replaces the database and source identity; partial attachment asks the replica executor to fence the expected base, then seeds `_catch_up`. New primary commits offered during installation enter `_queue`. The apply worker enforces `batch.seq == applied_seq + 1`, drains catch-up fully, transitions to streaming, then drains live queue.

### Mechanism blocks

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

Tracks active source identity beside generation, validates exact partial-resume base, and on promotion clears source fencing, mints a new replication ID, and empties inherited backlog.

```python
allowed = self._replica_read_only and self._active_source_id == replication_id and self.database.commit_seq == expected_applied_seq
```

All three facts are required: role, history epoch, and exact offset.

<!-- journey-file: src/miniredis/replication/sink.py -->
#### `src/miniredis/replication/sink.py`

Adds cursor/sync-mode status, reconnectable states, disconnect, catch-up deque, live queue separation, strict next-sequence validation, and shared NEEDS_RESYNC cleanup.

```python
if batch.seq != self._applied_seq + 1:
    await self._mark_needs_resync()
```

Even a selected partial suffix is revalidated at application time; continuity is never inferred from container order alone.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

Exposes backlog count and oldest retained sequence to explain why a reconnect selected partial or full sync.

### Verification evidence

Run all four focused modules from `tests.txt`, cumulatively build Stages 1–28, and require owned-tree parity with `c07182f`.

### Durable takeaways

- Catch-up suffix must retire before live queued batches.
- Resume validates role, source epoch, and exact local offset.
- Overflow may resume if backlog still covers the cursor.
- Restart and promotion create new source identities.

### Explain it in your own words

Why must a promoted replica clear its inherited backlog and mint a new ID even though its commit sequence continues monotonically?

### Textbook

Partial resynchronization is log catch-up under epoch fencing. The two-queue sink is a handoff protocol: a frozen historical suffix joins a concurrent live stream at one boundary.

## 中文

### 目标

让 Detached 或 Overflowed Replica 先应用可用 Missing Backlog Suffix，再进入 Live Batch，同时 Fencing 陈旧 Source Generation，并在无法证明连续性时回退 Full Replacement。

### 交付文件

- `src/miniredis/core/executor.py`
- `src/miniredis/replication/sink.py`
- `src/miniredis/runtime.py`
- `tests/reliability/test_restart.py`
- `tests/replication/test_partial_resync.py`
- `tests/replication/test_promotion.py`
- `tests/replication/test_sink_overflow.py`

### 当前遇到的问题

Stage 27 能选择 Partial Attachment，但 Sink 还不能安装它。Resume 必须验证 Replica 仍精确持有 Cursor State，在并发 Offer 的 Live Batch 前应用 Frozen Catch-up Batch，在 Disconnect/Overflow 后保留可 Reconnect Cursor，并在 Replica 成为新 Primary 时重置 Source Identity/Backlog。

### 先看会坏在哪里

在 Catch-up N+1 前应用 Live Batch N+2 会产生 Divergence。向 Local Sequence 或 Source Identity 已改变的 Replica Resume 会破坏 State。Promotion 后复用旧 Primary ID，会让 Descendant 请求无关 History。把 Queue Overflow 当永久死亡会浪费仍 Covered 的 Backlog；Rotation 后强制 Partial Sync 会遗留只有 Full Snapshot Replacement 才能移除的 Stale Key。

### 测试契约

<!-- journey-file: tests/replication/test_partial_resync.py -->
#### `tests/replication/test_partial_resync.py`

锁定 Current-cursor Empty Resume、Short-disconnect Delta-only Catch-up、Catch-up-before-concurrent-live、精确 Oldest-boundary Coverage，以及 Backlog Gap 后 Full Replacement。

<!-- journey-file: tests/replication/test_sink_overflow.py -->
#### `tests/replication/test_sink_overflow.py`

锁定 Covered 时通过 Partial Backlog 恢复 Overflow，Backlog Rotation 后回退 Full。

<!-- journey-file: tests/replication/test_promotion.py -->
#### `tests/replication/test_promotion.py`

锁定 Promotion 创建新 Replication Identity 与 Empty Backlog，拒绝旧 Source Cursor，并在下一 Local Commit 开始新 Backlog。

<!-- journey-file: tests/reliability/test_restart.py -->
#### `tests/reliability/test_restart.py`

锁定 Runtime Restart 创建新 Primary Identity，即使 Recovered Commit Sequence 等于 Replica Cursor 也强制 Full Sync。

### 基本概念

Partial Resume 有两个有序输入：到 Attachment Boundary B 的 Frozen Catch-up Deque，以及包含 B 之后 Commit 的 Live Queue。`CATCHING_UP` 排空前者后，`STREAMING` 才排空后者。Replica Executor 保存 Active Source ID 与 Generation；只有 Read-only State、Source ID、Current Sequence 都匹配 Cursor 才允许 Resume。Promotion 创建新 Epoch 并清空继承 Backlog。

### 为什么需要这个机制

只有 Sink 能把 Retained History 变成已验证 State Transition，Backlog Optimization 才真正有用。显式 Catch-up State 关闭 Concurrent Commit Race，Executor-side Source/Sequence Validation 防止把正确 Suffix 应用到错误 Local Base。Restart/Promotion 时旋转 Identity，则 Fencing 复用 Sequence Number 的独立 History。

### 运行时心智模型

Disconnect 停止 Apply Task 并清除 Volatile Queue，但保留 `(replication_id, applied_seq)`。Reattach 发送 Cursor。Full Attachment 替换 Database 与 Source Identity；Partial Attachment 请求 Replica Executor Fencing Expected Base，再填充 `_catch_up`。安装期间 New Primary Commit 进入 `_queue`。Apply Worker 强制 `batch.seq == applied_seq + 1`，完全排空 Catch-up，转入 Streaming，再排空 Live Queue。

### 机制板块

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

在 Generation 旁追踪 Active Source Identity，校验精确 Partial-resume Base，并在 Promotion 时清除 Source Fencing、生成新 Replication ID、清空继承 Backlog。

```python
allowed = self._replica_read_only and self._active_source_id == replication_id and self.database.commit_seq == expected_applied_seq
```

三个事实缺一不可：Role、History Epoch、Exact Offset。

<!-- journey-file: src/miniredis/replication/sink.py -->
#### `src/miniredis/replication/sink.py`

加入 Cursor/Sync-mode Status、可 Reconnect State、Disconnect、Catch-up Deque、Live Queue 分离、严格 Next-sequence Validation 与共享 NEEDS_RESYNC Cleanup。

```python
if batch.seq != self._applied_seq + 1:
    await self._mark_needs_resync()
```

即使 Partial Suffix 已被选择，应用时仍重新校验；绝不只根据 Container Order 推断 Continuity。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

暴露 Backlog Count 与 Oldest Retained Sequence，用于解释 Reconnect 为什么选择 Partial 或 Full Sync。

### 验证证据

运行 `tests.txt` 中四个聚焦模块，累计构建 Stage 1–28，并要求 Owned-tree 与 `c07182f` 一致。

### 需要真正记住的内容

- Catch-up Suffix 必须先于 Live Queued Batch Retire。
- Resume 校验 Role、Source Epoch 与 Exact Local Offset。
- Backlog 仍覆盖 Cursor 时，Overflow 可以 Resume。
- Restart 与 Promotion 创建新 Source Identity。

### 用自己的话讲清楚

为什么 Promoted Replica 即使 Commit Sequence 连续，也必须清空继承 Backlog 并生成新 ID？

### 教材

Partial Resynchronization 是 Epoch Fencing 下的 Log Catch-up。Two-queue Sink 是 Handoff Protocol：Frozen Historical Suffix 在一个 Boundary 与 Concurrent Live Stream 相接。

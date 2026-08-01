# Stage 27 · Replication backlog / 复制积压日志

<!-- journey: chapter=8 tests_added=2 -->

## English

### Goal

Retain a bounded contiguous window of recent commits and use source identity plus replica cursor to decide atomically whether an attachment can resume from deltas or needs a full snapshot.

### Deliverable files

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

### Failure preview

Using sequence alone can replay history from a restarted or promoted source with unrelated state. Returning a partial suffix with a gap presents divergence as synchronization. Reading backlog and current sequence in different executor turns can miss a concurrent commit between selection and registration. An empty suffix at the current sequence is valid partial sync, while an empty backlog that cannot cover an older cursor is not.

### Test contract

<!-- journey-file: tests/unit/replication/test_backlog.py -->
#### `tests/unit/replication/test_backlog.py`

Locks bounded oldest-first rotation, exposed sequence bounds, exact missing suffixes, current empty range, uncovered/future cursor rejection, contiguous append, clear, and positive capacity.

<!-- journey-file: tests/replication/test_partial_resync.py -->
#### `tests/replication/test_partial_resync.py`

Uses a deterministic source identity and attachment probe to lock first full sync, covered partial suffix, current-cursor empty partial sync, and full fallback for diverged/future/rotated cursors.

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

Injects only the replication-ID factory through the real runtime so tests can state source-generation boundaries without replacing attachment logic.

### Basic concepts

A replication cursor is `(replication_id, applied_seq)`. The replication ID names one source-history generation; sequence names a position inside it. The backlog is a bounded contiguous deque of committed batches. `missing_after` returns a complete suffix, an empty suffix when already current, or `None` when coverage cannot be proven. A full attachment carries a snapshot; a partial attachment carries the frozen missing suffix and boundary.

### Why this mechanism is necessary

Partial synchronization reduces transfer and installation cost after short disconnections, but optimization must never weaken history identity. A source-generation token prevents sequence-number reuse across restart or promotion, and explicit coverage prevents a bounded buffer from pretending it contains history that has rotated away.

### Runtime mental model

Every committed batch enters the backlog before it is offered live to sinks. On attach, the executor freezes current sequence, checks the cursor ID, asks backlog for the exact suffix through that boundary, chooses full or partial attachment, registers it with the sink, then releases the turn. Commits afterward are offered as live queued batches, so the frozen catch-up suffix and live stream meet without a gap.

### Mechanism blocks

<!-- journey-file: src/miniredis/replication/backlog.py -->
#### `src/miniredis/replication/backlog.py`

Defines cursor/attachment domain values and a bounded contiguous deque with a three-way coverage result: suffix, current empty suffix, or unavailable.

```python
if not selected or selected[0].seq != expected:
    return None
```

The first selected batch must be exactly the next missing sequence; later retained history cannot repair an earlier gap.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

Owns primary generation identity, appends each commit to backlog, and performs full/partial selection plus sink registration in one serialized turn.

<!-- journey-file: src/miniredis/replication/sink.py -->
#### `src/miniredis/replication/sink.py`

Consumes the shared attachment union but still rejects partial installation in this preparatory stage; Stage 28 will teach the sink to apply it.

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

Adds a positive retained-batch capacity, making resume coverage and memory use explicit.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

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

## 中文

### 目标

保留有界连续 Recent Commit Window，并使用 Source Identity + Replica Cursor 原子决定 Attachment 能从 Delta Resume，还是必须 Full Snapshot。

### 交付文件

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

### 先看会坏在哪里

只用 Sequence 会从 Restart/Promoted Source 重放无关 History。返回含 Gap 的 Partial Suffix 会把 Divergence 呈现成同步。在不同 Executor Turn 读取 Backlog 与 Current Sequence，会在 Selection/Registration 间漏 Concurrent Commit。Current Sequence 的 Empty Suffix 是合法 Partial Sync；不能覆盖旧 Cursor 的 Empty Backlog 则不是。

### 测试契约

<!-- journey-file: tests/unit/replication/test_backlog.py -->
#### `tests/unit/replication/test_backlog.py`

锁定有界 Oldest-first Rotation、Sequence Bound、精确 Missing Suffix、Current Empty Range、Uncovered/Future Cursor Rejection、Contiguous Append、Clear 与 Positive Capacity。

<!-- journey-file: tests/replication/test_partial_resync.py -->
#### `tests/replication/test_partial_resync.py`

用确定性 Source Identity 与 Attachment Probe 锁定首次 Full Sync、Covered Partial Suffix、Current-cursor Empty Partial Sync，以及 Diverged/Future/Rotated Cursor 的 Full Fallback。

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

只通过真实 Runtime 注入 Replication-ID Factory，让测试能表达 Source-generation Boundary，而不替换 Attachment Logic。

### 基本概念

Replication Cursor 是 `(replication_id, applied_seq)`。Replication ID 命名一次 Source-history Generation；Sequence 命名其中位置。Backlog 是 Bounded Contiguous CommitBatch Deque。`missing_after` 返回完整 Suffix、已经 Current 时的 Empty Suffix，或无法证明 Coverage 时的 `None`。Full Attachment 携带 Snapshot；Partial Attachment 携带 Frozen Missing Suffix 与 Boundary。

### 为什么需要这个机制

Partial Synchronization 降低短暂断连后的传输与安装成本，但 Optimization 不能削弱 History Identity。Source-generation Token 防止 Restart/Promotion 后 Sequence Reuse，Explicit Coverage 防止 Bounded Buffer 假装仍含已经 Rotate Away 的 History。

### 运行时心智模型

每个 Committed Batch 在 Live Offer 给 Sink 前先进入 Backlog。Attach 时 Executor 冻结 Current Sequence，检查 Cursor ID，向 Backlog 请求到该 Boundary 的精确 Suffix，选择 Full/Partial Attachment，注册 Sink，再释放 Turn。之后 Commit 作为 Live Queued Batch Offer，因此 Frozen Catch-up Suffix 与 Live Stream 无 Gap 相接。

### 机制板块

<!-- journey-file: src/miniredis/replication/backlog.py -->
#### `src/miniredis/replication/backlog.py`

定义 Cursor/Attachment Domain Value 与 Bounded Contiguous Deque，其 Coverage Result 有三种：Suffix、Current Empty Suffix、Unavailable。

```python
if not selected or selected[0].seq != expected:
    return None
```

第一个 Selected Batch 必须恰好是 Next Missing Sequence；更晚 Retained History 无法修复早期 Gap。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

持有 Primary Generation Identity，把每个 Commit Append 到 Backlog，并在一个 Serialized Turn 中完成 Full/Partial Selection 与 Sink Registration。

<!-- journey-file: src/miniredis/replication/sink.py -->
#### `src/miniredis/replication/sink.py`

消费共享 Attachment Union，但在本准备阶段仍拒绝 Partial Installation；Stage 28 会让 Sink 真正应用它。

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

加入正数 Retained-batch Capacity，使 Resume Coverage 与 Memory Use 显式化。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

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

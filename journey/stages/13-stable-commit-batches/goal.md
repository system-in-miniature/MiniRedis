# Stage 13 · Stable commit batches / 稳定 Commit Batch

<!-- journey: chapter=6 tests_added=4 -->

## English

### Goal

Turn in-memory mutations into deterministic deep-frozen commits and snapshot images suitable for replay.

### Deliverable files

- `src/miniredis/core/commit.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/executor.py`
- `tests/unit/core/test_commit.py`

### The problem at this point

The executor records operations, but live dictionaries, sets, deques, access ticks, and logical-size caches are not a durable contract. Persistence needs values whose bytes and meaning do not change after planning, plus contiguous sequence ownership that cannot be consumed by failed or no-op commands.

### Failure preview

Mutating a live Hash after freezing must not alter stored operations. Applying sequence 3 after sequence 1 must fail before changing state, and a snapshot must reject duplicate or unsorted keys. Otherwise replay can silently diverge from the state originally committed.

### Test contract

<!-- journey-file: tests/unit/core/test_commit.py -->
#### `tests/unit/core/test_commit.py`

##### What this test locks

It locks deep stable freezing for every value family, exclusion of live metadata, contiguous batch application, no partial state on invalid sequence, and sorted snapshot identity.

##### How it constructs the counterexample

It freezes mutable containers, mutates the originals, applies both legal and gapped batches, and constructs snapshot images at ordering boundaries.

##### Key test statement

```python
with pytest.raises(ValueError, match="expected commit seq 2, got 3"):
```

##### What a failure means

The durable vocabulary aliases live state, sequence validation occurs after mutation, or snapshot bytes can depend on map iteration order.

### Basic concepts

`StoredValue` is an immutable canonical representation of logical data. `PreparedCommit` contains operations and trigger but no sequence; `CommitBatch` adds the sequence only when the single executor is ready to append. `SnapshotImage` freezes one sorted checkpoint state.

### Why this mechanism is necessary

Persistence and replication must replay semantic state, not Python object identity or access-policy metadata. Late sequence allocation keeps failed/no-op plans from creating gaps, while staged application makes one batch all-or-nothing even during replay validation.

### Runtime mental model

Planners freeze replacement values into operations. A successful plan exposes an optional `PreparedCommit`. The executor chooses `database.commit_seq + 1`, turns it into a batch, sends it through the barrier, then applies it. Database replay clones the entry map, validates and applies every operation, calculates usage, and swaps state only after the full batch succeeds.

### Mechanism blocks

<!-- journey-file: src/miniredis/core/commit.py -->
#### `src/miniredis/core/commit.py`

##### What it is and why it appears

This module defines transport-independent stored values, operations, sequenced batches, prepared commits, and snapshot images.

##### Runtime role

It is the stable vocabulary shared by executor, database, codec, persistence, and later replication.

##### Key code

```python
def to_batch(self, seq: int) -> CommitBatch:
    if seq <= 0:
        raise ValueError("commit seq must be positive")
```

##### Statement understanding

Planning cannot claim global order; only the serialized owner supplies a positive sequence.

<!-- journey-file: src/miniredis/core/database.py -->
#### `src/miniredis/core/database.py`

##### What it is and why it appears

Database gains deep freeze/thaw conversion, staged batch replay, and deterministic export.

##### Runtime role

It validates contiguous sequence, builds a candidate map, recomputes logical usage, then atomically replaces live state.

##### Key code

```python
staged = dict(self.entries)
staged_access_tick = self.access_tick
```

##### Statement understanding

All operations target a candidate state; an exception before the final swap leaves the live database unchanged.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

Execution plans now expose sequence-free prepared commits.

##### Runtime role

The executor allocates the next sequence only for nonempty accepted operations at the commit barrier.

##### Key code

```python
if not self.operations:
    return None
return PreparedCommit(self.operations, self.trigger)
```

##### Statement understanding

Replies, errors, touches, and other no-op plans do not consume commit sequence space.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/13-stable-commit-batches/tests.txt)`. It proves the stable value vocabulary and atomic replay invariants independently of disk I/O.

### Durable takeaways

Freeze semantic state deeply; exclude policy-only live metadata; plan without sequence; allocate order at one owner; reject gaps before mutation; export snapshots with unique sorted keys.

### Explain it in your own words

A commit is no longer a debug trace of live Python objects. It is a stable replay instruction. Planning decides what should change, the executor decides where it sits in global order, and Database applies the complete instruction through a staged state swap.

### Textbook

[Chapter 6](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/06-aof.md)

## 中文

### 目标

把内存变更变成适合重放的确定深度冻结 Commit 与 Snapshot Image。

### 交付文件

- `src/miniredis/core/commit.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/executor.py`
- `tests/unit/core/test_commit.py`

### 当前遇到的问题

Executor 已记录 Operation，但实时 Dict、Set、Deque、Access Tick 与 Logical-size Cache 不是耐久契约。Persistence 需要在规划后 Bytes 与含义都不再变的值，以及不会被失败或 No-op 命令消耗的连续 Sequence 所有权。

### 先看会坏在哪里

冻结后再修改实时 Hash，不得改变 Stored Operation。Sequence 1 后应用 Sequence 3 必须在状态改变前失败，Snapshot 必须拒绝重复或无序 Key。否则重放会静默偏离原提交状态。

### 测试契约

<!-- journey-file: tests/unit/core/test_commit.py -->
#### `tests/unit/core/test_commit.py`

##### 测试锁定什么

它锁定所有值族的深度稳定 Freeze、排除 Live Metadata、连续 Batch Apply、非法 Sequence 不部分变更与有序 Snapshot Identity。

##### 如何构造反例

它冻结可变容器后修改原容器，应用合法与 Gap Batch，并在排序边界构造 Snapshot Image。

##### 关键测试语句

```python
with pytest.raises(ValueError, match="expected commit seq 2, got 3"):
```

##### 失败意味着什么

耐久词汇别名实时状态，Sequence 在变更后才校验，或 Snapshot Bytes 依赖 Map Iteration Order。

### 基本概念

`StoredValue` 是逻辑数据的不可变 Canonical 表示。`PreparedCommit` 含 Operation 与 Trigger，但无 Sequence；`CommitBatch` 只在单 Executor 准备 Append 时增加 Sequence。`SnapshotImage` 冻结一个有序 Checkpoint State。

### 为什么需要这个机制

Persistence 与 Replication 必须重放语义状态，而非 Python Object Identity 或 Access-policy Metadata。延后 Sequence 分配防止失败/No-op Plan 产生 Gap；Staged Apply 则让一个 Batch 即使在重放校验中也全有全无。

### 运行时心智模型

Planner 把替换值冻结进 Operation。成功 Plan 暴露可选 `PreparedCommit`。Executor 选 `database.commit_seq + 1`，变成 Batch，先经 Barrier，再 Apply。Database Replay 复制 Entry Map，校验并应用每个 Operation，计算 Usage，只在全 Batch 成功后 Swap State。

### 机制板块

<!-- journey-file: src/miniredis/core/commit.py -->
#### `src/miniredis/core/commit.py`

##### 是什么，为什么现在需要

该模块定义 Transport-independent Stored Value、Operation、Sequenced Batch、Prepared Commit 与 Snapshot Image。

##### 在运行时做什么

它是 Executor、Database、Codec、Persistence 与后续 Replication 共享的稳定词汇。

##### 关键代码

```python
def to_batch(self, seq: int) -> CommitBatch:
    if seq <= 0:
        raise ValueError("commit seq must be positive")
```

##### 关键语句理解

Planning 不能声称全局顺序；只有串行所有者提供正 Sequence。

<!-- journey-file: src/miniredis/core/database.py -->
#### `src/miniredis/core/database.py`

##### 是什么，为什么现在需要

Database 增加深度 Freeze/Thaw 转换、Staged Batch Replay 与确定 Export。

##### 在运行时做什么

它校验连续 Sequence，构建 Candidate Map，重算 Logical Usage，再原子替换实时 State。

##### 关键代码

```python
staged = dict(self.entries)
staged_access_tick = self.access_tick
```

##### 关键语句理解

所有 Operation 目标都是 Candidate State；Final Swap 前异常使 Live Database 不变。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么现在需要

Execution Plan 现在暴露无 Sequence Prepared Commit。

##### 在运行时做什么

Executor 只为非空已接受 Operation 在 Commit Barrier 分配下一 Sequence。

##### 关键代码

```python
if not self.operations:
    return None
return PreparedCommit(self.operations, self.trigger)
```

##### 关键语句理解

Reply、Error、Touch 与其他 No-op Plan 不消耗 Commit Sequence Space。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/13-stable-commit-batches/tests.txt)`。它在无 Disk I/O 时证明稳定 Value Vocabulary 与 Atomic Replay 不变量。

### 需要真正记住的内容

深度冻结语义状态；排除 Policy-only Live Metadata；规划时无 Sequence；在一个 Owner 分配顺序；变更前拒绝 Gap；用唯一有序 Key 导出 Snapshot。

### 用自己的话讲清楚

Commit 不再是 Live Python Object 的 Debug Trace，而是稳定 Replay Instruction。Planning 决定改什么，Executor 决定它在全局顺序中的位置，Database 通过 Staged State Swap 应用完整指令。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/06-aof.md)

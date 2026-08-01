# Stage 13 · 稳定 Commit Batch

### 目标

把内存变更变成适合重放的确定深度冻结 Commit 与 Snapshot Image。

??? note "交付文件"
    - `src/miniredis/core/commit.py`
    - `src/miniredis/core/database.py`
    - `src/miniredis/core/executor.py`
    - `tests/unit/core/test_commit.py`

### 当前遇到的问题

Executor 已记录 Operation，但实时 Dict、Set、Deque、Access Tick 与 Logical-size Cache 不是耐久契约。Persistence 需要在规划后 Bytes 与含义都不再变的值，以及不会被失败或 No-op 命令消耗的连续 Sequence 所有权。

### 测试契约

#### 先看会坏在哪里

冻结后再修改实时 Hash，不得改变 Stored Operation。Sequence 1 后应用 Sequence 3 必须在状态改变前失败，Snapshot 必须拒绝重复或无序 Key。否则重放会静默偏离原提交状态。

??? note "文件差异：tests/unit/core/test_commit.py"
    ```diff
    diff --git a/tests/unit/core/test_commit.py b/tests/unit/core/test_commit.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..db56bbf3b3dbd274417164b85f1fb30c5b6712fd
    --- /dev/null
    +++ b/tests/unit/core/test_commit.py
    @@ -0,0 +1,134 @@
    +from collections import deque
    +
    +import pytest
    +
    +from miniredis.core.commit import (
    +    CommitBatch,
    +    CommitTrigger,
    +    DeleteKey,
    +    DeleteReason,
    +    PreparedCommit,
    +    PutEntry,
    +    SnapshotImage,
    +    StoredEntry,
    +    StoredHash,
    +    StoredList,
    +    StoredSet,
    +    StoredString,
    +    StoredZSet,
    +)
    +from miniredis.core.database import Database, Entry, freeze_entry
    +from miniredis.core.values import (
    +    HashValue,
    +    ListValue,
    +    SetValue,
    +    StringValue,
    +    ZSetValue,
    +)
    +
    +
    +def test_freeze_entry_is_deep_stable_and_excludes_live_metadata():
    +    live = Entry(
    +        value=HashValue({b"z": b"last", b"a": b"first"}),
    +        expire_at_ms=9000,
    +        mutation_version=4,
    +        last_access_tick=71,
    +        logical_size=999,
    +    )
    +
    +    stored = freeze_entry(live)
    +    live.value.items[b"a"] = b"changed"
    +
    +    assert stored == StoredEntry(
    +        value=StoredHash(((b"a", b"first"), (b"z", b"last"))),
    +        expire_at_ms=9000,
    +        mutation_version=4,
    +    )
    +    assert not hasattr(stored, "last_access_tick")
    +    assert not hasattr(stored, "logical_size")
    +
    +
    +@pytest.mark.parametrize(
    +    ("value", "stored"),
    +    [
    +        (StringValue(b"a\x00b"), StoredString(b"a\x00b")),
    +        (ListValue(deque((b"b", b"a"))), StoredList((b"b", b"a"))),
    +        (SetValue({b"z", b"a"}), StoredSet((b"a", b"z"))),
    +        (
    +            ZSetValue({b"z": float("inf"), b"a": -1.5}),
    +            StoredZSet(((b"a", -1.5), (b"z", float("inf")))),
    +        ),
    +    ],
    +)
    +def test_all_live_values_have_immutable_stored_forms(value, stored):
    +    entry = Entry(value, None, 1, 3, 123)
    +    assert freeze_entry(entry).value == stored
    +
    +
    +def test_prepared_commit_is_sequence_free_until_executor_allocates_batch():
    +    prepared = PreparedCommit(
    +        operations=(
    +            PutEntry(
    +                b"k",
    +                StoredEntry(StoredString(b"v"), None, 1),
    +            ),
    +            DeleteKey(b"expired", DeleteReason.EXPIRED),
    +        ),
    +        trigger=CommitTrigger.CLIENT,
    +    )
    +
    +    assert not hasattr(prepared, "seq")
    +    assert prepared.to_batch(8) == CommitBatch(
    +        seq=8,
    +        operations=prepared.operations,
    +        trigger=CommitTrigger.CLIENT,
    +    )
    +
    +
    +def test_apply_batch_is_atomic_and_rejects_sequence_gaps():
    +    database = Database()
    +    batch = CommitBatch(
    +        seq=1,
    +        operations=(
    +            PutEntry(
    +                b"k",
    +                StoredEntry(StoredList((b"a", b"b")), 5000, 7),
    +            ),
    +            DeleteKey(b"missing", DeleteReason.CLIENT),
    +        ),
    +        trigger=CommitTrigger.CLIENT,
    +    )
    +
    +    database.apply_batch(batch, track_access=True)
    +
    +    assert database.commit_seq == 1
    +    assert list(database.entries[b"k"].value.items) == [b"a", b"b"]
    +    assert database.entries[b"k"].expire_at_ms == 5000
    +    assert database.entries[b"k"].mutation_version == 7
    +    assert database.entries[b"k"].logical_size > 0
    +    with pytest.raises(ValueError, match="expected commit seq 2, got 3"):
    +        database.apply_batch(
    +            CommitBatch(
    +                3,
    +                (
    +                    PutEntry(
    +                        b"later",
    +                        StoredEntry(StoredString(b"x"), None, 1),
    +                    ),
    +                ),
    +                CommitTrigger.ACTIVE_EXPIRE,
    +            ),
    +            track_access=False,
    +        )
    +
    +
    +def test_snapshot_image_has_sorted_stable_entries():
    +    image = SnapshotImage(
    +        checkpoint_seq=2,
    +        entries=(
    +            (b"a", StoredEntry(StoredString(b"1"), None, 1)),
    +            (b"z", StoredEntry(StoredSet((b"a", b"z")), 7000, 2)),
    +        ),
    +    )
    +    assert image.checkpoint_seq == 2
    +    assert tuple(key for key, _entry in image.entries) == (b"a", b"z")
    ```

**测试锁定什么**

它锁定所有值族的深度稳定 Freeze、排除 Live Metadata、连续 Batch Apply、非法 Sequence 不部分变更与有序 Snapshot Identity。

**如何构造反例**

它冻结可变容器后修改原容器，应用合法与 Gap Batch，并在排序边界构造 Snapshot Image。

**关键测试语句**

```python
with pytest.raises(ValueError, match="expected commit seq 2, got 3"):
```

**失败意味着什么**

耐久词汇别名实时状态，Sequence 在变更后才校验，或 Snapshot Bytes 依赖 Map Iteration Order。

### 基本概念

`StoredValue` 是逻辑数据的不可变 Canonical 表示。`PreparedCommit` 含 Operation 与 Trigger，但无 Sequence；`CommitBatch` 只在单 Executor 准备 Append 时增加 Sequence。`SnapshotImage` 冻结一个有序 Checkpoint State。

### 为什么需要这个机制

Persistence 与 Replication 必须重放语义状态，而非 Python Object Identity 或 Access-policy Metadata。延后 Sequence 分配防止失败/No-op Plan 产生 Gap；Staged Apply 则让一个 Batch 即使在重放校验中也全有全无。

### 运行时心智模型

Planner 把替换值冻结进 Operation。成功 Plan 暴露可选 `PreparedCommit`。Executor 选 `database.commit_seq + 1`，变成 Batch，先经 Barrier，再 Apply。Database Replay 复制 Entry Map，校验并应用每个 Operation，计算 Usage，只在全 Batch 成功后 Swap State。

### 机制板块

#### 稳定 Commit 与 Snapshot 值

把每个 Redis Value 冻结成确定、Transport-independent 的 Operation、Batch 与有序 Snapshot Image。

??? note "文件差异：src/miniredis/core/commit.py"
    ```diff
    diff --git a/src/miniredis/core/commit.py b/src/miniredis/core/commit.py
    index 2274b6d0a9026c4377d2e8f76f60d789242473e2..d262f838ea45d57befcaae1e5e2de3aeb6139aff 100644
    --- a/src/miniredis/core/commit.py
    +++ b/src/miniredis/core/commit.py
    @@ -77,3 +77,29 @@ class CommitBatch:
                 raise ValueError("commit seq must be positive")
             if not self.operations:
                 raise ValueError("commit batch operations cannot be empty")
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class PreparedCommit:
    +    operations: tuple[CommitOperation, ...]
    +    trigger: CommitTrigger
    +
    +    def to_batch(self, seq: int) -> CommitBatch:
    +        if seq <= 0:
    +            raise ValueError("commit seq must be positive")
    +        if not self.operations:
    +            raise ValueError("an empty prepared commit is a no-op")
    +        return CommitBatch(seq, self.operations, self.trigger)
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SnapshotImage:
    +    checkpoint_seq: int
    +    entries: tuple[tuple[bytes, StoredEntry], ...]
    +
    +    def __post_init__(self) -> None:
    +        if self.checkpoint_seq < 0:
    +            raise ValueError("checkpoint seq cannot be negative")
    +        keys = tuple(key for key, _entry in self.entries)
    +        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
    +            raise ValueError("snapshot entries must have unique sorted keys")
    ```

**是什么，为什么现在需要**

该模块定义 Transport-independent Stored Value、Operation、Sequenced Batch、Prepared Commit 与 Snapshot Image。

**在运行时做什么**

它是 Executor、Database、Codec、Persistence 与后续 Replication 共享的稳定词汇。

**关键代码**

```python
def to_batch(self, seq: int) -> CommitBatch:
    if seq <= 0:
        raise ValueError("commit seq must be positive")
```

**关键语句理解**

Planning 不能声称全局顺序；只有串行所有者提供正 Sequence。

#### 原子 Batch 重放

通过 Staged Map 应用一个连续 Batch，并导出深度冻结的逻辑或 Snapshot 状态。

??? note "文件差异：src/miniredis/core/database.py"
    ```diff
    diff --git a/src/miniredis/core/database.py b/src/miniredis/core/database.py
    index 25ad5ff367181660cd6fee8ed89dabecf8326f87..707b25de8cd248e89bd029c296dd18be41c3e2e4 100644
    --- a/src/miniredis/core/database.py
    +++ b/src/miniredis/core/database.py
    @@ -7,6 +7,7 @@ from miniredis.core.commit import (
         CommitBatch,
         DeleteKey,
         PutEntry,
    +    SnapshotImage,
         StoredEntry,
         StoredHash,
         StoredList,
    @@ -106,7 +107,7 @@ def thaw_value(value: StoredValue) -> RedisValue:
                 raise TypeError(f"unsupported stored value: {type(value)!r}")


    -def _freeze_entry(entry: Entry) -> StoredEntry:
    +def freeze_entry(entry: Entry) -> StoredEntry:
         return StoredEntry(
             value=freeze_value(entry.value),
             expire_at_ms=entry.expire_at_ms,
    @@ -172,5 +173,21 @@ class Database:

         def logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
             return tuple(
    -            (key, _freeze_entry(entry)) for key, entry in sorted(self.entries.items())
    +            (key, freeze_entry(entry)) for key, entry in sorted(self.entries.items())
    +        )
    +
    +    def export_stored_entries(
    +        self,
    +        now_ms: int,
    +    ) -> tuple[tuple[bytes, StoredEntry], ...]:
    +        return tuple(
    +            (key, freeze_entry(entry))
    +            for key, entry in sorted(self.entries.items())
    +            if entry.expire_at_ms is None or entry.expire_at_ms > now_ms
    +        )
    +
    +    def snapshot_image(self, now_ms: int) -> SnapshotImage:
    +        return SnapshotImage(
    +            checkpoint_seq=self.commit_seq,
    +            entries=self.export_stored_entries(now_ms),
             )
    ```

**是什么，为什么现在需要**

Database 增加深度 Freeze/Thaw 转换、Staged Batch Replay 与确定 Export。

**在运行时做什么**

它校验连续 Sequence，构建 Candidate Map，重算 Logical Usage，再原子替换实时 State。

**关键代码**

```python
staged = dict(self.entries)
staged_access_tick = self.access_tick
```

**关键语句理解**

所有 Operation 目标都是 Candidate State；Final Swap 前异常使 Live Database 不变。

#### 延后 Commit Sequence 分配

规划时让 Operation 不带 Sequence，只在串行 Append 边界分配下一个 Sequence。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index 5c884187bbb88292e1da922578363dfa32a49a9c..d9ea4d458917aeb8443bb40130abd5fa537777ed 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -28,6 +28,7 @@ from miniredis.core.commit import (
         CommitBatch,
         CommitOperation,
         CommitTrigger,
    +    PreparedCommit,
         PutEntry,
         StoredList,
     )
    @@ -98,6 +99,12 @@ class ExecutionPlan:
         trigger: CommitTrigger = CommitTrigger.CLIENT
         waiter_wakeups: tuple[WaiterWakeup, ...] = ()

    +    @property
    +    def prepared_commit(self) -> PreparedCommit | None:
    +        if not self.operations:
    +            return None
    +        return PreparedCommit(self.operations, self.trigger)
    +

     class CommitBarrier(Protocol):
         async def append(self, batch: CommitBatch) -> None: ...
    ```

**是什么，为什么现在需要**

Execution Plan 现在暴露无 Sequence Prepared Commit。

**在运行时做什么**

Executor 只为非空已接受 Operation 在 Commit Barrier 分配下一 Sequence。

**关键代码**

```python
if not self.operations:
    return None
return PreparedCommit(self.operations, self.trigger)
```

**关键语句理解**

Reply、Error、Touch 与其他 No-op Plan 不消耗 Commit Sequence Space。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/13-stable-commit-batches/tests.txt)`。它在无 Disk I/O 时证明稳定 Value Vocabulary 与 Atomic Replay 不变量。

### 需要真正记住的内容

深度冻结语义状态；排除 Policy-only Live Metadata；规划时无 Sequence；在一个 Owner 分配顺序；变更前拒绝 Gap；用唯一有序 Key 导出 Snapshot。

### 用自己的话讲清楚

Commit 不再是 Live Python Object 的 Debug Trace，而是稳定 Replay Instruction。Planning 决定改什么，Executor 决定它在全局顺序中的位置，Database 通过 Staged State Swap 应用完整指令。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/06-aof.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/6ff1e5f...5a40b5f)

完成后可运行 `python -m journey.tools.build_journey check 13` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/13-stable-commit-batches/stage.patch)

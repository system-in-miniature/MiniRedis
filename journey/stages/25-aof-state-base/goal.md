# Stage 25 · AOF state base / AOF 状态基线

<!-- journey: chapter=7 tests_added=8 -->

## English

### Goal

Allow an AOF to begin from a complete checkpoint image followed by contiguous delta commits, so later online rewrite can replace old history without requiring a separate snapshot file.

### Deliverable files

- `src/miniredis/persistence/aof.py`
- `src/miniredis/persistence/codec.py`
- `src/miniredis/persistence/recovery.py`
- `tests/reliability/test_final_acceptance.py`
- `tests/reliability/test_phase3_invariants.py`
- `tests/reliability/test_restart.py`
- `tests/replication/test_sink_attach.py`
- `tests/unit/persistence/test_aof_repair.py`
- `tests/unit/persistence/test_codec.py`
- `tests/unit/persistence/test_framing.py`
- `tests/unit/persistence/test_recovery.py`

### The problem at this point

The current AOF is only a sequence of commit deltas. Compacting it cannot discard early records unless another artifact supplies their resulting state. An online rewrite therefore needs one self-contained first record: state through checkpoint N, followed only by N+1, N+2, and so on. Recovery may also see an independent snapshot and must choose one base without double replay.

### Failure preview

A state base after a commit or a duplicate base makes history ambiguous. A first delta that skips or repeats the checkpoint boundary is incomplete. Choosing an older base over a newer snapshot loses state; applying deltas at or before the chosen base duplicates history. Tail repair that truncates into a partial base must fall back to the AOF header, not decode half a checkpoint.

### Test contract

<!-- journey-file: tests/unit/persistence/test_codec.py -->
#### `tests/unit/persistence/test_codec.py`

Locks deterministic state-base payload round trip and strict schema/version/record-type validation.

<!-- journey-file: tests/unit/persistence/test_framing.py -->
#### `tests/unit/persistence/test_framing.py`

Locks first-only placement, uniqueness, base-to-delta sequence continuity, base-only logs, legacy batch-only compatibility, and truncated-base boundaries.

<!-- journey-file: tests/unit/persistence/test_recovery.py -->
#### `tests/unit/persistence/test_recovery.py`

Locks newer-base selection, equal-checkpoint AOF preference, AOF-only base recovery, contiguous suffix replay, old-log rejection, and tail repair after a base.

<!-- journey-file: tests/unit/persistence/test_aof_repair.py -->
#### `tests/unit/persistence/test_aof_repair.py`

Locks the new `AofLog(state_base, batches)` return contract for repaired, missing, empty, and header-only logs.

<!-- journey-file: tests/reliability/test_restart.py -->
#### `tests/reliability/test_restart.py`

Locks restart as logical-state recovery while volatile LFU/access metadata resets to neutral.

<!-- journey-file: tests/replication/test_sink_attach.py -->
#### `tests/replication/test_sink_attach.py`

Locks full-sync snapshot installation with neutral LFU/access metadata on the follower.

<!-- journey-file: tests/reliability/test_phase3_invariants.py -->
#### `tests/reliability/test_phase3_invariants.py`

Updates invariant inspection to read commit deltas from the structured AOF log, preserving expiry/eviction evidence.

<!-- journey-file: tests/reliability/test_final_acceptance.py -->
#### `tests/reliability/test_final_acceptance.py`

Updates final durable-state evidence to inspect `AofLog.batches` without confusing a state base with a commit batch.

### Basic concepts

An AOF state base is a `SnapshotImage(checkpoint_seq, entries)` encoded inside ordinary length/CRC framing. It is a logical checkpoint, not a commit. `AofLog` separates the optional base from delta batches. The chosen recovery base is the newer of snapshot file and AOF state base; equal sequence prefers the AOF base because it belongs to the same log generation as its suffix.

### Why this mechanism is necessary

Online rewrite must publish one standalone AOF that recovers without coordinating two file replacements. A first-record state base gives the new file a complete starting state, while strict placement and sequence rules make rewritten and legacy logs equally auditable.

### Runtime mental model

The scanner verifies the header and each framed payload. If the first payload declares `state_base`, it decodes one checkpoint and seeds expected sequence to N+1; any later base is corruption. Loading returns `(base, batches)`. Recovery chooses the newest available base, validates the log end against it, and replays only batches with sequence greater than the chosen checkpoint.

### Mechanism blocks

<!-- journey-file: src/miniredis/persistence/codec.py -->
#### `src/miniredis/persistence/codec.py`

Adds strict payload encode/decode plus framed state-base records, and teaches the scanner the first-record-only state machine.

```python
if state_base is not None or batches:
    raise CodecError("AOF state base must be first")
```

The condition simultaneously forbids duplicates and bases after commits.

<!-- journey-file: src/miniredis/persistence/aof.py -->
#### `src/miniredis/persistence/aof.py`

Introduces `AofLog` and preserves both base and batches across normal load and truncated-tail repair.

<!-- journey-file: src/miniredis/persistence/recovery.py -->
#### `src/miniredis/persistence/recovery.py`

Selects the newer compatible checkpoint source, derives the AOF end from either base or final batch, and replays only the post-checkpoint suffix.

### Verification evidence

Run all eight focused modules in `tests.txt`, cumulatively build Stages 1–25, and require owned-tree parity with `b9b363e`.

### Durable takeaways

- A state base is checkpoint state, not commit delta.
- It may occur once, first, before a contiguous suffix.
- Recovery chooses one newest base and never double replays.
- Rewritten AOF can be self-contained.

### Explain it in your own words

Why does equal checkpoint sequence prefer the AOF state base, and why must the first following batch be exactly checkpoint plus one?

### Textbook

This is log compaction by checkpoint-plus-suffix. The base summarizes a prefix of state transitions; the remaining deltas retain causal order after that prefix.

## 中文

### 目标

允许 AOF 从完整 Checkpoint Image 开始，后接连续 Delta Commit，使后续 Online Rewrite 能替换旧 History 而不依赖单独 Snapshot File。

### 交付文件

- `src/miniredis/persistence/aof.py`
- `src/miniredis/persistence/codec.py`
- `src/miniredis/persistence/recovery.py`
- `tests/reliability/test_final_acceptance.py`
- `tests/reliability/test_phase3_invariants.py`
- `tests/reliability/test_restart.py`
- `tests/replication/test_sink_attach.py`
- `tests/unit/persistence/test_aof_repair.py`
- `tests/unit/persistence/test_codec.py`
- `tests/unit/persistence/test_framing.py`
- `tests/unit/persistence/test_recovery.py`

### 当前遇到的问题

当前 AOF 只有 Commit Delta Sequence。若没有其他 Artifact 提供早期记录的最终状态，Compact 就不能丢弃它们。Online Rewrite 因此需要一个自包含首 Record：State Through Checkpoint N，之后只能是 N+1、N+2……。Recovery 还可能同时看到独立 Snapshot，必须选一个 Base 且不能 Double Replay。

### 先看会坏在哪里

Commit 后出现 State Base 或重复 Base 会让 History 含糊。第一个 Delta 跳过或重复 Checkpoint Boundary 表示 History 不完整。用旧 Base 覆盖更新 Snapshot 会丢状态；应用所选 Base 以前的 Delta 会重复历史。Tail Repair 若截进 Partial Base，必须退回 AOF Header，而不能解码半个 Checkpoint。

### 测试契约

<!-- journey-file: tests/unit/persistence/test_codec.py -->
#### `tests/unit/persistence/test_codec.py`

锁定确定性 State-base Payload Round Trip 与严格 Schema/Version/Record-type Validation。

<!-- journey-file: tests/unit/persistence/test_framing.py -->
#### `tests/unit/persistence/test_framing.py`

锁定 First-only Placement、唯一性、Base-to-delta Sequence Continuity、Base-only Log、Legacy Batch-only Compatibility 与 Truncated-base Boundary。

<!-- journey-file: tests/unit/persistence/test_recovery.py -->
#### `tests/unit/persistence/test_recovery.py`

锁定 Newer-base Selection、Equal-checkpoint AOF Preference、AOF-only Base Recovery、Contiguous Suffix Replay、Old-log Rejection 与 Base 后 Tail Repair。

<!-- journey-file: tests/unit/persistence/test_aof_repair.py -->
#### `tests/unit/persistence/test_aof_repair.py`

锁定 Repaired、Missing、Empty、Header-only Log 的新 `AofLog(state_base, batches)` 返回契约。

<!-- journey-file: tests/reliability/test_restart.py -->
#### `tests/reliability/test_restart.py`

锁定 Restart 只恢复 Logical State，而 Volatile LFU/Access Metadata 归零。

<!-- journey-file: tests/replication/test_sink_attach.py -->
#### `tests/replication/test_sink_attach.py`

锁定 Full-sync Snapshot Installation 在 Follower 上使用中性 LFU/Access Metadata。

<!-- journey-file: tests/reliability/test_phase3_invariants.py -->
#### `tests/reliability/test_phase3_invariants.py`

把 Invariant Inspection 更新为从 Structured AOF Log 读取 Commit Delta，保留 Expiry/Eviction Evidence。

<!-- journey-file: tests/reliability/test_final_acceptance.py -->
#### `tests/reliability/test_final_acceptance.py`

把最终 Durable-state Evidence 更新为检查 `AofLog.batches`，不把 State Base 误当 Commit Batch。

### 基本概念

AOF State Base 是编码进普通 Length/CRC Framing 的 `SnapshotImage(checkpoint_seq, entries)`。它是 Logical Checkpoint，不是 Commit。`AofLog` 分离 Optional Base 与 Delta Batches。所选 Recovery Base 是 Snapshot File 与 AOF State Base 中较新的一个；Sequence 相等时偏好 AOF Base，因为它与 Suffix 属于同一 Log Generation。

### 为什么需要这个机制

Online Rewrite 必须发布一份无需协调两个 File Replacement 就能恢复的独立 AOF。First-record State Base 给新文件完整起点，严格 Placement/Sequence Rule 则让 Rewritten 与 Legacy Log 同样可审计。

### 运行时心智模型

Scanner 校验 Header 与每个 Framed Payload。若首 Payload 声明 `state_base`，解码一个 Checkpoint 并把 Expected Sequence 设为 N+1；之后任何 Base 都是 Corruption。Load 返回 `(base, batches)`。Recovery 选择最新 Base，校验 Log End，并只 Replay Sequence 大于所选 Checkpoint 的 Batch。

### 机制板块

<!-- journey-file: src/miniredis/persistence/codec.py -->
#### `src/miniredis/persistence/codec.py`

加入严格 Payload Encode/Decode 与 Framed State-base Record，并让 Scanner 实现 First-record-only State Machine。

```python
if state_base is not None or batches:
    raise CodecError("AOF state base must be first")
```

该条件同时禁止 Duplicate Base 与 Commit 后的 Base。

<!-- journey-file: src/miniredis/persistence/aof.py -->
#### `src/miniredis/persistence/aof.py`

引入 `AofLog`，并在普通 Load 与 Truncated-tail Repair 中保留 Base 与 Batches。

<!-- journey-file: src/miniredis/persistence/recovery.py -->
#### `src/miniredis/persistence/recovery.py`

选择更新的兼容 Checkpoint Source，从 Base 或 Final Batch 推导 AOF End，并只 Replay Post-checkpoint Suffix。

### 验证证据

运行 `tests.txt` 中八个聚焦模块，累计构建 Stage 1–25，并要求 Owned-tree 与 `b9b363e` 一致。

### 需要真正记住的内容

- State Base 是 Checkpoint State，不是 Commit Delta。
- 它只能出现一次、位于首位、先于连续 Suffix。
- Recovery 选择一个最新 Base，绝不 Double Replay。
- Rewritten AOF 可以自包含。

### 用自己的话讲清楚

为什么相同 Checkpoint Sequence 时偏好 AOF State Base？为什么第一个后续 Batch 必须恰好是 Checkpoint + 1？

### 教材

这是 Checkpoint-plus-suffix 形式的 Log Compaction。Base 汇总 State-transition Prefix，剩余 Delta 保留该 Prefix 之后的因果顺序。

# Stage 26 · Online AOF rewrite / 在线 AOF 重写

<!-- journey: chapter=7 tests_added=2 -->

## English

### Goal

Compact the live AOF into one checkpoint base plus every commit accepted during rewriting, without blocking normal appends or losing the authoritative old log before atomic publication succeeds.

### Deliverable files

- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/persistence/aof.py`
- `src/miniredis/runtime.py`
- `tests/helpers/runtime.py`
- `tests/reliability/test_aof_rewrite.py`
- `tests/unit/persistence/test_aof_writer.py`

### The problem at this point

Writing a checkpoint file takes blocking I/O while commits continue. The rewrite must capture exactly the suffix after its checkpoint, bound that in-memory delta, and switch the authoritative append descriptor only after the replacement is durably published. Failure before rename should leave the old AOF writable; uncertainty after rename must fail closed because the installed generation may already be authoritative.

### Failure preview

Capturing the image before registering delta collection creates a write-loss gap. Registering after the next append misses a batch. Unbounded delta capture can exhaust memory behind slow disk. Replacing before temp fsync publishes incomplete bytes; closing the old descriptor too early loses the fallback. Treating parent-fsync failure after rename as recoverable allows future writes against uncertain authority. Crash and graceful close also require different rewrite outcomes.

### Test contract

<!-- journey-file: tests/unit/persistence/test_aof_writer.py -->
#### `tests/unit/persistence/test_aof_writer.py`

Locks registration-before-append, BUSY overlap, disabled states, bounded delta overflow, unique temp paths, pre-rename failure isolation, ordered suffix capture, post-rename terminal failure, descriptor swap, graceful join, and crash cleanup. Failure identifies the exact publication phase whose ownership is wrong.

<!-- journey-file: tests/reliability/test_aof_rewrite.py -->
#### `tests/reliability/test_aof_rewrite.py`

Locks runtime-visible compaction, concurrent write recovery, no capture gap, BUSY behavior, disabled configuration, overflow preserving old history, newer-base recovery, graceful close, and simulated crash before rename. The decisive counterexample pauses base writing, commits `during`, then restarts and reads both values.

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

Provides a narrow rewrite-file gate through real AOF operations and exposes entered/release events; it does not replace writer ordering or publication logic.

### Basic concepts

A rewrite generation owns a checkpoint image, unique temp path/fd, completion future, base-writing task, and bounded delta buffer. The existing AOF remains authoritative until rename. Pre-rename errors are local rewrite failures. After rename, parent-directory durability is uncertain, so failure becomes terminal. Graceful close waits for publication; crash aborts and cleans temporary state.

### Why this mechanism is necessary

Append-only durability grows without bound. Online compaction must preserve availability while proving the rewritten file represents one state-machine prefix plus its complete concurrent suffix. Keeping delta capture inside the serial AOF writer reuses the authoritative commit order rather than reconstructing it from clocks or tasks.

### Runtime mental model

An executor control message captures SnapshotImage N and synchronously calls `begin_rewrite`, registering generation state before another commit turn. A background task writes header plus base. Meanwhile, the normal writer appends every committed record to the old AOF and copies the same encoded bytes into the bounded delta. A finalize item runs in writer order, appends the frozen delta to temp, fsyncs, renames, fsyncs the directory, swaps `_fd`, then closes the old descriptor.

### Mechanism blocks

<!-- journey-file: src/miniredis/persistence/aof.py -->
#### `src/miniredis/persistence/aof.py`

Owns rewrite outcomes, generation state, temp-file operations, delta capture, ordered finalization, descriptor handoff, cleanup, and close/crash semantics.

```python
self._capture_rewrite_delta(item.record)
self._settle(item.barrier, AofAppendOk(item.seq))
```

The exact encoded record accepted by the authoritative writer is also the rewrite suffix; no second serialization can drift.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

Captures the checkpoint and registers the AOF job in one serialized control turn, then bridges owned job completion into a cancellation-safe rewrite outcome.

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

Adds a positive maximum for concurrent rewrite delta bytes, turning slow rewrite memory growth into an explicit local failure.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

Wires writer registration before executor start, exposes `rewrite_aof`, reports active/checkpoint/delta stats, and includes rewrite ownership in shutdown.

### Verification evidence

Run both focused test modules from `tests.txt`, cumulatively build Stages 1–26, and require owned-tree parity with `8cd6d5e`.

### Durable takeaways

- Checkpoint capture and delta registration need one executor turn.
- The old AOF stays authoritative until durable replacement.
- Delta overflow fails rewrite, not normal appends.
- Post-rename durability uncertainty is terminal.

### Explain it in your own words

Why is a temp-write or replace failure recoverable for the running writer, while parent-directory fsync failure after rename makes the writer terminal?

### Textbook

This is copy-on-write log compaction with a concurrent delta buffer and an atomic publication protocol. Rename is the linearization point for file identity; directory fsync establishes its crash durability.

## 中文

### 目标

把 Live AOF Compact 成一个 Checkpoint Base 加 Rewrite 期间接纳的每个 Commit，同时不阻塞普通 Append，也不在 Atomic Publication 成功前丢失 Authoritative Old Log。

### 交付文件

- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/persistence/aof.py`
- `src/miniredis/runtime.py`
- `tests/helpers/runtime.py`
- `tests/reliability/test_aof_rewrite.py`
- `tests/unit/persistence/test_aof_writer.py`

### 当前遇到的问题

写 Checkpoint File 需要 Blocking I/O，而 Commit 仍继续。Rewrite 必须精确捕获 Checkpoint 后的 Suffix，限制 In-memory Delta，并且只在 Replacement Durable Publication 后切换 Authoritative Append Descriptor。Rename 前失败应让旧 AOF 继续可写；Rename 后的不确定性必须 Fail Closed，因为 Installed Generation 可能已经权威。

### 先看会坏在哪里

先 Capture Image、后注册 Delta Collection 会形成 Write-loss Gap。注册晚于下一 Append 会漏 Batch。无界 Delta 在慢磁盘后可耗尽内存。Temp Fsync 前 Replace 会发布不完整 Bytes；过早关闭旧 Descriptor 会丢 Fallback。把 Rename 后 Parent-fsync Failure 当可恢复，会让后续 Write 面对不确定 Authority。Crash 与 Graceful Close 也需要不同 Rewrite Outcome。

### 测试契约

<!-- journey-file: tests/unit/persistence/test_aof_writer.py -->
#### `tests/unit/persistence/test_aof_writer.py`

锁定 Registration-before-append、BUSY Overlap、Disabled State、有界 Delta Overflow、Unique Temp Path、Pre-rename Failure Isolation、Ordered Suffix Capture、Post-rename Terminal Failure、Descriptor Swap、Graceful Join 与 Crash Cleanup。失败会指出具体 Publication Phase 的所有权错误。

<!-- journey-file: tests/reliability/test_aof_rewrite.py -->
#### `tests/reliability/test_aof_rewrite.py`

锁定 Runtime-visible Compaction、Concurrent-write Recovery、无 Capture Gap、BUSY、Disabled Config、Overflow 保留旧 History、Newer-base Recovery、Graceful Close 与 Rename 前 Simulated Crash。关键反例暂停 Base Write，Commit `during`，Restart 后读取两个 Value。

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

通过真实 AOF Operation 提供窄范围 Rewrite-file Gate 与 Entered/Release Event；不替换 Writer Ordering 或 Publication Logic。

### 基本概念

Rewrite Generation 持有 Checkpoint Image、Unique Temp Path/Fd、Completion Future、Base-writing Task 与 Bounded Delta Buffer。Rename 前 Existing AOF 始终权威。Pre-rename Error 是局部 Rewrite Failure。Rename 后 Parent-directory Durability 不确定，所以 Failure 进入 Terminal。Graceful Close 等待 Publication；Crash Abort 并清理 Temporary State。

### 为什么需要这个机制

Append-only Durability 会无限增长。Online Compaction 必须在保持 Availability 的同时证明 Rewritten File 表示一个 State-machine Prefix 加完整 Concurrent Suffix。把 Delta Capture 放在串行 AOF Writer 内，复用 Authoritative Commit Order，而不从 Clock 或 Task 重建顺序。

### 运行时心智模型

Executor Control Message Capture SnapshotImage N，并同步调用 `begin_rewrite`，在另一个 Commit Turn 前注册 Generation State。后台 Task 写 Header + Base。与此同时，Normal Writer 把每个 Committed Record Append 到旧 AOF，并把相同 Encoded Bytes 复制进 Bounded Delta。Finalize Item 按 Writer Order 执行：Append Frozen Delta 到 Temp、Fsync、Rename、Fsync Directory、Swap `_fd`，最后 Close Old Descriptor。

### 机制板块

<!-- journey-file: src/miniredis/persistence/aof.py -->
#### `src/miniredis/persistence/aof.py`

持有 Rewrite Outcome、Generation State、Temp-file Operation、Delta Capture、Ordered Finalization、Descriptor Handoff、Cleanup 与 Close/Crash Semantics。

```python
self._capture_rewrite_delta(item.record)
self._settle(item.barrier, AofAppendOk(item.seq))
```

Authoritative Writer 接纳的精确 Encoded Record 同时成为 Rewrite Suffix；不存在第二套可能漂移的 Serialization。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

在一个 Serialized Control Turn 中 Capture Checkpoint 并注册 AOF Job，再把 Owned Job Completion Bridge 成 Cancellation-safe Rewrite Outcome。

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

为 Concurrent Rewrite Delta Bytes 加入正数上限，把慢 Rewrite 的内存增长变成显式局部 Failure。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

在 Executor Start 前接线 Writer Registration，暴露 `rewrite_aof`，报告 Active/Checkpoint/Delta Stats，并把 Rewrite Ownership 纳入 Shutdown。

### 验证证据

运行 `tests.txt` 中两个聚焦测试模块，累计构建 Stage 1–26，并要求 Owned-tree 与 `8cd6d5e` 一致。

### 需要真正记住的内容

- Checkpoint Capture 与 Delta Registration 需要同一个 Executor Turn。
- 旧 AOF 在 Durable Replacement 前保持权威。
- Delta Overflow 让 Rewrite 失败，不让普通 Append 失败。
- Rename 后 Durability 不确定性是 Terminal。

### 用自己的话讲清楚

为什么 Temp-write 或 Replace Failure 对 Running Writer 可恢复，而 Rename 后 Parent-directory Fsync Failure 会让 Writer 进入 Terminal？

### 教材

这是带 Concurrent Delta Buffer 与 Atomic Publication Protocol 的 Copy-on-write Log Compaction。Rename 是 File Identity 的 Linearization Point；Directory Fsync 建立 Crash Durability。

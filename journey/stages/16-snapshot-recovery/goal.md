# Stage 16 · Snapshot capture and recovery / Snapshot Capture 与恢复

<!-- journey: chapter=7 tests_added=20 -->

## English

### Goal

Capture a consistent checkpoint without blocking file I/O and recover it with exactly the compatible AOF suffix.

### Deliverable files

- `src/miniredis/config.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/persistence/recovery.py`
- `src/miniredis/persistence/snapshot.py`
- `src/miniredis/runtime.py`
- `tests/helpers/runtime.py`
- `tests/reliability/test_snapshot_barrier.py`
- `tests/unit/persistence/test_corruption.py`
- `tests/unit/persistence/test_recovery.py`
- `tests/unit/persistence/test_snapshot_manager.py`

### The problem at this point

AOF can reconstruct all history but grows with every commit. Snapshot capture must pair one checkpoint sequence with one logical state while commands continue; startup must then decide which AOF records are before, at, or after that checkpoint without hiding corruption.

### Failure preview

Commands after capture must not enter the image even if disk writing is still blocked. Startup must reject a bad snapshot checksum, an AOF gap after the checkpoint, or an AOF ending before it; expired entries must disappear using startup time while commit sequence remains recovered.

### Test contract

<!-- journey-file: tests/reliability/test_snapshot_barrier.py -->
#### `tests/reliability/test_snapshot_barrier.py`

##### What this test locks

It locks atomic sequence/state capture, exclusion of logically expired keys, and release of executor progress before slow file publication.

##### How it constructs the counterexample

It gates snapshot file write after capture, commits a second command, and proves the saved checkpoint still contains only the earlier state.

##### Key test statement

```python
assert runtime.debug_commit_seq == 2
assert outcome.checkpoint_seq == 1
```

##### What a failure means

Capture and checkpoint sequence came from different turns, or disk latency remained inside the single-writer critical path.

<!-- journey-file: tests/unit/persistence/test_snapshot_manager.py -->
#### `tests/unit/persistence/test_snapshot_manager.py`

##### What this test locks

It locks one active save, caller-cancellation shielding, atomic replacement, cleanup of temporary files, and preservation of the last good snapshot on failure.

##### How it constructs the counterexample

It overlaps saves, cancels a caller during capture, and injects replace failure after a previous snapshot exists.

##### Key test statement

```python
assert destination.read_bytes() == b"last-good"
```

##### What a failure means

Snapshot job ownership followed a caller, concurrent captures raced, or failed publication destroyed the recoverable checkpoint.

<!-- journey-file: tests/unit/persistence/test_recovery.py -->
#### `tests/unit/persistence/test_recovery.py`

##### What this test locks

It locks AOF-only, snapshot-only, and combined recovery; post-checkpoint segments; empty AOF behavior; expiry filtering; sequence retention; and reset of LRU metadata.

##### How it constructs the counterexample

It composes explicit checkpoint and AOF sequences, restarts twice, and chooses startup time exactly at one deadline.

##### Key test statement

```python
assert recovered.commit_seq == 3
```

##### What a failure means

Recovery replayed history twice, skipped committed history, or confused logical state with runtime access-policy metadata.

<!-- journey-file: tests/unit/persistence/test_corruption.py -->
#### `tests/unit/persistence/test_corruption.py`

##### What this test locks

It locks fail-closed snapshot/AOF corruption and exact sequence compatibility around the checkpoint.

##### How it constructs the counterexample

It corrupts checksums/headers and supplies AOF starts or ends that cannot form a contiguous suffix of the snapshot.

##### Key test statement

```python
with pytest.raises(RecoveryError, match="expected replay seq 8, got 9"):
```

##### What a failure means

Startup silently substituted an empty database or accepted a history whose missing commit cannot be reconstructed.

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

##### What this test locks

The helper gates only physical snapshot writing while keeping capture, executor, and runtime lifecycle production-shaped.

##### How it constructs the counterexample

An injected file-ops wrapper signals thread entry and blocks until the test releases publication.

##### Key test statement

```python
self._loop.call_soon_threadsafe(self.entered.set)
self.release.wait()
```

##### What a failure means

The contract cannot distinguish fast executor capture from slow blocking filesystem work.

### Basic concepts

A checkpoint is `(commit_seq, sorted logically live entries)` captured in one executor turn. Snapshot publication is a separate owned job using temporary file, file fsync, atomic rename, and directory fsync. Recovery installs the checkpoint, selects only contiguous AOF records after it, then removes entries expired at startup and resets access ticks.

### Why this mechanism is necessary

Snapshotting bounds replay cost without weakening the AOF commit contract. Separating capture from file write keeps command latency bounded, while strict checkpoint/AOF compatibility prevents missing or duplicate history from being normalized into plausible state.

### Runtime mental model

`save_snapshot` asks the manager for one job. The executor handles a capture control message between state events and returns a deep-frozen image immediately. A worker thread publishes its encoded bytes atomically. Startup loads and verifies snapshot/AOF before user admission, restores the database sequence and entries, replays only later batches, filters expired data, then starts writer and executor.

### Mechanism blocks

<!-- journey-file: src/miniredis/persistence/snapshot.py -->
#### `src/miniredis/persistence/snapshot.py`

##### What it is and why it appears

This module owns one shielded snapshot job and atomic POSIX file publication.

##### Runtime role

It rejects overlap as busy, captures through an async callback, writes off-loop, and preserves the last good destination on failure.

##### Key code

```python
os.replace(temporary, destination)
directory_fd = os.open(destination.parent, os.O_RDONLY)
try:
    os.fsync(directory_fd)
finally:
    os.close(directory_fd)
```

##### Statement understanding

Rename publishes the complete file; directory fsync makes the new directory entry durable across crash.

<!-- journey-file: src/miniredis/persistence/recovery.py -->
#### `src/miniredis/persistence/recovery.py`

##### What it is and why it appears

Recovery composes verified snapshot state and AOF history under explicit sequence rules.

##### Runtime role

It rejects incompatible histories, restores checkpoint state, replays the contiguous suffix without reappend, and applies startup-time expiry filtering.

##### Key code

```python
expected = staged.commit_seq + 1
if batch.seq != expected:
    raise RecoveryError(
        f"expected replay seq {expected}, got {batch.seq}"
    )
```

##### Statement understanding

Every recovered transition must be explainable; a gap is missing history, not an optional segment.

<!-- journey-file: src/miniredis/core/database.py -->
#### `src/miniredis/core/database.py`

##### What it is and why it appears

Database gains checkpoint installation and snapshot export of only logically live entries.

##### Runtime role

It restores frozen values with zero access ticks and retains checkpoint sequence independently of removed expired entries.

##### Key code

```python
return SnapshotImage(
    checkpoint_seq=self.commit_seq,
    entries=self.export_stored_entries(now_ms),
)
```

##### Statement understanding

Sequence describes durable history position; entries describe live logical state at capture time.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The executor accepts a snapshot-capture control message.

##### Runtime role

It reads clock and database in one ordered turn, returns the frozen image, and resumes user events before file I/O.

##### Key code

```python
image = self.database.snapshot_image(self.clock.now_ms())
if not message.future.done():
    message.future.set_result(image)
```

##### Statement understanding

Checkpoint sequence and entries are observed without an interleaving commit.

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### What it is and why it appears

Configuration adds snapshot path and validates persistence path relationships.

##### Runtime role

It makes recovery inputs explicit and prevents temporary/publication paths from aliasing unsafe locations.

##### Key code

```python
snapshot_path: Path | None = None
```

##### Statement understanding

Snapshotting is optional; recovery still supports AOF-only and empty configurations.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

Runtime performs recovery before admission, owns SnapshotManager, and exposes save as a lifecycle operation.

##### Runtime role

It installs recovered database state, starts durability from the recovered sequence, supervises jobs, and closes manager with other owned resources.

##### Key code

```python
async def save_snapshot(self) -> SnapshotOutcome:
    if self.state is not RuntimeState.RUNNING:
        return SnapshotFailed("runtime is not running")
```

##### Statement understanding

Snapshot capture is rejected outside running state so it cannot race startup recovery or terminal cleanup.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/16-snapshot-recovery/tests.txt)`. It covers manager ownership, capture ordering, atomic publication, all recovery combinations, expiry, and corruption/gap rejection.

### Durable takeaways

Capture sequence and state together; release executor before disk write; own and shield one save; publish with temp/fsync/rename/dir-fsync; recover before admission; replay only a contiguous suffix; never turn corruption into empty state.

### Explain it in your own words

A snapshot is a frozen cut through the commit stream, not a long lock around file writing. The executor produces that cut quickly, another owned job publishes it safely, and startup accepts only AOF history that continues exactly from the cut.

### Textbook

[Chapter 7](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/07-snapshots-recovery.md)

## 中文

### 目标

在不阻塞 File I/O 的情况下 Capture 一个一致 Checkpoint，再用精确兼容的 AOF Suffix 恢复它。

### 交付文件

- `src/miniredis/config.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/persistence/recovery.py`
- `src/miniredis/persistence/snapshot.py`
- `src/miniredis/runtime.py`
- `tests/helpers/runtime.py`
- `tests/reliability/test_snapshot_barrier.py`
- `tests/unit/persistence/test_corruption.py`
- `tests/unit/persistence/test_recovery.py`
- `tests/unit/persistence/test_snapshot_manager.py`

### 当前遇到的问题

AOF 可重建全部 History，但会随每个 Commit 增长。Snapshot Capture 必须把一个 Checkpoint Sequence 与一份 Logical State 配对，同时让命令继续；Startup 随后必须在不隐藏 Corruption 的前提下决定哪些 AOF Record 位于 Checkpoint 前、上或后。

### 先看会坏在哪里

Capture 后命令不得进 Image，即使 Disk Write 仍被阻塞。Startup 必须拒绝 Bad Snapshot Checksum、Checkpoint 后 AOF Gap 或早于 Checkpoint 结束的 AOF；Expired Entry 必须用 Startup Time 消失，但 Commit Sequence 仍保留恢复值。

### 测试契约

<!-- journey-file: tests/reliability/test_snapshot_barrier.py -->
#### `tests/reliability/test_snapshot_barrier.py`

##### 测试锁定什么

它锁定原子 Sequence/State Capture、排除逻辑过期 Key，以及在慢 File Publication 前释放 Executor 进度。

##### 如何构造反例

它在 Capture 后 Gate Snapshot File Write，提交第二条命令，并证明 Saved Checkpoint 仍只含更早 State。

##### 关键测试语句

```python
assert runtime.debug_commit_seq == 2
assert outcome.checkpoint_seq == 1
```

##### 失败意味着什么

Capture 与 Checkpoint Sequence 来自不同 Turn，或 Disk Latency 仍在 Single-writer Critical Path 内。

<!-- journey-file: tests/unit/persistence/test_snapshot_manager.py -->
#### `tests/unit/persistence/test_snapshot_manager.py`

##### 测试锁定什么

它锁定单 Active Save、Caller-cancellation Shield、Atomic Replace、Temporary File Cleanup 与 Failure 时保留最后 Good Snapshot。

##### 如何构造反例

它重叠 Save，在 Capture 中取消 Caller，并在已有 Snapshot 后注入 Replace Failure。

##### 关键测试语句

```python
assert destination.read_bytes() == b"last-good"
```

##### 失败意味着什么

Snapshot Job 所有权跟随 Caller，Concurrent Capture 竞争，或 Failed Publication 毁掉可恢复 Checkpoint。

<!-- journey-file: tests/unit/persistence/test_recovery.py -->
#### `tests/unit/persistence/test_recovery.py`

##### 测试锁定什么

它锁定 AOF-only、Snapshot-only 与 Combined Recovery、Post-checkpoint Segment、Empty AOF、Expiry Filtering、Sequence Retention 与 LRU Metadata Reset。

##### 如何构造反例

它组合显式 Checkpoint/AOF Sequence，Restart 两次，并把 Startup Time 选在精确 Deadline。

##### 关键测试语句

```python
assert recovered.commit_seq == 3
```

##### 失败意味着什么

Recovery 重放 History 两次、跳过 Committed History，或混淆 Logical State 与 Runtime Access-policy Metadata。

<!-- journey-file: tests/unit/persistence/test_corruption.py -->
#### `tests/unit/persistence/test_corruption.py`

##### 测试锁定什么

它锁定 Fail-closed Snapshot/AOF Corruption 与 Checkpoint 周围 Exact Sequence Compatibility。

##### 如何构造反例

它损坏 Checksum/Header，并提供无法形成 Snapshot Contiguous Suffix 的 AOF Start/End。

##### 关键测试语句

```python
with pytest.raises(RecoveryError, match="expected replay seq 8, got 9"):
```

##### 失败意味着什么

Startup 静默替换了 Empty Database，或接受了无法重建 Missing Commit 的 History。

<!-- journey-file: tests/helpers/runtime.py -->
#### `tests/helpers/runtime.py`

##### 测试锁定什么

Helper 只 Gate Physical Snapshot Write，同时保持 Capture、Executor 与 Runtime Lifecycle 为 Production-shaped。

##### 如何构造反例

注入的 File-ops Wrapper 通知 Thread Entry，并阻塞到测试释放 Publication。

##### 关键测试语句

```python
self._loop.call_soon_threadsafe(self.entered.set)
self.release.wait()
```

##### 失败意味着什么

契约无法区分 Fast Executor Capture 与 Slow Blocking Filesystem Work。

### 基本概念

Checkpoint 是在一个 Executor Turn 中 Capture 的 `(commit_seq, sorted logically live entries)`。Snapshot Publication 是独立 Owned Job，经 Temporary File、File Fsync、Atomic Rename 与 Directory Fsync。Recovery 安装 Checkpoint，只选其后 Contiguous AOF Record，再移除 Startup 时过期 Entry 并重置 Access Tick。

### 为什么需要这个机制

Snapshot 限制 Replay Cost，但不削弱 AOF Commit Contract。分开 Capture 与 File Write 使 Command Latency 有界；严格 Checkpoint/AOF Compatibility 防止 Missing 或 Duplicate History 被归一化成看似合理 State。

### 运行时心智模型

`save_snapshot` 请 Manager 启动一个 Job。Executor 在 State Event 之间处理 Capture Control Message，立即返回 Deep-frozen Image。Worker Thread 原子发布 Encoded Bytes。Startup 在 User Admission 前 Load/Verify Snapshot/AOF，恢复 Database Sequence/Entry，只 Replay 后续 Batch，Filter Expired Data，再启动 Writer 与 Executor。

### 机制板块

<!-- journey-file: src/miniredis/persistence/snapshot.py -->
#### `src/miniredis/persistence/snapshot.py`

##### 是什么，为什么现在需要

该模块拥有一个 Shielded Snapshot Job 与 Atomic POSIX File Publication。

##### 在运行时做什么

它把 Overlap 拒绝为 Busy，通过 Async Callback Capture，Off-loop Write，并在 Failure 时保留最后 Good Destination。

##### 关键代码

```python
os.replace(temporary, destination)
directory_fd = os.open(destination.parent, os.O_RDONLY)
try:
    os.fsync(directory_fd)
finally:
    os.close(directory_fd)
```

##### 关键语句理解

Rename 发布 Complete File；Directory Fsync 使新 Directory Entry 跨 Crash 耐久。

<!-- journey-file: src/miniredis/persistence/recovery.py -->
#### `src/miniredis/persistence/recovery.py`

##### 是什么，为什么现在需要

Recovery 在显式 Sequence Rule 下组合 Verified Snapshot State 与 AOF History。

##### 在运行时做什么

它拒绝不兼容 History，恢复 Checkpoint State，不 Reappend 地 Replay Contiguous Suffix，并做 Startup-time Expiry Filtering。

##### 关键代码

```python
expected = staged.commit_seq + 1
if batch.seq != expected:
    raise RecoveryError(
        f"expected replay seq {expected}, got {batch.seq}"
    )
```

##### 关键语句理解

每个 Recovered Transition 都必须可解释；Gap 是 Missing History，不是 Optional Segment。

<!-- journey-file: src/miniredis/core/database.py -->
#### `src/miniredis/core/database.py`

##### 是什么，为什么现在需要

Database 增加 Checkpoint Installation 与只导出 Logically Live Entry 的 Snapshot Export。

##### 在运行时做什么

它以零 Access Tick 恢复 Frozen Value，并与被移除 Expired Entry 无关地保留 Checkpoint Sequence。

##### 关键代码

```python
return SnapshotImage(
    checkpoint_seq=self.commit_seq,
    entries=self.export_stored_entries(now_ms),
)
```

##### 关键语句理解

Sequence 描述 Durable History Position；Entry 描述 Capture Time 的 Live Logical State。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么现在需要

Executor 接收 Snapshot-capture Control Message。

##### 在运行时做什么

它在一个 Ordered Turn 读 Clock/Database，返回 Frozen Image，并在 File I/O 前恢复 User Event。

##### 关键代码

```python
image = self.database.snapshot_image(self.clock.now_ms())
if not message.future.done():
    message.future.set_result(image)
```

##### 关键语句理解

Checkpoint Sequence 与 Entry 在无 Interleaving Commit 时被观察。

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### 是什么，为什么现在需要

Config 增加 Snapshot Path 并校验 Persistence Path Relationship。

##### 在运行时做什么

它使 Recovery Input 显式，并防止 Temporary/Publication Path 别名不安全位置。

##### 关键代码

```python
snapshot_path: Path | None = None
```

##### 关键语句理解

Snapshot 可选；Recovery 仍支持 AOF-only 与 Empty Configuration。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么现在需要

Runtime 在 Admission 前 Recovery，拥有 SnapshotManager，并把 Save 暴露为 Lifecycle Operation。

##### 在运行时做什么

它安装 Recovered Database State，从 Recovered Sequence 启动 Durability，监督 Job，并与其他 Owned Resource 一起关 Manager。

##### 关键代码

```python
async def save_snapshot(self) -> SnapshotOutcome:
    if self.state is not RuntimeState.RUNNING:
        return SnapshotFailed("runtime is not running")
```

##### 关键语句理解

Snapshot Capture 在非 Running State 被拒绝，因此不与 Startup Recovery 或 Terminal Cleanup 竞争。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/16-snapshot-recovery/tests.txt)`。它覆盖 Manager Ownership、Capture Ordering、Atomic Publication、全部 Recovery Combination、Expiry 与 Corruption/Gap Rejection。

### 需要真正记住的内容

同时 Capture Sequence/State；Disk Write 前释放 Executor；拥有并 Shield 单 Save；用 Temp/Fsync/Rename/Dir-fsync 发布；Admission 前 Recovery；只 Replay Contiguous Suffix；不把 Corruption 变成 Empty State。

### 用自己的话讲清楚

Snapshot 是 Commit Stream 上的 Frozen Cut，不是围绕 File Write 的长 Lock。Executor 快速生成 Cut，另一 Owned Job 安全发布，Startup 只接受从该 Cut 精确继续的 AOF History。

### 教材

[第 7 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/07-snapshots-recovery.md)

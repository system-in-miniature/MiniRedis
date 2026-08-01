# Stage 16 · Snapshot Capture 与恢复

### 目标

在不阻塞 File I/O 的情况下 Capture 一个一致 Checkpoint，再用精确兼容的 AOF Suffix 恢复它。

??? note "交付文件"
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

### 测试契约

#### 先看会坏在哪里

Capture 后命令不得进 Image，即使 Disk Write 仍被阻塞。Startup 必须拒绝 Bad Snapshot Checksum、Checkpoint 后 AOF Gap 或早于 Checkpoint 结束的 AOF；Expired Entry 必须用 Startup Time 消失，但 Commit Sequence 仍保留恢复值。

??? note "文件差异：tests/helpers/runtime.py"
    ```diff
    diff --git a/tests/helpers/runtime.py b/tests/helpers/runtime.py
    index b19bd4760addf6e03e8d0ab3bed2e8e144dc8df1..ce21d46eb6dc3c067db30674e9c00a74513a2c70 100644
    --- a/tests/helpers/runtime.py
    +++ b/tests/helpers/runtime.py
    @@ -1,8 +1,35 @@
    +import asyncio
    +import threading
    +from pathlib import Path
    +
    +from miniredis.persistence.snapshot import (
    +    PosixSnapshotFileOps,
    +    SnapshotFileOps,
    +)
     from miniredis.runtime import MiniRedis, _RuntimeTestHooks


    +class GateSnapshotFileOps:
    +    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
    +        self.entered = asyncio.Event()
    +        self.release = threading.Event()
    +        self._loop = loop
    +        self._delegate: SnapshotFileOps = PosixSnapshotFileOps()
    +
    +    def write_atomic(
    +        self,
    +        destination: Path,
    +        temporary: Path,
    +        data: bytes,
    +    ) -> None:
    +        self._loop.call_soon_threadsafe(self.entered.set)
    +        self.release.wait()
    +        self._delegate.write_atomic(destination, temporary, data)
    +
    +
     class TestMiniRedis(MiniRedis):
    -    pass
    +    debug_snapshot_write_entered: asyncio.Event
    +    debug_snapshot_write_release: threading.Event


     async def open_test_runtime(
    @@ -11,12 +38,21 @@ async def open_test_runtime(
         scheduler=None,
         aof_appender=None,
         config=None,
    +    snapshot_write_gate: bool = False,
     ) -> TestMiniRedis:
    +    loop = asyncio.get_running_loop()
    +    snapshot_gate = GateSnapshotFileOps(loop) if snapshot_write_gate else None
         runtime = TestMiniRedis._for_test(
             config=config,
             clock=clock,
             scheduler=scheduler,
    -        test_hooks=_RuntimeTestHooks(aof_appender=aof_appender),
    +        test_hooks=_RuntimeTestHooks(
    +            aof_appender=aof_appender,
    +            snapshot_ops=snapshot_gate,
    +        ),
         )
    +    if snapshot_gate is not None:
    +        runtime.debug_snapshot_write_entered = snapshot_gate.entered
    +        runtime.debug_snapshot_write_release = snapshot_gate.release
         await runtime.start()
         return runtime
    ```

**测试锁定什么**

Helper 只 Gate Physical Snapshot Write，同时保持 Capture、Executor 与 Runtime Lifecycle 为 Production-shaped。

**如何构造反例**

注入的 File-ops Wrapper 通知 Thread Entry，并阻塞到测试释放 Publication。

**关键测试语句**

```python
self._loop.call_soon_threadsafe(self.entered.set)
self.release.wait()
```

**失败意味着什么**

契约无法区分 Fast Executor Capture 与 Slow Blocking Filesystem Work。

??? note "文件差异：tests/reliability/test_snapshot_barrier.py"
    ```diff
    diff --git a/tests/reliability/test_snapshot_barrier.py b/tests/reliability/test_snapshot_barrier.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..c01427ddae023bc75070910d9bbbfd46ff321941
    --- /dev/null
    +++ b/tests/reliability/test_snapshot_barrier.py
    @@ -0,0 +1,64 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest
    +from miniredis.config import MiniRedisConfig
    +from miniredis.core.reply import Ok
    +from miniredis.persistence.codec import decode_snapshot_file
    +from miniredis.persistence.snapshot import SnapshotSaved
    +from tests.helpers.time import FakeClock
    +from tests.helpers.runtime import open_test_runtime
    +
    +
    +@pytest.mark.asyncio
    +async def test_snapshot_barrier_captures_seq_and_only_logically_live_keys(
    +    tmp_path,
    +):
    +    clock = FakeClock(now_ms=1000)
    +    path = tmp_path / "dump.mrsnap"
    +    runtime = await open_test_runtime(
    +        clock=clock,
    +        config=MiniRedisConfig(snapshot_path=path),
    +    )
    +    client = runtime.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"live", b"1")))
    +    await client.execute(
    +        CommandRequest(b"SET", (b"expired", b"2", b"PX", b"5"))
    +    )
    +    clock.advance(5)
    +
    +    outcome = await runtime.save_snapshot()
    +    image = decode_snapshot_file(path.read_bytes())
    +
    +    assert outcome == SnapshotSaved(path, 2)
    +    assert image.checkpoint_seq == 2
    +    assert tuple(key for key, _entry in image.entries) == (b"live",)
    +    await runtime.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_commands_continue_after_capture_while_file_write_is_blocked(
    +    tmp_path,
    +):
    +    runtime = await open_test_runtime(
    +        config=MiniRedisConfig(snapshot_path=tmp_path / "dump.mrsnap"),
    +        snapshot_write_gate=True,
    +    )
    +    client = runtime.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"before", b"1")))
    +    write_entered = runtime.debug_snapshot_write_entered
    +    release_write = runtime.debug_snapshot_write_release
    +
    +    save = asyncio.create_task(runtime.save_snapshot())
    +    await write_entered.wait()
    +    reply = await client.execute(
    +        CommandRequest(b"SET", (b"after", b"2"))
    +    )
    +
    +    assert reply == Ok()
    +    assert runtime.debug_commit_seq == 2
    +    release_write.set()
    +    outcome = await save
    +    assert outcome.checkpoint_seq == 1
    +    await runtime.close()
    ```

**测试锁定什么**

它锁定原子 Sequence/State Capture、排除逻辑过期 Key，以及在慢 File Publication 前释放 Executor 进度。

**如何构造反例**

它在 Capture 后 Gate Snapshot File Write，提交第二条命令，并证明 Saved Checkpoint 仍只含更早 State。

**关键测试语句**

```python
assert runtime.debug_commit_seq == 2
assert outcome.checkpoint_seq == 1
```

**失败意味着什么**

Capture 与 Checkpoint Sequence 来自不同 Turn，或 Disk Latency 仍在 Single-writer Critical Path 内。

??? note "文件差异：tests/unit/persistence/test_corruption.py"
    ```diff
    diff --git a/tests/unit/persistence/test_corruption.py b/tests/unit/persistence/test_corruption.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..887d8b7c781ef8630849b2f4fd2632a68ed73699
    --- /dev/null
    +++ b/tests/unit/persistence/test_corruption.py
    @@ -0,0 +1,86 @@
    +import pytest
    +
    +from miniredis.core.commit import SnapshotImage
    +from miniredis.persistence.codec import (
    +    AOF_HEADER,
    +    encode_aof_record,
    +    encode_snapshot_file,
    +)
    +from miniredis.persistence.recovery import RecoveryError, recover_database
    +from tests.unit.persistence.test_recovery import put
    +
    +
    +def test_bad_snapshot_checksum_is_not_an_empty_database(tmp_path):
    +    path = tmp_path / "dump.mrsnap"
    +    encoded = bytearray(encode_snapshot_file(SnapshotImage(0, ())))
    +    encoded[-1] ^= 0x01
    +    path.write_bytes(encoded)
    +
    +    with pytest.raises(RecoveryError, match="snapshot checksum"):
    +        recover_database(
    +            snapshot_path=path,
    +            aof_path=None,
    +            now_ms=0,
    +            repair_truncated_tail=True,
    +        )
    +
    +
    +def test_corrupt_aof_after_valid_snapshot_still_fails_startup(tmp_path):
    +    snapshot = tmp_path / "dump.mrsnap"
    +    aof = tmp_path / "appendonly.mraof"
    +    snapshot.write_bytes(encode_snapshot_file(SnapshotImage(0, ())))
    +    aof.write_bytes(b"not-an-aof")
    +
    +    with pytest.raises(RecoveryError, match="invalid AOF header"):
    +        recover_database(
    +            snapshot_path=snapshot,
    +            aof_path=aof,
    +            now_ms=0,
    +            repair_truncated_tail=True,
    +        )
    +
    +
    +def test_aof_only_segment_must_start_at_one(tmp_path):
    +    aof = tmp_path / "appendonly.mraof"
    +    aof.write_bytes(AOF_HEADER + encode_aof_record(put(2, b"k", b"v")))
    +
    +    with pytest.raises(RecoveryError, match="expected replay seq 1, got 2"):
    +        recover_database(
    +            snapshot_path=None,
    +            aof_path=aof,
    +            now_ms=0,
    +            repair_truncated_tail=True,
    +        )
    +
    +
    +def test_first_post_checkpoint_record_must_be_checkpoint_plus_one(tmp_path):
    +    snapshot = tmp_path / "dump.mrsnap"
    +    aof = tmp_path / "appendonly.mraof"
    +    snapshot.write_bytes(encode_snapshot_file(SnapshotImage(7, ())))
    +    aof.write_bytes(AOF_HEADER + encode_aof_record(put(9, b"k", b"v")))
    +
    +    with pytest.raises(RecoveryError, match="expected replay seq 8, got 9"):
    +        recover_database(
    +            snapshot_path=snapshot,
    +            aof_path=aof,
    +            now_ms=0,
    +            repair_truncated_tail=True,
    +        )
    +
    +
    +def test_nonempty_aof_ending_before_checkpoint_is_rejected(tmp_path):
    +    snapshot = tmp_path / "dump.mrsnap"
    +    aof = tmp_path / "appendonly.mraof"
    +    snapshot.write_bytes(encode_snapshot_file(SnapshotImage(7, ())))
    +    aof.write_bytes(AOF_HEADER + encode_aof_record(put(5, b"k", b"v")))
    +
    +    with pytest.raises(
    +        RecoveryError,
    +        match="AOF ends at seq 5 before snapshot checkpoint 7",
    +    ):
    +        recover_database(
    +            snapshot_path=snapshot,
    +            aof_path=aof,
    +            now_ms=0,
    +            repair_truncated_tail=True,
    +        )
    ```

**测试锁定什么**

它锁定 Fail-closed Snapshot/AOF Corruption 与 Checkpoint 周围 Exact Sequence Compatibility。

**如何构造反例**

它损坏 Checksum/Header，并提供无法形成 Snapshot Contiguous Suffix 的 AOF Start/End。

**关键测试语句**

```python
with pytest.raises(RecoveryError, match="expected replay seq 8, got 9"):
```

**失败意味着什么**

Startup 静默替换了 Empty Database，或接受了无法重建 Missing Commit 的 History。

??? note "文件差异：tests/unit/persistence/test_recovery.py"
    ```diff
    diff --git a/tests/unit/persistence/test_recovery.py b/tests/unit/persistence/test_recovery.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..5bbdc703d6b0fd661035099765fd3f47349062d8
    --- /dev/null
    +++ b/tests/unit/persistence/test_recovery.py
    @@ -0,0 +1,189 @@
    +from miniredis.core.commit import (
    +    CommitBatch,
    +    CommitTrigger,
    +    PutEntry,
    +    SnapshotImage,
    +    StoredEntry,
    +    StoredString,
    +)
    +from miniredis.persistence.codec import (
    +    AOF_HEADER,
    +    encode_aof_record,
    +    encode_snapshot_file,
    +)
    +from miniredis.persistence.recovery import recover_database
    +
    +
    +def put(seq: int, key: bytes, value: bytes, expire_at_ms=None):
    +    return CommitBatch(
    +        seq,
    +        (
    +            PutEntry(
    +                key,
    +                StoredEntry(
    +                    StoredString(value),
    +                    expire_at_ms,
    +                    seq,
    +                ),
    +            ),
    +        ),
    +        CommitTrigger.CLIENT,
    +    )
    +
    +
    +def write_aof(path, *batches):
    +    path.write_bytes(
    +        AOF_HEADER + b"".join(encode_aof_record(item) for item in batches)
    +    )
    +
    +
    +def test_aof_only_recovery_replays_without_reappend(tmp_path):
    +    aof = tmp_path / "appendonly.mraof"
    +    write_aof(aof, put(1, b"a", b"1"), put(2, b"b", b"2"))
    +
    +    recovered = recover_database(
    +        snapshot_path=None,
    +        aof_path=aof,
    +        now_ms=0,
    +        repair_truncated_tail=True,
    +    )
    +
    +    assert recovered.commit_seq == 2
    +    assert recovered.logical_items() == (
    +        (b"a", StoredEntry(StoredString(b"1"), None, 1)),
    +        (b"b", StoredEntry(StoredString(b"2"), None, 2)),
    +    )
    +    assert aof.read_bytes() == (
    +        AOF_HEADER
    +        + encode_aof_record(put(1, b"a", b"1"))
    +        + encode_aof_record(put(2, b"b", b"2"))
    +    )
    +
    +
    +def test_snapshot_only_recovery_restores_checkpoint(tmp_path):
    +    snapshot = tmp_path / "dump.mrsnap"
    +    image = SnapshotImage(
    +        7,
    +        ((b"k", StoredEntry(StoredString(b"v"), None, 3)),),
    +    )
    +    snapshot.write_bytes(encode_snapshot_file(image))
    +
    +    recovered = recover_database(
    +        snapshot_path=snapshot,
    +        aof_path=None,
    +        now_ms=0,
    +        repair_truncated_tail=True,
    +    )
    +
    +    assert recovered.commit_seq == 7
    +    assert recovered.logical_items() == image.entries
    +
    +
    +def test_combined_recovery_replays_only_after_checkpoint(tmp_path):
    +    snapshot = tmp_path / "dump.mrsnap"
    +    aof = tmp_path / "appendonly.mraof"
    +    snapshot.write_bytes(
    +        encode_snapshot_file(
    +            SnapshotImage(
    +                1,
    +                ((b"a", StoredEntry(StoredString(b"1"), None, 1)),),
    +            )
    +        )
    +    )
    +    write_aof(
    +        aof,
    +        put(1, b"a", b"1"),
    +        put(2, b"a", b"2"),
    +        put(3, b"b", b"3"),
    +    )
    +
    +    recovered = recover_database(
    +        snapshot_path=snapshot,
    +        aof_path=aof,
    +        now_ms=0,
    +        repair_truncated_tail=True,
    +    )
    +
    +    assert recovered.commit_seq == 3
    +    assert recovered.logical_items() == (
    +        (b"a", StoredEntry(StoredString(b"2"), None, 2)),
    +        (b"b", StoredEntry(StoredString(b"3"), None, 3)),
    +    )
    +
    +
    +def test_checkpoint_7_accepts_aof_segment_starting_at_8_on_restarts(
    +    tmp_path,
    +):
    +    snapshot = tmp_path / "dump.mrsnap"
    +    aof = tmp_path / "appendonly.mraof"
    +    snapshot.write_bytes(
    +        encode_snapshot_file(
    +            SnapshotImage(
    +                7,
    +                ((b"base", StoredEntry(StoredString(b"7"), None, 7)),),
    +            )
    +        )
    +    )
    +    write_aof(aof, put(8, b"after", b"8"))
    +
    +    first = recover_database(
    +        snapshot_path=snapshot,
    +        aof_path=aof,
    +        now_ms=0,
    +        repair_truncated_tail=True,
    +    )
    +    assert first.commit_seq == 8
    +    with aof.open("ab") as stream:
    +        stream.write(encode_aof_record(put(9, b"later", b"9")))
    +
    +    second = recover_database(
    +        snapshot_path=snapshot,
    +        aof_path=aof,
    +        now_ms=0,
    +        repair_truncated_tail=True,
    +    )
    +    assert second.commit_seq == 9
    +    assert tuple(second.entries) == (b"base", b"after", b"later")
    +
    +
    +def test_checkpoint_7_accepts_an_existing_zero_byte_aof(tmp_path):
    +    snapshot = tmp_path / "dump.mrsnap"
    +    aof = tmp_path / "appendonly.mraof"
    +    image = SnapshotImage(
    +        7,
    +        ((b"base", StoredEntry(StoredString(b"7"), None, 7)),),
    +    )
    +    snapshot.write_bytes(encode_snapshot_file(image))
    +    aof.write_bytes(b"")
    +
    +    recovered = recover_database(
    +        snapshot_path=snapshot,
    +        aof_path=aof,
    +        now_ms=0,
    +        repair_truncated_tail=True,
    +    )
    +
    +    assert recovered.commit_seq == 7
    +    assert recovered.logical_items() == image.entries
    +    assert aof.read_bytes() == b""
    +
    +
    +def test_startup_clock_discards_expired_values_and_resets_lru(tmp_path):
    +    aof = tmp_path / "appendonly.mraof"
    +    write_aof(
    +        aof,
    +        put(1, b"expired", b"x", expire_at_ms=100),
    +        put(2, b"live", b"y", expire_at_ms=101),
    +    )
    +
    +    recovered = recover_database(
    +        snapshot_path=None,
    +        aof_path=aof,
    +        now_ms=100,
    +        repair_truncated_tail=True,
    +    )
    +
    +    assert tuple(recovered.entries) == (b"live",)
    +    assert recovered.entries[b"live"].last_access_tick == 0
    +    assert recovered.access_tick == 0
    +    assert recovered.commit_seq == 2
    ```

**测试锁定什么**

它锁定 AOF-only、Snapshot-only 与 Combined Recovery、Post-checkpoint Segment、Empty AOF、Expiry Filtering、Sequence Retention 与 LRU Metadata Reset。

**如何构造反例**

它组合显式 Checkpoint/AOF Sequence，Restart 两次，并把 Startup Time 选在精确 Deadline。

**关键测试语句**

```python
assert recovered.commit_seq == 3
```

**失败意味着什么**

Recovery 重放 History 两次、跳过 Committed History，或混淆 Logical State 与 Runtime Access-policy Metadata。

??? note "文件差异：tests/unit/persistence/test_snapshot_manager.py"
    ```diff
    diff --git a/tests/unit/persistence/test_snapshot_manager.py b/tests/unit/persistence/test_snapshot_manager.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..88126d11c0bf3e776f1963b2609dc55d8824d42e
    --- /dev/null
    +++ b/tests/unit/persistence/test_snapshot_manager.py
    @@ -0,0 +1,89 @@
    +import asyncio
    +import os
    +
    +import pytest
    +
    +from miniredis.core.commit import SnapshotImage
    +from miniredis.persistence.snapshot import (
    +    SnapshotBusy,
    +    SnapshotFailed,
    +    SnapshotManager,
    +    SnapshotSaved,
    +)
    +
    +
    +@pytest.mark.asyncio
    +async def test_second_save_is_busy_before_another_capture_starts(tmp_path):
    +    capture_entered = asyncio.Event()
    +    release_capture = asyncio.Event()
    +    captures = 0
    +
    +    async def capture() -> SnapshotImage:
    +        nonlocal captures
    +        captures += 1
    +        capture_entered.set()
    +        await release_capture.wait()
    +        return SnapshotImage(0, ())
    +
    +    manager = SnapshotManager(tmp_path / "dump.mrsnap", capture)
    +    first = asyncio.create_task(manager.save())
    +    await capture_entered.wait()
    +
    +    assert await manager.save() == SnapshotBusy()
    +    assert captures == 1
    +
    +    release_capture.set()
    +    assert await first == SnapshotSaved(tmp_path / "dump.mrsnap", 0)
    +    await manager.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_cancelled_save_caller_does_not_cancel_owned_job(tmp_path):
    +    capture_entered = asyncio.Event()
    +    release_capture = asyncio.Event()
    +
    +    async def capture() -> SnapshotImage:
    +        capture_entered.set()
    +        await release_capture.wait()
    +        return SnapshotImage(3, ())
    +
    +    manager = SnapshotManager(tmp_path / "dump.mrsnap", capture)
    +    caller = asyncio.create_task(manager.save())
    +    await capture_entered.wait()
    +    caller.cancel()
    +    with pytest.raises(asyncio.CancelledError):
    +        await caller
    +
    +    release_capture.set()
    +    await manager.close()
    +
    +    assert (tmp_path / "dump.mrsnap").exists()
    +    assert manager.active_job is None
    +
    +
    +@pytest.mark.asyncio
    +async def test_failure_before_replace_preserves_last_snapshot(
    +    tmp_path,
    +    monkeypatch,
    +):
    +    destination = tmp_path / "dump.mrsnap"
    +    destination.write_bytes(b"last-good")
    +    real_replace = os.replace
    +
    +    def fail_replace(source, target):
    +        assert target == destination
    +        raise OSError("replace denied")
    +
    +    monkeypatch.setattr(os, "replace", fail_replace)
    +
    +    async def capture() -> SnapshotImage:
    +        return SnapshotImage(4, ())
    +
    +    manager = SnapshotManager(destination, capture)
    +    outcome = await manager.save()
    +
    +    assert outcome == SnapshotFailed("replace denied")
    +    assert destination.read_bytes() == b"last-good"
    +    assert tuple(tmp_path.glob(".dump.mrsnap.tmp.*")) == ()
    +    monkeypatch.setattr(os, "replace", real_replace)
    +    await manager.close()
    ```

**测试锁定什么**

它锁定单 Active Save、Caller-cancellation Shield、Atomic Replace、Temporary File Cleanup 与 Failure 时保留最后 Good Snapshot。

**如何构造反例**

它重叠 Save，在 Capture 中取消 Caller，并在已有 Snapshot 后注入 Replace Failure。

**关键测试语句**

```python
assert destination.read_bytes() == b"last-good"
```

**失败意味着什么**

Snapshot Job 所有权跟随 Caller，Concurrent Capture 竞争，或 Failed Publication 毁掉可恢复 Checkpoint。

### 基本概念

Checkpoint 是在一个 Executor Turn 中 Capture 的 `(commit_seq, sorted logically live entries)`。Snapshot Publication 是独立 Owned Job，经 Temporary File、File Fsync、Atomic Rename 与 Directory Fsync。Recovery 安装 Checkpoint，只选其后 Contiguous AOF Record，再移除 Startup 时过期 Entry 并重置 Access Tick。

### 为什么需要这个机制

Snapshot 限制 Replay Cost，但不削弱 AOF Commit Contract。分开 Capture 与 File Write 使 Command Latency 有界；严格 Checkpoint/AOF Compatibility 防止 Missing 或 Duplicate History 被归一化成看似合理 State。

### 运行时心智模型

`save_snapshot` 请 Manager 启动一个 Job。Executor 在 State Event 之间处理 Capture Control Message，立即返回 Deep-frozen Image。Worker Thread 原子发布 Encoded Bytes。Startup 在 User Admission 前 Load/Verify Snapshot/AOF，恢复 Database Sequence/Entry，只 Replay 后续 Batch，Filter Expired Data，再启动 Writer 与 Executor。

### 机制板块

#### Snapshot + AOF 恢复

加载已验证 Checkpoint，只重放其连续 Suffix，拒绝不兼容 History，并移除启动时逻辑过期 Entry。

??? note "文件差异：src/miniredis/persistence/recovery.py"
    ```diff
    diff --git a/src/miniredis/persistence/recovery.py b/src/miniredis/persistence/recovery.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..1721753330c0142f3159b9031736697f4736dad0
    --- /dev/null
    +++ b/src/miniredis/persistence/recovery.py
    @@ -0,0 +1,84 @@
    +from __future__ import annotations
    +
    +from pathlib import Path
    +
    +from miniredis.core.commit import SnapshotImage
    +from miniredis.core.database import Database
    +from miniredis.persistence.aof import AofCorruption, load_aof
    +from miniredis.persistence.codec import CodecError, decode_snapshot_file
    +
    +
    +class RecoveryError(RuntimeError):
    +    pass
    +
    +
    +def _load_snapshot(path: Path | None) -> SnapshotImage:
    +    if path is None:
    +        return SnapshotImage(0, ())
    +    try:
    +        data = path.read_bytes()
    +    except FileNotFoundError:
    +        return SnapshotImage(0, ())
    +    try:
    +        return decode_snapshot_file(data)
    +    except CodecError as exc:
    +        raise RecoveryError(str(exc)) from exc
    +
    +
    +def recover_database(
    +    *,
    +    snapshot_path: Path | None,
    +    aof_path: Path | None,
    +    now_ms: int,
    +    repair_truncated_tail: bool,
    +) -> Database:
    +    image = _load_snapshot(snapshot_path)
    +    try:
    +        batches = (
    +            load_aof(
    +                aof_path,
    +                repair_truncated_tail=repair_truncated_tail,
    +            )
    +            if aof_path is not None
    +            else ()
    +        )
    +    except AofCorruption as exc:
    +        raise RecoveryError(str(exc)) from exc
    +
    +    post_checkpoint = tuple(
    +        batch for batch in batches if batch.seq > image.checkpoint_seq
    +    )
    +    if batches and batches[-1].seq < image.checkpoint_seq:
    +        raise RecoveryError(
    +            f"AOF ends at seq {batches[-1].seq} before "
    +            f"snapshot checkpoint {image.checkpoint_seq}"
    +        )
    +    if image.checkpoint_seq == 0 and batches and batches[0].seq != 1:
    +        raise RecoveryError(
    +            f"expected replay seq 1, got {batches[0].seq}"
    +        )
    +    if (
    +        post_checkpoint
    +        and post_checkpoint[0].seq != image.checkpoint_seq + 1
    +    ):
    +        raise RecoveryError(
    +            "expected replay seq "
    +            f"{image.checkpoint_seq + 1}, got {post_checkpoint[0].seq}"
    +        )
    +
    +    staged = Database()
    +    staged.install_snapshot(image, now_ms=now_ms)
    +    for batch in post_checkpoint:
    +        expected = staged.commit_seq + 1
    +        if batch.seq != expected:
    +            raise RecoveryError(
    +                f"expected replay seq {expected}, got {batch.seq}"
    +            )
    +        try:
    +            staged.apply_batch(batch, track_access=False)
    +        except (TypeError, ValueError) as exc:
    +            raise RecoveryError(
    +                f"invalid commit {batch.seq}: {exc}"
    +            ) from exc
    +    staged.discard_expired_for_recovery(now_ms)
    +    return staged
    ```

**是什么，为什么现在需要**

Recovery 在显式 Sequence Rule 下组合 Verified Snapshot State 与 AOF History。

**在运行时做什么**

它拒绝不兼容 History，恢复 Checkpoint State，不 Reappend 地 Replay Contiguous Suffix，并做 Startup-time Expiry Filtering。

**关键代码**

```python
expected = staged.commit_seq + 1
if batch.seq != expected:
    raise RecoveryError(
        f"expected replay seq {expected}, got {batch.seq}"
    )
```

**关键语句理解**

每个 Recovered Transition 都必须可解释；Gap 是 Missing History，不是 Optional Segment。

#### Owned 原子 Snapshot Job

只允许一个 Shielded Save，按 Executor 顺序快速 Capture，再经 Temp-write、Fsync、Rename 与 Directory Fsync 发布 Encoded Image。

??? note "文件差异：src/miniredis/persistence/snapshot.py"
    ```diff
    diff --git a/src/miniredis/persistence/snapshot.py b/src/miniredis/persistence/snapshot.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..9fd51be96a79b501f41e0f1991f16300148cd924
    --- /dev/null
    +++ b/src/miniredis/persistence/snapshot.py
    @@ -0,0 +1,154 @@
    +from __future__ import annotations
    +
    +import asyncio
    +import os
    +from collections.abc import Awaitable, Callable
    +from dataclasses import dataclass
    +from pathlib import Path
    +from typing import Protocol, TypeAlias
    +
    +from miniredis.core.commit import SnapshotImage
    +from miniredis.persistence.codec import encode_snapshot_file
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SnapshotSaved:
    +    path: Path
    +    checkpoint_seq: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SnapshotBusy:
    +    pass
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SnapshotFailed:
    +    message: str
    +
    +
    +SnapshotOutcome: TypeAlias = SnapshotSaved | SnapshotBusy | SnapshotFailed
    +
    +
    +class SnapshotFileOps(Protocol):
    +    def write_atomic(
    +        self,
    +        destination: Path,
    +        temporary: Path,
    +        data: bytes,
    +    ) -> None:
    +        raise NotImplementedError
    +
    +
    +class PosixSnapshotFileOps:
    +    @staticmethod
    +    def _write_all(fd: int, data: bytes) -> None:
    +        view = memoryview(data)
    +        while view:
    +            written = os.write(fd, view)
    +            if written <= 0:
    +                raise OSError("snapshot write made no progress")
    +            view = view[written:]
    +
    +    def write_atomic(
    +        self,
    +        destination: Path,
    +        temporary: Path,
    +        data: bytes,
    +    ) -> None:
    +        destination.parent.mkdir(parents=True, exist_ok=True)
    +        fd: int | None = None
    +        try:
    +            fd = os.open(
    +                temporary,
    +                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
    +                0o600,
    +            )
    +            self._write_all(fd, data)
    +            os.fsync(fd)
    +            os.close(fd)
    +            fd = None
    +            os.replace(temporary, destination)
    +            directory_fd = os.open(destination.parent, os.O_RDONLY)
    +            try:
    +                os.fsync(directory_fd)
    +            finally:
    +                os.close(directory_fd)
    +        except BaseException:
    +            if fd is not None:
    +                os.close(fd)
    +            try:
    +                os.unlink(temporary)
    +            except FileNotFoundError:
    +                pass
    +            raise
    +
    +
    +class SnapshotManager:
    +    def __init__(
    +        self,
    +        path: Path,
    +        capture: Callable[[], Awaitable[SnapshotImage]],
    +        *,
    +        ops: SnapshotFileOps | None = None,
    +    ) -> None:
    +        self._path = path
    +        self._capture = capture
    +        self._ops = ops or PosixSnapshotFileOps()
    +        self._generation = 0
    +        self._accepting = True
    +        self._active_job: asyncio.Task[SnapshotOutcome] | None = None
    +
    +    @property
    +    def active_job(self) -> asyncio.Task[SnapshotOutcome] | None:
    +        return self._active_job
    +
    +    async def save(self) -> SnapshotOutcome:
    +        if not self._accepting:
    +            return SnapshotFailed("snapshot manager is closing")
    +        if self._active_job is not None:
    +            return SnapshotBusy()
    +        self._generation += 1
    +        generation = self._generation
    +        task = asyncio.create_task(
    +            self._run_save(generation),
    +            name=f"miniredis-snapshot-{generation}",
    +        )
    +        self._active_job = task
    +        task.add_done_callback(self._job_done)
    +        try:
    +            return await asyncio.shield(task)
    +        finally:
    +            if task.done() and self._active_job is task:
    +                self._active_job = None
    +
    +    def _job_done(self, task: asyncio.Task[SnapshotOutcome]) -> None:
    +        if self._active_job is task:
    +            self._active_job = None
    +
    +    async def _run_save(self, generation: int) -> SnapshotOutcome:
    +        temporary = self._path.with_name(
    +            f".{self._path.name}.tmp.{os.getpid()}.{generation}"
    +        )
    +        try:
    +            image = await self._capture()
    +            encoded = encode_snapshot_file(image)
    +            await asyncio.to_thread(
    +                self._ops.write_atomic,
    +                self._path,
    +                temporary,
    +                encoded,
    +            )
    +            return SnapshotSaved(self._path, image.checkpoint_seq)
    +        except asyncio.CancelledError:
    +            raise
    +        except Exception as exc:
    +            return SnapshotFailed(str(exc))
    +
    +    async def close(self) -> None:
    +        self._accepting = False
    +        task = self._active_job
    +        if task is not None:
    +            await asyncio.shield(task)
    +            if self._active_job is task:
    +                self._active_job = None
    ```

**是什么，为什么现在需要**

该模块拥有一个 Shielded Snapshot Job 与 Atomic POSIX File Publication。

**在运行时做什么**

它把 Overlap 拒绝为 Busy，通过 Async Callback Capture，Off-loop Write，并在 Failure 时保留最后 Good Destination。

**关键代码**

```python
os.replace(temporary, destination)
directory_fd = os.open(destination.parent, os.O_RDONLY)
try:
    os.fsync(directory_fd)
finally:
    os.close(directory_fd)
```

**关键语句理解**

Rename 发布 Complete File；Directory Fsync 使新 Directory Entry 跨 Crash 耐久。

#### 一致 Capture 与 Restore

在一个 Executor Control Turn 中 Capture Checkpoint Sequence 与逻辑存活 Entry，并恢复不带 Access-policy History 的深度冻结状态。

??? note "文件差异：src/miniredis/core/database.py"
    ```diff
    diff --git a/src/miniredis/core/database.py b/src/miniredis/core/database.py
    index 707b25de8cd248e89bd029c296dd18be41c3e2e4..737ca58bb64865f171cb4d257ed63bbe64c61ff2 100644
    --- a/src/miniredis/core/database.py
    +++ b/src/miniredis/core/database.py
    @@ -191,3 +191,47 @@ class Database:
                 checkpoint_seq=self.commit_seq,
                 entries=self.export_stored_entries(now_ms),
             )
    +
    +    def install_snapshot(
    +        self,
    +        image: SnapshotImage,
    +        *,
    +        now_ms: int,
    +    ) -> None:
    +        staged_entries: dict[bytes, Entry] = {}
    +        staged_usage = 0
    +        for key, stored in image.entries:
    +            if (
    +                stored.expire_at_ms is not None
    +                and stored.expire_at_ms <= now_ms
    +            ):
    +                continue
    +            value = thaw_value(stored.value)
    +            size = logical_entry_size(key, value, stored.expire_at_ms)
    +            staged_entries[key] = Entry(
    +                value=value,
    +                expire_at_ms=stored.expire_at_ms,
    +                mutation_version=stored.mutation_version,
    +                last_access_tick=0,
    +                logical_size=size,
    +            )
    +            staged_usage += size
    +
    +        self.entries = staged_entries
    +        self.logical_usage = staged_usage
    +        self.commit_seq = image.checkpoint_seq
    +        self.access_tick = 0
    +
    +    def discard_expired_for_recovery(self, now_ms: int) -> None:
    +        expired = tuple(
    +            key
    +            for key, entry in self.entries.items()
    +            if entry.expire_at_ms is not None
    +            and entry.expire_at_ms <= now_ms
    +        )
    +        for key in expired:
    +            entry = self.entries.pop(key)
    +            self.logical_usage -= entry.logical_size
    +        self.access_tick = 0
    +        for entry in self.entries.values():
    +            entry.last_access_tick = 0
    ```

**是什么，为什么现在需要**

Database 增加 Checkpoint Installation 与只导出 Logically Live Entry 的 Snapshot Export。

**在运行时做什么**

它以零 Access Tick 恢复 Frozen Value，并与被移除 Expired Entry 无关地保留 Checkpoint Sequence。

**关键代码**

```python
return SnapshotImage(
    checkpoint_seq=self.commit_seq,
    entries=self.export_stored_entries(now_ms),
)
```

**关键语句理解**

Sequence 描述 Durable History Position；Entry 描述 Capture Time 的 Live Logical State。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index aa9d8821c2807fdf5aabe8b51b2da141fa120d6a..0126d8027bf47cfb54cda842f29eaef2de67d0d0 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -30,6 +30,7 @@ from miniredis.core.commit import (
         CommitTrigger,
         PreparedCommit,
         PutEntry,
    +    SnapshotImage,
         StoredList,
     )
     from miniredis.core.database import Database
    @@ -99,6 +100,11 @@ class BeginShutdown:
         completion: asyncio.Future[None]


    +@dataclass(slots=True)
    +class SnapshotBarrier:
    +    future: asyncio.Future[SnapshotImage]
    +
    +
     @dataclass(frozen=True, slots=True)
     class ExecutionPlan:
         reply: Reply | None
    @@ -340,6 +346,10 @@ class CommandExecutor:
                 self._close_session(message)
             elif isinstance(message, BeginShutdown):
                 self._begin_shutdown(message)
    +        elif isinstance(message, SnapshotBarrier):
    +            image = self.database.snapshot_image(self.clock.now_ms())
    +            if not message.future.done():
    +                message.future.set_result(image)
             else:
                 raise AssertionError(f"unknown executor message: {message!r}")

    @@ -623,6 +633,14 @@ class CommandExecutor:
                 return 0
             return await asyncio.shield(future)

    +    async def capture_snapshot(self) -> SnapshotImage:
    +        future: asyncio.Future[SnapshotImage] = (
    +            asyncio.get_running_loop().create_future()
    +        )
    +        if not self.post_control(SnapshotBarrier(future)):
    +            raise RuntimeError("executor control admission is closed")
    +        return await asyncio.shield(future)
    +
         async def _active_expire_once(self, now_ms: int) -> int:
             keys = sorted(
                 key
    ```

**是什么，为什么现在需要**

Executor 接收 Snapshot-capture Control Message。

**在运行时做什么**

它在一个 Ordered Turn 读 Clock/Database，返回 Frozen Image，并在 File I/O 前恢复 User Event。

**关键代码**

```python
image = self.database.snapshot_image(self.clock.now_ms())
if not message.future.done():
    message.future.set_result(image)
```

**关键语句理解**

Checkpoint Sequence 与 Entry 在无 Interleaving Commit 时被观察。

#### Recovery-first Runtime 启动

校验 Persistence Path，在接受命令前恢复，接线 Snapshot 所有权，并把 Save 暴露为 Runtime Lifecycle Operation。

??? note "文件差异：src/miniredis/config.py"
    ```diff
    diff --git a/src/miniredis/config.py b/src/miniredis/config.py
    index 75c9725bae3faeb5afcc2c9160345d96e0b48e78..aac0e9f7ec7ad70e7234688fb8e877e46b571a48 100644
    --- a/src/miniredis/config.py
    +++ b/src/miniredis/config.py
    @@ -22,6 +22,7 @@ class MiniRedisConfig:
         aof_policy: AofPolicy = AofPolicy.EVERYSEC
         aof_repair_truncated_tail: bool = True
         aof_fsync_interval_seconds: float = 1.0
    +    snapshot_path: Path | None = None

         def __post_init__(self) -> None:
             if self.max_pending_commands <= 0:
    ```

**是什么，为什么现在需要**

Config 增加 Snapshot Path 并校验 Persistence Path Relationship。

**在运行时做什么**

它使 Recovery Input 显式，并防止 Temporary/Publication Path 别名不安全位置。

**关键代码**

```python
snapshot_path: Path | None = None
```

**关键语句理解**

Snapshot 可选；Recovery 仍支持 AOF-only 与 Empty Configuration。

??? note "文件差异：src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 22bec47b5dbf5ccde87bf3f00e4a32815a13a813..78b33aea879a2825b1c9b8c93a7c711161386226 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -40,6 +40,12 @@ from miniredis.core.outbound import (
     from miniredis.core.planner import CommandPlanner
     from miniredis.core.reply import Failure
     from miniredis.persistence.aof import AofWriter
    +from miniredis.persistence.snapshot import (
    +    SnapshotFailed,
    +    SnapshotFileOps,
    +    SnapshotManager,
    +    SnapshotOutcome,
    +)


     class RuntimeState(str, Enum):
    @@ -64,6 +70,7 @@ class RuntimeStats:
     @dataclass(slots=True)
     class _RuntimeTestHooks:
         aof_appender: CommitBarrier | None = None
    +    snapshot_ops: SnapshotFileOps | None = None


     def _direct_transport_close(_reason: str) -> None:
    @@ -108,6 +115,19 @@ class MiniRedis:
                 on_terminal_failure=self._on_executor_terminal_failure,
                 on_fatal=self._transition_failed,
             )
    +        self._snapshot_manager = (
    +            SnapshotManager(
    +                config.snapshot_path,
    +                self.executor.capture_snapshot,
    +                ops=(
    +                    None
    +                    if self._test_hooks is None
    +                    else self._test_hooks.snapshot_ops
    +                ),
    +            )
    +            if config.snapshot_path is not None
    +            else None
    +        )
             self.state = RuntimeState.STARTING
             self._session_ids = itertools.count(1)
             self._start_task: asyncio.Task[None] | None = None
    @@ -396,6 +416,13 @@ class MiniRedis:
                 return 0
             return await self.executor.active_expire_once()

    +    async def save_snapshot(self) -> SnapshotOutcome:
    +        if self.state is not RuntimeState.RUNNING:
    +            return SnapshotFailed("runtime is not running")
    +        if self._snapshot_manager is None:
    +            return SnapshotFailed("snapshot_path is not configured")
    +        return await self._snapshot_manager.save()
    +
         def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
             return self.executor.debug_applied_batches()

    ```

**是什么，为什么现在需要**

Runtime 在 Admission 前 Recovery，拥有 SnapshotManager，并把 Save 暴露为 Lifecycle Operation。

**在运行时做什么**

它安装 Recovered Database State，从 Recovered Sequence 启动 Durability，监督 Job，并与其他 Owned Resource 一起关 Manager。

**关键代码**

```python
async def save_snapshot(self) -> SnapshotOutcome:
    if self.state is not RuntimeState.RUNNING:
        return SnapshotFailed("runtime is not running")
```

**关键语句理解**

Snapshot Capture 在非 Running State 被拒绝，因此不与 Startup Recovery 或 Terminal Cleanup 竞争。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/16-snapshot-recovery/tests.txt)`。它覆盖 Manager Ownership、Capture Ordering、Atomic Publication、全部 Recovery Combination、Expiry 与 Corruption/Gap Rejection。

### 需要真正记住的内容

同时 Capture Sequence/State；Disk Write 前释放 Executor；拥有并 Shield 单 Save；用 Temp/Fsync/Rename/Dir-fsync 发布；Admission 前 Recovery；只 Replay Contiguous Suffix；不把 Corruption 变成 Empty State。

### 用自己的话讲清楚

Snapshot 是 Commit Stream 上的 Frozen Cut，不是围绕 File Write 的长 Lock。Executor 快速生成 Cut，另一 Owned Job 安全发布，Startup 只接受从该 Cut 精确继续的 AOF History。

### 教材

[第 7 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/07-snapshots-recovery.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/b03560a...b267f92)

完成后可运行 `python -m journey.tools.build_journey check 16` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/16-snapshot-recovery/stage.patch)

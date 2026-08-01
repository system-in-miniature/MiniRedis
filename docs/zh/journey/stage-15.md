# Stage 15 · AOF 提交屏障

### 目标

把已配置 AOF Ack 变成内存 Apply、Reply、Waiter Wakeup 或后续 Replication 前的 Gate。

??? note "交付文件"
    - `src/miniredis/config.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/persistence/aof.py`
    - `src/miniredis/runtime.py`
    - `tests/helpers/runtime.py`
    - `tests/reliability/test_commit_barrier.py`
    - `tests/unit/persistence/test_aof_writer.py`

### 当前遇到的问题

Frame 已可编码，但 Runtime 还需一个 Durability Linearization Point。如果 Memory 或 Reply 在 Append Ack 前前进，Crash 或 Disk Failure 会暴露 Recovery 无法重建的 Success。Background Writer Failure 也必须收束当前与未来 Append Waiter，而不是挂起。

### 测试契约

#### 先看会坏在哪里

Append 被 Gate 时，Database Sequence 与 Value 必须保持旧值，Reply 必须 Pending，后续 State Event 不得超车。Append Failure 必须什么都不 Apply，返回 Durability Error，把 Runtime 转为 Failed，并拒绝后续命令。

??? note "文件差异：tests/helpers/runtime.py"
    ```diff
    diff --git a/tests/helpers/runtime.py b/tests/helpers/runtime.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..b19bd4760addf6e03e8d0ab3bed2e8e144dc8df1
    --- /dev/null
    +++ b/tests/helpers/runtime.py
    @@ -0,0 +1,22 @@
    +from miniredis.runtime import MiniRedis, _RuntimeTestHooks
    +
    +
    +class TestMiniRedis(MiniRedis):
    +    pass
    +
    +
    +async def open_test_runtime(
    +    *,
    +    clock=None,
    +    scheduler=None,
    +    aof_appender=None,
    +    config=None,
    +) -> TestMiniRedis:
    +    runtime = TestMiniRedis._for_test(
    +        config=config,
    +        clock=clock,
    +        scheduler=scheduler,
    +        test_hooks=_RuntimeTestHooks(aof_appender=aof_appender),
    +    )
    +    await runtime.start()
    +    return runtime
    ```

**测试锁定什么**

Helper 通过 Private Runtime Hook 注入 Barrier，但保留正常 Executor 与 Lifecycle Path。

**如何构造反例**

它只为 Typed Test Access 子类化，并使用 `_RuntimeTestHooks(aof_appender=...)` 启动同一 Runtime。

**关键测试语句**

```python
test_hooks=_RuntimeTestHooks(aof_appender=aof_appender)
```

**失败意味着什么**

Durability Test 不再证明 Production Ownership Path，可能只在运行另一套 Fake Runtime。

??? note "文件差异：tests/reliability/test_commit_barrier.py"
    ```diff
    diff --git a/tests/reliability/test_commit_barrier.py b/tests/reliability/test_commit_barrier.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..0a9256d6198cb874b9741a2bdccee01d389f7649
    --- /dev/null
    +++ b/tests/reliability/test_commit_barrier.py
    @@ -0,0 +1,120 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis.commands.request import CommandRequest
    +from miniredis.core.reply import Failure, Ok
    +from miniredis.persistence.aof import AofAppendFailed, AofAppendOk
    +from miniredis.runtime import RuntimeState
    +from tests.helpers.runtime import open_test_runtime
    +
    +
    +class GateAofWriter:
    +    def __init__(self) -> None:
    +        self.entered = asyncio.Event()
    +        self.release = asyncio.Event()
    +        self.batches = []
    +        self.failure: str | None = None
    +
    +    async def append(self, batch):
    +        self.batches.append(batch)
    +        self.entered.set()
    +        await self.release.wait()
    +        if self.failure is not None:
    +            return AofAppendFailed(self.failure)
    +        return AofAppendOk(batch.seq)
    +
    +
    +@pytest.mark.asyncio
    +async def test_state_and_reply_wait_behind_the_aof_barrier():
    +    writer = GateAofWriter()
    +    runtime = await open_test_runtime(aof_appender=writer)
    +    client = runtime.direct_client()
    +
    +    pending = asyncio.create_task(
    +        client.execute(CommandRequest(b"SET", (b"k", b"v")))
    +    )
    +    await writer.entered.wait()
    +
    +    assert runtime.database.commit_seq == 0
    +    assert b"k" not in runtime.database.entries
    +    assert not pending.done()
    +    assert writer.batches[0].seq == 1
    +
    +    writer.release.set()
    +    assert await pending == Ok()
    +    assert runtime.database.commit_seq == 1
    +    assert runtime.database.entries[b"k"].value.data == b"v"
    +    await runtime.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_executor_processes_no_later_state_event_during_barrier():
    +    writer = GateAofWriter()
    +    runtime = await open_test_runtime(aof_appender=writer)
    +    client = runtime.direct_client()
    +
    +    first = asyncio.create_task(
    +        client.execute(CommandRequest(b"SET", (b"a", b"1")))
    +    )
    +    await writer.entered.wait()
    +    second = asyncio.create_task(
    +        client.execute(CommandRequest(b"SET", (b"b", b"2")))
    +    )
    +    await runtime.debug_wait_until_queued(1)
    +
    +    assert len(writer.batches) == 1
    +    assert not second.done()
    +
    +    writer.release.set()
    +    assert await first == Ok()
    +    assert await second == Ok()
    +    assert [item.seq for item in writer.batches] == [1, 2]
    +    await runtime.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_append_failure_does_not_apply_and_fails_the_runtime():
    +    writer = GateAofWriter()
    +    writer.failure = "disk full"
    +    runtime = await open_test_runtime(aof_appender=writer)
    +    client = runtime.direct_client()
    +
    +    pending = asyncio.create_task(
    +        client.execute(CommandRequest(b"SET", (b"k", b"v")))
    +    )
    +    await writer.entered.wait()
    +    writer.release.set()
    +
    +    reply = await pending
    +    assert isinstance(reply, Failure)
    +    assert reply.code == "ERR"
    +    assert "durability failure" in reply.message
    +    assert runtime.state is RuntimeState.FAILED
    +    assert runtime.database.commit_seq == 0
    +    assert b"k" not in runtime.database.entries
    +
    +    rejected = await client.execute(
    +        CommandRequest(b"SET", (b"later", b"x"))
    +    )
    +    assert isinstance(rejected, Failure)
    +    assert rejected.code == "CLOSED"
    +    await runtime.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_ordinary_error_and_noop_never_call_aof():
    +    writer = GateAofWriter()
    +    runtime = await open_test_runtime(aof_appender=writer)
    +    client = runtime.direct_client()
    +
    +    wrong_arity = await client.execute(CommandRequest(b"SET", (b"k",)))
    +    missing_delete = await client.execute(
    +        CommandRequest(b"DEL", (b"missing",))
    +    )
    +
    +    assert isinstance(wrong_arity, Failure)
    +    assert missing_delete.value == 0
    +    assert writer.batches == []
    +    assert runtime.database.commit_seq == 0
    +    await runtime.close()
    ```

**测试锁定什么**

它锁定 Append-before-apply/reply、Barrier 期间无后续 State Event、Fatal Append Failure、精确 Sequence Ack、以及 Error/No-op 不调 AOF。

**如何构造反例**

Gated Fake Appender 记录 Batch 并扣住 Ack，测试此时检查 Memory、Reply Completion、Queued Command 与 Failure State。

**关键测试语句**

```python
assert runtime.database.commit_seq == 0
assert b"k" not in runtime.database.entries
assert not pending.done()
```

**失败意味着什么**

Visibility 移到 Durability 前，Executor Ordering 跨过 Barrier，或 Failed/No-op Work 消耗 Durable Sequence。

??? note "文件差异：tests/unit/persistence/test_aof_writer.py"
    ```diff
    diff --git a/tests/unit/persistence/test_aof_writer.py b/tests/unit/persistence/test_aof_writer.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..64469f939c14cc12bb483f0742d1b8d11bea08fe
    --- /dev/null
    +++ b/tests/unit/persistence/test_aof_writer.py
    @@ -0,0 +1,223 @@
    +import asyncio
    +import os
    +import threading
    +
    +import pytest
    +
    +from miniredis.persistence.aof import (
    +    AofAppendFailed,
    +    AofAppendOk,
    +    AofPolicy,
    +    AofWriter,
    +    PosixAofFileOps,
    +)
    +from tests.unit.persistence.test_framing import batch
    +
    +
    +class ManualSleep:
    +    def __init__(self) -> None:
    +        self.entered = asyncio.Event()
    +        self.release = asyncio.Event()
    +
    +    async def __call__(self, _delay: float) -> None:
    +        self.entered.set()
    +        await self.release.wait()
    +        self.release.clear()
    +        self.entered.clear()
    +
    +
    +@pytest.mark.asyncio
    +async def test_always_acknowledges_only_after_record_fsync(
    +    tmp_path,
    +    monkeypatch,
    +):
    +    fsync_calls: list[int] = []
    +    monkeypatch.setattr(os, "fsync", lambda fd: fsync_calls.append(fd))
    +    writer = AofWriter(tmp_path / "appendonly.mraof", AofPolicy.ALWAYS)
    +    await writer.start()
    +    fsync_calls.clear()
    +
    +    outcome = await writer.append(batch(1))
    +
    +    assert outcome == AofAppendOk(1)
    +    assert len(fsync_calls) == 1
    +    await writer.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_everysec_acknowledges_after_write_then_owned_loop_fsyncs(
    +    tmp_path,
    +    monkeypatch,
    +):
    +    sleep = ManualSleep()
    +    loop = asyncio.get_running_loop()
    +    fsync_seen = asyncio.Event()
    +    fsync_calls: list[int] = []
    +
    +    def record_fsync(fd: int) -> None:
    +        fsync_calls.append(fd)
    +        loop.call_soon_threadsafe(fsync_seen.set)
    +
    +    monkeypatch.setattr(os, "fsync", record_fsync)
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.EVERYSEC,
    +        sleep=sleep,
    +    )
    +    await writer.start()
    +    await sleep.entered.wait()
    +    fsync_calls.clear()
    +    fsync_seen.clear()
    +
    +    assert await writer.append(batch(1)) == AofAppendOk(1)
    +    assert fsync_calls == []
    +
    +    sleep.release.set()
    +    await fsync_seen.wait()
    +    assert len(fsync_calls) == 1
    +    await writer.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_no_policy_never_fsyncs_records_or_graceful_close(
    +    tmp_path,
    +    monkeypatch,
    +):
    +    fsync_calls: list[int] = []
    +    monkeypatch.setattr(os, "fsync", lambda fd: fsync_calls.append(fd))
    +    writer = AofWriter(tmp_path / "appendonly.mraof", AofPolicy.NO)
    +    await writer.start()
    +    fsync_calls.clear()
    +
    +    assert await writer.append(batch(1)) == AofAppendOk(1)
    +    await writer.close()
    +
    +    assert fsync_calls == []
    +
    +
    +class FailingWriteOps(PosixAofFileOps):
    +    def __init__(self) -> None:
    +        self.fail_records = False
    +
    +    def write_all(self, fd: int, data: bytes) -> None:
    +        if self.fail_records:
    +            raise OSError("disk full")
    +        super().write_all(fd, data)
    +
    +
    +@pytest.mark.asyncio
    +async def test_worker_failure_settles_current_and_future_barriers(tmp_path):
    +    failures: list[BaseException] = []
    +    ops = FailingWriteOps()
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.NO,
    +        ops=ops,
    +        on_failure=failures.append,
    +    )
    +    await writer.start()
    +    ops.fail_records = True
    +
    +    first = await writer.append(batch(1))
    +    second = await writer.append(batch(2))
    +
    +    assert isinstance(first, AofAppendFailed)
    +    assert first.message == "disk full"
    +    assert isinstance(second, AofAppendFailed)
    +    assert len(failures) == 1
    +    await writer.close()
    +
    +
    +class FailingFsyncOps(PosixAofFileOps):
    +    def __init__(self) -> None:
    +        self.fail_fsync = False
    +
    +    def fsync(self, fd: int) -> None:
    +        if self.fail_fsync:
    +            raise OSError("fsync failed")
    +        super().fsync(fd)
    +
    +
    +@pytest.mark.asyncio
    +async def test_everysec_background_fsync_failure_is_supervised(tmp_path):
    +    sleep = ManualSleep()
    +    failure_seen = asyncio.Event()
    +    failures: list[BaseException] = []
    +    ops = FailingFsyncOps()
    +
    +    def record_failure(error: BaseException) -> None:
    +        failures.append(error)
    +        failure_seen.set()
    +
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.EVERYSEC,
    +        ops=ops,
    +        sleep=sleep,
    +        on_failure=record_failure,
    +    )
    +    await writer.start()
    +    await sleep.entered.wait()
    +    assert await writer.append(batch(1)) == AofAppendOk(1)
    +    ops.fail_fsync = True
    +
    +    sleep.release.set()
    +    await failure_seen.wait()
    +    assert len(failures) == 1
    +
    +    later = await writer.append(batch(2))
    +    assert isinstance(later, AofAppendFailed)
    +    assert later.message == "fsync failed"
    +    await writer.close()
    +
    +
    +class ConcurrentWriteAndFsyncFailureOps(PosixAofFileOps):
    +    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
    +        self._loop = loop
    +        self.block_record = False
    +        self.fail_fsync = False
    +        self.write_entered = asyncio.Event()
    +        self.release_write = threading.Event()
    +
    +    def write_all(self, fd: int, data: bytes) -> None:
    +        if self.block_record:
    +            self._loop.call_soon_threadsafe(self.write_entered.set)
    +            self.release_write.wait()
    +        super().write_all(fd, data)
    +
    +    def fsync(self, fd: int) -> None:
    +        if self.fail_fsync:
    +            raise OSError("background fsync failed")
    +        super().fsync(fd)
    +
    +
    +@pytest.mark.asyncio
    +async def test_background_fsync_failure_fails_a_concurrent_append(tmp_path):
    +    loop = asyncio.get_running_loop()
    +    sleep = ManualSleep()
    +    failure_seen = asyncio.Event()
    +    ops = ConcurrentWriteAndFsyncFailureOps(loop)
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.EVERYSEC,
    +        ops=ops,
    +        sleep=sleep,
    +        on_failure=lambda _error: failure_seen.set(),
    +    )
    +    await writer.start()
    +    await sleep.entered.wait()
    +    assert await writer.append(batch(1)) == AofAppendOk(1)
    +
    +    ops.block_record = True
    +    current = asyncio.create_task(writer.append(batch(2)))
    +    await ops.write_entered.wait()
    +    ops.fail_fsync = True
    +    sleep.release.set()
    +    await failure_seen.wait()
    +    ops.release_write.set()
    +
    +    outcome = await current
    +    assert isinstance(outcome, AofAppendFailed)
    +    assert outcome.message == "background fsync failed"
    +    assert isinstance(await writer.append(batch(3)), AofAppendFailed)
    +    await writer.close()
    ```

**测试锁定什么**

它锁定 ALWAYS/EVERYSEC/NO Ack Point、Header Durability、Complete Write、Owned Periodic Fsync、Current/Future Failure Settlement 与单次 Failure Notification。

**如何构造反例**

它注入 Manual Sleep 与会使 Write/Fsync 失败的 File Operation，包括 Concurrent Record Write + Background Fsync Failure。

**关键测试语句**

```python
assert outcome == AofAppendOk(1)
```

**失败意味着什么**

Policy 在错误 Durability Point Ack，Owned Loop 逃离 Supervision，或 Append Future 在 Failure 后未解决。

### 基本概念

Commit Barrier 是 Proposed State 到 Visible State 的迁移。`ALWAYS` 在 Record Fsync 后 Ack，`EVERYSEC` 在 Complete Write 后 Ack，Owned Loop 后续 Fsync，`NO` 在 Write 后 Ack 且不做 Record Fsync。全部 Policy 仍要求 Complete Append 与 Exact Sequence Ack。

### 为什么需要这个机制

Reply、Memory、Blocked-waiter Wakeup 与 Replication 必须只描述已被配置 Durability Contract 接受的 History。在单 Executor 内等待保留 Global Order。Fatal Disk Loss 必须 Fail Closed，因为继续会创建无法恢复的 Memory History。

### 运行时心智模型

Executor 把 Prepared Commit 变成 Next Batch 并 Await `commit_barrier.append`。Writer 串行 Encoded Record，在 Policy Point 返回 `AofAppendOk(seq)`。只有精确 Ack 允许 Database Apply 与下游 Effect。任何 Failure 都收束 Barrier、只通知 Supervision 一次，并通过已有 Failure Path 迁移 Runtime Shutdown。

### 机制板块

#### Owned AOF Writer 与 Fsync Policy

串行 Framed Append，在配置 Durability Point 确认，并在 Write 或 Fsync 失败后收束每个 Barrier。

??? note "文件差异：src/miniredis/persistence/aof.py"
    ```diff
    diff --git a/src/miniredis/persistence/aof.py b/src/miniredis/persistence/aof.py
    index 32808fb70d31265dccf90ed8bc62b4ef33598b8c..19d004cb72aa9dfb47c0577cf3097f38d14e795d 100644
    --- a/src/miniredis/persistence/aof.py
    +++ b/src/miniredis/persistence/aof.py
    @@ -1,16 +1,95 @@
     from __future__ import annotations

    +import asyncio
     import os
    +from collections.abc import Awaitable, Callable
    +from dataclasses import dataclass
    +from enum import StrEnum
     from pathlib import Path
    +from typing import Protocol, TypeAlias

     from miniredis.core.commit import CommitBatch
    -from miniredis.persistence.codec import CodecError, scan_aof_bytes
    +from miniredis.persistence.codec import (
    +    AOF_HEADER,
    +    CodecError,
    +    encode_aof_record,
    +    scan_aof_bytes,
    +)


     class AofCorruption(RuntimeError):
         pass


    +class AofPolicy(StrEnum):
    +    ALWAYS = "always"
    +    EVERYSEC = "everysec"
    +    NO = "no"
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class AofAppendOk:
    +    seq: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class AofAppendFailed:
    +    message: str
    +
    +
    +AofAppendOutcome: TypeAlias = AofAppendOk | AofAppendFailed
    +
    +
    +class AofFileOps(Protocol):
    +    def open_append(self, path: Path) -> int:
    +        raise NotImplementedError
    +
    +    def size(self, fd: int) -> int:
    +        raise NotImplementedError
    +
    +    def read_header(self, fd: int) -> bytes:
    +        raise NotImplementedError
    +
    +    def write_all(self, fd: int, data: bytes) -> None:
    +        raise NotImplementedError
    +
    +    def fsync(self, fd: int) -> None:
    +        raise NotImplementedError
    +
    +    def close(self, fd: int) -> None:
    +        raise NotImplementedError
    +
    +
    +class PosixAofFileOps:
    +    def open_append(self, path: Path) -> int:
    +        path.parent.mkdir(parents=True, exist_ok=True)
    +        return os.open(
    +            path,
    +            os.O_CREAT | os.O_RDWR | os.O_APPEND,
    +            0o600,
    +        )
    +
    +    def size(self, fd: int) -> int:
    +        return os.fstat(fd).st_size
    +
    +    def read_header(self, fd: int) -> bytes:
    +        return os.pread(fd, len(AOF_HEADER), 0)
    +
    +    def write_all(self, fd: int, data: bytes) -> None:
    +        view = memoryview(data)
    +        while view:
    +            written = os.write(fd, view)
    +            if written <= 0:
    +                raise OSError("AOF write made no progress")
    +            view = view[written:]
    +
    +    def fsync(self, fd: int) -> None:
    +        os.fsync(fd)
    +
    +    def close(self, fd: int) -> None:
    +        os.close(fd)
    +
    +
     def load_aof(
         path: Path,
         *,
    @@ -38,3 +117,250 @@ def load_aof(
         finally:
             os.close(fd)
         return scan.batches
    +
    +
    +@dataclass(slots=True)
    +class _AppendWork:
    +    record: bytes
    +    seq: int
    +    barrier: asyncio.Future[AofAppendOutcome]
    +
    +
    +_STOP = object()
    +
    +
    +class AofWriter:
    +    def __init__(
    +        self,
    +        path: Path,
    +        policy: AofPolicy,
    +        *,
    +        fsync_interval_seconds: float = 1.0,
    +        ops: AofFileOps | None = None,
    +        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    +        on_failure: Callable[[BaseException], None] | None = None,
    +    ) -> None:
    +        if fsync_interval_seconds <= 0:
    +            raise ValueError("fsync interval must be positive")
    +        self._path = path
    +        self._policy = policy
    +        self._interval = fsync_interval_seconds
    +        self._ops = ops or PosixAofFileOps()
    +        self._sleep = sleep
    +        self._on_failure = on_failure or (lambda _error: None)
    +        self._queue: asyncio.Queue[_AppendWork | object] = asyncio.Queue()
    +        self._fd: int | None = None
    +        self._worker: asyncio.Task[None] | None = None
    +        self._sync_task: asyncio.Task[None] | None = None
    +        self._current_work: _AppendWork | None = None
    +        self._dirty = False
    +        self._sync_inflight = False
    +        self._accepting = False
    +        self._failure: BaseException | None = None
    +        self._failure_reported = False
    +
    +    @property
    +    def failure(self) -> BaseException | None:
    +        return self._failure
    +
    +    async def start(self) -> None:
    +        if self._worker is not None:
    +            return
    +        fd = await asyncio.to_thread(self._ops.open_append, self._path)
    +        try:
    +            size = await asyncio.to_thread(self._ops.size, fd)
    +            if size == 0:
    +                await asyncio.to_thread(
    +                    self._ops.write_all,
    +                    fd,
    +                    AOF_HEADER,
    +                )
    +                await asyncio.to_thread(self._ops.fsync, fd)
    +            else:
    +                header = await asyncio.to_thread(
    +                    self._ops.read_header,
    +                    fd,
    +                )
    +                if header != AOF_HEADER:
    +                    raise AofCorruption("invalid AOF header")
    +        except BaseException:
    +            await asyncio.to_thread(self._ops.close, fd)
    +            raise
    +        self._fd = fd
    +        self._accepting = True
    +        self._worker = asyncio.create_task(
    +            self._run_writer(),
    +            name="miniredis-aof-writer",
    +        )
    +        self._worker.add_done_callback(self._writer_done)
    +        if self._policy is AofPolicy.EVERYSEC:
    +            self._sync_task = asyncio.create_task(
    +                self._run_everysec(),
    +                name="miniredis-aof-fsync",
    +            )
    +
    +    async def append(self, batch: CommitBatch) -> AofAppendOutcome:
    +        if self._failure is not None:
    +            return AofAppendFailed(str(self._failure))
    +        if not self._accepting or self._worker is None:
    +            return AofAppendFailed("AOF writer is not accepting")
    +        barrier = asyncio.get_running_loop().create_future()
    +        self._queue.put_nowait(
    +            _AppendWork(
    +                record=encode_aof_record(batch),
    +                seq=batch.seq,
    +                barrier=barrier,
    +            )
    +        )
    +        return await asyncio.shield(barrier)
    +
    +    async def _run_writer(self) -> None:
    +        while True:
    +            item = await self._queue.get()
    +            if item is _STOP:
    +                return
    +            assert isinstance(item, _AppendWork)
    +            self._current_work = item
    +            if self._failure is not None:
    +                self._settle(
    +                    item.barrier,
    +                    AofAppendFailed(str(self._failure)),
    +                )
    +                self._current_work = None
    +                continue
    +            try:
    +                assert self._fd is not None
    +                await asyncio.to_thread(
    +                    self._ops.write_all,
    +                    self._fd,
    +                    item.record,
    +                )
    +                if self._failure is not None:
    +                    self._settle(
    +                        item.barrier,
    +                        AofAppendFailed(str(self._failure)),
    +                    )
    +                    self._fail_queued(self._failure)
    +                    self._current_work = None
    +                    return
    +                if self._policy is AofPolicy.ALWAYS:
    +                    await asyncio.to_thread(self._ops.fsync, self._fd)
    +                elif self._policy is AofPolicy.EVERYSEC:
    +                    self._dirty = True
    +            except BaseException as exc:
    +                self._settle(
    +                    item.barrier,
    +                    AofAppendFailed(str(exc)),
    +                )
    +                self._record_failure(exc)
    +                self._fail_queued(exc)
    +                self._current_work = None
    +                return
    +            self._settle(item.barrier, AofAppendOk(item.seq))
    +            self._current_work = None
    +
    +    def _writer_done(self, task: asyncio.Task[None]) -> None:
    +        if task.cancelled():
    +            error: BaseException | None = RuntimeError(
    +                "AOF writer task was cancelled"
    +            )
    +        else:
    +            error = task.exception()
    +        if error is None:
    +            return
    +        current = self._current_work
    +        self._current_work = None
    +        if current is not None:
    +            self._settle(
    +                current.barrier,
    +                AofAppendFailed(str(error)),
    +            )
    +        self._record_failure(error)
    +        self._fail_queued(error)
    +
    +    async def _run_everysec(self) -> None:
    +        try:
    +            while self._accepting and self._failure is None:
    +                await self._sleep(self._interval)
    +                await self._sync_dirty()
    +        except asyncio.CancelledError:
    +            raise
    +        except BaseException as exc:
    +            current = self._current_work
    +            if current is not None:
    +                self._settle(
    +                    current.barrier,
    +                    AofAppendFailed(str(exc)),
    +                )
    +            self._record_failure(exc)
    +            self._fail_queued(exc)
    +
    +    async def _sync_dirty(self) -> None:
    +        if not self._dirty or self._failure is not None:
    +            return
    +        self._dirty = False
    +        try:
    +            assert self._fd is not None
    +            self._sync_inflight = True
    +            await asyncio.to_thread(self._ops.fsync, self._fd)
    +        except BaseException:
    +            self._dirty = True
    +            raise
    +        finally:
    +            self._sync_inflight = False
    +
    +    @staticmethod
    +    def _settle(
    +        barrier: asyncio.Future[AofAppendOutcome],
    +        outcome: AofAppendOutcome,
    +    ) -> None:
    +        if not barrier.done():
    +            barrier.set_result(outcome)
    +
    +    def _record_failure(self, error: BaseException) -> None:
    +        if self._failure is None:
    +            self._failure = error
    +        self._accepting = False
    +        if not self._failure_reported:
    +            self._failure_reported = True
    +            self._on_failure(self._failure)
    +
    +    def _fail_queued(self, error: BaseException) -> None:
    +        while True:
    +            try:
    +                item = self._queue.get_nowait()
    +            except asyncio.QueueEmpty:
    +                return
    +            if isinstance(item, _AppendWork):
    +                self._settle(
    +                    item.barrier,
    +                    AofAppendFailed(str(error)),
    +                )
    +
    +    async def close(self) -> None:
    +        if self._fd is None:
    +            return
    +        self._accepting = False
    +        if self._worker is not None and not self._worker.done():
    +            self._queue.put_nowait(_STOP)
    +            await asyncio.shield(self._worker)
    +        if self._sync_task is not None:
    +            if self._sync_inflight:
    +                await asyncio.shield(self._sync_task)
    +            else:
    +                self._sync_task.cancel()
    +                try:
    +                    await self._sync_task
    +                except asyncio.CancelledError:
    +                    pass
    +        if (
    +            self._policy is AofPolicy.EVERYSEC
    +            and self._dirty
    +            and self._failure is None
    +        ):
    +            try:
    +                await self._sync_dirty()
    +            except BaseException as exc:
    +                self._record_failure(exc)
    +        fd, self._fd = self._fd, None
    +        await asyncio.to_thread(self._ops.close, fd)
    ```

**是什么，为什么现在需要**

AOF 模块增加 Owned Async Writer、Durability Policy、可注入 File Operation 与 Typed Append Outcome。

**在运行时做什么**

它初始化 Durable Header，经单 Queue 串行 Record，执行 Policy Fsync，并在 Success、Close 或 Failure 时收束每个 Barrier。

**关键代码**

```python
return await asyncio.shield(barrier)
```

**关键语句理解**

取消 Append Caller 不能取消 Writer-owned Completion，因为 Bytes 已准入 Queue。

#### Append-before-apply Commit Gate

在应用内存、Reply、唤醒 Waiter 或 Offer Replication 前，等待精确 Sequence Ack。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index d9ea4d458917aeb8443bb40130abd5fa537777ed..aa9d8821c2807fdf5aabe8b51b2da141fa120d6a 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -5,7 +5,7 @@ import itertools
     from bisect import bisect_right
     from collections.abc import Callable
     from dataclasses import dataclass, replace
    -from typing import Protocol
    +from typing import TYPE_CHECKING, Protocol

     from miniredis.clock import Clock, TimerScheduler
     from miniredis.commands.model import (
    @@ -52,6 +52,14 @@ from miniredis.core.outbound import (
     )
     from miniredis.core.pubsub import PubSubRegistry
     from miniredis.core.reply import Bytes, Failure, Items, Number, Reply
    +from miniredis.persistence.aof import (
    +    AofAppendFailed,
    +    AofAppendOk,
    +    AofAppendOutcome,
    +)
    +
    +if TYPE_CHECKING:
    +    from miniredis.replication.sink import ReplicaSink


     @dataclass(slots=True)
    @@ -107,12 +115,17 @@ class ExecutionPlan:


     class CommitBarrier(Protocol):
    -    async def append(self, batch: CommitBatch) -> None: ...
    +    async def append(self, batch: CommitBatch) -> AofAppendOutcome:
    +        raise NotImplementedError


     class NullCommitBarrier:
    -    async def append(self, batch: CommitBatch) -> None:
    -        del batch
    +    async def append(self, batch: CommitBatch) -> AofAppendOutcome:
    +        return AofAppendOk(batch.seq)
    +
    +
    +class DurabilityFailure(RuntimeError):
    +    pass


     class Planner(Protocol):
    @@ -150,6 +163,7 @@ class CommandExecutor:
             scheduler: TimerScheduler,
             on_debug_change: Callable[[], None],
             on_terminal_failure: Callable[[BaseException], None] | None = None,
    +        on_fatal: Callable[[str], None] | None = None,
         ) -> None:
             self.database = database
             self.planner = planner
    @@ -165,6 +179,7 @@ class CommandExecutor:
             )
             self._on_debug_change = on_debug_change
             self._on_terminal_failure = on_terminal_failure
    +        self._on_fatal = on_fatal or (lambda _reason: None)
             self.waiters = WaiterRegistry(self._on_debug_change)
             self.pubsub = PubSubRegistry(self._on_debug_change)
             self.scheduler = scheduler
    @@ -181,6 +196,7 @@ class CommandExecutor:
             self._endpoints: dict[int, SessionEndpoint] = {}
             self._accepted_changed = asyncio.Event()
             self._applied_batches: list[CommitBatch] = []
    +        self._replica_sinks: dict[int, ReplicaSink] = {}
             self._handling_message = False
             self._failure: BaseException | None = None
             self._terminal_cleanup_complete = False
    @@ -468,7 +484,6 @@ class CommandExecutor:
                     return
             else:
                 plan = self.planner.plan(command, self.database, now_ms)
    -            plan = self._attach_push_wakeups(command, plan)
             await self._apply_plan(request, plan, now_ms)

         def _subscribe(self, request: ExecuteRequest, command: Subscribe) -> None:
    @@ -547,18 +562,17 @@ class CommandExecutor:
             plan: ExecutionPlan,
             now_ms: int,
         ) -> None:
    -        if plan.operations:
    -            batch = CommitBatch(
    -                self.database.commit_seq + 1,
    -                plan.operations,
    -                plan.trigger,
    -            )
    -            await self.commit_barrier.append(batch)
    -            self.database.apply_batch(
    -                batch,
    -                track_access=plan.trigger is CommitTrigger.CLIENT,
    +        plan = self._attach_push_wakeups(request.command, plan)
    +        try:
    +            if plan.prepared_commit is not None:
    +                await self._commit_prepared(plan.prepared_commit)
    +        except DurabilityFailure as exc:
    +            self._finish_reply(
    +                request.token,
    +                Failure("ERR", f"durability failure: {exc}"),
                 )
    -            self._applied_batches.append(batch)
    +            self._on_fatal(str(exc))
    +            return

             for key in dict.fromkeys(plan.touch_keys):
                 self.database.touch_if_live(key, now_ms)
    @@ -576,6 +590,30 @@ class CommandExecutor:
                     )
             self._finish_reply(request.token, plan.reply)

    +    async def _commit_prepared(
    +        self,
    +        prepared: PreparedCommit,
    +    ) -> CommitBatch:
    +        batch = prepared.to_batch(self.database.commit_seq + 1)
    +        outcome = await self.commit_barrier.append(batch)
    +        if isinstance(outcome, AofAppendFailed):
    +            raise DurabilityFailure(outcome.message)
    +        if outcome != AofAppendOk(batch.seq):
    +            raise DurabilityFailure("AOF acknowledged the wrong sequence")
    +
    +        self.database.apply_batch(
    +            batch,
    +            track_access=prepared.trigger is CommitTrigger.CLIENT,
    +        )
    +        self._applied_batches.append(batch)
    +        self._offer_replica_batch(batch)
    +        return batch
    +
    +    def _offer_replica_batch(self, batch: CommitBatch) -> None:
    +        for generation, sink in tuple(self._replica_sinks.items()):
    +            if not sink.offer(batch):
    +                self._replica_sinks.pop(generation, None)
    +
         async def active_expire_once(self) -> int:
             if self._worker_task is None or self._stopping:
                 return 0
    @@ -609,14 +647,15 @@ class CommandExecutor:
             )
             if not operations:
                 return 0
    -        batch = CommitBatch(
    -            self.database.commit_seq + 1,
    -            operations,
    -            CommitTrigger.ACTIVE_EXPIRE,
    -        )
    -        await self.commit_barrier.append(batch)
    -        self.database.apply_batch(batch, track_access=False)
    -        self._applied_batches.append(batch)
    +        try:
    +            prepared = PreparedCommit(
    +                operations,
    +                CommitTrigger.ACTIVE_EXPIRE,
    +            )
    +            await self._commit_prepared(prepared)
    +        except DurabilityFailure as exc:
    +            self._on_fatal(str(exc))
    +            return 0
             return len(operations)

         async def close(self) -> None:
    ```

**是什么，为什么现在需要**

Executor 把所有 State-changing Path 集中到 `_commit_prepared`。

**在运行时做什么**

它分配 Sequence，等 Exact AOF Ack，Apply Memory，记录 Batch，再 Offer 后续 Consumer。

**关键代码**

```python
outcome = await self.commit_barrier.append(batch)
if isinstance(outcome, AofAppendFailed):
    raise DurabilityFailure(outcome.message)
```

**关键语句理解**

Failure Branch 不执行 Database Apply；Durability Error 留在 Visibility Linearization Point 前。

#### AOF 配置与监督

从 Runtime Config 构造并监督 Writer，仅通过 Test Hook 注入，并在 Durability 丢失时 Fail Closed。

??? note "文件差异：src/miniredis/config.py"
    ```diff
    diff --git a/src/miniredis/config.py b/src/miniredis/config.py
    index 09c31aa8115c72d0213d6ddfa28bbb2b7f81ca77..75c9725bae3faeb5afcc2c9160345d96e0b48e78 100644
    --- a/src/miniredis/config.py
    +++ b/src/miniredis/config.py
    @@ -1,8 +1,11 @@
     from __future__ import annotations

     from dataclasses import dataclass
    +from pathlib import Path
     from typing import Literal

    +from miniredis.persistence.aof import AofPolicy
    +
     EvictionPolicy = Literal["noeviction", "allkeys-lru"]


    @@ -15,6 +18,10 @@ class MiniRedisConfig:
         outbox_limit: int = 64
         outbox_drain_grace_ms: int = 100
         active_expire_interval_ms: int = 100
    +    aof_path: Path | None = None
    +    aof_policy: AofPolicy = AofPolicy.EVERYSEC
    +    aof_repair_truncated_tail: bool = True
    +    aof_fsync_interval_seconds: float = 1.0

         def __post_init__(self) -> None:
             if self.max_pending_commands <= 0:
    @@ -31,3 +38,5 @@ class MiniRedisConfig:
                 raise ValueError("outbox_drain_grace_ms cannot be negative")
             if self.active_expire_interval_ms <= 0:
                 raise ValueError("active_expire_interval_ms must be positive")
    +        if self.aof_fsync_interval_seconds <= 0:
    +            raise ValueError("aof_fsync_interval_seconds must be positive")
    ```

**是什么，为什么现在需要**

Config 声明 AOF Path、Policy、Tail Repair 与 Fsync Interval。

**在运行时做什么**

它选择是否启用 Persistence 以及使用哪种 Ack Contract。

**关键代码**

```python
if self.aof_fsync_interval_seconds <= 0:
    raise ValueError("aof_fsync_interval_seconds must be positive")
```

**关键语句理解**

Periodic Policy 需要正 Owned Cadence，在构造时拒绝不可能 Loop。

??? note "文件差异：src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index e741b2d63038aec217503fcc05bea43bec434365..22bec47b5dbf5ccde87bf3f00e4a32815a13a813 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -39,6 +39,7 @@ from miniredis.core.outbound import (
     )
     from miniredis.core.planner import CommandPlanner
     from miniredis.core.reply import Failure
    +from miniredis.persistence.aof import AofWriter


     class RuntimeState(str, Enum):
    @@ -60,6 +61,11 @@ class RuntimeStats:
         owned_tasks: int


    +@dataclass(slots=True)
    +class _RuntimeTestHooks:
    +    aof_appender: CommitBarrier | None = None
    +
    +
     def _direct_transport_close(_reason: str) -> None:
         return None

    @@ -72,13 +78,21 @@ class MiniRedis:
             clock: Clock,
             commit_barrier: CommitBarrier,
             scheduler: TimerScheduler | None,
    +        test_hooks: _RuntimeTestHooks | None = None,
         ) -> None:
             self.config = config
             self.clock = clock
             self.scheduler = (
                 AsyncioTimerScheduler(clock) if scheduler is None else scheduler
             )
    -        self.commit_barrier = commit_barrier
    +        self._test_hooks = test_hooks
    +        actual_barrier = (
    +            test_hooks.aof_appender
    +            if test_hooks is not None and test_hooks.aof_appender is not None
    +            else commit_barrier
    +        )
    +        self.commit_barrier = actual_barrier
    +        self._aof_writer: AofWriter | None = None
             self.database = Database()
             self.planner = CommandPlanner(config)
             self._debug_changed = asyncio.Event()
    @@ -86,12 +100,13 @@ class MiniRedis:
                 database=self.database,
                 planner=self.planner,
                 clock=clock,
    -            commit_barrier=commit_barrier,
    +            commit_barrier=actual_barrier,
                 max_pending_commands=config.max_pending_commands,
                 active_expire_sample_size=config.active_expire_sample_size,
                 scheduler=self.scheduler,
                 on_debug_change=self._debug_notify,
                 on_terminal_failure=self._on_executor_terminal_failure,
    +            on_fatal=self._transition_failed,
             )
             self.state = RuntimeState.STARTING
             self._session_ids = itertools.count(1)
    @@ -123,6 +138,7 @@ class MiniRedis:
                 commit_barrier=(
                     commit_barrier if commit_barrier is not None else NullCommitBarrier()
                 ),
    +            test_hooks=None,
             )

         @classmethod
    @@ -133,14 +149,20 @@ class MiniRedis:
             clock: Clock | None = None,
             scheduler: TimerScheduler | None = None,
             commit_barrier: CommitBarrier | None = None,
    +        test_hooks: _RuntimeTestHooks | None = None,
             **options: Any,
         ) -> MiniRedis:
    -        return cls.open(
    -            config,
    -            clock=clock,
    +        if config is not None and options:
    +            raise TypeError("config cannot be combined with keyword options")
    +        resolved = config if config is not None else MiniRedisConfig(**options)
    +        return cls(
    +            resolved,
    +            clock=clock if clock is not None else SystemClock(),
                 scheduler=scheduler,
    -            commit_barrier=commit_barrier,
    -            **options,
    +            commit_barrier=(
    +                commit_barrier if commit_barrier is not None else NullCommitBarrier()
    +            ),
    +            test_hooks=test_hooks,
             )

         async def start(self) -> None:
    @@ -218,13 +240,16 @@ class MiniRedis:
                     self._track_owned_task(self._shutdown_task)
                 task = self._shutdown_task
             await asyncio.shield(task)
    +        if self.state is RuntimeState.FAILED:
    +            self._set_state(RuntimeState.CLOSED)

         async def _shutdown_once(self, crash: bool = False) -> None:
             del crash
             if self._shutdown_complete:
                 return
             failure = self._failure_reason
    -        if self.state is not RuntimeState.CLOSED:
    +        preserve_failed_state = self.state is RuntimeState.FAILED
    +        if self.state not in {RuntimeState.CLOSED, RuntimeState.FAILED}:
                 self._set_state(RuntimeState.DRAINING)
             self.executor.mailbox.close_user_admission()
             await asyncio.gather(
    @@ -276,7 +301,11 @@ class MiniRedis:
                 if owned.done() or owned is current:
                     self._owned_tasks.discard(owned)
             self._shutdown_complete = True
    -        self._set_state(RuntimeState.CLOSED)
    +        self._set_state(
    +            RuntimeState.FAILED
    +            if preserve_failed_state
    +            else RuntimeState.CLOSED
    +        )

         def _on_executor_terminal_failure(self, failure: BaseException) -> None:
             reason = str(failure) or type(failure).__name__
    ```

**是什么，为什么现在需要**

Runtime 构造、启动、监督并关闭 Configured Writer，同时保留 Private Injection Seam 供 Contract 使用。

**在运行时做什么**

它在 Executor 构造前选定 Actual Barrier，再把 Writer/Executor Fatal Error 映射到一个 Failed Lifecycle。

**关键代码**

```python
actual_barrier = (
    test_hooks.aof_appender
    if test_hooks is not None and test_hooks.aof_appender is not None
    else commit_barrier
)
```

**关键语句理解**

Production 只有一条 Barrier Path；Test 只替换其 Endpoint，不替换 Executor Ordering 或 Runtime Supervision。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/15-aof-commit-barrier/tests.txt)`。它证明 Policy-specific Writer 行为与经真实 Executor 的 End-to-end Visibility/Failure Ordering。

### 需要真正记住的内容

Append 先于 Apply；Ack Exact Sequence；让 Executor 停在 Barrier；不 Append Error/No-op；监督 Writer/Fsync Task；收束每个 Future；Durability Loss 时 Fail Closed。

### 用自己的话讲清楚

AOF 不是 Success 之后再写的 Log，它的 Ack 才允许 Success 可见。Barrier 通过前，Memory、Reply、Waiter Wakeup 与下游 Replica 都留在同一 Ordered Commit 后。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/06-aof.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/633bbe6...b03560a)

完成后可运行 `python -m journey.tools.build_journey check 15` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/15-aof-commit-barrier/stage.patch)

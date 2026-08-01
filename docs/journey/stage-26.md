# Stage 26 · Online AOF rewrite

### Goal

Compact the live AOF into one checkpoint base plus every commit accepted during rewriting, without blocking normal appends or losing the authoritative old log before atomic publication succeeds.

??? note "Deliverable files"
    - `src/miniredis/config.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/persistence/aof.py`
    - `src/miniredis/runtime.py`
    - `tests/helpers/runtime.py`
    - `tests/reliability/test_aof_rewrite.py`
    - `tests/unit/persistence/test_aof_writer.py`

### The problem at this point

Writing a checkpoint file takes blocking I/O while commits continue. The rewrite must capture exactly the suffix after its checkpoint, bound that in-memory delta, and switch the authoritative append descriptor only after the replacement is durably published. Failure before rename should leave the old AOF writable; uncertainty after rename must fail closed because the installed generation may already be authoritative.

### Test contract

#### See the failure first

Capturing the image before registering delta collection creates a write-loss gap. Registering after the next append misses a batch. Unbounded delta capture can exhaust memory behind slow disk. Replacing before temp fsync publishes incomplete bytes; closing the old descriptor too early loses the fallback. Treating parent-fsync failure after rename as recoverable allows future writes against uncertain authority. Crash and graceful close also require different rewrite outcomes.

??? note "File diff: tests/helpers/runtime.py"
    ```diff
    diff --git a/tests/helpers/runtime.py b/tests/helpers/runtime.py
    index 53e77515b06a20108b9ef21a05b85bd61a3856fe..25fb82f5e7ff5f6100067d93d1c577c215b2f26a 100644
    --- a/tests/helpers/runtime.py
    +++ b/tests/helpers/runtime.py
    @@ -7,7 +7,7 @@ from miniredis.persistence.snapshot import (
         PosixSnapshotFileOps,
         SnapshotFileOps,
     )
    -from miniredis.persistence.aof import AofFileOps
    +from miniredis.persistence.aof import AofFileOps, PosixAofFileOps
     from miniredis.runtime import MiniRedis, _RuntimeTestHooks


    @@ -29,11 +29,32 @@ class GateSnapshotFileOps:
             self._delegate.write_atomic(destination, temporary, data)


    +class GateAofRewriteOps(PosixAofFileOps):
    +    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
    +        self.entered = asyncio.Event()
    +        self.release = threading.Event()
    +        self._loop = loop
    +        self._rewrite_fd: int | None = None
    +
    +    def open_rewrite(self, path: Path) -> int:
    +        fd = super().open_rewrite(path)
    +        self._rewrite_fd = fd
    +        return fd
    +
    +    def write_all(self, fd: int, data: bytes) -> None:
    +        if fd == self._rewrite_fd:
    +            self._loop.call_soon_threadsafe(self.entered.set)
    +            self.release.wait()
    +        super().write_all(fd, data)
    +
    +
     class TestMiniRedis(MiniRedis):
         debug_snapshot_write_entered: asyncio.Event
         debug_snapshot_write_release: threading.Event
         debug_replica_apply_entered: asyncio.Event
         debug_replica_apply_release: asyncio.Event
    +    debug_aof_rewrite_entered: asyncio.Event
    +    debug_aof_rewrite_release: threading.Event


     async def open_test_runtime(
    @@ -47,10 +68,14 @@ async def open_test_runtime(
         replica_apply_gate: bool = False,
         aof_ops: AofFileOps | None = None,
         aof_sleep: Callable[[float], Awaitable[None]] | None = None,
    +    aof_rewrite_gate: bool = False,
         lifecycle_trace: bool = False,
     ) -> TestMiniRedis:
         loop = asyncio.get_running_loop()
         snapshot_gate = GateSnapshotFileOps(loop) if snapshot_write_gate else None
    +    if aof_rewrite_gate and aof_ops is not None:
    +        raise ValueError("AOF rewrite gate cannot be combined with aof_ops")
    +    rewrite_gate = GateAofRewriteOps(loop) if aof_rewrite_gate else None
         apply_entered = asyncio.Event() if replica_apply_gate else None
         apply_release = asyncio.Event() if replica_apply_gate else None
         runtime = TestMiniRedis._for_test(
    @@ -63,7 +88,7 @@ async def open_test_runtime(
                 replica_apply_failure=replica_apply_failure,
                 replica_apply_entered=apply_entered,
                 replica_apply_release=apply_release,
    -            aof_ops=aof_ops,
    +            aof_ops=rewrite_gate if rewrite_gate is not None else aof_ops,
                 aof_sleep=aof_sleep,
                 lifecycle_trace=[] if lifecycle_trace else None,
             ),
    @@ -74,5 +99,8 @@ async def open_test_runtime(
         if apply_entered is not None and apply_release is not None:
             runtime.debug_replica_apply_entered = apply_entered
             runtime.debug_replica_apply_release = apply_release
    +    if rewrite_gate is not None:
    +        runtime.debug_aof_rewrite_entered = rewrite_gate.entered
    +        runtime.debug_aof_rewrite_release = rewrite_gate.release
         await runtime.start()
         return runtime
    ```

Provides a narrow rewrite-file gate through real AOF operations and exposes entered/release events; it does not replace writer ordering or publication logic.

??? note "File diff: tests/reliability/test_aof_rewrite.py"
    ```diff
    diff --git a/tests/reliability/test_aof_rewrite.py b/tests/reliability/test_aof_rewrite.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..6ae97d06384a44de15bb577f2c54bdbf313afdc9
    --- /dev/null
    +++ b/tests/reliability/test_aof_rewrite.py
    @@ -0,0 +1,246 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.config import MiniRedisConfig
    +from miniredis.core.reply import Bytes, Items, Ok
    +from miniredis.persistence.aof import (
    +    AofPolicy,
    +    AofRewriteBusy,
    +    AofRewriteFailed,
    +    AofRewriteSaved,
    +    load_aof,
    +)
    +from tests.helpers.runtime import open_test_runtime
    +
    +
    +@pytest.mark.asyncio
    +async def test_write_during_paused_base_survives_rewrite_and_restart(
    +    tmp_path,
    +):
    +    config = MiniRedisConfig(
    +        aof_path=tmp_path / "appendonly.mraof",
    +        aof_policy=AofPolicy.ALWAYS,
    +    )
    +    runtime = await open_test_runtime(
    +        config=config,
    +        aof_rewrite_gate=True,
    +    )
    +    client = runtime.direct_client()
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"before", b"1"))
    +    ) == Ok()
    +
    +    rewriting = asyncio.create_task(runtime.rewrite_aof())
    +    await runtime.debug_aof_rewrite_entered.wait()
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"during", b"2"))
    +    ) == Ok()
    +    stats = runtime.debug_stats()
    +    assert stats.aof_rewrite_active is True
    +    assert stats.aof_rewrite_delta_bytes > 0
    +    assert stats.aof_rewrite_checkpoint_seq == 1
    +
    +    runtime.debug_aof_rewrite_release.set()
    +    assert isinstance(await rewriting, AofRewriteSaved)
    +    await runtime.close()
    +
    +    recovered = MiniRedis.open(config)
    +    await recovered.start()
    +    assert await recovered.direct_client().execute(
    +        CommandRequest(b"MGET", (b"before", b"during"))
    +    ) == Items((Bytes(b"1"), Bytes(b"2")))
    +    await recovered.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_runtime_reports_busy_for_concurrent_rewrite(tmp_path):
    +    runtime = await open_test_runtime(
    +        config=MiniRedisConfig(
    +            aof_path=tmp_path / "appendonly.mraof",
    +            aof_policy=AofPolicy.ALWAYS,
    +        ),
    +        aof_rewrite_gate=True,
    +    )
    +    first = asyncio.create_task(runtime.rewrite_aof())
    +    await runtime.debug_aof_rewrite_entered.wait()
    +
    +    assert await runtime.rewrite_aof() == AofRewriteBusy()
    +
    +    runtime.debug_aof_rewrite_release.set()
    +    assert isinstance(await first, AofRewriteSaved)
    +    await runtime.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_runtime_rewrite_without_aof_is_disabled():
    +    runtime = await open_test_runtime()
    +
    +    assert await runtime.rewrite_aof() == AofRewriteFailed(
    +        "aof_path is not configured"
    +    )
    +
    +    await runtime.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_rewrite_delta_overflow_preserves_old_aof_history(tmp_path):
    +    config = MiniRedisConfig(
    +        aof_path=tmp_path / "appendonly.mraof",
    +        aof_policy=AofPolicy.ALWAYS,
    +        aof_rewrite_delta_limit_bytes=1,
    +    )
    +    runtime = await open_test_runtime(
    +        config=config,
    +        aof_rewrite_gate=True,
    +    )
    +    client = runtime.direct_client()
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"before", b"1"))
    +    ) == Ok()
    +    rewriting = asyncio.create_task(runtime.rewrite_aof())
    +    await runtime.debug_aof_rewrite_entered.wait()
    +
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"during", b"2"))
    +    ) == Ok()
    +    assert await rewriting == AofRewriteFailed(
    +        "AOF rewrite delta limit exceeded"
    +    )
    +    runtime.debug_aof_rewrite_release.set()
    +    await runtime.close()
    +
    +    recovered = MiniRedis.open(config)
    +    await recovered.start()
    +    assert await recovered.direct_client().execute(
    +        CommandRequest(b"MGET", (b"before", b"during"))
    +    ) == Items((Bytes(b"1"), Bytes(b"2")))
    +    await recovered.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_newer_rewrite_base_wins_over_older_snapshot(tmp_path):
    +    config = MiniRedisConfig(
    +        aof_path=tmp_path / "appendonly.mraof",
    +        aof_policy=AofPolicy.ALWAYS,
    +        snapshot_path=tmp_path / "dump.mrsnap",
    +    )
    +    runtime = await open_test_runtime(config=config)
    +    client = runtime.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"k", b"snapshot")))
    +    await runtime.save_snapshot()
    +    await client.execute(CommandRequest(b"SET", (b"k", b"rewrite")))
    +
    +    assert isinstance(await runtime.rewrite_aof(), AofRewriteSaved)
    +    await runtime.close()
    +
    +    recovered = MiniRedis.open(config)
    +    await recovered.start()
    +    assert await recovered.direct_client().execute(
    +        CommandRequest(b"GET", (b"k",))
    +    ) == Bytes(b"rewrite")
    +    await recovered.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_graceful_close_waits_for_runtime_rewrite(tmp_path):
    +    runtime = await open_test_runtime(
    +        config=MiniRedisConfig(
    +            aof_path=tmp_path / "appendonly.mraof",
    +            aof_policy=AofPolicy.ALWAYS,
    +        ),
    +        aof_rewrite_gate=True,
    +    )
    +    rewriting = asyncio.create_task(runtime.rewrite_aof())
    +    await runtime.debug_aof_rewrite_entered.wait()
    +
    +    closing = asyncio.create_task(runtime.close())
    +    await asyncio.sleep(0)
    +    assert not closing.done()
    +    runtime.debug_aof_rewrite_release.set()
    +    assert isinstance(await rewriting, AofRewriteSaved)
    +    await closing
    +    assert runtime.debug_stats().aof_tasks == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_simulated_crash_before_rename_keeps_old_aof(tmp_path):
    +    config = MiniRedisConfig(
    +        aof_path=tmp_path / "appendonly.mraof",
    +        aof_policy=AofPolicy.ALWAYS,
    +    )
    +    runtime = await open_test_runtime(
    +        config=config,
    +        aof_rewrite_gate=True,
    +    )
    +    assert await runtime.direct_client().execute(
    +        CommandRequest(b"SET", (b"k", b"old"))
    +    ) == Ok()
    +    rewriting = asyncio.create_task(runtime.rewrite_aof())
    +    await runtime.debug_aof_rewrite_entered.wait()
    +
    +    crashing = asyncio.create_task(runtime.simulate_crash())
    +    await asyncio.sleep(0)
    +    runtime.debug_aof_rewrite_release.set()
    +    await crashing
    +    assert await rewriting == AofRewriteFailed(
    +        "AOF writer crashed during rewrite"
    +    )
    +
    +    recovered = MiniRedis.open(config)
    +    await recovered.start()
    +    assert await recovered.direct_client().execute(
    +        CommandRequest(b"GET", (b"k",))
    +    ) == Bytes(b"old")
    +    await recovered.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_successful_rewrite_compacts_history_to_base_only(tmp_path):
    +    path = tmp_path / "appendonly.mraof"
    +    runtime = await open_test_runtime(
    +        config=MiniRedisConfig(
    +            aof_path=path,
    +            aof_policy=AofPolicy.ALWAYS,
    +        )
    +    )
    +    client = runtime.direct_client()
    +    for value in (b"1", b"2", b"3"):
    +        await client.execute(CommandRequest(b"SET", (b"k", value)))
    +
    +    assert len(load_aof(path, repair_truncated_tail=False).batches) == 3
    +    assert isinstance(await runtime.rewrite_aof(), AofRewriteSaved)
    +    log = load_aof(path, repair_truncated_tail=False)
    +    assert log.state_base is not None
    +    assert log.state_base.checkpoint_seq == 3
    +    assert log.batches == ()
    +    await runtime.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_immediate_write_after_rewrite_request_has_no_capture_gap(
    +    tmp_path,
    +):
    +    path = tmp_path / "appendonly.mraof"
    +    config = MiniRedisConfig(
    +        aof_path=path,
    +        aof_policy=AofPolicy.ALWAYS,
    +    )
    +    runtime = await open_test_runtime(config=config)
    +    client = runtime.direct_client()
    +    rewriting = asyncio.create_task(runtime.rewrite_aof())
    +    writing = asyncio.create_task(
    +        client.execute(CommandRequest(b"SET", (b"k", b"v")))
    +    )
    +
    +    assert isinstance(await rewriting, AofRewriteSaved)
    +    assert await writing == Ok()
    +    await runtime.close()
    +
    +    recovered = MiniRedis.open(config)
    +    await recovered.start()
    +    assert await recovered.direct_client().execute(
    +        CommandRequest(b"GET", (b"k",))
    +    ) == Bytes(b"v")
    +    await recovered.close()
    ```

Locks runtime-visible compaction, concurrent write recovery, no capture gap, BUSY behavior, disabled configuration, overflow preserving old history, newer-base recovery, graceful close, and simulated crash before rename. The decisive counterexample pauses base writing, commits `during`, then restarts and reads both values.

??? note "File diff: tests/unit/persistence/test_aof_writer.py"
    ```diff
    diff --git a/tests/unit/persistence/test_aof_writer.py b/tests/unit/persistence/test_aof_writer.py
    index 64469f939c14cc12bb483f0742d1b8d11bea08fe..adbbfda2e4caf8e7e1c6d69c3d8076e12c86f79b 100644
    --- a/tests/unit/persistence/test_aof_writer.py
    +++ b/tests/unit/persistence/test_aof_writer.py
    @@ -1,16 +1,24 @@
     import asyncio
     import os
     import threading
    +from pathlib import Path

     import pytest

    +from miniredis.config import MiniRedisConfig
    +from miniredis.core.commit import SnapshotImage
     from miniredis.persistence.aof import (
         AofAppendFailed,
         AofAppendOk,
         AofPolicy,
    +    AofRewriteBusy,
    +    AofRewriteFailed,
    +    AofRewriteSaved,
         AofWriter,
         PosixAofFileOps,
    +    load_aof,
     )
    +from miniredis.persistence.codec import encode_aof_record
     from tests.unit.persistence.test_framing import batch


    @@ -221,3 +229,356 @@ async def test_background_fsync_failure_fails_a_concurrent_append(tmp_path):
         assert outcome.message == "background fsync failed"
         assert isinstance(await writer.append(batch(3)), AofAppendFailed)
         await writer.close()
    +
    +
    +class GateRewriteOps(PosixAofFileOps):
    +    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
    +        self._loop = loop
    +        self._rewrite_fd: int | None = None
    +        self.rewrite_paths: list[Path] = []
    +        self.base_entered = asyncio.Event()
    +        self.release_base = threading.Event()
    +
    +    def open_rewrite(self, path: Path) -> int:
    +        self.rewrite_paths.append(path)
    +        fd = super().open_rewrite(path)
    +        self._rewrite_fd = fd
    +        return fd
    +
    +    def write_all(self, fd: int, data: bytes) -> None:
    +        if fd == self._rewrite_fd:
    +            self._loop.call_soon_threadsafe(self.base_entered.set)
    +            self.release_base.wait()
    +        super().write_all(fd, data)
    +
    +
    +@pytest.mark.asyncio
    +async def test_begin_rewrite_registers_before_next_append(tmp_path):
    +    ops = GateRewriteOps(asyncio.get_running_loop())
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=4096,
    +        ops=ops,
    +    )
    +    await writer.start()
    +
    +    job = writer.begin_rewrite(SnapshotImage(0, ()))
    +
    +    assert writer.rewrite_active
    +    await ops.base_entered.wait()
    +    assert await writer.append(batch(1)) == AofAppendOk(1)
    +    assert writer.rewrite_delta_bytes == len(encode_aof_record(batch(1)))
    +    ops.release_base.set()
    +    assert await job == AofRewriteSaved(
    +        tmp_path / "appendonly.mraof",
    +        0,
    +    )
    +    await writer.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_second_rewrite_is_busy_while_base_is_active(tmp_path):
    +    ops = GateRewriteOps(asyncio.get_running_loop())
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=4096,
    +        ops=ops,
    +    )
    +    await writer.start()
    +    first = writer.begin_rewrite(SnapshotImage(0, ()))
    +    await ops.base_entered.wait()
    +
    +    assert await writer.begin_rewrite(SnapshotImage(0, ())) == AofRewriteBusy()
    +
    +    ops.release_base.set()
    +    assert isinstance(await first, AofRewriteSaved)
    +    await writer.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_rewrite_is_disabled_until_writer_is_started(tmp_path):
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=4096,
    +    )
    +
    +    outcome = await writer.begin_rewrite(SnapshotImage(0, ()))
    +
    +    assert outcome == AofRewriteFailed("AOF writer is not accepting")
    +
    +
    +@pytest.mark.asyncio
    +async def test_rewrite_delta_overflow_does_not_fail_authoritative_append(
    +    tmp_path,
    +):
    +    ops = GateRewriteOps(asyncio.get_running_loop())
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=1,
    +        ops=ops,
    +    )
    +    await writer.start()
    +    job = writer.begin_rewrite(SnapshotImage(0, ()))
    +    await ops.base_entered.wait()
    +
    +    assert await writer.append(batch(1)) == AofAppendOk(1)
    +    assert await job == AofRewriteFailed("AOF rewrite delta limit exceeded")
    +    assert writer.failure is None
    +
    +    ops.release_base.set()
    +    await writer.close()
    +    assert not tuple(tmp_path.glob("*.tmp"))
    +
    +
    +class FailingRewriteOpenOps(PosixAofFileOps):
    +    def open_rewrite(self, path: Path) -> int:
    +        raise OSError("cannot create rewrite")
    +
    +
    +@pytest.mark.asyncio
    +async def test_rewrite_base_failure_leaves_writer_available(tmp_path):
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=4096,
    +        ops=FailingRewriteOpenOps(),
    +    )
    +    await writer.start()
    +
    +    outcome = await writer.begin_rewrite(SnapshotImage(0, ()))
    +
    +    assert outcome == AofRewriteFailed("cannot create rewrite")
    +    assert await writer.append(batch(1)) == AofAppendOk(1)
    +    await writer.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_successive_rewrites_use_unique_temporary_paths(tmp_path):
    +    ops = GateRewriteOps(asyncio.get_running_loop())
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=4096,
    +        ops=ops,
    +    )
    +    await writer.start()
    +    first = writer.begin_rewrite(SnapshotImage(0, ()))
    +    await ops.base_entered.wait()
    +    ops.release_base.set()
    +    assert isinstance(await first, AofRewriteSaved)
    +
    +    ops.release_base.clear()
    +    ops.base_entered.clear()
    +    second = writer.begin_rewrite(SnapshotImage(0, ()))
    +    await ops.base_entered.wait()
    +    ops.release_base.set()
    +    assert isinstance(await second, AofRewriteSaved)
    +
    +    assert len(ops.rewrite_paths) == 2
    +    assert ops.rewrite_paths[0] != ops.rewrite_paths[1]
    +    await writer.close()
    +
    +
    +@pytest.mark.parametrize("limit", [0, -1])
    +def test_rewrite_delta_limit_must_be_positive(limit):
    +    with pytest.raises(
    +        ValueError,
    +        match="aof_rewrite_delta_limit_bytes must be positive",
    +    ):
    +        MiniRedisConfig(aof_rewrite_delta_limit_bytes=limit)
    +
    +
    +class RewriteFailureOps(PosixAofFileOps):
    +    def __init__(self) -> None:
    +        self.rewrite_fd: int | None = None
    +        self.fail_temp_write = False
    +        self.fail_temp_fsync = False
    +        self.fail_replace = False
    +        self.fail_parent_fsync = False
    +        self.writes: list[tuple[int, bytes]] = []
    +
    +    def open_rewrite(self, path: Path) -> int:
    +        fd = super().open_rewrite(path)
    +        self.rewrite_fd = fd
    +        return fd
    +
    +    def write_all(self, fd: int, data: bytes) -> None:
    +        self.writes.append((fd, data))
    +        if fd == self.rewrite_fd and self.fail_temp_write:
    +            raise OSError("temp write failed")
    +        super().write_all(fd, data)
    +
    +    def fsync(self, fd: int) -> None:
    +        if fd == self.rewrite_fd and self.fail_temp_fsync:
    +            raise OSError("temp fsync failed")
    +        super().fsync(fd)
    +
    +    def replace(self, source: Path, destination: Path) -> None:
    +        if self.fail_replace:
    +            raise OSError("replace failed")
    +        super().replace(source, destination)
    +
    +    def fsync_parent(self, path: Path) -> None:
    +        if self.fail_parent_fsync:
    +            raise OSError("parent fsync failed")
    +        super().fsync_parent(path)
    +
    +
    +@pytest.mark.asyncio
    +@pytest.mark.parametrize(
    +    ("failure", "message"),
    +    [
    +        ("fail_temp_write", "temp write failed"),
    +        ("fail_temp_fsync", "temp fsync failed"),
    +        ("fail_replace", "replace failed"),
    +    ],
    +)
    +async def test_pre_rename_rewrite_failure_keeps_old_aof_writable(
    +    tmp_path,
    +    failure,
    +    message,
    +):
    +    path = tmp_path / "appendonly.mraof"
    +    ops = RewriteFailureOps()
    +    writer = AofWriter(
    +        path,
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=4096,
    +        ops=ops,
    +    )
    +    await writer.start()
    +    assert await writer.append(batch(1)) == AofAppendOk(1)
    +    setattr(ops, failure, True)
    +
    +    outcome = await writer.begin_rewrite(SnapshotImage(1, ()))
    +
    +    assert outcome == AofRewriteFailed(message)
    +    setattr(ops, failure, False)
    +    assert await writer.append(batch(2)) == AofAppendOk(2)
    +    assert writer.failure is None
    +    await writer.close()
    +    assert load_aof(path, repair_truncated_tail=False).batches == (
    +        batch(1),
    +        batch(2),
    +    )
    +
    +
    +@pytest.mark.asyncio
    +async def test_paused_base_delta_is_ordered_into_rewritten_file(tmp_path):
    +    path = tmp_path / "appendonly.mraof"
    +    ops = GateRewriteOps(asyncio.get_running_loop())
    +    writer = AofWriter(
    +        path,
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=4096,
    +        ops=ops,
    +    )
    +    await writer.start()
    +    job = writer.begin_rewrite(SnapshotImage(0, ()))
    +    await ops.base_entered.wait()
    +
    +    assert await writer.append(batch(1)) == AofAppendOk(1)
    +    ops.release_base.set()
    +    assert isinstance(await job, AofRewriteSaved)
    +    await writer.close()
    +
    +    log = load_aof(path, repair_truncated_tail=False)
    +    assert log.state_base == SnapshotImage(0, ())
    +    assert log.batches == (batch(1),)
    +
    +
    +@pytest.mark.asyncio
    +async def test_parent_fsync_failure_after_rename_is_terminal(tmp_path):
    +    failures: list[BaseException] = []
    +    ops = RewriteFailureOps()
    +    ops.fail_parent_fsync = True
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=4096,
    +        ops=ops,
    +        on_failure=failures.append,
    +    )
    +    await writer.start()
    +
    +    outcome = await writer.begin_rewrite(SnapshotImage(0, ()))
    +
    +    assert outcome == AofRewriteFailed("parent fsync failed")
    +    assert len(failures) == 1
    +    assert str(writer.failure) == "parent fsync failed"
    +    later = await writer.append(batch(1))
    +    assert later == AofAppendFailed("parent fsync failed")
    +    await writer.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_append_after_successful_rewrite_uses_new_descriptor(tmp_path):
    +    ops = RewriteFailureOps()
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=4096,
    +        ops=ops,
    +    )
    +    await writer.start()
    +    assert isinstance(
    +        await writer.begin_rewrite(SnapshotImage(0, ())),
    +        AofRewriteSaved,
    +    )
    +    assert ops.rewrite_fd is not None
    +
    +    assert await writer.append(batch(1)) == AofAppendOk(1)
    +
    +    assert ops.writes[-1] == (ops.rewrite_fd, encode_aof_record(batch(1)))
    +    await writer.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_graceful_close_waits_for_active_rewrite(tmp_path):
    +    ops = GateRewriteOps(asyncio.get_running_loop())
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=4096,
    +        ops=ops,
    +    )
    +    await writer.start()
    +    job = writer.begin_rewrite(SnapshotImage(0, ()))
    +    await ops.base_entered.wait()
    +
    +    closing = asyncio.create_task(writer.close())
    +    await asyncio.sleep(0)
    +    assert not closing.done()
    +    ops.release_base.set()
    +    assert isinstance(await job, AofRewriteSaved)
    +    await closing
    +
    +    assert writer.owned_task_count == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_crash_close_aborts_rewrite_and_cleans_temp(tmp_path):
    +    ops = GateRewriteOps(asyncio.get_running_loop())
    +    writer = AofWriter(
    +        tmp_path / "appendonly.mraof",
    +        AofPolicy.ALWAYS,
    +        rewrite_delta_limit_bytes=4096,
    +        ops=ops,
    +    )
    +    await writer.start()
    +    job = writer.begin_rewrite(SnapshotImage(0, ()))
    +    await ops.base_entered.wait()
    +
    +    crashing = asyncio.create_task(writer.crash_close())
    +    await asyncio.sleep(0)
    +    assert not crashing.done()
    +    ops.release_base.set()
    +    await crashing
    +
    +    assert await job == AofRewriteFailed("AOF writer crashed during rewrite")
    +    assert writer.owned_task_count == 0
    +    assert not tuple(tmp_path.glob("*.tmp"))
    ```

Locks registration-before-append, BUSY overlap, disabled states, bounded delta overflow, unique temp paths, pre-rename failure isolation, ordered suffix capture, post-rename terminal failure, descriptor swap, graceful join, and crash cleanup. Failure identifies the exact publication phase whose ownership is wrong.

### Basic concepts

A rewrite generation owns a checkpoint image, unique temp path/fd, completion future, base-writing task, and bounded delta buffer. The existing AOF remains authoritative until rename. Pre-rename errors are local rewrite failures. After rename, parent-directory durability is uncertain, so failure becomes terminal. Graceful close waits for publication; crash aborts and cleans temporary state.

### Why this mechanism is necessary

Append-only durability grows without bound. Online compaction must preserve availability while proving the rewritten file represents one state-machine prefix plus its complete concurrent suffix. Keeping delta capture inside the serial AOF writer reuses the authoritative commit order rather than reconstructing it from clocks or tasks.

### Runtime mental model

An executor control message captures SnapshotImage N and synchronously calls `begin_rewrite`, registering generation state before another commit turn. A background task writes header plus base. Meanwhile, the normal writer appends every committed record to the old AOF and copies the same encoded bytes into the bounded delta. A finalize item runs in writer order, appends the frozen delta to temp, fsyncs, renames, fsyncs the directory, swaps `_fd`, then closes the old descriptor.

### Mechanism blocks

#### Atomic online AOF rewrite

Write a state-base temp file off the append path, capture every concurrent committed delta in writer order, then fsync, rename, fsync the parent, and swap descriptors.

??? note "File diff: src/miniredis/persistence/aof.py"
    ```diff
    diff --git a/src/miniredis/persistence/aof.py b/src/miniredis/persistence/aof.py
    index fea7ff4a4d608819e194384d2dfff1c4d10b8dd3..1f8ab9060aa86632f7df77be36e21665e4cd68ba 100644
    --- a/src/miniredis/persistence/aof.py
    +++ b/src/miniredis/persistence/aof.py
    @@ -13,6 +13,7 @@ from miniredis.persistence.codec import (
         AOF_HEADER,
         CodecError,
         encode_aof_record,
    +    encode_aof_state_base_record,
         scan_aof_bytes,
     )

    @@ -40,6 +41,27 @@ class AofAppendFailed:
     AofAppendOutcome: TypeAlias = AofAppendOk | AofAppendFailed


    +@dataclass(frozen=True, slots=True)
    +class AofRewriteSaved:
    +    path: Path
    +    checkpoint_seq: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class AofRewriteBusy:
    +    pass
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class AofRewriteFailed:
    +    message: str
    +
    +
    +AofRewriteOutcome: TypeAlias = (
    +    AofRewriteSaved | AofRewriteBusy | AofRewriteFailed
    +)
    +
    +
     @dataclass(frozen=True, slots=True)
     class AofLog:
         state_base: SnapshotImage | None
    @@ -50,6 +72,9 @@ class AofFileOps(Protocol):
         def open_append(self, path: Path) -> int:
             raise NotImplementedError

    +    def open_rewrite(self, path: Path) -> int:
    +        raise NotImplementedError
    +
         def size(self, fd: int) -> int:
             raise NotImplementedError

    @@ -65,6 +90,15 @@ class AofFileOps(Protocol):
         def close(self, fd: int) -> None:
             raise NotImplementedError

    +    def replace(self, source: Path, destination: Path) -> None:
    +        raise NotImplementedError
    +
    +    def fsync_parent(self, path: Path) -> None:
    +        raise NotImplementedError
    +
    +    def unlink(self, path: Path) -> None:
    +        raise NotImplementedError
    +

     class PosixAofFileOps:
         def open_append(self, path: Path) -> int:
    @@ -75,6 +109,13 @@ class PosixAofFileOps:
                 0o600,
             )

    +    def open_rewrite(self, path: Path) -> int:
    +        return os.open(
    +            path,
    +            os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_APPEND,
    +            0o600,
    +        )
    +
         def size(self, fd: int) -> int:
             return os.fstat(fd).st_size

    @@ -95,6 +136,19 @@ class PosixAofFileOps:
         def close(self, fd: int) -> None:
             os.close(fd)

    +    def replace(self, source: Path, destination: Path) -> None:
    +        os.replace(source, destination)
    +
    +    def fsync_parent(self, path: Path) -> None:
    +        directory_fd = os.open(path.parent, os.O_RDONLY)
    +        try:
    +            os.fsync(directory_fd)
    +        finally:
    +            os.close(directory_fd)
    +
    +    def unlink(self, path: Path) -> None:
    +        os.unlink(path)
    +

     def load_aof(
         path: Path,
    @@ -132,6 +186,24 @@ class _AppendWork:
         barrier: asyncio.Future[AofAppendOutcome]


    +@dataclass(frozen=True, slots=True)
    +class _FinalizeRewrite:
    +    generation: int
    +
    +
    +@dataclass(slots=True)
    +class _RewriteState:
    +    generation: int
    +    image: SnapshotImage
    +    temporary: Path
    +    completion: asyncio.Future[AofRewriteOutcome]
    +    delta: bytearray
    +    temporary_fd: int | None = None
    +    base_task: asyncio.Task[None] | None = None
    +    abort_reason: str | None = None
    +    renamed: bool = False
    +
    +
     _STOP = object()


    @@ -142,19 +214,25 @@ class AofWriter:
             policy: AofPolicy,
             *,
             fsync_interval_seconds: float = 1.0,
    +        rewrite_delta_limit_bytes: int = 8 * 1024 * 1024,
             ops: AofFileOps | None = None,
             sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
             on_failure: Callable[[BaseException], None] | None = None,
         ) -> None:
             if fsync_interval_seconds <= 0:
                 raise ValueError("fsync interval must be positive")
    +        if rewrite_delta_limit_bytes <= 0:
    +            raise ValueError("rewrite delta limit must be positive")
             self._path = path
             self._policy = policy
             self._interval = fsync_interval_seconds
    +        self._rewrite_delta_limit_bytes = rewrite_delta_limit_bytes
             self._ops = ops or PosixAofFileOps()
             self._sleep = sleep
             self._on_failure = on_failure or (lambda _error: None)
    -        self._queue: asyncio.Queue[_AppendWork | object] = asyncio.Queue()
    +        self._queue: asyncio.Queue[
    +            _AppendWork | _FinalizeRewrite | object
    +        ] = asyncio.Queue()
             self._fd: int | None = None
             self._worker: asyncio.Task[None] | None = None
             self._sync_task: asyncio.Task[None] | None = None
    @@ -164,17 +242,41 @@ class AofWriter:
             self._accepting = False
             self._failure: BaseException | None = None
             self._failure_reported = False
    +        self._rewrite_generation = 0
    +        self._rewrite: _RewriteState | None = None

         @property
         def failure(self) -> BaseException | None:
             return self._failure

    +    @property
    +    def rewrite_active(self) -> bool:
    +        return self._rewrite is not None
    +
    +    @property
    +    def rewrite_delta_bytes(self) -> int:
    +        state = self._rewrite
    +        return len(state.delta) if state is not None else 0
    +
    +    @property
    +    def rewrite_checkpoint_seq(self) -> int | None:
    +        state = self._rewrite
    +        return state.image.checkpoint_seq if state is not None else None
    +
         @property
         def owned_task_count(self) -> int:
    -        return sum(
    +        count = sum(
                 task is not None and not task.done()
                 for task in (self._worker, self._sync_task)
             )
    +        state = self._rewrite
    +        if (
    +            state is not None
    +            and state.base_task is not None
    +            and not state.base_task.done()
    +        ):
    +            count += 1
    +        return count

         async def start(self) -> None:
             if self._worker is not None:
    @@ -227,11 +329,80 @@ class AofWriter:
             )
             return await asyncio.shield(barrier)

    +    def begin_rewrite(
    +        self,
    +        image: SnapshotImage,
    +    ) -> asyncio.Future[AofRewriteOutcome]:
    +        loop = asyncio.get_running_loop()
    +        completion: asyncio.Future[AofRewriteOutcome] = loop.create_future()
    +        if (
    +            self._failure is not None
    +            or not self._accepting
    +            or self._worker is None
    +        ):
    +            completion.set_result(
    +                AofRewriteFailed(
    +                    str(self._failure)
    +                    if self._failure is not None
    +                    else "AOF writer is not accepting"
    +                )
    +            )
    +            return completion
    +        if self._rewrite is not None:
    +            completion.set_result(AofRewriteBusy())
    +            return completion
    +
    +        self._rewrite_generation += 1
    +        generation = self._rewrite_generation
    +        temporary = self._path.with_name(
    +            f".{self._path.name}.rewrite.{os.getpid()}."
    +            f"{generation}.tmp"
    +        )
    +        state = _RewriteState(
    +            generation=generation,
    +            image=image,
    +            temporary=temporary,
    +            completion=completion,
    +            delta=bytearray(),
    +        )
    +        self._rewrite = state
    +        state.base_task = asyncio.create_task(
    +            self._write_rewrite_base(state),
    +            name=f"miniredis-aof-rewrite-base-{generation}",
    +        )
    +        return completion
    +
    +    async def _write_rewrite_base(self, state: _RewriteState) -> None:
    +        try:
    +            fd = await asyncio.to_thread(
    +                self._ops.open_rewrite,
    +                state.temporary,
    +            )
    +            state.temporary_fd = fd
    +            await asyncio.to_thread(
    +                self._ops.write_all,
    +                fd,
    +                AOF_HEADER + encode_aof_state_base_record(state.image),
    +            )
    +            if state.abort_reason is not None:
    +                await self._cleanup_rewrite(state)
    +                return
    +            self._queue.put_nowait(_FinalizeRewrite(state.generation))
    +        except BaseException as exc:
    +            self._settle_rewrite(
    +                state.completion,
    +                AofRewriteFailed(str(exc)),
    +            )
    +            await self._cleanup_rewrite(state)
    +
         async def _run_writer(self) -> None:
             while True:
                 item = await self._queue.get()
                 if item is _STOP:
                     return
    +            if isinstance(item, _FinalizeRewrite):
    +                await self._finalize_rewrite(item)
    +                continue
                 assert isinstance(item, _AppendWork)
                 self._current_work = item
                 if self._failure is not None:
    @@ -269,9 +440,111 @@ class AofWriter:
                     self._fail_queued(exc)
                     self._current_work = None
                     return
    +            self._capture_rewrite_delta(item.record)
                 self._settle(item.barrier, AofAppendOk(item.seq))
                 self._current_work = None

    +    def _capture_rewrite_delta(self, record: bytes) -> None:
    +        state = self._rewrite
    +        if state is None or state.abort_reason is not None:
    +            return
    +        if (
    +            len(state.delta) + len(record)
    +            > self._rewrite_delta_limit_bytes
    +        ):
    +            state.abort_reason = "AOF rewrite delta limit exceeded"
    +            self._settle_rewrite(
    +                state.completion,
    +                AofRewriteFailed(state.abort_reason),
    +            )
    +            return
    +        state.delta.extend(record)
    +
    +    async def _finalize_rewrite(self, item: _FinalizeRewrite) -> None:
    +        state = self._rewrite
    +        if state is None or state.generation != item.generation:
    +            return
    +        if state.abort_reason is not None:
    +            await self._cleanup_rewrite(state)
    +            return
    +        temporary_fd = state.temporary_fd
    +        old_fd = self._fd
    +        try:
    +            assert temporary_fd is not None
    +            assert old_fd is not None
    +            await asyncio.to_thread(
    +                self._ops.write_all,
    +                temporary_fd,
    +                bytes(state.delta),
    +            )
    +            await asyncio.to_thread(self._ops.fsync, temporary_fd)
    +            await asyncio.to_thread(
    +                self._ops.replace,
    +                state.temporary,
    +                self._path,
    +            )
    +            state.renamed = True
    +            await asyncio.to_thread(self._ops.fsync_parent, self._path)
    +            self._fd = temporary_fd
    +            state.temporary_fd = None
    +            await asyncio.to_thread(self._ops.close, old_fd)
    +        except BaseException as exc:
    +            if state.renamed:
    +                installed_fd = state.temporary_fd
    +                if installed_fd is not None:
    +                    self._fd = installed_fd
    +                    state.temporary_fd = None
    +                if old_fd is not None and old_fd != self._fd:
    +                    try:
    +                        await asyncio.to_thread(
    +                            self._ops.close,
    +                            old_fd,
    +                        )
    +                    except OSError:
    +                        pass
    +                if self._rewrite is state:
    +                    self._rewrite = None
    +                self._record_failure(exc)
    +                self._fail_queued(exc)
    +                self._settle_rewrite(
    +                    state.completion,
    +                    AofRewriteFailed(str(exc)),
    +                )
    +                return
    +            self._settle_rewrite(
    +                state.completion,
    +                AofRewriteFailed(str(exc)),
    +            )
    +            await self._cleanup_rewrite(state)
    +            return
    +        self._settle_rewrite(
    +            state.completion,
    +            AofRewriteSaved(
    +                path=self._path,
    +                checkpoint_seq=state.image.checkpoint_seq,
    +            ),
    +        )
    +        if self._rewrite is state:
    +            self._rewrite = None
    +
    +    async def _cleanup_rewrite(self, state: _RewriteState) -> None:
    +        fd, state.temporary_fd = state.temporary_fd, None
    +        if fd is not None:
    +            try:
    +                await asyncio.to_thread(self._ops.close, fd)
    +            except OSError:
    +                pass
    +        if not state.renamed:
    +            try:
    +                await asyncio.to_thread(
    +                    self._ops.unlink,
    +                    state.temporary,
    +                )
    +            except FileNotFoundError:
    +                pass
    +        if self._rewrite is state:
    +            self._rewrite = None
    +
         def _writer_done(self, task: asyncio.Task[None]) -> None:
             if task.cancelled():
                 error: BaseException | None = RuntimeError("AOF writer task was cancelled")
    @@ -328,6 +601,14 @@ class AofWriter:
             if not barrier.done():
                 barrier.set_result(outcome)

    +    @staticmethod
    +    def _settle_rewrite(
    +        completion: asyncio.Future[AofRewriteOutcome],
    +        outcome: AofRewriteOutcome,
    +    ) -> None:
    +        if not completion.done():
    +            completion.set_result(outcome)
    +
         def _record_failure(self, error: BaseException) -> None:
             if self._failure is None:
                 self._failure = error
    @@ -352,6 +633,13 @@ class AofWriter:
             if self._fd is None:
                 return
             self._accepting = False
    +        rewrite = self._rewrite
    +        if (
    +            rewrite is not None
    +            and rewrite.base_task is not None
    +            and not rewrite.base_task.done()
    +        ):
    +            await asyncio.shield(rewrite.base_task)
             if self._worker is not None and not self._worker.done():
                 self._queue.put_nowait(_STOP)
                 await asyncio.shield(self._worker)
    @@ -376,6 +664,19 @@ class AofWriter:
             if self._fd is None:
                 return
             self._accepting = False
    +        rewrite = self._rewrite
    +        if rewrite is not None and not rewrite.renamed:
    +            rewrite.abort_reason = "AOF writer crashed during rewrite"
    +            self._settle_rewrite(
    +                rewrite.completion,
    +                AofRewriteFailed(rewrite.abort_reason),
    +            )
    +        if (
    +            rewrite is not None
    +            and rewrite.base_task is not None
    +            and not rewrite.base_task.done()
    +        ):
    +            await asyncio.shield(rewrite.base_task)
             if self._worker is not None and not self._worker.done():
                 self._queue.put_nowait(_STOP)
                 await asyncio.shield(self._worker)
    ```

Owns rewrite outcomes, generation state, temp-file operations, delta capture, ordered finalization, descriptor handoff, cleanup, and close/crash semantics.

```python
self._capture_rewrite_delta(item.record)
self._settle(item.barrier, AofAppendOk(item.seq))
```

The exact encoded record accepted by the authoritative writer is also the rewrite suffix; no second serialization can drift.

#### Gap-free executor registration

Capture the checkpoint and register rewrite delta collection in one executor control turn before later commits can interleave.

??? note "File diff: src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index 5391570d0166cc8310db6b7a4d6df62f2147178e..5c956da86e72f58179323ca204ed9aae458599ca 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -66,6 +66,8 @@ from miniredis.persistence.aof import (
         AofAppendFailed,
         AofAppendOk,
         AofAppendOutcome,
    +    AofRewriteFailed,
    +    AofRewriteOutcome,
     )
     from miniredis.replication.sink import ReplicaAttachment

    @@ -134,6 +136,11 @@ class SnapshotBarrier:
         future: asyncio.Future[SnapshotImage]


    +@dataclass(slots=True)
    +class BeginAofRewrite:
    +    future: asyncio.Future[AofRewriteOutcome]
    +
    +
     @dataclass(slots=True)
     class AttachReplica:
         sink: ReplicaSink
    @@ -252,6 +259,13 @@ class CommandExecutor:
             replica_apply_entered: asyncio.Event | None = None,
             replica_apply_release: asyncio.Event | None = None,
             allow_failure_injection: bool = False,
    +        begin_aof_rewrite: (
    +            Callable[
    +                [SnapshotImage],
    +                asyncio.Future[AofRewriteOutcome],
    +            ]
    +            | None
    +        ) = None,
         ) -> None:
             self.database = database
             self.planner = planner
    @@ -311,6 +325,7 @@ class CommandExecutor:
             self._stopping = False
             self._started = False
             self._allow_failure_injection = allow_failure_injection
    +        self._begin_aof_rewrite = begin_aof_rewrite

         def install_database_before_start(self, database: Database) -> None:
             if self._started:
    @@ -325,6 +340,19 @@ class CommandExecutor:
                 raise RuntimeError("commit barrier can only be installed before start")
             self.commit_barrier = commit_barrier

    +    def set_aof_rewrite_registration_before_start(
    +        self,
    +        begin_aof_rewrite: Callable[
    +            [SnapshotImage],
    +            asyncio.Future[AofRewriteOutcome],
    +        ],
    +    ) -> None:
    +        if self._started:
    +            raise RuntimeError(
    +                "AOF rewrite registration can only be installed before start"
    +            )
    +        self._begin_aof_rewrite = begin_aof_rewrite
    +
         async def start(self) -> None:
             if self._started:
                 if self._stopping:
    @@ -517,6 +545,28 @@ class CommandExecutor:
                 else:
                     if not message.future.done():
                         message.future.set_result(image)
    +        elif isinstance(message, BeginAofRewrite):
    +            if self._begin_aof_rewrite is None:
    +                if not message.future.done():
    +                    message.future.set_result(
    +                        AofRewriteFailed("AOF writer is not configured")
    +                    )
    +                return
    +            try:
    +                image = self.database.snapshot_image(self.clock.now_ms())
    +                job = self._begin_aof_rewrite(image)
    +            except BaseException as exc:
    +                if not message.future.done():
    +                    message.future.set_result(
    +                        AofRewriteFailed(str(exc))
    +                    )
    +            else:
    +                job.add_done_callback(
    +                    lambda completed: self._complete_aof_rewrite(
    +                        completed,
    +                        message.future,
    +                    )
    +                )
             elif isinstance(message, AttachReplica):
                 generation = self._next_replica_generation
                 self._next_replica_generation += 1
    @@ -1085,6 +1135,32 @@ class CommandExecutor:
                 raise RuntimeError("executor control admission is closed")
             return await asyncio.shield(future)

    +    @staticmethod
    +    def _complete_aof_rewrite(
    +        job: asyncio.Future[AofRewriteOutcome],
    +        destination: asyncio.Future[AofRewriteOutcome],
    +    ) -> None:
    +        if destination.done():
    +            return
    +        if job.cancelled():
    +            destination.set_result(
    +                AofRewriteFailed("AOF rewrite was cancelled")
    +            )
    +            return
    +        error = job.exception()
    +        if error is not None:
    +            destination.set_result(AofRewriteFailed(str(error)))
    +            return
    +        destination.set_result(job.result())
    +
    +    async def begin_aof_rewrite(self) -> AofRewriteOutcome:
    +        future: asyncio.Future[AofRewriteOutcome] = (
    +            asyncio.get_running_loop().create_future()
    +        )
    +        if not self.post_control(BeginAofRewrite(future)):
    +            return AofRewriteFailed("executor control admission is closed")
    +        return await asyncio.shield(future)
    +
         async def attach_replica(self, sink: ReplicaSink) -> ReplicaAttachment:
             future: asyncio.Future[ReplicaAttachment] = (
                 asyncio.get_running_loop().create_future()
    ```

Captures the checkpoint and registers the AOF job in one serialized control turn, then bridges owned job completion into a cancellation-safe rewrite outcome.

#### Rewrite lifecycle ownership

Validate the delta bound, expose one runtime rewrite operation and stats, and distinguish graceful join from crash abort.

??? note "File diff: src/miniredis/config.py"
    ```diff
    diff --git a/src/miniredis/config.py b/src/miniredis/config.py
    index c6e0e548b1f324adb75e7abefff094090dd8b0fc..fb4b46be7c3414ad76df3a7515610230387c9b39 100644
    --- a/src/miniredis/config.py
    +++ b/src/miniredis/config.py
    @@ -23,6 +23,7 @@ class MiniRedisConfig:
         aof_policy: AofPolicy = AofPolicy.EVERYSEC
         aof_repair_truncated_tail: bool = True
         aof_fsync_interval_seconds: float = 1.0
    +    aof_rewrite_delta_limit_bytes: int = 8 * 1024 * 1024
         snapshot_path: Path | None = None
         replica_queue_limit: int = 64
         replica_drain_grace_ms: int = 1000
    @@ -54,6 +55,10 @@ class MiniRedisConfig:
                 raise ValueError("active_expire_interval_ms must be positive")
             if self.aof_fsync_interval_seconds <= 0:
                 raise ValueError("aof_fsync_interval_seconds must be positive")
    +        if self.aof_rewrite_delta_limit_bytes <= 0:
    +            raise ValueError(
    +                "aof_rewrite_delta_limit_bytes must be positive"
    +            )
             if self.replica_queue_limit <= 0:
                 raise ValueError("replica_queue_limit must be positive")
             if self.replica_drain_grace_ms < 0:
    ```

Adds a positive maximum for concurrent rewrite delta bytes, turning slow rewrite memory growth into an explicit local failure.

??? note "File diff: src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 47019905cf08d4c97370b3451e5500a3c146091f..0414fb79e2083db83c34d5844d7b906e347ade38 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -42,7 +42,12 @@ from miniredis.core.outbound import (
     )
     from miniredis.core.planner import CommandPlanner
     from miniredis.core.reply import Failure
    -from miniredis.persistence.aof import AofFileOps, AofWriter
    +from miniredis.persistence.aof import (
    +    AofFileOps,
    +    AofRewriteFailed,
    +    AofRewriteOutcome,
    +    AofWriter,
    +)
     from miniredis.persistence.recovery import recover_database
     from miniredis.persistence.snapshot import (
         SnapshotFailed,
    @@ -88,6 +93,9 @@ class RuntimeStats:
         logical_memory_usage: int
         expired_key_count: int
         evicted_key_count: int
    +    aof_rewrite_active: bool
    +    aof_rewrite_delta_bytes: int
    +    aof_rewrite_checkpoint_seq: int | None


     @dataclass(slots=True)
    @@ -267,6 +275,9 @@ class MiniRedis:
                             self.config.aof_path,
                             self.config.aof_policy,
                             fsync_interval_seconds=(self.config.aof_fsync_interval_seconds),
    +                        rewrite_delta_limit_bytes=(
    +                            self.config.aof_rewrite_delta_limit_bytes
    +                        ),
                             ops=None if hooks is None else hooks.aof_ops,
                             sleep=(
                                 asyncio.sleep
    @@ -278,6 +289,9 @@ class MiniRedis:
                         await self._aof_writer.start()
                         self.commit_barrier = self._aof_writer
                         self.executor.set_commit_barrier_before_start(self._aof_writer)
    +                    self.executor.set_aof_rewrite_registration_before_start(
    +                        self._aof_writer.begin_rewrite
    +                    )
                     await self.executor.start()
                     if self.state is not RuntimeState.STARTING:
                         return
    @@ -698,6 +712,13 @@ class MiniRedis:
                 return SnapshotFailed("snapshot_path is not configured")
             return await self._snapshot_manager.save()

    +    async def rewrite_aof(self) -> AofRewriteOutcome:
    +        if self.state is not RuntimeState.RUNNING:
    +            return AofRewriteFailed("runtime is not running")
    +        if self._aof_writer is None:
    +            return AofRewriteFailed("aof_path is not configured")
    +        return await self.executor.begin_aof_rewrite()
    +
         def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
             return self.executor.debug_applied_batches()

    @@ -756,6 +777,20 @@ class MiniRedis:
                 logical_memory_usage=self.database.logical_usage,
                 expired_key_count=self.executor.expired_key_count,
                 evicted_key_count=self.executor.evicted_key_count,
    +            aof_rewrite_active=(
    +                self._aof_writer is not None
    +                and self._aof_writer.rewrite_active
    +            ),
    +            aof_rewrite_delta_bytes=(
    +                0
    +                if self._aof_writer is None
    +                else self._aof_writer.rewrite_delta_bytes
    +            ),
    +            aof_rewrite_checkpoint_seq=(
    +                None
    +                if self._aof_writer is None
    +                else self._aof_writer.rewrite_checkpoint_seq
    +            ),
             )

         def _debug_notify(self) -> None:
    ```

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

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/b9b363e...8cd6d5e)

After finishing, run `python -m journey.tools.build_journey check 26` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/26-online-aof-rewrite/stage.patch)

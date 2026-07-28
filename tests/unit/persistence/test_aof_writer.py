import asyncio
import os
import threading
from pathlib import Path

import pytest

from miniredis.config import MiniRedisConfig
from miniredis.core.commit import SnapshotImage
from miniredis.persistence.aof import (
    AofAppendFailed,
    AofAppendOk,
    AofPolicy,
    AofRewriteBusy,
    AofRewriteFailed,
    AofRewriteSaved,
    AofWriter,
    PosixAofFileOps,
    load_aof,
)
from miniredis.persistence.codec import encode_aof_record
from tests.unit.persistence.test_framing import batch


class ManualSleep:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, _delay: float) -> None:
        self.entered.set()
        await self.release.wait()
        self.release.clear()
        self.entered.clear()


@pytest.mark.asyncio
async def test_always_acknowledges_only_after_record_fsync(
    tmp_path,
    monkeypatch,
):
    fsync_calls: list[int] = []
    monkeypatch.setattr(os, "fsync", lambda fd: fsync_calls.append(fd))
    writer = AofWriter(tmp_path / "appendonly.mraof", AofPolicy.ALWAYS)
    await writer.start()
    fsync_calls.clear()

    outcome = await writer.append(batch(1))

    assert outcome == AofAppendOk(1)
    assert len(fsync_calls) == 1
    await writer.close()


@pytest.mark.asyncio
async def test_everysec_acknowledges_after_write_then_owned_loop_fsyncs(
    tmp_path,
    monkeypatch,
):
    sleep = ManualSleep()
    loop = asyncio.get_running_loop()
    fsync_seen = asyncio.Event()
    fsync_calls: list[int] = []

    def record_fsync(fd: int) -> None:
        fsync_calls.append(fd)
        loop.call_soon_threadsafe(fsync_seen.set)

    monkeypatch.setattr(os, "fsync", record_fsync)
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.EVERYSEC,
        sleep=sleep,
    )
    await writer.start()
    await sleep.entered.wait()
    fsync_calls.clear()
    fsync_seen.clear()

    assert await writer.append(batch(1)) == AofAppendOk(1)
    assert fsync_calls == []

    sleep.release.set()
    await fsync_seen.wait()
    assert len(fsync_calls) == 1
    await writer.close()


@pytest.mark.asyncio
async def test_no_policy_never_fsyncs_records_or_graceful_close(
    tmp_path,
    monkeypatch,
):
    fsync_calls: list[int] = []
    monkeypatch.setattr(os, "fsync", lambda fd: fsync_calls.append(fd))
    writer = AofWriter(tmp_path / "appendonly.mraof", AofPolicy.NO)
    await writer.start()
    fsync_calls.clear()

    assert await writer.append(batch(1)) == AofAppendOk(1)
    await writer.close()

    assert fsync_calls == []


class FailingWriteOps(PosixAofFileOps):
    def __init__(self) -> None:
        self.fail_records = False

    def write_all(self, fd: int, data: bytes) -> None:
        if self.fail_records:
            raise OSError("disk full")
        super().write_all(fd, data)


@pytest.mark.asyncio
async def test_worker_failure_settles_current_and_future_barriers(tmp_path):
    failures: list[BaseException] = []
    ops = FailingWriteOps()
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.NO,
        ops=ops,
        on_failure=failures.append,
    )
    await writer.start()
    ops.fail_records = True

    first = await writer.append(batch(1))
    second = await writer.append(batch(2))

    assert isinstance(first, AofAppendFailed)
    assert first.message == "disk full"
    assert isinstance(second, AofAppendFailed)
    assert len(failures) == 1
    await writer.close()


class FailingFsyncOps(PosixAofFileOps):
    def __init__(self) -> None:
        self.fail_fsync = False

    def fsync(self, fd: int) -> None:
        if self.fail_fsync:
            raise OSError("fsync failed")
        super().fsync(fd)


@pytest.mark.asyncio
async def test_everysec_background_fsync_failure_is_supervised(tmp_path):
    sleep = ManualSleep()
    failure_seen = asyncio.Event()
    failures: list[BaseException] = []
    ops = FailingFsyncOps()

    def record_failure(error: BaseException) -> None:
        failures.append(error)
        failure_seen.set()

    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.EVERYSEC,
        ops=ops,
        sleep=sleep,
        on_failure=record_failure,
    )
    await writer.start()
    await sleep.entered.wait()
    assert await writer.append(batch(1)) == AofAppendOk(1)
    ops.fail_fsync = True

    sleep.release.set()
    await failure_seen.wait()
    assert len(failures) == 1

    later = await writer.append(batch(2))
    assert isinstance(later, AofAppendFailed)
    assert later.message == "fsync failed"
    await writer.close()


class ConcurrentWriteAndFsyncFailureOps(PosixAofFileOps):
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self.block_record = False
        self.fail_fsync = False
        self.write_entered = asyncio.Event()
        self.release_write = threading.Event()

    def write_all(self, fd: int, data: bytes) -> None:
        if self.block_record:
            self._loop.call_soon_threadsafe(self.write_entered.set)
            self.release_write.wait()
        super().write_all(fd, data)

    def fsync(self, fd: int) -> None:
        if self.fail_fsync:
            raise OSError("background fsync failed")
        super().fsync(fd)


@pytest.mark.asyncio
async def test_background_fsync_failure_fails_a_concurrent_append(tmp_path):
    loop = asyncio.get_running_loop()
    sleep = ManualSleep()
    failure_seen = asyncio.Event()
    ops = ConcurrentWriteAndFsyncFailureOps(loop)
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.EVERYSEC,
        ops=ops,
        sleep=sleep,
        on_failure=lambda _error: failure_seen.set(),
    )
    await writer.start()
    await sleep.entered.wait()
    assert await writer.append(batch(1)) == AofAppendOk(1)

    ops.block_record = True
    current = asyncio.create_task(writer.append(batch(2)))
    await ops.write_entered.wait()
    ops.fail_fsync = True
    sleep.release.set()
    await failure_seen.wait()
    ops.release_write.set()

    outcome = await current
    assert isinstance(outcome, AofAppendFailed)
    assert outcome.message == "background fsync failed"
    assert isinstance(await writer.append(batch(3)), AofAppendFailed)
    await writer.close()


class GateRewriteOps(PosixAofFileOps):
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._rewrite_fd: int | None = None
        self.rewrite_paths: list[Path] = []
        self.base_entered = asyncio.Event()
        self.release_base = threading.Event()

    def open_rewrite(self, path: Path) -> int:
        self.rewrite_paths.append(path)
        fd = super().open_rewrite(path)
        self._rewrite_fd = fd
        return fd

    def write_all(self, fd: int, data: bytes) -> None:
        if fd == self._rewrite_fd:
            self._loop.call_soon_threadsafe(self.base_entered.set)
            self.release_base.wait()
        super().write_all(fd, data)


@pytest.mark.asyncio
async def test_begin_rewrite_registers_before_next_append(tmp_path):
    ops = GateRewriteOps(asyncio.get_running_loop())
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
        ops=ops,
    )
    await writer.start()

    job = writer.begin_rewrite(SnapshotImage(0, ()))

    assert writer.rewrite_active
    await ops.base_entered.wait()
    assert await writer.append(batch(1)) == AofAppendOk(1)
    assert writer.rewrite_delta_bytes == len(encode_aof_record(batch(1)))
    ops.release_base.set()
    assert await job == AofRewriteSaved(
        tmp_path / "appendonly.mraof",
        0,
    )
    await writer.close()


@pytest.mark.asyncio
async def test_second_rewrite_is_busy_while_base_is_active(tmp_path):
    ops = GateRewriteOps(asyncio.get_running_loop())
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
        ops=ops,
    )
    await writer.start()
    first = writer.begin_rewrite(SnapshotImage(0, ()))
    await ops.base_entered.wait()

    assert await writer.begin_rewrite(SnapshotImage(0, ())) == AofRewriteBusy()

    ops.release_base.set()
    assert isinstance(await first, AofRewriteSaved)
    await writer.close()


@pytest.mark.asyncio
async def test_rewrite_is_disabled_until_writer_is_started(tmp_path):
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
    )

    outcome = await writer.begin_rewrite(SnapshotImage(0, ()))

    assert outcome == AofRewriteFailed("AOF writer is not accepting")


@pytest.mark.asyncio
async def test_rewrite_delta_overflow_does_not_fail_authoritative_append(
    tmp_path,
):
    ops = GateRewriteOps(asyncio.get_running_loop())
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=1,
        ops=ops,
    )
    await writer.start()
    job = writer.begin_rewrite(SnapshotImage(0, ()))
    await ops.base_entered.wait()

    assert await writer.append(batch(1)) == AofAppendOk(1)
    assert await job == AofRewriteFailed("AOF rewrite delta limit exceeded")
    assert writer.failure is None

    ops.release_base.set()
    await writer.close()
    assert not tuple(tmp_path.glob("*.tmp"))


class FailingRewriteOpenOps(PosixAofFileOps):
    def open_rewrite(self, path: Path) -> int:
        raise OSError("cannot create rewrite")


@pytest.mark.asyncio
async def test_rewrite_base_failure_leaves_writer_available(tmp_path):
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
        ops=FailingRewriteOpenOps(),
    )
    await writer.start()

    outcome = await writer.begin_rewrite(SnapshotImage(0, ()))

    assert outcome == AofRewriteFailed("cannot create rewrite")
    assert await writer.append(batch(1)) == AofAppendOk(1)
    await writer.close()


@pytest.mark.asyncio
async def test_successive_rewrites_use_unique_temporary_paths(tmp_path):
    ops = GateRewriteOps(asyncio.get_running_loop())
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
        ops=ops,
    )
    await writer.start()
    first = writer.begin_rewrite(SnapshotImage(0, ()))
    await ops.base_entered.wait()
    ops.release_base.set()
    assert isinstance(await first, AofRewriteSaved)

    ops.release_base.clear()
    ops.base_entered.clear()
    second = writer.begin_rewrite(SnapshotImage(0, ()))
    await ops.base_entered.wait()
    ops.release_base.set()
    assert isinstance(await second, AofRewriteSaved)

    assert len(ops.rewrite_paths) == 2
    assert ops.rewrite_paths[0] != ops.rewrite_paths[1]
    await writer.close()


@pytest.mark.parametrize("limit", [0, -1])
def test_rewrite_delta_limit_must_be_positive(limit):
    with pytest.raises(
        ValueError,
        match="aof_rewrite_delta_limit_bytes must be positive",
    ):
        MiniRedisConfig(aof_rewrite_delta_limit_bytes=limit)


class RewriteFailureOps(PosixAofFileOps):
    def __init__(self) -> None:
        self.rewrite_fd: int | None = None
        self.fail_temp_write = False
        self.fail_temp_fsync = False
        self.fail_replace = False
        self.fail_parent_fsync = False
        self.writes: list[tuple[int, bytes]] = []

    def open_rewrite(self, path: Path) -> int:
        fd = super().open_rewrite(path)
        self.rewrite_fd = fd
        return fd

    def write_all(self, fd: int, data: bytes) -> None:
        self.writes.append((fd, data))
        if fd == self.rewrite_fd and self.fail_temp_write:
            raise OSError("temp write failed")
        super().write_all(fd, data)

    def fsync(self, fd: int) -> None:
        if fd == self.rewrite_fd and self.fail_temp_fsync:
            raise OSError("temp fsync failed")
        super().fsync(fd)

    def replace(self, source: Path, destination: Path) -> None:
        if self.fail_replace:
            raise OSError("replace failed")
        super().replace(source, destination)

    def fsync_parent(self, path: Path) -> None:
        if self.fail_parent_fsync:
            raise OSError("parent fsync failed")
        super().fsync_parent(path)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "message"),
    [
        ("fail_temp_write", "temp write failed"),
        ("fail_temp_fsync", "temp fsync failed"),
        ("fail_replace", "replace failed"),
    ],
)
async def test_pre_rename_rewrite_failure_keeps_old_aof_writable(
    tmp_path,
    failure,
    message,
):
    path = tmp_path / "appendonly.mraof"
    ops = RewriteFailureOps()
    writer = AofWriter(
        path,
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
        ops=ops,
    )
    await writer.start()
    assert await writer.append(batch(1)) == AofAppendOk(1)
    setattr(ops, failure, True)

    outcome = await writer.begin_rewrite(SnapshotImage(1, ()))

    assert outcome == AofRewriteFailed(message)
    setattr(ops, failure, False)
    assert await writer.append(batch(2)) == AofAppendOk(2)
    assert writer.failure is None
    await writer.close()
    assert load_aof(path, repair_truncated_tail=False).batches == (
        batch(1),
        batch(2),
    )


@pytest.mark.asyncio
async def test_paused_base_delta_is_ordered_into_rewritten_file(tmp_path):
    path = tmp_path / "appendonly.mraof"
    ops = GateRewriteOps(asyncio.get_running_loop())
    writer = AofWriter(
        path,
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
        ops=ops,
    )
    await writer.start()
    job = writer.begin_rewrite(SnapshotImage(0, ()))
    await ops.base_entered.wait()

    assert await writer.append(batch(1)) == AofAppendOk(1)
    ops.release_base.set()
    assert isinstance(await job, AofRewriteSaved)
    await writer.close()

    log = load_aof(path, repair_truncated_tail=False)
    assert log.state_base == SnapshotImage(0, ())
    assert log.batches == (batch(1),)


@pytest.mark.asyncio
async def test_parent_fsync_failure_after_rename_is_terminal(tmp_path):
    failures: list[BaseException] = []
    ops = RewriteFailureOps()
    ops.fail_parent_fsync = True
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
        ops=ops,
        on_failure=failures.append,
    )
    await writer.start()

    outcome = await writer.begin_rewrite(SnapshotImage(0, ()))

    assert outcome == AofRewriteFailed("parent fsync failed")
    assert len(failures) == 1
    assert str(writer.failure) == "parent fsync failed"
    later = await writer.append(batch(1))
    assert later == AofAppendFailed("parent fsync failed")
    await writer.close()


@pytest.mark.asyncio
async def test_append_after_successful_rewrite_uses_new_descriptor(tmp_path):
    ops = RewriteFailureOps()
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
        ops=ops,
    )
    await writer.start()
    assert isinstance(
        await writer.begin_rewrite(SnapshotImage(0, ())),
        AofRewriteSaved,
    )
    assert ops.rewrite_fd is not None

    assert await writer.append(batch(1)) == AofAppendOk(1)

    assert ops.writes[-1] == (ops.rewrite_fd, encode_aof_record(batch(1)))
    await writer.close()


@pytest.mark.asyncio
async def test_graceful_close_waits_for_active_rewrite(tmp_path):
    ops = GateRewriteOps(asyncio.get_running_loop())
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
        ops=ops,
    )
    await writer.start()
    job = writer.begin_rewrite(SnapshotImage(0, ()))
    await ops.base_entered.wait()

    closing = asyncio.create_task(writer.close())
    await asyncio.sleep(0)
    assert not closing.done()
    ops.release_base.set()
    assert isinstance(await job, AofRewriteSaved)
    await closing

    assert writer.owned_task_count == 0


@pytest.mark.asyncio
async def test_crash_close_aborts_rewrite_and_cleans_temp(tmp_path):
    ops = GateRewriteOps(asyncio.get_running_loop())
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
        ops=ops,
    )
    await writer.start()
    job = writer.begin_rewrite(SnapshotImage(0, ()))
    await ops.base_entered.wait()

    crashing = asyncio.create_task(writer.crash_close())
    await asyncio.sleep(0)
    assert not crashing.done()
    ops.release_base.set()
    await crashing

    assert await job == AofRewriteFailed("AOF writer crashed during rewrite")
    assert writer.owned_task_count == 0
    assert not tuple(tmp_path.glob("*.tmp"))

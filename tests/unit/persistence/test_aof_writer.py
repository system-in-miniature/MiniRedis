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

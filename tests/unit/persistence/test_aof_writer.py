import asyncio
import os
import threading

import pytest

from miniredis.persistence.aof import (
    AofAppendFailed,
    AofAppendOk,
    AofPolicy,
    AofWriter,
    PosixAofFileOps,
)
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

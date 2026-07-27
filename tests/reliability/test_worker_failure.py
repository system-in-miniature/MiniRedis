import asyncio

import pytest

from miniredis import CommandRequest
from miniredis.config import MiniRedisConfig
from miniredis.core.reply import Ok
from miniredis.persistence.aof import (
    AofPolicy,
    PosixAofFileOps,
)
from tests.helpers.runtime import open_test_runtime


class ManualAofSleep:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, _delay: float) -> None:
        self.entered.set()
        await self.release.wait()


class FailingFsyncOps(PosixAofFileOps):
    def __init__(self) -> None:
        self.fail_fsync = False

    def fsync(self, fd: int) -> None:
        if self.fail_fsync:
            raise OSError("fsync failed")
        super().fsync(fd)


@pytest.mark.asyncio
async def test_background_fsync_failure_terminalizes_pending_work(tmp_path):
    sleep = ManualAofSleep()
    ops = FailingFsyncOps()
    runtime = await open_test_runtime(
        config=MiniRedisConfig(
            aof_path=tmp_path / "appendonly.mraof",
            aof_policy=AofPolicy.EVERYSEC,
        ),
        aof_ops=ops,
        aof_sleep=sleep,
    )
    await sleep.entered.wait()
    client = runtime.direct_client()
    assert await client.execute(CommandRequest(b"SET", (b"dirty", b"1"))) == Ok()
    blocked = asyncio.create_task(
        client.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
    )
    await runtime.debug_wait_for_waiters(1)
    ops.fail_fsync = True
    sleep.release.set()
    await runtime.debug_wait_for_failure()

    reply = await blocked
    assert reply.code == "ERR"
    assert "runtime failed" in reply.message
    stats = runtime.debug_stats()
    assert stats.accepted_requests == 0
    assert stats.waiters == 0
    assert stats.pending_futures == 0
    await runtime.close()
    closed = runtime.debug_stats()
    assert closed.owned_tasks == 0
    assert closed.snapshot_jobs == 0
    assert closed.replica_links == 0


@pytest.mark.asyncio
async def test_unexpected_executor_death_uses_one_fallback_drain():
    runtime = await open_test_runtime()
    blocked = asyncio.create_task(
        runtime.direct_client().execute(CommandRequest(b"BLPOP", (b"q", b"0")))
    )
    await runtime.debug_wait_for_waiters(1)

    runtime.debug_fail_executor(RuntimeError("executor died"))
    await runtime.debug_wait_for_failure()

    reply = await blocked
    assert reply.code == "ERR"
    stats = runtime.debug_stats()
    assert stats.accepted_requests == 0
    assert stats.waiters == 0
    assert stats.pending_futures == 0
    await runtime.close()
    closed = runtime.debug_stats()
    assert closed.owned_tasks == 0
    assert closed.snapshot_jobs == 0
    assert closed.replica_links == 0

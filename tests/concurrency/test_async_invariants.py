import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Failure, Items, Number
from tests.helpers.time import FakeClock, ManualScheduler


@pytest.mark.asyncio
async def test_push_then_cancel_consumes_once_and_cancel_is_stale():
    async with MiniRedis.open(outbox_drain_grace_ms=0) as runtime:
        waiter = runtime.direct_client()
        producer = runtime.direct_client()
        blocked = asyncio.create_task(
            waiter.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
        )
        await runtime.debug_wait_for_waiters(1)
        assert await producer.execute(
            CommandRequest(b"RPUSH", (b"q", b"x"))
        ) == Number(1)
        assert await blocked == Items((Bytes(b"q"), Bytes(b"x")))
        blocked.cancel()
        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(
            None
        )


@pytest.mark.asyncio
async def test_push_then_session_close_keeps_consumption():
    async with MiniRedis.open(outbox_drain_grace_ms=0) as runtime:
        waiter = runtime.direct_client()
        producer = runtime.direct_client()
        blocked = asyncio.create_task(
            waiter.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
        )
        await runtime.debug_wait_for_waiters(1)
        await producer.execute(CommandRequest(b"RPUSH", (b"q", b"x")))
        assert await blocked == Items((Bytes(b"q"), Bytes(b"x")))
        await waiter.close()
        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(
            None
        )


@pytest.mark.asyncio
async def test_push_before_shutdown_completes_and_post_barrier_push_is_closed():
    runtime = MiniRedis.open(outbox_drain_grace_ms=0)
    await runtime.start()
    client = runtime.direct_client()
    assert await client.execute(
        CommandRequest(b"RPUSH", (b"q", b"x"))
    ) == Number(1)
    await runtime.close()
    assert await client.execute(
        CommandRequest(b"RPUSH", (b"q", b"y"))
    ) == Failure("CLOSED", "runtime is not accepting commands")


@pytest.mark.asyncio
async def test_close_clears_all_async_owned_resources():
    clock = FakeClock()
    scheduler = ManualScheduler(clock)
    runtime = MiniRedis.open(
        clock=clock,
        scheduler=scheduler,
        active_expire_interval_ms=100,
        outbox_drain_grace_ms=0,
    )
    await runtime.start()
    subscriber = runtime.direct_client()
    waiter = runtime.direct_client()
    await subscriber.execute(CommandRequest(b"SUBSCRIBE", (b"c",)))
    blocked = asyncio.create_task(
        waiter.execute(CommandRequest(b"BLPOP", (b"q", b"5")))
    )
    await runtime.debug_wait_for_waiters(1)
    await runtime.close()
    assert await blocked == Failure("CLOSED", "runtime closed")
    stats = runtime.debug_stats()
    assert stats.pending_futures == 0
    assert stats.waiters == 0
    assert stats.subscriptions == 0
    assert stats.sessions == 0
    assert stats.timer_handles == 0
    assert stats.owned_tasks == 0
    assert runtime.debug_waiter_index_counts == (0, 0, 0)
    assert scheduler.pending_count == 0

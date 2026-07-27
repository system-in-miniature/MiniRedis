import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.outbound import OutboxClosed
from miniredis.core.reply import Bytes, Items, Number
from tests.helpers.time import FakeClock, ManualScheduler


async def blocked(client, runtime):
    task = asyncio.create_task(client.execute(CommandRequest(b"BLPOP", (b"q", b"5"))))
    await runtime.debug_wait_for_waiters(1)
    return task


@pytest.mark.asyncio
async def test_clock_advance_alone_does_not_fire_timeout():
    clock = FakeClock()
    scheduler = ManualScheduler(clock)
    async with MiniRedis.open(clock=clock, scheduler=scheduler) as runtime:
        task = await blocked(runtime.direct_client(), runtime)
        clock.advance(5_000)
        assert not task.done()
        scheduler.fire_due()
        assert await task == Bytes(None)
        assert runtime.debug_waiter_index_counts == (0, 0, 0)


@pytest.mark.asyncio
async def test_timeout_then_push_leaves_item_but_push_then_timeout_consumes():
    clock = FakeClock()
    scheduler = ManualScheduler(clock)
    async with MiniRedis.open(clock=clock, scheduler=scheduler) as runtime:
        producer = runtime.direct_client()
        first = await blocked(runtime.direct_client(), runtime)
        clock.advance(5_000)
        scheduler.fire_due()
        assert await first == Bytes(None)
        assert runtime.debug_waiter_index_counts == (0, 0, 0)
        assert await producer.execute(CommandRequest(b"RPUSH", (b"q", b"a"))) == Number(
            1
        )
        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"a")

        second = await blocked(runtime.direct_client(), runtime)
        push = await producer.execute(CommandRequest(b"RPUSH", (b"q", b"b")))
        clock.advance(5_000)
        scheduler.fire_due()
        assert push == Number(1)
        assert await second == Items((Bytes(b"q"), Bytes(b"b")))


@pytest.mark.asyncio
async def test_cancel_and_session_close_are_mailbox_ordered():
    async with MiniRedis.open() as runtime:
        producer = runtime.direct_client()
        cancelled_client = runtime.direct_client()
        cancelled = await blocked(cancelled_client, runtime)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        await runtime.debug_wait_for_waiters(0)
        assert runtime.debug_waiter_index_counts == (0, 0, 0)
        await producer.execute(CommandRequest(b"RPUSH", (b"q", b"c")))
        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"c")

        closed_client = runtime.direct_client()
        closed = await blocked(closed_client, runtime)
        await closed_client.close()
        assert await closed == Bytes(None)
        assert runtime.debug_waiter_index_counts == (0, 0, 0)
        await producer.execute(CommandRequest(b"RPUSH", (b"q", b"d")))
        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"d")


@pytest.mark.asyncio
async def test_session_close_wakes_pending_outbox_receive():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()
        receiving = asyncio.create_task(client.receive())
        await client.close()
        with pytest.raises(OutboxClosed, match="session closed"):
            await receiving

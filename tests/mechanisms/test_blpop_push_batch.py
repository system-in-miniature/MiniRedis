import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.commit import DeleteKey
from miniredis.core.reply import Bytes, Items, Number


@pytest.mark.asyncio
async def test_full_push_then_fifo_pops_are_one_commit_batch():
    async with MiniRedis.open(
        debug_record_applied_batches=True
    ) as runtime:
        first_client = runtime.direct_client()
        second_client = runtime.direct_client()
        producer = runtime.direct_client()
        first = asyncio.create_task(
            first_client.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
        )
        second = asyncio.create_task(
            second_client.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
        )
        await runtime.debug_wait_for_waiters(2)
        before = runtime.debug_commit_seq
        assert await producer.execute(
            CommandRequest(b"RPUSH", (b"q", b"a", b"b"))
        ) == Number(2)
        assert await first == Items((Bytes(b"q"), Bytes(b"a")))
        assert await second == Items((Bytes(b"q"), Bytes(b"b")))
        assert runtime.debug_waiter_index_counts == (0, 0, 0)
        assert runtime.debug_commit_seq == before + 1
        batch = runtime.debug_applied_batches()[-1]
        assert len(batch.operations) == 1
        assert isinstance(batch.operations[0], DeleteKey)


@pytest.mark.asyncio
async def test_lpush_order_is_observed_after_the_complete_push():
    async with MiniRedis.open() as runtime:
        waiter = runtime.direct_client()
        producer = runtime.direct_client()
        blocked = asyncio.create_task(
            waiter.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
        )
        await runtime.debug_wait_for_waiters(1)
        assert await producer.execute(
            CommandRequest(b"LPUSH", (b"q", b"a", b"b"))
        ) == Number(2)
        assert await blocked == Items((Bytes(b"q"), Bytes(b"b")))
        assert runtime.debug_waiter_index_counts == (0, 0, 0)
        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"a")

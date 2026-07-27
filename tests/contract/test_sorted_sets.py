import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Failure, Items, Number


@pytest.mark.asyncio
async def test_zset_orders_equal_scores_by_member_bytes():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        assert await c.execute(
            CommandRequest(b"ZADD", (b"z", b"1", b"b", b"1", b"a", b"2", b"c"))
        ) == Number(3)
        assert await c.execute(CommandRequest(b"ZRANGE", (b"z", b"0", b"-1"))) == Items(
            (Bytes(b"a"), Bytes(b"b"), Bytes(b"c"))
        )
        assert await c.execute(CommandRequest(b"ZRANK", (b"z", b"b"))) == Number(1)
        assert await c.execute(
            CommandRequest(b"ZRANGEBYSCORE", (b"z", b"(1", b"+inf"))
        ) == Items((Bytes(b"c"),))


@pytest.mark.asyncio
async def test_nan_in_later_pair_prevents_all_mutation():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        before = runtime.debug_commit_seq
        reply = await c.execute(
            CommandRequest(b"ZADD", (b"z", b"1", b"a", b"nan", b"b"))
        )
        assert isinstance(reply, Failure)
        assert runtime.debug_commit_seq == before
        assert await c.execute(CommandRequest(b"ZRANGE", (b"z", b"0", b"-1"))) == Items(
            ()
        )


@pytest.mark.asyncio
async def test_zscore_zrem_and_empty_key_removal():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"ZADD", (b"z", b"1.5", b"a")))
        assert await c.execute(CommandRequest(b"ZSCORE", (b"z", b"a"))) == Bytes(b"1.5")
        assert await c.execute(
            CommandRequest(b"ZREM", (b"z", b"a", b"missing"))
        ) == Number(1)
        assert await c.execute(CommandRequest(b"TYPE", (b"z",))) == Bytes(b"none")

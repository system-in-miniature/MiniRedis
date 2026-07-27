import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Failure, Items, Number


@pytest.mark.asyncio
async def test_list_push_pop_and_range():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        assert await c.execute(
            CommandRequest(b"LPUSH", (b"l", b"a", b"b", b"c"))
        ) == Number(3)
        assert await c.execute(CommandRequest(b"LRANGE", (b"l", b"0", b"-1"))) == Items(
            (Bytes(b"c"), Bytes(b"b"), Bytes(b"a"))
        )
        assert await c.execute(CommandRequest(b"RPOP", (b"l",))) == Bytes(b"a")
        assert await c.execute(
            CommandRequest(b"LRANGE", (b"l", b"-99", b"99"))
        ) == Items((Bytes(b"c"), Bytes(b"b")))


@pytest.mark.asyncio
async def test_rpush_lpop_and_last_element_removal():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        assert await c.execute(CommandRequest(b"RPUSH", (b"q", b"a", b"b"))) == Number(
            2
        )
        assert await c.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"a")
        assert await c.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"b")
        assert await c.execute(CommandRequest(b"TYPE", (b"q",))) == Bytes(b"none")
        assert await c.execute(CommandRequest(b"RPOP", (b"missing",))) == Bytes(None)


@pytest.mark.asyncio
async def test_list_wrongtype_and_range_boundaries_are_side_effect_safe():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SET", (b"string", b"value")))
        before = runtime.debug_commit_seq
        assert await c.execute(
            CommandRequest(b"LPUSH", (b"string", b"item"))
        ) == Failure(
            "WRONGTYPE",
            "operation against a key holding the wrong kind of value",
        )
        assert runtime.debug_commit_seq == before

        assert await c.execute(
            CommandRequest(b"RPUSH", (b"l", b"a", b"b", b"c"))
        ) == Number(3)
        assert await c.execute(CommandRequest(b"LRANGE", (b"l", b"2", b"1"))) == Items(
            ()
        )
        assert await c.execute(
            CommandRequest(b"LRANGE", (b"l", b"-1", b"-1"))
        ) == Items((Bytes(b"c"),))
        assert await c.execute(
            CommandRequest(b"LRANGE", (b"l", b"0", b"-99"))
        ) == Items(())

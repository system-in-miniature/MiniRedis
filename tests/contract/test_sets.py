import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Failure, Items, Number


@pytest.mark.asyncio
async def test_set_counts_and_membership():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        assert await c.execute(
            CommandRequest(b"SADD", (b"s", b"a", b"a", b"b"))
        ) == Number(2)
        assert await c.execute(CommandRequest(b"SISMEMBER", (b"s", b"a"))) == Number(1)
        assert await c.execute(CommandRequest(b"SREM", (b"s", b"a", b"x"))) == Number(1)


@pytest.mark.asyncio
async def test_sinter_does_not_hide_later_wrongtype_after_missing_key():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SET", (b"wrong", b"x")))
        reply = await c.execute(CommandRequest(b"SINTER", (b"missing", b"wrong")))
        assert isinstance(reply, Failure)
        assert reply.code == "WRONGTYPE"


@pytest.mark.asyncio
async def test_smembers_sinter_and_last_member_removal():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SADD", (b"a", b"x", b"y")))
        await c.execute(CommandRequest(b"SADD", (b"b", b"y", b"z")))
        assert await c.execute(CommandRequest(b"SMEMBERS", (b"a",))) == Items(
            (Bytes(b"x"), Bytes(b"y"))
        )
        assert await c.execute(CommandRequest(b"SINTER", (b"a", b"b"))) == Items(
            (Bytes(b"y"),)
        )
        assert await c.execute(CommandRequest(b"SREM", (b"b", b"y", b"z"))) == Number(2)
        assert await c.execute(CommandRequest(b"TYPE", (b"b",))) == Bytes(b"none")

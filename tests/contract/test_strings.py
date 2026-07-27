import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Failure, Number, Ok


@pytest.mark.asyncio
async def test_set_conditions_replace_type_and_clear_old_state():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        assert await c.execute(CommandRequest(b"SET", (b"k", b"1"))) == Ok()
        before = runtime.debug_commit_seq
        assert await c.execute(CommandRequest(b"SET", (b"k", b"2", b"NX"))) == Bytes(
            None
        )
        assert runtime.debug_commit_seq == before
        assert await c.execute(CommandRequest(b"SET", (b"k", b"2", b"XX"))) == Ok()
        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(b"2")


@pytest.mark.asyncio
async def test_invalid_integer_and_overflow_do_not_commit():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SET", (b"k", b"01")))
        before = runtime.debug_commit_seq
        assert isinstance(await c.execute(CommandRequest(b"INCR", (b"k",))), Failure)
        assert runtime.debug_commit_seq == before
        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(b"01")

        maximum = b"9223372036854775807"
        assert await c.execute(CommandRequest(b"SET", (b"k", maximum))) == Ok()
        before = runtime.debug_commit_seq
        assert await c.execute(CommandRequest(b"INCR", (b"k",))) == Failure(
            "ERR", "value is not an integer or out of range"
        )
        assert runtime.debug_commit_seq == before
        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(maximum)

        minimum = b"-9223372036854775808"
        assert await c.execute(CommandRequest(b"SET", (b"k", minimum))) == Ok()
        before = runtime.debug_commit_seq
        assert await c.execute(CommandRequest(b"INCRBY", (b"k", b"-1"))) == Failure(
            "ERR", "value is not an integer or out of range"
        )
        assert runtime.debug_commit_seq == before
        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(minimum)


@pytest.mark.asyncio
async def test_general_commands_and_incrby_cover_the_frozen_subset():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        assert await c.execute(CommandRequest(b"PING")) == Ok(b"PONG")
        assert await c.execute(CommandRequest(b"PING", (b"\x00pong",))) == Bytes(
            b"\x00pong"
        )
        assert await c.execute(CommandRequest(b"ECHO", (b"\xff",))) == Bytes(b"\xff")
        assert await c.execute(CommandRequest(b"SET", (b"k", b"1"))) == Ok()
        assert await c.execute(
            CommandRequest(b"EXISTS", (b"k", b"k", b"missing"))
        ) == Number(2)
        assert await c.execute(CommandRequest(b"TYPE", (b"k",))) == Bytes(b"string")
        assert await c.execute(CommandRequest(b"INCRBY", (b"k", b"4"))) == Number(5)
        assert await c.execute(CommandRequest(b"DEL", (b"k", b"k"))) == Number(1)
        assert await c.execute(CommandRequest(b"TYPE", (b"k",))) == Bytes(b"none")

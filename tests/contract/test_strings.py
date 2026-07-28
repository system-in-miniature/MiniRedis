import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Failure, Items, Number, Ok
from tests.helpers.time import FakeClock


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


@pytest.mark.asyncio
async def test_mget_is_ordered_and_treats_non_strings_as_null():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        assert await c.execute(CommandRequest(b"SET", (b"s", b"v"))) == Ok()
        assert await c.execute(CommandRequest(b"LPUSH", (b"list", b"x"))) == Number(1)

        assert await c.execute(
            CommandRequest(b"MGET", (b"s", b"missing", b"list", b"s"))
        ) == Items((Bytes(b"v"), Bytes(None), Bytes(None), Bytes(b"v")))


@pytest.mark.asyncio
async def test_mget_expired_key_is_logically_missing_without_commit():
    clock = FakeClock(0)
    async with MiniRedis.open(clock=clock) as runtime:
        c = runtime.direct_client()
        assert await c.execute(
            CommandRequest(b"SET", (b"k", b"v", b"PX", b"1"))
        ) == Ok()
        clock.advance(1)
        before = runtime.debug_commit_seq

        assert await c.execute(CommandRequest(b"MGET", (b"k",))) == Items(
            (Bytes(None),)
        )
        assert runtime.debug_commit_seq == before


@pytest.mark.asyncio
async def test_mset_is_one_atomic_commit_and_last_duplicate_wins():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        assert await c.execute(CommandRequest(b"LPUSH", (b"a", b"old"))) == Number(1)
        assert await c.execute(
            CommandRequest(b"SET", (b"b", b"old", b"PX", b"1000"))
        ) == Ok()
        before = runtime.debug_commit_seq

        assert await c.execute(
            CommandRequest(b"MSET", (b"a", b"1", b"a", b"2", b"b", b"3"))
        ) == Ok()

        assert runtime.debug_commit_seq == before + 1
        assert await c.execute(CommandRequest(b"MGET", (b"a", b"b"))) == Items(
            (Bytes(b"2"), Bytes(b"3"))
        )
        assert await c.execute(CommandRequest(b"PTTL", (b"b",))) == Number(-1)


@pytest.mark.asyncio
async def test_decr_reuses_integer_and_ttl_semantics():
    clock = FakeClock(0)
    async with MiniRedis.open(clock=clock) as runtime:
        c = runtime.direct_client()
        assert await c.execute(
            CommandRequest(b"SET", (b"k", b"2", b"PX", b"1000"))
        ) == Ok()

        assert await c.execute(CommandRequest(b"DECR", (b"k",))) == Number(1)
        assert await c.execute(CommandRequest(b"PTTL", (b"k",))) == Number(1000)

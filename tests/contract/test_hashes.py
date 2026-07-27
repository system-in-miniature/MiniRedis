import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Failure, Items, Number


@pytest.mark.asyncio
async def test_hash_semantics_and_last_field_removal():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        assert await c.execute(
            CommandRequest(b"HSET", (b"h", b"a", b"1", b"a", b"2", b"b", b"3"))
        ) == Number(2)
        assert await c.execute(CommandRequest(b"HGET", (b"h", b"a"))) == Bytes(b"2")
        assert await c.execute(
            CommandRequest(b"HINCRBY", (b"h", b"a", b"5"))
        ) == Number(7)
        assert await c.execute(CommandRequest(b"HDEL", (b"h", b"a", b"b"))) == Number(2)
        assert await c.execute(CommandRequest(b"TYPE", (b"h",))) == Bytes(b"none")


@pytest.mark.asyncio
async def test_hash_integer_error_is_atomic():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        assert await c.execute(CommandRequest(b"HSET", (b"h", b"f", b"01"))) == Number(
            1
        )
        before = runtime.debug_commit_seq
        reply = await c.execute(CommandRequest(b"HINCRBY", (b"h", b"f", b"1")))
        assert reply == Failure("ERR", "value is not an integer or out of range")
        assert runtime.debug_commit_seq == before
        assert await c.execute(CommandRequest(b"HGET", (b"h", b"f"))) == Bytes(b"01")


@pytest.mark.asyncio
async def test_hgetall_is_alternating_and_missing_fields_are_nil():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"HSET", (b"h", b"b", b"2", b"a", b"1")))
        reply = await c.execute(CommandRequest(b"HGETALL", (b"h",)))
        assert isinstance(reply, Items)
        assert {
            (reply.values[index].value, reply.values[index + 1].value)
            for index in range(0, len(reply.values), 2)
        } == {(b"a", b"1"), (b"b", b"2")}
        assert await c.execute(CommandRequest(b"HGET", (b"h", b"missing"))) == Bytes(
            None
        )


@pytest.mark.asyncio
async def test_hash_wrongtype_overflow_and_noop_delete_do_not_commit():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SET", (b"string", b"value")))
        before = runtime.debug_commit_seq
        assert await c.execute(
            CommandRequest(b"HSET", (b"string", b"field", b"value"))
        ) == Failure(
            "WRONGTYPE",
            "operation against a key holding the wrong kind of value",
        )
        assert runtime.debug_commit_seq == before

        maximum = b"9223372036854775807"
        assert await c.execute(
            CommandRequest(b"HSET", (b"h", b"field", maximum))
        ) == Number(1)
        before = runtime.debug_commit_seq
        assert await c.execute(
            CommandRequest(b"HINCRBY", (b"h", b"field", b"1"))
        ) == Failure("ERR", "value is not an integer or out of range")
        assert runtime.debug_commit_seq == before
        assert await c.execute(CommandRequest(b"HGET", (b"h", b"field"))) == Bytes(
            maximum
        )

        before_tick = runtime.database.entries[b"h"].last_access_tick
        assert await c.execute(
            CommandRequest(b"HDEL", (b"h", b"missing", b"missing"))
        ) == Number(0)
        assert runtime.debug_commit_seq == before
        assert runtime.database.entries[b"h"].last_access_tick > before_tick

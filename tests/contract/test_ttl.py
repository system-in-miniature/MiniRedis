import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.commit import CommitTrigger, DeleteKey, DeleteReason
from miniredis.core.reply import Bytes, Failure, Number, Ok
from tests.helpers.time import FakeClock


@pytest.mark.asyncio
async def test_set_px_is_lazy_invisible_and_set_replacement_clears_ttl():
    clock = FakeClock(1_000)
    async with MiniRedis.open(clock=clock) as runtime:
        c = runtime.direct_client()
        assert (
            await c.execute(CommandRequest(b"SET", (b"k", b"v", b"PX", b"100"))) == Ok()
        )
        assert await c.execute(CommandRequest(b"PTTL", (b"k",))) == Number(100)
        clock.advance(100)
        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(None)
        assert await c.execute(CommandRequest(b"PTTL", (b"k",))) == Number(-2)
        await c.execute(CommandRequest(b"SET", (b"k", b"v", b"PX", b"100")))
        await c.execute(CommandRequest(b"SET", (b"k", b"new")))
        assert await c.execute(CommandRequest(b"TTL", (b"k",))) == Number(-1)
        assert await c.execute(CommandRequest(b"EXPIRE", (b"k", b"0"))) == Number(1)
        assert await c.execute(CommandRequest(b"PTTL", (b"k",))) == Number(-2)


@pytest.mark.asyncio
async def test_expire_ttl_persist_and_bounded_active_cleanup():
    clock = FakeClock(10_000)
    async with MiniRedis.open(
        clock=clock,
        active_expire_sample_size=1,
    ) as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SET", (b"a", b"1")))
        await c.execute(CommandRequest(b"SET", (b"b", b"2")))
        assert await c.execute(CommandRequest(b"EXPIRE", (b"a", b"2"))) == Number(1)
        assert await c.execute(CommandRequest(b"TTL", (b"a",))) == Number(2)
        assert await c.execute(CommandRequest(b"PERSIST", (b"a",))) == Number(1)
        assert await c.execute(CommandRequest(b"PERSIST", (b"a",))) == Number(0)
        await c.execute(CommandRequest(b"EXPIRE", (b"a", b"1")))
        await c.execute(CommandRequest(b"EXPIRE", (b"b", b"1")))
        clock.advance(1_000)
        assert await runtime.debug_active_expire_once() == 1
        first_active = runtime.executor.debug_applied_batches()[-1]
        assert first_active.trigger is CommitTrigger.ACTIVE_EXPIRE
        assert all(
            isinstance(operation, DeleteKey)
            and operation.reason is DeleteReason.EXPIRED
            for operation in first_active.operations
        )
        assert runtime.debug_physical_key_count == 1
        assert await runtime.debug_active_expire_once() == 1
        assert runtime.debug_physical_key_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("setup", "mutation"),
    [
        (
            CommandRequest(b"SET", (b"k", b"1")),
            CommandRequest(b"INCR", (b"k",)),
        ),
        (
            CommandRequest(b"HSET", (b"k", b"f", b"1")),
            CommandRequest(b"HINCRBY", (b"k", b"f", b"1")),
        ),
        (
            CommandRequest(b"RPUSH", (b"k", b"a", b"b")),
            CommandRequest(b"LPOP", (b"k",)),
        ),
        (
            CommandRequest(b"SADD", (b"k", b"a", b"b")),
            CommandRequest(b"SREM", (b"k", b"a")),
        ),
        (
            CommandRequest(b"ZADD", (b"k", b"1", b"a", b"2", b"b")),
            CommandRequest(b"ZREM", (b"k", b"a")),
        ),
    ],
)
async def test_every_in_place_value_mutation_preserves_absolute_ttl(
    setup: CommandRequest,
    mutation: CommandRequest,
) -> None:
    clock = FakeClock(5_000)
    async with MiniRedis.open(clock=clock) as runtime:
        c = runtime.direct_client()
        assert not isinstance(await c.execute(setup), Failure)
        assert await c.execute(CommandRequest(b"EXPIRE", (b"k", b"10"))) == Number(1)
        assert not isinstance(await c.execute(mutation), Failure)
        assert await c.execute(CommandRequest(b"PTTL", (b"k",))) == Number(10_000)


@pytest.mark.asyncio
async def test_error_discards_pending_lazy_expiry_delete():
    clock = FakeClock(0)
    async with MiniRedis.open(clock=clock) as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SET", (b"elapsed", b"x", b"PX", b"1")))
        await c.execute(CommandRequest(b"SET", (b"wrong", b"x")))
        clock.advance(1)
        before = runtime.debug_commit_seq
        reply = await c.execute(CommandRequest(b"SINTER", (b"elapsed", b"wrong")))
        assert isinstance(reply, Failure)
        assert reply.code == "WRONGTYPE"
        assert runtime.debug_commit_seq == before
        assert runtime.debug_physical_key_count == 2
        assert await c.execute(CommandRequest(b"GET", (b"elapsed",))) == Bytes(None)
        assert runtime.debug_physical_key_count == 1

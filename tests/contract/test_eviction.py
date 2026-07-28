import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.commit import DeleteKey, DeleteReason, PutEntry
from miniredis.core.reply import Bytes, Failure, Number, Ok
from tests.helpers.time import FakeClock


@pytest.mark.asyncio
async def test_oversized_target_does_not_evict_unrelated_key():
    async with MiniRedis.open(maxmemory=120, eviction_policy="allkeys-lru") as r:
        c = r.direct_client()
        assert await c.execute(CommandRequest(b"SET", (b"a", b"x"))) == Ok()
        before = r.debug_commit_seq
        reply = await c.execute(CommandRequest(b"SET", (b"huge", b"x" * 500)))
        assert reply == Failure("OOM", "command exceeds maxmemory")
        assert r.debug_commit_seq == before
        assert await c.execute(CommandRequest(b"GET", (b"a",))) == Bytes(b"x")


@pytest.mark.asyncio
async def test_exact_lru_evicts_cold_key_in_same_commit_as_write():
    async with MiniRedis.open(maxmemory=260, eviction_policy="allkeys-lru") as r:
        c = r.direct_client()
        await c.execute(CommandRequest(b"SET", (b"cold", b"x")))
        await c.execute(CommandRequest(b"SET", (b"hot", b"x")))
        await c.execute(CommandRequest(b"GET", (b"hot",)))
        before = r.debug_commit_seq
        before_tick = r.database.access_tick
        assert await c.execute(CommandRequest(b"SET", (b"new", b"x" * 60))) == Ok()
        assert r.debug_commit_seq == before + 1
        assert r.database.access_tick == before_tick + 1
        batch = r.executor.debug_applied_batches()[-1]
        assert any(
            isinstance(operation, DeleteKey)
            and operation.key == b"cold"
            and operation.reason is DeleteReason.EVICTED
            for operation in batch.operations
        )
        assert any(
            isinstance(operation, PutEntry) and operation.key == b"new"
            for operation in batch.operations
        )
        assert await c.execute(CommandRequest(b"GET", (b"cold",))) == Bytes(None)
        assert await c.execute(CommandRequest(b"GET", (b"hot",))) == Bytes(b"x")


@pytest.mark.asyncio
async def test_noeviction_allows_delete_but_rejects_growth_atomically():
    async with MiniRedis.open(maxmemory=90, eviction_policy="noeviction") as r:
        c = r.direct_client()
        assert await c.execute(CommandRequest(b"SET", (b"a", b"x"))) == Ok()
        before = r.debug_commit_seq
        assert await c.execute(CommandRequest(b"SET", (b"b", b"x"))) == Failure(
            "OOM", "command exceeds maxmemory"
        )
        assert r.debug_commit_seq == before
        assert await c.execute(CommandRequest(b"DEL", (b"a",))) == Number(1)


@pytest.mark.asyncio
async def test_expired_budget_is_purged_in_same_batch_before_noeviction_check():
    clock = FakeClock(0)
    async with MiniRedis.open(
        clock=clock,
        maxmemory=100,
        eviction_policy="noeviction",
    ) as r:
        c = r.direct_client()
        assert await c.execute(CommandRequest(b"SET", (b"old", b"x"))) == Ok()
        assert await c.execute(CommandRequest(b"EXPIRE", (b"old", b"1"))) == Number(1)
        clock.advance(1_000)
        before = r.debug_commit_seq
        assert await c.execute(CommandRequest(b"SET", (b"new", b"x"))) == Ok()
        assert r.debug_commit_seq == before + 1
        batch = r.executor.debug_applied_batches()[-1]
        assert any(
            isinstance(operation, DeleteKey)
            and operation.key == b"old"
            and operation.reason is DeleteReason.EXPIRED
            for operation in batch.operations
        )
        assert any(
            isinstance(operation, PutEntry) and operation.key == b"new"
            for operation in batch.operations
        )
        assert r.debug_physical_key_count == 1
        assert r.debug_stats().expired_key_count == 1


@pytest.mark.asyncio
async def test_lfu_evicts_lowest_effective_frequency():
    clock = FakeClock(0)
    async with MiniRedis.open(
        clock=clock,
        maxmemory=260,
        eviction_policy="allkeys-lfu",
        lfu_decay_interval_ms=1000,
    ) as runtime:
        client = runtime.direct_client()
        await client.execute(CommandRequest(b"SET", (b"hot", b"x")))
        for _ in range(4):
            await client.execute(CommandRequest(b"GET", (b"hot",)))
        await client.execute(CommandRequest(b"SET", (b"cold", b"x")))

        assert await client.execute(
            CommandRequest(b"SET", (b"new", b"x" * 60))
        ) == Ok()
        assert await client.execute(CommandRequest(b"GET", (b"cold",))) == Bytes(None)
        assert await client.execute(CommandRequest(b"GET", (b"hot",))) == Bytes(b"x")
        assert runtime.debug_stats().evicted_key_count == 1


@pytest.mark.asyncio
async def test_lfu_decay_can_cool_an_old_hot_key_below_recent_key():
    clock = FakeClock(0)
    async with MiniRedis.open(
        clock=clock,
        maxmemory=260,
        eviction_policy="allkeys-lfu",
        lfu_decay_interval_ms=1000,
    ) as runtime:
        client = runtime.direct_client()
        await client.execute(CommandRequest(b"SET", (b"old", b"x")))
        for _ in range(7):
            await client.execute(CommandRequest(b"GET", (b"old",)))
        old_anchor = runtime.database.entries[b"old"].last_frequency_decay_ms
        clock.advance(3_000)
        await client.execute(CommandRequest(b"SET", (b"recent", b"x")))

        assert await client.execute(
            CommandRequest(b"SET", (b"new", b"x" * 60))
        ) == Ok()
        assert await client.execute(CommandRequest(b"GET", (b"old",))) == Bytes(None)
        assert await client.execute(CommandRequest(b"GET", (b"recent",))) == Bytes(
            b"x"
        )
        assert old_anchor == 0


@pytest.mark.asyncio
async def test_lfu_planning_projects_without_materializing_survivor_decay():
    clock = FakeClock(0)
    async with MiniRedis.open(
        clock=clock,
        maxmemory=260,
        eviction_policy="allkeys-lfu",
        lfu_decay_interval_ms=1000,
    ) as runtime:
        client = runtime.direct_client()
        await client.execute(CommandRequest(b"SET", (b"hot", b"x")))
        for _ in range(7):
            await client.execute(CommandRequest(b"GET", (b"hot",)))
        await client.execute(CommandRequest(b"SET", (b"cold", b"x")))
        clock.advance(2_000)
        before = (
            runtime.database.entries[b"hot"].frequency,
            runtime.database.entries[b"hot"].last_frequency_decay_ms,
        )

        assert await client.execute(
            CommandRequest(b"SET", (b"new", b"x" * 60))
        ) == Ok()

        survivor = runtime.database.entries[b"hot"]
        assert (survivor.frequency, survivor.last_frequency_decay_ms) == before

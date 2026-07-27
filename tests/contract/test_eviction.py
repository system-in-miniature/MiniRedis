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

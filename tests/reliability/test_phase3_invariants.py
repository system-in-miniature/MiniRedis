import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.config import MiniRedisConfig
from miniredis.core.commit import DeleteKey, DeleteReason
from miniredis.persistence.aof import AofPolicy, load_aof
from miniredis.replication.sink import ReplicaSink
from tests.helpers.runtime import open_test_runtime
from tests.helpers.time import FakeClock


@pytest.mark.asyncio
async def test_seq_n_is_equal_live_recovered_and_on_caught_up_replica(
    tmp_path,
):
    config = MiniRedisConfig(
        aof_path=tmp_path / "appendonly.mraof",
        aof_policy=AofPolicy.ALWAYS,
        snapshot_path=tmp_path / "dump.mrsnap",
    )
    primary = await open_test_runtime(config=config)
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=16)
    await primary.attach_replica(sink)
    client = primary.direct_client()

    await client.execute(CommandRequest(b"SET", (b"s", b"1")))
    await client.execute(CommandRequest(b"HSET", (b"h", b"f", b"v")))
    await client.execute(CommandRequest(b"RPUSH", (b"l", b"a", b"b")))
    await primary.save_snapshot()
    await client.execute(CommandRequest(b"SADD", (b"set", b"a", b"b")))
    await client.execute(CommandRequest(b"ZADD", (b"z", b"1.5", b"m")))
    seq = primary.debug_commit_seq
    await sink.wait_until_applied(seq)
    expected = primary.debug_logical_items()

    assert replica.debug_commit_seq == seq
    assert replica.debug_logical_items() == expected
    await primary.close()

    recovered = MiniRedis.open(config)
    await recovered.start()
    assert recovered.debug_commit_seq == seq
    assert recovered.debug_logical_items() == expected
    await recovered.close()
    await replica.close()


@pytest.mark.asyncio
async def test_expiration_and_eviction_reasons_are_in_the_same_aof_stream(
    tmp_path,
):
    clock = FakeClock(1000)
    path = tmp_path / "appendonly.mraof"
    runtime = await open_test_runtime(
        clock=clock,
        config=MiniRedisConfig(
            aof_path=path,
            aof_policy=AofPolicy.ALWAYS,
            maxmemory=260,
            eviction_policy="allkeys-lru",
        ),
    )
    client = runtime.direct_client()
    await client.execute(CommandRequest(b"SET", (b"exp", b"x", b"PX", b"1")))
    clock.advance(1)
    await client.execute(CommandRequest(b"GET", (b"exp",)))
    await client.execute(CommandRequest(b"SET", (b"cold", b"x")))
    await client.execute(CommandRequest(b"SET", (b"hot", b"x")))
    await client.execute(CommandRequest(b"GET", (b"hot",)))
    await client.execute(CommandRequest(b"SET", (b"new", b"x" * 60)))
    await runtime.close()

    reasons = tuple(
        operation.reason
        for batch in load_aof(path, repair_truncated_tail=False)
        for operation in batch.operations
        if isinstance(operation, DeleteKey)
    )
    assert DeleteReason.EXPIRED in reasons
    assert DeleteReason.EVICTED in reasons


@pytest.mark.asyncio
async def test_waiters_and_pubsub_allocate_no_commit_or_durable_state():
    runtime = await open_test_runtime()
    blocked_client = runtime.direct_client()
    publisher = runtime.direct_client()
    before = runtime.debug_commit_seq

    blocked = asyncio.create_task(
        blocked_client.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
    )
    await runtime.debug_wait_for_waiters(1)
    delivered = await publisher.execute(
        CommandRequest(b"PUBLISH", (b"channel", b"payload"))
    )

    assert delivered.value == 0
    assert runtime.debug_commit_seq == before
    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked
    await runtime.close()

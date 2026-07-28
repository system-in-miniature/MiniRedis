import pytest

from miniredis import CommandRequest, MiniRedis, MiniRedisConfig
from miniredis.core.reply import Bytes, Items, Ok
from miniredis.persistence.aof import AofPolicy


@pytest.mark.asyncio
async def test_transaction_is_one_aof_batch_and_recovers_as_one_commit(tmp_path):
    config = MiniRedisConfig(
        aof_path=tmp_path / "appendonly.mraof",
        aof_policy=AofPolicy.ALWAYS,
    )
    first = MiniRedis.open(config)
    await first.start()
    client = first.direct_client()
    before = first.debug_commit_seq
    await client.execute(CommandRequest(b"MULTI"))
    await client.execute(CommandRequest(b"SET", (b"a", b"1")))
    await client.execute(CommandRequest(b"SET", (b"b", b"2")))

    assert await client.execute(CommandRequest(b"EXEC")) == Items((Ok(), Ok()))
    assert first.debug_commit_seq == before + 1
    assert len(first.debug_applied_batches()[-1].operations) == 2
    await first.close()

    second = MiniRedis.open(config)
    await second.start()
    recovered = second.direct_client()
    assert await recovered.execute(CommandRequest(b"GET", (b"a",))) == Bytes(b"1")
    assert await recovered.execute(CommandRequest(b"GET", (b"b",))) == Bytes(b"2")
    assert second.debug_commit_seq == before + 1
    await second.close()

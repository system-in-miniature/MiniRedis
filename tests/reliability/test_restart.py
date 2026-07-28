import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.config import MiniRedisConfig
from miniredis.core.reply import Bytes, Ok
from miniredis.persistence.aof import AofPolicy
from miniredis.persistence.recovery import RecoveryError
from miniredis.runtime import RuntimeState
from miniredis.replication.sink import ReplicaSink, ReplicaSyncMode
from tests.helpers.runtime import open_test_runtime


@pytest.mark.asyncio
async def test_runtime_recovers_snapshot_then_later_aof_commits(tmp_path):
    config = MiniRedisConfig(
        aof_path=tmp_path / "appendonly.mraof",
        aof_policy=AofPolicy.ALWAYS,
        snapshot_path=tmp_path / "dump.mrsnap",
    )
    first = MiniRedis.open(config)
    await first.start()
    client = first.direct_client()
    assert await client.execute(CommandRequest(b"SET", (b"before", b"1"))) == Ok()
    saved = await first.save_snapshot()
    assert saved.checkpoint_seq == 1
    assert await client.execute(CommandRequest(b"SET", (b"after", b"2"))) == Ok()
    await first.close()

    second = MiniRedis.open(config)
    await second.start()
    recovered = second.direct_client()
    assert await recovered.execute(CommandRequest(b"GET", (b"before",))) == Bytes(b"1")
    assert await recovered.execute(CommandRequest(b"GET", (b"after",))) == Bytes(b"2")
    assert second.debug_commit_seq == 2
    await second.close()


@pytest.mark.asyncio
async def test_corrupt_startup_never_accepts_clients_or_leaks_workers(
    tmp_path,
):
    snapshot = tmp_path / "dump.mrsnap"
    snapshot.write_bytes(b"corrupt")
    runtime = MiniRedis.open(MiniRedisConfig(snapshot_path=snapshot))
    prestart = runtime.direct_client()
    rejected = await prestart.execute(CommandRequest(b"PING"))
    assert rejected.code == "CLOSED"

    with pytest.raises(RecoveryError, match="snapshot"):
        await runtime.start()

    assert runtime.state is RuntimeState.FAILED
    stats = runtime.debug_stats()
    assert stats.accepting_users is False
    assert stats.owned_tasks == 0
    assert stats.sessions == 0
    await runtime.close()


@pytest.mark.asyncio
async def test_restart_resets_volatile_lfu_metadata(tmp_path):
    config = MiniRedisConfig(
        aof_path=tmp_path / "appendonly.mraof",
        aof_policy=AofPolicy.ALWAYS,
        eviction_policy="allkeys-lfu",
    )
    first = MiniRedis.open(config)
    await first.start()
    client = first.direct_client()
    await client.execute(CommandRequest(b"SET", (b"k", b"v")))
    for _ in range(4):
        await client.execute(CommandRequest(b"GET", (b"k",)))
    assert first.database.entries[b"k"].frequency == 5
    await first.close()

    second = MiniRedis.open(config)
    await second.start()
    assert second.database.entries[b"k"].frequency == 0
    assert second.database.entries[b"k"].last_access_tick == 0
    await second.close()


@pytest.mark.asyncio
async def test_primary_restart_changes_identity_and_forces_full_sync(
    tmp_path,
):
    config = MiniRedisConfig(
        aof_path=tmp_path / "appendonly.mraof",
        aof_policy=AofPolicy.ALWAYS,
    )
    first = await open_test_runtime(
        config=config,
        replication_id_factory=lambda: "primary-A",
    )
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=4)
    await first.attach_replica(sink)
    await first.direct_client().execute(
        CommandRequest(b"SET", (b"k", b"v"))
    )
    await sink.wait_until_applied(1)
    await sink.disconnect()
    await first.close()

    restarted = await open_test_runtime(
        config=config,
        replication_id_factory=lambda: "primary-B",
    )
    status = await restarted.attach_replica(sink)

    assert status.sync_mode is ReplicaSyncMode.FULL
    assert status.replication_id == "primary-B"
    assert status.applied_seq == 1
    await restarted.close()
    await replica.close()

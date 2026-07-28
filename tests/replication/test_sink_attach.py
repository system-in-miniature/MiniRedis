import asyncio

import pytest

from miniredis import CommandRequest
from miniredis.config import MiniRedisConfig
from miniredis.core.reply import Bytes, Ok
from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
from miniredis.replication.backlog import ReplicationCursor
from tests.helpers.runtime import open_test_runtime


@pytest.mark.asyncio
async def test_attach_registers_incremental_stream_with_snapshot_capture():
    primary = await open_test_runtime()
    replica = await open_test_runtime()
    client = primary.direct_client()
    assert await client.execute(
        CommandRequest(b"SET", (b"before", b"1"))
    ) == Ok()

    install_gate = asyncio.Event()
    sink = ReplicaSink(
        replica,
        queue_limit=4,
        install_gate=install_gate,
    )
    attaching = asyncio.create_task(primary.attach_replica(sink))
    await sink.attachment_captured.wait()

    assert await client.execute(
        CommandRequest(b"SET", (b"during", b"2"))
    ) == Ok()
    assert sink.status.baseline_seq == 1
    assert sink.status.queued == 1

    install_gate.set()
    await attaching
    await sink.wait_until_applied(2)

    replica_client = replica.direct_client()
    assert await replica_client.execute(
        CommandRequest(b"GET", (b"before",))
    ) == Bytes(b"1")
    assert await replica_client.execute(
        CommandRequest(b"GET", (b"during",))
    ) == Bytes(b"2")
    assert sink.status.state is ReplicaSinkState.STREAMING
    assert sink.status.applied_seq == 2
    await primary.close()
    await replica.close()


@pytest.mark.asyncio
async def test_attached_replica_rejects_user_writes():
    primary = await open_test_runtime()
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=4)
    await primary.attach_replica(sink)

    reply = await replica.direct_client().execute(
        CommandRequest(b"SET", (b"k", b"v"))
    )

    assert reply.code == "READONLY"
    assert replica.debug_commit_seq == sink.status.applied_seq
    await primary.close()
    await replica.close()


@pytest.mark.asyncio
async def test_full_sync_resets_volatile_lfu_metadata():
    config = MiniRedisConfig(eviction_policy="allkeys-lfu")
    primary = await open_test_runtime(config=config)
    replica = await open_test_runtime(config=config)
    client = primary.direct_client()
    await client.execute(CommandRequest(b"SET", (b"k", b"v")))
    for _ in range(4):
        await client.execute(CommandRequest(b"GET", (b"k",)))
    assert primary.database.entries[b"k"].frequency == 5

    sink = ReplicaSink(replica, queue_limit=4)
    await primary.attach_replica(sink)

    assert replica.database.entries[b"k"].frequency == 0
    assert replica.database.entries[b"k"].last_access_tick == 0
    await primary.close()
    await replica.close()


@pytest.mark.asyncio
async def test_full_sync_cursor_exists_only_after_snapshot_install():
    primary = await open_test_runtime(
        replication_id_factory=lambda: "primary-A"
    )
    replica = await open_test_runtime()
    await primary.direct_client().execute(
        CommandRequest(b"SET", (b"k", b"v"))
    )
    install_gate = asyncio.Event()
    sink = ReplicaSink(
        replica,
        queue_limit=4,
        install_gate=install_gate,
    )
    attaching = asyncio.create_task(primary.attach_replica(sink))
    await sink.attachment_captured.wait()

    assert sink.status.cursor is None

    install_gate.set()
    await attaching
    assert sink.status.cursor == ReplicationCursor("primary-A", 1)
    await primary.close()
    await replica.close()

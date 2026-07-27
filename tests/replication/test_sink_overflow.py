import asyncio

import pytest

from miniredis import CommandRequest
from miniredis.core.reply import Bytes, Ok
from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
from tests.helpers.runtime import open_test_runtime


@pytest.mark.asyncio
async def test_full_sink_detaches_as_needs_resync_without_blocking_primary():
    primary = await open_test_runtime()
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=1)
    await primary.attach_replica(sink)
    sink.pause()
    client = primary.direct_client()

    assert await client.execute(
        CommandRequest(b"SET", (b"a", b"1"))
    ) == Ok()
    assert await client.execute(
        CommandRequest(b"SET", (b"b", b"2"))
    ) == Ok()
    assert await client.execute(
        CommandRequest(b"SET", (b"c", b"3"))
    ) == Ok()

    assert sink.status.state is ReplicaSinkState.NEEDS_RESYNC
    assert sink.status.queued == 0
    assert primary.debug_commit_seq == 3
    assert primary.debug_stats().replica_links == 0
    await primary.close()
    await replica.close()


@pytest.mark.asyncio
async def test_overflow_wakes_applied_sequence_waiters_without_a_sleep():
    primary = await open_test_runtime()
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=1)
    await primary.attach_replica(sink)
    sink.pause()
    client = primary.direct_client()
    waiting = asyncio.create_task(sink.wait_until_applied(2))

    assert await client.execute(
        CommandRequest(b"SET", (b"a", b"1"))
    ) == Ok()
    assert await client.execute(
        CommandRequest(b"SET", (b"b", b"2"))
    ) == Ok()

    with pytest.raises(RuntimeError, match="replica stopped at seq 0"):
        await asyncio.wait_for(waiting, timeout=1)
    assert sink.status.state is ReplicaSinkState.NEEDS_RESYNC
    await primary.close()
    await replica.close()


@pytest.mark.asyncio
async def test_bootstrap_overflow_never_installs_the_stale_snapshot():
    primary = await open_test_runtime()
    replica = await open_test_runtime()
    replica_client = replica.direct_client()
    assert await replica_client.execute(
        CommandRequest(b"SET", (b"local", b"keep"))
    ) == Ok()
    install_gate = asyncio.Event()
    sink = ReplicaSink(
        replica,
        queue_limit=1,
        install_gate=install_gate,
    )
    attaching = asyncio.create_task(primary.attach_replica(sink))
    await sink.attachment_captured.wait()

    client = primary.direct_client()
    assert await client.execute(
        CommandRequest(b"SET", (b"a", b"1"))
    ) == Ok()
    assert await client.execute(
        CommandRequest(b"SET", (b"b", b"2"))
    ) == Ok()
    assert sink.status.state is ReplicaSinkState.NEEDS_RESYNC
    install_gate.set()

    status = await attaching
    assert status.state is ReplicaSinkState.NEEDS_RESYNC
    assert await replica_client.execute(
        CommandRequest(b"GET", (b"local",))
    ) == Bytes(b"keep")
    assert primary.debug_stats().replica_links == 0
    await primary.close()
    await replica.close()

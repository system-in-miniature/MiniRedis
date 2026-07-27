import asyncio

import pytest

from miniredis import CommandRequest
from miniredis.config import MiniRedisConfig
from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
from miniredis.runtime import RuntimeState
from tests.helpers.runtime import open_test_runtime


@pytest.mark.asyncio
async def test_close_awaits_owned_snapshot_before_closing_aof(tmp_path):
    runtime = await open_test_runtime(
        config=MiniRedisConfig(
            aof_path=tmp_path / "appendonly.mraof",
            snapshot_path=tmp_path / "dump.mrsnap",
        ),
        snapshot_write_gate=True,
        lifecycle_trace=True,
    )
    await runtime.direct_client().execute(CommandRequest(b"SET", (b"k", b"v")))
    saving = asyncio.create_task(runtime.save_snapshot())
    await runtime.debug_snapshot_write_entered.wait()

    closing = asyncio.create_task(runtime.close())
    await runtime.debug_wait_for_state(RuntimeState.DRAINING.value)
    assert not closing.done()
    runtime.debug_snapshot_write_release.set()
    await saving
    await closing

    trace = runtime.debug_lifecycle_trace()
    assert trace.index("snapshot-job-done") < trace.index("aof-closed")
    assert trace.index("aof-closed") < trace.index("replicas-stopped")
    assert trace.index("replicas-stopped") < trace.index("executor-stopped")
    stats = runtime.debug_stats()
    assert stats.owned_tasks == 0
    assert stats.snapshot_jobs == 0
    assert stats.replica_links == 0


@pytest.mark.asyncio
async def test_graceful_close_bounds_replica_drain_then_detaches():
    primary = await open_test_runtime(config=MiniRedisConfig(replica_drain_grace_ms=0))
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=2)
    await primary.attach_replica(sink)
    sink.pause()
    await primary.direct_client().execute(CommandRequest(b"SET", (b"k", b"v")))

    await primary.close()

    assert sink.status.state is ReplicaSinkState.STOPPED
    assert sink.status.applied_seq == 0
    stats = primary.debug_stats()
    assert stats.owned_tasks == 0
    assert stats.snapshot_jobs == 0
    assert stats.replica_links == 0
    await replica.close()


@pytest.mark.asyncio
async def test_cancelling_one_close_waiter_cannot_cancel_cleanup(tmp_path):
    runtime = await open_test_runtime(
        config=MiniRedisConfig(snapshot_path=tmp_path / "dump.mrsnap"),
        snapshot_write_gate=True,
    )
    saving = asyncio.create_task(runtime.save_snapshot())
    await runtime.debug_snapshot_write_entered.wait()
    first = asyncio.create_task(runtime.close())
    second = asyncio.create_task(runtime.close())
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    runtime.debug_snapshot_write_release.set()
    await saving
    await second
    stats = runtime.debug_stats()
    assert stats.owned_tasks == 0
    assert stats.snapshot_jobs == 0
    assert stats.replica_links == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("crash", [False, True])
async def test_close_or_crash_joins_a_bootstrap_blocked_on_install_gate(
    crash,
):
    primary = await open_test_runtime()
    replica = await open_test_runtime()
    install_gate = asyncio.Event()
    sink = ReplicaSink(
        replica,
        queue_limit=2,
        install_gate=install_gate,
    )
    attaching = asyncio.create_task(primary.attach_replica(sink))
    await sink.attachment_captured.wait()

    if crash:
        await primary.simulate_crash()
    else:
        await primary.close()

    with pytest.raises(asyncio.CancelledError):
        await attaching
    expected = ReplicaSinkState.SOURCE_LOST if crash else ReplicaSinkState.STOPPED
    assert sink.status.state is expected
    assert primary.debug_stats().replica_links == 0
    await replica.close()

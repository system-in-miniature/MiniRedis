import pytest

from miniredis import CommandRequest
from miniredis.core.reply import Ok
from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
from tests.helpers.runtime import open_test_runtime


@pytest.mark.asyncio
async def test_replica_apply_exception_detaches_only_that_sink():
    primary = await open_test_runtime()
    replica = await open_test_runtime(
        replica_apply_failure=RuntimeError("replica failed"),
    )
    sink = ReplicaSink(replica, queue_limit=2)
    await primary.attach_replica(sink)

    assert await primary.direct_client().execute(
        CommandRequest(b"SET", (b"k", b"v"))
    ) == Ok()
    await sink.wait_until_stopped()

    assert sink.status.state is ReplicaSinkState.FAILED
    assert primary.debug_commit_seq == 1
    assert primary.state.name == "RUNNING"
    assert primary.debug_stats().replica_links == 0
    await primary.close()
    await replica.close()

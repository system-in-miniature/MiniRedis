import pytest

from miniredis import CommandRequest
from miniredis.replication.sink import ReplicaSink
from tests.helpers.runtime import open_test_runtime


@pytest.mark.asyncio
async def test_pause_gate_exposes_exact_sequence_lag():
    primary = await open_test_runtime()
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=4)
    await primary.attach_replica(sink)
    sink.pause()

    client = primary.direct_client()
    await client.execute(CommandRequest(b"SET", (b"a", b"1")))
    await client.execute(CommandRequest(b"SET", (b"b", b"2")))

    assert sink.status.primary_seq == 2
    assert sink.status.applied_seq == 0
    assert sink.status.lag == 2
    sink.resume()
    await sink.wait_until_applied(2)
    assert sink.status.lag == 0
    await primary.close()
    await replica.close()

import asyncio

import pytest

from miniredis import CommandRequest
from miniredis.core.reply import Bytes, Ok
from miniredis.replication.sink import ReplicaSink
from tests.helpers.runtime import open_test_runtime


@pytest.mark.asyncio
async def test_acknowledged_primary_write_can_be_lost_on_lagging_promotion():
    primary = await open_test_runtime()
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=4)
    await primary.attach_replica(sink)
    sink.pause()

    acknowledged = await primary.direct_client().execute(
        CommandRequest(b"SET", (b"x", b"1"))
    )
    assert acknowledged == Ok()
    assert sink.status.lag == 1
    waiting = asyncio.create_task(sink.wait_until_applied(1))

    await primary.simulate_crash()
    with pytest.raises(RuntimeError, match="replica stopped at seq 0"):
        await asyncio.wait_for(waiting, timeout=1)
    promotion = await sink.promote(source_alive=False)

    assert promotion.applied_seq == 0
    assert await replica.direct_client().execute(
        CommandRequest(b"GET", (b"x",))
    ) == Bytes(None)
    await replica.close()

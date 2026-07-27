import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes


@pytest.mark.asyncio
async def test_one_hundred_concurrent_increments_are_serialized():
    async with MiniRedis.open(max_pending_commands=256) as runtime:
        clients = [runtime.direct_client() for _ in range(100)]
        await asyncio.gather(
            *(
                client.execute(CommandRequest(b"INCR", (b"counter",)))
                for client in clients
            )
        )
        assert await clients[0].execute(CommandRequest(b"GET", (b"counter",))) == Bytes(
            b"100"
        )

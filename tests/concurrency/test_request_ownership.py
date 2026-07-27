import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Ok


@pytest.mark.asyncio
async def test_accepted_tokens_are_runtime_unique_and_never_reused():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()
        assert await client.execute(CommandRequest(b"PING")) == Ok(b"PONG")
        assert await client.execute(CommandRequest(b"PING")) == Ok(b"PONG")
        assert [token.value for token in runtime.debug_accepted_tokens] == [1, 2]


@pytest.mark.asyncio
async def test_caller_cancellation_does_not_cancel_owned_future_or_commit():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()
        runtime.debug_pause_executor()
        request = asyncio.create_task(client.execute(CommandRequest(b"INCR", (b"n",))))
        await runtime.debug_wait_until_queued(1)
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        runtime.debug_resume_executor()
        await runtime.debug_wait_until_idle()
        assert await client.execute(CommandRequest(b"GET", (b"n",))) == Bytes(b"1")
        stats = runtime.debug_stats()
        assert stats.accepted_requests == 0
        assert stats.pending_futures == 0
        assert [token.value for token in runtime.debug_accepted_tokens] == [1, 2]

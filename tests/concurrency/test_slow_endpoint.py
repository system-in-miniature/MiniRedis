import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.outbound import PubSubMessage, SubscriptionAck
from miniredis.core.reply import Number, Ok


@pytest.mark.asyncio
async def test_full_subscriber_closes_without_blocking_fast_endpoint():
    async with MiniRedis.open(outbox_limit=1) as runtime:
        slow = runtime.direct_client()
        fast = runtime.direct_client()
        publisher = runtime.direct_client()
        assert await slow.execute(CommandRequest(b"SUBSCRIBE", (b"c",))) is None
        assert await fast.execute(CommandRequest(b"SUBSCRIBE", (b"c",))) is None
        assert await fast.receive() == SubscriptionAck("subscribe", b"c", 1)

        assert await publisher.execute(
            CommandRequest(b"PUBLISH", (b"c", b"m"))
        ) == Number(1)
        assert await fast.receive() == PubSubMessage(b"c", b"m")
        await runtime.debug_wait_for_sessions(2)
        assert await publisher.execute(CommandRequest(b"PING")) == Ok(b"PONG")
        assert runtime.debug_stats().subscriptions == 1

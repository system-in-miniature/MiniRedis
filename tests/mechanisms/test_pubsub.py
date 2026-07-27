import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.outbound import (
    PubSubMessage,
    PubSubPong,
    SubscriptionAck,
)
from miniredis.core.reply import Failure, Number


@pytest.mark.asyncio
async def test_exact_binary_channel_and_repeated_subscription_count():
    async with MiniRedis.open(outbox_limit=8) as runtime:
        subscriber = runtime.direct_client()
        publisher = runtime.direct_client()
        assert (
            await subscriber.execute(CommandRequest(b"SUBSCRIBE", (b"a\x00", b"a\x00")))
            is None
        )
        assert await subscriber.receive() == SubscriptionAck("subscribe", b"a\x00", 1)
        assert await subscriber.receive() == SubscriptionAck("subscribe", b"a\x00", 1)
        assert await publisher.execute(
            CommandRequest(b"PUBLISH", (b"a", b"miss"))
        ) == Number(0)
        before = runtime.debug_commit_seq
        assert await publisher.execute(
            CommandRequest(b"PUBLISH", (b"a\x00", b"hit"))
        ) == Number(1)
        assert runtime.debug_commit_seq == before
        assert await subscriber.receive() == PubSubMessage(b"a\x00", b"hit")


@pytest.mark.asyncio
async def test_subscribed_mode_and_unsubscribe_all():
    async with MiniRedis.open() as runtime:
        subscriber = runtime.direct_client()
        await subscriber.execute(CommandRequest(b"SUBSCRIBE", (b"b", b"a")))
        await subscriber.receive()
        await subscriber.receive()
        denied = await subscriber.execute(CommandRequest(b"SET", (b"k", b"v")))
        assert denied == Failure(
            "ERR",
            "only PING, SUBSCRIBE and UNSUBSCRIBE are allowed in subscribed mode",
        )
        assert await subscriber.execute(CommandRequest(b"PING", (b"x",))) is None
        assert await subscriber.receive() == PubSubPong(b"x")
        assert await subscriber.execute(CommandRequest(b"UNSUBSCRIBE")) is None
        assert await subscriber.receive() == SubscriptionAck("unsubscribe", b"b", 1)
        assert await subscriber.receive() == SubscriptionAck("unsubscribe", b"a", 0)
        assert await subscriber.execute(CommandRequest(b"UNSUBSCRIBE")) is None
        assert await subscriber.receive() == SubscriptionAck("unsubscribe", None, 0)

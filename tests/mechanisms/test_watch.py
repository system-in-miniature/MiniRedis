import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Failure, NullArray, Ok


@pytest.mark.asyncio
async def test_unwatch_clears_recorded_revisions():
    async with MiniRedis.open() as runtime:
        owner = runtime.direct_client()

        assert await owner.execute(CommandRequest(b"WATCH", (b"k", b"other"))) == Ok()
        assert runtime.executor.watched_key_count == 2
        assert await owner.execute(CommandRequest(b"UNWATCH")) == Ok()
        assert runtime.executor.watched_key_count == 0


@pytest.mark.asyncio
async def test_watch_after_multi_is_rejected_and_session_close_cleans_state():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()

        assert await client.execute(CommandRequest(b"WATCH", (b"k",))) == Ok()
        assert await client.execute(CommandRequest(b"MULTI")) == Ok()
        assert await client.execute(CommandRequest(b"WATCH", (b"other",))) == Failure(
            "ERR", "WATCH inside MULTI is not allowed"
        )
        assert runtime.executor.dirty_transaction_count == 1

        await client.close()

        assert runtime.executor.active_transaction_count == 0
        assert runtime.executor.watched_key_count == 0


@pytest.mark.asyncio
async def test_watch_detects_create_then_delete():
    async with MiniRedis.open() as runtime:
        owner = runtime.direct_client()
        rival = runtime.direct_client()
        assert await owner.execute(CommandRequest(b"WATCH", (b"k",))) == Ok()
        await rival.execute(CommandRequest(b"SET", (b"k", b"v")))
        await rival.execute(CommandRequest(b"DEL", (b"k",)))
        await owner.execute(CommandRequest(b"MULTI"))
        await owner.execute(CommandRequest(b"GET", (b"k",)))

        assert await owner.execute(CommandRequest(b"EXEC")) == NullArray()
        assert runtime.debug_stats().watch_aborts == 1
        assert runtime.executor.watched_key_count == 0
        assert await owner.execute(CommandRequest(b"GET", (b"k",))) == Bytes(None)

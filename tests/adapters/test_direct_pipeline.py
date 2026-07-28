import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Failure, Number, Ok


@pytest.mark.asyncio
async def test_direct_pipeline_preserves_result_slots_and_is_not_atomic():
    async with MiniRedis.open() as runtime:
        pipeline = runtime.direct_pipeline()
        pipeline.queue(CommandRequest(b"SET", (b"k", b"1")))
        pipeline.queue(CommandRequest(b"NOPE"))
        pipeline.queue(CommandRequest(b"INCR", (b"k",)))

        assert await pipeline.execute() == (
            Ok(),
            Failure("ERR", "unknown command"),
            Number(2),
        )
        assert pipeline.pending_count == 0


@pytest.mark.asyncio
async def test_direct_pipeline_submits_parse_failures_in_mailbox_order():
    async with MiniRedis.open() as runtime:
        runtime.debug_pause_executor()
        pipeline = runtime.direct_pipeline()
        pipeline.queue(CommandRequest(b"SET", (b"k", b"1")))
        pipeline.queue(CommandRequest(b"PING", (b"too", b"many")))
        pipeline.queue(CommandRequest(b"INCR", (b"k",)))

        executing = asyncio.create_task(pipeline.execute())
        await runtime.debug_wait_accepted_at_least(3)

        assert [token.value for token in runtime.debug_accepted_tokens] == [1, 2, 3]
        runtime.debug_resume_executor()
        assert await executing == (
            Ok(),
            Failure("ERR", "wrong number of arguments for PING"),
            Number(2),
        )


@pytest.mark.asyncio
async def test_direct_pipeline_close_discards_queued_requests_and_closes_client():
    async with MiniRedis.open() as runtime:
        pipeline = runtime.direct_pipeline()
        pipeline.queue(CommandRequest(b"SET", (b"k", b"1")))

        await pipeline.close()

        assert pipeline.pending_count == 0
        with pytest.raises(RuntimeError, match="client is closed"):
            pipeline.queue(CommandRequest(b"PING"))

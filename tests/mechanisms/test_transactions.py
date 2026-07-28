import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Failure, Ok


@pytest.mark.asyncio
async def test_multi_queues_and_discard_clears_state():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()

        assert await client.execute(CommandRequest(b"MULTI")) == Ok()
        assert await client.execute(
            CommandRequest(b"SET", (b"k", b"v"))
        ) == Ok(b"QUEUED")
        assert await client.execute(CommandRequest(b"DISCARD")) == Ok()
        assert await client.execute(CommandRequest(b"GET", (b"k",))) == Bytes(None)


@pytest.mark.asyncio
async def test_transaction_control_errors_and_parse_error_marks_dirty():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()

        assert await client.execute(CommandRequest(b"EXEC")) == Failure(
            "ERR", "EXEC without MULTI"
        )
        assert await client.execute(CommandRequest(b"DISCARD")) == Failure(
            "ERR", "DISCARD without MULTI"
        )
        assert await client.execute(CommandRequest(b"MULTI")) == Ok()
        assert await client.execute(CommandRequest(b"MULTI")) == Failure(
            "ERR", "MULTI calls can not be nested"
        )
        assert await client.execute(CommandRequest(b"GET")) == Failure(
            "ERR", "wrong number of arguments for GET"
        )
        assert runtime.executor.active_transaction_count == 1
        assert runtime.executor.dirty_transaction_count == 1
        assert await client.execute(CommandRequest(b"DISCARD")) == Ok()


@pytest.mark.asyncio
async def test_disallowed_blocking_command_marks_transaction_dirty():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()

        assert await client.execute(CommandRequest(b"MULTI")) == Ok()
        assert await client.execute(
            CommandRequest(b"BLPOP", (b"q", b"0"))
        ) == Failure("ERR", "command is not allowed inside MULTI")
        assert runtime.executor.dirty_transaction_count == 1
        assert await client.execute(CommandRequest(b"DISCARD")) == Ok()

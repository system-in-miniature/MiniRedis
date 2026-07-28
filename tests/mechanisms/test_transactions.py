import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Failure, Items, Number, Ok


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


@pytest.mark.asyncio
async def test_exec_reads_prior_write_and_keeps_runtime_error_slots():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()
        await client.execute(CommandRequest(b"MULTI"))
        await client.execute(CommandRequest(b"SET", (b"k", b"1")))
        await client.execute(CommandRequest(b"LPUSH", (b"k", b"x")))
        await client.execute(CommandRequest(b"INCR", (b"k",)))
        before = runtime.debug_commit_seq

        assert await client.execute(CommandRequest(b"EXEC")) == Items(
            (
                Ok(),
                Failure(
                    "WRONGTYPE",
                    "operation against a key holding the wrong kind of value",
                ),
                Number(2),
            )
        )
        assert runtime.debug_commit_seq == before + 1
        assert await client.execute(CommandRequest(b"GET", (b"k",))) == Bytes(b"2")


@pytest.mark.asyncio
async def test_dirty_exec_aborts_without_applying_queued_commands():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()
        await client.execute(CommandRequest(b"MULTI"))
        await client.execute(CommandRequest(b"SET", (b"k", b"v")))
        await client.execute(CommandRequest(b"GET"))

        assert await client.execute(CommandRequest(b"EXEC")) == Failure(
            "EXECABORT", "transaction discarded because of previous errors"
        )
        assert await client.execute(CommandRequest(b"GET", (b"k",))) == Bytes(None)


@pytest.mark.asyncio
async def test_empty_exec_is_a_noop_without_commit():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()
        before = runtime.debug_commit_seq
        await client.execute(CommandRequest(b"MULTI"))

        assert await client.execute(CommandRequest(b"EXEC")) == Items(())
        assert runtime.debug_commit_seq == before


@pytest.mark.asyncio
async def test_exec_reserves_each_blocked_waiter_at_most_once():
    async with MiniRedis.open() as runtime:
        first = runtime.direct_client()
        second = runtime.direct_client()
        producer = runtime.direct_client()
        blocked_first = asyncio.create_task(
            first.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
        )
        blocked_second = asyncio.create_task(
            second.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
        )
        await runtime.debug_wait_for_waiters(2)
        await producer.execute(CommandRequest(b"MULTI"))
        await producer.execute(CommandRequest(b"RPUSH", (b"q", b"a")))
        await producer.execute(CommandRequest(b"RPUSH", (b"q", b"b")))

        assert await producer.execute(CommandRequest(b"EXEC")) == Items(
            (Number(1), Number(1))
        )
        assert await blocked_first == Items((Bytes(b"q"), Bytes(b"a")))
        assert await blocked_second == Items((Bytes(b"q"), Bytes(b"b")))

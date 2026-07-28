import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.reply import Bytes, Failure, Items, Number, Ok
from tests.helpers.time import FakeClock


@pytest.mark.asyncio
async def test_comparedel_only_removes_matching_string():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()
        await client.execute(
            CommandRequest(b"SET", (b"lock", b"owner", b"PX", b"1000"))
        )
        before = runtime.debug_commit_seq

        assert await client.execute(
            CommandRequest(b"COMPAREDEL", (b"lock", b"other"))
        ) == Number(0)
        assert runtime.debug_commit_seq == before
        assert await client.execute(
            CommandRequest(b"COMPAREDEL", (b"lock", b"owner"))
        ) == Number(1)
        assert await client.execute(CommandRequest(b"GET", (b"lock",))) == Bytes(None)


@pytest.mark.asyncio
async def test_comparedel_missing_and_wrongtype_contracts():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()
        assert await client.execute(
            CommandRequest(b"COMPAREDEL", (b"missing", b"token"))
        ) == Number(0)
        await client.execute(CommandRequest(b"LPUSH", (b"list", b"x")))
        assert await client.execute(
            CommandRequest(b"COMPAREDEL", (b"list", b"token"))
        ) == Failure(
            "WRONGTYPE",
            "operation against a key holding the wrong kind of value",
        )


@pytest.mark.asyncio
async def test_checkdecr_preserves_ttl_and_rejects_insufficient_stock():
    clock = FakeClock(0)
    async with MiniRedis.open(clock=clock) as runtime:
        client = runtime.direct_client()
        await client.execute(
            CommandRequest(b"SET", (b"stock", b"5", b"PX", b"1000"))
        )
        ttl = await client.execute(CommandRequest(b"PTTL", (b"stock",)))

        assert await client.execute(
            CommandRequest(b"CHECKDECR", (b"stock", b"2"))
        ) == Number(3)
        assert await client.execute(CommandRequest(b"PTTL", (b"stock",))) == ttl
        before = runtime.debug_commit_seq
        assert await client.execute(
            CommandRequest(b"CHECKDECR", (b"stock", b"4"))
        ) == Failure("INSUFFICIENT", "insufficient value")
        assert runtime.debug_commit_seq == before


@pytest.mark.asyncio
async def test_checkdecr_missing_wrongtype_and_invalid_integer_contracts():
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()
        assert await client.execute(
            CommandRequest(b"CHECKDECR", (b"missing", b"1"))
        ) == Failure("INSUFFICIENT", "insufficient value")
        await client.execute(CommandRequest(b"LPUSH", (b"list", b"x")))
        assert await client.execute(
            CommandRequest(b"CHECKDECR", (b"list", b"1"))
        ) == Failure(
            "WRONGTYPE",
            "operation against a key holding the wrong kind of value",
        )
        await client.execute(CommandRequest(b"SET", (b"stock", b"01")))
        assert await client.execute(
            CommandRequest(b"CHECKDECR", (b"stock", b"1"))
        ) == Failure("ERR", "value is not an integer or out of range")


@pytest.mark.asyncio
async def test_checkdecr_is_single_winner_and_queues_inside_transaction():
    async with MiniRedis.open() as runtime:
        owner = runtime.direct_client()
        rival = runtime.direct_client()
        await owner.execute(CommandRequest(b"SET", (b"stock", b"1")))

        results = await asyncio.gather(
            owner.execute(CommandRequest(b"CHECKDECR", (b"stock", b"1"))),
            rival.execute(CommandRequest(b"CHECKDECR", (b"stock", b"1"))),
        )
        assert sorted(results, key=repr) == sorted(
            [Number(0), Failure("INSUFFICIENT", "insufficient value")],
            key=repr,
        )

        await owner.execute(CommandRequest(b"SET", (b"stock", b"2")))
        assert await owner.execute(CommandRequest(b"MULTI")) == Ok()
        assert await owner.execute(
            CommandRequest(b"CHECKDECR", (b"stock", b"1"))
        ) == Ok(b"QUEUED")
        assert await owner.execute(CommandRequest(b"EXEC")) == Items((Number(1),))

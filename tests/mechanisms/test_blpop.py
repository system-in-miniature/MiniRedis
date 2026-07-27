import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.commands.model import BlPop
from miniredis.commands.parser import CommandParseError, parse_request
from miniredis.core.reply import Bytes, Failure, Items


def test_blpop_parser_freezes_keys_and_milliseconds():
    assert parse_request(CommandRequest(b"BLPOP", (b"a", b"b", b"1.25"))) == BlPop(
        (b"a", b"b"), 1250
    )


@pytest.mark.parametrize(
    "raw",
    [b"1_0", b" 1", b"1 ", b"NaN", b"Inf", b"+Inf", b"-Inf"],
)
def test_blpop_timeout_rejects_non_redis_numeric_syntax(raw):
    with pytest.raises(CommandParseError):
        parse_request(CommandRequest(b"BLPOP", (b"a", raw)))


def test_blpop_timeout_rejects_huge_finite_exponent():
    with pytest.raises(CommandParseError, match="timeout is out of range"):
        parse_request(CommandRequest(b"BLPOP", (b"a", b"1e999999")))


@pytest.mark.asyncio
async def test_blpop_uses_first_ready_key_and_stops_type_checks():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"RPUSH", (b"ready", b"x")))
        await c.execute(CommandRequest(b"SET", (b"wrong", b"s")))
        assert await c.execute(
            CommandRequest(b"BLPOP", (b"ready", b"wrong", b"1"))
        ) == Items((Bytes(b"ready"), Bytes(b"x")))
        assert isinstance(
            await c.execute(CommandRequest(b"BLPOP", (b"wrong", b"ready", b"1"))),
            Failure,
        )


@pytest.mark.asyncio
async def test_empty_scan_registers_once_under_every_key():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        blocked = asyncio.create_task(
            c.execute(CommandRequest(b"BLPOP", (b"a", b"b", b"0")))
        )
        await runtime.debug_wait_for_waiters(1)
        assert runtime.debug_waiter_ids(b"a") == runtime.debug_waiter_ids(b"b")
        blocked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await blocked
        await runtime.debug_wait_for_waiters(0)
        assert runtime.debug_waiter_ids(b"a") == ()
        assert runtime.debug_waiter_ids(b"b") == ()
        assert runtime.debug_waiter_index_counts == (0, 0, 0)

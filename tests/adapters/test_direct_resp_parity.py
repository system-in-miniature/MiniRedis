import asyncio

import pytest

from miniredis import MiniRedis
from miniredis.adapters.resp2 import (
    RespArray,
    RespBulk,
    encode_frame,
    encode_outbound,
)
from miniredis.commands.request import CommandRequest


COMMAND_SEQUENCE = (
    CommandRequest(b"SET", (b"k", b"1")),
    CommandRequest(b"INCR", (b"k",)),
    CommandRequest(b"GET", (b"k",)),
    CommandRequest(b"HSET", (b"h", b"f", b"v")),
    CommandRequest(b"HGET", (b"h", b"f")),
    CommandRequest(b"HGETALL", (b"h",)),
    CommandRequest(b"RPUSH", (b"l", b"a", b"b")),
    CommandRequest(b"LRANGE", (b"l", b"0", b"-1")),
    CommandRequest(b"SADD", (b"s", b"a", b"b")),
    CommandRequest(b"SMEMBERS", (b"s",)),
    CommandRequest(b"ZADD", (b"z", b"1", b"a")),
    CommandRequest(b"ZRANGE", (b"z", b"0", b"-1")),
    CommandRequest(b"TYPE", (b"z",)),
    CommandRequest(b"GET", (b"h",)),
    CommandRequest(b"DEL", (b"k", b"missing")),
)


def request_wire(request: CommandRequest) -> bytes:
    return encode_frame(
        RespArray(
            (
                RespBulk(request.name),
                *(RespBulk(arg) for arg in request.args),
            )
        )
    )


@pytest.mark.asyncio
async def test_selected_sequence_has_state_and_reply_parity():
    async with (
        MiniRedis.open() as direct_runtime,
        MiniRedis.open() as tcp_runtime,
    ):
        direct = direct_runtime.direct_client()
        server = await tcp_runtime.start_tcp("127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(*server.address)
        try:
            for request in COMMAND_SEQUENCE:
                expected = encode_outbound(await direct.execute(request))
                writer.write(request_wire(request))
                await writer.drain()
                assert await reader.readexactly(len(expected)) == expected

            assert (
                tcp_runtime.debug_logical_items()
                == direct_runtime.debug_logical_items()
            )
            assert tcp_runtime.debug_commit_seq == direct_runtime.debug_commit_seq
        finally:
            writer.close()
            await writer.wait_closed()
            await server.close()

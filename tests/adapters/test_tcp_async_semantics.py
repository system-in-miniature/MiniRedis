import asyncio

import pytest

from miniredis import MiniRedis
from miniredis.commands.request import CommandRequest
from miniredis.core.reply import Bytes, Number


async def send(writer, wire):
    writer.write(wire)
    await writer.drain()


async def expect(reader, wire):
    assert await reader.readexactly(len(wire)) == wire


async def close_writers(*writers):
    for writer in writers:
        writer.close()
    await asyncio.gather(
        *(writer.wait_closed() for writer in writers),
        return_exceptions=True,
    )


class CloseReleasedWriter:
    def __init__(self, inner) -> None:
        self._inner = inner
        self.drain_started = asyncio.Event()
        self._closed = asyncio.Event()

    def write(self, data: bytes) -> None:
        self._inner.write(data)

    async def drain(self) -> None:
        self.drain_started.set()
        await self._closed.wait()
        raise ConnectionError("transport closed")

    def close(self) -> None:
        self._inner.close()
        self._closed.set()

    async def wait_closed(self) -> None:
        await self._inner.wait_closed()

    def force_release(self) -> None:
        self._closed.set()


@pytest.mark.asyncio
async def test_blpop_does_not_block_another_connection():
    async with MiniRedis.open() as redis:
        server = await redis.start_tcp("127.0.0.1", 0)
        r1, w1 = await asyncio.open_connection(*server.address)
        r2, w2 = await asyncio.open_connection(*server.address)
        await send(w1, b"*3\r\n$5\r\nBLPOP\r\n$1\r\nq\r\n$1\r\n0\r\n")
        await send(w2, b"*3\r\n$5\r\nRPUSH\r\n$1\r\nq\r\n$1\r\nx\r\n")
        await expect(r2, b":1\r\n")
        await expect(r1, b"*2\r\n$1\r\nq\r\n$1\r\nx\r\n")
        await close_writers(w1, w2)
        await server.close()


@pytest.mark.asyncio
async def test_infinite_blpop_eof_closes_waiter_before_later_push():
    async with MiniRedis.open() as redis:
        server = await redis.start_tcp("127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(*server.address)
        await send(writer, b"*3\r\n$5\r\nBLPOP\r\n$1\r\nq\r\n$1\r\n0\r\n")
        await redis.debug_wait_for_waiters(1)
        writer.close()
        await writer.wait_closed()
        await redis.debug_wait_for_waiters(0)
        await redis.debug_wait_for_sessions(0)
        producer = redis.direct_client()
        assert await producer.execute(CommandRequest(b"RPUSH", (b"q", b"x"))) == Number(
            1
        )
        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"x")
        assert await reader.read() == b""
        await server.close()


@pytest.mark.asyncio
async def test_subscribe_message_ping_unsubscribe_share_one_ordered_outbox():
    async with MiniRedis.open() as redis:
        server = await redis.start_tcp("127.0.0.1", 0)
        sub_r, sub_w = await asyncio.open_connection(*server.address)
        pub_r, pub_w = await asyncio.open_connection(*server.address)
        await send(sub_w, b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nc\r\n")
        await expect(
            sub_r,
            b"*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n",
        )
        await send(
            pub_w,
            b"*3\r\n$7\r\nPUBLISH\r\n$1\r\nc\r\n$1\r\nm\r\n",
        )
        await expect(pub_r, b":1\r\n")
        await expect(
            sub_r,
            b"*3\r\n$7\r\nmessage\r\n$1\r\nc\r\n$1\r\nm\r\n",
        )
        await send(sub_w, b"*2\r\n$4\r\nPING\r\n$1\r\nx\r\n")
        await expect(sub_r, b"*2\r\n$4\r\npong\r\n$1\r\nx\r\n")
        await send(
            sub_w,
            b"*2\r\n$11\r\nUNSUBSCRIBE\r\n$1\r\nc\r\n",
        )
        await expect(
            sub_r,
            b"*3\r\n$11\r\nunsubscribe\r\n$1\r\nc\r\n:0\r\n",
        )
        await send(sub_w, b"*1\r\n$4\r\nPING\r\n")
        await expect(sub_r, b"+PONG\r\n")
        await close_writers(sub_w, pub_w)
        await server.close()


@pytest.mark.asyncio
async def test_full_tcp_outbox_closes_only_the_slow_subscriber():
    async with MiniRedis.open(outbox_limit=1) as redis:
        server = await redis.start_tcp("127.0.0.1", 0)
        server.debug_pause_new_writers()
        slow_r, slow_w = await asyncio.open_connection(*server.address)
        await redis.debug_wait_for_sessions(1)
        server.debug_resume_new_writers()
        fast_r, fast_w = await asyncio.open_connection(*server.address)
        pub_r, pub_w = await asyncio.open_connection(*server.address)
        await redis.debug_wait_for_sessions(3)

        await send(slow_w, b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nc\r\n")
        await redis.debug_wait_until_idle()
        await send(fast_w, b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nc\r\n")
        await expect(
            fast_r,
            b"*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n",
        )
        await send(
            pub_w,
            b"*3\r\n$7\r\nPUBLISH\r\n$1\r\nc\r\n$1\r\nm\r\n",
        )
        await expect(pub_r, b":1\r\n")
        await expect(
            fast_r,
            b"*3\r\n$7\r\nmessage\r\n$1\r\nc\r\n$1\r\nm\r\n",
        )
        await redis.debug_wait_for_sessions(2)
        assert await slow_r.read() == b""

        await close_writers(fast_w, pub_w, slow_w)
        await server.close()


@pytest.mark.asyncio
async def test_runtime_close_does_not_spend_outbox_grace_twice():
    redis = MiniRedis.open(outbox_drain_grace_ms=60_000)
    await redis.start()
    server = await redis.start_tcp("127.0.0.1", 0)
    reader, client_writer = await asyncio.open_connection(*server.address)
    await redis.debug_wait_for_sessions(1)
    session = server.debug_sessions()[0]
    gated = CloseReleasedWriter(session.writer)
    session.writer = gated

    await send(client_writer, b"*1\r\n$4\r\nPING\r\n")
    await gated.drain_started.wait()
    assert session.endpoint.outbox.pending_count == 0
    try:
        async with asyncio.timeout(1):
            await redis.close()
    finally:
        gated.force_release()
        await redis.close()

    assert redis.closed
    assert server.closed
    assert server.owned_task_count == 0
    assert await reader.read() == b"+PONG\r\n"
    client_writer.close()
    await client_writer.wait_closed()

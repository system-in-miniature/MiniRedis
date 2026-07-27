import asyncio

import pytest

from miniredis import MiniRedis


async def expect(reader, wire):
    assert await reader.readexactly(len(wire)) == wire


@pytest.mark.asyncio
async def test_tcp_fragmentation_multiple_commands_and_close():
    async with MiniRedis.open() as redis:
        server = await redis.start_tcp("127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(*server.address)
        writer.write(b"*1\r\n$4\r\nPI")
        await writer.drain()
        writer.write(
            b"NG\r\n"
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
            b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
        )
        await writer.drain()
        await expect(reader, b"+PONG\r\n")
        await expect(reader, b"+OK\r\n")
        await expect(reader, b"$1\r\nv\r\n")
        writer.close()
        await writer.wait_closed()
        await server.close()
        await server.close()
        assert server.closed


@pytest.mark.asyncio
async def test_protocol_error_is_written_by_outbox_writer_then_closes():
    async with MiniRedis.open() as redis:
        server = await redis.start_tcp("127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(*server.address)
        writer.write(b"+not-a-command\r\n")
        await writer.drain()
        assert await reader.readline() == (
            b"-CLOSED protocol error: command must be a non-empty array\r\n"
        )
        assert await reader.read() == b""
        writer.close()
        await writer.wait_closed()
        await server.close()


@pytest.mark.asyncio
async def test_truncated_frame_at_eof_uses_the_protocol_error_path():
    async with MiniRedis.open() as redis:
        server = await redis.start_tcp("127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(*server.address)
        writer.write(b"*2\r\n$3\r\nGET\r\n$1\r\n")
        await writer.drain()
        writer.write_eof()
        await writer.drain()
        assert await reader.readline() == (
            b"-CLOSED protocol error: truncated RESP frame\r\n"
        )
        assert await reader.read() == b""
        writer.close()
        await writer.wait_closed()
        await server.close()


@pytest.mark.asyncio
async def test_server_close_settles_reader_writer_and_registration():
    async with MiniRedis.open() as redis:
        server = await redis.start_tcp("127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(*server.address)
        await redis.debug_wait_for_sessions(1)
        await server.close()
        assert await reader.read() == b""
        assert server.closed
        assert server.owned_task_count == 0
        assert redis.debug_stats().sessions == 0
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_start_tcp_rejects_non_running_runtime_before_bind():
    redis = MiniRedis.open()
    with pytest.raises(RuntimeError, match="runtime is not running"):
        await redis.start_tcp("127.0.0.1", 0)
    await redis.start()
    await redis.close()
    with pytest.raises(RuntimeError, match="runtime is not running"):
        await redis.start_tcp("127.0.0.1", 0)

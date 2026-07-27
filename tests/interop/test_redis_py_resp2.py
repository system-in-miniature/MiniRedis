import pytest
import redis.asyncio as redis_async

from miniredis import MiniRedis


@pytest.mark.interop
@pytest.mark.asyncio
async def test_redis_py_resp2_string_and_hash_smoke():
    async with MiniRedis.open() as runtime:
        server = await runtime.start_tcp("127.0.0.1", 0)
        host, port = server.address
        client = redis_async.Redis(
            host=host,
            port=port,
            protocol=2,
            decode_responses=False,
            driver_info=None,
        )
        try:
            assert await client.ping() is True
            assert await client.set(b"k", b"1")
            assert await client.incr(b"k") == 2
            assert await client.hset(b"h", b"f", b"v") == 1
            assert await client.hget(b"h", b"f") == b"v"
        finally:
            await client.aclose()
            await server.close()

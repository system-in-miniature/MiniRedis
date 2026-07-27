"""The one deliberately failing contract shipped for Day 01."""

import pytest

from miniredis import server as server_module


@pytest.mark.red
@pytest.mark.asyncio
async def test_server_binds_ephemeral_port_and_closes() -> None:
    server_type = getattr(server_module, "MiniRedisServer", None)
    assert server_type is not None, (
        "Day 01 RED: miniredis.server.MiniRedisServer is not implemented"
    )

    server = server_type(host="127.0.0.1", port=0)
    await server.start()
    try:
        host, port = server.address
        assert host == "127.0.0.1"
        assert port > 0
        assert server.closed is False
    finally:
        await server.close()

    await server.close()
    assert server.closed is True


import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.core.database import Database
from miniredis.core.reply import Failure


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "command_request",
    [
        CommandRequest(b"GET", (b"k",)),
        CommandRequest(b"HGET", (b"k", b"f")),
        CommandRequest(b"LRANGE", (b"k", b"0", b"-1")),
        CommandRequest(b"SISMEMBER", (b"k", b"m")),
        CommandRequest(b"ZSCORE", (b"k", b"m")),
    ],
)
async def test_wrongtype_never_allocates_commit(command_request):
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        if command_request.name == b"GET":
            await c.execute(CommandRequest(b"HSET", (b"k", b"f", b"v")))
        else:
            await c.execute(CommandRequest(b"SET", (b"k", b"v")))
        before = runtime.debug_commit_seq
        reply = await c.execute(command_request)
        assert isinstance(reply, Failure)
        assert reply.code == "WRONGTYPE"
        assert runtime.debug_commit_seq == before


@pytest.mark.asyncio
async def test_commits_rebuild_the_same_logical_database():
    runtime = MiniRedis.open(debug_record_applied_batches=True)
    await runtime.start()
    client = runtime.direct_client()
    await client.execute(CommandRequest(b"SET", (b"s", b"1")))
    await client.execute(CommandRequest(b"HSET", (b"h", b"f", b"v")))
    await client.execute(CommandRequest(b"RPUSH", (b"l", b"a", b"b")))
    await client.execute(CommandRequest(b"SADD", (b"set", b"a", b"b")))
    await client.execute(CommandRequest(b"ZADD", (b"z", b"1", b"a")))
    batches = runtime.debug_applied_batches()
    expected = runtime.debug_logical_items()
    await runtime.close()

    replay = Database()
    for batch in batches:
        replay.apply_batch(batch, track_access=False)
    assert replay.logical_items() == expected


@pytest.mark.asyncio
async def test_applied_batches_are_not_recorded_by_default():
    runtime = MiniRedis.open()
    await runtime.start()
    await runtime.direct_client().execute(
        CommandRequest(b"SET", (b"k", b"v"))
    )
    batches = runtime.debug_applied_batches()
    await runtime.close()

    assert batches == ()

import asyncio

import pytest

from miniredis.commands.request import CommandRequest
from miniredis.core.reply import Failure, Ok
from miniredis.persistence.aof import AofAppendFailed, AofAppendOk
from miniredis.runtime import RuntimeState
from tests.helpers.runtime import open_test_runtime


class GateAofWriter:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.batches = []
        self.failure: str | None = None

    async def append(self, batch):
        self.batches.append(batch)
        self.entered.set()
        await self.release.wait()
        if self.failure is not None:
            return AofAppendFailed(self.failure)
        return AofAppendOk(batch.seq)


@pytest.mark.asyncio
async def test_state_and_reply_wait_behind_the_aof_barrier():
    writer = GateAofWriter()
    runtime = await open_test_runtime(aof_appender=writer)
    client = runtime.direct_client()

    pending = asyncio.create_task(
        client.execute(CommandRequest(b"SET", (b"k", b"v")))
    )
    await writer.entered.wait()

    assert runtime.database.commit_seq == 0
    assert b"k" not in runtime.database.entries
    assert not pending.done()
    assert writer.batches[0].seq == 1

    writer.release.set()
    assert await pending == Ok()
    assert runtime.database.commit_seq == 1
    assert runtime.database.entries[b"k"].value.data == b"v"
    await runtime.close()


@pytest.mark.asyncio
async def test_executor_processes_no_later_state_event_during_barrier():
    writer = GateAofWriter()
    runtime = await open_test_runtime(aof_appender=writer)
    client = runtime.direct_client()

    first = asyncio.create_task(
        client.execute(CommandRequest(b"SET", (b"a", b"1")))
    )
    await writer.entered.wait()
    second = asyncio.create_task(
        client.execute(CommandRequest(b"SET", (b"b", b"2")))
    )
    await runtime.debug_wait_until_queued(1)

    assert len(writer.batches) == 1
    assert not second.done()

    writer.release.set()
    assert await first == Ok()
    assert await second == Ok()
    assert [item.seq for item in writer.batches] == [1, 2]
    await runtime.close()


@pytest.mark.asyncio
async def test_append_failure_does_not_apply_and_fails_the_runtime():
    writer = GateAofWriter()
    writer.failure = "disk full"
    runtime = await open_test_runtime(aof_appender=writer)
    client = runtime.direct_client()

    pending = asyncio.create_task(
        client.execute(CommandRequest(b"SET", (b"k", b"v")))
    )
    await writer.entered.wait()
    writer.release.set()

    reply = await pending
    assert isinstance(reply, Failure)
    assert reply.code == "ERR"
    assert "durability failure" in reply.message
    assert runtime.state is RuntimeState.FAILED
    assert runtime.database.commit_seq == 0
    assert b"k" not in runtime.database.entries

    rejected = await client.execute(
        CommandRequest(b"SET", (b"later", b"x"))
    )
    assert isinstance(rejected, Failure)
    assert rejected.code == "CLOSED"
    await runtime.close()


@pytest.mark.asyncio
async def test_ordinary_error_and_noop_never_call_aof():
    writer = GateAofWriter()
    runtime = await open_test_runtime(aof_appender=writer)
    client = runtime.direct_client()

    wrong_arity = await client.execute(CommandRequest(b"SET", (b"k",)))
    missing_delete = await client.execute(
        CommandRequest(b"DEL", (b"missing",))
    )

    assert isinstance(wrong_arity, Failure)
    assert missing_delete.value == 0
    assert writer.batches == []
    assert runtime.database.commit_seq == 0
    await runtime.close()

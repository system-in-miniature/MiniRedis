import asyncio

import pytest

from miniredis.commands.request import CommandRequest
from miniredis.config import MiniRedisConfig
from miniredis.core.commit import PutEntry
from miniredis.core.reply import Bytes
from miniredis.persistence.aof import AofPolicy, load_aof
from miniredis.persistence.codec import decode_snapshot_file
from miniredis.persistence.snapshot import SnapshotSaved
from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
from tests.helpers.runtime import open_test_runtime


OWNER_FIELDS = (
    "accepted_requests",
    "active_transactions",
    "aof_tasks",
    "control_producers",
    "executor_tasks",
    "owned_tasks",
    "pending_futures",
    "replica_links",
    "replica_tasks",
    "sessions",
    "snapshot_jobs",
    "subscriptions",
    "tcp_servers",
    "tcp_sessions",
    "tcp_tasks",
    "timer_handles",
    "waiters",
    "watched_keys",
)


def assert_zero_owners(runtime) -> None:
    stats = runtime.debug_stats()
    observed = {field: getattr(stats, field) for field in OWNER_FIELDS}
    assert observed == dict.fromkeys(OWNER_FIELDS, 0)


def command_wire(*parts: bytes) -> bytes:
    return (
        b"*"
        + str(len(parts)).encode("ascii")
        + b"\r\n"
        + b"".join(
            b"$" + str(len(part)).encode("ascii") + b"\r\n" + part + b"\r\n"
            for part in parts
        )
    )


async def send(
    writer: asyncio.StreamWriter,
    *parts: bytes,
) -> None:
    writer.write(command_wire(*parts))
    await writer.drain()


async def expect(
    reader: asyncio.StreamReader,
    wire: bytes,
) -> None:
    assert await reader.readexactly(len(wire)) == wire


@pytest.mark.asyncio
async def test_final_acceptance_activates_components_then_leaves_no_owners(
    tmp_path,
):
    aof_path = tmp_path / "appendonly.mraof"
    snapshot_path = tmp_path / "dump.mrsnap"
    primary = await open_test_runtime(
        config=MiniRedisConfig(
            aof_path=aof_path,
            aof_policy=AofPolicy.ALWAYS,
            snapshot_path=snapshot_path,
            replica_drain_grace_ms=1000,
        ),
        snapshot_write_gate=True,
    )
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=8)
    await primary.attach_replica(sink)
    server = await primary.start_tcp("127.0.0.1", 0)

    blpop_reader, blpop_writer = await asyncio.open_connection(*server.address)
    command_reader, command_writer = await asyncio.open_connection(*server.address)
    sub_reader, sub_writer = await asyncio.open_connection(*server.address)
    writers = (blpop_writer, command_writer, sub_writer)
    await primary.debug_wait_for_sessions(3)

    await send(blpop_writer, b"BLPOP", b"queue", b"5")
    await primary.debug_wait_for_waiters(1)
    blocked = primary.debug_stats()
    assert blocked.accepted_requests == 1
    assert blocked.pending_futures == 1
    assert blocked.timer_handles == 1
    assert blocked.waiters == 1
    assert blocked.sessions == 3
    await send(command_writer, b"RPUSH", b"queue", b"item")
    await expect(command_reader, b":1\r\n")
    await expect(
        blpop_reader,
        b"*2\r\n$5\r\nqueue\r\n$4\r\nitem\r\n",
    )

    await send(command_writer, b"SET", b"replicated", b"durable")
    await expect(command_reader, b"+OK\r\n")

    await send(sub_writer, b"SUBSCRIBE", b"news")
    await expect(
        sub_reader,
        b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n",
    )
    await send(command_writer, b"PUBLISH", b"news", b"payload")
    await expect(command_reader, b":1\r\n")
    await expect(
        sub_reader,
        b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$7\r\npayload\r\n",
    )

    saving = asyncio.create_task(primary.save_snapshot())
    await primary.debug_snapshot_write_entered.wait()
    active = primary.debug_stats()
    primary.debug_snapshot_write_release.set()
    saved = await saving
    assert isinstance(saved, SnapshotSaved)

    assert active.accepting_users is True
    assert active.accepted_requests == 0
    assert active.aof_tasks >= 1
    assert active.control_producers >= 1
    assert active.executor_tasks == 1
    assert active.pending_futures == 0
    assert active.replica_links == 1
    assert active.replica_tasks == 1
    assert active.sessions == 3
    assert active.snapshot_jobs == 1
    assert active.subscriptions == 1
    assert active.tcp_servers == 1
    assert active.tcp_sessions == 3
    assert active.tcp_tasks >= 6
    assert active.timer_handles == 0
    assert active.waiters == 0

    await sink.wait_until_applied(primary.debug_commit_seq)
    assert await replica.direct_client().execute(
        CommandRequest(b"GET", (b"replicated",))
    ) == Bytes(b"durable")

    await primary.close()
    await primary.close()
    assert primary.closed
    assert server.closed
    assert server.owned_task_count == 0
    assert sink.status.state is ReplicaSinkState.STOPPED
    assert sink.owned_task_count == 0

    for reader in (blpop_reader, command_reader, sub_reader):
        assert await asyncio.wait_for(reader.read(), timeout=1) == b""
    for writer in writers:
        writer.close()
        await writer.wait_closed()

    batches = load_aof(
        aof_path, repair_truncated_tail=False
    ).batches
    assert batches[-1].seq == primary.debug_commit_seq
    aof_entries = {
        operation.key: operation.entry
        for batch in batches
        for operation in batch.operations
        if isinstance(operation, PutEntry)
    }
    assert aof_entries[b"replicated"].value.data == b"durable"
    image = decode_snapshot_file(snapshot_path.read_bytes())
    assert image.checkpoint_seq == primary.debug_commit_seq
    assert dict(image.entries)[b"replicated"].value.data == b"durable"

    await replica.close()
    assert primary.debug_stats().accepting_users is False
    assert replica.debug_stats().accepting_users is False
    assert_zero_owners(primary)
    assert_zero_owners(replica)

    current = asyncio.current_task()
    leaked = [
        task
        for task in asyncio.all_tasks()
        if task is not current
        and not task.done()
        and task.get_name().startswith("miniredis")
    ]
    assert leaked == []

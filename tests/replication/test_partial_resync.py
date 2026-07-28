import asyncio
from dataclasses import dataclass

import pytest

from miniredis import CommandRequest
from miniredis.config import MiniRedisConfig
from miniredis.core.reply import Ok
from miniredis.replication.backlog import (
    FullSyncAttachment,
    PartialSyncAttachment,
    ReplicationCursor,
)
from miniredis.replication.sink import (
    ReplicaSink,
    ReplicaSinkState,
    ReplicaSyncMode,
)
from tests.helpers.runtime import open_test_runtime


@dataclass
class AttachmentProbe:
    attachment: FullSyncAttachment | PartialSyncAttachment | None = None

    def register_attachment(
        self,
        attachment: FullSyncAttachment | PartialSyncAttachment,
    ) -> None:
        self.attachment = attachment

    def offer(self, _batch) -> bool:
        return True


async def seeded_primary(tmp_path, *, backlog_batches=4):
    primary = await open_test_runtime(
        config=MiniRedisConfig(
            replication_backlog_batches=backlog_batches,
        ),
        replication_id_factory=lambda: "primary-A",
    )
    client = primary.direct_client()
    for seq in range(1, 4):
        assert await client.execute(
            CommandRequest(
                b"SET",
                (f"k{seq}".encode(), str(seq).encode()),
            )
        ) == Ok()
    return primary


@pytest.mark.asyncio
async def test_first_attachment_is_full_sync(tmp_path):
    primary = await seeded_primary(tmp_path)
    probe = AttachmentProbe()

    attachment = await primary.executor.attach_replica(probe, None)

    assert isinstance(attachment, FullSyncAttachment)
    assert attachment.replication_id == "primary-A"
    assert attachment.image.checkpoint_seq == 3
    await primary.executor.detach_replica(attachment.generation)
    await primary.close()


@pytest.mark.asyncio
async def test_matching_cursor_uses_captured_backlog_range(tmp_path):
    primary = await seeded_primary(tmp_path)
    probe = AttachmentProbe()
    cursor = ReplicationCursor("primary-A", 1)

    attachment = await primary.executor.attach_replica(probe, cursor)

    assert isinstance(attachment, PartialSyncAttachment)
    assert attachment.cursor == cursor
    assert tuple(batch.seq for batch in attachment.batches) == (2, 3)
    assert attachment.boundary_seq == 3
    await primary.executor.detach_replica(attachment.generation)
    await primary.close()


@pytest.mark.asyncio
async def test_matching_current_cursor_uses_empty_partial_sync(tmp_path):
    primary = await seeded_primary(tmp_path)
    probe = AttachmentProbe()
    cursor = ReplicationCursor("primary-A", 3)

    attachment = await primary.executor.attach_replica(probe, cursor)

    assert isinstance(attachment, PartialSyncAttachment)
    assert attachment.batches == ()
    assert attachment.boundary_seq == 3
    await primary.executor.detach_replica(attachment.generation)
    await primary.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cursor",
    [
        ReplicationCursor("other-primary", 1),
        ReplicationCursor("primary-A", 4),
        ReplicationCursor("primary-A", 0),
    ],
)
async def test_diverged_or_uncovered_cursor_falls_back_to_full_sync(
    tmp_path,
    cursor,
):
    primary = await seeded_primary(tmp_path, backlog_batches=2)
    probe = AttachmentProbe()

    attachment = await primary.executor.attach_replica(probe, cursor)

    assert isinstance(attachment, FullSyncAttachment)
    assert attachment.replication_id == "primary-A"
    assert attachment.image.checkpoint_seq == 3
    await primary.executor.detach_replica(attachment.generation)
    await primary.close()


@pytest.mark.asyncio
async def test_short_disconnect_resumes_only_missing_batches():
    primary = await open_test_runtime(
        replication_id_factory=lambda: "primary-A"
    )
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=4)
    await primary.attach_replica(sink)
    client = primary.direct_client()
    assert await client.execute(
        CommandRequest(b"SET", (b"a", b"1"))
    ) == Ok()
    await sink.wait_until_applied(primary.debug_commit_seq)

    retained = await sink.disconnect()
    assert retained.state is ReplicaSinkState.DETACHED
    assert retained.cursor == ReplicationCursor("primary-A", 1)
    assert await client.execute(
        CommandRequest(b"SET", (b"b", b"2"))
    ) == Ok()

    status = await primary.attach_replica(sink)
    await sink.wait_until_applied(primary.debug_commit_seq)

    assert status.sync_mode is ReplicaSyncMode.PARTIAL
    assert sink.status.applied_seq == primary.debug_commit_seq
    assert tuple(replica.database.entries) == (b"a", b"b")
    await primary.close()
    await replica.close()


@pytest.mark.asyncio
async def test_partial_catchup_precedes_concurrent_live_batch():
    install_gate = asyncio.Event()
    install_gate.set()
    primary = await open_test_runtime(
        replication_id_factory=lambda: "primary-A"
    )
    replica = await open_test_runtime()
    sink = ReplicaSink(
        replica,
        queue_limit=4,
        install_gate=install_gate,
    )
    await primary.attach_replica(sink)
    client = primary.direct_client()
    await client.execute(CommandRequest(b"SET", (b"a", b"1")))
    await sink.wait_until_applied(1)
    await sink.disconnect()
    await client.execute(CommandRequest(b"SET", (b"b", b"2")))

    install_gate.clear()
    attaching = asyncio.create_task(primary.attach_replica(sink))
    await sink.attachment_captured.wait()
    assert sink.status.primary_seq == 2
    await client.execute(CommandRequest(b"SET", (b"c", b"3")))
    assert sink.status.queued == 1

    install_gate.set()
    status = await attaching
    await sink.wait_until_applied(3)

    assert status.sync_mode is ReplicaSyncMode.PARTIAL
    assert replica.debug_commit_seq == 3
    assert tuple(replica.database.entries) == (b"a", b"b", b"c")
    assert sink.status.state is ReplicaSinkState.STREAMING
    await primary.close()
    await replica.close()

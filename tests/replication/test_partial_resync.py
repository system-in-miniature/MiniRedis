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
from tests.unit.persistence.test_framing import batch


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
async def test_empty_backlog_accepts_current_cursor():
    primary = await open_test_runtime(
        replication_id_factory=lambda: "primary-A"
    )
    probe = AttachmentProbe()

    attachment = await primary.executor.attach_replica(
        probe,
        ReplicationCursor("primary-A", 0),
    )

    assert isinstance(attachment, PartialSyncAttachment)
    assert attachment.batches == ()
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


@pytest.mark.asyncio
async def test_cursor_exactly_before_oldest_backlog_batch_is_partial():
    primary = await open_test_runtime(
        config=MiniRedisConfig(replication_backlog_batches=2),
        replication_id_factory=lambda: "primary-A",
    )
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=4)
    await primary.attach_replica(sink)
    client = primary.direct_client()
    await client.execute(CommandRequest(b"SET", (b"a", b"1")))
    await sink.wait_until_applied(1)
    await sink.disconnect()
    await client.execute(CommandRequest(b"SET", (b"b", b"2")))
    await client.execute(CommandRequest(b"SET", (b"c", b"3")))

    status = await primary.attach_replica(sink)
    await sink.wait_until_applied(3)

    assert status.sync_mode is ReplicaSyncMode.PARTIAL
    await primary.close()
    await replica.close()


@pytest.mark.asyncio
async def test_backlog_gap_falls_back_to_full_and_replaces_stale_keys():
    primary = await open_test_runtime(
        config=MiniRedisConfig(replication_backlog_batches=2),
        replication_id_factory=lambda: "primary-A",
    )
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=4)
    await primary.attach_replica(sink)
    client = primary.direct_client()
    await client.execute(CommandRequest(b"SET", (b"obsolete", b"1")))
    await sink.wait_until_applied(1)
    await sink.disconnect()
    await client.execute(CommandRequest(b"DEL", (b"obsolete",)))
    await client.execute(CommandRequest(b"SET", (b"b", b"3")))
    await client.execute(CommandRequest(b"SET", (b"c", b"4")))

    status = await primary.attach_replica(sink)

    assert status.sync_mode is ReplicaSyncMode.FULL
    assert b"obsolete" not in replica.database.entries
    assert tuple(replica.database.entries) == (b"b", b"c")
    await primary.close()
    await replica.close()


@pytest.mark.asyncio
async def test_replication_stats_report_history_without_mutating_it():
    primary = await open_test_runtime(
        config=MiniRedisConfig(replication_backlog_batches=4),
        replication_id_factory=lambda: "primary-A",
    )
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=4)
    first = await primary.attach_replica(sink)
    client = primary.direct_client()
    await client.execute(CommandRequest(b"SET", (b"a", b"1")))
    await sink.wait_until_applied(1)
    await sink.disconnect()
    await client.execute(CommandRequest(b"SET", (b"b", b"2")))
    resumed = await primary.attach_replica(sink)
    await sink.wait_until_applied(2)

    before = primary.debug_stats()
    after = primary.debug_stats()

    assert first.sync_mode is ReplicaSyncMode.FULL
    assert resumed.sync_mode is ReplicaSyncMode.PARTIAL
    assert before.replication_id == "primary-A"
    assert before.primary_seq == 2
    assert before.backlog_oldest_seq == 1
    assert before.backlog_newest_seq == 2
    assert before.backlog_batch_count == 2
    assert before.full_sync_count == 1
    assert before.partial_sync_count == 1
    assert after == before
    await primary.close()
    await replica.close()


@pytest.mark.asyncio
async def test_failed_replica_resume_validation_detaches_primary_link():
    primary = await open_test_runtime(
        replication_id_factory=lambda: "primary-A"
    )
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=4)
    await primary.attach_replica(sink)
    await sink.disconnect()
    replica.database.apply_batch(batch(1), track_access=False)

    status = await primary.attach_replica(sink)

    assert status.state is ReplicaSinkState.NEEDS_RESYNC
    assert primary.debug_stats().replica_links == 0
    await primary.close()
    await replica.close()

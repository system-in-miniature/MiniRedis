import asyncio
from dataclasses import dataclass

import pytest

from miniredis import CommandRequest
from miniredis.core.commit import (
    CommitBatch,
    CommitTrigger,
    PutEntry,
    StoredEntry,
    StoredString,
)
from miniredis.core.reply import Bytes, Ok
from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
from miniredis.replication.backlog import (
    FullSyncAttachment,
    ReplicationCursor,
)
from tests.helpers.runtime import open_test_runtime


def batch(seq: int, value: bytes) -> CommitBatch:
    return CommitBatch(
        seq,
        (
            PutEntry(
                b"k",
                StoredEntry(StoredString(value), None, seq),
            ),
        ),
        CommitTrigger.CLIENT,
    )


@dataclass
class AttachmentProbe:
    attachment: object | None = None

    def register_attachment(self, attachment) -> None:
        self.attachment = attachment

    def offer(self, _batch) -> bool:
        return True


@pytest.mark.asyncio
async def test_apply_accepted_before_promotion_finishes_before_barrier():
    primary = await open_test_runtime()
    replica = await open_test_runtime(replica_apply_gate=True)
    sink = ReplicaSink(replica, queue_limit=4)
    await primary.attach_replica(sink)
    generation = sink.status.generation
    assert generation is not None

    accepted = asyncio.create_task(
        replica.executor.apply_replica_batch(generation, batch(1, b"old"))
    )
    await replica.debug_replica_apply_entered.wait()
    promoting = asyncio.create_task(sink.promote(source_alive=True))
    replica.debug_replica_apply_release.set()
    promotion = await promoting

    assert await accepted is True
    assert promotion.applied_seq == 1
    assert promotion.writable is True
    assert sink.status.state is ReplicaSinkState.PROMOTED
    assert await replica.direct_client().execute(
        CommandRequest(b"GET", (b"k",))
    ) == Bytes(b"old")
    await primary.close()
    await replica.close()


@pytest.mark.asyncio
async def test_late_old_generation_cannot_overwrite_post_promotion_write():
    primary = await open_test_runtime()
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=4)
    await primary.attach_replica(sink)
    generation = sink.status.generation
    assert generation is not None
    await sink.promote(source_alive=True)

    assert await replica.direct_client().execute(
        CommandRequest(b"SET", (b"k", b"new"))
    ) == Ok()
    applied = await replica.executor.apply_replica_batch(
        generation,
        batch(2, b"stale"),
    )

    assert applied is False
    assert await replica.direct_client().execute(
        CommandRequest(b"GET", (b"k",))
    ) == Bytes(b"new")
    await primary.close()
    await replica.close()


@pytest.mark.asyncio
async def test_link_generations_are_never_reused():
    primary = await open_test_runtime()
    first_replica = await open_test_runtime()
    second_replica = await open_test_runtime()
    first = ReplicaSink(first_replica, queue_limit=2)
    second = ReplicaSink(second_replica, queue_limit=2)
    await primary.attach_replica(first)
    first_generation = first.status.generation
    await first.promote(source_alive=True)
    await primary.attach_replica(second)

    assert first_generation is not None
    assert second.status.generation == first_generation + 1
    await primary.close()
    await first_replica.close()
    await second_replica.close()


@pytest.mark.asyncio
async def test_promotion_fences_old_history_and_starts_new_backlog():
    promoted_ids = iter(("replica-seed", "promoted-B"))
    primary = await open_test_runtime(
        replication_id_factory=lambda: "primary-A"
    )
    promoted = await open_test_runtime(
        replication_id_factory=lambda: next(promoted_ids)
    )
    sink = ReplicaSink(promoted, queue_limit=4)
    await primary.attach_replica(sink)
    old_id = sink.status.replication_id

    result = await sink.promote(source_alive=True)

    assert old_id == "primary-A"
    assert result.replication_id == "promoted-B"
    assert result.replication_id != old_id
    assert sink.status.replication_id == "promoted-B"
    assert promoted.debug_replication_backlog_count == 0

    probe = AttachmentProbe()
    attachment = await promoted.executor.attach_replica(
        probe,
        ReplicationCursor(old_id, result.applied_seq),
    )
    assert isinstance(attachment, FullSyncAttachment)
    await promoted.executor.detach_replica(attachment.generation)

    assert await promoted.direct_client().execute(
        CommandRequest(b"SET", (b"k", b"new"))
    ) == Ok()
    assert promoted.debug_replication_backlog_count == 1
    assert promoted.debug_replication_backlog_oldest_seq == (
        result.applied_seq + 1
    )
    await primary.close()
    await promoted.close()

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from miniredis.core.commit import CommitBatch
from miniredis.core.commit import SnapshotImage


@dataclass(frozen=True, slots=True)
class ReplicationCursor:
    replication_id: str
    applied_seq: int


@dataclass(frozen=True, slots=True)
class FullSyncAttachment:
    generation: int
    replication_id: str
    image: SnapshotImage


@dataclass(frozen=True, slots=True)
class PartialSyncAttachment:
    generation: int
    replication_id: str
    cursor: ReplicationCursor
    boundary_seq: int
    batches: tuple[CommitBatch, ...]


ReplicaAttachment = FullSyncAttachment | PartialSyncAttachment


class ReplicationBacklog:
    def __init__(self, capacity_batches: int) -> None:
        if capacity_batches <= 0:
            raise ValueError(
                "replication backlog capacity must be positive"
            )
        self._capacity = capacity_batches
        self._batches: deque[CommitBatch] = deque()

    @property
    def oldest_seq(self) -> int | None:
        return self._batches[0].seq if self._batches else None

    @property
    def newest_seq(self) -> int | None:
        return self._batches[-1].seq if self._batches else None

    @property
    def batch_count(self) -> int:
        return len(self._batches)

    def append(self, batch: CommitBatch) -> None:
        if (
            self._batches
            and batch.seq != self._batches[-1].seq + 1
        ):
            raise ValueError("replication backlog must be contiguous")
        self._batches.append(batch)
        while len(self._batches) > self._capacity:
            self._batches.popleft()

    def missing_after(
        self,
        applied_seq: int,
        *,
        current_seq: int,
    ) -> tuple[CommitBatch, ...] | None:
        if applied_seq > current_seq:
            return None
        if applied_seq == current_seq:
            return ()
        expected = applied_seq + 1
        selected = tuple(
            batch for batch in self._batches if batch.seq >= expected
        )
        if not selected or selected[0].seq != expected:
            return None
        if selected[-1].seq != current_seq:
            return None
        return selected

    def clear(self) -> None:
        self._batches.clear()

"""Define immutable state and the ordered unit used for durability and propagation.

``CommitBatch`` corresponds to one atomic Redis propagation unit: it crosses
AOF, local apply, replication, and recovery without exposing partial operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TypeAlias


class CommitTrigger(str, Enum):
    CLIENT = "client"
    ACTIVE_EXPIRE = "active_expire"


class DeleteReason(str, Enum):
    CLIENT = "client"
    EXPIRED = "expired"
    EVICTED = "evicted"


@dataclass(frozen=True, slots=True)
class StoredString:
    data: bytes


@dataclass(frozen=True, slots=True)
class StoredHash:
    items: tuple[tuple[bytes, bytes], ...]


@dataclass(frozen=True, slots=True)
class StoredList:
    items: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class StoredSet:
    items: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class StoredZSet:
    items: tuple[tuple[bytes, float], ...]


StoredValue: TypeAlias = StoredString | StoredHash | StoredList | StoredSet | StoredZSet


@dataclass(frozen=True, slots=True)
class StoredEntry:
    value: StoredValue
    expire_at_ms: int | None
    mutation_version: int


@dataclass(frozen=True, slots=True)
class PutEntry:
    key: bytes
    entry: StoredEntry


@dataclass(frozen=True, slots=True)
class DeleteKey:
    key: bytes
    reason: DeleteReason


CommitOperation: TypeAlias = PutEntry | DeleteKey


@dataclass(frozen=True, slots=True)
class CommitBatch:
    seq: int
    operations: tuple[CommitOperation, ...]
    trigger: CommitTrigger

    def __post_init__(self) -> None:
        if self.seq <= 0:
            raise ValueError("commit seq must be positive")
        if not self.operations:
            raise ValueError("commit batch operations cannot be empty")


@dataclass(frozen=True, slots=True)
class PreparedCommit:
    operations: tuple[CommitOperation, ...]
    trigger: CommitTrigger

    def to_batch(self, seq: int) -> CommitBatch:
        if seq <= 0:
            raise ValueError("commit seq must be positive")
        if not self.operations:
            raise ValueError("an empty prepared commit is a no-op")
        return CommitBatch(seq, self.operations, self.trigger)


@dataclass(frozen=True, slots=True)
class SnapshotImage:
    checkpoint_seq: int
    entries: tuple[tuple[bytes, StoredEntry], ...]

    def __post_init__(self) -> None:
        if self.checkpoint_seq < 0:
            raise ValueError("checkpoint seq cannot be negative")
        keys = tuple(key for key, _entry in self.entries)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise ValueError("snapshot entries must have unique sorted keys")

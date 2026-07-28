from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from miniredis.core.commit import (
    CommitBatch,
    DeleteKey,
    PutEntry,
    SnapshotImage,
    StoredEntry,
    StoredHash,
    StoredList,
    StoredSet,
    StoredString,
    StoredValue,
    StoredZSet,
)
from miniredis.core.frequency import project_frequency
from miniredis.core.values import (
    HashValue,
    ListValue,
    RedisValue,
    SetValue,
    StringValue,
    ZSetValue,
)


ENTRY_OVERHEAD = 64
EXPIRY_OVERHEAD = 16


@dataclass(slots=True)
class Entry:
    value: RedisValue
    expire_at_ms: int | None
    mutation_version: int
    last_access_tick: int
    logical_size: int
    frequency: int = 0
    last_frequency_decay_ms: int = 0


def logical_value_size(value: RedisValue | StoredValue) -> int:
    match value:
        case StringValue(data=data) | StoredString(data=data):
            return 16 + len(data)
        case HashValue(items=items):
            return 32 + sum(
                16 + len(field) + len(item) for field, item in items.items()
            )
        case StoredHash(items=items):
            return 32 + sum(16 + len(field) + len(item) for field, item in items)
        case (
            ListValue(items=items)
            | StoredList(items=items)
            | SetValue(items=items)
            | StoredSet(items=items)
        ):
            return 32 + sum(8 + len(item) for item in items)
        case ZSetValue(scores=scores):
            return 32 + sum(24 + len(member) for member in scores)
        case StoredZSet(items=items):
            return 32 + sum(24 + len(member) for member, _score in items)
        case _:
            raise TypeError(f"unsupported Redis value: {type(value)!r}")


def logical_entry_size(
    key: bytes, value: RedisValue | StoredValue, expire_at_ms: int | None
) -> int:
    return (
        ENTRY_OVERHEAD
        + len(key)
        + logical_value_size(value)
        + (EXPIRY_OVERHEAD if expire_at_ms is not None else 0)
    )


def freeze_value(value: RedisValue) -> StoredValue:
    match value:
        case StringValue(data=data):
            return StoredString(data)
        case HashValue(items=items):
            return StoredHash(tuple(sorted(items.items())))
        case ListValue(items=items):
            return StoredList(tuple(items))
        case SetValue(items=items):
            return StoredSet(tuple(sorted(items)))
        case ZSetValue(scores=scores):
            return StoredZSet(tuple(sorted(scores.items())))
        case _:
            raise TypeError(f"unsupported Redis value: {type(value)!r}")


def thaw_value(value: StoredValue) -> RedisValue:
    match value:
        case StoredString(data=data):
            return StringValue(data)
        case StoredHash(items=items):
            return HashValue(dict(items))
        case StoredList(items=items):
            return ListValue(deque(items))
        case StoredSet(items=items):
            return SetValue(set(items))
        case StoredZSet(items=items):
            return ZSetValue(dict(items))
        case _:
            raise TypeError(f"unsupported stored value: {type(value)!r}")


def freeze_entry(entry: Entry) -> StoredEntry:
    return StoredEntry(
        value=freeze_value(entry.value),
        expire_at_ms=entry.expire_at_ms,
        mutation_version=entry.mutation_version,
    )


class Database:
    def __init__(self) -> None:
        self.entries: dict[bytes, Entry] = {}
        self.commit_seq = 0
        self.access_tick = 0
        self.logical_usage = 0
        self.key_revisions: dict[bytes, int] = {}
        self.revision_clock = 0

    def revision(self, key: bytes) -> int:
        return self.key_revisions.get(key, 0)

    def apply_batch(
        self,
        batch: CommitBatch,
        *,
        track_access: bool,
        now_ms: int = 0,
        lfu_decay_interval_ms: int = 60_000,
    ) -> None:
        next_seq = self.commit_seq + 1
        if batch.seq != next_seq:
            raise ValueError(f"expected commit seq {next_seq}, got {batch.seq}")

        staged = dict(self.entries)
        staged_access_tick = self.access_tick
        staged_key_revisions = dict(self.key_revisions)
        staged_revision_clock = self.revision_clock

        for operation in batch.operations:
            match operation:
                case DeleteKey(key=key):
                    staged.pop(key, None)
                case PutEntry(key=key, entry=entry):
                    previous = staged.get(key)
                    if track_access:
                        staged_access_tick += 1
                        if previous is None:
                            frequency = 1
                            last_frequency_decay_ms = now_ms
                        else:
                            frequency, last_frequency_decay_ms = project_frequency(
                                previous.frequency,
                                previous.last_frequency_decay_ms,
                                now_ms,
                                lfu_decay_interval_ms,
                            )
                            frequency += 1
                    else:
                        frequency = 0
                        last_frequency_decay_ms = now_ms
                    value = thaw_value(entry.value)
                    staged[key] = Entry(
                        value=value,
                        expire_at_ms=entry.expire_at_ms,
                        mutation_version=entry.mutation_version,
                        last_access_tick=staged_access_tick if track_access else 0,
                        logical_size=logical_entry_size(key, value, entry.expire_at_ms),
                        frequency=frequency,
                        last_frequency_decay_ms=last_frequency_decay_ms,
                    )
                case _:
                    raise TypeError(
                        f"unsupported commit operation: {type(operation)!r}"
                    )
            staged_revision_clock += 1
            staged_key_revisions[operation.key] = staged_revision_clock

        staged_usage = sum(entry.logical_size for entry in staged.values())
        if staged_usage < 0:
            raise AssertionError("logical usage cannot be negative")
        if any(entry.logical_size <= 0 for entry in staged.values()):
            raise AssertionError("entry logical size must be positive")

        self.entries = staged
        self.logical_usage = staged_usage
        self.access_tick = staged_access_tick
        self.key_revisions = staged_key_revisions
        self.revision_clock = staged_revision_clock
        self.commit_seq = batch.seq

    def fork(self) -> Database:
        forked = Database()
        forked.entries = {
            key: Entry(
                value=thaw_value(freeze_value(entry.value)),
                expire_at_ms=entry.expire_at_ms,
                mutation_version=entry.mutation_version,
                last_access_tick=entry.last_access_tick,
                logical_size=entry.logical_size,
                frequency=entry.frequency,
                last_frequency_decay_ms=entry.last_frequency_decay_ms,
            )
            for key, entry in self.entries.items()
        }
        forked.commit_seq = self.commit_seq
        forked.access_tick = self.access_tick
        forked.logical_usage = self.logical_usage
        forked.key_revisions = dict(self.key_revisions)
        forked.revision_clock = self.revision_clock
        return forked

    def touch_if_live(
        self,
        key: bytes,
        now_ms: int,
        lfu_decay_interval_ms: int = 60_000,
    ) -> bool:
        entry = self.entries.get(key)
        if entry is None or (
            entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms
        ):
            return False
        self.access_tick += 1
        entry.last_access_tick = self.access_tick
        entry.frequency, entry.last_frequency_decay_ms = project_frequency(
            entry.frequency,
            entry.last_frequency_decay_ms,
            now_ms,
            lfu_decay_interval_ms,
        )
        entry.frequency += 1
        return True

    def logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
        return tuple(
            (key, freeze_entry(entry)) for key, entry in sorted(self.entries.items())
        )

    def export_stored_entries(
        self,
        now_ms: int,
    ) -> tuple[tuple[bytes, StoredEntry], ...]:
        return tuple(
            (key, freeze_entry(entry))
            for key, entry in sorted(self.entries.items())
            if entry.expire_at_ms is None or entry.expire_at_ms > now_ms
        )

    def snapshot_image(self, now_ms: int) -> SnapshotImage:
        return SnapshotImage(
            checkpoint_seq=self.commit_seq,
            entries=self.export_stored_entries(now_ms),
        )

    def install_snapshot(
        self,
        image: SnapshotImage,
        *,
        now_ms: int,
    ) -> None:
        staged_entries: dict[bytes, Entry] = {}
        staged_usage = 0
        for key, stored in image.entries:
            if (
                stored.expire_at_ms is not None
                and stored.expire_at_ms <= now_ms
            ):
                continue
            value = thaw_value(stored.value)
            size = logical_entry_size(key, value, stored.expire_at_ms)
            staged_entries[key] = Entry(
                value=value,
                expire_at_ms=stored.expire_at_ms,
                mutation_version=stored.mutation_version,
                last_access_tick=0,
                logical_size=size,
                frequency=0,
                last_frequency_decay_ms=now_ms,
            )
            staged_usage += size

        self.entries = staged_entries
        self.logical_usage = staged_usage
        self.commit_seq = image.checkpoint_seq
        self.access_tick = 0
        self.key_revisions = {
            key: revision
            for revision, key in enumerate(sorted(staged_entries), start=1)
        }
        self.revision_clock = len(self.key_revisions)

    def discard_expired_for_recovery(self, now_ms: int) -> None:
        expired = tuple(
            key
            for key, entry in self.entries.items()
            if entry.expire_at_ms is not None
            and entry.expire_at_ms <= now_ms
        )
        for key in expired:
            entry = self.entries.pop(key)
            self.logical_usage -= entry.logical_size
        self.access_tick = 0
        for entry in self.entries.values():
            entry.last_access_tick = 0

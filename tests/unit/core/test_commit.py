from collections import deque

import pytest

from miniredis.core.commit import (
    CommitBatch,
    CommitTrigger,
    DeleteKey,
    DeleteReason,
    PreparedCommit,
    PutEntry,
    SnapshotImage,
    StoredEntry,
    StoredHash,
    StoredList,
    StoredSet,
    StoredString,
    StoredZSet,
)
from miniredis.core.database import Database, Entry, freeze_entry
from miniredis.core.values import (
    HashValue,
    ListValue,
    SetValue,
    StringValue,
    ZSetValue,
)


def test_freeze_entry_is_deep_stable_and_excludes_live_metadata():
    live = Entry(
        value=HashValue({b"z": b"last", b"a": b"first"}),
        expire_at_ms=9000,
        mutation_version=4,
        last_access_tick=71,
        logical_size=999,
    )

    stored = freeze_entry(live)
    live.value.items[b"a"] = b"changed"

    assert stored == StoredEntry(
        value=StoredHash(((b"a", b"first"), (b"z", b"last"))),
        expire_at_ms=9000,
        mutation_version=4,
    )
    assert not hasattr(stored, "last_access_tick")
    assert not hasattr(stored, "logical_size")


@pytest.mark.parametrize(
    ("value", "stored"),
    [
        (StringValue(b"a\x00b"), StoredString(b"a\x00b")),
        (ListValue(deque((b"b", b"a"))), StoredList((b"b", b"a"))),
        (SetValue({b"z", b"a"}), StoredSet((b"a", b"z"))),
        (
            ZSetValue({b"z": float("inf"), b"a": -1.5}),
            StoredZSet(((b"a", -1.5), (b"z", float("inf")))),
        ),
    ],
)
def test_all_live_values_have_immutable_stored_forms(value, stored):
    entry = Entry(value, None, 1, 3, 123)
    assert freeze_entry(entry).value == stored


def test_prepared_commit_is_sequence_free_until_executor_allocates_batch():
    prepared = PreparedCommit(
        operations=(
            PutEntry(
                b"k",
                StoredEntry(StoredString(b"v"), None, 1),
            ),
            DeleteKey(b"expired", DeleteReason.EXPIRED),
        ),
        trigger=CommitTrigger.CLIENT,
    )

    assert not hasattr(prepared, "seq")
    assert prepared.to_batch(8) == CommitBatch(
        seq=8,
        operations=prepared.operations,
        trigger=CommitTrigger.CLIENT,
    )


def test_apply_batch_is_atomic_and_rejects_sequence_gaps():
    database = Database()
    batch = CommitBatch(
        seq=1,
        operations=(
            PutEntry(
                b"k",
                StoredEntry(StoredList((b"a", b"b")), 5000, 7),
            ),
            DeleteKey(b"missing", DeleteReason.CLIENT),
        ),
        trigger=CommitTrigger.CLIENT,
    )

    database.apply_batch(batch, track_access=True)

    assert database.commit_seq == 1
    assert list(database.entries[b"k"].value.items) == [b"a", b"b"]
    assert database.entries[b"k"].expire_at_ms == 5000
    assert database.entries[b"k"].mutation_version == 7
    assert database.entries[b"k"].logical_size > 0
    with pytest.raises(ValueError, match="expected commit seq 2, got 3"):
        database.apply_batch(
            CommitBatch(
                3,
                (
                    PutEntry(
                        b"later",
                        StoredEntry(StoredString(b"x"), None, 1),
                    ),
                ),
                CommitTrigger.ACTIVE_EXPIRE,
            ),
            track_access=False,
        )


def test_snapshot_image_has_sorted_stable_entries():
    image = SnapshotImage(
        checkpoint_seq=2,
        entries=(
            (b"a", StoredEntry(StoredString(b"1"), None, 1)),
            (b"z", StoredEntry(StoredSet((b"a", b"z")), 7000, 2)),
        ),
    )
    assert image.checkpoint_seq == 2
    assert tuple(key for key, _entry in image.entries) == (b"a", b"z")

from collections import deque
from dataclasses import FrozenInstanceError
import os
from pathlib import Path
import subprocess
import sys

import pytest

from miniredis.core.commit import (
    CommitBatch,
    CommitTrigger,
    DeleteKey,
    DeleteReason,
    PutEntry,
    StoredEntry,
    StoredHash,
    StoredList,
    StoredSet,
    StoredString,
    StoredZSet,
)
from miniredis.core.database import (
    Database,
    freeze_value,
    logical_entry_size,
    logical_value_size,
    thaw_value,
)
from miniredis.core.reply import Bytes, Failure, Items, Number, Ok
from miniredis.core.values import HashValue, ListValue, SetValue, StringValue, ZSetValue


def test_values_are_binary_safe_and_client_put_applies_stored_entry() -> None:
    string = StringValue(b"\x00string\xff")
    hash_value = HashValue({b"\x00field": b"\xffitem"})
    list_value = ListValue(deque([b"\x00first", b"\xffsecond"]))
    set_value = SetValue({b"\x00member", b"\xffmember"})
    zset_value = ZSetValue({b"\x00member": 1.5, b"\xffmember": -2.0})

    assert string.data == b"\x00string\xff"
    assert hash_value.items == {b"\x00field": b"\xffitem"}
    assert list_value.items == deque([b"\x00first", b"\xffsecond"])
    assert set_value.items == {b"\x00member", b"\xffmember"}
    assert zset_value.scores == {b"\x00member": 1.5, b"\xffmember": -2.0}

    database = Database()
    database.apply_batch(
        CommitBatch(
            seq=1,
            operations=(
                PutEntry(
                    key=b"k",
                    entry=StoredEntry(
                        value=StoredString(b"v"),
                        expire_at_ms=10,
                        mutation_version=1,
                    ),
                ),
            ),
            trigger=CommitTrigger.CLIENT,
        ),
        track_access=False,
    )

    entry = database.entries[b"k"]
    assert isinstance(entry.value, StringValue)
    assert entry.value.data == b"v"
    assert entry.expire_at_ms == 10
    assert entry.mutation_version == 1
    assert entry.logical_size > 0
    assert entry.last_access_tick == 0
    assert database.commit_seq == 1


def test_commit_batch_rejects_invalid_sequence_and_operations() -> None:
    with pytest.raises(ValueError, match="positive"):
        CommitBatch(
            seq=0,
            operations=(
                PutEntry(
                    key=b"key",
                    entry=StoredEntry(StoredString(b"value"), None, 1),
                ),
            ),
            trigger=CommitTrigger.CLIENT,
        )

    with pytest.raises(ValueError, match="cannot be empty"):
        CommitBatch(seq=1, operations=(), trigger=CommitTrigger.CLIENT)


def test_database_constructor_accepts_no_arguments_only() -> None:
    with pytest.raises(TypeError):
        Database(entries={})


def test_apply_batch_thaws_stored_hash() -> None:
    database = Database()
    database.apply_batch(
        CommitBatch(
            seq=1,
            operations=(
                PutEntry(
                    key=b"hash",
                    entry=StoredEntry(
                        value=StoredHash(((b"field", b"value"),)),
                        expire_at_ms=None,
                        mutation_version=1,
                    ),
                ),
            ),
            trigger=CommitTrigger.CLIENT,
        ),
        track_access=False,
    )

    entry = database.entries[b"hash"]
    assert isinstance(entry.value, HashValue)
    assert entry.value.items == {b"field": b"value"}
    assert entry.logical_size > 0


def test_replies_are_immutable_and_have_stable_shapes() -> None:
    ok = Ok()
    assert ok == Ok(b"OK")
    assert Bytes(b"value").value == b"value"
    assert Bytes(None).value is None
    assert Number(7).value == 7
    assert Items((ok, Number(1))).values == (Ok(), Number(1))
    assert Failure("ERR", "bad input") == Failure("ERR", "bad input")

    with pytest.raises(FrozenInstanceError):
        ok.message = b"changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("live", "stored", "value_size"),
    [
        (StringValue(b"abc"), StoredString(b"abc"), 19),
        (
            HashValue({b"b": b"22", b"a": b"1"}),
            StoredHash(((b"a", b"1"), (b"b", b"22"))),
            32 + 16 + 1 + 1 + 16 + 1 + 2,
        ),
        (ListValue(deque([b"a", b"bc"])), StoredList((b"a", b"bc")), 32 + 9 + 10),
        (SetValue({b"b", b"a"}), StoredSet((b"a", b"b")), 32 + 9 + 9),
        (
            ZSetValue({b"b": 2.0, b"a": 1.0}),
            StoredZSet(((b"a", 1.0), (b"b", 2.0))),
            32 + 25 + 25,
        ),
    ],
)
def test_values_round_trip_and_use_exact_logical_sizes(
    live, stored, value_size
) -> None:
    assert freeze_value(live) == stored
    assert thaw_value(stored) == live
    assert logical_value_size(live) == value_size
    assert logical_value_size(stored) == value_size
    assert logical_entry_size(b"key", live, 10) == 64 + 3 + value_size + 16


def test_freezing_is_sorted_and_isolated_from_live_and_thawed_containers() -> None:
    live_hash = HashValue({b"b": b"2", b"a": b"1"})
    live_set = SetValue({b"b", b"a"})
    live_zset = ZSetValue({b"b": 2.0, b"a": 1.0})

    stored_hash = freeze_value(live_hash)
    stored_set = freeze_value(live_set)
    stored_zset = freeze_value(live_zset)
    assert stored_hash == StoredHash(((b"a", b"1"), (b"b", b"2")))
    assert stored_set == StoredSet((b"a", b"b"))
    assert stored_zset == StoredZSet(((b"a", 1.0), (b"b", 2.0)))

    live_hash.items[b"c"] = b"3"
    live_set.items.add(b"c")
    live_zset.scores[b"c"] = 3.0
    assert stored_hash == StoredHash(((b"a", b"1"), (b"b", b"2")))
    assert stored_set == StoredSet((b"a", b"b"))
    assert stored_zset == StoredZSet(((b"a", 1.0), (b"b", 2.0)))

    thawed_hash = thaw_value(stored_hash)
    thawed_set = thaw_value(stored_set)
    thawed_zset = thaw_value(stored_zset)
    assert isinstance(thawed_hash, HashValue)
    assert isinstance(thawed_set, SetValue)
    assert isinstance(thawed_zset, ZSetValue)
    thawed_hash.items[b"d"] = b"4"
    thawed_set.items.add(b"d")
    thawed_zset.scores[b"d"] = 4.0
    assert stored_hash == StoredHash(((b"a", b"1"), (b"b", b"2")))
    assert stored_set == StoredSet((b"a", b"b"))
    assert stored_zset == StoredZSet(((b"a", 1.0), (b"b", 2.0)))


def test_apply_batch_deletes_and_mixes_operations() -> None:
    database = Database()
    database.apply_batch(
        CommitBatch(
            1,
            (
                PutEntry(b"delete", StoredEntry(StoredString(b"old"), None, 1)),
                PutEntry(b"replace", StoredEntry(StoredString(b"old"), None, 1)),
            ),
            CommitTrigger.CLIENT,
        ),
        track_access=False,
    )
    database.apply_batch(
        CommitBatch(
            2,
            (
                DeleteKey(b"delete", DeleteReason.CLIENT),
                PutEntry(b"replace", StoredEntry(StoredString(b"new"), None, 2)),
                PutEntry(b"added", StoredEntry(StoredString(b"value"), None, 1)),
            ),
            CommitTrigger.CLIENT,
        ),
        track_access=False,
    )

    assert set(database.entries) == {b"replace", b"added"}
    assert database.entries[b"replace"].value == StringValue(b"new")
    assert database.commit_seq == 2
    assert database.logical_usage == sum(
        entry.logical_size for entry in database.entries.values()
    )


def test_apply_batch_unsupported_operation_is_atomic() -> None:
    database = Database()
    database.apply_batch(
        CommitBatch(
            1,
            (PutEntry(b"old", StoredEntry(StoredString(b"v"), None, 1)),),
            CommitTrigger.CLIENT,
        ),
        track_access=True,
    )
    before_items = database.logical_items()
    before_state = (database.commit_seq, database.logical_usage, database.access_tick)

    with pytest.raises(TypeError, match="unsupported commit operation"):
        database.apply_batch(
            CommitBatch(
                2,
                (
                    PutEntry(b"new", StoredEntry(StoredString(b"v"), None, 1)),
                    object(),
                ),
                CommitTrigger.CLIENT,
            ),
            track_access=True,
        )

    assert database.logical_items() == before_items
    assert (
        database.commit_seq,
        database.logical_usage,
        database.access_tick,
    ) == before_state


def test_revision_survives_create_delete_cycle_and_absent_delete():
    database = Database()
    database.apply_batch(
        CommitBatch(
            1,
            (PutEntry(b"k", StoredEntry(StoredString(b"v"), None, 1)),),
            CommitTrigger.CLIENT,
        ),
        track_access=True,
    )
    created = database.revision(b"k")
    database.apply_batch(
        CommitBatch(2, (DeleteKey(b"k", DeleteReason.CLIENT),), CommitTrigger.CLIENT),
        track_access=True,
    )
    deleted = database.revision(b"k")
    database.apply_batch(
        CommitBatch(
            3,
            (DeleteKey(b"missing", DeleteReason.CLIENT),),
            CommitTrigger.CLIENT,
        ),
        track_access=True,
    )

    assert b"k" not in database.entries
    assert deleted > created
    assert database.revision(b"missing") > deleted


def test_database_fork_is_deep_and_preserves_runtime_metadata():
    database = Database()
    database.apply_batch(
        CommitBatch(
            1,
            (PutEntry(b"k", StoredEntry(StoredString(b"v"), None, 1)),),
            CommitTrigger.CLIENT,
        ),
        track_access=True,
    )

    fork = database.fork()
    fork.touch_if_live(b"k", 0)
    fork.apply_batch(
        CommitBatch(
            2,
            (DeleteKey(b"k", DeleteReason.CLIENT),),
            CommitTrigger.CLIENT,
        ),
        track_access=True,
    )

    assert b"k" in database.entries
    assert b"k" not in fork.entries
    assert database.access_tick == 1
    assert fork.access_tick == 2
    assert database.revision(b"k") != fork.revision(b"k")
    assert database.logical_usage > 0
    assert fork.logical_usage == 0


def test_client_updates_preserve_decay_and_increment_frequency():
    database = Database()
    database.apply_batch(
        CommitBatch(
            1,
            (PutEntry(b"k", StoredEntry(StoredString(b"one"), None, 1)),),
            CommitTrigger.CLIENT,
        ),
        track_access=True,
        now_ms=0,
        lfu_decay_interval_ms=1000,
    )
    assert database.entries[b"k"].frequency == 1
    database.apply_batch(
        CommitBatch(
            2,
            (PutEntry(b"k", StoredEntry(StoredString(b"two"), None, 2)),),
            CommitTrigger.CLIENT,
        ),
        track_access=True,
        now_ms=2000,
        lfu_decay_interval_ms=1000,
    )

    assert database.entries[b"k"].frequency == 1
    assert database.entries[b"k"].last_frequency_decay_ms == 2000


def test_recovery_puts_start_neutral_and_fork_copies_lfu_metadata():
    database = Database()
    database.apply_batch(
        CommitBatch(
            1,
            (PutEntry(b"k", StoredEntry(StoredString(b"v"), None, 1)),),
            CommitTrigger.CLIENT,
        ),
        track_access=False,
        now_ms=5000,
        lfu_decay_interval_ms=1000,
    )
    assert database.entries[b"k"].frequency == 0
    assert database.entries[b"k"].last_access_tick == 0
    assert database.entries[b"k"].last_frequency_decay_ms == 5000

    fork = database.fork()
    assert fork.touch_if_live(b"k", 5000, 1000) is True
    assert fork.entries[b"k"].frequency == 1
    assert database.entries[b"k"].frequency == 0


def test_apply_batch_tracks_each_put_and_touch_only_live_entries() -> None:
    database = Database()
    database.apply_batch(
        CommitBatch(
            1,
            (
                PutEntry(b"live", StoredEntry(StoredString(b"v"), None, 1)),
                PutEntry(b"expired", StoredEntry(StoredString(b"v"), 10, 1)),
            ),
            CommitTrigger.CLIENT,
        ),
        track_access=True,
    )
    assert database.access_tick == 2
    assert database.entries[b"live"].last_access_tick == 1
    assert database.entries[b"expired"].last_access_tick == 2

    assert database.touch_if_live(b"live", now_ms=10) is True
    assert database.entries[b"live"].last_access_tick == 3
    assert database.touch_if_live(b"expired", now_ms=10) is False
    assert database.touch_if_live(b"missing", now_ms=10) is False
    assert database.access_tick == 3


def test_logical_items_are_sorted_and_stably_frozen() -> None:
    database = Database()
    database.apply_batch(
        CommitBatch(
            1,
            (
                PutEntry(b"z", StoredEntry(StoredString(b"last"), None, 2)),
                PutEntry(
                    b"a", StoredEntry(StoredHash(((b"b", b"2"), (b"a", b"1"))), 20, 3)
                ),
            ),
            CommitTrigger.CLIENT,
        ),
        track_access=False,
    )

    assert database.logical_items() == (
        (b"a", StoredEntry(StoredHash(((b"a", b"1"), (b"b", b"2"))), 20, 3)),
        (b"z", StoredEntry(StoredString(b"last"), None, 2)),
    )


def test_apply_batch_rejects_nonsequential_commit_with_exact_message() -> None:
    with pytest.raises(ValueError, match="expected commit seq 1, got 2"):
        Database().apply_batch(
            CommitBatch(
                2,
                (PutEntry(b"key", StoredEntry(StoredString(b"v"), None, 1)),),
                CommitTrigger.CLIENT,
            ),
            track_access=False,
        )


def test_database_invariant_checks_remain_active_under_optimized_python() -> None:
    repository_root = Path(__file__).resolve().parents[3]
    environment = os.environ | {
        "PYTHONPATH": str(repository_root / "src"),
    }
    script = """
from miniredis.core.commit import CommitBatch, CommitTrigger, PutEntry, StoredEntry, StoredString
from miniredis.core.database import Database, Entry
from miniredis.core.values import StringValue

database = Database()
database.entries[b"invalid"] = Entry(StringValue(b"v"), None, 1, 0, -1)
try:
    database.apply_batch(
        CommitBatch(1, (PutEntry(b"valid", StoredEntry(StoredString(b"v"), None, 1)),), CommitTrigger.CLIENT),
        track_access=False,
    )
except AssertionError as error:
    if str(error) == "entry logical size must be positive":
        raise SystemExit(0)
raise SystemExit(1)
"""

    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == 0, result.stderr

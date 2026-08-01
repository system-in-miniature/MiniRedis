# Stage 13 · Stable commit batches

### Goal

Turn in-memory mutations into deterministic deep-frozen commits and snapshot images suitable for replay.

??? note "Deliverable files"
    - `src/miniredis/core/commit.py`
    - `src/miniredis/core/database.py`
    - `src/miniredis/core/executor.py`
    - `tests/unit/core/test_commit.py`

### The problem at this point

The executor records operations, but live dictionaries, sets, deques, access ticks, and logical-size caches are not a durable contract. Persistence needs values whose bytes and meaning do not change after planning, plus contiguous sequence ownership that cannot be consumed by failed or no-op commands.

### Test contract

#### See the failure first

Mutating a live Hash after freezing must not alter stored operations. Applying sequence 3 after sequence 1 must fail before changing state, and a snapshot must reject duplicate or unsorted keys. Otherwise replay can silently diverge from the state originally committed.

??? note "File diff: tests/unit/core/test_commit.py"
    ```diff
    diff --git a/tests/unit/core/test_commit.py b/tests/unit/core/test_commit.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..db56bbf3b3dbd274417164b85f1fb30c5b6712fd
    --- /dev/null
    +++ b/tests/unit/core/test_commit.py
    @@ -0,0 +1,134 @@
    +from collections import deque
    +
    +import pytest
    +
    +from miniredis.core.commit import (
    +    CommitBatch,
    +    CommitTrigger,
    +    DeleteKey,
    +    DeleteReason,
    +    PreparedCommit,
    +    PutEntry,
    +    SnapshotImage,
    +    StoredEntry,
    +    StoredHash,
    +    StoredList,
    +    StoredSet,
    +    StoredString,
    +    StoredZSet,
    +)
    +from miniredis.core.database import Database, Entry, freeze_entry
    +from miniredis.core.values import (
    +    HashValue,
    +    ListValue,
    +    SetValue,
    +    StringValue,
    +    ZSetValue,
    +)
    +
    +
    +def test_freeze_entry_is_deep_stable_and_excludes_live_metadata():
    +    live = Entry(
    +        value=HashValue({b"z": b"last", b"a": b"first"}),
    +        expire_at_ms=9000,
    +        mutation_version=4,
    +        last_access_tick=71,
    +        logical_size=999,
    +    )
    +
    +    stored = freeze_entry(live)
    +    live.value.items[b"a"] = b"changed"
    +
    +    assert stored == StoredEntry(
    +        value=StoredHash(((b"a", b"first"), (b"z", b"last"))),
    +        expire_at_ms=9000,
    +        mutation_version=4,
    +    )
    +    assert not hasattr(stored, "last_access_tick")
    +    assert not hasattr(stored, "logical_size")
    +
    +
    +@pytest.mark.parametrize(
    +    ("value", "stored"),
    +    [
    +        (StringValue(b"a\x00b"), StoredString(b"a\x00b")),
    +        (ListValue(deque((b"b", b"a"))), StoredList((b"b", b"a"))),
    +        (SetValue({b"z", b"a"}), StoredSet((b"a", b"z"))),
    +        (
    +            ZSetValue({b"z": float("inf"), b"a": -1.5}),
    +            StoredZSet(((b"a", -1.5), (b"z", float("inf")))),
    +        ),
    +    ],
    +)
    +def test_all_live_values_have_immutable_stored_forms(value, stored):
    +    entry = Entry(value, None, 1, 3, 123)
    +    assert freeze_entry(entry).value == stored
    +
    +
    +def test_prepared_commit_is_sequence_free_until_executor_allocates_batch():
    +    prepared = PreparedCommit(
    +        operations=(
    +            PutEntry(
    +                b"k",
    +                StoredEntry(StoredString(b"v"), None, 1),
    +            ),
    +            DeleteKey(b"expired", DeleteReason.EXPIRED),
    +        ),
    +        trigger=CommitTrigger.CLIENT,
    +    )
    +
    +    assert not hasattr(prepared, "seq")
    +    assert prepared.to_batch(8) == CommitBatch(
    +        seq=8,
    +        operations=prepared.operations,
    +        trigger=CommitTrigger.CLIENT,
    +    )
    +
    +
    +def test_apply_batch_is_atomic_and_rejects_sequence_gaps():
    +    database = Database()
    +    batch = CommitBatch(
    +        seq=1,
    +        operations=(
    +            PutEntry(
    +                b"k",
    +                StoredEntry(StoredList((b"a", b"b")), 5000, 7),
    +            ),
    +            DeleteKey(b"missing", DeleteReason.CLIENT),
    +        ),
    +        trigger=CommitTrigger.CLIENT,
    +    )
    +
    +    database.apply_batch(batch, track_access=True)
    +
    +    assert database.commit_seq == 1
    +    assert list(database.entries[b"k"].value.items) == [b"a", b"b"]
    +    assert database.entries[b"k"].expire_at_ms == 5000
    +    assert database.entries[b"k"].mutation_version == 7
    +    assert database.entries[b"k"].logical_size > 0
    +    with pytest.raises(ValueError, match="expected commit seq 2, got 3"):
    +        database.apply_batch(
    +            CommitBatch(
    +                3,
    +                (
    +                    PutEntry(
    +                        b"later",
    +                        StoredEntry(StoredString(b"x"), None, 1),
    +                    ),
    +                ),
    +                CommitTrigger.ACTIVE_EXPIRE,
    +            ),
    +            track_access=False,
    +        )
    +
    +
    +def test_snapshot_image_has_sorted_stable_entries():
    +    image = SnapshotImage(
    +        checkpoint_seq=2,
    +        entries=(
    +            (b"a", StoredEntry(StoredString(b"1"), None, 1)),
    +            (b"z", StoredEntry(StoredSet((b"a", b"z")), 7000, 2)),
    +        ),
    +    )
    +    assert image.checkpoint_seq == 2
    +    assert tuple(key for key, _entry in image.entries) == (b"a", b"z")
    ```

**What this test locks**

It locks deep stable freezing for every value family, exclusion of live metadata, contiguous batch application, no partial state on invalid sequence, and sorted snapshot identity.

**How it constructs the counterexample**

It freezes mutable containers, mutates the originals, applies both legal and gapped batches, and constructs snapshot images at ordering boundaries.

**Key test statement**

```python
with pytest.raises(ValueError, match="expected commit seq 2, got 3"):
```

**What a failure means**

The durable vocabulary aliases live state, sequence validation occurs after mutation, or snapshot bytes can depend on map iteration order.

### Basic concepts

`StoredValue` is an immutable canonical representation of logical data. `PreparedCommit` contains operations and trigger but no sequence; `CommitBatch` adds the sequence only when the single executor is ready to append. `SnapshotImage` freezes one sorted checkpoint state.

### Why this mechanism is necessary

Persistence and replication must replay semantic state, not Python object identity or access-policy metadata. Late sequence allocation keeps failed/no-op plans from creating gaps, while staged application makes one batch all-or-nothing even during replay validation.

### Runtime mental model

Planners freeze replacement values into operations. A successful plan exposes an optional `PreparedCommit`. The executor chooses `database.commit_seq + 1`, turns it into a batch, sends it through the barrier, then applies it. Database replay clones the entry map, validates and applies every operation, calculates usage, and swaps state only after the full batch succeeds.

### Mechanism blocks

#### Stable commit and snapshot values

Freeze every Redis value into deterministic transport-independent operations, batches, and sorted snapshot images.

??? note "File diff: src/miniredis/core/commit.py"
    ```diff
    diff --git a/src/miniredis/core/commit.py b/src/miniredis/core/commit.py
    index 2274b6d0a9026c4377d2e8f76f60d789242473e2..d262f838ea45d57befcaae1e5e2de3aeb6139aff 100644
    --- a/src/miniredis/core/commit.py
    +++ b/src/miniredis/core/commit.py
    @@ -77,3 +77,29 @@ class CommitBatch:
                 raise ValueError("commit seq must be positive")
             if not self.operations:
                 raise ValueError("commit batch operations cannot be empty")
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class PreparedCommit:
    +    operations: tuple[CommitOperation, ...]
    +    trigger: CommitTrigger
    +
    +    def to_batch(self, seq: int) -> CommitBatch:
    +        if seq <= 0:
    +            raise ValueError("commit seq must be positive")
    +        if not self.operations:
    +            raise ValueError("an empty prepared commit is a no-op")
    +        return CommitBatch(seq, self.operations, self.trigger)
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SnapshotImage:
    +    checkpoint_seq: int
    +    entries: tuple[tuple[bytes, StoredEntry], ...]
    +
    +    def __post_init__(self) -> None:
    +        if self.checkpoint_seq < 0:
    +            raise ValueError("checkpoint seq cannot be negative")
    +        keys = tuple(key for key, _entry in self.entries)
    +        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
    +            raise ValueError("snapshot entries must have unique sorted keys")
    ```

**What it is and why it appears**

This module defines transport-independent stored values, operations, sequenced batches, prepared commits, and snapshot images.

**Runtime role**

It is the stable vocabulary shared by executor, database, codec, persistence, and later replication.

**Key code**

```python
def to_batch(self, seq: int) -> CommitBatch:
    if seq <= 0:
        raise ValueError("commit seq must be positive")
```

**Statement understanding**

Planning cannot claim global order; only the serialized owner supplies a positive sequence.

#### Atomic batch replay

Apply one contiguous batch through a staged map and export deep-frozen logical or snapshot state.

??? note "File diff: src/miniredis/core/database.py"
    ```diff
    diff --git a/src/miniredis/core/database.py b/src/miniredis/core/database.py
    index 25ad5ff367181660cd6fee8ed89dabecf8326f87..707b25de8cd248e89bd029c296dd18be41c3e2e4 100644
    --- a/src/miniredis/core/database.py
    +++ b/src/miniredis/core/database.py
    @@ -7,6 +7,7 @@ from miniredis.core.commit import (
         CommitBatch,
         DeleteKey,
         PutEntry,
    +    SnapshotImage,
         StoredEntry,
         StoredHash,
         StoredList,
    @@ -106,7 +107,7 @@ def thaw_value(value: StoredValue) -> RedisValue:
                 raise TypeError(f"unsupported stored value: {type(value)!r}")


    -def _freeze_entry(entry: Entry) -> StoredEntry:
    +def freeze_entry(entry: Entry) -> StoredEntry:
         return StoredEntry(
             value=freeze_value(entry.value),
             expire_at_ms=entry.expire_at_ms,
    @@ -172,5 +173,21 @@ class Database:

         def logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
             return tuple(
    -            (key, _freeze_entry(entry)) for key, entry in sorted(self.entries.items())
    +            (key, freeze_entry(entry)) for key, entry in sorted(self.entries.items())
    +        )
    +
    +    def export_stored_entries(
    +        self,
    +        now_ms: int,
    +    ) -> tuple[tuple[bytes, StoredEntry], ...]:
    +        return tuple(
    +            (key, freeze_entry(entry))
    +            for key, entry in sorted(self.entries.items())
    +            if entry.expire_at_ms is None or entry.expire_at_ms > now_ms
    +        )
    +
    +    def snapshot_image(self, now_ms: int) -> SnapshotImage:
    +        return SnapshotImage(
    +            checkpoint_seq=self.commit_seq,
    +            entries=self.export_stored_entries(now_ms),
             )
    ```

**What it is and why it appears**

Database gains deep freeze/thaw conversion, staged batch replay, and deterministic export.

**Runtime role**

It validates contiguous sequence, builds a candidate map, recomputes logical usage, then atomically replaces live state.

**Key code**

```python
staged = dict(self.entries)
staged_access_tick = self.access_tick
```

**Statement understanding**

All operations target a candidate state; an exception before the final swap leaves the live database unchanged.

#### Late commit-sequence allocation

Keep operations sequence-free during planning and allocate the next sequence only at the serialized append boundary.

??? note "File diff: src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index 5c884187bbb88292e1da922578363dfa32a49a9c..d9ea4d458917aeb8443bb40130abd5fa537777ed 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -28,6 +28,7 @@ from miniredis.core.commit import (
         CommitBatch,
         CommitOperation,
         CommitTrigger,
    +    PreparedCommit,
         PutEntry,
         StoredList,
     )
    @@ -98,6 +99,12 @@ class ExecutionPlan:
         trigger: CommitTrigger = CommitTrigger.CLIENT
         waiter_wakeups: tuple[WaiterWakeup, ...] = ()

    +    @property
    +    def prepared_commit(self) -> PreparedCommit | None:
    +        if not self.operations:
    +            return None
    +        return PreparedCommit(self.operations, self.trigger)
    +

     class CommitBarrier(Protocol):
         async def append(self, batch: CommitBatch) -> None: ...
    ```

**What it is and why it appears**

Execution plans now expose sequence-free prepared commits.

**Runtime role**

The executor allocates the next sequence only for nonempty accepted operations at the commit barrier.

**Key code**

```python
if not self.operations:
    return None
return PreparedCommit(self.operations, self.trigger)
```

**Statement understanding**

Replies, errors, touches, and other no-op plans do not consume commit sequence space.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/13-stable-commit-batches/tests.txt)`. It proves the stable value vocabulary and atomic replay invariants independently of disk I/O.

### Durable takeaways

Freeze semantic state deeply; exclude policy-only live metadata; plan without sequence; allocate order at one owner; reject gaps before mutation; export snapshots with unique sorted keys.

### Explain it in your own words

A commit is no longer a debug trace of live Python objects. It is a stable replay instruction. Planning decides what should change, the executor decides where it sits in global order, and Database applies the complete instruction through a staged state swap.

### Textbook

[Chapter 6](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/06-aof.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/6ff1e5f...5a40b5f)

After finishing, run `python -m journey.tools.build_journey check 13` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/13-stable-commit-batches/stage.patch)

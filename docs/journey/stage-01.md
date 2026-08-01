# Stage 01 · Domain state and commit vocabulary

### Goal

Define the binary-safe values, immutable commit vocabulary, replies, and database boundary that every later command shares.

??? note "Deliverable files"
    - `pyproject.toml`
    - `src/miniredis/__init__.py`
    - `src/miniredis/adapters/__init__.py`
    - `src/miniredis/commands/__init__.py`
    - `src/miniredis/commands/request.py`
    - `src/miniredis/core/__init__.py`
    - `src/miniredis/core/commit.py`
    - `src/miniredis/core/database.py`
    - `src/miniredis/core/reply.py`
    - `src/miniredis/core/values.py`
    - `src/miniredis/persistence/__init__.py`
    - `src/miniredis/replication/__init__.py`
    - `tests/__init__.py`
    - `tests/helpers/__init__.py`
    - `tests/unit/core/test_domain_types.py`
    - `uv.lock`

### The problem at this point

An in-memory server cannot safely start from a dictionary of arbitrary Python objects. Later persistence and replication need stable values that cannot change behind their backs, while command execution needs mutable containers and transport adapters need replies that do not know about RESP2.

### Test contract

#### See the failure first

The first contract mutates live Hash, List, Set, and Sorted Set containers after freezing them. If the stored forms share those containers, an already-created commit changes without receiving a new sequence. Another case submits a mixed batch with an unsupported operation and requires the database to remain completely unchanged.

??? note "File diff: tests/__init__.py"
    ```diff
    diff --git a/tests/__init__.py b/tests/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..eac63ed7cec146ff32f88a014b5014f3c85bf279
    --- /dev/null
    +++ b/tests/__init__.py
    @@ -0,0 +1,2 @@
    +"""MiniRedis test package."""
    +
    ```

Marks the cumulative test tree as importable support; it introduces no behavior on its own.

??? note "File diff: tests/helpers/__init__.py"
    ```diff
    diff --git a/tests/helpers/__init__.py b/tests/helpers/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a40b6614917d646b2fac0c1fba72bce668da29ca
    --- /dev/null
    +++ b/tests/helpers/__init__.py
    @@ -0,0 +1,2 @@
    +"""Thin test-boundary helpers; never an alternate MiniRedis implementation."""
    +
    ```

Reserves one shared helper package so later deterministic clocks and runtime fixtures do not leak into production modules.

??? note "File diff: tests/unit/core/test_domain_types.py"
    ```diff
    diff --git a/tests/unit/core/test_domain_types.py b/tests/unit/core/test_domain_types.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..94046c22fc982415ce8833b79c105a1b016b0525
    --- /dev/null
    +++ b/tests/unit/core/test_domain_types.py
    @@ -0,0 +1,355 @@
    +from collections import deque
    +from dataclasses import FrozenInstanceError
    +import os
    +from pathlib import Path
    +import subprocess
    +import sys
    +
    +import pytest
    +
    +from miniredis.core.commit import (
    +    CommitBatch,
    +    CommitTrigger,
    +    DeleteKey,
    +    DeleteReason,
    +    PutEntry,
    +    StoredEntry,
    +    StoredHash,
    +    StoredList,
    +    StoredSet,
    +    StoredString,
    +    StoredZSet,
    +)
    +from miniredis.core.database import (
    +    Database,
    +    freeze_value,
    +    logical_entry_size,
    +    logical_value_size,
    +    thaw_value,
    +)
    +from miniredis.core.reply import Bytes, Failure, Items, Number, Ok
    +from miniredis.core.values import HashValue, ListValue, SetValue, StringValue, ZSetValue
    +
    +
    +def test_values_are_binary_safe_and_client_put_applies_stored_entry() -> None:
    +    string = StringValue(b"\x00string\xff")
    +    hash_value = HashValue({b"\x00field": b"\xffitem"})
    +    list_value = ListValue(deque([b"\x00first", b"\xffsecond"]))
    +    set_value = SetValue({b"\x00member", b"\xffmember"})
    +    zset_value = ZSetValue({b"\x00member": 1.5, b"\xffmember": -2.0})
    +
    +    assert string.data == b"\x00string\xff"
    +    assert hash_value.items == {b"\x00field": b"\xffitem"}
    +    assert list_value.items == deque([b"\x00first", b"\xffsecond"])
    +    assert set_value.items == {b"\x00member", b"\xffmember"}
    +    assert zset_value.scores == {b"\x00member": 1.5, b"\xffmember": -2.0}
    +
    +    database = Database()
    +    database.apply_batch(
    +        CommitBatch(
    +            seq=1,
    +            operations=(
    +                PutEntry(
    +                    key=b"k",
    +                    entry=StoredEntry(
    +                        value=StoredString(b"v"),
    +                        expire_at_ms=10,
    +                        mutation_version=1,
    +                    ),
    +                ),
    +            ),
    +            trigger=CommitTrigger.CLIENT,
    +        ),
    +        track_access=False,
    +    )
    +
    +    entry = database.entries[b"k"]
    +    assert isinstance(entry.value, StringValue)
    +    assert entry.value.data == b"v"
    +    assert entry.expire_at_ms == 10
    +    assert entry.mutation_version == 1
    +    assert entry.logical_size > 0
    +    assert entry.last_access_tick == 0
    +    assert database.commit_seq == 1
    +
    +
    +def test_commit_batch_rejects_invalid_sequence_and_operations() -> None:
    +    with pytest.raises(ValueError, match="positive"):
    +        CommitBatch(
    +            seq=0,
    +            operations=(
    +                PutEntry(
    +                    key=b"key",
    +                    entry=StoredEntry(StoredString(b"value"), None, 1),
    +                ),
    +            ),
    +            trigger=CommitTrigger.CLIENT,
    +        )
    +
    +    with pytest.raises(ValueError, match="cannot be empty"):
    +        CommitBatch(seq=1, operations=(), trigger=CommitTrigger.CLIENT)
    +
    +
    +def test_database_constructor_accepts_no_arguments_only() -> None:
    +    with pytest.raises(TypeError):
    +        Database(entries={})
    +
    +
    +def test_apply_batch_thaws_stored_hash() -> None:
    +    database = Database()
    +    database.apply_batch(
    +        CommitBatch(
    +            seq=1,
    +            operations=(
    +                PutEntry(
    +                    key=b"hash",
    +                    entry=StoredEntry(
    +                        value=StoredHash(((b"field", b"value"),)),
    +                        expire_at_ms=None,
    +                        mutation_version=1,
    +                    ),
    +                ),
    +            ),
    +            trigger=CommitTrigger.CLIENT,
    +        ),
    +        track_access=False,
    +    )
    +
    +    entry = database.entries[b"hash"]
    +    assert isinstance(entry.value, HashValue)
    +    assert entry.value.items == {b"field": b"value"}
    +    assert entry.logical_size > 0
    +
    +
    +def test_replies_are_immutable_and_have_stable_shapes() -> None:
    +    ok = Ok()
    +    assert ok == Ok(b"OK")
    +    assert Bytes(b"value").value == b"value"
    +    assert Bytes(None).value is None
    +    assert Number(7).value == 7
    +    assert Items((ok, Number(1))).values == (Ok(), Number(1))
    +    assert Failure("ERR", "bad input") == Failure("ERR", "bad input")
    +
    +    with pytest.raises(FrozenInstanceError):
    +        ok.message = b"changed"  # type: ignore[misc]
    +
    +
    +@pytest.mark.parametrize(
    +    ("live", "stored", "value_size"),
    +    [
    +        (StringValue(b"abc"), StoredString(b"abc"), 19),
    +        (
    +            HashValue({b"b": b"22", b"a": b"1"}),
    +            StoredHash(((b"a", b"1"), (b"b", b"22"))),
    +            32 + 16 + 1 + 1 + 16 + 1 + 2,
    +        ),
    +        (ListValue(deque([b"a", b"bc"])), StoredList((b"a", b"bc")), 32 + 9 + 10),
    +        (SetValue({b"b", b"a"}), StoredSet((b"a", b"b")), 32 + 9 + 9),
    +        (
    +            ZSetValue({b"b": 2.0, b"a": 1.0}),
    +            StoredZSet(((b"a", 1.0), (b"b", 2.0))),
    +            32 + 25 + 25,
    +        ),
    +    ],
    +)
    +def test_values_round_trip_and_use_exact_logical_sizes(
    +    live, stored, value_size
    +) -> None:
    +    assert freeze_value(live) == stored
    +    assert thaw_value(stored) == live
    +    assert logical_value_size(live) == value_size
    +    assert logical_value_size(stored) == value_size
    +    assert logical_entry_size(b"key", live, 10) == 64 + 3 + value_size + 16
    +
    +
    +def test_freezing_is_sorted_and_isolated_from_live_and_thawed_containers() -> None:
    +    live_hash = HashValue({b"b": b"2", b"a": b"1"})
    +    live_set = SetValue({b"b", b"a"})
    +    live_zset = ZSetValue({b"b": 2.0, b"a": 1.0})
    +
    +    stored_hash = freeze_value(live_hash)
    +    stored_set = freeze_value(live_set)
    +    stored_zset = freeze_value(live_zset)
    +    assert stored_hash == StoredHash(((b"a", b"1"), (b"b", b"2")))
    +    assert stored_set == StoredSet((b"a", b"b"))
    +    assert stored_zset == StoredZSet(((b"a", 1.0), (b"b", 2.0)))
    +
    +    live_hash.items[b"c"] = b"3"
    +    live_set.items.add(b"c")
    +    live_zset.scores[b"c"] = 3.0
    +    assert stored_hash == StoredHash(((b"a", b"1"), (b"b", b"2")))
    +    assert stored_set == StoredSet((b"a", b"b"))
    +    assert stored_zset == StoredZSet(((b"a", 1.0), (b"b", 2.0)))
    +
    +    thawed_hash = thaw_value(stored_hash)
    +    thawed_set = thaw_value(stored_set)
    +    thawed_zset = thaw_value(stored_zset)
    +    assert isinstance(thawed_hash, HashValue)
    +    assert isinstance(thawed_set, SetValue)
    +    assert isinstance(thawed_zset, ZSetValue)
    +    thawed_hash.items[b"d"] = b"4"
    +    thawed_set.items.add(b"d")
    +    thawed_zset.scores[b"d"] = 4.0
    +    assert stored_hash == StoredHash(((b"a", b"1"), (b"b", b"2")))
    +    assert stored_set == StoredSet((b"a", b"b"))
    +    assert stored_zset == StoredZSet(((b"a", 1.0), (b"b", 2.0)))
    +
    +
    +def test_apply_batch_deletes_and_mixes_operations() -> None:
    +    database = Database()
    +    database.apply_batch(
    +        CommitBatch(
    +            1,
    +            (
    +                PutEntry(b"delete", StoredEntry(StoredString(b"old"), None, 1)),
    +                PutEntry(b"replace", StoredEntry(StoredString(b"old"), None, 1)),
    +            ),
    +            CommitTrigger.CLIENT,
    +        ),
    +        track_access=False,
    +    )
    +    database.apply_batch(
    +        CommitBatch(
    +            2,
    +            (
    +                DeleteKey(b"delete", DeleteReason.CLIENT),
    +                PutEntry(b"replace", StoredEntry(StoredString(b"new"), None, 2)),
    +                PutEntry(b"added", StoredEntry(StoredString(b"value"), None, 1)),
    +            ),
    +            CommitTrigger.CLIENT,
    +        ),
    +        track_access=False,
    +    )
    +
    +    assert set(database.entries) == {b"replace", b"added"}
    +    assert database.entries[b"replace"].value == StringValue(b"new")
    +    assert database.commit_seq == 2
    +    assert database.logical_usage == sum(
    +        entry.logical_size for entry in database.entries.values()
    +    )
    +
    +
    +def test_apply_batch_unsupported_operation_is_atomic() -> None:
    +    database = Database()
    +    database.apply_batch(
    +        CommitBatch(
    +            1,
    +            (PutEntry(b"old", StoredEntry(StoredString(b"v"), None, 1)),),
    +            CommitTrigger.CLIENT,
    +        ),
    +        track_access=True,
    +    )
    +    before_items = database.logical_items()
    +    before_state = (database.commit_seq, database.logical_usage, database.access_tick)
    +
    +    with pytest.raises(TypeError, match="unsupported commit operation"):
    +        database.apply_batch(
    +            CommitBatch(
    +                2,
    +                (
    +                    PutEntry(b"new", StoredEntry(StoredString(b"v"), None, 1)),
    +                    object(),
    +                ),
    +                CommitTrigger.CLIENT,
    +            ),
    +            track_access=True,
    +        )
    +
    +    assert database.logical_items() == before_items
    +    assert (
    +        database.commit_seq,
    +        database.logical_usage,
    +        database.access_tick,
    +    ) == before_state
    +
    +
    +def test_apply_batch_tracks_each_put_and_touch_only_live_entries() -> None:
    +    database = Database()
    +    database.apply_batch(
    +        CommitBatch(
    +            1,
    +            (
    +                PutEntry(b"live", StoredEntry(StoredString(b"v"), None, 1)),
    +                PutEntry(b"expired", StoredEntry(StoredString(b"v"), 10, 1)),
    +            ),
    +            CommitTrigger.CLIENT,
    +        ),
    +        track_access=True,
    +    )
    +    assert database.access_tick == 2
    +    assert database.entries[b"live"].last_access_tick == 1
    +    assert database.entries[b"expired"].last_access_tick == 2
    +
    +    assert database.touch_if_live(b"live", now_ms=10) is True
    +    assert database.entries[b"live"].last_access_tick == 3
    +    assert database.touch_if_live(b"expired", now_ms=10) is False
    +    assert database.touch_if_live(b"missing", now_ms=10) is False
    +    assert database.access_tick == 3
    +
    +
    +def test_logical_items_are_sorted_and_stably_frozen() -> None:
    +    database = Database()
    +    database.apply_batch(
    +        CommitBatch(
    +            1,
    +            (
    +                PutEntry(b"z", StoredEntry(StoredString(b"last"), None, 2)),
    +                PutEntry(
    +                    b"a", StoredEntry(StoredHash(((b"b", b"2"), (b"a", b"1"))), 20, 3)
    +                ),
    +            ),
    +            CommitTrigger.CLIENT,
    +        ),
    +        track_access=False,
    +    )
    +
    +    assert database.logical_items() == (
    +        (b"a", StoredEntry(StoredHash(((b"a", b"1"), (b"b", b"2"))), 20, 3)),
    +        (b"z", StoredEntry(StoredString(b"last"), None, 2)),
    +    )
    +
    +
    +def test_apply_batch_rejects_nonsequential_commit_with_exact_message() -> None:
    +    with pytest.raises(ValueError, match="expected commit seq 1, got 2"):
    +        Database().apply_batch(
    +            CommitBatch(
    +                2,
    +                (PutEntry(b"key", StoredEntry(StoredString(b"v"), None, 1)),),
    +                CommitTrigger.CLIENT,
    +            ),
    +            track_access=False,
    +        )
    +
    +
    +def test_database_invariant_checks_remain_active_under_optimized_python() -> None:
    +    repository_root = Path(__file__).resolve().parents[3]
    +    environment = os.environ | {
    +        "PYTHONPATH": str(repository_root / "src"),
    +    }
    +    script = """
    +from miniredis.core.commit import CommitBatch, CommitTrigger, PutEntry, StoredEntry, StoredString
    +from miniredis.core.database import Database, Entry
    +from miniredis.core.values import StringValue
    +
    +database = Database()
    +database.entries[b"invalid"] = Entry(StringValue(b"v"), None, 1, 0, -1)
    +try:
    +    database.apply_batch(
    +        CommitBatch(1, (PutEntry(b"valid", StoredEntry(StoredString(b"v"), None, 1)),), CommitTrigger.CLIENT),
    +        track_access=False,
    +    )
    +except AssertionError as error:
    +    if str(error) == "entry logical size must be positive":
    +        raise SystemExit(0)
    +raise SystemExit(1)
    +"""
    +
    +    result = subprocess.run(
    +        [sys.executable, "-O", "-c", script],
    +        check=False,
    +        capture_output=True,
    +        env=environment,
    +        text=True,
    +    )
    +
    +    assert result.returncode == 0, result.stderr
    ```

**What this test locks**

It locks binary safety, deep freeze/thaw isolation, exact logical sizes, contiguous commit sequences, immutable reply shapes, and non-mutating invariant failure.

**How it constructs the counterexample**

It retains aliases to mutable containers, freezes them, mutates the aliases, and compares the stored values. It also applies invalid and out-of-order batches around a captured database snapshot.

**Key test statement**

```python
assert freeze_value(live) == stored
```

**What a failure means**

A failure means an atomic or durable unit can be changed indirectly, or a rejected batch can leak partial state before the executor even exists.

### Basic concepts

MiniRedis distinguishes live `RedisValue` containers from immutable `StoredValue` records. A `CommitBatch` is an ordered tuple of `PutEntry` and `DeleteKey` operations. A `Reply` is a transport-neutral result. `Database.apply_batch` is the only state transition in this Stage.

Binary safe means keys and values remain `bytes`; no UTF-8 decoding is required to store or compare them. Atomic application means either every operation in one batch becomes visible or the original database remains intact.

### Why this mechanism is necessary

Without a frozen state vocabulary, AOF, snapshots, and replicas would observe mutable Python aliases rather than one historical fact. Without a closed reply vocabulary, Direct and RESP2 adapters would create separate semantics. Without sequence validation, recovery could silently accept a missing or reordered commit.

### Runtime mental model

A future planner will produce immutable operations. The database first stages copies of its tables, validates every operation and logical-size invariant, then swaps the staged state into place and advances exactly one commit sequence. No command or socket is needed to understand that transition.

### Mechanism blocks

#### Binary-safe state and commit vocabulary

Separate live mutable values, immutable propagated state, transport-neutral replies, and atomic database application before adding commands.

??? note "File diff: src/miniredis/commands/request.py"
    ```diff
    diff --git a/src/miniredis/commands/request.py b/src/miniredis/commands/request.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..ba02069b3dd4bc716d2795ef55690b75eddf8978
    --- /dev/null
    +++ b/src/miniredis/commands/request.py
    @@ -0,0 +1,7 @@
    +from dataclasses import dataclass
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class CommandRequest:
    +    name: bytes
    +    args: tuple[bytes, ...] = ()
    ```

**What it is and why it appears**

`CommandRequest` is the smallest transport-neutral input: a binary command name and an immutable tuple of binary arguments.

**Runtime role**

Direct clients and the later RESP2 adapter will construct the same value before parsing.

**Key code**

```python
class CommandRequest:
    name: bytes
    args: tuple[bytes, ...] = ()
```

**Statement understanding**

Keeping the request binary and transport-neutral prevents the network adapter from owning command meaning.

??? note "File diff: src/miniredis/core/values.py"
    ```diff
    diff --git a/src/miniredis/core/values.py b/src/miniredis/core/values.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..3c0069ece455c305f21c7ea64767aef5ac716ad4
    --- /dev/null
    +++ b/src/miniredis/core/values.py
    @@ -0,0 +1,33 @@
    +from __future__ import annotations
    +
    +from collections import deque
    +from dataclasses import dataclass
    +from typing import TypeAlias
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class StringValue:
    +    data: bytes
    +
    +
    +@dataclass(slots=True)
    +class HashValue:
    +    items: dict[bytes, bytes]
    +
    +
    +@dataclass(slots=True)
    +class ListValue:
    +    items: deque[bytes]
    +
    +
    +@dataclass(slots=True)
    +class SetValue:
    +    items: set[bytes]
    +
    +
    +@dataclass(slots=True)
    +class ZSetValue:
    +    scores: dict[bytes, float]
    +
    +
    +RedisValue: TypeAlias = StringValue | HashValue | ListValue | SetValue | ZSetValue
    ```

**What it is and why it appears**

These five live value types expose the Python containers planners will copy and modify.

**Runtime role**

The database stores one `RedisValue` per key while command-specific planners enforce type compatibility.

**Key code**

```python
RedisValue: TypeAlias = StringValue | HashValue | ListValue | SetValue | ZSetValue
```

**Statement understanding**

The closed union makes every supported live shape explicit; adding a new value type must update freezing, sizing, persistence, and planners.

??? note "File diff: src/miniredis/core/reply.py"
    ```diff
    diff --git a/src/miniredis/core/reply.py b/src/miniredis/core/reply.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..cb58e6b129d8439bafce04320ae09a11f38c4d6b
    --- /dev/null
    +++ b/src/miniredis/core/reply.py
    @@ -0,0 +1,33 @@
    +from __future__ import annotations
    +
    +from dataclasses import dataclass
    +from typing import TypeAlias
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Ok:
    +    message: bytes = b"OK"
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Bytes:
    +    value: bytes | None
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Number:
    +    value: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Items:
    +    values: tuple[Reply, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Failure:
    +    code: str
    +    message: str
    +
    +
    +Reply: TypeAlias = Ok | Bytes | Number | Items | Failure
    ```

**What it is and why it appears**

Replies describe semantic outcomes before any adapter chooses wire bytes.

**Runtime role**

Direct callers receive these values directly; RESP2 later maps the same values to frames.

**Key code**

```python
Reply: TypeAlias = Ok | Bytes | Number | Items | Failure
```

**Statement understanding**

An error is data in the reply union, not an exception that can accidentally bypass ordered request completion.

??? note "File diff: src/miniredis/core/commit.py"
    ```diff
    diff --git a/src/miniredis/core/commit.py b/src/miniredis/core/commit.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..2274b6d0a9026c4377d2e8f76f60d789242473e2
    --- /dev/null
    +++ b/src/miniredis/core/commit.py
    @@ -0,0 +1,79 @@
    +from __future__ import annotations
    +
    +from dataclasses import dataclass
    +from enum import Enum
    +from typing import TypeAlias
    +
    +
    +class CommitTrigger(str, Enum):
    +    CLIENT = "client"
    +    ACTIVE_EXPIRE = "active_expire"
    +
    +
    +class DeleteReason(str, Enum):
    +    CLIENT = "client"
    +    EXPIRED = "expired"
    +    EVICTED = "evicted"
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class StoredString:
    +    data: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class StoredHash:
    +    items: tuple[tuple[bytes, bytes], ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class StoredList:
    +    items: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class StoredSet:
    +    items: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class StoredZSet:
    +    items: tuple[tuple[bytes, float], ...]
    +
    +
    +StoredValue: TypeAlias = StoredString | StoredHash | StoredList | StoredSet | StoredZSet
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class StoredEntry:
    +    value: StoredValue
    +    expire_at_ms: int | None
    +    mutation_version: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class PutEntry:
    +    key: bytes
    +    entry: StoredEntry
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class DeleteKey:
    +    key: bytes
    +    reason: DeleteReason
    +
    +
    +CommitOperation: TypeAlias = PutEntry | DeleteKey
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class CommitBatch:
    +    seq: int
    +    operations: tuple[CommitOperation, ...]
    +    trigger: CommitTrigger
    +
    +    def __post_init__(self) -> None:
    +        if self.seq <= 0:
    +            raise ValueError("commit seq must be positive")
    +        if not self.operations:
    +            raise ValueError("commit batch operations cannot be empty")
    ```

**What it is and why it appears**

Immutable stored values and commit operations are the vocabulary that later crosses AOF, recovery, and replication.

**Runtime role**

One positive sequence number orders one non-empty tuple of operations as a single atomic fact.

**Key code**

```python
class CommitBatch:
    seq: int
    operations: tuple[CommitOperation, ...]
```

**Statement understanding**

The batch, rather than an individual key mutation, is the propagation unit; splitting it later would break transaction and waiter atomicity.

??? note "File diff: src/miniredis/core/database.py"
    ```diff
    diff --git a/src/miniredis/core/database.py b/src/miniredis/core/database.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..25ad5ff367181660cd6fee8ed89dabecf8326f87
    --- /dev/null
    +++ b/src/miniredis/core/database.py
    @@ -0,0 +1,176 @@
    +from __future__ import annotations
    +
    +from collections import deque
    +from dataclasses import dataclass
    +
    +from miniredis.core.commit import (
    +    CommitBatch,
    +    DeleteKey,
    +    PutEntry,
    +    StoredEntry,
    +    StoredHash,
    +    StoredList,
    +    StoredSet,
    +    StoredString,
    +    StoredValue,
    +    StoredZSet,
    +)
    +from miniredis.core.values import (
    +    HashValue,
    +    ListValue,
    +    RedisValue,
    +    SetValue,
    +    StringValue,
    +    ZSetValue,
    +)
    +
    +
    +ENTRY_OVERHEAD = 64
    +EXPIRY_OVERHEAD = 16
    +
    +
    +@dataclass(slots=True)
    +class Entry:
    +    value: RedisValue
    +    expire_at_ms: int | None
    +    mutation_version: int
    +    last_access_tick: int
    +    logical_size: int
    +
    +
    +def logical_value_size(value: RedisValue | StoredValue) -> int:
    +    match value:
    +        case StringValue(data=data) | StoredString(data=data):
    +            return 16 + len(data)
    +        case HashValue(items=items):
    +            return 32 + sum(
    +                16 + len(field) + len(item) for field, item in items.items()
    +            )
    +        case StoredHash(items=items):
    +            return 32 + sum(16 + len(field) + len(item) for field, item in items)
    +        case (
    +            ListValue(items=items)
    +            | StoredList(items=items)
    +            | SetValue(items=items)
    +            | StoredSet(items=items)
    +        ):
    +            return 32 + sum(8 + len(item) for item in items)
    +        case ZSetValue(scores=scores):
    +            return 32 + sum(24 + len(member) for member in scores)
    +        case StoredZSet(items=items):
    +            return 32 + sum(24 + len(member) for member, _score in items)
    +        case _:
    +            raise TypeError(f"unsupported Redis value: {type(value)!r}")
    +
    +
    +def logical_entry_size(
    +    key: bytes, value: RedisValue | StoredValue, expire_at_ms: int | None
    +) -> int:
    +    return (
    +        ENTRY_OVERHEAD
    +        + len(key)
    +        + logical_value_size(value)
    +        + (EXPIRY_OVERHEAD if expire_at_ms is not None else 0)
    +    )
    +
    +
    +def freeze_value(value: RedisValue) -> StoredValue:
    +    match value:
    +        case StringValue(data=data):
    +            return StoredString(data)
    +        case HashValue(items=items):
    +            return StoredHash(tuple(sorted(items.items())))
    +        case ListValue(items=items):
    +            return StoredList(tuple(items))
    +        case SetValue(items=items):
    +            return StoredSet(tuple(sorted(items)))
    +        case ZSetValue(scores=scores):
    +            return StoredZSet(tuple(sorted(scores.items())))
    +        case _:
    +            raise TypeError(f"unsupported Redis value: {type(value)!r}")
    +
    +
    +def thaw_value(value: StoredValue) -> RedisValue:
    +    match value:
    +        case StoredString(data=data):
    +            return StringValue(data)
    +        case StoredHash(items=items):
    +            return HashValue(dict(items))
    +        case StoredList(items=items):
    +            return ListValue(deque(items))
    +        case StoredSet(items=items):
    +            return SetValue(set(items))
    +        case StoredZSet(items=items):
    +            return ZSetValue(dict(items))
    +        case _:
    +            raise TypeError(f"unsupported stored value: {type(value)!r}")
    +
    +
    +def _freeze_entry(entry: Entry) -> StoredEntry:
    +    return StoredEntry(
    +        value=freeze_value(entry.value),
    +        expire_at_ms=entry.expire_at_ms,
    +        mutation_version=entry.mutation_version,
    +    )
    +
    +
    +class Database:
    +    def __init__(self) -> None:
    +        self.entries: dict[bytes, Entry] = {}
    +        self.commit_seq = 0
    +        self.access_tick = 0
    +        self.logical_usage = 0
    +
    +    def apply_batch(self, batch: CommitBatch, *, track_access: bool) -> None:
    +        next_seq = self.commit_seq + 1
    +        if batch.seq != next_seq:
    +            raise ValueError(f"expected commit seq {next_seq}, got {batch.seq}")
    +
    +        staged = dict(self.entries)
    +        staged_access_tick = self.access_tick
    +
    +        for operation in batch.operations:
    +            match operation:
    +                case DeleteKey(key=key):
    +                    staged.pop(key, None)
    +                case PutEntry(key=key, entry=entry):
    +                    if track_access:
    +                        staged_access_tick += 1
    +                    value = thaw_value(entry.value)
    +                    staged[key] = Entry(
    +                        value=value,
    +                        expire_at_ms=entry.expire_at_ms,
    +                        mutation_version=entry.mutation_version,
    +                        last_access_tick=staged_access_tick if track_access else 0,
    +                        logical_size=logical_entry_size(key, value, entry.expire_at_ms),
    +                    )
    +                case _:
    +                    raise TypeError(
    +                        f"unsupported commit operation: {type(operation)!r}"
    +                    )
    +
    +        staged_usage = sum(entry.logical_size for entry in staged.values())
    +        if staged_usage < 0:
    +            raise AssertionError("logical usage cannot be negative")
    +        if any(entry.logical_size <= 0 for entry in staged.values()):
    +            raise AssertionError("entry logical size must be positive")
    +
    +        self.entries = staged
    +        self.logical_usage = staged_usage
    +        self.access_tick = staged_access_tick
    +        self.commit_seq = batch.seq
    +
    +    def touch_if_live(self, key: bytes, now_ms: int) -> bool:
    +        entry = self.entries.get(key)
    +        if entry is None or (
    +            entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms
    +        ):
    +            return False
    +        self.access_tick += 1
    +        entry.last_access_tick = self.access_tick
    +        return True
    +
    +    def logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
    +        return tuple(
    +            (key, _freeze_entry(entry)) for key, entry in sorted(self.entries.items())
    +        )
    ```

**What it is and why it appears**

The database owns live entries, sequence order, access metadata, and logical memory accounting.

**Runtime role**

`apply_batch` stages changes, thaws immutable values into fresh containers, verifies invariants, and only then replaces live state.

**Key code**

```python
next_seq = self.commit_seq + 1
if batch.seq != next_seq:
    raise ValueError(f"expected commit seq {next_seq}, got {batch.seq}")
```

**Statement understanding**

Sequence validation happens before publication, so a gap cannot be normalized into a plausible but incomplete history.

#### Package and test scaffold

The remaining files install the package, create package namespaces, and pin the test environment. They are necessary to run the Stage but add no MiniRedis mechanism.

#### Package and test scaffold

Install the package and test environment without treating exports, empty package markers, or the lockfile as Redis mechanisms.

??? note "Supporting file diffs (8 files)"
    **`pyproject.toml`**

    ```diff
    diff --git a/pyproject.toml b/pyproject.toml
    new file mode 100644
    index 0000000000000000000000000000000000000000..1e9b86283e2c41e7bd279011744db73208baeaf3
    --- /dev/null
    +++ b/pyproject.toml
    @@ -0,0 +1,26 @@
    +[build-system]
    +requires = ["hatchling"]
    +build-backend = "hatchling.build"
    +
    +[project]
    +name = "miniredis-reference"
    +version = "0.1.0"
    +description = "Direct-first MiniRedis reference implementation"
    +requires-python = ">=3.12"
    +dependencies = []
    +
    +[dependency-groups]
    +dev = [
    +    "pytest>=9,<10",
    +    "pytest-asyncio>=1.3,<2",
    +]
    +
    +[tool.hatch.build.targets.wheel]
    +packages = ["src/miniredis"]
    +
    +[tool.pytest.ini_options]
    +asyncio_mode = "auto"
    +asyncio_default_fixture_loop_scope = "function"
    +asyncio_default_test_loop_scope = "function"
    +pythonpath = ["src"]
    +testpaths = ["tests"]
    ```

    **`src/miniredis/__init__.py`**

    ```diff
    diff --git a/src/miniredis/__init__.py b/src/miniredis/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..d4792a841229d7110cbaed9f6b8bde385f210188
    --- /dev/null
    +++ b/src/miniredis/__init__.py
    @@ -0,0 +1 @@
    +"""MiniRedis reference package."""
    ```

    **`src/miniredis/adapters/__init__.py`**

    ```diff
    diff --git a/src/miniredis/adapters/__init__.py b/src/miniredis/adapters/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..5cbc66c7be00c157dce6147cb71c005718ae0e57
    --- /dev/null
    +++ b/src/miniredis/adapters/__init__.py
    @@ -0,0 +1 @@
    +"""Transport adapter package with no package-level exports."""
    ```

    **`src/miniredis/commands/__init__.py`**

    ```diff
    diff --git a/src/miniredis/commands/__init__.py b/src/miniredis/commands/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..3d5e5d836355823caa35969c44d02eb4582a5030
    --- /dev/null
    +++ b/src/miniredis/commands/__init__.py
    @@ -0,0 +1 @@
    +"""Typed command requests, parsing, and model package."""
    ```

    **`src/miniredis/core/__init__.py`**

    ```diff
    diff --git a/src/miniredis/core/__init__.py b/src/miniredis/core/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..19dbae656496aab912631630b84455db11e3cd1e
    --- /dev/null
    +++ b/src/miniredis/core/__init__.py
    @@ -0,0 +1 @@
    +"""MiniRedis domain and executor package."""
    ```

    **`src/miniredis/persistence/__init__.py`**

    ```diff
    diff --git a/src/miniredis/persistence/__init__.py b/src/miniredis/persistence/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..604d7ccdfa6bc5e807ffd482855fc322c6bede35
    --- /dev/null
    +++ b/src/miniredis/persistence/__init__.py
    @@ -0,0 +1 @@
    +"""Persistence implementation package with no package-level exports."""
    ```

    **`src/miniredis/replication/__init__.py`**

    ```diff
    diff --git a/src/miniredis/replication/__init__.py b/src/miniredis/replication/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..ccf53543ac32f7f3dc193fe00f93c675a94a5733
    --- /dev/null
    +++ b/src/miniredis/replication/__init__.py
    @@ -0,0 +1 @@
    +"""Replication implementation package with no package-level exports."""
    ```

    **`uv.lock`**

    ```diff
    diff --git a/uv.lock b/uv.lock
    new file mode 100644
    index 0000000000000000000000000000000000000000..0650428f45c9f2a1905285a21710cb07872234f5
    --- /dev/null
    +++ b/uv.lock
    @@ -0,0 +1,105 @@
    +version = 1
    +revision = 3
    +requires-python = ">=3.12"
    +
    +[[package]]
    +name = "colorama"
    +version = "0.4.6"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/d8/53/6f443c9a4a8358a93a6792e2acffb9d9d5cb0a5cfd8802644b7b1c9a02e4/colorama-0.4.6.tar.gz", hash = "sha256:08695f5cb7ed6e0531a20572697297273c47b8cae5a63ffc6d6ed5c201be6e44", size = 27697, upload-time = "2022-10-25T02:36:22.414Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/d1/d6/3965ed04c63042e047cb6a3e6ed1a63a35087b6a609aa3a15ed8ac56c221/colorama-0.4.6-py2.py3-none-any.whl", hash = "sha256:4f1d9991f5acc0ca119f9d443620b77f9d6b33703e51011c16baf57afb285fc6", size = 25335, upload-time = "2022-10-25T02:36:20.889Z" },
    +]
    +
    +[[package]]
    +name = "iniconfig"
    +version = "2.3.0"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/72/34/14ca021ce8e5dfedc35312d08ba8bf51fdd999c576889fc2c24cb97f4f10/iniconfig-2.3.0.tar.gz", hash = "sha256:c76315c77db068650d49c5b56314774a7804df16fee4402c1f19d6d15d8c4730", size = 20503, upload-time = "2025-10-18T21:55:43.219Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/cb/b1/3846dd7f199d53cb17f49cba7e651e9ce294d8497c8c150530ed11865bb8/iniconfig-2.3.0-py3-none-any.whl", hash = "sha256:f631c04d2c48c52b84d0d0549c99ff3859c98df65b3101406327ecc7d53fbf12", size = 7484, upload-time = "2025-10-18T21:55:41.639Z" },
    +]
    +
    +[[package]]
    +name = "miniredis-reference"
    +version = "0.1.0"
    +source = { editable = "." }
    +
    +[package.dev-dependencies]
    +dev = [
    +    { name = "pytest" },
    +    { name = "pytest-asyncio" },
    +]
    +
    +[package.metadata]
    +
    +[package.metadata.requires-dev]
    +dev = [
    +    { name = "pytest", specifier = ">=9,<10" },
    +    { name = "pytest-asyncio", specifier = ">=1.3,<2" },
    +]
    +
    +[[package]]
    +name = "packaging"
    +version = "26.2"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/d7/f1/e7a6dd94a8d4a5626c03e4e99c87f241ba9e350cd9e6d75123f992427270/packaging-26.2.tar.gz", hash = "sha256:ff452ff5a3e828ce110190feff1178bb1f2ea2281fa2075aadb987c2fb221661", size = 228134, upload-time = "2026-04-24T20:15:23.917Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/df/b2/87e62e8c3e2f4b32e5fe99e0b86d576da1312593b39f47d8ceef365e95ed/packaging-26.2-py3-none-any.whl", hash = "sha256:5fc45236b9446107ff2415ce77c807cee2862cb6fac22b8a73826d0693b0980e", size = 100195, upload-time = "2026-04-24T20:15:22.081Z" },
    +]
    +
    +[[package]]
    +name = "pluggy"
    +version = "1.6.0"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/f9/e2/3e91f31a7d2b083fe6ef3fa267035b518369d9511ffab804f839851d2779/pluggy-1.6.0.tar.gz", hash = "sha256:7dcc130b76258d33b90f61b658791dede3486c3e6bfb003ee5c9bfb396dd22f3", size = 69412, upload-time = "2025-05-15T12:30:07.975Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/54/20/4d324d65cc6d9205fabedc306948156824eb9f0ee1633355a8f7ec5c66bf/pluggy-1.6.0-py3-none-any.whl", hash = "sha256:e920276dd6813095e9377c0bc5566d94c932c33b27a3e3945d8389c374dd4746", size = 20538, upload-time = "2025-05-15T12:30:06.134Z" },
    +]
    +
    +[[package]]
    +name = "pygments"
    +version = "2.20.0"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/c3/b2/bc9c9196916376152d655522fdcebac55e66de6603a76a02bca1b6414f6c/pygments-2.20.0.tar.gz", hash = "sha256:6757cd03768053ff99f3039c1a36d6c0aa0b263438fcab17520b30a303a82b5f", size = 4955991, upload-time = "2026-03-29T13:29:33.898Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/f4/7e/a72dd26f3b0f4f2bf1dd8923c85f7ceb43172af56d63c7383eb62b332364/pygments-2.20.0-py3-none-any.whl", hash = "sha256:81a9e26dd42fd28a23a2d169d86d7ac03b46e2f8b59ed4698fb4785f946d0176", size = 1231151, upload-time = "2026-03-29T13:29:30.038Z" },
    +]
    +
    +[[package]]
    +name = "pytest"
    +version = "9.1.1"
    +source = { registry = "https://pypi.org/simple" }
    +dependencies = [
    +    { name = "colorama", marker = "sys_platform == 'win32'" },
    +    { name = "iniconfig" },
    +    { name = "packaging" },
    +    { name = "pluggy" },
    +    { name = "pygments" },
    +]
    +sdist = { url = "https://files.pythonhosted.org/packages/e4/47/b9efed96c114afcfa3c9d3fe98a76a1d14c74a9e266d397cf6eb64be5e01/pytest-9.1.1.tar.gz", hash = "sha256:1088fbde8f2b49d95a549a195707afa7a76a3ce9bcadc26b6d71f0ffda5fe313", size = 1636369, upload-time = "2026-06-19T10:58:32.857Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/24/25/1de2678b631f5a49215c6c96fff41ba892b0a34df68d6d80292b1b48aa7f/pytest-9.1.1-py3-none-any.whl", hash = "sha256:37a86b45efb9a47a61a36449063e8e18d0cab3161329fc099eb21783169c4f0c", size = 386536, upload-time = "2026-06-19T10:58:31.347Z" },
    +]
    +
    +[[package]]
    +name = "pytest-asyncio"
    +version = "1.4.0"
    +source = { registry = "https://pypi.org/simple" }
    +dependencies = [
    +    { name = "pytest" },
    +    { name = "typing-extensions", marker = "python_full_version < '3.13'" },
    +]
    +sdist = { url = "https://files.pythonhosted.org/packages/43/7c/d36d04db312ecf4298932ef77e6e4a9e8ad017906e24e34f0b0c361a2473/pytest_asyncio-1.4.0.tar.gz", hash = "sha256:c6c0d2259945122819f171a32ecea2c349ead889ee28176caaf492143424be42", size = 58514, upload-time = "2026-05-26T09:56:04.083Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/03/e2/08a497ef684b88559c9cc5f4ad53a37e7b99e727094a86d6ea32536d5d3c/pytest_asyncio-1.4.0-py3-none-any.whl", hash = "sha256:933ca923a23075a87fb7070c0ec272a6848489824d887c85c812670932835aa1", size = 16930, upload-time = "2026-05-26T09:56:02.576Z" },
    +]
    +
    +[[package]]
    +name = "typing-extensions"
    +version = "4.16.0"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/f6/cc/6253133b5bb138fc3306cebfbda2c520f545d36b5be2c7255cc528bb45d6/typing_extensions-4.16.0.tar.gz", hash = "sha256:dc983d19a509c94dba722ee6abd33940f7c05a89e243c47e907eb4db6f1a43e5", size = 113555, upload-time = "2026-07-02T08:40:05.92Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/49/d3/b8441a820a491ddfc024b0b0cf0393375b75ea13866d9c66727e54c2fc80/typing_extensions-4.16.0-py3-none-any.whl", hash = "sha256:481caa481374e813c1b176ada14e97f1f67a4539ce9cfeb3f350d78d6370c2e8", size = 45571, upload-time = "2026-07-02T08:40:04.659Z" },
    +]
    ```


### Verification evidence

Run `uv run pytest -q $(cat journey/stages/01-domain-state/tests.txt)`. It proves the value, reply, commit, and atomic database contracts; it does not yet prove command execution or concurrency.

### Durable takeaways

Live values and historical values are different representations. One immutable batch is the ordered state-transition unit. Replies are independent of transports, and rejected database transitions publish nothing.

### Explain it in your own words

MiniRedis begins by defining what may exist and how one change becomes a stable fact. Mutable containers stay inside the database, immutable stored values cross boundaries, and a sequence-checked batch becomes visible only after the whole staged state is valid.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/02-command-life.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/tree/f68b061)

After finishing, run `python -m journey.tools.build_journey check 1` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/01-domain-state/stage.patch)

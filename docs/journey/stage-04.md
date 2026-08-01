# Stage 04 · Atomic String planning

### Goal

Plan String commands as side-effect-free replies plus one serialized commit.

??? note "Deliverable files"
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/expiration.py`
    - `src/miniredis/core/planner.py`
    - `src/miniredis/core/planning.py`
    - `tests/concurrency/test_atomic_incr.py`
    - `tests/contract/test_strings.py`

### The problem at this point

The executor can order requests but only answers `PING` and `ECHO`. A String mutation needs to inspect old state, reject wrong types or overflow without allocating a commit, and let one hundred concurrent `INCR` calls each observe a distinct serialized predecessor.

### Test contract

#### See the failure first

The contract stores the non-canonical integer `01`, attempts `INCR`, and requires both the value and commit sequence to remain unchanged. Another case starts one hundred concurrent increments and requires the final value and sequence to account for every accepted mutation exactly once.

??? note "File diff: tests/concurrency/test_atomic_incr.py"
    ```diff
    diff --git a/tests/concurrency/test_atomic_incr.py b/tests/concurrency/test_atomic_incr.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..3026ab9282c7187d8b1276012bc96a319245b604
    --- /dev/null
    +++ b/tests/concurrency/test_atomic_incr.py
    @@ -0,0 +1,21 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes
    +
    +
    +@pytest.mark.asyncio
    +async def test_one_hundred_concurrent_increments_are_serialized():
    +    async with MiniRedis.open(max_pending_commands=256) as runtime:
    +        clients = [runtime.direct_client() for _ in range(100)]
    +        await asyncio.gather(
    +            *(
    +                client.execute(CommandRequest(b"INCR", (b"counter",)))
    +                for client in clients
    +            )
    +        )
    +        assert await clients[0].execute(CommandRequest(b"GET", (b"counter",))) == Bytes(
    +            b"100"
    +        )
    ```

**What this test locks**

It locks read-plan-apply as one executor turn under concurrent callers.

**How it constructs the counterexample**

One hundred tasks submit `INCR` without external locking, then the test checks the final value and unique numeric replies.

**Key test statement**

```python
assert await client.execute(CommandRequest(b"GET", (b"counter",))) == Bytes(b"100")
```

**What a failure means**

A lost or duplicated increment means callers read stale state outside the serialized owner.

??? note "File diff: tests/contract/test_strings.py"
    ```diff
    diff --git a/tests/contract/test_strings.py b/tests/contract/test_strings.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..00d39808ad88d37f51f3cb51c60bb4cc13a01820
    --- /dev/null
    +++ b/tests/contract/test_strings.py
    @@ -0,0 +1,66 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes, Failure, Number, Ok
    +
    +
    +@pytest.mark.asyncio
    +async def test_set_conditions_replace_type_and_clear_old_state():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(CommandRequest(b"SET", (b"k", b"1"))) == Ok()
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(CommandRequest(b"SET", (b"k", b"2", b"NX"))) == Bytes(
    +            None
    +        )
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"SET", (b"k", b"2", b"XX"))) == Ok()
    +        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(b"2")
    +
    +
    +@pytest.mark.asyncio
    +async def test_invalid_integer_and_overflow_do_not_commit():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"SET", (b"k", b"01")))
    +        before = runtime.debug_commit_seq
    +        assert isinstance(await c.execute(CommandRequest(b"INCR", (b"k",))), Failure)
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(b"01")
    +
    +        maximum = b"9223372036854775807"
    +        assert await c.execute(CommandRequest(b"SET", (b"k", maximum))) == Ok()
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(CommandRequest(b"INCR", (b"k",))) == Failure(
    +            "ERR", "value is not an integer or out of range"
    +        )
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(maximum)
    +
    +        minimum = b"-9223372036854775808"
    +        assert await c.execute(CommandRequest(b"SET", (b"k", minimum))) == Ok()
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(CommandRequest(b"INCRBY", (b"k", b"-1"))) == Failure(
    +            "ERR", "value is not an integer or out of range"
    +        )
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(minimum)
    +
    +
    +@pytest.mark.asyncio
    +async def test_general_commands_and_incrby_cover_the_frozen_subset():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(CommandRequest(b"PING")) == Ok(b"PONG")
    +        assert await c.execute(CommandRequest(b"PING", (b"\x00pong",))) == Bytes(
    +            b"\x00pong"
    +        )
    +        assert await c.execute(CommandRequest(b"ECHO", (b"\xff",))) == Bytes(b"\xff")
    +        assert await c.execute(CommandRequest(b"SET", (b"k", b"1"))) == Ok()
    +        assert await c.execute(
    +            CommandRequest(b"EXISTS", (b"k", b"k", b"missing"))
    +        ) == Number(2)
    +        assert await c.execute(CommandRequest(b"TYPE", (b"k",))) == Bytes(b"string")
    +        assert await c.execute(CommandRequest(b"INCRBY", (b"k", b"4"))) == Number(5)
    +        assert await c.execute(CommandRequest(b"DEL", (b"k", b"k"))) == Number(1)
    +        assert await c.execute(CommandRequest(b"TYPE", (b"k",))) == Bytes(b"none")
    ```

**What this test locks**

It locks `SET` conditions, missing values, signed 64-bit arithmetic, overflow, type replacement, ordered multi-key replies, and no-commit error/no-op paths.

**How it constructs the counterexample**

It captures `debug_commit_seq` before invalid integers, overflow, and failed `NX`, then verifies both sequence and stored bytes are unchanged.

**Key test statement**

```python
assert runtime.debug_commit_seq == before
```

**What a failure means**

A semantic error has leaked an operation or allocated a false historical commit.

### Basic concepts

An `ExecutionPlan` contains a reply, immutable operations, optional touched keys, and a trigger. Planning reads state but does not publish it. A no-op or failure can return a reply with no operations; only a non-empty successful plan becomes a sequenced `CommitBatch`.

### Why this mechanism is necessary

Separating planning from application makes error atomicity visible and keeps the executor as the sole sequence allocator. It also gives later AOF and replication one stable batch rather than a command-specific mutation procedure.

### Runtime mental model

Inside one executor turn, the planner looks up the key, proposes expiry cleanup and a new stored String if valid, and returns a reply. The executor allocates `commit_seq + 1`, applies the whole batch, touches successful reads, and only then completes the request.

### Mechanism blocks

#### Pure String planning

Turn String reads and mutations into replies plus immutable operations, including conditional no-ops and overflow failures.

??? note "File diff: src/miniredis/core/expiration.py"
    ```diff
    diff --git a/src/miniredis/core/expiration.py b/src/miniredis/core/expiration.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..2595dcc4c0099fc860e11de6d8903ea0ba587bfd
    --- /dev/null
    +++ b/src/miniredis/core/expiration.py
    @@ -0,0 +1,10 @@
    +from miniredis.core.commit import DeleteKey, DeleteReason
    +from miniredis.core.database import Entry
    +
    +
    +def is_expired(entry: Entry, now_ms: int) -> bool:
    +    return entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms
    +
    +
    +def expiry_delete(key: bytes) -> DeleteKey:
    +    return DeleteKey(key, DeleteReason.EXPIRED)
    ```

**What it is and why it appears**

The initial expiry helpers classify an entry against the executor's sampled time and build an explicit delete operation.

**Runtime role**

String lookup can treat elapsed data as absent without mutating during planning.

**Key code**

```python
def is_expired(entry: Entry, now_ms: int) -> bool:
    return entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms
```

**Statement understanding**

Logical absence and physical cleanup are separated; a later commit decides whether the proposed delete publishes.

??? note "File diff: src/miniredis/core/planning.py"
    ```diff
    diff --git a/src/miniredis/core/planning.py b/src/miniredis/core/planning.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..527ed614b6dc95788368088598b5b27b78c15bab
    --- /dev/null
    +++ b/src/miniredis/core/planning.py
    @@ -0,0 +1,206 @@
    +from __future__ import annotations
    +
    +from collections.abc import Iterable
    +
    +from miniredis.commands import model as cmd
    +from miniredis.commands.parser import (
    +    INT64_MAX,
    +    INT64_MIN,
    +    CommandParseError,
    +    parse_int64,
    +)
    +from miniredis.core.commit import (
    +    CommitOperation,
    +    DeleteKey,
    +    DeleteReason,
    +    PutEntry,
    +    StoredEntry,
    +)
    +from miniredis.core.database import Database, Entry, freeze_value
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.expiration import expiry_delete, is_expired
    +from miniredis.core.reply import Bytes, Failure, Number, Ok
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
    +WRONGTYPE = Failure(
    +    "WRONGTYPE",
    +    "operation against a key holding the wrong kind of value",
    +)
    +
    +
    +def lookup(
    +    database: Database,
    +    key: bytes,
    +    now_ms: int,
    +) -> tuple[Entry | None, tuple[CommitOperation, ...]]:
    +    entry = database.entries.get(key)
    +    if entry is None:
    +        return None, ()
    +    if is_expired(entry, now_ms):
    +        return None, (expiry_delete(key),)
    +    return entry, ()
    +
    +
    +def dedupe_operations(
    +    operations: Iterable[CommitOperation],
    +) -> tuple[CommitOperation, ...]:
    +    result: list[CommitOperation] = []
    +    delete_indexes: dict[bytes, int] = {}
    +    put_indexes: dict[bytes, int] = {}
    +    for operation in operations:
    +        if isinstance(operation, DeleteKey):
    +            previous = delete_indexes.get(operation.key)
    +            if previous is None:
    +                delete_indexes[operation.key] = len(result)
    +                result.append(operation)
    +            else:
    +                result[previous] = operation
    +        else:
    +            previous = put_indexes.get(operation.key)
    +            if previous is None:
    +                put_indexes[operation.key] = len(result)
    +                result.append(operation)
    +            else:
    +                result[previous] = operation
    +    return tuple(result)
    +
    +
    +def make_put(
    +    key: bytes,
    +    value: RedisValue,
    +    previous: Entry | None,
    +    expire_at_ms: int | None,
    +) -> PutEntry:
    +    return PutEntry(
    +        key,
    +        StoredEntry(
    +            freeze_value(value),
    +            expire_at_ms,
    +            1 if previous is None else previous.mutation_version + 1,
    +        ),
    +    )
    +
    +
    +def type_name(entry: Entry | None) -> bytes:
    +    if entry is None:
    +        return b"none"
    +    match entry.value:
    +        case StringValue():
    +            return b"string"
    +        case HashValue():
    +            return b"hash"
    +        case ListValue():
    +            return b"list"
    +        case SetValue():
    +            return b"set"
    +        case ZSetValue():
    +            return b"zset"
    +    raise AssertionError(f"unhandled value: {entry.value!r}")
    +
    +
    +def _integer_failure() -> ExecutionPlan:
    +    return ExecutionPlan(Failure("ERR", "value is not an integer or out of range"))
    +
    +
    +def plan_general_and_strings(
    +    command: cmd.Command,
    +    database: Database,
    +    now_ms: int,
    +) -> ExecutionPlan | None:
    +    match command:
    +        case cmd.Ping(None):
    +            return ExecutionPlan(Ok(b"PONG"))
    +        case cmd.Ping(message):
    +            return ExecutionPlan(Bytes(message))
    +        case cmd.Echo(message):
    +            return ExecutionPlan(Bytes(message))
    +        case cmd.Delete(keys):
    +            operations: list[CommitOperation] = []
    +            removed = 0
    +            seen: set[bytes] = set()
    +            for key in keys:
    +                entry, expired = lookup(database, key, now_ms)
    +                operations.extend(expired)
    +                if key in seen:
    +                    continue
    +                seen.add(key)
    +                if entry is not None:
    +                    removed += 1
    +                    operations.append(DeleteKey(key, DeleteReason.CLIENT))
    +            return ExecutionPlan(Number(removed), dedupe_operations(operations))
    +        case cmd.Exists(keys):
    +            operations = []
    +            touches: list[bytes] = []
    +            count = 0
    +            for key in keys:
    +                entry, expired = lookup(database, key, now_ms)
    +                operations.extend(expired)
    +                if entry is not None:
    +                    count += 1
    +                    touches.append(key)
    +            return ExecutionPlan(
    +                Number(count),
    +                dedupe_operations(operations),
    +                tuple(touches),
    +            )
    +        case cmd.TypeOf(key):
    +            entry, expired = lookup(database, key, now_ms)
    +            return ExecutionPlan(
    +                Bytes(type_name(entry)),
    +                expired,
    +                (key,) if entry is not None else (),
    +            )
    +        case cmd.GetString(key):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Bytes(None), expired)
    +            if not isinstance(entry.value, StringValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            return ExecutionPlan(Bytes(entry.value.data), expired, (key,))
    +        case cmd.SetString(key, value, only_if, expire_ms):
    +            previous, expired = lookup(database, key, now_ms)
    +            if only_if == "nx" and previous is not None:
    +                return ExecutionPlan(Bytes(None))
    +            if only_if == "xx" and previous is None:
    +                return ExecutionPlan(Bytes(None))
    +            expire_at_ms = None if expire_ms is None else now_ms + expire_ms
    +            put = make_put(
    +                key,
    +                StringValue(value),
    +                previous,
    +                expire_at_ms,
    +            )
    +            return ExecutionPlan(Ok(), expired + (put,))
    +        case cmd.Increment(key, amount):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is None:
    +                old_value = 0
    +                old_expiry = None
    +            elif not isinstance(previous.value, StringValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            else:
    +                old_expiry = previous.expire_at_ms
    +                try:
    +                    old_value = parse_int64(previous.value.data)
    +                except CommandParseError:
    +                    return _integer_failure()
    +            new_value = old_value + amount
    +            if not INT64_MIN <= new_value <= INT64_MAX:
    +                return _integer_failure()
    +            put = make_put(
    +                key,
    +                StringValue(str(new_value).encode("ascii")),
    +                previous,
    +                old_expiry,
    +            )
    +            return ExecutionPlan(Number(new_value), expired + (put,))
    +        case _:
    +            return None
    ```

**What it is and why it appears**

This module owns shared lookup/building rules and String command semantics.

**Runtime role**

It returns precise replies plus frozen `PutEntry`/`DeleteKey` operations while preserving old expiry when required.

**Key code**

```python
new_value = old_value + amount
if not INT64_MIN <= new_value <= INT64_MAX:
    return _integer_failure()
```

**Statement understanding**

Overflow returns a plan without operations; checking after applying would corrupt both value and history.

??? note "File diff: src/miniredis/core/planner.py"
    ```diff
    diff --git a/src/miniredis/core/planner.py b/src/miniredis/core/planner.py
    index 697ce08de85ee834c15469c9ef6297c65bf2e1da..7672f517999802f4fda3327fb30322d356eb2ab1 100644
    --- a/src/miniredis/core/planner.py
    +++ b/src/miniredis/core/planner.py
    @@ -1,22 +1,22 @@
    -from __future__ import annotations
    -
    -from miniredis.commands.model import Command, Echo, Ping
    +from miniredis.commands.model import Command
     from miniredis.config import MiniRedisConfig
     from miniredis.core.database import Database
     from miniredis.core.executor import ExecutionPlan
    -from miniredis.core.reply import Bytes, Failure, Ok
    +from miniredis.core.planning import plan_general_and_strings
    +from miniredis.core.reply import Failure


     class CommandPlanner:
         def __init__(self, config: MiniRedisConfig) -> None:
             self.config = config

    -    def plan(self, database: Database, command: Command, now_ms: int) -> ExecutionPlan:
    -        del database, now_ms
    -        match command:
    -            case Ping(message=None):
    -                return ExecutionPlan(Ok(b"PONG"))
    -            case Ping(message=message) | Echo(message=message):
    -                return ExecutionPlan(Bytes(message))
    -            case _:
    -                return ExecutionPlan(Failure("ERR", "unknown command"))
    +    def plan(
    +        self,
    +        command: Command,
    +        database: Database,
    +        now_ms: int,
    +    ) -> ExecutionPlan:
    +        plan = plan_general_and_strings(command, database, now_ms)
    +        if plan is not None:
    +            return plan
    +        return ExecutionPlan(Failure("ERR", "unknown command"))
    ```

**What it is and why it appears**

`CommandPlanner` is the stable routing facade between typed commands and per-family pure planners.

**Runtime role**

The executor invokes one method without learning String-specific branches.

**Key code**

```python
plan = plan_general_and_strings(command, database, now_ms)
if plan is not None:
    return plan
```

**Statement understanding**

Command-family growth stays behind the planner boundary rather than expanding executor ownership.

#### Serialized commit allocation

Allocate the next sequence and apply a planned mutation only inside the executor's ordered turn.

??? note "File diff: src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index cb776bb2ea5098f029ece2e3965b59bc10f71c40..54dcf634f42947df50fb9bc43b21e68ad15c1b51 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -64,7 +64,7 @@ class NullCommitBarrier:

     class Planner(Protocol):
         def plan(
    -        self, database: Database, command: Command, now_ms: int
    +        self, command: Command, database: Database, now_ms: int
         ) -> ExecutionPlan: ...


    @@ -201,7 +201,7 @@ class CommandExecutor:

         async def _execute(self, request: ExecuteRequest) -> None:
             now_ms = self.clock.now_ms()
    -        plan = self.planner.plan(self.database, request.command, now_ms)
    +        plan = self.planner.plan(request.command, self.database, now_ms)
             if plan.operations:
                 batch = CommitBatch(
                     seq=self.database.commit_seq + 1,
    ```

**What it is and why it appears**

The executor now turns non-empty plans into ordered commit batches and applies them exactly once.

**Runtime role**

It samples time, plans against current state, allocates the next sequence, applies, then completes the reply.

**Key code**

```python
batch = CommitBatch(
    seq=self.database.commit_seq + 1,
    operations=plan.operations,
    trigger=plan.trigger,
)
await self.commit_barrier.append(batch)
self.database.apply_batch(
    batch, track_access=plan.trigger is CommitTrigger.CLIENT
)
```

**Statement understanding**

Sequence allocation occurs beside application under the same owner, so concurrent `INCR` calls cannot share a predecessor.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/04-atomic-strings/tests.txt)`. It proves String semantics; also run `tests/concurrency/test_atomic_incr.py` to observe serialized concurrent increments.

### Durable takeaways

Planning is pure, errors and no-ops have no commit, and only the executor allocates and applies the next batch. Concurrency is resolved by ownership, not by command-specific locks.

### Explain it in your own words

A String command first becomes a proposal. If the proposal is valid and mutating, the executor turns it into the next immutable batch and applies it before replying; otherwise it returns a semantic result without inventing history.

### Textbook

[Chapter 3](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/03-data-types.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/a5f7a27...be7969d)

After finishing, run `python -m journey.tools.build_journey check 4` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/04-atomic-strings/stage.patch)

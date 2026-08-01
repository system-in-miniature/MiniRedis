# Stage 05 · Hash and List planning

### Goal

Extend pure planning to field maps and direction-sensitive ordered sequences.

??? note "Deliverable files"
    - `src/miniredis/core/hash_planner.py`
    - `src/miniredis/core/list_planner.py`
    - `src/miniredis/core/planner.py`
    - `tests/contract/test_hashes.py`
    - `tests/contract/test_lists.py`

### The problem at this point

String replacement has one scalar value. Hash commands must distinguish new fields from overwritten fields and delete an empty key; List commands must preserve left/right direction and Redis's inclusive negative range rules. Neither should mutate the live container while deciding its reply.

### Test contract

#### See the failure first

The Hash contract stores `01`, attempts `HINCRBY`, and requires no commit or field change. The List contract asks for reversed and far-negative ranges and performs the final pop, proving boundary calculations cannot accidentally retain an empty key or slice with Python's different conventions.

??? note "File diff: tests/contract/test_hashes.py"
    ```diff
    diff --git a/tests/contract/test_hashes.py b/tests/contract/test_hashes.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..d3be0b9c9339c6f63ba431ad18c04c7d4fe26afe
    --- /dev/null
    +++ b/tests/contract/test_hashes.py
    @@ -0,0 +1,84 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes, Failure, Items, Number
    +
    +
    +@pytest.mark.asyncio
    +async def test_hash_semantics_and_last_field_removal():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(
    +            CommandRequest(b"HSET", (b"h", b"a", b"1", b"a", b"2", b"b", b"3"))
    +        ) == Number(2)
    +        assert await c.execute(CommandRequest(b"HGET", (b"h", b"a"))) == Bytes(b"2")
    +        assert await c.execute(
    +            CommandRequest(b"HINCRBY", (b"h", b"a", b"5"))
    +        ) == Number(7)
    +        assert await c.execute(CommandRequest(b"HDEL", (b"h", b"a", b"b"))) == Number(2)
    +        assert await c.execute(CommandRequest(b"TYPE", (b"h",))) == Bytes(b"none")
    +
    +
    +@pytest.mark.asyncio
    +async def test_hash_integer_error_is_atomic():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(CommandRequest(b"HSET", (b"h", b"f", b"01"))) == Number(
    +            1
    +        )
    +        before = runtime.debug_commit_seq
    +        reply = await c.execute(CommandRequest(b"HINCRBY", (b"h", b"f", b"1")))
    +        assert reply == Failure("ERR", "value is not an integer or out of range")
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"HGET", (b"h", b"f"))) == Bytes(b"01")
    +
    +
    +@pytest.mark.asyncio
    +async def test_hgetall_is_alternating_and_missing_fields_are_nil():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"HSET", (b"h", b"b", b"2", b"a", b"1")))
    +        reply = await c.execute(CommandRequest(b"HGETALL", (b"h",)))
    +        assert isinstance(reply, Items)
    +        assert {
    +            (reply.values[index].value, reply.values[index + 1].value)
    +            for index in range(0, len(reply.values), 2)
    +        } == {(b"a", b"1"), (b"b", b"2")}
    +        assert await c.execute(CommandRequest(b"HGET", (b"h", b"missing"))) == Bytes(
    +            None
    +        )
    +
    +
    +@pytest.mark.asyncio
    +async def test_hash_wrongtype_overflow_and_noop_delete_do_not_commit():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"SET", (b"string", b"value")))
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(
    +            CommandRequest(b"HSET", (b"string", b"field", b"value"))
    +        ) == Failure(
    +            "WRONGTYPE",
    +            "operation against a key holding the wrong kind of value",
    +        )
    +        assert runtime.debug_commit_seq == before
    +
    +        maximum = b"9223372036854775807"
    +        assert await c.execute(
    +            CommandRequest(b"HSET", (b"h", b"field", maximum))
    +        ) == Number(1)
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(
    +            CommandRequest(b"HINCRBY", (b"h", b"field", b"1"))
    +        ) == Failure("ERR", "value is not an integer or out of range")
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"HGET", (b"h", b"field"))) == Bytes(
    +            maximum
    +        )
    +
    +        before_tick = runtime.database.entries[b"h"].last_access_tick
    +        assert await c.execute(
    +            CommandRequest(b"HDEL", (b"h", b"missing", b"missing"))
    +        ) == Number(0)
    +        assert runtime.debug_commit_seq == before
    +        assert runtime.database.entries[b"h"].last_access_tick > before_tick
    ```

**What this test locks**

It locks duplicate-field counting, overwrite, integer error atomicity, alternating `HGETALL`, wrong-type behavior, no-op touches, and last-field key removal.

**How it constructs the counterexample**

It combines duplicate fields and invalid stored integers while checking both replies and commit sequence.

**Key test statement**

```python
assert runtime.debug_commit_seq == before
```

**What a failure means**

The planner mutated while validating, counted arguments instead of new fields, or represented an empty Hash as a live key.

??? note "File diff: tests/contract/test_lists.py"
    ```diff
    diff --git a/tests/contract/test_lists.py b/tests/contract/test_lists.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..f86017b8f908d91e31fc27b15f6c558ede5ef122
    --- /dev/null
    +++ b/tests/contract/test_lists.py
    @@ -0,0 +1,61 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes, Failure, Items, Number
    +
    +
    +@pytest.mark.asyncio
    +async def test_list_push_pop_and_range():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(
    +            CommandRequest(b"LPUSH", (b"l", b"a", b"b", b"c"))
    +        ) == Number(3)
    +        assert await c.execute(CommandRequest(b"LRANGE", (b"l", b"0", b"-1"))) == Items(
    +            (Bytes(b"c"), Bytes(b"b"), Bytes(b"a"))
    +        )
    +        assert await c.execute(CommandRequest(b"RPOP", (b"l",))) == Bytes(b"a")
    +        assert await c.execute(
    +            CommandRequest(b"LRANGE", (b"l", b"-99", b"99"))
    +        ) == Items((Bytes(b"c"), Bytes(b"b")))
    +
    +
    +@pytest.mark.asyncio
    +async def test_rpush_lpop_and_last_element_removal():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(CommandRequest(b"RPUSH", (b"q", b"a", b"b"))) == Number(
    +            2
    +        )
    +        assert await c.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"a")
    +        assert await c.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"b")
    +        assert await c.execute(CommandRequest(b"TYPE", (b"q",))) == Bytes(b"none")
    +        assert await c.execute(CommandRequest(b"RPOP", (b"missing",))) == Bytes(None)
    +
    +
    +@pytest.mark.asyncio
    +async def test_list_wrongtype_and_range_boundaries_are_side_effect_safe():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"SET", (b"string", b"value")))
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(
    +            CommandRequest(b"LPUSH", (b"string", b"item"))
    +        ) == Failure(
    +            "WRONGTYPE",
    +            "operation against a key holding the wrong kind of value",
    +        )
    +        assert runtime.debug_commit_seq == before
    +
    +        assert await c.execute(
    +            CommandRequest(b"RPUSH", (b"l", b"a", b"b", b"c"))
    +        ) == Number(3)
    +        assert await c.execute(CommandRequest(b"LRANGE", (b"l", b"2", b"1"))) == Items(
    +            ()
    +        )
    +        assert await c.execute(
    +            CommandRequest(b"LRANGE", (b"l", b"-1", b"-1"))
    +        ) == Items((Bytes(b"c"),))
    +        assert await c.execute(
    +            CommandRequest(b"LRANGE", (b"l", b"0", b"-99"))
    +        ) == Items(())
    ```

**What this test locks**

It locks LPUSH/RPUSH order, LPOP/RPOP direction, inclusive negative ranges, wrong-type safety, missing pops, and last-element removal.

**How it constructs the counterexample**

It pushes the same values from both ends and probes ranges such as `-99..99`, `2..1`, and `-1..-1`.

**Key test statement**

```python
assert await c.execute(CommandRequest(b"TYPE", (b"q",))) == Bytes(b"none")
```

**What a failure means**

Direction, boundary normalization, or empty-container deletion differs from the public List contract.

### Basic concepts

Both planners use copy-on-plan: clone the current container, calculate the reply and final frozen value, then return an operation. Wrong type returns `WRONGTYPE` without operations. Removing the last member of a collection produces `DeleteKey`, not an empty stored container.

### Why this mechanism is necessary

Mutable Python dictionaries and deques are convenient live representations but unsafe planning workspaces. Copying keeps validation side-effect free and preserves the same executor commit protocol used by Strings.

### Runtime mental model

The router selects a command-family planner. That planner performs logical lookup, copies the container, applies field or directional rules, freezes a replacement or proposes deletion, and returns a reply. The executor remains unaware of collection details.

### Mechanism blocks

#### Hash field planning

Copy one field map, compute exact added/removed counts, preserve TTL, and delete the key when its last field disappears.

??? note "File diff: src/miniredis/core/hash_planner.py"
    ```diff
    diff --git a/src/miniredis/core/hash_planner.py b/src/miniredis/core/hash_planner.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..479755793f0246af9143ec8f7faf8edfabacc052
    --- /dev/null
    +++ b/src/miniredis/core/hash_planner.py
    @@ -0,0 +1,112 @@
    +from miniredis.commands import model as cmd
    +from miniredis.commands.parser import (
    +    INT64_MAX,
    +    INT64_MIN,
    +    CommandParseError,
    +    parse_int64,
    +)
    +from miniredis.core.commit import DeleteKey, DeleteReason
    +from miniredis.core.database import Database
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.planning import WRONGTYPE, lookup, make_put
    +from miniredis.core.reply import Bytes, Failure, Items, Number
    +from miniredis.core.values import HashValue
    +
    +
    +def _integer_failure() -> ExecutionPlan:
    +    return ExecutionPlan(Failure("ERR", "value is not an integer or out of range"))
    +
    +
    +def plan_hash(
    +    command: cmd.Command,
    +    database: Database,
    +    now_ms: int,
    +) -> ExecutionPlan | None:
    +    match command:
    +        case cmd.HashSet(key, pairs):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is not None and not isinstance(previous.value, HashValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = {} if previous is None else dict(previous.value.items)
    +            added = 0
    +            for field, value in pairs:
    +                if field not in items:
    +                    added += 1
    +                items[field] = value
    +            put = make_put(
    +                key,
    +                HashValue(items),
    +                previous,
    +                None if previous is None else previous.expire_at_ms,
    +            )
    +            return ExecutionPlan(Number(added), expired + (put,))
    +        case cmd.HashGet(key, field):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Bytes(None), expired)
    +            if not isinstance(entry.value, HashValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            return ExecutionPlan(
    +                Bytes(entry.value.items.get(field)),
    +                expired,
    +                (key,),
    +            )
    +        case cmd.HashDelete(key, fields):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is None:
    +                return ExecutionPlan(Number(0), expired)
    +            if not isinstance(previous.value, HashValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = dict(previous.value.items)
    +            removed = 0
    +            for field in dict.fromkeys(fields):
    +                if field in items:
    +                    removed += 1
    +                    del items[field]
    +            if removed == 0:
    +                return ExecutionPlan(Number(0), (), (key,))
    +            if not items:
    +                operation = DeleteKey(key, DeleteReason.CLIENT)
    +            else:
    +                operation = make_put(
    +                    key,
    +                    HashValue(items),
    +                    previous,
    +                    previous.expire_at_ms,
    +                )
    +            return ExecutionPlan(Number(removed), expired + (operation,))
    +        case cmd.HashGetAll(key):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Items(()), expired)
    +            if not isinstance(entry.value, HashValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            values = tuple(
    +                item
    +                for field, value in sorted(entry.value.items.items())
    +                for item in (Bytes(field), Bytes(value))
    +            )
    +            return ExecutionPlan(Items(values), expired, (key,))
    +        case cmd.HashIncrement(key, field, amount):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is not None and not isinstance(previous.value, HashValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = {} if previous is None else dict(previous.value.items)
    +            raw_old = items.get(field, b"0")
    +            try:
    +                old_value = parse_int64(raw_old)
    +            except CommandParseError:
    +                return _integer_failure()
    +            new_value = old_value + amount
    +            if not INT64_MIN <= new_value <= INT64_MAX:
    +                return _integer_failure()
    +            items[field] = str(new_value).encode("ascii")
    +            put = make_put(
    +                key,
    +                HashValue(items),
    +                previous,
    +                None if previous is None else previous.expire_at_ms,
    +            )
    +            return ExecutionPlan(Number(new_value), expired + (put,))
    +        case _:
    +            return None
    ```

**What it is and why it appears**

Hash planning owns field-level counts, integer updates, sorted reply materialization, and empty-key removal.

**Runtime role**

It copies `items`, changes the copy, and produces one replacement or delete operation.

**Key code**

```python
items = {} if previous is None else dict(previous.value.items)
```

**Statement understanding**

The copy prevents duplicate-field validation or failed integer conversion from editing the live Hash.

#### Deque order and inclusive ranges

Make left/right push-pop direction and Redis-style inclusive negative ranges explicit over copied deques.

??? note "File diff: src/miniredis/core/list_planner.py"
    ```diff
    diff --git a/src/miniredis/core/list_planner.py b/src/miniredis/core/list_planner.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..ead234743a406735df6e8ca5750510bd7a53a6aa
    --- /dev/null
    +++ b/src/miniredis/core/list_planner.py
    @@ -0,0 +1,83 @@
    +from collections import deque
    +
    +from miniredis.commands import model as cmd
    +from miniredis.core.commit import DeleteKey, DeleteReason
    +from miniredis.core.database import Database
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.planning import WRONGTYPE, lookup, make_put
    +from miniredis.core.reply import Bytes, Items, Number
    +from miniredis.core.values import ListValue
    +
    +
    +def inclusive_slice(length: int, start: int, stop: int) -> tuple[int, int]:
    +    if start < 0:
    +        start += length
    +    if stop < 0:
    +        stop += length
    +    start = max(start, 0)
    +    stop = min(stop, length - 1)
    +    if start >= length or stop < 0 or start > stop:
    +        return 0, 0
    +    return start, stop + 1
    +
    +
    +def plan_list(
    +    command: cmd.Command,
    +    database: Database,
    +    now_ms: int,
    +) -> ExecutionPlan | None:
    +    match command:
    +        case cmd.ListPush(key, values, left):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is not None and not isinstance(previous.value, ListValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = deque() if previous is None else deque(previous.value.items)
    +            if left:
    +                for value in values:
    +                    items.appendleft(value)
    +            else:
    +                items.extend(values)
    +            put = make_put(
    +                key,
    +                ListValue(items),
    +                previous,
    +                None if previous is None else previous.expire_at_ms,
    +            )
    +            return ExecutionPlan(Number(len(items)), expired + (put,))
    +        case cmd.ListPop(key, left):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is None:
    +                return ExecutionPlan(Bytes(None), expired)
    +            if not isinstance(previous.value, ListValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = deque(previous.value.items)
    +            value = items.popleft() if left else items.pop()
    +            if items:
    +                operation = make_put(
    +                    key,
    +                    ListValue(items),
    +                    previous,
    +                    previous.expire_at_ms,
    +                )
    +            else:
    +                operation = DeleteKey(key, DeleteReason.CLIENT)
    +            return ExecutionPlan(Bytes(value), expired + (operation,))
    +        case cmd.ListRange(key, start, stop):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Items(()), expired)
    +            if not isinstance(entry.value, ListValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            begin, end = inclusive_slice(
    +                len(entry.value.items),
    +                start,
    +                stop,
    +            )
    +            selected = tuple(entry.value.items)[begin:end]
    +            return ExecutionPlan(
    +                Items(tuple(Bytes(value) for value in selected)),
    +                expired,
    +                (key,),
    +            )
    +        case _:
    +            return None
    ```

**What it is and why it appears**

List planning defines directional deque changes and converts Redis inclusive ranges into Python half-open slices.

**Runtime role**

It copies the deque, changes one end, and deletes the key when no items remain.

**Key code**

```python
return start, stop + 1
```

**Statement understanding**

The `+1` is the semantic bridge from an inclusive public stop index to an exclusive Python slice end.

#### Command-family routing

Route Hash and List typed commands without moving their semantics into the executor.

??? note "File diff: src/miniredis/core/planner.py"
    ```diff
    diff --git a/src/miniredis/core/planner.py b/src/miniredis/core/planner.py
    index 7672f517999802f4fda3327fb30322d356eb2ab1..d8de65d98cbbc4f3382ab5f6700140f9f7262eab 100644
    --- a/src/miniredis/core/planner.py
    +++ b/src/miniredis/core/planner.py
    @@ -2,6 +2,8 @@ from miniredis.commands.model import Command
     from miniredis.config import MiniRedisConfig
     from miniredis.core.database import Database
     from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.hash_planner import plan_hash
    +from miniredis.core.list_planner import plan_list
     from miniredis.core.planning import plan_general_and_strings
     from miniredis.core.reply import Failure

    @@ -17,6 +19,10 @@ class CommandPlanner:
             now_ms: int,
         ) -> ExecutionPlan:
             plan = plan_general_and_strings(command, database, now_ms)
    +        if plan is None:
    +            plan = plan_hash(command, database, now_ms)
    +        if plan is None:
    +            plan = plan_list(command, database, now_ms)
             if plan is not None:
                 return plan
             return ExecutionPlan(Failure("ERR", "unknown command"))
    ```

**What it is and why it appears**

The router now tries general/String, Hash, then List planners in a stable chain.

**Runtime role**

It returns the first plan that owns the typed command.

**Key code**

```python
if plan is None:
    plan = plan_hash(command, database, now_ms)
```

**Statement understanding**

`None` means “not my command family,” while a `Failure` inside an `ExecutionPlan` is an owned semantic result.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/05-hashes-and-lists/tests.txt)`. It proves collection-specific replies and mutation boundaries through the shared executor.

### Durable takeaways

Copy before planning, distinguish “unhandled” from “handled failure,” translate public index conventions explicitly, and remove keys when their final collection member disappears.

### Explain it in your own words

Hash and List add different data rules without adding new ownership rules. Each planner works on a copy, returns a frozen final operation, and leaves the executor to publish it in the same ordered commit path.

### Textbook

[Chapter 3](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/03-data-types.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/be7969d...eb41b6e)

After finishing, run `python -m journey.tools.build_journey check 5` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/05-hashes-and-lists/stage.patch)

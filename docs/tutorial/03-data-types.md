# Chapter 3: Data Types and the Command Surface

> **Language:** English | [简体中文](../zh/tutorial/03-data-types.md)

## Learning objectives

By the end of this chapter, you will be able to:

- describe MiniRedis’s string, hash, list, set, and sorted-set value models;
- locate the parser model and planner responsible for a command;
- predict missing-key, empty-key, ordering, and `WRONGTYPE` behavior;
- explain how immutable stored values cross the commit boundary;
- design and test an `APPEND` command without changing unrelated layers.

## One keyspace, five value variants

MiniRedis stores every user key in one `Database.entries` dictionary, but an
entry’s value belongs to a closed union. `src/miniredis/core/values.py`
defines five mutable planning-time wrappers:

```python
@dataclass(slots=True)
class StringValue:
    data: bytes

@dataclass(slots=True)
class HashValue:
    items: dict[bytes, bytes]
```

The other variants are `ListValue(deque[bytes])`, `SetValue(set[bytes])`, and
`ZSetValue(dict[bytes, float])`. The wrappers make type checks explicit while
using familiar Python containers. `RedisValue` is their type union.

The live value is not written directly into a commit. `freeze_value` in
`src/miniredis/core/database.py` converts it into one of the frozen types in
`src/miniredis/core/commit.py`: `StoredString`, `StoredHash`, `StoredList`,
`StoredSet`, or `StoredZSet`. Hashes, sets, and sorted sets are sorted while
freezing so persistence and snapshots receive deterministic tuples. On apply,
`thaw_value` reconstructs fresh mutable containers.

This two-form model reinforces the boundary from Chapter 2. Planners can copy
and modify a Python container locally. A `PutEntry` carries an immutable
snapshot of the proposed value. `Database.apply_batch` then owns publication.
No planner passes a live dictionary reference that later code could mutate
behind the commit sequence.

Every `Entry` also carries an absolute expiry, mutation version, access tick,
logical size, frequency, and last LFU decay time. Those fields are shared across
types. Chapters 4 and 5 use them without changing the five value APIs.

## Parsing and routing the command surface

Raw names are recognized in `src/miniredis/commands/parser.py`,
`parse_request`. Arity, integer syntax, score syntax, and supported options are
validated there. The result is a typed data class from
`src/miniredis/commands/model.py`.

`CommandPlanner.plan` in `src/miniredis/core/planner.py` tries planners in this
order:

1. `plan_general_and_strings`;
2. `plan_hash`;
3. `plan_list`;
4. `plan_set`;
5. `plan_zset`;
6. `plan_ttl`.

A planner returns `None` when a command belongs elsewhere. Once one returns an
`ExecutionPlan`, the router applies memory enforcement. This structure keeps a
small command implementation near its value-specific invariants while preserving
one cross-cutting maxmemory decision.

`lookup` in `src/miniredis/core/planning.py` is the common read primitive. It
returns `(None, ())` for a missing key, `(None, (expiry_delete(key),))` for a
logically expired key, or the live `Entry`. Each planner must decide how absence
maps to its command reply.

## Strings: bytes with strict integer operations

Strings are raw bytes, not Python text. `SET`, `GET`, `MGET`, `MSET`, `INCR`,
`DECR`, `INCRBY`, plus the teaching atomics `COMPAREDEL` and `CHECKDECR` are
handled by `plan_general_and_strings` in
`src/miniredis/core/planning.py`.

`GET` on a missing key returns `Bytes(None)`. `INCR` on a missing key starts
from zero. For an existing string, integer operations call `parse_int64`, whose
grammar rejects non-canonical forms such as `01` and `-0`, and whose result must
remain in signed 64-bit range. A failure returns an error plan with no write.

Ordinary `SET` replaces the value and clears an old TTL unless an `EX` or `PX`
option supplies a new deadline. In-place operations such as `INCR` preserve the
existing absolute expiry. Conditional `NX` and `XX` are parsed into
`SetString.only_if`, so the planner does not reinterpret raw options.

## Hashes: field-value maps

`plan_hash` in `src/miniredis/core/hash_planner.py` implements `HSET`, `HGET`,
`HDEL`, `HGETALL`, and `HINCRBY`. A missing hash is created by write commands.
`HSET` counts newly added fields, not assignments. Duplicate fields in one
request end with the final value.

The planner copies `previous.value.items` before changing it. Removing the last
field produces `DeleteKey` rather than storing an empty hash. `HGETALL`
materializes alternating field/value replies in sorted field order. The behavior
matrix calls the public order unspecified, so callers must not build a
compatibility promise from that deterministic implementation detail.

`HINCRBY` applies the same strict int64 rules as string increment. A malformed
field value or overflow returns an error without committing the copied map.

## Lists: ordered deques

Lists use `collections.deque` and are planned in
`src/miniredis/core/list_planner.py`, `plan_list`. `LPUSH` repeatedly calls
`appendleft`, so `LPUSH l a b c` yields `c, b, a`, matching the left-push
mental model. `RPUSH` extends the right side. `LPOP` and `RPOP` remove the key
when the last item leaves.

`LRANGE` uses `inclusive_slice` to translate Redis-style inclusive, possibly
negative bounds into a Python half-open slice. Out-of-range bounds produce an
empty `Items`, not an error.

Blocking pops are only partly in this planner. `CommandPlanner.plan_blocking_pop_now`
performs an immediate pop if a listed key is ready. If none is ready,
`CommandExecutor._execute` registers a waiter. This split keeps ordinary list
value logic separate from time and session ownership; Chapter 9 follows the
waiting path.

## Sets: uniqueness and unordered semantics

`plan_set` in `src/miniredis/core/set_planner.py` implements `SADD`, `SREM`,
`SISMEMBER`, `SMEMBERS`, and `SINTER`. The live model is a Python set. `SADD`
counts genuinely new members even if an argument repeats. `SREM` deduplicates
requested members and deletes the key when the set becomes empty.

`SMEMBERS` and `SINTER` sort bytes before constructing their `Items` reply, which
makes tests deterministic. Redis set result order is not a semantic guarantee,
and the behavior matrix explicitly calls result order unspecified. Treat
sorting here as observability scaffolding, not a portable ordering contract.

`SINTER` illustrates why planning must inspect all relevant keys. Even if one
input is missing and the mathematical intersection is already empty, a later
key with the wrong type still returns `WRONGTYPE`. It does not silently stop at
the missing key.

## Sorted sets: scores plus a derived order

A `ZSetValue` stores `member -> float` in a dictionary.
`src/miniredis/core/zset_planner.py`, `plan_zset`, derives ordering with
`sorted(scores.items(), key=lambda item: (item[1], item[0]))`. Equal scores are
therefore ordered by member bytes.

The supported surface is deliberately small: `ZADD`, `ZREM`, `ZSCORE`,
`ZRANK`, `ZRANGE`, and `ZRANGEBYSCORE`. Score bounds can be inclusive or use
the `(` prefix for exclusivity. NaN is rejected by the parser; infinities are
accepted as range bounds. Removing the last member deletes the key.

There is a documented Redis difference: `_format_score` uses Python
`repr(float)`. MiniRedis may return `b"1.0"` where Redis commonly formats the
same score as `b"1"`. The Sorted Set row of
[`docs/behavior-matrix.md`](../behavior-matrix.md) records this exact boundary.

## Type safety and `WRONGTYPE`

All five type planners use the shared `WRONGTYPE` value from
`src/miniredis/core/planning.py`. A command first distinguishes absence from a
live entry, then checks `isinstance` against the expected wrapper. A wrong-type
plan contains no operations. Consequently, `_apply_plan` publishes the failure
without advancing the commit sequence.

Not every multi-key string read uses `WRONGTYPE`: `MGET` returns nil for keys
that are missing or not strings, matching its documented subset. This is why the
type rule belongs to each command planner rather than a global pre-check.

No-op writes deserve similar care. `HDEL` or `SREM` with no matching member
returns zero and touches the live key for access metadata, but does not create a
commit. A successful value change creates one `PutEntry` or final `DeleteKey`.

## How to extend the command surface safely

Adding a command is a vertical change, not just a new parser branch. First define
a frozen typed command so raw syntax cannot leak into planning. Add it to the
`Command` union and classify whether it mutates dataset state. Parse exact arity
and options before mailbox execution. Then place semantics in the planner that
already owns the value type, returning a reply, operations, and touches rather
than mutating the database.

Tests should cover four boundaries independently: normal state change, missing
key behavior, wrong type or invalid value, and metadata such as TTL preservation.
For failures, capture `debug_commit_seq` before the command and prove it does not
advance. For success, inspect public replies first; record batches only when the
propagation shape itself is under test.

This checklist prevents a common architecture drift: implementing a Direct-only
helper that bypasses parsing or the executor. If the new command enters through
`CommandRequest` and the shared typed model, both Direct and RESP2 adapters can
reuse it. The exercise below applies this method to `APPEND`.

## Compared with real Redis

Redis implements these command families in production files including
`t_string.c`, `t_hash.c`, `t_list.c`, `t_set.c`, and `t_zset.c`. It uses
specialized internal encodings selected for size and workload: strings,
listpacks, hash tables, intsets, skip lists, and other representations have
changed across versions.

MiniRedis intentionally replaces that encoding layer with Python containers.
The lesson preserved is the command-level type contract, mutation atomicity, and
ordered propagation—not Redis memory density or algorithmic performance.
MiniRedis also stages database copies, while Redis normally updates its live
dictionaries and metadata in place.

Consult the String, Hash, List, Set, and Sorted Set rows in
[`docs/behavior-matrix.md`](../behavior-matrix.md). Each row names the supported
subset, executable test evidence, and differences. The architecture mapping
labels Python containers as an intentional simplification.

## Hands-on experiment: exercise all five types

Run:

```bash
uv run python - <<'PY'
import asyncio
from miniredis import CommandRequest, MiniRedis

async def main():
    async with MiniRedis.open() as server:
        c = server.direct_client()
        commands = [
            CommandRequest(b"SET", (b"s", b"hello")),
            CommandRequest(b"HSET", (b"h", b"language", b"Python")),
            CommandRequest(b"RPUSH", (b"l", b"one", b"two")),
            CommandRequest(b"SADD", (b"set", b"red", b"blue")),
            CommandRequest(
                b"ZADD", (b"z", b"2", b"second", b"1", b"first")
            ),
            CommandRequest(b"ZRANGE", (b"z", b"0", b"-1")),
            CommandRequest(b"HGET", (b"s", b"field")),
        ]
        for command in commands:
            print(command.name.decode(), "=>", await c.execute(command))
        await c.close()

asyncio.run(main())
PY
```

Measured output:

```text
SET => Ok(message=b'OK')
HSET => Number(value=1)
RPUSH => Number(value=2)
SADD => Number(value=2)
ZADD => Number(value=2)
ZRANGE => Items(values=(Bytes(value=b'first'), Bytes(value=b'second')))
HGET => Failure(code='WRONGTYPE', message='operation against a key holding the wrong kind of value')
```

The sorted set is ordered by score despite insertion order. The final `HGET`
targets a string and returns a domain error; the Python script continues because
command failures are replies.

## Exercises

### 1. Understanding: live versus stored values

Why does a planner freeze a copied `HashValue` into `StoredHash` instead of
placing the mutable dictionary directly in `PutEntry`?

??? note "Reference answer"
    The commit must be an immutable snapshot of the proposed state. Sharing the
    mutable planning dictionary would allow later mutation to change an already
    sequenced batch, breaking durability, replication, and recovery agreement.

### 2. Understanding: empty collections

What happens when `HDEL`, `LPOP`, `SREM`, or `ZREM` removes the final element?

??? note "Reference answer"
    The planner emits `DeleteKey` and the key becomes absent. MiniRedis does not
    retain empty hash, list, set, or sorted-set entries for these commands.

### 3. Hands-on: implement String `APPEND`

Add `APPEND key value` across the typed model, parser, string planner, mutating
classification, and tests. It must create a missing string, preserve an existing
TTL, return the new byte length, and return `WRONGTYPE` without committing for a
non-string. Do not add a separate planner or mutate `Database` directly.
Acceptance:

```bash
uv run pytest tests/contract/test_strings.py -q
```

Add four focused cases; this file should report four more passing tests than
before your change. Then run `uv run pytest -q` to check the full suite.

??? note "Reference answer"
    The key changes are:

    ```diff
    # src/miniredis/commands/model.py
    +@dataclass(frozen=True, slots=True)
    +class Append:
    +    key: bytes
    +    value: bytes

    # src/miniredis/commands/parser.py, parse_request
    +case b"APPEND":
    +    _require_arity(name, args, 2)
    +    return Append(args[0], args[1])

    # src/miniredis/core/planning.py, plan_general_and_strings
    +case cmd.Append(key, suffix):
    +    previous, expired = lookup(database, key, now_ms)
    +    if previous is not None and not isinstance(previous.value, StringValue):
    +        return ExecutionPlan(WRONGTYPE)
    +    old = b"" if previous is None else previous.value.data
    +    value = StringValue(old + suffix)
    +    expiry = None if previous is None else previous.expire_at_ms
    +    return ExecutionPlan(
    +        Number(len(value.data)),
    +        expired + (make_put(key, value, previous, expiry),),
    +    )
    ```

    Also add `Append` to the `Command` union, `_DATASET_MUTATING_TYPES`, and
    parser imports. Tests should cover missing, existing, TTL preservation, and
    wrong type with unchanged `debug_commit_seq`.

## Summary

MiniRedis models five Redis value families with simple mutable Python containers
for planning and frozen stored variants for commit propagation. Per-type planners
own command-specific absence, ordering, integer, and type rules; the shared
executor owns publication. With values understood, Chapter 4 adds time: an entry
can remain physically present while already being logically absent.

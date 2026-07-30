# Chapter 5: Memory and Eviction

> **Language:** English | [简体中文](../zh/tutorial/05-eviction.md)

## Learning objectives

By the end of this chapter, you will be able to:

- calculate MiniRedis logical entry sizes and explain why they are not RSS;
- locate the exact point where maxmemory enforcement changes a command plan;
- contrast `noeviction`, exact `allkeys-lru`, and deterministic
  `allkeys-lfu`;
- explain LFU decay, candidate exclusion, and deterministic tie-breaking;
- verify that a victim deletion and triggering write share one commit batch.

## A deterministic memory model

Production memory is difficult to make predictable. An object’s cost includes
allocator headers, fragmentation, container capacity, shared structures, and
implementation details. Python adds another runtime and garbage collector.
MiniRedis therefore does not compare `maxmemory` with RSS or Python’s allocator.

`src/miniredis/core/database.py`, `logical_value_size`, assigns a simple
deterministic cost to each value. A string costs `16 + len(data)`. Hash fields,
list items, set members, and sorted-set members have fixed teaching overheads
plus byte lengths. `logical_entry_size` adds:

- 64 bytes of entry overhead;
- the key length;
- the logical value cost;
- 16 bytes when an expiration deadline exists.

Every `Entry` stores its own `logical_size`, and `Database.logical_usage` is the
sum across live physical entries. `Database.apply_batch` recomputes the sum from
staged state and checks that no entry size is non-positive.

This model is intentionally artificial but useful. Given the same keys, values,
and TTLs, every run reaches the same budget decision. Tests can select a
meaningful threshold without depending on platform allocator behavior. The
Eviction row in [`docs/behavior-matrix.md`](../behavior-matrix.md) explicitly
calls the budget “logical byte” accounting.

Never translate “260 logical bytes” into “260 bytes of RAM.” The number is a
policy input inside this teaching runtime, not a capacity estimate.

## Enforcement changes the plan, not live state

`CommandPlanner.plan` in `src/miniredis/core/planner.py` first obtains a normal
type-specific `ExecutionPlan`, then calls `enforce_memory` from
`src/miniredis/core/eviction.py`. Eviction is therefore a cross-cutting planning
step. It can:

- leave the plan unchanged;
- add expired-key and victim deletions;
- replace the plan with `Failure("OOM", "command exceeds maxmemory")`.

It does not call `Database.apply_batch`. The executor still owns durability,
atomic apply, replication, and reply publication.

`enforce_memory` returns immediately when no `maxmemory` is configured or the
plan has no operations. It then asks whether operations actually write client
data. Pure expiry cleanup does not recursively invoke eviction. A client delete
is allowed even above budget because it can reduce usage.

Before selecting a live victim, `_expired_operations` proposes deletion of every
logically expired physical entry. `dedupe_operations` combines those with the
original plan. Thus stale space is reclaimed before live data is sacrificed.

The function computes two values with `projected_usage`: a baseline after
expired cleanup, and usage after the complete proposed write. If final usage is
within budget, or the command does not grow usage beyond that cleaned baseline,
the plan proceeds. This lets reducing writes succeed even when the starting
database is already over a newly imposed budget.

## Atomic OOM and oversized targets

Each target `PutEntry` is checked individually. If that one entry’s logical size
exceeds the entire budget, `enforce_memory` returns OOM immediately. It does not
evict unrelated keys in a futile attempt to fit an impossible target.

Under `noeviction`, any remaining growth above budget becomes OOM. Because the
result is an `ExecutionPlan` with only a failure reply, `_apply_plan` has no
prepared commit. The commit sequence, keyspace, persistence history, and replica
history remain unchanged.

This is stronger than “we detected an error after writing.” Planning makes the
rejection atomic by construction. The test
`tests/contract/test_eviction.py::test_oversized_target_does_not_evict_unrelated_key`
verifies that an existing key survives an oversized write. The adjacent
`test_noeviction_allows_delete_but_rejects_growth_atomically` verifies that
deletion still works.

The target keys of a write are excluded from victim candidates. Without that
rule, a planner might “succeed” by immediately evicting the value it was asked
to store. Keys already scheduled for deletion are also excluded.

## Exact allkeys-LRU

LRU means least recently used. MiniRedis tracks a global
`Database.access_tick`. `Database.apply_batch` increments it for every
client-triggered `PutEntry`. `Database.touch_if_live` increments it for
successful read touches. Each entry stores `last_access_tick`.

Planners identify touches separately from mutations. For example, a live `GET`
returns `touch_keys=(key,)` and no write. After any required commit,
`CommandExecutor._apply_plan` deduplicates those keys and calls
`Database.touch_if_live`. Missing or expired keys are not touched.

For `allkeys-lru`, `enforce_memory` sorts eligible candidates by
`(last_access_tick, key)`. The smallest tick is coldest; binary key order makes
a tie deterministic. It adds `DeleteKey(key, DeleteReason.EVICTED)` victims one
at a time until projected usage fits.

This LRU is **exact** over all eligible keys. Real Redis uses bounded candidate
sampling to avoid a full-keyspace sort on the write path. MiniRedis pays the
extra cost so a reader can derive the victim from visible metadata and tests
remain repeatable.

Victim deletion and the triggering `PutEntry` remain in one plan. When committed,
they share one sequence. There is no moment at a command boundary where a client
can observe the victim gone but the requested write absent.

## Deterministic decaying LFU

LFU means least frequently used, but a lifetime counter would make formerly hot
keys immortal. MiniRedis combines a frequency with decay. Every entry contains
`frequency` and `last_frequency_decay_ms`.

`project_frequency` in `src/miniredis/core/frequency.py` computes complete decay
windows:

```python
windows = (now_ms - last_decay_ms) // interval_ms
projected = (
    0 if windows >= frequency.bit_length()
    else frequency >> windows
)
```

Each elapsed window halves the counter with a right shift. The returned decay
timestamp advances by whole windows. Reads and client writes materialize a
projection and then increment frequency in `Database.touch_if_live` or
`Database.apply_batch`.

Eviction planning is different: `_lfu_candidates` projects every eligible
frequency for comparison but does not write the projected values back to
survivors. This keeps a failed or unrelated plan side-effect free. The candidate
tuple is `(effective_frequency, last_access_tick, key)`. MiniRedis first evicts
the least frequent, then the older access, then the lexicographically smaller
binary key.

Redis uses an approximate logarithmic counter and probabilistic increments, plus
sampling. MiniRedis uses exact increment and deterministic halving. The policy
teaches recency-versus-frequency trade-offs, but it is not Redis’s byte-for-byte
metadata algorithm.

Decay changes outcomes. An old key read eight times starts with frequency eight,
but after three complete windows projects to one. A recently created key can
then tie or exceed it, allowing yesterday’s hot item to cool. The injected clock
lets tests advance those windows without waiting.

## Touch semantics and operational metadata

Successful client reads and writes materialize exactly one touch in the normal
path. No-op commands may still touch an existing key, because observing it is an
access even when no value changes. Failed wrong-type commands generally do not
touch the mismatched value.

Eviction planning itself reads projected metadata without becoming an access.
Otherwise inspecting candidates would make them all “recent” and perturb the
policy. Similarly, active-expiration commits use
`CommitTrigger.ACTIVE_EXPIRE`, so `Database.apply_batch` receives
`track_access=False`.

LRU and LFU metadata is operational rather than durable semantic state.
`Database.install_snapshot` and recovery initialize access ticks and frequencies
to neutral values. A full replica sync also installs a snapshot with neutral
metadata. The Eviction row in the behavior matrix documents policy behavior;
it does not promise that victim history survives restart.

## Complexity and teaching trade-offs

Both policies perform work a production hot path would avoid. `projected_usage`
builds a size dictionary and sums it. LRU sorts all candidates. LFU projects and
sorts all candidates. `Database.apply_batch` later copies the key table and
recomputes usage again.

These choices make invariants inspectable:

- projected decisions do not mutate survivors;
- a failed OOM has no rollback path because nothing was applied;
- tie-breaking is stable;
- logical usage can be recomputed and checked;
- victim and trigger are one propagation unit.

They also mean MiniRedis is not a performance benchmark for eviction. A
production design would use allocator-aware accounting, compact metadata,
bounded sampling, and carefully budgeted work.

When debugging an unexpected victim, capture four inputs before changing policy:
the logical size of each entry, current time, access tick, and stored
frequency/decay timestamp. LRU and LFU are deterministic functions of those
values. Inspecting them turns “the cache chose badly” into a reproducible ordering
question and avoids confusing a TTL cleanup with an eviction.

## Compared with real Redis

Redis enforces maxmemory in `evict.c` and exposes policies through configuration
such as `maxmemory-policy`. Its available policies and modern implementation
surface are broader than MiniRedis’s three choices. Redis’s LRU is approximated
through sampling, and its LFU uses a probabilistic logarithmic counter with
decay configuration.

MiniRedis preserves the decision point: memory must be made available before the
triggering write is committed, and failure must leave the keyspace unchanged.
It simplifies accounting to logical bytes, supports only `noeviction`,
`allkeys-lru`, and `allkeys-lfu`, and makes victim selection exact and
deterministic.

The architecture mapping row `core/eviction.py` points to Redis `evict.c` and
labels the implementation an intentional simplification. The Eviction row in
[`docs/behavior-matrix.md`](../behavior-matrix.md) cites
`tests/contract/test_eviction.py::test_lfu_planning_projects_without_materializing_survivor_decay`
as direct evidence.

## Hands-on experiment: observe LFU through public replies

Run:

```bash
uv run python - <<'PY'
import asyncio
from miniredis import CommandRequest, MiniRedis

async def main():
    async with MiniRedis.open(
        maxmemory=260,
        eviction_policy="allkeys-lfu",
    ) as server:
        c = server.direct_client()
        print("hot:", await c.execute(
            CommandRequest(b"SET", (b"hot", b"x"))
        ))
        print("cold:", await c.execute(
            CommandRequest(b"SET", (b"cold", b"x"))
        ))
        for _ in range(4):
            await c.execute(CommandRequest(b"GET", (b"hot",)))
        print("new:", await c.execute(
            CommandRequest(b"SET", (b"new", b"x" * 60))
        ))
        for key in (b"cold", b"hot", b"new"):
            print(key.decode() + ":", await c.execute(
                CommandRequest(b"GET", (key,))
            ))
        print("evictions:", server.debug_stats().evicted_key_count)
        await c.close()

asyncio.run(main())
PY
```

Measured output:

```text
hot: Ok(message=b'OK')
cold: Ok(message=b'OK')
new: Ok(message=b'OK')
cold: Bytes(value=None)
hot: Bytes(value=b'x')
new: Bytes(value=b'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
evictions: 1
```

Both initial writes give their keys one frequency touch. Four extra `GET`
operations make `hot` more frequent, so `cold` is the unique victim when the
larger `new` value exceeds the budget.

## Exercises

### 1. Understanding: logical memory

Why can MiniRedis produce deterministic eviction tests while its maxmemory value
cannot be used to size a production host?

??? note "Reference answer"
    It sums a fixed formula over keys and values, independent of Python and OS
    allocation. That repeatability omits allocator overhead, fragmentation,
    container capacity, process memory, and other production costs.

### 2. Understanding: LFU planning

Why does `_lfu_candidates` project decay without writing the projected frequency
back to every surviving entry?

??? note "Reference answer"
    Planning must be side-effect free. Materializing candidate decay would alter
    operational state even if the write later failed with OOM. A survivor
    materializes decay only on an actual client touch.

### 3. Hands-on: prove LRU atomicity

Add a focused test to `tests/contract/test_eviction.py`. Create `cold` and
`hot`, touch `hot`, then write a larger `new` value under exact LRU. Assert that
the final recorded batch contains both the `cold` eviction and `new` put, and
that the commit sequence advanced exactly once. Acceptance:

```bash
uv run pytest tests/contract/test_eviction.py -q
```

The file must report one more passing test than its baseline.

??? note "Reference answer"
    Open with `debug_record_applied_batches=True`, capture `before`, then inspect
    the last batch:

    ```diff
    +assert runtime.debug_commit_seq == before + 1
    +batch = runtime.debug_applied_batches()[-1]
    +assert any(
    +    isinstance(op, DeleteKey)
    +    and op.key == b"cold"
    +    and op.reason is DeleteReason.EVICTED
    +    for op in batch.operations
    +)
    +assert any(
    +    isinstance(op, PutEntry) and op.key == b"new"
    +    for op in batch.operations
    +)
    ```

    Use `maxmemory=260`, one-byte initial values, and a 60-byte new value, as in
    the measured experiment.

### 4. Hands-on: add a tie-break test

Add one test showing that equally frequent, equally recent LFU candidates are
ordered by binary key. Build the state with `Database.apply_batch` using
`track_access=False`, then call `enforce_memory` through a normal write.
Acceptance:

```bash
uv run pytest tests/contract/test_eviction.py -q
```

Together with Exercise 3, the file should report two more passing tests than its
baseline.

??? note "Reference answer"
    Insert `b"a"` and `b"b"` with neutral frequency/access metadata, then trigger
    one required eviction. The important assertion is:

    ```diff
    +assert await client.execute(
    +    CommandRequest(b"GET", (b"a",))
    +) == Bytes(None)
    +assert await client.execute(
    +    CommandRequest(b"GET", (b"b",))
    +) == Bytes(b"x")
    ```

    Keep this as a policy test: do not change the tie-break implementation.

## Summary

MiniRedis enforces a deterministic logical budget during planning. It reclaims
expired space first, rejects impossible or disallowed growth atomically, and can
add exact LRU or deterministic decaying-LFU victims to the triggering write’s
single commit batch. The next chapter follows that batch beyond memory and into
the append-only persistence barrier.

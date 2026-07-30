# Chapter 4: Expiration

> **Language:** English | [简体中文](../zh/tutorial/04-expiration.md)

## Learning objectives

By the end of this chapter, you will be able to:

- explain the difference between physical presence and logical expiration;
- trace lazy expiration from a read lookup into an expiry commit;
- describe bounded, cursor-based active expiration;
- explain why a replica hides expired keys without deleting them locally;
- verify expiration deterministically with the repository’s injected clock.

## Time is stored as an absolute deadline

An expiring MiniRedis entry stores `expire_at_ms: int | None` in
`src/miniredis/core/database.py`, `Entry`. `None` means persistent. A number is
an absolute millisecond deadline according to the runtime’s injected `Clock`.
`is_expired` in `src/miniredis/core/expiration.py` is deliberately small:

```python
def is_expired(entry: Entry, now_ms: int) -> bool:
    return (
        entry.expire_at_ms is not None
        and entry.expire_at_ms <= now_ms
    )
```

Using an absolute deadline avoids decrementing every TTL as time advances.
`SET ... EX/PX` is parsed into a relative millisecond duration by
`src/miniredis/commands/parser.py`, `_parse_set`; then
`plan_general_and_strings` adds the planner’s `now_ms`. `EXPIRE` similarly
stores `now_ms + seconds * 1_000` in `src/miniredis/core/ttl_planner.py`,
`plan_ttl`.

`TTL` and `PTTL` calculate from that absolute value. MiniRedis returns `-2` for
an absent or logically expired key and `-1` for a persistent key. Seconds use
`(remaining_ms + 500) // 1_000`, a rounded teaching behavior. Always consult
the TTL row of [`docs/behavior-matrix.md`](../behavior-matrix.md) rather than
assuming every Redis edge case is reproduced.

An ordinary `SET` replacement clears an old TTL unless the new command supplies
`EX` or `PX`. In-place mutations—`INCR`, `HINCRBY`, list pops and pushes, set
and sorted-set updates—pass the previous absolute deadline to `make_put`, so the
deadline survives the value change. `PERSIST` rewrites the entry with no
deadline. `EXPIRE key 0` plans an immediate client deletion.

## Logical absence can precede physical deletion

A database cannot afford a timer per key at large scale. More importantly, a
read must never return stale data merely because background cleanup has not
visited it. MiniRedis therefore separates logical visibility from physical
storage.

`lookup` in `src/miniredis/core/planning.py` reads `database.entries`. If the
entry is expired at `now_ms`, it returns:

```python
return None, (expiry_delete(key),)
```

The planner sees `None` and constructs the same reply it would use for a missing
key. The extra operation is a `DeleteKey` whose reason is
`DeleteReason.EXPIRED`; `expiry_delete` is defined in
`src/miniredis/core/expiration.py`.

For `GET`, this becomes `ExecutionPlan(Bytes(None), expired)`. The read’s reply
is already known, but on a primary the executor first commits the expiry delete.
`CommandExecutor._apply_plan` calls `_commit_prepared`, which sends the deletion
through the configured AOF barrier, applies it, increments
`expired_key_count`, appends it to the replication backlog, and offers it to the
replica. Only then is the nil reply published.

This is **lazy expiration**: the access that discovers staleness performs
cleanup. It is still a real propagated state change, not an untracked dictionary
pop. If the commit barrier fails, MiniRedis does not pretend that cleanup
succeeded.

There are command-specific subtleties. `MGET` treats expired or wrong-type
entries as nil but deliberately discards the returned expiry operation, so it
does not create a commit. A multi-key planner may accumulate expiry operations,
but if a later key causes `WRONGTYPE`, the error plan drops them. The test
`tests/contract/test_ttl.py::test_error_discards_pending_lazy_expiry_delete`
verifies that an unsuccessful command cannot smuggle partial cleanup into state.

## Active expiration is bounded work

Lazy expiration alone leaves never-read keys occupying physical and logical
memory. MiniRedis adds a periodic producer and a serialized consumer.

`ActiveExpireProducer` in `src/miniredis/core/expiration.py` owns scheduling.
`ActiveExpireProducer.start` schedules the next deadline. `_fire` posts an
`ActiveExpireTick` control message and schedules another tick. It never mutates
the database. This ownership keeps timer callbacks lightweight and preserves the
executor as the only state-changing owner.

`MiniRedis._start_owned` in `src/miniredis/runtime.py` creates that producer
after the executor starts, using `active_expire_interval_ms` from
`MiniRedisConfig`. The default interval is 100 ms. Shutdown calls
`ActiveExpireProducer.quiesce`, cancels its scheduled handle, and removes the
control producer before executor teardown.

The executor receives the tick in `CommandExecutor._dispatch` and calls
`CommandExecutor._active_expire_once`. That function:

1. sorts only keys that have deadlines;
2. starts after a remembered binary-key cursor, wrapping at the end;
3. takes at most `active_expire_sample_size` candidates;
4. emits expiry deletes only for candidates expired at the tick time;
5. commits all selected deletes in one `CommitBatch` with trigger
   `ACTIVE_EXPIRE`.

The default sample size is 20. A pass is bounded by configuration even if the
database contains millions of TTL keys. The cursor gives deterministic
round-robin coverage instead of choosing random samples. A pass can inspect
unexpired keys and delete zero; later passes continue from the cursor.

This is a teaching analogue of sampled active cleanup, not a reproduction of
Redis’s adaptive time-budget algorithm. Sorting keys also costs more than Redis
would accept in a hot production cycle. The mechanism retained is bounded
background effort combined with mandatory lazy visibility.

The cursor is a progress hint, not durable state. Restarting the runtime may
begin traversal from the first sorted key again, which changes cleanup timing but
not correctness because every access still checks the absolute deadline. This is
another example of separating semantic data—the deadline—from operational
metadata—the maintenance scan position.

## Why there are no per-key timers

Creating one asynchronous timer for every expiring key appears simple but creates
poor ownership and scaling behavior. Millions of timers consume memory, scheduler
work, and cancellation bookkeeping. A key can be overwritten, persisted, or
deleted before its timer fires, so every callback also needs generation checks.

MiniRedis stores one integer per expiring entry and owns one periodic producer.
Reads guarantee correctness; the producer only reclaims physical space. This
division is a common backend design pattern: a cheap synchronous validity check
protects semantics, while bounded maintenance controls resource retention.

Injected time makes the design testable. Production uses `SystemClock` and
`AsyncioTimerScheduler` from `src/miniredis/clock.py`. Tests pass a `FakeClock`
and can call `MiniRedis.debug_active_expire_once` without sleeping. The same
executor logic runs in both cases; only the source of time and scheduling is
replaced.

## Replica-safe expiration

Expiration is a replicated write concern. Suppose a primary and replica used
their local clocks to create independent deletion commits. Clock skew or
scheduling order could make each assign a different operation to the same
sequence, causing history divergence.

MiniRedis follows Redis’s ownership rule: the primary creates and propagates
expiration deletes. A read-only replica still treats an elapsed entry as
logically absent, but it must not advance its own sequence.

Two executor branches enforce this:

- `CommandExecutor._active_expire_once` returns zero immediately when
  `_replica_read_only` is true.
- `CommandExecutor._apply_plan` skips a prepared commit when
  `_replica_read_only` is true. The planner’s expired lookup still supplies the
  nil reply, but no local deletion is applied.

The physical entry remains until the primary’s `DeleteReason.EXPIRED` batch
arrives through the replica sink. The Replica row of
[`docs/behavior-matrix.md`](../behavior-matrix.md) names
`tests/replication/test_sink_attach.py::test_replica_logically_hides_expired_key_without_advancing_sequence`
as evidence.

This is a valuable separation: logical time decides what a client may see;
replication authority decides who may publish history.

It also provides a useful debugging rule. If a replica returns nil but its
physical key count has not fallen, first compare the clock and replicated commit
sequence; do not assume active cleanup is broken. Physical removal is expected
only after the primary-originated batch arrives.

## Expiration and memory enforcement

Chapter 5’s `enforce_memory` also scans expired entries before sacrificing live
victims. `_expired_operations` in `src/miniredis/core/eviction.py` builds
sorted expiry deletes. The triggering write, expired-space reclamation, and any
evictions are deduplicated into one proposed plan and later one batch.

That means stale physical entries need not force an avoidable OOM or live
eviction. It also means cleanup remains ordered and propagated. There is no
side-channel that mutates memory accounting separately from the committed
keyspace.

Recovery and snapshots apply the same logical rule. `Database.export_stored_entries`
excludes deadlines at or before `now_ms`, and
`Database.discard_expired_for_recovery` removes elapsed entries while resetting
operational access metadata. Later persistence chapters explain when those
functions run.

## Compared with real Redis

Redis combines passive expiration on key access with active expiration cycles.
The production implementation is historically associated with expiration logic
in `expire.c` and `server.c`, including `activeExpireCycle`. Replicas logically
hide expired data but wait for deletion propagated by the primary.

MiniRedis preserves those two semantic ideas. It simplifies policy to an
injected clock, a fixed interval, a configured sample count, and deterministic
cursor traversal. It does not reproduce Redis’s adaptive CPU budget, sampling
heuristics, or all expiry command options.

The architecture guide row “`ActiveExpireProducer` + expiry planning” marks this
as an intentional simplification, while “replica expiry suppression” is labeled
equivalent at the observable contract level. The TTL and Replica rows in
[`docs/behavior-matrix.md`](../behavior-matrix.md) state the supported commands
and tests.

## Hands-on experiment: lazy and bounded active cleanup

This deterministic experiment reuses the test clock. Run:

```bash
uv run python - <<'PY'
import asyncio
from miniredis import CommandRequest, MiniRedis
from tests.helpers.time import FakeClock

async def main():
    clock = FakeClock(1_000)
    async with MiniRedis.open(
        clock=clock,
        active_expire_sample_size=1,
        debug_record_applied_batches=True,
    ) as server:
        c = server.direct_client()
        print(await c.execute(
            CommandRequest(b"SET", (b"lazy", b"v", b"PX", b"100"))
        ))
        clock.advance(100)
        before = server.debug_commit_seq
        print("GET lazy:", await c.execute(
            CommandRequest(b"GET", (b"lazy",))
        ))
        print("lazy commit delta:", server.debug_commit_seq - before)
        for key in (b"a", b"b"):
            await c.execute(CommandRequest(b"SET", (key, b"v")))
            await c.execute(CommandRequest(b"EXPIRE", (key, b"1")))
        clock.advance(1_000)
        print("active pass 1:", await server.debug_active_expire_once())
        print("physical keys:", server.debug_physical_key_count)
        print("active pass 2:", await server.debug_active_expire_once())
        print("physical keys:", server.debug_physical_key_count)
        await c.close()

asyncio.run(main())
PY
```

Measured output:

```text
Ok(message=b'OK')
GET lazy: Bytes(value=None)
lazy commit delta: 1
active pass 1: 1
physical keys: 1
active pass 2: 1
physical keys: 0
```

The `GET` both returns logical absence and creates one cleanup commit. With a
sample size of one, each active pass removes at most one of the two later keys.

## Exercises

### 1. Understanding: physical versus logical state

Can a key be physically present in `Database.entries` while `GET` returns nil?
Why is that not a correctness bug?

??? note "Reference answer"
    Yes. Once `expire_at_ms <= now_ms`, `lookup` treats the entry as absent.
    Physical removal may wait for lazy commit, active cleanup, or primary
    propagation. Client visibility is therefore correct before reclamation.

### 2. Understanding: replica authority

Why does a replica suppress both active-expiry commits and lazy-expiry commits?

??? note "Reference answer"
    Local clocks and scheduling could assign different deletes to the replica’s
    sequence history. The replica may hide expired data, but only the primary
    publishes the deletion batch that advances replicated history.

### 3. Hands-on: add a bounded-cleanup regression test

Add a test to `tests/contract/test_ttl.py` with five expired keys and
`active_expire_sample_size=2`. Assert that three manual passes delete 2, 2, and
1 keys, and that every recorded cleanup batch uses
`CommitTrigger.ACTIVE_EXPIRE`. Acceptance:

```bash
uv run pytest tests/contract/test_ttl.py -q
```

The file must report one more passing test than its baseline.

??? note "Reference answer"
    Use `FakeClock`, create and expire five keys, then advance once:

    ```diff
    +deleted = [
    +    await runtime.debug_active_expire_once()
    +    for _ in range(3)
    +]
    +assert deleted == [2, 2, 1]
    +assert runtime.debug_physical_key_count == 0
    +cleanup = runtime.debug_applied_batches()[-3:]
    +assert all(
    +    batch.trigger is CommitTrigger.ACTIVE_EXPIRE
    +    for batch in cleanup
    +)
    ```

    Configure `debug_record_applied_batches=True` and explicitly close the
    direct client.

## Summary

MiniRedis stores absolute deadlines, guarantees correctness through lazy logical
visibility, and reclaims unvisited keys through bounded cursor-based active
passes. All primary deletions remain ordinary committed and replicated
operations, while replicas hide elapsed values without creating independent
history. Chapter 5 now asks what happens when live, non-expired data still
exceeds a configured logical memory budget.

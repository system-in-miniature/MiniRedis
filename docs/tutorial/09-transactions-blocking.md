> **Language**: English | [简体中文](../zh/tutorial/09-transactions-blocking.md)

# Transactions and Blocking Commands

## Learning objectives

By the end of this chapter, you will be able to:

- distinguish command queueing, queue-time failure, and runtime failure in
  `MULTI`/`EXEC`;
- explain how per-key revisions make `WATCH` detect create-delete races;
- trace why a successful `EXEC` emits at most one `CommitBatch`;
- model `BLPOP` as a registered waiter rather than a blocked executor; and
- test transaction and waiter cleanup at session and timeout boundaries.

## Two coordination problems

Transactions and blocking list pops can look unrelated. A transaction delays
*execution* until `EXEC`; a blocking pop delays a *reply* until data arrives.
Both mechanisms are difficult in a server with one serialized state owner:
waiting must not freeze that owner, and grouped work must not leak partial
commits.

MiniRedis keeps the state machines explicit:

- `src/miniredis/core/transactions.py`, `TransactionState`, stores one
  session's queued commands, dirty flag, and watched revisions.
- `src/miniredis/core/blocking.py`, `WaiterRegistry`, indexes suspended
  requests by waiter ID, request token, session, and key.
- `src/miniredis/core/executor.py`, `CommandExecutor`, is the only owner that
  changes those structures and the database.

The session ID is the common boundary. `MULTI` state belongs to one client
session, and a blocked request is cancelled when its session closes.

## MULTI queues; EXEC evaluates

`src/miniredis/core/executor.py`,
`CommandExecutor._route_transaction_command`, handles transaction control
before normal planning.

Outside a transaction:

- `MULTI` creates `TransactionState`;
- `WATCH` records the current revision of each key;
- `UNWATCH` clears recorded revisions;
- `EXEC` and `DISCARD` return errors because no transaction exists.

Inside a transaction:

- ordinary allowed commands are appended to `state.queued` and reply
  `Ok(b"QUEUED")`;
- `DISCARD` removes queued commands and watch state;
- another `MULTI` is an error;
- parse or queue-time errors mark `state.dirty`;
- blocking pops and `PUBLISH` are rejected and mark the transaction dirty;
- `WATCH` is rejected **and marks the transaction dirty**.

That last behavior differs from Redis. Redis rejects `WATCH` inside `MULTI`,
but the error does not make the later `EXEC` fail with `EXECABORT`. MiniRedis
does. This semantically opposite edge is documented in the
[Transactions row](../behavior-matrix.md).

When `EXEC` reaches `CommandExecutor._execute_transaction`, three outcomes are
possible:

1. **Dirty transaction:** return `EXECABORT`; none of the queued commands are
   evaluated.
2. **Watched revision changed:** return `NullArray`; this is an optimistic
   concurrency abort, not an error reply.
3. **Valid transaction:** evaluate queued commands against a forked database
   and return an array with one result slot per command.

Runtime command errors remain in their array slots. For example, `SET k 1`,
`LPUSH k x`, `INCR k` yields `OK`, `WRONGTYPE`, and `2`. The failed list
operation does not roll back the successful string operations. This matches
the Redis distinction between queue-time errors that abort execution and
runtime errors that occupy result slots, but the implementation route is
different.

## WATCH uses revisions, not values

Comparing current values is insufficient for optimistic concurrency. Imagine:

```text
client A: WATCH k when k is absent
client B: SET k v
client B: DEL k
client A: EXEC
```

The value is absent both when watched and when executed, but the key changed
twice. MiniRedis records a per-key revision from the database. Every relevant
commit advances that revision, including deletion. During
`CommandExecutor._execute_transaction`, `_watched_keys_changed` compares the
recorded revisions with the current database revisions. The create-delete race
therefore returns `NullArray`.

Watch state exists before `MULTI` and survives entry into the transaction. A
successful `EXEC`, aborted `EXEC`, `DISCARD`, `UNWATCH`, or session close
clears it. Cleanup is part of correctness: stale watch ownership would leak
memory and could make a later transaction observe an unrelated revision set.
`src/miniredis/core/transactions.py`, `TransactionState.clear_all`, resets both
transaction and watched state.

## One EXEC becomes one propagation unit

MiniRedis evaluates a transaction on a deep database fork. In
`CommandExecutor._execute_transaction`, each queued command is planned against
the workspace and successful operations are applied to that workspace. The
executor accumulates operations and replies, rather than committing each
command to the live database.

After evaluation, it creates at most one `PreparedCommit` and calls
`_commit_prepared` once. Its trigger remains `CommitTrigger.CLIENT`; grouping
is represented by the single batch containing all accumulated operations, not
by a distinct transaction trigger. Consequently the live database, AOF,
replication backlog, and replica sink observe one sequence-numbered
`CommitBatch`.

This gives a clear atomic visibility boundary: no other command mailbox turn
can observe the state between queued operations. It also preserves the
transaction as one recovery and replication unit.

The price is intentional inefficiency.
`src/miniredis/core/database.py`, `Database.fork`, deep-copies values, and
`Database.apply_batch` stages the key table and recomputes logical memory.
Real Redis executes queued commands against its live data structures while the
event loop serializes clients. MiniRedis spends O(N + transaction work) to
make planning and the final batch easy to inspect. The
[architecture guide](../architecture.md#where-the-model-deliberately-costs-more)
calls out this teaching cost.

An empty `EXEC` produces `Items(())` and no commit. A transaction containing
only reads also needs no mutation batch. “One batch” means **at most** one, not
exactly one.

## BLPOP does not block the executor

A naive async server might implement `BLPOP` by awaiting inside the executor
until a list changes. That would deadlock progress: the executor could not
process the `RPUSH` needed to wake the pop.

MiniRedis first plans a blocking pop against current state. If any requested
key has a list item, it pops immediately, respecting key order and left/right
direction. If all keys are empty, `CommandExecutor._apply_plan` registers a
`BlockingWaiter` instead of finishing the request.

`src/miniredis/core/blocking.py`, `WaiterRegistry.register`, stores:

- a unique `WaiterId`;
- the request token and session ID;
- the ordered tuple of keys;
- whether to pop left or right;
- an optional timer handle; and
- explicit `WAITING`, `WOKEN`, `TIMED_OUT`, or `CANCELLED` state.

One waiter is indexed under every requested key but remains one logical
request. `WaiterRegistry.transition` is the single state transition point. It
removes all indexes and cancels the timer before returning the waiter. Timeout,
push wakeup, cancellation, and session close therefore compete to transition
the same ID; only the first transition succeeds.

`src/miniredis/core/blocking.py`, `prepare_list_wakeups`, runs while planning a
commit that pushes list values. It examines waiters in deterministic order,
reserves at most one available element per waiter, and returns wakeups attached
to the same execution plan. `CommandExecutor._attach_push_wakeups` then ties
the pop operations and waiter replies to the triggering commit.

This avoids a subtle race: replying to a waiter before the list mutation
commits could report an item that remains visible in the list after a
durability failure. MiniRedis publishes the wakeup only with the successful
commit path.

## Timeout, cancellation, and direction

The command parser freezes a decimal timeout into milliseconds.
`BLPOP key 0` means no timeout. A positive timeout schedules a control event;
when `TimeoutWaiter` reaches the executor, `_timeout_waiter` transitions the
waiter and completes the request with a null result.

For the Direct adapter, `src/miniredis/adapters/direct.py`,
`DirectClient.resolve`, maps transport closure of a `BlockingPop` to
`Bytes(None)`. RESP mapping expresses a blocking-pop timeout as a null array
where appropriate. The domain and wire representations are adapter concerns;
the waiter state machine remains in core.

Direction is frozen in the waiter. `BRPOP` awakened by a later push still pops
from the right; it does not degrade into `BLPOP`. Key priority is also stable:
the first ready key in command order wins. These contracts are exercised in
`tests/mechanisms/test_blpop.py`.

## Compared with real Redis

Redis transaction machinery lives primarily in `src/multi.c`. `MULTI` queues
commands, `EXEC` runs them serially, `DISCARD` clears the queue, and `WATCH`
implements optimistic locking. Redis does not provide rollback for runtime
errors. MiniRedis preserves those observable lessons, but evaluates against a
deep fork and converts successful changes into one explicit logical batch.

Redis blocking list commands are implemented across list command code and the
blocked-client subsystem (notably `src/blocked.c` and list-related source such
as `src/t_list.c`, depending on Redis version). Redis detaches a blocked client
from normal command processing and wakes it when a key becomes ready.
MiniRedis's waiter registry is the small teaching analogue.

The deliberate differences are listed in the
[Transactions and List rows](../behavior-matrix.md): MiniRedis forbids
blocking commands and `PUBLISH` inside `MULTI`; its `WATCH`-inside-`MULTI`
dirtying differs from Redis; and its deep-copy transaction strategy is not a
production performance model.

## Hands-on lab: an optimistic abort and a waiter wakeup

Save as `/tmp/miniredis_coordination.py`:

```python
import asyncio

from miniredis import CommandRequest, MiniRedis


async def main():
    async with MiniRedis.open() as runtime:
        owner = runtime.direct_client()
        rival = runtime.direct_client()
        producer = runtime.direct_client()

        print("WATCH:", await owner.execute(
            CommandRequest(b"WATCH", (b"counter",))
        ))
        await rival.execute(CommandRequest(b"SET", (b"counter", b"9")))
        print("MULTI:", await owner.execute(CommandRequest(b"MULTI")))
        print("queued GET:", await owner.execute(
            CommandRequest(b"GET", (b"counter",))
        ))
        print("EXEC after rival write:",
              await owner.execute(CommandRequest(b"EXEC")))

        blocked = asyncio.create_task(owner.execute(
            CommandRequest(b"BLPOP", (b"jobs", b"1"))
        ))
        await runtime.debug_wait_for_waiters(1)
        print("waiters while empty:", runtime.debug_stats().waiters)
        print("RPUSH:", await producer.execute(
            CommandRequest(b"RPUSH", (b"jobs", b"compile"))
        ))
        print("BLPOP result:", await blocked)


asyncio.run(main())
```

Run:

```bash
uv run python /tmp/miniredis_coordination.py
```

Measured output:

```text
WATCH: Ok(message=b'OK')
MULTI: Ok(message=b'OK')
queued GET: Ok(message=b'QUEUED')
EXEC after rival write: NullArray()
waiters while empty: 1
RPUSH: Number(value=1)
BLPOP result: Items(values=(Bytes(value=b'jobs'), Bytes(value=b'compile')))
```

The rival write changes `counter` after `WATCH`; `EXEC` aborts before evaluating
the queued `GET`. The later `BLPOP` registers one waiter without blocking the
executor, so another client can submit `RPUSH`. The push commit consumes and
delivers `compile`.

Run the focused tests:

```bash
uv run pytest -q tests/mechanisms/test_transactions.py \
  tests/mechanisms/test_watch.py \
  tests/mechanisms/test_blpop.py
```

Expected repository result:

```text
23 passed
```

## Exercises

### 1. Understanding: classify errors

Inside `MULTI`, compare an invalid-arity `GET` with an `LPUSH` that encounters
a string key during `EXEC`. Which aborts the transaction, and which occupies a
result slot?

??? note "Reference answer"

    Invalid arity is detected before queueing, marks the transaction dirty, and
    makes `EXEC` return `EXECABORT`. The type error is discovered while
    evaluating the queued `LPUSH`; it occupies its array slot, while other
    commands can succeed and contribute operations to the final batch.

### 2. Understanding: explain the create-delete watch race

Why does watching only the current value fail to detect `SET k v; DEL k`?

??? note "Reference answer"

    The value is absent before and after, so value equality loses the history.
    A per-key revision advances for both commits. Comparing the recorded and
    current revisions detects that the watched key changed even though its
    final value matches its initial value.

### 3. Hands-on: verify one transaction batch

Task boundary: add a test under `tests/reliability/` using
`debug_record_applied_batches=True`. Queue two mutating commands and one read,
then execute. Do not modify `src/`.

Acceptance: the commit sequence advances by one; exactly one new applied batch
contains both writes; its trigger is `CommitTrigger.CLIENT`; the reply array
still has three slots.

??? note "Reference answer"

    Follow `tests/reliability/test_transaction_commit.py`. Record the batch
    count before `MULTI`, queue commands whose effects do not conflict, execute,
    and inspect only the newly appended batch. The expected change is test-only
    and should not assert private planner implementation details beyond the
    documented `CLIENT` trigger, operation effects, sequence increment, and
    reply slots.

### 4. Hands-on: race timeout against push

Task boundary: add a deterministic test using the repository's fake scheduler.
Register a blocking pop, then arrange a timeout control event and a push around
the same executor boundary. Do not use real sleeps.

Acceptance: exactly one terminal reply occurs, waiter indexes and timers return
to zero, and the list state agrees with the winner: timeout leaves the pushed
value available; wakeup consumes it once.

??? note "Reference answer"

    Reuse the fake clock/scheduler helpers and waiter assertions from
    `tests/concurrency/test_blpop_races.py`. Write two cases, one for each
    ordering of control messages. After each case assert
    `debug_waiter_index_counts == (0, 0, 0)`. The registry's one-way
    `transition` should make duplicate completion impossible.

## Summary

MiniRedis transactions delay evaluation, detect optimistic conflicts with
per-key revisions, and collapse successful mutations into at most one durable
and replicated commit batch. Blocking pops suspend a request in an indexed
state machine while leaving the executor free to process the push that can
wake it. Both mechanisms rely on one serialized owner and explicit cleanup.
The final chapter crosses the adapter boundary to show how these same domain
semantics are carried over RESP2 and TCP without being reimplemented there.

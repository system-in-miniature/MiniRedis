# Chapter 2: The Life of a Command

> **Language:** English | [简体中文](../zh/tutorial/02-life-of-a-command.md)

## Learning objectives

By the end of this chapter, you will be able to:

- trace a request through parsing, mailbox admission, planning, commit, apply,
  propagation, and reply publication;
- distinguish `CommandRequest`, a typed `Command`, `ExecutionPlan`,
  `PreparedCommit`, and `CommitBatch`;
- explain why planning is side-effect free and why commit is serialized;
- identify the durability point after which a successful write may be observed;
- verify the pipeline by recording applied batches without changing production
  code.

## Five representations, five responsibilities

A command changes shape as it moves through MiniRedis. Each shape removes an
ambiguity or adds a guarantee.

`CommandRequest` in `src/miniredis/commands/request.py` is transport-neutral:

```python
@dataclass(frozen=True, slots=True)
class CommandRequest:
    name: bytes
    args: tuple[bytes, ...] = ()
```

It says nothing about whether `SET` has the right number of arguments. That is
the parser’s job. `parse_request` in `src/miniredis/commands/parser.py`
uppercases the command name, checks arity and option combinations, parses strict
integers or scores, and returns one data class from
`src/miniredis/commands/model.py`. For example,
`CommandRequest(b"SET", (b"k", b"v"))` becomes
`SetString(key=b"k", value=b"v", only_if=None, expire_ms=None)`.

This typed command freezes interpretation before concurrency begins. A planner
does not repeatedly inspect a bag of raw arguments or wonder whether `EX 2` was
already converted to milliseconds. The `Command` type alias is a closed union,
and `is_dataset_mutating` asserts if a new command type has not been classified.

The third shape is `ExecutionPlan`, defined in
`src/miniredis/core/executor.py`, `ExecutionPlan`. It contains:

- the reply that should eventually be returned;
- immutable `PutEntry` or `DeleteKey` operations;
- keys whose access metadata should be touched;
- a `CommitTrigger`, such as client or active expiration;
- optional blocking-waiter wakeups.

An execution plan is still a proposal. It has no sequence number and has not
crossed durability. If it contains operations, its `prepared_commit` property
creates a `PreparedCommit`. `_commit_prepared` assigns the next sequence number
and produces the final `CommitBatch`. That fifth representation is the atomic
unit consumed by AOF, `Database.apply_batch`, the replication backlog, a replica
sink, and recovery.

## Parse before admission, order after admission

The Direct path starts at `DirectClient.execute` in
`src/miniredis/adapters/direct.py`. `DirectClient.submit` asks the runtime to
submit the request. `MiniRedis.submit_request` in
`src/miniredis/runtime.py` calls `MiniRedis.parse`; a parse failure becomes a
`Failure` reply and is admitted through `CommandExecutor.submit_rejection`.

Why place a rejected request in the mailbox? Ordering still matters. If a client
pipeline submits valid, invalid, then valid frames, result slots must remain in
that order. A parse failure inside `MULTI` must also dirty the transaction at the
right mailbox turn. `CommandExecutor._dispatch` handles `RejectRequest` in the
same serialized loop and marks an active transaction dirty before completing the
reply.

For a valid typed command, `CommandExecutor._admit_request` creates a unique
`RequestToken` and executor-owned future, records the request, registers its
token with the session endpoint, and uses `EventLoopMailbox.admit_user`.
The bounded checks occur before acceptance: a stopped runtime returns `CLOSED`,
and a full request set returns `BUSY`. Once accepted, the executor owns a terminal
outcome for that token.

`CommandExecutor._run` then repeatedly calls `mailbox.take` and awaits
`_dispatch`. There is one worker. Planning, durability, apply, and reply
publication for one request therefore share a total order. Async I/O may suspend
the worker at the durability barrier, but a second command cannot race ahead and
mutate the database behind the first plan.

## Pure planning and per-type routing

At `CommandExecutor._execute`, ordinary commands capture `now_ms` and call
`CommandPlanner.plan`. The router in `src/miniredis/core/planner.py`,
`CommandPlanner.plan`, tries general/string, hash, list, set, sorted-set, and TTL
planners. It then applies `enforce_memory` to the resulting plan.

Consider `SET counter 41`. `plan_general_and_strings` in
`src/miniredis/core/planning.py` calls `lookup`, computes an absolute expiration
if needed, and calls `make_put`. `make_put` freezes the live Python value into a
`StoredEntry` and increments its mutation version. It does not change
`Database.entries`.

This plan/commit split has several benefits:

- a type error can return `WRONGTYPE` with no partial mutation;
- integer overflow can fail after inspection without restoring state;
- memory enforcement can add eviction operations or replace the plan with OOM;
- a lazy-expiration delete can be proposed and later suppressed on a replica;
- a transaction can combine several planned operations into one final batch;
- tests can inspect the decision before or after propagation.

The split is not free. `Database.apply_batch` in
`src/miniredis/core/database.py` stages a shallow copy of the entire key table
and recomputes logical usage, making each commit O(N) in key count. The
architecture guide calls this teaching scaffolding. Real Redis usually mutates
its dictionaries and accounting in place.

## Commit ordering: durable before visible

`CommandExecutor._apply_plan` is the handoff from proposal to publication. It
first attaches any list-push wakeups. If there is a prepared commit and the
runtime is not a read-only replica, it awaits `_commit_prepared`.

The critical order inside `CommandExecutor._commit_prepared` is:

1. assign `database.commit_seq + 1`;
2. await `commit_barrier.append(batch)`;
3. reject a failed or wrong-sequence acknowledgement;
4. call `Database.apply_batch`;
5. update expiration and eviction counters;
6. append the batch to the replication backlog;
7. offer it to attached replica sinks.

Only after `_commit_prepared` returns does `_apply_plan` materialize read touches,
fulfil waiter wakeups, and call `_finish_reply` for the requesting token. A
successful mutating reply therefore cannot be published before the configured
commit barrier accepts the exact sequence.

With no AOF path, the runtime uses `NullCommitBarrier.append`, which returns
`AofAppendOk(batch.seq)`. That proves ordering but not disk durability. With AOF
configured, Chapters 6 and 7 show how policies change the strength of that
barrier. The interface is the same, so command semantics do not depend on a
transport or persistence format.

If append returns `AofAppendFailed`, `_commit_prepared` raises
`DurabilityFailure`. `_apply_plan` completes the request with an error and
transitions the runtime toward failure; it never calls `Database.apply_batch`.
That is the essential “acknowledged means crossed the selected barrier” contract.

## Applying one immutable batch

`CommitBatch` and its operations live in `src/miniredis/core/commit.py`.
The data classes are frozen. A batch requires a positive sequence and at least
one operation. `PutEntry` carries a fully frozen value, absolute expiry, and
mutation version. `DeleteKey` carries a reason: client, expired, or evicted.

`Database.apply_batch` first checks that the sequence is exactly the next one.
It applies operations to staged state. A client-triggered `PutEntry` also
materializes an access tick and LFU frequency update; replicated or active-expiry
application can disable that client access tracking. It recomputes logical
usage, validates positive entry sizes, and only then swaps the staged tables into
the live database and advances `commit_seq`.

The sequence is more than a counter for tests. It connects local state, AOF
records, snapshots, replication backlog entries, and replica cursors. A gap or
duplicate is rejected rather than silently producing a different history.

Reads commonly have no operations. A live `GET` returns a reply and a touch key,
so `_apply_plan` updates LRU/LFU metadata but produces no `CommitBatch`. A read
that discovers an expired key is different: `lookup` returns logical absence plus
an expiry `DeleteKey`, so the read may produce a commit. Chapter 4 explores that
case.

Reply calculation and reply publication are therefore separate events. A planner
may know the answer immediately, but only the executor knows whether every
required ownership and commit step reached a terminal state. Keeping those events
separate is what lets the same planner serve an in-memory run, an AOF-backed run,
and a replicated run without weakening acknowledgement semantics.

## Reply publication and ownership

`_finish_reply` in `src/miniredis/core/executor.py` maps the plan reply back to
the accepted token. Direct clients resolve their future. TCP sessions instead
use the endpoint’s ordered outbox, preventing a later completed request from
overtaking an earlier response. Both paths end in `_finish_request`, which
removes the request from executor ownership and resolves the future exactly once.

Cancellation does not cancel the executor-owned future directly.
`DirectClient.resolve` shields it and posts `AbandonRequest` when the caller is
cancelled. This is a subtle but important ownership rule: client impatience and
server completion are separate events. The serialized owner decides whether the
request had already committed, was waiting, or can be abandoned safely.

## Compared with real Redis

Redis also serializes ordinary command execution through its main event loop and
propagates atomic command or transaction effects to AOF and replicas. Command
implementations are commonly located in files such as `t_string.c`,
`t_hash.c`, and `db.c`; propagation and networking have their own production
machinery.

MiniRedis’s planner corresponds conceptually to a command implementation, but
returning `ExecutionPlan` is an intentional simplification. Redis generally
mutates live state inside the command implementation rather than first building a
frozen operation list. MiniRedis’s `CommitBatch` nevertheless preserves the
important observable lesson: one ordered propagation unit is seen consistently
by durability, local apply, replication, and recovery.

See the mapping rows “per-type planners + `CommandPlanner`,” “executor mailbox,”
“`ExecutionPlan` / `PreparedCommit`,” and “`CommitBatch`” in
[`docs/architecture.md`](../architecture.md). The String row in
[`docs/behavior-matrix.md`](../behavior-matrix.md) points to contract and RESP
tests rather than claiming the full Redis command table.

## Hands-on experiment: inspect a committed SET

Run from the repository root:

```bash
uv run python - <<'PY'
import asyncio
from miniredis import CommandRequest, MiniRedis

async def main():
    async with MiniRedis.open(debug_record_applied_batches=True) as server:
        client = server.direct_client()
        request = CommandRequest(b"SET", (b"counter", b"41"))
        print("parsed:", server.parse(request))
        print("reply:", await client.execute(request))
        print("read:", await client.execute(
            CommandRequest(b"GET", (b"counter",))
        ))
        print("batches:", server.debug_applied_batches())
        await client.close()

asyncio.run(main())
PY
```

Measured output:

```text
parsed: SetString(key=b'counter', value=b'41', only_if=None, expire_ms=None)
reply: Ok(message=b'OK')
read: Bytes(value=b'41')
batches: (CommitBatch(seq=1, operations=(PutEntry(key=b'counter', entry=StoredEntry(value=StoredString(data=b'41'), expire_at_ms=None, mutation_version=1)),), trigger=<CommitTrigger.CLIENT: 'client'>),)
```

The raw request is already typed before submission. The write produces exactly
one operation in sequence 1. The later `GET` returns the value but adds no batch,
which is why the recorded tuple still contains one element.

## Exercises

### 1. Understanding: identify the safe reply point

Why would returning the planner’s successful reply before
`commit_barrier.append` be incorrect?

??? note "Reference answer"
    The plan is only a proposal. Append can fail or acknowledge the wrong
    sequence. Returning success first would let a client observe an acknowledged
    write that was never applied or durably accepted. MiniRedis publishes the
    reply only after barrier acceptance and `Database.apply_batch`.

### 2. Understanding: parse failures and ordering

Why does a parser rejection enter the executor mailbox instead of returning
directly from the adapter?

??? note "Reference answer"
    A rejection still occupies an ordered result slot and can affect transaction
    state. Serializing `RejectRequest` preserves pipeline order and lets an
    invalid command dirty the correct active transaction.

### 3. Hands-on: test “read does not commit”

Add `tests/tutorial/test_command_lifecycle.py`. Open with
`debug_record_applied_batches=True`, perform one `SET` followed by three `GET`
commands, and assert that the batch count and sequence remain one. Acceptance:

```bash
uv run pytest tests/tutorial/test_command_lifecycle.py -q
```

It must report `1 passed` and the test must not inspect private name-mangled
state.

??? note "Reference answer"
    The key part of the test is:

    ```diff
    +async with MiniRedis.open(debug_record_applied_batches=True) as server:
    +    client = server.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"k", b"v")))
    +    for _ in range(3):
    +        assert await client.execute(
    +            CommandRequest(b"GET", (b"k",))
    +        ) == Bytes(b"v")
    +    assert server.debug_commit_seq == 1
    +    assert len(server.debug_applied_batches()) == 1
    +    await client.close()
    ```

## Summary

A MiniRedis command becomes progressively more constrained: transport-neutral
request, validated typed command, side-effect-free execution plan, prepared
operations, and sequence-numbered commit batch. The single executor orders the
whole transition, and successful writes cross the selected durability barrier
before database apply and reply publication. Chapter 3 now opens the planners
that build those plans for Redis’s five core value types.

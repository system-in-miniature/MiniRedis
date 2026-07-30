# Chapter 1: Meet MiniRedis

> **Language:** English | [简体中文](../zh/tutorial/01-getting-started.md)

## Learning objectives

By the end of this chapter, you will be able to:

- explain what MiniRedis preserves from Redis and what it deliberately leaves out;
- create, start, use, and close an in-process MiniRedis runtime;
- issue binary-safe commands through the Direct API and read domain replies;
- identify the runtime, command, planner, commit, persistence, replication, and protocol layers;
- use the behavior matrix to distinguish an implemented contract from a production claim.

## Why study a teaching kernel?

Redis is unusually approachable for a production database, but “approachable” is
relative. Its source must solve portability, allocator behavior, event-loop
integration, compact encodings, backwards compatibility, observability, modules,
clustering, and years of operational edge cases at once. Those concerns are real,
yet they can hide the mechanism a first-time reader is looking for: how does a
command become one ordered state change?

MiniRedis is not a Python port of Redis. It is a small executable model that keeps
that mechanism visible. The project README calls it a **Direct-first**, RESP2/TCP
teaching runtime. “Direct-first” means that the in-process Python client and the
network adapter eventually submit the same `CommandRequest`; the network path does
not own a second implementation of `SET` or `GET`. The public assembly point is
`src/miniredis/runtime.py`, where `MiniRedis.open`, `MiniRedis.start`,
`MiniRedis.direct_client`, and `MiniRedis.close` own the lifecycle.

The model preserves several production-shaped boundaries:

1. bytes enter as a transport-neutral request;
2. parsing produces a closed typed command;
3. a serialized executor chooses one total order;
4. a pure planner proposes a reply and immutable operations;
5. a commit crosses the durability barrier before local publication;
6. the same committed batch enters replication and recovery history.

That is enough structure to study atomicity, expiration, eviction, persistence,
replication, transactions, blocking operations, and RESP framing without first
learning every Redis optimization. The cost is deliberate simplification. Values
use Python containers. Memory is a deterministic logical estimate rather than
resident set size. Replication is an in-process sink, not PSYNC over the network.
AOF and snapshots use custom formats. The complete boundary is recorded in the
“Deliberate difference” column of
[`docs/behavior-matrix.md`](../behavior-matrix.md).

This distinction matters. A mechanism can be faithful enough to teach while its
implementation remains unsuitable for production. MiniRedis demonstrates how a
durability barrier orders an acknowledged write; it does not claim Redis’s
throughput, crash-testing history, or operational compatibility.

## The smallest useful conversation

`MiniRedis.open` constructs a runtime but does not make it accept commands.
`MiniRedis.start` performs recovery, creates optional AOF workers, starts the
executor, starts the active-expiration producer, and only then opens mailbox user
admission. These steps are visible in `src/miniredis/runtime.py`,
`MiniRedis._start_owned`. The asynchronous context manager calls this startup path
on entry and the coordinated shutdown path on exit, so it is the safest default:

```python
async with MiniRedis.open() as server:
    client = server.direct_client()
    reply = await client.execute(CommandRequest(b"PING"))
    await client.close()
```

The request name and every argument are `bytes`. The definition in
`src/miniredis/commands/request.py`, `CommandRequest`, contains only
`name: bytes` and `args: tuple[bytes, ...]`. No UTF-8 decoding is required to
store a key or value. Text is merely one possible interpretation at the edge.

`DirectClient.execute` in `src/miniredis/adapters/direct.py` is intentionally
thin. It calls `DirectClient.submit`, waits for the executor-owned future in
`DirectClient.resolve`, and returns a domain reply. Replies are frozen data
classes in `src/miniredis/core/reply.py`: `Ok`, `Bytes`, `Number`, `Items`,
`NullArray`, and `Failure`. Seeing `Bytes(value=None)` therefore means a nil bulk
value, not the Python function returning no result. Seeing
`Failure(code="WRONGTYPE", ...)` is a normal command reply, not a raised Python
exception.

Explicitly closing the client is useful even inside the runtime context. A direct
client owns a session endpoint. `DirectClient.close` posts `SessionClosed` to the
same executor mailbox, and `MiniRedis.close` can then drain a runtime whose client
ownership is already resolved. This mirrors the project’s lifecycle rule: an
accepted request, session, waiter, timer, sink, or worker always has an owner that
must terminalize it.

## What the runtime assembles

Open `src/miniredis/runtime.py` at `MiniRedis.__init__`. The constructor creates:

- a `Database`, the live keyspace;
- a `CommandPlanner`, which routes typed commands to per-type planners;
- a `CommandExecutor`, the serialized owner of semantic state changes;
- an optional `SnapshotManager`;
- sets that own background tasks, replica sinks, and TCP servers;
- an `ActiveExpireProducer`, installed during startup.

The runtime is a facade, not the semantic center. Its module docstring says this
directly: semantic execution remains in the serialized executor. That separation
keeps adapters and lifecycle policy from silently changing command behavior.

The default `MiniRedisConfig` in `src/miniredis/config.py`,
`MiniRedisConfig`, makes the teaching boundaries concrete. The mailbox admits at
most 1,024 pending commands. Active expiration examines 20 candidates per tick.
No logical memory limit is enabled. The default eviction policy is
`noeviction`. AOF and snapshots are disabled unless paths are supplied. These are
bounded operational choices, not claims about Redis defaults.

The runtime exposes `debug_stats` for observation. Its `RuntimeStats` result
includes semantic counters such as key count, logical memory usage, expired and
evicted keys, commit sequence, replication cursor, and owner counts. Debug
surfaces are especially valuable in a teaching kernel: they let an experiment
verify a mechanism without reaching into arbitrary private fields.

## A practical source-reading method

Use the repository as an executable specification rather than reading modules in
alphabetical order. Begin with one public request and keep three questions in
view: who owns the input now, what immutable value crosses the next boundary, and
what evidence proves the transition completed?

For a basic write, start at `MiniRedis.submit_request` in
`src/miniredis/runtime.py`. Follow `parse_command_request` into the typed model,
then jump to `CommandExecutor._execute` in `src/miniredis/core/executor.py`.
The command type tells you which planner to open. Read the returned
`ExecutionPlan` before following `_apply_plan` and `_commit_prepared`. Finally,
inspect `Database.apply_batch` and the domain reply. This path is small enough to
hold in working memory and becomes the spine for later mechanisms.

Tests are the second half of the specification. A behavior-matrix row names a
focused pytest node; read the assertion, then trace only the functions necessary
to explain it. For example, the String row points to atomic command tests, while
the Lifecycle row points to shutdown acceptance. This avoids two common errors:
inferring behavior from a class name alone, and mistaking an implemented
interface for a verified end-to-end path.

Keep operational and semantic state separate as you read. A value and its expiry
are semantic and enter stored batches. LRU ticks, task sets, pending futures, and
outbox occupancy are operational ownership metadata. Some operational state
changes on reads without producing a commit; some resets on restart. Later
chapters call out those boundaries explicitly.

Use public experiments before debug hooks. A reply demonstrates what an
application can observe; a debug counter then explains why it happened; a
recorded batch is appropriate only when propagation structure is the question.
This evidence order keeps lessons honest and transferable. It also prevents an
internal field from becoming an accidental public API merely because a tutorial
found it convenient. When a debug surface is used, name it as instrumentation
and pair it with the client-visible result.

## A map of the book

The remaining chapters follow dependencies rather than presenting unrelated
features:

- Chapter 2 traces one command from parser to reply and explains plan/commit.
- Chapter 3 opens the five value models and their command planners.
- Chapter 4 studies lazy and bounded active expiration.
- Chapter 5 adds logical maxmemory, exact LRU, and deterministic LFU.
- Chapters 6 and 7 follow committed batches into AOF, rewrite, snapshots, and
  recovery.
- Chapter 8 adds replication identity, offsets, backlog, and resynchronization.
- Chapter 9 studies transactions and blocking waiters on the serialized owner.
- Chapter 10 returns to the edge: RESP2/TCP and redis-py interoperability.

This order is important. Persistence and replication do not consume arbitrary
dictionary mutations; they consume `CommitBatch` values. Transactions are easier
to understand after that propagation unit is familiar. RESP2 is last because it
is an adapter over semantics already learned through the Direct API.

## Compared with real Redis

The corresponding Redis concepts are spread across its server lifecycle, command
table, networking layer, database dictionaries, and event loop. Redis commands
are commonly implemented in files such as `t_string.c`, while keyspace mechanics
appear in `db.c` and the event loop serializes command execution. MiniRedis turns
those relationships into Python module boundaries.

The architecture mapping labels `src/miniredis/runtime.py` as an intentional
simplification of Redis server lifecycle, and the RESP2/TCP adapter as a bounded
correctness adapter rather than a throughput target. See
[`docs/architecture.md`](../architecture.md) under “Mapping to real Redis.”
The behavior matrix is the compatibility contract: for example, its String row
lists the supported subset and its Lifecycle row cites zero-owner shutdown tests.

Do not infer omitted features. MiniRedis has no RESP3, Lua VM, Streams, ACL,
multiple databases, Cluster, Sentinel, TLS, or network replication. It does
support important mechanisms that can be tested directly, but “mechanism
present,” “wire compatible,” and “production equivalent” are different claims.

## Hands-on experiment: first commands and observable state

From the repository root, run:

```bash
uv run python - <<'PY'
import asyncio
from miniredis import CommandRequest, MiniRedis

async def main():
    async with MiniRedis.open() as server:
        client = server.direct_client()
        for request in [
            CommandRequest(b"PING"),
            CommandRequest(b"SET", (b"greeting", b"hello")),
            CommandRequest(b"GET", (b"greeting",)),
        ]:
            print(await client.execute(request))
        stats = server.debug_stats()
        print("state:", server.state.value)
        print("commits:", server.debug_commit_seq)
        print("keys:", stats.key_count)
        await client.close()

asyncio.run(main())
PY
```

Measured output:

```text
Ok(message=b'PONG')
Ok(message=b'OK')
Bytes(value=b'hello')
state: running
commits: 1
keys: 1
```

`PING` and `GET` do not mutate the keyspace, so only `SET` creates a commit.
The context is still active when the stats are printed, hence `state: running`.
The single logical key matches both `keys: 1` and commit sequence 1.

## Exercises

### 1. Understanding: request and reply boundaries

Why does `CommandRequest` keep bytes instead of decoding strings immediately,
and why is a missing value represented by `Bytes(None)` rather than Python
`None`?

??? note "Reference answer"
    Bytes preserve binary-safe keys and values across both Direct and RESP2
    adapters. `Bytes(None)` remains a value inside the closed `Reply` union, so
    it can be encoded as a RESP nil bulk and distinguished from “this operation
    produced no reply,” which is represented by an actual `None`.

### 2. Hands-on: add a lifecycle smoke test

Create `tests/tutorial/test_getting_started.py`. Start a runtime, issue `PING`,
`SET`, and `GET`, explicitly close the client, then assert that all owner counts
reported after `server.close()` are zero. Acceptance:

```bash
uv run pytest tests/tutorial/test_getting_started.py -q
```

It must report `1 passed`; the full suite should then report one more passing
test than its baseline.

??? note "Reference answer"
    Use `server = MiniRedis.open()`, `await server.start()`, and a
    `try/finally` that closes both client and server. The key assertions are:

    ```diff
    +assert await client.execute(CommandRequest(b"PING")) == Ok(b"PONG")
    +assert await client.execute(
    +    CommandRequest(b"SET", (b"k", b"v"))
    +) == Ok()
    +assert await client.execute(
    +    CommandRequest(b"GET", (b"k",))
    +) == Bytes(b"v")
    +await client.close()
    +await server.close()
    +stats = server.debug_stats()
    +assert stats.pending_futures == stats.sessions == stats.owned_tasks == 0
    ```

### 3. Understanding: teaching fidelity

Classify each statement as “implemented mechanism,” “adapter compatibility,” or
“production parity”: MiniRedis orders commits through a durability barrier;
redis-py can use the RESP2 adapter; MiniRedis matches Redis throughput.

??? note "Reference answer"
    The first is an implemented mechanism. The second is bounded adapter
    compatibility when configured as documented. The third is false: throughput
    parity is explicitly a non-goal.

## Summary

MiniRedis is useful because it keeps Redis-shaped ownership and propagation
boundaries executable while making its simplifications explicit. You can now
start the runtime, speak through the binary-safe Direct API, interpret domain
replies, and verify state through debug statistics. Chapter 2 opens the facade
and follows one `SET` through every semantic stage until its reply becomes safe
to publish.

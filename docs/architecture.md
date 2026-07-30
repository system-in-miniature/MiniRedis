> **Language**: English | [简体中文](zh/architecture.md)

# MiniRedis Architecture Guide

MiniRedis is easiest to understand as a small, explicit model of Redis's
mechanisms, not as a source-compatible rewrite. It keeps the important
boundaries visible: parse a command, plan its reply and mutations, durably
publish one commit unit, then apply and propagate that unit.

## Recommended reading order

1. Start with `commands/request.py`, `commands/parser.py`, and
   `commands/model.py`. They show how one binary-safe request becomes a closed,
   typed command.
2. Read `core/reply.py`, `core/values.py`, and `core/commit.py`. These are the
   vocabulary used on both sides of execution: replies, live values, and frozen
   propagation records.
3. Read `core/planning.py` and one small type planner such as
   `core/hash_planner.py`, then use `core/planner.py` as the routing index for
   the other planners.
4. Read `core/database.py` beside `core/executor.py`. The executor serializes
   decisions; the database applies the resulting `CommitBatch`.
5. Add cross-cutting mechanisms one at a time:
   `core/expiration.py`, `core/eviction.py`, `core/blocking.py`,
   `core/transactions.py`, and `core/pubsub.py`.
6. Follow a write through `persistence/aof.py`, then read
   `persistence/snapshot.py`, `persistence/recovery.py`, and
   `persistence/codec.py`.
7. Finish with `replication/backlog.py`, `replication/sink.py`, and
   `runtime.py`. These assemble resumable replication and own startup/shutdown.
8. Only then read the Direct and RESP2/TCP adapters. They deliberately do not
   own command semantics.

For observable compatibility boundaries and test evidence, keep
[`behavior-matrix.md`](behavior-matrix.md) open while reading.

## One command from API to propagation

```text
CommandRequest
  -> parser -> typed Command
  -> mailbox -> CommandExecutor
  -> CommandPlanner -> ExecutionPlan(reply, operations)
  -> PreparedCommit -> AOF durability barrier
  -> CommitBatch(seq, operations)
  -> Database.apply_batch
  -> replication backlog -> ReplicaSink
  -> reply / Pub/Sub outbox
```

Planning is side-effect free. A reply may be known before a write is durable,
but the executor does not publish that successful reply until the commit
barrier and database apply have completed. `EXEC` may evaluate several queued
commands against a fork, yet it emits at most one final `CommitBatch`, so AOF,
replication, and recovery see the transaction as one propagation unit.

## Mapping to real Redis

The labels below describe the teaching relationship:

- **Equivalent**: preserves the mechanism's important observable contract,
  although the implementation language and data structures differ.
- **Intentional simplification**: preserves the lesson while dropping scale,
  wire compatibility, or production optimizations.
- **Semantically opposite**: intentionally or currently behaves in the
  opposite direction and must not be inferred as Redis behavior.

| MiniRedis module or term | Redis source / concept | Level | What to carry across |
|---|---|---|---|
| `commands/parser.py` + typed command model | command table, argument validation, and command implementations under Redis `src/` | Intentional simplification | MiniRedis freezes a small command grammar up front instead of reproducing Redis's full command metadata and option surface. |
| per-type planners + `CommandPlanner` | command implementations such as `t_string.c`, `t_hash.c`, `t_list.c`, `t_set.c`, `t_zset.c` | Intentional simplification | A planner is the conceptual command implementation, but it returns data instead of mutating the keyspace directly. |
| executor mailbox | Redis event loop's serialized command execution | Equivalent | One owner chooses a total order for commands and control events. MiniRedis uses an asyncio queue rather than Redis's event loop internals. |
| `ExecutionPlan` / `PreparedCommit` | command result plus the mutations Redis is about to make | Intentional simplification | The split makes the pre-commit decision inspectable; Redis generally mutates in place inside the command implementation. |
| `CommitBatch` | one atomic propagation unit sent to AOF and replicas | Equivalent | A transaction's successful writes stay grouped and ordered across durability, replication, and recovery. |
| `Database.apply_batch` | Redis keyspace mutation in `db.c` and command implementations | Intentional simplification | MiniRedis copies the table and recomputes logical usage for invariant clarity; Redis normally updates dictionaries and accounting in place. |
| transaction fork | `multi.c` queueing and `EXEC` execution | Intentional simplification | MiniRedis deep-copies the database to calculate a final batch; Redis executes queued commands against the live database. |
| `WATCH` inside `MULTI` marks the transaction dirty | Redis rejects `WATCH` inside `MULTI` without dirtying the transaction | Semantically opposite | In MiniRedis, the later `EXEC` aborts with `EXECABORT`; do not treat that as Redis behavior. |
| `ActiveExpireProducer` + expiry planning | `activeExpireCycle`, expiration logic historically associated with `expire.c` and `server.c` | Intentional simplification | Both combine lazy visibility with bounded active sampling; MiniRedis uses an injected scheduler and deterministic cursor-sized work. |
| replica expiry suppression | Redis replicas logically hide expired keys but wait for the primary's propagated deletion | Equivalent | A replica never creates its own expiry-delete commit, preventing its sequence from diverging from the primary. |
| `core/eviction.py` | `evict.c` maxmemory enforcement | Intentional simplification | The policy is enforced before committing the triggering write, but MiniRedis uses exact deterministic ordering and logical bytes instead of sampling and allocator memory. |
| `core/pubsub.py` + outbox | `pubsub.c` and per-client output buffers | Intentional simplification | Fan-out is bounded and at-most-once, with exact channels only and no pattern subscriptions. |
| AOF writer and rewrite | `aof.c`, append-only propagation, and background AOF rewrite | Intentional simplification | Base-plus-delta and atomic replacement preserve the rewrite lesson; the file format and execution model are custom. |
| snapshot and staged recovery | `rdb.c`, RDB loading, and AOF replay | Intentional simplification | A stable checkpoint is installed before contiguous later batches; MiniRedis's snapshot is not RDB compatible. |
| replication backlog | replication backlog in `replication.c` | Intentional simplification | Both retain bounded history for resume. MiniRedis retains whole logical batches rather than bytes in the PSYNC wire stream. |
| `ReplicaSink` | a replica link / client state in `replication.c` | Intentional simplification | It models async delivery, disconnect, cursor retention, catch-up, and failure, but stays in process and has no ACK or network protocol. |
| partial/full attachment | PSYNC partial resynchronization and full synchronization | Equivalent | Matching history identity plus complete backlog coverage permits catch-up; otherwise the replica receives a full snapshot. |
| `runtime.py` | Redis server lifecycle and subsystem ownership | Intentional simplification | Runtime owns startup, worker failure, draining, and zero-owner shutdown without reproducing Redis process management. |
| RESP2/TCP adapter | networking layer, RESP parsing, and client reply buffers | Intentional simplification | It proves protocol interoperability and ordered pipelines, not production throughput or RESP3 compatibility. |

## MiniRedis terminology

| Term | Meaning in this repository |
|---|---|
| **Direct-first** | Direct Python calls and RESP2/TCP requests share the same parser and executor; the network adapter does not define semantics. |
| **planner** | A pure command implementation that reads a database view and returns an `ExecutionPlan`. |
| **execution plan** | The immediate reply, proposed commit operations, access touches, and commit trigger for one command. |
| **prepared commit** | A non-empty, sequence-free set of operations ready to cross the durability barrier. |
| **commit batch** | The immutable, sequence-numbered unit applied locally, appended to AOF, retained in the backlog, and offered to a replica. |
| **commit barrier** | The durability boundary that must accept a batch before the executor applies it and releases success. |
| **mailbox turn** | One serialized executor step. Capturing related state in the same turn prevents commands from interleaving with that decision. |
| **sink** | The primary-side asynchronous link that installs or resumes a replica and applies offered batches in order. |
| **replication cursor** | `(replication_id, applied_seq)`, identifying both a history and the last fully applied batch. |
| **history fence** | A new replication ID after restart or promotion, preventing a cursor from an old primary history from being resumed accidentally. |
| **state base** | A complete logical database image at an AOF rewrite checkpoint, followed by later batch records. |
| **rewrite delta** | Commits that arrive after the rewrite base snapshot; they are appended before the new AOF becomes authoritative. |
| **logical memory** | A deterministic byte estimate used for teaching eviction, not Python allocator usage or RSS. |
| **logical expiry** | An expired key behaves as absent even if its physical entry remains until lazy or active deletion. |
| **owner** | A runtime component responsible for closing a task, timer, session, outbox, or replica link during shutdown. |

## Where the model deliberately costs more

`Database.apply_batch` stages a shallow copy of the whole key table and
recomputes total logical usage, so a write is O(N) in the number of keys.
Transaction execution additionally uses `Database.fork()`, which deep-copies
all values, so `EXEC` is O(N + transaction work) before applying its final
batch. These choices make failed planning non-mutating and make invariants easy
to inspect. Real Redis normally mutates its dictionaries and memory counters in
place, making an ordinary single-key update O(1) on average (apart from the
command's own data-structure work).

These complexity differences are teaching scaffolding, not performance claims.

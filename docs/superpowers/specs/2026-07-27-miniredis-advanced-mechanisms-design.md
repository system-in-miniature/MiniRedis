# MiniRedis Advanced Mechanisms Design

Date: 2026-07-27

Status: Historical design record

## 1. Context

MiniRedis is a domain-mechanism-first implementation of an in-memory,
typed data-structure server. Its core is intentionally independent of TCP and
RESP2:

```text
Direct adapter ──┐
                 ├── CommandRequest → CommandExecutor → Redis Core
RESP2 adapter ───┘
```

The existing project already provides typed values, one serialized command
executor, String/Hash/List/Set/ZSet commands, TTL, LRU-like eviction, blocking
list operations, Pub/Sub, AOF, snapshots, full asynchronous replication,
promotion, and thin Direct and RESP2 adapters.

This design fills the remaining domain-level gaps without changing the
project's identity. TCP/RESP remain adapters. Replication transport,
heartbeats, elections, and application-level cache patterns remain outside
the project.

## 2. Goals

The recorded scope added the following capabilities:

- `MGET`, `MSET`, `DECR`, and `BRPOP`;
- Direct and RESP2 pipelining semantics;
- `MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH`;
- two built-in atomic functions, `COMPAREDEL` and `CHECKDECR`;
- deterministic decaying LFU eviction;
- online AOF rewrite with concurrent-write capture and safe fallback;
- logical replication identity, bounded backlog, and partial resync;
- explicit lifecycle, observability, and failure contracts for these features.

The implementation must preserve:

- one serialized state-mutation owner;
- a transport-independent core;
- deterministic tests through injected time and failure hooks;
- crash and recovery semantics expressed in `CommitBatch` units;
- bounded queues and owned-task cleanup;
- Redis-shaped behavior where it adds conceptual value, without claiming full
  Redis compatibility.

## 3. Non-goals

This work does not add:

- RESP3;
- a Lua VM, scripting language, plugin API, or general atomic-function DSL;
- Redis RDB compatibility or Redis AOF file compatibility;
- production Redis's probabilistic LFU counter;
- TCP replication or PSYNC wire compatibility;
- automatic reconnect, heartbeat, failure detection, leader election,
  Sentinel, Cluster, slots, `MOVED`, or `ASK`;
- `WAIT`, quorum writes, or synchronous replication;
- application projects for cache penetration, breakdown, avalanche,
  Cache Aside, distributed locks, or rate limiting;
- course chapters. The project repository is completed first; the course is
  designed separately afterward.

## 4. Architecture and ownership

### 4.1 Command path

Both adapters produce `CommandRequest` values. Parsing produces typed command
objects. The `CommandExecutor` remains the sole owner of:

- database mutations;
- commit sequence allocation;
- session state;
- blocking waiter registration and wakeup;
- Pub/Sub connection state;
- transaction execution;
- snapshot and replication attachment barriers;
- backlog insertion and live replication offers.

No adapter may mutate the database or transaction state directly.

### 4.2 Component ownership

The extended ownership model is:

```text
MiniRedis runtime
├── CommandExecutor
│   ├── Database
│   ├── SessionState registry
│   ├── WaiterRegistry
│   ├── PubSubRegistry
│   ├── ReplicationBacklog
│   └── attached ReplicaSink registrations
├── AofWriter
│   ├── authoritative AOF descriptor
│   └── at most one rewrite job
├── SnapshotManager
├── DirectClient / DirectPipeline
└── TCP server / RESP2 sessions
```

`Database` owns volatile access metadata used by LRU/LFU. `AofWriter` owns all
AOF descriptors, temporary rewrite files, rewrite delta data, and atomic file
replacement. `ReplicaSink` owns downstream apply queues and its resume cursor.

### 4.3 Commit boundary

Every successful dataset mutation is represented by one `CommitBatch` with a
monotonically increasing logical `seq`. A batch is the common unit for:

- durable AOF append;
- atomic database application;
- replication backlog retention;
- full-sync boundary handoff;
- partial resync;
- replica application.

Network byte positions are not domain offsets in this project.

## 5. Replies and error model

The core reply model gains an explicit null-array reply in addition to the
existing simple string, bulk string, integer, array, null bulk, and error
forms:

```text
Ok
Bytes(value | None)
Number
Items
NullArray
Failure(code, message)
```

`NullArray` represents an `EXEC` aborted by a `WATCH` conflict. RESP2 maps it
to a null array. `Failure` remains an ordinary command result and must not
terminate the executor or transport.

Ordinary commands follow:

```text
parse and validate arguments
→ validate types and conditions
→ construct an ExecutionPlan
→ durably commit once if it mutates state
```

Argument errors, integer errors, `WRONGTYPE`, `OOM`, and failed atomic
conditions must not leave partial state.

## 6. Commands and pipelining

### 6.1 `MGET`

`MGET key [key ...]` returns one ordered array item per requested key.

- A live String returns its bytes.
- A missing or expired key returns null bulk.
- A key holding a non-String value also returns null bulk, matching the useful
  Redis behavior for heterogeneous bulk reads.
- Only live String values that are returned count as successful accesses.
- `MGET` never creates a commit.

### 6.2 `MSET`

`MSET key value [key value ...]` requires at least one pair and an even number
of arguments.

- All pairs are validated before planning.
- Duplicate keys use the last value in argument order.
- Existing values of any type are replaced with Strings.
- Existing TTLs on replaced keys are cleared.
- All resulting writes form one `CommitBatch`.
- Memory-policy failure rejects the entire command.

### 6.3 `DECR`

`DECR key` has the same behavior as `INCRBY key -1`, including missing-key,
integer parsing, overflow, TTL preservation, commit, and error behavior.

### 6.4 `BRPOP`

`BRPOP key [key ...] timeout` shares the blocking-list machinery used by
`BLPOP`, but removes from the right.

- Keys are checked in argument order.
- The immediate result is the first non-empty List.
- The reply is `[key, value]`.
- A timeout returns null bulk, consistent with the current blocking reply
  model.
- Waiters record both key order and pop direction.
- Push wakeups preserve existing exactly-once waiter ownership.
- Closing a session unregisters the waiter.

### 6.5 Direct pipeline

`DirectPipeline` is an adapter convenience, not a core batch command.

```text
pipeline.queue(request)
pipeline.execute() → tuple[Reply | None, ...]
```

`execute()` submits buffered requests in order without waiting between
submissions, then collects results in the same order. Individual errors occupy
their corresponding result positions. The pipeline is cleared after an
execution attempt.

The executor still sees independent commands. Commands from another session
may execute between them. Therefore a pipeline reduces submission and
round-trip overhead but adds no atomicity.

### 6.6 RESP2 pipeline

The RESP2 adapter accepts multiple complete command arrays from one read
buffer, submits them without waiting for each preceding reply, and emits
ordered replies. Existing streaming framing continues to handle partial and
coalesced reads. No pipeline opcode or core batch API is introduced.

Per-session admission and output bounds remain enforced. A client cannot use
one arbitrarily large coalesced payload to bypass `max_session_frames` or
outbox backpressure.

## 7. Transactions and built-in atomic functions

### 7.1 Session transaction state

Each executor session has:

```text
SessionState
├── queued_commands
├── transaction_active
├── transaction_dirty
├── watched_revisions
├── subscription state
└── blocking waiter references
```

`MULTI` starts a transaction and returns `OK`. Subsequent allowed commands are
parsed immediately, queued, and return `QUEUED`. Nested `MULTI` is an error.

`DISCARD` clears queued commands, exits transaction mode, clears the dirty
flag, and removes all watches. `UNWATCH` removes watches without entering or
leaving transaction mode.

`WATCH` is allowed only before `MULTI`. The following commands are not allowed
inside a transaction:

- `BLPOP` and `BRPOP`;
- `SUBSCRIBE`, `UNSUBSCRIBE`, and `PUBLISH`;
- `MULTI` and `WATCH`;
- lifecycle, persistence, or replication control operations.

### 7.2 Queue-time and execution-time errors

A syntax, arity, or command-admission error encountered while queuing marks
the transaction dirty. `EXEC` on a dirty transaction returns `EXECABORT`,
executes nothing, and clears transaction and watch state.

Execution-time errors such as `WRONGTYPE` are returned in the corresponding
`EXEC` result slot. They do not roll back earlier successful commands and do
not prevent later queued commands from running.

### 7.3 Transaction workspace

`EXEC` processes the queued commands in one executor turn. It does not read
the next mailbox message until the transaction finishes.

Commands are evaluated sequentially against a private transaction workspace
initialized from the current database:

- later commands observe earlier successful writes in the same transaction;
- a failed command leaves the workspace unchanged;
- TTL, expiration, type, and memory-policy decisions use the workspace state;
- per-command replies are retained in queue order;
- successful operations are accumulated in their execution order.

If no command mutates state, `EXEC` returns the reply array without a commit.
Otherwise all successful mutations are emitted as one final `CommitBatch`.
That batch is durably appended and atomically installed before the result is
published. This preserves non-interleaving and prevents AOF recovery from
observing only part of an executed transaction.

There is no rollback. One-batch persistence is a crash-atomicity boundary, not
a claim of database-style ACID transactions.

### 7.4 Revision ledger and `WATCH`

`Database` gains a monotonic per-key revision ledger independent of live
entries. Every committed logical change to a key advances its revision,
including:

- create;
- overwrite;
- delete;
- expiry deletion;
- eviction;
- transaction mutation;
- replicated mutation.

Deleting a key does not erase its revision. This allows `WATCH` to detect
create-then-delete changes that end with the key absent.

`WATCH key [key ...]` stores the current revision for each key. Immediately
before `EXEC`, the executor compares all watched revisions:

- if any differ, `EXEC` clears the transaction and watches and returns
  `NullArray`;
- if all match, execution proceeds;
- changes made by the same session outside the transaction count as changes;
- merely reading or touching LRU/LFU metadata does not advance a key revision.

Revision ledger state is reconstructed from committed mutations during
recovery and replication. Exact numeric revision identity across restart is
not a public compatibility promise; change detection within one running
history is.

### 7.5 `COMPAREDEL`

`COMPAREDEL key expected` is the minimal built-in primitive needed to
demonstrate safe lock release:

- missing key: return `0`;
- non-String key: `WRONGTYPE`;
- unequal bytes: return `0`;
- equal bytes: delete the key and return `1`.

Only the successful delete creates a commit. The check and delete occur in
one executor turn.

### 7.6 `CHECKDECR`

`CHECKDECR key amount` demonstrates atomic conditional stock decrement:

- `amount` must be a positive integer;
- missing key or current value smaller than `amount`: return an
  `INSUFFICIENT` error;
- non-String key: `WRONGTYPE`;
- invalid stored integer: the existing integer error;
- success: subtract `amount`, preserve the key's TTL, commit once, and return
  the remaining value.

There is no Lua VM or general function registration mechanism.

## 8. Deterministic decaying LFU

### 8.1 Configuration and metadata

`eviction_policy` additionally accepts `allkeys-lfu`.

New configuration:

```text
lfu_decay_interval_ms > 0
```

Each live `Database.Entry` gains volatile metadata:

```text
frequency
last_frequency_decay_ms
last_access_tick
```

A new key starts at frequency `1`. Successful reads and client-originated
writes touch the key. A touch first materializes elapsed decay and then
increments frequency.

Updating an existing key preserves its current LFU history: the executor
materializes decay, increments the counter, and carries that volatile metadata
onto the replacement entry. Replica application and recovery do not count as
client access.

### 8.2 Decay

For every complete decay interval since `last_frequency_decay_ms`, frequency
is halved with integer division:

```text
frequency = frequency // 2
```

The calculation may apply multiple elapsed intervals in one step. It uses the
injected `Clock`; tests do not sleep in wall-clock time.

Frequency is a deterministic, unbounded Python integer for teaching
semantics. It does not emulate Redis's logarithmic eight-bit counter.

### 8.3 Victim ordering

The LFU eviction planner computes a read-only effective projection at
`now_ms`; candidate inspection must not mutate database metadata.

Victims are ordered by:

1. lowest effective decayed frequency;
2. oldest `last_access_tick`;
3. binary key order.

Expired keys are removed before LFU candidates, as in existing memory
enforcement. Keys being written by the current plan are not eviction
candidates.

### 8.4 Persistence and replication

LFU frequency and access ticks are operational cache metadata. They are not:

- included in `StoredEntry`;
- written to AOF or snapshots;
- copied through replication.

Recovered or full-synced entries start with neutral metadata and relearn their
access pattern. Dataset values, TTLs, mutation versions, and commit sequences
remain durable.

## 9. Online AOF rewrite

### 9.1 File model

The rewritten MiniRedis AOF begins with one AOF-specific state-base record:

```text
AOF header
AofStateBase(
    checkpoint_seq=N,
    entries=stable sorted StoredEntry pairs
)
CommitBatch(seq=N+1)
CommitBatch(seq=N+2)
...
```

`AofStateBase` uses the canonical state carried by `SnapshotImage`, but it is
not a Redis RDB preamble and does not reuse the snapshot file as an external
dependency. AOF scanning and recovery accept either:

- the existing header followed only by commit batches; or
- the header, exactly one leading state base, then contiguous later batches.

A state base anywhere except the first record is corruption. A batch at or
below its checkpoint is corruption.

When both an external snapshot and an AOF state base exist, recovery selects
the complete baseline with the greater checkpoint sequence:

- if the snapshot checkpoint is newer, install it and apply only AOF batches
  above that checkpoint;
- if the AOF state base checkpoint is newer or equal, install the state base
  and apply its following batches;
- if there is no state base, preserve the existing snapshot-plus-batch
  recovery behavior.

This makes rewrite compatible with snapshots created before or after it.

### 9.2 Starting without a capture gap

`MiniRedis.rewrite_aof()` posts `BeginAofRewrite` to the executor. In one
executor turn the executor:

1. captures stable entries and checkpoint `seq=N`;
2. asks `AofWriter` to register an active rewrite at `N`;
3. resumes ordinary mailbox processing only after registration succeeds.

This prevents a write from landing between state capture and delta capture.
Only one rewrite may be active.

The API returns a structured `Saved`, `Busy`, or `Failed` outcome. No
automatic size threshold and no special RESP `BGREWRITEAOF` command are added
in this scope.

### 9.3 Concurrent writes

The current AOF remains the durability authority throughout rewrite.

For every new committed batch:

1. its record is written to the current AOF under the configured fsync policy;
2. after the ordinary append barrier succeeds, the encoded record is retained
   in the bounded rewrite delta;
3. the client-visible commit continues normally.

The background rewrite job writes a temporary file containing the header and
state base. It does not own or mutate the current AOF descriptor.

### 9.4 Finalization barrier

When the base file is ready, finalization is enqueued into the same ordered
`AofWriter` work queue used for appends.

All append work before the finalization item has therefore reached both the
old AOF and the rewrite delta. The finalization item:

1. appends the complete ordered delta to the temporary file;
2. fsyncs the temporary file;
3. atomically renames it over the configured AOF path;
4. fsyncs the parent directory;
5. installs the already-open temporary descriptor as the new append
   descriptor;
6. closes the old descriptor;
7. clears rewrite state.

Append items ordered after finalization write to the new AOF. This creates one
unambiguous descriptor-switch boundary.

### 9.5 Bounded failure behavior

New configuration:

```text
aof_rewrite_delta_limit_bytes > 0
```

If the delta would exceed this limit, the rewrite aborts. Temporary-file
creation, base writing, temporary-file fsync, or a failed atomic-rename call
also aborts the rewrite while the old path and descriptor are still
authoritative.

An aborted rewrite:

- deletes its temporary file when possible;
- leaves the old AOF and current descriptor authoritative;
- reports `Failed`;
- does not fail the runtime solely because the optimization failed.

Ordinary append or required fsync failure on the authoritative AOF retains
the existing terminal durability-failure behavior.

After atomic rename succeeds, fallback to the old descriptor is no longer
safe because it refers to an unlinked history. A parent-directory fsync or
descriptor-installation failure after that point is therefore terminal rather
than a non-terminal rewrite failure. The writer must not acknowledge later
commits against the obsolete descriptor.

### 9.6 Shutdown and crash

Graceful shutdown orders itself with the writer:

- if finalization has begun, it completes the switch;
- if only base generation is active, it may cancel and clean up the rewrite;
- it then performs the normal configured flush and descriptor close.

Crash simulation does not grant the rewrite an extra durability flush. A
fully renamed file or the prior authoritative file must remain recoverable.
No rewrite task, descriptor, or temporary file may leak after graceful close.

## 10. Replication backlog and partial resync

### 10.1 Logical replication identity

Each writable Primary has an opaque `replication_id` representing one
replication history. Tests may inject a deterministic ID; production runtime
creation generates one.

A Replica resume cursor is:

```text
ReplicationCursor(replication_id, applied_seq)
```

`applied_seq` is the last completely installed `CommitBatch.seq`. It is not a
byte count.

A fresh Primary process and a promoted Replica generate a new
`replication_id`. Therefore restart and promotion fence old histories.

### 10.2 Bounded backlog

The executor owns:

```text
ReplicationBacklog
├── capacity_batches
├── oldest_seq
├── newest_seq
└── deque[CommitBatch]
```

New configuration:

```text
replication_backlog_batches > 0
```

After a successful commit is durably accepted and applied locally, the
executor appends the batch to the backlog and offers it to online Replica
sinks. Capacity is measured in batches. Oldest batches are dropped first.
The backlog is operational memory and is not persisted.

### 10.3 Resume decision

A reconnecting Replica submits its cursor. Partial resync is allowed only
when:

- cursor `replication_id` equals the Primary's current ID;
- cursor `applied_seq` is not greater than the Primary's current sequence;
- every batch in `applied_seq + 1 ... primary_seq` is still available.

If all conditions hold, the result is `CONTINUE` with the missing batch
sequence. An already-current Replica receives an empty catch-up sequence and
enters live streaming.

Any ID mismatch, future sequence, or backlog gap produces `FULLRESYNC`.

### 10.4 Race-free attachment

Full-sync attachment is one executor control turn:

```text
capture SnapshotImage at seq=N
→ register the live sink at boundary N
```

The Replica installs the image, applies queued batches `N+1...`, then streams.

Partial-resync attachment is also one executor control turn:

```text
validate cursor and backlog coverage
→ capture the missing ordered batches
→ register the live sink at current boundary
```

The Replica applies the captured backlog range before the live queue. New
writes may continue during either sync path, but the sink applies strictly
increasing contiguous sequence numbers without duplicates.

### 10.5 Slow Replica behavior

The existing positive `replica_queue_limit` remains the canonical bound,
measured in `CommitBatch` items. No second, overlapping live-queue setting is
introduced. If a live queue overflows:

- the sink drops queued live data;
- enters `NEEDS_RESYNC`;
- stops receiving ordinary offers;
- retains the last completely applied cursor;
- does not block or fail the Primary.

Manual reattachment may use partial resync if the backlog still covers the
gap; otherwise it falls back to full sync.

### 10.6 Promotion fencing

Manual promotion:

- keeps the Replica's current dataset and commit sequence;
- makes it writable;
- creates a new `replication_id`;
- clears the inherited backlog;
- begins the new backlog with future commits.

Any source or Replica carrying the old identity must full-sync to the promoted
Primary. Equal visible state is not sufficient proof that histories did not
diverge.

### 10.7 Preserved asynchronous failure semantic

Backlog improves reconnect efficiency only while the same Primary history
exists. It does not strengthen acknowledgement semantics:

```text
Primary commits seq=42 and replies success
→ Replica has applied only seq=41
→ Primary crashes
→ Replica is manually promoted
→ seq=42 is absent
```

This experiment remains a required acceptance test.

## 11. Configuration and validation

The configuration object adds:

```text
eviction_policy = noeviction | allkeys-lru | allkeys-lfu
lfu_decay_interval_ms
replication_backlog_batches
aof_rewrite_delta_limit_bytes
```

All sizes and intervals must be positive. Invalid configuration fails before
runtime startup. Runtime `CONFIG SET` is not added.

The existing positive `replica_queue_limit` remains unchanged and continues
to bound each Replica sink's live `CommitBatch` queue.

## 12. Lifecycle and observability

### 12.1 Session close

Closing a session:

- discards any queued transaction;
- clears watched revisions;
- unregisters BLPOP/BRPOP waiters;
- removes subscriptions;
- settles owned outbound state;
- does not undo committed commands.

### 12.2 Graceful shutdown

The shutdown order is:

```text
stop user admission
→ drain accepted executor work
→ terminate session waiters and subscriptions
→ stop replica propagation
→ settle or cancel AOF rewrite safely
→ flush and close authoritative AOF
→ close snapshot/runtime resources
```

Crash simulation intentionally skips graceful flushing and is used to test
durability windows.

### 12.3 Runtime statistics

`RuntimeStats` is extended with read-only fields sufficient to inspect:

- key count and logical memory usage;
- expiration and eviction counters;
- active AOF rewrite state and delta bytes;
- replication ID and Primary sequence;
- backlog oldest/newest sequence and batch count;
- full-sync and partial-sync counts;
- Replica cursor, lag, queue size, and state;
- active transactions, watches, waiters, subscriptions, and sessions;
- transaction abort counts;
- owned tasks and resource counts.

Diagnostics do not mutate LFU/LRU state and are not exposed as new Redis
commands in this scope.

## 13. Testing strategy

Each capability is developed test-first and verified at four levels where
applicable:

1. pure unit tests for parsing, planning, codecs, and deterministic ordering;
2. contract tests through `DirectClient`;
3. selected Direct/RESP2 parity and streaming pipeline tests;
4. reliability tests with injected time, gates, file failures, shutdown, and
   crash simulation.

No correctness test depends on arbitrary sleeps.

### 13.1 Phase A: commands and pipeline

Recorded implementation shape:

- `MGET`, `MSET`, `DECR`;
- `BRPOP`;
- `DirectPipeline`;
- coalesced RESP2 pipeline behavior.

Acceptance includes ordered replies, per-item errors, cross-session
interleaving, MSET's single commit, BRPOP direction and key order, bounded
session admission, and full regression.

### 13.2 Phase B: transactions and atomic functions

Recorded implementation shape:

- session transaction state;
- revision ledger;
- `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`;
- `COMPAREDEL`, `CHECKDECR`;
- transaction workspace and one-batch commit.

Acceptance includes non-interleaving, dirty queue abort, runtime-error
continuation, read-your-prior-write behavior, WATCH create-delete detection,
null-array mapping, one AOF batch, replication of the one batch, and session
cleanup.

### 13.3 Phase C: LFU

The recorded implementation provided deterministic decay, LFU projections, victim selection, and
configuration.

Acceptance includes FakeClock decay, stable tie-breaking, comparison of
long-term frequency and recency, no mutation during eviction planning, and
neutral metadata after recovery/full sync.

### 13.4 Phase D: online AOF rewrite

The recorded implementation provided the state-base codec, recovery, rewrite registration, background
base generation, bounded delta, ordered finalization, atomic replacement, and
failure cleanup.

Acceptance includes:

- identical state and sequence before and after rewrite;
- writes while base generation is paused;
- writes ordered on both sides of finalization;
- delta overflow fallback;
- temporary write, fsync, rename, and switch failure injection;
- close and crash behavior;
- recovery when snapshots also exist;
- no leaked task, descriptor, or temporary file.

### 13.5 Phase E: backlog and partial resync

The recorded implementation provided logical identity, bounded batch backlog, resume decisions, ordered
catch-up, full-sync fallback, and promotion fencing.

Acceptance includes:

- short disconnect partial resync;
- long disconnect full-sync fallback;
- exact oldest/newest backlog boundaries;
- identity mismatch and future cursor;
- writes during full and partial sync;
- queue overflow followed by both partial and full recovery paths;
- promotion rejecting old history;
- restart requiring full sync;
- preserved acknowledged-write-loss experiment.

Every phase ends with the complete existing and new test suite green,
behavior-matrix and README updates, and an independently reviewable commit.

## 14. Final acceptance

The finished repository must provide executable evidence for:

| Capability | Required evidence |
|---|---|
| Atomic data commands | concurrent `INCR`; one-batch `MSET` |
| Pipeline | ordered batch submission with allowed client interleaving |
| Transactions | non-interleaved `EXEC`; runtime errors without rollback |
| WATCH | revision conflict produces null array |
| Atomic functions | safe compare-delete and conditional decrement |
| TTL and eviction | expiration remains separate from deterministic LFU |
| AOF rewrite | concurrent writes plus recoverable failure fallback |
| Full sync | stable state boundary followed by later batches |
| Partial sync | backlog-only gap recovery |
| Async replication limit | acknowledged write can still be lost |
| Lifecycle | no session, waiter, task, descriptor, or temp-file leak |
| Adapter separation | core and primary contracts run without TCP/RESP |

The resulting project remains a MiniRedis domain-mechanism lab, not a
production Redis clone or a general distributed-systems runtime.

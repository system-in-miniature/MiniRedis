> **Language**: English | [简体中文](../zh/tutorial/08-replication.md)

# Replication: Backlogs, Resynchronization, and Promotion

## Learning objectives

By the end of this chapter, you will be able to:

- describe a replication cursor as both a history identity and an applied
  sequence;
- derive when `ReplicationBacklog.missing_after` permits partial
  resynchronization;
- trace full and partial attachment through `ReplicaSink`;
- explain why asynchronous acknowledgment permits confirmed write loss during
  promotion; and
- run experiments that demonstrate partial resume, full fallback, and a lost
  acknowledged write.

## Replication reuses the commit stream

Persistence and replication answer different questions. AOF asks, “Can this
runtime reconstruct its state after restart?” Replication asks, “Can another
runtime apply the same ordered changes?” MiniRedis intentionally gives them
the same unit: `CommitBatch`.

After `src/miniredis/core/executor.py`,
`CommandExecutor._commit_prepared`, crosses the durability barrier and applies
the batch locally, it appends the batch to `ReplicationBacklog` and calls
`_offer_replica_batch`. The primary does **not** wait for the replica to apply
it before replying to the client. This is asynchronous replication.

The simplification is important: MiniRedis replication is in-process. A
`ReplicaSink` models the primary-side link and calls methods on another
`MiniRedis` runtime. There is no RESP `PSYNC`, replication socket, heartbeat,
ACK stream, `WAIT`, election, Sentinel, or Cluster. The
[Replica row in the behavior matrix](../behavior-matrix.md) states those
limits explicitly.

What remains is a surprisingly rich core: identity, ordered offsets, bounded
history, disconnect, catch-up, overflow, full-state installation, and manual
promotion.

## A cursor needs identity and position

`src/miniredis/replication/backlog.py`, `ReplicationCursor`, contains:

```python
@dataclass(frozen=True, slots=True)
class ReplicationCursor:
    replication_id: str
    applied_seq: int
```

The sequence alone is insufficient. Two primaries can both be at sequence 20
while representing unrelated histories. The `replication_id` fences those
histories. `CommandExecutor` creates a new ID for a primary history; restart
creates a new runtime and ID, and
`CommandExecutor.promote_replica` creates another ID when a replica becomes
writable.

`applied_seq` means the last **fully applied** batch. It is not the newest
batch seen, queued, or partially processed. `src/miniredis/replication/sink.py`,
`ReplicaSink._run_apply`, advances the sink cursor only after
`MiniRedis.apply_replica_batch` succeeds. Disconnect or failure can therefore
retain a trustworthy resume point.

This pair resembles Redis replication ID plus offset, but MiniRedis offsets are
logical batch sequence numbers, not byte offsets in a wire replication stream.

## The bounded backlog and the resume equation

`src/miniredis/replication/backlog.py`, `ReplicationBacklog`, stores a deque of
whole batches. `append` requires contiguous sequences and explicitly removes
oldest items with `popleft` while the configured batch capacity is exceeded.

The executor first checks the replication ID, then calls
`missing_after(applied_seq, current_seq=...)` to decide whether a cursor can
resume:

1. the replication ID must match;
2. the cursor cannot be in the future (`applied_seq > current_seq`);
3. a cursor at `current_seq` needs no batches and is a valid empty partial
   sync;
4. otherwise the backlog must not be empty;
5. the first retained batch must be no later than `applied_seq + 1`; and
6. retained batches from that point must run contiguously through
   `current_seq`.

For example, if the primary is at 6 and retains batches 4, 5, and 6:

- cursor 6: partial, empty suffix;
- cursor 5: partial with batch 6;
- cursor 3: partial with batches 4–6;
- cursor 2: full sync, because batch 3 is gone;
- cursor 7: full sync, because the cursor claims future history;
- any different replication ID: full sync.

The boundary “cursor 3 is covered when oldest batch is 4” follows directly
from the meaning of `applied_seq`: the replica already has everything through
3 and needs 4 next.

## Full attachment versus partial attachment

`src/miniredis/core/executor.py`, `CommandExecutor.attach_replica`, runs in the
primary executor mailbox. It asks the backlog about the supplied cursor. A
covered cursor produces `PartialSyncAttachment` with the missing batches. Any
unusable cursor produces `FullSyncAttachment` with a stable `SnapshotImage`.
The attachment also registers a generation, preventing messages from an older
link incarnation from mutating current state.

`src/miniredis/replication/sink.py`, `ReplicaSink.attach`, performs different
handshakes:

- **Full:** call `primary.install_replica_snapshot`, then publish a cursor for
  that successfully installed image.
- **Partial:** call `primary.prepare_replica_resume` to verify that the
  replica's current state still matches the cursor, then enqueue the missing
  batches before accepting live batches.

Publishing the cursor only after full installation prevents a failed snapshot
from appearing resumable. Preparing a partial resume prevents a cursor from
being trusted merely because the sink object claims it. If validation fails,
the link is detached.

Catch-up and live delivery share one ordered sink queue.
`ReplicaSink.offer` rejects a full queue and marks the sink as needing
resynchronization. `ReplicaSink._run_apply` separately checks that the next
dequeued batch is exactly `applied_seq + 1`; a mismatch also enters resync.
Neither overflow nor a gap silently skips a batch. On the next attach,
`missing_after` either supplies the complete suffix or selects a full image.

The sink status exposes `lag = primary_seq - applied_seq`, but it is an
observation, not an acknowledgment quorum. A lag of zero says this sink caught
up at the moment observed. It does not turn a prior client reply into a
replicated-write guarantee.

## Promotion and history fencing

`src/miniredis/replication/sink.py`, `ReplicaSink.promote`, requires the sink
to be in a promotable state and requires the caller to state whether the source
is still alive. If `source_alive=True`, it explicitly detaches the live source;
if false, it proceeds from the source-lost state. It then asks the replica
runtime to promote its current applied sequence.
`src/miniredis/core/executor.py`, `CommandExecutor.promote_replica`, makes the
database writable, assigns a new replication ID, and clears the backlog.

The new ID is essential. Suppose the old primary history was ID A at sequence
10, while a promoted replica had applied only through 8. Reusing ID A would
let another node present an A/10 cursor even though the new primary never
contained commits 9–10. The new ID declares a new lineage at the promoted
state.

Promotion is manual and does not choose the “best” replica. MiniRedis has no
election or majority term. That omission sets up the most important failure
experiment in this chapter.

## Why an acknowledged write can be lost

The primary replies after local commit, not after replica apply.
`CommandExecutor._commit_prepared` offers a batch through
`_offer_replica_batch`, but `ReplicaSink.offer` only enqueues it. If the sink is
paused or slow, the primary can acknowledge while replica lag is positive.

Now let the primary crash before the sink applies that batch. The surviving
replica's cursor still names the last fully applied sequence. Promoting it must
not invent the missing value. The client saw `OK`; the new primary does not
contain the write. This is **confirmed write loss** under asynchronous
replication, explicitly tested by
`tests/reliability/test_lost_acked_write.py::test_acknowledged_primary_write_can_be_lost_on_lagging_promotion`.

This result is not a MiniRedis bug. It is the guarantee being modeled. Real
Redis asynchronous replication has the same fundamental window. Redis offers
operational controls such as `WAIT` and `min-replicas-to-write` /
`min-replicas-max-lag`, but these do not transform an asynchronous primary into
a consensus system with linearizable failover.

The lesson connects MiniRedis to the broader MiniDist family: replication,
durability, failure detection, leader choice, and client acknowledgment are
separate protocol decisions. Copying state to a follower is not, by itself, a
proof that an acknowledged write will survive failover.

## Compared with real Redis

Redis replication is implemented primarily in `src/replication.c`. A replica
uses `PSYNC` with replication IDs and offsets; the primary maintains a
byte-oriented backlog. If the requested history is still present, it streams a
partial suffix. Otherwise, full synchronization transfers an RDB snapshot and
then buffered commands. `REPLICAOF`, `ROLE`, `INFO replication`, and
`WAIT` expose operational controls and state.

MiniRedis preserves the decision rule—matching identity plus complete retained
history permits partial resync—and the safety rule that a gap forces full
state. It simplifies almost everything around that rule:

- batch sequence numbers replace byte offsets;
- `ReplicaSink` calls another runtime in-process;
- attachment is a Python API, not `PSYNC`;
- sinks are in-process objects rather than a networked replica fleet;
- there are no periodic ACKs, heartbeats, diskless sync, cascading replicas,
  authentication, or automatic failover; and
- promotion is explicit and always creates a history fence.

The [Replica row](../behavior-matrix.md) is the compatibility contract. Do not
describe the example output as network replication evidence.

## Hands-on lab 1: partial resume and full fallback

Run the repository example:

```bash
uv run python examples/replication_resync.py
```

Measured output:

```text
Short disconnect: complete history is still in the backlog.
1. Initial attachment: full
   SET b'a' -> Ok(message=b'OK')
   SET b'b' -> Ok(message=b'OK')
2. Short disconnect resumed with: partial
   Expected partial: True
   Replica GET b: Bytes(value=b'2')

Long disconnect: a required batch has fallen out of the backlog.
   SET b'old' -> Ok(message=b'OK')
   SET b'k2' -> Ok(message=b'OK')
   SET b'k3' -> Ok(message=b'OK')
   SET b'k4' -> Ok(message=b'OK')
3. Cursor older than backlog resumed with: full
   Expected full: True
   Replica GET k4: Bytes(value=b'4')
```

Both primaries use `replication_backlog_batches=2`. During the short
disconnect, only batch 2 is missing and retained, so the cursor resumes
partially. During the long disconnect, three new batches push the required
oldest batch out of the two-batch deque. Full sync installs a current image,
which is why `k4` is present.

## Hands-on lab 2: demonstrate confirmed write loss

Save this as `/tmp/miniredis_lost_ack.py`:

```python
import asyncio

from miniredis import CommandRequest, MiniRedis
from miniredis.replication.sink import ReplicaSink


async def main():
    primary = MiniRedis.open()
    replica = MiniRedis.open()
    await primary.start()
    await replica.start()
    sink = ReplicaSink(replica, queue_limit=4)
    attached = await primary.attach_replica(sink)
    print("attached:", attached.sync_mode, "seq", attached.applied_seq)

    sink.pause()
    reply = await primary.direct_client().execute(
        CommandRequest(b"SET", (b"x", b"1"))
    )
    print("primary acknowledged:", reply)
    print("replica lag before crash:", sink.status.lag)

    await primary.simulate_crash()
    promoted = await sink.promote(source_alive=False)
    print("promoted at seq:", promoted.applied_seq)
    value = await replica.direct_client().execute(
        CommandRequest(b"GET", (b"x",))
    )
    print("GET x after promotion:", value)
    print("confirmed write lost:", value.value is None)
    await replica.close()


asyncio.run(main())
```

Run:

```bash
uv run python /tmp/miniredis_lost_ack.py
```

Measured output:

```text
attached: full seq 0
primary acknowledged: Ok(message=b'OK')
replica lag before crash: 1
promoted at seq: 0
GET x after promotion: Bytes(value=None)
confirmed write lost: True
```

This is a deliberately negative reliability result. The `OK` is real, and the
absence after promotion is real. The sink's cursor correctly stays at zero
because queued batch 1 was never applied.

Run the focused contract tests as well:

```bash
uv run pytest -q tests/replication/test_partial_resync.py \
  tests/replication/test_promotion.py \
  tests/reliability/test_lost_acked_write.py
```

Expected repository result:

```text
18 passed
```

## Exercises

### 1. Understanding: calculate backlog coverage

The primary ID matches, `current_seq=20`, and the backlog retains batches
17–20. Classify cursors at sequences 20, 16, 15, and 21.

??? note "Reference answer"

    Cursor 20 is an empty partial sync. Cursor 16 is partial with batches
    17–20. Cursor 15 requires batch 16, which is gone, so it receives full
    sync. Cursor 21 is in the future and also receives full sync.

### 2. Understanding: explain the new ID on promotion

Why is clearing the backlog insufficient if promotion keeps the old
replication ID?

??? note "Reference answer"

    A remote cursor could still claim the old ID and a sequence that refers to
    commits the promoted replica never applied. A new ID fences every cursor
    from the prior lineage. Clearing only local retained batches prevents
    replay but does not make old cursor identities unambiguous.

### 3. Hands-on: test exact-current partial sync

Task boundary: add a test under `tests/replication/` that disconnects a
caught-up sink and immediately reattaches it without another write. Do not
modify `src/`.

Acceptance: assert `ReplicaSyncMode.PARTIAL`, unchanged `applied_seq`, zero
lag, and unchanged replica contents.

??? note "Reference answer"

    Follow the setup in `tests/replication/test_partial_resync.py`. Attach,
    write one key, wait through sequence 1, disconnect, and attach again. The
    returned partial attachment has an empty missing suffix; this is valid
    because `applied_seq == current_seq`.

### 4. Hands-on: force queue-overflow resync

Task boundary: create a sink with `queue_limit=1`, pause it, and perform enough
primary writes to overflow its queue. Use only public/debug test APIs; do not
change production code.

Acceptance: prove the sink enters `needs-resync`, never advances its cursor for
the unapplied batch, and later attachment either catches up from complete
backlog history or falls back to full sync after a backlog gap.

??? note "Reference answer"

    Use the structure in
    `tests/replication/test_sink_overflow.py`. Capture the cursor before
    pausing, issue two writes, and assert the cursor remains unchanged. For the
    partial variant, configure a backlog large enough for both batches. For the
    full variant, use capacity one and add another batch before reattachment.
    The expected diff is test-only.

## Summary

MiniRedis replication copies the ordered logical commit stream through an
in-process asynchronous sink. A cursor is trustworthy only when both its
history ID and last fully applied sequence match; a bounded backlog enables
partial resume only when it contains the entire missing suffix. Promotion
fences old history with a new ID, but it cannot recover a batch the replica
never applied—so asynchronous acknowledgment can produce confirmed write
loss. The next chapter returns to a single runtime and studies two other forms
of coordination: grouping commands into one commit and suspending a request
until data arrives.

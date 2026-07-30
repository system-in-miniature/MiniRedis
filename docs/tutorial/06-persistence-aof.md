> **Language**: English | [简体中文](../zh/tutorial/06-persistence-aof.md)

# Persistence I: Append-Only Files

## Learning objectives

By the end of this chapter, you will be able to:

- trace one successful write from a `CommitBatch` to the AOF durability barrier;
- distinguish the guarantees of `always`, `everysec`, and `no` without calling all three “durable”;
- explain why MiniRedis repairs only an incomplete final record, not arbitrary corruption;
- run a crash-and-restart experiment and identify the exact point at which the write became acknowledged; and
- design a bounded change to AOF policy handling without modifying the command planners.

## From an in-memory commit to a durable history

An in-memory database can acknowledge `SET` and still forget it when the
process dies. AOF persistence addresses that gap by recording every committed
mutation in order. The important word is *committed*: MiniRedis does not log
the original command text. It logs the same immutable `CommitBatch` that is
applied to the database and later offered to replication.

That choice is visible in `src/miniredis/persistence/aof.py`,
`AofWriter.append`. The method receives a `CommitBatch`, encodes it with
`encode_aof_record`, places `_AppendWork` on the writer queue, and awaits a
future called `barrier`.

```python
barrier = asyncio.get_running_loop().create_future()
self._queue.put_nowait(
    _AppendWork(
        record=encode_aof_record(batch),
        seq=batch.seq,
        barrier=barrier,
    )
)
return await asyncio.shield(barrier)
```

The future matters more than the queue. Enqueuing a write merely transfers
ownership to the AOF worker; it does not mean the bytes have reached the
configured durability level. In `src/miniredis/core/executor.py`,
`CommandExecutor._commit_prepared`, the executor assigns the next sequence
number, awaits `commit_barrier.append(batch)`, and applies the batch only after
an `AofAppendOk`. A failed append becomes a `DurabilityFailure`, so the client
does not receive a false success while the in-memory database moves ahead of
the log.

This produces a useful order:

```text
plan mutations
  -> create CommitBatch(seq)
  -> await AOF barrier
  -> apply batch to Database
  -> retain/offer it for replication
  -> publish successful reply
```

When no AOF is configured, `NullCommitBarrier.append` in
`src/miniredis/core/executor.py` immediately returns `AofAppendOk`. The command
pipeline is unchanged; only the barrier implementation changes. This is a
small example of why the project is “Direct-first”: persistence is a core
execution concern, not a feature of either the Python or TCP adapter.

## A custom, framed, binary-safe file

MiniRedis AOF is deliberately **not** a Redis-compatible command stream. The
file begins with a versioned header and contains framed logical records.
`src/miniredis/persistence/codec.py`, `encode_aof_record`, serializes a
`CommitBatch`; `_encode_aof_payload_record` adds a length and checksum.
`scan_aof_bytes` walks complete frames and returns the valid prefix, decoded
state base, decoded batches, and whether the last frame was truncated.

Framing solves two different problems:

1. the length says where one record ends, even when values contain arbitrary
   bytes; and
2. the checksum distinguishes an intact record from corrupted content.

The batch sequence number is equally important. During recovery,
`src/miniredis/persistence/recovery.py`, `recover_database`, installs a staged
base and accepts only contiguous later batches. If the staged database is at
sequence 7, the next record must be sequence 8. A gap or reordering is a hard
error rather than an invitation to guess.

The staging database prevents partial startup. Recovery does not expose a
runtime and then mutate it record by record. It builds a new `Database`,
installs the chosen snapshot image, replays valid later batches, drops keys
that are expired at the recovery clock, and returns the completed database.
`src/miniredis/runtime.py`, `MiniRedis._start_owned`, installs that database
before opening user admission.

## Three fsync policies, three different promises

Writing bytes and making them survive power loss are different operations.
MiniRedis makes the policy explicit through `AofPolicy` in
`src/miniredis/persistence/aof.py`.

| Policy | What `AofWriter._run_writer` does before acknowledging | Crash exposure |
|---|---|---|
| `always` | writes the frame and calls `fsync` | acknowledged batches have crossed the file fsync barrier |
| `everysec` | writes the frame, marks the descriptor dirty, and acknowledges | roughly the unsynced interval may be lost on an abrupt crash |
| `no` | writes the frame and acknowledges without an application fsync | the operating system decides when dirty data reaches stable storage |

For `everysec`, `AofWriter.start` creates `_run_everysec`. That task sleeps for
the configured interval and calls `_sync_dirty`. Graceful `AofWriter.close`
also syncs remaining dirty data. In contrast, `AofWriter.crash_close`
deliberately does not perform that final sync. This difference is how
`MiniRedis.simulate_crash` models abrupt process loss without pretending to
model a machine, filesystem, or storage controller perfectly.

Do not overread `AofAppendOk`. Under `always`, it means the writer completed
the configured fsync. Under `everysec` and `no`, it means the record was
accepted at their weaker barrier. The word “acknowledged” therefore describes
the server/client protocol event; the policy defines the persistence promise.

The default in `src/miniredis/config.py`, `MiniRedisConfig`, is `everysec`.
This is a teaching choice with a familiar Redis trade-off: much lower fsync
frequency than `always`, but a bounded intended loss window rather than no
application-level sync at all.

## Crash recovery and the one safe repair

Open `src/miniredis/persistence/aof.py`, `load_aof`. A missing or zero-byte AOF
is treated as empty. A complete file is returned unchanged. If the scanner
finds an incomplete **final** frame and `repair_truncated_tail=True`,
`load_aof` truncates the file to `scan.valid_offset` and fsyncs it.

This repair is conservative. A process can die after writing only part of the
last frame, so removing that suffix has a clear interpretation: preserve every
complete earlier commit and discard a commit that never became a complete
record. A checksum failure in a complete frame is different. It may indicate
bit corruption in the middle of history. `load_aof` raises `AofCorruption`
instead of deleting an unknown amount of data.

The distinction is verified by
`tests/unit/persistence/test_aof_repair.py`:
`test_repair_enabled_truncates_one_incomplete_tail` preserves the first record
and truncates the second; `test_checksum_corruption_never_changes_the_file`
requires an error and byte-for-byte preservation. The relevant compatibility
boundary is the [AOF row in the behavior matrix](../behavior-matrix.md):
the mechanism is represented, but the format is custom.

## Compared with real Redis

Real Redis implements AOF machinery primarily in `src/aof.c`. Depending on
version and configuration, its AOF contains Redis command protocol data and
may use a multi-part layout with a manifest, base file, and incremental files.
The user-facing configuration includes `appendonly` and `appendfsync` values
`always`, `everysec`, and `no`.

The transferable lesson is the ordering contract: mutations must be
propagated in a recoverable order, and the server must define what has happened
before it reports success. The differences are substantial:

- MiniRedis logs logical `CommitBatch` records, not Redis commands.
- Its file header, JSON-like payload encoding, framing, and checksum are
  project-specific.
- It has one teaching-process writer and no Redis manifest or multi-part AOF
  management.
- Its `everysec` task is a straightforward timer, not Redis's production
  bio/fsync scheduling and latency controls.

These are intentional simplifications, documented in the
[AOF and Recovery rows](../behavior-matrix.md). An `.mraof` file cannot be
opened by Redis, and `redis-check-aof` is not a repair tool for it.

## Hands-on lab: an acknowledged write survives a simulated crash

The repository already provides the experiment in
`examples/aof_crash_recovery.py`. Run it from the repository root:

```bash
uv run python examples/aof_crash_recovery.py
```

Measured output:

```text
1. SET before crash: Ok(message=b'OK')
2. Simulating a crash (no graceful AOF drain)...
3. GET after restart: Bytes(value=b'durable')
4. Recovery verified: True
```

The script configures `AofPolicy.ALWAYS`. The first line is printed only after
`SET` has crossed `AofWriter._run_writer`'s write and fsync path. It then calls
`MiniRedis.simulate_crash`, which uses `AofWriter.crash_close` rather than the
graceful final-sync path. The second runtime executes
`recover_database` during `MiniRedis._start_owned`; `GET` observes the replayed
value.

This experiment establishes the MiniRedis contract for one process and its
configured file operations. It does not prove that every physical storage
device honors fsync identically, nor does it make `everysec` equivalent to
`always`.

For focused regression evidence, run:

```bash
uv run pytest -q tests/unit/persistence/test_aof_repair.py
```

Expected repository result:

```text
6 passed
```

Read the six cases after running them. In particular, compare an incomplete
tail with a checksum-corrupted complete frame; “repair” is intentionally not a
general corruption eraser.

## Exercises

### 1. Understanding: locate the acknowledgment boundary

Why is returning immediately after `_queue.put_nowait` unsafe? Name the two
functions that keep the client success behind the configured AOF barrier.

??? note "Reference answer"

    The queue only says the worker owns the request. It does not say that the
    frame was written or fsynced. `src/miniredis/persistence/aof.py`,
    `AofWriter.append`, awaits the per-item barrier, and
    `src/miniredis/core/executor.py`, `CommandExecutor._commit_prepared`,
    awaits that append outcome before applying the database batch and
    completing the reply.

### 2. Understanding: classify failure windows

For each policy, state whether a client can receive success before an
application-issued fsync for that batch: `always`, `everysec`, `no`.

??? note "Reference answer"

    `always`: no, because the worker fsyncs before settling the barrier.
    `everysec`: yes, because the worker marks the file dirty and the periodic
    task syncs later. `no`: yes, because MiniRedis issues no per-batch or
    periodic fsync for the data. Graceful close can narrow the `everysec`
    window, but an abrupt crash intentionally skips that cleanup.

### 3. Hands-on: add a policy-observation test

Task boundary: add one test under `tests/unit/persistence/` using the existing
fake file-operations pattern. Do not change `src/`. Record the ordered calls
for one append under `always` and `no`.

Acceptance:

```bash
uv run pytest -q tests/unit/persistence/test_aof_writer.py
```

Your new assertions must prove that `always` calls `fsync` before the append
future resolves and `no` does not. Restore the worktree afterward if you are
only using the repository as a reading exercise.

??? note "Reference answer"

    Reuse the fake `AofFileOps` and its call log from
    `tests/unit/persistence/test_aof_writer.py`. Start one writer per policy,
    append a one-operation batch, await the result, and inspect the log before
    closing. The expected diff is test-only: one new parametrized test, with
    `fsync` present after the record write for `always` and absent for `no`.
    Ignore the header fsync by clearing the log immediately after `start`.

### 4. Hands-on: prove repair is suffix-only

Task boundary: create a temporary AOF containing two complete frames and a
third truncated frame. Call `load_aof(..., repair_truncated_tail=True)`. Do not
edit production code.

Acceptance: assert that two batches are returned and that the resulting bytes
equal the header plus exactly the first two encoded records.

??? note "Reference answer"

    Construct batches with sequence numbers 1, 2, and 3 using the helpers in
    `tests/unit/persistence/test_framing.py`. Write
    `AOF_HEADER + first + second + third[:-1]`, load with repair enabled, and
    compare both `log.batches` and `path.read_bytes()`. A second assertion with
    a flipped checksum byte should raise `AofCorruption` and leave the file
    unchanged.

## Summary

MiniRedis AOF is a durability barrier over ordered `CommitBatch` records, not
a copy of Redis's wire commands. `always`, `everysec`, and `no` acknowledge at
different persistence levels; recovery accepts a complete contiguous history
and repairs only a clearly incomplete final frame. An ever-growing log is
correct but inefficient, so the next chapter asks how to replace history with
a stable state base without losing writes that arrive during the rewrite.

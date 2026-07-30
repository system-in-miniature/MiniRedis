> **Language**: English | [简体中文](../zh/tutorial/07-rewrite-snapshot.md)

# Persistence II: Rewrite and Snapshots

## Learning objectives

By the end of this chapter, you will be able to:

- explain why AOF rewrite needs both a stable base and a concurrent-write
  delta;
- trace the no-gap rewrite handshake across the executor and AOF writer;
- distinguish a snapshot checkpoint from an AOF state base;
- predict which baseline `recover_database` selects and which batches it
  replays; and
- test atomic replacement and failure behavior without changing `src/`.

## Why an append-only history needs compaction

Suppose a client executes `SET counter 1`, then overwrites it with 2, 3, and
4. Replaying all four commits is correct, but only the final value is needed to
describe current state. A long-lived AOF therefore trades simple appends for
increasing disk use and restart work.

MiniRedis compacts that history into an AOF **state base**: a complete logical
database image at sequence `S`, followed by ordinary batch records with
sequences greater than `S`. `src/miniredis/persistence/codec.py`,
`encode_aof_state_base_record`, gives the base its own record type. A rewritten
file is still a versioned MiniRedis AOF, but its first logical item may say
“this is the entire state at checkpoint 400” instead of replaying batches
1–400.

Compaction would be trivial if the server stopped accepting writes. Capture
state, write a new file, rename it, resume traffic. The interesting mechanism
is *online* rewrite: commands continue while the potentially slow base file is
written.

## The base-plus-delta invariant

An online rewrite must preserve this invariant:

```text
new AOF = exact state at checkpoint S
          + every committed batch after S, in sequence order
```

There are two classic failure gaps:

1. a commit lands after the snapshot is captured but before delta collection
   begins; or
2. the new file is installed before the last collected delta is appended.

MiniRedis closes the first gap through one serialized executor mailbox turn.
`src/miniredis/core/executor.py`, `CommandExecutor.begin_aof_rewrite`, posts a
`BeginAofRewrite` control message. In `CommandExecutor._dispatch`, handling
that message captures the logically live database image and invokes the
registered AOF rewrite function before the executor processes another
command. The registration is set by `src/miniredis/runtime.py`,
`MiniRedis._start_owned`, using `AofWriter.begin_rewrite`.

Inside `src/miniredis/persistence/aof.py`, `AofWriter.begin_rewrite`, the
writer stores `_RewriteState` **before** it creates the base-writing task:

```python
self._rewrite = state
state.base_task = asyncio.create_task(
    self._write_rewrite_base(state),
    name=f"miniredis-aof-rewrite-base-{generation}",
)
```

From that moment, every later append handled by `AofWriter._run_writer` calls
`_capture_rewrite_delta`. Because executor commits have total sequence order,
the bytearray delta inherits that order. There is no interval in which the
checkpoint exists but the writer does not know a rewrite is active.

The delta is bounded by
`MiniRedisConfig.aof_rewrite_delta_limit_bytes`. In
`AofWriter._capture_rewrite_delta`, exceeding the bound aborts the rewrite.
The old AOF remains authoritative and continues to contain the writes. This is
preferable to allowing a slow rewrite to become a second unbounded in-memory
log.

## Finalization: make the replacement durable

`AofWriter._write_rewrite_base` creates an exclusive temporary file and writes
the AOF header plus encoded state base. It then queues `_FinalizeRewrite`
behind ordinary append work. This queue placement closes the second gap:
commits already accepted by the writer are processed and copied into the delta
before finalization runs.

`AofWriter._finalize_rewrite` performs the critical sequence:

1. append the captured delta to the temporary descriptor;
2. fsync the temporary file;
3. atomically replace the configured AOF path with `os.replace`;
4. fsync the parent directory so the directory entry is durable;
5. switch the active writer descriptor and close the old one.

The distinction between pre-rename and post-rename failure is deliberate. A
failure before `replace` can clean up the temporary file and keep using the
old AOF. After `replace`, the path may already identify the new file; if the
directory fsync fails, the process cannot prove which path mapping will
survive a machine failure. `AofWriter._finalize_rewrite` records that ambiguity
as a terminal writer failure rather than silently continuing.

Tests in `tests/reliability/test_aof_rewrite.py` cover the seams:
`test_write_during_paused_base_survives_rewrite_and_restart` proves delta
capture, `test_rewrite_delta_overflow_preserves_old_aof_history` proves safe
pre-rename abort, and `test_immediate_write_after_rewrite_request_has_no_capture_gap`
exercises the mailbox boundary.

## Snapshots are stable checkpoints, not rewrite files

A snapshot stores a `SnapshotImage(checkpoint_seq, entries)` in its own custom
file format. `src/miniredis/core/executor.py`,
`CommandExecutor.capture_snapshot`, executes a mailbox barrier and exports
only logically live entries at the current sequence. It does not hold the
executor while disk I/O runs.

`src/miniredis/persistence/snapshot.py`, `SnapshotManager.save`, permits one
active job. `_run_save` first awaits the capture callback, then hands the
encoded image to `PosixSnapshotFileOps.write_atomic` in a worker thread. The
file operation follows the familiar durable-replacement pattern: exclusive
temporary file, complete write, file fsync, `os.replace`, and parent-directory
fsync.

This split is important for latency. The mailbox turn produces a stable
immutable image; the slower encoding and file installation use that image
after commands resume. The test
`tests/reliability/test_snapshot_barrier.py::test_commands_continue_after_capture_while_file_write_is_blocked`
holds the file write and confirms that a later `SET` commits while the saved
image remains at the earlier sequence.

A snapshot and an AOF state base contain similar logical data, but they have
different ownership:

- the snapshot is an independent checkpoint file configured by
  `snapshot_path`;
- the state base is the compacted prefix inside `appendonly.mraof`;
- later AOF batches may extend either baseline during combined recovery.

Neither format is Redis RDB.

## How combined recovery chooses a baseline

Read `src/miniredis/persistence/recovery.py`, `recover_database`. It loads the
snapshot image and AOF log separately. If the AOF has a state base, it selects
the image with the larger checkpoint sequence. On an equal sequence, the AOF
base wins because the comparison keeps the snapshot only when its sequence is
strictly greater.

It then filters AOF batches to those after the selected checkpoint and checks
continuity. Examples:

| Snapshot | AOF base | AOF batches | Result |
|---|---|---|---|
| seq 5 | none | 1…8 | install snapshot 5, replay 6…8 |
| seq 5 | seq 7 | 8…9 | install AOF base 7, replay 8…9 |
| seq 9 | seq 7 | 8…9 | install snapshot 9, replay nothing |
| seq 7 | seq 7 | 8 | choose AOF base 7, replay 8 |

The third row deserves care. The AOF ends at 9, equal to the newer snapshot,
so it is consistent even though no AOF record must be replayed. By contrast,
if the selected snapshot is sequence 9 and the AOF ends at 7, recovery raises
`RecoveryError`: the log is demonstrably older than the chosen checkpoint.

All replay occurs in a staged `Database`. Gaps, invalid mutations, bad
checksums, or an AOF that starts at the wrong sequence fail startup before
`MiniRedis._start_owned` opens command admission. See the
[Snapshot and Recovery rows](../behavior-matrix.md) for the documented
compatibility boundary.

## Compared with real Redis

Redis snapshot persistence is RDB, implemented primarily in `src/rdb.c` and
configured with commands and settings such as `SAVE`, `BGSAVE`, and `save`.
Redis AOF rewrite is implemented primarily in `src/aof.c` and exposed through
`BGREWRITEAOF`. Modern Redis may use a base file plus incremental AOF files and
a manifest; an AOF base can also be RDB-encoded depending on configuration.

MiniRedis preserves the mechanisms worth studying:

- capture a point-in-time state;
- continue accepting writes;
- retain every write after that point;
- install complete files through atomic replacement; and
- recover from the newest valid baseline plus a contiguous suffix.

It simplifies process isolation, child-process copy-on-write behavior,
incremental manifests, command-compatible AOF encoding, automatic rewrite
thresholds, and operational progress reporting. The
[AOF, Snapshot, and Recovery rows](../behavior-matrix.md) explicitly classify
the formats as custom. Do not use MiniRedis snapshot or AOF files with Redis
tools.

## Hands-on lab: observe a base and a concurrent delta

This experiment writes one key, saves a snapshot, starts AOF rewrite, and
writes another key while rewrite is active. Save the following as
`/tmp/miniredis_rewrite_demo.py`:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from miniredis import CommandRequest, MiniRedis, MiniRedisConfig
from miniredis.persistence.aof import AofPolicy, load_aof


async def main():
    with TemporaryDirectory(prefix="miniredis-rewrite-") as directory:
        root = Path(directory)
        config = MiniRedisConfig(
            aof_path=root / "appendonly.mraof",
            snapshot_path=root / "dump.mrsnap",
            aof_policy=AofPolicy.ALWAYS,
        )
        runtime = MiniRedis.open(config)
        await runtime.start()
        client = runtime.direct_client()
        print("SET before:", await client.execute(
            CommandRequest(b"SET", (b"before", b"1"))
        ))
        saved = await runtime.save_snapshot()
        print("snapshot seq:", saved.checkpoint_seq)

        rewriting = asyncio.create_task(runtime.rewrite_aof())
        await asyncio.sleep(0)
        print("SET during:", await client.execute(
            CommandRequest(b"SET", (b"during", b"2"))
        ))
        rewritten = await rewriting
        print("rewrite seq:", rewritten.checkpoint_seq)
        log = load_aof(config.aof_path, repair_truncated_tail=False)
        print("AOF base seq:", log.state_base.checkpoint_seq)
        print("AOF delta seqs:", [batch.seq for batch in log.batches])
        await runtime.close()

        recovered = MiniRedis.open(config)
        await recovered.start()
        print("recovered:", await recovered.direct_client().execute(
            CommandRequest(b"MGET", (b"before", b"during"))
        ))
        print("recovered seq:", recovered.debug_commit_seq)
        await recovered.close()


asyncio.run(main())
```

Run:

```bash
uv run python /tmp/miniredis_rewrite_demo.py
```

Measured output, with temporary-directory names omitted:

```text
SET before: Ok(message=b'OK')
snapshot seq: 1
SET during: Ok(message=b'OK')
rewrite seq: 1
AOF base seq: 1
AOF delta seqs: [2]
recovered: Items(values=(Bytes(value=b'1'), Bytes(value=b'2')))
recovered seq: 2
```

The snapshot and rewrite base both describe sequence 1. The `SET during`
becomes batch 2 and appears in the rewrite delta. Recovery chooses a sequence-1
base and applies the contiguous sequence-2 suffix. The two values and final
sequence demonstrate both state and ordering preservation.

Also run the focused repository tests:

```bash
uv run pytest -q tests/reliability/test_aof_rewrite.py \
  tests/reliability/test_snapshot_barrier.py
```

Expected repository result:

```text
11 passed
```

## Exercises

### 1. Understanding: find the no-gap point

Why would capturing a snapshot in one executor message and calling
`AofWriter.begin_rewrite` in a later message be unsafe?

??? note "Reference answer"

    A command could commit between the two messages. It would be newer than the
    captured base, but the writer would not yet have `_rewrite` installed, so
    `_capture_rewrite_delta` would ignore it. MiniRedis captures the image and
    registers rewrite in the same serialized dispatch turn.

### 2. Understanding: choose the recovery base

A snapshot is at sequence 12. The AOF state base is at 10 and contains batches
11 and 12. What does recovery install and replay?

??? note "Reference answer"

    It installs the snapshot because 12 is greater than 10. It filters out
    batches whose sequence is not greater than 12, so it replays nothing. The
    AOF still ends at 12, so the consistency check passes.

### 3. Hands-on: test rewrite compaction

Task boundary: add a test under `tests/reliability/` that writes the same key
five times under `AofPolicy.ALWAYS`, calls `rewrite_aof`, and inspects the AOF
with `load_aof`. Do not modify `src/`.

Acceptance:

```bash
uv run pytest -q tests/reliability/test_aof_rewrite.py
```

Assert that the pre-rewrite log contains five batches, the post-rewrite log has
a state base at sequence 5, no delta batches, and restart returns the fifth
value.

??? note "Reference answer"

    Follow
    `test_successful_rewrite_compacts_history_to_base_only`. The test-only diff
    should use a temporary path, start a configured runtime, execute five
    `SET`s, inspect before and after rewrite, close, reopen, and assert
    `GET == Bytes(b"5")`. No sleeps or private mutation are needed.

### 4. Hands-on: explore a safe abort

Task boundary: using existing test hooks, pause base writing, configure
`aof_rewrite_delta_limit_bytes=1`, then commit a write. Do not change
production code.

Acceptance: the rewrite returns `AofRewriteFailed("AOF rewrite delta limit
exceeded")`; after restart, both the pre-rewrite and concurrent values exist.

??? note "Reference answer"

    Reuse `open_test_runtime(..., aof_rewrite_gate=True)` and the structure of
    `test_rewrite_delta_overflow_preserves_old_aof_history`. The important
    assertions are not only the failure result but successful restart from the
    old AOF. Release the gate and close all runtimes in `finally` cleanup.

## Summary

Online rewrite is not “take a snapshot and overwrite a file.” It is a
carefully ordered base-plus-delta protocol: capture and register without a
mailbox gap, bound concurrent history, fsync the completed replacement, rename
atomically, and make the directory entry durable. Independent snapshots use
the same stable-image and atomic-file lessons, while recovery chooses the
newest complete baseline and replays only a contiguous suffix. The next
chapter sends those same ordered batches to another runtime and asks when a
missing suffix can be resumed instead of replaced wholesale.

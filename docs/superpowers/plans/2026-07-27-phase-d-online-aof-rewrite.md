# Phase D Online AOF Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. The user selected inline execution;
> do not dispatch subagents.

**Goal:** Compact AOF online into one stable state base plus concurrent commit
deltas, with bounded memory, atomic replacement, deterministic failures, and
recoverable shutdown/crash behavior.

**Architecture:** Executor capture and writer rewrite registration share one
mailbox turn. The old AOF remains authoritative while a background task writes
the base. Ordinary writer-queue appends also populate a bounded delta; an
ordered finalize item fsyncs, renames, fsyncs the directory, and switches the
descriptor without an append gap.

**Tech Stack:** Python 3.13, asyncio worker queues, POSIX file descriptors,
CRC-framed JSON codec, pytest failure-injection file ops.

---

## File map

- Modify `src/miniredis/config.py`: rewrite delta bound.
- Modify `src/miniredis/persistence/codec.py`: state-base record and richer
  AOF scan result.
- Modify `src/miniredis/persistence/aof.py`: rewrite outcomes, file operations,
  rewrite state, background base, delta capture, finalize barrier, cleanup.
- Modify `src/miniredis/persistence/recovery.py`: choose newest complete
  snapshot/AOF baseline.
- Modify `src/miniredis/core/executor.py`: `BeginAofRewrite` capture and
  registration control.
- Modify `src/miniredis/runtime.py`: `rewrite_aof`, writer wiring, test hooks,
  shutdown, and stats.
- Modify `tests/helpers/runtime.py`: gated/failing AOF rewrite operations.
- Modify `tests/unit/persistence/test_codec.py`.
- Modify `tests/unit/persistence/test_framing.py`.
- Modify `tests/unit/persistence/test_recovery.py`.
- Modify `tests/unit/persistence/test_aof_writer.py`.
- Create `tests/reliability/test_aof_rewrite.py`.
- Modify `tests/reliability/test_restart.py`.
- Modify `tests/reliability/test_reliability_shutdown.py`.
- Modify `docs/behavior-matrix.md` and `README.md`.

### Task 1: AOF state-base codec and backward-compatible scan

- [ ] **Step 1: Add failing codec tests**

Add:

```python
def test_aof_state_base_round_trips_before_contiguous_batches():
    image = SnapshotImage(
        7,
        ((b"k", StoredEntry(StoredString(b"v"), None, 3)),),
    )
    data = (
        AOF_HEADER
        + encode_aof_state_base_record(image)
        + encode_aof_record(batch(8))
    )
    scan = scan_aof_bytes(data)
    assert scan.state_base == image
    assert scan.batches == (batch(8),)
    assert not scan.has_truncated_tail


def test_state_base_after_commit_is_corruption():
    data = (
        AOF_HEADER
        + encode_aof_record(batch(1))
        + encode_aof_state_base_record(SnapshotImage(1, ()))
    )
    with pytest.raises(CodecError, match="state base must be first"):
        scan_aof_bytes(data)
```

Also test base-only files, batch at/below checkpoint, non-contiguous batch
after base, duplicate base, truncated base tail repair boundary, and legacy
batch-only files.

- [ ] **Step 2: Run codec tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/unit/persistence/test_codec.py \
  tests/unit/persistence/test_framing.py
```

Expected: no state-base encoder or scan field.

- [ ] **Step 3: Add framed state-base payload**

Keep existing commit payloads unchanged for backward compatibility. Encode a
base payload with a disjoint strict shape:

```python
def encode_aof_state_base_payload(image: SnapshotImage) -> bytes:
    return _dumps(
        {
            "record": "state_base",
            "version": PAYLOAD_VERSION,
            "checkpoint_seq": image.checkpoint_seq,
            "entries": [
                {"key": _bytes(key), "entry": _encode_entry(entry)}
                for key, entry in image.entries
            ],
        }
    )
```

Frame it with the existing length plus CRC envelope. Detect a state base by
strictly loading the payload and checking `record == "state_base"`; otherwise
decode it as the existing commit payload.

Extend:

```python
@dataclass(frozen=True, slots=True)
class AofScan:
    state_base: SnapshotImage | None
    batches: tuple[CommitBatch, ...]
    valid_offset: int
    has_truncated_tail: bool
```

Enforce exactly one leading base and contiguous sequence starting at
`checkpoint_seq + 1`.

- [ ] **Step 4: Run codec tests and commit**

Run:

```bash
uv run pytest -q \
  tests/unit/persistence/test_codec.py \
  tests/unit/persistence/test_framing.py \
  tests/unit/persistence/test_corruption.py
```

Expected: PASS.

Commit:

```bash
git add src/miniredis/persistence/codec.py tests/unit/persistence
git commit -m "feat: encode AOF state base records"
```

### Task 2: Recovery baseline selection

- [ ] **Step 1: Add failing recovery matrix tests**

Cover a newer AOF base explicitly:

```python
def test_recovery_prefers_newer_aof_state_base(tmp_path):
    snapshot_path = tmp_path / "dump.snapshot"
    aof_path = tmp_path / "appendonly.mraof"
    snapshot_path.write_bytes(
        encode_snapshot_file(
            SnapshotImage(
                1,
                ((b"k", StoredEntry(StoredString(b"snapshot"), None, 1)),),
            )
        )
    )
    base = SnapshotImage(
        2,
        ((b"k", StoredEntry(StoredString(b"base"), None, 2)),),
    )
    later = CommitBatch(
        3,
        (
            PutEntry(
                b"later",
                StoredEntry(StoredString(b"value"), None, 1),
            ),
        ),
        CommitTrigger.CLIENT,
    )
    aof_path.write_bytes(
        AOF_HEADER
        + encode_aof_state_base_record(base)
        + encode_aof_record(later)
    )
    recovered = recover_database(
        snapshot_path=snapshot_path,
        aof_path=aof_path,
        now_ms=0,
        repair_truncated_tail=False,
    )
    assert recovered.commit_seq == 3
    assert recovered.export_stored_entries(0) == (
        (b"k", StoredEntry(StoredString(b"base"), None, 2)),
        (b"later", StoredEntry(StoredString(b"value"), None, 1)),
    )
```

Add the inverse newer-snapshot case, equal checkpoints, base-without-snapshot,
legacy AOF plus snapshot, missing post-baseline sequence, and
truncated-final-delta repair.

- [ ] **Step 2: Run recovery tests and verify RED**

Run:

```bash
uv run pytest -q tests/unit/persistence/test_recovery.py
```

Expected: `load_aof` returns only batches and cannot select the base.

- [ ] **Step 3: Return an AOF log object and select baseline**

Add:

```python
@dataclass(frozen=True, slots=True)
class AofLog:
    state_base: SnapshotImage | None
    batches: tuple[CommitBatch, ...]
```

`load_aof` returns `AofLog`. Tail repair still truncates to
`scan.valid_offset`.

In recovery:

```python
snapshot = _load_snapshot(snapshot_path)
log = load_aof(
    aof_path,
    repair_truncated_tail=repair_truncated_tail,
)
base = log.state_base
image = (
    snapshot
    if base is None or snapshot.checkpoint_seq > base.checkpoint_seq
    else base
)
post_checkpoint = tuple(
    batch for batch in log.batches if batch.seq > image.checkpoint_seq
)
```

Retain explicit gap and "AOF ends before selected checkpoint" validation.
Install the selected image, replay contiguous later batches, then discard
expired entries.

- [ ] **Step 4: Run recovery/restart tests and commit**

Run:

```bash
uv run pytest -q \
  tests/unit/persistence/test_recovery.py \
  tests/unit/persistence/test_aof_repair.py \
  tests/reliability/test_restart.py
```

Expected: PASS for legacy and state-base histories.

Commit:

```bash
git add src/miniredis/persistence/aof.py \
  src/miniredis/persistence/recovery.py tests/unit/persistence \
  tests/reliability/test_restart.py
git commit -m "feat: recover from AOF state baselines"
```

### Task 3: Rewrite file operations and bounded writer state

- [ ] **Step 1: Add failing writer state/outcome tests**

Add tests for Disabled/Busy registration, temp-file path uniqueness, base
write gating, and delta overflow:

```python
class GateRewriteOps(PosixAofFileOps):
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._rewrite_fd: int | None = None
        self.base_entered = asyncio.Event()
        self.release_base = threading.Event()

    def open_rewrite(self, path: Path) -> int:
        fd = super().open_rewrite(path)
        self._rewrite_fd = fd
        return fd

    def write_all(self, fd: int, data: bytes) -> None:
        if fd == self._rewrite_fd:
            self._loop.call_soon_threadsafe(self.base_entered.set)
            self.release_base.wait()
        super().write_all(fd, data)


@pytest.mark.asyncio
async def test_begin_rewrite_registers_before_next_append(tmp_path):
    ops = GateRewriteOps(asyncio.get_running_loop())
    writer = AofWriter(
        tmp_path / "appendonly.mraof",
        AofPolicy.ALWAYS,
        rewrite_delta_limit_bytes=4096,
        ops=ops,
    )
    await writer.start()
    job = writer.begin_rewrite(SnapshotImage(0, ()))
    assert writer.rewrite_active
    await ops.base_entered.wait()
    assert await writer.append(batch(1)) == AofAppendOk(1)
    assert writer.rewrite_delta_bytes == len(encode_aof_record(batch(1)))
    ops.release_base.set()
    assert await job == AofRewriteSaved(tmp_path / "appendonly.mraof", 0)
```

- [ ] **Step 2: Run writer tests and verify RED**

Run:

```bash
uv run pytest -q tests/unit/persistence/test_aof_writer.py
```

Expected: missing rewrite API and file operations.

- [ ] **Step 3: Extend file operations**

Add protocol and POSIX methods:

```python
def open_rewrite(self, path: Path) -> int:
    return os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_APPEND,
        0o600,
    )

def replace(self, source: Path, destination: Path) -> None:
    os.replace(source, destination)

def fsync_parent(self, path: Path) -> None:
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)

def unlink(self, path: Path) -> None:
    os.unlink(path)
```

Tests implement the same protocol and inject gates/errors without monkeypatching
writer internals.

- [ ] **Step 4: Define rewrite outcomes and state**

Add:

```python
aof_rewrite_delta_limit_bytes: int = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class AofRewriteSaved:
    path: Path
    checkpoint_seq: int

@dataclass(frozen=True, slots=True)
class AofRewriteBusy:
    pass

@dataclass(frozen=True, slots=True)
class AofRewriteFailed:
    message: str
```

Integrate the field into the existing config dataclass rather than creating a
second config type, and reject values less than or equal to zero in
`__post_init__`.

Internal state contains image, temp path/fd, bounded `bytearray` delta,
completion future, base task, abort reason, and rename flag. Generate temp
paths as:

```python
self._path.with_name(
    f".{self._path.name}.rewrite.{os.getpid()}.{generation}.tmp"
)
```

`begin_rewrite(image)` synchronously installs state and starts one owned task
before returning the shieldable completion future. A second call returns an
already-set `AofRewriteBusy`.

- [ ] **Step 5: Write base in the background**

The owned task:

1. opens temp with `open_rewrite`;
2. writes `AOF_HEADER + encode_aof_state_base_record(image)`;
3. reports its open fd back to the event loop;
4. enqueues `_FinalizeRewrite(generation)` into the writer queue.

It does not fsync or rename; finalization owns those ordered steps. On pre-fd
failure it settles `AofRewriteFailed`. On cancellation/abort it closes any fd
and unlinks the temp after the thread call returns.

- [ ] **Step 6: Capture bounded delta after authoritative append**

After each `_AppendWork` completes its required old-AOF write/fsync and before
settling `AofAppendOk`, append its already encoded record to active rewrite
delta. If the new total exceeds `aof_rewrite_delta_limit_bytes`:

- mark rewrite aborted;
- settle only the rewrite outcome as `AofRewriteFailed`;
- keep the ordinary append successful;
- let base completion perform fd/temp cleanup instead of finalization.

- [ ] **Step 7: Run writer tests and commit**

Run:

```bash
uv run pytest -q tests/unit/persistence/test_aof_writer.py
```

Expected: registration, delta, Busy, overflow, and base failure cases pass.

Commit:

```bash
git add src/miniredis/config.py src/miniredis/persistence/aof.py \
  tests/unit/persistence/test_aof_writer.py
git commit -m "feat: capture bounded online AOF rewrite deltas"
```

### Task 4: Ordered finalization and failure boundary

- [ ] **Step 1: Add failing finalization failure-injection tests**

Test:

- temp write failure leaves old AOF writable;
- temp fsync failure leaves old path;
- failed rename leaves old path;
- delta is ordered through a paused base;
- parent-directory fsync failure after rename is terminal;
- later append work uses the new fd after success.

Use operation classes with one explicit failure flag per syscall:

```python
class RewriteFailureOps(PosixAofFileOps):
    fail_temp_fsync = False
    fail_replace = False
    fail_parent_fsync = False
```

- [ ] **Step 2: Run finalization tests and verify RED**

Run:

```bash
uv run pytest -q tests/unit/persistence/test_aof_writer.py -k rewrite
```

Expected: rewrite never atomically switches files.

- [ ] **Step 3: Implement `_FinalizeRewrite` in writer queue order**

For the matching active generation:

```python
old_fd = self._fd
self._ops.write_all(temp_fd, bytes(state.delta))
self._ops.fsync(temp_fd)
self._ops.replace(state.temporary, self._path)
state.renamed = True
self._ops.fsync_parent(self._path)
self._fd = temp_fd
state.temporary_fd = None
self._ops.close(old_fd)
```

Then settle `AofRewriteSaved` and clear state. Queue items after finalization
observe the new `self._fd`.

Before successful rename, catch failure, close/unlink temp, settle only the
rewrite as Failed, and continue writer service. After successful rename, any
failure is passed to `_record_failure`, fails queued appends, and makes the
runtime terminal; never resume writes through `old_fd`.

- [ ] **Step 4: Account for owned tasks and close modes**

`owned_task_count` includes the rewrite base task.

Graceful `close()` stops new rewrites, waits active base/finalization, then
performs ordinary writer drain/fsync/close. `crash_close()` marks an
unfinalized rewrite aborted, waits only for thread ownership cleanup, deletes
the temp, and closes the current authoritative fd without an extra configured
fsync. If rename already succeeded, it closes the installed fd.

- [ ] **Step 5: Run writer and shutdown tests and commit**

Run:

```bash
uv run pytest -q \
  tests/unit/persistence/test_aof_writer.py \
  tests/reliability/test_reliability_shutdown.py \
  tests/reliability/test_worker_failure.py
```

Expected: PASS and zero writer-owned tasks after every close path.

Commit:

```bash
git add src/miniredis/persistence/aof.py tests/unit/persistence \
  tests/reliability/test_reliability_shutdown.py \
  tests/reliability/test_worker_failure.py
git commit -m "feat: atomically finalize AOF rewrites"
```

### Task 5: Race-free runtime API and end-to-end rewrite

- [ ] **Step 1: Add failing end-to-end reliability tests**

Create `tests/reliability/test_aof_rewrite.py` covering:

```python
@pytest.mark.asyncio
async def test_write_during_paused_base_survives_rewrite_and_restart(tmp_path):
    runtime = await open_test_runtime(
        config=MiniRedisConfig(
            aof_path=tmp_path / "appendonly.mraof",
            aof_policy=AofPolicy.ALWAYS,
        ),
        aof_rewrite_gate=True,
    )
    c = runtime.direct_client()
    await c.execute(CommandRequest(b"SET", (b"before", b"1")))
    rewriting = asyncio.create_task(runtime.rewrite_aof())
    await runtime.debug_aof_rewrite_entered.wait()
    await c.execute(CommandRequest(b"SET", (b"during", b"2")))
    runtime.debug_aof_rewrite_release.set()
    assert isinstance(await rewriting, AofRewriteSaved)
    await runtime.close()
    recovered = MiniRedis.open(config=runtime.config)
    await recovered.start()
    assert await recovered.direct_client().execute(
        CommandRequest(b"MGET", (b"before", b"during"))
    ) == Items((Bytes(b"1"), Bytes(b"2")))
    await recovered.close()
```

Add Busy, no-AOF Failed, delta overflow, combined snapshot precedence,
graceful close, simulated crash before rename, successful compacted record
count, and no capture gap.

- [ ] **Step 2: Run reliability tests and verify RED**

Run:

```bash
uv run pytest -q tests/reliability/test_aof_rewrite.py
```

Expected: runtime has no rewrite method or executor control.

- [ ] **Step 3: Add `BeginAofRewrite` executor control**

Define:

```python
@dataclass(slots=True)
class BeginAofRewrite:
    future: asyncio.Future[AofRewriteOutcome]
```

Pass an optional synchronous writer registration callback into the executor.
In one control-message turn:

```python
image = self.database.snapshot_image(self.clock.now_ms())
job = self._begin_aof_rewrite(image)
```

Bridge the job result to the message future with an executor-owned callback or
small supervised task. Crucially, registration occurs before the handler
returns to the mailbox.

- [ ] **Step 4: Expose runtime API, hooks, and stats**

Add:

```python
async def rewrite_aof(self) -> AofRewriteOutcome:
    if self.state is not RuntimeState.RUNNING:
        return AofRewriteFailed("runtime is not running")
    if self._aof_writer is None:
        return AofRewriteFailed("aof_path is not configured")
    return await self.executor.begin_aof_rewrite()
```

Wire writer callback after AOF startup and before executor start. Add test
hooks for rewrite file ops/gates.

Extend stats with:

```python
aof_rewrite_active: bool
aof_rewrite_delta_bytes: int
aof_rewrite_checkpoint_seq: int | None
```

- [ ] **Step 5: Run AOF/restart/lifecycle matrix**

Run:

```bash
uv run pytest -q \
  tests/reliability/test_aof_rewrite.py \
  tests/reliability/test_restart.py \
  tests/reliability/test_reliability_shutdown.py \
  tests/unit/persistence
```

Expected: PASS.

- [ ] **Step 6: Commit runtime rewrite API**

```bash
git add src/miniredis/core/executor.py src/miniredis/runtime.py \
  tests/helpers/runtime.py tests/reliability tests/unit/persistence
git commit -m "feat: expose race-free online AOF rewrite"
```

### Task 6: Phase D documentation and acceptance

- [ ] **Step 1: Update README and behavior matrix**

Document state-base format, legacy compatibility, rewrite delta bound,
non-terminal pre-rename failure, terminal post-rename ambiguity, snapshot
baseline selection, and manual `rewrite_aof()` API. Remove AOF rewrite from
non-goals; retain real Redis AOF/RDB compatibility as non-goals.

- [ ] **Step 2: Run complete verification**

Run:

```bash
uv run ruff check .
uv run pytest -q
git diff --check
```

Expected: all checks pass.

- [ ] **Step 3: Commit Phase D acceptance**

```bash
git add README.md docs/behavior-matrix.md
git commit -m "docs: accept online AOF rewrite phase"
```

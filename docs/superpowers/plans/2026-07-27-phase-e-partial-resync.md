# Phase E Replication Backlog and Partial Resync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. The user selected inline execution;
> do not dispatch subagents.

**Goal:** Add logical replication identity, a bounded CommitBatch backlog,
manual partial resync, full-sync fallback, and promotion fencing without
adding network replication or automatic distributed-system machinery.

**Architecture:** The executor owns one Primary history ID and backlog. A
ReplicaSink retains its last fully applied `(replication_id, seq)` cursor.
Attachment is decided and registered in one executor turn, yielding either a
stable full image or a captured backlog range followed by the same ordered
live queue.

**Tech Stack:** Python 3.13, asyncio, deque ring buffer, frozen dataclasses,
pytest deterministic gates.

---

## File map

- Modify `src/miniredis/config.py`: positive backlog capacity.
- Create `src/miniredis/replication/backlog.py`: replication identity, cursor,
  backlog, coverage, and attachment types.
- Modify `src/miniredis/replication/sink.py`: retained cursor, full/partial
  attachment, ordered catch-up, reattachment, status, and promotion identity.
- Modify `src/miniredis/core/executor.py`: history ownership, backlog insert,
  attach decision, replica source preparation, promotion fencing, and stats.
- Modify `src/miniredis/runtime.py`: identity generation/injection, reattach
  API ownership, and runtime stats.
- Modify `tests/helpers/runtime.py`: deterministic replication ID hook.
- Create `tests/unit/replication/test_backlog.py`.
- Modify `tests/replication/test_sink_attach.py`.
- Modify `tests/replication/test_sink_overflow.py`.
- Modify `tests/replication/test_sink_lag.py`.
- Modify `tests/replication/test_promotion.py`.
- Create `tests/replication/test_partial_resync.py`.
- Modify `tests/reliability/test_lost_acked_write.py`.
- Modify `tests/reliability/test_restart.py`.
- Modify `tests/reliability/test_reliability_shutdown.py`.
- Modify `docs/behavior-matrix.md` and `README.md`.

### Task 1: Replication identity and bounded backlog

- [ ] **Step 1: Add failing pure backlog tests**

Create `tests/unit/replication/test_backlog.py`:

```python
from tests.unit.persistence.test_framing import batch


def test_backlog_drops_oldest_batches_and_reports_bounds():
    backlog = ReplicationBacklog(capacity_batches=2)
    backlog.append(batch(1))
    backlog.append(batch(2))
    backlog.append(batch(3))
    assert backlog.oldest_seq == 2
    assert backlog.newest_seq == 3
    assert backlog.batch_count == 2
    assert backlog.missing_after(1, current_seq=3) == (batch(2), batch(3))


def test_backlog_distinguishes_current_empty_range_from_gap():
    backlog = ReplicationBacklog(capacity_batches=2)
    backlog.append(batch(4))
    backlog.append(batch(5))
    assert backlog.missing_after(5, current_seq=5) == ()
    assert backlog.missing_after(3, current_seq=5) is None
    assert backlog.missing_after(6, current_seq=5) is None
```

Test non-contiguous append rejection and clear.

- [ ] **Step 2: Run unit tests and verify RED**

Run:

```bash
uv run pytest -q tests/unit/replication/test_backlog.py
```

Expected: replication backlog module is missing.

- [ ] **Step 3: Implement pure replication types**

Create:

```python
@dataclass(frozen=True, slots=True)
class ReplicationCursor:
    replication_id: str
    applied_seq: int


class ReplicationBacklog:
    def __init__(self, capacity_batches: int) -> None:
        if capacity_batches <= 0:
            raise ValueError("replication backlog capacity must be positive")
        self._capacity = capacity_batches
        self._batches: deque[CommitBatch] = deque()

    def append(self, batch: CommitBatch) -> None:
        if self._batches and batch.seq != self._batches[-1].seq + 1:
            raise ValueError("replication backlog must be contiguous")
        self._batches.append(batch)
        while len(self._batches) > self._capacity:
            self._batches.popleft()

    def missing_after(
        self, applied_seq: int, *, current_seq: int
    ) -> tuple[CommitBatch, ...] | None:
        if applied_seq > current_seq:
            return None
        if applied_seq == current_seq:
            return ()
        expected = applied_seq + 1
        selected = tuple(batch for batch in self._batches if batch.seq >= expected)
        if not selected or selected[0].seq != expected:
            return None
        if selected[-1].seq != current_seq:
            return None
        return selected
```

Expose read-only oldest/newest/count and `clear`.

- [ ] **Step 4: Add config and run tests**

Add:

```python
replication_backlog_batches: int = 1024
```

and positive validation. Retain existing `replica_queue_limit` unchanged.

Run:

```bash
uv run pytest -q \
  tests/unit/replication/test_backlog.py \
  tests/test_project_contract.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/miniredis/config.py src/miniredis/replication/backlog.py \
  tests/unit/replication/test_backlog.py tests/test_project_contract.py
git commit -m "feat: add bounded logical replication backlog"
```

### Task 2: Executor history ownership and attachment decision

- [ ] **Step 1: Add failing attachment-decision tests**

Define attachment assertions:

```python
assert isinstance(first, FullSyncAttachment)
assert first.replication_id == "primary-A"
assert first.image.checkpoint_seq == 3

assert isinstance(resumed, PartialSyncAttachment)
assert resumed.cursor == ReplicationCursor("primary-A", 1)
assert tuple(batch.seq for batch in resumed.batches) == (2, 3)
assert resumed.boundary_seq == 3
```

Test matching current cursor gives an empty partial attachment; identity
mismatch, future cursor, and backlog gap give full attachment.

- [ ] **Step 2: Run attachment tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/replication/test_sink_attach.py \
  tests/replication/test_partial_resync.py
```

Expected: only full `ReplicaAttachment` exists.

- [ ] **Step 3: Define full and partial attachment types**

Add:

```python
@dataclass(frozen=True, slots=True)
class FullSyncAttachment:
    generation: int
    replication_id: str
    image: SnapshotImage


@dataclass(frozen=True, slots=True)
class PartialSyncAttachment:
    generation: int
    replication_id: str
    cursor: ReplicationCursor
    boundary_seq: int
    batches: tuple[CommitBatch, ...]


ReplicaAttachment = FullSyncAttachment | PartialSyncAttachment
```

- [ ] **Step 4: Own identity and backlog in executor**

Add constructor arguments:

```python
replication_id: str
replication_backlog_batches: int
replication_id_factory: Callable[[], str]
```

Initialize one `ReplicationBacklog`. In `_commit_prepared`, after successful
database apply and before live offers:

```python
self.replication_backlog.append(batch)
self._offer_replica_batch(batch)
```

Recovered history starts with an empty backlog and a new ID even if
`database.commit_seq > 0`.

Wire identity construction through runtime now, before sink resume tests.
Extend `_RuntimeTestHooks` and `open_test_runtime` with:

```python
replication_id_factory: Callable[[], str] | None = None
```

Production uses a stored callable equivalent to:

```python
def new_replication_id() -> str:
    return uuid.uuid4().hex
```

The executor calls it once for initial history and retains the callable for
promotion.

- [ ] **Step 5: Decide attachment in one executor turn**

Extend `AttachReplica` with `cursor: ReplicationCursor | None`. In its handler:

```python
generation = self._allocate_replica_generation()
boundary = self.database.commit_seq
missing = (
    None
    if cursor is None or cursor.replication_id != self.replication_id
    else self.replication_backlog.missing_after(
        cursor.applied_seq,
        current_seq=boundary,
    )
)
if missing is None:
    attachment = FullSyncAttachment(
        generation,
        self.replication_id,
        self.database.snapshot_image(self.clock.now_ms()),
    )
    self.full_sync_count += 1
else:
    attachment = PartialSyncAttachment(
        generation,
        self.replication_id,
        cursor,
        boundary,
        missing,
    )
    self.partial_sync_count += 1
message.sink.register_attachment(attachment)
self._replica_sinks[generation] = message.sink
```

Registration precedes returning to the mailbox, so later batches enter the
same sink's live queue.

- [ ] **Step 6: Run executor/full-sync regressions and commit**

Run:

```bash
uv run pytest -q \
  tests/unit/replication/test_backlog.py \
  tests/replication/test_sink_attach.py \
  tests/replication/test_sink_lag.py
```

Expected: existing full sync remains green and decision tests pass.

Commit:

```bash
git add src/miniredis/core/executor.py src/miniredis/runtime.py \
  src/miniredis/replication/backlog.py \
  src/miniredis/replication/sink.py tests/helpers/runtime.py tests/replication
git commit -m "feat: choose full or partial replica attachment"
```

### Task 3: Replica cursor and ordered partial catch-up

- [ ] **Step 1: Add failing short-disconnect and concurrent-write tests**

Create cases:

```python
@pytest.mark.asyncio
async def test_short_disconnect_resumes_only_missing_batches():
    primary = await open_test_runtime()
    replica = await open_test_runtime()
    sink = ReplicaSink(replica, queue_limit=4)
    await primary.attach_replica(sink)
    client = primary.direct_client()
    assert await client.execute(CommandRequest(b"SET", (b"a", b"1"))) == Ok()
    await sink.wait_until_applied(primary.debug_commit_seq)
    await sink.disconnect()
    assert await client.execute(CommandRequest(b"SET", (b"b", b"2"))) == Ok()
    status = await primary.attach_replica(sink)
    await sink.wait_until_applied(primary.debug_commit_seq)
    assert status.sync_mode is ReplicaSyncMode.PARTIAL
    assert sink.status.applied_seq == primary.debug_commit_seq
    await primary.close()
    await replica.close()
```

Pause partial installation, write again on Primary, then prove captured backlog
is applied before the live queue with contiguous sequence.

- [ ] **Step 2: Run partial tests and verify RED**

Run:

```bash
uv run pytest -q tests/replication/test_partial_resync.py
```

Expected: sink cannot reconnect and always installs a snapshot.

- [ ] **Step 3: Retain cursor and add source preparation control**

`ReplicaSink` retains:

```python
class ReplicaSyncMode(StrEnum):
    FULL = "full"
    PARTIAL = "partial"


self._replication_id: str | None
self._applied_seq: int
self._sync_mode: ReplicaSyncMode | None
```

Add `CATCHING_UP = "catching_up"` to the existing `ReplicaSinkState` enum.

Its cursor property returns `None` until a full image is installed, then
returns the last completely applied pair.

Add a Replica executor control:

```python
@dataclass(slots=True)
class PrepareReplicaResume:
    generation: int
    replication_id: str
    expected_applied_seq: int
    future: asyncio.Future[bool]
```

It succeeds only if the replica's recorded active source ID matches,
`database.commit_seq == expected_applied_seq`, and it is still read-only. On
success it replaces the active source generation for this link without
clearing data.

Full snapshot installation records both generation and source ID.

- [ ] **Step 4: Implement one connect path for full and partial**

Allow `attach` from `DETACHED`, `NEEDS_RESYNC`, or `SOURCE_LOST`. Pass the
retained cursor to Primary.

For full attachment:

1. install `SnapshotImage`;
2. record source ID and baseline;
3. start live apply with any queued later batches.

For partial attachment:

1. call `prepare_replica_resume`;
2. preload captured missing batches into a dedicated catch-up deque;
3. enter `CATCHING_UP`;
4. apply catch-up completely;
5. then consume the live queue and enter `STREAMING`.

Every apply requires exactly `applied_seq + 1`. A duplicate or gap moves the
sink to `NEEDS_RESYNC` and detaches it.

- [ ] **Step 5: Preserve cursor on disconnect/overflow/source loss**

Add manual:

```python
async def disconnect(self) -> ReplicaStatus:
```

It detaches the current generation, stops the apply task, clears unapplied
queues, asks the Primary runtime to release sink ownership, enters `DETACHED`,
and preserves the last completed cursor.

Overflow and source loss also preserve the cursor. They never advance
`applied_seq` for a queued-but-unapplied batch.

- [ ] **Step 6: Run partial, overflow, lag, and shutdown tests**

Run:

```bash
uv run pytest -q \
  tests/replication/test_partial_resync.py \
  tests/replication/test_sink_overflow.py \
  tests/replication/test_sink_lag.py \
  tests/reliability/test_reliability_shutdown.py
```

Expected: PASS with no sink task leak.

- [ ] **Step 7: Commit partial catch-up**

```bash
git add src/miniredis/core/executor.py \
  src/miniredis/replication/backlog.py \
  src/miniredis/replication/sink.py tests/replication tests/reliability
git commit -m "feat: resume replicas from logical backlog"
```

### Task 4: Full-sync fallback, restart, and queue-overflow recovery

- [ ] **Step 1: Add failing fallback matrix**

Test:

- cursor exactly one before backlog oldest: partial succeeds;
- cursor two before backlog oldest: full sync;
- same sequence with wrong ID: full sync;
- future sequence: full sync;
- Primary restart with same recovered data but new ID: full sync;
- queue overflow then quick reattach: partial;
- queue overflow plus enough writes to rotate backlog: full;
- empty backlog plus current cursor: empty partial.

Assert full sync replaces stale extra Replica keys while partial sync does not
clear unrelated current state.

- [ ] **Step 2: Run fallback tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/replication/test_partial_resync.py \
  tests/replication/test_sink_overflow.py \
  tests/reliability/test_restart.py
```

Expected: boundary or restart cases fail until all cursor paths are wired.

- [ ] **Step 3: Inject deterministic Primary IDs in tests**

Extend `_RuntimeTestHooks`:

```python
replication_id_factory: Callable[[], str] | None = None
```

Runtime production construction uses:

```python
lambda: uuid.uuid4().hex
```

Use the factory hook introduced in Task 2. Test construction passes a
deterministic iterator-backed callable:

```python
identities = iter(("primary-A", "promoted-B"))

def factory() -> str:
    return next(identities)
```

Pass that same factory into executor so initial startup and later promotion
consume distinct deterministic IDs without hardcoding global state.

- [ ] **Step 4: Complete coverage and fallback behavior**

Make `ReplicationBacklog.missing_after` the only coverage decision. Do not
infer coverage from Replica lag or queue size. A full attachment always
captures the current `SnapshotImage`; later writes remain queued behind its
boundary.

On full install, reset Replica operational LFU/LRU metadata as already defined
by `Database.install_snapshot`.

- [ ] **Step 5: Run fallback matrix and commit**

Run:

```bash
uv run pytest -q \
  tests/replication \
  tests/reliability/test_restart.py \
  tests/reliability/test_reliability_shutdown.py
```

Expected: PASS.

Commit:

```bash
git add src/miniredis/runtime.py src/miniredis/core/executor.py \
  src/miniredis/replication tests/helpers/runtime.py \
  tests/replication tests/reliability
git commit -m "feat: fall back to full sync when history diverges"
```

### Task 5: Promotion fencing and preserved asynchronous loss

- [ ] **Step 1: Add failing promotion-history tests**

Extend promotion tests:

```python
old_id = sink.status.replication_id
result = await sink.promote(source_alive=False)
assert result.replication_id != old_id
assert promoted.debug_replication_backlog_count == 0
```

Reconnect a Replica with the old ID and same visible state; assert full sync.
Write after promotion and assert the new backlog begins at the next global
sequence.

- [ ] **Step 2: Run promotion tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/replication/test_promotion.py \
  tests/reliability/test_lost_acked_write.py
```

Expected: promotion has no history ID or backlog fencing.

- [ ] **Step 3: Fence history on promotion**

In the executor promotion control:

```python
self._active_source_generation = None
self._active_source_id = None
self._replica_read_only = False
self.replication_id = self._replication_id_factory()
self.replication_backlog.clear()
```

Return:

```python
PromotionResult(
    applied_seq=self.database.commit_seq,
    writable=True,
    replication_id=self.replication_id,
)
```

Update sink state/status with the new ID.

- [ ] **Step 4: Keep acknowledged-write loss test explicit**

The test must still perform:

```text
Primary acknowledges seq=N
Replica remains at N-1
Primary simulated crash
Replica manual promotion
GET for the seq=N write returns null
```

Do not auto-recover from a dead source or consult a quorum.

- [ ] **Step 5: Run promotion/reliability suites and commit**

Run:

```bash
uv run pytest -q \
  tests/replication/test_promotion.py \
  tests/reliability/test_lost_acked_write.py \
  tests/reliability/test_final_acceptance.py
```

Expected: PASS.

Commit:

```bash
git add src/miniredis/core/executor.py \
  src/miniredis/replication/sink.py \
  tests/replication/test_promotion.py \
  tests/reliability/test_lost_acked_write.py \
  tests/reliability/test_final_acceptance.py
git commit -m "feat: fence replication history on promotion"
```

### Task 6: Observability, docs, and final acceptance

- [ ] **Step 1: Extend status and runtime statistics**

Expose read-only:

```python
replication_id: str
primary_seq: int
backlog_oldest_seq: int | None
backlog_newest_seq: int | None
backlog_batch_count: int
full_sync_count: int
partial_sync_count: int
```

Extend `ReplicaStatus` with `replication_id`, `sync_mode`, and retained cursor.
Stats inspection must not mutate backlog or Replica state.

- [ ] **Step 2: Add final acceptance assertions**

Extend final acceptance to prove:

- adapter-free core contracts still pass;
- short gap uses partial sync;
- backlog gap uses full sync;
- promotion changes identity;
- asynchronous acknowledged loss still occurs;
- shutdown leaves zero Replica/runtime owned tasks.

- [ ] **Step 3: Update README and behavior matrix**

Document logical batch offsets, manual reconnect, backlog capacity, full-sync
fallback, new Primary ID on restart/promotion, and acknowledged-write loss.
Remove partial resync from non-goals. Keep PSYNC wire compatibility,
heartbeats, elections, Sentinel, Cluster, and WAIT explicitly excluded.

- [ ] **Step 4: Run complete repository verification**

Run:

```bash
uv run ruff check .
uv run pytest -q
git diff --check
```

Expected: all checks pass.

- [ ] **Step 5: Commit final project acceptance**

```bash
git add src/miniredis/runtime.py src/miniredis/core/executor.py \
  src/miniredis/replication tests README.md docs/behavior-matrix.md
git commit -m "docs: accept advanced MiniRedis mechanisms"
```

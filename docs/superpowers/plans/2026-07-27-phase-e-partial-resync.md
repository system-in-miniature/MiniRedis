# Phase E Replication Backlog and Partial Resync Design and Implementation History

**Historical objective:** Add logical replication identity, a bounded CommitBatch backlog,
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

- Changed `src/miniredis/config.py`: positive backlog capacity.
- Added `src/miniredis/replication/backlog.py`: replication identity, cursor,
  backlog, coverage, and attachment types.
- Changed `src/miniredis/replication/sink.py`: retained cursor, full/partial
  attachment, ordered catch-up, reattachment, status, and promotion identity.
- Changed `src/miniredis/core/executor.py`: history ownership, backlog insert,
  attach decision, replica source preparation, promotion fencing, and stats.
- Changed `src/miniredis/runtime.py`: identity generation/injection, reattach
  API ownership, and runtime stats.
- Changed `tests/helpers/runtime.py`: deterministic replication ID hook.
- Added `tests/unit/replication/test_backlog.py`.
- Changed `tests/replication/test_sink_attach.py`.
- Changed `tests/replication/test_sink_overflow.py`.
- Changed `tests/replication/test_sink_lag.py`.
- Changed `tests/replication/test_promotion.py`.
- Added `tests/replication/test_partial_resync.py`.
- Changed `tests/reliability/test_lost_acked_write.py`.
- Changed `tests/reliability/test_restart.py`.
- Changed `tests/reliability/test_reliability_shutdown.py`.
- Changed `docs/behavior-matrix.md` and `README.md`.

### Milestone 1: Replication identity and bounded backlog

**Recorded activity 1 — Design outcome: failing pure backlog tests**

The recorded scope added `tests/unit/replication/test_backlog.py`:

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

Historical test coverage included non-contiguous append rejection and clear.

**Recorded activity 2 — Verification intent: unit tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/unit/replication/test_backlog.py`.

Historical expected evidence: replication backlog module is missing.

**Recorded activity 3 — Design outcome: pure replication types**

The recorded scope added:

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

The interface exposed read-only oldest/newest/count and `clear`.

**Recorded activity 4 — Design outcome: config and run tests**

The recorded scope added:

```python
replication_backlog_batches: int = 1024
```

and positive validation. Retain existing `replica_queue_limit` unchanged.

Historical verification covered targeted or full test coverage, including `tests/unit/replication/test_backlog.py`, `tests/test_project_contract.py`.

Historical expected evidence: PASS.

### Milestone 2: Executor history ownership and attachment decision

**Recorded activity 1 — Design outcome: failing attachment-decision tests**

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

Historical test coverage included matching current cursor gives an empty partial attachment; identity
mismatch, future cursor, and backlog gap give full attachment.

**Recorded activity 2 — Verification intent: attachment tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/replication/test_sink_attach.py`, `tests/replication/test_partial_resync.py`.

Historical expected evidence: only full `ReplicaAttachment` exists.

**Recorded activity 3 — Define full and partial attachment types**

The recorded scope added:

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

**Recorded activity 4 — Own identity and backlog in executor**

The recorded scope added constructor arguments:

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

The recorded integration wired identity construction through runtime now, before sink resume tests.
The recorded change extended `_RuntimeTestHooks` and `open_test_runtime` with:

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

**Recorded activity 5 — Decide attachment in one executor turn**

The recorded change extended `AttachReplica` with `cursor: ReplicationCursor | None`. In its handler:

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

**Recorded activity 6 — Verification intent: executor/full-sync regressions and commit**

Historical verification covered targeted or full test coverage, including `tests/unit/replication/test_backlog.py`, `tests/replication/test_sink_attach.py`, `tests/replication/test_sink_lag.py`.

Historical expected evidence: existing full sync remains green and decision tests pass.

### Milestone 3: Replica cursor and ordered partial catch-up

**Recorded activity 1 — Design outcome: failing short-disconnect and concurrent-write tests**

The recorded scope added cases:

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

**Recorded activity 2 — Verification intent: partial tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/replication/test_partial_resync.py`.

Historical expected evidence: sink cannot reconnect and always installs a snapshot.

**Recorded activity 3 — Retain cursor and add source preparation control**

`ReplicaSink` retains:

```python
class ReplicaSyncMode(StrEnum):
    FULL = "full"
    PARTIAL = "partial"


self._replication_id: str | None
self._applied_seq: int
self._sync_mode: ReplicaSyncMode | None
```

The recorded scope added `CATCHING_UP = "catching_up"` to the existing `ReplicaSinkState` enum.

Its cursor property returns `None` until a full image is installed, then
returns the last completely applied pair.

The recorded scope added a Replica executor control:

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

**Recorded activity 4 — Design outcome: one connect path for full and partial**

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

**Recorded activity 5 — Preserve cursor on disconnect/overflow/source loss**

The recorded scope added manual:

```python
async def disconnect(self) -> ReplicaStatus:
```

It detaches the current generation, stops the apply task, clears unapplied
queues, asks the Primary runtime to release sink ownership, enters `DETACHED`,
and preserves the last completed cursor.

Overflow and source loss also preserve the cursor. They never advance
`applied_seq` for a queued-but-unapplied batch.

**Recorded activity 6 — Verification intent: partial, overflow, lag, and shutdown tests**

Historical verification covered targeted or full test coverage, including `tests/replication/test_partial_resync.py`, `tests/replication/test_sink_overflow.py`, `tests/replication/test_sink_lag.py`, `tests/reliability/test_reliability_shutdown.py`.

Historical expected evidence: PASS with no sink task leak.

**Recorded activity 7 — Commit partial catch-up**

### Milestone 4: Full-sync fallback, restart, and queue-overflow recovery

**Recorded activity 1 — Design outcome: failing fallback matrix**

Historical test coverage included:

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

**Recorded activity 2 — Verification intent: fallback tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/replication/test_partial_resync.py`, `tests/replication/test_sink_overflow.py`, `tests/reliability/test_restart.py`.

Historical expected evidence: boundary or restart cases fail until all cursor paths are wired.

**Recorded activity 3 — Inject deterministic Primary IDs in tests**

The recorded change extended `_RuntimeTestHooks`:

```python
replication_id_factory: Callable[[], str] | None = None
```

Runtime production construction uses:

```python
lambda: uuid.uuid4().hex
```

The design used the factory hook introduced in Milestone 2. Test construction passes a
deterministic iterator-backed callable:

```python
identities = iter(("primary-A", "promoted-B"))

def factory() -> str:
    return next(identities)
```

Pass that same factory into executor so initial startup and later promotion
consume distinct deterministic IDs without hardcoding global state.

**Recorded activity 4 — Complete coverage and fallback behavior**

The design made `ReplicationBacklog.missing_after` the only coverage decision. Do not
infer coverage from Replica lag or queue size. A full attachment always
captures the current `SnapshotImage`; later writes remain queued behind its
boundary.

On full install, reset Replica operational LFU/LRU metadata as already defined
by `Database.install_snapshot`.

**Recorded activity 5 — Verification intent: fallback matrix and commit**

Historical verification covered targeted or full test coverage, including `tests/replication`, `tests/reliability/test_restart.py`, `tests/reliability/test_reliability_shutdown.py`.

Historical expected evidence: PASS.

### Milestone 5: Promotion fencing and preserved asynchronous loss

**Recorded activity 1 — Design outcome: failing promotion-history tests**

The recorded change extended promotion tests:

```python
old_id = sink.status.replication_id
result = await sink.promote(source_alive=False)
assert result.replication_id != old_id
assert promoted.debug_replication_backlog_count == 0
```

Reconnect a Replica with the old ID and same visible state; assert full sync.
Historical test and implementation coverage included after promotion and assert the new backlog begins at the next global
sequence.

**Recorded activity 2 — Verification intent: promotion tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/replication/test_promotion.py`, `tests/reliability/test_lost_acked_write.py`.

Historical expected evidence: promotion has no history ID or backlog fencing.

**Recorded activity 3 — Fence history on promotion**

In the executor promotion control:

```python
self._active_source_generation = None
self._active_source_id = None
self._replica_read_only = False
self.replication_id = self._replication_id_factory()
self.replication_backlog.clear()
```

The interface returned:

```python
PromotionResult(
    applied_seq=self.database.commit_seq,
    writable=True,
    replication_id=self.replication_id,
)
```

The recorded change updated sink state/status with the new ID.

**Recorded activity 4 — Keep acknowledged-write loss test explicit**

The test must still perform:

```text
Primary acknowledges seq=N
Replica remains at N-1
Primary simulated crash
Replica manual promotion
GET for the seq=N write returns null
```

The design did not auto-recover from a dead source or consult a quorum.

**Recorded activity 5 — Verification intent: promotion/reliability suites and commit**

Historical verification covered targeted or full test coverage, including `tests/replication/test_promotion.py`, `tests/reliability/test_lost_acked_write.py`, `tests/reliability/test_final_acceptance.py`.

Historical expected evidence: PASS.

### Milestone 6: Observability, docs, and final acceptance

**Recorded activity 1 — Extend status and runtime statistics**

The interface exposed read-only:

```python
replication_id: str
primary_seq: int
backlog_oldest_seq: int | None
backlog_newest_seq: int | None
backlog_batch_count: int
full_sync_count: int
partial_sync_count: int
```

The recorded change extended `ReplicaStatus` with `replication_id`, `sync_mode`, and retained cursor.
Stats inspection must not mutate backlog or Replica state.

**Recorded activity 2 — Design outcome: final acceptance assertions**

The recorded change extended final acceptance to prove:

- adapter-free core contracts still pass;
- short gap uses partial sync;
- backlog gap uses full sync;
- promotion changes identity;
- asynchronous acknowledged loss still occurs;
- shutdown leaves zero Replica/runtime owned tasks.

**Recorded activity 3 — Update README and behavior matrix**

Historical documentation covered logical batch offsets, manual reconnect, backlog capacity, full-sync
fallback, new Primary ID on restart/promotion, and acknowledged-write loss.
Remove partial resync from non-goals. Keep PSYNC wire compatibility,
heartbeats, elections, Sentinel, Cluster, and WAIT explicitly excluded.

**Recorded activity 4 — Verification intent: complete repository verification**

Historical verification covered targeted or full test coverage, static analysis, diff hygiene.

Historical expected evidence: all checks pass.

**Recorded activity 5 — Commit final project acceptance**

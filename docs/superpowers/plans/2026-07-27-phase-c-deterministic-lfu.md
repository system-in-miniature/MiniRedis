# Phase C Deterministic LFU Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. The user selected inline execution;
> do not dispatch subagents.

**Goal:** Add deterministic, decaying allkeys-LFU eviction without persisting
or replicating operational access metadata.

**Architecture:** Database entries own volatile frequency and decay-anchor
metadata. One pure helper projects decayed frequency, database touches
materialize it on actual access, and eviction only reads projections. The
executor passes injected-clock time and the configured interval into client
write/read touches.

**Tech Stack:** Python 3.13, injected `Clock`, dataclasses, pytest,
pytest-asyncio.

---

## File map

- Modify `src/miniredis/config.py`: LFU policy and decay interval validation.
- Create `src/miniredis/core/frequency.py`: pure decay projection.
- Modify `src/miniredis/core/database.py`: LFU metadata, client-write
  preservation, read touches, recovery neutrality, and transaction fork
  support.
- Modify `src/miniredis/core/executor.py`: pass clock/interval and count
  expiry/eviction operations.
- Modify `src/miniredis/core/eviction.py`: deterministic LFU candidate order.
- Modify `src/miniredis/runtime.py`: LFU and eviction statistics.
- Modify `tests/unit/core/test_domain_types.py`.
- Create `tests/unit/core/test_frequency.py`.
- Modify `tests/contract/test_eviction.py`.
- Modify `tests/unit/persistence/test_codec.py`.
- Modify `tests/reliability/test_restart.py`.
- Modify `tests/replication/test_sink_attach.py`.
- Modify `tests/mechanisms/test_transactions.py`.
- Modify `docs/behavior-matrix.md` and `README.md`.

### Task 1: Configuration and pure decay projection

- [ ] **Step 1: Add failing configuration and projection tests**

Create `tests/unit/core/test_frequency.py`:

```python
@pytest.mark.parametrize(
    ("frequency", "last_ms", "now_ms", "interval_ms", "expected"),
    [
        (8, 0, 999, 1000, (8, 0)),
        (8, 0, 1000, 1000, (4, 1000)),
        (9, 0, 3000, 1000, (1, 3000)),
        (1, 0, 5000, 1000, (0, 5000)),
    ],
)
def test_project_frequency_decay(
    frequency, last_ms, now_ms, interval_ms, expected
):
    assert project_frequency(
        frequency, last_ms, now_ms, interval_ms
    ) == expected
```

Extend config tests:

```python
assert MiniRedisConfig(eviction_policy="allkeys-lfu").eviction_policy == "allkeys-lfu"
with pytest.raises(ValueError, match="lfu_decay_interval_ms"):
    MiniRedisConfig(lfu_decay_interval_ms=0)
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/unit/core/test_frequency.py \
  tests/test_project_contract.py
```

Expected: missing module and rejected LFU policy.

- [ ] **Step 3: Implement the pure helper and config**

Create:

```python
def project_frequency(
    frequency: int,
    last_decay_ms: int,
    now_ms: int,
    interval_ms: int,
) -> tuple[int, int]:
    if frequency < 0:
        raise ValueError("frequency cannot be negative")
    if interval_ms <= 0:
        raise ValueError("LFU decay interval must be positive")
    if now_ms <= last_decay_ms:
        return frequency, last_decay_ms
    windows = (now_ms - last_decay_ms) // interval_ms
    if windows == 0:
        return frequency, last_decay_ms
    return frequency // (2**windows), last_decay_ms + windows * interval_ms
```

Use a shift or early-zero branch if necessary to avoid constructing an
unbounded `2**windows`; behavior must remain identical.

Update:

```python
EvictionPolicy = Literal["noeviction", "allkeys-lru", "allkeys-lfu"]
lfu_decay_interval_ms: int = 60_000
```

and validate it as positive.

- [ ] **Step 4: Run and commit**

Run:

```bash
uv run pytest -q tests/unit/core/test_frequency.py tests/test_project_contract.py
```

Expected: PASS.

Commit:

```bash
git add src/miniredis/config.py src/miniredis/core/frequency.py \
  tests/unit/core/test_frequency.py tests/test_project_contract.py
git commit -m "feat: define deterministic LFU decay"
```

### Task 2: Volatile LFU metadata and touch semantics

- [ ] **Step 1: Add failing Database metadata tests**

Add:

```python
def test_client_updates_preserve_decay_and_increment_frequency():
    database = Database()
    database.apply_batch(
        put_batch(1, b"k", b"one"),
        track_access=True,
        now_ms=0,
        lfu_decay_interval_ms=1000,
    )
    assert database.entries[b"k"].frequency == 1
    database.apply_batch(
        put_batch(2, b"k", b"two", mutation_version=2),
        track_access=True,
        now_ms=2000,
        lfu_decay_interval_ms=1000,
    )
    assert database.entries[b"k"].frequency == 1
    assert database.entries[b"k"].last_frequency_decay_ms == 2000


def test_recovery_and_replica_puts_start_neutral():
    database = Database()
    database.apply_batch(
        put_batch(1, b"k", b"v"),
        track_access=False,
        now_ms=5000,
        lfu_decay_interval_ms=1000,
    )
    assert database.entries[b"k"].frequency == 0
    assert database.entries[b"k"].last_access_tick == 0
```

Also verify `Database.fork()` from Phase B copies LFU metadata independently.

- [ ] **Step 2: Run Database tests and verify RED**

Run:

```bash
uv run pytest -q tests/unit/core/test_domain_types.py
```

Expected: `Entry` has no LFU fields and `apply_batch` rejects new keywords.

- [ ] **Step 3: Add metadata and materialized touches**

Extend `Entry`:

```python
frequency: int
last_frequency_decay_ms: int
```

Extend `Database.apply_batch`:

```python
def apply_batch(
    self,
    batch: CommitBatch,
    *,
    track_access: bool,
    now_ms: int = 0,
    lfu_decay_interval_ms: int = 60_000,
) -> None:
```

For each `PutEntry`, inspect the previous staged entry. If `track_access`:

- a new key gets frequency `1`;
- an existing key uses `project_frequency`, then adds one;
- access tick advances exactly once per Put operation.

If not `track_access`, assign neutral frequency/access metadata. Snapshot
installation also assigns neutral metadata anchored at `now_ms`.

Update:

```python
def touch_if_live(
    self,
    key: bytes,
    now_ms: int,
    lfu_decay_interval_ms: int = 60_000,
) -> bool:
```

It projects decay, increments frequency, advances access tick, and mutates
only a live entry.

Update `fork()` to copy both fields.

- [ ] **Step 4: Pass access context from executor**

Add `lfu_decay_interval_ms` to `CommandExecutor.__init__`. Pass config from
runtime. In `_commit_prepared`:

```python
self.database.apply_batch(
    batch,
    track_access=prepared.trigger is CommitTrigger.CLIENT,
    now_ms=self.clock.now_ms(),
    lfu_decay_interval_ms=self.lfu_decay_interval_ms,
)
```

Pass the same interval to all `touch_if_live` calls. Replica application
continues with `track_access=False`.

- [ ] **Step 5: Run core, transaction, recovery, and replica tests**

Run:

```bash
uv run pytest -q \
  tests/unit/core \
  tests/mechanisms/test_transactions.py \
  tests/reliability/test_restart.py \
  tests/replication/test_sink_attach.py
```

Expected: PASS.

- [ ] **Step 6: Commit metadata support**

```bash
git add src/miniredis/core/database.py src/miniredis/core/executor.py \
  src/miniredis/runtime.py tests/unit/core tests/mechanisms \
  tests/reliability tests/replication
git commit -m "feat: track volatile LFU access metadata"
```

### Task 3: Read-only LFU eviction planning

- [ ] **Step 1: Add failing LFU contract tests**

Add tests using `FakeClock`:

```python
@pytest.mark.asyncio
async def test_lfu_evicts_lowest_effective_frequency():
    clock = FakeClock(0)
    async with MiniRedis.open(
        clock=clock,
        maxmemory=260,
        eviction_policy="allkeys-lfu",
        lfu_decay_interval_ms=1000,
    ) as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SET", (b"hot", b"x")))
        await c.execute(CommandRequest(b"SET", (b"cold", b"x")))
        for _ in range(4):
            await c.execute(CommandRequest(b"GET", (b"hot",)))
        assert await c.execute(
            CommandRequest(b"SET", (b"new", b"x" * 60))
        ) == Ok()
        assert await c.execute(CommandRequest(b"GET", (b"cold",))) == Bytes(None)
        assert await c.execute(CommandRequest(b"GET", (b"hot",))) == Bytes(b"x")
```

Add tests for multi-window decay, access-tick tie-break, binary-key tie-break,
expired-first behavior, target exclusion, and no mutation during
`enforce_memory`.

- [ ] **Step 2: Run LFU contracts and verify RED**

Run:

```bash
uv run pytest -q tests/contract/test_eviction.py
```

Expected: LFU currently falls through LRU ordering.

- [ ] **Step 3: Implement pure candidate ordering**

Add:

```python
def _lfu_candidates(
    database: Database,
    *,
    now_ms: int,
    decay_interval_ms: int,
    excluded: set[bytes],
) -> list[tuple[int, int, bytes]]:
    return sorted(
        (
            project_frequency(
                entry.frequency,
                entry.last_frequency_decay_ms,
                now_ms,
                decay_interval_ms,
            )[0],
            entry.last_access_tick,
            key,
        )
        for key, entry in database.entries.items()
        if key not in excluded and not is_expired(entry, now_ms)
    )
```

In `enforce_memory`, select LRU or LFU candidate tuples explicitly. Iterate
keys from the selected ordering and preserve the existing same-commit eviction
operations. Never call `touch_if_live` or update a decay anchor while
planning.

- [ ] **Step 4: Add eviction/expiry counters**

In `_commit_prepared`, count `DeleteKey` reasons after a successful database
apply:

```python
self.expired_key_count += sum(
    isinstance(op, DeleteKey) and op.reason is DeleteReason.EXPIRED
    for op in batch.operations
)
self.evicted_key_count += sum(
    isinstance(op, DeleteKey) and op.reason is DeleteReason.EVICTED
    for op in batch.operations
)
```

Expose them through `RuntimeStats` without changing state.

Also expose:

```python
key_count: int
logical_memory_usage: int
expired_key_count: int
evicted_key_count: int
```

`key_count` is physical live-table size at inspection time; diagnostics do not
run lazy expiry or touch access metadata.

- [ ] **Step 5: Run eviction and memory regressions**

Run:

```bash
uv run pytest -q \
  tests/contract/test_eviction.py \
  tests/contract/test_ttl.py \
  tests/reliability/test_phase3_invariants.py \
  tests/mechanisms/test_transactions.py
```

Expected: PASS.

- [ ] **Step 6: Commit LFU policy**

```bash
git add src/miniredis/core/eviction.py src/miniredis/core/executor.py \
  src/miniredis/runtime.py tests/contract/test_eviction.py \
  tests/reliability tests/mechanisms
git commit -m "feat: evict with deterministic decaying LFU"
```

### Task 4: Persistence neutrality and Phase C acceptance

- [ ] **Step 1: Add failing restart and full-sync neutrality tests**

Write frequency up through repeated GETs, save/restart or full-sync, and assert
the recovered entry has:

```python
assert recovered.database.entries[b"k"].frequency == 0
assert recovered.database.entries[b"k"].last_access_tick == 0
```

Also assert encoded `StoredEntry` and AOF/snapshot payloads do not gain LFU
fields.

- [ ] **Step 2: Run reliability tests**

Run:

```bash
uv run pytest -q \
  tests/unit/persistence/test_codec.py \
  tests/reliability/test_restart.py \
  tests/replication/test_sink_attach.py
```

Expected: PASS after Task 2; failures indicate accidental metadata
persistence.

- [ ] **Step 3: Update documentation**

Document:

- deterministic halving by injected time;
- successful read/client-write touch rules;
- LFU/LRU tie-breaking;
- metadata reset on restart/full sync;
- intentional difference from production Redis's probabilistic counter.

- [ ] **Step 4: Run complete verification**

Run:

```bash
uv run ruff check .
uv run pytest -q
git diff --check
```

Expected: all checks pass.

- [ ] **Step 5: Commit Phase C acceptance**

```bash
git add tests README.md docs/behavior-matrix.md
git commit -m "docs: accept deterministic LFU phase"
```

# Phase B Transactions and Atomic Functions Design and Implementation History

**Historical objective:** Add Redis-shaped session transactions, optimistic WATCH, and the
COMPAREDEL/CHECKDECR atomic primitives with one executor-owned commit boundary.

**Architecture:** Parse and queue typed commands per executor session. EXEC
evaluates them sequentially against a deep transaction database fork, retains
per-command replies, and emits all successful operations as one durable
CommitBatch. A persistent-in-history per-key revision ledger detects WATCH
changes, including create-delete cycles.

**Tech Stack:** Python 3.13, asyncio, frozen/slotted dataclasses, pytest,
pytest-asyncio, existing AOF and replication CommitBatch path.

---

## File map

- Changed `src/miniredis/commands/model.py`: transaction and atomic command
  types and traits.
- Changed `src/miniredis/commands/parser.py`: transaction and atomic parsing.
- Changed `src/miniredis/core/reply.py`: explicit `NullArray`.
- Changed `src/miniredis/adapters/resp2.py`: null-array encoding.
- Added `src/miniredis/core/transactions.py`: session transaction state and
  workspace result types.
- Changed `src/miniredis/core/database.py`: revision ledger and deep fork.
- Changed `src/miniredis/core/blocking.py`: transaction-local waiter
  reservations.
- Changed `src/miniredis/core/planning.py`: atomic primitive plans.
- Changed `src/miniredis/core/executor.py`: queueing, WATCH validation, EXEC,
  cleanup, and statistics.
- Changed `src/miniredis/runtime.py`: transaction statistics.
- Evidence covered `tests/unit/commands/test_parser.py`.
- Evidence covered `tests/unit/commands/test_command_traits.py`.
- Changed `tests/adapters/test_resp2_mapping.py`.
- Added `tests/contract/test_atomic_functions.py`.
- Added `tests/mechanisms/test_transactions.py`.
- Added `tests/mechanisms/test_watch.py`.
- Added `tests/reliability/test_transaction_commit.py`.
- Changed `tests/concurrency/test_shutdown.py`.
- Changed `docs/behavior-matrix.md` and `README.md`.

### Milestone 1: Reply and command surface

**Recorded activity 1 — Test intent: failing parser and RESP2 mapping tests**

The recorded scope added exact typed expectations:

```python
assert parse(CommandRequest(b"MULTI")) == Multi()
assert parse(CommandRequest(b"EXEC")) == Exec()
assert parse(CommandRequest(b"DISCARD")) == Discard()
assert parse(CommandRequest(b"WATCH", (b"a", b"b"))) == Watch((b"a", b"b"))
assert parse(CommandRequest(b"UNWATCH")) == Unwatch()
assert parse(CommandRequest(b"COMPAREDEL", (b"k", b"token"))) == CompareDelete(
    b"k", b"token"
)
assert parse(CommandRequest(b"CHECKDECR", (b"stock", b"2"))) == CheckDecrement(
    b"stock", 2
)
```

Reject empty WATCH, non-positive CHECKDECR amounts, and wrong arities. Add:

```python
assert encode_outbound(NullArray()) == b"*-1\r\n"
```

**Recorded activity 2 — Verification intent: focused tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/unit/commands/test_parser.py`, `tests/unit/commands/test_command_traits.py`, `tests/adapters/test_resp2_mapping.py`.

Historical expected evidence: import/collection failures for the new types.

**Recorded activity 3 — Design outcome: command and reply types**

The recorded scope added frozen slotted command dataclasses:

```python
@dataclass(frozen=True, slots=True)
class Multi: pass

@dataclass(frozen=True, slots=True)
class Exec: pass

@dataclass(frozen=True, slots=True)
class Discard: pass

@dataclass(frozen=True, slots=True)
class Watch:
    keys: tuple[bytes, ...]

@dataclass(frozen=True, slots=True)
class Unwatch: pass

@dataclass(frozen=True, slots=True)
class CompareDelete:
    key: bytes
    expected: bytes

@dataclass(frozen=True, slots=True)
class CheckDecrement:
    key: bytes
    amount: int
```

Classify only `CompareDelete` and `CheckDecrement` as dataset-mutating.
Transaction controls are non-dataset-mutating.

The recorded scope added:

```python
@dataclass(frozen=True, slots=True)
class NullArray:
    pass
```

to `Reply`, and map it to `RespArray(None)`.

**Recorded activity 4 — Design outcome: parser branches**

The design used strict arity and existing `parse_int64`:

```python
case b"MULTI":
    _require_arity(name, args, 0)
    return Multi()
case b"EXEC":
    _require_arity(name, args, 0)
    return Exec()
case b"DISCARD":
    _require_arity(name, args, 0)
    return Discard()
case b"WATCH":
    _require_min_arity(name, args, 1)
    return Watch(args)
case b"UNWATCH":
    _require_arity(name, args, 0)
    return Unwatch()
case b"COMPAREDEL":
    _require_arity(name, args, 2)
    return CompareDelete(args[0], args[1])
case b"CHECKDECR":
    _require_arity(name, args, 2)
    amount = parse_int64(args[1])
    if amount <= 0:
        raise CommandParseError("amount must be a positive integer")
    return CheckDecrement(args[0], amount)
```

**Recorded activity 5 — Verification intent: focused tests and commit**

Historical verification covered targeted or full test coverage, including `tests/unit/commands/test_parser.py`, `tests/unit/commands/test_command_traits.py`, `tests/adapters/test_resp2_mapping.py`, `tests/adapters/test_resp2_encode.py`.

Historical expected evidence: PASS.

### Milestone 2: Per-key revision ledger and transaction database fork

**Recorded activity 1 — Design outcome: failing Database unit tests**

The recorded scope added or extend `tests/unit/core/test_domain_types.py`:

```python
def put_batch(
    seq: int,
    key: bytes,
    value: bytes,
    mutation_version: int = 1,
) -> CommitBatch:
    return CommitBatch(
        seq,
        (
            PutEntry(
                key,
                StoredEntry(
                    StoredString(value),
                    None,
                    mutation_version,
                ),
            ),
        ),
        CommitTrigger.CLIENT,
    )


def delete_batch(seq: int, key: bytes) -> CommitBatch:
    return CommitBatch(
        seq,
        (DeleteKey(key, DeleteReason.CLIENT),),
        CommitTrigger.CLIENT,
    )


def populated_database() -> Database:
    database = Database()
    database.apply_batch(put_batch(1, b"k", b"v"), track_access=True)
    return database


def test_revision_survives_create_delete_cycle():
    database = Database()
    database.apply_batch(put_batch(1, b"k", b"v"), track_access=True)
    created = database.revision(b"k")
    database.apply_batch(delete_batch(2, b"k"), track_access=True)
    assert b"k" not in database.entries
    assert database.revision(b"k") > created


def test_fork_is_deep_and_preserves_runtime_metadata():
    database = populated_database()
    fork = database.fork()
    fork.touch_if_live(b"k", 0)
    fork.apply_batch(delete_batch(fork.commit_seq + 1, b"k"), track_access=True)
    assert b"k" in database.entries
    assert database.revision(b"k") != fork.revision(b"k")
```

**Recorded activity 2 — Verification intent: the unit test and verify RED**

Historical verification covered targeted or full test coverage, including `tests/unit/core/test_domain_types.py`.

Historical expected evidence: `Database` has no `revision` or `fork`.

**Recorded activity 3 — Design outcome: revision tracking**

The recorded scope added:

```python
self.key_revisions: dict[bytes, int] = {}
self.revision_clock = 0

def revision(self, key: bytes) -> int:
    return self.key_revisions.get(key, 0)

def _advance_revision(self, key: bytes) -> None:
    self.revision_clock += 1
    self.key_revisions[key] = self.revision_clock
```

During `apply_batch`, call `_advance_revision(operation.key)` for every
operation in batch order, including deletion of an absent key. Stage revision
state alongside entries so a failed batch cannot partially advance it.

During `install_snapshot`, seed each live key's revision deterministically
from sorted entry order and keep `revision_clock` at the greatest assigned
value. Recovery replay then advances revisions through batches.

**Recorded activity 4 — Design outcome: a deep `Database.fork`**

The recorded scope added a new Database and copy:

- thawed values via `freeze_value`/`thaw_value`;
- TTL, mutation version, access tick, logical size;
- commit/access/revision clocks;
- the key revision dictionary;
- LFU fields added later by Phase C through named constructor arguments.

The design used an explicit constructor:

```python
def fork(self) -> Database:
    forked = Database()
    forked.entries = {
        key: Entry(
            value=thaw_value(freeze_value(entry.value)),
            expire_at_ms=entry.expire_at_ms,
            mutation_version=entry.mutation_version,
            last_access_tick=entry.last_access_tick,
            logical_size=entry.logical_size,
        )
        for key, entry in self.entries.items()
    }
    forked.commit_seq = self.commit_seq
    forked.access_tick = self.access_tick
    forked.logical_usage = self.logical_usage
    forked.key_revisions = dict(self.key_revisions)
    forked.revision_clock = self.revision_clock
    return forked
```

**Recorded activity 5 — Verification intent: core, recovery, and replication tests and commit**

Historical verification covered targeted or full test coverage, including `tests/unit/core`, `tests/unit/persistence/test_recovery.py`, `tests/reliability/test_restart.py`, `tests/replication/test_sink_attach.py`.

Historical expected evidence: PASS.

### Milestone 3: Session transaction state and WATCH lifecycle

**Recorded activity 1 — Design outcome: failing MULTI and WATCH state tests**

The recorded scope added `tests/mechanisms/test_transactions.py` and
`tests/mechanisms/test_watch.py` covering:

```python
@pytest.mark.asyncio
async def test_multi_queues_and_discard_clears_state():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        assert await c.execute(CommandRequest(b"MULTI")) == Ok()
        assert await c.execute(CommandRequest(b"SET", (b"k", b"v"))) == Ok(
            b"QUEUED"
        )
        assert await c.execute(CommandRequest(b"DISCARD")) == Ok()
        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(None)


@pytest.mark.asyncio
async def test_unwatch_clears_recorded_revisions():
    async with MiniRedis.open() as runtime:
        owner = runtime.direct_client()
        assert await owner.execute(CommandRequest(b"WATCH", (b"k",))) == Ok()
        assert runtime.executor.watched_key_count == 1
        assert await owner.execute(CommandRequest(b"UNWATCH")) == Ok()
        assert runtime.executor.watched_key_count == 0
```

Also test nested MULTI, EXEC/DISCARD without MULTI, WATCH after MULTI,
UNWATCH, and session close. WATCH conflict cases are added with EXEC in Milestone 4.

**Recorded activity 2 — Verification intent: the mechanism tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/mechanisms/test_transactions.py`, `tests/mechanisms/test_watch.py`.

Historical expected evidence: controls currently fall through ordinary planning.

**Recorded activity 3 — Design outcome: focused transaction state types**

The recorded scope added `core/transactions.py`:

```python
@dataclass(slots=True)
class TransactionState:
    active: bool = False
    dirty: bool = False
    queued: list[Command] = field(default_factory=list)
    watched: dict[bytes, int] = field(default_factory=dict)

    def reset_transaction(self) -> None:
        self.active = False
        self.dirty = False
        self.queued.clear()

    def clear_all(self) -> None:
        self.reset_transaction()
        self.watched.clear()
```

The executor stores `dict[int, TransactionState]` and creates state lazily.
Session close, shutdown, and terminal cleanup remove session state.

**Recorded activity 4 — Route controls and parse rejections through session state**

Before ordinary command execution:

- `MULTI`: require inactive, set active, return `OK`;
- `DISCARD`: require active, clear all, return `OK`;
- `WATCH`: require inactive, record current revisions, return `OK`;
- `UNWATCH`: clear watched revisions, return `OK`;
- `EXEC`: delegate to Milestone 4;
- an allowed ordinary command while active: append and return `QUEUED`;
- a disallowed command while active: mark dirty and return an error;
- `RejectRequest` while active: mark dirty before returning its parse error.

The design used an explicit frozen set of disallowed command types:

```python
TRANSACTION_DISALLOWED = (
    BlockingPop,
    Subscribe,
    Unsubscribe,
    Publish,
    Multi,
    Watch,
)
```

**Recorded activity 5 — Verification intent: lifecycle tests and commit**

Historical verification covered targeted or full test coverage, including `tests/mechanisms/test_transactions.py`, `tests/mechanisms/test_watch.py`, `tests/concurrency/test_shutdown.py`, `tests/reliability/test_worker_failure.py`.

Historical expected evidence: basic queue, discard, watch, and cleanup cases pass. EXEC execution
and conflict cases are introduced in Milestone 4.

### Milestone 4: EXEC workspace and one-batch commit

**Recorded activity 1 — Design outcome: failing execution semantics tests**

The recorded scope added cases for read-your-prior-write, runtime error continuation, dirty abort,
cross-client non-interleaving, no-op EXEC, one commit, and WATCH conflicts:

```python
@pytest.mark.asyncio
async def test_exec_reads_prior_write_and_keeps_runtime_error_slots():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"MULTI"))
        await c.execute(CommandRequest(b"SET", (b"k", b"1")))
        await c.execute(CommandRequest(b"LPUSH", (b"k", b"x")))
        await c.execute(CommandRequest(b"INCR", (b"k",)))
        before = runtime.debug_commit_seq
        assert await c.execute(CommandRequest(b"EXEC")) == Items(
            (
                Ok(),
                Failure(
                    "WRONGTYPE",
                    "operation against a key holding the wrong kind of value",
                ),
                Number(2),
            )
        )
        assert runtime.debug_commit_seq == before + 1


@pytest.mark.asyncio
async def test_watch_detects_create_then_delete():
    async with MiniRedis.open() as runtime:
        owner, rival = runtime.direct_client(), runtime.direct_client()
        assert await owner.execute(CommandRequest(b"WATCH", (b"k",))) == Ok()
        await rival.execute(CommandRequest(b"SET", (b"k", b"v")))
        await rival.execute(CommandRequest(b"DEL", (b"k",)))
        await owner.execute(CommandRequest(b"MULTI"))
        await owner.execute(CommandRequest(b"GET", (b"k",)))
        assert await owner.execute(CommandRequest(b"EXEC")) == NullArray()
```

The recorded scope added an AOF restart test asserting the complete transaction state recovers from
one `CommitBatch`.

**Recorded activity 2 — Verification intent: transaction/reliability tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/mechanisms/test_transactions.py`, `tests/reliability/test_transaction_commit.py`.

Historical expected evidence: EXEC has no workspace implementation.

**Recorded activity 3 — Design outcome: transaction-local waiter reservations**

Change `prepare_list_wakeups` to accept an optional caller-owned reservation
set:

```python
def prepare_list_wakeups(
    key: bytes,
    pushed: PutEntry,
    waiters: WaiterRegistry,
    reserved: set[WaiterId] | None = None,
) -> tuple[CommitOperation, tuple[WaiterWakeup, ...]]:
    owned = set() if reserved is None else reserved
    stored = pushed.entry.value
    if not isinstance(stored, StoredList):
        raise TypeError("push operation must contain StoredList")
    remaining = deque(stored.items)
    wakeups: list[WaiterWakeup] = []
    while remaining:
        waiter = waiters.peek(key, owned)
        if waiter is None:
            break
        owned.add(waiter.waiter_id)
        item = remaining.popleft() if waiter.left else remaining.pop()
        wakeups.append(
            WaiterWakeup(
                waiter.waiter_id,
                waiter.generation,
                key,
                item,
            )
        )
    final: CommitOperation
    if remaining:
        final = PutEntry(
            key,
            StoredEntry(
                StoredList(tuple(remaining)),
                pushed.entry.expire_at_ms,
                pushed.entry.mutation_version,
            ),
        )
    else:
        final = DeleteKey(key, DeleteReason.CLIENT)
    return final, tuple(wakeups)
```

Ordinary execution passes no set. One EXEC uses one set across all queued
commands so a waiter cannot consume twice before commit.

**Recorded activity 4 — Design outcome: TransactionWorkspace evaluation**

The recorded scope added:

```python
@dataclass(slots=True)
class TransactionWorkspace:
    database: Database
    operations: list[CommitOperation] = field(default_factory=list)
    replies: list[Reply] = field(default_factory=list)
    touch_keys: list[bytes] = field(default_factory=list)
    wakeups: list[WaiterWakeup] = field(default_factory=list)
    reserved_waiters: set[WaiterId] = field(default_factory=set)
```

For each queued command:

1. enforce replica read-only rules;
2. plan against `workspace.database`;
3. attach list wakeups using `workspace.reserved_waiters`;
4. append the reply;
5. if successful and mutating, apply an ephemeral contiguous batch to the
   fork and append the plan operations in order;
6. materialize touches only on the fork and retain keys for the real database.

After all commands:

- if operations exist, call `_commit_prepared` once with their ordered tuple;
- then touch the real database and transition retained waiter wakeups;
- return `Items(tuple(replies))`;
- if durability fails, return the normal durability error and wake nobody.

Before evaluation, a dirty transaction returns
`Failure("EXECABORT", "transaction discarded because of previous errors")`.
A watch mismatch returns `NullArray`. Every EXEC path clears transaction and
watch state in a `finally` block.

**Recorded activity 5 — Verification intent: transaction, blocking, AOF, and replication tests**

Historical verification covered targeted or full test coverage, including `tests/mechanisms/test_transactions.py`, `tests/mechanisms/test_watch.py`, `tests/mechanisms/test_blpop_push_batch.py`, `tests/reliability/test_transaction_commit.py`, `tests/reliability/test_restart.py`, `tests/replication/test_sink_attach.py`.

Historical expected evidence: PASS; one EXEC mutation produces one source batch and one replicated
batch.

**Recorded activity 6 — Commit EXEC**

### Milestone 5: COMPAREDEL and CHECKDECR

**Recorded activity 1 — Design outcome: failing atomic-function contracts**

The recorded scope added `tests/contract/test_atomic_functions.py`:

```python
@pytest.mark.asyncio
async def test_comparedel_only_removes_matching_string():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SET", (b"lock", b"owner", b"PX", b"1000")))
        before = runtime.debug_commit_seq
        assert await c.execute(
            CommandRequest(b"COMPAREDEL", (b"lock", b"other"))
        ) == Number(0)
        assert runtime.debug_commit_seq == before
        assert await c.execute(
            CommandRequest(b"COMPAREDEL", (b"lock", b"owner"))
        ) == Number(1)


@pytest.mark.asyncio
async def test_checkdecr_preserves_ttl_and_rejects_insufficient_stock():
    clock = FakeClock(0)
    async with MiniRedis.open(clock=clock) as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SET", (b"stock", b"5", b"PX", b"1000")))
        ttl = await c.execute(CommandRequest(b"PTTL", (b"stock",)))
        assert await c.execute(
            CommandRequest(b"CHECKDECR", (b"stock", b"2"))
        ) == Number(3)
        assert await c.execute(CommandRequest(b"PTTL", (b"stock",))) == ttl
        before = runtime.debug_commit_seq
        assert await c.execute(
            CommandRequest(b"CHECKDECR", (b"stock", b"4"))
        ) == Failure("INSUFFICIENT", "insufficient value")
        assert runtime.debug_commit_seq == before
```

Also cover missing, WRONGTYPE, invalid stored integer, overflow boundaries,
transaction queueing, and concurrent single-winner behavior.

**Recorded activity 2 — Verification intent: the contract and verify RED**

Historical verification covered targeted or full test coverage, including `tests/contract/test_atomic_functions.py`.

Historical expected evidence: both commands plan as unknown.

**Recorded activity 3 — Design outcome: atomic plans**

In `plan_general_and_strings`:

```python
case cmd.CompareDelete(key, expected):
    entry, expired = lookup(database, key, now_ms)
    if entry is None:
        return ExecutionPlan(Number(0), expired)
    if not isinstance(entry.value, StringValue):
        return ExecutionPlan(WRONGTYPE)
    if entry.value.data != expected:
        return ExecutionPlan(Number(0), touch_keys=(key,))
    return ExecutionPlan(
        Number(1),
        expired + (DeleteKey(key, DeleteReason.CLIENT),),
    )
```

For `CheckDecrement`, reuse `parse_int64`, enforce current value at least
amount, preserve expiry, and return:

```python
Failure("INSUFFICIENT", "insufficient value")
```

for missing or insufficient values. Only a successful decrement emits
`make_put`.

**Recorded activity 4 — Verification intent: atomic, transaction, concurrency, and TTL tests**

Historical verification covered targeted or full test coverage, including `tests/contract/test_atomic_functions.py`, `tests/mechanisms/test_transactions.py`, `tests/concurrency/test_atomic_incr.py`, `tests/contract/test_ttl.py`.

Historical expected evidence: PASS.

**Recorded activity 5 — Commit atomic primitives**

### Milestone 6: Phase B observability, acceptance, and docs

**Recorded activity 1 — Extend RuntimeStats and cleanup assertions**

The recorded scope added executor properties and stats fields:

```python
active_transactions: int
watched_keys: int
transaction_aborts: int
watch_aborts: int
```

Count state without mutating it. Extend shutdown and final-acceptance tests to
assert all transaction/session counts are zero after close.

**Recorded activity 2 — Update behavior documentation**

Historical documentation covered queue-time versus runtime errors, no rollback, one-batch crash
boundary, WATCH revision semantics, and the two custom commands. Remove
transactions from non-goals; keep Lua VM explicitly out of scope.

**Recorded activity 3 — Verification intent: complete verification**

Historical verification covered targeted or full test coverage, static analysis, diff hygiene.

Historical expected evidence: all checks pass.

**Recorded activity 4 — Recorded Phase B acceptance**

# Stage 08 · Deterministic eviction

### Goal

Enforce a logical maxmemory budget with atomic noeviction and exact-LRU decisions.

??? note "Deliverable files"
    - `src/miniredis/core/eviction.py`
    - `src/miniredis/core/planner.py`
    - `src/miniredis/runtime.py`
    - `tests/contract/test_domain_invariants.py`
    - `tests/contract/test_eviction.py`

### The problem at this point

Atomic command plans can still grow without a budget. Reading process RSS would make outcomes allocator-dependent, while evicting first and discovering that the target itself is oversized would destroy unrelated data for a command that ultimately fails.

### Test contract

#### See the failure first

An oversized target must return OOM without removing an existing key. Under exact LRU, the cold-key delete and the triggering put must share one commit; under noeviction, growth fails but a client delete remains legal. Expired bytes must be reclaimed before any of these choices.

??? note "File diff: tests/contract/test_domain_invariants.py"
    ```diff
    diff --git a/tests/contract/test_domain_invariants.py b/tests/contract/test_domain_invariants.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..cf5194487d4a80dfaa3b1c9640153796e0577706
    --- /dev/null
    +++ b/tests/contract/test_domain_invariants.py
    @@ -0,0 +1,50 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.database import Database
    +from miniredis.core.reply import Failure
    +
    +
    +@pytest.mark.asyncio
    +@pytest.mark.parametrize(
    +    "command_request",
    +    [
    +        CommandRequest(b"GET", (b"k",)),
    +        CommandRequest(b"HGET", (b"k", b"f")),
    +        CommandRequest(b"LRANGE", (b"k", b"0", b"-1")),
    +        CommandRequest(b"SISMEMBER", (b"k", b"m")),
    +        CommandRequest(b"ZSCORE", (b"k", b"m")),
    +    ],
    +)
    +async def test_wrongtype_never_allocates_commit(command_request):
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        if command_request.name == b"GET":
    +            await c.execute(CommandRequest(b"HSET", (b"k", b"f", b"v")))
    +        else:
    +            await c.execute(CommandRequest(b"SET", (b"k", b"v")))
    +        before = runtime.debug_commit_seq
    +        reply = await c.execute(command_request)
    +        assert isinstance(reply, Failure)
    +        assert reply.code == "WRONGTYPE"
    +        assert runtime.debug_commit_seq == before
    +
    +
    +@pytest.mark.asyncio
    +async def test_commits_rebuild_the_same_logical_database():
    +    runtime = MiniRedis.open()
    +    await runtime.start()
    +    client = runtime.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"s", b"1")))
    +    await client.execute(CommandRequest(b"HSET", (b"h", b"f", b"v")))
    +    await client.execute(CommandRequest(b"RPUSH", (b"l", b"a", b"b")))
    +    await client.execute(CommandRequest(b"SADD", (b"set", b"a", b"b")))
    +    await client.execute(CommandRequest(b"ZADD", (b"z", b"1", b"a")))
    +    batches = runtime.debug_applied_batches()
    +    expected = runtime.debug_logical_items()
    +    await runtime.close()
    +
    +    replay = Database()
    +    for batch in batches:
    +        replay.apply_batch(batch, track_access=False)
    +    assert replay.logical_items() == expected
    ```

**What this test locks**

It locks no-commit WRONGTYPE behavior across value families and proves that emitted batches rebuild the same logical database.

**How it constructs the counterexample**

It sends each read command to the wrong value type, then separately replays every observed batch into a fresh `Database`.

**Key test statement**

```python
for batch in batches:
    replay.apply_batch(batch, track_access=False)
assert replay.logical_items() == expected
```

**What a failure means**

A semantic failure allocated a commit, or live database changes escaped the operation log and cannot be replayed later.

??? note "File diff: tests/contract/test_eviction.py"
    ```diff
    diff --git a/tests/contract/test_eviction.py b/tests/contract/test_eviction.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..5973527b9f17d7c4afef29ee883ebe507dc996a6
    --- /dev/null
    +++ b/tests/contract/test_eviction.py
    @@ -0,0 +1,87 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.commit import DeleteKey, DeleteReason, PutEntry
    +from miniredis.core.reply import Bytes, Failure, Number, Ok
    +from tests.helpers.time import FakeClock
    +
    +
    +@pytest.mark.asyncio
    +async def test_oversized_target_does_not_evict_unrelated_key():
    +    async with MiniRedis.open(maxmemory=120, eviction_policy="allkeys-lru") as r:
    +        c = r.direct_client()
    +        assert await c.execute(CommandRequest(b"SET", (b"a", b"x"))) == Ok()
    +        before = r.debug_commit_seq
    +        reply = await c.execute(CommandRequest(b"SET", (b"huge", b"x" * 500)))
    +        assert reply == Failure("OOM", "command exceeds maxmemory")
    +        assert r.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"GET", (b"a",))) == Bytes(b"x")
    +
    +
    +@pytest.mark.asyncio
    +async def test_exact_lru_evicts_cold_key_in_same_commit_as_write():
    +    async with MiniRedis.open(maxmemory=260, eviction_policy="allkeys-lru") as r:
    +        c = r.direct_client()
    +        await c.execute(CommandRequest(b"SET", (b"cold", b"x")))
    +        await c.execute(CommandRequest(b"SET", (b"hot", b"x")))
    +        await c.execute(CommandRequest(b"GET", (b"hot",)))
    +        before = r.debug_commit_seq
    +        before_tick = r.database.access_tick
    +        assert await c.execute(CommandRequest(b"SET", (b"new", b"x" * 60))) == Ok()
    +        assert r.debug_commit_seq == before + 1
    +        assert r.database.access_tick == before_tick + 1
    +        batch = r.executor.debug_applied_batches()[-1]
    +        assert any(
    +            isinstance(operation, DeleteKey)
    +            and operation.key == b"cold"
    +            and operation.reason is DeleteReason.EVICTED
    +            for operation in batch.operations
    +        )
    +        assert any(
    +            isinstance(operation, PutEntry) and operation.key == b"new"
    +            for operation in batch.operations
    +        )
    +        assert await c.execute(CommandRequest(b"GET", (b"cold",))) == Bytes(None)
    +        assert await c.execute(CommandRequest(b"GET", (b"hot",))) == Bytes(b"x")
    +
    +
    +@pytest.mark.asyncio
    +async def test_noeviction_allows_delete_but_rejects_growth_atomically():
    +    async with MiniRedis.open(maxmemory=90, eviction_policy="noeviction") as r:
    +        c = r.direct_client()
    +        assert await c.execute(CommandRequest(b"SET", (b"a", b"x"))) == Ok()
    +        before = r.debug_commit_seq
    +        assert await c.execute(CommandRequest(b"SET", (b"b", b"x"))) == Failure(
    +            "OOM", "command exceeds maxmemory"
    +        )
    +        assert r.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"DEL", (b"a",))) == Number(1)
    +
    +
    +@pytest.mark.asyncio
    +async def test_expired_budget_is_purged_in_same_batch_before_noeviction_check():
    +    clock = FakeClock(0)
    +    async with MiniRedis.open(
    +        clock=clock,
    +        maxmemory=100,
    +        eviction_policy="noeviction",
    +    ) as r:
    +        c = r.direct_client()
    +        assert await c.execute(CommandRequest(b"SET", (b"old", b"x"))) == Ok()
    +        assert await c.execute(CommandRequest(b"EXPIRE", (b"old", b"1"))) == Number(1)
    +        clock.advance(1_000)
    +        before = r.debug_commit_seq
    +        assert await c.execute(CommandRequest(b"SET", (b"new", b"x"))) == Ok()
    +        assert r.debug_commit_seq == before + 1
    +        batch = r.executor.debug_applied_batches()[-1]
    +        assert any(
    +            isinstance(operation, DeleteKey)
    +            and operation.key == b"old"
    +            and operation.reason is DeleteReason.EXPIRED
    +            for operation in batch.operations
    +        )
    +        assert any(
    +            isinstance(operation, PutEntry) and operation.key == b"new"
    +            for operation in batch.operations
    +        )
    +        assert r.debug_physical_key_count == 1
    ```

**What this test locks**

It locks oversized-target safety, exact LRU, one-batch victim publication, noeviction shrink behavior, and expired-budget reclamation.

**How it constructs the counterexample**

It makes one key hot, attempts an impossible target, and inspects both commit sequence and the operations inside the accepted write batch.

**Key test statement**

```python
assert r.debug_commit_seq == before + 1
```

**What a failure means**

Eviction occurred as a separate visible mutation, an OOM command caused damage, or policy rejected an operation that reduces usage.

### Basic concepts

MiniRedis budgets a deterministic logical size derived from keys, values, and expiry metadata; it does not promise process-memory accounting. Exact LRU orders candidates by access tick and key. `noeviction` prevents net growth over budget but does not forbid deletes or other usage-reducing plans.

### Why this mechanism is necessary

Eviction is part of accepting one command, not background cleanup. Planning the target, expiry cleanup, victim deletes, and final put together preserves all-or-nothing publication and creates one replayable decision for future persistence and replication.

### Runtime mental model

The normal family planner first produces semantic reply and operations. The memory policy projects their post-commit usage over a copied size map. It rejects an individually oversized target immediately, includes expired deletes, and if necessary adds deterministic cold victims until the whole plan fits. Only then does the executor allocate one commit sequence.

### Mechanism blocks

#### Logical memory and deterministic victims

Project post-commit usage, purge expired entries first, and select exact-LRU victims without mutating live state.

??? note "File diff: src/miniredis/core/eviction.py"
    ```diff
    diff --git a/src/miniredis/core/eviction.py b/src/miniredis/core/eviction.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..141b42622f1f1c6f08926cfb12e06b94c31d48d8
    --- /dev/null
    +++ b/src/miniredis/core/eviction.py
    @@ -0,0 +1,125 @@
    +from __future__ import annotations
    +
    +from collections.abc import Iterable
    +
    +from miniredis.config import MiniRedisConfig
    +from miniredis.core.commit import (
    +    CommitOperation,
    +    DeleteKey,
    +    DeleteReason,
    +    PutEntry,
    +)
    +from miniredis.core.database import Database, logical_entry_size
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.expiration import expiry_delete, is_expired
    +from miniredis.core.planning import dedupe_operations
    +from miniredis.core.reply import Failure
    +
    +
    +OOM = Failure("OOM", "command exceeds maxmemory")
    +
    +
    +def projected_usage(
    +    database: Database,
    +    operations: Iterable[CommitOperation],
    +) -> int:
    +    sizes = {key: entry.logical_size for key, entry in database.entries.items()}
    +    for operation in operations:
    +        if isinstance(operation, DeleteKey):
    +            sizes.pop(operation.key, None)
    +        else:
    +            sizes[operation.key] = logical_entry_size(
    +                operation.key,
    +                operation.entry.value,
    +                operation.entry.expire_at_ms,
    +            )
    +    usage = sum(sizes.values())
    +    if usage < 0:
    +        raise AssertionError("projected usage cannot be negative")
    +    return usage
    +
    +
    +def _expired_operations(
    +    database: Database,
    +    now_ms: int,
    +) -> tuple[CommitOperation, ...]:
    +    return tuple(
    +        expiry_delete(key)
    +        for key, entry in sorted(database.entries.items())
    +        if is_expired(entry, now_ms)
    +    )
    +
    +
    +def enforce_memory(
    +    plan: ExecutionPlan,
    +    database: Database,
    +    config: MiniRedisConfig,
    +    now_ms: int,
    +) -> ExecutionPlan:
    +    maxmemory = config.maxmemory
    +    if maxmemory is None or not plan.operations:
    +        return plan
    +
    +    writes_data = any(
    +        isinstance(operation, PutEntry)
    +        or (
    +            isinstance(operation, DeleteKey) and operation.reason is DeleteReason.CLIENT
    +        )
    +        for operation in plan.operations
    +    )
    +    if not writes_data:
    +        return plan
    +
    +    expired = _expired_operations(database, now_ms)
    +    operations = dedupe_operations(expired + plan.operations)
    +
    +    target_keys = {
    +        operation.key for operation in operations if isinstance(operation, PutEntry)
    +    }
    +    for operation in operations:
    +        if not isinstance(operation, PutEntry):
    +            continue
    +        target_size = logical_entry_size(
    +            operation.key,
    +            operation.entry.value,
    +            operation.entry.expire_at_ms,
    +        )
    +        if target_size > maxmemory:
    +            return ExecutionPlan(OOM)
    +
    +    baseline = projected_usage(database, expired)
    +    usage = projected_usage(database, operations)
    +    if usage <= maxmemory or usage <= baseline:
    +        return ExecutionPlan(
    +            plan.reply,
    +            operations,
    +            plan.touch_keys,
    +            plan.trigger,
    +        )
    +    if config.eviction_policy == "noeviction":
    +        return ExecutionPlan(OOM)
    +
    +    already_deleted = {
    +        operation.key for operation in operations if isinstance(operation, DeleteKey)
    +    }
    +    candidates = sorted(
    +        (entry.last_access_tick, key)
    +        for key, entry in database.entries.items()
    +        if key not in target_keys
    +        and key not in already_deleted
    +        and not is_expired(entry, now_ms)
    +    )
    +    victims: list[CommitOperation] = []
    +    for _tick, key in candidates:
    +        victims.append(DeleteKey(key, DeleteReason.EVICTED))
    +        candidate_operations = dedupe_operations(
    +            expired + tuple(victims) + plan.operations
    +        )
    +        if projected_usage(database, candidate_operations) <= maxmemory:
    +            return ExecutionPlan(
    +                plan.reply,
    +                candidate_operations,
    +                plan.touch_keys,
    +                plan.trigger,
    +            )
    +    return ExecutionPlan(OOM)
    ```

**What it is and why it appears**

This policy layer transforms a successful semantic plan into either an OOM failure or another complete plan containing required cleanup and victims.

**Runtime role**

It calculates projected usage without mutation, refuses impossible target entries, reclaims expired entries first, and selects exact-LRU candidates outside the target set.

**Key code**

```python
candidates = sorted(
    (entry.last_access_tick, key)
    for key, entry in database.entries.items()
    if key not in target_keys
    and key not in already_deleted
    and not is_expired(entry, now_ms)
)
```

**Statement understanding**

Sorting `(tick, key)` provides deterministic oldest-first selection and a byte-key tie-break. Target keys cannot be evicted to pretend their own write fits.

#### Policy enforcement and replay visibility

Enforce maxmemory after semantic planning and expose logical state only for commit-replay contract evidence.

??? note "File diff: src/miniredis/core/planner.py"
    ```diff
    diff --git a/src/miniredis/core/planner.py b/src/miniredis/core/planner.py
    index 114f1b5edba78f4bd131ee82022e57dc1b6b1850..0fe5754bc713b5287898046fea3d18c2152186d3 100644
    --- a/src/miniredis/core/planner.py
    +++ b/src/miniredis/core/planner.py
    @@ -1,6 +1,7 @@
     from miniredis.commands.model import Command
     from miniredis.config import MiniRedisConfig
     from miniredis.core.database import Database
    +from miniredis.core.eviction import enforce_memory
     from miniredis.core.executor import ExecutionPlan
     from miniredis.core.hash_planner import plan_hash
     from miniredis.core.list_planner import plan_list
    @@ -33,5 +34,5 @@ class CommandPlanner:
             if plan is None:
                 plan = plan_ttl(command, database, now_ms)
             if plan is not None:
    -            return plan
    +            return enforce_memory(plan, database, self.config, now_ms)
             return ExecutionPlan(Failure("ERR", "unknown command"))
    ```

**What it is and why it appears**

The planner facade becomes the composition point between command semantics and global memory policy.

**Runtime role**

Every recognized family plan passes through the same budget enforcement before reaching the executor.

**Key code**

```python
if plan is not None:
    return enforce_memory(plan, database, self.config, now_ms)
```

**Statement understanding**

Policy runs after command semantics are known but before a commit exists, so rejection remains side-effect free.

??? note "File diff: src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 1ccca65f5ebeb14d9ce767f36eaa734aad3aa13b..a173895009de1a661615d8adab43ce1bcc74a3b5 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -7,7 +7,7 @@ from typing import Any, Self
     from miniredis.adapters.direct import DirectClient
     from miniredis.clock import Clock, SystemClock
     from miniredis.config import MiniRedisConfig
    -from miniredis.core.commit import CommitBatch
    +from miniredis.core.commit import CommitBatch, StoredEntry
     from miniredis.core.database import Database
     from miniredis.core.executor import (
         CommandExecutor,
    @@ -150,6 +150,9 @@ class MiniRedis:
         def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
             return self.executor.debug_applied_batches()

    +    def debug_logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
    +        return self.database.logical_items()
    +
         def debug_pause_executor(self) -> None:
             self.executor.debug_pause()

    ```

**What it is and why it appears**

The runtime exposes a frozen logical view for the replay invariant without making its mutable entry map a public API.

**Runtime role**

Tests compare the live logical state with a fresh database reconstructed only from applied batches.

**Key code**

```python
def debug_logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
    return self.database.logical_items()
```

**Statement understanding**

The diagnostic observes the result; it does not provide a bypass around the executor's single-writer boundary.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/08-deterministic-eviction/tests.txt)`. It proves policy behavior, atomic batch composition, wrong-type no-commit behavior, and operation-log replay through public commands plus narrow diagnostics.

### Durable takeaways

Budget logical state, not RSS; reject impossible targets before choosing victims; purge expired entries first; allow shrinking plans; publish victim deletes and the accepted mutation in one batch; keep all live state reconstructible from commits.

### Explain it in your own words

Eviction wraps an already planned command. It asks what the complete post-commit database would cost, then either returns OOM unchanged or adds enough deterministic deletes to make that exact command fit. The executor still sees and publishes only one plan.

### Textbook

[Chapter 5](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/05-eviction.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/ddfd69e...7628635)

After finishing, run `python -m journey.tools.build_journey check 8` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/08-deterministic-eviction/stage.patch)

# Stage 08 · 确定性淘汰

### 目标

用原子 Noeviction 和精确 LRU 决策执行逻辑 Maxmemory 预算。

??? note "交付文件"
    - `src/miniredis/core/eviction.py`
    - `src/miniredis/core/planner.py`
    - `src/miniredis/runtime.py`
    - `tests/contract/test_domain_invariants.py`
    - `tests/contract/test_eviction.py`

### 当前遇到的问题

原子命令 Plan 仍可无限增长。如果读取进程 RSS，结果会依赖 Allocator；如果先淘汰、后发现目标本身过大，则一个最终失败的命令会毁掉无关数据。

### 测试契约

#### 先看会坏在哪里

过大目标必须返回 OOM，且不删除已有 Key。精确 LRU 下，冷 Key 删除与触发它的 Put 必须属于同一 Commit；Noeviction 下，增长失败，但客户端删除仍合法。所有决策前还要先回收过期字节。

??? note "文件差异：tests/contract/test_domain_invariants.py"
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

**测试锁定什么**

它锁定跨值族 WRONGTYPE 不提交，并证明已发出 Batch 可重建同一逻辑 Database。

**如何构造反例**

它把每个读命令发给错误值类型，再单独把所有观察到的 Batch 重放到全新 `Database`。

**关键测试语句**

```python
for batch in batches:
    replay.apply_batch(batch, track_access=False)
assert replay.logical_items() == expected
```

**失败意味着什么**

语义失败分配了 Commit，或实时 Database 变化逃离操作日志，无法在后续重放。

??? note "文件差异：tests/contract/test_eviction.py"
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

**测试锁定什么**

它锁定过大目标安全性、精确 LRU、单 Batch Victim 发布、Noeviction 缩减行为与过期预算回收。

**如何构造反例**

它把一个 Key 变热，尝试一个不可能容纳的目标，并同时检查 Commit Sequence 与已接受写入 Batch 内的操作。

**关键测试语句**

```python
assert r.debug_commit_seq == before + 1
```

**失败意味着什么**

淘汰作为独立可见变更发生，OOM 命令造成了破坏，或 Policy 拒绝了减少用量的操作。

### 基本概念

MiniRedis 预算由 Key、Value 与过期元数据导出的确定逻辑大小，不承诺进程内存计量。精确 LRU 按 Access Tick 与 Key 排序候选者。`noeviction` 阻止超预算净增长，但不禁止删除或其他降低用量的 Plan。

### 为什么需要这个机制

淘汰是接受一条命令的一部分，不是后台清理。把目标、过期清理、Victim 删除与最终 Put 一起规划，才能保持全有全无发布，并为未来持久化与复制留下单个可重放决策。

### 运行时心智模型

普通命令族 Planner 先生成语义 Reply 与操作。Memory Policy 在复制的 Size Map 上投影提交后用量，立即拒绝单个过大目标，包入过期删除，并在需要时追加确定冷 Victim，直到完整 Plan 可容纳。只有此后 Executor 才分配一个 Commit Sequence。

### 机制板块

#### 逻辑内存与确定 Victim

投影提交后用量，先清理过期 Entry，再在不修改实时状态的前提下选择精确 LRU Victim。

??? note "文件差异：src/miniredis/core/eviction.py"
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

**是什么，为什么现在需要**

这个 Policy 层把成功语义 Plan 变成 OOM 失败，或另一个包含必要清理与 Victim 的完整 Plan。

**在运行时做什么**

它在不变更状态的前提下计算投影用量，拒绝不可能的目标 Entry，先回收过期 Entry，再从目标集外选择精确 LRU 候选者。

**关键代码**

```python
candidates = sorted(
    (entry.last_access_tick, key)
    for key, entry in database.entries.items()
    if key not in target_keys
    and key not in already_deleted
    and not is_expired(entry, now_ms)
)
```

**关键语句理解**

对 `(tick, key)` 排序提供确定的最旧优先选择和 Bytes Key Tie-break。目标 Key 不能被淘汰来伪装其自身写入可容纳。

#### Policy 执行与重放可见性

在语义规划后执行 Maxmemory，并仅为 Commit 重放契约暴露逻辑状态。

??? note "文件差异：src/miniredis/core/planner.py"
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

**是什么，为什么现在需要**

Planner 门面成为命令语义与全局 Memory Policy 的组合点。

**在运行时做什么**

每个已识别命令族 Plan 在到达 Executor 以前都经过同一预算执行。

**关键代码**

```python
if plan is not None:
    return enforce_memory(plan, database, self.config, now_ms)
```

**关键语句理解**

Policy 在命令语义确定后、Commit 存在前运行，因此拒绝仍无副作用。

??? note "文件差异：src/miniredis/runtime.py"
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

**是什么，为什么现在需要**

Runtime 暴露一份冻结逻辑视图供重放不变量使用，而不把可变 Entry Map 变成公开 API。

**在运行时做什么**

测试比较实时逻辑状态与仅由已应用 Batch 重建的全新 Database。

**关键代码**

```python
def debug_logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
    return self.database.logical_items()
```

**关键语句理解**

该诊断只观察结果，不提供绕过 Executor 单 Writer 边界的通道。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/08-deterministic-eviction/tests.txt)`。它通过公开命令与窄化诊断，证明 Policy 行为、原子 Batch 组成、Wrong-type 不提交与操作日志重放。

### 需要真正记住的内容

预算逻辑状态而非 RSS；选 Victim 前拒绝不可能目标；先清理过期 Entry；允许缩减 Plan；在一个 Batch 中发布 Victim 删除与已接受变更；保持所有实时状态可由 Commit 重建。

### 用自己的话讲清楚

淘汰包裹一个已规划命令。它询问完整提交后 Database 需要多少成本，再要么原样返回 OOM，要么追加足够的确定删除使这条精确命令可容纳。Executor 仍只看见并发布一个 Plan。

### 教材

[第 5 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/05-eviction.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/ddfd69e...7628635)

完成后可运行 `python -m journey.tools.build_journey check 8` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/08-deterministic-eviction/stage.patch)

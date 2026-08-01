# Stage 06 · Set 与 Sorted Set 投影

### 目标

加入唯一性与 Score 顺序语义，并提供确定性公开投影。

??? note "交付文件"
    - `src/miniredis/core/planner.py`
    - `src/miniredis/core/set_planner.py`
    - `src/miniredis/core/zset_planner.py`
    - `tests/contract/test_sets.py`
    - `tests/contract/test_sorted_sets.py`

### 当前遇到的问题

Python Set 没有稳定迭代顺序，Sorted Set 除了 Score 顺序还必须定义 Tie。多项请求的后续解析失败也必须拒绝整个请求，不能让前面的合法 Pair 已经变更状态。

### 测试契约

#### 先看会坏在哪里

一条契约把 Missing Set 与后面的 String 做 Intersection，仍要求 `WRONGTYPE`；遇到 Missing 就停止会隐藏非法 Operand。另一条先提交合法 ZADD Pair，再提交 `nan`，要求没有 Member 或 Commit 出现。

??? note "文件差异：tests/contract/test_sets.py"
    ```diff
    diff --git a/tests/contract/test_sets.py b/tests/contract/test_sets.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..e4489f857b21b59496aa28d9d005e28ccabfac63
    --- /dev/null
    +++ b/tests/contract/test_sets.py
    @@ -0,0 +1,41 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes, Failure, Items, Number
    +
    +
    +@pytest.mark.asyncio
    +async def test_set_counts_and_membership():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(
    +            CommandRequest(b"SADD", (b"s", b"a", b"a", b"b"))
    +        ) == Number(2)
    +        assert await c.execute(CommandRequest(b"SISMEMBER", (b"s", b"a"))) == Number(1)
    +        assert await c.execute(CommandRequest(b"SREM", (b"s", b"a", b"x"))) == Number(1)
    +
    +
    +@pytest.mark.asyncio
    +async def test_sinter_does_not_hide_later_wrongtype_after_missing_key():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"SET", (b"wrong", b"x")))
    +        reply = await c.execute(CommandRequest(b"SINTER", (b"missing", b"wrong")))
    +        assert isinstance(reply, Failure)
    +        assert reply.code == "WRONGTYPE"
    +
    +
    +@pytest.mark.asyncio
    +async def test_smembers_sinter_and_last_member_removal():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"SADD", (b"a", b"x", b"y")))
    +        await c.execute(CommandRequest(b"SADD", (b"b", b"y", b"z")))
    +        assert await c.execute(CommandRequest(b"SMEMBERS", (b"a",))) == Items(
    +            (Bytes(b"x"), Bytes(b"y"))
    +        )
    +        assert await c.execute(CommandRequest(b"SINTER", (b"a", b"b"))) == Items(
    +            (Bytes(b"y"),)
    +        )
    +        assert await c.execute(CommandRequest(b"SREM", (b"b", b"y", b"z"))) == Number(2)
    +        assert await c.execute(CommandRequest(b"TYPE", (b"b",))) == Bytes(b"none")
    ```

**测试锁定什么**

锁定唯一性计数、确定性 Member、完整 Operand 类型检查、Intersection 与最后 Member 删除。

**如何构造反例**

把 Missing Key 放在 Wrong Type Key 前，暴露错误的 Early-empty Optimization。

**关键测试语句**

```python
assert reply.code == "WRONGTYPE"
```

**失败意味着什么**

优化改变了校验语义，或无序存储泄漏进公开 Reply。

??? note "文件差异：tests/contract/test_sorted_sets.py"
    ```diff
    diff --git a/tests/contract/test_sorted_sets.py b/tests/contract/test_sorted_sets.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..c09f296c8792a896b9cf6bebb9419e6054280bba
    --- /dev/null
    +++ b/tests/contract/test_sorted_sets.py
    @@ -0,0 +1,47 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes, Failure, Items, Number
    +
    +
    +@pytest.mark.asyncio
    +async def test_zset_orders_equal_scores_by_member_bytes():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(
    +            CommandRequest(b"ZADD", (b"z", b"1", b"b", b"1", b"a", b"2", b"c"))
    +        ) == Number(3)
    +        assert await c.execute(CommandRequest(b"ZRANGE", (b"z", b"0", b"-1"))) == Items(
    +            (Bytes(b"a"), Bytes(b"b"), Bytes(b"c"))
    +        )
    +        assert await c.execute(CommandRequest(b"ZRANK", (b"z", b"b"))) == Number(1)
    +        assert await c.execute(
    +            CommandRequest(b"ZRANGEBYSCORE", (b"z", b"(1", b"+inf"))
    +        ) == Items((Bytes(b"c"),))
    +
    +
    +@pytest.mark.asyncio
    +async def test_nan_in_later_pair_prevents_all_mutation():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        before = runtime.debug_commit_seq
    +        reply = await c.execute(
    +            CommandRequest(b"ZADD", (b"z", b"1", b"a", b"nan", b"b"))
    +        )
    +        assert isinstance(reply, Failure)
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"ZRANGE", (b"z", b"0", b"-1"))) == Items(
    +            ()
    +        )
    +
    +
    +@pytest.mark.asyncio
    +async def test_zscore_zrem_and_empty_key_removal():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"ZADD", (b"z", b"1.5", b"a")))
    +        assert await c.execute(CommandRequest(b"ZSCORE", (b"z", b"a"))) == Bytes(b"1.5")
    +        assert await c.execute(
    +            CommandRequest(b"ZREM", (b"z", b"a", b"missing"))
    +        ) == Number(1)
    +        assert await c.execute(CommandRequest(b"TYPE", (b"z",))) == Bytes(b"none")
    ```

**测试锁定什么**

锁定 Score/Member 顺序、二进制 Tie-break、Exclusive Bound、Rank、Score 格式、完整请求校验与空 Key 删除。

**如何构造反例**

相同 Score 按反向二进制顺序到达，后续 NaN 跟在前一个合法 Pair 之后。

**关键测试语句**

```python
assert runtime.debug_commit_seq == before
```

**失败意味着什么**

Score 校验是增量的，或结果顺序依赖 Dict 插入顺序而不是公开规则。

### 基本概念

Set 存储拥有唯一性，但不拥有展示顺序。MiniRedis 在物化 Reply 与 Stored State 时按 Bytes 排序。Sorted Set 把 Member 映射到 Score，其总序是 `(score, member_bytes)`，使相同 Score 行为确定。

### 为什么需要这个机制

确定性投影把数学集合语义与 Python 容器迭代分离。完整请求解析和 Copy-on-plan 保证后续非法 Operand 不会留下前面的部分状态。

### 运行时心智模型

类型化命令已包含校验过的 Member 与 Score。命令族 Planner 复制实时集合，计算计数和顺序，提出冻结 Replacement 或 Delete，并返回确定性 `Items`/`Number`/`Bytes` Reply。

### 机制板块

#### 确定性 Set 投影

用 Set 保存唯一性语义，同时按二进制顺序物化公开多项 Reply。

??? note "文件差异：src/miniredis/core/set_planner.py"
    ```diff
    diff --git a/src/miniredis/core/set_planner.py b/src/miniredis/core/set_planner.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..ba915cb4fb370fbc39fd26c0b29d42d6066af9fa
    --- /dev/null
    +++ b/src/miniredis/core/set_planner.py
    @@ -0,0 +1,113 @@
    +from miniredis.commands import model as cmd
    +from miniredis.core.commit import (
    +    CommitOperation,
    +    DeleteKey,
    +    DeleteReason,
    +)
    +from miniredis.core.database import Database
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.planning import (
    +    WRONGTYPE,
    +    dedupe_operations,
    +    lookup,
    +    make_put,
    +)
    +from miniredis.core.reply import Bytes, Items, Number
    +from miniredis.core.values import SetValue
    +
    +
    +def plan_set(
    +    command: cmd.Command,
    +    database: Database,
    +    now_ms: int,
    +) -> ExecutionPlan | None:
    +    match command:
    +        case cmd.SetAdd(key, members):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is not None and not isinstance(previous.value, SetValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = set() if previous is None else set(previous.value.items)
    +            before = len(items)
    +            items.update(members)
    +            put = make_put(
    +                key,
    +                SetValue(items),
    +                previous,
    +                None if previous is None else previous.expire_at_ms,
    +            )
    +            return ExecutionPlan(
    +                Number(len(items) - before),
    +                expired + (put,),
    +            )
    +        case cmd.SetRemove(key, members):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is None:
    +                return ExecutionPlan(Number(0), expired)
    +            if not isinstance(previous.value, SetValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = set(previous.value.items)
    +            removed = 0
    +            for member in dict.fromkeys(members):
    +                if member in items:
    +                    items.remove(member)
    +                    removed += 1
    +            if removed == 0:
    +                return ExecutionPlan(Number(0), (), (key,))
    +            if items:
    +                operation = make_put(
    +                    key,
    +                    SetValue(items),
    +                    previous,
    +                    previous.expire_at_ms,
    +                )
    +            else:
    +                operation = DeleteKey(key, DeleteReason.CLIENT)
    +            return ExecutionPlan(Number(removed), expired + (operation,))
    +        case cmd.SetIsMember(key, member):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Number(0), expired)
    +            if not isinstance(entry.value, SetValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            return ExecutionPlan(
    +                Number(int(member in entry.value.items)),
    +                expired,
    +                (key,),
    +            )
    +        case cmd.SetMembers(key):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Items(()), expired)
    +            if not isinstance(entry.value, SetValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            return ExecutionPlan(
    +                Items(tuple(Bytes(member) for member in sorted(entry.value.items))),
    +                expired,
    +                (key,),
    +            )
    +        case cmd.SetIntersection(keys):
    +            operations: list[CommitOperation] = []
    +            sets: list[set[bytes] | None] = []
    +            touches: list[bytes] = []
    +            for key in keys:
    +                entry, expired = lookup(database, key, now_ms)
    +                operations.extend(expired)
    +                if entry is None:
    +                    sets.append(None)
    +                    continue
    +                if not isinstance(entry.value, SetValue):
    +                    return ExecutionPlan(WRONGTYPE)
    +                sets.append(set(entry.value.items))
    +                touches.append(key)
    +            if any(items is None for items in sets):
    +                intersection: set[bytes] = set()
    +            else:
    +                concrete = [items for items in sets if items is not None]
    +                intersection = set.intersection(*concrete)
    +            return ExecutionPlan(
    +                Items(tuple(Bytes(member) for member in sorted(intersection))),
    +                dedupe_operations(operations),
    +                tuple(touches),
    +            )
    +        case _:
    +            return None
    ```

**是什么，为什么现在需要**

Set Planner 拥有改变唯一性的操作与确定性读取投影。

**在运行时做什么**

用实时 Set 做 Membership 运算，只在返回公开 Item 时二进制排序。

**关键代码**

```python
Items(tuple(Bytes(member) for member in sorted(entry.value.items))),
```

**关键语句理解**

排序是投影规则，不表示实时 Set 是有序容器。

#### Score 顺序与二进制 Tie-break

先按 Score、再按 Member Bytes 排序，并在提出变更前校验完整 Score/Member 列表。

??? note "文件差异：src/miniredis/core/zset_planner.py"
    ```diff
    diff --git a/src/miniredis/core/zset_planner.py b/src/miniredis/core/zset_planner.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..07d1b1646d48b659818d53ec35b868460a4d3b5e
    --- /dev/null
    +++ b/src/miniredis/core/zset_planner.py
    @@ -0,0 +1,137 @@
    +import math
    +
    +from miniredis.commands import model as cmd
    +from miniredis.core.commit import DeleteKey, DeleteReason
    +from miniredis.core.database import Database
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.list_planner import inclusive_slice
    +from miniredis.core.planning import WRONGTYPE, lookup, make_put
    +from miniredis.core.reply import Bytes, Items, Number
    +from miniredis.core.values import ZSetValue
    +
    +
    +def _ordered(scores: dict[bytes, float]) -> list[tuple[bytes, float]]:
    +    return sorted(scores.items(), key=lambda item: (item[1], item[0]))
    +
    +
    +def _format_score(score: float) -> bytes:
    +    if math.isinf(score):
    +        return b"inf" if score > 0 else b"-inf"
    +    return repr(score).encode("ascii")
    +
    +
    +def _within(score: float, bound: cmd.ScoreBound, *, lower: bool) -> bool:
    +    if lower:
    +        return score >= bound.value if bound.inclusive else score > bound.value
    +    return score <= bound.value if bound.inclusive else score < bound.value
    +
    +
    +def plan_zset(
    +    command: cmd.Command,
    +    database: Database,
    +    now_ms: int,
    +) -> ExecutionPlan | None:
    +    match command:
    +        case cmd.ZAdd(key, pairs):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is not None and not isinstance(previous.value, ZSetValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            scores = {} if previous is None else dict(previous.value.scores)
    +            previously_present = set(scores)
    +            newly_added: set[bytes] = set()
    +            for score, member in pairs:
    +                if member not in previously_present:
    +                    newly_added.add(member)
    +                scores[member] = score
    +            put = make_put(
    +                key,
    +                ZSetValue(scores),
    +                previous,
    +                None if previous is None else previous.expire_at_ms,
    +            )
    +            return ExecutionPlan(
    +                Number(len(newly_added)),
    +                expired + (put,),
    +            )
    +        case cmd.ZRemove(key, members):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is None:
    +                return ExecutionPlan(Number(0), expired)
    +            if not isinstance(previous.value, ZSetValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            scores = dict(previous.value.scores)
    +            removed = 0
    +            for member in dict.fromkeys(members):
    +                if member in scores:
    +                    del scores[member]
    +                    removed += 1
    +            if removed == 0:
    +                return ExecutionPlan(Number(0), (), (key,))
    +            if scores:
    +                operation = make_put(
    +                    key,
    +                    ZSetValue(scores),
    +                    previous,
    +                    previous.expire_at_ms,
    +                )
    +            else:
    +                operation = DeleteKey(key, DeleteReason.CLIENT)
    +            return ExecutionPlan(Number(removed), expired + (operation,))
    +        case cmd.ZScore(key, member):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Bytes(None), expired)
    +            if not isinstance(entry.value, ZSetValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            score = entry.value.scores.get(member)
    +            return ExecutionPlan(
    +                Bytes(None if score is None else _format_score(score)),
    +                expired,
    +                (key,),
    +            )
    +        case cmd.ZRank(key, member):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Bytes(None), expired)
    +            if not isinstance(entry.value, ZSetValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            rank = next(
    +                (
    +                    index
    +                    for index, (candidate, _score) in enumerate(
    +                        _ordered(entry.value.scores)
    +                    )
    +                    if candidate == member
    +                ),
    +                None,
    +            )
    +            reply = Bytes(None) if rank is None else Number(rank)
    +            return ExecutionPlan(reply, expired, (key,))
    +        case cmd.ZRange(key, start, stop):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Items(()), expired)
    +            if not isinstance(entry.value, ZSetValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            ordered = _ordered(entry.value.scores)
    +            begin, end = inclusive_slice(len(ordered), start, stop)
    +            return ExecutionPlan(
    +                Items(tuple(Bytes(member) for member, _score in ordered[begin:end])),
    +                expired,
    +                (key,),
    +            )
    +        case cmd.ZRangeByScore(key, minimum, maximum):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Items(()), expired)
    +            if not isinstance(entry.value, ZSetValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            selected = tuple(
    +                Bytes(member)
    +                for member, score in _ordered(entry.value.scores)
    +                if _within(score, minimum, lower=True)
    +                and _within(score, maximum, lower=False)
    +            )
    +            return ExecutionPlan(Items(selected), expired, (key,))
    +        case _:
    +            return None
    ```

**是什么，为什么现在需要**

Sorted Set Planner 定义唯一总序与 Score Bound 过滤。

**在运行时做什么**

复制 Member-Score Map，应用类型化 Pair，并通过 `_ordered` 投影 Range 与 Rank。

**关键代码**

```python
return sorted(scores.items(), key=lambda item: (item[1], item[0]))
```

**关键语句理解**

Score 相等时用 Member Bytes 稳定 Tie-break，结果不继承插入顺序。

#### Set 命令族路由

用 Set 与 Sorted Set Handler 扩展稳定 Planner 门面。

??? note "文件差异：src/miniredis/core/planner.py"
    ```diff
    diff --git a/src/miniredis/core/planner.py b/src/miniredis/core/planner.py
    index d8de65d98cbbc4f3382ab5f6700140f9f7262eab..6ab2e0b94da903fcd461e9f293b730f22d55c3a8 100644
    --- a/src/miniredis/core/planner.py
    +++ b/src/miniredis/core/planner.py
    @@ -6,6 +6,8 @@ from miniredis.core.hash_planner import plan_hash
     from miniredis.core.list_planner import plan_list
     from miniredis.core.planning import plan_general_and_strings
     from miniredis.core.reply import Failure
    +from miniredis.core.set_planner import plan_set
    +from miniredis.core.zset_planner import plan_zset


     class CommandPlanner:
    @@ -23,6 +25,10 @@ class CommandPlanner:
                 plan = plan_hash(command, database, now_ms)
             if plan is None:
                 plan = plan_list(command, database, now_ms)
    +        if plan is None:
    +            plan = plan_set(command, database, now_ms)
    +        if plan is None:
    +            plan = plan_zset(command, database, now_ms)
             if plan is not None:
                 return plan
             return ExecutionPlan(Failure("ERR", "unknown command"))
    ```

**是什么，为什么现在需要**

Planner 门面在早期命令族后增加 Set 与 Sorted Set Handler。

**在运行时做什么**

保留一个 Executor-facing Planning Call，同时维持命令族所有权。

**关键代码**

```python
if plan is None:
    plan = plan_zset(command, database, now_ms)
```

**关键语句理解**

路由顺序不改变语义，因为每个类型化命令只有一个拥有者命令族。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/06-sets-and-sorted-sets/tests.txt)`，证明两个命令族的确定性投影与全有全无校验。

### 需要真正记住的内容

容器顺序与公开顺序分离；相同 Score 需要显式 Tie-break；后续非法参数阻止所有前序 Proposal。

### 用自己的话讲清楚

Set 用 Python 容器做集合运算，在边界排序。Sorted Set 定义完整 `(score, bytes)` 顺序。两者都返回一个确定性 Proposal，而不是边解析边变更。

### 教材

[第 3 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/03-data-types.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/eb41b6e...79fc734)

完成后可运行 `python -m journey.tools.build_journey check 6` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/06-sets-and-sorted-sets/stage.patch)

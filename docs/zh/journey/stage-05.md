# Stage 05 · Hash 与 List 规划

### 目标

把纯规划扩展到 Field Map 与方向敏感的有序序列。

??? note "交付文件"
    - `src/miniredis/core/hash_planner.py`
    - `src/miniredis/core/list_planner.py`
    - `src/miniredis/core/planner.py`
    - `tests/contract/test_hashes.py`
    - `tests/contract/test_lists.py`

### 当前遇到的问题

String 替换只有一个标量。Hash 命令必须区分新增与覆盖 Field，并删除空 Key；List 命令必须保留左右方向与 Redis 闭区间负索引规则。两者都不能在决定 Reply 时修改实时容器。

### 测试契约

#### 先看会坏在哪里

Hash 契约保存 `01` 后尝试 `HINCRBY`，要求 Commit 与 Field 都不变。List 契约请求反向和远负 Range，并执行最后一次 Pop，证明边界计算不能错误保留空 Key，也不能直接套用 Python 的不同切片约定。

??? note "文件差异：tests/contract/test_hashes.py"
    ```diff
    diff --git a/tests/contract/test_hashes.py b/tests/contract/test_hashes.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..d3be0b9c9339c6f63ba431ad18c04c7d4fe26afe
    --- /dev/null
    +++ b/tests/contract/test_hashes.py
    @@ -0,0 +1,84 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes, Failure, Items, Number
    +
    +
    +@pytest.mark.asyncio
    +async def test_hash_semantics_and_last_field_removal():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(
    +            CommandRequest(b"HSET", (b"h", b"a", b"1", b"a", b"2", b"b", b"3"))
    +        ) == Number(2)
    +        assert await c.execute(CommandRequest(b"HGET", (b"h", b"a"))) == Bytes(b"2")
    +        assert await c.execute(
    +            CommandRequest(b"HINCRBY", (b"h", b"a", b"5"))
    +        ) == Number(7)
    +        assert await c.execute(CommandRequest(b"HDEL", (b"h", b"a", b"b"))) == Number(2)
    +        assert await c.execute(CommandRequest(b"TYPE", (b"h",))) == Bytes(b"none")
    +
    +
    +@pytest.mark.asyncio
    +async def test_hash_integer_error_is_atomic():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(CommandRequest(b"HSET", (b"h", b"f", b"01"))) == Number(
    +            1
    +        )
    +        before = runtime.debug_commit_seq
    +        reply = await c.execute(CommandRequest(b"HINCRBY", (b"h", b"f", b"1")))
    +        assert reply == Failure("ERR", "value is not an integer or out of range")
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"HGET", (b"h", b"f"))) == Bytes(b"01")
    +
    +
    +@pytest.mark.asyncio
    +async def test_hgetall_is_alternating_and_missing_fields_are_nil():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"HSET", (b"h", b"b", b"2", b"a", b"1")))
    +        reply = await c.execute(CommandRequest(b"HGETALL", (b"h",)))
    +        assert isinstance(reply, Items)
    +        assert {
    +            (reply.values[index].value, reply.values[index + 1].value)
    +            for index in range(0, len(reply.values), 2)
    +        } == {(b"a", b"1"), (b"b", b"2")}
    +        assert await c.execute(CommandRequest(b"HGET", (b"h", b"missing"))) == Bytes(
    +            None
    +        )
    +
    +
    +@pytest.mark.asyncio
    +async def test_hash_wrongtype_overflow_and_noop_delete_do_not_commit():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"SET", (b"string", b"value")))
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(
    +            CommandRequest(b"HSET", (b"string", b"field", b"value"))
    +        ) == Failure(
    +            "WRONGTYPE",
    +            "operation against a key holding the wrong kind of value",
    +        )
    +        assert runtime.debug_commit_seq == before
    +
    +        maximum = b"9223372036854775807"
    +        assert await c.execute(
    +            CommandRequest(b"HSET", (b"h", b"field", maximum))
    +        ) == Number(1)
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(
    +            CommandRequest(b"HINCRBY", (b"h", b"field", b"1"))
    +        ) == Failure("ERR", "value is not an integer or out of range")
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"HGET", (b"h", b"field"))) == Bytes(
    +            maximum
    +        )
    +
    +        before_tick = runtime.database.entries[b"h"].last_access_tick
    +        assert await c.execute(
    +            CommandRequest(b"HDEL", (b"h", b"missing", b"missing"))
    +        ) == Number(0)
    +        assert runtime.debug_commit_seq == before
    +        assert runtime.database.entries[b"h"].last_access_tick > before_tick
    ```

**测试锁定什么**

锁定重复 Field 计数、覆盖、整数错误原子性、交替 `HGETALL`、Wrong Type、No-op Touch 与最后 Field 删除 Key。

**如何构造反例**

组合重复 Field 与非法存储整数，同时检查 Reply 和 Commit Sequence。

**关键测试语句**

```python
assert runtime.debug_commit_seq == before
```

**失败意味着什么**

Planner 在校验时已变更、按参数而非新 Field 计数，或把空 Hash 表示为实时 Key。

??? note "文件差异：tests/contract/test_lists.py"
    ```diff
    diff --git a/tests/contract/test_lists.py b/tests/contract/test_lists.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..f86017b8f908d91e31fc27b15f6c558ede5ef122
    --- /dev/null
    +++ b/tests/contract/test_lists.py
    @@ -0,0 +1,61 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes, Failure, Items, Number
    +
    +
    +@pytest.mark.asyncio
    +async def test_list_push_pop_and_range():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(
    +            CommandRequest(b"LPUSH", (b"l", b"a", b"b", b"c"))
    +        ) == Number(3)
    +        assert await c.execute(CommandRequest(b"LRANGE", (b"l", b"0", b"-1"))) == Items(
    +            (Bytes(b"c"), Bytes(b"b"), Bytes(b"a"))
    +        )
    +        assert await c.execute(CommandRequest(b"RPOP", (b"l",))) == Bytes(b"a")
    +        assert await c.execute(
    +            CommandRequest(b"LRANGE", (b"l", b"-99", b"99"))
    +        ) == Items((Bytes(b"c"), Bytes(b"b")))
    +
    +
    +@pytest.mark.asyncio
    +async def test_rpush_lpop_and_last_element_removal():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(CommandRequest(b"RPUSH", (b"q", b"a", b"b"))) == Number(
    +            2
    +        )
    +        assert await c.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"a")
    +        assert await c.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"b")
    +        assert await c.execute(CommandRequest(b"TYPE", (b"q",))) == Bytes(b"none")
    +        assert await c.execute(CommandRequest(b"RPOP", (b"missing",))) == Bytes(None)
    +
    +
    +@pytest.mark.asyncio
    +async def test_list_wrongtype_and_range_boundaries_are_side_effect_safe():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"SET", (b"string", b"value")))
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(
    +            CommandRequest(b"LPUSH", (b"string", b"item"))
    +        ) == Failure(
    +            "WRONGTYPE",
    +            "operation against a key holding the wrong kind of value",
    +        )
    +        assert runtime.debug_commit_seq == before
    +
    +        assert await c.execute(
    +            CommandRequest(b"RPUSH", (b"l", b"a", b"b", b"c"))
    +        ) == Number(3)
    +        assert await c.execute(CommandRequest(b"LRANGE", (b"l", b"2", b"1"))) == Items(
    +            ()
    +        )
    +        assert await c.execute(
    +            CommandRequest(b"LRANGE", (b"l", b"-1", b"-1"))
    +        ) == Items((Bytes(b"c"),))
    +        assert await c.execute(
    +            CommandRequest(b"LRANGE", (b"l", b"0", b"-99"))
    +        ) == Items(())
    ```

**测试锁定什么**

锁定 LPUSH/RPUSH 顺序、LPOP/RPOP 方向、闭区间负索引、Wrong Type 安全、Missing Pop 与最后元素删除。

**如何构造反例**

从两端 Push 相同值，并探测 `-99..99`、`2..1` 与 `-1..-1` 等 Range。

**关键测试语句**

```python
assert await c.execute(CommandRequest(b"TYPE", (b"q",))) == Bytes(b"none")
```

**失败意味着什么**

方向、边界归一化或空容器删除偏离公开 List 契约。

### 基本概念

两个 Planner 都使用 Copy-on-plan：复制当前容器，计算 Reply 与最终冻结值，再返回 Operation。Wrong Type 返回无操作 `WRONGTYPE`。删除集合最后一个成员产生 `DeleteKey`，而不是保存空容器。

### 为什么需要这个机制

可变 Python Dict 与 Deque 适合作为实时表示，却不是安全的规划工作区。复制让校验无副作用，并保留 String 使用的同一 Executor Commit 协议。

### 运行时心智模型

Router 选择命令族 Planner。Planner 做逻辑 Lookup、复制容器、应用 Field 或方向规则、冻结 Replacement 或提出 Delete，再返回 Reply。Executor 不学习集合细节。

### 机制板块

#### Hash Field 规划

复制一份 Field Map，计算精确新增/删除数量，保留 TTL，并在最后一个 Field 消失时删除 Key。

??? note "文件差异：src/miniredis/core/hash_planner.py"
    ```diff
    diff --git a/src/miniredis/core/hash_planner.py b/src/miniredis/core/hash_planner.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..479755793f0246af9143ec8f7faf8edfabacc052
    --- /dev/null
    +++ b/src/miniredis/core/hash_planner.py
    @@ -0,0 +1,112 @@
    +from miniredis.commands import model as cmd
    +from miniredis.commands.parser import (
    +    INT64_MAX,
    +    INT64_MIN,
    +    CommandParseError,
    +    parse_int64,
    +)
    +from miniredis.core.commit import DeleteKey, DeleteReason
    +from miniredis.core.database import Database
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.planning import WRONGTYPE, lookup, make_put
    +from miniredis.core.reply import Bytes, Failure, Items, Number
    +from miniredis.core.values import HashValue
    +
    +
    +def _integer_failure() -> ExecutionPlan:
    +    return ExecutionPlan(Failure("ERR", "value is not an integer or out of range"))
    +
    +
    +def plan_hash(
    +    command: cmd.Command,
    +    database: Database,
    +    now_ms: int,
    +) -> ExecutionPlan | None:
    +    match command:
    +        case cmd.HashSet(key, pairs):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is not None and not isinstance(previous.value, HashValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = {} if previous is None else dict(previous.value.items)
    +            added = 0
    +            for field, value in pairs:
    +                if field not in items:
    +                    added += 1
    +                items[field] = value
    +            put = make_put(
    +                key,
    +                HashValue(items),
    +                previous,
    +                None if previous is None else previous.expire_at_ms,
    +            )
    +            return ExecutionPlan(Number(added), expired + (put,))
    +        case cmd.HashGet(key, field):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Bytes(None), expired)
    +            if not isinstance(entry.value, HashValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            return ExecutionPlan(
    +                Bytes(entry.value.items.get(field)),
    +                expired,
    +                (key,),
    +            )
    +        case cmd.HashDelete(key, fields):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is None:
    +                return ExecutionPlan(Number(0), expired)
    +            if not isinstance(previous.value, HashValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = dict(previous.value.items)
    +            removed = 0
    +            for field in dict.fromkeys(fields):
    +                if field in items:
    +                    removed += 1
    +                    del items[field]
    +            if removed == 0:
    +                return ExecutionPlan(Number(0), (), (key,))
    +            if not items:
    +                operation = DeleteKey(key, DeleteReason.CLIENT)
    +            else:
    +                operation = make_put(
    +                    key,
    +                    HashValue(items),
    +                    previous,
    +                    previous.expire_at_ms,
    +                )
    +            return ExecutionPlan(Number(removed), expired + (operation,))
    +        case cmd.HashGetAll(key):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Items(()), expired)
    +            if not isinstance(entry.value, HashValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            values = tuple(
    +                item
    +                for field, value in sorted(entry.value.items.items())
    +                for item in (Bytes(field), Bytes(value))
    +            )
    +            return ExecutionPlan(Items(values), expired, (key,))
    +        case cmd.HashIncrement(key, field, amount):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is not None and not isinstance(previous.value, HashValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = {} if previous is None else dict(previous.value.items)
    +            raw_old = items.get(field, b"0")
    +            try:
    +                old_value = parse_int64(raw_old)
    +            except CommandParseError:
    +                return _integer_failure()
    +            new_value = old_value + amount
    +            if not INT64_MIN <= new_value <= INT64_MAX:
    +                return _integer_failure()
    +            items[field] = str(new_value).encode("ascii")
    +            put = make_put(
    +                key,
    +                HashValue(items),
    +                previous,
    +                None if previous is None else previous.expire_at_ms,
    +            )
    +            return ExecutionPlan(Number(new_value), expired + (put,))
    +        case _:
    +            return None
    ```

**是什么，为什么现在需要**

Hash Planner 拥有 Field 计数、整数更新、有序 Reply 物化与空 Key 删除。

**在运行时做什么**

复制 `items`，修改副本，再产生一个 Replacement 或 Delete Operation。

**关键代码**

```python
items = {} if previous is None else dict(previous.value.items)
```

**关键语句理解**

复制阻止重复 Field 校验或整数转换失败修改实时 Hash。

#### Deque 顺序与闭区间范围

在复制的 Deque 上显式定义左右 Push/Pop 方向与 Redis 风格的含负数闭区间。

??? note "文件差异：src/miniredis/core/list_planner.py"
    ```diff
    diff --git a/src/miniredis/core/list_planner.py b/src/miniredis/core/list_planner.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..ead234743a406735df6e8ca5750510bd7a53a6aa
    --- /dev/null
    +++ b/src/miniredis/core/list_planner.py
    @@ -0,0 +1,83 @@
    +from collections import deque
    +
    +from miniredis.commands import model as cmd
    +from miniredis.core.commit import DeleteKey, DeleteReason
    +from miniredis.core.database import Database
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.planning import WRONGTYPE, lookup, make_put
    +from miniredis.core.reply import Bytes, Items, Number
    +from miniredis.core.values import ListValue
    +
    +
    +def inclusive_slice(length: int, start: int, stop: int) -> tuple[int, int]:
    +    if start < 0:
    +        start += length
    +    if stop < 0:
    +        stop += length
    +    start = max(start, 0)
    +    stop = min(stop, length - 1)
    +    if start >= length or stop < 0 or start > stop:
    +        return 0, 0
    +    return start, stop + 1
    +
    +
    +def plan_list(
    +    command: cmd.Command,
    +    database: Database,
    +    now_ms: int,
    +) -> ExecutionPlan | None:
    +    match command:
    +        case cmd.ListPush(key, values, left):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is not None and not isinstance(previous.value, ListValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = deque() if previous is None else deque(previous.value.items)
    +            if left:
    +                for value in values:
    +                    items.appendleft(value)
    +            else:
    +                items.extend(values)
    +            put = make_put(
    +                key,
    +                ListValue(items),
    +                previous,
    +                None if previous is None else previous.expire_at_ms,
    +            )
    +            return ExecutionPlan(Number(len(items)), expired + (put,))
    +        case cmd.ListPop(key, left):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is None:
    +                return ExecutionPlan(Bytes(None), expired)
    +            if not isinstance(previous.value, ListValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            items = deque(previous.value.items)
    +            value = items.popleft() if left else items.pop()
    +            if items:
    +                operation = make_put(
    +                    key,
    +                    ListValue(items),
    +                    previous,
    +                    previous.expire_at_ms,
    +                )
    +            else:
    +                operation = DeleteKey(key, DeleteReason.CLIENT)
    +            return ExecutionPlan(Bytes(value), expired + (operation,))
    +        case cmd.ListRange(key, start, stop):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Items(()), expired)
    +            if not isinstance(entry.value, ListValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            begin, end = inclusive_slice(
    +                len(entry.value.items),
    +                start,
    +                stop,
    +            )
    +            selected = tuple(entry.value.items)[begin:end]
    +            return ExecutionPlan(
    +                Items(tuple(Bytes(value) for value in selected)),
    +                expired,
    +                (key,),
    +            )
    +        case _:
    +            return None
    ```

**是什么，为什么现在需要**

List Planner 定义方向性 Deque 变更，并把 Redis 闭区间转换为 Python 半开切片。

**在运行时做什么**

复制 Deque，改变一端，并在无 Item 时删除 Key。

**关键代码**

```python
return start, stop + 1
```

**关键语句理解**

`+1` 是公开闭 Stop Index 到 Python Exclusive Slice End 的语义桥梁。

#### 命令族路由

路由 Hash 与 List 类型命令，不把它们的语义移入 Executor。

??? note "文件差异：src/miniredis/core/planner.py"
    ```diff
    diff --git a/src/miniredis/core/planner.py b/src/miniredis/core/planner.py
    index 7672f517999802f4fda3327fb30322d356eb2ab1..d8de65d98cbbc4f3382ab5f6700140f9f7262eab 100644
    --- a/src/miniredis/core/planner.py
    +++ b/src/miniredis/core/planner.py
    @@ -2,6 +2,8 @@ from miniredis.commands.model import Command
     from miniredis.config import MiniRedisConfig
     from miniredis.core.database import Database
     from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.hash_planner import plan_hash
    +from miniredis.core.list_planner import plan_list
     from miniredis.core.planning import plan_general_and_strings
     from miniredis.core.reply import Failure

    @@ -17,6 +19,10 @@ class CommandPlanner:
             now_ms: int,
         ) -> ExecutionPlan:
             plan = plan_general_and_strings(command, database, now_ms)
    +        if plan is None:
    +            plan = plan_hash(command, database, now_ms)
    +        if plan is None:
    +            plan = plan_list(command, database, now_ms)
             if plan is not None:
                 return plan
             return ExecutionPlan(Failure("ERR", "unknown command"))
    ```

**是什么，为什么现在需要**

Router 现在按稳定顺序尝试 General/String、Hash、List Planner。

**在运行时做什么**

返回第一个拥有该类型命令的 Plan。

**关键代码**

```python
if plan is None:
    plan = plan_hash(command, database, now_ms)
```

**关键语句理解**

`None` 表示“不属于本命令族”，`ExecutionPlan` 内的 `Failure` 则是已拥有的语义结果。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/05-hashes-and-lists/tests.txt)`，通过共享 Executor 证明集合专属 Reply 与变更边界。

### 需要真正记住的内容

规划前复制；区分“未处理”与“已处理失败”；显式翻译公开索引约定；最后一个集合成员消失时删除 Key。

### 用自己的话讲清楚

Hash 与 List 增加不同数据规则，却不增加新所有权规则。各 Planner 在副本上工作，返回冻结最终操作，再由 Executor 沿同一有序提交路径发布。

### 教材

[第 3 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/03-data-types.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/be7969d...eb41b6e)

完成后可运行 `python -m journey.tools.build_journey check 5` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/05-hashes-and-lists/stage.patch)

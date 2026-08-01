# Stage 04 · 原子 String 规划

### 目标

把 String 命令规划成无副作用的 Reply 加一次串行提交。

??? note "交付文件"
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/expiration.py`
    - `src/miniredis/core/planner.py`
    - `src/miniredis/core/planning.py`
    - `tests/concurrency/test_atomic_incr.py`
    - `tests/contract/test_strings.py`

### 当前遇到的问题

Executor 能排序请求，却只能回答 `PING`/`ECHO`。String 变更需要检查旧状态，在 Wrong Type 或 Overflow 时不分配 Commit，还要让一百个并发 `INCR` 各自观察不同的串行前驱。

### 测试契约

#### 先看会坏在哪里

契约保存非规范整数 `01`，尝试 `INCR`，要求 Value 与 Commit Sequence 都不变。另一条用例启动一百个并发 Increment，要求最终值与序列对每个已接受变更恰好计数一次。

??? note "文件差异：tests/concurrency/test_atomic_incr.py"
    ```diff
    diff --git a/tests/concurrency/test_atomic_incr.py b/tests/concurrency/test_atomic_incr.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..3026ab9282c7187d8b1276012bc96a319245b604
    --- /dev/null
    +++ b/tests/concurrency/test_atomic_incr.py
    @@ -0,0 +1,21 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes
    +
    +
    +@pytest.mark.asyncio
    +async def test_one_hundred_concurrent_increments_are_serialized():
    +    async with MiniRedis.open(max_pending_commands=256) as runtime:
    +        clients = [runtime.direct_client() for _ in range(100)]
    +        await asyncio.gather(
    +            *(
    +                client.execute(CommandRequest(b"INCR", (b"counter",)))
    +                for client in clients
    +            )
    +        )
    +        assert await clients[0].execute(CommandRequest(b"GET", (b"counter",))) == Bytes(
    +            b"100"
    +        )
    ```

**测试锁定什么**

锁定并发调用方下 Read-Plan-Apply 仍是一个 Executor Turn。

**如何构造反例**

一百个 Task 在没有外部锁时提交 `INCR`，再检查最终值与互不重复的数字 Reply。

**关键测试语句**

```python
assert await client.execute(CommandRequest(b"GET", (b"counter",))) == Bytes(b"100")
```

**失败意味着什么**

丢失或重复 Increment 表示调用方在串行 Owner 外读取了过期状态。

??? note "文件差异：tests/contract/test_strings.py"
    ```diff
    diff --git a/tests/contract/test_strings.py b/tests/contract/test_strings.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..00d39808ad88d37f51f3cb51c60bb4cc13a01820
    --- /dev/null
    +++ b/tests/contract/test_strings.py
    @@ -0,0 +1,66 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes, Failure, Number, Ok
    +
    +
    +@pytest.mark.asyncio
    +async def test_set_conditions_replace_type_and_clear_old_state():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(CommandRequest(b"SET", (b"k", b"1"))) == Ok()
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(CommandRequest(b"SET", (b"k", b"2", b"NX"))) == Bytes(
    +            None
    +        )
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"SET", (b"k", b"2", b"XX"))) == Ok()
    +        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(b"2")
    +
    +
    +@pytest.mark.asyncio
    +async def test_invalid_integer_and_overflow_do_not_commit():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"SET", (b"k", b"01")))
    +        before = runtime.debug_commit_seq
    +        assert isinstance(await c.execute(CommandRequest(b"INCR", (b"k",))), Failure)
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(b"01")
    +
    +        maximum = b"9223372036854775807"
    +        assert await c.execute(CommandRequest(b"SET", (b"k", maximum))) == Ok()
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(CommandRequest(b"INCR", (b"k",))) == Failure(
    +            "ERR", "value is not an integer or out of range"
    +        )
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(maximum)
    +
    +        minimum = b"-9223372036854775808"
    +        assert await c.execute(CommandRequest(b"SET", (b"k", minimum))) == Ok()
    +        before = runtime.debug_commit_seq
    +        assert await c.execute(CommandRequest(b"INCRBY", (b"k", b"-1"))) == Failure(
    +            "ERR", "value is not an integer or out of range"
    +        )
    +        assert runtime.debug_commit_seq == before
    +        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(minimum)
    +
    +
    +@pytest.mark.asyncio
    +async def test_general_commands_and_incrby_cover_the_frozen_subset():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(CommandRequest(b"PING")) == Ok(b"PONG")
    +        assert await c.execute(CommandRequest(b"PING", (b"\x00pong",))) == Bytes(
    +            b"\x00pong"
    +        )
    +        assert await c.execute(CommandRequest(b"ECHO", (b"\xff",))) == Bytes(b"\xff")
    +        assert await c.execute(CommandRequest(b"SET", (b"k", b"1"))) == Ok()
    +        assert await c.execute(
    +            CommandRequest(b"EXISTS", (b"k", b"k", b"missing"))
    +        ) == Number(2)
    +        assert await c.execute(CommandRequest(b"TYPE", (b"k",))) == Bytes(b"string")
    +        assert await c.execute(CommandRequest(b"INCRBY", (b"k", b"4"))) == Number(5)
    +        assert await c.execute(CommandRequest(b"DEL", (b"k", b"k"))) == Number(1)
    +        assert await c.execute(CommandRequest(b"TYPE", (b"k",))) == Bytes(b"none")
    ```

**测试锁定什么**

锁定 `SET` 条件、Missing Value、有符号 64 位运算、Overflow、类型替换、有序多 Key Reply 与 Error/No-op 不提交。

**如何构造反例**

在非法整数、Overflow 与失败 `NX` 前捕获 `debug_commit_seq`，再确认序列与存储 Bytes 都未变化。

**关键测试语句**

```python
assert runtime.debug_commit_seq == before
```

**失败意味着什么**

语义错误泄漏了 Operation，或分配了不存在的历史 Commit。

### 基本概念

`ExecutionPlan` 包含 Reply、不可变 Operation、可选 Touch Key 与 Trigger。规划读取状态但不发布。No-op 或 Failure 可以返回无操作 Reply；只有非空成功 Plan 才成为带序列 `CommitBatch`。

### 为什么需要这个机制

分离规划与应用让错误原子性可见，并让 Executor 保持唯一序列分配者。后续 AOF 与复制也得到稳定 Batch，而不是命令专属变更过程。

### 运行时心智模型

在一个 Executor Turn 内，Planner 查找 Key，提出 Expiry Cleanup 和合法时的新 Stored String，再返回 Reply。Executor 分配 `commit_seq + 1`，应用整个 Batch，Touch 成功读取，最后完成请求。

### 机制板块

#### 纯 String 规划

把 String 读取与变更变成 Reply 加不可变操作，同时覆盖条件 No-op 与溢出失败。

??? note "文件差异：src/miniredis/core/expiration.py"
    ```diff
    diff --git a/src/miniredis/core/expiration.py b/src/miniredis/core/expiration.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..2595dcc4c0099fc860e11de6d8903ea0ba587bfd
    --- /dev/null
    +++ b/src/miniredis/core/expiration.py
    @@ -0,0 +1,10 @@
    +from miniredis.core.commit import DeleteKey, DeleteReason
    +from miniredis.core.database import Entry
    +
    +
    +def is_expired(entry: Entry, now_ms: int) -> bool:
    +    return entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms
    +
    +
    +def expiry_delete(key: bytes) -> DeleteKey:
    +    return DeleteKey(key, DeleteReason.EXPIRED)
    ```

**是什么，为什么现在需要**

初始 Expiry Helper 按 Executor 采样时间分类 Entry，并建立显式 Delete Operation。

**在运行时做什么**

String Lookup 可以把已过期数据视为不存在，而不在规划中直接修改。

**关键代码**

```python
def is_expired(entry: Entry, now_ms: int) -> bool:
    return entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms
```

**关键语句理解**

逻辑不可见与物理清理分离；后续 Commit 决定提出的 Delete 是否发布。

??? note "文件差异：src/miniredis/core/planning.py"
    ```diff
    diff --git a/src/miniredis/core/planning.py b/src/miniredis/core/planning.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..527ed614b6dc95788368088598b5b27b78c15bab
    --- /dev/null
    +++ b/src/miniredis/core/planning.py
    @@ -0,0 +1,206 @@
    +from __future__ import annotations
    +
    +from collections.abc import Iterable
    +
    +from miniredis.commands import model as cmd
    +from miniredis.commands.parser import (
    +    INT64_MAX,
    +    INT64_MIN,
    +    CommandParseError,
    +    parse_int64,
    +)
    +from miniredis.core.commit import (
    +    CommitOperation,
    +    DeleteKey,
    +    DeleteReason,
    +    PutEntry,
    +    StoredEntry,
    +)
    +from miniredis.core.database import Database, Entry, freeze_value
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.expiration import expiry_delete, is_expired
    +from miniredis.core.reply import Bytes, Failure, Number, Ok
    +from miniredis.core.values import (
    +    HashValue,
    +    ListValue,
    +    RedisValue,
    +    SetValue,
    +    StringValue,
    +    ZSetValue,
    +)
    +
    +
    +WRONGTYPE = Failure(
    +    "WRONGTYPE",
    +    "operation against a key holding the wrong kind of value",
    +)
    +
    +
    +def lookup(
    +    database: Database,
    +    key: bytes,
    +    now_ms: int,
    +) -> tuple[Entry | None, tuple[CommitOperation, ...]]:
    +    entry = database.entries.get(key)
    +    if entry is None:
    +        return None, ()
    +    if is_expired(entry, now_ms):
    +        return None, (expiry_delete(key),)
    +    return entry, ()
    +
    +
    +def dedupe_operations(
    +    operations: Iterable[CommitOperation],
    +) -> tuple[CommitOperation, ...]:
    +    result: list[CommitOperation] = []
    +    delete_indexes: dict[bytes, int] = {}
    +    put_indexes: dict[bytes, int] = {}
    +    for operation in operations:
    +        if isinstance(operation, DeleteKey):
    +            previous = delete_indexes.get(operation.key)
    +            if previous is None:
    +                delete_indexes[operation.key] = len(result)
    +                result.append(operation)
    +            else:
    +                result[previous] = operation
    +        else:
    +            previous = put_indexes.get(operation.key)
    +            if previous is None:
    +                put_indexes[operation.key] = len(result)
    +                result.append(operation)
    +            else:
    +                result[previous] = operation
    +    return tuple(result)
    +
    +
    +def make_put(
    +    key: bytes,
    +    value: RedisValue,
    +    previous: Entry | None,
    +    expire_at_ms: int | None,
    +) -> PutEntry:
    +    return PutEntry(
    +        key,
    +        StoredEntry(
    +            freeze_value(value),
    +            expire_at_ms,
    +            1 if previous is None else previous.mutation_version + 1,
    +        ),
    +    )
    +
    +
    +def type_name(entry: Entry | None) -> bytes:
    +    if entry is None:
    +        return b"none"
    +    match entry.value:
    +        case StringValue():
    +            return b"string"
    +        case HashValue():
    +            return b"hash"
    +        case ListValue():
    +            return b"list"
    +        case SetValue():
    +            return b"set"
    +        case ZSetValue():
    +            return b"zset"
    +    raise AssertionError(f"unhandled value: {entry.value!r}")
    +
    +
    +def _integer_failure() -> ExecutionPlan:
    +    return ExecutionPlan(Failure("ERR", "value is not an integer or out of range"))
    +
    +
    +def plan_general_and_strings(
    +    command: cmd.Command,
    +    database: Database,
    +    now_ms: int,
    +) -> ExecutionPlan | None:
    +    match command:
    +        case cmd.Ping(None):
    +            return ExecutionPlan(Ok(b"PONG"))
    +        case cmd.Ping(message):
    +            return ExecutionPlan(Bytes(message))
    +        case cmd.Echo(message):
    +            return ExecutionPlan(Bytes(message))
    +        case cmd.Delete(keys):
    +            operations: list[CommitOperation] = []
    +            removed = 0
    +            seen: set[bytes] = set()
    +            for key in keys:
    +                entry, expired = lookup(database, key, now_ms)
    +                operations.extend(expired)
    +                if key in seen:
    +                    continue
    +                seen.add(key)
    +                if entry is not None:
    +                    removed += 1
    +                    operations.append(DeleteKey(key, DeleteReason.CLIENT))
    +            return ExecutionPlan(Number(removed), dedupe_operations(operations))
    +        case cmd.Exists(keys):
    +            operations = []
    +            touches: list[bytes] = []
    +            count = 0
    +            for key in keys:
    +                entry, expired = lookup(database, key, now_ms)
    +                operations.extend(expired)
    +                if entry is not None:
    +                    count += 1
    +                    touches.append(key)
    +            return ExecutionPlan(
    +                Number(count),
    +                dedupe_operations(operations),
    +                tuple(touches),
    +            )
    +        case cmd.TypeOf(key):
    +            entry, expired = lookup(database, key, now_ms)
    +            return ExecutionPlan(
    +                Bytes(type_name(entry)),
    +                expired,
    +                (key,) if entry is not None else (),
    +            )
    +        case cmd.GetString(key):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Bytes(None), expired)
    +            if not isinstance(entry.value, StringValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            return ExecutionPlan(Bytes(entry.value.data), expired, (key,))
    +        case cmd.SetString(key, value, only_if, expire_ms):
    +            previous, expired = lookup(database, key, now_ms)
    +            if only_if == "nx" and previous is not None:
    +                return ExecutionPlan(Bytes(None))
    +            if only_if == "xx" and previous is None:
    +                return ExecutionPlan(Bytes(None))
    +            expire_at_ms = None if expire_ms is None else now_ms + expire_ms
    +            put = make_put(
    +                key,
    +                StringValue(value),
    +                previous,
    +                expire_at_ms,
    +            )
    +            return ExecutionPlan(Ok(), expired + (put,))
    +        case cmd.Increment(key, amount):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is None:
    +                old_value = 0
    +                old_expiry = None
    +            elif not isinstance(previous.value, StringValue):
    +                return ExecutionPlan(WRONGTYPE)
    +            else:
    +                old_expiry = previous.expire_at_ms
    +                try:
    +                    old_value = parse_int64(previous.value.data)
    +                except CommandParseError:
    +                    return _integer_failure()
    +            new_value = old_value + amount
    +            if not INT64_MIN <= new_value <= INT64_MAX:
    +                return _integer_failure()
    +            put = make_put(
    +                key,
    +                StringValue(str(new_value).encode("ascii")),
    +                previous,
    +                old_expiry,
    +            )
    +            return ExecutionPlan(Number(new_value), expired + (put,))
    +        case _:
    +            return None
    ```

**是什么，为什么现在需要**

该模块拥有共享 Lookup/Building 规则与 String 命令语义。

**在运行时做什么**

它返回精确 Reply 加冻结 `PutEntry`/`DeleteKey`，并在需要时保留旧 Expiry。

**关键代码**

```python
new_value = old_value + amount
if not INT64_MIN <= new_value <= INT64_MAX:
    return _integer_failure()
```

**关键语句理解**

Overflow 返回无操作 Plan；如果应用后才检查，会同时破坏值与历史。

??? note "文件差异：src/miniredis/core/planner.py"
    ```diff
    diff --git a/src/miniredis/core/planner.py b/src/miniredis/core/planner.py
    index 697ce08de85ee834c15469c9ef6297c65bf2e1da..7672f517999802f4fda3327fb30322d356eb2ab1 100644
    --- a/src/miniredis/core/planner.py
    +++ b/src/miniredis/core/planner.py
    @@ -1,22 +1,22 @@
    -from __future__ import annotations
    -
    -from miniredis.commands.model import Command, Echo, Ping
    +from miniredis.commands.model import Command
     from miniredis.config import MiniRedisConfig
     from miniredis.core.database import Database
     from miniredis.core.executor import ExecutionPlan
    -from miniredis.core.reply import Bytes, Failure, Ok
    +from miniredis.core.planning import plan_general_and_strings
    +from miniredis.core.reply import Failure


     class CommandPlanner:
         def __init__(self, config: MiniRedisConfig) -> None:
             self.config = config

    -    def plan(self, database: Database, command: Command, now_ms: int) -> ExecutionPlan:
    -        del database, now_ms
    -        match command:
    -            case Ping(message=None):
    -                return ExecutionPlan(Ok(b"PONG"))
    -            case Ping(message=message) | Echo(message=message):
    -                return ExecutionPlan(Bytes(message))
    -            case _:
    -                return ExecutionPlan(Failure("ERR", "unknown command"))
    +    def plan(
    +        self,
    +        command: Command,
    +        database: Database,
    +        now_ms: int,
    +    ) -> ExecutionPlan:
    +        plan = plan_general_and_strings(command, database, now_ms)
    +        if plan is not None:
    +            return plan
    +        return ExecutionPlan(Failure("ERR", "unknown command"))
    ```

**是什么，为什么现在需要**

`CommandPlanner` 是类型化命令与各命令族纯 Planner 间的稳定路由门面。

**在运行时做什么**

Executor 调用一个方法，不学习 String 专属分支。

**关键代码**

```python
plan = plan_general_and_strings(command, database, now_ms)
if plan is not None:
    return plan
```

**关键语句理解**

命令族增长留在 Planner 边界后，不扩大 Executor 所有权。

#### 串行提交分配

只在 Executor 的有序 Turn 内分配下一个序列并应用规划好的变更。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index cb776bb2ea5098f029ece2e3965b59bc10f71c40..54dcf634f42947df50fb9bc43b21e68ad15c1b51 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -64,7 +64,7 @@ class NullCommitBarrier:

     class Planner(Protocol):
         def plan(
    -        self, database: Database, command: Command, now_ms: int
    +        self, command: Command, database: Database, now_ms: int
         ) -> ExecutionPlan: ...


    @@ -201,7 +201,7 @@ class CommandExecutor:

         async def _execute(self, request: ExecuteRequest) -> None:
             now_ms = self.clock.now_ms()
    -        plan = self.planner.plan(self.database, request.command, now_ms)
    +        plan = self.planner.plan(request.command, self.database, now_ms)
             if plan.operations:
                 batch = CommitBatch(
                     seq=self.database.commit_seq + 1,
    ```

**是什么，为什么现在需要**

Executor 现在把非空 Plan 变成有序 Commit Batch，并恰好应用一次。

**在运行时做什么**

它采样时间、基于当前状态规划、分配下一序列、应用，再完成 Reply。

**关键代码**

```python
batch = CommitBatch(
    seq=self.database.commit_seq + 1,
    operations=plan.operations,
    trigger=plan.trigger,
)
await self.commit_barrier.append(batch)
self.database.apply_batch(
    batch, track_access=plan.trigger is CommitTrigger.CLIENT
)
```

**关键语句理解**

序列分配与应用在同一 Owner 下相邻发生，因此并发 `INCR` 不能共享前驱。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/04-atomic-strings/tests.txt)`；再运行 `tests/concurrency/test_atomic_incr.py` 观察并发 Increment 串行化。

### 需要真正记住的内容

规划是纯的；Error 与 No-op 没有 Commit；只有 Executor 分配并应用下一 Batch。并发通过所有权解决，不靠命令专属锁。

### 用自己的话讲清楚

String 命令先成为 Proposal。合法且变更时，Executor 把它变成下一不可变 Batch，在回复前应用；否则返回语义结果而不伪造历史。

### 教材

[第 3 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/03-data-types.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/a5f7a27...be7969d)

完成后可运行 `python -m journey.tools.build_journey check 4` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/04-atomic-strings/stage.patch)

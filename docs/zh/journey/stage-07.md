# Stage 07 · 绝对 TTL 与有界过期

### 目标

用绝对截止时间、惰性不可见与有界主动清理，把过期变成可确定的状态迁移。

??? note "交付文件"
    - `pyproject.toml`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/planner.py`
    - `src/miniredis/core/ttl_planner.py`
    - `src/miniredis/runtime.py`
    - `tests/contract/test_ttl.py`
    - `tests/helpers/time.py`

### 当前遇到的问题

值已能原子更新，但时间还不会改变可见性。相对倒计时会在暂停或重启时漂移；如果读路径直接删数据，又会绕过串行提交所有者。

### 测试契约

#### 先看会坏在哪里

过期 Key 即使还有物理 Entry，逻辑上也必须已经不可见。后续命令若失败，不得顺手提交待处理的惰性删除；原地变更也不得把旧截止时间向后延。

??? note "文件差异：tests/contract/test_ttl.py"
    ```diff
    diff --git a/tests/contract/test_ttl.py b/tests/contract/test_ttl.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..0de350ebf6ddd7606305414b10363f714d10dc51
    --- /dev/null
    +++ b/tests/contract/test_ttl.py
    @@ -0,0 +1,112 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.commit import CommitTrigger, DeleteKey, DeleteReason
    +from miniredis.core.reply import Bytes, Failure, Number, Ok
    +from tests.helpers.time import FakeClock
    +
    +
    +@pytest.mark.asyncio
    +async def test_set_px_is_lazy_invisible_and_set_replacement_clears_ttl():
    +    clock = FakeClock(1_000)
    +    async with MiniRedis.open(clock=clock) as runtime:
    +        c = runtime.direct_client()
    +        assert (
    +            await c.execute(CommandRequest(b"SET", (b"k", b"v", b"PX", b"100"))) == Ok()
    +        )
    +        assert await c.execute(CommandRequest(b"PTTL", (b"k",))) == Number(100)
    +        clock.advance(100)
    +        assert await c.execute(CommandRequest(b"GET", (b"k",))) == Bytes(None)
    +        assert await c.execute(CommandRequest(b"PTTL", (b"k",))) == Number(-2)
    +        await c.execute(CommandRequest(b"SET", (b"k", b"v", b"PX", b"100")))
    +        await c.execute(CommandRequest(b"SET", (b"k", b"new")))
    +        assert await c.execute(CommandRequest(b"TTL", (b"k",))) == Number(-1)
    +        assert await c.execute(CommandRequest(b"EXPIRE", (b"k", b"0"))) == Number(1)
    +        assert await c.execute(CommandRequest(b"PTTL", (b"k",))) == Number(-2)
    +
    +
    +@pytest.mark.asyncio
    +async def test_expire_ttl_persist_and_bounded_active_cleanup():
    +    clock = FakeClock(10_000)
    +    async with MiniRedis.open(
    +        clock=clock,
    +        active_expire_sample_size=1,
    +    ) as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"SET", (b"a", b"1")))
    +        await c.execute(CommandRequest(b"SET", (b"b", b"2")))
    +        assert await c.execute(CommandRequest(b"EXPIRE", (b"a", b"2"))) == Number(1)
    +        assert await c.execute(CommandRequest(b"TTL", (b"a",))) == Number(2)
    +        assert await c.execute(CommandRequest(b"PERSIST", (b"a",))) == Number(1)
    +        assert await c.execute(CommandRequest(b"PERSIST", (b"a",))) == Number(0)
    +        await c.execute(CommandRequest(b"EXPIRE", (b"a", b"1")))
    +        await c.execute(CommandRequest(b"EXPIRE", (b"b", b"1")))
    +        clock.advance(1_000)
    +        assert await runtime.debug_active_expire_once() == 1
    +        first_active = runtime.executor.debug_applied_batches()[-1]
    +        assert first_active.trigger is CommitTrigger.ACTIVE_EXPIRE
    +        assert all(
    +            isinstance(operation, DeleteKey)
    +            and operation.reason is DeleteReason.EXPIRED
    +            for operation in first_active.operations
    +        )
    +        assert runtime.debug_physical_key_count == 1
    +        assert await runtime.debug_active_expire_once() == 1
    +        assert runtime.debug_physical_key_count == 0
    +
    +
    +@pytest.mark.asyncio
    +@pytest.mark.parametrize(
    +    ("setup", "mutation"),
    +    [
    +        (
    +            CommandRequest(b"SET", (b"k", b"1")),
    +            CommandRequest(b"INCR", (b"k",)),
    +        ),
    +        (
    +            CommandRequest(b"HSET", (b"k", b"f", b"1")),
    +            CommandRequest(b"HINCRBY", (b"k", b"f", b"1")),
    +        ),
    +        (
    +            CommandRequest(b"RPUSH", (b"k", b"a", b"b")),
    +            CommandRequest(b"LPOP", (b"k",)),
    +        ),
    +        (
    +            CommandRequest(b"SADD", (b"k", b"a", b"b")),
    +            CommandRequest(b"SREM", (b"k", b"a")),
    +        ),
    +        (
    +            CommandRequest(b"ZADD", (b"k", b"1", b"a", b"2", b"b")),
    +            CommandRequest(b"ZREM", (b"k", b"a")),
    +        ),
    +    ],
    +)
    +async def test_every_in_place_value_mutation_preserves_absolute_ttl(
    +    setup: CommandRequest,
    +    mutation: CommandRequest,
    +) -> None:
    +    clock = FakeClock(5_000)
    +    async with MiniRedis.open(clock=clock) as runtime:
    +        c = runtime.direct_client()
    +        assert not isinstance(await c.execute(setup), Failure)
    +        assert await c.execute(CommandRequest(b"EXPIRE", (b"k", b"10"))) == Number(1)
    +        assert not isinstance(await c.execute(mutation), Failure)
    +        assert await c.execute(CommandRequest(b"PTTL", (b"k",))) == Number(10_000)
    +
    +
    +@pytest.mark.asyncio
    +async def test_error_discards_pending_lazy_expiry_delete():
    +    clock = FakeClock(0)
    +    async with MiniRedis.open(clock=clock) as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"SET", (b"elapsed", b"x", b"PX", b"1")))
    +        await c.execute(CommandRequest(b"SET", (b"wrong", b"x")))
    +        clock.advance(1)
    +        before = runtime.debug_commit_seq
    +        reply = await c.execute(CommandRequest(b"SINTER", (b"elapsed", b"wrong")))
    +        assert isinstance(reply, Failure)
    +        assert reply.code == "WRONGTYPE"
    +        assert runtime.debug_commit_seq == before
    +        assert runtime.debug_physical_key_count == 2
    +        assert await c.execute(CommandRequest(b"GET", (b"elapsed",))) == Bytes(None)
    +        assert runtime.debug_physical_key_count == 1
    ```

**测试锁定什么**

它锁定惰性不可见、TTL 取整、PERSIST、有界主动清理、所有值族的截止时间保留与错误原子性。

**如何构造反例**

测试把注入时间精确推进到截止点，分别观察逻辑读取与物理计数，并把过期操作数与后续 WRONGTYPE 操作数组合。

**关键测试语句**

```python
assert runtime.debug_commit_seq == before
```

**失败意味着什么**

时间在提交协议外改变了状态，错误泄漏了拟议过期删除，或变更偷偷重置了绝对截止时间。

??? note "文件差异：tests/helpers/time.py"
    ```diff
    diff --git a/tests/helpers/time.py b/tests/helpers/time.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..4f25f86fb5cb69f8df5d6544d8d5d39138431720
    --- /dev/null
    +++ b/tests/helpers/time.py
    @@ -0,0 +1,9 @@
    +class FakeClock:
    +    def __init__(self, now_ms: int = 0) -> None:
    +        self.value = now_ms
    +
    +    def now_ms(self) -> int:
    +        return self.value
    +
    +    def advance(self, milliseconds: int) -> None:
    +        self.value += milliseconds
    ```

**测试锁定什么**

这个 Helper 把流逝时间变成显式输入，而不是 Sleep 或墙上时钟假设。

**如何构造反例**

每个测试选定精确毫秒并同步推进，因此边界等值可重现。

**关键测试语句**

```python
def advance(self, milliseconds: int) -> None:
    self.value += milliseconds
```

**失败意味着什么**

TTL 行为将无法与调度时机或不稳定的真实延迟区分。

### 基本概念

MiniRedis 存储 `expire_at_ms` 这个绝对截止时间。惰性过期在 Lookup 时让已到期 Entry 不可见，并提出 `DeleteKey(EXPIRED)`；主动过期独立采样物理 TTL Entry，使冷 Key 最终被回收。因此，逻辑消失与物理回收是两个时刻。

### 为什么需要这个机制

绝对时间能经过暂停与后续持久化，而无需重算倒计时。惰性和主动删除都走 `CommitBatch`，才能保留排序、耐久性 Hook 与未来复制语义。有界采样则防止一次维护 Tick 长时间占住 Executor。

### 运行时心智模型

注入的 Clock 提供 `now_ms`。Command Planner 用它与 Entry 截止时间比较，返回 Reply 与拟议操作。Executor 要么提交完整成功 Plan，要么在失败时丢弃它。Active Tick 也进入同一 Mailbox，最多选 N 个 TTL Key，再把过期删除作为一个维护 Batch 提交。

### 机制板块

#### 绝对 TTL 规划

把 EXPIRE、TTL/PTTL 与 PERSIST 翻译成绝对截止时间和普通提交操作。

??? note "文件差异：src/miniredis/core/ttl_planner.py"
    ```diff
    diff --git a/src/miniredis/core/ttl_planner.py b/src/miniredis/core/ttl_planner.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..bb1276ef96495974bf07f7c859250f75b7be45fe
    --- /dev/null
    +++ b/src/miniredis/core/ttl_planner.py
    @@ -0,0 +1,49 @@
    +from miniredis.commands import model as cmd
    +from miniredis.core.commit import DeleteKey, DeleteReason
    +from miniredis.core.database import Database
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.planning import lookup, make_put
    +from miniredis.core.reply import Number
    +
    +
    +def plan_ttl(
    +    command: cmd.Command,
    +    database: Database,
    +    now_ms: int,
    +) -> ExecutionPlan | None:
    +    match command:
    +        case cmd.Expire(key, seconds):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is None:
    +                return ExecutionPlan(Number(0), expired)
    +            if seconds <= 0:
    +                return ExecutionPlan(
    +                    Number(1),
    +                    expired + (DeleteKey(key, DeleteReason.CLIENT),),
    +                )
    +            put = make_put(
    +                key,
    +                previous.value,
    +                previous,
    +                now_ms + seconds * 1_000,
    +            )
    +            return ExecutionPlan(Number(1), expired + (put,))
    +        case cmd.TimeToLive(key, milliseconds):
    +            entry, expired = lookup(database, key, now_ms)
    +            if entry is None:
    +                return ExecutionPlan(Number(-2), expired)
    +            if entry.expire_at_ms is None:
    +                return ExecutionPlan(Number(-1), (), (key,))
    +            remaining_ms = entry.expire_at_ms - now_ms
    +            value = remaining_ms if milliseconds else (remaining_ms + 500) // 1_000
    +            return ExecutionPlan(Number(value), expired, (key,))
    +        case cmd.Persist(key):
    +            previous, expired = lookup(database, key, now_ms)
    +            if previous is None:
    +                return ExecutionPlan(Number(0), expired)
    +            if previous.expire_at_ms is None:
    +                return ExecutionPlan(Number(0), (), (key,))
    +            put = make_put(key, previous.value, previous, None)
    +            return ExecutionPlan(Number(1), expired + (put,))
    +        case _:
    +            return None
    ```

**是什么，为什么现在需要**

这个命令族 Planner 拥有 EXPIRE、TTL/PTTL 与 PERSIST，不需要把命令语义教给 Executor。

**在运行时做什么**

它先解析惰性过期，存储 `now_ms + duration`，再把立即过期或持久化变化表示为普通操作。

**关键代码**

```python
put = make_put(
    key,
    previous.value,
    previous,
    now_ms + seconds * 1_000,
)
```

**关键语句理解**

存储的是绝对截止时间；原地变更可原样复制，TTL 也可用一次时钟读取计算剩余时间。

#### 串行化主动过期

通过 Executor Mailbox 有界采样 TTL Key，再经同一 Commit Barrier 发布过期删除。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index 54dcf634f42947df50fb9bc43b21e68ad15c1b51..e138ca7c47c34dc7db31463fcfd6de7896d3f217 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -1,6 +1,7 @@
     from __future__ import annotations

     import asyncio
    +from bisect import bisect_right
     from collections.abc import Callable
     from dataclasses import dataclass
     from typing import Protocol
    @@ -9,6 +10,7 @@ from miniredis.clock import Clock
     from miniredis.commands.model import Command
     from miniredis.core.commit import CommitBatch, CommitOperation, CommitTrigger
     from miniredis.core.database import Database
    +from miniredis.core.expiration import expiry_delete, is_expired
     from miniredis.core.mailbox import EventLoopMailbox
     from miniredis.core.reply import Failure, Reply

    @@ -73,7 +75,13 @@ class _StopExecutor:
         pass


    -type ExecutorMessage = ExecuteRequest | _StopExecutor
    +@dataclass(slots=True)
    +class ActiveExpireTick:
    +    now_ms: int
    +    future: asyncio.Future[int] | None = None
    +
    +
    +type ExecutorMessage = ExecuteRequest | ActiveExpireTick | _StopExecutor


     class CommandExecutor:
    @@ -85,6 +93,7 @@ class CommandExecutor:
             clock: Clock,
             commit_barrier: CommitBarrier,
             max_pending_commands: int,
    +        active_expire_sample_size: int = 20,
             on_terminal_failure: Callable[[BaseException], None] | None = None,
         ) -> None:
             self.database = database
    @@ -92,6 +101,10 @@ class CommandExecutor:
             self.clock = clock
             self.commit_barrier = commit_barrier
             self.max_pending_commands = max_pending_commands
    +        if active_expire_sample_size <= 0:
    +            raise ValueError("active_expire_sample_size must be positive")
    +        self.active_expire_sample_size = active_expire_sample_size
    +        self._active_expire_cursor: bytes | None = None
             self.mailbox: EventLoopMailbox[ExecutorMessage] = EventLoopMailbox(
                 max_pending_commands
             )
    @@ -160,7 +173,7 @@ class CommandExecutor:
             self._accepted_changed.set()
             return SubmittedRequest(token, future)

    -    def post_control(self, message: _StopExecutor) -> bool:
    +    def post_control(self, message: ActiveExpireTick | _StopExecutor) -> bool:
             return self.mailbox.post_control(message)

         async def _run(self) -> None:
    @@ -173,7 +186,12 @@ class CommandExecutor:
                     await self._run_gate.wait()
                     if isinstance(message, _StopExecutor):
                         return
    -                await self._execute(message)
    +                if isinstance(message, ExecuteRequest):
    +                    await self._execute(message)
    +                elif isinstance(message, ActiveExpireTick):
    +                    deleted = await self._active_expire_once(message.now_ms)
    +                    if message.future is not None and not message.future.done():
    +                        message.future.set_result(deleted)
             except asyncio.CancelledError as error:
                 failure = error
             except Exception as error:  # noqa: BLE001 - worker failures are terminal
    @@ -221,6 +239,49 @@ class CommandExecutor:
                 raise AssertionError("Phase 1 execution plan requires a reply")
             self._finish(request.token, Replied(plan.reply))

    +    async def active_expire_once(self) -> int:
    +        if self._worker_task is None or self._stopping:
    +            return 0
    +        future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    +        tick = ActiveExpireTick(self.clock.now_ms(), future)
    +        if not self.post_control(tick):
    +            return 0
    +        return await asyncio.shield(future)
    +
    +    async def _active_expire_once(self, now_ms: int) -> int:
    +        keys = sorted(
    +            key
    +            for key, entry in self.database.entries.items()
    +            if entry.expire_at_ms is not None
    +        )
    +        if not keys:
    +            self._active_expire_cursor = None
    +            return 0
    +        start = (
    +            0
    +            if self._active_expire_cursor is None
    +            else bisect_right(keys, self._active_expire_cursor)
    +        )
    +        ordered_keys = keys[start:] + keys[:start]
    +        candidate_keys = ordered_keys[: self.active_expire_sample_size]
    +        self._active_expire_cursor = candidate_keys[-1]
    +        operations = tuple(
    +            expiry_delete(key)
    +            for key in candidate_keys
    +            if is_expired(self.database.entries[key], now_ms)
    +        )
    +        if not operations:
    +            return 0
    +        batch = CommitBatch(
    +            self.database.commit_seq + 1,
    +            operations,
    +            CommitTrigger.ACTIVE_EXPIRE,
    +        )
    +        await self.commit_barrier.append(batch)
    +        self.database.apply_batch(batch, track_access=False)
    +        self._applied_batches.append(batch)
    +        return len(operations)
    +
         def _finish(self, token: RequestToken, outcome: RequestOutcome) -> None:
             future = self._accepted.pop(token, None)
             if future is not None and not future.done():
    @@ -272,6 +333,5 @@ class CommandExecutor:
         def debug_failure(self) -> BaseException | None:
             return self._failure

    -    @property
         def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
             return tuple(self._applied_batches)
    ```

**是什么，为什么现在需要**

单一 Executor 增加有界主动过期 Control Message，而不是让第二个 Task 直接修改 Database。

**在运行时做什么**

它循环遍历排序 TTL Key，提出过期删除，追加一个 `ACTIVE_EXPIRE` Batch，再按 Mailbox 顺序应用。

**关键代码**

```python
candidate_keys = ordered_keys[: self.active_expire_sample_size]
self._active_expire_cursor = candidate_keys[-1]
```

**关键语句理解**

切片是每 Tick 工作量上界；Cursor 防止每次都只检查同一前缀。

#### TTL 路由与可注入时间

路由类型化 TTL 命令，并用单一公开构造路径接收可确定 Clock。

??? note "文件差异：src/miniredis/core/planner.py"
    ```diff
    diff --git a/src/miniredis/core/planner.py b/src/miniredis/core/planner.py
    index 6ab2e0b94da903fcd461e9f293b730f22d55c3a8..114f1b5edba78f4bd131ee82022e57dc1b6b1850 100644
    --- a/src/miniredis/core/planner.py
    +++ b/src/miniredis/core/planner.py
    @@ -7,6 +7,7 @@ from miniredis.core.list_planner import plan_list
     from miniredis.core.planning import plan_general_and_strings
     from miniredis.core.reply import Failure
     from miniredis.core.set_planner import plan_set
    +from miniredis.core.ttl_planner import plan_ttl
     from miniredis.core.zset_planner import plan_zset


    @@ -29,6 +30,8 @@ class CommandPlanner:
                 plan = plan_set(command, database, now_ms)
             if plan is None:
                 plan = plan_zset(command, database, now_ms)
    +        if plan is None:
    +            plan = plan_ttl(command, database, now_ms)
             if plan is not None:
                 return plan
             return ExecutionPlan(Failure("ERR", "unknown command"))
    ```

**是什么，为什么现在需要**

稳定 Planner 门面在既有值 Planner 之后增加 TTL 命令族。

**在运行时做什么**

它把一个类型化 TTL 命令路由到唯一语义所有者，并保留既有未知命令 Fallback。

**关键代码**

```python
if plan is None:
    plan = plan_ttl(command, database, now_ms)
```

**关键语句理解**

`None` 仍表示“不属于我的命令族”；不带操作的 `ExecutionPlan` 仍可以是完整 TTL 结果。

??? note "文件差异：src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 320a112b68bef7c9d40088309fff888ee4407339..1ccca65f5ebeb14d9ce767f36eaa734aad3aa13b 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -43,6 +43,7 @@ class MiniRedis:
                 clock=clock,
                 commit_barrier=commit_barrier,
                 max_pending_commands=config.max_pending_commands,
    +            active_expire_sample_size=config.active_expire_sample_size,
                 on_terminal_failure=self._on_executor_terminal_failure,
             )
             self.state = RuntimeState.STARTING
    @@ -54,6 +55,9 @@ class MiniRedis:
         def open(
             cls,
             config: MiniRedisConfig | None = None,
    +        *,
    +        clock: Clock | None = None,
    +        commit_barrier: CommitBarrier | None = None,
             **options: Any,
         ) -> MiniRedis:
             if config is not None and options:
    @@ -61,8 +65,10 @@ class MiniRedis:
             resolved = config if config is not None else MiniRedisConfig(**options)
             return cls(
                 resolved,
    -            clock=SystemClock(),
    -            commit_barrier=NullCommitBarrier(),
    +            clock=clock if clock is not None else SystemClock(),
    +            commit_barrier=(
    +                commit_barrier if commit_barrier is not None else NullCommitBarrier()
    +            ),
             )

         @classmethod
    @@ -74,15 +80,11 @@ class MiniRedis:
             commit_barrier: CommitBarrier | None = None,
             **options: Any,
         ) -> MiniRedis:
    -        if config is not None and options:
    -            raise TypeError("config cannot be combined with keyword options")
    -        resolved = config if config is not None else MiniRedisConfig(**options)
    -        return cls(
    -            resolved,
    -            clock=clock if clock is not None else SystemClock(),
    -            commit_barrier=(
    -                commit_barrier if commit_barrier is not None else NullCommitBarrier()
    -            ),
    +        return cls.open(
    +            config,
    +            clock=clock,
    +            commit_barrier=commit_barrier,
    +            **options,
             )

         async def start(self) -> None:
    @@ -137,8 +139,16 @@ class MiniRedis:
             return self.database.commit_seq

         @property
    +    def debug_physical_key_count(self) -> int:
    +        return len(self.database.entries)
    +
    +    async def debug_active_expire_once(self) -> int:
    +        if self.state is not RuntimeState.RUNNING:
    +            return 0
    +        return await self.executor.active_expire_once()
    +
         def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
    -        return self.executor.debug_applied_batches
    +        return self.executor.debug_applied_batches()

         def debug_pause_executor(self) -> None:
             self.executor.debug_pause()
    ```

**是什么，为什么现在需要**

公开 Runtime 构造路径现在可接收 Clock，并暴露窄化过期诊断供契约观察。

**在运行时做什么**

它把配置的采样上限传入 Executor，并委托主动清理，而不暴露数据库直接变更。

**关键代码**

```python
return cls.open(
    config,
    clock=clock,
    commit_barrier=commit_barrier,
    **options,
)
```

**关键语句理解**

`open_with_dependencies` 与 `open` 收敛到同一构造路径，生产环境与确定性测试不会在接线上漂移。

#### 确定性测试 Import 支撑

让契约测试导入共享 Fake Clock，但不把测试路径接线当作运行时机制。

??? note "支撑文件差异（1 个文件）"
    **`pyproject.toml`**

    ```diff
    diff --git a/pyproject.toml b/pyproject.toml
    index 1e9b86283e2c41e7bd279011744db73208baeaf3..c0d64b9b863a57c9f4053018f0ef842aab273075 100644
    --- a/pyproject.toml
    +++ b/pyproject.toml
    @@ -22,5 +22,5 @@ packages = ["src/miniredis"]
     asyncio_mode = "auto"
     asyncio_default_fixture_loop_scope = "function"
     asyncio_default_test_loop_scope = "function"
    -pythonpath = ["src"]
    +pythonpath = ["src", "."]
     testpaths = ["tests"]
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/07-absolute-ttl/tests.txt)`。它经公开 Direct Client 与 Executor 证明 TTL 契约，包括确定的边界时间与有界物理清理。

### 需要真正记住的内容

存储绝对截止时间；分开逻辑不可见与物理回收；把过期删除当作 Proposal；命令失败时丢弃全部拟议操作；原地变更保留截止时间。

### 用自己的话讲清楚

过期不会创建第二个 Writer。Clock 时间改变 Lookup 的 Proposal，但只有 Executor 发布删除。惰性读取立即提供逻辑不可见，有界 Active Tick 则经同一有序提交路径最终回收未触碰的物理 Entry。

### 教材

[第 4 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/04-expiration.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/79fc734...ddfd69e)

完成后可运行 `python -m journey.tools.build_journey check 7` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/07-absolute-ttl/stage.patch)

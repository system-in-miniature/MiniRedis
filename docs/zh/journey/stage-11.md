# Stage 11 · 阻塞 Pop 竞态所有权

### 目标

实现 BLPOP，使 Push、Timeout、取消与 Session 关闭仅有一个按 Mailbox 顺序产生的胜者。

??? note "交付文件"
    - `src/miniredis/adapters/direct.py`
    - `src/miniredis/clock.py`
    - `src/miniredis/commands/model.py`
    - `src/miniredis/commands/parser.py`
    - `src/miniredis/core/blocking.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/planner.py`
    - `src/miniredis/runtime.py`
    - `tests/concurrency/test_blpop_races.py`
    - `tests/helpers/time.py`
    - `tests/mechanisms/test_blpop.py`
    - `tests/mechanisms/test_blpop_push_batch.py`

### 当前遇到的问题

命令现在可在立即执行 Turn 之后仍被所有。同一 Waiter 可被 List Push、Timer Callback、调用方取消或 Session 关闭同时目标。如果这些路径修改不同 Future 或索引，一个 Item 可被消费两次，或过期 Waiter 永久残留。

### 测试契约

#### 先看会坏在哪里

Timeout 先于 Push 时，后来的 Item 必须保留；Push 先于 Timeout 时，Item 只消费一次，Timer 变成过期事件。一次多 Item Push 唤醒两个 FIFO Waiter 仍只能分配一个 Commit，每个终态路径都要移除全部 Waiter 索引与 Timer 所有权。

??? note "文件差异：tests/concurrency/test_blpop_races.py"
    ```diff
    diff --git a/tests/concurrency/test_blpop_races.py b/tests/concurrency/test_blpop_races.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..d9c5fb308f97485ea059ccb4785da5fa0c969955
    --- /dev/null
    +++ b/tests/concurrency/test_blpop_races.py
    @@ -0,0 +1,84 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.outbound import OutboxClosed
    +from miniredis.core.reply import Bytes, Items, Number
    +from tests.helpers.time import FakeClock, ManualScheduler
    +
    +
    +async def blocked(client, runtime):
    +    task = asyncio.create_task(client.execute(CommandRequest(b"BLPOP", (b"q", b"5"))))
    +    await runtime.debug_wait_for_waiters(1)
    +    return task
    +
    +
    +@pytest.mark.asyncio
    +async def test_clock_advance_alone_does_not_fire_timeout():
    +    clock = FakeClock()
    +    scheduler = ManualScheduler(clock)
    +    async with MiniRedis.open(clock=clock, scheduler=scheduler) as runtime:
    +        task = await blocked(runtime.direct_client(), runtime)
    +        clock.advance(5_000)
    +        assert not task.done()
    +        scheduler.fire_due()
    +        assert await task == Bytes(None)
    +        assert runtime.debug_waiter_index_counts == (0, 0, 0)
    +
    +
    +@pytest.mark.asyncio
    +async def test_timeout_then_push_leaves_item_but_push_then_timeout_consumes():
    +    clock = FakeClock()
    +    scheduler = ManualScheduler(clock)
    +    async with MiniRedis.open(clock=clock, scheduler=scheduler) as runtime:
    +        producer = runtime.direct_client()
    +        first = await blocked(runtime.direct_client(), runtime)
    +        clock.advance(5_000)
    +        scheduler.fire_due()
    +        assert await first == Bytes(None)
    +        assert runtime.debug_waiter_index_counts == (0, 0, 0)
    +        assert await producer.execute(CommandRequest(b"RPUSH", (b"q", b"a"))) == Number(
    +            1
    +        )
    +        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"a")
    +
    +        second = await blocked(runtime.direct_client(), runtime)
    +        push = await producer.execute(CommandRequest(b"RPUSH", (b"q", b"b")))
    +        clock.advance(5_000)
    +        scheduler.fire_due()
    +        assert push == Number(1)
    +        assert await second == Items((Bytes(b"q"), Bytes(b"b")))
    +
    +
    +@pytest.mark.asyncio
    +async def test_cancel_and_session_close_are_mailbox_ordered():
    +    async with MiniRedis.open() as runtime:
    +        producer = runtime.direct_client()
    +        cancelled_client = runtime.direct_client()
    +        cancelled = await blocked(cancelled_client, runtime)
    +        cancelled.cancel()
    +        with pytest.raises(asyncio.CancelledError):
    +            await cancelled
    +        await runtime.debug_wait_for_waiters(0)
    +        assert runtime.debug_waiter_index_counts == (0, 0, 0)
    +        await producer.execute(CommandRequest(b"RPUSH", (b"q", b"c")))
    +        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"c")
    +
    +        closed_client = runtime.direct_client()
    +        closed = await blocked(closed_client, runtime)
    +        await closed_client.close()
    +        assert await closed == Bytes(None)
    +        assert runtime.debug_waiter_index_counts == (0, 0, 0)
    +        await producer.execute(CommandRequest(b"RPUSH", (b"q", b"d")))
    +        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"d")
    +
    +
    +@pytest.mark.asyncio
    +async def test_session_close_wakes_pending_outbox_receive():
    +    async with MiniRedis.open() as runtime:
    +        client = runtime.direct_client()
    +        receiving = asyncio.create_task(client.receive())
    +        await client.close()
    +        with pytest.raises(OutboxClosed, match="session closed"):
    +            await receiving
    ```

**测试锁定什么**

它锁定显式 Timer 触发、Timeout/Push 顺序、取消、Session 关闭与过期事件无害性。

**如何构造反例**

它推进 Fake Clock 但不触发 Callback，然后选择 Timeout、Push、Cancel 或 Close 中谁先进 Mailbox。

**关键测试语句**

```python
scheduler.fire_due()
assert await task == Bytes(None)
```

**失败意味着什么**

Clock Read 执行了隐式调度，或失败竞态路径仍可迁移已终态 Waiter。

??? note "文件差异：tests/helpers/time.py"
    ```diff
    diff --git a/tests/helpers/time.py b/tests/helpers/time.py
    index 4f25f86fb5cb69f8df5d6544d8d5d39138431720..327e80c33f892f06e335be0fc0fc6bbb913dc6bb 100644
    --- a/tests/helpers/time.py
    +++ b/tests/helpers/time.py
    @@ -1,3 +1,8 @@
    +import heapq
    +from collections.abc import Callable
    +from dataclasses import dataclass, field
    +
    +
     class FakeClock:
         def __init__(self, now_ms: int = 0) -> None:
             self.value = now_ms
    @@ -7,3 +12,44 @@ class FakeClock:

         def advance(self, milliseconds: int) -> None:
             self.value += milliseconds
    +
    +
    +@dataclass(order=True)
    +class ManualHandle:
    +    deadline_ms: int
    +    order: int
    +    callback: Callable[[], None] = field(compare=False)
    +    cancelled: bool = field(default=False, compare=False)
    +
    +    def cancel(self) -> None:
    +        self.cancelled = True
    +
    +
    +class ManualScheduler:
    +    def __init__(self, clock: FakeClock) -> None:
    +        self.clock = clock
    +        self._next_order = 0
    +        self._calls: list[ManualHandle] = []
    +
    +    def call_at_ms(
    +        self,
    +        deadline_ms: int,
    +        callback: Callable[[], None],
    +    ) -> ManualHandle:
    +        handle = ManualHandle(deadline_ms, self._next_order, callback)
    +        self._next_order += 1
    +        heapq.heappush(self._calls, handle)
    +        return handle
    +
    +    def fire_due(self) -> int:
    +        fired = 0
    +        while self._calls and self._calls[0].deadline_ms <= self.clock.now_ms():
    +            handle = heapq.heappop(self._calls)
    +            if not handle.cancelled:
    +                handle.callback()
    +                fired += 1
    +        return fired
    +
    +    @property
    +    def pending_count(self) -> int:
    +        return sum(not handle.cancelled for handle in self._calls)
    ```

**测试锁定什么**

Manual Scheduler 分开 Deadline 注册、Clock 推进、Callback 触发与取消。

**如何构造反例**

它用 Sequence 排序相同 Deadline Handle，并只触发已到期且未取消的 Callback。

**关键测试语句**

```python
while self._calls and self._calls[0].deadline_ms <= self.clock.now_ms():
```

**失败意味着什么**

并发测试将无法声明哪个 Timer 事件先进 Mailbox。

??? note "文件差异：tests/mechanisms/test_blpop.py"
    ```diff
    diff --git a/tests/mechanisms/test_blpop.py b/tests/mechanisms/test_blpop.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..2ed0f36f45127eff9e74322ac63c0f2960130950
    --- /dev/null
    +++ b/tests/mechanisms/test_blpop.py
    @@ -0,0 +1,61 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.commands.model import BlPop
    +from miniredis.commands.parser import CommandParseError, parse_request
    +from miniredis.core.reply import Bytes, Failure, Items
    +
    +
    +def test_blpop_parser_freezes_keys_and_milliseconds():
    +    assert parse_request(CommandRequest(b"BLPOP", (b"a", b"b", b"1.25"))) == BlPop(
    +        (b"a", b"b"), 1250
    +    )
    +
    +
    +@pytest.mark.parametrize(
    +    "raw",
    +    [b"1_0", b" 1", b"1 ", b"NaN", b"Inf", b"+Inf", b"-Inf"],
    +)
    +def test_blpop_timeout_rejects_non_redis_numeric_syntax(raw):
    +    with pytest.raises(CommandParseError):
    +        parse_request(CommandRequest(b"BLPOP", (b"a", raw)))
    +
    +
    +def test_blpop_timeout_rejects_huge_finite_exponent():
    +    with pytest.raises(CommandParseError, match="timeout is out of range"):
    +        parse_request(CommandRequest(b"BLPOP", (b"a", b"1e999999")))
    +
    +
    +@pytest.mark.asyncio
    +async def test_blpop_uses_first_ready_key_and_stops_type_checks():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"RPUSH", (b"ready", b"x")))
    +        await c.execute(CommandRequest(b"SET", (b"wrong", b"s")))
    +        assert await c.execute(
    +            CommandRequest(b"BLPOP", (b"ready", b"wrong", b"1"))
    +        ) == Items((Bytes(b"ready"), Bytes(b"x")))
    +        assert isinstance(
    +            await c.execute(CommandRequest(b"BLPOP", (b"wrong", b"ready", b"1"))),
    +            Failure,
    +        )
    +
    +
    +@pytest.mark.asyncio
    +async def test_empty_scan_registers_once_under_every_key():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        blocked = asyncio.create_task(
    +            c.execute(CommandRequest(b"BLPOP", (b"a", b"b", b"0")))
    +        )
    +        await runtime.debug_wait_for_waiters(1)
    +        assert runtime.debug_waiter_ids(b"a") == runtime.debug_waiter_ids(b"b")
    +        blocked.cancel()
    +        with pytest.raises(asyncio.CancelledError):
    +            await blocked
    +        await runtime.debug_wait_for_waiters(0)
    +        assert runtime.debug_waiter_ids(b"a") == ()
    +        assert runtime.debug_waiter_ids(b"b") == ()
    +        assert runtime.debug_waiter_index_counts == (0, 0, 0)
    ```

**测试锁定什么**

它锁定严格有限 Timeout Parse、第一就绪 Key 顺序、类型检查停止与一个 Waiter 在所有请求 Key 下的索引。

**如何构造反例**

它混合已就绪 List 与后续 Wrong Type，再反转它们，并取消一个注册在两个 Key 下的无限 Waiter。

**关键测试语句**

```python
assert runtime.debug_waiter_index_counts == (0, 0, 0)
```

**失败意味着什么**

Parser 语法漂移、Scan 顺序改变语义，或终态清理留下身份、Key 或 Session 索引。

??? note "文件差异：tests/mechanisms/test_blpop_push_batch.py"
    ```diff
    diff --git a/tests/mechanisms/test_blpop_push_batch.py b/tests/mechanisms/test_blpop_push_batch.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..4acd1ac4efba899523bc585cbdba084813d3de6e
    --- /dev/null
    +++ b/tests/mechanisms/test_blpop_push_batch.py
    @@ -0,0 +1,50 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.commit import DeleteKey
    +from miniredis.core.reply import Bytes, Items, Number
    +
    +
    +@pytest.mark.asyncio
    +async def test_full_push_then_fifo_pops_are_one_commit_batch():
    +    async with MiniRedis.open() as runtime:
    +        first_client = runtime.direct_client()
    +        second_client = runtime.direct_client()
    +        producer = runtime.direct_client()
    +        first = asyncio.create_task(
    +            first_client.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
    +        )
    +        second = asyncio.create_task(
    +            second_client.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
    +        )
    +        await runtime.debug_wait_for_waiters(2)
    +        before = runtime.debug_commit_seq
    +        assert await producer.execute(
    +            CommandRequest(b"RPUSH", (b"q", b"a", b"b"))
    +        ) == Number(2)
    +        assert await first == Items((Bytes(b"q"), Bytes(b"a")))
    +        assert await second == Items((Bytes(b"q"), Bytes(b"b")))
    +        assert runtime.debug_waiter_index_counts == (0, 0, 0)
    +        assert runtime.debug_commit_seq == before + 1
    +        batch = runtime.debug_applied_batches()[-1]
    +        assert len(batch.operations) == 1
    +        assert isinstance(batch.operations[0], DeleteKey)
    +
    +
    +@pytest.mark.asyncio
    +async def test_lpush_order_is_observed_after_the_complete_push():
    +    async with MiniRedis.open() as runtime:
    +        waiter = runtime.direct_client()
    +        producer = runtime.direct_client()
    +        blocked = asyncio.create_task(
    +            waiter.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
    +        )
    +        await runtime.debug_wait_for_waiters(1)
    +        assert await producer.execute(
    +            CommandRequest(b"LPUSH", (b"q", b"a", b"b"))
    +        ) == Number(2)
    +        assert await blocked == Items((Bytes(b"q"), Bytes(b"b")))
    +        assert runtime.debug_waiter_index_counts == (0, 0, 0)
    +        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"a")
    ```

**测试锁定什么**

它锁定 FIFO Waiter 分配、完整 LPUSH/RPUSH 顺序与 Storage + Wakeup 的单一原子 Batch。

**如何构造反例**

两个 Client 在一次两 Item Push 前阻塞，契约随后检查 Reply、Commit Sequence 与最终 Storage Operation。

**关键测试语句**

```python
assert runtime.debug_commit_seq == before + 1
```

**失败意味着什么**

Wakeup 变成独立 Commit，观察到部分 Push，或同一 Item 被消费多次。

### 基本概念

Waiter 拥有身份、Generation、所属 Token/Session、有序 Key、可选 Deadline 与唯一状态迁移。多索引加速 Lookup，但不创建多个所有者。Timer Callback 只发 Control Message；Executor 决定其 Generation 是否仍 Active。

### 为什么需要这个机制

Blocking 把 Request Lifetime 延长到多个 Turn，所以 Single-writer 所有权也必须延伸。把全部终态事件按 Mailbox 排序，可把竞态变成确定事件顺序，并让 List Storage 变化与 Waiter Wakeup 共享一个 Commit 决策。

### 运行时心智模型

BLPOP 先做立即有序 Scan。无就绪项时，Executor 注册一个 Waiter 与可选 Timer。Push 规划完整 List 结果，预留 FIFO Waiter，调整唯一 Storage Operation，只提交一次，再迁移并 Reply 预留 Waiter。Timeout、Cancel 与 Close 走同一 Generation-checked Gate。

### 机制板块

#### 类型化 BLPOP 边界

在 Blocking 状态存在前冻结有序 Key 与向上取整的毫秒 Timeout。

??? note "文件差异：src/miniredis/commands/model.py"
    ```diff
    diff --git a/src/miniredis/commands/model.py b/src/miniredis/commands/model.py
    index db978c7fbdac19c1bbe18ae254149a434b6d5df5..712e25b6718caf452ab7605751a9c8ca8dfadab5 100644
    --- a/src/miniredis/commands/model.py
    +++ b/src/miniredis/commands/model.py
    @@ -98,6 +98,12 @@ class ListRange:
         stop: int


    +@dataclass(frozen=True, slots=True)
    +class BlPop:
    +    keys: tuple[bytes, ...]
    +    timeout_ms: int
    +
    +
     @dataclass(frozen=True, slots=True)
     class SetAdd:
         key: bytes
    @@ -204,6 +210,7 @@ Command: TypeAlias = (
         | ListPush
         | ListPop
         | ListRange
    +    | BlPop
         | SetAdd
         | SetRemove
         | SetIsMember
    ```

**是什么，为什么现在需要**

类型化命令冻结 BLPOP Key 顺序与 Timeout 毫秒。

**在运行时做什么**

下游代码接收已校验不可变意图，不再解析 Transport Bytes。

**关键代码**

```python
class BlPop:
    keys: tuple[bytes, ...]
    timeout_ms: int
```

**关键语句理解**

Key Tuple 顺序可观察，因为 BLPOP 选第一就绪 Key。

??? note "文件差异：src/miniredis/commands/parser.py"
    ```diff
    diff --git a/src/miniredis/commands/parser.py b/src/miniredis/commands/parser.py
    index aaeb44009f85d5b6256d323d4c23a3ddd22072c7..a812c9ca18d42cbdc2358ba2f9ee8ae218a8cfaa 100644
    --- a/src/miniredis/commands/parser.py
    +++ b/src/miniredis/commands/parser.py
    @@ -2,9 +2,16 @@ from __future__ import annotations

     import math
     import re
    +from decimal import (
    +    ROUND_CEILING,
    +    Decimal,
    +    InvalidOperation,
    +    Overflow as DecimalOverflow,
    +)
     from typing import Literal

     from miniredis.commands.model import (
    +    BlPop,
         Command,
         Delete,
         Echo,
    @@ -51,6 +58,10 @@ _INTEGER = re.compile(rb"-?(?:0|[1-9][0-9]*)\Z")
     _SCORE = re.compile(
         rb"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?(?:0|[1-9][0-9]*))?\Z"
     )
    +_BLPOP_TIMEOUT = re.compile(
    +    rb"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?"
    +)
    +_MAX_BLPOP_TIMEOUT_MS = (1 << 63) - 1


     def parse_int64(value: bytes) -> int:
    @@ -145,7 +156,7 @@ def _parse_set(args: tuple[bytes, ...]) -> SetString:
         return SetString(args[0], args[1], only_if=only_if, expire_ms=expire_ms)


    -def parse_command_request(request: CommandRequest) -> Command:
    +def parse_request(request: CommandRequest) -> Command:
         name = request.name.upper()
         args = request.args
         match name:
    @@ -201,6 +212,28 @@ def parse_command_request(request: CommandRequest) -> Command:
             case b"LRANGE":
                 _require_arity(name, args, 3)
                 return ListRange(args[0], parse_int64(args[1]), parse_int64(args[2]))
    +        case b"BLPOP":
    +            if len(args) < 2:
    +                raise CommandParseError("wrong number of arguments")
    +            raw_timeout = args[-1]
    +            if not _BLPOP_TIMEOUT.fullmatch(raw_timeout):
    +                raise CommandParseError("timeout is not a finite non-negative number")
    +            try:
    +                seconds = Decimal(raw_timeout.decode("ascii"))
    +            except (UnicodeDecodeError, InvalidOperation):
    +                raise CommandParseError(
    +                    "timeout is not a finite non-negative number"
    +                ) from None
    +            if not seconds.is_finite() or seconds < 0:
    +                raise CommandParseError("timeout is not a finite non-negative number")
    +            try:
    +                timeout_ms = seconds * Decimal(1000)
    +            except (InvalidOperation, DecimalOverflow):
    +                raise CommandParseError("timeout is out of range") from None
    +            if not timeout_ms.is_finite() or timeout_ms > _MAX_BLPOP_TIMEOUT_MS:
    +                raise CommandParseError("timeout is out of range")
    +            milliseconds = int(timeout_ms.to_integral_value(rounding=ROUND_CEILING))
    +            return BlPop(tuple(args[:-1]), milliseconds)
             case b"SADD":
                 _require_min_arity(name, args, 2)
                 return SetAdd(args[0], args[1:])
    @@ -247,3 +280,6 @@ def parse_command_request(request: CommandRequest) -> Command:
                 return Persist(args[0])
             case _:
                 raise CommandParseError("unknown command")
    +
    +
    +parse_command_request = parse_request
    ```

**是什么，为什么现在需要**

严格 Parser 接收 Redis-style 有限小数 Timeout，拒绝过大或非数字形式。

**在运行时做什么**

在 Waiter 或 Timer 存在前，它把秒向上取整为毫秒。

**关键代码**

```python
milliseconds = int(timeout_ms.to_integral_value(rounding=ROUND_CEILING))
return BlPop(tuple(args[:-1]), milliseconds)
```

**关键语句理解**

向上取整避免比请求的正小数 Timeout 更早触发。

#### 可注入 Timer 调度

分开读取时间与调度 Callback，使 Timeout 与 Push 顺序可被确定驱动。

??? note "文件差异：src/miniredis/clock.py"
    ```diff
    diff --git a/src/miniredis/clock.py b/src/miniredis/clock.py
    index cb80d29a067bf65d39f60c118f7c07294bc9795b..18d8ffc37a9c21622f60be0f13e907fa30e1e2aa 100644
    --- a/src/miniredis/clock.py
    +++ b/src/miniredis/clock.py
    @@ -1,6 +1,8 @@
     from __future__ import annotations

    +import asyncio
     import time
    +from collections.abc import Callable
     from typing import Protocol


    @@ -11,3 +13,30 @@ class Clock(Protocol):
     class SystemClock:
         def now_ms(self) -> int:
             return time.time_ns() // 1_000_000
    +
    +
    +class ScheduledHandle(Protocol):
    +    def cancel(self) -> None:
    +        raise NotImplementedError
    +
    +
    +class TimerScheduler(Protocol):
    +    def call_at_ms(
    +        self,
    +        deadline_ms: int,
    +        callback: Callable[[], None],
    +    ) -> ScheduledHandle:
    +        raise NotImplementedError
    +
    +
    +class AsyncioTimerScheduler:
    +    def __init__(self, clock: Clock) -> None:
    +        self._clock = clock
    +
    +    def call_at_ms(
    +        self,
    +        deadline_ms: int,
    +        callback: Callable[[], None],
    +    ) -> asyncio.TimerHandle:
    +        delay = max(0, deadline_ms - self._clock.now_ms()) / 1000
    +        return asyncio.get_running_loop().call_later(delay, callback)
    ```

**是什么，为什么现在需要**

Timer 调度与 Clock 值源分离。

**在运行时做什么**

生产环境用 Event-loop Callback；测试针对同一 Deadline 契约注入 Manual Scheduler。

**关键代码**

```python
delay = max(0, deadline_ms - self._clock.now_ms()) / 1000
return asyncio.get_running_loop().call_later(delay, callback)
```

**关键语句理解**

Scheduler 在注册时导出 Delay，Callback 仍只经 Mailbox Control 进入状态所有权。

#### 索引化 Waiter 所有权

按身份、Key 与 Session 索引一个 Waiter，并只允许一次经 Generation 校验的终态迁移。

??? note "文件差异：src/miniredis/core/blocking.py"
    ```diff
    diff --git a/src/miniredis/core/blocking.py b/src/miniredis/core/blocking.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..d1e91622d737f0a4fb8248560aecfa6ede624245
    --- /dev/null
    +++ b/src/miniredis/core/blocking.py
    @@ -0,0 +1,200 @@
    +from __future__ import annotations
    +
    +from collections import defaultdict, deque
    +from collections.abc import Callable
    +from dataclasses import dataclass
    +from enum import Enum
    +from typing import Protocol
    +
    +from miniredis.core.commit import (
    +    CommitOperation,
    +    DeleteKey,
    +    DeleteReason,
    +    PutEntry,
    +    StoredEntry,
    +    StoredList,
    +)
    +from miniredis.core.outbound import RequestToken
    +
    +
    +class CancelHandle(Protocol):
    +    def cancel(self) -> None:
    +        raise NotImplementedError
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class WaiterId:
    +    value: int
    +
    +
    +class WaiterState(str, Enum):
    +    ACTIVE = "active"
    +    FULFILLED = "fulfilled"
    +    TIMED_OUT = "timed_out"
    +    CANCELLED = "cancelled"
    +    CLOSED = "closed"
    +
    +
    +@dataclass(slots=True)
    +class BlockingWaiter:
    +    waiter_id: WaiterId
    +    generation: int
    +    token: RequestToken
    +    session_id: int
    +    keys: tuple[bytes, ...]
    +    deadline_ms: int | None
    +    state: WaiterState = WaiterState.ACTIVE
    +    timer: CancelHandle | None = None
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class WaiterWakeup:
    +    waiter_id: WaiterId
    +    generation: int
    +    key: bytes
    +    item: bytes
    +
    +
    +class WaiterRegistry:
    +    def __init__(self, on_debug_change: Callable[[], None]) -> None:
    +        self._next_id = 1
    +        self._by_id: dict[WaiterId, BlockingWaiter] = {}
    +        self._by_key: dict[bytes, deque[WaiterId]] = defaultdict(deque)
    +        self._by_session: dict[int, set[WaiterId]] = defaultdict(set)
    +        self._on_debug_change = on_debug_change
    +
    +    def register(
    +        self,
    +        token: RequestToken,
    +        session_id: int,
    +        keys: tuple[bytes, ...],
    +        deadline_ms: int | None,
    +    ) -> BlockingWaiter:
    +        waiter = BlockingWaiter(
    +            WaiterId(self._next_id), 1, token, session_id, keys, deadline_ms
    +        )
    +        self._next_id += 1
    +        self._by_id[waiter.waiter_id] = waiter
    +        self._by_session[session_id].add(waiter.waiter_id)
    +        for key in dict.fromkeys(keys):
    +            self._by_key[key].append(waiter.waiter_id)
    +        self._on_debug_change()
    +        return waiter
    +
    +    def peek(
    +        self,
    +        key: bytes,
    +        excluded: set[WaiterId],
    +    ) -> BlockingWaiter | None:
    +        for waiter_id in self._by_key.get(key, ()):
    +            waiter = self._by_id.get(waiter_id)
    +            if (
    +                waiter is not None
    +                and waiter.state is WaiterState.ACTIVE
    +                and waiter_id not in excluded
    +            ):
    +                return waiter
    +        return None
    +
    +    def transition(
    +        self,
    +        waiter_id: WaiterId,
    +        generation: int,
    +        state: WaiterState,
    +    ) -> BlockingWaiter | None:
    +        waiter = self._by_id.get(waiter_id)
    +        if (
    +            waiter is None
    +            or waiter.generation != generation
    +            or waiter.state is not WaiterState.ACTIVE
    +        ):
    +            return None
    +        waiter.state = state
    +        if waiter.timer is not None:
    +            waiter.timer.cancel()
    +        del self._by_id[waiter_id]
    +        session_index = self._by_session.get(waiter.session_id)
    +        if session_index is not None:
    +            session_index.discard(waiter_id)
    +            if not session_index:
    +                del self._by_session[waiter.session_id]
    +        for key in dict.fromkeys(waiter.keys):
    +            index = self._by_key[key]
    +            self._by_key[key] = deque(item for item in index if item != waiter_id)
    +            if not self._by_key[key]:
    +                del self._by_key[key]
    +        self._on_debug_change()
    +        return waiter
    +
    +    def for_session(self, session_id: int) -> tuple[BlockingWaiter, ...]:
    +        return tuple(
    +            self._by_id[item]
    +            for item in tuple(self._by_session.get(session_id, ()))
    +            if item in self._by_id
    +        )
    +
    +    def for_token(self, token: RequestToken) -> BlockingWaiter | None:
    +        return next(
    +            (waiter for waiter in self._by_id.values() if waiter.token == token),
    +            None,
    +        )
    +
    +    def active(self) -> tuple[BlockingWaiter, ...]:
    +        return tuple(self._by_id.values())
    +
    +    @property
    +    def active_count(self) -> int:
    +        return len(self._by_id)
    +
    +    @property
    +    def timer_count(self) -> int:
    +        return sum(waiter.timer is not None for waiter in self._by_id.values())
    +
    +    def ids_for_key(self, key: bytes) -> tuple[WaiterId, ...]:
    +        return tuple(
    +            waiter_id
    +            for waiter_id in self._by_key.get(key, ())
    +            if waiter_id in self._by_id
    +        )
    +
    +    @property
    +    def index_counts(self) -> tuple[int, int, int]:
    +        return len(self._by_id), len(self._by_key), len(self._by_session)
    +
    +
    +def prepare_list_wakeups(
    +    key: bytes,
    +    pushed: PutEntry,
    +    waiters: WaiterRegistry,
    +) -> tuple[CommitOperation, tuple[WaiterWakeup, ...]]:
    +    stored = pushed.entry.value
    +    if not isinstance(stored, StoredList):
    +        raise TypeError("push operation must contain StoredList")
    +    remaining = deque(stored.items)
    +    reserved: set[WaiterId] = set()
    +    wakeups: list[WaiterWakeup] = []
    +    while remaining:
    +        waiter = waiters.peek(key, reserved)
    +        if waiter is None:
    +            break
    +        reserved.add(waiter.waiter_id)
    +        wakeups.append(
    +            WaiterWakeup(
    +                waiter.waiter_id,
    +                waiter.generation,
    +                key,
    +                remaining.popleft(),
    +            )
    +        )
    +    if remaining:
    +        final: CommitOperation = PutEntry(
    +            key,
    +            StoredEntry(
    +                StoredList(tuple(remaining)),
    +                pushed.entry.expire_at_ms,
    +                pushed.entry.mutation_version,
    +            ),
    +        )
    +    else:
    +        final = DeleteKey(key, DeleteReason.CLIENT)
    +    return final, tuple(wakeups)
    ```

**是什么，为什么现在需要**

该模块拥有 Waiter 索引、状态迁移与 Push List Item 的确定预留。

**在运行时做什么**

它查找 FIFO 合格 Waiter，在一次迁移中移除全部索引、取消 Timer，并返回 Wakeup Proposal。

**关键代码**

```python
if (
    waiter is None
    or waiter.generation != generation
    or waiter.state is not WaiterState.ACTIVE
):
    return None
```

**关键语句理解**

Identity + Generation 使另一事件已获胜后到达的 Timeout/Cancel 无害。

#### Mailbox 有序 Blocking 裁决

通过单一 Executor 裁决立即 Pop、注册、Push、Timeout、取消与 Session 关闭。

??? note "文件差异：src/miniredis/core/planner.py"
    ```diff
    diff --git a/src/miniredis/core/planner.py b/src/miniredis/core/planner.py
    index 0fe5754bc713b5287898046fea3d18c2152186d3..02eafc09d88365aeaa7fcaa3212dec01213713c1 100644
    --- a/src/miniredis/core/planner.py
    +++ b/src/miniredis/core/planner.py
    @@ -1,15 +1,27 @@
    +from collections import deque
    +
    +from miniredis.commands.model import BlPop
     from miniredis.commands.model import Command
     from miniredis.config import MiniRedisConfig
    +from miniredis.core.commit import (
    +    CommitOperation,
    +    DeleteKey,
    +    DeleteReason,
    +    PutEntry,
    +    StoredEntry,
    +    StoredList,
    +)
     from miniredis.core.database import Database
     from miniredis.core.eviction import enforce_memory
     from miniredis.core.executor import ExecutionPlan
     from miniredis.core.hash_planner import plan_hash
     from miniredis.core.list_planner import plan_list
     from miniredis.core.planning import plan_general_and_strings
    -from miniredis.core.reply import Failure
    +from miniredis.core.reply import Bytes, Failure, Items
     from miniredis.core.set_planner import plan_set
     from miniredis.core.ttl_planner import plan_ttl
     from miniredis.core.zset_planner import plan_zset
    +from miniredis.core.values import ListValue


     class CommandPlanner:
    @@ -36,3 +48,43 @@ class CommandPlanner:
             if plan is not None:
                 return enforce_memory(plan, database, self.config, now_ms)
             return ExecutionPlan(Failure("ERR", "unknown command"))
    +
    +    def plan_blpop_now(
    +        self,
    +        command: BlPop,
    +        database: Database,
    +        now_ms: int,
    +    ) -> ExecutionPlan | None:
    +        for key in command.keys:
    +            entry = database.entries.get(key)
    +            if entry is None:
    +                continue
    +            if entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms:
    +                continue
    +            if not isinstance(entry.value, ListValue):
    +                return ExecutionPlan(
    +                    Failure(
    +                        "WRONGTYPE",
    +                        "operation against a key holding the wrong kind of value",
    +                    )
    +                )
    +            if not entry.value.items:
    +                continue
    +            items = deque(entry.value.items)
    +            item = items.popleft()
    +            if items:
    +                operation: CommitOperation = PutEntry(
    +                    key,
    +                    StoredEntry(
    +                        StoredList(tuple(items)),
    +                        entry.expire_at_ms,
    +                        entry.mutation_version + 1,
    +                    ),
    +                )
    +            else:
    +                operation = DeleteKey(key, DeleteReason.CLIENT)
    +            return ExecutionPlan(
    +                Items((Bytes(key), Bytes(item))),
    +                operations=(operation,),
    +            )
    +        return None
    ```

**是什么，为什么现在需要**

Planner 拥有立即、非阻塞 BLPOP Scan。

**在运行时做什么**

它按序检查 Key，在第一就绪 List 停止，并只提出一个更新或删除的 List Entry。

**关键代码**

```python
for key in command.keys:
    entry = database.entries.get(key)
    if entry is None:
        continue
    if entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms:
        continue
```

**关键语句理解**

有序 Lookup 属于语义：更早就绪 Key 会在后续 Wrong Type 前结束 Scan。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index 9342732be4efc087056c88f6dedbf75638f0117c..f90d1c615c192dd2612da1a499adc121c9d2e503 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -4,12 +4,25 @@ import asyncio
     import itertools
     from bisect import bisect_right
     from collections.abc import Callable
    -from dataclasses import dataclass
    +from dataclasses import dataclass, replace
     from typing import Protocol

    -from miniredis.clock import Clock
    -from miniredis.commands.model import Command
    -from miniredis.core.commit import CommitBatch, CommitOperation, CommitTrigger
    +from miniredis.clock import Clock, TimerScheduler
    +from miniredis.commands.model import BlPop, Command, ListPush
    +from miniredis.core.blocking import (
    +    WaiterId,
    +    WaiterRegistry,
    +    WaiterState,
    +    WaiterWakeup,
    +    prepare_list_wakeups,
    +)
    +from miniredis.core.commit import (
    +    CommitBatch,
    +    CommitOperation,
    +    CommitTrigger,
    +    PutEntry,
    +    StoredList,
    +)
     from miniredis.core.database import Database
     from miniredis.core.expiration import expiry_delete, is_expired
     from miniredis.core.mailbox import EventLoopMailbox
    @@ -23,7 +36,7 @@ from miniredis.core.outbound import (
         SessionEndpoint,
         TransportClosed,
     )
    -from miniredis.core.reply import Failure, Reply
    +from miniredis.core.reply import Bytes, Failure, Items, Reply


     @dataclass(slots=True)
    @@ -45,9 +58,16 @@ class AbandonRequest:
         token: RequestToken


    -@dataclass(frozen=True, slots=True)
    +@dataclass(slots=True)
     class SessionClosed:
         session_id: int
    +    completion: asyncio.Future[None] | None = None
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class TimeoutWaiter:
    +    waiter_id: WaiterId
    +    generation: int


     @dataclass(frozen=True, slots=True)
    @@ -56,6 +76,7 @@ class ExecutionPlan:
         operations: tuple[CommitOperation, ...] = ()
         touch_keys: tuple[bytes, ...] = ()
         trigger: CommitTrigger = CommitTrigger.CLIENT
    +    waiter_wakeups: tuple[WaiterWakeup, ...] = ()


     class CommitBarrier(Protocol):
    @@ -99,6 +120,7 @@ class CommandExecutor:
             commit_barrier: CommitBarrier,
             max_pending_commands: int,
             active_expire_sample_size: int = 20,
    +        scheduler: TimerScheduler,
             on_debug_change: Callable[[], None],
             on_terminal_failure: Callable[[BaseException], None] | None = None,
         ) -> None:
    @@ -116,6 +138,8 @@ class CommandExecutor:
             )
             self._on_debug_change = on_debug_change
             self._on_terminal_failure = on_terminal_failure
    +        self.waiters = WaiterRegistry(self._on_debug_change)
    +        self.scheduler = scheduler

             self._worker_task: asyncio.Task[None] | None = None
             self._worker_started_or_done = asyncio.Event()
    @@ -263,19 +287,50 @@ class CommandExecutor:
                 deleted = await self._active_expire_once(message.now_ms)
                 if message.future is not None and not message.future.done():
                     message.future.set_result(deleted)
    +        elif isinstance(message, TimeoutWaiter):
    +            self._timeout_waiter(message)
             elif isinstance(message, SessionClosed):
    -            self._close_session(message.session_id)
    +            self._close_session(message)
             else:
                 raise AssertionError(f"unknown executor message: {message!r}")

         def _abandon(self, event: AbandonRequest) -> None:
    +        waiter = self.waiters.for_token(event.token)
    +        if waiter is not None:
    +            transitioned = self.waiters.transition(
    +                waiter.waiter_id,
    +                waiter.generation,
    +                WaiterState.CANCELLED,
    +            )
    +            if transitioned is not None:
    +                self._finish_request(event.token, Abandoned())
    +                return
             self._finish_request(event.token, Abandoned())

    -    def _close_session(self, session_id: int) -> None:
    -        self._endpoints.pop(session_id, None)
    -        for token, request in tuple(self._requests.items()):
    -            if request.session_id == session_id:
    -                self._finish_request(token, TransportClosed())
    +    def _timeout_waiter(self, event: TimeoutWaiter) -> None:
    +        waiter = self.waiters.transition(
    +            event.waiter_id,
    +            event.generation,
    +            WaiterState.TIMED_OUT,
    +        )
    +        if waiter is not None:
    +            self._finish_reply(waiter.token, Bytes(None))
    +
    +    def _close_session(self, event: SessionClosed) -> None:
    +        for waiter in self.waiters.for_session(event.session_id):
    +            closed = self.waiters.transition(
    +                waiter.waiter_id,
    +                waiter.generation,
    +                WaiterState.CLOSED,
    +            )
    +            if closed is not None:
    +                self._finish_request(closed.token, TransportClosed())
    +        endpoint = self._endpoints.pop(event.session_id, None)
    +        if endpoint is not None:
    +            endpoint.outbox.abort("session closed")
    +            endpoint.request_transport_close("session closed")
    +        if event.completion is not None and not event.completion.done():
    +            event.completion.set_result(None)
             self._on_debug_change()

         def _complete_terminal_failure(self, failure: BaseException) -> None:
    @@ -295,24 +350,97 @@ class CommandExecutor:

         async def _execute(self, request: ExecuteRequest) -> None:
             now_ms = self.clock.now_ms()
    -        plan = self.planner.plan(request.command, self.database, now_ms)
    +        if isinstance(request.command, BlPop):
    +            plan = self.planner.plan_blpop_now(request.command, self.database, now_ms)
    +            if plan is None:
    +                deadline = (
    +                    None
    +                    if request.command.timeout_ms == 0
    +                    else now_ms + request.command.timeout_ms
    +                )
    +                waiter = self.waiters.register(
    +                    request.token,
    +                    request.session_id,
    +                    request.command.keys,
    +                    deadline,
    +                )
    +                if waiter.deadline_ms is not None:
    +                    waiter.timer = self.scheduler.call_at_ms(
    +                        waiter.deadline_ms,
    +                        lambda: self.post_control(
    +                            TimeoutWaiter(
    +                                waiter.waiter_id,
    +                                waiter.generation,
    +                            )
    +                        ),
    +                    )
    +                    self._on_debug_change()
    +                return
    +        else:
    +            plan = self.planner.plan(request.command, self.database, now_ms)
    +            plan = self._attach_push_wakeups(request.command, plan)
    +        await self._apply_plan(request, plan, now_ms)
    +
    +    def _attach_push_wakeups(
    +        self,
    +        command: Command,
    +        plan: ExecutionPlan,
    +    ) -> ExecutionPlan:
    +        if not isinstance(command, ListPush) or isinstance(plan.reply, Failure):
    +            return plan
    +        operations = list(plan.operations)
    +        for index, operation in enumerate(operations):
    +            if (
    +                isinstance(operation, PutEntry)
    +                and operation.key == command.key
    +                and isinstance(operation.entry.value, StoredList)
    +            ):
    +                final, wakeups = prepare_list_wakeups(
    +                    command.key,
    +                    operation,
    +                    self.waiters,
    +                )
    +                operations[index] = final
    +                return replace(
    +                    plan,
    +                    operations=tuple(operations),
    +                    waiter_wakeups=wakeups,
    +                )
    +        raise AssertionError("successful list push has no target PutEntry")
    +
    +    async def _apply_plan(
    +        self,
    +        request: ExecuteRequest,
    +        plan: ExecutionPlan,
    +        now_ms: int,
    +    ) -> None:
             if plan.operations:
                 batch = CommitBatch(
    -                seq=self.database.commit_seq + 1,
    -                operations=plan.operations,
    -                trigger=plan.trigger,
    +                self.database.commit_seq + 1,
    +                plan.operations,
    +                plan.trigger,
                 )
                 await self.commit_barrier.append(batch)
                 self.database.apply_batch(
    -                batch, track_access=plan.trigger is CommitTrigger.CLIENT
    +                batch,
    +                track_access=plan.trigger is CommitTrigger.CLIENT,
                 )
                 self._applied_batches.append(batch)

             for key in dict.fromkeys(plan.touch_keys):
                 self.database.touch_if_live(key, now_ms)

    -        if plan.reply is None:
    -            raise AssertionError("Phase 1 execution plan requires a reply")
    +        for wakeup in plan.waiter_wakeups:
    +            waiter = self.waiters.transition(
    +                wakeup.waiter_id,
    +                wakeup.generation,
    +                WaiterState.FULFILLED,
    +            )
    +            if waiter is not None:
    +                self._finish_reply(
    +                    waiter.token,
    +                    Items((Bytes(wakeup.key), Bytes(wakeup.item))),
    +                )
             self._finish_reply(request.token, plan.reply)

         async def active_expire_once(self) -> int:
    ```

**是什么，为什么现在需要**

Executor 成为 Waiter 注册、Wakeup、Timeout、Abandon 与 Close 的唯一裁决者。

**在运行时做什么**

它把 Timer 注册为 Control Producer，把 Push Wakeup 折叠进 Storage Plan，提交一次，再终结获胜 Waiter。

**关键代码**

```python
waiter = self.waiters.register(
    request.token,
    request.session_id,
    request.command.keys,
    deadline,
)
```

**关键语句理解**

原请求在阻塞时仍被所有；无需单独 Waiter Future，否则会拆分完成所有权。

#### Blocking Client 与 Runtime 生命周期

注入 Scheduler、暴露有因果的 Waiter 证据，并把已关闭 Blocking Session 映射为公开 Nil 结果。

??? note "文件差异：src/miniredis/adapters/direct.py"
    ```diff
    diff --git a/src/miniredis/adapters/direct.py b/src/miniredis/adapters/direct.py
    index caf1b93dd8d56e19891d70c9c3150fe542b5cc66..49d7f1b05278bccb390f370d35959b07f5d850ee 100644
    --- a/src/miniredis/adapters/direct.py
    +++ b/src/miniredis/adapters/direct.py
    @@ -4,7 +4,12 @@ import asyncio
     from typing import TYPE_CHECKING

     from miniredis.commands.request import CommandRequest
    -from miniredis.core.executor import AbandonRequest, SubmittedRequest
    +from miniredis.commands.model import BlPop
    +from miniredis.core.executor import (
    +    AbandonRequest,
    +    SessionClosed,
    +    SubmittedRequest,
    +)
     from miniredis.core.outbound import (
         Abandoned,
         Outbound,
    @@ -14,7 +19,7 @@ from miniredis.core.outbound import (
         SessionEndpoint,
         TransportClosed,
     )
    -from miniredis.core.reply import Failure, Reply
    +from miniredis.core.reply import Bytes, Failure, Reply

     if TYPE_CHECKING:
         from miniredis.runtime import MiniRedis
    @@ -60,6 +65,8 @@ class DirectClient:
                     return reply
                 case RuntimeClosed():
                     return Failure("CLOSED", "runtime closed before reply")
    +            case TransportClosed() if isinstance(parsed, BlPop):
    +                return Bytes(None)
                 case TransportClosed():
                     return Failure("CLOSED", "session closed")
                 case RuntimeFailed(reason):
    @@ -73,3 +80,18 @@ class DirectClient:

         async def close(self) -> None:
             self._closed = True
    +        if self._close_task is None:
    +            self._close_task = asyncio.create_task(
    +                self._close_once(),
    +                name=f"miniredis:direct-close:{self.session_id}",
    +            )
    +        await asyncio.shield(self._close_task)
    +
    +    async def _close_once(self) -> None:
    +        completion = asyncio.get_running_loop().create_future()
    +        if not self._runtime.executor.post_control(
    +            SessionClosed(self.session_id, completion)
    +        ):
    +            self.endpoint.outbox.abort("runtime closed")
    +            return
    +        await asyncio.shield(completion)
    ```

**是什么，为什么现在需要**

Direct 边界把 BLPOP 的 Session 丢失映射为公开 Nil 结果。

**在运行时做什么**

它在其他方面保留所有请求的类型化终态处理。

**关键代码**

```python
case TransportClosed() if isinstance(parsed, BlPop):
    return Bytes(None)
```

**关键语句理解**

Transport Lifecycle 在 Adapter 边界翻译；Executor 仍报告 Transport Outcome，而非 Protocol Bytes。

??? note "文件差异：src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 65ce40063d2356bca5d7fa1e5b408fb499524e3a..0e944f527c57972b9bc745caef90d46fcaa1709e 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -8,11 +8,17 @@ from enum import Enum
     from typing import Any, Self

     from miniredis.adapters.direct import DirectClient
    -from miniredis.clock import Clock, SystemClock
    +from miniredis.clock import (
    +    AsyncioTimerScheduler,
    +    Clock,
    +    SystemClock,
    +    TimerScheduler,
    +)
     from miniredis.config import MiniRedisConfig
     from miniredis.commands.model import Command
     from miniredis.commands.parser import CommandParseError, parse_command_request
     from miniredis.commands.request import CommandRequest
    +from miniredis.core.blocking import WaiterId
     from miniredis.core.commit import CommitBatch, StoredEntry
     from miniredis.core.database import Database
     from miniredis.core.executor import (
    @@ -55,9 +61,13 @@ class MiniRedis:
             *,
             clock: Clock,
             commit_barrier: CommitBarrier,
    +        scheduler: TimerScheduler | None,
         ) -> None:
             self.config = config
             self.clock = clock
    +        self.scheduler = (
    +            AsyncioTimerScheduler(clock) if scheduler is None else scheduler
    +        )
             self.commit_barrier = commit_barrier
             self.database = Database()
             self.planner = CommandPlanner(config)
    @@ -69,6 +79,7 @@ class MiniRedis:
                 commit_barrier=commit_barrier,
                 max_pending_commands=config.max_pending_commands,
                 active_expire_sample_size=config.active_expire_sample_size,
    +            scheduler=self.scheduler,
                 on_debug_change=self._debug_notify,
                 on_terminal_failure=self._on_executor_terminal_failure,
             )
    @@ -83,6 +94,7 @@ class MiniRedis:
             config: MiniRedisConfig | None = None,
             *,
             clock: Clock | None = None,
    +        scheduler: TimerScheduler | None = None,
             commit_barrier: CommitBarrier | None = None,
             **options: Any,
         ) -> MiniRedis:
    @@ -92,6 +104,7 @@ class MiniRedis:
             return cls(
                 resolved,
                 clock=clock if clock is not None else SystemClock(),
    +            scheduler=scheduler,
                 commit_barrier=(
                     commit_barrier if commit_barrier is not None else NullCommitBarrier()
                 ),
    @@ -103,12 +116,14 @@ class MiniRedis:
             config: MiniRedisConfig | None = None,
             *,
             clock: Clock | None = None,
    +        scheduler: TimerScheduler | None = None,
             commit_barrier: CommitBarrier | None = None,
             **options: Any,
         ) -> MiniRedis:
             return cls.open(
                 config,
                 clock=clock,
    +            scheduler=scheduler,
                 commit_barrier=commit_barrier,
                 **options,
             )
    @@ -213,10 +228,10 @@ class MiniRedis:
             return RuntimeStats(
                 accepted_requests=self.executor.accepted_request_count,
                 pending_futures=self.executor.pending_request_count,
    -            waiters=0,
    +            waiters=self.executor.waiters.active_count,
                 subscriptions=0,
                 sessions=self.executor.endpoint_count,
    -            timer_handles=0,
    +            timer_handles=self.executor.waiters.timer_count,
                 owned_tasks=0,
             )

    @@ -238,3 +253,13 @@ class MiniRedis:

         async def debug_wait_for_sessions(self, count: int) -> None:
             await self._debug_wait(lambda: self.executor.endpoint_count == count)
    +
    +    async def debug_wait_for_waiters(self, count: int) -> None:
    +        await self._debug_wait(lambda: self.executor.waiters.active_count == count)
    +
    +    def debug_waiter_ids(self, key: bytes) -> tuple[WaiterId, ...]:
    +        return self.executor.waiters.ids_for_key(key)
    +
    +    @property
    +    def debug_waiter_index_counts(self) -> tuple[int, int, int]:
    +        return self.executor.waiters.index_counts
    ```

**是什么，为什么现在需要**

Runtime 注入 TimerScheduler，并暴露窄化 Waiter Lifecycle 证据。

**在运行时做什么**

它报告 Waiter/Timer 计数，并通过 Debug Notification 而非 Sleep 等待。

**关键代码**

```python
async def debug_wait_for_waiters(self, count: int) -> None:
    await self._debug_wait(lambda: self.executor.waiters.active_count == count)
```

**关键语句理解**

契约直接观察 Registry 所有权，使竞态设置可确定。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/11-blocking-pop-races/tests.txt)`。它覆盖 Parser 边界、立即 Scan、Waiter 索引、Push/Timeout 顺序、取消、Session Close 与单 Batch FIFO Wakeup。

### 需要真正记住的内容

一个阻塞请求仍由 Runtime 拥有；索引但不复制所有权；把 Timer Callback 变成 Control Message；用 Generation 校验；针对完整 Push 预留 Wakeup；Reply 前只提交一次 Storage。

### 用自己的话讲清楚

BLPOP 不是在 Executor 旁等待的 Future。它是一个已接受请求，Waiter 元数据留在 Executor 所有权内。Push、Timeout、Cancel 与 Close 变成有序消息，第一个有效迁移获胜，所有过期事件成为 No-op。

### 教材

[第 9 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/09-blocking-pubsub-transactions.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/5436512...bb842dd)

完成后可运行 `python -m journey.tools.build_journey check 11` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/11-blocking-pop-races/stage.patch)

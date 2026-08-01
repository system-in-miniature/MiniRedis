# Stage 12 · Pub/Sub 与受监督关闭

### 目标

增加二进制 Pub/Sub，并通过一个显式 Shutdown Barrier 关闭每个异步所有者。

??? note "交付文件"
    - `src/miniredis/adapters/direct.py`
    - `src/miniredis/commands/model.py`
    - `src/miniredis/commands/parser.py`
    - `src/miniredis/config.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/expiration.py`
    - `src/miniredis/core/pubsub.py`
    - `src/miniredis/runtime.py`
    - `tests/concurrency/test_async_invariants.py`
    - `tests/concurrency/test_shutdown.py`
    - `tests/concurrency/test_slow_endpoint.py`
    - `tests/mechanisms/test_pubsub.py`

### 当前遇到的问题

Session 已有 Outbox，BLPOP 已有长生命 Waiter，但还没有 Push Protocol 使用它们。Runtime Close 也需协调多个 Producer 与 Owner：User 准入、Timer Callback、Executor Control、Waiter、Subscription、Endpoint 输出、Owned Task 与 Failure Fallback。

### 测试契约

#### 先看会坏在哪里

慢 Subscriber 必须被移除，且不延迟快 Subscriber 或 Publisher。取消 Close 调用方不得取消清理，Shutdown Control 必须绕过已满 User Queue，无限 Waiter 必须在 Failure 时终结，最终统计必须证明每个异步所有者都已消失。

??? note "文件差异：tests/concurrency/test_async_invariants.py"
    ```diff
    diff --git a/tests/concurrency/test_async_invariants.py b/tests/concurrency/test_async_invariants.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..6732376b906bf40f7188d87fbe5b04365208dcb4
    --- /dev/null
    +++ b/tests/concurrency/test_async_invariants.py
    @@ -0,0 +1,88 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes, Failure, Items, Number
    +from tests.helpers.time import FakeClock, ManualScheduler
    +
    +
    +@pytest.mark.asyncio
    +async def test_push_then_cancel_consumes_once_and_cancel_is_stale():
    +    async with MiniRedis.open(outbox_drain_grace_ms=0) as runtime:
    +        waiter = runtime.direct_client()
    +        producer = runtime.direct_client()
    +        blocked = asyncio.create_task(
    +            waiter.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
    +        )
    +        await runtime.debug_wait_for_waiters(1)
    +        assert await producer.execute(
    +            CommandRequest(b"RPUSH", (b"q", b"x"))
    +        ) == Number(1)
    +        assert await blocked == Items((Bytes(b"q"), Bytes(b"x")))
    +        blocked.cancel()
    +        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(
    +            None
    +        )
    +
    +
    +@pytest.mark.asyncio
    +async def test_push_then_session_close_keeps_consumption():
    +    async with MiniRedis.open(outbox_drain_grace_ms=0) as runtime:
    +        waiter = runtime.direct_client()
    +        producer = runtime.direct_client()
    +        blocked = asyncio.create_task(
    +            waiter.execute(CommandRequest(b"BLPOP", (b"q", b"0")))
    +        )
    +        await runtime.debug_wait_for_waiters(1)
    +        await producer.execute(CommandRequest(b"RPUSH", (b"q", b"x")))
    +        assert await blocked == Items((Bytes(b"q"), Bytes(b"x")))
    +        await waiter.close()
    +        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(
    +            None
    +        )
    +
    +
    +@pytest.mark.asyncio
    +async def test_push_before_shutdown_completes_and_post_barrier_push_is_closed():
    +    runtime = MiniRedis.open(outbox_drain_grace_ms=0)
    +    await runtime.start()
    +    client = runtime.direct_client()
    +    assert await client.execute(
    +        CommandRequest(b"RPUSH", (b"q", b"x"))
    +    ) == Number(1)
    +    await runtime.close()
    +    assert await client.execute(
    +        CommandRequest(b"RPUSH", (b"q", b"y"))
    +    ) == Failure("CLOSED", "runtime is not accepting commands")
    +
    +
    +@pytest.mark.asyncio
    +async def test_close_clears_all_async_owned_resources():
    +    clock = FakeClock()
    +    scheduler = ManualScheduler(clock)
    +    runtime = MiniRedis.open(
    +        clock=clock,
    +        scheduler=scheduler,
    +        active_expire_interval_ms=100,
    +        outbox_drain_grace_ms=0,
    +    )
    +    await runtime.start()
    +    subscriber = runtime.direct_client()
    +    waiter = runtime.direct_client()
    +    await subscriber.execute(CommandRequest(b"SUBSCRIBE", (b"c",)))
    +    blocked = asyncio.create_task(
    +        waiter.execute(CommandRequest(b"BLPOP", (b"q", b"5")))
    +    )
    +    await runtime.debug_wait_for_waiters(1)
    +    await runtime.close()
    +    assert await blocked == Failure("CLOSED", "runtime closed")
    +    stats = runtime.debug_stats()
    +    assert stats.pending_futures == 0
    +    assert stats.waiters == 0
    +    assert stats.subscriptions == 0
    +    assert stats.sessions == 0
    +    assert stats.timer_handles == 0
    +    assert stats.owned_tasks == 0
    +    assert runtime.debug_waiter_index_counts == (0, 0, 0)
    +    assert scheduler.pending_count == 0
    ```

**测试锁定什么**

它锁定过期 Cancel/Close 无害、Barrier 前命令完成、Barrier 后拒绝，以及 Request、Waiter、Subscription、Session、Timer 与 Task 完整清理。

**如何构造反例**

它让 Push 与 Cancel/Close 竞争，再在 Close 前创建 Subscriber、Waiter 与 Maintenance Timer。

**关键测试语句**

```python
assert stats.pending_futures == 0
assert stats.waiters == 0
assert stats.subscriptions == 0
assert stats.sessions == 0
```

**失败意味着什么**

某个异步 Registry 与 Runtime Lifecycle 使用了不同终态边界或清理顺序。

??? note "文件差异：tests/concurrency/test_shutdown.py"
    ```diff
    diff --git a/tests/concurrency/test_shutdown.py b/tests/concurrency/test_shutdown.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..1b1dd9ad3213ca403c954bccee1bb6a70441ea6b
    --- /dev/null
    +++ b/tests/concurrency/test_shutdown.py
    @@ -0,0 +1,96 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis, RuntimeState
    +from miniredis.core.reply import Failure, Ok
    +
    +
    +class GateProducer:
    +    def __init__(self) -> None:
    +        self.quiesce_started = asyncio.Event()
    +        self.release = asyncio.Event()
    +        self.quiesced = False
    +
    +    async def quiesce(self) -> None:
    +        self.quiesce_started.set()
    +        await self.release.wait()
    +        self.quiesced = True
    +
    +
    +@pytest.mark.asyncio
    +async def test_cancelled_close_caller_does_not_cancel_cleanup():
    +    runtime = MiniRedis.open(outbox_drain_grace_ms=0)
    +    await runtime.start()
    +    producer = GateProducer()
    +    runtime.debug_register_control_producer(producer)
    +    closing = asyncio.create_task(runtime.close())
    +    await producer.quiesce_started.wait()
    +    closing.cancel()
    +    with pytest.raises(asyncio.CancelledError):
    +        await closing
    +    producer.release.set()
    +    await runtime.close()
    +    assert producer.quiesced
    +    assert runtime.closed
    +    assert runtime.debug_stats().owned_tasks == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_shutdown_barrier_bypasses_full_user_admission():
    +    runtime = MiniRedis.open(
    +        max_pending_commands=1,
    +        outbox_drain_grace_ms=0,
    +    )
    +    await runtime.start()
    +    client = runtime.direct_client()
    +    runtime.debug_pause_executor()
    +    accepted = asyncio.create_task(client.execute(CommandRequest(b"PING")))
    +    await runtime.debug_wait_until_queued(1)
    +    closing = asyncio.create_task(runtime.close())
    +    await runtime.debug_wait_for_state(RuntimeState.DRAINING.value)
    +    assert await client.execute(CommandRequest(b"PING")) == Failure(
    +        "CLOSED", "runtime is not accepting commands"
    +    )
    +    runtime.debug_resume_executor()
    +    assert await accepted == Ok(b"PONG")
    +    await closing
    +    assert runtime.debug_stats().pending_futures == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_failed_runtime_terminalizes_infinite_waiter():
    +    runtime = MiniRedis.open(outbox_drain_grace_ms=0)
    +    await runtime.start()
    +    blocked = asyncio.create_task(
    +        runtime.direct_client().execute(
    +            CommandRequest(b"BLPOP", (b"q", b"0"))
    +        )
    +    )
    +    await runtime.debug_wait_for_waiters(1)
    +    runtime._transition_failed("injected worker failure")
    +    assert await blocked == Failure(
    +        "ERR", "runtime failed: injected worker failure"
    +    )
    +    await runtime.close()
    +    assert runtime.debug_stats().pending_futures == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_abrupt_worker_stop_uses_supervisor_fallback_once():
    +    runtime = MiniRedis.open(outbox_drain_grace_ms=0)
    +    await runtime.start()
    +    blocked = asyncio.create_task(
    +        runtime.direct_client().execute(
    +            CommandRequest(b"BLPOP", (b"q", b"0"))
    +        )
    +    )
    +    await runtime.debug_wait_for_waiters(1)
    +    worker = runtime.executor.worker_task
    +    assert worker is not None
    +    worker.cancel()
    +    outcome = await blocked
    +    assert isinstance(outcome, Failure)
    +    assert outcome.code == "ERR"
    +    await runtime.close()
    +    assert runtime.debug_stats().pending_futures == 0
    ```

**测试锁定什么**

它锁定 Shielded 幂等 Close、Producer Quiescence、Shutdown Control 准入、Failure 终结与突然 Worker Stop 后的 Supervisor Fallback。

**如何构造反例**

它取消 Close Waiter，填满并暂停 User Mailbox，注入 Runtime Failure，并直接取消 Executor Worker。

**关键测试语句**

```python
assert runtime.debug_stats().owned_tasks == 0
```

**失败意味着什么**

Shutdown 依赖其调用方，Control Barrier 可被 User 饿饿，或 Worker Failure 留下 Runtime-owned 孤儿资源。

??? note "文件差异：tests/concurrency/test_slow_endpoint.py"
    ```diff
    diff --git a/tests/concurrency/test_slow_endpoint.py b/tests/concurrency/test_slow_endpoint.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..410f9083556603bddb64de3fa12a888b10495b3a
    --- /dev/null
    +++ b/tests/concurrency/test_slow_endpoint.py
    @@ -0,0 +1,24 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.outbound import PubSubMessage, SubscriptionAck
    +from miniredis.core.reply import Number, Ok
    +
    +
    +@pytest.mark.asyncio
    +async def test_full_subscriber_closes_without_blocking_fast_endpoint():
    +    async with MiniRedis.open(outbox_limit=1) as runtime:
    +        slow = runtime.direct_client()
    +        fast = runtime.direct_client()
    +        publisher = runtime.direct_client()
    +        assert await slow.execute(CommandRequest(b"SUBSCRIBE", (b"c",))) is None
    +        assert await fast.execute(CommandRequest(b"SUBSCRIBE", (b"c",))) is None
    +        assert await fast.receive() == SubscriptionAck("subscribe", b"c", 1)
    +
    +        assert await publisher.execute(
    +            CommandRequest(b"PUBLISH", (b"c", b"m"))
    +        ) == Number(1)
    +        assert await fast.receive() == PubSubMessage(b"c", b"m")
    +        await runtime.debug_wait_for_sessions(2)
    +        assert await publisher.execute(CommandRequest(b"PING")) == Ok(b"PONG")
    +        assert runtime.debug_stats().subscriptions == 1
    ```

**测试锁定什么**

它锁定慢 Subscriber 隔离与 Subscription 清理，不阻塞快 Endpoint。

**如何构造反例**

一个 Subscriber 不读容量为一的 Ack，另一个正常 Drain；一次 Publish 只溢出慢 Session。

**关键测试语句**

```python
assert await publisher.execute(CommandRequest(b"PING")) == Ok(b"PONG")
```

**失败意味着什么**

Per-session 压力逃到全局 Executor，或已关闭 Subscriber 仍留在 Delivery 所有权中。

??? note "文件差异：tests/mechanisms/test_pubsub.py"
    ```diff
    diff --git a/tests/mechanisms/test_pubsub.py b/tests/mechanisms/test_pubsub.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..e5ec8c83c54f8812164b9803b9570aa1f93dcdbb
    --- /dev/null
    +++ b/tests/mechanisms/test_pubsub.py
    @@ -0,0 +1,52 @@
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.outbound import (
    +    PubSubMessage,
    +    PubSubPong,
    +    SubscriptionAck,
    +)
    +from miniredis.core.reply import Failure, Number
    +
    +
    +@pytest.mark.asyncio
    +async def test_exact_binary_channel_and_repeated_subscription_count():
    +    async with MiniRedis.open(outbox_limit=8) as runtime:
    +        subscriber = runtime.direct_client()
    +        publisher = runtime.direct_client()
    +        assert (
    +            await subscriber.execute(CommandRequest(b"SUBSCRIBE", (b"a\x00", b"a\x00")))
    +            is None
    +        )
    +        assert await subscriber.receive() == SubscriptionAck("subscribe", b"a\x00", 1)
    +        assert await subscriber.receive() == SubscriptionAck("subscribe", b"a\x00", 1)
    +        assert await publisher.execute(
    +            CommandRequest(b"PUBLISH", (b"a", b"miss"))
    +        ) == Number(0)
    +        before = runtime.debug_commit_seq
    +        assert await publisher.execute(
    +            CommandRequest(b"PUBLISH", (b"a\x00", b"hit"))
    +        ) == Number(1)
    +        assert runtime.debug_commit_seq == before
    +        assert await subscriber.receive() == PubSubMessage(b"a\x00", b"hit")
    +
    +
    +@pytest.mark.asyncio
    +async def test_subscribed_mode_and_unsubscribe_all():
    +    async with MiniRedis.open() as runtime:
    +        subscriber = runtime.direct_client()
    +        await subscriber.execute(CommandRequest(b"SUBSCRIBE", (b"b", b"a")))
    +        await subscriber.receive()
    +        await subscriber.receive()
    +        denied = await subscriber.execute(CommandRequest(b"SET", (b"k", b"v")))
    +        assert denied == Failure(
    +            "ERR",
    +            "only PING, SUBSCRIBE and UNSUBSCRIBE are allowed in subscribed mode",
    +        )
    +        assert await subscriber.execute(CommandRequest(b"PING", (b"x",))) is None
    +        assert await subscriber.receive() == PubSubPong(b"x")
    +        assert await subscriber.execute(CommandRequest(b"UNSUBSCRIBE")) is None
    +        assert await subscriber.receive() == SubscriptionAck("unsubscribe", b"b", 1)
    +        assert await subscriber.receive() == SubscriptionAck("unsubscribe", b"a", 0)
    +        assert await subscriber.execute(CommandRequest(b"UNSUBSCRIBE")) is None
    +        assert await subscriber.receive() == SubscriptionAck("unsubscribe", None, 0)
    ```

**测试锁定什么**

它锁定精确二进制 Channel、重复 Ack、Subscribed-mode 限制、无 Commit Publish Count、PING Push Reply 与 Unsubscribe-all 顺序。

**如何构造反例**

它区分 `b"a"` 与 `b"a\x00"`，重复 Subscription，再在 Subscribed 状态下执行普通命令与空 UNSUBSCRIBE。

**关键测试语句**

```python
assert runtime.debug_commit_seq == before
```

**失败意味着什么**

Pub/Sub 修改了 Database、归一化了二进制身份，或把 Request Reply 与有序 Push 输出混合。

### 基本概念

Pub/Sub 是短暂 Session Output，不是 Database State：PUBLISH 不分配 Commit。双向 Registry 拥有 Channel Membership 与 Session Cleanup。Shutdown 是 Barrier Sequence：停止新 User、静止 Control Producer、终结 Executor-owned State、在上限内 Drain Output、Abort 剩余项、Join Task，并证明 Registry 为空。

### 为什么需要这个机制

Push Delivery 与 Shutdown 共享同一所有权图。没有一个有序 Barrier，Timer 或 Publisher 可在 Cleanup 后入队，慢 Endpoint 可占住全局进度，失败 Worker 可留下未解决无限 Waiter。Supervision 使每个 Producer 与终态路径显式。

### 运行时心智模型

Subscribe/Unsubscribe/Publish 命令像其他请求一样进 Executor，但只更新 Session Registry 与 Outbox。Close 先拒绝新命令并静止 Scheduled Producer。`BeginShutdown` 再经 Control Lane 收束 Waiter、Request、Subscription 与 Notice。Runtime 短暂 Drain Endpoint，Abort 其余项，Join Executor，并清空 Task 所有权。

### 机制板块

#### 类型化 Pub/Sub 命令

在 Parser 边界保留精确二进制 Channel 与显式 Subscribe、Unsubscribe、Publish 意图。

??? note "文件差异：src/miniredis/commands/model.py"
    ```diff
    diff --git a/src/miniredis/commands/model.py b/src/miniredis/commands/model.py
    index 712e25b6718caf452ab7605751a9c8ca8dfadab5..e0d48a4d6d518f5c8f357d869bedfd6a9425367d 100644
    --- a/src/miniredis/commands/model.py
    +++ b/src/miniredis/commands/model.py
    @@ -104,6 +104,22 @@ class BlPop:
         timeout_ms: int


    +@dataclass(frozen=True, slots=True)
    +class Subscribe:
    +    channels: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Unsubscribe:
    +    channels: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Publish:
    +    channel: bytes
    +    payload: bytes
    +
    +
     @dataclass(frozen=True, slots=True)
     class SetAdd:
         key: bytes
    @@ -211,6 +227,9 @@ Command: TypeAlias = (
         | ListPop
         | ListRange
         | BlPop
    +    | Subscribe
    +    | Unsubscribe
    +    | Publish
         | SetAdd
         | SetRemove
         | SetIsMember
    ```

**是什么，为什么现在需要**

类型化 Subscribe、Unsubscribe 与 Publish 值保留精确 Channel Bytes。

**在运行时做什么**

它们让 Executor 拥有 Session-mode 语义，不需 Transport Parse Branch。

**关键代码**

```python
class Publish:
    channel: bytes
    payload: bytes
```

**关键语句理解**

Channel 身份保持 Bytes-exact，不做文本归一化或前缀解释。

??? note "文件差异：src/miniredis/commands/parser.py"
    ```diff
    diff --git a/src/miniredis/commands/parser.py b/src/miniredis/commands/parser.py
    index a812c9ca18d42cbdc2358ba2f9ee8ae218a8cfaa..af71f2f3a5175c76c446a13aa9c967bb33c77091 100644
    --- a/src/miniredis/commands/parser.py
    +++ b/src/miniredis/commands/parser.py
    @@ -29,6 +29,7 @@ from miniredis.commands.model import (
         ListRange,
         Persist,
         Ping,
    +    Publish,
         ScoreBound,
         SetAdd,
         SetIntersection,
    @@ -36,8 +37,10 @@ from miniredis.commands.model import (
         SetMembers,
         SetRemove,
         SetString,
    +    Subscribe,
         TimeToLive,
         TypeOf,
    +    Unsubscribe,
         ZAdd,
         ZRange,
         ZRangeByScore,
    @@ -234,6 +237,15 @@ def parse_request(request: CommandRequest) -> Command:
                     raise CommandParseError("timeout is out of range")
                 milliseconds = int(timeout_ms.to_integral_value(rounding=ROUND_CEILING))
                 return BlPop(tuple(args[:-1]), milliseconds)
    +        case b"SUBSCRIBE":
    +            if not args:
    +                raise CommandParseError("wrong number of arguments")
    +            return Subscribe(tuple(args))
    +        case b"UNSUBSCRIBE":
    +            return Unsubscribe(tuple(args))
    +        case b"PUBLISH":
    +            _require_arity(name, args, 2)
    +            return Publish(args[0], args[1])
             case b"SADD":
                 _require_min_arity(name, args, 2)
                 return SetAdd(args[0], args[1:])
    ```

**是什么，为什么现在需要**

Parser 为每个 Pub/Sub 命令提供显式 Arity 契约。

**在运行时做什么**

它允许空 UNSUBSCRIBE 表示 All，要求 SUBSCRIBE 至少一个 Channel，并冻结 PUBLISH Channel/Payload。

**关键代码**

```python
case b"UNSUBSCRIBE":
    return Unsubscribe(tuple(args))
```

**关键语句理解**

空参数在此是有意义的 Domain Intent，不是通用 Arity Error。

#### 双向 Subscription 所有权

同时按 Session 索引 Channel、按 Channel 索引 Session，使 Publish 与 Close 清理都有界。

??? note "文件差异：src/miniredis/core/pubsub.py"
    ```diff
    diff --git a/src/miniredis/core/pubsub.py b/src/miniredis/core/pubsub.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..cd9877e7d139502f5cc7e71d10e7ff29dea1f9fe
    --- /dev/null
    +++ b/src/miniredis/core/pubsub.py
    @@ -0,0 +1,61 @@
    +from collections import defaultdict
    +from collections.abc import Callable
    +
    +
    +class PubSubRegistry:
    +    def __init__(self, on_debug_change: Callable[[], None]) -> None:
    +        self._channels: dict[bytes, dict[int, None]] = defaultdict(dict)
    +        self._sessions: dict[int, dict[bytes, None]] = defaultdict(dict)
    +        self._on_debug_change = on_debug_change
    +
    +    def count(self, session_id: int) -> int:
    +        return len(self._sessions.get(session_id, ()))
    +
    +    def subscribe(self, session_id: int, channel: bytes) -> int:
    +        self._channels[channel][session_id] = None
    +        self._sessions[session_id][channel] = None
    +        count = self.count(session_id)
    +        self._on_debug_change()
    +        return count
    +
    +    def unsubscribe(self, session_id: int, channel: bytes) -> int:
    +        members = self._channels.get(channel)
    +        if members is not None:
    +            members.pop(session_id, None)
    +            if not members:
    +                del self._channels[channel]
    +        owned = self._sessions.get(session_id)
    +        if owned is not None:
    +            owned.pop(channel, None)
    +            if not owned:
    +                del self._sessions[session_id]
    +        count = self.count(session_id)
    +        self._on_debug_change()
    +        return count
    +
    +    def unsubscribe_targets(
    +        self,
    +        session_id: int,
    +        requested: tuple[bytes, ...],
    +    ) -> tuple[bytes | None, ...]:
    +        if requested:
    +            return requested
    +        current = tuple(self._sessions.get(session_id, ()))
    +        return current if current else (None,)
    +
    +    def subscribers(self, channel: bytes) -> tuple[int, ...]:
    +        return tuple(self._channels.get(channel, ()))
    +
    +    def remove_session(self, session_id: int) -> None:
    +        for channel in tuple(self._sessions.get(session_id, ())):
    +            self.unsubscribe(session_id, channel)
    +        self._on_debug_change()
    +
    +    def clear(self) -> None:
    +        self._channels.clear()
    +        self._sessions.clear()
    +        self._on_debug_change()
    +
    +    @property
    +    def membership_count(self) -> int:
    +        return sum(len(channels) for channels in self._sessions.values())
    ```

**是什么，为什么现在需要**

Registry 拥有双向 Channel/Session Membership。

**在运行时做什么**

它保留 Per-session Subscription 顺序，查找 Publish Target，并在不扫描无关 Session 的情况下移除已关闭 Session 的所有 Channel。

**关键代码**

```python
self._channels: dict[bytes, dict[int, None]] = defaultdict(dict)
self._sessions: dict[int, dict[bytes, None]] = defaultdict(dict)
```

**关键语句理解**

双索引是同一 Owner 下的重复 Lookup Structure，不是重复 Subscription Truth。

#### 串行化 Pub/Sub 与 Shutdown Barrier

按 Executor 顺序发送 Push 输出，并终结 Waiter、Request、Subscription 与 Endpoint。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index f90d1c615c192dd2612da1a499adc121c9d2e503..5c884187bbb88292e1da922578363dfa32a49a9c 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -8,7 +8,15 @@ from dataclasses import dataclass, replace
     from typing import Protocol

     from miniredis.clock import Clock, TimerScheduler
    -from miniredis.commands.model import BlPop, Command, ListPush
    +from miniredis.commands.model import (
    +    BlPop,
    +    Command,
    +    ListPush,
    +    Ping,
    +    Publish,
    +    Subscribe,
    +    Unsubscribe,
    +)
     from miniredis.core.blocking import (
         WaiterId,
         WaiterRegistry,
    @@ -28,15 +36,21 @@ from miniredis.core.expiration import expiry_delete, is_expired
     from miniredis.core.mailbox import EventLoopMailbox
     from miniredis.core.outbound import (
         Abandoned,
    +    PubSubMessage,
    +    PubSubPong,
         ReplyMessage,
         RequestOutcome,
         RequestToken,
         Replied,
         RuntimeClosed,
    +    RuntimeFailed,
    +    ServerClosed,
         SessionEndpoint,
    +    SubscriptionAck,
         TransportClosed,
     )
    -from miniredis.core.reply import Bytes, Failure, Items, Reply
    +from miniredis.core.pubsub import PubSubRegistry
    +from miniredis.core.reply import Bytes, Failure, Items, Number, Reply


     @dataclass(slots=True)
    @@ -70,6 +84,12 @@ class TimeoutWaiter:
         generation: int


    +@dataclass(slots=True)
    +class BeginShutdown:
    +    outcome: RequestOutcome
    +    completion: asyncio.Future[None]
    +
    +
     @dataclass(frozen=True, slots=True)
     class ExecutionPlan:
         reply: Reply | None
    @@ -139,6 +159,7 @@ class CommandExecutor:
             self._on_debug_change = on_debug_change
             self._on_terminal_failure = on_terminal_failure
             self.waiters = WaiterRegistry(self._on_debug_change)
    +        self.pubsub = PubSubRegistry(self._on_debug_change)
             self.scheduler = scheduler

             self._worker_task: asyncio.Task[None] | None = None
    @@ -156,6 +177,7 @@ class CommandExecutor:
             self._handling_message = False
             self._failure: BaseException | None = None
             self._terminal_cleanup_complete = False
    +        self._stop_after_current_message = False
             self._stopping = False
             self._started = False

    @@ -263,6 +285,8 @@ class CommandExecutor:
                         if isinstance(message, _StopExecutor):
                             return
                         await self._dispatch(message)
    +                    if self._stop_after_current_message:
    +                        return
                     finally:
                         self._handling_message = False
                         self._on_debug_change()
    @@ -291,6 +315,8 @@ class CommandExecutor:
                 self._timeout_waiter(message)
             elif isinstance(message, SessionClosed):
                 self._close_session(message)
    +        elif isinstance(message, BeginShutdown):
    +            self._begin_shutdown(message)
             else:
                 raise AssertionError(f"unknown executor message: {message!r}")

    @@ -325,6 +351,7 @@ class CommandExecutor:
                 )
                 if closed is not None:
                     self._finish_request(closed.token, TransportClosed())
    +        self.pubsub.remove_session(event.session_id)
             endpoint = self._endpoints.pop(event.session_id, None)
             if endpoint is not None:
                 endpoint.outbox.abort("session closed")
    @@ -333,6 +360,25 @@ class CommandExecutor:
                 event.completion.set_result(None)
             self._on_debug_change()

    +    def _begin_shutdown(self, event: BeginShutdown) -> None:
    +        self.mailbox.close_control_admission()
    +        for waiter in self.waiters.active():
    +            closed = self.waiters.transition(
    +                waiter.waiter_id,
    +                waiter.generation,
    +                WaiterState.CLOSED,
    +            )
    +            if closed is not None:
    +                self._finish_request(closed.token, event.outcome)
    +        self.pubsub.clear()
    +        for token in tuple(self._requests):
    +            self._finish_request(token, event.outcome)
    +        for endpoint in self._endpoints.values():
    +            endpoint.offer_best_effort(ServerClosed("runtime closed"))
    +        if not event.completion.done():
    +            event.completion.set_result(None)
    +        self._stop_after_current_message = True
    +
         def _complete_terminal_failure(self, failure: BaseException) -> None:
             if self._terminal_cleanup_complete:
                 return
    @@ -342,26 +388,63 @@ class CommandExecutor:
             self.mailbox.close_user_admission()
             self.mailbox.drain()
             for token in tuple(self._requests):
    -            self._finish_request(token, RuntimeClosed())
    +            waiter = self.waiters.for_token(token)
    +            if waiter is not None:
    +                self.waiters.transition(
    +                    waiter.waiter_id,
    +                    waiter.generation,
    +                    WaiterState.CLOSED,
    +                )
    +                self._finish_request(
    +                    token,
    +                    RuntimeFailed(str(failure) or type(failure).__name__),
    +                )
    +            else:
    +                self._finish_request(token, RuntimeClosed())
    +        self.pubsub.clear()
             self.mailbox.close_control_admission()
             self._on_debug_change()
             if self._on_terminal_failure is not None:
                 self._on_terminal_failure(failure)

         async def _execute(self, request: ExecuteRequest) -> None:
    +        command = request.command
    +        if self.pubsub.count(request.session_id) > 0 and not isinstance(
    +            command, (Ping, Subscribe, Unsubscribe)
    +        ):
    +            self._finish_reply(
    +                request.token,
    +                Failure(
    +                    "ERR",
    +                    "only PING, SUBSCRIBE and UNSUBSCRIBE are allowed "
    +                    "in subscribed mode",
    +                ),
    +            )
    +            return
    +        if isinstance(command, Subscribe):
    +            self._subscribe(request, command)
    +            return
    +        if isinstance(command, Unsubscribe):
    +            self._unsubscribe(request, command)
    +            return
    +        if isinstance(command, Publish):
    +            self._publish(request, command)
    +            return
    +        if isinstance(command, Ping) and self.pubsub.count(request.session_id) > 0:
    +            self._subscribed_ping(request, command)
    +            return
    +
             now_ms = self.clock.now_ms()
    -        if isinstance(request.command, BlPop):
    -            plan = self.planner.plan_blpop_now(request.command, self.database, now_ms)
    +        if isinstance(command, BlPop):
    +            plan = self.planner.plan_blpop_now(command, self.database, now_ms)
                 if plan is None:
                     deadline = (
    -                    None
    -                    if request.command.timeout_ms == 0
    -                    else now_ms + request.command.timeout_ms
    +                    None if command.timeout_ms == 0 else now_ms + command.timeout_ms
                     )
                     waiter = self.waiters.register(
                         request.token,
                         request.session_id,
    -                    request.command.keys,
    +                    command.keys,
                         deadline,
                     )
                     if waiter.deadline_ms is not None:
    @@ -377,10 +460,53 @@ class CommandExecutor:
                         self._on_debug_change()
                     return
             else:
    -            plan = self.planner.plan(request.command, self.database, now_ms)
    -            plan = self._attach_push_wakeups(request.command, plan)
    +            plan = self.planner.plan(command, self.database, now_ms)
    +            plan = self._attach_push_wakeups(command, plan)
             await self._apply_plan(request, plan, now_ms)

    +    def _subscribe(self, request: ExecuteRequest, command: Subscribe) -> None:
    +        endpoint = self._endpoints[request.session_id]
    +        for channel in command.channels:
    +            count = self.pubsub.subscribe(request.session_id, channel)
    +            if not endpoint.offer(SubscriptionAck("subscribe", channel, count)):
    +                self._finish_request(request.token, TransportClosed())
    +                return
    +        self._finish_request(request.token, Replied(None))
    +
    +    def _unsubscribe(self, request: ExecuteRequest, command: Unsubscribe) -> None:
    +        endpoint = self._endpoints[request.session_id]
    +        for channel in self.pubsub.unsubscribe_targets(
    +            request.session_id,
    +            command.channels,
    +        ):
    +            count = (
    +                self.pubsub.count(request.session_id)
    +                if channel is None
    +                else self.pubsub.unsubscribe(request.session_id, channel)
    +            )
    +            if not endpoint.offer(SubscriptionAck("unsubscribe", channel, count)):
    +                self._finish_request(request.token, TransportClosed())
    +                return
    +        self._finish_request(request.token, Replied(None))
    +
    +    def _publish(self, request: ExecuteRequest, command: Publish) -> None:
    +        delivered = 0
    +        for session_id in self.pubsub.subscribers(command.channel):
    +            endpoint = self._endpoints.get(session_id)
    +            if endpoint is not None and endpoint.offer(
    +                PubSubMessage(command.channel, command.payload)
    +            ):
    +                delivered += 1
    +        self._finish_reply(request.token, Number(delivered))
    +
    +    def _subscribed_ping(self, request: ExecuteRequest, command: Ping) -> None:
    +        payload = b"" if command.message is None else command.message
    +        endpoint = self._endpoints[request.session_id]
    +        if endpoint.offer(PubSubPong(payload)):
    +            self._finish_request(request.token, Replied(None))
    +        else:
    +            self._finish_request(request.token, TransportClosed())
    +
         def _attach_push_wakeups(
             self,
             command: Command,
    @@ -564,6 +690,42 @@ class CommandExecutor:
         def endpoint_count(self) -> int:
             return len(self._endpoints)

    +    @property
    +    def worker_task(self) -> asyncio.Task[None] | None:
    +        return self._worker_task
    +
    +    @property
    +    def worker_done(self) -> bool:
    +        return self._worker_task is None or self._worker_task.done()
    +
    +    async def join(self) -> None:
    +        if self._worker_task is not None:
    +            await asyncio.gather(self._worker_task, return_exceptions=True)
    +
    +    def fallback_terminalize(self, outcome: RequestOutcome) -> None:
    +        if not self.worker_done:
    +            raise RuntimeError(
    +                "fallback terminalization requires a stopped worker"
    +            )
    +        self.mailbox.close_control_admission()
    +        for waiter in self.waiters.active():
    +            closed = self.waiters.transition(
    +                waiter.waiter_id,
    +                waiter.generation,
    +                WaiterState.CLOSED,
    +            )
    +            if closed is not None:
    +                self._finish_request(closed.token, outcome)
    +        self.pubsub.clear()
    +        for token in tuple(self._requests):
    +            self._finish_request(token, outcome)
    +        for endpoint in self._endpoints.values():
    +            endpoint.outbox.abort("runtime stopped")
    +
    +    def release_endpoints(self) -> None:
    +        self._endpoints.clear()
    +        self._on_debug_change()
    +
         @property
         def debug_failure(self) -> BaseException | None:
             return self._failure
    ```

**是什么，为什么现在需要**

Executor 集成 Subscribed-mode 命令与终态 Shutdown Control。

**在运行时做什么**

它 Offer 有序 Ack/Message，移除慢 Session，并在一个 Barrier 关闭 Waiter、Request、Subscription 与 Endpoint。

**关键代码**

```python
self.pubsub.clear()
for token in tuple(self._requests):
    self._finish_request(token, event.outcome)
for endpoint in self._endpoints.values():
    endpoint.offer_best_effort(ServerClosed("runtime closed"))
```

**关键语句理解**

终结发生在 Executor 仍拥有每个 Registry 时，且早于 Control Lane 永久关闭。

#### 受监督维护与关闭

静止 Control Producer，用 Shutdown Control 绕过已满 User 准入，短暂排空 Endpoint，并释放每个 Owned Task。

??? note "文件差异：src/miniredis/config.py"
    ```diff
    diff --git a/src/miniredis/config.py b/src/miniredis/config.py
    index 275f68be1c8a2109f6dbdd54fe0b7e3f3afd0d9f..09c31aa8115c72d0213d6ddfa28bbb2b7f81ca77 100644
    --- a/src/miniredis/config.py
    +++ b/src/miniredis/config.py
    @@ -13,6 +13,8 @@ class MiniRedisConfig:
         maxmemory: int | None = None
         eviction_policy: EvictionPolicy = "noeviction"
         outbox_limit: int = 64
    +    outbox_drain_grace_ms: int = 100
    +    active_expire_interval_ms: int = 100

         def __post_init__(self) -> None:
             if self.max_pending_commands <= 0:
    @@ -25,3 +27,7 @@ class MiniRedisConfig:
                 raise ValueError("eviction_policy must be 'noeviction' or 'allkeys-lru'")
             if self.outbox_limit <= 0:
                 raise ValueError("outbox_limit must be positive")
    +        if self.outbox_drain_grace_ms < 0:
    +            raise ValueError("outbox_drain_grace_ms cannot be negative")
    +        if self.active_expire_interval_ms <= 0:
    +            raise ValueError("active_expire_interval_ms must be positive")
    ```

**是什么，为什么现在需要**

Config 增加有界 Outbox Drain Grace 与 Active-expiry Interval。

**在运行时做什么**

它使 Shutdown Latency 与 Maintenance Cadence 显式且可校验。

**关键代码**

```python
if self.active_expire_interval_ms <= 0:
    raise ValueError("active_expire_interval_ms must be positive")
```

**关键语句理解**

Active Producer 需要正 Cadence；Graceful Drain 可故意为零以立即 Teardown。

??? note "文件差异：src/miniredis/core/expiration.py"
    ```diff
    diff --git a/src/miniredis/core/expiration.py b/src/miniredis/core/expiration.py
    index 2595dcc4c0099fc860e11de6d8903ea0ba587bfd..67c746aca7d520a88f5fb6c10bab91cc299a2a6e 100644
    --- a/src/miniredis/core/expiration.py
    +++ b/src/miniredis/core/expiration.py
    @@ -1,3 +1,6 @@
    +from collections.abc import Callable
    +
    +from miniredis.clock import Clock, ScheduledHandle, TimerScheduler
     from miniredis.core.commit import DeleteKey, DeleteReason
     from miniredis.core.database import Entry

    @@ -8,3 +11,44 @@ def is_expired(entry: Entry, now_ms: int) -> bool:

     def expiry_delete(key: bytes) -> DeleteKey:
         return DeleteKey(key, DeleteReason.EXPIRED)
    +
    +
    +class ActiveExpireProducer:
    +    def __init__(
    +        self,
    +        clock: Clock,
    +        scheduler: TimerScheduler,
    +        interval_ms: int,
    +        post_control: Callable[[object], bool],
    +        tick_factory: Callable[[int], object],
    +    ) -> None:
    +        self._clock = clock
    +        self._scheduler = scheduler
    +        self._interval_ms = interval_ms
    +        self._post_control = post_control
    +        self._tick_factory = tick_factory
    +        self._running = False
    +        self._handle: ScheduledHandle | None = None
    +
    +    def start(self) -> None:
    +        if self._running:
    +            return
    +        self._running = True
    +        self._schedule_next()
    +
    +    def _schedule_next(self) -> None:
    +        deadline = self._clock.now_ms() + self._interval_ms
    +        self._handle = self._scheduler.call_at_ms(deadline, self._fire)
    +
    +    def _fire(self) -> None:
    +        if not self._running:
    +            return
    +        self._post_control(self._tick_factory(self._clock.now_ms()))
    +        if self._running:
    +            self._schedule_next()
    +
    +    async def quiesce(self) -> None:
    +        self._running = False
    +        if self._handle is not None:
    +            self._handle.cancel()
    +            self._handle = None
    ```

**是什么，为什么现在需要**

Active Expiry 获得 Lifecycle-owned Periodic Control Producer。

**在运行时做什么**

运行时它只调度下一 Tick，Quiescence 时取消 Outstanding Handle。

**关键代码**

```python
async def quiesce(self) -> None:
    self._running = False
    if self._handle is not None:
        self._handle.cancel()
```

**关键语句理解**

Quiescence 在 Shutdown 关 Control Admission 前移除 Source，防止 Barrier 后 Tick。

??? note "文件差异：src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 0e944f527c57972b9bc745caef90d46fcaa1709e..e741b2d63038aec217503fcc05bea43bec434365 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -22,12 +22,21 @@ from miniredis.core.blocking import WaiterId
     from miniredis.core.commit import CommitBatch, StoredEntry
     from miniredis.core.database import Database
     from miniredis.core.executor import (
    +    ActiveExpireTick,
    +    BeginShutdown,
         CommandExecutor,
         CommitBarrier,
         NullCommitBarrier,
         SessionClosed,
     )
    -from miniredis.core.outbound import RequestToken, SessionEndpoint
    +from miniredis.core.expiration import ActiveExpireProducer
    +from miniredis.core.outbound import (
    +    RequestOutcome,
    +    RequestToken,
    +    RuntimeClosed,
    +    RuntimeFailed,
    +    SessionEndpoint,
    +)
     from miniredis.core.planner import CommandPlanner
     from miniredis.core.reply import Failure

    @@ -37,6 +46,7 @@ class RuntimeState(str, Enum):
         RUNNING = "running"
         DRAINING = "draining"
         CLOSED = "closed"
    +    FAILED = "failed"


     @dataclass(frozen=True, slots=True)
    @@ -86,7 +96,12 @@ class MiniRedis:
             self.state = RuntimeState.STARTING
             self._session_ids = itertools.count(1)
             self._start_task: asyncio.Task[None] | None = None
    -        self._close_task: asyncio.Task[None] | None = None
    +        self._lifecycle_lock = asyncio.Lock()
    +        self._shutdown_task: asyncio.Task[None] | None = None
    +        self._control_producers: set[object] = set()
    +        self._owned_tasks: set[asyncio.Task[object]] = set()
    +        self._failure_reason: str | None = None
    +        self._shutdown_complete = False

         @classmethod
         def open(
    @@ -131,7 +146,11 @@ class MiniRedis:
         async def start(self) -> None:
             if self.state is RuntimeState.RUNNING:
                 return
    -        if self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}:
    +        if self.state in {
    +            RuntimeState.DRAINING,
    +            RuntimeState.CLOSED,
    +            RuntimeState.FAILED,
    +        }:
                 raise RuntimeError("runtime is closed")
             if self._start_task is None:
                 self._start_task = asyncio.create_task(
    @@ -141,8 +160,23 @@ class MiniRedis:

         async def _start_once(self) -> None:
             await self.executor.start()
    -        if self.state is RuntimeState.STARTING:
    -            self.state = RuntimeState.RUNNING
    +        if self.state is not RuntimeState.STARTING:
    +            return
    +        worker = self.executor.worker_task
    +        if worker is None:
    +            raise RuntimeError("executor did not create its worker")
    +        self._track_owned_task(worker)
    +        worker.add_done_callback(self._executor_stopped)
    +        producer = ActiveExpireProducer(
    +            self.clock,
    +            self.scheduler,
    +            self.config.active_expire_interval_ms,
    +            self.executor.post_control,
    +            lambda now_ms: ActiveExpireTick(now_ms, None),
    +        )
    +        self._control_producers.add(producer)
    +        producer.start()
    +        self._set_state(RuntimeState.RUNNING)

         def parse(self, request: CommandRequest) -> Command | Failure:
             try:
    @@ -151,7 +185,11 @@ class MiniRedis:
                 return Failure("ERR", str(error))

         def direct_client(self) -> DirectClient:
    -        if self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}:
    +        if self.state in {
    +            RuntimeState.DRAINING,
    +            RuntimeState.CLOSED,
    +            RuntimeState.FAILED,
    +        }:
                 raise RuntimeError("runtime is closed")
             session_id = next(self._session_ids)
             endpoint = SessionEndpoint(
    @@ -164,26 +202,132 @@ class MiniRedis:
             self.executor.register_endpoint(endpoint)
             return DirectClient(self, endpoint)

    -    def _session_became_slow(self, session_id: int, _reason: str) -> None:
    +    def _session_became_slow(self, session_id: int, reason: str) -> None:
    +        endpoint = self.executor.endpoint(session_id)
    +        if endpoint is not None:
    +            endpoint.request_transport_close(reason)
             self.executor.post_control(SessionClosed(session_id))

         async def close(self) -> None:
    -        if self._close_task is None:
    -            self._close_task = asyncio.create_task(
    -                self._close(), name="miniredis:runtime-close"
    +        async with self._lifecycle_lock:
    +            if self._shutdown_task is None:
    +                self._shutdown_task = asyncio.create_task(
    +                    self._shutdown_once(),
    +                    name="miniredis:shutdown",
    +                )
    +                self._track_owned_task(self._shutdown_task)
    +            task = self._shutdown_task
    +        await asyncio.shield(task)
    +
    +    async def _shutdown_once(self, crash: bool = False) -> None:
    +        del crash
    +        if self._shutdown_complete:
    +            return
    +        failure = self._failure_reason
    +        if self.state is not RuntimeState.CLOSED:
    +            self._set_state(RuntimeState.DRAINING)
    +        self.executor.mailbox.close_user_admission()
    +        await asyncio.gather(
    +            *(
    +                producer.quiesce()  # type: ignore[attr-defined]
    +                for producer in tuple(self._control_producers)
                 )
    -        await asyncio.shield(self._close_task)
    +        )
    +        outcome: RequestOutcome = (
    +            RuntimeFailed(failure) if failure is not None else RuntimeClosed()
    +        )
    +        if self.executor.worker_done:
    +            self.executor.fallback_terminalize(outcome)
    +        else:
    +            completion = asyncio.get_running_loop().create_future()
    +            if self.executor.post_control(BeginShutdown(outcome, completion)):
    +                worker = self.executor.worker_task
    +                assert worker is not None
    +                await asyncio.wait(
    +                    (completion, worker),
    +                    return_when=asyncio.FIRST_COMPLETED,
    +                )
    +                if not completion.done():
    +                    self.executor.fallback_terminalize(outcome)
    +            else:
    +                await self.executor.join()
    +                self.executor.fallback_terminalize(outcome)
    +
    +        endpoints = self.executor.endpoints()
    +        for endpoint in endpoints:
    +            endpoint.outbox.begin_close("runtime closed")
    +        drainers = [endpoint.outbox.wait_empty() for endpoint in endpoints]
    +        if drainers:
    +            try:
    +                async with asyncio.timeout(
    +                    self.config.outbox_drain_grace_ms / 1000
    +                ):
    +                    await asyncio.gather(*drainers)
    +            except TimeoutError:
    +                pass
    +        for endpoint in endpoints:
    +            endpoint.outbox.abort("runtime closed")
    +            endpoint.request_transport_close("runtime closed")
    +        self.executor.release_endpoints()
    +        await self.executor.join()
    +        self._control_producers.clear()
    +        current = asyncio.current_task()
    +        for owned in tuple(self._owned_tasks):
    +            if owned.done() or owned is current:
    +                self._owned_tasks.discard(owned)
    +        self._shutdown_complete = True
    +        self._set_state(RuntimeState.CLOSED)

    -    async def _close(self) -> None:
    -        if self.state is RuntimeState.CLOSED:
    +    def _on_executor_terminal_failure(self, failure: BaseException) -> None:
    +        reason = str(failure) or type(failure).__name__
    +        self._failure_reason = reason
    +        self._set_state(RuntimeState.CLOSED)
    +        self.executor.mailbox.close_user_admission()
    +        if self._shutdown_task is None:
    +            self._shutdown_task = asyncio.create_task(
    +                self._shutdown_once(),
    +                name="miniredis:failed-shutdown",
    +            )
    +            self._track_owned_task(self._shutdown_task)
    +
    +    def _executor_stopped(self, task: asyncio.Task[None]) -> None:
    +        if self.state in {
    +            RuntimeState.DRAINING,
    +            RuntimeState.CLOSED,
    +            RuntimeState.FAILED,
    +        }:
                 return
    -        self.state = RuntimeState.DRAINING
    -        await self.executor.close()
    -        self.state = RuntimeState.CLOSED
    +        if task.cancelled():
    +            reason = "executor worker cancelled"
    +        else:
    +            error = task.exception()
    +            reason = (
    +                "executor worker stopped"
    +                if error is None
    +                else f"executor worker failed: {error}"
    +            )
    +        self._transition_failed(reason)

    -    def _on_executor_terminal_failure(self, failure: BaseException) -> None:
    -        del failure
    -        self.state = RuntimeState.CLOSED
    +    def _transition_failed(self, reason: str) -> None:
    +        if self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}:
    +            return
    +        self._failure_reason = reason
    +        self._set_state(RuntimeState.FAILED)
    +        self.executor.mailbox.close_user_admission()
    +        if self._shutdown_task is None:
    +            self._shutdown_task = asyncio.create_task(
    +                self._shutdown_once(),
    +                name="miniredis:failed-shutdown",
    +            )
    +            self._track_owned_task(self._shutdown_task)
    +
    +    def _set_state(self, state: RuntimeState) -> None:
    +        self.state = state
    +        self._debug_notify()
    +
    +    def _track_owned_task(self, task: asyncio.Task[object]) -> None:
    +        self._owned_tasks.add(task)
    +        task.add_done_callback(self._owned_tasks.discard)

         async def __aenter__(self) -> Self:
             await self.start()
    @@ -200,6 +344,24 @@ class MiniRedis:
         def debug_physical_key_count(self) -> int:
             return len(self.database.entries)

    +    @property
    +    def closed(self) -> bool:
    +        return self.state is RuntimeState.CLOSED
    +
    +    @property
    +    def accepting_commands(self) -> bool:
    +        return (
    +            self.state is RuntimeState.RUNNING
    +            and self.executor.mailbox.accepting_users
    +        )
    +
    +    @property
    +    def normal_shutdown_started(self) -> bool:
    +        return (
    +            self._failure_reason is None
    +            and self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}
    +        )
    +
         async def debug_active_expire_once(self) -> int:
             if self.state is not RuntimeState.RUNNING:
                 return 0
    @@ -229,10 +391,14 @@ class MiniRedis:
                 accepted_requests=self.executor.accepted_request_count,
                 pending_futures=self.executor.pending_request_count,
                 waiters=self.executor.waiters.active_count,
    -            subscriptions=0,
    +            subscriptions=self.executor.pubsub.membership_count,
                 sessions=self.executor.endpoint_count,
                 timer_handles=self.executor.waiters.timer_count,
    -            owned_tasks=0,
    +            owned_tasks=sum(
    +                not task.done()
    +                for task in self._owned_tasks
    +                if task is not asyncio.current_task()
    +            ),
             )

         def _debug_notify(self) -> None:
    @@ -257,6 +423,15 @@ class MiniRedis:
         async def debug_wait_for_waiters(self, count: int) -> None:
             await self._debug_wait(lambda: self.executor.waiters.active_count == count)

    +    async def debug_wait_for_state(self, value: str) -> None:
    +        await self._debug_wait(lambda: self.state.value == value)
    +
    +    def debug_register_control_producer(self, producer: object) -> None:
    +        if self.state is not RuntimeState.RUNNING:
    +            raise RuntimeError("control producers register only while running")
    +        self._control_producers.add(producer)
    +        self._debug_notify()
    +
         def debug_waiter_ids(self, key: bytes) -> tuple[WaiterId, ...]:
             return self.executor.waiters.ids_for_key(key)

    ```

**是什么，为什么现在需要**

Runtime 成为 Producer、Executor、Endpoint、Shutdown Task 与 Failure Fallback 的 Supervisor。

**在运行时做什么**

它 Shield 一个幂等 Shutdown Task，静止 Producer，发 Barrier，设界 Drain Outbox，Join 所有权，并暴露最终证据。

**关键代码**

```python
self.executor.mailbox.close_user_admission()
await asyncio.gather(
    *(
        producer.quiesce()  # type: ignore[attr-defined]
        for producer in tuple(self._control_producers)
    )
)
```

**关键语句理解**

新 User Work 在 Producer Quiesce 前停止；Control Admission 保持足够长以交付 Shutdown Barrier。

#### 幂等 Direct Session 关闭

把 Client Close 变成 Shielded Executor Control，并在不绕过 Session 所有权的前提下映射生命周期结果。

??? note "文件差异：src/miniredis/adapters/direct.py"
    ```diff
    diff --git a/src/miniredis/adapters/direct.py b/src/miniredis/adapters/direct.py
    index 49d7f1b05278bccb390f370d35959b07f5d850ee..cb5930cca661407b0f0cd4d50f699760fd38c388 100644
    --- a/src/miniredis/adapters/direct.py
    +++ b/src/miniredis/adapters/direct.py
    @@ -43,6 +43,10 @@ class DirectClient:
         async def execute(self, request: CommandRequest) -> Reply | None:
             if self._closed:
                 return Failure("CLOSED", "client is closed")
    +        if not self._runtime.accepting_commands:
    +            if self._runtime.normal_shutdown_started:
    +                return Failure("CLOSED", "runtime is not accepting commands")
    +            return Failure("CLOSED", "runtime is closed")
             parsed = self._runtime.parse(request)
             if isinstance(parsed, Failure):
                 return parsed
    @@ -63,6 +67,8 @@ class DirectClient:
             match outcome:
                 case Replied(reply=reply):
                     return reply
    +            case RuntimeClosed() if self._runtime.normal_shutdown_started:
    +                return Failure("CLOSED", "runtime closed")
                 case RuntimeClosed():
                     return Failure("CLOSED", "runtime closed before reply")
                 case TransportClosed() if isinstance(parsed, BlPop):
    ```

**是什么，为什么现在需要**

Direct-client Close 变成幂等、Shielded 且 Executor-owned。

**在运行时做什么**

它发送带 Completion Evidence 的 `SessionClosed`，并把 Runtime 与 Session 终态映射为公开行为。

**关键代码**

```python
await asyncio.shield(self._close_task)
```

**关键语句理解**

取消一个 Close Caller 不能取消其启动的 Session Cleanup Task。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/12-pubsub-and-shutdown/tests.txt)`。它覆盖 Pub/Sub 语义、慢 Session 隔离、Shutdown 准入、Worker Failure、竞态过期性与最终零资源不变量。

### 需要真正记住的内容

Pub/Sub 是 Session Output 而非 Database Commit State；在一个 Owner 下双向索引 Membership；隔离慢 Endpoint；关 Control 前停 Producer；Shield 幂等 Cleanup；终结每个 Registry；验证零 Owned Resource。

### 用自己的话讲清楚

排序命令的同一所有权设计也排序 Push 与 Shutdown。Pub/Sub 在 Executor 内改 Session Registry 与 Outbox。Close 先停新 Source，再把一个 Barrier 发进该 Owner，有界 Drain Output，并证明没有 Future、Waiter、Subscription、Session、Timer 或 Task 存活。

### 教材

[第 9 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/09-blocking-pubsub-transactions.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/bb842dd...6ff1e5f)

完成后可运行 `python -m journey.tools.build_journey check 12` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/12-pubsub-and-shutdown/stage.patch)

# Stage 10 · 有序 Outbox 与慢 Session

### 目标

为每个 Session 提供一个有界有序输出通道，并明确优雅关闭与溢出行为。

??? note "交付文件"
    - `src/miniredis/adapters/direct.py`
    - `src/miniredis/config.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/outbound.py`
    - `src/miniredis/runtime.py`
    - `tests/unit/core/test_outbound.py`

### 当前遇到的问题

Request Future 可返回直接 Reply，但后续 Pub/Sub 与 TCP 需要同一 Per-session 顺序中的非请求输出。无界队列会让一个慢 Consumer 耗尽内存；基于 Sentinel 的关闭可能挤掉已接受输出，或需预留容量。

### 测试契约

#### 先看会坏在哪里

优雅关闭必须在报告关闭前排空两条已接受消息。溢出必须丢弃待发输出、只请求一次 Transport 关闭，并且不让 Best-effort Notice 挤掉已接受数据。

??? note "文件差异：tests/unit/core/test_outbound.py"
    ```diff
    diff --git a/tests/unit/core/test_outbound.py b/tests/unit/core/test_outbound.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..b393305446934307b4736609c50df7be06af8e23
    --- /dev/null
    +++ b/tests/unit/core/test_outbound.py
    @@ -0,0 +1,48 @@
    +import pytest
    +
    +from miniredis.core.outbound import (
    +    CloseAwareOutbox,
    +    OutboxClosed,
    +    PubSubMessage,
    +)
    +
    +
    +@pytest.mark.asyncio
    +async def test_graceful_close_drains_without_a_sentinel():
    +    outbox = CloseAwareOutbox(capacity=2)
    +    first = PubSubMessage(b"c", b"1")
    +    second = PubSubMessage(b"c", b"2")
    +    assert outbox.offer(first)
    +    assert outbox.offer(second)
    +    outbox.begin_close("runtime closed")
    +    assert await outbox.receive() == first
    +    assert await outbox.receive() == second
    +    with pytest.raises(OutboxClosed, match="runtime closed"):
    +        await outbox.receive()
    +    assert outbox.pending_count == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_full_outbox_discards_pending_output_and_closes_once():
    +    overflows = 0
    +
    +    def overflow() -> None:
    +        nonlocal overflows
    +        overflows += 1
    +
    +    outbox = CloseAwareOutbox(capacity=1, on_overflow=overflow)
    +    assert outbox.offer(PubSubMessage(b"c", b"1"))
    +    assert not outbox.offer(PubSubMessage(b"c", b"2"))
    +    assert outbox.closed
    +    assert outbox.pending_count == 0
    +    assert overflows == 1
    +    assert not outbox.offer(PubSubMessage(b"c", b"3"))
    +    assert overflows == 1
    +
    +
    +def test_best_effort_notice_never_displaces_accepted_output():
    +    outbox = CloseAwareOutbox(capacity=1)
    +    assert outbox.offer(PubSubMessage(b"c", b"accepted"))
    +    assert not outbox.offer_best_effort(PubSubMessage(b"c", b"notice"))
    +    assert not outbox.closed
    +    assert outbox.pending_count == 1
    ```

**测试锁定什么**

它锁定 FIFO 排空、无 Sentinel 关闭、破坏性溢出、单次溢出通知与不挤占的 Best-effort 输出。

**如何构造反例**

它填满容量为一或二的小队列，在 Receive 前关闭，再尝试额外的普通或 Best-effort Offer。

**关键测试语句**

```python
assert outbox.pending_count == 0
assert overflows == 1
```

**失败意味着什么**

已接受顺序丢失，慢 Session 清理重复，或 Lifecycle 输出占用了承诺给用户可见消息的容量。

### 基本概念

Outbox 是 Runtime 到单个 Session 的唯一有序流。`begin_close` 是优雅关闭：停止接收，但排空已缓冲项。`abort` 是破坏性关闭：丢弃待发项并唤醒 Receiver。溢出是 Transport 失败，不是对全局 Executor 施加背压。

### 为什么需要这个机制

Reply 与未来非请求消息必须共享 Transport 顺序。为每个 Session 设界可隔离慢 Consumer；显式 Close State 则避免 Outbound Type 内的魔法值，并给 Shutdown 一个精确 Drain 契约。

### 运行时心智模型

Runtime 分配单调 Session ID 并注册一个 `SessionEndpoint`。Executor 拥有 Registry，只通过 Endpoint Offer 输出。Receiver 按 FIFO Pop。容量耗尽时，Outbox Abort、只请求一次 Transport 关闭，并发送 `SessionClosed`，使该 Session 所有已接受请求以 `TransportClosed` 收束。

### 机制板块

#### Session-backed Direct Client

把每个 Direct Client 绑定到已注册 Endpoint，使 Reply、非请求输出与关闭共用一个 Session 身份。

??? note "文件差异：src/miniredis/adapters/direct.py"
    ```diff
    diff --git a/src/miniredis/adapters/direct.py b/src/miniredis/adapters/direct.py
    index e85e99af86a596b1d506d624f4e7d980e6e2eb71..caf1b93dd8d56e19891d70c9c3150fe542b5cc66 100644
    --- a/src/miniredis/adapters/direct.py
    +++ b/src/miniredis/adapters/direct.py
    @@ -7,9 +7,11 @@ from miniredis.commands.request import CommandRequest
     from miniredis.core.executor import AbandonRequest, SubmittedRequest
     from miniredis.core.outbound import (
         Abandoned,
    +    Outbound,
         Replied,
         RuntimeClosed,
         RuntimeFailed,
    +    SessionEndpoint,
         TransportClosed,
     )
     from miniredis.core.reply import Failure, Reply
    @@ -19,10 +21,15 @@ if TYPE_CHECKING:


     class DirectClient:
    -    def __init__(self, runtime: MiniRedis, session_id: int) -> None:
    +    def __init__(self, runtime: MiniRedis, endpoint: SessionEndpoint) -> None:
             self._runtime = runtime
    -        self.session_id = session_id
    +        self.endpoint = endpoint
             self._closed = False
    +        self._close_task: asyncio.Task[None] | None = None
    +
    +    @property
    +    def session_id(self) -> int:
    +        return self.endpoint.session_id

         @property
         def closed(self) -> bool:
    @@ -61,8 +68,8 @@ class DirectClient:
                     return Failure("ERR", "request abandoned")
             raise AssertionError(f"unknown request outcome: {outcome!r}")

    -    async def receive(self) -> Reply:
    -        raise NotImplementedError("DirectClient.receive is unavailable in Phase 1")
    +    async def receive(self) -> Outbound:
    +        return await self.endpoint.receive()

         async def close(self) -> None:
             self._closed = True
    ```

**是什么，为什么现在需要**

Direct Client 现在携带已注册 Endpoint，而不只是数字 Session ID。

**在运行时做什么**

普通 Execute 仍等 Request Outcome，`receive` 则消费未来 Push-style 输出也会使用的同一 Endpoint Stream。

**关键代码**

```python
async def receive(self) -> Outbound:
    return await self.endpoint.receive()
```

**关键语句理解**

Adapter 不创建第二个 Queue，也不重排输出，只委托给 Session-owned Stream。

#### 有界 Close-aware Outbox

保留已接受输出顺序，无 Sentinel 排空优雅关闭，并在溢出时中止慢 Session。

??? note "文件差异：src/miniredis/core/outbound.py"
    ```diff
    diff --git a/src/miniredis/core/outbound.py b/src/miniredis/core/outbound.py
    index fffad96fd7ac983d633e7ca35704be4abb536143..c45767b0fb2e22a8c4195fb41031d7c883d6a2c9 100644
    --- a/src/miniredis/core/outbound.py
    +++ b/src/miniredis/core/outbound.py
    @@ -1,5 +1,8 @@
     from __future__ import annotations

    +import asyncio
    +from collections import deque
    +from collections.abc import Callable
     from dataclasses import dataclass
     from typing import TypeAlias

    @@ -39,3 +42,163 @@ class RuntimeFailed:
     RequestOutcome: TypeAlias = (
         Replied | Abandoned | TransportClosed | RuntimeClosed | RuntimeFailed
     )
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ReplyMessage:
    +    request_id: RequestToken
    +    reply: Reply
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SubscriptionAck:
    +    kind: str
    +    channel: bytes | None
    +    subscription_count: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class PubSubMessage:
    +    channel: bytes
    +    payload: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class PubSubPong:
    +    payload: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ServerClosed:
    +    reason: str
    +
    +
    +Outbound: TypeAlias = (
    +    ReplyMessage | SubscriptionAck | PubSubMessage | PubSubPong | ServerClosed
    +)
    +
    +
    +class OutboxClosed(RuntimeError):
    +    pass
    +
    +
    +class CloseAwareOutbox:
    +    def __init__(
    +        self,
    +        capacity: int,
    +        on_overflow: Callable[[], None] | None = None,
    +    ) -> None:
    +        if capacity <= 0:
    +            raise ValueError("outbox capacity must be positive")
    +        self._capacity = capacity
    +        self._items: deque[Outbound] = deque()
    +        self._changed = asyncio.Event()
    +        self._empty = asyncio.Event()
    +        self._empty.set()
    +        self._closed = False
    +        self._reason = ""
    +        self._overflow_notified = False
    +        self._on_overflow = on_overflow
    +
    +    @property
    +    def closed(self) -> bool:
    +        return self._closed
    +
    +    @property
    +    def close_reason(self) -> str:
    +        return self._reason
    +
    +    @property
    +    def pending_count(self) -> int:
    +        return len(self._items)
    +
    +    def offer(self, item: Outbound) -> bool:
    +        if self._closed:
    +            return False
    +        if len(self._items) == self._capacity:
    +            self.abort("outbox full")
    +            if not self._overflow_notified:
    +                self._overflow_notified = True
    +                if self._on_overflow is not None:
    +                    self._on_overflow()
    +            return False
    +        self._items.append(item)
    +        self._empty.clear()
    +        self._changed.set()
    +        return True
    +
    +    def offer_best_effort(self, item: Outbound) -> bool:
    +        if self._closed or len(self._items) == self._capacity:
    +            return False
    +        self._items.append(item)
    +        self._empty.clear()
    +        self._changed.set()
    +        return True
    +
    +    async def receive(self) -> Outbound:
    +        while True:
    +            if self._items:
    +                item = self._items.popleft()
    +                if not self._items:
    +                    self._empty.set()
    +                return item
    +            if self._closed:
    +                raise OutboxClosed(self._reason)
    +            self._changed.clear()
    +            if self._items or self._closed:
    +                continue
    +            await self._changed.wait()
    +
    +    def begin_close(self, reason: str) -> None:
    +        if self._closed:
    +            return
    +        self._closed = True
    +        self._reason = reason
    +        self._changed.set()
    +
    +    def abort(self, reason: str) -> None:
    +        if not self._closed:
    +            self._closed = True
    +            self._reason = reason
    +        self._items.clear()
    +        self._empty.set()
    +        self._changed.set()
    +
    +    async def wait_empty(self) -> None:
    +        await self._empty.wait()
    +
    +
    +class SessionEndpoint:
    +    def __init__(
    +        self,
    +        session_id: int,
    +        capacity: int,
    +        reply_via_outbox: bool,
    +        on_slow: Callable[[int, str], None],
    +        close_transport: Callable[[str], None],
    +    ) -> None:
    +        self.session_id = session_id
    +        self.reply_via_outbox = reply_via_outbox
    +        self._on_slow = on_slow
    +        self._close_transport = close_transport
    +        self._transport_close_requested = False
    +        self.outbox = CloseAwareOutbox(capacity, self._overflow)
    +
    +    def _overflow(self) -> None:
    +        self.request_transport_close("outbox full")
    +        self._on_slow(self.session_id, "outbox full")
    +
    +    def offer(self, item: Outbound) -> bool:
    +        return self.outbox.offer(item)
    +
    +    def offer_best_effort(self, item: Outbound) -> bool:
    +        return self.outbox.offer_best_effort(item)
    +
    +    async def receive(self) -> Outbound:
    +        return await self.outbox.receive()
    +
    +    def request_transport_close(self, reason: str) -> None:
    +        if self._transport_close_requested:
    +            return
    +        self._transport_close_requested = True
    +        self._close_transport(reason)
    ```

**是什么，为什么现在需要**

该模块增加类型化 Outbound Message、Close-aware 有界队列与拥有 Transport Callback 的 Endpoint Wrapper。

**在运行时做什么**

它保留 FIFO 顺序，区分优雅 Close 与 Abort，并把第一次溢出变成且仅变成一个慢 Session Signal。

**关键代码**

```python
if len(self._items) == self._capacity:
    self.abort("outbox full")
```

**关键语句理解**

溢出使 Session Stream 失效；如果只丢最新项，Transport 会留下不可解释的部分历史。

#### Executor 所有的 Endpoint

由单 Writer 注册 Endpoint，并在 Transport 关闭或 Outbox 拒绝输出时收束已接受请求。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index 5d38dc5bf9be2199dafb4b4d8b1860ef526ec48f..9342732be4efc087056c88f6dedbf75638f0117c 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -15,10 +15,13 @@ from miniredis.core.expiration import expiry_delete, is_expired
     from miniredis.core.mailbox import EventLoopMailbox
     from miniredis.core.outbound import (
         Abandoned,
    +    ReplyMessage,
         RequestOutcome,
         RequestToken,
         Replied,
         RuntimeClosed,
    +    SessionEndpoint,
    +    TransportClosed,
     )
     from miniredis.core.reply import Failure, Reply

    @@ -42,6 +45,11 @@ class AbandonRequest:
         token: RequestToken


    +@dataclass(frozen=True, slots=True)
    +class SessionClosed:
    +    session_id: int
    +
    +
     @dataclass(frozen=True, slots=True)
     class ExecutionPlan:
         reply: Reply | None
    @@ -118,6 +126,7 @@ class CommandExecutor:
             self._request_tokens = itertools.count(1)
             self._requests: dict[RequestToken, ExecuteRequest] = {}
             self._accepted_tokens: list[RequestToken] = []
    +        self._endpoints: dict[int, SessionEndpoint] = {}
             self._accepted_changed = asyncio.Event()
             self._applied_batches: list[CommitBatch] = []
             self._handling_message = False
    @@ -194,6 +203,22 @@ class CommandExecutor:
             self._on_debug_change()
             return True

    +    def _finish_reply(
    +        self,
    +        token: RequestToken,
    +        reply: Reply | None,
    +    ) -> bool:
    +        request = self._requests.get(token)
    +        if request is None:
    +            return False
    +        endpoint = self._endpoints.get(request.session_id)
    +        if endpoint is None:
    +            return self._finish_request(token, TransportClosed())
    +        if reply is not None and endpoint.reply_via_outbox:
    +            if not endpoint.offer(ReplyMessage(token, reply)):
    +                return self._finish_request(token, TransportClosed())
    +        return self._finish_request(token, Replied(reply))
    +
         def post_control(self, message: object) -> bool:
             posted = self.mailbox.post_control(message)
             if posted:
    @@ -238,12 +263,21 @@ class CommandExecutor:
                 deleted = await self._active_expire_once(message.now_ms)
                 if message.future is not None and not message.future.done():
                     message.future.set_result(deleted)
    +        elif isinstance(message, SessionClosed):
    +            self._close_session(message.session_id)
             else:
                 raise AssertionError(f"unknown executor message: {message!r}")

         def _abandon(self, event: AbandonRequest) -> None:
             self._finish_request(event.token, Abandoned())

    +    def _close_session(self, session_id: int) -> None:
    +        self._endpoints.pop(session_id, None)
    +        for token, request in tuple(self._requests.items()):
    +            if request.session_id == session_id:
    +                self._finish_request(token, TransportClosed())
    +        self._on_debug_change()
    +
         def _complete_terminal_failure(self, failure: BaseException) -> None:
             if self._terminal_cleanup_complete:
                 return
    @@ -279,7 +313,7 @@ class CommandExecutor:

             if plan.reply is None:
                 raise AssertionError("Phase 1 execution plan requires a reply")
    -        self._finish_request(request.token, Replied(plan.reply))
    +        self._finish_reply(request.token, plan.reply)

         async def active_expire_once(self) -> int:
             if self._worker_task is None or self._stopping:
    @@ -386,6 +420,22 @@ class CommandExecutor:
                 and not self._requests
             )

    +    def register_endpoint(self, endpoint: SessionEndpoint) -> None:
    +        if endpoint.session_id in self._endpoints:
    +            raise ValueError(f"duplicate session: {endpoint.session_id}")
    +        self._endpoints[endpoint.session_id] = endpoint
    +        self._on_debug_change()
    +
    +    def endpoint(self, session_id: int) -> SessionEndpoint | None:
    +        return self._endpoints.get(session_id)
    +
    +    def endpoints(self) -> tuple[SessionEndpoint, ...]:
    +        return tuple(self._endpoints.values())
    +
    +    @property
    +    def endpoint_count(self) -> int:
    +        return len(self._endpoints)
    +
         @property
         def debug_failure(self) -> BaseException | None:
             return self._failure
    ```

**是什么，为什么现在需要**

单 Writer 也拥有 Session 注册与已接受请求到 Endpoint 的映射。

**在运行时做什么**

它按配置 Offer Reply，移除已关闭 Endpoint，并收束该 Session 仍所有的每个请求。

**关键代码**

```python
for token, request in tuple(self._requests.items()):
    if request.session_id == session_id:
        self._finish_request(token, TransportClosed())
```

**关键语句理解**

Session 关闭有有限所有权范围：全部且仅该 Session 关联请求获得 Transport 终态。

#### Session 容量与 Runtime 接线

校验单一 Outbox 上限，分配单调 Session ID，再把慢 Session 关闭经 Executor Control 路由回去。

??? note "文件差异：src/miniredis/config.py"
    ```diff
    diff --git a/src/miniredis/config.py b/src/miniredis/config.py
    index f8c7715fae0e36e2c2af7d0fb861e1be26a69d77..275f68be1c8a2109f6dbdd54fe0b7e3f3afd0d9f 100644
    --- a/src/miniredis/config.py
    +++ b/src/miniredis/config.py
    @@ -12,6 +12,7 @@ class MiniRedisConfig:
         active_expire_sample_size: int = 20
         maxmemory: int | None = None
         eviction_policy: EvictionPolicy = "noeviction"
    +    outbox_limit: int = 64

         def __post_init__(self) -> None:
             if self.max_pending_commands <= 0:
    @@ -22,3 +23,5 @@ class MiniRedisConfig:
                 raise ValueError("maxmemory must be positive")
             if self.eviction_policy not in {"noeviction", "allkeys-lru"}:
                 raise ValueError("eviction_policy must be 'noeviction' or 'allkeys-lru'")
    +        if self.outbox_limit <= 0:
    +            raise ValueError("outbox_limit must be positive")
    ```

**是什么，为什么现在需要**

Config 增加一个正的 Per-session 输出容量。

**在运行时做什么**

除非部署选择其他上限，每个 Endpoint 都获得同一已校验默认值。

**关键代码**

```python
if self.outbox_limit <= 0:
    raise ValueError("outbox_limit must be positive")
```

**关键语句理解**

零容量无法保留“已接受输出可入队”的承诺，因此在构造时拒绝。

??? note "文件差异：src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 7d17d68bb55381159780bff3aff8d2ea1cefa5ef..65ce40063d2356bca5d7fa1e5b408fb499524e3a 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -1,6 +1,7 @@
     from __future__ import annotations

     import asyncio
    +import itertools
     from collections.abc import Callable
     from dataclasses import dataclass
     from enum import Enum
    @@ -18,8 +19,9 @@ from miniredis.core.executor import (
         CommandExecutor,
         CommitBarrier,
         NullCommitBarrier,
    +    SessionClosed,
     )
    -from miniredis.core.outbound import RequestToken
    +from miniredis.core.outbound import RequestToken, SessionEndpoint
     from miniredis.core.planner import CommandPlanner
     from miniredis.core.reply import Failure

    @@ -42,6 +44,10 @@ class RuntimeStats:
         owned_tasks: int


    +def _direct_transport_close(_reason: str) -> None:
    +    return None
    +
    +
     class MiniRedis:
         def __init__(
             self,
    @@ -67,7 +73,7 @@ class MiniRedis:
                 on_terminal_failure=self._on_executor_terminal_failure,
             )
             self.state = RuntimeState.STARTING
    -        self._next_session_id = 0
    +        self._session_ids = itertools.count(1)
             self._start_task: asyncio.Task[None] | None = None
             self._close_task: asyncio.Task[None] | None = None

    @@ -132,8 +138,19 @@ class MiniRedis:
         def direct_client(self) -> DirectClient:
             if self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}:
                 raise RuntimeError("runtime is closed")
    -        self._next_session_id += 1
    -        return DirectClient(self, self._next_session_id)
    +        session_id = next(self._session_ids)
    +        endpoint = SessionEndpoint(
    +            session_id=session_id,
    +            capacity=self.config.outbox_limit,
    +            reply_via_outbox=False,
    +            on_slow=self._session_became_slow,
    +            close_transport=_direct_transport_close,
    +        )
    +        self.executor.register_endpoint(endpoint)
    +        return DirectClient(self, endpoint)
    +
    +    def _session_became_slow(self, session_id: int, _reason: str) -> None:
    +        self.executor.post_control(SessionClosed(session_id))

         async def close(self) -> None:
             if self._close_task is None:
    @@ -198,7 +215,7 @@ class MiniRedis:
                 pending_futures=self.executor.pending_request_count,
                 waiters=0,
                 subscriptions=0,
    -            sessions=0,
    +            sessions=self.executor.endpoint_count,
                 timer_handles=0,
                 owned_tasks=0,
             )
    @@ -218,3 +235,6 @@ class MiniRedis:

         async def debug_wait_until_idle(self) -> None:
             await self._debug_wait(lambda: self.executor.idle)
    +
    +    async def debug_wait_for_sessions(self, count: int) -> None:
    +        await self._debug_wait(lambda: self.executor.endpoint_count == count)
    ```

**是什么，为什么现在需要**

Runtime 成为 Session Factory，并把慢 Session Signal 路由回 Executor Control。

**在运行时做什么**

它分配 ID、用配置容量构建 Endpoint、注册它，再在生命周期证据中报告 Session 数。

**关键代码**

```python
self.executor.register_endpoint(endpoint)
return DirectClient(self, endpoint)
```

**关键语句理解**

注册在 Client 逃出前完成，所以不会有请求引用 Executor 未知的 Session。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/10-ordered-outbox/tests.txt)`。该焦点集组合 Queue Unit 契约与已有慢 Endpoint 并发证据。

### 需要真正记住的内容

一个 Session 拥有一条有序流；给它设界；区分优雅 Drain 与破坏性 Abort；不预留魔法 Sentinel 槽；只关闭慢 Transport 一次；显式收束每个关联请求。

### 用自己的话讲清楚

Outbox 不只是 Queue。它是 Session 的排序与生命周期契约：已接受消息按序离开，优雅关闭排空它们，溢出只使该慢 Session 失效，不阻塞全局 Executor。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/02-architecture.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/319d14a...5436512)

完成后可运行 `python -m journey.tools.build_journey check 10` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/10-ordered-outbox/stage.patch)

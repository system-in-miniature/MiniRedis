# Stage 09 · 请求所有权与终态结果

### 目标

让每个已接受请求从准入到唯一类型化终态结果，都由 Runtime 拥有。

??? note "交付文件"
    - `src/miniredis/adapters/direct.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/mailbox.py`
    - `src/miniredis/core/outbound.py`
    - `src/miniredis/runtime.py`
    - `tests/concurrency/test_request_ownership.py`

### 当前遇到的问题

Executor 已串行化 Commit，但调用方可在请求排队时取消。如果取消方拥有共享 Future，即使命令仍会提交，Runtime 也会失去唯一完成通道。关闭与 Transport 丢失也需要可区分结果，而非通用异常。

### 测试契约

#### 先看会坏在哪里

Executor 暂停时取消调用方，不得取消已接受请求，也不得复用 Token。恢复后变更仍会提交，每个 Owned Future 到达且仅到达一个终态，Runtime 统计最终回到零。

??? note "文件差异：tests/concurrency/test_request_ownership.py"
    ```diff
    diff --git a/tests/concurrency/test_request_ownership.py b/tests/concurrency/test_request_ownership.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..b943f5690bd2c0e02216d9aee0e9000af2824bfe
    --- /dev/null
    +++ b/tests/concurrency/test_request_ownership.py
    @@ -0,0 +1,34 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Bytes, Ok
    +
    +
    +@pytest.mark.asyncio
    +async def test_accepted_tokens_are_runtime_unique_and_never_reused():
    +    async with MiniRedis.open() as runtime:
    +        client = runtime.direct_client()
    +        assert await client.execute(CommandRequest(b"PING")) == Ok(b"PONG")
    +        assert await client.execute(CommandRequest(b"PING")) == Ok(b"PONG")
    +        assert [token.value for token in runtime.debug_accepted_tokens] == [1, 2]
    +
    +
    +@pytest.mark.asyncio
    +async def test_caller_cancellation_does_not_cancel_owned_future_or_commit():
    +    async with MiniRedis.open() as runtime:
    +        client = runtime.direct_client()
    +        runtime.debug_pause_executor()
    +        request = asyncio.create_task(client.execute(CommandRequest(b"INCR", (b"n",))))
    +        await runtime.debug_wait_until_queued(1)
    +        request.cancel()
    +        with pytest.raises(asyncio.CancelledError):
    +            await request
    +        runtime.debug_resume_executor()
    +        await runtime.debug_wait_until_idle()
    +        assert await client.execute(CommandRequest(b"GET", (b"n",))) == Bytes(b"1")
    +        stats = runtime.debug_stats()
    +        assert stats.accepted_requests == 0
    +        assert stats.pending_futures == 0
    +        assert [token.value for token in runtime.debug_accepted_tokens] == [1, 2]
    ```

**测试锁定什么**

它锁定 Runtime 唯一单调 Token、取消 Shield、取消后 Commit 行为与完整请求清理。

**如何构造反例**

它暂停 Executor，等 INCR 准入后只取消调用方 Task，再恢复 Mailbox，观察值与所有权计数。

**关键测试语句**

```python
assert stats.accepted_requests == 0
assert stats.pending_futures == 0
```

**失败意味着什么**

调用方生命周期泄漏进 Runtime 所有权，已接受请求成为孤儿，或 Token/Future 完成了非一次。

### 基本概念

准入会把请求所有权转给 MiniRedis。`RequestToken` 是关联身份，Executor-owned Future 是完成槽，`RequestOutcome` 是封闭终态词汇。调用方取消只请求 Abandon，不能直接取消 Owned Slot。

### 为什么需要这个机制

Commit 与 Client Await 是不同生命周期。分开它们，才能在保留单一有序状态变更所有者的同时，明确表示取消、关闭、Transport 丢失与内部失败，也防止不可见 Pending Future 累积。

### 运行时心智模型

Adapter 解析并提交请求，再 Shield Future。Executor 在 Mailbox 准入前记录 Token、Request 与 Future。取消会把 `AbandonRequest` 投入 Control Lane。无论哪个有序事件先拥有完成，都会移除请求并设置且仅设置一个类型化结果；终态清理则收束全部剩余 Token。

### 机制板块

#### 调用方取消边界

Shield Executor 所有的完成态，再把调用方取消翻译成有序 Abandon Control Message。

??? note "文件差异：src/miniredis/adapters/direct.py"
    ```diff
    diff --git a/src/miniredis/adapters/direct.py b/src/miniredis/adapters/direct.py
    index b15e6a15ad24edd4273cd5550eabebeae8843338..e85e99af86a596b1d506d624f4e7d980e6e2eb71 100644
    --- a/src/miniredis/adapters/direct.py
    +++ b/src/miniredis/adapters/direct.py
    @@ -3,9 +3,15 @@ from __future__ import annotations
     import asyncio
     from typing import TYPE_CHECKING

    -from miniredis.commands.parser import CommandParseError, parse_command_request
     from miniredis.commands.request import CommandRequest
    -from miniredis.core.executor import Replied, RuntimeClosed, SubmittedRequest
    +from miniredis.core.executor import AbandonRequest, SubmittedRequest
    +from miniredis.core.outbound import (
    +    Abandoned,
    +    Replied,
    +    RuntimeClosed,
    +    RuntimeFailed,
    +    TransportClosed,
    +)
     from miniredis.core.reply import Failure, Reply

     if TYPE_CHECKING:
    @@ -14,20 +20,22 @@ if TYPE_CHECKING:

     class DirectClient:
         def __init__(self, runtime: MiniRedis, session_id: int) -> None:
    -        self.runtime = runtime
    +        self._runtime = runtime
             self.session_id = session_id
    -        self.closed = False
    +        self._closed = False

    -    async def execute(self, request: CommandRequest) -> Reply:
    -        if self.closed:
    -            return Failure("CLOSED", "client is closed")
    -        try:
    -            command = parse_command_request(request)
    -        except CommandParseError as error:
    -            return Failure("ERR", str(error))
    +    @property
    +    def closed(self) -> bool:
    +        return self._closed

    -        submitted = self.runtime.executor.submit(
    -            session_id=self.session_id, command=command
    +    async def execute(self, request: CommandRequest) -> Reply | None:
    +        if self._closed:
    +            return Failure("CLOSED", "client is closed")
    +        parsed = self._runtime.parse(request)
    +        if isinstance(parsed, Failure):
    +            return parsed
    +        submitted = self._runtime.executor.submit(
    +            session_id=self.session_id, command=parsed
             )
             if isinstance(submitted, Failure):
                 return submitted
    @@ -35,17 +43,26 @@ class DirectClient:
                 f"unexpected submission: {submitted!r}"
             )

    -        outcome = await asyncio.shield(submitted.future)
    +        try:
    +            outcome = await asyncio.shield(submitted.future)
    +        except asyncio.CancelledError:
    +            self._runtime.executor.post_control(AbandonRequest(submitted.token))
    +            raise
             match outcome:
                 case Replied(reply=reply):
                     return reply
                 case RuntimeClosed():
                     return Failure("CLOSED", "runtime closed before reply")
    -            case _:
    -                raise AssertionError(f"unexpected request outcome: {outcome!r}")
    +            case TransportClosed():
    +                return Failure("CLOSED", "session closed")
    +            case RuntimeFailed(reason):
    +                return Failure("ERR", f"runtime failed: {reason}")
    +            case Abandoned():
    +                return Failure("ERR", "request abandoned")
    +        raise AssertionError(f"unknown request outcome: {outcome!r}")

         async def receive(self) -> Reply:
             raise NotImplementedError("DirectClient.receive is unavailable in Phase 1")

         async def close(self) -> None:
    -        self.closed = True
    +        self._closed = True
    ```

**是什么，为什么现在需要**

Direct Adapter 成为生命周期边界，而不再拥有 Parse 和完成状态。

**在运行时做什么**

它 Shield Runtime Future，在调用方取消后发送 Abandon，再把类型化终态结果映射为 Direct Client 结果。

**关键代码**

```python
except asyncio.CancelledError:
    self._runtime.executor.post_control(AbandonRequest(submitted.token))
    raise
```

**关键语句理解**

调用方仍立即获得取消，但清理变成有序 Executor 事件，而非从外部修改共享所有权。

#### Runtime 所有的请求结果

让每个已接受请求拥有单调 Token、一个 Runtime-owned Future 与唯一终态结果。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index e138ca7c47c34dc7db31463fcfd6de7896d3f217..5d38dc5bf9be2199dafb4b4d8b1860ef526ec48f 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -1,6 +1,7 @@
     from __future__ import annotations

     import asyncio
    +import itertools
     from bisect import bisect_right
     from collections.abc import Callable
     from dataclasses import dataclass
    @@ -12,14 +13,16 @@ from miniredis.core.commit import CommitBatch, CommitOperation, CommitTrigger
     from miniredis.core.database import Database
     from miniredis.core.expiration import expiry_delete, is_expired
     from miniredis.core.mailbox import EventLoopMailbox
    +from miniredis.core.outbound import (
    +    Abandoned,
    +    RequestOutcome,
    +    RequestToken,
    +    Replied,
    +    RuntimeClosed,
    +)
     from miniredis.core.reply import Failure, Reply


    -@dataclass(frozen=True, slots=True)
    -class RequestToken:
    -    value: int
    -
    -
     @dataclass(slots=True)
     class ExecuteRequest:
         token: RequestToken
    @@ -35,16 +38,8 @@ class SubmittedRequest:


     @dataclass(frozen=True, slots=True)
    -class Replied:
    -    reply: Reply
    -
    -
    -@dataclass(frozen=True, slots=True)
    -class RuntimeClosed:
    -    pass
    -
    -
    -type RequestOutcome = Replied | RuntimeClosed
    +class AbandonRequest:
    +    token: RequestToken


     @dataclass(frozen=True, slots=True)
    @@ -81,7 +76,9 @@ class ActiveExpireTick:
         future: asyncio.Future[int] | None = None


    -type ExecutorMessage = ExecuteRequest | ActiveExpireTick | _StopExecutor
    +type ExecutorMessage = (
    +    ExecuteRequest | AbandonRequest | ActiveExpireTick | _StopExecutor | object
    +)


     class CommandExecutor:
    @@ -94,6 +91,7 @@ class CommandExecutor:
             commit_barrier: CommitBarrier,
             max_pending_commands: int,
             active_expire_sample_size: int = 20,
    +        on_debug_change: Callable[[], None],
             on_terminal_failure: Callable[[BaseException], None] | None = None,
         ) -> None:
             self.database = database
    @@ -108,6 +106,7 @@ class CommandExecutor:
             self.mailbox: EventLoopMailbox[ExecutorMessage] = EventLoopMailbox(
                 max_pending_commands
             )
    +        self._on_debug_change = on_debug_change
             self._on_terminal_failure = on_terminal_failure

             self._worker_task: asyncio.Task[None] | None = None
    @@ -116,10 +115,12 @@ class CommandExecutor:
             self._close_task: asyncio.Task[None] | None = None
             self._run_gate = asyncio.Event()
             self._run_gate.set()
    -        self._next_token = 0
    -        self._accepted: dict[RequestToken, asyncio.Future[RequestOutcome]] = {}
    +        self._request_tokens = itertools.count(1)
    +        self._requests: dict[RequestToken, ExecuteRequest] = {}
    +        self._accepted_tokens: list[RequestToken] = []
             self._accepted_changed = asyncio.Event()
             self._applied_batches: list[CommitBatch] = []
    +        self._handling_message = False
             self._failure: BaseException | None = None
             self._terminal_cleanup_complete = False
             self._stopping = False
    @@ -154,27 +155,50 @@ class CommandExecutor:
             if (
                 not self._started
                 or self._stopping
    +            or not self.mailbox.accepting_users
                 or (self._worker_task is not None and self._worker_task.cancelling() != 0)
                 or (self._worker_task is not None and self._worker_task.done())
             ):
                 return Failure("CLOSED", "runtime is closed")
    -        if len(self._accepted) >= self.max_pending_commands:
    +        if len(self._requests) >= self.max_pending_commands:
                 return Failure("BUSY", "command queue is full")

    -        self._next_token += 1
    -        token = RequestToken(self._next_token)
    +        token = RequestToken(next(self._request_tokens))
             future: asyncio.Future[RequestOutcome] = (
                 asyncio.get_running_loop().create_future()
             )
             request = ExecuteRequest(token, session_id, command, future)
    +        self._requests[token] = request
             if not self.mailbox.admit_user(request):
    -            return Failure("CLOSED", "runtime is closed")
    -        self._accepted[token] = future
    +            del self._requests[token]
    +            if not self.mailbox.accepting_users:
    +                return Failure("CLOSED", "runtime is closed")
    +            return Failure("BUSY", "command queue is full")
    +        self._accepted_tokens.append(token)
             self._accepted_changed.set()
    +        self._on_debug_change()
             return SubmittedRequest(token, future)

    -    def post_control(self, message: ActiveExpireTick | _StopExecutor) -> bool:
    -        return self.mailbox.post_control(message)
    +    def _finish_request(
    +        self,
    +        token: RequestToken,
    +        outcome: RequestOutcome,
    +    ) -> bool:
    +        message = self._requests.pop(token, None)
    +        if message is None:
    +            return False
    +        if message.future.done():
    +            raise RuntimeError(f"executor-owned Future already done: {token.value}")
    +        message.future.set_result(outcome)
    +        self._accepted_changed.set()
    +        self._on_debug_change()
    +        return True
    +
    +    def post_control(self, message: object) -> bool:
    +        posted = self.mailbox.post_control(message)
    +        if posted:
    +            self._on_debug_change()
    +        return posted

         async def _run(self) -> None:
             failure: BaseException | None = None
    @@ -184,14 +208,15 @@ class CommandExecutor:
                 while True:
                     message = await self.mailbox.take()
                     await self._run_gate.wait()
    -                if isinstance(message, _StopExecutor):
    -                    return
    -                if isinstance(message, ExecuteRequest):
    -                    await self._execute(message)
    -                elif isinstance(message, ActiveExpireTick):
    -                    deleted = await self._active_expire_once(message.now_ms)
    -                    if message.future is not None and not message.future.done():
    -                        message.future.set_result(deleted)
    +                self._handling_message = True
    +                self._on_debug_change()
    +                try:
    +                    if isinstance(message, _StopExecutor):
    +                        return
    +                    await self._dispatch(message)
    +                finally:
    +                    self._handling_message = False
    +                    self._on_debug_change()
             except asyncio.CancelledError as error:
                 failure = error
             except Exception as error:  # noqa: BLE001 - worker failures are terminal
    @@ -200,8 +225,24 @@ class CommandExecutor:
                 if failure is not None:
                     self._complete_terminal_failure(failure)
                 else:
    -                for token in tuple(self._accepted):
    -                    self._finish(token, RuntimeClosed())
    +                for token in tuple(self._requests):
    +                    self._finish_request(token, RuntimeClosed())
    +            self._on_debug_change()
    +
    +    async def _dispatch(self, message: object) -> None:
    +        if isinstance(message, ExecuteRequest):
    +            await self._execute(message)
    +        elif isinstance(message, AbandonRequest):
    +            self._abandon(message)
    +        elif isinstance(message, ActiveExpireTick):
    +            deleted = await self._active_expire_once(message.now_ms)
    +            if message.future is not None and not message.future.done():
    +                message.future.set_result(deleted)
    +        else:
    +            raise AssertionError(f"unknown executor message: {message!r}")
    +
    +    def _abandon(self, event: AbandonRequest) -> None:
    +        self._finish_request(event.token, Abandoned())

         def _complete_terminal_failure(self, failure: BaseException) -> None:
             if self._terminal_cleanup_complete:
    @@ -211,9 +252,10 @@ class CommandExecutor:
             self._stopping = True
             self.mailbox.close_user_admission()
             self.mailbox.drain()
    -        for token in tuple(self._accepted):
    -            self._finish(token, RuntimeClosed())
    +        for token in tuple(self._requests):
    +            self._finish_request(token, RuntimeClosed())
             self.mailbox.close_control_admission()
    +        self._on_debug_change()
             if self._on_terminal_failure is not None:
                 self._on_terminal_failure(failure)

    @@ -237,7 +279,7 @@ class CommandExecutor:

             if plan.reply is None:
                 raise AssertionError("Phase 1 execution plan requires a reply")
    -        self._finish(request.token, Replied(plan.reply))
    +        self._finish_request(request.token, Replied(plan.reply))

         async def active_expire_once(self) -> int:
             if self._worker_task is None or self._stopping:
    @@ -282,12 +324,6 @@ class CommandExecutor:
             self._applied_batches.append(batch)
             return len(operations)

    -    def _finish(self, token: RequestToken, outcome: RequestOutcome) -> None:
    -        future = self._accepted.pop(token, None)
    -        if future is not None and not future.done():
    -            future.set_result(outcome)
    -        self._accepted_changed.set()
    -
         async def close(self) -> None:
             if self._close_task is None:
                 self._close_task = asyncio.create_task(
    @@ -308,9 +344,10 @@ class CommandExecutor:
                     self._failure = error
             finally:
                 self.mailbox.drain()
    -            for token in tuple(self._accepted):
    -                self._finish(token, RuntimeClosed())
    +            for token in tuple(self._requests):
    +                self._finish_request(token, RuntimeClosed())
                 self.mailbox.close_control_admission()
    +            self._on_debug_change()

         def debug_pause(self) -> None:
             self._run_gate.clear()
    @@ -319,15 +356,35 @@ class CommandExecutor:
             self._run_gate.set()

         async def debug_wait_accepted_at_least(self, count: int) -> None:
    -        while len(self._accepted) < count:
    +        while len(self._requests) < count:
                 self._accepted_changed.clear()
    -            if len(self._accepted) >= count:
    +            if len(self._requests) >= count:
                     return
                 await self._accepted_changed.wait()

         @property
         def debug_accepted_count(self) -> int:
    -        return len(self._accepted)
    +        return len(self._requests)
    +
    +    @property
    +    def accepted_tokens(self) -> tuple[RequestToken, ...]:
    +        return tuple(self._accepted_tokens)
    +
    +    @property
    +    def accepted_request_count(self) -> int:
    +        return len(self._requests)
    +
    +    @property
    +    def pending_request_count(self) -> int:
    +        return sum(not request.future.done() for request in self._requests.values())
    +
    +    @property
    +    def idle(self) -> bool:
    +        return (
    +            not self._handling_message
    +            and self.mailbox.pending_items == 0
    +            and not self._requests
    +        )

         @property
         def debug_failure(self) -> BaseException | None:
    ```

**是什么，为什么现在需要**

Executor 现在拥有完整已接受请求 Registry 与单调关联序列。

**在运行时做什么**

它在准入前记录，按 Mailbox 顺序 Dispatch 命令与 Control，并通过唯一 `_finish_request` Gate 移除每个请求。

**关键代码**

```python
message = self._requests.pop(token, None)
if message is None:
    return False
```

**关键语句理解**

Pop 是到终态的唯一所有权转移；后到的竞争事件只会看到缺失，无法再次完成 Future。

??? note "文件差异：src/miniredis/core/outbound.py"
    ```diff
    diff --git a/src/miniredis/core/outbound.py b/src/miniredis/core/outbound.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..fffad96fd7ac983d633e7ca35704be4abb536143
    --- /dev/null
    +++ b/src/miniredis/core/outbound.py
    @@ -0,0 +1,41 @@
    +from __future__ import annotations
    +
    +from dataclasses import dataclass
    +from typing import TypeAlias
    +
    +from miniredis.core.reply import Reply
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RequestToken:
    +    value: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Replied:
    +    reply: Reply | None
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Abandoned:
    +    pass
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class TransportClosed:
    +    pass
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RuntimeClosed:
    +    pass
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RuntimeFailed:
    +    reason: str
    +
    +
    +RequestOutcome: TypeAlias = (
    +    Replied | Abandoned | TransportClosed | RuntimeClosed | RuntimeFailed
    +)
    ```

**是什么，为什么现在需要**

该模块独立于 Transport Adapter 定义请求关联与封闭终态结果集。

**在运行时做什么**

Adapter 模式匹配 `Replied`、`Abandoned`、`TransportClosed`、`RuntimeClosed` 或 `RuntimeFailed`，不从异常猜测原因。

**关键代码**

```python
RequestOutcome: TypeAlias = (
    Replied | Abandoned | TransportClosed | RuntimeClosed | RuntimeFailed
)
```

**关键语句理解**

Union 使每个终态原因显式化，并迫使 Consumer 处理新生命周期情况。

#### User 与 Control 准入通道

限制 User 压力，同时在关闭期间保持 Lifecycle 与 Abandon Control 可准入。

??? note "文件差异：src/miniredis/core/mailbox.py"
    ```diff
    diff --git a/src/miniredis/core/mailbox.py b/src/miniredis/core/mailbox.py
    index fed74e9f740f8ed80d267375df7d2fbe01b946b5..e0d20c2a3574215b82178beceed920abbb47c10c 100644
    --- a/src/miniredis/core/mailbox.py
    +++ b/src/miniredis/core/mailbox.py
    @@ -22,6 +22,10 @@ class EventLoopMailbox[T]:
         def pending_users(self) -> int:
             return self._pending_users

    +    @property
    +    def accepting_users(self) -> bool:
    +        return self._user_open
    +
         @property
         def pending_items(self) -> int:
             return len(self._items)
    ```

**是什么，为什么现在需要**

Mailbox 区分有界 User 准入与内部 Control 准入。

**在运行时做什么**

User 命令占用容量；Abandon 与 Shutdown Control 在 User 准入关闭后仍可进入。

**关键代码**

```python
def post_control(self, item: T) -> bool:
    if not self._control_open:
        return False
```

**关键语句理解**

关闭 User 压力不等于关闭生命周期协调；Control Lane 要保持到终态清理安全完成。

#### Runtime 解析与生命周期证据

集中 Parse 所有权，并用窄化 Predicate 观察已接受请求清理，不依赖 Sleep。

??? note "文件差异：src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index a173895009de1a661615d8adab43ce1bcc74a3b5..7d17d68bb55381159780bff3aff8d2ea1cefa5ef 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -1,12 +1,17 @@
     from __future__ import annotations

     import asyncio
    +from collections.abc import Callable
    +from dataclasses import dataclass
     from enum import Enum
     from typing import Any, Self

     from miniredis.adapters.direct import DirectClient
     from miniredis.clock import Clock, SystemClock
     from miniredis.config import MiniRedisConfig
    +from miniredis.commands.model import Command
    +from miniredis.commands.parser import CommandParseError, parse_command_request
    +from miniredis.commands.request import CommandRequest
     from miniredis.core.commit import CommitBatch, StoredEntry
     from miniredis.core.database import Database
     from miniredis.core.executor import (
    @@ -14,7 +19,9 @@ from miniredis.core.executor import (
         CommitBarrier,
         NullCommitBarrier,
     )
    +from miniredis.core.outbound import RequestToken
     from miniredis.core.planner import CommandPlanner
    +from miniredis.core.reply import Failure


     class RuntimeState(str, Enum):
    @@ -24,6 +31,17 @@ class RuntimeState(str, Enum):
         CLOSED = "closed"


    +@dataclass(frozen=True, slots=True)
    +class RuntimeStats:
    +    accepted_requests: int
    +    pending_futures: int
    +    waiters: int
    +    subscriptions: int
    +    sessions: int
    +    timer_handles: int
    +    owned_tasks: int
    +
    +
     class MiniRedis:
         def __init__(
             self,
    @@ -37,6 +55,7 @@ class MiniRedis:
             self.commit_barrier = commit_barrier
             self.database = Database()
             self.planner = CommandPlanner(config)
    +        self._debug_changed = asyncio.Event()
             self.executor = CommandExecutor(
                 database=self.database,
                 planner=self.planner,
    @@ -44,6 +63,7 @@ class MiniRedis:
                 commit_barrier=commit_barrier,
                 max_pending_commands=config.max_pending_commands,
                 active_expire_sample_size=config.active_expire_sample_size,
    +            on_debug_change=self._debug_notify,
                 on_terminal_failure=self._on_executor_terminal_failure,
             )
             self.state = RuntimeState.STARTING
    @@ -103,6 +123,12 @@ class MiniRedis:
             if self.state is RuntimeState.STARTING:
                 self.state = RuntimeState.RUNNING

    +    def parse(self, request: CommandRequest) -> Command | Failure:
    +        try:
    +            return parse_command_request(request)
    +        except CommandParseError as error:
    +            return Failure("ERR", str(error))
    +
         def direct_client(self) -> DirectClient:
             if self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}:
                 raise RuntimeError("runtime is closed")
    @@ -161,3 +187,34 @@ class MiniRedis:

         async def debug_wait_accepted_at_least(self, count: int) -> None:
             await self.executor.debug_wait_accepted_at_least(count)
    +
    +    @property
    +    def debug_accepted_tokens(self) -> tuple[RequestToken, ...]:
    +        return self.executor.accepted_tokens
    +
    +    def debug_stats(self) -> RuntimeStats:
    +        return RuntimeStats(
    +            accepted_requests=self.executor.accepted_request_count,
    +            pending_futures=self.executor.pending_request_count,
    +            waiters=0,
    +            subscriptions=0,
    +            sessions=0,
    +            timer_handles=0,
    +            owned_tasks=0,
    +        )
    +
    +    def _debug_notify(self) -> None:
    +        self._debug_changed.set()
    +
    +    async def _debug_wait(self, predicate: Callable[[], bool]) -> None:
    +        while not predicate():
    +            self._debug_changed.clear()
    +            if predicate():
    +                return
    +            await self._debug_changed.wait()
    +
    +    async def debug_wait_until_queued(self, count: int) -> None:
    +        await self._debug_wait(lambda: self.executor.accepted_request_count >= count)
    +
    +    async def debug_wait_until_idle(self) -> None:
    +        await self._debug_wait(lambda: self.executor.idle)
    ```

**是什么，为什么现在需要**

Runtime 集中 Parse 所有权，并暴露基于 Predicate 的生命周期诊断。

**在运行时做什么**

它构造 Executor Debug Notification 边界，并不用 Scheduler Sleep 等待 Queued 或 Idle 状态。

**关键代码**

```python
async def debug_wait_until_idle(self) -> None:
    await self._debug_wait(lambda: self.executor.idle)
```

**关键语句理解**

测试等待 Owned State Transition，而非估计的墙上时间，因此并发证据具有因果性。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/09-request-outcomes/tests.txt)`。它围绕真实 Executor Mailbox 与公开 Direct Adapter，证明 Token 唯一性与取消清理。

### 需要真正记住的内容

准入转移所有权；Shield Runtime Future；用数据表示终态原因；通过唯一 Pop Gate 完成；分开 Control 准入；用 Predicate 而非 Sleep 观察并发。

### 用自己的话讲清楚

被取消的 Waiter 与已接受命令不是同一生命周期。准入后 MiniRedis 拥有命令，所以 Adapter 可停止等待，Executor 仍会确定地提交或 Abandon，并且只关闭 Future 一次。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/02-architecture.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/7628635...319d14a)

完成后可运行 `python -m journey.tools.build_journey check 9` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/09-request-outcomes/stage.patch)

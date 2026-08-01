# Stage 22 · 有序 Pipeline

### 目标

在更早 Reply 尚未完成时接纳多个 Request，同时保持精确的 Per-session Reply Order、有界容量、Parse-error 位置与 Cancellation Ownership。

??? note "交付文件"
    - `src/miniredis/__init__.py`
    - `src/miniredis/adapters/direct.py`
    - `src/miniredis/adapters/tcp.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/outbound.py`
    - `src/miniredis/runtime.py`
    - `tests/adapters/test_direct_pipeline.py`
    - `tests/adapters/test_tcp_async_semantics.py`

### 当前遇到的问题

Stage 20 通过只允许一个 Pending Command 保持 Connection Order。这虽然正确，却不是真正 Pipelining：第一条命令阻塞时 Client 无法提交后续工作。直接启动所有 Command Task 也不正确，因为后完成的 Reply 可能越过先前 BLPOP，Parse Failure 可能绕过 Mailbox Order，全局 Queue 压力还可能把内部 BUSY 暴露到 Wire。

### 测试契约

#### 先看会坏在哪里

Executor 暂停时可能只看到第一个 Frame，而不是完整 Pipeline。BLPOP 后的快速 PING 可能先写出。中间非法 Command 可能直接 Emit 并跳过已接纳工作。`max_pending_commands=1` 时，后续 TCP Frame 可能收到 BUSY 而非等待 Admission。Session Close 也可能遗留 Admission 或 Command Task。

??? note "文件差异：tests/adapters/test_direct_pipeline.py"
    ```diff
    diff --git a/tests/adapters/test_direct_pipeline.py b/tests/adapters/test_direct_pipeline.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..1d94293a5f245c38a717d4f6bf40a0abdd87dc1f
    --- /dev/null
    +++ b/tests/adapters/test_direct_pipeline.py
    @@ -0,0 +1,56 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import CommandRequest, MiniRedis
    +from miniredis.core.reply import Failure, Number, Ok
    +
    +
    +@pytest.mark.asyncio
    +async def test_direct_pipeline_preserves_result_slots_and_is_not_atomic():
    +    async with MiniRedis.open() as runtime:
    +        pipeline = runtime.direct_pipeline()
    +        pipeline.queue(CommandRequest(b"SET", (b"k", b"1")))
    +        pipeline.queue(CommandRequest(b"NOPE"))
    +        pipeline.queue(CommandRequest(b"INCR", (b"k",)))
    +
    +        assert await pipeline.execute() == (
    +            Ok(),
    +            Failure("ERR", "unknown command"),
    +            Number(2),
    +        )
    +        assert pipeline.pending_count == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_direct_pipeline_submits_parse_failures_in_mailbox_order():
    +    async with MiniRedis.open() as runtime:
    +        runtime.debug_pause_executor()
    +        pipeline = runtime.direct_pipeline()
    +        pipeline.queue(CommandRequest(b"SET", (b"k", b"1")))
    +        pipeline.queue(CommandRequest(b"PING", (b"too", b"many")))
    +        pipeline.queue(CommandRequest(b"INCR", (b"k",)))
    +
    +        executing = asyncio.create_task(pipeline.execute())
    +        await runtime.debug_wait_accepted_at_least(3)
    +
    +        assert [token.value for token in runtime.debug_accepted_tokens] == [1, 2, 3]
    +        runtime.debug_resume_executor()
    +        assert await executing == (
    +            Ok(),
    +            Failure("ERR", "wrong number of arguments for PING"),
    +            Number(2),
    +        )
    +
    +
    +@pytest.mark.asyncio
    +async def test_direct_pipeline_close_discards_queued_requests_and_closes_client():
    +    async with MiniRedis.open() as runtime:
    +        pipeline = runtime.direct_pipeline()
    +        pipeline.queue(CommandRequest(b"SET", (b"k", b"1")))
    +
    +        await pipeline.close()
    +
    +        assert pipeline.pending_count == 0
    +        with pytest.raises(RuntimeError, match="client is closed"):
    +            pipeline.queue(CommandRequest(b"PING"))
    ```

**锁定什么**

锁定每个 Queued Request 一个结果槽、非原子执行、Parse Failure 进入 Executor-token Order、Queue Reset 与 Close 行为。

**如何构造反例**

排入 SET、未知或格式错误命令、INCR；暂停 Executor；证明恢复执行前三个 Token 已全部接纳。

**关键测试语句**

```python
assert [token.value for token in runtime.debug_accepted_tokens] == [1, 2, 3]
```

**失败意味着什么**

Direct Pipeline 在串行等待、丢失输入位置，或把 Parse Failure 路由到正常请求所有权之外。

??? note "文件差异：tests/adapters/test_tcp_async_semantics.py"
    ```diff
    diff --git a/tests/adapters/test_tcp_async_semantics.py b/tests/adapters/test_tcp_async_semantics.py
    index 4ee11c8acd8ee276a4ffcf485866ee8d5c6edc46..8ed992243a8a3f21bdb98e3fa8eb26337f4adc79 100644
    --- a/tests/adapters/test_tcp_async_semantics.py
    +++ b/tests/adapters/test_tcp_async_semantics.py
    @@ -25,6 +25,92 @@ async def close_writers(*writers):
         )


    +@pytest.mark.asyncio
    +async def test_tcp_pipeline_preserves_reply_order_with_invalid_middle_command():
    +    async with MiniRedis.open() as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        wire = (
    +            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n"
    +            b"*1\r\n$4\r\nNOPE\r\n"
    +            b"*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n"
    +        )
    +
    +        await send(writer, wire)
    +
    +        await expect(reader, b"+OK\r\n-ERR unknown command\r\n:2\r\n")
    +        await close_writers(writer)
    +        await server.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_tcp_pipeline_submits_all_decoded_frames_before_first_executes():
    +    async with MiniRedis.open() as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        redis.debug_pause_executor()
    +        wire = (
    +            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n"
    +            b"*1\r\n$4\r\nNOPE\r\n"
    +            b"*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n"
    +        )
    +
    +        await send(writer, wire)
    +        try:
    +            async with asyncio.timeout(1):
    +                await redis.debug_wait_accepted_at_least(3)
    +        finally:
    +            redis.debug_resume_executor()
    +
    +        await expect(reader, b"+OK\r\n-ERR unknown command\r\n:2\r\n")
    +        await close_writers(writer)
    +        await server.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_tcp_pipeline_holds_later_reply_behind_blocking_command():
    +    async with MiniRedis.open() as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        producer = redis.direct_client()
    +        wire = (
    +            b"*3\r\n$5\r\nBLPOP\r\n$1\r\nq\r\n$1\r\n0\r\n"
    +            b"*1\r\n$4\r\nPING\r\n"
    +        )
    +
    +        await send(writer, wire)
    +        await redis.debug_wait_for_waiters(1)
    +        assert await producer.execute(
    +            CommandRequest(b"RPUSH", (b"q", b"x"))
    +        ) == Number(1)
    +
    +        await expect(reader, b"*2\r\n$1\r\nq\r\n$1\r\nx\r\n+PONG\r\n")
    +        await close_writers(writer)
    +        await server.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_tcp_pipeline_retries_busy_frame_without_exposing_busy_reply():
    +    async with MiniRedis.open(max_pending_commands=1) as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        redis.debug_pause_executor()
    +        wire = (
    +            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n"
    +            b"*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n"
    +            b"*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n"
    +        )
    +
    +        await send(writer, wire)
    +        await redis.debug_wait_accepted_at_least(1)
    +        assert redis.executor.debug_accepted_count == 1
    +        redis.debug_resume_executor()
    +
    +        await expect(reader, b"+OK\r\n:2\r\n:3\r\n")
    +        await close_writers(writer)
    +        await server.close()
    +
    +
     class CloseReleasedWriter:
         def __init__(self, inner) -> None:
             self._inner = inner
    ```

**锁定什么**

锁定 Eager Decoded-frame Admission、Blocking Work 后的精确 Reply Order、临时全局容量压力重试，以及既有 TCP Close/Slow-client 语义。

**如何构造反例**

在暂停 Executor 时 Pipeline SET/Error/INCR，在 PING 前 Pipeline BLPOP，并让三个 Command 经过容量为 1 的全局 Queue。

**关键测试语句**

```python
await expect(reader, b"*2\r\n$1\r\nq\r\n$1\r\nx\r\n+PONG\r\n")
await expect(reader, b"+OK\r\n:2\r\n:3\r\n")
```

**失败意味着什么**

Completion Order 被误当成 Request Order，或临时 Admission Pressure 改变了协议可见语义。

### 基本概念

Pipelining 分离 Admission Order 与 Completion Time。每个已接纳 Request 获得单调 Token。Session Endpoint 保存 Token Order 与已完成 Outbound Bundle；只有完成前缀能进入物理 Outbox。Parse Rejection 仍是 Request Outcome。共享 Executor 返回的 BUSY 是 TCP Admission 背压，而不是 Command Reply。

### 为什么需要这个机制

只有多个 Request 能同时 In-flight，Pipelining 才能提高利用率。Token-ordered Buffer 即使面对更晚完成的 Blocking Command，也保持 Connection 可观察的串行语义。让所有 Outcome 经过同一 Executor Ownership，还能统一 Shutdown、Abandonment、Pub/Sub Ack 与 Slow-client 行为。

### 运行时心智模型

TCP Reader 把 Frame 解码进有界 Deque，并在 Per-session Window 与全局 Executor Capacity 都允许时提交。每个 Accepted Token 都注册到 Endpoint。Command 可以按任意时间顺序完成，但 Outbound Bundle 要在 `_completed_requests` 中等待所有更早 Token 完成。唯一 Writer Drain 有序 Outbox。DirectPipeline 使用同一 Submit/Resolve 分离，但不假装 Batch 原子。

### 机制板块

#### 按 Token 排序的 Pipeline Completion

注册每个已接纳 Request，缓存乱序 Completion，并只从最老的连续完成前缀 Flush Reply。

??? note "文件差异：src/miniredis/core/outbound.py"
    ```diff
    diff --git a/src/miniredis/core/outbound.py b/src/miniredis/core/outbound.py
    index c45767b0fb2e22a8c4195fb41031d7c883d6a2c9..178ff18c625d6585c8a3389c48acc064aa7748fb 100644
    --- a/src/miniredis/core/outbound.py
    +++ b/src/miniredis/core/outbound.py
    @@ -182,6 +182,9 @@ class SessionEndpoint:
             self._on_slow = on_slow
             self._close_transport = close_transport
             self._transport_close_requested = False
    +        self._request_order: deque[RequestToken] = deque()
    +        self._request_tokens: set[RequestToken] = set()
    +        self._completed_requests: dict[RequestToken, tuple[Outbound, ...]] = {}
             self.outbox = CloseAwareOutbox(capacity, self._overflow)

         def _overflow(self) -> None:
    @@ -194,6 +197,51 @@ class SessionEndpoint:
         def offer_best_effort(self, item: Outbound) -> bool:
             return self.outbox.offer_best_effort(item)

    +    def register_request(self, token: RequestToken) -> None:
    +        if not self.reply_via_outbox:
    +            return
    +        if token in self._request_tokens:
    +            raise ValueError(f"duplicate request token: {token.value}")
    +        self._request_order.append(token)
    +        self._request_tokens.add(token)
    +
    +    def complete_request(
    +        self,
    +        token: RequestToken,
    +        items: tuple[Outbound, ...],
    +    ) -> bool:
    +        if not self.reply_via_outbox:
    +            return True
    +        if token not in self._request_tokens:
    +            return not self.outbox.closed
    +        if token in self._completed_requests:
    +            raise ValueError(f"request already completed: {token.value}")
    +        self._completed_requests[token] = items
    +        return self._flush_completed_requests()
    +
    +    def cancel_request(self, token: RequestToken) -> bool:
    +        return self.complete_request(token, ())
    +
    +    @property
    +    def pending_request_count(self) -> int:
    +        return len(self._request_order)
    +
    +    def _flush_completed_requests(self) -> bool:
    +        while (
    +            self._request_order
    +            and self._request_order[0] in self._completed_requests
    +        ):
    +            token = self._request_order.popleft()
    +            self._request_tokens.remove(token)
    +            items = self._completed_requests.pop(token)
    +            for item in items:
    +                if not self.outbox.offer(item):
    +                    self._request_order.clear()
    +                    self._request_tokens.clear()
    +                    self._completed_requests.clear()
    +                    return False
    +        return not self.outbox.closed
    +
         async def receive(self) -> Outbound:
             return await self.outbox.receive()

    ```

**是什么，为什么出现**

`SessionEndpoint` 成为 Per-session Reply Reorder Buffer。

**运行时角色**

注册 Token Order，保存完整 Outbound Bundle，把取消 Token 作为空 Bundle，并只 Flush 最老连续完成前缀。

**关键代码**

```python
while (
    self._request_order
    and self._request_order[0] in self._completed_requests
):
    token = self._request_order.popleft()
```

**关键语句理解**

更晚完成的 Request 在每个更早 Token 获得终态 Bundle 前不可见，因此无需串行执行也能保持 Wire Order。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index 1fee9e7bec7c7922ab9f4737be61cddca73c93bc..f635151b0e2bdd61c13485fecc4af05ed1477ad8 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -39,6 +39,7 @@ from miniredis.core.expiration import expiry_delete, is_expired
     from miniredis.core.mailbox import EventLoopMailbox
     from miniredis.core.outbound import (
         Abandoned,
    +    Outbound,
         PubSubMessage,
         PubSubPong,
         ReplyMessage,
    @@ -72,10 +73,19 @@ class ExecuteRequest:
         future: asyncio.Future[RequestOutcome]


    +@dataclass(slots=True)
    +class RejectRequest:
    +    token: RequestToken
    +    session_id: int
    +    reply: Failure
    +    future: asyncio.Future[RequestOutcome]
    +
    +
     @dataclass(frozen=True, slots=True)
     class SubmittedRequest:
         token: RequestToken
         future: asyncio.Future[RequestOutcome]
    +    command: Command | None = None


     @dataclass(frozen=True, slots=True)
    @@ -196,7 +206,12 @@ class ActiveExpireTick:


     type ExecutorMessage = (
    -    ExecuteRequest | AbandonRequest | ActiveExpireTick | _StopExecutor | object
    +    ExecuteRequest
    +    | RejectRequest
    +    | AbandonRequest
    +    | ActiveExpireTick
    +    | _StopExecutor
    +    | object
     )


    @@ -245,7 +260,7 @@ class CommandExecutor:
             self._run_gate = asyncio.Event()
             self._run_gate.set()
             self._request_tokens = itertools.count(1)
    -        self._requests: dict[RequestToken, ExecuteRequest] = {}
    +        self._requests: dict[RequestToken, ExecuteRequest | RejectRequest] = {}
             self._accepted_tokens: list[RequestToken] = []
             self._endpoints: dict[int, SessionEndpoint] = {}
             self._accepted_changed = asyncio.Event()
    @@ -309,6 +324,24 @@ class CommandExecutor:
                 self._worker_started_or_done.set()

         def submit(self, session_id: int, command: Command) -> SubmittedRequest | Failure:
    +        return self._admit_request(session_id, command=command)
    +
    +    def submit_rejection(
    +        self,
    +        session_id: int,
    +        reply: Failure,
    +    ) -> SubmittedRequest | Failure:
    +        return self._admit_request(session_id, rejection=reply)
    +
    +    def _admit_request(
    +        self,
    +        session_id: int,
    +        *,
    +        command: Command | None = None,
    +        rejection: Failure | None = None,
    +    ) -> SubmittedRequest | Failure:
    +        if (command is None) == (rejection is None):
    +            raise ValueError("exactly one request payload is required")
             if (
                 not self._started
                 or self._stopping
    @@ -324,17 +357,25 @@ class CommandExecutor:
             future: asyncio.Future[RequestOutcome] = (
                 asyncio.get_running_loop().create_future()
             )
    -        request = ExecuteRequest(token, session_id, command, future)
    +        request: ExecuteRequest | RejectRequest
    +        if command is not None:
    +            request = ExecuteRequest(token, session_id, command, future)
    +        else:
    +            assert rejection is not None
    +            request = RejectRequest(token, session_id, rejection, future)
             self._requests[token] = request
             if not self.mailbox.admit_user(request):
                 del self._requests[token]
                 if not self.mailbox.accepting_users:
                     return Failure("CLOSED", "runtime is closed")
                 return Failure("BUSY", "command queue is full")
    +        endpoint = self._endpoints.get(session_id)
    +        if endpoint is not None:
    +            endpoint.register_request(token)
             self._accepted_tokens.append(token)
             self._accepted_changed.set()
             self._on_debug_change()
    -        return SubmittedRequest(token, future)
    +        return SubmittedRequest(token, future, command)

         def new_request_token(self) -> RequestToken:
             return RequestToken(next(self._request_tokens))
    @@ -347,6 +388,10 @@ class CommandExecutor:
             message = self._requests.pop(token, None)
             if message is None:
                 return False
    +        if not isinstance(outcome, Replied):
    +            endpoint = self._endpoints.get(message.session_id)
    +            if endpoint is not None:
    +                endpoint.cancel_request(token)
             if message.future.done():
                 raise RuntimeError(f"executor-owned Future already done: {token.value}")
             message.future.set_result(outcome)
    @@ -366,8 +411,13 @@ class CommandExecutor:
             if endpoint is None:
                 return self._finish_request(token, TransportClosed())
             if reply is not None and endpoint.reply_via_outbox:
    -            if not endpoint.offer(ReplyMessage(token, reply)):
    +            if not endpoint.complete_request(
    +                token,
    +                (ReplyMessage(token, reply),),
    +            ):
                     return self._finish_request(token, TransportClosed())
    +        elif endpoint.reply_via_outbox:
    +            endpoint.complete_request(token, ())
             return self._finish_request(token, Replied(reply))

         def post_control(self, message: object) -> bool:
    @@ -411,6 +461,8 @@ class CommandExecutor:
         async def _dispatch(self, message: object) -> None:
             if isinstance(message, ExecuteRequest):
                 await self._execute(message)
    +        elif isinstance(message, RejectRequest):
    +            self._finish_reply(message.token, message.reply)
             elif isinstance(message, AbandonRequest):
                 self._abandon(message)
             elif isinstance(message, ActiveExpireTick):
    @@ -642,16 +694,14 @@ class CommandExecutor:
             await self._apply_plan(request, plan, now_ms)

         def _subscribe(self, request: ExecuteRequest, command: Subscribe) -> None:
    -        endpoint = self._endpoints[request.session_id]
    +        items: list[SubscriptionAck] = []
             for channel in command.channels:
                 count = self.pubsub.subscribe(request.session_id, channel)
    -            if not endpoint.offer(SubscriptionAck("subscribe", channel, count)):
    -                self._finish_request(request.token, TransportClosed())
    -                return
    -        self._finish_request(request.token, Replied(None))
    +            items.append(SubscriptionAck("subscribe", channel, count))
    +        self._finish_outbound_request(request, tuple(items))

         def _unsubscribe(self, request: ExecuteRequest, command: Unsubscribe) -> None:
    -        endpoint = self._endpoints[request.session_id]
    +        items: list[SubscriptionAck] = []
             for channel in self.pubsub.unsubscribe_targets(
                 request.session_id,
                 command.channels,
    @@ -661,10 +711,8 @@ class CommandExecutor:
                     if channel is None
                     else self.pubsub.unsubscribe(request.session_id, channel)
                 )
    -            if not endpoint.offer(SubscriptionAck("unsubscribe", channel, count)):
    -                self._finish_request(request.token, TransportClosed())
    -                return
    -        self._finish_request(request.token, Replied(None))
    +            items.append(SubscriptionAck("unsubscribe", channel, count))
    +        self._finish_outbound_request(request, tuple(items))

         def _publish(self, request: ExecuteRequest, command: Publish) -> None:
             delivered = 0
    @@ -678,8 +726,19 @@ class CommandExecutor:

         def _subscribed_ping(self, request: ExecuteRequest, command: Ping) -> None:
             payload = b"" if command.message is None else command.message
    +        self._finish_outbound_request(request, (PubSubPong(payload),))
    +
    +    def _finish_outbound_request(
    +        self,
    +        request: ExecuteRequest,
    +        items: tuple[Outbound, ...],
    +    ) -> None:
             endpoint = self._endpoints[request.session_id]
    -        if endpoint.offer(PubSubPong(payload)):
    +        if endpoint.reply_via_outbox:
    +            offered = endpoint.complete_request(request.token, items)
    +        else:
    +            offered = all(endpoint.offer(item) for item in items)
    +        if offered:
                 self._finish_request(request.token, Replied(None))
             else:
                 self._finish_request(request.token, TransportClosed())
    @@ -915,6 +974,20 @@ class CommandExecutor:
                     return
                 await self._accepted_changed.wait()

    +    async def wait_for_submission_capacity(self) -> bool:
    +        while len(self._requests) >= self.max_pending_commands:
    +            if (
    +                self._stopping
    +                or not self.mailbox.accepting_users
    +                or (self._worker_task is not None and self._worker_task.done())
    +            ):
    +                return False
    +            self._accepted_changed.clear()
    +            if len(self._requests) < self.max_pending_commands:
    +                break
    +            await self._accepted_changed.wait()
    +        return not self._stopping and self.mailbox.accepting_users
    +
         @property
         def debug_accepted_count(self) -> int:
             return len(self._requests)
    ```

**是什么，为什么出现**

Executor 通过一条 Request 路径接纳 Typed Command 与 Parse Rejection，并协调 Endpoint Registration/Completion。

**运行时角色**

分配 Token、执行前注册、原子打包多项 Pub/Sub Outcome，并在全局 Submission Capacity 恢复时通知 Waiter。

**关键代码**

```python
endpoint = self._endpoints.get(session_id)
if endpoint is not None:
    endpoint.register_request(token)
self._accepted_tokens.append(token)
self._accepted_changed.set()
self._on_debug_change()
return SubmittedRequest(token, future, command)
```

**关键语句理解**

Admission 在任何 Outcome 产生前，同时建立 Executor Ownership 与 Reply-order Slot。

#### Direct Pipeline Submission

等待结果前提交全部排队 Request，为每个输入保留一个结果槽，并让 Parse Failure 进入 Mailbox Order。

??? note "文件差异：src/miniredis/adapters/direct.py"
    ```diff
    diff --git a/src/miniredis/adapters/direct.py b/src/miniredis/adapters/direct.py
    index cd8ec423f3756a293837509790c3cca3a560d2df..b93fb5a0a318c2a480dbadacdfbc209bdcef32dc 100644
    --- a/src/miniredis/adapters/direct.py
    +++ b/src/miniredis/adapters/direct.py
    @@ -1,10 +1,11 @@
     from __future__ import annotations

     import asyncio
    +from dataclasses import dataclass
     from typing import TYPE_CHECKING

    +from miniredis.commands.model import BlockingPop, Command
     from miniredis.commands.request import CommandRequest
    -from miniredis.commands.model import BlockingPop
     from miniredis.core.executor import (
         AbandonRequest,
         SessionClosed,
    @@ -25,6 +26,12 @@ if TYPE_CHECKING:
         from miniredis.runtime import MiniRedis


    +@dataclass(frozen=True, slots=True)
    +class _DirectSubmission:
    +    submitted: SubmittedRequest | Failure
    +    command: Command | None
    +
    +
     class DirectClient:
         def __init__(self, runtime: MiniRedis, endpoint: SessionEndpoint) -> None:
             self._runtime = runtime
    @@ -41,23 +48,28 @@ class DirectClient:
             return self._closed

         async def execute(self, request: CommandRequest) -> Reply | None:
    +        return await self.resolve(self.submit(request))
    +
    +    def submit(self, request: CommandRequest) -> _DirectSubmission:
             if self._closed:
    -            return Failure("CLOSED", "client is closed")
    +            return _DirectSubmission(Failure("CLOSED", "client is closed"), None)
             if not self._runtime.accepting_commands:
                 if self._runtime.normal_shutdown_started:
    -                return Failure("CLOSED", "runtime is not accepting commands")
    -            return Failure("CLOSED", "runtime is closed")
    -        parsed = self._runtime.parse(request)
    -        if isinstance(parsed, Failure):
    -            return parsed
    -        submitted = self._runtime.executor.submit(
    -            session_id=self.session_id, command=parsed
    +                failure = Failure("CLOSED", "runtime is not accepting commands")
    +            else:
    +                failure = Failure("CLOSED", "runtime is closed")
    +            return _DirectSubmission(failure, None)
    +        submitted = self._runtime.submit_request(
    +            session_id=self.session_id,
    +            request=request,
             )
    +        command = submitted.command if isinstance(submitted, SubmittedRequest) else None
    +        return _DirectSubmission(submitted, command)
    +
    +    async def resolve(self, item: _DirectSubmission) -> Reply | None:
    +        submitted = item.submitted
             if isinstance(submitted, Failure):
                 return submitted
    -        assert isinstance(submitted, SubmittedRequest), (
    -            f"unexpected submission: {submitted!r}"
    -        )

             try:
                 outcome = await asyncio.shield(submitted.future)
    @@ -71,7 +83,7 @@ class DirectClient:
                     return Failure("CLOSED", "runtime closed")
                 case RuntimeClosed():
                     return Failure("CLOSED", "runtime closed before reply")
    -            case TransportClosed() if isinstance(parsed, BlockingPop):
    +            case TransportClosed() if isinstance(item.command, BlockingPop):
                     return Bytes(None)
                 case TransportClosed():
                     return Failure("CLOSED", "session closed")
    @@ -101,3 +113,28 @@ class DirectClient:
                 self.endpoint.outbox.abort("runtime closed")
                 return
             await asyncio.shield(completion)
    +
    +
    +class DirectPipeline:
    +    def __init__(self, client: DirectClient) -> None:
    +        self._client = client
    +        self._requests: list[CommandRequest] = []
    +
    +    @property
    +    def pending_count(self) -> int:
    +        return len(self._requests)
    +
    +    def queue(self, request: CommandRequest) -> DirectPipeline:
    +        if self._client.closed:
    +            raise RuntimeError("client is closed")
    +        self._requests.append(request)
    +        return self
    +
    +    async def execute(self) -> tuple[Reply | None, ...]:
    +        requests, self._requests = self._requests, []
    +        submitted = [self._client.submit(request) for request in requests]
    +        return tuple([await self._client.resolve(item) for item in submitted])
    +
    +    async def close(self) -> None:
    +        self._requests.clear()
    +        await self._client.close()
    ```

**是什么，为什么出现**

Direct Client 分离 Submission 与 Resolution，`DirectPipeline` 批量复用这一原语。

**运行时角色**

先提交全部 Queued Request，再按输入顺序 Resolve Future；Close 丢弃未发送 Request 并关闭 Owned Client。

**关键代码**

```python
submitted = [self._client.submit(request) for request in requests]
return tuple([await self._client.resolve(item) for item in submitted])
```

**关键语句理解**

结果收集有序，但 Command 已经全部接纳；这个 Batch 是 Pipelined，而不是 Transactional。

#### 有界 TCP Pipeline Admission

无需逐个等待 Reply 地接纳多个已解码 Frame，重试临时 BUSY Admission，并在 Close 时收敛全部 Command/Admission Task。

??? note "文件差异：src/miniredis/adapters/tcp.py"
    ```diff
    diff --git a/src/miniredis/adapters/tcp.py b/src/miniredis/adapters/tcp.py
    index 581f719c84ee55a75b9bdb2960d06e8b02fec2e0..3b5e2f6cc99e6d652cdd166e9c446ef9130bc31e 100644
    --- a/src/miniredis/adapters/tcp.py
    +++ b/src/miniredis/adapters/tcp.py
    @@ -14,9 +14,11 @@ from miniredis.adapters.resp2 import (
     from miniredis.commands.request import CommandRequest
     from miniredis.core.outbound import (
         OutboxClosed,
    +    ReplyMessage,
         ServerClosed,
         SessionEndpoint,
     )
    +from miniredis.core.reply import Failure

     if TYPE_CHECKING:
         from miniredis.runtime import MiniRedis
    @@ -49,7 +51,8 @@ class TcpSession:
             self._on_closed = on_closed
             self._reader_task: asyncio.Task[None] | None = None
             self._writer_task: asyncio.Task[None] | None = None
    -        self._pending_command: asyncio.Task[None] | None = None
    +        self._pending_commands: set[asyncio.Task[None]] = set()
    +        self._admission_task: asyncio.Task[None] | None = None
             self._close_task: asyncio.Task[None] | None = None
             self._reader_quiescing = False
             self._reader_quiesced = asyncio.Event()
    @@ -103,25 +106,61 @@ class TcpSession:
             if reason != "runtime closed":
                 self.writer.close()

    -    def _submit_next(self) -> None:
    -        if (
    -            self._closed
    -            or self._reader_quiescing
    -            or self._pending_command is not None
    -            or not self._frames
    +    def _submit_available(self) -> None:
    +        while (
    +            not self._closed
    +            and not self._reader_quiescing
    +            and self._frames
    +            and self.endpoint.pending_request_count < self._max_buffered_frames
             ):
    +            request = self._frames[0]
    +            submitted = self.runtime.submit_request(self.session_id, request)
    +            if isinstance(submitted, Failure):
    +                if submitted.code == "BUSY":
    +                    self._ensure_admission_waiter()
    +                    return
    +                self._frames.popleft()
    +                token = self.runtime.executor.new_request_token()
    +                self.endpoint.offer(ReplyMessage(token, submitted))
    +                continue
    +
    +            self._frames.popleft()
    +            task = asyncio.create_task(
    +                self.runtime.wait_for_session_submission(submitted),
    +                name=f"miniredis:tcp-command:{self.session_id}",
    +            )
    +            self._pending_commands.add(task)
    +            task.add_done_callback(self._command_done)
    +
    +    def _ensure_admission_waiter(self) -> None:
    +        if self._admission_task is not None and not self._admission_task.done():
                 return
    -        request = self._frames.popleft()
    -        task = asyncio.create_task(
    -            self.runtime.execute_for_session(self.session_id, request),
    -            name=f"miniredis:tcp-command:{self.session_id}",
    +        self._admission_task = asyncio.create_task(
    +            self._wait_for_admission(),
    +            name=f"miniredis:tcp-admission:{self.session_id}",
             )
    -        self._pending_command = task
    -        task.add_done_callback(self._command_done)
    +        self._admission_task.add_done_callback(self._admission_done)
    +
    +    async def _wait_for_admission(self) -> None:
    +        if await self.runtime.wait_for_submission_capacity():
    +            self._submit_available()
    +
    +    def _admission_done(self, task: asyncio.Task[None]) -> None:
    +        if self._admission_task is task:
    +            self._admission_task = None
    +        try:
    +            task.result()
    +        except asyncio.CancelledError:
    +            return
    +        except BaseException as exc:
    +            self.endpoint.offer_best_effort(
    +                ServerClosed(f"session admission failed: {exc}")
    +            )
    +            self.endpoint.outbox.begin_close("session admission failed")
    +            self.request_close()

         def _command_done(self, task: asyncio.Task[None]) -> None:
    -        if self._pending_command is task:
    -            self._pending_command = None
    +        self._pending_commands.discard(task)
             try:
                 task.result()
             except asyncio.CancelledError:
    @@ -131,7 +170,7 @@ class TcpSession:
                     ServerClosed(f"session command failed: {exc}")
                 )
                 self.endpoint.outbox.begin_close("session command failed")
    -        self._submit_next()
    +        self._submit_available()

         async def _read_loop(self) -> None:
             protocol_error: RespProtocolError | None = None
    @@ -148,14 +187,19 @@ class TcpSession:
                         break
                     try:
                         frames = self.decoder.feed(data)
    +                    if (
    +                        len(self._frames)
    +                        + self.endpoint.pending_request_count
    +                        + len(frames)
    +                        > self._max_buffered_frames
    +                    ):
    +                        raise RespProtocolError("too many buffered command frames")
                         for frame in frames:
                             self._frames.append(frame_to_request(frame))
    -                    if len(self._frames) > self._max_buffered_frames:
    -                        raise RespProtocolError("too many buffered command frames")
                     except RespProtocolError as exc:
                         protocol_error = exc
                         break
    -                self._submit_next()
    +                self._submit_available()
             except asyncio.CancelledError:
                 if not self._reader_quiescing:
                     raise
    @@ -172,11 +216,7 @@ class TcpSession:
                 await self._drain_protocol_error_best_effort()
             if saw_eof or protocol_error is not None:
                 await self.runtime.close_session(self.session_id)
    -            if self._pending_command is not None:
    -                await asyncio.gather(
    -                    self._pending_command,
    -                    return_exceptions=True,
    -                )
    +            await self._settle_commands()
                 await self._finish_transport()

         async def _write_loop(self) -> None:
    @@ -241,11 +281,7 @@ class TcpSession:
             await self.wait_reader_quiesced()
             await self._settle_reader()
             await self.runtime.close_session(self.session_id)
    -        if self._pending_command is not None:
    -            await asyncio.gather(
    -                self._pending_command,
    -                return_exceptions=True,
    -            )
    +        await self._settle_commands()
             self.endpoint.outbox.abort("session closed")
             await self._finish_transport()

    @@ -255,11 +291,7 @@ class TcpSession:
             self.endpoint.outbox.abort("runtime closed")
             await self._finish_transport()
             await self._settle_reader()
    -        if self._pending_command is not None:
    -            await asyncio.gather(
    -                self._pending_command,
    -                return_exceptions=True,
    -            )
    +        await self._settle_commands()

         async def _settle_reader(self) -> None:
             if (
    @@ -271,6 +303,15 @@ class TcpSession:
                     return_exceptions=True,
                 )

    +    async def _settle_commands(self) -> None:
    +        if self._admission_task is not None:
    +            self._admission_task.cancel()
    +        tasks = tuple(self._pending_commands)
    +        if self._admission_task is not None:
    +            tasks += (self._admission_task,)
    +        if tasks:
    +            await asyncio.gather(*tasks, return_exceptions=True)
    +
         async def _finish_transport(self) -> None:
             if self._transport_finishing:
                 await self._transport_finished.wait()
    @@ -303,12 +344,12 @@ class TcpSession:

         @property
         def owned_task_count(self) -> int:
    -        return sum(
    +        return sum(not task.done() for task in self._pending_commands) + sum(
                 task is not None and not task.done()
                 for task in (
                     self._reader_task,
                     self._writer_task,
    -                self._pending_command,
    +                self._admission_task,
                     self._close_task,
                 )
             )
    ```

**是什么，为什么出现**

TCP Session 用有界 Command Task Set 与一个 Admission Waiter 替代单 Pending Command。

**运行时角色**

提交可用 Frame，遇到临时 BUSY 时等待而不回复，容量变化后恢复 Admission，并在 Close 时 Join 每个 Task。

**关键代码**

```python
if submitted.code == "BUSY":
    self._ensure_admission_waiter()
    return
```

**关键语句理解**

全局 Capacity Pressure 延迟 Socket 消费，但不会虚构 Redis Command Failure。

??? note "文件差异：src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index cf78913c77efef0c5ce568148ed7b2e0adfbeee0..82d59947fc55c6352ba6292cb9142cf5a8d55135 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -7,7 +7,7 @@ from dataclasses import dataclass
     from enum import Enum
     from typing import Any, Self

    -from miniredis.adapters.direct import DirectClient
    +from miniredis.adapters.direct import DirectClient, DirectPipeline
     from miniredis.clock import (
         AsyncioTimerScheduler,
         Clock,
    @@ -29,6 +29,7 @@ from miniredis.core.executor import (
         CommitBarrier,
         NullCommitBarrier,
         SessionClosed,
    +    SubmittedRequest,
     )
     from miniredis.core.expiration import ActiveExpireProducer
     from miniredis.core.outbound import (
    @@ -348,6 +349,9 @@ class MiniRedis:
             self.executor.register_endpoint(endpoint)
             return DirectClient(self, endpoint)

    +    def direct_pipeline(self) -> DirectPipeline:
    +        return DirectPipeline(self.direct_client())
    +
         def _session_became_slow(self, session_id: int, reason: str) -> None:
             endpoint = self.executor.endpoint(session_id)
             if endpoint is not None:
    @@ -385,25 +389,39 @@ class MiniRedis:
             session_id: int,
             request: CommandRequest,
         ) -> None:
    -        parsed = self.parse(request)
    +        submitted = self.submit_request(session_id, request)
             endpoint = self.executor.endpoint(session_id)
             if endpoint is None:
                 return
    -        if isinstance(parsed, Failure):
    -            token = self.executor.new_request_token()
    -            endpoint.offer(ReplyMessage(token, parsed))
    -            return
    -        submitted = self.executor.submit(session_id, parsed)
             if isinstance(submitted, Failure):
                 token = self.executor.new_request_token()
                 endpoint.offer(ReplyMessage(token, submitted))
                 return
    +        await self.wait_for_session_submission(submitted)
    +
    +    def submit_request(
    +        self,
    +        session_id: int,
    +        request: CommandRequest,
    +    ) -> SubmittedRequest | Failure:
    +        parsed = self.parse(request)
    +        if isinstance(parsed, Failure):
    +            return self.executor.submit_rejection(session_id, parsed)
    +        return self.executor.submit(session_id, parsed)
    +
    +    async def wait_for_session_submission(
    +        self,
    +        submitted: SubmittedRequest,
    +    ) -> None:
             try:
                 await asyncio.shield(submitted.future)
             except asyncio.CancelledError:
                 self.executor.post_control(AbandonRequest(submitted.token))
                 raise

    +    async def wait_for_submission_capacity(self) -> bool:
    +        return await self.executor.wait_for_submission_capacity()
    +
         async def start_tcp(self, host: str, port: int) -> Any:
             from miniredis.adapters.tcp import TcpServer

    ```

**是什么，为什么出现**

Runtime 为两个 Adapter 集中 Request Submission 与等待。

**运行时角色**

把 Parse Failure 转成 Executor Rejection，暴露 Capacity Waiting，并在 Session Task 取消时 Abandon Accepted Token。

**关键代码**

```python
if isinstance(parsed, Failure):
    return self.executor.submit_rejection(session_id, parsed)
```

**关键语句理解**

非法 Syntax 进入与合法 Command 相同的 Mailbox/Token Sequence，因此保留精确 Pipeline 位置。

#### Pipeline 公共接口

通过 Package 暴露 DirectPipeline，同时让普通 Direct-client 语义继续作为其可复用原语。

??? note "文件差异：src/miniredis/__init__.py"
    ```diff
    diff --git a/src/miniredis/__init__.py b/src/miniredis/__init__.py
    index e6e2ed4810e520b4749fc6f2e90aed9d29a7703b..02570d909b840aa544ccda4ba82632e3ce144160 100644
    --- a/src/miniredis/__init__.py
    +++ b/src/miniredis/__init__.py
    @@ -1,9 +1,11 @@
    +from miniredis.adapters.direct import DirectPipeline
     from miniredis.commands.request import CommandRequest
     from miniredis.config import MiniRedisConfig
     from miniredis.runtime import MiniRedis, RuntimeState

     __all__ = [  # noqa: RUF022 - keep the documented public order
         "CommandRequest",
    +    "DirectPipeline",
         "MiniRedisConfig",
         "MiniRedis",
         "RuntimeState",
    ```

**是什么，为什么出现**

Package 把 `DirectPipeline` 暴露为有文档的学习者接口。

**运行时角色**

只改变 Public Import Surface；创建仍经过 `MiniRedis.direct_pipeline()`。

**关键代码**

```python
from miniredis.adapters.direct import DirectPipeline
```

**关键语句理解**

Export 明确新 Adapter 能力，却不复制其 Ownership Factory。

### 验证证据

运行 `tests.txt` 中两个聚焦 Adapter 模块，累计构建 Stage 1–22，并要求 Owned-tree 与 `0016059` 一致。

### 需要真正记住的内容

- Pipeline Admission Order 与 Completion Time 是不同维度。
- Reply Bundle 只从已完成 Token Prefix Flush。
- Parse Failure 占据正常有序 Request Slot。
- Direct Pipeline 有序但不原子。

### 用自己的话讲清楚

为什么 PING 可以在更早 BLPOP 等待时执行，但 Reply 不能先出现？为什么 Executor BUSY 对 TCP 要重试而不是发给 Client？

### 教材

Endpoint 是 Reorder Buffer，类似并发工作必须按 Program Order Retire 的系统。Token 是 Sequence Number；Completion 可以乱序，而 Publication 必须有序退休。

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/40d00de...0016059)

完成后可运行 `python -m journey.tools.build_journey check 22` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/22-ordered-pipelines/stage.patch)

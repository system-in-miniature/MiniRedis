# Stage 10 · Ordered outbox and slow sessions

### Goal

Give every session one bounded ordered output channel with explicit graceful-close and overflow behavior.

??? note "Deliverable files"
    - `src/miniredis/adapters/direct.py`
    - `src/miniredis/config.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/outbound.py`
    - `src/miniredis/runtime.py`
    - `tests/unit/core/test_outbound.py`

### The problem at this point

Request Futures can return direct replies, but later Pub/Sub and TCP work need unsolicited output in the same per-session order. An unbounded queue lets one slow consumer exhaust memory; a sentinel-based close can displace accepted output or require reserved capacity.

### Test contract

#### See the failure first

Graceful close must drain two accepted messages before reporting closure. Overflow must discard pending output, request transport close once, and never let a best-effort notice displace already accepted data.

??? note "File diff: tests/unit/core/test_outbound.py"
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

**What this test locks**

It locks FIFO drain, sentinel-free close, destructive overflow, single overflow notification, and non-displacing best-effort output.

**How it constructs the counterexample**

It fills tiny capacities of one or two, closes before receiving, then attempts one additional normal or best-effort offer.

**Key test statement**

```python
assert outbox.pending_count == 0
assert overflows == 1
```

**What a failure means**

Accepted order was lost, slow-session cleanup repeated, or lifecycle output consumed capacity promised to user-visible messages.

### Basic concepts

An outbox is the sole ordered stream from runtime to one session. `begin_close` is graceful: stop accepting but drain buffered items. `abort` is destructive: discard pending items and wake receivers. Overflow is a transport failure, not backpressure on the global executor.

### Why this mechanism is necessary

Replies and future unsolicited messages must share transport ordering. Bounding each session isolates slow consumers, while explicit close state avoids magic values in the output type and gives shutdown a precise drain contract.

### Runtime mental model

The runtime allocates a monotonic session ID and registers one `SessionEndpoint`. The executor owns that registry and offers output only through the endpoint. A receiver pops FIFO items. If capacity is exhausted, the outbox aborts, requests transport closure once, and posts `SessionClosed` so all accepted requests for that session terminate as `TransportClosed`.

### Mechanism blocks

#### Session-backed Direct client

Bind each Direct client to a registered endpoint so replies, unsolicited output, and close share one session identity.

??? note "File diff: src/miniredis/adapters/direct.py"
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

**What it is and why it appears**

The Direct client now carries a registered endpoint instead of only a numeric session ID.

**Runtime role**

Normal execute still awaits request outcome, while `receive` consumes the same endpoint stream future push-style output will use.

**Key code**

```python
async def receive(self) -> Outbound:
    return await self.endpoint.receive()
```

**Statement understanding**

The adapter does not create a second queue or reorder output; it delegates to the session-owned stream.

#### Bounded close-aware outbox

Preserve accepted output order, drain graceful closes without sentinels, and abort a slow session on overflow.

??? note "File diff: src/miniredis/core/outbound.py"
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

**What it is and why it appears**

This module adds typed outbound messages, a close-aware bounded queue, and the endpoint wrapper that owns transport callbacks.

**Runtime role**

It preserves FIFO order, distinguishes graceful close from abort, and turns the first overflow into exactly one slow-session signal.

**Key code**

```python
if len(self._items) == self._capacity:
    self.abort("outbox full")
```

**Statement understanding**

Overflow invalidates the session stream; dropping only the newest item would leave the transport with an unknowable partial history.

#### Executor-owned endpoints

Register endpoints with the single writer and resolve accepted requests when their transport closes or outbox rejects output.

??? note "File diff: src/miniredis/core/executor.py"
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

**What it is and why it appears**

The single writer also owns session registration and the mapping from accepted requests to endpoints.

**Runtime role**

It offers replies when configured, removes closed endpoints, and resolves every still-owned request for that session.

**Key code**

```python
for token, request in tuple(self._requests.items()):
    if request.session_id == session_id:
        self._finish_request(token, TransportClosed())
```

**Statement understanding**

Session closure has finite ownership scope: all and only requests correlated with that session receive the transport terminal outcome.

#### Session capacity and runtime wiring

Validate one outbox bound, allocate monotonic session IDs, and route slow-session closure back through executor control.

??? note "File diff: src/miniredis/config.py"
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

**What it is and why it appears**

Configuration gains one positive per-session output capacity.

**Runtime role**

Every endpoint receives the same validated default unless a deployment chooses a different bound.

**Key code**

```python
if self.outbox_limit <= 0:
    raise ValueError("outbox_limit must be positive")
```

**Statement understanding**

Zero capacity cannot preserve the promise that an accepted output can be queued, so it is rejected at construction.

??? note "File diff: src/miniredis/runtime.py"
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

**What it is and why it appears**

The runtime becomes the session factory and routes slow-session signals back to executor control.

**Runtime role**

It allocates IDs, builds endpoints with configured capacity, registers them, and reports session counts in lifecycle evidence.

**Key code**

```python
self.executor.register_endpoint(endpoint)
return DirectClient(self, endpoint)
```

**Statement understanding**

Registration happens before the client escapes, so no request can reference a session unknown to the executor.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/10-ordered-outbox/tests.txt)`. The focused set combines queue-unit contracts with the existing slow-endpoint concurrency evidence.

### Durable takeaways

One session owns one ordered stream; bound it; separate graceful drain from destructive abort; never reserve a magic sentinel slot; close a slow transport once; finish every correlated request explicitly.

### Explain it in your own words

The outbox is not just a queue. It is the ordering and lifecycle contract for a session: accepted messages leave in order, graceful closure drains them, and overflow invalidates only that slow session without blocking the global executor.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/02-architecture.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/319d14a...5436512)

After finishing, run `python -m journey.tools.build_journey check 10` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/10-ordered-outbox/stage.patch)

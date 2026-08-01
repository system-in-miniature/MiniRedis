# Stage 09 · Request ownership and terminal outcomes

### Goal

Give every accepted request a runtime-owned lifetime from admission to one typed terminal outcome.

??? note "Deliverable files"
    - `src/miniredis/adapters/direct.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/mailbox.py`
    - `src/miniredis/core/outbound.py`
    - `src/miniredis/runtime.py`
    - `tests/concurrency/test_request_ownership.py`

### The problem at this point

The executor serializes commits, but a caller can cancel while its request is queued. If cancellation owns the shared Future, the runtime loses the only completion channel even though the command may still commit. Shutdown and transport loss also need distinguishable outcomes rather than one generic exception.

### Test contract

#### See the failure first

Canceling a caller while the executor is paused must not cancel the accepted request or reuse its token. After resume, the mutation still commits, every owned Future reaches exactly one terminal state, and runtime statistics return to zero.

??? note "File diff: tests/concurrency/test_request_ownership.py"
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

**What this test locks**

It locks runtime-unique monotonic tokens, cancellation shielding, post-cancel commit behavior, and complete request cleanup.

**How it constructs the counterexample**

It pauses the executor, waits until INCR is admitted, cancels only the caller task, resumes the mailbox, and observes the resulting value plus ownership counters.

**Key test statement**

```python
assert stats.accepted_requests == 0
assert stats.pending_futures == 0
```

**What a failure means**

Caller lifetime leaked into runtime ownership, an accepted request became orphaned, or a token/Future was completed more or less than once.

### Basic concepts

Admission transfers request ownership to MiniRedis. A `RequestToken` is correlation identity; the executor-owned Future is the completion slot; `RequestOutcome` is a closed terminal vocabulary. Caller cancellation requests abandonment but cannot directly cancel the owned slot.

### Why this mechanism is necessary

Commit and client-await lifetimes are different. Keeping them separate makes cancellation, shutdown, transport closure, and internal failure explicit while preserving one ordered owner for state changes. It also prevents invisible pending Futures from accumulating.

### Runtime mental model

The adapter parses and submits a request, then shields its Future. The executor records token, request, and Future before mailbox admission. Cancellation posts `AbandonRequest` into the control lane. Whichever ordered event owns completion first removes the request and sets exactly one typed outcome; terminal cleanup finishes every remaining token.

### Mechanism blocks

#### Caller cancellation boundary

Shield executor-owned completion and translate caller cancellation into an ordered abandonment control message.

??? note "File diff: src/miniredis/adapters/direct.py"
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

**What it is and why it appears**

The Direct adapter becomes a lifetime boundary rather than the owner of parsing and completion state.

**Runtime role**

It shields the runtime Future, posts abandonment after caller cancellation, and maps typed terminal outcomes to Direct-client results.

**Key code**

```python
except asyncio.CancelledError:
    self._runtime.executor.post_control(AbandonRequest(submitted.token))
    raise
```

**Statement understanding**

The caller still receives cancellation immediately, but cleanup becomes an ordered executor event instead of mutating shared ownership from outside.

#### Runtime-owned request outcomes

Give every accepted request a monotonic token, one runtime-owned Future, and exactly one terminal outcome.

??? note "File diff: src/miniredis/core/executor.py"
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

**What it is and why it appears**

The executor now owns the complete accepted-request registry and its monotonic correlation sequence.

**Runtime role**

It records before admission, dispatches commands and controls in mailbox order, and removes each request through one `_finish_request` gate.

**Key code**

```python
message = self._requests.pop(token, None)
if message is None:
    return False
```

**Statement understanding**

Pop is the single ownership transfer to terminal state. A later competing event observes absence and cannot complete the Future again.

??? note "File diff: src/miniredis/core/outbound.py"
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

**What it is and why it appears**

This module defines request correlation and the closed set of terminal outcomes independently of a transport adapter.

**Runtime role**

Adapters pattern-match `Replied`, `Abandoned`, `TransportClosed`, `RuntimeClosed`, or `RuntimeFailed` without inferring cause from exceptions.

**Key code**

```python
RequestOutcome: TypeAlias = (
    Replied | Abandoned | TransportClosed | RuntimeClosed | RuntimeFailed
)
```

**Statement understanding**

The union makes every terminal cause explicit and forces new lifecycle cases to be handled at consumers.

#### User and control admission lanes

Bound user pressure while keeping lifecycle and abandonment controls admissible during shutdown.

??? note "File diff: src/miniredis/core/mailbox.py"
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

**What it is and why it appears**

The mailbox distinguishes bounded user admission from internal control admission.

**Runtime role**

User commands consume capacity; abandonment and shutdown controls can still enter after user admission closes.

**Key code**

```python
def post_control(self, item: T) -> bool:
    if not self._control_open:
        return False
```

**Statement understanding**

Closing user pressure is not the same as closing lifecycle coordination. The control lane stays available until terminal cleanup is safe.

#### Runtime parsing and lifecycle evidence

Centralize parse ownership and expose narrow predicates that observe accepted-request cleanup without timing sleeps.

??? note "File diff: src/miniredis/runtime.py"
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

**What it is and why it appears**

The runtime centralizes parse ownership and exposes predicate-based lifecycle diagnostics.

**Runtime role**

It constructs the executor's debug notification boundary and waits for queued or idle states without scheduler sleeps.

**Key code**

```python
async def debug_wait_until_idle(self) -> None:
    await self._debug_wait(lambda: self.executor.idle)
```

**Statement understanding**

Tests wait for an owned state transition, not an estimated amount of wall time, so concurrency evidence is causal.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/09-request-outcomes/tests.txt)`. It proves token uniqueness and cancellation cleanup around the real executor mailbox and public Direct adapter.

### Durable takeaways

Admission transfers ownership; shield runtime Futures; represent terminal causes as data; complete through one pop gate; keep control admission separate; observe concurrency with predicates rather than sleeps.

### Explain it in your own words

A canceled waiter and an accepted command are not the same lifetime. MiniRedis owns the command after admission, so the adapter can stop waiting while the executor still deterministically commits or abandons it and closes its Future exactly once.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/02-architecture.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/7628635...319d14a)

After finishing, run `python -m journey.tools.build_journey check 9` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/09-request-outcomes/stage.patch)

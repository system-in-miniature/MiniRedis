# Stage 03 · Serialized Direct executor

### Goal

Create a bounded Direct-first runtime whose single executor owns command and lifecycle order.

??? note "Deliverable files"
    - `src/miniredis/__init__.py`
    - `src/miniredis/adapters/direct.py`
    - `src/miniredis/clock.py`
    - `src/miniredis/config.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/mailbox.py`
    - `src/miniredis/core/planner.py`
    - `src/miniredis/runtime.py`
    - `tests/concurrency/test_direct_executor.py`

### The problem at this point

Typed commands still have no owner. If every caller reads and writes the database directly, concurrent requests can allocate the same next sequence, shutdown can race accepted work, and a full command queue can prevent the control message needed to close it.

### Test contract

#### See the failure first

The contract pauses an executor with capacity one, submits a first request, observes a second request receive `BUSY`, and then posts control work that must still resume and close the runtime. Failure-cleanup cases also inject planner and barrier exceptions and require every accepted caller to reach one terminal outcome.

??? note "File diff: tests/concurrency/test_direct_executor.py"
    ```diff
    diff --git a/tests/concurrency/test_direct_executor.py b/tests/concurrency/test_direct_executor.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a5e364e8f67956d943055fec0fe7a3d149067b73
    --- /dev/null
    +++ b/tests/concurrency/test_direct_executor.py
    @@ -0,0 +1,503 @@
    +from __future__ import annotations
    +
    +import asyncio
    +from collections.abc import Coroutine
    +from dataclasses import FrozenInstanceError
    +from typing import Any
    +
    +import pytest
    +
    +import miniredis.core.executor as executor_module
    +from miniredis import CommandRequest, MiniRedis, MiniRedisConfig, RuntimeState
    +from miniredis.commands.model import Command, Ping
    +from miniredis.core.commit import PutEntry, StoredEntry, StoredString
    +from miniredis.core.database import Database
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.reply import Bytes, Failure, Ok
    +
    +
    +class SentinelFailure(RuntimeError):
    +    pass
    +
    +
    +class FailingPlanner:
    +    def __init__(self, failure: SentinelFailure) -> None:
    +        self.failure = failure
    +        self.entered = asyncio.Event()
    +
    +    def plan(self, database: Database, command: Command, now_ms: int) -> ExecutionPlan:
    +        del database, command, now_ms
    +        self.entered.set()
    +        raise self.failure
    +
    +
    +class PutPlanner:
    +    def plan(self, database: Database, command: Command, now_ms: int) -> ExecutionPlan:
    +        del database, command, now_ms
    +        return ExecutionPlan(
    +            Ok(b"planned"),
    +            operations=(
    +                PutEntry(
    +                    b"key",
    +                    StoredEntry(
    +                        StoredString(b"value"), expire_at_ms=None, mutation_version=1
    +                    ),
    +                ),
    +            ),
    +        )
    +
    +
    +class FailingBarrier:
    +    def __init__(self, failure: SentinelFailure) -> None:
    +        self.failure = failure
    +        self.entered = asyncio.Event()
    +
    +    async def append(self, batch: object) -> None:
    +        del batch
    +        self.entered.set()
    +        raise self.failure
    +
    +
    +class GatedFailingBarrier(FailingBarrier):
    +    def __init__(self, failure: SentinelFailure) -> None:
    +        super().__init__(failure)
    +        self.release = asyncio.Event()
    +
    +    async def append(self, batch: object) -> None:
    +        del batch
    +        self.entered.set()
    +        await self.release.wait()
    +        raise self.failure
    +
    +
    +def test_config_defaults_are_frozen_and_reject_invalid_bounds_and_policy() -> None:
    +    config = MiniRedisConfig()
    +
    +    assert config.max_pending_commands == 1024
    +    assert config.active_expire_sample_size == 20
    +    assert config.maxmemory is None
    +    assert config.eviction_policy == "noeviction"
    +    with pytest.raises(FrozenInstanceError):
    +        config.max_pending_commands = 1  # type: ignore[misc]
    +
    +    for options in (
    +        {"max_pending_commands": 0},
    +        {"active_expire_sample_size": 0},
    +        {"maxmemory": 0},
    +        {"eviction_policy": "volatile-lru"},
    +    ):
    +        with pytest.raises(ValueError):
    +            MiniRedisConfig(**options)  # type: ignore[arg-type]
    +
    +
    +def test_open_rejects_config_mixed_with_keyword_options() -> None:
    +    with pytest.raises(TypeError):
    +        MiniRedis.open(MiniRedisConfig(), max_pending_commands=1)
    +
    +
    +@pytest.mark.asyncio
    +async def test_runtime_context_executes_ping_and_binary_echo_then_closes() -> None:
    +    runtime = MiniRedis.open()
    +
    +    async with runtime:
    +        assert runtime.state is RuntimeState.RUNNING
    +        client = runtime.direct_client()
    +        assert await client.execute(CommandRequest(b"PING")) == Ok(b"PONG")
    +        assert await client.execute(
    +            CommandRequest(b"ECHO", (b"\x00binary\xff",))
    +        ) == Bytes(b"\x00binary\xff")
    +        await client.close()
    +        assert client.closed is True
    +        await client.close()
    +        assert client.closed is True
    +
    +    assert runtime.state is RuntimeState.CLOSED
    +    await runtime.close()
    +    assert runtime.state is RuntimeState.CLOSED
    +
    +
    +@pytest.mark.asyncio
    +async def test_execute_maps_inactive_client_and_parse_errors() -> None:
    +    runtime = MiniRedis.open()
    +    client = runtime.direct_client()
    +
    +    assert await client.execute(CommandRequest(b"PING")) == Failure(
    +        "CLOSED", "runtime is closed"
    +    )
    +    await runtime.start()
    +    assert await client.execute(CommandRequest(b"PING", (b"a", b"b"))) == Failure(
    +        "ERR", "wrong number of arguments for PING"
    +    )
    +
    +    await client.close()
    +    assert await client.execute(CommandRequest(b"PING")) == Failure(
    +        "CLOSED", "client is closed"
    +    )
    +    await runtime.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_bounded_admission_returns_busy_without_blocking_control_close() -> None:
    +    runtime = MiniRedis.open(max_pending_commands=1)
    +    await runtime.start()
    +    runtime.debug_pause_executor()
    +    client = runtime.direct_client()
    +
    +    first = asyncio.create_task(client.execute(CommandRequest(b"PING")))
    +    await runtime.debug_wait_accepted_at_least(1)
    +
    +    assert await client.execute(CommandRequest(b"PING")) == Failure(
    +        "BUSY", "command queue is full"
    +    )
    +
    +    runtime.debug_resume_executor()
    +    assert await first == Ok(b"PONG")
    +    await runtime.close()
    +    assert runtime.state is RuntimeState.CLOSED
    +
    +
    +@pytest.mark.asyncio
    +async def test_concurrent_close_is_idempotent() -> None:
    +    runtime = MiniRedis.open()
    +    await runtime.start()
    +
    +    await asyncio.gather(runtime.close(), runtime.close(), runtime.close())
    +
    +    assert runtime.state is RuntimeState.CLOSED
    +
    +
    +@pytest.mark.asyncio
    +async def test_planner_failure_terminally_cleans_all_accepted_requests() -> None:
    +    failure = SentinelFailure("planner failed")
    +    planner = FailingPlanner(failure)
    +    runtime = MiniRedis.open(max_pending_commands=3)
    +    runtime.executor.planner = planner
    +    await runtime.start()
    +    runtime.debug_pause_executor()
    +    client = runtime.direct_client()
    +    callers = tuple(
    +        asyncio.create_task(client.execute(CommandRequest(b"PING"))) for _ in range(3)
    +    )
    +    await runtime.debug_wait_accepted_at_least(3)
    +
    +    runtime.debug_resume_executor()
    +    await planner.entered.wait()
    +    outcomes = await asyncio.gather(*callers)
    +
    +    assert runtime.state is RuntimeState.CLOSED
    +    assert outcomes == [Failure("CLOSED", "runtime closed before reply")] * 3
    +    assert runtime.executor.debug_failure is failure
    +    assert runtime.executor.debug_accepted_count == 0
    +    assert runtime.executor.mailbox.pending_users == 0
    +    assert runtime.executor.mailbox.pending_items == 0
    +    assert runtime.executor.mailbox.post_control(object()) is False
    +    assert await client.execute(CommandRequest(b"PING")) == Failure(
    +        "CLOSED", "runtime is closed"
    +    )
    +    await runtime.close()
    +    await runtime.close()
    +    assert runtime.state is RuntimeState.CLOSED
    +
    +
    +@pytest.mark.asyncio
    +async def test_barrier_failure_does_not_apply_and_terminally_cleans_runtime() -> None:
    +    failure = SentinelFailure("barrier failed")
    +    barrier = FailingBarrier(failure)
    +    runtime = MiniRedis._for_test(max_pending_commands=2, commit_barrier=barrier)
    +    runtime.executor.planner = PutPlanner()
    +    await runtime.start()
    +    runtime.debug_pause_executor()
    +    client = runtime.direct_client()
    +    callers = tuple(
    +        asyncio.create_task(client.execute(CommandRequest(b"PING"))) for _ in range(2)
    +    )
    +    await runtime.debug_wait_accepted_at_least(2)
    +
    +    runtime.debug_resume_executor()
    +    await barrier.entered.wait()
    +    outcomes = await asyncio.gather(*callers)
    +
    +    assert runtime.state is RuntimeState.CLOSED
    +    assert outcomes == [Failure("CLOSED", "runtime closed before reply")] * 2
    +    assert runtime.database.commit_seq == 0
    +    assert runtime.database.logical_items() == ()
    +    assert runtime.executor.debug_failure is failure
    +    assert runtime.executor.debug_accepted_count == 0
    +    assert runtime.executor.mailbox.pending_users == 0
    +    assert runtime.executor.mailbox.pending_items == 0
    +    assert runtime.executor.mailbox.post_control(object()) is False
    +    assert await client.execute(CommandRequest(b"PING")) == Failure(
    +        "CLOSED", "runtime is closed"
    +    )
    +    await runtime.close()
    +    await runtime.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_cancelled_close_waiter_does_not_abandon_failure_cleanup() -> None:
    +    failure = SentinelFailure("gated barrier failed")
    +    barrier = GatedFailingBarrier(failure)
    +    runtime = MiniRedis._for_test(max_pending_commands=1, commit_barrier=barrier)
    +    runtime.executor.planner = PutPlanner()
    +    await runtime.start()
    +    client = runtime.direct_client()
    +    caller = asyncio.create_task(client.execute(CommandRequest(b"PING")))
    +    await barrier.entered.wait()
    +
    +    close_waiter = asyncio.create_task(runtime.close())
    +    await runtime.executor.mailbox.wait_items_at_least(1)
    +    assert runtime.state is RuntimeState.DRAINING
    +    close_waiter.cancel()
    +    with pytest.raises(asyncio.CancelledError):
    +        await close_waiter
    +
    +    barrier.release.set()
    +    assert await caller == Failure("CLOSED", "runtime closed before reply")
    +    await runtime.close()
    +
    +    assert runtime.state is RuntimeState.CLOSED
    +    assert runtime.executor.debug_failure is failure
    +    assert runtime.executor.debug_accepted_count == 0
    +    assert runtime.executor.mailbox.pending_users == 0
    +    assert runtime.executor.mailbox.pending_items == 0
    +    assert runtime.executor.mailbox.post_control(object()) is False
    +
    +
    +@pytest.mark.asyncio
    +async def test_owned_worker_cancellation_terminally_cleans_runtime() -> None:
    +    runtime = MiniRedis.open(max_pending_commands=3)
    +    await runtime.start()
    +    runtime.debug_pause_executor()
    +    client = runtime.direct_client()
    +    callers = tuple(
    +        asyncio.create_task(client.execute(CommandRequest(b"PING"))) for _ in range(3)
    +    )
    +    await runtime.debug_wait_accepted_at_least(3)
    +    worker = runtime.executor._worker_task
    +    assert worker is not None
    +    cancellation_marker = object()
    +
    +    assert worker.cancel(cancellation_marker) is True
    +    outcomes = await asyncio.gather(*callers)
    +
    +    assert runtime.state is RuntimeState.CLOSED
    +    assert outcomes == [Failure("CLOSED", "runtime closed before reply")] * 3
    +    failure = runtime.executor.debug_failure
    +    assert isinstance(failure, asyncio.CancelledError)
    +    assert failure.args == (cancellation_marker,)
    +    assert worker.cancelled() is False
    +    assert runtime.executor.debug_accepted_count == 0
    +    assert runtime.executor.mailbox.pending_users == 0
    +    assert runtime.executor.mailbox.pending_items == 0
    +    assert runtime.executor.mailbox.admit_user(object()) is False
    +    assert runtime.executor.mailbox.post_control(object()) is False
    +    assert await client.execute(CommandRequest(b"PING")) == Failure(
    +        "CLOSED", "runtime is closed"
    +    )
    +    await runtime.close()
    +    await runtime.close()
    +    assert runtime.state is RuntimeState.CLOSED
    +
    +
    +@pytest.mark.asyncio
    +async def test_start_handshake_rejects_submit_while_worker_is_cancelling() -> None:
    +    runtime = MiniRedis.open()
    +    await runtime.start()
    +    worker = runtime.executor._worker_task
    +    assert worker is not None
    +    cancellation_marker = object()
    +
    +    assert worker.cancel(cancellation_marker) is True
    +    submitted = runtime.executor.submit(session_id=1, command=Ping())
    +
    +    assert submitted == Failure("CLOSED", "runtime is closed")
    +    await worker
    +    assert runtime.state is RuntimeState.CLOSED
    +    failure = runtime.executor.debug_failure
    +    assert isinstance(failure, asyncio.CancelledError)
    +    assert failure.args == (cancellation_marker,)
    +    assert runtime.executor.debug_accepted_count == 0
    +    assert runtime.executor.mailbox.pending_users == 0
    +    assert runtime.executor.mailbox.pending_items == 0
    +    assert runtime.executor.mailbox.admit_user(object()) is False
    +    assert runtime.executor.mailbox.post_control(object()) is False
    +    await runtime.close()
    +    await runtime.close()
    +    assert runtime.state is RuntimeState.CLOSED
    +
    +
    +@pytest.mark.asyncio
    +async def test_pre_entry_worker_cancellation_is_owned_by_done_supervision(
    +    monkeypatch: pytest.MonkeyPatch,
    +) -> None:
    +    runtime = MiniRedis.open()
    +    client = runtime.direct_client()
    +    worker_created = asyncio.Event()
    +    worker_entry_gate = asyncio.Event()
    +    original_create_task = asyncio.create_task
    +
    +    def controlled_create_task(
    +        coroutine: Coroutine[Any, Any, None],
    +        *,
    +        name: str | None = None,
    +        context: Any = None,
    +    ) -> asyncio.Task[None]:
    +        if name != "miniredis:executor":
    +            return original_create_task(coroutine, name=name, context=context)
    +
    +        async def delayed_worker_entry() -> None:
    +            worker_created.set()
    +            try:
    +                await worker_entry_gate.wait()
    +            except asyncio.CancelledError:
    +                coroutine.close()
    +                raise
    +            await coroutine
    +
    +        return original_create_task(delayed_worker_entry(), name=name, context=context)
    +
    +    monkeypatch.setattr(executor_module.asyncio, "create_task", controlled_create_task)
    +    first_start = original_create_task(runtime.start())
    +    await worker_created.wait()
    +    worker = runtime.executor._worker_task
    +    assert worker is not None
    +    assert runtime.executor._worker_entered is False
    +
    +    first_start.cancel()
    +    with pytest.raises(asyncio.CancelledError):
    +        await first_start
    +    cancellation_marker = object()
    +    assert worker.cancel(cancellation_marker) is True
    +    await runtime.executor._worker_started_or_done.wait()
    +    monkeypatch.setattr(executor_module.asyncio, "create_task", original_create_task)
    +
    +    assert runtime.state is RuntimeState.CLOSED
    +    failure = runtime.executor.debug_failure
    +    assert isinstance(failure, asyncio.CancelledError)
    +    assert failure.args == (cancellation_marker,)
    +    assert runtime.executor.debug_accepted_count == 0
    +    assert runtime.executor.mailbox.pending_users == 0
    +    assert runtime.executor.mailbox.pending_items == 0
    +    assert runtime.executor.mailbox.admit_user(object()) is False
    +    assert runtime.executor.mailbox.post_control(object()) is False
    +    with pytest.raises(RuntimeError, match="runtime is closed"):
    +        await runtime.start()
    +    assert await client.execute(CommandRequest(b"PING")) == Failure(
    +        "CLOSED", "runtime is closed"
    +    )
    +    await runtime.close()
    +    await runtime.close()
    +    assert runtime.state is RuntimeState.CLOSED
    +
    +
    +@pytest.mark.asyncio
    +async def test_cancelled_start_waiter_does_not_orphan_runtime_starting(
    +    monkeypatch: pytest.MonkeyPatch,
    +) -> None:
    +    runtime = MiniRedis.open()
    +    client = runtime.direct_client()
    +    worker_created = asyncio.Event()
    +    worker_entry_gate = asyncio.Event()
    +    original_create_task = asyncio.create_task
    +
    +    def controlled_create_task(
    +        coroutine: Coroutine[Any, Any, None],
    +        *,
    +        name: str | None = None,
    +        context: Any = None,
    +    ) -> asyncio.Task[None]:
    +        if name != "miniredis:executor":
    +            return original_create_task(coroutine, name=name, context=context)
    +
    +        async def delayed_worker_entry() -> None:
    +            worker_created.set()
    +            try:
    +                await worker_entry_gate.wait()
    +            except asyncio.CancelledError:
    +                coroutine.close()
    +                raise
    +            await coroutine
    +
    +        return original_create_task(delayed_worker_entry(), name=name, context=context)
    +
    +    monkeypatch.setattr(executor_module.asyncio, "create_task", controlled_create_task)
    +    first_start = original_create_task(runtime.start())
    +    await worker_created.wait()
    +    worker = runtime.executor._worker_task
    +    assert worker is not None
    +    assert runtime.executor._worker_entered is False
    +
    +    first_start.cancel()
    +    with pytest.raises(asyncio.CancelledError):
    +        await first_start
    +    worker_entry_gate.set()
    +    await runtime.executor._worker_started_or_done.wait()
    +    monkeypatch.setattr(executor_module.asyncio, "create_task", original_create_task)
    +
    +    assert runtime.state is RuntimeState.RUNNING
    +    live_workers = tuple(
    +        task
    +        for task in asyncio.all_tasks()
    +        if task.get_name() == "miniredis:executor" and not task.done()
    +    )
    +    assert live_workers == (worker,)
    +    assert await client.execute(CommandRequest(b"PING")) == Ok(b"PONG")
    +    await runtime.close()
    +    assert runtime.state is RuntimeState.CLOSED
    +    assert runtime.executor.debug_accepted_count == 0
    +    assert runtime.executor.mailbox.pending_users == 0
    +    assert runtime.executor.mailbox.pending_items == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_concurrent_start_waiters_share_startup_when_one_is_cancelled(
    +    monkeypatch: pytest.MonkeyPatch,
    +) -> None:
    +    runtime = MiniRedis.open()
    +    worker_created = asyncio.Event()
    +    worker_entry_gate = asyncio.Event()
    +    original_create_task = asyncio.create_task
    +
    +    def controlled_create_task(
    +        coroutine: Coroutine[Any, Any, None],
    +        *,
    +        name: str | None = None,
    +        context: Any = None,
    +    ) -> asyncio.Task[None]:
    +        if name != "miniredis:executor":
    +            return original_create_task(coroutine, name=name, context=context)
    +
    +        async def delayed_worker_entry() -> None:
    +            worker_created.set()
    +            try:
    +                await worker_entry_gate.wait()
    +            except asyncio.CancelledError:
    +                coroutine.close()
    +                raise
    +            await coroutine
    +
    +        return original_create_task(delayed_worker_entry(), name=name, context=context)
    +
    +    monkeypatch.setattr(executor_module.asyncio, "create_task", controlled_create_task)
    +    first_start = original_create_task(runtime.start())
    +    second_start = original_create_task(runtime.start())
    +    await worker_created.wait()
    +    worker = runtime.executor._worker_task
    +    assert worker is not None
    +
    +    first_start.cancel()
    +    with pytest.raises(asyncio.CancelledError):
    +        await first_start
    +    worker_entry_gate.set()
    +    await second_start
    +    monkeypatch.setattr(executor_module.asyncio, "create_task", original_create_task)
    +
    +    assert runtime.state is RuntimeState.RUNNING
    +    live_workers = tuple(
    +        task
    +        for task in asyncio.all_tasks()
    +        if task.get_name() == "miniredis:executor" and not task.done()
    +    )
    +    assert live_workers == (worker,)
    +    await runtime.close()
    +    assert runtime.state is RuntimeState.CLOSED
    ```

**What this test locks**

It locks bounded user admission, independent control admission, one owned worker, cancellation-safe start/close, terminal failure cleanup, binary Direct calls, and idempotent lifecycle operations.

**How it constructs the counterexample**

Gates pause the executor at specific ownership boundaries while multiple callers, shutdown, cancellation, or an injected failure race for the next mailbox turn.

**Key test statement**

```python
assert await client.execute(CommandRequest(b"PING")) == Failure(
    "BUSY", "command queue is full"
)
```

**What a failure means**

A failure means accepted work can be orphaned, control work can deadlock behind user pressure, or more than one component can decide runtime order.

### Basic concepts

Serialization means state-affecting events are processed one at a time in a total mailbox order. Admission is separate from execution: rejecting excess user work does not consume a turn. Control messages use the same ordered owner but a separate unbounded-by-user admission path.

An owned task is created, supervised, and terminalized by the runtime. Caller cancellation must not silently cancel shared startup, shutdown, or already-accepted state work.

### Why this mechanism is necessary

Atomic planners are insufficient if multiple tasks can apply them concurrently. One owner makes sequence allocation and state transition indivisible. Bounded admission prevents memory growth; separate control admission guarantees overload cannot prevent cleanup.

### Runtime mental model

The Direct client parses a request and asks the executor to admit it. A user event enters the mailbox with a runtime-unique token. The one worker plans and completes it, while control events close sessions or the runtime in the same total order. The runtime assembles and supervises this owner.

### Mechanism blocks

#### Bounded admission and one state owner

Admit user work through a bounded mailbox while control work remains available, then let one executor own parse, plan, commit, and completion order.

??? note "File diff: src/miniredis/core/mailbox.py"
    ```diff
    diff --git a/src/miniredis/core/mailbox.py b/src/miniredis/core/mailbox.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..fed74e9f740f8ed80d267375df7d2fbe01b946b5
    --- /dev/null
    +++ b/src/miniredis/core/mailbox.py
    @@ -0,0 +1,91 @@
    +from __future__ import annotations
    +
    +import asyncio
    +from collections import deque
    +
    +
    +class EventLoopMailbox[T]:
    +    """A single-event-loop mailbox with separate user and control admission."""
    +
    +    def __init__(self, max_pending_users: int) -> None:
    +        if max_pending_users <= 0:
    +            raise ValueError("max_pending_users must be positive")
    +        self._max_pending_users = max_pending_users
    +        self._items: deque[tuple[bool, T]] = deque()
    +        self._pending_users = 0
    +        self._ready = asyncio.Event()
    +        self._changed = asyncio.Event()
    +        self._user_open = True
    +        self._control_open = True
    +
    +    @property
    +    def pending_users(self) -> int:
    +        return self._pending_users
    +
    +    @property
    +    def pending_items(self) -> int:
    +        return len(self._items)
    +
    +    def admit_user(self, item: T) -> bool:
    +        if not self._user_open or self._pending_users >= self._max_pending_users:
    +            return False
    +        self._items.append((True, item))
    +        self._pending_users += 1
    +        self._ready.set()
    +        self._changed.set()
    +        return True
    +
    +    def post_control(self, item: T) -> bool:
    +        if not self._control_open:
    +            return False
    +        self._items.append((False, item))
    +        self._ready.set()
    +        self._changed.set()
    +        return True
    +
    +    async def take(self) -> T:
    +        while not self._items:
    +            self._ready.clear()
    +            if self._items:
    +                continue
    +            await self._ready.wait()
    +
    +        is_user, item = self._items.popleft()
    +        if is_user:
    +            self._pending_users -= 1
    +            self._changed.set()
    +        return item
    +
    +    async def wait_pending_at_least(self, count: int) -> None:
    +        while self._pending_users < count:
    +            self._changed.clear()
    +            if self._pending_users >= count:
    +                return
    +            await self._changed.wait()
    +
    +    async def wait_items_at_least(self, count: int) -> None:
    +        while len(self._items) < count:
    +            self._changed.clear()
    +            if len(self._items) >= count:
    +                return
    +            await self._changed.wait()
    +
    +    def drain(self) -> tuple[T, ...]:
    +        items = tuple(item for _is_user, item in self._items)
    +        self._items.clear()
    +        self._pending_users = 0
    +        self._ready.clear()
    +        self._changed.set()
    +        return items
    +
    +    def close_user_admission(self) -> None:
    +        self._user_open = False
    +        self._changed.set()
    +
    +    def close_control_admission(self) -> None:
    +        self._control_open = False
    +        self._changed.set()
    +
    +    def close_admissions(self) -> None:
    +        self.close_user_admission()
    +        self.close_control_admission()
    ```

**What it is and why it appears**

`EventLoopMailbox` is a single-loop queue with bounded user slots and independent control admission.

**Runtime role**

It preserves event order, exposes user pressure, and always leaves a path for shutdown until control admission closes.

**Key code**

```python
def admit_user(self, item: T) -> bool:
    if not self._user_open or self._pending_users >= self._max_pending_users:
        return False
```

**Statement understanding**

Capacity rejection occurs before enqueue, so a `BUSY` request never becomes accepted work that later needs terminalization.

??? note "File diff: src/miniredis/core/planner.py"
    ```diff
    diff --git a/src/miniredis/core/planner.py b/src/miniredis/core/planner.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..697ce08de85ee834c15469c9ef6297c65bf2e1da
    --- /dev/null
    +++ b/src/miniredis/core/planner.py
    @@ -0,0 +1,22 @@
    +from __future__ import annotations
    +
    +from miniredis.commands.model import Command, Echo, Ping
    +from miniredis.config import MiniRedisConfig
    +from miniredis.core.database import Database
    +from miniredis.core.executor import ExecutionPlan
    +from miniredis.core.reply import Bytes, Failure, Ok
    +
    +
    +class CommandPlanner:
    +    def __init__(self, config: MiniRedisConfig) -> None:
    +        self.config = config
    +
    +    def plan(self, database: Database, command: Command, now_ms: int) -> ExecutionPlan:
    +        del database, now_ms
    +        match command:
    +            case Ping(message=None):
    +                return ExecutionPlan(Ok(b"PONG"))
    +            case Ping(message=message) | Echo(message=message):
    +                return ExecutionPlan(Bytes(message))
    +            case _:
    +                return ExecutionPlan(Failure("ERR", "unknown command"))
    ```

**What it is and why it appears**

The initial planner handles side-effect-free `PING` and `ECHO` as `ExecutionPlan` values.

**Runtime role**

It demonstrates the future boundary: planning returns a reply and operations rather than mutating the database.

**Key code**

```python
return ExecutionPlan(Ok(b"PONG"))
```

**Statement understanding**

A plan with no operations is a semantic result but not a commit; the executor can reply without advancing state.

??? note "File diff: src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..cb776bb2ea5098f029ece2e3965b59bc10f71c40
    --- /dev/null
    +++ b/src/miniredis/core/executor.py
    @@ -0,0 +1,277 @@
    +from __future__ import annotations
    +
    +import asyncio
    +from collections.abc import Callable
    +from dataclasses import dataclass
    +from typing import Protocol
    +
    +from miniredis.clock import Clock
    +from miniredis.commands.model import Command
    +from miniredis.core.commit import CommitBatch, CommitOperation, CommitTrigger
    +from miniredis.core.database import Database
    +from miniredis.core.mailbox import EventLoopMailbox
    +from miniredis.core.reply import Failure, Reply
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RequestToken:
    +    value: int
    +
    +
    +@dataclass(slots=True)
    +class ExecuteRequest:
    +    token: RequestToken
    +    session_id: int
    +    command: Command
    +    future: asyncio.Future[RequestOutcome]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SubmittedRequest:
    +    token: RequestToken
    +    future: asyncio.Future[RequestOutcome]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Replied:
    +    reply: Reply
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RuntimeClosed:
    +    pass
    +
    +
    +type RequestOutcome = Replied | RuntimeClosed
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ExecutionPlan:
    +    reply: Reply | None
    +    operations: tuple[CommitOperation, ...] = ()
    +    touch_keys: tuple[bytes, ...] = ()
    +    trigger: CommitTrigger = CommitTrigger.CLIENT
    +
    +
    +class CommitBarrier(Protocol):
    +    async def append(self, batch: CommitBatch) -> None: ...
    +
    +
    +class NullCommitBarrier:
    +    async def append(self, batch: CommitBatch) -> None:
    +        del batch
    +
    +
    +class Planner(Protocol):
    +    def plan(
    +        self, database: Database, command: Command, now_ms: int
    +    ) -> ExecutionPlan: ...
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class _StopExecutor:
    +    pass
    +
    +
    +type ExecutorMessage = ExecuteRequest | _StopExecutor
    +
    +
    +class CommandExecutor:
    +    def __init__(
    +        self,
    +        *,
    +        database: Database,
    +        planner: Planner,
    +        clock: Clock,
    +        commit_barrier: CommitBarrier,
    +        max_pending_commands: int,
    +        on_terminal_failure: Callable[[BaseException], None] | None = None,
    +    ) -> None:
    +        self.database = database
    +        self.planner = planner
    +        self.clock = clock
    +        self.commit_barrier = commit_barrier
    +        self.max_pending_commands = max_pending_commands
    +        self.mailbox: EventLoopMailbox[ExecutorMessage] = EventLoopMailbox(
    +            max_pending_commands
    +        )
    +        self._on_terminal_failure = on_terminal_failure
    +
    +        self._worker_task: asyncio.Task[None] | None = None
    +        self._worker_started_or_done = asyncio.Event()
    +        self._worker_entered = False
    +        self._close_task: asyncio.Task[None] | None = None
    +        self._run_gate = asyncio.Event()
    +        self._run_gate.set()
    +        self._next_token = 0
    +        self._accepted: dict[RequestToken, asyncio.Future[RequestOutcome]] = {}
    +        self._accepted_changed = asyncio.Event()
    +        self._applied_batches: list[CommitBatch] = []
    +        self._failure: BaseException | None = None
    +        self._terminal_cleanup_complete = False
    +        self._stopping = False
    +        self._started = False
    +
    +    async def start(self) -> None:
    +        if self._started:
    +            if self._stopping:
    +                raise RuntimeError("executor is stopping")
    +            await self._worker_started_or_done.wait()
    +            return
    +        if self._stopping:
    +            raise RuntimeError("executor is stopping")
    +        self._started = True
    +        self._worker_task = asyncio.create_task(self._run(), name="miniredis:executor")
    +        self._worker_task.add_done_callback(self._on_worker_done)
    +        await self._worker_started_or_done.wait()
    +
    +    def _on_worker_done(self, task: asyncio.Task[None]) -> None:
    +        try:
    +            if not self._worker_entered:
    +                try:
    +                    task.result()
    +                except asyncio.CancelledError as error:
    +                    self._complete_terminal_failure(error)
    +                except Exception as error:  # noqa: BLE001 - startup is terminal
    +                    self._complete_terminal_failure(error)
    +        finally:
    +            self._worker_started_or_done.set()
    +
    +    def submit(self, session_id: int, command: Command) -> SubmittedRequest | Failure:
    +        if (
    +            not self._started
    +            or self._stopping
    +            or (self._worker_task is not None and self._worker_task.cancelling() != 0)
    +            or (self._worker_task is not None and self._worker_task.done())
    +        ):
    +            return Failure("CLOSED", "runtime is closed")
    +        if len(self._accepted) >= self.max_pending_commands:
    +            return Failure("BUSY", "command queue is full")
    +
    +        self._next_token += 1
    +        token = RequestToken(self._next_token)
    +        future: asyncio.Future[RequestOutcome] = (
    +            asyncio.get_running_loop().create_future()
    +        )
    +        request = ExecuteRequest(token, session_id, command, future)
    +        if not self.mailbox.admit_user(request):
    +            return Failure("CLOSED", "runtime is closed")
    +        self._accepted[token] = future
    +        self._accepted_changed.set()
    +        return SubmittedRequest(token, future)
    +
    +    def post_control(self, message: _StopExecutor) -> bool:
    +        return self.mailbox.post_control(message)
    +
    +    async def _run(self) -> None:
    +        failure: BaseException | None = None
    +        self._worker_entered = True
    +        self._worker_started_or_done.set()
    +        try:
    +            while True:
    +                message = await self.mailbox.take()
    +                await self._run_gate.wait()
    +                if isinstance(message, _StopExecutor):
    +                    return
    +                await self._execute(message)
    +        except asyncio.CancelledError as error:
    +            failure = error
    +        except Exception as error:  # noqa: BLE001 - worker failures are terminal
    +            failure = error
    +        finally:
    +            if failure is not None:
    +                self._complete_terminal_failure(failure)
    +            else:
    +                for token in tuple(self._accepted):
    +                    self._finish(token, RuntimeClosed())
    +
    +    def _complete_terminal_failure(self, failure: BaseException) -> None:
    +        if self._terminal_cleanup_complete:
    +            return
    +        self._terminal_cleanup_complete = True
    +        self._failure = failure
    +        self._stopping = True
    +        self.mailbox.close_user_admission()
    +        self.mailbox.drain()
    +        for token in tuple(self._accepted):
    +            self._finish(token, RuntimeClosed())
    +        self.mailbox.close_control_admission()
    +        if self._on_terminal_failure is not None:
    +            self._on_terminal_failure(failure)
    +
    +    async def _execute(self, request: ExecuteRequest) -> None:
    +        now_ms = self.clock.now_ms()
    +        plan = self.planner.plan(self.database, request.command, now_ms)
    +        if plan.operations:
    +            batch = CommitBatch(
    +                seq=self.database.commit_seq + 1,
    +                operations=plan.operations,
    +                trigger=plan.trigger,
    +            )
    +            await self.commit_barrier.append(batch)
    +            self.database.apply_batch(
    +                batch, track_access=plan.trigger is CommitTrigger.CLIENT
    +            )
    +            self._applied_batches.append(batch)
    +
    +        for key in dict.fromkeys(plan.touch_keys):
    +            self.database.touch_if_live(key, now_ms)
    +
    +        if plan.reply is None:
    +            raise AssertionError("Phase 1 execution plan requires a reply")
    +        self._finish(request.token, Replied(plan.reply))
    +
    +    def _finish(self, token: RequestToken, outcome: RequestOutcome) -> None:
    +        future = self._accepted.pop(token, None)
    +        if future is not None and not future.done():
    +            future.set_result(outcome)
    +        self._accepted_changed.set()
    +
    +    async def close(self) -> None:
    +        if self._close_task is None:
    +            self._close_task = asyncio.create_task(
    +                self._close_once(), name="miniredis:executor-close"
    +            )
    +        await asyncio.shield(self._close_task)
    +
    +    async def _close_once(self) -> None:
    +        self._stopping = True
    +        self.mailbox.close_user_admission()
    +        self._run_gate.set()
    +        try:
    +            if self._worker_task is not None and not self._worker_task.done():
    +                self.post_control(_StopExecutor())
    +                await self._worker_task
    +        except Exception as error:  # noqa: BLE001 - close must finish cleanup
    +            if self._failure is None:
    +                self._failure = error
    +        finally:
    +            self.mailbox.drain()
    +            for token in tuple(self._accepted):
    +                self._finish(token, RuntimeClosed())
    +            self.mailbox.close_control_admission()
    +
    +    def debug_pause(self) -> None:
    +        self._run_gate.clear()
    +
    +    def debug_resume(self) -> None:
    +        self._run_gate.set()
    +
    +    async def debug_wait_accepted_at_least(self, count: int) -> None:
    +        while len(self._accepted) < count:
    +            self._accepted_changed.clear()
    +            if len(self._accepted) >= count:
    +                return
    +            await self._accepted_changed.wait()
    +
    +    @property
    +    def debug_accepted_count(self) -> int:
    +        return len(self._accepted)
    +
    +    @property
    +    def debug_failure(self) -> BaseException | None:
    +        return self._failure
    +
    +    @property
    +    def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
    +        return tuple(self._applied_batches)
    ```

**What it is and why it appears**

The executor owns accepted tokens, the mailbox worker, planning, control events, and terminal failure cleanup.

**Runtime role**

It turns one mailbox event into one outcome and prevents later events from observing a partially handled turn.

**Key code**

```python
message = await self.mailbox.take()
await self._run_gate.wait()
if isinstance(message, _StopExecutor):
    return
await self._execute(message)
```

**Statement understanding**

Only this loop chooses the next state event; asyncio callers may run concurrently but cannot bypass mailbox order.

#### Direct client and runtime lifecycle

Assemble the owner behind a binary-safe in-process client with explicit startup, shutdown, clock, and bounded configuration.

??? note "File diff: src/miniredis/adapters/direct.py"
    ```diff
    diff --git a/src/miniredis/adapters/direct.py b/src/miniredis/adapters/direct.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..b15e6a15ad24edd4273cd5550eabebeae8843338
    --- /dev/null
    +++ b/src/miniredis/adapters/direct.py
    @@ -0,0 +1,51 @@
    +from __future__ import annotations
    +
    +import asyncio
    +from typing import TYPE_CHECKING
    +
    +from miniredis.commands.parser import CommandParseError, parse_command_request
    +from miniredis.commands.request import CommandRequest
    +from miniredis.core.executor import Replied, RuntimeClosed, SubmittedRequest
    +from miniredis.core.reply import Failure, Reply
    +
    +if TYPE_CHECKING:
    +    from miniredis.runtime import MiniRedis
    +
    +
    +class DirectClient:
    +    def __init__(self, runtime: MiniRedis, session_id: int) -> None:
    +        self.runtime = runtime
    +        self.session_id = session_id
    +        self.closed = False
    +
    +    async def execute(self, request: CommandRequest) -> Reply:
    +        if self.closed:
    +            return Failure("CLOSED", "client is closed")
    +        try:
    +            command = parse_command_request(request)
    +        except CommandParseError as error:
    +            return Failure("ERR", str(error))
    +
    +        submitted = self.runtime.executor.submit(
    +            session_id=self.session_id, command=command
    +        )
    +        if isinstance(submitted, Failure):
    +            return submitted
    +        assert isinstance(submitted, SubmittedRequest), (
    +            f"unexpected submission: {submitted!r}"
    +        )
    +
    +        outcome = await asyncio.shield(submitted.future)
    +        match outcome:
    +            case Replied(reply=reply):
    +                return reply
    +            case RuntimeClosed():
    +                return Failure("CLOSED", "runtime closed before reply")
    +            case _:
    +                raise AssertionError(f"unexpected request outcome: {outcome!r}")
    +
    +    async def receive(self) -> Reply:
    +        raise NotImplementedError("DirectClient.receive is unavailable in Phase 1")
    +
    +    async def close(self) -> None:
    +        self.closed = True
    ```

**What it is and why it appears**

The Direct adapter is the first public client and contains no data-structure semantics.

**Runtime role**

It submits binary requests, awaits executor-owned outcomes, and maps inactive lifecycle states to stable failures.

**Key code**

```python
submitted = self.runtime.executor.submit(
    session_id=self.session_id, command=command
)
```

**Statement understanding**

The adapter delegates both parsing and ordering; a future socket adapter can meet it at the same request boundary.

??? note "File diff: src/miniredis/clock.py"
    ```diff
    diff --git a/src/miniredis/clock.py b/src/miniredis/clock.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..cb80d29a067bf65d39f60c118f7c07294bc9795b
    --- /dev/null
    +++ b/src/miniredis/clock.py
    @@ -0,0 +1,13 @@
    +from __future__ import annotations
    +
    +import time
    +from typing import Protocol
    +
    +
    +class Clock(Protocol):
    +    def now_ms(self) -> int: ...
    +
    +
    +class SystemClock:
    +    def now_ms(self) -> int:
    +        return time.time_ns() // 1_000_000
    ```

**What it is and why it appears**

The `Clock` protocol makes time an injected observation before TTL is introduced.

**Runtime role**

The executor samples one `now_ms` for a planning turn instead of letting commands read wall time independently.

**Key code**

```python
class Clock(Protocol):
    def now_ms(self) -> int: ...
```

**Statement understanding**

Explicit time enables deterministic expiry tests and prevents one command from observing multiple inconsistent instants.

??? note "File diff: src/miniredis/config.py"
    ```diff
    diff --git a/src/miniredis/config.py b/src/miniredis/config.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..f8c7715fae0e36e2c2af7d0fb861e1be26a69d77
    --- /dev/null
    +++ b/src/miniredis/config.py
    @@ -0,0 +1,24 @@
    +from __future__ import annotations
    +
    +from dataclasses import dataclass
    +from typing import Literal
    +
    +EvictionPolicy = Literal["noeviction", "allkeys-lru"]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class MiniRedisConfig:
    +    max_pending_commands: int = 1024
    +    active_expire_sample_size: int = 20
    +    maxmemory: int | None = None
    +    eviction_policy: EvictionPolicy = "noeviction"
    +
    +    def __post_init__(self) -> None:
    +        if self.max_pending_commands <= 0:
    +            raise ValueError("max_pending_commands must be positive")
    +        if self.active_expire_sample_size <= 0:
    +            raise ValueError("active_expire_sample_size must be positive")
    +        if self.maxmemory is not None and self.maxmemory <= 0:
    +            raise ValueError("maxmemory must be positive")
    +        if self.eviction_policy not in {"noeviction", "allkeys-lru"}:
    +            raise ValueError("eviction_policy must be 'noeviction' or 'allkeys-lru'")
    ```

**What it is and why it appears**

Frozen configuration collects bounded runtime choices and validates them at construction.

**Runtime role**

The runtime and executor receive one immutable set of limits.

**Key code**

```python
if self.max_pending_commands <= 0:
    raise ValueError("max_pending_commands must be positive")
```

**Statement understanding**

An invalid bound is rejected before tasks start, so runtime code never needs a zero-capacity special state.

??? note "File diff: src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..320a112b68bef7c9d40088309fff888ee4407339
    --- /dev/null
    +++ b/src/miniredis/runtime.py
    @@ -0,0 +1,150 @@
    +from __future__ import annotations
    +
    +import asyncio
    +from enum import Enum
    +from typing import Any, Self
    +
    +from miniredis.adapters.direct import DirectClient
    +from miniredis.clock import Clock, SystemClock
    +from miniredis.config import MiniRedisConfig
    +from miniredis.core.commit import CommitBatch
    +from miniredis.core.database import Database
    +from miniredis.core.executor import (
    +    CommandExecutor,
    +    CommitBarrier,
    +    NullCommitBarrier,
    +)
    +from miniredis.core.planner import CommandPlanner
    +
    +
    +class RuntimeState(str, Enum):
    +    STARTING = "starting"
    +    RUNNING = "running"
    +    DRAINING = "draining"
    +    CLOSED = "closed"
    +
    +
    +class MiniRedis:
    +    def __init__(
    +        self,
    +        config: MiniRedisConfig,
    +        *,
    +        clock: Clock,
    +        commit_barrier: CommitBarrier,
    +    ) -> None:
    +        self.config = config
    +        self.clock = clock
    +        self.commit_barrier = commit_barrier
    +        self.database = Database()
    +        self.planner = CommandPlanner(config)
    +        self.executor = CommandExecutor(
    +            database=self.database,
    +            planner=self.planner,
    +            clock=clock,
    +            commit_barrier=commit_barrier,
    +            max_pending_commands=config.max_pending_commands,
    +            on_terminal_failure=self._on_executor_terminal_failure,
    +        )
    +        self.state = RuntimeState.STARTING
    +        self._next_session_id = 0
    +        self._start_task: asyncio.Task[None] | None = None
    +        self._close_task: asyncio.Task[None] | None = None
    +
    +    @classmethod
    +    def open(
    +        cls,
    +        config: MiniRedisConfig | None = None,
    +        **options: Any,
    +    ) -> MiniRedis:
    +        if config is not None and options:
    +            raise TypeError("config cannot be combined with keyword options")
    +        resolved = config if config is not None else MiniRedisConfig(**options)
    +        return cls(
    +            resolved,
    +            clock=SystemClock(),
    +            commit_barrier=NullCommitBarrier(),
    +        )
    +
    +    @classmethod
    +    def _for_test(
    +        cls,
    +        config: MiniRedisConfig | None = None,
    +        *,
    +        clock: Clock | None = None,
    +        commit_barrier: CommitBarrier | None = None,
    +        **options: Any,
    +    ) -> MiniRedis:
    +        if config is not None and options:
    +            raise TypeError("config cannot be combined with keyword options")
    +        resolved = config if config is not None else MiniRedisConfig(**options)
    +        return cls(
    +            resolved,
    +            clock=clock if clock is not None else SystemClock(),
    +            commit_barrier=(
    +                commit_barrier if commit_barrier is not None else NullCommitBarrier()
    +            ),
    +        )
    +
    +    async def start(self) -> None:
    +        if self.state is RuntimeState.RUNNING:
    +            return
    +        if self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}:
    +            raise RuntimeError("runtime is closed")
    +        if self._start_task is None:
    +            self._start_task = asyncio.create_task(
    +                self._start_once(), name="miniredis:runtime-start"
    +            )
    +        await asyncio.shield(self._start_task)
    +
    +    async def _start_once(self) -> None:
    +        await self.executor.start()
    +        if self.state is RuntimeState.STARTING:
    +            self.state = RuntimeState.RUNNING
    +
    +    def direct_client(self) -> DirectClient:
    +        if self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}:
    +            raise RuntimeError("runtime is closed")
    +        self._next_session_id += 1
    +        return DirectClient(self, self._next_session_id)
    +
    +    async def close(self) -> None:
    +        if self._close_task is None:
    +            self._close_task = asyncio.create_task(
    +                self._close(), name="miniredis:runtime-close"
    +            )
    +        await asyncio.shield(self._close_task)
    +
    +    async def _close(self) -> None:
    +        if self.state is RuntimeState.CLOSED:
    +            return
    +        self.state = RuntimeState.DRAINING
    +        await self.executor.close()
    +        self.state = RuntimeState.CLOSED
    +
    +    def _on_executor_terminal_failure(self, failure: BaseException) -> None:
    +        del failure
    +        self.state = RuntimeState.CLOSED
    +
    +    async def __aenter__(self) -> Self:
    +        await self.start()
    +        return self
    +
    +    async def __aexit__(self, *exc_info: object) -> None:
    +        await self.close()
    +
    +    @property
    +    def debug_commit_seq(self) -> int:
    +        return self.database.commit_seq
    +
    +    @property
    +    def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
    +        return self.executor.debug_applied_batches
    +
    +    def debug_pause_executor(self) -> None:
    +        self.executor.debug_pause()
    +
    +    def debug_resume_executor(self) -> None:
    +        self.executor.debug_resume()
    +
    +    async def debug_wait_accepted_at_least(self, count: int) -> None:
    +        await self.executor.debug_wait_accepted_at_least(count)
    ```

**What it is and why it appears**

`MiniRedis` assembles database, parser, planner, executor, clock, clients, and lifecycle state.

**Runtime role**

It starts one worker, admits clients only while running, and shields owned close work from cancelling callers.

**Key code**

```python
await asyncio.shield(self._close_task)
```

**Statement understanding**

Cancelling one waiter does not cancel shared cleanup; runtime ownership outlives the caller awaiting it.

#### Public API wiring

#### Public API wiring

Expose the Direct-first runtime without creating a separate mechanism at the package export layer.

??? note "Supporting file diffs (1 file)"
    **`src/miniredis/__init__.py`**

    ```diff
    diff --git a/src/miniredis/__init__.py b/src/miniredis/__init__.py
    index d4792a841229d7110cbaed9f6b8bde385f210188..e6e2ed4810e520b4749fc6f2e90aed9d29a7703b 100644
    --- a/src/miniredis/__init__.py
    +++ b/src/miniredis/__init__.py
    @@ -1 +1,10 @@
    -"""MiniRedis reference package."""
    +from miniredis.commands.request import CommandRequest
    +from miniredis.config import MiniRedisConfig
    +from miniredis.runtime import MiniRedis, RuntimeState
    +
    +__all__ = [  # noqa: RUF022 - keep the documented public order
    +    "CommandRequest",
    +    "MiniRedisConfig",
    +    "MiniRedis",
    +    "RuntimeState",
    +]
    ```


### Verification evidence

Run `uv run pytest -q $(cat journey/stages/03-serialized-direct-executor/tests.txt)`. It proves ownership and lifecycle under controlled races; no data mutation exists yet.

### Durable takeaways

Admission and execution are different. User capacity never blocks control cleanup. One supervised worker owns state order, and accepted requests always receive one terminal outcome.

### Explain it in your own words

MiniRedis allows callers to be concurrent but makes state ownership single-threaded in the logical sense. Requests queue through bounded admission, control events keep a guaranteed route, and one runtime-owned executor decides the only observable order.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/02-command-life.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/67f0d73...a5f7a27)

After finishing, run `python -m journey.tools.build_journey check 3` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/03-serialized-direct-executor/stage.patch)

# Stage 07 · Absolute TTL and bounded expiry

### Goal

Make expiration a deterministic state transition with absolute deadlines, lazy invisibility, and bounded active cleanup.

??? note "Deliverable files"
    - `pyproject.toml`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/planner.py`
    - `src/miniredis/core/ttl_planner.py`
    - `src/miniredis/runtime.py`
    - `tests/contract/test_ttl.py`
    - `tests/helpers/time.py`

### The problem at this point

Values can now be updated atomically, but time does not yet affect visibility. Relative countdowns would drift during pause or restart, while deleting directly from a read path would bypass the serialized commit owner.

### Test contract

#### See the failure first

An expired key must already be logically absent even if its physical entry remains. A later command error must not accidentally commit the pending lazy delete, and in-place mutations must preserve the original deadline instead of extending the key's life.

??? note "File diff: tests/contract/test_ttl.py"
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

**What this test locks**

It locks lazy invisibility, TTL rounding, PERSIST, bounded active cleanup, deadline preservation for every value family, and error atomicity.

**How it constructs the counterexample**

It advances injected time exactly to a deadline, separates logical reads from physical counts, and combines an expired operand with a later WRONGTYPE operand.

**Key test statement**

```python
assert runtime.debug_commit_seq == before
```

**What a failure means**

Time changed state outside the commit protocol, an error leaked a proposed expiry delete, or a mutation silently reset an absolute deadline.

??? note "File diff: tests/helpers/time.py"
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

**What this test locks**

The helper makes elapsed time an explicit input rather than a sleep or wall-clock assumption.

**How it constructs the counterexample**

Each test selects an exact millisecond and advances it synchronously, so boundary equality is reproducible.

**Key test statement**

```python
def advance(self, milliseconds: int) -> None:
    self.value += milliseconds
```

**What a failure means**

TTL behavior can no longer be distinguished from scheduler timing or a flaky real-time delay.

### Basic concepts

MiniRedis stores `expire_at_ms`, an absolute deadline. Lazy expiry makes an elapsed entry invisible during lookup and proposes `DeleteKey(EXPIRED)`; active expiry independently samples physical TTL entries so cold keys are eventually reclaimed. Logical absence and physical reclamation are therefore separate moments.

### Why this mechanism is necessary

Absolute time survives pauses and later persistence without recomputing a countdown. Routing both lazy and active deletes through `CommitBatch` preserves ordering, durability hooks, and future replication semantics. A bounded sample prevents one maintenance tick from monopolizing the executor.

### Runtime mental model

The injected Clock supplies `now_ms`. A command planner compares it with an entry deadline and returns a reply plus proposed operations. The executor either commits the complete successful plan or discards it on failure. Active ticks enter the same mailbox, select at most N TTL keys, and commit expired deletes as one maintenance batch.

### Mechanism blocks

#### Absolute TTL planning

Translate EXPIRE, TTL/PTTL, and PERSIST into absolute deadlines and ordinary commit operations.

??? note "File diff: src/miniredis/core/ttl_planner.py"
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

**What it is and why it appears**

This command-family planner owns EXPIRE, TTL/PTTL, and PERSIST without teaching the executor command semantics.

**Runtime role**

It resolves lazy expiry first, stores `now_ms + duration`, and represents immediate expiry or persistence changes as ordinary operations.

**Key code**

```python
put = make_put(
    key,
    previous.value,
    previous,
    now_ms + seconds * 1_000,
)
```

**Statement understanding**

The stored value is an absolute deadline. In-place updates can copy it unchanged, while TTL can compute remaining time from one clock reading.

#### Serialized active expiry

Sample bounded TTL keys through the executor mailbox and publish expired deletes through the same commit barrier.

??? note "File diff: src/miniredis/core/executor.py"
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

**What it is and why it appears**

The single executor gains a control message for bounded active expiry rather than a second task mutating the database directly.

**Runtime role**

It rotates through sorted TTL keys, proposes expired deletes, appends one `ACTIVE_EXPIRE` batch, and applies it in mailbox order.

**Key code**

```python
candidate_keys = ordered_keys[: self.active_expire_sample_size]
self._active_expire_cursor = candidate_keys[-1]
```

**Statement understanding**

The slice is the per-tick work bound; the cursor prevents every tick from repeatedly examining only the same prefix.

#### TTL routing and injectable time

Route typed TTL commands and expose one public construction path that can receive a deterministic clock.

??? note "File diff: src/miniredis/core/planner.py"
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

**What it is and why it appears**

The stable planner facade adds the TTL command family after the existing value planners.

**Runtime role**

It routes a typed TTL command to exactly one semantic owner and preserves the existing unknown-command fallback.

**Key code**

```python
if plan is None:
    plan = plan_ttl(command, database, now_ms)
```

**Statement understanding**

`None` still means “not my command family”; an `ExecutionPlan` with no operations can still be a complete TTL result.

??? note "File diff: src/miniredis/runtime.py"
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

**What it is and why it appears**

The public runtime construction path now accepts a Clock and exposes narrow expiry diagnostics for contracts.

**Runtime role**

It passes the configured sample bound into the executor and delegates active cleanup without exposing direct database mutation.

**Key code**

```python
return cls.open(
    config,
    clock=clock,
    commit_barrier=commit_barrier,
    **options,
)
```

**Statement understanding**

`open_with_dependencies` and `open` converge on one construction path, so production and deterministic tests cannot drift in wiring.

#### Deterministic test import support

Let contract tests import their shared fake clock without treating test-path wiring as a runtime mechanism.

??? note "Supporting file diffs (1 file)"
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


### Verification evidence

Run `uv run pytest -q $(cat journey/stages/07-absolute-ttl/tests.txt)`. It proves the TTL contract through the public Direct client and executor, including deterministic boundary time and bounded physical cleanup.

### Durable takeaways

Store absolute deadlines; separate logical invisibility from physical reclamation; propose expiry deletes; discard every proposed operation when a command fails; preserve deadlines across in-place mutations.

### Explain it in your own words

Expiration does not create a second writer. Clock time changes what lookup proposes, but only the executor publishes deletion. Lazy reads provide immediate logical absence, while bounded active ticks eventually reclaim untouched physical entries through the same ordered commit path.

### Textbook

[Chapter 4](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/04-expiration.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/79fc734...ddfd69e)

After finishing, run `python -m journey.tools.build_journey check 7` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/07-absolute-ttl/stage.patch)

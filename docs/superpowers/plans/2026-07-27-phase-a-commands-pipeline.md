# Phase A Commands and Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. The user selected inline execution;
> do not dispatch subagents.

**Goal:** Add MGET, MSET, DECR, BRPOP, DirectPipeline, and true ordered RESP2
pipelining while keeping the executor as the single state owner.

**Architecture:** New commands follow the existing typed-command and planner
path. Blocking direction becomes waiter state. Both adapters use one
synchronous submission boundary that posts valid commands and parse failures
into executor mailbox order; DirectPipeline batches submissions, while TCP
submits all buffered frames without waiting for preceding replies.

**Tech Stack:** Python 3.13, asyncio, frozen dataclasses, pytest,
pytest-asyncio, RESP2.

---

## File map

- Modify `src/miniredis/commands/model.py`: typed commands and mutability
  classification.
- Modify `src/miniredis/commands/parser.py`: new command parsing.
- Modify `src/miniredis/core/planning.py`: MGET/MSET/DECR planning.
- Modify `src/miniredis/core/planner.py`: direction-aware blocking pop.
- Modify `src/miniredis/core/blocking.py`: waiter pop direction and wakeups.
- Modify `src/miniredis/core/executor.py`: ordered rejection messages and
  direction-aware blocking registration.
- Modify `src/miniredis/runtime.py`: one ordered adapter submission API and
  pipeline factory.
- Modify `src/miniredis/adapters/direct.py`: submission primitive and
  `DirectPipeline`.
- Modify `src/miniredis/adapters/tcp.py`: submit all decoded frames without a
  per-command await gate.
- Modify `src/miniredis/__init__.py`: export `DirectPipeline`.
- Test `tests/unit/commands/test_parser.py`.
- Test `tests/unit/commands/test_command_traits.py`.
- Test `tests/contract/test_strings.py`.
- Test `tests/contract/test_lists.py`.
- Create `tests/adapters/test_direct_pipeline.py`.
- Modify `tests/adapters/test_tcp_async_semantics.py`.
- Modify `tests/concurrency/test_blpop_races.py`.
- Modify `docs/behavior-matrix.md` and `README.md`.

### Task 1: MGET, MSET, and DECR command contracts

- [ ] **Step 1: Add failing parser and command-trait tests**

Add cases equivalent to:

```python
def test_parse_bulk_string_commands():
    assert parse(CommandRequest(b"MGET", (b"a", b"b"))) == MultiGet((b"a", b"b"))
    assert parse(CommandRequest(b"MSET", (b"a", b"1", b"b", b"2"))) == MultiSet(
        ((b"a", b"1"), (b"b", b"2"))
    )
    assert parse(CommandRequest(b"DECR", (b"a",))) == Increment(b"a", -1)


@pytest.mark.parametrize(
    "request",
    [
        CommandRequest(b"MGET"),
        CommandRequest(b"MSET", (b"a",)),
        CommandRequest(b"MSET", (b"a", b"1", b"b")),
        CommandRequest(b"DECR"),
    ],
)
def test_bulk_string_command_arity_is_rejected(request):
    with pytest.raises(CommandParseError):
        parse(request)
```

Classify `MultiSet` as mutating and `MultiGet` as non-mutating.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/unit/commands/test_parser.py \
  tests/unit/commands/test_command_traits.py
```

Expected: collection or assertion failure because `MultiGet` and `MultiSet`
do not exist.

- [ ] **Step 3: Add typed models and parser branches**

Implement:

```python
@dataclass(frozen=True, slots=True)
class MultiGet:
    keys: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class MultiSet:
    pairs: tuple[tuple[bytes, bytes], ...]
```

Parser behavior:

```python
case b"MGET":
    _require_min_arity(name, args, 1)
    return MultiGet(args)
case b"MSET":
    _require_min_arity(name, args, 2)
    if len(args) % 2 != 0:
        raise CommandParseError("wrong number of arguments for MSET")
    return MultiSet(_byte_pairs(args))
case b"DECR":
    _require_arity(name, args, 1)
    return Increment(args[0], -1)
```

Add both types to `Command` and to exactly one trait set.

- [ ] **Step 4: Add failing Direct contract tests**

Cover ordered nulls/non-Strings, last-duplicate-wins, type replacement, TTL
clearing, one commit, OOM all-or-nothing, and DECR TTL preservation:

```python
@pytest.mark.asyncio
async def test_mget_is_ordered_and_treats_non_strings_as_null():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SET", (b"s", b"v")))
        await c.execute(CommandRequest(b"LPUSH", (b"list", b"x")))
        assert await c.execute(
            CommandRequest(b"MGET", (b"s", b"missing", b"list", b"s"))
        ) == Items((Bytes(b"v"), Bytes(None), Bytes(None), Bytes(b"v")))


@pytest.mark.asyncio
async def test_mget_expired_key_is_logically_missing_without_commit():
    clock = FakeClock(0)
    async with MiniRedis.open(clock=clock) as runtime:
        c = runtime.direct_client()
        await c.execute(CommandRequest(b"SET", (b"k", b"v", b"PX", b"1")))
        clock.advance(1)
        before = runtime.debug_commit_seq
        assert await c.execute(CommandRequest(b"MGET", (b"k",))) == Items(
            (Bytes(None),)
        )
        assert runtime.debug_commit_seq == before


@pytest.mark.asyncio
async def test_mset_is_one_atomic_commit_and_last_duplicate_wins():
    async with MiniRedis.open() as runtime:
        c = runtime.direct_client()
        before = runtime.debug_commit_seq
        assert await c.execute(
            CommandRequest(b"MSET", (b"a", b"1", b"a", b"2", b"b", b"3"))
        ) == Ok()
        assert runtime.debug_commit_seq == before + 1
        assert await c.execute(CommandRequest(b"MGET", (b"a", b"b"))) == Items(
            (Bytes(b"2"), Bytes(b"3"))
        )
```

- [ ] **Step 5: Implement planners**

Add `MultiGet` and `MultiSet` cases in `plan_general_and_strings`.

`MultiGet` must return expired and non-String entries as null without
materializing expiry deletion. It touches only returned live Strings:

```python
case cmd.MultiGet(keys):
    touches: list[bytes] = []
    replies: list[Reply] = []
    for key in keys:
        entry, _expired = lookup(database, key, now_ms)
        if entry is None or not isinstance(entry.value, StringValue):
            replies.append(Bytes(None))
        else:
            replies.append(Bytes(entry.value.data))
            touches.append(key)
    return ExecutionPlan(
        Items(tuple(replies)),
        (),
        tuple(touches),
    )
```

`MultiSet` must collapse duplicate pairs before looking up previous entries,
clear TTLs, and return one plan:

```python
case cmd.MultiSet(pairs):
    final_values: dict[bytes, bytes] = {}
    order: list[bytes] = []
    for key, value in pairs:
        if key not in final_values:
            order.append(key)
        final_values[key] = value
    operations: list[CommitOperation] = []
    for key in order:
        previous, expired = lookup(database, key, now_ms)
        operations.extend(expired)
        operations.append(
            make_put(key, StringValue(final_values[key]), previous, None)
        )
    return ExecutionPlan(Ok(), tuple(operations))
```

Do not deduplicate Put/Delete across different operation kinds after lookup;
expired-then-put ordering is intentional.

- [ ] **Step 6: Run command contracts and commit**

Run:

```bash
uv run pytest -q \
  tests/unit/commands/test_parser.py \
  tests/unit/commands/test_command_traits.py \
  tests/contract/test_strings.py \
  tests/contract/test_ttl.py \
  tests/contract/test_eviction.py
```

Expected: PASS.

Commit:

```bash
git add src/miniredis/commands src/miniredis/core/planning.py \
  tests/unit/commands tests/contract/test_strings.py \
  tests/contract/test_ttl.py tests/contract/test_eviction.py
git commit -m "feat: add bulk string commands"
```

### Task 2: Direction-aware BRPOP

- [ ] **Step 1: Add failing parser and list contract tests**

Use one typed command with direction:

```python
assert parse(CommandRequest(b"BLPOP", (b"a", b"0"))) == BlockingPop(
    (b"a",), 0, left=True
)
assert parse(CommandRequest(b"BRPOP", (b"a", b"b", b"1.5"))) == BlockingPop(
    (b"a", b"b"), 1500, left=False
)
```

Contract cases must prove immediate right pop, first-key priority, wrong type,
timeout, and closed-session cleanup.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/unit/commands/test_parser.py \
  tests/contract/test_lists.py \
  tests/concurrency/test_blpop_races.py
```

Expected: FAIL because only `BlPop` exists.

- [ ] **Step 3: Replace BlPop with BlockingPop**

Define:

```python
@dataclass(frozen=True, slots=True)
class BlockingPop:
    keys: tuple[bytes, ...]
    timeout_ms: int
    left: bool
```

Parse both names through the existing timeout validation and set
`left=name == b"BLPOP"`. Update command traits and every import/pattern match.

Change `CommandPlanner.plan_blocking_pop_now` to select:

```python
item = items.popleft() if command.left else items.pop()
```

- [ ] **Step 4: Carry direction through waiting and push wakeup**

Add `left: bool` to `BlockingWaiter` and `WaiterRegistry.register`. In
`prepare_list_wakeups`, reserve from the requested side:

```python
item = remaining.popleft() if waiter.left else remaining.pop()
wakeups.append(
    WaiterWakeup(
        waiter.waiter_id,
        waiter.generation,
        key,
        item,
    )
)
```

Register `command.left` in the executor. Keep key ordering, timer ownership,
transition rules, and `[key, value]` replies unchanged.

- [ ] **Step 5: Run blocking suites and commit**

Run:

```bash
uv run pytest -q \
  tests/contract/test_lists.py \
  tests/mechanisms/test_blpop.py \
  tests/mechanisms/test_blpop_push_batch.py \
  tests/concurrency/test_blpop_races.py \
  tests/concurrency/test_shutdown.py
```

Expected: PASS.

Commit:

```bash
git add src/miniredis/commands src/miniredis/core/blocking.py \
  src/miniredis/core/planner.py src/miniredis/core/executor.py \
  src/miniredis/adapters/direct.py tests
git commit -m "feat: add direction-aware blocking pop"
```

### Task 3: Ordered adapter submission and DirectPipeline

- [ ] **Step 1: Add failing DirectPipeline tests**

Create `tests/adapters/test_direct_pipeline.py` with:

```python
@pytest.mark.asyncio
async def test_direct_pipeline_preserves_result_slots_and_is_not_atomic():
    async with MiniRedis.open() as runtime:
        pipeline = runtime.direct_pipeline()
        pipeline.queue(CommandRequest(b"SET", (b"k", b"1")))
        pipeline.queue(CommandRequest(b"NOPE"))
        pipeline.queue(CommandRequest(b"INCR", (b"k",)))
        assert await pipeline.execute() == (
            Ok(),
            Failure("ERR", "unknown command"),
            Number(2),
        )
        assert pipeline.pending_count == 0
```

Add a paused-executor test that submits one pipeline, inserts another
session's command between individual submissions using a test hook, and proves
that no atomic batch promise exists.

- [ ] **Step 2: Run the new test and verify RED**

Run:

```bash
uv run pytest -q tests/adapters/test_direct_pipeline.py
```

Expected: FAIL because `direct_pipeline` does not exist.

- [ ] **Step 3: Introduce ordered rejection submission**

In `core/executor.py`, add:

```python
@dataclass(slots=True)
class RejectRequest:
    token: RequestToken
    session_id: int
    reply: Failure
    future: asyncio.Future[RequestOutcome]
```

Give command and rejection submissions the same admission checks, token
tracking, mailbox, abandonment, and completion path. When the executor handles
`RejectRequest`, call `_finish_reply(message.token, message.reply)`. This
mailbox ordering is required by TCP pipeline errors and Phase B transaction
queue errors.

In `runtime.py`, add one synchronous boundary:

```python
def submit_request(
    self,
    session_id: int,
    request: CommandRequest,
) -> SubmittedRequest | Failure:
    parsed = self.parse(request)
    if isinstance(parsed, Failure):
        return self.executor.submit_rejection(session_id, parsed)
    return self.executor.submit(session_id, parsed)
```

Refactor `DirectClient.execute` and `execute_for_session` to use it.

Add:

```python
async def wait_for_session_submission(
    self,
    session_id: int,
    submitted: SubmittedRequest | Failure,
) -> None:
    if isinstance(submitted, Failure):
        endpoint = self.executor.endpoint(session_id)
        if endpoint is not None:
            token = self.executor.new_request_token()
            endpoint.offer(ReplyMessage(token, submitted))
        return
    try:
        await asyncio.shield(submitted.future)
    except asyncio.CancelledError:
        self.executor.post_control(AbandonRequest(submitted.token))
        raise
```

`execute_for_session` becomes submit plus this wait helper.

- [ ] **Step 4: Implement DirectPipeline**

Implement a focused adapter:

```python
class DirectPipeline:
    def __init__(self, client: DirectClient) -> None:
        self._client = client
        self._requests: list[CommandRequest] = []

    @property
    def pending_count(self) -> int:
        return len(self._requests)

    def queue(self, request: CommandRequest) -> DirectPipeline:
        if self._client.closed:
            raise RuntimeError("client is closed")
        self._requests.append(request)
        return self

    async def execute(self) -> tuple[Reply | None, ...]:
        requests, self._requests = self._requests, []
        submitted = [self._client.submit(request) for request in requests]
        return tuple(
            await self._client.resolve(item)
            for item in submitted
        )

    async def close(self) -> None:
        self._requests.clear()
        await self._client.close()
```

Extract `DirectClient.submit` and `DirectClient.resolve` from the existing
`execute` flow. Preserve cancellation by posting `AbandonRequest`. Add
`MiniRedis.direct_pipeline()` that owns a new DirectClient and export the type.
Tests must close a standalone pipeline or let runtime shutdown prove its
session is reclaimed.

- [ ] **Step 5: Run Direct and ownership tests and commit**

Run:

```bash
uv run pytest -q \
  tests/adapters/test_direct_pipeline.py \
  tests/concurrency/test_direct_executor.py \
  tests/concurrency/test_request_ownership.py \
  tests/concurrency/test_shutdown.py
```

Expected: PASS.

Commit:

```bash
git add src/miniredis/core/executor.py src/miniredis/runtime.py \
  src/miniredis/adapters/direct.py src/miniredis/__init__.py \
  tests/adapters/test_direct_pipeline.py tests/concurrency
git commit -m "feat: add ordered direct pipelines"
```

### Task 4: RESP2 pipelined submission

- [ ] **Step 1: Add failing TCP pipeline tests**

Add tests that write three RESP arrays in one `writer.write`, then read three
ordered replies. Include an invalid middle command and a paused writer to
prove that decoded commands reach the executor before the first reply drains:

```python
wire = (
    b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n"
    b"*1\r\n$4\r\nNOPE\r\n"
    b"*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n"
)
await send(writer, wire)
await expect(reader, b"+OK\r\n-ERR unknown command\r\n:2\r\n")
```

- [ ] **Step 2: Run the TCP test and verify RED**

Run:

```bash
uv run pytest -q \
  tests/adapters/test_tcp_async_semantics.py \
  tests/adapters/test_tcp_smoke.py
```

Expected: the pipeline concurrency assertion fails because `_pending_command`
serializes submit-and-wait.

- [ ] **Step 3: Replace the single pending command with submitted ownership**

In `TcpSession`, remove `_pending_command` and add:

```python
self._pending_commands: set[asyncio.Task[None]] = set()
```

For every decoded frame, synchronously call the ordered runtime submission
boundary. Create only a small outcome-wait task for ownership/cancellation:

```python
def _submit_available(self) -> None:
    while self._frames and not self._reader_quiescing and not self._closed:
        request = self._frames.popleft()
        submitted = self.runtime.submit_request(self.session_id, request)
        task = asyncio.create_task(
            self.runtime.wait_for_session_submission(submitted),
            name=f"miniredis:tcp-command:{self.session_id}",
        )
        self._pending_commands.add(task)
        task.add_done_callback(self._command_done)
```

`_command_done` removes the exact task and preserves existing error-to-close
behavior. Close paths gather the whole set. `owned_task_count` includes all
unfinished command tasks.

Enforce `max_session_frames` against queued plus in-flight session commands;
decrement when each task settles. Do not let immediate parser errors bypass
that bound.

If global executor admission returns `BUSY`, leave that frame at the front,
stop submitting newer frames, and wait for one of this session's in-flight
tasks to settle before retrying. If this session has no in-flight task because
other sessions own all global capacity, schedule one bounded
`executor.wait_for_capacity()` task; the executor signals its capacity event
whenever `_finish_request` removes an accepted request. Cancellation and
session close cancel this waiter. `CLOSED` is terminal and closes the session
after already ordered replies drain.

- [ ] **Step 4: Run adapter, backpressure, and shutdown tests**

Run:

```bash
uv run pytest -q \
  tests/adapters \
  tests/concurrency/test_slow_endpoint.py \
  tests/concurrency/test_shutdown.py \
  tests/reliability/test_reliability_shutdown.py
```

Expected: PASS with ordered replies and zero owned tasks after close.

- [ ] **Step 5: Commit RESP2 pipeline support**

```bash
git add src/miniredis/adapters/tcp.py src/miniredis/runtime.py \
  tests/adapters tests/concurrency tests/reliability
git commit -m "feat: submit RESP2 pipelines without round trips"
```

### Task 5: Phase A acceptance and documentation

- [ ] **Step 1: Update behavior claims**

Update `docs/behavior-matrix.md` and `README.md` to list the exact new command
subset and state:

```text
Pipeline batches adapter submission only. It does not provide atomic
execution, rollback, or cross-client isolation.
```

Remove MGET/MSET/DECR/BRPOP/Pipeline from current non-goals. Do not claim
transactions yet.

- [ ] **Step 2: Run formatting and complete regression**

Run:

```bash
uv run ruff check .
uv run pytest -q
git diff --check
```

Expected: Ruff clean, all tests pass, diff check clean.

- [ ] **Step 3: Commit Phase A acceptance**

```bash
git add README.md docs/behavior-matrix.md
git commit -m "docs: accept command and pipeline phase"
```

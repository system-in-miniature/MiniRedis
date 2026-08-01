# Stage 21 · Bulk strings and directional blocking pop

### Goal

Add MGET, MSET, DECR, and BRPOP without weakening atomic commits, ordered results, blocking-waiter direction, or whole-runtime ownership evidence.

??? note "Deliverable files"
    - `pyproject.toml`
    - `src/miniredis/adapters/direct.py`
    - `src/miniredis/commands/model.py`
    - `src/miniredis/commands/parser.py`
    - `src/miniredis/core/blocking.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/core/planner.py`
    - `src/miniredis/core/planning.py`
    - `src/miniredis/persistence/aof.py`
    - `src/miniredis/replication/sink.py`
    - `src/miniredis/runtime.py`
    - `tests/contract/test_strings.py`
    - `tests/mechanisms/test_blpop.py`
    - `tests/reliability/test_final_acceptance.py`
    - `tests/unit/commands/test_command_traits.py`
    - `tests/unit/commands/test_parser.py`

### The problem at this point

Multi-key commands are not loops around single-key client calls: MSET needs one commit and deterministic duplicate-key semantics, while MGET must preserve argument order and treat non-strings as null. BRPOP is not a separate waiter machine; its right-pop choice must survive both immediate scan and later wake-up.

### Test contract

#### See the failure first

A naïve MSET can expose partial state or allocate several commit sequences. A dict-only MGET can lose duplicates and order. A blocked BRPOP can wake using BLPOP direction. At full-system scale, successful behavior can still hide tasks, sessions, waiters, durability jobs, or replica links after close.

??? note "File diff: tests/contract/test_strings.py"
    ```diff
    diff --git a/tests/contract/test_strings.py b/tests/contract/test_strings.py
    index 00d39808ad88d37f51f3cb51c60bb4cc13a01820..2c0cffeb4d356e2ba7668c830c6f6ccd6ba29daf 100644
    --- a/tests/contract/test_strings.py
    +++ b/tests/contract/test_strings.py
    @@ -1,7 +1,8 @@
     import pytest

     from miniredis import CommandRequest, MiniRedis
    -from miniredis.core.reply import Bytes, Failure, Number, Ok
    +from miniredis.core.reply import Bytes, Failure, Items, Number, Ok
    +from tests.helpers.time import FakeClock


     @pytest.mark.asyncio
    @@ -64,3 +65,66 @@ async def test_general_commands_and_incrby_cover_the_frozen_subset():
             assert await c.execute(CommandRequest(b"INCRBY", (b"k", b"4"))) == Number(5)
             assert await c.execute(CommandRequest(b"DEL", (b"k", b"k"))) == Number(1)
             assert await c.execute(CommandRequest(b"TYPE", (b"k",))) == Bytes(b"none")
    +
    +
    +@pytest.mark.asyncio
    +async def test_mget_is_ordered_and_treats_non_strings_as_null():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(CommandRequest(b"SET", (b"s", b"v"))) == Ok()
    +        assert await c.execute(CommandRequest(b"LPUSH", (b"list", b"x"))) == Number(1)
    +
    +        assert await c.execute(
    +            CommandRequest(b"MGET", (b"s", b"missing", b"list", b"s"))
    +        ) == Items((Bytes(b"v"), Bytes(None), Bytes(None), Bytes(b"v")))
    +
    +
    +@pytest.mark.asyncio
    +async def test_mget_expired_key_is_logically_missing_without_commit():
    +    clock = FakeClock(0)
    +    async with MiniRedis.open(clock=clock) as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(
    +            CommandRequest(b"SET", (b"k", b"v", b"PX", b"1"))
    +        ) == Ok()
    +        clock.advance(1)
    +        before = runtime.debug_commit_seq
    +
    +        assert await c.execute(CommandRequest(b"MGET", (b"k",))) == Items(
    +            (Bytes(None),)
    +        )
    +        assert runtime.debug_commit_seq == before
    +
    +
    +@pytest.mark.asyncio
    +async def test_mset_is_one_atomic_commit_and_last_duplicate_wins():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(CommandRequest(b"LPUSH", (b"a", b"old"))) == Number(1)
    +        assert await c.execute(
    +            CommandRequest(b"SET", (b"b", b"old", b"PX", b"1000"))
    +        ) == Ok()
    +        before = runtime.debug_commit_seq
    +
    +        assert await c.execute(
    +            CommandRequest(b"MSET", (b"a", b"1", b"a", b"2", b"b", b"3"))
    +        ) == Ok()
    +
    +        assert runtime.debug_commit_seq == before + 1
    +        assert await c.execute(CommandRequest(b"MGET", (b"a", b"b"))) == Items(
    +            (Bytes(b"2"), Bytes(b"3"))
    +        )
    +        assert await c.execute(CommandRequest(b"PTTL", (b"b",))) == Number(-1)
    +
    +
    +@pytest.mark.asyncio
    +async def test_decr_reuses_integer_and_ttl_semantics():
    +    clock = FakeClock(0)
    +    async with MiniRedis.open(clock=clock) as runtime:
    +        c = runtime.direct_client()
    +        assert await c.execute(
    +            CommandRequest(b"SET", (b"k", b"2", b"PX", b"1000"))
    +        ) == Ok()
    +
    +        assert await c.execute(CommandRequest(b"DECR", (b"k",))) == Number(1)
    +        assert await c.execute(CommandRequest(b"PTTL", (b"k",))) == Number(1000)
    ```

Locks ordered/null MGET, one-commit MSET with last duplicate winning, TTL replacement, and DECR integer/TTL reuse. The decisive evidence is `runtime.debug_commit_seq == before + 1`; failure means a bulk command was decomposed into externally visible transitions.

??? note "File diff: tests/mechanisms/test_blpop.py"
    ```diff
    diff --git a/tests/mechanisms/test_blpop.py b/tests/mechanisms/test_blpop.py
    index 2ed0f36f45127eff9e74322ac63c0f2960130950..3d261179e147ed4843e4f3b8caa057de1247818e 100644
    --- a/tests/mechanisms/test_blpop.py
    +++ b/tests/mechanisms/test_blpop.py
    @@ -3,14 +3,18 @@ import asyncio
     import pytest

     from miniredis import CommandRequest, MiniRedis
    -from miniredis.commands.model import BlPop
    +from miniredis.commands.model import BlockingPop
     from miniredis.commands.parser import CommandParseError, parse_request
     from miniredis.core.reply import Bytes, Failure, Items


     def test_blpop_parser_freezes_keys_and_milliseconds():
    -    assert parse_request(CommandRequest(b"BLPOP", (b"a", b"b", b"1.25"))) == BlPop(
    -        (b"a", b"b"), 1250
    +    assert parse_request(
    +        CommandRequest(b"BLPOP", (b"a", b"b", b"1.25"))
    +    ) == BlockingPop(
    +        (b"a", b"b"),
    +        1250,
    +        left=True,
         )


    @@ -59,3 +63,31 @@ async def test_empty_scan_registers_once_under_every_key():
             assert runtime.debug_waiter_ids(b"a") == ()
             assert runtime.debug_waiter_ids(b"b") == ()
             assert runtime.debug_waiter_index_counts == (0, 0, 0)
    +
    +
    +@pytest.mark.asyncio
    +async def test_brpop_uses_first_ready_key_and_right_side():
    +    async with MiniRedis.open() as runtime:
    +        c = runtime.direct_client()
    +        await c.execute(CommandRequest(b"RPUSH", (b"first", b"a", b"b")))
    +        await c.execute(CommandRequest(b"RPUSH", (b"second", b"c")))
    +
    +        assert await c.execute(
    +            CommandRequest(b"BRPOP", (b"first", b"second", b"1"))
    +        ) == Items((Bytes(b"first"), Bytes(b"b")))
    +
    +
    +@pytest.mark.asyncio
    +async def test_blocked_brpop_preserves_right_pop_direction_when_woken():
    +    async with MiniRedis.open() as runtime:
    +        consumer = runtime.direct_client()
    +        producer = runtime.direct_client()
    +        blocked = asyncio.create_task(
    +            consumer.execute(CommandRequest(b"BRPOP", (b"q", b"0")))
    +        )
    +        await runtime.debug_wait_for_waiters(1)
    +
    +        await producer.execute(CommandRequest(b"LPUSH", (b"q", b"a", b"b")))
    +
    +        assert await blocked == Items((Bytes(b"q"), Bytes(b"a")))
    +        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"b")
    ```

Locks BRPOP's first-ready-key rule and right-side choice both immediately and after blocking. A failure means direction was not frozen into waiter ownership.

??? note "File diff: tests/reliability/test_final_acceptance.py"
    ```diff
    diff --git a/tests/reliability/test_final_acceptance.py b/tests/reliability/test_final_acceptance.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..6441eab6a1cbd0db08e687f54d3b7b0ee0b9ae60
    --- /dev/null
    +++ b/tests/reliability/test_final_acceptance.py
    @@ -0,0 +1,195 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis.commands.request import CommandRequest
    +from miniredis.config import MiniRedisConfig
    +from miniredis.core.commit import PutEntry
    +from miniredis.core.reply import Bytes
    +from miniredis.persistence.aof import AofPolicy, load_aof
    +from miniredis.persistence.codec import decode_snapshot_file
    +from miniredis.persistence.snapshot import SnapshotSaved
    +from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
    +from tests.helpers.runtime import open_test_runtime
    +
    +
    +OWNER_FIELDS = (
    +    "accepted_requests",
    +    "aof_tasks",
    +    "control_producers",
    +    "executor_tasks",
    +    "owned_tasks",
    +    "pending_futures",
    +    "replica_links",
    +    "replica_tasks",
    +    "sessions",
    +    "snapshot_jobs",
    +    "subscriptions",
    +    "tcp_servers",
    +    "tcp_sessions",
    +    "tcp_tasks",
    +    "timer_handles",
    +    "waiters",
    +)
    +
    +
    +def assert_zero_owners(runtime) -> None:
    +    stats = runtime.debug_stats()
    +    observed = {field: getattr(stats, field) for field in OWNER_FIELDS}
    +    assert observed == dict.fromkeys(OWNER_FIELDS, 0)
    +
    +
    +def command_wire(*parts: bytes) -> bytes:
    +    return (
    +        b"*"
    +        + str(len(parts)).encode("ascii")
    +        + b"\r\n"
    +        + b"".join(
    +            b"$" + str(len(part)).encode("ascii") + b"\r\n" + part + b"\r\n"
    +            for part in parts
    +        )
    +    )
    +
    +
    +async def send(
    +    writer: asyncio.StreamWriter,
    +    *parts: bytes,
    +) -> None:
    +    writer.write(command_wire(*parts))
    +    await writer.drain()
    +
    +
    +async def expect(
    +    reader: asyncio.StreamReader,
    +    wire: bytes,
    +) -> None:
    +    assert await reader.readexactly(len(wire)) == wire
    +
    +
    +@pytest.mark.asyncio
    +async def test_final_acceptance_activates_components_then_leaves_no_owners(
    +    tmp_path,
    +):
    +    aof_path = tmp_path / "appendonly.mraof"
    +    snapshot_path = tmp_path / "dump.mrsnap"
    +    primary = await open_test_runtime(
    +        config=MiniRedisConfig(
    +            aof_path=aof_path,
    +            aof_policy=AofPolicy.ALWAYS,
    +            snapshot_path=snapshot_path,
    +            replica_drain_grace_ms=1000,
    +        ),
    +        snapshot_write_gate=True,
    +    )
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=8)
    +    await primary.attach_replica(sink)
    +    server = await primary.start_tcp("127.0.0.1", 0)
    +
    +    blpop_reader, blpop_writer = await asyncio.open_connection(*server.address)
    +    command_reader, command_writer = await asyncio.open_connection(*server.address)
    +    sub_reader, sub_writer = await asyncio.open_connection(*server.address)
    +    writers = (blpop_writer, command_writer, sub_writer)
    +    await primary.debug_wait_for_sessions(3)
    +
    +    await send(blpop_writer, b"BLPOP", b"queue", b"5")
    +    await primary.debug_wait_for_waiters(1)
    +    blocked = primary.debug_stats()
    +    assert blocked.accepted_requests == 1
    +    assert blocked.pending_futures == 1
    +    assert blocked.timer_handles == 1
    +    assert blocked.waiters == 1
    +    assert blocked.sessions == 3
    +    await send(command_writer, b"RPUSH", b"queue", b"item")
    +    await expect(command_reader, b":1\r\n")
    +    await expect(
    +        blpop_reader,
    +        b"*2\r\n$5\r\nqueue\r\n$4\r\nitem\r\n",
    +    )
    +
    +    await send(command_writer, b"SET", b"replicated", b"durable")
    +    await expect(command_reader, b"+OK\r\n")
    +
    +    await send(sub_writer, b"SUBSCRIBE", b"news")
    +    await expect(
    +        sub_reader,
    +        b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n",
    +    )
    +    await send(command_writer, b"PUBLISH", b"news", b"payload")
    +    await expect(command_reader, b":1\r\n")
    +    await expect(
    +        sub_reader,
    +        b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$7\r\npayload\r\n",
    +    )
    +
    +    saving = asyncio.create_task(primary.save_snapshot())
    +    await primary.debug_snapshot_write_entered.wait()
    +    active = primary.debug_stats()
    +    primary.debug_snapshot_write_release.set()
    +    saved = await saving
    +    assert isinstance(saved, SnapshotSaved)
    +
    +    assert active.accepting_users is True
    +    assert active.accepted_requests == 0
    +    assert active.aof_tasks >= 1
    +    assert active.control_producers >= 1
    +    assert active.executor_tasks == 1
    +    assert active.pending_futures == 0
    +    assert active.replica_links == 1
    +    assert active.replica_tasks == 1
    +    assert active.sessions == 3
    +    assert active.snapshot_jobs == 1
    +    assert active.subscriptions == 1
    +    assert active.tcp_servers == 1
    +    assert active.tcp_sessions == 3
    +    assert active.tcp_tasks >= 6
    +    assert active.timer_handles == 0
    +    assert active.waiters == 0
    +
    +    await sink.wait_until_applied(primary.debug_commit_seq)
    +    assert await replica.direct_client().execute(
    +        CommandRequest(b"GET", (b"replicated",))
    +    ) == Bytes(b"durable")
    +
    +    await primary.close()
    +    await primary.close()
    +    assert primary.closed
    +    assert server.closed
    +    assert server.owned_task_count == 0
    +    assert sink.status.state is ReplicaSinkState.STOPPED
    +    assert sink.owned_task_count == 0
    +
    +    for reader in (blpop_reader, command_reader, sub_reader):
    +        assert await asyncio.wait_for(reader.read(), timeout=1) == b""
    +    for writer in writers:
    +        writer.close()
    +        await writer.wait_closed()
    +
    +    batches = load_aof(aof_path, repair_truncated_tail=False)
    +    assert batches[-1].seq == primary.debug_commit_seq
    +    aof_entries = {
    +        operation.key: operation.entry
    +        for batch in batches
    +        for operation in batch.operations
    +        if isinstance(operation, PutEntry)
    +    }
    +    assert aof_entries[b"replicated"].value.data == b"durable"
    +    image = decode_snapshot_file(snapshot_path.read_bytes())
    +    assert image.checkpoint_seq == primary.debug_commit_seq
    +    assert dict(image.entries)[b"replicated"].value.data == b"durable"
    +
    +    await replica.close()
    +    assert primary.debug_stats().accepting_users is False
    +    assert replica.debug_stats().accepting_users is False
    +    assert_zero_owners(primary)
    +    assert_zero_owners(replica)
    +
    +    current = asyncio.current_task()
    +    leaked = [
    +        task
    +        for task in asyncio.all_tasks()
    +        if task is not current
    +        and not task.done()
    +        and task.get_name().startswith("miniredis")
    +    ]
    +    assert leaked == []
    ```

Activates TCP, BLPOP, AOF, snapshot, Pub/Sub, and replication together, then closes twice and asserts every owner field and named MiniRedis task reaches zero. A failure identifies a lifecycle owner that never settled.

??? note "File diff: tests/unit/commands/test_command_traits.py"
    ```diff
    diff --git a/tests/unit/commands/test_command_traits.py b/tests/unit/commands/test_command_traits.py
    index 26c6ae2110a3a499d517bfd4c0f0448ef96c5ac9..dc39bfc816d792fc1ab652a36e9155d4d8ede1bb 100644
    --- a/tests/unit/commands/test_command_traits.py
    +++ b/tests/unit/commands/test_command_traits.py
    @@ -16,7 +16,9 @@ def test_every_frozen_command_type_has_exactly_one_dataset_trait():


     def test_blpop_is_mutating_and_pubsub_is_explicitly_non_dataset():
    -    assert model.is_dataset_mutating(model.BlPop((b"q",), 0)) is True
    +    assert (
    +        model.is_dataset_mutating(model.BlockingPop((b"q",), 0, left=True)) is True
    +    )
         assert (
             model.is_dataset_mutating(model.Subscribe((b"c",))) is False
         )
    ```

Keeps the exhaustive read/write trait partition valid after `BlockingPop`, `MultiGet`, and `MultiSet` enter the command union.

??? note "File diff: tests/unit/commands/test_parser.py"
    ```diff
    diff --git a/tests/unit/commands/test_parser.py b/tests/unit/commands/test_parser.py
    index d0214580f53a32ddd1efcd64df7d9ae1450560f7..27846170c493ee7b1df68fa0561568affa8996f0 100644
    --- a/tests/unit/commands/test_parser.py
    +++ b/tests/unit/commands/test_parser.py
    @@ -14,6 +14,8 @@ from miniredis.commands.model import (
         HashGetAll,
         ListPop,
         ListPush,
    +    MultiGet,
    +    MultiSet,
         SetMembers,
         ZRemove,
         ZRangeByScore,
    @@ -66,6 +68,12 @@ def test_parse_set_rejects_invalid_entire_option_set(args: tuple[bytes, ...]) ->
         [
             (CommandRequest(b"EXISTS", (b"a", b"a")), Exists((b"a", b"a"))),
             (CommandRequest(b"INCRBY", (b"a", b"2")), Increment(b"a", 2)),
    +        (CommandRequest(b"DECR", (b"a",)), Increment(b"a", -1)),
    +        (CommandRequest(b"MGET", (b"a", b"b")), MultiGet((b"a", b"b"))),
    +        (
    +            CommandRequest(b"MSET", (b"a", b"1", b"b", b"2")),
    +            MultiSet(((b"a", b"1"), (b"b", b"2"))),
    +        ),
             (CommandRequest(b"HGETALL", (b"h",)), HashGetAll(b"h")),
             (
                 CommandRequest(b"LPUSH", (b"l", b"a", b"b")),
    @@ -90,6 +98,10 @@ def test_parse_representative_commands_return_exact_typed_command(
         "command_request",
         [
             CommandRequest(b"GET"),
    +        CommandRequest(b"MGET"),
    +        CommandRequest(b"MSET", (b"a",)),
    +        CommandRequest(b"MSET", (b"a", b"1", b"b")),
    +        CommandRequest(b"DECR"),
             CommandRequest(b"HSET", (b"h", b"f")),
             CommandRequest(b"LRANGE", (b"l", b"0")),
             CommandRequest(b"SADD", (b"s",)),
    ```

Locks exact typed parsing and invalid arity for MGET, MSET, and DECR. Odd MSET arguments must be rejected before planning.

### Basic concepts

MGET is an ordered observation over keys; MSET is one normalized state transition. Duplicate MSET keys use last-value-wins before commit construction. A `BlockingPop` freezes keys, deadline, and direction. Acceptance ownership counts are terminal invariants, not performance metrics.

### Why this mechanism is necessary

Bulk APIs exist to express one semantic operation, not save client syntax. Planning them together preserves atomicity and one commit sequence. Carrying pop direction in the typed command and waiter prevents the immediate and deferred paths from drifting. Whole-runtime acceptance catches leaks that isolated feature tests cannot see.

### Runtime mental model

The parser creates `MultiGet`, `MultiSet`, `Increment(-1)`, or `BlockingPop(left=...)`. Planning walks MGET keys in input order, normalizes MSET pairs before one operation tuple, and chooses the correct list end. If no item exists, the waiter stores that same direction until a push produces wake-up operations.

### Mechanism blocks

#### Atomic bulk string commands

Model and parse MGET, MSET, and DECR, then plan ordered reads and one-commit duplicate-normalized writes.

??? note "File diff: src/miniredis/commands/model.py"
    ```diff
    diff --git a/src/miniredis/commands/model.py b/src/miniredis/commands/model.py
    index a35ef7c39779d6045e3909f2f07df78b544b4b96..176bec4afa7c6434ac106bb0dcb4bdb85d65012e 100644
    --- a/src/miniredis/commands/model.py
    +++ b/src/miniredis/commands/model.py
    @@ -27,6 +27,16 @@ class GetString:
         key: bytes


    +@dataclass(frozen=True, slots=True)
    +class MultiGet:
    +    keys: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class MultiSet:
    +    pairs: tuple[tuple[bytes, bytes], ...]
    +
    +
     @dataclass(frozen=True, slots=True)
     class Delete:
         keys: tuple[bytes, ...]
    @@ -99,9 +109,10 @@ class ListRange:


     @dataclass(frozen=True, slots=True)
    -class BlPop:
    +class BlockingPop:
         keys: tuple[bytes, ...]
         timeout_ms: int
    +    left: bool


     @dataclass(frozen=True, slots=True)
    @@ -214,6 +225,8 @@ Command: TypeAlias = (
         | Echo
         | SetString
         | GetString
    +    | MultiGet
    +    | MultiSet
         | Delete
         | Exists
         | TypeOf
    @@ -226,7 +239,7 @@ Command: TypeAlias = (
         | ListPush
         | ListPop
         | ListRange
    -    | BlPop
    +    | BlockingPop
         | Subscribe
         | Unsubscribe
         | Publish
    @@ -250,6 +263,7 @@ Command: TypeAlias = (
     _DATASET_MUTATING_TYPES = frozenset(
         {
             SetString,
    +        MultiSet,
             Delete,
             Increment,
             HashSet,
    @@ -257,7 +271,7 @@ _DATASET_MUTATING_TYPES = frozenset(
             HashIncrement,
             ListPush,
             ListPop,
    -        BlPop,
    +        BlockingPop,
             SetAdd,
             SetRemove,
             ZAdd,
    @@ -272,6 +286,7 @@ _NON_DATASET_MUTATING_TYPES = frozenset(
             Ping,
             Echo,
             GetString,
    +        MultiGet,
             Exists,
             TypeOf,
             HashGet,
    ```

Defines immutable bulk commands and replaces `BlPop` with direction-bearing `BlockingPop`; exhaustive traits classify MGET read-only and MSET mutating.

??? note "File diff: src/miniredis/commands/parser.py"
    ```diff
    diff --git a/src/miniredis/commands/parser.py b/src/miniredis/commands/parser.py
    index af71f2f3a5175c76c446a13aa9c967bb33c77091..b8d38cdebd5149134e591b4b663bf2fc395454f0 100644
    --- a/src/miniredis/commands/parser.py
    +++ b/src/miniredis/commands/parser.py
    @@ -11,7 +11,7 @@ from decimal import (
     from typing import Literal

     from miniredis.commands.model import (
    -    BlPop,
    +    BlockingPop,
         Command,
         Delete,
         Echo,
    @@ -27,6 +27,8 @@ from miniredis.commands.model import (
         ListPop,
         ListPush,
         ListRange,
    +    MultiGet,
    +    MultiSet,
         Persist,
         Ping,
         Publish,
    @@ -181,11 +183,22 @@ def parse_request(request: CommandRequest) -> Command:
             case b"GET":
                 _require_arity(name, args, 1)
                 return GetString(args[0])
    +        case b"MGET":
    +            _require_min_arity(name, args, 1)
    +            return MultiGet(args)
    +        case b"MSET":
    +            _require_min_arity(name, args, 2)
    +            if len(args) % 2 != 0:
    +                raise CommandParseError("wrong number of arguments for MSET")
    +            return MultiSet(_byte_pairs(args))
             case b"SET":
                 return _parse_set(args)
             case b"INCR":
                 _require_arity(name, args, 1)
                 return Increment(args[0], 1)
    +        case b"DECR":
    +            _require_arity(name, args, 1)
    +            return Increment(args[0], -1)
             case b"INCRBY":
                 _require_arity(name, args, 2)
                 return Increment(args[0], parse_int64(args[1]))
    @@ -215,7 +228,7 @@ def parse_request(request: CommandRequest) -> Command:
             case b"LRANGE":
                 _require_arity(name, args, 3)
                 return ListRange(args[0], parse_int64(args[1]), parse_int64(args[2]))
    -        case b"BLPOP":
    +        case b"BLPOP" | b"BRPOP":
                 if len(args) < 2:
                     raise CommandParseError("wrong number of arguments")
                 raw_timeout = args[-1]
    @@ -236,7 +249,11 @@ def parse_request(request: CommandRequest) -> Command:
                 if not timeout_ms.is_finite() or timeout_ms > _MAX_BLPOP_TIMEOUT_MS:
                     raise CommandParseError("timeout is out of range")
                 milliseconds = int(timeout_ms.to_integral_value(rounding=ROUND_CEILING))
    -            return BlPop(tuple(args[:-1]), milliseconds)
    +            return BlockingPop(
    +                tuple(args[:-1]),
    +                milliseconds,
    +                left=name == b"BLPOP",
    +            )
             case b"SUBSCRIBE":
                 if not args:
                     raise CommandParseError("wrong number of arguments")
    ```

Validates arity/pairs and maps BLPOP/BRPOP into one command with `left=name == b"BLPOP"`.

??? note "File diff: src/miniredis/core/planning.py"
    ```diff
    diff --git a/src/miniredis/core/planning.py b/src/miniredis/core/planning.py
    index 527ed614b6dc95788368088598b5b27b78c15bab..6641aabca7e5bef9d02df61ecce431119fa6722c 100644
    --- a/src/miniredis/core/planning.py
    +++ b/src/miniredis/core/planning.py
    @@ -19,7 +19,7 @@ from miniredis.core.commit import (
     from miniredis.core.database import Database, Entry, freeze_value
     from miniredis.core.executor import ExecutionPlan
     from miniredis.core.expiration import expiry_delete, is_expired
    -from miniredis.core.reply import Bytes, Failure, Number, Ok
    +from miniredis.core.reply import Bytes, Failure, Items, Number, Ok, Reply
     from miniredis.core.values import (
         HashValue,
         ListValue,
    @@ -165,6 +165,29 @@ def plan_general_and_strings(
                 if not isinstance(entry.value, StringValue):
                     return ExecutionPlan(WRONGTYPE)
                 return ExecutionPlan(Bytes(entry.value.data), expired, (key,))
    +        case cmd.MultiGet(keys):
    +            touches: list[bytes] = []
    +            replies: list[Reply] = []
    +            for key in keys:
    +                entry, _expired = lookup(database, key, now_ms)
    +                if entry is None or not isinstance(entry.value, StringValue):
    +                    replies.append(Bytes(None))
    +                else:
    +                    replies.append(Bytes(entry.value.data))
    +                    touches.append(key)
    +            return ExecutionPlan(Items(tuple(replies)), (), tuple(touches))
    +        case cmd.MultiSet(pairs):
    +            final_values: dict[bytes, bytes] = {}
    +            for key, value in pairs:
    +                final_values[key] = value
    +            operations: list[CommitOperation] = []
    +            for key, value in final_values.items():
    +                previous, expired = lookup(database, key, now_ms)
    +                operations.extend(expired)
    +                operations.append(
    +                    make_put(key, StringValue(value), previous, None)
    +                )
    +            return ExecutionPlan(Ok(), tuple(operations))
             case cmd.SetString(key, value, only_if, expire_ms):
                 previous, expired = lookup(database, key, now_ms)
                 if only_if == "nx" and previous is not None:
    ```

Preserves MGET order and duplicates, treats missing/non-string values as null, and collapses MSET duplicates before returning one `ExecutionPlan`.

#### Direction-preserving blocking pop

Unify BLPOP and BRPOP under one typed direction flag and preserve that choice through immediate execution and deferred wake-up.

??? note "File diff: src/miniredis/core/planner.py"
    ```diff
    diff --git a/src/miniredis/core/planner.py b/src/miniredis/core/planner.py
    index 02eafc09d88365aeaa7fcaa3212dec01213713c1..9f408c8bd2c9cefb5b0275c86dabc0d60c363ba5 100644
    --- a/src/miniredis/core/planner.py
    +++ b/src/miniredis/core/planner.py
    @@ -1,6 +1,6 @@
     from collections import deque

    -from miniredis.commands.model import BlPop
    +from miniredis.commands.model import BlockingPop
     from miniredis.commands.model import Command
     from miniredis.config import MiniRedisConfig
     from miniredis.core.commit import (
    @@ -49,9 +49,9 @@ class CommandPlanner:
                 return enforce_memory(plan, database, self.config, now_ms)
             return ExecutionPlan(Failure("ERR", "unknown command"))

    -    def plan_blpop_now(
    +    def plan_blocking_pop_now(
             self,
    -        command: BlPop,
    +        command: BlockingPop,
             database: Database,
             now_ms: int,
         ) -> ExecutionPlan | None:
    @@ -71,7 +71,7 @@ class CommandPlanner:
                 if not entry.value.items:
                     continue
                 items = deque(entry.value.items)
    -            item = items.popleft()
    +            item = items.popleft() if command.left else items.pop()
                 if items:
                     operation: CommitOperation = PutEntry(
                         key,
    ```

Uses `popleft()` or `pop()` from the frozen direction during the immediate blocking-pop scan.

??? note "File diff: src/miniredis/core/blocking.py"
    ```diff
    diff --git a/src/miniredis/core/blocking.py b/src/miniredis/core/blocking.py
    index d1e91622d737f0a4fb8248560aecfa6ede624245..ed2b95e0cbe7eaf8e71d885594c2bb0205418bd3 100644
    --- a/src/miniredis/core/blocking.py
    +++ b/src/miniredis/core/blocking.py
    @@ -43,6 +43,7 @@ class BlockingWaiter:
         session_id: int
         keys: tuple[bytes, ...]
         deadline_ms: int | None
    +    left: bool
         state: WaiterState = WaiterState.ACTIVE
         timer: CancelHandle | None = None

    @@ -69,9 +70,16 @@ class WaiterRegistry:
             session_id: int,
             keys: tuple[bytes, ...],
             deadline_ms: int | None,
    +        left: bool,
         ) -> BlockingWaiter:
             waiter = BlockingWaiter(
    -            WaiterId(self._next_id), 1, token, session_id, keys, deadline_ms
    +            WaiterId(self._next_id),
    +            1,
    +            token,
    +            session_id,
    +            keys,
    +            deadline_ms,
    +            left,
             )
             self._next_id += 1
             self._by_id[waiter.waiter_id] = waiter
    @@ -183,7 +191,7 @@ def prepare_list_wakeups(
                     waiter.waiter_id,
                     waiter.generation,
                     key,
    -                remaining.popleft(),
    +                remaining.popleft() if waiter.left else remaining.pop(),
                 )
             )
         if remaining:
    ```

Stores `left` in `BlockingWaiter`, so deferred wake-up removes from the same side chosen at admission.

??? note "File diff: src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index 8c0b2622ca8adda611fb12a49e15406458a25593..1fee9e7bec7c7922ab9f4737be61cddca73c93bc 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -9,7 +9,7 @@ from typing import TYPE_CHECKING, Protocol

     from miniredis.clock import Clock, TimerScheduler
     from miniredis.commands.model import (
    -    BlPop,
    +    BlockingPop,
         Command,
         ListPush,
         Ping,
    @@ -608,8 +608,12 @@ class CommandExecutor:
                 return

             now_ms = self.clock.now_ms()
    -        if isinstance(command, BlPop):
    -            plan = self.planner.plan_blpop_now(command, self.database, now_ms)
    +        if isinstance(command, BlockingPop):
    +            plan = self.planner.plan_blocking_pop_now(
    +                command,
    +                self.database,
    +                now_ms,
    +            )
                 if plan is None:
                     deadline = (
                         None if command.timeout_ms == 0 else now_ms + command.timeout_ms
    @@ -619,6 +623,7 @@ class CommandExecutor:
                         request.session_id,
                         command.keys,
                         deadline,
    +                    command.left,
                     )
                     if waiter.deadline_ms is not None:
                         waiter.timer = self.scheduler.call_at_ms(
    ```

Registers a waiter only after the direction-aware immediate plan returns no item, forwarding keys, deadline, and direction as one ownership record.

#### Whole-runtime acceptance observability

Expose ownership counts across adapters, durability, replication, and runtime so the full-system acceptance test can prove zero leaks.

??? note "File diff: src/miniredis/adapters/direct.py"
    ```diff
    diff --git a/src/miniredis/adapters/direct.py b/src/miniredis/adapters/direct.py
    index cb5930cca661407b0f0cd4d50f699760fd38c388..cd8ec423f3756a293837509790c3cca3a560d2df 100644
    --- a/src/miniredis/adapters/direct.py
    +++ b/src/miniredis/adapters/direct.py
    @@ -4,7 +4,7 @@ import asyncio
     from typing import TYPE_CHECKING

     from miniredis.commands.request import CommandRequest
    -from miniredis.commands.model import BlPop
    +from miniredis.commands.model import BlockingPop
     from miniredis.core.executor import (
         AbandonRequest,
         SessionClosed,
    @@ -71,7 +71,7 @@ class DirectClient:
                     return Failure("CLOSED", "runtime closed")
                 case RuntimeClosed():
                     return Failure("CLOSED", "runtime closed before reply")
    -            case TransportClosed() if isinstance(parsed, BlPop):
    +            case TransportClosed() if isinstance(parsed, BlockingPop):
                     return Bytes(None)
                 case TransportClosed():
                     return Failure("CLOSED", "session closed")
    ```

??? note "File diff: src/miniredis/persistence/aof.py"
    ```diff
    diff --git a/src/miniredis/persistence/aof.py b/src/miniredis/persistence/aof.py
    index a14ead1a5a0d3becde9249d4de5dd67830caa2b6..2fc237a340657b585e2cd7f364f2826faac41aa2 100644
    --- a/src/miniredis/persistence/aof.py
    +++ b/src/miniredis/persistence/aof.py
    @@ -163,6 +163,13 @@ class AofWriter:
         def failure(self) -> BaseException | None:
             return self._failure

    +    @property
    +    def owned_task_count(self) -> int:
    +        return sum(
    +            task is not None and not task.done()
    +            for task in (self._worker, self._sync_task)
    +        )
    +
         async def start(self) -> None:
             if self._worker is not None:
                 return
    @@ -261,9 +268,7 @@ class AofWriter:

         def _writer_done(self, task: asyncio.Task[None]) -> None:
             if task.cancelled():
    -            error: BaseException | None = RuntimeError(
    -                "AOF writer task was cancelled"
    -            )
    +            error: BaseException | None = RuntimeError("AOF writer task was cancelled")
             else:
                 error = task.exception()
             if error is None:
    @@ -353,11 +358,7 @@ class AofWriter:
                         await self._sync_task
                     except asyncio.CancelledError:
                         pass
    -        if (
    -            self._policy is AofPolicy.EVERYSEC
    -            and self._dirty
    -            and self._failure is None
    -        ):
    +        if self._policy is AofPolicy.EVERYSEC and self._dirty and self._failure is None:
                 try:
                     await self._sync_dirty()
                 except BaseException as exc:
    ```

??? note "File diff: src/miniredis/replication/sink.py"
    ```diff
    diff --git a/src/miniredis/replication/sink.py b/src/miniredis/replication/sink.py
    index 184e3509d4d81289aab17372ed283f4ab62c68eb..68d29ac442aed2f97f169024bc16d7a42361c971 100644
    --- a/src/miniredis/replication/sink.py
    +++ b/src/miniredis/replication/sink.py
    @@ -89,6 +89,13 @@ class ReplicaSink:
                 queued=len(self._queue),
             )

    +    @property
    +    def owned_task_count(self) -> int:
    +        return sum(
    +            task is not None and not task.done()
    +            for task in (self._attach_task, self._task)
    +        )
    +
         def pause(self) -> None:
             self._apply_allowed.clear()

    ```

??? note "File diff: src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index dab3ad3adc40ea8861197322e15ef9453fd6d8b8..cf78913c77efef0c5ce568148ed7b2e0adfbeee0 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -72,6 +72,13 @@ class RuntimeStats:
         replica_links: int
         accepting_users: bool
         snapshot_jobs: int
    +    aof_tasks: int
    +    control_producers: int
    +    executor_tasks: int
    +    replica_tasks: int
    +    tcp_servers: int
    +    tcp_sessions: int
    +    tcp_tasks: int


     @dataclass(slots=True)
    @@ -684,6 +691,9 @@ class MiniRedis:
             return self.executor.accepted_tokens

         def debug_stats(self) -> RuntimeStats:
    +        servers = tuple(self._tcp_servers)
    +        sinks = tuple(self._owned_replica_sinks)
    +        worker = self.executor.worker_task
             return RuntimeStats(
                 accepted_requests=self.executor.accepted_request_count,
                 pending_futures=self.executor.pending_request_count,
    @@ -702,6 +712,15 @@ class MiniRedis:
                     self._snapshot_manager is not None
                     and self._snapshot_manager.active_job is not None
                 ),
    +            aof_tasks=(
    +                0 if self._aof_writer is None else self._aof_writer.owned_task_count
    +            ),
    +            control_producers=len(self._control_producers),
    +            executor_tasks=int(worker is not None and not worker.done()),
    +            replica_tasks=sum(sink.owned_task_count for sink in sinks),
    +            tcp_servers=len(servers),
    +            tcp_sessions=sum(server.session_count for server in servers),
    +            tcp_tasks=sum(server.owned_task_count for server in servers),
             )

         def _debug_notify(self) -> None:
    ```

Direct, AOF, replica sink, and runtime surfaces expose their owned task/link/session counts. They do not change command semantics; they let final acceptance prove shutdown convergence across components.

#### Acceptance test scaffold

Adjust the test configuration used by the whole-runtime acceptance contract without treating project metadata as a runtime mechanism.

??? note "File diff: pyproject.toml"
    ```diff
    diff --git a/pyproject.toml b/pyproject.toml
    index 5b06675d2886e7456b9578561e28a0256b03e2f5..3e5a906ada1af60b9fec36110e2374abee0a1171 100644
    --- a/pyproject.toml
    +++ b/pyproject.toml
    @@ -23,7 +23,7 @@ packages = ["src/miniredis"]
     asyncio_mode = "auto"
     asyncio_default_fixture_loop_scope = "function"
     asyncio_default_test_loop_scope = "function"
    -pythonpath = ["src"]
    +pythonpath = ["src", "."]
     testpaths = ["tests"]
     markers = [
         "interop: redis-py RESP2 smoke with client metadata disabled",
    ```

This metadata adjusts the acceptance-test boundary. It is grouped separately because it does not explain the runtime mechanism introduced by MGET/MSET/DECR/BRPOP.

### Verification evidence

Run all five focused modules in `tests.txt`, cumulatively build Stages 1–21, and require owned-tree parity with `40d00de`.

### Durable takeaways

- MSET is one normalized commit, not repeated SET.
- MGET preserves input position, including duplicates and nulls.
- Blocking direction belongs to waiter state.
- Full acceptance must prove zero owners after close.

### Explain it in your own words

Why does MSET normalize duplicate keys before creating commit operations, and why must BRPOP store direction after its initial scan fails?

### Textbook

Bulk commands demonstrate transaction granularity inside a single-writer state machine. Direction-bearing waiters show continuation state: deferred execution must retain every semantic choice required to resume correctly.

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/5419f99...40d00de)

After finishing, run `python -m journey.tools.build_journey check 21` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/21-bulk-and-directional-pop/stage.patch)

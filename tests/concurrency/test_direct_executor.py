from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import FrozenInstanceError
from typing import Any

import pytest

import miniredis.core.executor as executor_module
from miniredis import CommandRequest, MiniRedis, MiniRedisConfig, RuntimeState
from miniredis.commands.model import Command, Ping
from miniredis.core.commit import PutEntry, StoredEntry, StoredString
from miniredis.core.database import Database
from miniredis.core.executor import ExecutionPlan
from miniredis.core.reply import Bytes, Failure, Ok


class SentinelFailure(RuntimeError):
    pass


class FailingPlanner:
    def __init__(self, failure: SentinelFailure) -> None:
        self.failure = failure
        self.entered = asyncio.Event()

    def plan(self, database: Database, command: Command, now_ms: int) -> ExecutionPlan:
        del database, command, now_ms
        self.entered.set()
        raise self.failure


class PutPlanner:
    def plan(self, database: Database, command: Command, now_ms: int) -> ExecutionPlan:
        del database, command, now_ms
        return ExecutionPlan(
            Ok(b"planned"),
            operations=(
                PutEntry(
                    b"key",
                    StoredEntry(
                        StoredString(b"value"), expire_at_ms=None, mutation_version=1
                    ),
                ),
            ),
        )


class FailingBarrier:
    def __init__(self, failure: SentinelFailure) -> None:
        self.failure = failure
        self.entered = asyncio.Event()

    async def append(self, batch: object) -> None:
        del batch
        self.entered.set()
        raise self.failure


class GatedFailingBarrier(FailingBarrier):
    def __init__(self, failure: SentinelFailure) -> None:
        super().__init__(failure)
        self.release = asyncio.Event()

    async def append(self, batch: object) -> None:
        del batch
        self.entered.set()
        await self.release.wait()
        raise self.failure


def test_config_defaults_are_frozen_and_reject_invalid_bounds_and_policy() -> None:
    config = MiniRedisConfig()

    assert config.max_pending_commands == 1024
    assert config.active_expire_sample_size == 20
    assert config.maxmemory is None
    assert config.eviction_policy == "noeviction"
    with pytest.raises(FrozenInstanceError):
        config.max_pending_commands = 1  # type: ignore[misc]

    for options in (
        {"max_pending_commands": 0},
        {"active_expire_sample_size": 0},
        {"maxmemory": 0},
        {"eviction_policy": "volatile-lru"},
    ):
        with pytest.raises(ValueError):
            MiniRedisConfig(**options)  # type: ignore[arg-type]


def test_open_rejects_config_mixed_with_keyword_options() -> None:
    with pytest.raises(TypeError):
        MiniRedis.open(MiniRedisConfig(), max_pending_commands=1)


@pytest.mark.asyncio
async def test_runtime_context_executes_ping_and_binary_echo_then_closes() -> None:
    runtime = MiniRedis.open()

    async with runtime:
        assert runtime.state is RuntimeState.RUNNING
        client = runtime.direct_client()
        assert await client.execute(CommandRequest(b"PING")) == Ok(b"PONG")
        assert await client.execute(
            CommandRequest(b"ECHO", (b"\x00binary\xff",))
        ) == Bytes(b"\x00binary\xff")
        await client.close()
        assert client.closed is True
        await client.close()
        assert client.closed is True

    assert runtime.state is RuntimeState.CLOSED
    await runtime.close()
    assert runtime.state is RuntimeState.CLOSED


@pytest.mark.asyncio
async def test_execute_maps_inactive_client_and_parse_errors() -> None:
    runtime = MiniRedis.open()
    client = runtime.direct_client()

    assert await client.execute(CommandRequest(b"PING")) == Failure(
        "CLOSED", "runtime is closed"
    )
    await runtime.start()
    assert await client.execute(CommandRequest(b"PING", (b"a", b"b"))) == Failure(
        "ERR", "wrong number of arguments for PING"
    )

    await client.close()
    assert await client.execute(CommandRequest(b"PING")) == Failure(
        "CLOSED", "client is closed"
    )
    await runtime.close()


@pytest.mark.asyncio
async def test_bounded_admission_returns_busy_without_blocking_control_close() -> None:
    runtime = MiniRedis.open(max_pending_commands=1)
    await runtime.start()
    runtime.debug_pause_executor()
    client = runtime.direct_client()

    first = asyncio.create_task(client.execute(CommandRequest(b"PING")))
    await runtime.debug_wait_accepted_at_least(1)

    assert await client.execute(CommandRequest(b"PING")) == Failure(
        "BUSY", "command queue is full"
    )

    runtime.debug_resume_executor()
    assert await first == Ok(b"PONG")
    await runtime.close()
    assert runtime.state is RuntimeState.CLOSED


@pytest.mark.asyncio
async def test_concurrent_close_is_idempotent() -> None:
    runtime = MiniRedis.open()
    await runtime.start()

    await asyncio.gather(runtime.close(), runtime.close(), runtime.close())

    assert runtime.state is RuntimeState.CLOSED


@pytest.mark.asyncio
async def test_planner_failure_terminally_cleans_all_accepted_requests() -> None:
    failure = SentinelFailure("planner failed")
    planner = FailingPlanner(failure)
    runtime = MiniRedis.open(max_pending_commands=3)
    runtime.executor.planner = planner
    await runtime.start()
    runtime.debug_pause_executor()
    client = runtime.direct_client()
    callers = tuple(
        asyncio.create_task(client.execute(CommandRequest(b"PING"))) for _ in range(3)
    )
    await runtime.debug_wait_accepted_at_least(3)

    runtime.debug_resume_executor()
    await planner.entered.wait()
    outcomes = await asyncio.gather(*callers)

    assert runtime.state is RuntimeState.CLOSED
    assert outcomes == [Failure("CLOSED", "runtime closed before reply")] * 3
    assert runtime.executor.debug_failure is failure
    assert runtime.executor.debug_accepted_count == 0
    assert runtime.executor.mailbox.pending_users == 0
    assert runtime.executor.mailbox.pending_items == 0
    assert runtime.executor.mailbox.post_control(object()) is False
    assert await client.execute(CommandRequest(b"PING")) == Failure(
        "CLOSED", "runtime is closed"
    )
    await runtime.close()
    await runtime.close()
    assert runtime.state is RuntimeState.CLOSED


@pytest.mark.asyncio
async def test_barrier_failure_does_not_apply_and_terminally_cleans_runtime() -> None:
    failure = SentinelFailure("barrier failed")
    barrier = FailingBarrier(failure)
    runtime = MiniRedis._for_test(max_pending_commands=2, commit_barrier=barrier)
    runtime.executor.planner = PutPlanner()
    await runtime.start()
    runtime.debug_pause_executor()
    client = runtime.direct_client()
    callers = tuple(
        asyncio.create_task(client.execute(CommandRequest(b"PING"))) for _ in range(2)
    )
    await runtime.debug_wait_accepted_at_least(2)

    runtime.debug_resume_executor()
    await barrier.entered.wait()
    outcomes = await asyncio.gather(*callers)

    assert runtime.state is RuntimeState.CLOSED
    assert outcomes == [Failure("CLOSED", "runtime closed before reply")] * 2
    assert runtime.database.commit_seq == 0
    assert runtime.database.logical_items() == ()
    assert runtime.executor.debug_failure is failure
    assert runtime.executor.debug_accepted_count == 0
    assert runtime.executor.mailbox.pending_users == 0
    assert runtime.executor.mailbox.pending_items == 0
    assert runtime.executor.mailbox.post_control(object()) is False
    assert await client.execute(CommandRequest(b"PING")) == Failure(
        "CLOSED", "runtime is closed"
    )
    await runtime.close()
    await runtime.close()


@pytest.mark.asyncio
async def test_cancelled_close_waiter_does_not_abandon_failure_cleanup() -> None:
    failure = SentinelFailure("gated barrier failed")
    barrier = GatedFailingBarrier(failure)
    runtime = MiniRedis._for_test(max_pending_commands=1, commit_barrier=barrier)
    runtime.executor.planner = PutPlanner()
    await runtime.start()
    client = runtime.direct_client()
    caller = asyncio.create_task(client.execute(CommandRequest(b"PING")))
    await barrier.entered.wait()

    close_waiter = asyncio.create_task(runtime.close())
    await runtime.executor.mailbox.wait_items_at_least(1)
    assert runtime.state is RuntimeState.DRAINING
    close_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_waiter

    barrier.release.set()
    assert await caller == Failure("CLOSED", "runtime closed before reply")
    await runtime.close()

    assert runtime.state is RuntimeState.CLOSED
    assert runtime.executor.debug_failure is failure
    assert runtime.executor.debug_accepted_count == 0
    assert runtime.executor.mailbox.pending_users == 0
    assert runtime.executor.mailbox.pending_items == 0
    assert runtime.executor.mailbox.post_control(object()) is False


@pytest.mark.asyncio
async def test_owned_worker_cancellation_terminally_cleans_runtime() -> None:
    runtime = MiniRedis.open(max_pending_commands=3)
    await runtime.start()
    runtime.debug_pause_executor()
    client = runtime.direct_client()
    callers = tuple(
        asyncio.create_task(client.execute(CommandRequest(b"PING"))) for _ in range(3)
    )
    await runtime.debug_wait_accepted_at_least(3)
    worker = runtime.executor._worker_task
    assert worker is not None
    cancellation_marker = object()

    assert worker.cancel(cancellation_marker) is True
    outcomes = await asyncio.gather(*callers)

    assert runtime.state is RuntimeState.CLOSED
    assert outcomes == [Failure("CLOSED", "runtime closed before reply")] * 3
    failure = runtime.executor.debug_failure
    assert isinstance(failure, asyncio.CancelledError)
    assert failure.args == (cancellation_marker,)
    assert worker.cancelled() is False
    assert runtime.executor.debug_accepted_count == 0
    assert runtime.executor.mailbox.pending_users == 0
    assert runtime.executor.mailbox.pending_items == 0
    assert runtime.executor.mailbox.admit_user(object()) is False
    assert runtime.executor.mailbox.post_control(object()) is False
    assert await client.execute(CommandRequest(b"PING")) == Failure(
        "CLOSED", "runtime is closed"
    )
    await runtime.close()
    await runtime.close()
    assert runtime.state is RuntimeState.CLOSED


@pytest.mark.asyncio
async def test_start_handshake_rejects_submit_while_worker_is_cancelling() -> None:
    runtime = MiniRedis.open()
    await runtime.start()
    worker = runtime.executor._worker_task
    assert worker is not None
    cancellation_marker = object()

    assert worker.cancel(cancellation_marker) is True
    submitted = runtime.executor.submit(session_id=1, command=Ping())

    assert submitted == Failure("CLOSED", "runtime is closed")
    await worker
    assert runtime.state is RuntimeState.CLOSED
    failure = runtime.executor.debug_failure
    assert isinstance(failure, asyncio.CancelledError)
    assert failure.args == (cancellation_marker,)
    assert runtime.executor.debug_accepted_count == 0
    assert runtime.executor.mailbox.pending_users == 0
    assert runtime.executor.mailbox.pending_items == 0
    assert runtime.executor.mailbox.admit_user(object()) is False
    assert runtime.executor.mailbox.post_control(object()) is False
    await runtime.close()
    await runtime.close()
    assert runtime.state is RuntimeState.CLOSED


@pytest.mark.asyncio
async def test_pre_entry_worker_cancellation_is_owned_by_done_supervision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = MiniRedis.open()
    client = runtime.direct_client()
    worker_created = asyncio.Event()
    worker_entry_gate = asyncio.Event()
    original_create_task = asyncio.create_task

    def controlled_create_task(
        coroutine: Coroutine[Any, Any, None],
        *,
        name: str | None = None,
        context: Any = None,
    ) -> asyncio.Task[None]:
        if name != "miniredis:executor":
            return original_create_task(coroutine, name=name, context=context)

        async def delayed_worker_entry() -> None:
            worker_created.set()
            try:
                await worker_entry_gate.wait()
            except asyncio.CancelledError:
                coroutine.close()
                raise
            await coroutine

        return original_create_task(delayed_worker_entry(), name=name, context=context)

    monkeypatch.setattr(executor_module.asyncio, "create_task", controlled_create_task)
    first_start = original_create_task(runtime.start())
    await worker_created.wait()
    worker = runtime.executor._worker_task
    assert worker is not None
    assert runtime.executor._worker_entered is False

    first_start.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_start
    cancellation_marker = object()
    assert worker.cancel(cancellation_marker) is True
    await runtime.executor._worker_started_or_done.wait()
    monkeypatch.setattr(executor_module.asyncio, "create_task", original_create_task)

    assert runtime.state is RuntimeState.CLOSED
    failure = runtime.executor.debug_failure
    assert isinstance(failure, asyncio.CancelledError)
    assert failure.args == (cancellation_marker,)
    assert runtime.executor.debug_accepted_count == 0
    assert runtime.executor.mailbox.pending_users == 0
    assert runtime.executor.mailbox.pending_items == 0
    assert runtime.executor.mailbox.admit_user(object()) is False
    assert runtime.executor.mailbox.post_control(object()) is False
    with pytest.raises(RuntimeError, match="runtime is closed"):
        await runtime.start()
    assert await client.execute(CommandRequest(b"PING")) == Failure(
        "CLOSED", "runtime is closed"
    )
    await runtime.close()
    await runtime.close()
    assert runtime.state is RuntimeState.CLOSED


@pytest.mark.asyncio
async def test_cancelled_start_waiter_does_not_orphan_runtime_starting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = MiniRedis.open()
    client = runtime.direct_client()
    worker_created = asyncio.Event()
    worker_entry_gate = asyncio.Event()
    original_create_task = asyncio.create_task

    def controlled_create_task(
        coroutine: Coroutine[Any, Any, None],
        *,
        name: str | None = None,
        context: Any = None,
    ) -> asyncio.Task[None]:
        if name != "miniredis:executor":
            return original_create_task(coroutine, name=name, context=context)

        async def delayed_worker_entry() -> None:
            worker_created.set()
            try:
                await worker_entry_gate.wait()
            except asyncio.CancelledError:
                coroutine.close()
                raise
            await coroutine

        return original_create_task(delayed_worker_entry(), name=name, context=context)

    monkeypatch.setattr(executor_module.asyncio, "create_task", controlled_create_task)
    first_start = original_create_task(runtime.start())
    await worker_created.wait()
    worker = runtime.executor._worker_task
    assert worker is not None
    assert runtime.executor._worker_entered is False

    first_start.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_start
    worker_entry_gate.set()
    await runtime.executor._worker_started_or_done.wait()
    monkeypatch.setattr(executor_module.asyncio, "create_task", original_create_task)

    assert runtime.state is RuntimeState.RUNNING
    live_workers = tuple(
        task
        for task in asyncio.all_tasks()
        if task.get_name() == "miniredis:executor" and not task.done()
    )
    assert live_workers == (worker,)
    assert await client.execute(CommandRequest(b"PING")) == Ok(b"PONG")
    await runtime.close()
    assert runtime.state is RuntimeState.CLOSED
    assert runtime.executor.debug_accepted_count == 0
    assert runtime.executor.mailbox.pending_users == 0
    assert runtime.executor.mailbox.pending_items == 0


@pytest.mark.asyncio
async def test_concurrent_start_waiters_share_startup_when_one_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = MiniRedis.open()
    worker_created = asyncio.Event()
    worker_entry_gate = asyncio.Event()
    original_create_task = asyncio.create_task

    def controlled_create_task(
        coroutine: Coroutine[Any, Any, None],
        *,
        name: str | None = None,
        context: Any = None,
    ) -> asyncio.Task[None]:
        if name != "miniredis:executor":
            return original_create_task(coroutine, name=name, context=context)

        async def delayed_worker_entry() -> None:
            worker_created.set()
            try:
                await worker_entry_gate.wait()
            except asyncio.CancelledError:
                coroutine.close()
                raise
            await coroutine

        return original_create_task(delayed_worker_entry(), name=name, context=context)

    monkeypatch.setattr(executor_module.asyncio, "create_task", controlled_create_task)
    first_start = original_create_task(runtime.start())
    second_start = original_create_task(runtime.start())
    await worker_created.wait()
    worker = runtime.executor._worker_task
    assert worker is not None

    first_start.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_start
    worker_entry_gate.set()
    await second_start
    monkeypatch.setattr(executor_module.asyncio, "create_task", original_create_task)

    assert runtime.state is RuntimeState.RUNNING
    live_workers = tuple(
        task
        for task in asyncio.all_tasks()
        if task.get_name() == "miniredis:executor" and not task.done()
    )
    assert live_workers == (worker,)
    await runtime.close()
    assert runtime.state is RuntimeState.CLOSED

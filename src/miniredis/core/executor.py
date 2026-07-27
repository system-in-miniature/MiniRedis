from __future__ import annotations

import asyncio
from bisect import bisect_right
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from miniredis.clock import Clock
from miniredis.commands.model import Command
from miniredis.core.commit import CommitBatch, CommitOperation, CommitTrigger
from miniredis.core.database import Database
from miniredis.core.expiration import expiry_delete, is_expired
from miniredis.core.mailbox import EventLoopMailbox
from miniredis.core.reply import Failure, Reply


@dataclass(frozen=True, slots=True)
class RequestToken:
    value: int


@dataclass(slots=True)
class ExecuteRequest:
    token: RequestToken
    session_id: int
    command: Command
    future: asyncio.Future[RequestOutcome]


@dataclass(frozen=True, slots=True)
class SubmittedRequest:
    token: RequestToken
    future: asyncio.Future[RequestOutcome]


@dataclass(frozen=True, slots=True)
class Replied:
    reply: Reply


@dataclass(frozen=True, slots=True)
class RuntimeClosed:
    pass


type RequestOutcome = Replied | RuntimeClosed


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    reply: Reply | None
    operations: tuple[CommitOperation, ...] = ()
    touch_keys: tuple[bytes, ...] = ()
    trigger: CommitTrigger = CommitTrigger.CLIENT


class CommitBarrier(Protocol):
    async def append(self, batch: CommitBatch) -> None: ...


class NullCommitBarrier:
    async def append(self, batch: CommitBatch) -> None:
        del batch


class Planner(Protocol):
    def plan(
        self, command: Command, database: Database, now_ms: int
    ) -> ExecutionPlan: ...


@dataclass(frozen=True, slots=True)
class _StopExecutor:
    pass


@dataclass(slots=True)
class ActiveExpireTick:
    now_ms: int
    future: asyncio.Future[int] | None = None


type ExecutorMessage = ExecuteRequest | ActiveExpireTick | _StopExecutor


class CommandExecutor:
    def __init__(
        self,
        *,
        database: Database,
        planner: Planner,
        clock: Clock,
        commit_barrier: CommitBarrier,
        max_pending_commands: int,
        active_expire_sample_size: int = 20,
        on_terminal_failure: Callable[[BaseException], None] | None = None,
    ) -> None:
        self.database = database
        self.planner = planner
        self.clock = clock
        self.commit_barrier = commit_barrier
        self.max_pending_commands = max_pending_commands
        if active_expire_sample_size <= 0:
            raise ValueError("active_expire_sample_size must be positive")
        self.active_expire_sample_size = active_expire_sample_size
        self._active_expire_cursor: bytes | None = None
        self.mailbox: EventLoopMailbox[ExecutorMessage] = EventLoopMailbox(
            max_pending_commands
        )
        self._on_terminal_failure = on_terminal_failure

        self._worker_task: asyncio.Task[None] | None = None
        self._worker_started_or_done = asyncio.Event()
        self._worker_entered = False
        self._close_task: asyncio.Task[None] | None = None
        self._run_gate = asyncio.Event()
        self._run_gate.set()
        self._next_token = 0
        self._accepted: dict[RequestToken, asyncio.Future[RequestOutcome]] = {}
        self._accepted_changed = asyncio.Event()
        self._applied_batches: list[CommitBatch] = []
        self._failure: BaseException | None = None
        self._terminal_cleanup_complete = False
        self._stopping = False
        self._started = False

    async def start(self) -> None:
        if self._started:
            if self._stopping:
                raise RuntimeError("executor is stopping")
            await self._worker_started_or_done.wait()
            return
        if self._stopping:
            raise RuntimeError("executor is stopping")
        self._started = True
        self._worker_task = asyncio.create_task(self._run(), name="miniredis:executor")
        self._worker_task.add_done_callback(self._on_worker_done)
        await self._worker_started_or_done.wait()

    def _on_worker_done(self, task: asyncio.Task[None]) -> None:
        try:
            if not self._worker_entered:
                try:
                    task.result()
                except asyncio.CancelledError as error:
                    self._complete_terminal_failure(error)
                except Exception as error:  # noqa: BLE001 - startup is terminal
                    self._complete_terminal_failure(error)
        finally:
            self._worker_started_or_done.set()

    def submit(self, session_id: int, command: Command) -> SubmittedRequest | Failure:
        if (
            not self._started
            or self._stopping
            or (self._worker_task is not None and self._worker_task.cancelling() != 0)
            or (self._worker_task is not None and self._worker_task.done())
        ):
            return Failure("CLOSED", "runtime is closed")
        if len(self._accepted) >= self.max_pending_commands:
            return Failure("BUSY", "command queue is full")

        self._next_token += 1
        token = RequestToken(self._next_token)
        future: asyncio.Future[RequestOutcome] = (
            asyncio.get_running_loop().create_future()
        )
        request = ExecuteRequest(token, session_id, command, future)
        if not self.mailbox.admit_user(request):
            return Failure("CLOSED", "runtime is closed")
        self._accepted[token] = future
        self._accepted_changed.set()
        return SubmittedRequest(token, future)

    def post_control(self, message: ActiveExpireTick | _StopExecutor) -> bool:
        return self.mailbox.post_control(message)

    async def _run(self) -> None:
        failure: BaseException | None = None
        self._worker_entered = True
        self._worker_started_or_done.set()
        try:
            while True:
                message = await self.mailbox.take()
                await self._run_gate.wait()
                if isinstance(message, _StopExecutor):
                    return
                if isinstance(message, ExecuteRequest):
                    await self._execute(message)
                elif isinstance(message, ActiveExpireTick):
                    deleted = await self._active_expire_once(message.now_ms)
                    if message.future is not None and not message.future.done():
                        message.future.set_result(deleted)
        except asyncio.CancelledError as error:
            failure = error
        except Exception as error:  # noqa: BLE001 - worker failures are terminal
            failure = error
        finally:
            if failure is not None:
                self._complete_terminal_failure(failure)
            else:
                for token in tuple(self._accepted):
                    self._finish(token, RuntimeClosed())

    def _complete_terminal_failure(self, failure: BaseException) -> None:
        if self._terminal_cleanup_complete:
            return
        self._terminal_cleanup_complete = True
        self._failure = failure
        self._stopping = True
        self.mailbox.close_user_admission()
        self.mailbox.drain()
        for token in tuple(self._accepted):
            self._finish(token, RuntimeClosed())
        self.mailbox.close_control_admission()
        if self._on_terminal_failure is not None:
            self._on_terminal_failure(failure)

    async def _execute(self, request: ExecuteRequest) -> None:
        now_ms = self.clock.now_ms()
        plan = self.planner.plan(request.command, self.database, now_ms)
        if plan.operations:
            batch = CommitBatch(
                seq=self.database.commit_seq + 1,
                operations=plan.operations,
                trigger=plan.trigger,
            )
            await self.commit_barrier.append(batch)
            self.database.apply_batch(
                batch, track_access=plan.trigger is CommitTrigger.CLIENT
            )
            self._applied_batches.append(batch)

        for key in dict.fromkeys(plan.touch_keys):
            self.database.touch_if_live(key, now_ms)

        if plan.reply is None:
            raise AssertionError("Phase 1 execution plan requires a reply")
        self._finish(request.token, Replied(plan.reply))

    async def active_expire_once(self) -> int:
        if self._worker_task is None or self._stopping:
            return 0
        future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        tick = ActiveExpireTick(self.clock.now_ms(), future)
        if not self.post_control(tick):
            return 0
        return await asyncio.shield(future)

    async def _active_expire_once(self, now_ms: int) -> int:
        keys = sorted(
            key
            for key, entry in self.database.entries.items()
            if entry.expire_at_ms is not None
        )
        if not keys:
            self._active_expire_cursor = None
            return 0
        start = (
            0
            if self._active_expire_cursor is None
            else bisect_right(keys, self._active_expire_cursor)
        )
        ordered_keys = keys[start:] + keys[:start]
        candidate_keys = ordered_keys[: self.active_expire_sample_size]
        self._active_expire_cursor = candidate_keys[-1]
        operations = tuple(
            expiry_delete(key)
            for key in candidate_keys
            if is_expired(self.database.entries[key], now_ms)
        )
        if not operations:
            return 0
        batch = CommitBatch(
            self.database.commit_seq + 1,
            operations,
            CommitTrigger.ACTIVE_EXPIRE,
        )
        await self.commit_barrier.append(batch)
        self.database.apply_batch(batch, track_access=False)
        self._applied_batches.append(batch)
        return len(operations)

    def _finish(self, token: RequestToken, outcome: RequestOutcome) -> None:
        future = self._accepted.pop(token, None)
        if future is not None and not future.done():
            future.set_result(outcome)
        self._accepted_changed.set()

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close_once(), name="miniredis:executor-close"
            )
        await asyncio.shield(self._close_task)

    async def _close_once(self) -> None:
        self._stopping = True
        self.mailbox.close_user_admission()
        self._run_gate.set()
        try:
            if self._worker_task is not None and not self._worker_task.done():
                self.post_control(_StopExecutor())
                await self._worker_task
        except Exception as error:  # noqa: BLE001 - close must finish cleanup
            if self._failure is None:
                self._failure = error
        finally:
            self.mailbox.drain()
            for token in tuple(self._accepted):
                self._finish(token, RuntimeClosed())
            self.mailbox.close_control_admission()

    def debug_pause(self) -> None:
        self._run_gate.clear()

    def debug_resume(self) -> None:
        self._run_gate.set()

    async def debug_wait_accepted_at_least(self, count: int) -> None:
        while len(self._accepted) < count:
            self._accepted_changed.clear()
            if len(self._accepted) >= count:
                return
            await self._accepted_changed.wait()

    @property
    def debug_accepted_count(self) -> int:
        return len(self._accepted)

    @property
    def debug_failure(self) -> BaseException | None:
        return self._failure

    def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
        return tuple(self._applied_batches)
